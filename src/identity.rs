//! Local identity and keystore: the per-machine keypair, signing, and on-disk state.
//!
//! Owns everything under `~/.config/conclave` (`Constant::CONFIG_DIR_NAME`): the machine's own
//! Ed25519 keypair (the 32-byte seed wrapped in a `secrecy` `SecretBox`, never logged), and the
//! local `config.toml` — known-server registrations (username + machine name) and the permission
//! config (default level + per-`(server, channel)` / whisper overrides, DESIGN.md §9).
//!
//! Auth is challenge-response: the machine signs a server-issued nonce and the server resolves the
//! public key to a `(user, machine)` (DESIGN.md §5). Keys are generated from OS entropy via
//! `getrandom` and reconstructed from the seed on demand, so the private scalar is only ever
//! materialized transiently for a signature.

use std::{
    fmt, fs,
    path::{Path, PathBuf},
};

use anyhow::Context as _;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ed25519_dalek::{PUBLIC_KEY_LENGTH, SECRET_KEY_LENGTH, SIGNATURE_LENGTH, Signature, Signer as _, SigningKey, Verifier as _, VerifyingKey};
use secrecy::{ExposeSecret as _, SecretBox};
use serde::{Deserialize, Serialize};

use crate::{
    base::{Constant, PermissionLevel, Res, SessionPath, Void},
    protocol::ProtocolError,
};

/// Errors at the authentication / identity boundary (DESIGN.md §16), matched on by the server.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    /// The presented public key is not enrolled on the server.
    #[error("unknown machine key")]
    UnknownKey,
    /// The presented key has been revoked (lost-laptop kill switch, DESIGN.md §5.1).
    #[error("machine key has been revoked")]
    RevokedKey,
    /// The signature did not verify against the challenge and public key.
    #[error("signature verification failed")]
    BadSignature,
    /// A key or signature was the wrong length or otherwise unparseable.
    #[error("malformed key or signature: {0}")]
    Malformed(String),
    /// The requested username is already claimed on this server.
    #[error("username `{0}` is already taken")]
    UsernameTaken(String),
    /// The session handle collides with a live session for this `(user, machine)` (DESIGN.md §5).
    #[error("session handle `{0}` collides with a live session")]
    HandleCollision(String),
}

impl From<AuthError> for ProtocolError {
    fn from(err: AuthError) -> Self {
        let message = err.to_string();
        match err {
            AuthError::Malformed(_) => Self::MalformedFrame(message),
            _ => Self::Unauthorized(message),
        }
    }
}

/// This machine's Ed25519 identity: a `secrecy`-wrapped seed plus the derived public key.
///
/// The seed never leaves the [`SecretBox`] except transiently to sign or to be persisted to the
/// (0600) keyfile; the [`fmt::Debug`] impl redacts it.
pub struct Identity {
    secret_seed: SecretBox<[u8; SECRET_KEY_LENGTH]>,
    verifying_key: VerifyingKey,
}

impl Identity {
    /// Generates a fresh identity from operating-system entropy.
    ///
    /// # Errors
    ///
    /// Returns an error if the OS random source cannot be read.
    pub fn generate() -> Res<Self> {
        let mut seed = [0_u8; SECRET_KEY_LENGTH];
        getrandom::getrandom(&mut seed).map_err(|e| anyhow::anyhow!("failed to gather entropy: {e}"))?;
        Ok(Self::from_seed(seed))
    }

    /// Reconstructs an identity from its 32-byte seed.
    #[must_use]
    pub fn from_seed(seed: [u8; SECRET_KEY_LENGTH]) -> Self {
        let signing = SigningKey::from_bytes(&seed);
        let verifying_key = signing.verifying_key();

        Self {
            secret_seed: SecretBox::new(Box::new(seed)),
            verifying_key,
        }
    }

    /// This identity's raw 32-byte public key.
    #[must_use]
    pub fn public_key(&self) -> [u8; PUBLIC_KEY_LENGTH] {
        self.verifying_key.to_bytes()
    }

    /// This identity's public key as a URL-safe base64 string, for display and pasting.
    #[must_use]
    pub fn public_key_base64(&self) -> String {
        encode_key(&self.public_key())
    }

    /// Signs `message` (e.g. a server challenge nonce), returning the 64-byte signature.
    #[must_use]
    pub fn sign(&self, message: &[u8]) -> [u8; SIGNATURE_LENGTH] {
        // Reconstruct the signing key transiently so the private scalar is not held resident.
        let signing = SigningKey::from_bytes(self.secret_seed.expose_secret());
        signing.sign(message).to_bytes()
    }

    /// The base64 seed, materialized only to persist the keyfile.
    fn secret_seed_base64(&self) -> String {
        encode_key(self.secret_seed.expose_secret())
    }
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity").field("public_key", &self.public_key_base64()).field("secret_seed", &"<redacted>").finish()
    }
}

/// Verifies `signature` over `message` against the raw `public_key`.
///
/// # Errors
///
/// Returns [`AuthError::Malformed`] if the key or signature is the wrong length, or
/// [`AuthError::BadSignature`] if verification fails.
pub fn verify(public_key: &[u8], message: &[u8], signature: &[u8]) -> Result<(), AuthError> {
    let key_bytes: [u8; PUBLIC_KEY_LENGTH] = public_key.try_into().map_err(|_| AuthError::Malformed("public key must be 32 bytes".to_owned()))?;
    let verifying = VerifyingKey::from_bytes(&key_bytes).map_err(|e| AuthError::Malformed(e.to_string()))?;

    let signature_bytes: [u8; SIGNATURE_LENGTH] = signature.try_into().map_err(|_| AuthError::Malformed("signature must be 64 bytes".to_owned()))?;
    let signature = Signature::from_bytes(&signature_bytes);

    verifying.verify(message, &signature).map_err(|_| AuthError::BadSignature)
}

/// Generates a fresh random challenge nonce (server-side, DESIGN.md §5).
///
/// # Errors
///
/// Returns an error if the OS random source cannot be read.
pub fn generate_challenge() -> Res<[u8; Constant::CHALLENGE_SIZE]> {
    let mut nonce = [0_u8; Constant::CHALLENGE_SIZE];
    getrandom::getrandom(&mut nonce).map_err(|e| anyhow::anyhow!("failed to gather entropy: {e}"))?;
    Ok(nonce)
}

fn encode_key(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Decodes a URL-safe base64 key string back to bytes.
///
/// # Errors
///
/// Returns [`AuthError::Malformed`] if the string is not valid base64.
pub fn decode_key(text: &str) -> Result<Vec<u8>, AuthError> {
    URL_SAFE_NO_PAD.decode(text.trim()).map_err(|e| AuthError::Malformed(e.to_string()))
}

// ---------------------------------------------------------------------------
// On-disk local configuration (`~/.config/conclave/config.toml`).
// ---------------------------------------------------------------------------

/// This machine's registration on one server (DESIGN.md §5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerRegistration {
    /// The server URL.
    pub url: String,
    /// The username claimed on that server.
    pub username: String,
    /// The machine name this key is enrolled under.
    pub machine: String,
}

/// A local permission override, keyed by `(server, scope)` (DESIGN.md §9).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermissionOverride {
    /// The server the override applies to.
    pub server: String,
    /// The channel the override applies to; `None` denotes the whisper scope.
    #[serde(default)]
    pub channel: Option<String>,
    /// The autonomy level for that scope.
    pub level: PermissionLevel,
}

/// The scope a permission level resolves for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
    /// A named channel.
    Channel(String),
    /// The whisper (direct-message) scope.
    Whisper,
}

/// The local machine configuration: identity-adjacent state and the permission policy.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// The machine-wide default autonomy level (ships `notify`).
    #[serde(default)]
    pub default_permission: PermissionLevel,
    /// Known-server registrations.
    #[serde(default)]
    pub servers: Vec<ServerRegistration>,
    /// Per-`(server, scope)` overrides.
    #[serde(default)]
    pub overrides: Vec<PermissionOverride>,
}

impl Config {
    /// Resolves the autonomy level for a `(server, scope)`: a matching override, else the default.
    #[must_use]
    pub fn resolve_permission(&self, server: &str, scope: &Scope) -> PermissionLevel {
        let target_channel = match scope {
            Scope::Channel(name) => Some(name.as_str()),
            Scope::Whisper => None,
        };

        self.overrides
            .iter()
            .find(|o| o.server == server && o.channel.as_deref() == target_channel)
            .map_or(self.default_permission, |o| o.level)
    }
}

/// The default keystore / config directory, `~/.config/conclave` (`Constant::CONFIG_DIR_NAME`).
///
/// # Errors
///
/// Returns an error if the OS configuration directory cannot be determined.
pub fn default_config_dir() -> Res<PathBuf> {
    let base = dirs::config_dir().context("could not determine the OS configuration directory")?;
    Ok(base.join(Constant::CONFIG_DIR_NAME))
}

/// Writes the identity's seed to `dir/key` with owner-only (0600) permissions.
///
/// # Errors
///
/// Returns an error if the directory or file cannot be created or written.
pub fn save_identity(dir: &Path, identity: &Identity) -> Void {
    fs::create_dir_all(dir).with_context(|| format!("failed to create keystore directory `{}`", dir.display()))?;

    let key_file = dir.join("key");
    fs::write(&key_file, identity.secret_seed_base64()).with_context(|| format!("failed to write keyfile `{}`", key_file.display()))?;
    restrict_permissions(&key_file)?;

    Ok(())
}

/// Loads the identity whose seed is stored at `dir/key`.
///
/// # Errors
///
/// Returns an error if the keyfile is missing or does not contain a valid 32-byte seed.
pub fn load_identity(dir: &Path) -> Res<Identity> {
    let key_file = dir.join("key");
    let contents = fs::read_to_string(&key_file).with_context(|| format!("failed to read keyfile `{}` (run `conclave key` first)", key_file.display()))?;

    let seed_bytes = decode_key(&contents)?;
    let seed: [u8; SECRET_KEY_LENGTH] = seed_bytes.as_slice().try_into().context("keyfile does not contain a 32-byte seed")?;

    Ok(Identity::from_seed(seed))
}

/// Writes the local configuration to `dir/config.toml`.
///
/// # Errors
///
/// Returns an error if the config cannot be serialized or written.
pub fn save_config(dir: &Path, config: &Config) -> Void {
    fs::create_dir_all(dir).with_context(|| format!("failed to create config directory `{}`", dir.display()))?;

    let path = dir.join("config.toml");
    let text = toml::to_string_pretty(config).context("failed to serialize config")?;
    fs::write(&path, text).with_context(|| format!("failed to write config `{}`", path.display()))?;

    Ok(())
}

/// Loads the local configuration from `dir/config.toml`, returning the default if it is absent.
///
/// # Errors
///
/// Returns an error if the config file exists but cannot be read or parsed.
pub fn load_config(dir: &Path) -> Res<Config> {
    let path = dir.join("config.toml");
    if !path.exists() {
        return Ok(Config::default());
    }

    let text = fs::read_to_string(&path).with_context(|| format!("failed to read config `{}`", path.display()))?;
    toml::from_str(&text).with_context(|| format!("failed to parse config `{}`", path.display()))
}

#[cfg(unix)]
fn restrict_permissions(path: &Path) -> Void {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).with_context(|| format!("failed to restrict permissions on `{}`", path.display()))
}

#[cfg(not(unix))]
fn restrict_permissions(_path: &Path) -> Void {
    // Best-effort only; non-unix platforms rely on the user profile directory's ACLs.
    Ok(())
}

/// The default session handle for a working directory — its final path component (DESIGN.md §5).
#[must_use]
pub fn default_handle(working_dir: &Path) -> String {
    working_dir.file_name().map_or_else(|| "session".to_owned(), |name| name.to_string_lossy().into_owned())
}

/// Builds a full participant path from a registration and a session handle.
#[must_use]
pub fn session_path(registration: &ServerRegistration, handle: &str) -> SessionPath {
    SessionPath::new(registration.username.clone(), registration.machine.clone(), handle.to_owned())
}

#[cfg(test)]
mod tests {
    // Tests relax `unwrap_used` (house convention; DESIGN.md §22).
    #![allow(clippy::unwrap_used)]

    use super::*;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    #[test]
    fn signs_and_verifies_a_server_challenge() {
        let identity = Identity::generate().unwrap();
        let challenge = generate_challenge().unwrap();

        let signature = identity.sign(&challenge);

        verify(&identity.public_key(), &challenge, &signature).unwrap();
    }

    #[test]
    fn verification_rejects_a_wrong_key() {
        let signer = Identity::generate().unwrap();
        let impostor = Identity::generate().unwrap();
        let challenge = generate_challenge().unwrap();
        let signature = signer.sign(&challenge);

        assert_eq!(verify(&impostor.public_key(), &challenge, &signature), Err(AuthError::BadSignature));
    }

    #[test]
    fn verification_rejects_a_tampered_message_or_signature() {
        let identity = Identity::generate().unwrap();
        let challenge = generate_challenge().unwrap();
        let mut signature = identity.sign(&challenge);

        let mut tampered_challenge = challenge;
        tampered_challenge[0] ^= 0xFF;
        assert_eq!(verify(&identity.public_key(), &tampered_challenge, &signature), Err(AuthError::BadSignature));

        signature[0] ^= 0xFF;
        assert_eq!(verify(&identity.public_key(), &challenge, &signature), Err(AuthError::BadSignature));
    }

    #[test]
    fn verification_rejects_a_malformed_key() {
        let identity = Identity::generate().unwrap();
        let challenge = generate_challenge().unwrap();
        let signature = identity.sign(&challenge);

        assert!(matches!(verify(&[0_u8; 8], &challenge, &signature), Err(AuthError::Malformed(_))));
    }

    #[test]
    fn debug_never_reveals_the_secret_seed() {
        let seed = [7_u8; SECRET_KEY_LENGTH];
        let identity = Identity::from_seed(seed);
        let rendered = format!("{identity:?}");

        assert!(!rendered.contains(&encode_key(&seed)), "debug output leaked the secret seed: {rendered}");
        assert!(rendered.contains("redacted"));
        assert!(rendered.contains(&identity.public_key_base64()));
    }

    #[test]
    fn challenges_are_random() {
        assert_ne!(generate_challenge().unwrap(), generate_challenge().unwrap());
    }

    #[test]
    fn keystore_round_trips_the_identity() {
        let dir = TempDir::new().unwrap();
        let identity = Identity::generate().unwrap();

        save_identity(dir.path(), &identity).unwrap();
        let loaded = load_identity(dir.path()).unwrap();

        assert_eq!(loaded.public_key(), identity.public_key());

        // The reloaded key still produces verifiable signatures.
        let challenge = generate_challenge().unwrap();
        verify(&loaded.public_key(), &challenge, &loaded.sign(&challenge)).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn keyfile_is_owner_only() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = TempDir::new().unwrap();
        save_identity(dir.path(), &Identity::generate().unwrap()).unwrap();

        let mode = fs::metadata(dir.path().join("key")).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    #[test]
    fn permission_resolution_prefers_the_most_specific_override() {
        let config = Config {
            default_permission: PermissionLevel::Notify,
            servers: vec![],
            overrides: vec![
                PermissionOverride {
                    server: "s1".to_owned(),
                    channel: Some("ops".to_owned()),
                    level: PermissionLevel::Act,
                },
                PermissionOverride {
                    server: "s1".to_owned(),
                    channel: None,
                    level: PermissionLevel::Converse,
                },
            ],
        };

        assert_eq!(config.resolve_permission("s1", &Scope::Channel("ops".to_owned())), PermissionLevel::Act);
        assert_eq!(config.resolve_permission("s1", &Scope::Whisper), PermissionLevel::Converse);
        // No matching override falls back to the machine default.
        assert_eq!(config.resolve_permission("s1", &Scope::Channel("other".to_owned())), PermissionLevel::Notify);
        assert_eq!(config.resolve_permission("s2", &Scope::Channel("ops".to_owned())), PermissionLevel::Notify);
    }

    #[test]
    fn config_round_trips_through_toml_with_lowercase_levels() {
        let dir = TempDir::new().unwrap();
        let config = Config {
            default_permission: PermissionLevel::Notify,
            servers: vec![ServerRegistration {
                url: "wss://s1".to_owned(),
                username: "aaron".to_owned(),
                machine: "workstation".to_owned(),
            }],
            overrides: vec![PermissionOverride {
                server: "wss://s1".to_owned(),
                channel: Some("ops".to_owned()),
                level: PermissionLevel::Act,
            }],
        };

        save_config(dir.path(), &config).unwrap();
        let text = fs::read_to_string(dir.path().join("config.toml")).unwrap();
        assert!(text.contains("notify"), "levels should serialize lowercase: {text}");
        assert!(text.contains("act"));

        assert_eq!(load_config(dir.path()).unwrap(), config);
    }

    #[test]
    fn load_config_defaults_when_absent() {
        let dir = TempDir::new().unwrap();
        assert_eq!(load_config(dir.path()).unwrap(), Config::default());
    }

    #[test]
    fn auth_errors_map_onto_wire_protocol_errors() {
        assert!(matches!(ProtocolError::from(AuthError::BadSignature), ProtocolError::Unauthorized(_)));
        assert!(matches!(ProtocolError::from(AuthError::Malformed("x".to_owned())), ProtocolError::MalformedFrame(_)));
    }

    #[test]
    fn default_handle_uses_the_final_path_component() {
        assert_eq!(default_handle(Path::new("/home/aaron/projects/razel")), "razel");
    }
}
