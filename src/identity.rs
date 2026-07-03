//! Local identity and keystore: the per-machine keypair, signing, and on-disk state.
//!
//! Owns everything under `~/.config/conclave` (`Constant::CONFIG_DIR_NAME`): the machine's own
//! Ed25519 keypair (the 32-byte seed wrapped in a `secrecy` `SecretBox`, never logged), and the
//! local `config.toml` — known-server registrations (username + machine name) and the permission
//! config (default level + per-`(server, channel)` / whisper overrides, DESIGN.md §9).
//!
//! Auth is challenge-response: the machine signs a server-issued nonce and the server resolves the
//! public key to a `(user, machine)` (DESIGN.md §5). Keys are generated from OS entropy via `ring`'s
//! system RNG and the key pair is reconstructed from the seed on demand, so the private seed is only
//! ever materialized transiently for a signature.

use std::{
    fmt, fs,
    path::{Path, PathBuf},
};

use anyhow::Context as _;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::{
    rand::{SecureRandom as _, SystemRandom},
    signature::{self, Ed25519KeyPair, KeyPair as _, UnparsedPublicKey},
};
use secrecy::{
    ExposeSecret as _, SecretBox,
    zeroize::{Zeroize as _, Zeroizing},
};
use serde::{Deserialize, Serialize};

use crate::{
    base::{Constant, PermissionLevel, Res, SessionPath, Void},
    protocol::ProtocolError,
};

/// Byte lengths of an Ed25519 private seed, public key, and signature.
const SEED_LEN: usize = 32;
const PUBLIC_KEY_LEN: usize = 32;
const SIGNATURE_LEN: usize = 64;

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
    /// The username is on the admin allowlist pinned to a different key (anti-squat, PRD-0007 §7).
    #[error("username `{0}` is reserved for a different key")]
    Reserved(String),
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
    secret_seed: SecretBox<[u8; SEED_LEN]>,
    public_key: [u8; PUBLIC_KEY_LEN],
}

impl Identity {
    /// Generates a fresh identity from operating-system entropy.
    ///
    /// # Errors
    ///
    /// Returns an error if the OS random source cannot be read or the seed is rejected.
    pub fn generate() -> Res<Self> {
        // The generated seed is zeroized on drop; `from_seed` zeroizes the copy it takes (#28).
        let mut seed = Zeroizing::new([0_u8; SEED_LEN]);
        SystemRandom::new().fill(&mut *seed).map_err(|_| anyhow::anyhow!("failed to gather entropy"))?;
        Self::from_seed(*seed)
    }

    /// Reconstructs an identity from its 32-byte seed.
    ///
    /// # Errors
    ///
    /// Returns an error if the seed is rejected by the signing backend.
    pub fn from_seed(mut seed: [u8; SEED_LEN]) -> Res<Self> {
        let key_pair = Ed25519KeyPair::from_seed_unchecked(&seed).map_err(|e| anyhow::anyhow!("invalid Ed25519 seed: {e}"))?;
        let public_key = key_pair.public_key().as_ref().try_into().context("unexpected public key length")?;

        let identity = Self {
            secret_seed: SecretBox::new(Box::new(seed)),
            public_key,
        };
        // The `SecretBox` holds the only resident copy — wipe the plaintext argument (#28).
        seed.zeroize();
        Ok(identity)
    }

    /// This identity's raw 32-byte public key.
    #[must_use]
    pub fn public_key(&self) -> [u8; PUBLIC_KEY_LEN] {
        self.public_key
    }

    /// This identity's public key as a URL-safe base64 string, for display and pasting.
    #[must_use]
    pub fn public_key_base64(&self) -> String {
        encode_key(&self.public_key)
    }

    /// Signs `message` (e.g. a server challenge nonce), returning the 64-byte signature.
    ///
    /// # Errors
    ///
    /// Returns an error if the signing key cannot be reconstructed from the seed.
    pub fn sign(&self, message: &[u8]) -> Res<[u8; SIGNATURE_LEN]> {
        // Reconstruct the key pair transiently so the private seed is not held resident beyond signing.
        let key_pair = Ed25519KeyPair::from_seed_unchecked(self.secret_seed.expose_secret()).map_err(|e| anyhow::anyhow!("failed to load signing key: {e}"))?;
        key_pair.sign(message).as_ref().try_into().context("unexpected signature length")
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
    if public_key.len() != PUBLIC_KEY_LEN {
        return Err(AuthError::Malformed("public key must be 32 bytes".to_owned()));
    }
    if signature.len() != SIGNATURE_LEN {
        return Err(AuthError::Malformed("signature must be 64 bytes".to_owned()));
    }

    UnparsedPublicKey::new(&signature::ED25519, public_key).verify(message, signature).map_err(|_| AuthError::BadSignature)
}

/// Generates a fresh random challenge nonce (server-side, DESIGN.md §5).
///
/// # Errors
///
/// Returns an error if the OS random source cannot be read.
pub fn generate_challenge() -> Res<[u8; Constant::CHALLENGE_SIZE]> {
    let mut nonce = [0_u8; Constant::CHALLENGE_SIZE];
    SystemRandom::new().fill(&mut nonce).map_err(|_| anyhow::anyhow!("failed to gather entropy"))?;
    Ok(nonce)
}

/// Mints a fresh, opaque invite token: 24 bytes of OS entropy, URL-safe base64 (DESIGN.md §6).
///
/// # Errors
///
/// Returns an error if the OS random source cannot be read.
pub fn generate_token() -> Res<String> {
    let mut bytes = [0_u8; 24];
    SystemRandom::new().fill(&mut bytes).map_err(|_| anyhow::anyhow!("failed to gather entropy"))?;
    Ok(encode_key(&bytes))
}

/// Encodes raw key bytes as a URL-safe base64 string — the canonical on-wire / on-disk key
/// encoding, symmetric with [`decode_key`]. The server stores machine public keys this way.
#[must_use]
pub fn encode_key(bytes: &[u8]) -> String {
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
    use std::io::Write as _;

    fs::create_dir_all(dir).with_context(|| format!("failed to create keystore directory `{}`", dir.display()))?;

    let key_file = dir.join("key");
    // Create with owner-only mode from the start so the seed is never briefly world-readable (#33);
    // the base64 seed is wiped once written (#28).
    let seed_b64 = Zeroizing::new(identity.secret_seed_base64());
    let mut file = create_key_file(&key_file)?;
    file.write_all(seed_b64.as_bytes()).with_context(|| format!("failed to write keyfile `{}`", key_file.display()))?;
    // Belt-and-suspenders: fix the mode if the file already existed with looser permissions.
    restrict_permissions(&key_file)?;

    Ok(())
}

/// Opens `dir/key` for writing, creating it with owner-only (0600) permissions on unix so the seed
/// is never written through a world-readable file (PRD-0008 T-007, #33).
#[cfg(unix)]
fn create_key_file(path: &Path) -> Res<fs::File> {
    use std::os::unix::fs::OpenOptionsExt as _;
    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("failed to create keyfile `{}`", path.display()))
}

#[cfg(not(unix))]
fn create_key_file(path: &Path) -> Res<fs::File> {
    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("failed to create keyfile `{}`", path.display()))
}

/// Loads the identity whose seed is stored at `dir/key`.
///
/// # Errors
///
/// Returns an error if the keyfile is missing or does not contain a valid 32-byte seed.
pub fn load_identity(dir: &Path) -> Res<Identity> {
    let key_file = dir.join("key");
    // Wipe every plaintext copy of the seed material on the way in (#28).
    let contents = Zeroizing::new(fs::read_to_string(&key_file).with_context(|| format!("failed to read keyfile `{}` (run `conclave key` first)", key_file.display()))?);

    let mut seed_bytes = decode_key(&contents)?;
    let mut seed: [u8; SEED_LEN] = seed_bytes.as_slice().try_into().context("keyfile does not contain a 32-byte seed")?;
    seed_bytes.zeroize();

    let identity = Identity::from_seed(seed);
    seed.zeroize();
    identity
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
    atomic_write(&path, &text)?;

    Ok(())
}

/// Writes `contents` to `path` atomically: a per-process temp file in the same directory, then a
/// rename. A crash mid-write can only truncate the temp — the live file is replaced in one atomic
/// step, so it is never left partial to brick every later verb (PRD-0008 T-006, #26).
fn atomic_write(path: &Path, contents: &str) -> Void {
    let name = path.file_name().and_then(std::ffi::OsStr::to_str).unwrap_or("conclave");
    let tmp = path.with_file_name(format!("{name}.{}.tmp", std::process::id()));
    fs::write(&tmp, contents).with_context(|| format!("failed to write temp file `{}`", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("failed to replace `{}`", path.display()))?;
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

        let signature = identity.sign(&challenge).unwrap();

        verify(&identity.public_key(), &challenge, &signature).unwrap();
    }

    #[test]
    fn verification_rejects_a_wrong_key() {
        let signer = Identity::generate().unwrap();
        let impostor = Identity::generate().unwrap();
        let challenge = generate_challenge().unwrap();
        let signature = signer.sign(&challenge).unwrap();

        assert_eq!(verify(&impostor.public_key(), &challenge, &signature), Err(AuthError::BadSignature));
    }

    #[test]
    fn verification_rejects_a_tampered_message_or_signature() {
        let identity = Identity::generate().unwrap();
        let challenge = generate_challenge().unwrap();
        let mut signature = identity.sign(&challenge).unwrap();

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
        let signature = identity.sign(&challenge).unwrap();

        assert!(matches!(verify(&[0_u8; 8], &challenge, &signature), Err(AuthError::Malformed(_))));
    }

    #[test]
    fn debug_never_reveals_the_secret_seed() {
        let seed = [7_u8; SEED_LEN];
        let identity = Identity::from_seed(seed).unwrap();
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
    fn invite_tokens_are_random_and_url_safe() {
        let a = generate_token().unwrap();
        let b = generate_token().unwrap();
        assert_ne!(a, b);
        assert!(!a.is_empty());
        // URL-safe base64 (no `+`, `/`, or `=` padding).
        assert!(a.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'), "token is not URL-safe: {a}");
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
        verify(&loaded.public_key(), &challenge, &loaded.sign(&challenge).unwrap()).unwrap();
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
    fn save_config_atomic_round_trips_and_leaves_no_temp() {
        let dir = TempDir::new().unwrap();
        let config = Config {
            default_permission: PermissionLevel::Converse,
            ..Config::default()
        };

        save_config(dir.path(), &config).unwrap();
        assert_eq!(load_config(dir.path()).unwrap(), config);

        // The atomic temp file was renamed away — no `.tmp` residue is left to confuse later runs.
        let residue: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
            .collect();
        assert!(residue.is_empty(), "atomic write must leave no temp file: {residue:?}");

        // Overwriting an existing config replaces it completely (no truncation / partial merge).
        let updated = Config {
            default_permission: PermissionLevel::Act,
            ..config
        };
        save_config(dir.path(), &updated).unwrap();
        assert_eq!(load_config(dir.path()).unwrap(), updated);
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
