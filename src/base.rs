//! Foundational constants, error aliases, and domain types shared across `conclavelib`.
//!
//! This module is the single source of truth for the small vocabulary every other
//! module speaks: the `anyhow` error aliases used for application glue, the
//! [`Constant`] home for magic values, and the core domain enums / paths from
//! DESIGN.md §5, §6, and §9. Wire-crossing boundary errors (`ProtocolError`,
//! `AuthError`, `AclError`) live in their respective modules, not here.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// A helper type for errors.
pub type Err = anyhow::Error;
/// A helper type for results.
pub type Res<T> = anyhow::Result<T, Err>;
/// A helper type for void results.
pub type Void = Res<()>;

/// Installs the process-default `rustls` crypto provider (aws-lc-rs) once, so the client's
/// `tokio_tungstenite::connect_async` can build a TLS config for a `wss://` server. rustls 0.23
/// cannot auto-select a provider when several are compiled in (aws-lc-rs via the store, ring via the
/// identity keys), so the client installs one explicitly before dialing. Idempotent (PRD-0009 T-001).
pub(crate) fn ensure_tls_provider() {
    use std::sync::Once;
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// A home for the project's magic values, so they are named once and reused.
pub struct Constant;

impl Constant {
    /// Size in bytes of a server-issued authentication challenge nonce (DESIGN.md §5).
    pub const CHALLENGE_SIZE: usize = 32;
    /// Name of the per-user configuration / keystore directory under the OS config
    /// dir (i.e. `~/.config/conclave`), where identity and permission state live.
    pub const CONFIG_DIR_NAME: &'static str = "conclave";
    /// Upper bound on a single decoded wire frame (16 MiB), rejecting a bogus length
    /// prefix before it can drive a large allocation.
    pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
    /// The wire protocol version negotiated at connect time. Peers advertising an
    /// incompatible version are rejected or upgraded (DESIGN.md §13).
    pub const PROTOCOL_VERSION: u32 = 1;
    /// The HTTP header on the WS upgrade response carrying the server's persistent instance ID,
    /// so a bridge can recognize the same server reached under two URLs (PRD-0012 T-003). Rides
    /// the upgrade — out-of-band of the wire protocol — so old peers are unaffected.
    pub const SERVER_ID_HEADER: &'static str = "x-conclave-server-id";
    /// The separator between the components of a [`SessionPath`] (`user/machine/session`).
    pub const SESSION_PATH_SEPARATOR: char = '/';
}

/// How much an inbound message may drive the *recipient's* agent (DESIGN.md §9).
///
/// This is a **local** autonomy policy, never sent to the server. Variants are
/// ordered by ascending autonomy, so a resolved level can be compared against a
/// threshold (e.g. "below [`PermissionLevel::Converse`] rejects outbound emit").
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionLevel {
    /// Delivery is suppressed entirely; the message is dropped on your side (lurk).
    Mute,
    /// Injected read-only: surface to the human, do not reply or act. The default.
    #[default]
    Notify,
    /// May reply / whisper in conversation, but not take side-effecting actions.
    Converse,
    /// May reply *and* act on the message.
    Act,
}

impl PermissionLevel {
    /// Whether the bridge will emit (`send` / `whisper`) on this channel's behalf.
    ///
    /// True at [`PermissionLevel::Converse`] and above; the call-time rejection of
    /// emits below `converse` (DESIGN.md §9) is expressed in terms of this.
    #[must_use]
    pub const fn may_emit(self) -> bool {
        matches!(self, Self::Converse | Self::Act)
    }
}

/// The error returned when a permission-level string is not a known level.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown permission level `{0}` (expected mute, notify, converse, or act)")]
pub struct ParsePermissionError(pub String);

impl FromStr for PermissionLevel {
    type Err = ParsePermissionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mute" => Ok(Self::Mute),
            "notify" => Ok(Self::Notify),
            "converse" => Ok(Self::Converse),
            "act" => Ok(Self::Act),
            other => Err(ParsePermissionError(other.to_owned())),
        }
    }
}

/// A channel's discovery / join visibility tier, stored on the channel record (DESIGN.md §6).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Visibility {
    /// Appears in discovery; anyone on the server may join.
    Public,
    /// Not listed, but joinable by anyone who knows the exact name ("secret-link").
    Unlisted,
    /// Not listed; join is authorized via ACL or invite token.
    Private,
}

impl Visibility {
    /// The lowercase wire / storage token for this tier (`public` / `unlisted` / `private`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::Unlisted => "unlisted",
            Self::Private => "private",
        }
    }
}

/// The error returned when a visibility string is not a known tier.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown visibility tier `{0}` (expected public, unlisted, or private)")]
pub struct ParseVisibilityError(pub String);

impl FromStr for Visibility {
    type Err = ParseVisibilityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "public" => Ok(Self::Public),
            "unlisted" => Ok(Self::Unlisted),
            "private" => Ok(Self::Private),
            other => Err(ParseVisibilityError(other.to_owned())),
        }
    }
}

/// A fully-qualified live participant path, `{user}/{machine}/{session}` (DESIGN.md §5).
///
/// Every message's sender is a full path, so a reply or whisper target is always
/// unambiguous. The `server` component that disambiguates a multi-homed session
/// (DESIGN.md §8) is carried alongside a path, not embedded in it.
#[derive(Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct SessionPath {
    /// The account name (unique per server).
    pub user: String,
    /// The enrolled machine (its own keypair) the session runs on.
    pub machine: String,
    /// The live-session handle (`--as`, defaulting to the repo/dir name).
    pub session: String,
}

impl SessionPath {
    /// Builds a path from its three components.
    #[must_use]
    pub fn new(user: impl Into<String>, machine: impl Into<String>, session: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            machine: machine.into(),
            session: session.into(),
        }
    }

    /// Validates one path component (a username, machine name, or session handle): it must be
    /// non-empty and free of the path separator, so the assembled `{user}/{machine}/{session}`
    /// stays unambiguous and `from`-attribution cannot be spoofed (DESIGN.md §5).
    ///
    /// # Errors
    ///
    /// Returns [`ParsePathError::Malformed`] if the component is empty or contains the separator.
    pub fn validate_component(component: &str) -> Result<(), ParsePathError> {
        if component.is_empty() || component.contains(Constant::SESSION_PATH_SEPARATOR) {
            return Err(ParsePathError::Malformed(component.to_owned()));
        }
        Ok(())
    }
}

impl fmt::Display for SessionPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sep = Constant::SESSION_PATH_SEPARATOR;
        write!(f, "{}{sep}{}{sep}{}", self.user, self.machine, self.session)
    }
}

/// The error returned when a [`SessionPath`] string is malformed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ParsePathError {
    /// The string was not exactly three `/`-separated, non-empty components.
    #[error("session path must be `user/machine/session`, got `{0}`")]
    Malformed(String),
}

impl FromStr for SessionPath {
    type Err = ParsePathError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(Constant::SESSION_PATH_SEPARATOR);
        let (Some(user), Some(machine), Some(session), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
            return Err(ParsePathError::Malformed(s.to_owned()));
        };

        if user.is_empty() || machine.is_empty() || session.is_empty() {
            return Err(ParsePathError::Malformed(s.to_owned()));
        }

        Ok(Self::new(user, machine, session))
    }
}

/// Parses a human duration (`30s`, `10m`, `24h`, `7d`, or bare seconds) into seconds — shared by
/// the CLI (`invite create --expires-in`, `tail --since`) and the bridge's `catch_up` tool.
///
/// # Errors
///
/// Returns an error if the numeric part does not parse.
pub fn parse_duration_secs(value: &str) -> Res<u64> {
    use anyhow::Context as _;
    let value = value.trim();
    let (digits, mult) = match value.chars().last() {
        Some('s') => (&value[..value.len() - 1], 1),
        Some('m') => (&value[..value.len() - 1], 60),
        Some('h') => (&value[..value.len() - 1], 3600),
        Some('d') => (&value[..value.len() - 1], 86_400),
        _ => (value, 1),
    };
    let count: u64 = digits.trim().parse().with_context(|| format!("invalid duration `{value}`"))?;
    Ok(count * mult)
}

#[cfg(test)]
mod tests {
    // Tests relax `unwrap_used` (house convention; DESIGN.md §22).
    #![allow(clippy::unwrap_used)]

    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn permission_levels_order_by_ascending_autonomy() {
        assert!(PermissionLevel::Mute < PermissionLevel::Notify);
        assert!(PermissionLevel::Notify < PermissionLevel::Converse);
        assert!(PermissionLevel::Converse < PermissionLevel::Act);
    }

    #[test]
    fn default_permission_level_is_notify() {
        assert_eq!(PermissionLevel::default(), PermissionLevel::Notify);
    }

    #[test]
    fn only_converse_and_above_may_emit() {
        assert!(!PermissionLevel::Mute.may_emit());
        assert!(!PermissionLevel::Notify.may_emit());
        assert!(PermissionLevel::Converse.may_emit());
        assert!(PermissionLevel::Act.may_emit());
    }

    #[test]
    fn permission_level_parses_from_its_lowercase_token() {
        for (token, level) in [
            ("mute", PermissionLevel::Mute),
            ("notify", PermissionLevel::Notify),
            ("converse", PermissionLevel::Converse),
            ("act", PermissionLevel::Act),
        ] {
            assert_eq!(token.parse::<PermissionLevel>().unwrap(), level);
        }
        assert!("bogus".parse::<PermissionLevel>().is_err());
    }

    #[test]
    fn session_path_displays_as_slash_separated_triple() {
        let path = SessionPath::new("aaron", "workstation", "razel");
        assert_eq!(path.to_string(), "aaron/workstation/razel");
    }

    #[test]
    fn session_path_parses_a_slash_separated_triple() {
        let path: SessionPath = "aaron/workstation/razel".parse().unwrap();
        assert_eq!(path, SessionPath::new("aaron", "workstation", "razel"));
    }

    #[test]
    fn session_path_round_trips_through_display_and_parse() {
        let path = SessionPath::new("aaron", "sno-box", "dotagent");
        assert_eq!(path.to_string().parse::<SessionPath>().unwrap(), path);
    }

    #[test]
    fn session_path_rejects_malformed_strings() {
        for bad in ["", "a", "a/b", "a/b/c/d", "a//c", "/b/c", "a/b/"] {
            assert!(bad.parse::<SessionPath>().is_err(), "expected `{bad}` to be rejected");
        }
    }

    #[test]
    fn session_path_component_validation_rejects_empty_and_separators() {
        for good in ["aaron", "sno-box", "repo.name", "a_b"] {
            assert!(SessionPath::validate_component(good).is_ok(), "expected `{good}` to be accepted");
        }
        for bad in ["", "a/b", "/", "a/", "/b"] {
            assert!(SessionPath::validate_component(bad).is_err(), "expected `{bad}` to be rejected");
        }
    }

    #[test]
    fn visibility_round_trips_through_its_wire_token() {
        for tier in [Visibility::Public, Visibility::Unlisted, Visibility::Private] {
            assert_eq!(tier.as_str().parse::<Visibility>().unwrap(), tier);
        }
        assert!("bogus".parse::<Visibility>().is_err());
    }
}
