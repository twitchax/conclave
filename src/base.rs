//! Foundational constants, error aliases, and domain types shared across `conclavelib`.
//!
//! This module is the single source of truth for the small vocabulary every other
//! module speaks: the `anyhow` error aliases used for application glue, the
//! [`Constant`] home for magic values, and the core domain enums / paths from
//! DESIGN.md §5, §6, and §9. Wire-crossing boundary errors (`ProtocolError`,
//! `AuthError`, `AclError`) live in their respective modules, not here.

use std::fmt;

/// A helper type for errors.
pub type Err = anyhow::Error;
/// A helper type for results.
pub type Res<T> = anyhow::Result<T, Err>;
/// A helper type for void results.
pub type Void = Res<()>;

/// A home for the project's magic values, so they are named once and reused.
pub struct Constant;

impl Constant {
    /// Name of the per-user configuration / keystore directory under the OS config
    /// dir (i.e. `~/.config/conclave`), where identity and permission state live.
    pub const CONFIG_DIR_NAME: &'static str = "conclave";
    /// The wire protocol version negotiated at connect time. Peers advertising an
    /// incompatible version are rejected or upgraded (DESIGN.md §13).
    pub const PROTOCOL_VERSION: u32 = 1;
    /// The separator between the components of a [`SessionPath`] (`user/machine/session`).
    pub const SESSION_PATH_SEPARATOR: char = '/';
}

/// How much an inbound message may drive the *recipient's* agent (DESIGN.md §9).
///
/// This is a **local** autonomy policy, never sent to the server. Variants are
/// ordered by ascending autonomy, so a resolved level can be compared against a
/// threshold (e.g. "below [`PermissionLevel::Converse`] rejects outbound emit").
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
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

/// A channel's discovery / join visibility tier, stored on the channel record (DESIGN.md §6).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Visibility {
    /// Appears in discovery; anyone on the server may join.
    Public,
    /// Not listed, but joinable by anyone who knows the exact name ("secret-link").
    Unlisted,
    /// Not listed; join is authorized via ACL or invite token.
    Private,
}

/// A fully-qualified live participant path, `{user}/{machine}/{session}` (DESIGN.md §5).
///
/// Every message's sender is a full path, so a reply or whisper target is always
/// unambiguous. The `server` component that disambiguates a multi-homed session
/// (DESIGN.md §8) is carried alongside a path, not embedded in it.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
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
}

impl fmt::Display for SessionPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sep = Constant::SESSION_PATH_SEPARATOR;
        write!(f, "{}{sep}{}{sep}{}", self.user, self.machine, self.session)
    }
}

#[cfg(test)]
mod tests {
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
    fn session_path_displays_as_slash_separated_triple() {
        let path = SessionPath::new("aaron", "workstation", "razel");
        assert_eq!(path.to_string(), "aaron/workstation/razel");
    }
}
