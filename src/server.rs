//! The central server (`conclave serve`): the axum WSS endpoint and control plane.
//!
//! A single self-contained binary that owns: the `axum` WSS endpoint and control RPCs; the
//! embedded store (see [`crate::store`]); in-memory presence with heartbeat reaping and the
//! fan-out router; and admin authorization against the config `users` allowlist plus each
//! channel's `created_by` (DESIGN.md §7).
//!
//! Durable state is config only — no message history. Presence, subscriptions, permission levels,
//! and the admin allowlist are deliberately *not* in the DB (DESIGN.md §15).
//!
//! The subsystem is split by responsibility: `hub` is the transport-agnostic core (store +
//! in-memory presence, subscriptions, and the fan-out router); `session` is the per-connection
//! handshake + frame loop; `wss` is the axum WebSocket adapter and the [`serve`] entrypoint. This
//! module owns the [`AclError`] authorization boundary type and re-exports the public surface.

mod hub;
mod session;
mod wss;

#[cfg(test)]
mod integration;

pub use wss::{ServerConfig, serve};

use crate::protocol::ProtocolError;

/// The server-admin allowlist (DESIGN.md §7): each admin username mapped to the public key
/// (base64) permitted to claim it, or `None` if unpinned. Pinning stops a fresh-deploy admin
/// username from being squatted by the first client to register it (PRD-0007 T-002).
pub type AdminAllowlist = std::collections::HashMap<String, Option<String>>;

/// Errors at the access-control / authorization boundary (DESIGN.md §7, §16), matched on by the
/// server and surfaced to the caller as a wire error frame.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AclError {
    /// The issuing user is not an admin for the attempted action.
    #[error("not authorized: admin role required")]
    NotAdmin,
    /// The user is not a member of the channel.
    #[error("not a member of channel `{0}`")]
    NotMember(String),
    /// The channel is private and the user has no ACL entry or valid invite.
    #[error("channel `{0}` is private")]
    ChannelPrivate(String),
    /// The named channel does not exist.
    #[error("channel `{0}` not found")]
    ChannelNotFound(String),
}

impl From<AclError> for ProtocolError {
    fn from(err: AclError) -> Self {
        let message = err.to_string();
        match err {
            AclError::ChannelNotFound(_) => Self::NotFound(message),
            _ => Self::Unauthorized(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn acl_errors_map_onto_wire_protocol_errors() {
        assert!(matches!(ProtocolError::from(AclError::NotAdmin), ProtocolError::Unauthorized(_)));
        assert!(matches!(ProtocolError::from(AclError::ChannelNotFound("ops".to_owned())), ProtocolError::NotFound(_)));
    }

    #[test]
    fn acl_error_messages_are_descriptive() {
        assert_eq!(AclError::NotMember("ops".to_owned()).to_string(), "not a member of channel `ops`");
    }
}
