//! Wire frames shared between the bridge and the central server.
//!
//! Both peers serialize [`ProtocolMessage`] with a fixed `bincode` configuration behind a
//! length-delimited framing (the [`ProtocolWrite`] / [`ProtocolRead`] stream extensions). Two
//! properties are fixed here for forward-compat (DESIGN.md §13), so later additions are additive
//! rather than breaking:
//!
//! - a **protocol-version field** carried in [`ProtocolMessage::Hello`] and checked with
//!   [`negotiate_version`]; peers advertising an incompatible version are rejected,
//! - an opaque **encrypted-payload envelope + key-id** ([`Payload::Encrypted`]), so end-to-end
//!   encryption (DESIGN.md §19) can be layered in without a wire break.
//!
//! `ProtocolError` is the typed, wire-crossing error surfaced as a [`ProtocolMessage::Error`]
//! frame; application glue elsewhere uses `anyhow` via the `Res` / `Void` aliases.

use std::future::Future;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use crate::base::{Constant, Res, SessionPath, Visibility};

/// An opaque end-to-end-encrypted payload envelope, reserved now for v2 (DESIGN.md §13, §19).
///
/// The server fans this out without reading it; `key_id` names the per-channel key the sender
/// wrapped the content with. Unused in v1 — reserving it keeps E2E an additive change.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    /// Ciphertext the server relays but cannot read.
    pub ciphertext: Vec<u8>,
    /// Identifier of the per-channel key this ciphertext was wrapped with.
    pub key_id: Option<String>,
}

/// A message body: plaintext in v1, or the reserved E2E [`Envelope`] in v2.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Payload {
    /// A plaintext UTF-8 body (v1).
    Plain(String),
    /// An opaque end-to-end-encrypted body (v2; the envelope is reserved now).
    Encrypted(Envelope),
}

/// A channel as surfaced by discovery ([`ProtocolMessage::ChannelList`], DESIGN.md §6).
///
/// Only channels the caller is allowed to see are ever listed, so no private name leaks to a
/// non-member; `member` marks the ones the caller already belongs to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelInfo {
    /// The channel name.
    pub name: String,
    /// The visibility tier.
    pub visibility: Visibility,
    /// Whether the requesting user is already a member.
    pub member: bool,
}

/// An enrolled machine as surfaced by [`ProtocolMessage::MachineList`] (`machine list`, §5.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineInfo {
    /// The machine name (unique within the user).
    pub name: String,
    /// The machine's public key, base64-encoded.
    pub pubkey: String,
    /// RFC 3339 enrollment timestamp.
    pub added_at: String,
}

/// An admin / moderation operation (DESIGN.md §7), authorized server-side by user role.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AdminOp {
    /// Create a channel with a visibility tier.
    CreateChannel {
        /// Channel name.
        name: String,
        /// Visibility tier.
        visibility: Visibility,
    },
    /// Delete a channel.
    DeleteChannel {
        /// Channel name.
        name: String,
    },
    /// Rename a channel.
    RenameChannel {
        /// Current name.
        name: String,
        /// New name.
        new_name: String,
    },
    /// Change a channel's visibility tier.
    SetVisibility {
        /// Channel name.
        name: String,
        /// New visibility tier.
        visibility: Visibility,
    },
    /// Add a user to a channel's access-control list.
    AclAdd {
        /// Channel name.
        channel: String,
        /// Username to add.
        user: String,
    },
    /// Remove a user from a channel's access-control list.
    AclRemove {
        /// Channel name.
        channel: String,
        /// Username to remove.
        user: String,
    },
    /// Create an invite token for a channel.
    InviteCreate {
        /// Channel name.
        channel: String,
        /// Maximum redemptions, or unlimited if absent.
        uses: Option<u32>,
        /// Lifetime in seconds, or non-expiring if absent.
        expires_in_secs: Option<u64>,
    },
    /// Revoke an invite token.
    InviteRevoke {
        /// The token to revoke.
        token: String,
    },
    /// Kick a live session or user from a channel.
    Kick {
        /// Channel name.
        channel: String,
        /// Session path or username to kick.
        target: String,
    },
    /// Ban a user from a channel.
    Ban {
        /// Channel name.
        channel: String,
        /// Username to ban.
        user: String,
    },
    /// Remove a user from the server (server-admin).
    UserRemove {
        /// Username to remove.
        username: String,
    },
    /// Revoke an enrolled machine (server-admin / self), force-dropping its live sessions.
    MachineRemove {
        /// Machine name to revoke.
        name: String,
    },
    /// Enroll a new machine key under the authenticated user (self-service, DESIGN.md §5.1).
    ///
    /// Appended after `MachineRemove` so existing variant indices are unchanged (forward-compat).
    MachineAdd {
        /// Unique-within-user name for the new machine.
        name: String,
        /// The new machine's Ed25519 public key (proves possession on its own first connect).
        pubkey: Vec<u8>,
    },
    /// List a channel's ACL members (channel-admin; answered with a `UserList`).
    AclList {
        /// Channel name.
        channel: String,
    },
}

/// The versioned frame exchanged between a bridge and a central server.
///
/// Variants are append-only across protocol versions: later milestones may add variants but must
/// not renumber or repurpose existing ones without a version bump (see [`negotiate_version`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolMessage {
    /// Client → server on connect: advertise the protocol version and the session handle.
    Hello {
        /// The client's protocol version.
        protocol_version: u32,
        /// The session handle (`--as`, defaulting to the repo/dir name).
        session: String,
    },
    /// Server → client: a random nonce for the client to sign (challenge-response).
    Challenge {
        /// The random nonce.
        nonce: Vec<u8>,
    },
    /// Client → server: the machine public key and its signature over the nonce.
    Auth {
        /// The machine's Ed25519 public key.
        pubkey: Vec<u8>,
        /// The signature over the server's nonce.
        signature: Vec<u8>,
    },
    /// Server → client: authentication succeeded; the resolved full participant path.
    Established {
        /// The resolved `user/machine/session` path.
        path: SessionPath,
    },
    /// Client → server: claim a username and enroll this machine as its first key.
    Register {
        /// Username to claim.
        username: String,
        /// Machine name for this key.
        machine: String,
        /// The machine's Ed25519 public key.
        pubkey: Vec<u8>,
    },
    /// Client → server: join a channel, optionally redeeming an invite token.
    Join {
        /// Channel name.
        channel: String,
        /// Invite token, if required.
        token: Option<String>,
    },
    /// Client → server: leave a channel.
    Leave {
        /// Channel name.
        channel: String,
    },
    /// Client → server: request presence, optionally scoped to one channel.
    Who {
        /// Channel to scope to, or all subscribed channels if absent.
        channel: Option<String>,
    },
    /// Client → server: an admin / moderation operation.
    Admin(AdminOp),
    /// A message addressed to all sessions subscribed to a channel.
    ChannelMsg {
        /// Channel name.
        channel: String,
        /// The sender's full participant path.
        from: SessionPath,
        /// The message body.
        payload: Payload,
    },
    /// A direct message to exactly one session path.
    Whisper {
        /// The sender's full participant path.
        from: SessionPath,
        /// The single recipient's full participant path.
        target: SessionPath,
        /// The message body.
        payload: Payload,
    },
    /// Server → client: presence enumerated as full session paths.
    Presence {
        /// Channel the presence is scoped to, or server-wide if absent.
        channel: Option<String>,
        /// The present sessions.
        sessions: Vec<SessionPath>,
    },
    /// A typed error surfaced to the peer that triggered it.
    Error(ProtocolError),
    // ---------------------------------------------------------------------
    // M2 additions — appended after `Error` so every existing variant keeps
    // its wire index (the append-only, forward-compat discipline of §13).
    // ---------------------------------------------------------------------
    /// Client → server: request the channels visible to the authenticated user (discovery).
    ListChannels,
    /// Server → client: the discovery result, already visibility-gated.
    ChannelList {
        /// The channels the caller may see.
        channels: Vec<ChannelInfo>,
    },
    /// Server → client: a [`ProtocolMessage::Join`] succeeded; the session is now subscribed.
    Joined {
        /// The channel that was joined.
        channel: String,
    },
    /// Server → client: a control / admin operation succeeded, with an optional human detail.
    Ack {
        /// A short human-readable detail (e.g. the affected name), if any.
        detail: Option<String>,
    },
    /// Server → client: the token minted by an [`AdminOp::InviteCreate`].
    InviteToken {
        /// The opaque invite token.
        token: String,
    },
    /// Client → server: liveness keepalive; refreshes presence and draws a [`ProtocolMessage::Pong`]
    /// (the application-level realization of the §10 heartbeat, uniform across transports).
    Ping,
    /// Server → client: keepalive acknowledgement.
    Pong,
    // ---------------------------------------------------------------------
    // M4 additions — appended (forward-compat): machine / user listing and the
    // post-auth server-role signal that gates the bridge's admin tools.
    // ---------------------------------------------------------------------
    /// Server → client, immediately after [`ProtocolMessage::Established`]: the authenticated user's
    /// server-wide role, so the bridge can gate its admin tools (DESIGN.md §7).
    ServerInfo {
        /// Whether the user is a server admin (on the serve-config allowlist).
        admin: bool,
    },
    /// Client → server: list the machines enrolled under the authenticated user.
    ListMachines,
    /// Server → client: the caller's enrolled machines.
    MachineList {
        /// The machines under the caller's account.
        machines: Vec<MachineInfo>,
    },
    /// Client → server: list the server's users (server-admin only).
    ListUsers,
    /// Server → client: the registered usernames (server-admin only).
    UserList {
        /// The registered usernames.
        users: Vec<String>,
    },
}

/// Errors that cross the wire as a [`ProtocolMessage::Error`] frame and are matched on by the
/// server and bridge (DESIGN.md §16). Application glue elsewhere uses `anyhow` via `Res` / `Void`.
// The public name `ProtocolError` is fixed by DESIGN.md §13 / §22; `module_name_repetitions` is a
// false positive against that mandated vocabulary (the sibling ratrod uses the same name).
#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
pub enum ProtocolError {
    /// The peer's protocol version is incompatible with ours.
    #[error("incompatible protocol version: ours={ours}, theirs={theirs}")]
    VersionMismatch {
        /// This build's protocol version.
        ours: u32,
        /// The peer's advertised version.
        theirs: u32,
    },
    /// A frame could not be decoded, or violated the schema.
    #[error("malformed frame: {0}")]
    MalformedFrame(String),
    /// The operation was denied (authentication or authorization).
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    /// The named channel, session, or target does not exist / is not visible.
    #[error("not found: {0}")]
    NotFound(String),
    /// An unexpected server-side error.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Returns this build's protocol version if `theirs` is compatible, else a [`ProtocolError::VersionMismatch`].
///
/// v1 speaks exactly one version, so compatibility is equality; a later minor-compatible range
/// widens this without changing the call sites.
///
/// # Errors
///
/// Returns [`ProtocolError::VersionMismatch`] when the peer's version is not compatible.
pub fn negotiate_version(theirs: u32) -> Result<u32, ProtocolError> {
    if theirs == Constant::PROTOCOL_VERSION {
        Ok(Constant::PROTOCOL_VERSION)
    } else {
        Err(ProtocolError::VersionMismatch { ours: Constant::PROTOCOL_VERSION, theirs })
    }
}

/// Encodes a frame to its wire bytes with the fixed codec configuration.
///
/// # Errors
///
/// Returns an error if the frame cannot be serialized.
pub fn encode(message: &ProtocolMessage) -> Res<Vec<u8>> {
    bincode::serde::encode_to_vec(message, bincode::config::standard()).context("failed to encode protocol frame")
}

/// Decodes a frame from its wire bytes with the fixed codec configuration.
///
/// # Errors
///
/// Returns an error if the bytes are not a valid encoded frame.
pub fn decode(bytes: &[u8]) -> Res<ProtocolMessage> {
    let (message, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard()).context("failed to decode protocol frame")?;
    Ok(message)
}

/// Length-delimited sending of protocol frames over any async writer.
pub trait ProtocolWrite: AsyncWrite + Unpin {
    /// Encodes `message` and writes it as a `u32`-length-prefixed frame, then flushes.
    ///
    /// # Errors
    ///
    /// Returns an error if the frame cannot be encoded, exceeds `u32` in length, or the write fails.
    fn send_message(&mut self, message: &ProtocolMessage) -> impl Future<Output = Res<()>> {
        async move {
            let body = encode(message)?;
            let len = u32::try_from(body.len()).context("protocol frame exceeds u32 length")?;
            self.write_all(&len.to_be_bytes()).await?;
            self.write_all(&body).await?;
            self.flush().await?;
            Ok(())
        }
    }
}

impl<T: AsyncWrite + Unpin + ?Sized> ProtocolWrite for T {}

/// Length-delimited receiving of protocol frames over any async reader.
pub trait ProtocolRead: AsyncRead + Unpin {
    /// Reads one `u32`-length-prefixed frame and decodes it.
    ///
    /// # Errors
    ///
    /// Returns an error on EOF / read failure, a length prefix beyond [`Constant::MAX_FRAME_SIZE`],
    /// or a body that does not decode.
    fn recv_message(&mut self) -> impl Future<Output = Res<ProtocolMessage>> {
        async move {
            let mut len_buf = [0_u8; 4];
            self.read_exact(&mut len_buf).await?;
            let len = usize::try_from(u32::from_be_bytes(len_buf)).context("frame length overflow")?;

            anyhow::ensure!(len <= Constant::MAX_FRAME_SIZE, "protocol frame of {len} bytes exceeds the {} byte cap", Constant::MAX_FRAME_SIZE);

            let mut body = vec![0_u8; len];
            self.read_exact(&mut body).await?;
            decode(&body)
        }
    }
}

impl<T: AsyncRead + Unpin + ?Sized> ProtocolRead for T {}

#[cfg(test)]
mod tests {
    // Tests relax `unwrap_used` (house convention; DESIGN.md §22).
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::tests::duplex;
    use pretty_assertions::assert_eq;

    fn assert_round_trips(message: &ProtocolMessage) {
        let bytes = encode(message).unwrap();
        assert_eq!(&decode(&bytes).unwrap(), message);
    }

    #[test]
    fn hello_round_trips_with_version_field() {
        assert_round_trips(&ProtocolMessage::Hello {
            protocol_version: Constant::PROTOCOL_VERSION,
            session: "razel".to_owned(),
        });
    }

    #[test]
    fn channel_message_round_trips_plaintext() {
        assert_round_trips(&ProtocolMessage::ChannelMsg {
            channel: "ops".to_owned(),
            from: SessionPath::new("aaron", "workstation", "razel"),
            payload: Payload::Plain("hello, agents".to_owned()),
        });
    }

    #[test]
    fn data_frame_round_trips_the_reserved_e2e_envelope() {
        assert_round_trips(&ProtocolMessage::Whisper {
            from: SessionPath::new("aaron", "workstation", "razel"),
            target: SessionPath::new("david", "desktop", "main"),
            payload: Payload::Encrypted(Envelope {
                ciphertext: vec![0xDE, 0xAD, 0xBE, 0xEF],
                key_id: Some("channel-key-1".to_owned()),
            }),
        });
    }

    #[test]
    fn admin_op_round_trips() {
        assert_round_trips(&ProtocolMessage::Admin(AdminOp::CreateChannel {
            name: "ops".to_owned(),
            visibility: Visibility::Private,
        }));
    }

    #[test]
    fn machine_add_admin_op_round_trips() {
        assert_round_trips(&ProtocolMessage::Admin(AdminOp::MachineAdd {
            name: "sno-box".to_owned(),
            pubkey: vec![1, 2, 3, 4],
        }));
    }

    #[test]
    fn acl_list_admin_op_round_trips() {
        assert_round_trips(&ProtocolMessage::Admin(AdminOp::AclList { channel: "ops".to_owned() }));
    }

    #[test]
    fn m2_response_frames_round_trip() {
        assert_round_trips(&ProtocolMessage::ListChannels);
        assert_round_trips(&ProtocolMessage::ChannelList {
            channels: vec![ChannelInfo {
                name: "ops".to_owned(),
                visibility: Visibility::Private,
                member: true,
            }],
        });
        assert_round_trips(&ProtocolMessage::Joined { channel: "ops".to_owned() });
        assert_round_trips(&ProtocolMessage::Ack { detail: Some("ops".to_owned()) });
        assert_round_trips(&ProtocolMessage::InviteToken { token: "tok-abc".to_owned() });
        assert_round_trips(&ProtocolMessage::Ping);
        assert_round_trips(&ProtocolMessage::Pong);
    }

    #[test]
    fn m4_frames_round_trip() {
        assert_round_trips(&ProtocolMessage::ServerInfo { admin: true });
        assert_round_trips(&ProtocolMessage::ListMachines);
        assert_round_trips(&ProtocolMessage::MachineList {
            machines: vec![MachineInfo {
                name: "workstation".to_owned(),
                pubkey: "PUBKEY".to_owned(),
                added_at: "2026-07-02T00:00:00Z".to_owned(),
            }],
        });
        assert_round_trips(&ProtocolMessage::ListUsers);
        assert_round_trips(&ProtocolMessage::UserList {
            users: vec!["aaron".to_owned(), "david".to_owned()],
        });
    }

    #[test]
    fn appending_variants_preserves_existing_wire_indices() {
        // The forward-compat guarantee (§13): an old variant's encoding must be byte-identical
        // after new variants are appended. `Hello` is the first variant (index 0) and must still
        // start with a 0 discriminant byte.
        let hello = ProtocolMessage::Hello {
            protocol_version: Constant::PROTOCOL_VERSION,
            session: "razel".to_owned(),
        };
        assert_eq!(encode(&hello).unwrap()[0], 0, "the first variant's discriminant must remain 0");
    }

    #[test]
    fn error_frame_round_trips() {
        assert_round_trips(&ProtocolMessage::Error(ProtocolError::VersionMismatch { ours: 1, theirs: 2 }));
    }

    #[tokio::test]
    async fn frames_stream_over_an_async_duplex() {
        let (mut a, mut b) = duplex();
        let sent = ProtocolMessage::Presence {
            channel: Some("ops".to_owned()),
            sessions: vec![SessionPath::new("aaron", "workstation", "razel"), SessionPath::new("david", "desktop", "main")],
        };

        a.send_message(&sent).await.unwrap();
        let got = b.recv_message().await.unwrap();

        assert_eq!(got, sent);
    }

    #[test]
    fn version_negotiation_accepts_matching_and_rejects_mismatch() {
        assert_eq!(negotiate_version(Constant::PROTOCOL_VERSION).unwrap(), Constant::PROTOCOL_VERSION);
        assert_eq!(
            negotiate_version(999),
            Err(ProtocolError::VersionMismatch {
                ours: Constant::PROTOCOL_VERSION,
                theirs: 999,
            })
        );
    }

    #[tokio::test]
    async fn recv_rejects_a_frame_larger_than_the_cap() {
        // A length prefix beyond the cap is rejected before the body is allocated.
        let oversized = u32::try_from(Constant::MAX_FRAME_SIZE + 1).unwrap();
        let framed = oversized.to_be_bytes();
        let mut reader = framed.as_slice();

        assert!(reader.recv_message().await.is_err());
    }
}
