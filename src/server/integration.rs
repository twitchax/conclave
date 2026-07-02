//! Integration tests for the central server, driving simulated bridge clients over an in-memory
//! duplex against a real [`Hub`] + embedded store (DESIGN.md §17). These cover the M2 UATs; each
//! test name is prefixed with its UAT group (`server_register` / `server_auth` / `server_channel`
//! / `server_fanout` / `server_presence`) so the PRD's `test(/…/)` filters select them.

// Tests relax `unwrap_used` (house convention; DESIGN.md §22).
#![allow(clippy::unwrap_used)]

use std::{sync::Arc, time::Duration};

use pretty_assertions::assert_eq;
use tokio::io::DuplexStream;

use crate::{
    base::{Constant, SessionPath, Visibility},
    identity::Identity,
    protocol::{AdminOp, ProtocolError, ProtocolMessage, ProtocolRead as _, ProtocolWrite as _},
};

use super::{hub::Hub, session::handle_connection};

// -----------------------------------------------------------------------------
// Harness.
// -----------------------------------------------------------------------------

async fn hub_with(admins: &[&str]) -> Arc<Hub> {
    let store = crate::store::Store::open_in_memory().await.unwrap();
    Hub::new(store, admins.iter().map(|a| (*a).to_owned()).collect())
}

async fn hub() -> Arc<Hub> {
    hub_with(&[]).await
}

/// A simulated bridge client speaking the wire protocol over an in-memory duplex to a spawned
/// session driver.
struct Client {
    stream: DuplexStream,
}

impl Client {
    fn connect(hub: &Arc<Hub>) -> Self {
        let (client_end, server_end) = tokio::io::duplex(64 * 1024);
        tokio::spawn(handle_connection(Arc::clone(hub), server_end));
        Self { stream: client_end }
    }

    /// Sends a frame; a closed peer is tolerated (the test asserts on what it *receives*).
    async fn send(&mut self, frame: ProtocolMessage) {
        let _ = self.stream.send_message(&frame).await;
    }

    async fn recv(&mut self) -> ProtocolMessage {
        self.stream.recv_message().await.unwrap()
    }

    /// Receives the next frame, or `None` if nothing arrives within a short window (for asserting
    /// that a message did *not* reach this client).
    async fn try_recv(&mut self) -> Option<ProtocolMessage> {
        tokio::time::timeout(Duration::from_millis(200), self.stream.recv_message()).await.ok().and_then(Result::ok)
    }

    /// Claims a fresh username + machine and proves possession; returns the server's final frame.
    async fn register(&mut self, id: &Identity, username: &str, machine: &str, session: &str) -> ProtocolMessage {
        self.hello(session).await;
        let nonce = self.challenge().await;
        let pubkey = id.public_key().to_vec();
        let signature = id.sign(&nonce).unwrap().to_vec();
        self.send(ProtocolMessage::Register {
            username: username.to_owned(),
            machine: machine.to_owned(),
            pubkey: pubkey.clone(),
        })
        .await;
        self.send(ProtocolMessage::Auth { pubkey, signature }).await;
        self.recv_auth_result().await
    }

    /// Authenticates an already-enrolled key under a session handle; returns the server's response.
    async fn authenticate(&mut self, id: &Identity, session: &str) -> ProtocolMessage {
        self.hello(session).await;
        let nonce = self.challenge().await;
        let signature = id.sign(&nonce).unwrap().to_vec();
        self.send(ProtocolMessage::Auth {
            pubkey: id.public_key().to_vec(),
            signature,
        })
        .await;
        self.recv_auth_result().await
    }

    /// Reads the post-`Auth` frame, consuming the trailing `ServerInfo` on a successful `Established`.
    async fn recv_auth_result(&mut self) -> ProtocolMessage {
        let response = self.recv().await;
        if matches!(response, ProtocolMessage::Established { .. }) {
            let _ = self.recv().await;
        }
        response
    }

    async fn hello(&mut self, session: &str) {
        self.send(ProtocolMessage::Hello {
            protocol_version: Constant::PROTOCOL_VERSION,
            session: session.to_owned(),
        })
        .await;
    }

    async fn challenge(&mut self) -> Vec<u8> {
        match self.recv().await {
            ProtocolMessage::Challenge { nonce } => nonce,
            other => panic!("expected Challenge, got {other:?}"),
        }
    }

    async fn admin(&mut self, op: AdminOp) -> ProtocolMessage {
        self.send(ProtocolMessage::Admin(op)).await;
        self.recv().await
    }

    async fn join(&mut self, channel: &str, token: Option<&str>) -> ProtocolMessage {
        self.send(ProtocolMessage::Join {
            channel: channel.to_owned(),
            token: token.map(str::to_owned),
        })
        .await;
        self.recv().await
    }
}

fn established_path(frame: ProtocolMessage) -> SessionPath {
    match frame {
        ProtocolMessage::Established { path } => path,
        other => panic!("expected Established, got {other:?}"),
    }
}

fn invite_token(frame: ProtocolMessage) -> String {
    match frame {
        ProtocolMessage::InviteToken { token } => token,
        other => panic!("expected InviteToken, got {other:?}"),
    }
}

fn sorted_channel_names(frame: ProtocolMessage) -> Vec<String> {
    match frame {
        ProtocolMessage::ChannelList { channels } => {
            let mut names: Vec<String> = channels.into_iter().map(|c| c.name).collect();
            names.sort();
            names
        }
        other => panic!("expected ChannelList, got {other:?}"),
    }
}

fn is_unauthorized(frame: &ProtocolMessage) -> bool {
    matches!(frame, ProtocolMessage::Error(ProtocolError::Unauthorized(_)))
}

// -----------------------------------------------------------------------------
// uat-001 — registration + enrollment; duplicate username / live handle rejected.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn server_register_claims_username_and_enrolls_machine() {
    let hub = hub().await;
    let id = Identity::generate().unwrap();

    let mut client = Client::connect(&hub);
    let path = established_path(client.register(&id, "aaron", "workstation", "razel").await);

    assert_eq!(path, SessionPath::new("aaron", "workstation", "razel"));
    assert!(hub.is_present(&path));

    // The key is now enrolled: a second connection authenticates with it and resolves the same
    // (user, machine).
    let mut again = Client::connect(&hub);
    let resolved = established_path(again.authenticate(&id, "dotagent").await);
    assert_eq!(resolved, SessionPath::new("aaron", "workstation", "dotagent"));
}

#[tokio::test]
async fn server_register_rejects_duplicate_username() {
    let hub = hub().await;
    let first = Identity::generate().unwrap();
    let second = Identity::generate().unwrap();

    let mut a = Client::connect(&hub);
    established_path(a.register(&first, "aaron", "workstation", "s1").await);

    // A different machine key trying to claim the same username is refused.
    let mut b = Client::connect(&hub);
    let response = b.register(&second, "aaron", "laptop", "s2").await;
    assert!(is_unauthorized(&response), "duplicate username must be rejected, got {response:?}");
}

#[tokio::test]
async fn server_register_rejects_duplicate_live_handle() {
    let hub = hub().await;
    let id = Identity::generate().unwrap();

    let mut a = Client::connect(&hub);
    established_path(a.register(&id, "aaron", "workstation", "razel").await);

    // The same (user, machine) with the same live handle collides — two sessions can't share a path.
    let mut b = Client::connect(&hub);
    let response = b.authenticate(&id, "razel").await;
    assert!(is_unauthorized(&response), "duplicate live handle must be rejected, got {response:?}");
}

// -----------------------------------------------------------------------------
// uat-002 — challenge-response resolves (user, machine); bad / revoked key refused.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn server_auth_resolves_enrolled_key_to_user_and_machine() {
    let hub = hub().await;
    let id = Identity::generate().unwrap();

    let mut enroller = Client::connect(&hub);
    established_path(enroller.register(&id, "aaron", "workstation", "s1").await);

    let mut session = Client::connect(&hub);
    let path = established_path(session.authenticate(&id, "s2").await);
    assert_eq!(path.user, "aaron");
    assert_eq!(path.machine, "workstation");
    assert_eq!(path.session, "s2");
}

#[tokio::test]
async fn server_auth_refuses_unknown_key() {
    let hub = hub().await;
    let stranger = Identity::generate().unwrap();

    // A valid signature over the real nonce, but the key was never enrolled.
    let mut client = Client::connect(&hub);
    let response = client.authenticate(&stranger, "s").await;
    assert!(is_unauthorized(&response), "an unknown key must be refused, got {response:?}");
}

#[tokio::test]
async fn server_auth_refuses_bad_signature() {
    let hub = hub().await;
    let id = Identity::generate().unwrap();

    let mut enroller = Client::connect(&hub);
    established_path(enroller.register(&id, "aaron", "workstation", "s1").await);

    // Enrolled key, but the signature is over the wrong message.
    let mut client = Client::connect(&hub);
    client.hello("s2").await;
    let _nonce = client.challenge().await;
    let bad_signature = id.sign(&[0_u8; Constant::CHALLENGE_SIZE]).unwrap().to_vec();
    client
        .send(ProtocolMessage::Auth {
            pubkey: id.public_key().to_vec(),
            signature: bad_signature,
        })
        .await;
    let response = client.recv().await;
    assert!(is_unauthorized(&response), "a bad signature must be refused, got {response:?}");
}

#[tokio::test]
async fn server_auth_refuses_revoked_key() {
    let hub = hub().await;
    let id = Identity::generate().unwrap();

    let mut client = Client::connect(&hub);
    established_path(client.register(&id, "aaron", "workstation", "s1").await);

    // The user revokes their own machine (the lost-laptop kill switch, §5.1).
    let ack = client.admin(AdminOp::MachineRemove { name: "workstation".to_owned() }).await;
    assert!(matches!(ack, ProtocolMessage::Ack { .. }), "expected an ack for MachineRemove, got {ack:?}");

    // Reconnecting with the revoked key is now refused — the record is gone.
    let mut reconnect = Client::connect(&hub);
    let response = reconnect.authenticate(&id, "s2").await;
    assert!(is_unauthorized(&response), "a revoked key must be refused, got {response:?}");
}

// -----------------------------------------------------------------------------
// PRD-0007 T-001 (register_proof) — possession is proven before any durable write.
// An aborted registration handshake (a failed or mismatched Auth after Register) must
// persist nothing: no squatted username, no enrolled key (DESIGN.md §5.1).
// -----------------------------------------------------------------------------

#[tokio::test]
async fn server_register_proof_bad_signature_persists_nothing() {
    let hub = hub().await;
    let attacker = Identity::generate().unwrap();

    // An unauthenticated client claims the victim's username but fails the possession proof.
    let mut evil = Client::connect(&hub);
    evil.hello("s1").await;
    let _nonce = evil.challenge().await;
    let attacker_key = attacker.public_key().to_vec();
    evil.send(ProtocolMessage::Register {
        username: "aaron".to_owned(),
        machine: "evil".to_owned(),
        pubkey: attacker_key.clone(),
    })
    .await;
    let bad_signature = attacker.sign(&[0_u8; Constant::CHALLENGE_SIZE]).unwrap().to_vec();
    evil.send(ProtocolMessage::Auth {
        pubkey: attacker_key,
        signature: bad_signature,
    })
    .await;
    assert!(is_unauthorized(&evil.recv().await), "a failed possession proof must be rejected");

    // Nothing was persisted: the rightful owner can still claim the username with their own key...
    let owner = Identity::generate().unwrap();
    let mut legit = Client::connect(&hub);
    let result = legit.register(&owner, "aaron", "workstation", "s2").await;
    assert!(
        matches!(result, ProtocolMessage::Established { .. }),
        "an aborted handshake must not squat the username; got {result:?}"
    );

    // ...and the attacker's unproven key was never enrolled, so it cannot authenticate.
    let mut replay = Client::connect(&hub);
    assert!(is_unauthorized(&replay.authenticate(&attacker, "s3").await), "an unproven key must not have been enrolled");
}

#[tokio::test]
async fn server_register_proof_key_mismatch_persists_nothing() {
    let hub = hub().await;
    let claimed = Identity::generate().unwrap();
    let other = Identity::generate().unwrap();

    // Register one key, then prove possession of a *different* key (validly signed).
    let mut evil = Client::connect(&hub);
    evil.hello("s1").await;
    let nonce = evil.challenge().await;
    evil.send(ProtocolMessage::Register {
        username: "aaron".to_owned(),
        machine: "evil".to_owned(),
        pubkey: claimed.public_key().to_vec(),
    })
    .await;
    let signature = other.sign(&nonce).unwrap().to_vec();
    evil.send(ProtocolMessage::Auth {
        pubkey: other.public_key().to_vec(),
        signature,
    })
    .await;
    assert!(is_unauthorized(&evil.recv().await), "an auth key that does not match the registered key must be rejected");

    // The username is still free and neither key was enrolled.
    let owner = Identity::generate().unwrap();
    let mut legit = Client::connect(&hub);
    let result = legit.register(&owner, "aaron", "workstation", "s2").await;
    assert!(matches!(result, ProtocolMessage::Established { .. }), "a mismatched-key handshake must persist nothing; got {result:?}");
}

// -----------------------------------------------------------------------------
// uat-003 — channel create + ACL + invite redeem; private names never leak.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn server_channel_create_acl_and_invite_redeem() {
    let hub = hub().await;
    let owner = Identity::generate().unwrap();
    let guest = Identity::generate().unwrap();
    let latecomer = Identity::generate().unwrap();

    let mut a = Client::connect(&hub);
    established_path(a.register(&owner, "aaron", "wa", "sa").await);
    let create = a
        .admin(AdminOp::CreateChannel {
            name: "ops".to_owned(),
            visibility: Visibility::Private,
        })
        .await;
    assert!(matches!(create, ProtocolMessage::Ack { .. }));

    // A non-member cannot join a private channel without an invite.
    let mut b = Client::connect(&hub);
    let bpath = established_path(b.register(&guest, "david", "wd", "sb").await);
    let denied = b.join("ops", None).await;
    assert!(is_unauthorized(&denied), "private join without a token must be denied, got {denied:?}");

    // The channel admin mints a single-use invite; redeeming it joins and adds david to the ACL.
    let token = invite_token(
        a.admin(AdminOp::InviteCreate {
            channel: "ops".to_owned(),
            uses: Some(1),
            expires_in_secs: None,
        })
        .await,
    );
    let joined = b.join("ops", Some(&token)).await;
    assert!(matches!(joined, ProtocolMessage::Joined { .. }), "redeeming a valid invite must join, got {joined:?}");
    assert!(hub.subscribers("ops").contains(&bpath));

    // The single-use token is now spent — a second redeemer is refused.
    let mut c = Client::connect(&hub);
    established_path(c.register(&latecomer, "carol", "wc", "sc").await);
    let spent = c.join("ops", Some(&token)).await;
    assert!(is_unauthorized(&spent), "a spent single-use invite must be refused, got {spent:?}");
}

#[tokio::test]
async fn server_channel_acl_add_grants_membership() {
    let hub = hub().await;
    let owner = Identity::generate().unwrap();
    let guest = Identity::generate().unwrap();

    let mut a = Client::connect(&hub);
    established_path(a.register(&owner, "aaron", "wa", "sa").await);
    a.admin(AdminOp::CreateChannel {
        name: "ops".to_owned(),
        visibility: Visibility::Private,
    })
    .await;

    // Adding david to the ACL lets him join the private channel with no token.
    let acl = a
        .admin(AdminOp::AclAdd {
            channel: "ops".to_owned(),
            user: "david".to_owned(),
        })
        .await;
    assert!(matches!(acl, ProtocolMessage::Ack { .. }));

    let mut b = Client::connect(&hub);
    let bpath = established_path(b.register(&guest, "david", "wd", "sb").await);
    let joined = b.join("ops", None).await;
    assert!(matches!(joined, ProtocolMessage::Joined { .. }), "an ACL member must join without a token, got {joined:?}");
    assert!(hub.subscribers("ops").contains(&bpath));
}

#[tokio::test]
async fn server_channel_private_names_do_not_leak() {
    let hub = hub().await;
    let owner = Identity::generate().unwrap();
    let outsider = Identity::generate().unwrap();

    let mut a = Client::connect(&hub);
    established_path(a.register(&owner, "aaron", "wa", "sa").await);
    a.admin(AdminOp::CreateChannel {
        name: "lobby".to_owned(),
        visibility: Visibility::Public,
    })
    .await;
    a.admin(AdminOp::CreateChannel {
        name: "ops".to_owned(),
        visibility: Visibility::Private,
    })
    .await;

    // A non-member sees only the public channel; the private name never appears.
    let mut b = Client::connect(&hub);
    established_path(b.register(&outsider, "david", "wd", "sb").await);
    b.send(ProtocolMessage::ListChannels).await;
    assert_eq!(sorted_channel_names(b.recv().await), vec!["lobby".to_owned()]);

    // The owner (a member of both) sees both.
    a.send(ProtocolMessage::ListChannels).await;
    assert_eq!(sorted_channel_names(a.recv().await), vec!["lobby".to_owned(), "ops".to_owned()]);
}

// -----------------------------------------------------------------------------
// uat-002 (M5) — visibility tiers end-to-end.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn server_visibility_unlisted_is_joinable_by_name_but_not_listed() {
    let hub = hub().await;
    let owner = Identity::generate().unwrap();
    let outsider = Identity::generate().unwrap();

    let mut a = Client::connect(&hub);
    established_path(a.register(&owner, "aaron", "wa", "sa").await);
    a.admin(AdminOp::CreateChannel {
        name: "secret-link".to_owned(),
        visibility: Visibility::Unlisted,
    })
    .await;

    // A non-member who knows the exact name can join an unlisted channel...
    let mut b = Client::connect(&hub);
    let bpath = established_path(b.register(&outsider, "david", "wd", "sb").await);
    assert!(matches!(b.join("secret-link", None).await, ProtocolMessage::Joined { .. }), "unlisted must be joinable by name");
    assert!(hub.subscribers("secret-link").contains(&bpath));

    // ...but it never appears in discovery.
    b.send(ProtocolMessage::ListChannels).await;
    assert_eq!(sorted_channel_names(b.recv().await), Vec::<String>::new(), "unlisted must not be listed in discovery");
}

#[tokio::test]
async fn server_visibility_private_is_hidden_and_gated() {
    let hub = hub().await;
    let owner = Identity::generate().unwrap();
    let outsider = Identity::generate().unwrap();

    let mut a = Client::connect(&hub);
    established_path(a.register(&owner, "aaron", "wa", "sa").await);
    a.admin(AdminOp::CreateChannel {
        name: "ops".to_owned(),
        visibility: Visibility::Private,
    })
    .await;

    let mut b = Client::connect(&hub);
    established_path(b.register(&outsider, "david", "wd", "sb").await);

    // Private is not joinable without an ACL entry or token...
    assert!(is_unauthorized(&b.join("ops", None).await), "private join without a token must be denied");
    // ...and never appears in discovery for a non-member.
    b.send(ProtocolMessage::ListChannels).await;
    assert_eq!(sorted_channel_names(b.recv().await), Vec::<String>::new(), "private must not leak into discovery");
}

// -----------------------------------------------------------------------------
// uat-004 — fan-out: channel message to all subscribers; whisper to exactly one.
// -----------------------------------------------------------------------------

#[tokio::test]
async fn server_fanout_channel_message_reaches_all_subscribers() {
    let hub = hub().await;
    let ida = Identity::generate().unwrap();
    let idb = Identity::generate().unwrap();
    let idc = Identity::generate().unwrap();

    let mut a = Client::connect(&hub);
    let pa = established_path(a.register(&ida, "aaron", "wa", "sa").await);
    a.admin(AdminOp::CreateChannel {
        name: "lobby".to_owned(),
        visibility: Visibility::Public,
    })
    .await;
    assert!(matches!(a.join("lobby", None).await, ProtocolMessage::Joined { .. }));

    let mut b = Client::connect(&hub);
    established_path(b.register(&idb, "david", "wd", "sb").await);
    assert!(matches!(b.join("lobby", None).await, ProtocolMessage::Joined { .. }));

    let mut c = Client::connect(&hub);
    established_path(c.register(&idc, "carol", "wc", "sc").await);
    assert!(matches!(c.join("lobby", None).await, ProtocolMessage::Joined { .. }));

    // A posts; the message reaches the other two subscribers, stamped with A's path.
    a.send(ProtocolMessage::ChannelMsg {
        channel: "lobby".to_owned(),
        from: pa.clone(),
        payload: crate::protocol::Payload::Plain("hello, agents".to_owned()),
    })
    .await;

    for listener in [&mut b, &mut c] {
        match listener.recv().await {
            ProtocolMessage::ChannelMsg { channel, from, payload } => {
                assert_eq!(channel, "lobby");
                assert_eq!(from, pa);
                assert_eq!(payload, crate::protocol::Payload::Plain("hello, agents".to_owned()));
            }
            other => panic!("expected a fanned-out ChannelMsg, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn server_fanout_whisper_reaches_exactly_one_session() {
    let hub = hub().await;
    let ida = Identity::generate().unwrap();
    let idb = Identity::generate().unwrap();
    let idc = Identity::generate().unwrap();

    let mut a = Client::connect(&hub);
    let pa = established_path(a.register(&ida, "aaron", "wa", "sa").await);
    let mut b = Client::connect(&hub);
    let pb = established_path(b.register(&idb, "david", "wd", "sb").await);
    let mut c = Client::connect(&hub);
    established_path(c.register(&idc, "carol", "wc", "sc").await);

    // A whispers to B's exact path.
    a.send(ProtocolMessage::Whisper {
        from: pa.clone(),
        target: pb.clone(),
        payload: crate::protocol::Payload::Plain("just for you".to_owned()),
    })
    .await;

    match b.recv().await {
        ProtocolMessage::Whisper { from, target, payload } => {
            assert_eq!(from, pa);
            assert_eq!(target, pb);
            assert_eq!(payload, crate::protocol::Payload::Plain("just for you".to_owned()));
        }
        other => panic!("expected a Whisper, got {other:?}"),
    }

    // C — a third live session — receives nothing.
    assert!(c.try_recv().await.is_none(), "a whisper must not reach a third session");

    // Whispering to an offline / unknown path errors back to the sender.
    a.send(ProtocolMessage::Whisper {
        from: pa.clone(),
        target: SessionPath::new("ghost", "box", "sess"),
        payload: crate::protocol::Payload::Plain("anyone?".to_owned()),
    })
    .await;
    assert!(matches!(a.recv().await, ProtocolMessage::Error(ProtocolError::NotFound(_))));
}

// -----------------------------------------------------------------------------
// uat-005 — heartbeat reaps a half-open connection; revocation force-drops sessions.
// -----------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn server_presence_reaps_half_open_connection() {
    let hub = hub().await;
    let id = Identity::generate().unwrap();

    let mut client = Client::connect(&hub);
    let path = established_path(client.register(&id, "aaron", "workstation", "razel").await);
    assert!(hub.is_present(&path));

    // Idle well past the timeout with no keepalive → the reaper drops the zombie connection.
    tokio::time::advance(Duration::from_secs(120)).await;
    assert_eq!(hub.reap_idle(Duration::from_secs(60)), 1);
    assert!(!hub.is_present(&path), "a half-open connection must be reaped");
}

#[tokio::test(start_paused = true)]
async fn server_presence_ping_keeps_a_session_alive() {
    let hub = hub().await;
    let id = Identity::generate().unwrap();

    let mut client = Client::connect(&hub);
    let path = established_path(client.register(&id, "aaron", "workstation", "razel").await);

    // A keepalive refreshes the heartbeat, so a subsequent reap with the same timeout spares it.
    tokio::time::advance(Duration::from_secs(50)).await;
    client.send(ProtocolMessage::Ping).await;
    assert!(matches!(client.recv().await, ProtocolMessage::Pong));

    tokio::time::advance(Duration::from_secs(50)).await;
    assert_eq!(hub.reap_idle(Duration::from_secs(60)), 0);
    assert!(hub.is_present(&path), "a heartbeated session must not be reaped");
}

#[tokio::test]
async fn server_presence_machine_remove_force_drops_sessions() {
    let hub = hub().await;
    let id = Identity::generate().unwrap();

    let mut c1 = Client::connect(&hub);
    let p1 = established_path(c1.register(&id, "aaron", "workstation", "s1").await);
    let mut c2 = Client::connect(&hub);
    let p2 = established_path(c2.authenticate(&id, "s2").await);
    assert!(hub.is_present(&p1) && hub.is_present(&p2));

    // Revoking the machine force-drops every live session for that (user, machine) immediately.
    let ack = c1.admin(AdminOp::MachineRemove { name: "workstation".to_owned() }).await;
    assert!(matches!(ack, ProtocolMessage::Ack { .. }));
    assert!(!hub.is_present(&p1));
    assert!(!hub.is_present(&p2));
}

#[tokio::test]
async fn server_presence_kick_removes_from_channel() {
    let hub = hub().await;
    let owner = Identity::generate().unwrap();
    let guest = Identity::generate().unwrap();

    let mut a = Client::connect(&hub);
    let pa = established_path(a.register(&owner, "aaron", "wa", "sa").await);
    a.admin(AdminOp::CreateChannel {
        name: "lobby".to_owned(),
        visibility: Visibility::Public,
    })
    .await;
    assert!(matches!(a.join("lobby", None).await, ProtocolMessage::Joined { .. }));

    let mut b = Client::connect(&hub);
    let pb = established_path(b.register(&guest, "david", "wd", "sb").await);
    assert!(matches!(b.join("lobby", None).await, ProtocolMessage::Joined { .. }));
    assert_eq!(hub.subscribers("lobby").len(), 2);

    // The channel admin kicks david; he leaves the channel but stays connected.
    let ack = a
        .admin(AdminOp::Kick {
            channel: "lobby".to_owned(),
            target: "david".to_owned(),
        })
        .await;
    assert!(matches!(ack, ProtocolMessage::Ack { .. }));

    let subscribers = hub.subscribers("lobby");
    assert!(subscribers.contains(&pa));
    assert!(!subscribers.contains(&pb), "the kicked session must be dropped from the channel");
    assert!(hub.is_present(&pb), "a channel kick must not disconnect the session");
}

#[tokio::test]
async fn server_presence_ban_drops_from_channel_and_blocks_rejoin() {
    let hub = hub().await;
    let owner = Identity::generate().unwrap();
    let guest = Identity::generate().unwrap();

    let mut a = Client::connect(&hub);
    established_path(a.register(&owner, "aaron", "wa", "sa").await);
    a.admin(AdminOp::CreateChannel {
        name: "lobby".to_owned(),
        visibility: Visibility::Public,
    })
    .await;
    assert!(matches!(a.join("lobby", None).await, ProtocolMessage::Joined { .. }));

    let mut b = Client::connect(&hub);
    let pb = established_path(b.register(&guest, "david", "wd", "sb").await);
    assert!(matches!(b.join("lobby", None).await, ProtocolMessage::Joined { .. }));

    // Banning david removes him from the channel and blocks him from re-joining, even though the
    // channel is public.
    let ack = a
        .admin(AdminOp::Ban {
            channel: "lobby".to_owned(),
            user: "david".to_owned(),
        })
        .await;
    assert!(matches!(ack, ProtocolMessage::Ack { .. }));
    assert!(!hub.subscribers("lobby").contains(&pb), "a ban must drop the session from the channel");

    let rejoin = b.join("lobby", None).await;
    assert!(is_unauthorized(&rejoin), "a banned user must not be able to rejoin, got {rejoin:?}");
}
