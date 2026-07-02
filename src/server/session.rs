//! The per-connection session driver: the handshake state machine and the inbound frame loop.
//!
//! [`run_session`] is transport-agnostic — it consumes an inbound frame stream and an outbound
//! frame sink (plain `mpsc` channels), so the exact same logic backs the in-memory duplex used by
//! the tests, a raw byte stream, and the axum WebSocket (see [`super::wss`]). [`handle_connection`]
//! adapts any length-delimited byte stream by spawning reader / writer pump tasks around it.
//!
//! Flow (DESIGN.md §5, §14): `Hello` (version-negotiate) → `Challenge` → either `Register`+`Auth`
//! (a brand-new user proving possession of the key it just enrolled) or `Auth` (an already-enrolled
//! machine) → `Established`. Thereafter each inbound frame refreshes the heartbeat and dispatches to
//! the [`Hub`]; a force-drop (revocation / reaping) fires the session's kill signal.

use std::{ops::ControlFlow, sync::Arc};

use tokio::sync::{Notify, mpsc};

use crate::{
    base::SessionPath,
    identity,
    protocol::{ProtocolError, ProtocolMessage, negotiate_version},
};

use super::hub::Hub;

/// The inbound frame stream and outbound frame sink a session is driven over.
type Inbound = mpsc::UnboundedReceiver<ProtocolMessage>;
type Outbound = mpsc::UnboundedSender<ProtocolMessage>;

/// The authenticated identity of a live session (its resolved path plus its kill signal).
struct SessionCtx {
    path: SessionPath,
    kill: Arc<Notify>,
}

/// Drives one authenticated session to completion over the given frame channels.
///
/// Returns when the transport closes, the handshake fails, or the session is force-dropped.
pub(crate) async fn run_session(hub: Arc<Hub>, mut inbound: Inbound, outbound: Outbound) {
    let Some(ctx) = handshake(&hub, &mut inbound, &outbound).await else {
        return;
    };

    let kill = Arc::clone(&ctx.kill);
    loop {
        tokio::select! {
            () = kill.notified() => {
                let _ = outbound.send(err(ProtocolError::Unauthorized("session terminated".to_owned())));
                break;
            }
            frame = inbound.recv() => {
                let Some(frame) = frame else { break };
                hub.touch(&ctx.path);
                if handle_frame(&hub, &ctx, &outbound, frame).await.is_break() {
                    break;
                }
            }
        }
    }

    hub.detach(&ctx.path);
}

/// Adapts a length-delimited byte stream (duplex / TCP) into [`run_session`] via pump tasks.
///
/// This byte-stream transport backs the in-crate integration tests (which drive simulated bridge
/// clients over `tokio::io::duplex`); production runs over the WebSocket adapter ([`super::wss`]).
#[cfg(test)]
pub(crate) async fn handle_connection<S>(hub: Arc<Hub>, stream: S)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();

    let read_task = tokio::spawn(read_pump(reader, inbound_tx));
    let write_task = tokio::spawn(write_pump(writer, outbound_rx));

    run_session(hub, inbound_rx, outbound_tx).await;

    // `run_session` dropped its outbound sender, so the writer drains any final frame (e.g. a
    // handshake-failure or force-drop `Error`) and then ends on channel close — await it so that
    // frame is flushed. The reader may be parked on an idle-but-open socket, so abort it.
    read_task.abort();
    let _ = write_task.await;
}

/// Reads length-delimited frames off the transport into the inbound channel until EOF / error.
#[cfg(test)]
async fn read_pump<R: tokio::io::AsyncRead + Unpin>(mut reader: R, inbound: Outbound) {
    use crate::protocol::ProtocolRead as _;
    while let Ok(frame) = reader.recv_message().await {
        if inbound.send(frame).is_err() {
            break;
        }
    }
}

/// Writes outbound frames to the transport as length-delimited frames until the channel closes.
#[cfg(test)]
async fn write_pump<W: tokio::io::AsyncWrite + Unpin>(mut writer: W, mut outbound: Inbound) {
    use crate::protocol::ProtocolWrite as _;
    while let Some(frame) = outbound.recv().await {
        if writer.send_message(&frame).await.is_err() {
            break;
        }
    }
}

/// Runs the connect handshake, returning the authenticated context or `None` (error already sent).
async fn handshake(hub: &Arc<Hub>, inbound: &mut Inbound, outbound: &Outbound) -> Option<SessionCtx> {
    let ProtocolMessage::Hello { protocol_version, session } = inbound.recv().await? else {
        let _ = outbound.send(err(ProtocolError::MalformedFrame("expected Hello".to_owned())));
        return None;
    };
    if let Err(mismatch) = negotiate_version(protocol_version) {
        let _ = outbound.send(err(mismatch));
        return None;
    }

    let nonce = match identity::generate_challenge() {
        Ok(nonce) => nonce,
        Err(e) => {
            let _ = outbound.send(err(ProtocolError::Internal(e.to_string())));
            return None;
        }
    };
    let _ = outbound.send(ProtocolMessage::Challenge { nonce: nonce.to_vec() });

    let (user, machine) = match inbound.recv().await? {
        ProtocolMessage::Register { username, machine, pubkey } => {
            if let Err(e) = hub.register(&username, &machine, &pubkey).await {
                let _ = outbound.send(err(e));
                return None;
            }
            // Prove possession of the just-enrolled key against the same challenge (§5.1).
            let ProtocolMessage::Auth { pubkey: auth_pubkey, signature } = inbound.recv().await? else {
                let _ = outbound.send(err(ProtocolError::MalformedFrame("expected Auth after Register".to_owned())));
                return None;
            };
            if auth_pubkey != pubkey {
                let _ = outbound.send(err(ProtocolError::Unauthorized("auth key does not match the registered key".to_owned())));
                return None;
            }
            if let Err(e) = identity::verify(&auth_pubkey, &nonce, &signature) {
                let _ = outbound.send(err(e.into()));
                return None;
            }
            (username, machine)
        }
        ProtocolMessage::Auth { pubkey, signature } => {
            if let Err(e) = identity::verify(&pubkey, &nonce, &signature) {
                let _ = outbound.send(err(e.into()));
                return None;
            }
            match hub.resolve(&pubkey).await {
                Ok(resolved) => resolved,
                Err(e) => {
                    let _ = outbound.send(err(e));
                    return None;
                }
            }
        }
        _ => {
            let _ = outbound.send(err(ProtocolError::MalformedFrame("expected Register or Auth".to_owned())));
            return None;
        }
    };

    let path = SessionPath::new(user.clone(), machine.clone(), session);
    let kill = match hub.attach(&path, &user, &machine, outbound.clone()) {
        Ok(kill) => kill,
        Err(e) => {
            let _ = outbound.send(err(e));
            return None;
        }
    };
    let _ = outbound.send(ProtocolMessage::Established { path: path.clone() });
    // Advertise the server-wide role so the bridge can gate its admin tools (DESIGN.md §7).
    let _ = outbound.send(ProtocolMessage::ServerInfo { admin: hub.is_admin(&user) });
    Some(SessionCtx { path, kill })
}

/// Dispatches one inbound frame from an authenticated session to the hub.
async fn handle_frame(hub: &Arc<Hub>, ctx: &SessionCtx, outbound: &Outbound, frame: ProtocolMessage) -> ControlFlow<()> {
    let user = &ctx.path.user;
    match frame {
        ProtocolMessage::Ping => {
            let _ = outbound.send(ProtocolMessage::Pong);
        }
        ProtocolMessage::Join { channel, token } => match hub.join(user, &ctx.path, &channel, token.as_deref()).await {
            Ok(()) => {
                let _ = outbound.send(ProtocolMessage::Joined { channel });
            }
            Err(e) => {
                let _ = outbound.send(err(e));
            }
        },
        ProtocolMessage::Leave { channel } => {
            hub.leave(&ctx.path, &channel);
            let _ = outbound.send(ProtocolMessage::Ack { detail: Some(channel) });
        }
        ProtocolMessage::Who { channel } => match hub.who(user, channel.as_deref()).await {
            Ok(sessions) => {
                let _ = outbound.send(ProtocolMessage::Presence { channel, sessions });
            }
            Err(e) => {
                let _ = outbound.send(err(e));
            }
        },
        ProtocolMessage::ListChannels => match hub.list_channels(user).await {
            Ok(channels) => {
                let _ = outbound.send(ProtocolMessage::ChannelList { channels });
            }
            Err(e) => {
                let _ = outbound.send(err(e));
            }
        },
        ProtocolMessage::ListMachines => match hub.list_machines(user).await {
            Ok(machines) => {
                let _ = outbound.send(ProtocolMessage::MachineList { machines });
            }
            Err(e) => {
                let _ = outbound.send(err(e));
            }
        },
        ProtocolMessage::ListUsers => match hub.list_users(user).await {
            Ok(users) => {
                let _ = outbound.send(ProtocolMessage::UserList { users });
            }
            Err(e) => {
                let _ = outbound.send(err(e));
            }
        },
        ProtocolMessage::Admin(op) => match hub.admin(user, op).await {
            Ok(reply) => {
                let _ = outbound.send(reply);
            }
            Err(e) => {
                let _ = outbound.send(err(e));
            }
        },
        // The client-supplied `from` is ignored; the server stamps the authenticated path (§12).
        ProtocolMessage::ChannelMsg { channel, payload, .. } => {
            if let Err(e) = hub.post(&ctx.path, &channel, payload) {
                let _ = outbound.send(err(e));
            }
        }
        ProtocolMessage::Whisper { target, payload, .. } => {
            if let Err(e) = hub.whisper(&ctx.path, &target, payload) {
                let _ = outbound.send(err(e));
            }
        }
        // Server→client frames (and the handshake frames) are never valid inbound mid-session.
        _ => {
            let _ = outbound.send(err(ProtocolError::MalformedFrame("unexpected frame from client".to_owned())));
        }
    }
    ControlFlow::Continue(())
}

/// Wraps a wire error as an [`ProtocolMessage::Error`] frame.
fn err(error: ProtocolError) -> ProtocolMessage {
    ProtocolMessage::Error(error)
}
