//! The one-shot WS control client used by the CLI verbs (DESIGN.md §13).
//!
//! Unlike the bridge's persistent, reconnecting link ([`crate::bridge`]), each CLI control/admin
//! verb is a single request/response: connect → challenge-response auth (or register) → send one
//! control frame → read the reply → disconnect. The server authorizes admin ops by role, so a
//! non-admin op comes back as a [`ProtocolMessage::Error`] here.

use anyhow::Context as _;
use futures_util::{SinkExt as _, StreamExt as _};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::{
    base::{Constant, Res, SessionPath},
    identity::Identity,
    protocol::{self, ProtocolMessage},
};

/// A live one-shot WebSocket to a server.
type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Claims a username + enrolls this machine on `url`, returning the resolved session path.
///
/// # Errors
///
/// Returns an error if the connection, handshake, or registration is rejected.
pub async fn register(url: &str, identity: &Identity, username: &str, machine: &str, session: &str) -> Res<SessionPath> {
    let mut ws = connect(url).await?;
    let nonce = hello_challenge(&mut ws, session).await?;
    let pubkey = identity.public_key().to_vec();
    send(
        &mut ws,
        &ProtocolMessage::Register {
            username: username.to_owned(),
            machine: machine.to_owned(),
            pubkey: pubkey.clone(),
        },
    )
    .await?;
    send(
        &mut ws,
        &ProtocolMessage::Auth {
            pubkey,
            signature: identity.sign(&nonce)?.to_vec(),
        },
    )
    .await?;

    match recv(&mut ws).await? {
        ProtocolMessage::Established { path } => Ok(path),
        ProtocolMessage::Error(err) => anyhow::bail!("registration rejected: {err}"),
        other => anyhow::bail!("unexpected response to register: {other:?}"),
    }
}

/// Authenticates with the local key on `url` and performs one request/response control exchange.
///
/// # Errors
///
/// Returns an error if the connection or authentication fails; a server-side rejection of the
/// request itself is returned as a [`ProtocolMessage::Error`] value (not an `Err`).
pub async fn one_shot(url: &str, identity: &Identity, session: &str, request: ProtocolMessage) -> Res<ProtocolMessage> {
    let mut ws = connect(url).await?;
    let nonce = hello_challenge(&mut ws, session).await?;
    send(
        &mut ws,
        &ProtocolMessage::Auth {
            pubkey: identity.public_key().to_vec(),
            signature: identity.sign(&nonce)?.to_vec(),
        },
    )
    .await?;

    match recv(&mut ws).await? {
        ProtocolMessage::Established { .. } => {}
        ProtocolMessage::Error(err) => anyhow::bail!("authentication rejected: {err}"),
        other => anyhow::bail!("unexpected response before request: {other:?}"),
    }

    send(&mut ws, &request).await?;
    recv(&mut ws).await
}

async fn connect(url: &str) -> Res<Ws> {
    let (ws, _response) = tokio_tungstenite::connect_async(url).await.with_context(|| format!("failed to connect to `{url}`"))?;
    Ok(ws)
}

async fn hello_challenge(ws: &mut Ws, session: &str) -> Res<Vec<u8>> {
    send(
        ws,
        &ProtocolMessage::Hello {
            protocol_version: Constant::PROTOCOL_VERSION,
            session: session.to_owned(),
        },
    )
    .await?;
    match recv(ws).await? {
        ProtocolMessage::Challenge { nonce } => Ok(nonce),
        other => anyhow::bail!("expected a challenge, got {other:?}"),
    }
}

async fn send(ws: &mut Ws, frame: &ProtocolMessage) -> Res<()> {
    ws.send(Message::Binary(protocol::encode(frame)?.into())).await.context("failed to send control frame")?;
    Ok(())
}

/// Reads the next protocol frame, skipping keepalive and the post-auth `ServerInfo` signal.
async fn recv(ws: &mut Ws) -> Res<ProtocolMessage> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Binary(data))) => match protocol::decode(&data)? {
                ProtocolMessage::ServerInfo { .. } | ProtocolMessage::Pong => {}
                frame => return Ok(frame),
            },
            Some(Ok(Message::Close(_))) | None => anyhow::bail!("connection closed before a response arrived"),
            Some(Ok(_)) => {}
            Some(Err(err)) => anyhow::bail!("websocket error: {err}"),
        }
    }
}
