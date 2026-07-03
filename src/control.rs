//! The one-shot WS control client used by the CLI verbs (DESIGN.md §13).
//!
//! Unlike the bridge's persistent, reconnecting link ([`crate::bridge`]), each CLI control/admin
//! verb is a single request/response: connect → challenge-response auth (or register) → send one
//! control frame → read the reply → disconnect. The server authorizes admin ops by role, so a
//! non-admin op comes back as a [`ProtocolMessage::Error`] here.

use std::time::Duration;

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

/// Deadlines so a CLI verb never hangs on a dead-but-listening server (PRD-0008 T-004): a bound on
/// the connect + WS upgrade, and a bound on each wait for a server reply.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(15);

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
    connect_with_timeout(url, CONNECT_TIMEOUT).await
}

async fn connect_with_timeout(url: &str, timeout: Duration) -> Res<Ws> {
    crate::base::ensure_tls_provider();
    match tokio::time::timeout(timeout, tokio_tungstenite::connect_async(url)).await {
        Ok(result) => {
            let (ws, _response) = result.with_context(|| format!("failed to connect to `{url}`"))?;
            Ok(ws)
        }
        Err(_) => anyhow::bail!("timed out after {}s connecting to `{url}`", timeout.as_secs()),
    }
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

/// Reads the next protocol frame (bounded by `RESPONSE_TIMEOUT`), skipping keepalive and the
/// post-auth `ServerInfo` signal.
async fn recv(ws: &mut Ws) -> Res<ProtocolMessage> {
    recv_with_timeout(ws, RESPONSE_TIMEOUT).await
}

async fn recv_with_timeout(ws: &mut Ws, timeout: Duration) -> Res<ProtocolMessage> {
    match tokio::time::timeout(timeout, recv_frame(ws)).await {
        Ok(result) => result,
        Err(_) => anyhow::bail!("timed out after {}s waiting for a server response", timeout.as_secs()),
    }
}

async fn recv_frame(ws: &mut Ws) -> Res<ProtocolMessage> {
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

#[cfg(test)]
mod tests {
    // Tests relax `unwrap_used` (house convention; DESIGN.md §22).
    #![allow(clippy::unwrap_used)]

    use std::time::Duration;

    use tokio::net::TcpListener;

    use super::{connect_with_timeout, recv_with_timeout};

    /// A verb against a server that accepts the TCP connection but never completes the WS upgrade
    /// must time out, not hang forever (PRD-0008 T-004, #23).
    #[tokio::test]
    async fn control_timeout_connecting_to_a_silent_server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _accepted = listener.accept().await; // hold the TCP connection open, never speak
            std::future::pending::<()>().await;
        });

        let url = format!("ws://{addr}");
        let err = connect_with_timeout(&url, Duration::from_millis(150)).await.expect_err("a silent server must time out");
        assert!(err.to_string().to_lowercase().contains("timed out"), "expected a timeout error, got: {err}");
    }

    /// A server that completes the WS handshake but never replies to a request must time out on the
    /// read, not hang forever.
    #[tokio::test]
    async fn control_timeout_waiting_for_a_reply_from_a_silent_server() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ws = tokio_tungstenite::accept_async(stream).await.unwrap(); // upgrade, then silence
            std::future::pending::<()>().await;
        });

        let url = format!("ws://{addr}");
        let mut ws = connect_with_timeout(&url, Duration::from_secs(5)).await.unwrap();
        let err = recv_with_timeout(&mut ws, Duration::from_millis(150)).await.expect_err("a silent reply must time out");
        assert!(err.to_string().to_lowercase().contains("timed out"), "expected a timeout error, got: {err}");
    }
}
