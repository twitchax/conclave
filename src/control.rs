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
/// `tail` keepalive cadence — comfortably inside the server's 60s idle-reap window.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);

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
    authenticate(&mut ws, identity, session).await?;
    send(&mut ws, &request).await?;
    recv(&mut ws).await
}

/// Joins `channel` and posts one message as the authenticated session, awaiting the server's ack
/// (sends are server-confirmed) — the CLI-as-a-human-client post (PRD-0011 T-004).
///
/// # Errors
///
/// Returns an error if the connection, authentication, join, or send is rejected or times out.
pub async fn send_message(url: &str, identity: &Identity, session: &str, channel: &str, text: &str) -> Res<()> {
    let mut ws = connect(url).await?;
    let from = authenticate(&mut ws, identity, session).await?;

    send(&mut ws, &ProtocolMessage::Join { channel: channel.to_owned(), token: None }).await?;
    match recv(&mut ws).await? {
        ProtocolMessage::Joined { .. } => {}
        ProtocolMessage::Error(err) => anyhow::bail!("join rejected: {err}"),
        other => anyhow::bail!("unexpected response to join: {other:?}"),
    }

    send(
        &mut ws,
        &ProtocolMessage::ChannelMsg {
            channel: channel.to_owned(),
            from,
            payload: protocol::Payload::Plain(text.to_owned()),
        },
    )
    .await?;
    match recv(&mut ws).await? {
        ProtocolMessage::Ack { .. } => Ok(()),
        ProtocolMessage::Error(err) => anyhow::bail!("send rejected: {err}"),
        other => anyhow::bail!("unexpected response to send: {other:?}"),
    }
}

/// Base and cap for tail's reconnect backoff (PRD-0013 T-004): a server restart (deploy) should
/// not kill the watch — reconnect quietly and resume from the watermark.
const TAIL_BACKOFF_BASE: Duration = Duration::from_secs(1);
const TAIL_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Slack subtracted from the resume watermark, preferring a repeated line to a missed one.
const TAIL_RESUME_SLACK_MS: i64 = 5_000;

/// A tail failure that must not be retried (a rejection, not a transport loss).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct TailFatal(String);

/// Joins `channel` and streams its traffic to stdout until Ctrl-C — the minimal human "watch the
/// agents talk" view (PRD-0011 T-004; the §19 aggregation log's smallest sibling). App-level pings
/// keep the session inside the server's idle-reap window.
///
/// With `since_secs`, the retained backlog is replayed first (PRD-0013 T-004). A dropped link
/// (server restart / deploy) reconnects with backoff and resumes from the watermark — status goes
/// to stderr so stdout stays a clean message stream.
///
/// # Errors
///
/// Returns an error if the first connection, authentication, or join fails, or the server rejects
/// the stream; a transport drop after a working stream reconnects instead of erroring.
pub async fn tail(url: &str, identity: &Identity, session: &str, channel: &str, since_secs: Option<u64>) -> Res<()> {
    // The replay/resume watermark: server-stamped ms for history rows, local wall-clock for live
    // rows (close enough within the slack window). `None` = live-only.
    let mut watermark_ms: Option<i64> = since_secs.map(|secs| chrono::Utc::now().timestamp_millis().saturating_sub(i64::try_from(secs).unwrap_or(i64::MAX).saturating_mul(1000)));
    let mut established = false;
    let mut backoff = TAIL_BACKOFF_BASE;

    loop {
        match tail_once(url, identity, session, channel, &mut watermark_ms, &mut established).await {
            Ok(()) => return Ok(()),
            Err(err) if !established || err.downcast_ref::<TailFatal>().is_some() => return Err(err),
            Err(_) => {
                // Transport loss after a working stream: reconnect, don't die (PRD-0013 T-004).
                eprintln!("⚠ connection to `{url}` lost — reconnecting (Ctrl-C to stop)");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => return Ok(()),
                    () = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(TAIL_BACKOFF_MAX);
            }
        }
    }
}

/// One tail connection: connect → join → optional backlog replay → stream. Returns `Ok(())` on
/// Ctrl-C; a [`TailFatal`] error on rejection; any other error is a retryable transport loss.
async fn tail_once(url: &str, identity: &Identity, session: &str, channel: &str, watermark_ms: &mut Option<i64>, established: &mut bool) -> Res<()> {
    use std::io::Write as _;

    let mut ws = connect(url).await?;
    let path = authenticate(&mut ws, identity, session).await?;

    send(&mut ws, &ProtocolMessage::Join { channel: channel.to_owned(), token: None }).await?;
    match recv(&mut ws).await? {
        ProtocolMessage::Joined { channel } => {
            if *established {
                eprintln!("✓ reconnected; resuming #{channel}");
            } else {
                // Announce readiness (and flush: stdout is block-buffered when piped).
                let mut out = std::io::stdout();
                writeln!(out, "tailing #{channel} as {path} — Ctrl-C to stop")?;
                out.flush()?;
                *established = true;
            }
        }
        ProtocolMessage::Error(err) => return Err(TailFatal(format!("join rejected: {err}")).into()),
        other => return Err(TailFatal(format!("unexpected response to join: {other:?}")).into()),
    }

    // Replay the retained backlog from the watermark, minus slack — a repeated line beats a
    // missed one. The page arrives as a History frame in the stream loop below.
    if let Some(since) = *watermark_ms {
        send(
            &mut ws,
            &ProtocolMessage::ReadSince {
                channel: channel.to_owned(),
                since_ms: since.saturating_sub(TAIL_RESUME_SLACK_MS),
            },
        )
        .await?;
    }

    let mut keepalive = tokio::time::interval(KEEPALIVE_INTERVAL);
    keepalive.tick().await; // consume the immediate first tick
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(()),
            _ = keepalive.tick() => send(&mut ws, &ProtocolMessage::Ping).await?,
            frame = recv_frame(&mut ws) => {
                let mut out = std::io::stdout();
                match frame? {
                    ProtocolMessage::ChannelMsg { channel, from, payload } => {
                        writeln!(out, "[{channel}] {from}: {}", render_payload(&payload))?;
                        *watermark_ms = Some(chrono::Utc::now().timestamp_millis());
                    }
                    ProtocolMessage::Whisper { from, payload, .. } => writeln!(out, "[whisper] {from}: {}", render_payload(&payload))?,
                    ProtocolMessage::History { channel, messages } => {
                        for message in &messages {
                            writeln!(out, "[{channel}] {}: {}", message.from, render_payload(&message.payload))?;
                        }
                        if let Some(newest) = messages.iter().map(|m| m.ts_ms).max() {
                            *watermark_ms = Some(watermark_ms.unwrap_or(newest).max(newest));
                        }
                    }
                    // A mid-stream error frame is the server ending us on purpose (superseded,
                    // revoked) — surface it and stop rather than fight to reconnect.
                    ProtocolMessage::Error(err) => return Err(TailFatal(format!("server terminated the stream: {err}")).into()),
                    // Control frames (acks of our own pings etc.) are not part of the stream.
                    _ => continue,
                }
                out.flush()?;
            }
        }
    }
}

/// Renders a message payload for the terminal (plaintext in v1).
fn render_payload(payload: &protocol::Payload) -> &str {
    match payload {
        protocol::Payload::Plain(text) => text,
        protocol::Payload::Encrypted(_) => "<end-to-end-encrypted payload>",
    }
}

/// Completes the challenge-response prologue, returning the session's established path.
async fn authenticate(ws: &mut Ws, identity: &Identity, session: &str) -> Res<SessionPath> {
    let nonce = hello_challenge(ws, session).await?;
    send(
        ws,
        &ProtocolMessage::Auth {
            pubkey: identity.public_key().to_vec(),
            signature: identity.sign(&nonce)?.to_vec(),
        },
    )
    .await?;

    match recv(ws).await? {
        ProtocolMessage::Established { path } => Ok(path),
        ProtocolMessage::Error(err) => anyhow::bail!("authentication rejected: {err}"),
        other => anyhow::bail!("unexpected response before request: {other:?}"),
    }
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
