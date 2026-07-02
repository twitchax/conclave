//! The outbound WS client to central servers: one link per `(session, server)`, with backoff
//! reconnect and re-subscribe (DESIGN.md §11, §16).
//!
//! Each link is a reconnect loop ([`run_link`]) around a `connect` step that yields an
//! authenticated [`LinkIo`] (frames in / frames out). On every (re)connect the joined channels are
//! re-subscribed and the backoff resets; a drop backs off and retries. The `connect` step is a
//! closure so the loop is testable without a socket; the production step ([`connect_ws`]) dials a
//! WebSocket and runs the challenge-response handshake.

use std::{
    collections::HashSet,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context as _;
use futures_util::{SinkExt as _, StreamExt as _, stream::SplitSink};
use tokio::{
    net::TcpStream,
    sync::{Notify, mpsc},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

use crate::{
    base::{Constant, Res, Void},
    identity::Identity,
    protocol::{self, ProtocolMessage},
};

/// Base and cap for the exponential reconnect backoff (DESIGN.md §16).
const BACKOFF_BASE: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Frames in / frames out for one authenticated server link (post-handshake).
pub(crate) struct LinkIo {
    /// Frames to send to the server.
    pub to_server: mpsc::UnboundedSender<ProtocolMessage>,
    /// Frames received from the server.
    pub from_server: mpsc::UnboundedReceiver<ProtocolMessage>,
}

/// Exponential backoff with a cap, reset on a successful connect.
struct Backoff {
    current: Duration,
}

impl Backoff {
    fn new() -> Self {
        Self { current: BACKOFF_BASE }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(BACKOFF_MAX);
        delay
    }

    fn reset(&mut self) {
        self.current = BACKOFF_BASE;
    }
}

/// The reconnect loop for one server link: (re)connect → re-subscribe → pump → back off → repeat.
pub(crate) async fn run_link<C, Fut>(
    server: String,
    mut connect: C,
    joined: Arc<Mutex<HashSet<String>>>,
    inbound_tx: mpsc::UnboundedSender<(String, ProtocolMessage)>,
    mut outbound_rx: mpsc::UnboundedReceiver<ProtocolMessage>,
    shutdown: Arc<Notify>,
) where
    C: FnMut() -> Fut,
    Fut: Future<Output = Res<LinkIo>>,
{
    let mut backoff = Backoff::new();
    loop {
        match connect().await {
            Ok(io) => {
                backoff.reset();
                resubscribe(&joined, &io.to_server);
                outbound_rx = pump(server.clone(), io, inbound_tx.clone(), outbound_rx, &shutdown).await;
            }
            Err(err) => tracing::warn!(%server, error = %err, "server connect failed; will retry"),
        }

        tokio::select! {
            () = shutdown.notified() => break,
            () = tokio::time::sleep(backoff.next_delay()) => {}
        }
    }
}

/// Re-issues a `Join` for every currently-joined channel on a fresh connection (DESIGN.md §16).
fn resubscribe(joined: &Arc<Mutex<HashSet<String>>>, to_server: &mpsc::UnboundedSender<ProtocolMessage>) {
    for channel in joined.lock().expect("joined mutex poisoned").iter() {
        let _ = to_server.send(ProtocolMessage::Join { channel: channel.clone(), token: None });
    }
}

/// Shuttles frames between the server link and the orchestrator until either side closes, returning
/// the outbound receiver so the next reconnect can reuse it.
async fn pump(
    server: String,
    mut io: LinkIo,
    inbound_tx: mpsc::UnboundedSender<(String, ProtocolMessage)>,
    mut outbound_rx: mpsc::UnboundedReceiver<ProtocolMessage>,
    shutdown: &Arc<Notify>,
) -> mpsc::UnboundedReceiver<ProtocolMessage> {
    loop {
        tokio::select! {
            () = shutdown.notified() => break,
            frame = io.from_server.recv() => {
                let Some(frame) = frame else { break };
                if inbound_tx.send((server.clone(), frame)).is_err() {
                    break;
                }
            }
            outbound = outbound_rx.recv() => {
                let Some(outbound) = outbound else { break };
                if io.to_server.send(outbound).is_err() {
                    break;
                }
            }
        }
    }
    outbound_rx
}

/// The type of a live WebSocket to a central server.
type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Dials a server over WebSocket and completes the challenge-response handshake, returning an
/// authenticated [`LinkIo`] backed by translation pumps (frames ⇄ WS binary messages).
pub(crate) async fn connect_ws(url: &str, identity: &Identity, session: &str) -> Res<LinkIo> {
    crate::base::ensure_tls_provider();
    let (ws, _response) = tokio_tungstenite::connect_async(url).await.with_context(|| format!("failed to connect to `{url}`"))?;
    let (mut sink, mut stream) = ws.split();

    ws_send(
        &mut sink,
        &ProtocolMessage::Hello {
            protocol_version: Constant::PROTOCOL_VERSION,
            session: session.to_owned(),
        },
    )
    .await?;

    let nonce = match ws_recv(&mut stream).await? {
        ProtocolMessage::Challenge { nonce } => nonce,
        other => anyhow::bail!("expected Challenge, got {other:?}"),
    };
    let signature = identity.sign(&nonce)?;
    ws_send(
        &mut sink,
        &ProtocolMessage::Auth {
            pubkey: identity.public_key().to_vec(),
            signature: signature.to_vec(),
        },
    )
    .await?;

    match ws_recv(&mut stream).await? {
        ProtocolMessage::Established { .. } => {}
        ProtocolMessage::Error(err) => anyhow::bail!("authentication rejected: {err}"),
        other => anyhow::bail!("expected Established, got {other:?}"),
    }

    let (to_tx, to_rx) = mpsc::unbounded_channel();
    let (from_tx, from_rx) = mpsc::unbounded_channel();
    tokio::spawn(ws_write_pump(sink, to_rx));
    tokio::spawn(ws_read_pump(stream, from_tx));
    Ok(LinkIo { to_server: to_tx, from_server: from_rx })
}

async fn ws_send(sink: &mut SplitSink<Ws, Message>, frame: &ProtocolMessage) -> Void {
    let bytes = protocol::encode(frame)?;
    sink.send(Message::Binary(bytes.into())).await.context("websocket send failed")?;
    Ok(())
}

async fn ws_recv(stream: &mut futures_util::stream::SplitStream<Ws>) -> Res<ProtocolMessage> {
    loop {
        match stream.next().await {
            Some(Ok(Message::Binary(data))) => return protocol::decode(&data),
            Some(Ok(Message::Close(_))) | None => anyhow::bail!("websocket closed during handshake"),
            Some(Ok(_)) => {}
            Some(Err(err)) => anyhow::bail!("websocket error: {err}"),
        }
    }
}

async fn ws_write_pump(mut sink: SplitSink<Ws, Message>, mut rx: mpsc::UnboundedReceiver<ProtocolMessage>) {
    while let Some(frame) = rx.recv().await {
        let Ok(bytes) = protocol::encode(&frame) else { break };
        if sink.send(Message::Binary(bytes.into())).await.is_err() {
            break;
        }
    }
    let _ = sink.close().await;
}

async fn ws_read_pump(mut stream: futures_util::stream::SplitStream<Ws>, tx: mpsc::UnboundedSender<ProtocolMessage>) {
    while let Some(message) = stream.next().await {
        match message {
            Ok(Message::Binary(data)) => {
                if let Ok(frame) = protocol::decode(&data)
                    && tx.send(frame).is_err()
                {
                    break;
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests relax `unwrap_used` (house convention; DESIGN.md §22).
    #![allow(clippy::unwrap_used)]

    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn bridge_reconnect_backoff_grows_and_resets() {
        let mut backoff = Backoff::new();
        assert_eq!(backoff.next_delay(), Duration::from_millis(200));
        assert_eq!(backoff.next_delay(), Duration::from_millis(400));
        assert_eq!(backoff.next_delay(), Duration::from_millis(800));

        backoff.reset();
        assert_eq!(backoff.next_delay(), Duration::from_millis(200));
    }

    #[test]
    fn bridge_reconnect_backoff_saturates_at_the_cap() {
        let mut backoff = Backoff::new();
        for _ in 0..20 {
            backoff.next_delay();
        }
        assert_eq!(backoff.next_delay(), BACKOFF_MAX);
    }

    #[tokio::test]
    async fn bridge_reconnect_pump_forwards_both_directions() {
        let (to_tx, mut to_rx) = mpsc::unbounded_channel();
        let (from_tx, from_rx) = mpsc::unbounded_channel();
        let (inbound_tx, mut inbound_rx) = mpsc::unbounded_channel();
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
        let shutdown = Arc::new(Notify::new());

        let pump_shutdown = Arc::clone(&shutdown);
        let handle = tokio::spawn(async move {
            pump("s1".to_owned(), LinkIo { to_server: to_tx, from_server: from_rx }, inbound_tx, outbound_rx, &pump_shutdown).await;
        });

        // Server → orchestrator: a frame from the link is tagged with the server name.
        from_tx.send(ProtocolMessage::Pong).unwrap();
        assert_eq!(inbound_rx.recv().await, Some(("s1".to_owned(), ProtocolMessage::Pong)));

        // Orchestrator → server: an outbound frame reaches the link.
        outbound_tx.send(ProtocolMessage::Ping).unwrap();
        assert_eq!(to_rx.recv().await, Some(ProtocolMessage::Ping));

        shutdown.notify_waiters();
        let _ = handle.await;
    }

    #[tokio::test(start_paused = true)]
    async fn bridge_reconnect_retries_then_resubscribes_joined_channels() {
        let joined = Arc::new(Mutex::new(HashSet::from(["ops".to_owned()])));
        let (inbound_tx, _inbound_rx) = mpsc::unbounded_channel();
        let (_outbound_tx, outbound_rx) = mpsc::unbounded_channel::<ProtocolMessage>();
        let shutdown = Arc::new(Notify::new());
        let (capture_tx, mut capture_rx) = mpsc::unbounded_channel();
        let attempts = Arc::new(AtomicUsize::new(0));

        let connect = {
            let attempts = Arc::clone(&attempts);
            move || {
                let attempts = Arc::clone(&attempts);
                let capture_tx = capture_tx.clone();
                async move {
                    // Fail the first two attempts, then hand a live link back to the test.
                    if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                        anyhow::bail!("simulated connect failure");
                    }
                    let (to_tx, to_rx) = mpsc::unbounded_channel();
                    let (_from_tx, from_rx) = mpsc::unbounded_channel();
                    let _ = capture_tx.send(to_rx);
                    Ok(LinkIo { to_server: to_tx, from_server: from_rx })
                }
            }
        };

        let handle = tokio::spawn(run_link("s1".to_owned(), connect, joined, inbound_tx, outbound_rx, Arc::clone(&shutdown)));

        // Drive the two backoff sleeps forward until the third (successful) attempt captures a link.
        let mut to_rx = loop {
            for _ in 0..5 {
                tokio::task::yield_now().await;
            }
            if let Ok(rx) = capture_rx.try_recv() {
                break rx;
            }
            tokio::time::advance(Duration::from_secs(60)).await;
        };

        // The successful connect re-subscribed the joined channel.
        match to_rx.recv().await {
            Some(ProtocolMessage::Join { channel, .. }) => assert_eq!(channel, "ops"),
            other => panic!("expected a re-subscribe Join, got {other:?}"),
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        shutdown.notify_waiters();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn connect_ws_dials_wss_with_tls_enabled() {
        // A plain-TCP listener that reads the client's TLS ClientHello then closes — NOT a TLS
        // server. Dialing it over wss:// must install the crypto provider and attempt TLS (rustls
        // 0.23 would otherwise panic), then fail on the non-TLS peer — never returning tungstenite's
        // "TLS support not compiled in" as it did before PRD-0009 T-001.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                use tokio::io::AsyncReadExt as _;
                let mut buf = [0_u8; 1024];
                let _ = socket.read(&mut buf).await;
                drop(socket);
            }
        });

        let identity = Identity::generate().unwrap();
        let url = format!("wss://127.0.0.1:{}/", addr.port());
        let dial = connect_ws(&url, &identity, "s");
        // Ok(Err(_)) = dialed within the timeout and failed at the TLS handshake (the desired
        // outcome); Ok(Ok(_)) would be an impossible success; Err(_) would be a hang.
        let dialed_and_failed = matches!(tokio::time::timeout(Duration::from_secs(10), dial).await, Ok(Err(_)));
        assert!(dialed_and_failed, "a wss:// dial to a non-TLS peer must fail at the TLS handshake, not panic, hang, or succeed");
    }
}
