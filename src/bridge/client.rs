//! The outbound WS client to central servers: one link per `(session, server)`, with backoff
//! reconnect and re-subscribe (DESIGN.md §11, §16).
//!
//! Each link is a reconnect loop ([`run_link`]) around a `connect` step that yields an
//! authenticated [`LinkIo`] (frames in / frames out). On every (re)connect the joined channels are
//! re-subscribed; a drop backs off and retries, and the backoff resets only once a link has stayed
//! up for [`STABLE_UPTIME`] (an instantly-killed connect keeps backing off, PRD-0012 T-001). The
//! `connect` step is a closure so the loop is testable without a socket; the production step
//! ([`connect_ws`]) dials a WebSocket and runs the challenge-response handshake.

use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

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
/// A link must stay up this long before a drop resets the backoff. An instantly-killed connect
/// (a supersede fight between two links holding the same session path, PRD-0012 T-001) counts as
/// a failure, not a success — reset-on-connect is what let that fight loop at the base delay.
/// Shared with the orchestrator's notice policy (PRD-0015 T-002): the same window decides when a
/// flapping link has "stabilized".
pub(crate) const STABLE_UPTIME: Duration = Duration::from_secs(30);

/// Frames in / frames out for one authenticated server link (post-handshake).
pub(crate) struct LinkIo {
    /// Frames to send to the server.
    pub to_server: mpsc::UnboundedSender<ProtocolMessage>,
    /// Frames received from the server.
    pub from_server: mpsc::UnboundedReceiver<ProtocolMessage>,
}

/// An event on a server link, carried to the orchestrator over the shared inbound channel: link
/// lifecycle plus each received frame. The lifecycle events let the dispatcher re-establish and fail
/// pending request↔response correlation across reconnects (PRD-0008 T-001/T-002).
pub(crate) enum LinkEvent {
    /// A fresh connection is up — the orchestrator re-subscribes the joined channels.
    Up,
    /// The link dropped — the orchestrator fails this server's pending tool calls.
    Down,
    /// The URL reached a server another link already claims; this link has permanently shut down
    /// (PRD-0012 T-003) — `canonical` is the URL that owns the server.
    Duplicate {
        /// The URL that first claimed this server's instance ID.
        canonical: String,
    },
    /// A protocol frame received from the server.
    Frame(ProtocolMessage),
}

/// Process-wide map of server instance ID → the URL that first claimed it. Two configured URLs
/// reaching the same server would otherwise supersede each other's session forever (PRD-0012).
pub(crate) type ServerClaims = Arc<std::sync::Mutex<HashMap<String, String>>>;

/// A connect refused because the URL reached a server another link already claims.
#[derive(Debug, thiserror::Error)]
#[error("`{url}` reaches the same server as `{canonical}`")]
pub(crate) struct DuplicateServer {
    /// The URL whose link is being disabled.
    pub url: String,
    /// The URL that first claimed the server's instance ID.
    pub canonical: String,
}

/// Claims `instance_id` for `url`: idempotent for the claim holder (reconnects), refused for any
/// other URL. Claims live for the process lifetime — this is a startup dedupe, not a handoff.
fn claim_server_id(claims: &ServerClaims, instance_id: &str, url: &str) -> Result<(), DuplicateServer> {
    let mut claims = claims.lock().expect("server claims mutex poisoned");
    match claims.get(instance_id) {
        Some(canonical) if canonical != url => Err(DuplicateServer {
            url: url.to_owned(),
            canonical: canonical.clone(),
        }),
        Some(_) => Ok(()),
        None => {
            claims.insert(instance_id.to_owned(), url.to_owned());
            Ok(())
        }
    }
}

/// Exponential backoff with a cap, reset once a connect proves stable ([`STABLE_UPTIME`]).
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

    /// Notes how long the dropped link had been up; only a stable link resets the backoff.
    fn on_disconnect(&mut self, uptime: Duration) {
        if uptime >= STABLE_UPTIME {
            self.current = BACKOFF_BASE;
        }
    }
}

/// The reconnect loop for one server link: (re)connect → re-subscribe → pump → back off → repeat.
pub(crate) async fn run_link<C, Fut>(
    server: String,
    mut connect: C,
    inbound_tx: mpsc::UnboundedSender<(String, LinkEvent)>,
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
                let connected_at = tokio::time::Instant::now();
                // The orchestrator re-subscribes joined channels on `Up` and fails this server's
                // pending tool calls on `Down`, so correlation survives reconnects (T-001/T-002).
                let _ = inbound_tx.send((server.clone(), LinkEvent::Up));
                outbound_rx = pump(server.clone(), io, inbound_tx.clone(), outbound_rx, &shutdown).await;
                let _ = inbound_tx.send((server.clone(), LinkEvent::Down));
                backoff.on_disconnect(connected_at.elapsed());
            }
            Err(err) => {
                // A duplicate-server refusal is terminal: the canonical link owns this server,
                // and retrying could only re-create the supersede fight the dedupe prevents.
                if let Some(dup) = err.downcast_ref::<DuplicateServer>() {
                    let _ = inbound_tx.send((server.clone(), LinkEvent::Duplicate { canonical: dup.canonical.clone() }));
                    return;
                }
                tracing::warn!(%server, error = %err, "server connect failed; will retry");
            }
        }

        tokio::select! {
            () = shutdown.notified() => break,
            () = tokio::time::sleep(backoff.next_delay()) => {}
        }
    }
}

/// Shuttles frames between the server link and the orchestrator until either side closes, returning
/// the outbound receiver so the next reconnect can reuse it.
async fn pump(
    server: String,
    mut io: LinkIo,
    inbound_tx: mpsc::UnboundedSender<(String, LinkEvent)>,
    mut outbound_rx: mpsc::UnboundedReceiver<ProtocolMessage>,
    shutdown: &Arc<Notify>,
) -> mpsc::UnboundedReceiver<ProtocolMessage> {
    loop {
        tokio::select! {
            () = shutdown.notified() => break,
            frame = io.from_server.recv() => {
                let Some(frame) = frame else { break };
                if inbound_tx.send((server.clone(), LinkEvent::Frame(frame))).is_err() {
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
///
/// The upgrade response's instance-ID header is checked against `claims` *before* the handshake,
/// so a duplicate URL never authenticates — and therefore never supersedes the canonical link's
/// session (PRD-0012 T-003). A server without the header (pre-T-003) skips the check.
pub(crate) async fn connect_ws(url: &str, identity: &Identity, session: &str, claims: &ServerClaims) -> Res<LinkIo> {
    crate::base::ensure_tls_provider();
    let (ws, response) = tokio_tungstenite::connect_async(url).await.with_context(|| format!("failed to connect to `{url}`"))?;
    if let Some(id) = response.headers().get(Constant::SERVER_ID_HEADER).and_then(|v| v.to_str().ok()) {
        claim_server_id(claims, id, url)?;
    }
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
    fn bridge_reconnect_backoff_resets_only_after_stable_uptime() {
        let mut backoff = Backoff::new();
        assert_eq!(backoff.next_delay(), Duration::from_millis(200));
        assert_eq!(backoff.next_delay(), Duration::from_millis(400));

        // An instantly-killed connect (a supersede fight, PRD-0012 T-001) must keep backing off —
        // reset-on-connect is what let the storm run at the 200ms base forever...
        backoff.on_disconnect(Duration::from_millis(50));
        assert_eq!(backoff.next_delay(), Duration::from_millis(800));

        // ...and only a link that stayed up past the stability window earns a reset.
        backoff.on_disconnect(STABLE_UPTIME);
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
        match inbound_rx.recv().await {
            Some((server, LinkEvent::Frame(ProtocolMessage::Pong))) => assert_eq!(server, "s1"),
            _ => panic!("expected a forwarded Pong frame tagged with the server name"),
        }

        // Orchestrator → server: an outbound frame reaches the link.
        outbound_tx.send(ProtocolMessage::Ping).unwrap();
        assert_eq!(to_rx.recv().await, Some(ProtocolMessage::Ping));

        shutdown.notify_waiters();
        let _ = handle.await;
    }

    #[tokio::test(start_paused = true)]
    async fn bridge_reconnect_retries_then_signals_link_up() {
        let (inbound_tx, mut inbound_rx) = mpsc::unbounded_channel();
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

        let handle = tokio::spawn(run_link("s1".to_owned(), connect, inbound_tx, outbound_rx, Arc::clone(&shutdown)));

        // Drive the two backoff sleeps forward until the third (successful) attempt captures a link.
        loop {
            for _ in 0..5 {
                tokio::task::yield_now().await;
            }
            if capture_rx.try_recv().is_ok() {
                break;
            }
            tokio::time::advance(Duration::from_secs(60)).await;
        }

        // A successful (re)connect signals `Up` to the orchestrator, which then re-subscribes.
        match inbound_rx.recv().await {
            Some((server, LinkEvent::Up)) => assert_eq!(server, "s1"),
            _ => panic!("expected a LinkEvent::Up from the successful reconnect"),
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        shutdown.notify_waiters();
        let _ = handle.await;
    }

    #[test]
    fn bridge_server_claims_dedupe_by_instance_id() {
        let claims = ServerClaims::default();
        // The first URL claims the ID; re-claims by the same URL (reconnects) stay fine.
        claim_server_id(&claims, "id-1", "wss://a").unwrap();
        claim_server_id(&claims, "id-1", "wss://a").unwrap();
        // A different URL landing on the same server is refused, naming the canonical URL —
        // this is the two-URLs-one-server supersede storm's root fix (PRD-0012 T-003).
        let err = claim_server_id(&claims, "id-1", "wss://b").unwrap_err();
        assert_eq!(err.canonical, "wss://a");
        // A genuinely different server is unaffected.
        claim_server_id(&claims, "id-2", "wss://b").unwrap();
    }

    #[tokio::test]
    async fn bridge_reconnect_duplicate_server_disables_the_link() {
        let (inbound_tx, mut inbound_rx) = mpsc::unbounded_channel();
        let (_outbound_tx, outbound_rx) = mpsc::unbounded_channel::<ProtocolMessage>();
        let shutdown = Arc::new(Notify::new());

        let connect = || async {
            Err::<LinkIo, _>(anyhow::Error::new(DuplicateServer {
                url: "wss://b".to_owned(),
                canonical: "wss://a".to_owned(),
            }))
        };

        // The loop must surface the duplicate once and exit on its own — retrying a refused
        // duplicate could only re-create the supersede fight the dedupe exists to prevent.
        run_link("wss://b".to_owned(), connect, inbound_tx, outbound_rx, shutdown).await;
        match inbound_rx.recv().await {
            Some((server, LinkEvent::Duplicate { canonical })) => {
                assert_eq!(server, "wss://b");
                assert_eq!(canonical, "wss://a");
            }
            _ => panic!("expected a LinkEvent::Duplicate from the refused connect"),
        }
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
        let claims = ServerClaims::default();
        let dial = connect_ws(&url, &identity, "s", &claims);
        // Ok(Err(_)) = dialed within the timeout and failed at the TLS handshake (the desired
        // outcome); Ok(Ok(_)) would be an impossible success; Err(_) would be a hang.
        let dialed_and_failed = matches!(tokio::time::timeout(Duration::from_secs(10), dial).await, Ok(Err(_)));
        assert!(dialed_and_failed, "a wss:// dial to a non-TLS peer must fail at the TLS handshake, not panic, hang, or succeed");
    }
}
