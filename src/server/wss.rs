//! The production transport: the axum WebSocket endpoint and the `conclave serve` entrypoint.
//!
//! TLS terminates at cloudflared and the origin hop is local loopback (DESIGN.md §11/§12), so this
//! is a plain-HTTP axum server whose single route upgrades to a WebSocket. Each accepted socket is
//! split into reader / writer pumps that translate between WS binary messages and
//! [`ProtocolMessage`](crate::protocol::ProtocolMessage) frames, then driven by the shared
//! [`run_session`]. A background reaper enforces the idle-heartbeat timeout (DESIGN.md §10).

use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context as _;
use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
    routing::get,
};
use tokio::{net::TcpListener, sync::mpsc};

use crate::{
    base::{Constant, Void},
    protocol,
    store::Store,
};

use super::{hub::Hub, session::run_session};

/// How often the heartbeat reaper sweeps for idle sessions.
const REAP_INTERVAL: Duration = Duration::from_secs(15);
/// How long a session may go without any inbound frame before it is reaped (DESIGN.md §10).
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// The operator-supplied `serve` configuration (DESIGN.md §7, §13).
pub struct ServerConfig {
    /// Address to bind the WebSocket endpoint to (e.g. `127.0.0.1:4390`).
    pub bind: String,
    /// Data directory for the embedded store; `None` runs a purely in-memory store.
    pub data_dir: Option<PathBuf>,
    /// The server-admin allowlist — usernames that may administer server-wide (§7), each
    /// optionally pinned to the public key permitted to claim it (see [`super::AdminAllowlist`]).
    pub admins: super::AdminAllowlist,
}

/// Runs the central server until a shutdown signal (Ctrl-C) is received.
///
/// # Errors
///
/// Returns an error if the store cannot be opened, the bind address is unavailable, or the
/// underlying HTTP server fails.
pub async fn serve(config: ServerConfig) -> Void {
    let store = match &config.data_dir {
        Some(path) => Store::open(path).await?,
        None => Store::open_in_memory().await?,
    };
    for (name, pin) in &config.admins {
        if pin.is_none() {
            tracing::warn!(admin = %name, "admin username is unpinned and can be squatted by the first client to register it; pin it as `--admin <user>=<pubkey>`");
        }
    }
    let hub = Hub::new(store, config.admins);

    spawn_reaper(Arc::clone(&hub));

    let app = Router::new().route("/", get(ws_handler)).with_state(hub);
    let listener = TcpListener::bind(&config.bind).await.with_context(|| format!("failed to bind `{}`", config.bind))?;
    let addr = listener.local_addr().context("failed to read the bound address")?;
    tracing::info!(%addr, "conclave server listening");

    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await.context("server terminated with an error")?;
    Ok(())
}

/// Spawns the background heartbeat reaper (DESIGN.md §10).
fn spawn_reaper(hub: Arc<Hub>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(REAP_INTERVAL);
        loop {
            ticker.tick().await;
            let reaped = hub.reap_idle(IDLE_TIMEOUT);
            if reaped > 0 {
                tracing::debug!(reaped, "reaped idle sessions");
            }
        }
    });
}

/// The WebSocket upgrade handler; every connection is dispatched to [`handle_ws`].
async fn ws_handler(ws: WebSocketUpgrade, State(hub): State<Arc<Hub>>) -> Response {
    // Enforce the protocol's frame cap (Constant::MAX_FRAME_SIZE) instead of tungstenite's 64 MiB
    // default, so a pre-auth peer cannot force a large buffer per message (finding #17/#19).
    ws.max_message_size(Constant::MAX_FRAME_SIZE)
        .max_frame_size(Constant::MAX_FRAME_SIZE)
        .on_upgrade(move |socket| handle_ws(hub, socket))
}

/// Bridges a WebSocket to [`run_session`]: each WS binary message is one protocol frame.
async fn handle_ws(hub: Arc<Hub>, socket: WebSocket) {
    use futures_util::{SinkExt as _, StreamExt as _};

    let (mut sink, mut stream) = socket.split();
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();

    let read_task = tokio::spawn(async move {
        while let Some(Ok(message)) = stream.next().await {
            match message {
                Message::Binary(data) => match protocol::decode(&data) {
                    Ok(frame) => {
                        if inbound_tx.send(frame).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                },
                Message::Close(_) => break,
                // Text / ping / pong are ignored: the heartbeat is an app-level Ping/Pong frame.
                _ => {}
            }
        }
    });

    let write_task = tokio::spawn(async move {
        while let Some(frame) = outbound_rx.recv().await {
            let Ok(bytes) = protocol::encode(&frame) else { break };
            if sink.send(Message::Binary(bytes.into())).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });

    run_session(hub, inbound_rx, outbound_tx).await;
    // Await the writer so a final handshake-failure / force-drop frame is flushed and the socket
    // closed cleanly; abort the reader, which may be parked on an idle-but-open socket.
    read_task.abort();
    let _ = write_task.await;
}

/// Resolves when the process receives Ctrl-C, driving the graceful shutdown.
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received; draining connections");
}
