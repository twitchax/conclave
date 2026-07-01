//! End-to-end tests that drive the real `conclave` binary.
//!
//! Each test spawns `env!("CARGO_BIN_EXE_conclave")` inside a `tempfile::TempDir`, so the
//! process runs against a hermetic working directory. The `e2e_serve_*` test stands up a real
//! `conclave serve` on a staggered loopback port and drives it with a WebSocket client through the
//! full register → auth → channel → fan-out flow (DESIGN.md §17), exercising the axum WSS adapter
//! and the binary's `serve` wiring end-to-end.

// Tests relax `unwrap_used` (house convention; DESIGN.md §22).
#![allow(clippy::unwrap_used)]

use std::{
    net::{SocketAddr, TcpListener},
    process::{Child, Command, Stdio},
    time::Duration,
};

use conclavelib::{
    base::{Constant, SessionPath},
    identity::Identity,
    protocol::{Payload, ProtocolMessage, decode, encode},
};
use futures_util::{SinkExt as _, StreamExt as _};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

/// Path to the freshly-built `conclave` binary, injected by Cargo at compile time.
const CONCLAVE_BIN: &str = env!("CARGO_BIN_EXE_conclave");

#[test]
fn e2e_help_advertises_the_command_surface() {
    let workdir = TempDir::new().unwrap();

    let output = Command::new(CONCLAVE_BIN)
        .arg("--help")
        .current_dir(workdir.path())
        .output()
        .expect("failed to spawn `conclave --help`");

    assert!(output.status.success(), "`--help` exited non-zero: {:?}", output.status);

    let stdout = String::from_utf8(output.stdout).unwrap();
    for verb in ["serve", "bridge", "register", "machine", "join", "perm", "key"] {
        assert!(stdout.contains(verb), "help output is missing the `{verb}` subcommand");
    }
}

#[test]
fn e2e_unimplemented_command_fails_cleanly() {
    let workdir = TempDir::new().unwrap();

    let output = Command::new(CONCLAVE_BIN).arg("key").current_dir(workdir.path()).output().expect("failed to spawn `conclave key`");

    // M0 stub: the command parses but the verb is unimplemented, so it exits non-zero with a
    // clear message rather than a panic or a silent success.
    assert!(!output.status.success(), "expected `key` to fail in the M0 scaffold");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("not yet implemented"), "stderr is missing the unimplemented notice: {stderr}");
}

/// A live WebSocket connection to the spawned server, framed one [`ProtocolMessage`] per message.
type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Kills the spawned `conclave serve` process on drop, even if a test assertion panics.
struct ServerProcess(Child);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn e2e_serve_channel_fanout_over_websocket() {
    let data_dir = TempDir::new().unwrap();
    let addr = free_loopback_addr();

    let _server = ServerProcess(
        Command::new(CONCLAVE_BIN)
            .args(["serve", "--bind", &addr.to_string(), "--data-dir", data_dir.path().to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn `conclave serve`"),
    );
    wait_for_listener(addr).await;

    // Two agents connect over real WebSockets.
    let mut alice = ws_connect(addr).await;
    let alice_id = Identity::generate().unwrap();
    let alice_path = ws_register(&mut alice, &alice_id, "aaron", "workstation", "razel").await;

    let mut david = ws_connect(addr).await;
    let david_id = Identity::generate().unwrap();
    ws_register(&mut david, &david_id, "david", "desktop", "main").await;

    // Alice creates a public channel; both join it.
    ws_send(
        &mut alice,
        &ProtocolMessage::Admin(conclavelib::protocol::AdminOp::CreateChannel {
            name: "lobby".to_owned(),
            visibility: conclavelib::base::Visibility::Public,
        }),
    )
    .await;
    assert!(matches!(ws_recv(&mut alice).await, ProtocolMessage::Ack { .. }));

    join(&mut alice, "lobby").await;
    join(&mut david, "lobby").await;

    // Alice posts; the message fans out to David over the wire, stamped with Alice's path.
    ws_send(
        &mut alice,
        &ProtocolMessage::ChannelMsg {
            channel: "lobby".to_owned(),
            from: alice_path.clone(),
            payload: Payload::Plain("hello over websockets".to_owned()),
        },
    )
    .await;

    match ws_recv(&mut david).await {
        ProtocolMessage::ChannelMsg { channel, from, payload } => {
            assert_eq!(channel, "lobby");
            assert_eq!(from, alice_path);
            assert_eq!(payload, Payload::Plain("hello over websockets".to_owned()));
        }
        other => panic!("expected a fanned-out ChannelMsg, got {other:?}"),
    }
}

/// Reserves an ephemeral loopback port (staggered ports, DESIGN.md §17) and frees it for the server.
fn free_loopback_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

/// Polls until the server accepts TCP connections (up to ~5s).
async fn wait_for_listener(addr: SocketAddr) {
    for _ in 0..100 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server never started listening at {addr}");
}

async fn ws_connect(addr: SocketAddr) -> Ws {
    let (ws, _response) = connect_async(format!("ws://{addr}/")).await.expect("failed to open websocket");
    ws
}

async fn ws_send(ws: &mut Ws, frame: &ProtocolMessage) {
    ws.send(Message::Binary(encode(frame).unwrap().into())).await.unwrap();
}

async fn ws_recv(ws: &mut Ws) -> ProtocolMessage {
    loop {
        match ws.next().await.expect("websocket closed").unwrap() {
            Message::Binary(data) => return decode(&data).unwrap(),
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("unexpected websocket message: {other:?}"),
        }
    }
}

async fn ws_register(ws: &mut Ws, id: &Identity, username: &str, machine: &str, session: &str) -> SessionPath {
    ws_send(
        ws,
        &ProtocolMessage::Hello {
            protocol_version: Constant::PROTOCOL_VERSION,
            session: session.to_owned(),
        },
    )
    .await;
    let ProtocolMessage::Challenge { nonce } = ws_recv(ws).await else {
        panic!("expected Challenge");
    };
    let pubkey = id.public_key().to_vec();
    let signature = id.sign(&nonce).unwrap().to_vec();
    ws_send(
        ws,
        &ProtocolMessage::Register {
            username: username.to_owned(),
            machine: machine.to_owned(),
            pubkey: pubkey.clone(),
        },
    )
    .await;
    ws_send(ws, &ProtocolMessage::Auth { pubkey, signature }).await;
    match ws_recv(ws).await {
        ProtocolMessage::Established { path } => path,
        other => panic!("expected Established, got {other:?}"),
    }
}

async fn join(ws: &mut Ws, channel: &str) {
    ws_send(ws, &ProtocolMessage::Join { channel: channel.to_owned(), token: None }).await;
    assert!(matches!(ws_recv(ws).await, ProtocolMessage::Joined { .. }));
}
