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
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

use conclavelib::{
    base::{Constant, PermissionLevel, SessionPath, Visibility},
    identity::{Config, Identity, ServerRegistration, save_config, save_identity},
    protocol::{AdminOp, Payload, ProtocolMessage, decode, encode},
};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader, Lines},
    net::TcpStream,
    process::{ChildStdin, ChildStdout},
    time::timeout,
};
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
fn e2e_key_generates_a_keypair_into_the_config_dir() {
    let config = TempDir::new().unwrap();
    let output = Command::new(CONCLAVE_BIN)
        .args(["--config-dir", config.path().to_str().unwrap(), "key"])
        .output()
        .expect("failed to spawn `conclave key`");

    assert!(output.status.success(), "`key` failed: {}", String::from_utf8_lossy(&output.stderr));
    assert!(!String::from_utf8_lossy(&output.stdout).trim().is_empty(), "`key` should print a public key");
    assert!(config.path().join("key").exists(), "`key` should write the keyfile into the config dir");
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

#[tokio::test]
async fn e2e_serve_drops_a_connection_that_sends_an_oversized_frame() {
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

    let mut ws = ws_connect(addr).await;
    // A binary frame larger than the protocol cap (16 MiB) must be rejected by the transport rather
    // than buffered; the server drops the connection (PRD-0007 T-008, finding #17/#19).
    let oversized = vec![0_u8; Constant::MAX_FRAME_SIZE + 1];
    let _ = ws.send(Message::Binary(oversized.into())).await;

    let dropped = loop {
        match ws.next().await {
            None | Some(Ok(Message::Close(_)) | Err(_)) => break true,
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
            Some(Ok(_)) => break false,
        }
    };
    assert!(dropped, "the server must drop a connection that sends an oversized frame");
}

#[test]
fn e2e_serve_requires_a_data_dir_or_ephemeral() {
    let config = TempDir::new().unwrap();
    // Omitting both --data-dir and --ephemeral must fail loudly rather than silently run in-memory
    // and wipe state on the next restart (PRD-0009 T-002).
    let output = run_cli(config.path(), &["serve"]);
    assert!(!output.status.success(), "serve without --data-dir or --ephemeral must fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("data-dir"), "the error must point at --data-dir; got: {stderr}");
}

#[tokio::test]
async fn e2e_serve_health_endpoint_returns_ok() {
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

    // A raw HTTP GET /health over TCP — the path a platform health check uses (PRD-0009 T-004).
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(format!("GET /health HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "health check must return 200, got: {:?}", response.lines().next());
    assert!(response.trim_end().ends_with("ok"), "health check body must be `ok`, got: {response:?}");
}

#[test]
fn e2e_completions_generate_for_common_shells() {
    for shell in ["bash", "zsh", "fish"] {
        let output = run_cli(TempDir::new().unwrap().path(), &["completions", shell]);
        assert!(output.status.success(), "completions {shell} failed");
        assert!(stdout_of(&output).contains("conclave"), "completions {shell} must mention the binary");
    }
}

/// `conclave send` posts one message; `conclave tail` streams a channel to stdout — the CLI as a
/// human client (PRD-0011 T-004): what a person uses to watch/join agent chatter without a session.
#[tokio::test]
async fn cli_send_and_tail_relay_a_message_through_a_live_channel() {
    let addr = free_loopback_addr();
    let url = format!("ws://{addr}/");
    let server_dir = TempDir::new().unwrap();
    let _server = spawn_server(addr, server_dir.path(), &[]);
    wait_for_listener(addr).await;

    let home = TempDir::new().unwrap();
    let dir = home.path();
    assert!(run_cli(dir, &["register", "--server", &url, "--username", "aaron", "--machine", "workstation"]).status.success());
    assert!(run_cli(dir, &["channel", "create", "--server", &url, "watch", "--visibility", "public"]).status.success());

    // Start `tail` streaming the channel...
    let mut tail = tokio::process::Command::new(CONCLAVE_BIN)
        .args(["tail", "--config-dir", dir.to_str().unwrap(), "--server", &url, "--channel", "watch"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("failed to spawn `conclave tail`");
    let mut tail_lines = BufReader::new(tail.stdout.take().unwrap()).lines();
    // ...and wait for it to announce its subscription so the send can't race the join.
    let ready = timeout(Duration::from_secs(15), tail_lines.next_line()).await.unwrap().unwrap().unwrap();
    assert!(ready.contains("watch"), "tail must announce the joined channel: {ready}");

    // A one-shot `send` posts to the channel (server-acked)...
    let send = run_cli(dir, &["send", "--server", &url, "--channel", "watch", "hello from the plain CLI"]);
    assert!(send.status.success(), "send failed: {}", String::from_utf8_lossy(&send.stderr));

    // ...and the tail streams it with the sender path.
    let line = timeout(Duration::from_secs(15), tail_lines.next_line()).await.unwrap().unwrap().unwrap();
    assert!(
        line.contains("hello from the plain CLI") && line.contains("aaron/workstation"),
        "tail must stream the message with its sender: {line}"
    );
}

/// `conclave status` is the one-command identity/connectivity view: registrations, per-server
/// reachability, and the resolved permission table; an unreachable server exits non-zero.
#[tokio::test]
async fn cli_status_reports_registrations_reachability_and_perms() {
    let home = TempDir::new().unwrap();
    let dir = home.path();

    // With no registrations at all, status succeeds and says so.
    let empty = run_cli(dir, &["status"]);
    assert!(empty.status.success(), "status with no servers must succeed: {}", String::from_utf8_lossy(&empty.stderr));
    assert!(stdout_of(&empty).contains("no servers"), "empty status must say no servers: {}", stdout_of(&empty));

    // Register against a live server and set a channel perm.
    let addr = free_loopback_addr();
    let url = format!("ws://{addr}/");
    let server_dir = TempDir::new().unwrap();
    let server = spawn_server(addr, server_dir.path(), &[]);
    wait_for_listener(addr).await;
    assert!(run_cli(dir, &["register", "--server", &url, "--username", "aaron", "--machine", "workstation"]).status.success());
    assert!(run_cli(dir, &["perm", "set", "converse", "--server", &url, "--channel", "ops"]).status.success());

    // Status shows the registration, reachability, and the perm table.
    let up = run_cli(dir, &["status"]);
    let text = stdout_of(&up);
    assert!(up.status.success(), "status against a live server must succeed: {}", String::from_utf8_lossy(&up.stderr));
    assert!(text.contains(&url) && text.contains("aaron/workstation"), "status must show the registration: {text}");
    assert!(text.contains("reachable"), "status must report reachability: {text}");
    assert!(text.contains("ops") && text.contains("converse"), "status must include the perm table: {text}");

    // With the server down, status reports the failure and exits non-zero.
    drop(server);
    let down = run_cli(dir, &["status"]);
    assert!(!down.status.success(), "status must exit non-zero when a server is unreachable");
    assert!(stdout_of(&down).contains("unreachable"), "status must mark the dead server: {}", stdout_of(&down));
}

/// Fly.io (and every container platform) stops a machine with SIGTERM: the server must drain and
/// exit cleanly (code 0) rather than ignore it and wait out the platform's kill timeout.
#[cfg(unix)]
#[tokio::test]
async fn e2e_serve_drains_gracefully_on_sigterm() {
    let data_dir = TempDir::new().unwrap();
    let addr = free_loopback_addr();
    let mut server = ServerProcess(
        Command::new(CONCLAVE_BIN)
            .args(["serve", "--bind", &addr.to_string(), "--data-dir", data_dir.path().to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn `conclave serve`"),
    );
    wait_for_listener(addr).await;

    // Deliver SIGTERM (what `fly deploy` / `docker stop` send).
    let pid = server.0.id().to_string();
    assert!(Command::new("kill").args(["-TERM", &pid]).status().unwrap().success());

    // The server must exit promptly and cleanly — a signal-terminated process has no exit code.
    let status = 'wait: {
        for _ in 0..100 {
            if let Some(status) = server.0.try_wait().unwrap() {
                break 'wait status;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("server did not exit within 5s of SIGTERM");
    };
    assert_eq!(status.code(), Some(0), "SIGTERM must drain gracefully (exit 0), not kill the process: {status:?}");
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
            Message::Binary(data) => match decode(&data).unwrap() {
                // The post-auth ServerInfo role signal is not asserted here; skip it.
                ProtocolMessage::ServerInfo { .. } => {}
                frame => return frame,
            },
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

// ---------------------------------------------------------------------------
// M3 — serve + two bridge processes; a channel message and a whisper cross the fabric.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_channel_message_and_whisper_between_bridges() {
    let addr = free_loopback_addr();
    let url = format!("ws://{addr}/");
    let server_dir = TempDir::new().unwrap();
    let _server = ServerProcess(
        Command::new(CONCLAVE_BIN)
            .args(["serve", "--bind", &addr.to_string(), "--data-dir", server_dir.path().to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn `conclave serve`"),
    );
    wait_for_listener(addr).await;

    // Provision aaron (creates a public "lobby") and david, each enrolled on the server. Aaron
    // defaults to `converse` so he may emit to the channel and whisper (its own scope, §9).
    let aaron_dir = TempDir::new().unwrap();
    let aaron_id = Identity::generate().unwrap();
    provision(aaron_dir.path(), &aaron_id, &url, "aaron", "workstation", PermissionLevel::Converse);
    {
        let mut ws = ws_connect(addr).await;
        ws_register(&mut ws, &aaron_id, "aaron", "workstation", "setup").await;
        ws_send(
            &mut ws,
            &ProtocolMessage::Admin(AdminOp::CreateChannel {
                name: "lobby".to_owned(),
                visibility: Visibility::Public,
            }),
        )
        .await;
        assert!(matches!(ws_recv(&mut ws).await, ProtocolMessage::Ack { .. }));
    }

    let david_dir = TempDir::new().unwrap();
    let david_id = Identity::generate().unwrap();
    provision(david_dir.path(), &david_id, &url, "david", "desktop", PermissionLevel::Notify);
    {
        let mut ws = ws_connect(addr).await;
        ws_register(&mut ws, &david_id, "david", "desktop", "setup").await;
    }

    // Both bridges come up as MCP servers over stdio; the test plays the role of Claude Code.
    let mut alice = Bridge::spawn(aaron_dir.path(), &url, "alice");
    let mut david = Bridge::spawn(david_dir.path(), &url, "davidsession");
    alice.initialize().await;
    david.initialize().await;

    // Both join lobby; alice's converse default lets her emit, david receives read-only.
    alice.call(1, "join_channel", json!({ "channel": "lobby" })).await;
    david.call(1, "join_channel", json!({ "channel": "lobby" })).await;

    // A channel message from alice is injected into david's session as a <channel> tag.
    alice.call(2, "send_channel", json!({ "channel": "lobby", "text": "hello over the fabric" })).await;
    let note = david.read_injection("hello over the fabric").await;
    assert_eq!(note.pointer("/params/meta/channel").and_then(Value::as_str), Some("lobby"));
    assert!(note.pointer("/params/content").and_then(Value::as_str).unwrap().contains("<channel"));

    // A whisper from alice reaches exactly david's session as a <whisper> tag.
    alice.call(3, "whisper", json!({ "target": "david/desktop/davidsession", "text": "psst — just you" })).await;
    let whisper = david.read_injection("psst — just you").await;
    assert_eq!(whisper.pointer("/params/meta/kind").and_then(Value::as_str), Some("whisper"));
}

/// Writes a bridge's keystore + `config.toml` (identity, permission default, server registration).
fn provision(dir: &Path, identity: &Identity, url: &str, username: &str, machine: &str, default_permission: PermissionLevel) {
    save_identity(dir, identity).unwrap();
    save_config(
        dir,
        &Config {
            default_permission,
            servers: vec![ServerRegistration {
                url: url.to_owned(),
                username: username.to_owned(),
                machine: machine.to_owned(),
            }],
            overrides: vec![],
        },
    )
    .unwrap();
}

/// A spawned `conclave bridge` process, driven over MCP stdio as Claude Code would.
struct Bridge {
    child: tokio::process::Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

impl Bridge {
    fn spawn(config_dir: &Path, url: &str, session: &str) -> Self {
        let mut command = tokio::process::Command::new(CONCLAVE_BIN);
        command.args(["bridge", "--config-dir", config_dir.to_str().unwrap(), "--server", url, "--as", session]);
        Self::from_command(command)
    }

    /// Spawns a bridge that connects to *all* servers in its config (multi-home).
    fn spawn_all(config_dir: &Path, session: &str) -> Self {
        let mut command = tokio::process::Command::new(CONCLAVE_BIN);
        command.args(["bridge", "--config-dir", config_dir.to_str().unwrap(), "--as", session]);
        Self::from_command(command)
    }

    fn from_command(mut command: tokio::process::Command) -> Self {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("failed to spawn `conclave bridge`");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap()).lines();
        Self { child, stdin, stdout }
    }

    async fn send(&mut self, message: Value) {
        let mut line = serde_json::to_vec(&message).unwrap();
        line.push(b'\n');
        self.stdin.write_all(&line).await.unwrap();
        self.stdin.flush().await.unwrap();
    }

    async fn read_matching<F: Fn(&Value) -> bool>(&mut self, predicate: F) -> Value {
        loop {
            let line = timeout(Duration::from_secs(15), self.stdout.next_line())
                .await
                .expect("timed out waiting on bridge stdout")
                .expect("error reading bridge stdout")
                .expect("bridge stdout closed unexpectedly");
            if let Ok(value) = serde_json::from_str::<Value>(&line)
                && predicate(&value)
            {
                return value;
            }
        }
    }

    async fn initialize(&mut self) {
        self.send(json!({
            "jsonrpc": "2.0", "id": 0, "method": "initialize",
            "params": { "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": { "name": "e2e", "version": "0" } }
        }))
        .await;
        let result = self.read_matching(|v| v.get("id") == Some(&json!(0)) && v.get("result").is_some()).await;
        assert!(
            result.pointer("/result/capabilities/experimental").and_then(|e| e.get("claude/channel")).is_some(),
            "bridge must declare the claude/channel capability: {result}"
        );
        self.send(json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })).await;
    }

    async fn call(&mut self, id: i64, name: &str, arguments: Value) -> Value {
        self.send(json!({ "jsonrpc": "2.0", "id": id, "method": "tools/call", "params": { "name": name, "arguments": arguments } }))
            .await;
        self.read_matching(|v| v.get("id") == Some(&json!(id))).await
    }

    async fn read_injection(&mut self, needle: &str) -> Value {
        let needle = needle.to_owned();
        self.read_matching(move |v| v.get("method") == Some(&json!("notifications/claude/channel")) && v.pointer("/params/content").and_then(Value::as_str).is_some_and(|c| c.contains(&needle)))
            .await
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

// ---------------------------------------------------------------------------
// M4 — control & admin CLI verbs, and the /join skill flow.
// ---------------------------------------------------------------------------

/// Spawns `conclave serve` with an optional server-admin allowlist.
fn spawn_server(addr: SocketAddr, data_dir: &Path, admins: &[&str]) -> ServerProcess {
    let mut command = Command::new(CONCLAVE_BIN);
    command.args(["serve", "--bind", &addr.to_string(), "--data-dir", data_dir.to_str().unwrap()]);
    for admin in admins {
        command.args(["--admin", admin]);
    }
    ServerProcess(command.stdout(Stdio::null()).stderr(Stdio::null()).spawn().expect("failed to spawn `conclave serve`"))
}

/// Runs a `conclave` CLI verb against a config directory and returns its captured output.
fn run_cli(config_dir: &Path, args: &[&str]) -> std::process::Output {
    Command::new(CONCLAVE_BIN)
        .arg("--config-dir")
        .arg(config_dir)
        .args(args)
        .output()
        .expect("failed to run a conclave CLI verb")
}

fn stdout_of(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[tokio::test]
async fn cli_control_register_machine_key_join_and_perm() {
    let addr = free_loopback_addr();
    let url = format!("ws://{addr}/");
    let server_dir = TempDir::new().unwrap();
    let _server = spawn_server(addr, server_dir.path(), &[]);
    wait_for_listener(addr).await;

    let home = TempDir::new().unwrap();
    let dir = home.path();

    // `key` generates + prints this machine's public key.
    let key = run_cli(dir, &["key"]);
    assert!(key.status.success());
    assert!(!stdout_of(&key).trim().is_empty());

    // `register` claims a username and enrolls this machine.
    let register = run_cli(dir, &["register", "--server", &url, "--username", "aaron", "--machine", "workstation"]);
    assert!(register.status.success(), "register failed: {}", String::from_utf8_lossy(&register.stderr));

    // `machine add` enrolls a second key; `machine list` shows both.
    let laptop_home = TempDir::new().unwrap();
    let laptop_key = stdout_of(&run_cli(laptop_home.path(), &["key"]));
    let add = run_cli(dir, &["machine", "add", "--server", &url, "--name", "laptop", "--pubkey", laptop_key.trim()]);
    assert!(add.status.success(), "machine add failed: {}", String::from_utf8_lossy(&add.stderr));

    let listing = stdout_of(&run_cli(dir, &["machine", "list", "--server", &url]));
    assert!(listing.contains("workstation") && listing.contains("laptop"), "machine list missing entries: {listing}");

    // `machine remove` revokes it.
    assert!(run_cli(dir, &["machine", "remove", "--server", &url, "laptop"]).status.success());

    // `perm set` / `perm show` are local.
    assert!(run_cli(dir, &["perm", "set", "converse", "--server", &url, "--channel", "ops"]).status.success());
    assert!(stdout_of(&run_cli(dir, &["perm", "show"])).contains("converse"));

    // `join` verifies access to a channel (created here first).
    assert!(run_cli(dir, &["channel", "create", "--server", &url, "ops"]).status.success());
    let join = run_cli(dir, &["join", "--server", &url, "ops"]);
    assert!(join.status.success(), "join failed: {}", String::from_utf8_lossy(&join.stderr));
}

#[tokio::test]
async fn cli_admin_verbs_are_role_gated() {
    let addr = free_loopback_addr();
    let url = format!("ws://{addr}/");
    let server_dir = TempDir::new().unwrap();
    let _server = spawn_server(addr, server_dir.path(), &["aaron"]); // aaron is a server admin
    wait_for_listener(addr).await;

    let admin = TempDir::new().unwrap();
    let user = TempDir::new().unwrap();
    assert!(run_cli(admin.path(), &["register", "--server", &url, "--username", "aaron", "--machine", "wa"]).status.success());
    assert!(run_cli(user.path(), &["register", "--server", &url, "--username", "david", "--machine", "wd"]).status.success());

    // The admin can administer channels, ACLs, invites, and list users.
    assert!(run_cli(admin.path(), &["channel", "create", "--server", &url, "ops", "--visibility", "private"]).status.success());
    assert!(run_cli(admin.path(), &["acl", "add", "--server", &url, "--channel", "ops", "david"]).status.success());
    let invite = run_cli(admin.path(), &["invite", "create", "--server", &url, "--channel", "ops", "--uses", "1"]);
    assert!(invite.status.success() && stdout_of(&invite).contains("invite token"), "invite create failed: {invite:?}");
    let users = run_cli(admin.path(), &["user", "list", "--server", &url]);
    assert!(users.status.success() && stdout_of(&users).contains("aaron"));

    // The non-admin is refused server-admin and other-channel-admin operations.
    assert!(!run_cli(user.path(), &["user", "list", "--server", &url]).status.success(), "non-admin must be refused user list");
    assert!(
        !run_cli(user.path(), &["channel", "delete", "--server", &url, "ops"]).status.success(),
        "non-admin must be refused deleting another's channel"
    );
    assert!(
        !run_cli(user.path(), &["invite", "create", "--server", &url, "--channel", "ops"]).status.success(),
        "non-admin must be refused minting invites"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn e2e_join_skill_join_with_perm_connects_subscribes_and_emits() {
    // The packaged skill documents the join tool and the /join entry point.
    let skill = run_cli(TempDir::new().unwrap().path(), &["skill"]);
    let skill_text = stdout_of(&skill);
    assert!(skill.status.success());
    assert!(
        skill_text.contains("join_channel") && skill_text.contains("/join") && skill_text.contains("perm"),
        "skill must document /join with a perm"
    );
    // The first-time flow must be walkable by an agent: MCP registration, the channels research-
    // preview gate (by registered server name), and the shared-handle (--as) footgun warning.
    assert!(skill_text.contains("claude mcp add"), "skill must document registering the bridge with Claude Code");
    assert!(
        skill_text.contains("--dangerously-load-development-channels server:conclave"),
        "skill must document the channels research-preview flag with the registered-server form"
    );
    assert!(skill_text.contains("--as"), "skill must warn about baking --as into the registered command");

    // And the underlying flow works: join_channel with perm=converse subscribes AND lets the session emit.
    let addr = free_loopback_addr();
    let url = format!("ws://{addr}/");
    let server_dir = TempDir::new().unwrap();
    let _server = spawn_server(addr, server_dir.path(), &[]);
    wait_for_listener(addr).await;

    let aaron_dir = TempDir::new().unwrap();
    let aaron_id = Identity::generate().unwrap();
    provision(aaron_dir.path(), &aaron_id, &url, "aaron", "workstation", PermissionLevel::Notify);
    {
        let mut ws = ws_connect(addr).await;
        ws_register(&mut ws, &aaron_id, "aaron", "workstation", "setup").await;
        ws_send(
            &mut ws,
            &ProtocolMessage::Admin(AdminOp::CreateChannel {
                name: "lobby".to_owned(),
                visibility: Visibility::Public,
            }),
        )
        .await;
        assert!(matches!(ws_recv(&mut ws).await, ProtocolMessage::Ack { .. }));
    }

    let mut alice = Bridge::spawn(aaron_dir.path(), &url, "alice");
    alice.initialize().await;
    // /join with a converse perm.
    alice.call(1, "join_channel", json!({ "channel": "lobby", "perm": "converse" })).await;
    // Because the perm took effect, the emit tool is now permitted at call time.
    let sent = alice.call(2, "send_channel", json!({ "channel": "lobby", "text": "joined via the skill" })).await;
    assert_ne!(
        sent.pointer("/result/isError").and_then(Value::as_bool),
        Some(true),
        "converse perm from --perm must permit send: {sent}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_multihome_routes_to_the_correct_server_and_target() {
    let addr_a = free_loopback_addr();
    let addr_b = free_loopback_addr();
    let url_a = format!("ws://{addr_a}/");
    let url_b = format!("ws://{addr_b}/");
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let _server_a = spawn_server(addr_a, dir_a.path(), &[]);
    let _server_b = spawn_server(addr_b, dir_b.path(), &[]);
    wait_for_listener(addr_a).await;
    wait_for_listener(addr_b).await;

    // Aaron is enrolled on BOTH servers (one key, two registrations) and owns a channel on each.
    let aaron_id = Identity::generate().unwrap();
    let aaron_dir = TempDir::new().unwrap();
    save_identity(aaron_dir.path(), &aaron_id).unwrap();
    save_config(
        aaron_dir.path(),
        &Config {
            // Converse everywhere so aaron may emit to channels and whisper (its own scope, §9).
            default_permission: PermissionLevel::Converse,
            servers: vec![
                ServerRegistration {
                    url: url_a.clone(),
                    username: "aaron".to_owned(),
                    machine: "workstation".to_owned(),
                },
                ServerRegistration {
                    url: url_b.clone(),
                    username: "aaron".to_owned(),
                    machine: "workstation".to_owned(),
                },
            ],
            overrides: vec![],
        },
    )
    .unwrap();
    for (addr, channel) in [(addr_a, "a-chan"), (addr_b, "b-chan")] {
        let mut ws = ws_connect(addr).await;
        ws_register(&mut ws, &aaron_id, "aaron", "workstation", "setup").await;
        ws_send(
            &mut ws,
            &ProtocolMessage::Admin(AdminOp::CreateChannel {
                name: channel.to_owned(),
                visibility: Visibility::Public,
            }),
        )
        .await;
        assert!(matches!(ws_recv(&mut ws).await, ProtocolMessage::Ack { .. }));
    }

    // Listeners: david on server A (joined a-chan), evan on server B (joined b-chan).
    let mut david = ws_connect(addr_a).await;
    let david_path = ws_register(&mut david, &Identity::generate().unwrap(), "david", "desktop", "dsession").await;
    join(&mut david, "a-chan").await;
    let mut evan = ws_connect(addr_b).await;
    ws_register(&mut evan, &Identity::generate().unwrap(), "evan", "laptop", "esession").await;
    join(&mut evan, "b-chan").await;

    // One bridge, both servers; join a channel on each.
    let mut aaron = Bridge::spawn_all(aaron_dir.path(), "multi");
    aaron.initialize().await;
    aaron.call(1, "join_channel", json!({ "server": url_a, "channel": "a-chan" })).await;
    aaron.call(2, "join_channel", json!({ "server": url_b, "channel": "b-chan" })).await;

    // A message to (A, a-chan) reaches david; a message to (B, b-chan) reaches evan.
    aaron.call(3, "send_channel", json!({ "server": url_a, "channel": "a-chan", "text": "for A" })).await;
    aaron.call(4, "send_channel", json!({ "server": url_b, "channel": "b-chan", "text": "for B" })).await;

    match ws_recv(&mut david).await {
        ProtocolMessage::ChannelMsg { channel, payload, .. } => {
            assert_eq!(channel, "a-chan");
            assert_eq!(payload, Payload::Plain("for A".to_owned()));
        }
        other => panic!("david (server A) expected the a-chan message, got {other:?}"),
    }
    match ws_recv(&mut evan).await {
        ProtocolMessage::ChannelMsg { channel, payload, .. } => {
            assert_eq!(channel, "b-chan");
            assert_eq!(payload, Payload::Plain("for B".to_owned()));
        }
        other => panic!("evan (server B) expected the b-chan message, got {other:?}"),
    }

    // A whisper to a (server, target-path) reaches exactly that session on that server.
    aaron.call(5, "whisper", json!({ "server": url_a, "target": david_path.to_string(), "text": "psst A" })).await;
    match ws_recv(&mut david).await {
        ProtocolMessage::Whisper { payload, .. } => assert_eq!(payload, Payload::Plain("psst A".to_owned())),
        other => panic!("david expected a whisper, got {other:?}"),
    }
}
