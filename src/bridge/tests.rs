//! Orchestrator-level tests: drive [`BridgeCore`] with synthetic MCP events and inbound server
//! frames, asserting injections, tool results, and outbound frames — no real stdio or WS. Covers
//! the inbound-injection (`bridge_inject`) and permission-enforcement (`bridge_perm`) UATs.

// Tests relax `unwrap_used` (house convention; DESIGN.md §22).
#![allow(clippy::unwrap_used)]

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use pretty_assertions::assert_eq;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use super::{BridgeCore, mcp::FromMcp, mcp::McpSink};
use crate::{
    base::{PermissionLevel, SessionPath},
    identity::{Config, PermissionOverride, ServerRegistration},
    protocol::{Payload, ProtocolMessage},
};

use super::sink::{Injection, NotificationSink};

/// Collects injections in-memory for assertions (stands in for the CC session pane).
struct CapturingSink {
    injections: Arc<Mutex<Vec<Injection>>>,
}

impl NotificationSink for CapturingSink {
    fn deliver(&self, injection: &Injection) {
        self.injections.lock().unwrap().push(injection.clone());
    }
}

struct Harness {
    core: BridgeCore,
    to_mcp_rx: mpsc::UnboundedReceiver<Value>,
    to_server_rx: mpsc::UnboundedReceiver<ProtocolMessage>,
    injections: Arc<Mutex<Vec<Injection>>>,
    joined: Arc<Mutex<HashSet<String>>>,
}

fn make_config(default: PermissionLevel, overrides: Vec<PermissionOverride>) -> Config {
    Config {
        default_permission: default,
        servers: vec![],
        overrides,
    }
}

fn override_for(server: &str, channel: Option<&str>, level: PermissionLevel) -> PermissionOverride {
    PermissionOverride {
        server: server.to_owned(),
        channel: channel.map(str::to_owned),
        level,
    }
}

fn harness(config: Config) -> Harness {
    let (to_mcp_tx, to_mcp_rx) = mpsc::unbounded_channel();
    let injections = Arc::new(Mutex::new(Vec::new()));
    let sink = Box::new(CapturingSink { injections: Arc::clone(&injections) });
    let mut core = BridgeCore::new(config, "razel".to_owned(), to_mcp_tx, sink);

    let (to_server_tx, to_server_rx) = mpsc::unbounded_channel();
    let joined = Arc::new(Mutex::new(HashSet::new()));
    core.register_server(
        ServerRegistration {
            url: "s1".to_owned(),
            username: "aaron".to_owned(),
            machine: "workstation".to_owned(),
        },
        to_server_tx,
        Arc::clone(&joined),
    );

    Harness {
        core,
        to_mcp_rx,
        to_server_rx,
        injections,
        joined,
    }
}

fn channel_msg(channel: &str, body: &str) -> ProtocolMessage {
    ProtocolMessage::ChannelMsg {
        channel: channel.to_owned(),
        from: SessionPath::new("david", "desktop", "main"),
        payload: Payload::Plain(body.to_owned()),
    }
}

// -----------------------------------------------------------------------------
// uat-001 — inbound injection.
// -----------------------------------------------------------------------------

#[test]
fn bridge_inject_channel_message_reaches_the_sink() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));
    harness.core.handle_inbound("s1", channel_msg("ops", "deploy is green"));

    let injections = harness.injections.lock().unwrap();
    assert_eq!(injections.len(), 1);
    assert_eq!(injections[0].channel.as_deref(), Some("ops"));
    assert_eq!(injections[0].body, "deploy is green");
    assert_eq!(injections[0].level, PermissionLevel::Notify);
}

#[test]
fn bridge_inject_whisper_reaches_the_sink_as_a_whisper() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));
    harness.core.handle_inbound(
        "s1",
        ProtocolMessage::Whisper {
            from: SessionPath::new("david", "desktop", "main"),
            target: SessionPath::new("aaron", "workstation", "razel"),
            payload: Payload::Plain("just you".to_owned()),
        },
    );

    let injections = harness.injections.lock().unwrap();
    assert_eq!(injections.len(), 1);
    assert_eq!(injections[0].channel, None);
    assert_eq!(injections[0].kind(), "whisper");
}

#[test]
fn bridge_inject_delivers_a_notifications_claude_channel_frame() {
    // With the real MCP sink, an inbound message becomes a `notifications/claude/channel`.
    let (to_mcp_tx, mut to_mcp_rx) = mpsc::unbounded_channel();
    let sink = Box::new(McpSink::new(to_mcp_tx.clone()));
    let mut core = BridgeCore::new(make_config(PermissionLevel::Notify, vec![]), "razel".to_owned(), to_mcp_tx, sink);
    core.register_server(
        ServerRegistration {
            url: "s1".to_owned(),
            username: "aaron".to_owned(),
            machine: "workstation".to_owned(),
        },
        mpsc::unbounded_channel().0,
        Arc::new(Mutex::new(HashSet::new())),
    );

    core.handle_inbound("s1", channel_msg("ops", "hello"));

    let note = to_mcp_rx.try_recv().unwrap();
    assert_eq!(note.get("method").and_then(Value::as_str), Some("notifications/claude/channel"));
    assert!(note.pointer("/params/content").and_then(Value::as_str).unwrap().contains("<channel"));
    assert_eq!(note.pointer("/params/meta/channel").and_then(Value::as_str), Some("ops"));
}

// -----------------------------------------------------------------------------
// uat-003 — permission enforcement.
// -----------------------------------------------------------------------------

#[test]
fn bridge_perm_mute_drops_inbound() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![override_for("s1", Some("ops"), PermissionLevel::Mute)]));
    harness.core.handle_inbound("s1", channel_msg("ops", "spam"));
    assert!(harness.injections.lock().unwrap().is_empty(), "a muted channel must drop delivery");
}

#[test]
fn bridge_perm_send_below_converse_is_rejected_at_call_time() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(1),
        name: "send_channel".to_owned(),
        args: json!({ "channel": "ops", "text": "hi" }),
    });

    // The tool call is rejected...
    let result = harness.to_mcp_rx.try_recv().unwrap();
    assert_eq!(result.pointer("/result/isError").and_then(Value::as_bool), Some(true));
    // ...and nothing was emitted to the server.
    assert!(harness.to_server_rx.try_recv().is_err(), "a below-converse send must not reach the server");
}

#[test]
fn bridge_perm_send_is_allowed_at_converse() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![override_for("s1", Some("ops"), PermissionLevel::Converse)]));
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(2),
        name: "send_channel".to_owned(),
        args: json!({ "channel": "ops", "text": "deploying" }),
    });

    let result = harness.to_mcp_rx.try_recv().unwrap();
    assert_ne!(result.pointer("/result/isError").and_then(Value::as_bool), Some(true));
    match harness.to_server_rx.try_recv().unwrap() {
        ProtocolMessage::ChannelMsg { channel, payload, .. } => {
            assert_eq!(channel, "ops");
            assert_eq!(payload, Payload::Plain("deploying".to_owned()));
        }
        other => panic!("expected a ChannelMsg, got {other:?}"),
    }
}

#[test]
fn bridge_perm_emit_tools_are_gated_by_converse() {
    let harness = harness(make_config(PermissionLevel::Notify, vec![override_for("s1", Some("ops"), PermissionLevel::Converse)]));

    // No joined channel yet is >= converse → emit tools withheld (session-global gating).
    let names: Vec<&str> = harness.core.tools().iter().map(|t| t.name).collect();
    assert!(!names.contains(&"send_channel"));
    assert!(!names.contains(&"whisper"));
    assert!(names.contains(&"join_channel"), "control tools are always offered");

    // Joining the converse channel exposes the emit tools.
    harness.joined.lock().unwrap().insert("ops".to_owned());
    let names: Vec<&str> = harness.core.tools().iter().map(|t| t.name).collect();
    assert!(names.contains(&"send_channel"));
    assert!(names.contains(&"whisper"));
}

// -----------------------------------------------------------------------------
// Tool ⇄ response correlation.
// -----------------------------------------------------------------------------

#[test]
fn bridge_join_channel_tool_defers_then_resolves_on_the_ack() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(9),
        name: "join_channel".to_owned(),
        args: json!({ "channel": "ops" }),
    });

    // The join is sent to the server and the tool result is deferred (no MCP reply yet).
    assert!(matches!(harness.to_server_rx.try_recv().unwrap(), ProtocolMessage::Join { channel, .. } if channel == "ops"));
    assert!(harness.to_mcp_rx.try_recv().is_err());

    // The server's ack resolves the original tool call (matched to its id).
    harness.core.handle_inbound("s1", ProtocolMessage::Joined { channel: "ops".to_owned() });
    let result = harness.to_mcp_rx.try_recv().unwrap();
    assert_eq!(result.get("id"), Some(&json!(9)));
    assert!(result.pointer("/result/content/0/text").and_then(Value::as_str).unwrap().contains("joined ops"));
}
