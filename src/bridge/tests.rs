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

use super::{BridgeCore, client, mcp::FromMcp, mcp::McpSink};
use crate::{
    base::{PermissionLevel, SessionPath, Visibility},
    identity::{Config, PermissionOverride, ServerRegistration},
    protocol::{AdminOp, Payload, ProtocolError, ProtocolMessage},
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

    // The send is deferred (no immediate MCP result) and reaches the server as a ChannelMsg.
    assert!(harness.to_mcp_rx.try_recv().is_err(), "an allowed send defers its result until the server acks");
    match harness.to_server_rx.try_recv().unwrap() {
        ProtocolMessage::ChannelMsg { channel, payload, .. } => {
            assert_eq!(channel, "ops");
            assert_eq!(payload, Payload::Plain("deploying".to_owned()));
        }
        other => panic!("expected a ChannelMsg, got {other:?}"),
    }

    // The server's Ack resolves the deferred tool call as a success.
    harness.core.handle_inbound("s1", ProtocolMessage::Ack { detail: None });
    let result = harness.to_mcp_rx.try_recv().unwrap();
    assert_ne!(result.pointer("/result/isError").and_then(Value::as_bool), Some(true));
}

// -----------------------------------------------------------------------------
// uat-003 — live per-(server, channel) permission override (no reconnect).
// -----------------------------------------------------------------------------

#[test]
fn perm_live_set_perm_applies_without_reconnect() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));

    // Default notify → inbound injected read-only.
    harness.core.handle_inbound("s1", channel_msg("ops", "one"));
    assert_eq!(harness.injections.lock().unwrap().last().unwrap().level, PermissionLevel::Notify);

    // Bump to converse live via set_perm — the very next inbound resolves at converse.
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(1),
        name: "set_perm".to_owned(),
        args: json!({ "channel": "ops", "level": "converse" }),
    });
    harness.core.handle_inbound("s1", channel_msg("ops", "two"));
    assert_eq!(harness.injections.lock().unwrap().last().unwrap().level, PermissionLevel::Converse);

    // Dropping to mute live suppresses delivery entirely.
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(2),
        name: "set_perm".to_owned(),
        args: json!({ "channel": "ops", "level": "mute" }),
    });
    let before = harness.injections.lock().unwrap().len();
    harness.core.handle_inbound("s1", channel_msg("ops", "three"));
    assert_eq!(harness.injections.lock().unwrap().len(), before, "a live mute must drop delivery");
}

#[test]
fn perm_live_set_perm_is_always_offered() {
    let harness = harness(make_config(PermissionLevel::Notify, vec![]));
    assert!(harness.core.tools().iter().any(|t| t.name == "set_perm"), "set_perm must always be available");
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

// -----------------------------------------------------------------------------
// uat-003 — gated admin MCP tools.
// -----------------------------------------------------------------------------

#[test]
fn bridge_admin_tools_hidden_until_the_server_marks_admin() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));

    // No ServerInfo yet → the admin tools are not offered.
    let names: Vec<&str> = harness.core.tools().iter().map(|t| t.name).collect();
    assert!(!names.contains(&"create_channel"));
    assert!(!names.contains(&"kick"));

    // The server signals admin → every admin tool is offered.
    harness.core.handle_inbound("s1", ProtocolMessage::ServerInfo { admin: true });
    let names: Vec<&str> = harness.core.tools().iter().map(|t| t.name).collect();
    for tool in [
        "create_channel",
        "delete_channel",
        "set_visibility",
        "acl_add",
        "acl_remove",
        "invite_create",
        "invite_revoke",
        "kick",
        "ban",
    ] {
        assert!(names.contains(&tool), "admin tool `{tool}` must be offered to an admin");
    }

    // A subsequent non-admin signal withdraws them.
    harness.core.handle_inbound("s1", ProtocolMessage::ServerInfo { admin: false });
    assert!(!harness.core.tools().iter().any(|t| t.name == "create_channel"), "admin tools must be withdrawn for a non-admin");
}

#[test]
fn bridge_admin_tools_dispatch_an_admin_op_and_resolve_on_ack() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));
    harness.core.handle_inbound("s1", ProtocolMessage::ServerInfo { admin: true });

    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(5),
        name: "create_channel".to_owned(),
        args: json!({ "name": "ops", "visibility": "private" }),
    });

    // The admin op is sent to the server; the tool result is deferred.
    match harness.to_server_rx.try_recv().unwrap() {
        ProtocolMessage::Admin(AdminOp::CreateChannel { name, visibility }) => {
            assert_eq!(name, "ops");
            assert_eq!(visibility, Visibility::Private);
        }
        other => panic!("expected Admin(CreateChannel), got {other:?}"),
    }
    assert!(harness.to_mcp_rx.try_recv().is_err());

    // The server's ack resolves the tool call by id.
    harness.core.handle_inbound("s1", ProtocolMessage::Ack { detail: Some("ops".to_owned()) });
    assert_eq!(harness.to_mcp_rx.try_recv().unwrap().get("id"), Some(&json!(5)));
}

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

// -----------------------------------------------------------------------------
// PRD-0008 T-001 / T-002 — response correlation across sends and link churn.
// -----------------------------------------------------------------------------

#[test]
fn bridge_correlation_out_of_band_error_does_not_misroute_a_pending_tool() {
    let mut harness = harness(make_config(PermissionLevel::Converse, vec![]));
    // A whisper (deferred) then a who (deferred), in that order.
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(1),
        name: "whisper".to_owned(),
        args: json!({ "target": "ghost/box/sess", "text": "hi" }),
    });
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(2),
        name: "who".to_owned(),
        args: json!({}),
    });

    // The server responds in order: the whisper fails, then the who succeeds.
    harness.core.handle_inbound("s1", ProtocolMessage::Error(ProtocolError::NotFound("ghost".to_owned())));
    harness.core.handle_inbound("s1", ProtocolMessage::Presence { channel: None, sessions: vec![] });

    // The error resolves the whisper (id 1); the presence resolves the who (id 2) — not swapped.
    let first = harness.to_mcp_rx.try_recv().unwrap();
    assert_eq!(first.get("id"), Some(&json!(1)));
    assert_eq!(first.pointer("/result/isError").and_then(Value::as_bool), Some(true));
    let second = harness.to_mcp_rx.try_recv().unwrap();
    assert_eq!(second.get("id"), Some(&json!(2)));
    assert_ne!(second.pointer("/result/isError").and_then(Value::as_bool), Some(true));
}

#[test]
fn bridge_link_down_fails_pending_tool_calls() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(7),
        name: "who".to_owned(),
        args: json!({}),
    });
    assert!(harness.to_mcp_rx.try_recv().is_err(), "the who defers");

    // The link drops before the server responds — the pending call is failed, not left to hang.
    harness.core.link_down("s1");
    let result = harness.to_mcp_rx.try_recv().unwrap();
    assert_eq!(result.get("id"), Some(&json!(7)));
    assert_eq!(result.pointer("/result/isError").and_then(Value::as_bool), Some(true));
}

#[test]
fn bridge_link_up_resubscribe_ack_is_consumed_silently() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));
    harness.joined.lock().unwrap().insert("ops".to_owned());

    // On reconnect the dispatcher re-subscribes the joined channel...
    harness.core.link_up("s1");
    assert!(matches!(harness.to_server_rx.try_recv().unwrap(), ProtocolMessage::Join { channel, .. } if channel == "ops"));

    // ...and its Joined ack is internal — it must not surface as a tool result.
    harness.core.handle_inbound("s1", ProtocolMessage::Joined { channel: "ops".to_owned() });
    assert!(harness.to_mcp_rx.try_recv().is_err(), "a re-subscribe Joined must not answer a tool call");
}

// -----------------------------------------------------------------------------
// PRD-0008 T-003 — confirm joins before recording them locally.
// -----------------------------------------------------------------------------

#[test]
fn bridge_join_records_only_after_confirmation() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(8),
        name: "join_channel".to_owned(),
        args: json!({ "channel": "ops" }),
    });
    assert!(!harness.joined.lock().unwrap().contains("ops"), "a join must not be recorded before the server confirms");

    harness.core.handle_inbound("s1", ProtocolMessage::Joined { channel: "ops".to_owned() });
    assert!(harness.joined.lock().unwrap().contains("ops"), "a confirmed join must be recorded");
}

#[test]
fn bridge_rejected_join_is_not_recorded() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(4),
        name: "join_channel".to_owned(),
        args: json!({ "channel": "secret" }),
    });
    harness.core.handle_inbound("s1", ProtocolMessage::Error(ProtocolError::NotFound("secret".to_owned())));

    assert!(!harness.joined.lock().unwrap().contains("secret"), "a rejected join must not be recorded");
    let result = harness.to_mcp_rx.try_recv().unwrap();
    assert_eq!(result.pointer("/result/isError").and_then(Value::as_bool), Some(true));
}

#[test]
fn bridge_leave_channel_unsubscribes_without_disconnecting() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));

    // Join (confirmed) — the channel is recorded locally.
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(1),
        name: "join_channel".to_owned(),
        args: json!({ "channel": "ops" }),
    });
    assert!(matches!(harness.to_server_rx.try_recv().unwrap(), ProtocolMessage::Join { .. }));
    harness.core.handle_inbound("s1", ProtocolMessage::Joined { channel: "ops".to_owned() });
    let _ = harness.to_mcp_rx.try_recv().unwrap();
    assert!(harness.joined.lock().unwrap().contains("ops"));

    // leave_channel emits the Leave frame and forgets the subscription (no resubscribe on reconnect).
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(2),
        name: "leave_channel".to_owned(),
        args: json!({ "channel": "ops" }),
    });
    assert!(
        matches!(harness.to_server_rx.try_recv().unwrap(), ProtocolMessage::Leave { channel } if channel == "ops"),
        "leave_channel must emit a Leave frame",
    );
    assert!(!harness.joined.lock().unwrap().contains("ops"), "the local subscription must be forgotten");

    // The server's ack resolves the tool call.
    harness.core.handle_inbound("s1", ProtocolMessage::Ack { detail: Some("ops".to_owned()) });
    let result = harness.to_mcp_rx.try_recv().unwrap();
    assert_eq!(result.get("id"), Some(&json!(2)));
    assert!(result.pointer("/result/content/0/text").and_then(Value::as_str).unwrap().contains("left ops"));
}

#[test]
fn bridge_link_state_changes_notify_the_session() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));

    // A drop surfaces a disconnect notice to the session...
    harness.core.link_down("s1");
    let note = harness.to_mcp_rx.try_recv().unwrap();
    assert!(
        note.pointer("/params/content").and_then(Value::as_str).unwrap().to_lowercase().contains("disconnected"),
        "a link drop must surface a disconnect notice",
    );

    // ...but a repeated drop does not re-notify...
    harness.core.link_down("s1");
    assert!(harness.to_mcp_rx.try_recv().is_err(), "repeated disconnects notify only once");

    // ...and a reconnect surfaces a reconnect notice.
    harness.core.link_up("s1");
    let note = harness.to_mcp_rx.try_recv().unwrap();
    assert!(
        note.pointer("/params/content").and_then(Value::as_str).unwrap().to_lowercase().contains("reconnected"),
        "a reconnect must surface a reconnect notice",
    );
}

#[test]
fn bridge_admin_tool_is_scoped_to_a_server_that_asserted_admin() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));

    // No ServerInfo{admin} received yet → an admin op on s1 is refused and never reaches the server.
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(1),
        name: "create_channel".to_owned(),
        args: json!({ "name": "ops" }),
    });
    let result = harness.to_mcp_rx.try_recv().unwrap();
    assert_eq!(result.pointer("/result/isError").and_then(Value::as_bool), Some(true));
    assert!(harness.to_server_rx.try_recv().is_err(), "a non-admin admin op must not reach the server");

    // Once s1 asserts admin, the same op is forwarded.
    harness.core.handle_inbound("s1", ProtocolMessage::ServerInfo { admin: true });
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(2),
        name: "create_channel".to_owned(),
        args: json!({ "name": "ops" }),
    });
    assert!(
        matches!(harness.to_server_rx.try_recv().unwrap(), ProtocolMessage::Admin(_)),
        "an admin op on a server that asserted admin is forwarded",
    );
}

// -----------------------------------------------------------------------------
// PRD-0015 T-001 — dynamic tool list (notifications/tools/list_changed).
// -----------------------------------------------------------------------------

/// Drains queued MCP messages, counting `tools/list_changed` notifications.
fn drain_list_changed(rx: &mut mpsc::UnboundedReceiver<Value>) -> usize {
    let mut count = 0;
    while let Ok(msg) = rx.try_recv() {
        if msg.get("method") == Some(&json!("notifications/tools/list_changed")) {
            count += 1;
        }
    }
    count
}

/// Drains queued MCP messages, returning channel-notification texts.
fn drain_notice_texts(rx: &mut mpsc::UnboundedReceiver<Value>) -> Vec<String> {
    let mut texts = Vec::new();
    while let Ok(msg) = rx.try_recv() {
        if msg.get("method") == Some(&json!("notifications/claude/channel"))
            && let Some(content) = msg.pointer("/params/content").and_then(Value::as_str)
        {
            texts.push(content.to_owned());
        }
    }
    texts
}

#[test]
fn bridge_initialize_declares_tools_list_changed_capability() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));
    harness.core.handle_mcp(FromMcp::Initialize {
        id: json!(0),
        protocol_version: "2025-06-18".to_owned(),
    });

    let reply = harness.to_mcp_rx.try_recv().unwrap();
    assert_eq!(
        reply.pointer("/result/capabilities/tools/listChanged"),
        Some(&json!(true)),
        "the client only honors list_changed if the capability is declared: {reply}"
    );
}

#[test]
fn bridge_tools_list_changed_fires_when_set_perm_enables_emit() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));
    harness.joined.lock().unwrap().insert("ops".to_owned());

    // Gating flips: a joined channel goes notify → converse, so send/whisper must appear — and
    // Claude Code caches tools/list, so it must be told to re-fetch (found live: send_channel
    // could never appear mid-session, PRD-0015 T-001).
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(1),
        name: "set_perm".to_owned(),
        args: json!({ "channel": "ops", "level": "converse" }),
    });
    assert_eq!(drain_list_changed(&mut harness.to_mcp_rx), 1, "the client must be told to re-fetch tools/list");

    // Idempotent: re-setting the same level changes no gating, so no re-notify.
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(2),
        name: "set_perm".to_owned(),
        args: json!({ "channel": "ops", "level": "converse" }),
    });
    assert_eq!(drain_list_changed(&mut harness.to_mcp_rx), 0, "an unchanged toolset must not re-notify");
}

#[test]
fn bridge_tools_list_changed_fires_when_admin_status_arrives() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));

    harness.core.handle_link_event("s1", client::LinkEvent::Frame(ProtocolMessage::ServerInfo { admin: true }));
    assert_eq!(drain_list_changed(&mut harness.to_mcp_rx), 1, "admin arrival exposes the admin tools");

    // An ordinary frame that changes no gating must not re-notify.
    harness.core.handle_link_event("s1", client::LinkEvent::Frame(channel_msg("ops", "hi")));
    assert_eq!(drain_list_changed(&mut harness.to_mcp_rx), 0);
}

// -----------------------------------------------------------------------------
// PRD-0015 T-002 — handle-conflict circuit breaker.
// -----------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn bridge_link_flapping_diagnoses_a_handle_conflict_and_quiets_down() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));

    // The first two instant drops keep the normal notices…
    for _ in 0..2 {
        harness.core.handle_link_event("s1", client::LinkEvent::Up);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        harness.core.handle_link_event("s1", client::LinkEvent::Down);
    }
    let texts = drain_notice_texts(&mut harness.to_mcp_rx);
    assert!(texts.iter().any(|t| t.contains("Disconnected")), "early drops keep the normal notice: {texts:?}");
    assert!(!texts.iter().any(|t| t.contains("--as")), "no diagnosis before the threshold: {texts:?}");

    // …the third diagnoses the likely handle conflict, once, with the remedy…
    harness.core.handle_link_event("s1", client::LinkEvent::Up);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    harness.core.handle_link_event("s1", client::LinkEvent::Down);
    let texts = drain_notice_texts(&mut harness.to_mcp_rx);
    assert!(texts.iter().any(|t| t.contains("--as")), "the third instant drop must diagnose the handle conflict: {texts:?}");

    // …further flapping is quiet…
    for _ in 0..3 {
        harness.core.handle_link_event("s1", client::LinkEvent::Up);
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        harness.core.handle_link_event("s1", client::LinkEvent::Down);
    }
    assert!(drain_notice_texts(&mut harness.to_mcp_rx).is_empty(), "flapping past the threshold must not stream notices");

    // …until a link survives the stability window: the breaker resets, notices resume.
    harness.core.handle_link_event("s1", client::LinkEvent::Up);
    tokio::time::advance(std::time::Duration::from_secs(31)).await;
    harness.core.handle_link_event("s1", client::LinkEvent::Down);
    let texts = drain_notice_texts(&mut harness.to_mcp_rx);
    assert!(texts.iter().any(|t| t.contains("Disconnected")), "a stable link resets the breaker: {texts:?}");
}

// -----------------------------------------------------------------------------
// PRD-0013 T-003 — the catch_up tool (agent-driven read-since).
// -----------------------------------------------------------------------------

#[test]
fn bridge_catch_up_requests_history_and_renders_the_page() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));

    // An explicit duration computes the watermark from the wall clock.
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(7),
        name: "catch_up".to_owned(),
        args: json!({ "channel": "ops", "since": "2h" }),
    });
    let expected = chrono::Utc::now().timestamp_millis() - 2 * 3600 * 1000;
    match harness.to_server_rx.try_recv().unwrap() {
        ProtocolMessage::ReadSince { channel, since_ms } => {
            assert_eq!(channel, "ops");
            assert!((since_ms - expected).abs() < 5_000, "since must be ~now-2h, got {since_ms} vs {expected}");
        }
        other => panic!("expected a ReadSince frame, got {other:?}"),
    }

    // The History response resolves the deferred call with a rendered, clearly-untrusted page.
    harness.core.handle_link_event(
        "s1",
        client::LinkEvent::Frame(ProtocolMessage::History {
            channel: "ops".to_owned(),
            messages: vec![
                crate::protocol::HistoryMessage {
                    from: SessionPath::new("david", "desktop", "main"),
                    ts_ms: 1_751_500_000_000,
                    payload: Payload::Plain("one".to_owned()),
                },
                crate::protocol::HistoryMessage {
                    from: SessionPath::new("david", "desktop", "main"),
                    ts_ms: 1_751_500_060_000,
                    payload: Payload::Plain("two".to_owned()),
                },
            ],
        }),
    );
    let reply = loop {
        let msg = harness.to_mcp_rx.try_recv().expect("expected the catch_up tool result");
        if msg.get("id") == Some(&json!(7)) {
            break msg;
        }
    };
    let text = reply.pointer("/result/content/0/text").and_then(Value::as_str).unwrap();
    assert!(text.contains("one") && text.contains("two"), "the page must render every message: {text}");
    assert!(text.contains("david/desktop/main"), "the page must attribute senders: {text}");
    assert!(text.contains("untrusted"), "the page must be framed as untrusted quoted content: {text}");
}

#[test]
fn bridge_catch_up_defaults_to_the_live_watermark() {
    let mut harness = harness(make_config(PermissionLevel::Notify, vec![]));

    // A live message sets the channel watermark…
    let before = chrono::Utc::now().timestamp_millis();
    harness.core.handle_link_event("s1", client::LinkEvent::Frame(channel_msg("ops", "seen live")));

    // …so a no-args catch_up asks from (watermark - slack), not from zero.
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(8),
        name: "catch_up".to_owned(),
        args: json!({ "channel": "ops" }),
    });
    match harness.to_server_rx.try_recv().unwrap() {
        ProtocolMessage::ReadSince { since_ms, .. } => {
            assert!(since_ms > 0, "a seen channel must not re-read everything");
            assert!(since_ms <= chrono::Utc::now().timestamp_millis(), "the watermark cannot be in the future");
            assert!(since_ms >= before - 61_000, "the slack window must stay bounded: {since_ms}");
        }
        other => panic!("expected a ReadSince frame, got {other:?}"),
    }

    // A never-seen channel reads everything retained.
    harness.core.handle_mcp(FromMcp::CallTool {
        id: json!(9),
        name: "catch_up".to_owned(),
        args: json!({ "channel": "fresh" }),
    });
    match harness.to_server_rx.try_recv().unwrap() {
        ProtocolMessage::ReadSince { since_ms, .. } => assert_eq!(since_ms, 0, "no watermark -> read the full retained window"),
        other => panic!("expected a ReadSince frame, got {other:?}"),
    }
}
