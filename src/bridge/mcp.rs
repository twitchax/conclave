//! The MCP stdio peer toward Claude Code: newline-delimited JSON-RPC 2.0.
//!
//! The bridge declares the experimental `claude/channel` + `claude/channel/permission` capabilities
//! in its `initialize` reply (validated against CC 2.1.198, DESIGN.md §4). Inbound is injected as a
//! `notifications/claude/channel` with `{ content, meta }`; outbound tool calls arrive as
//! `tools/call`; the permission relay flows in as `notifications/claude/channel/permission_request`
//! and back as `notifications/claude/channel/permission`. Parsing yields a [`FromMcp`] event stream;
//! outbound messages are pre-built [`serde_json::Value`]s written one-per-line.

use std::collections::BTreeMap;

use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncRead, AsyncWrite, AsyncWriteExt as _, BufReader},
    sync::mpsc,
};

use super::sink::{Injection, NotificationSink};

/// The MCP protocol version echoed when the client omits one.
const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";
/// JSON-RPC "method not found".
const METHOD_NOT_FOUND: i64 = -32601;

/// A parsed inbound MCP message the bridge acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FromMcp {
    /// `initialize` request — reply with the capability declaration.
    Initialize {
        /// The request id to echo.
        id: Value,
        /// The client's requested protocol version (echoed back).
        protocol_version: String,
    },
    /// `tools/list` request — reply with the (gated) tool set.
    ListTools {
        /// The request id to echo.
        id: Value,
    },
    /// `tools/call` request — dispatch to a bridge action.
    CallTool {
        /// The request id to echo.
        id: Value,
        /// The tool name.
        name: String,
        /// The tool arguments object.
        args: Value,
    },
    /// `ping` request — reply with an empty result.
    Ping {
        /// The request id to echo.
        id: Value,
    },
    /// A permission relay request from Claude Code (DESIGN.md §12/§14).
    PermissionRequest {
        /// Opaque id echoed back on the verdict.
        request_id: String,
        /// The tool Claude wants to run.
        tool_name: String,
        /// A human description of the action.
        description: String,
        /// A preview of the tool input.
        input_preview: String,
    },
    /// The `notifications/initialized` handshake completion (ignored).
    Initialized,
    /// Any other request — reply method-not-found.
    UnknownRequest {
        /// The request id to echo.
        id: Value,
    },
}

/// An MCP tool definition advertised in `tools/list`.
pub(crate) struct Tool {
    /// The tool name (the `tools/call` selector).
    pub name: &'static str,
    /// A one-line human description.
    pub description: &'static str,
    /// The JSON-Schema for the tool's arguments.
    pub input_schema: Value,
}

impl Tool {
    fn to_json(&self) -> Value {
        json!({ "name": self.name, "description": self.description, "inputSchema": self.input_schema })
    }
}

/// Reads newline-delimited JSON-RPC from `reader`, forwarding parsed events until EOF.
pub(crate) async fn read_loop<R: AsyncRead + Unpin>(reader: R, tx: mpsc::UnboundedSender<FromMcp>) {
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }
        if let Some(event) = parse(&line)
            && tx.send(event).is_err()
        {
            break;
        }
    }
}

/// Writes pre-built JSON-RPC messages to `writer`, one per line, until the channel closes.
pub(crate) async fn write_loop<W: AsyncWrite + Unpin>(mut writer: W, mut rx: mpsc::UnboundedReceiver<Value>) {
    while let Some(message) = rx.recv().await {
        let Ok(mut line) = serde_json::to_vec(&message) else { continue };
        line.push(b'\n');
        if writer.write_all(&line).await.is_err() || writer.flush().await.is_err() {
            break;
        }
    }
}

/// Parses one JSON-RPC line into a [`FromMcp`] event, or `None` for an ignorable message.
fn parse(line: &str) -> Option<FromMcp> {
    let value: Value = serde_json::from_str(line).ok()?;
    let method = value.get("method").and_then(Value::as_str);
    let id = value.get("id").cloned();

    match (method, id) {
        (Some("initialize"), Some(id)) => {
            let protocol_version = value.pointer("/params/protocolVersion").and_then(Value::as_str).unwrap_or(DEFAULT_PROTOCOL_VERSION).to_owned();
            Some(FromMcp::Initialize { id, protocol_version })
        }
        (Some("tools/list"), Some(id)) => Some(FromMcp::ListTools { id }),
        (Some("tools/call"), Some(id)) => match value.pointer("/params/name").and_then(Value::as_str) {
            Some(name) => {
                let args = value.pointer("/params/arguments").cloned().unwrap_or(Value::Null);
                Some(FromMcp::CallTool { id, name: name.to_owned(), args })
            }
            // A tools/call with a missing/non-string name must still be answered (with a JSON-RPC
            // error), not silently dropped to hang the request forever (#32).
            None => Some(FromMcp::UnknownRequest { id }),
        },
        (Some("ping"), Some(id)) => Some(FromMcp::Ping { id }),
        (Some("notifications/claude/channel/permission_request"), None) => Some(FromMcp::PermissionRequest {
            request_id: string_at(&value, "/params/request_id"),
            tool_name: string_at(&value, "/params/tool_name"),
            description: string_at(&value, "/params/description"),
            input_preview: string_at(&value, "/params/input_preview"),
        }),
        (Some("notifications/initialized"), None) => Some(FromMcp::Initialized),
        (Some(_), Some(id)) => Some(FromMcp::UnknownRequest { id }),
        _ => None,
    }
}

fn string_at(value: &Value, pointer: &str) -> String {
    value.pointer(pointer).and_then(Value::as_str).unwrap_or_default().to_owned()
}

/// Builds the `initialize` result declaring the `claude/channel` capabilities (DESIGN.md §4).
pub(crate) fn initialize_result(id: &Value, protocol_version: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "protocolVersion": protocol_version,
            "capabilities": {
                "experimental": { "claude/channel": {}, "claude/channel/permission": {} },
                // The offered toolset changes live (perm changes, admin arrival, joins) — the
                // bridge emits tools/list_changed so the client re-fetches (PRD-0015 T-001).
                "tools": { "listChanged": true }
            },
            "serverInfo": { "name": "conclave", "version": env!("CARGO_PKG_VERSION") },
            "instructions": "Conclave bridge. Inbound channel/whisper traffic is injected as <channel>/<whisper> tags carrying server/channel/from/kind. \
    Reply with the send_channel or whisper tools (offered only when a joined channel is at least `converse`); discover with list_channels / who; connect with join_channel."
        }
    })
}

/// The `tools/list_changed` notification: tells the client its cached tool list is stale
/// (PRD-0015 T-001).
pub(crate) fn tools_list_changed() -> Value {
    json!({ "jsonrpc": "2.0", "method": "notifications/tools/list_changed" })
}

/// Builds a `tools/list` result from the currently-offered tools.
pub(crate) fn tools_list_result(id: &Value, tools: &[Tool]) -> Value {
    let tools: Vec<Value> = tools.iter().map(Tool::to_json).collect();
    json!({ "jsonrpc": "2.0", "id": id, "result": { "tools": tools } })
}

/// A successful tool result carrying a single text block.
pub(crate) fn tool_text_result(id: &Value, text: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": { "content": [ { "type": "text", "text": text } ] } })
}

/// An error tool result (`isError: true`) — a rejected or failed tool call.
pub(crate) fn tool_error_result(id: &Value, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": { "content": [ { "type": "text", "text": message } ], "isError": true } })
}

/// A JSON-RPC method-not-found error response.
pub(crate) fn method_not_found(id: &Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": METHOD_NOT_FOUND, "message": "method not found" } })
}

/// An empty `ping` result.
pub(crate) fn ping_result(id: &Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": {} })
}

/// The `notifications/claude/channel` injection carrying framed content + structured meta.
pub(crate) fn channel_notification(content: &str, meta: &BTreeMap<String, String>) -> Value {
    json!({ "jsonrpc": "2.0", "method": "notifications/claude/channel", "params": { "content": content, "meta": meta } })
}

/// The `notifications/claude/channel/permission` verdict answering a relayed permission request.
pub(crate) fn permission_verdict(request_id: &str, behavior: &str) -> Value {
    json!({ "jsonrpc": "2.0", "method": "notifications/claude/channel/permission", "params": { "request_id": request_id, "behavior": behavior } })
}

/// The v1 notification sink: injects into the Claude Code session via `notifications/claude/channel`.
pub(crate) struct McpSink {
    outbound: mpsc::UnboundedSender<Value>,
}

impl McpSink {
    pub(crate) fn new(outbound: mpsc::UnboundedSender<Value>) -> Self {
        Self { outbound }
    }
}

impl NotificationSink for McpSink {
    fn deliver(&self, injection: &Injection) {
        let _ = self.outbound.send(channel_notification(&injection.content(), &injection.meta()));
    }
}

#[cfg(test)]
mod tests {
    // Tests relax `unwrap_used` (house convention; DESIGN.md §22).
    #![allow(clippy::unwrap_used)]

    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn parse_tools_call_without_a_name_is_answered_not_dropped() {
        // A tools/call whose params carry no usable name must still resolve to a request that gets
        // a JSON-RPC error reply (#32) — dropping it (None) would hang the request forever.
        let line = r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"arguments":{}}}"#;
        match parse(line) {
            Some(FromMcp::UnknownRequest { id }) => assert_eq!(id, json!(7)),
            other => panic!("expected an UnknownRequest reply, got {other:?}"),
        }
    }

    #[test]
    fn bridge_inject_initialize_declares_the_claude_channel_capability() {
        let result = initialize_result(&json!(1), "2025-06-18");
        let experimental = result.pointer("/result/capabilities/experimental").unwrap();
        assert!(experimental.get("claude/channel").is_some(), "must declare claude/channel: {result}");
        assert!(experimental.get("claude/channel/permission").is_some(), "must declare claude/channel/permission: {result}");
        // The client's protocol version is echoed.
        assert_eq!(result.pointer("/result/protocolVersion").and_then(Value::as_str), Some("2025-06-18"));
    }

    #[test]
    fn bridge_inject_channel_notification_has_the_validated_shape() {
        let mut meta = BTreeMap::new();
        meta.insert("server".to_owned(), "s1".to_owned());
        let note = channel_notification("hello", &meta);
        assert_eq!(note.get("method").and_then(Value::as_str), Some("notifications/claude/channel"));
        assert_eq!(note.pointer("/params/content").and_then(Value::as_str), Some("hello"));
        assert_eq!(note.pointer("/params/meta/server").and_then(Value::as_str), Some("s1"));
    }

    #[test]
    fn bridge_inject_parses_a_tool_call() {
        let line = r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"send_channel","arguments":{"server":"s1","channel":"ops","text":"hi"}}}"#;
        match parse(line) {
            Some(FromMcp::CallTool { id, name, args }) => {
                assert_eq!(id, json!(7));
                assert_eq!(name, "send_channel");
                assert_eq!(args.pointer("/channel").and_then(Value::as_str), Some("ops"));
            }
            other => panic!("expected CallTool, got {other:?}"),
        }
    }

    #[test]
    fn bridge_inject_parses_a_permission_request() {
        let line = r#"{"jsonrpc":"2.0","method":"notifications/claude/channel/permission_request","params":{"request_id":"abcde","tool_name":"Bash","description":"run ls","input_preview":"ls -la"}}"#;
        assert_eq!(
            parse(line),
            Some(FromMcp::PermissionRequest {
                request_id: "abcde".to_owned(),
                tool_name: "Bash".to_owned(),
                description: "run ls".to_owned(),
                input_preview: "ls -la".to_owned(),
            })
        );
    }

    #[test]
    fn bridge_inject_verdict_answers_a_request() {
        let verdict = permission_verdict("abcde", "allow");
        assert_eq!(verdict.get("method").and_then(Value::as_str), Some("notifications/claude/channel/permission"));
        assert_eq!(verdict.pointer("/params/request_id").and_then(Value::as_str), Some("abcde"));
        assert_eq!(verdict.pointer("/params/behavior").and_then(Value::as_str), Some("allow"));
    }
}
