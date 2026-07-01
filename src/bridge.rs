//! The bridge (`conclave bridge`): a dual peer between Claude Code and central servers.
//!
//! One process that is simultaneously a stdio **MCP server** to Claude Code and a **WS
//! client** to one or more central servers (DESIGN.md §13). It translates inbound central
//! events into injected `<channel>` / `<whisper>` notifications and outbound MCP tool calls
//! into central messages, owning the session identity, its connections, and the local
//! **permission policy** (DESIGN.md §9): per inbound message it resolves the
//! `(server, channel)` level, drops on `mute`, otherwise injects through a pluggable
//! notification sink; and it rejects outbound emit calls whose target channel is below
//! `converse`.
//!
//! Split by responsibility: [`policy`] resolves the local autonomy level and gates emit;
//! [`sink`] frames a delivered message and pushes it to the session; [`mcp`] is the JSON-RPC
//! stdio peer toward Claude Code; [`client`] holds the outbound WS connections to central with
//! reconnect + re-subscribe. [`BridgeCore`] is the transport-free dispatcher those feed.

mod client;
mod mcp;
mod policy;
mod sink;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::{Value, json};
use tokio::sync::{Notify, mpsc};

use crate::{
    base::{PermissionLevel, Res, SessionPath, Void},
    identity::{Config, Identity, PermissionOverride, Scope, ServerRegistration},
    protocol::{Payload, ProtocolError, ProtocolMessage},
};

use mcp::{FromMcp, McpSink, Tool};
use policy::Delivery;
use sink::{Injection, NotificationSink};

/// How often the bridge sends a keepalive `Ping` to each server (well under the server's 60s idle
/// reaper, DESIGN.md §10).
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(20);

/// Everything the running bridge needs: this machine's key, the local config, the session handle,
/// and (optionally) a subset of configured servers to connect to.
pub struct BridgeSetup {
    /// This machine's identity (signs the challenge).
    pub identity: Identity,
    /// The local config: permission policy + known-server registrations (M1).
    pub config: Config,
    /// The live-session handle (`--as`, default = repo/dir name).
    pub session: String,
    /// A subset of `config.servers` URLs to connect to; empty means all of them.
    pub servers: Vec<String>,
}

/// Runs the bridge: the MCP stdio peer, one reconnecting WS link per server, and the dispatcher.
///
/// # Errors
///
/// Returns an error if no known server is configured to connect to.
pub async fn run(setup: BridgeSetup) -> Void {
    let registrations = resolve_registrations(&setup.config, &setup.servers)?;

    let (from_mcp_tx, from_mcp_rx) = mpsc::unbounded_channel();
    let (to_mcp_tx, to_mcp_rx) = mpsc::unbounded_channel();
    tokio::spawn(mcp::read_loop(tokio::io::stdin(), from_mcp_tx));
    tokio::spawn(mcp::write_loop(tokio::io::stdout(), to_mcp_rx));

    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
    let shutdown = Arc::new(Notify::new());
    let identity = Arc::new(setup.identity);

    let sink = Box::new(McpSink::new(to_mcp_tx.clone()));
    let mut core = BridgeCore::new(setup.config.clone(), setup.session.clone(), to_mcp_tx, sink);

    for registration in registrations {
        let joined = Arc::new(Mutex::new(HashSet::new()));
        let (out_tx, out_rx) = mpsc::unbounded_channel();
        core.register_server(registration.clone(), out_tx.clone(), Arc::clone(&joined));

        let identity = Arc::clone(&identity);
        let url = registration.url.clone();
        let session = setup.session.clone();
        let connect = move || {
            let identity = Arc::clone(&identity);
            let url = url.clone();
            let session = session.clone();
            async move { client::connect_ws(&url, &identity, &session).await }
        };
        tokio::spawn(client::run_link(registration.url.clone(), connect, joined, inbound_tx.clone(), out_rx, Arc::clone(&shutdown)));
        spawn_keepalive(out_tx, Arc::clone(&shutdown));
    }

    core.run(from_mcp_rx, inbound_rx, shutdown).await
}

/// Selects the server registrations to connect to: the requested subset, or all if none named.
fn resolve_registrations(config: &Config, requested: &[String]) -> Res<Vec<ServerRegistration>> {
    let selected: Vec<ServerRegistration> = if requested.is_empty() {
        config.servers.clone()
    } else {
        config.servers.iter().filter(|r| requested.iter().any(|u| u == &r.url)).cloned().collect()
    };
    anyhow::ensure!(!selected.is_empty(), "no known server to connect to (register one first, or pass --server)");
    Ok(selected)
}

/// Sends a periodic keepalive `Ping` so the server's heartbeat reaper keeps the session present.
fn spawn_keepalive(to_server: mpsc::UnboundedSender<ProtocolMessage>, shutdown: Arc<Notify>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(KEEPALIVE_INTERVAL);
        loop {
            tokio::select! {
                () = shutdown.notified() => break,
                _ = ticker.tick() => {
                    if to_server.send(ProtocolMessage::Ping).is_err() {
                        break;
                    }
                }
            }
        }
    });
}

/// A connected server: its registration (for the local path), outbound sink, and joined channels.
struct ServerHandle {
    registration: ServerRegistration,
    to_server: mpsc::UnboundedSender<ProtocolMessage>,
    joined: Arc<Mutex<HashSet<String>>>,
}

/// The transport-free bridge dispatcher: MCP events and inbound server frames in, MCP responses /
/// injections / outbound frames out. Everything I/O lives in [`run`]; this is unit-testable.
struct BridgeCore {
    config: Config,
    session: String,
    to_mcp: mpsc::UnboundedSender<Value>,
    sink: Box<dyn NotificationSink>,
    servers: HashMap<String, ServerHandle>,
    /// Per-server FIFO of MCP request ids awaiting a control response (join / list / who).
    pending: HashMap<String, VecDeque<Value>>,
}

impl BridgeCore {
    fn new(config: Config, session: String, to_mcp: mpsc::UnboundedSender<Value>, sink: Box<dyn NotificationSink>) -> Self {
        Self {
            config,
            session,
            to_mcp,
            sink,
            servers: HashMap::new(),
            pending: HashMap::new(),
        }
    }

    fn register_server(&mut self, registration: ServerRegistration, to_server: mpsc::UnboundedSender<ProtocolMessage>, joined: Arc<Mutex<HashSet<String>>>) {
        self.servers.insert(registration.url.clone(), ServerHandle { registration, to_server, joined });
    }

    /// The dispatcher loop: MCP events, inbound server frames, and Ctrl-C / stdin-EOF shutdown.
    async fn run(mut self, mut from_mcp: mpsc::UnboundedReceiver<FromMcp>, mut inbound: mpsc::UnboundedReceiver<(String, ProtocolMessage)>, shutdown: Arc<Notify>) -> Void {
        loop {
            tokio::select! {
                () = shutdown.notified() => break,
                _ = tokio::signal::ctrl_c() => break,
                event = from_mcp.recv() => match event {
                    Some(event) => self.handle_mcp(event),
                    None => break,
                },
                frame = inbound.recv() => match frame {
                    Some((server, frame)) => self.handle_inbound(&server, frame),
                    None => break,
                },
            }
        }
        shutdown.notify_waiters();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // MCP → bridge.
    // -----------------------------------------------------------------------

    fn handle_mcp(&mut self, event: FromMcp) {
        match event {
            FromMcp::Initialize { id, protocol_version } => self.send_mcp(mcp::initialize_result(&id, &protocol_version)),
            FromMcp::ListTools { id } => {
                let tools = self.tools();
                self.send_mcp(mcp::tools_list_result(&id, &tools));
            }
            FromMcp::Ping { id } => self.send_mcp(mcp::ping_result(&id)),
            FromMcp::CallTool { id, name, args } => self.dispatch_tool(&id, &name, &args),
            FromMcp::PermissionRequest { request_id, tool_name, description, .. } => self.relay_permission(&request_id, &tool_name, &description),
            FromMcp::Initialized => {}
            FromMcp::UnknownRequest { id } => self.send_mcp(mcp::method_not_found(&id)),
        }
    }

    /// Dispatches a tool call. Each tool sends its own MCP reply; the `join`/`list`/`who` tools
    /// **defer** (send nothing now) and are resolved when the server's response arrives.
    fn dispatch_tool(&mut self, id: &Value, name: &str, args: &Value) {
        match name {
            "join_channel" => self.tool_join(id, args),
            "send_channel" => self.tool_send(id, args),
            "whisper" => self.tool_whisper(id, args),
            "list_channels" => self.tool_list(id, args),
            "who" => self.tool_who(id, args),
            "submit_permission" => self.tool_submit_permission(id, args),
            other => self.send_mcp(mcp::tool_error_result(id, &format!("unknown tool `{other}`"))),
        }
    }

    fn tool_join(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let Some(channel) = arg_str(args, "channel") else {
            return self.send_mcp(mcp::tool_error_result(id, "`channel` is required"));
        };
        let token = arg_str(args, "token").map(str::to_owned);

        if let Some(perm) = arg_str(args, "perm") {
            match perm.parse::<PermissionLevel>() {
                Ok(level) => self.set_override(&server, channel, level),
                Err(err) => return self.send_mcp(mcp::tool_error_result(id, &err.to_string())),
            }
        }

        if let Some(handle) = self.servers.get(&server) {
            handle.joined.lock().expect("joined mutex poisoned").insert(channel.to_owned());
        }
        // Deferred: the result is sent when the server's `Joined` / `Error` arrives.
        self.pending.entry(server.clone()).or_default().push_back(id.clone());
        self.send_to_server(&server, ProtocolMessage::Join { channel: channel.to_owned(), token });
    }

    fn tool_send(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let (Some(channel), Some(text)) = (arg_str(args, "channel"), arg_str(args, "text")) else {
            return self.send_mcp(mcp::tool_error_result(id, "`channel` and `text` are required"));
        };

        // Call-time per-channel rejection (DESIGN.md §9): below `converse` cannot emit.
        if !policy::emit_allowed(&self.config, &server, &Scope::Channel(channel.to_owned())) {
            return self.send_mcp(mcp::tool_error_result(id, &format!("permission denied: `{channel}` on `{server}` is below `converse`")));
        }

        let from = self.our_path(&server);
        self.send_to_server(
            &server,
            ProtocolMessage::ChannelMsg {
                channel: channel.to_owned(),
                from,
                payload: Payload::Plain(text.to_owned()),
            },
        );
        self.send_mcp(mcp::tool_text_result(id, &format!("sent to {channel}")));
    }

    fn tool_whisper(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let (Some(target), Some(text)) = (arg_str(args, "target"), arg_str(args, "text")) else {
            return self.send_mcp(mcp::tool_error_result(id, "`target` and `text` are required"));
        };
        let Ok(target) = target.parse::<SessionPath>() else {
            return self.send_mcp(mcp::tool_error_result(id, "`target` must be a `user/machine/session` path"));
        };

        if !policy::emit_allowed(&self.config, &server, &Scope::Whisper) {
            return self.send_mcp(mcp::tool_error_result(id, &format!("permission denied: whispers on `{server}` are below `converse`")));
        }

        let from = self.our_path(&server);
        self.send_to_server(
            &server,
            ProtocolMessage::Whisper {
                from,
                target,
                payload: Payload::Plain(text.to_owned()),
            },
        );
        self.send_mcp(mcp::tool_text_result(id, "whisper sent"));
    }

    fn tool_list(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        self.pending.entry(server.clone()).or_default().push_back(id.clone());
        self.send_to_server(&server, ProtocolMessage::ListChannels);
    }

    fn tool_who(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let channel = arg_str(args, "channel").map(str::to_owned);
        self.pending.entry(server.clone()).or_default().push_back(id.clone());
        self.send_to_server(&server, ProtocolMessage::Who { channel });
    }

    fn tool_submit_permission(&mut self, id: &Value, args: &Value) {
        let Some(request_id) = arg_str(args, "request_id") else {
            return self.send_mcp(mcp::tool_error_result(id, "`request_id` is required"));
        };
        let behavior = if arg_str(args, "decision") == Some("allow") { "allow" } else { "deny" };
        self.send_mcp(mcp::permission_verdict(request_id, behavior));
        self.send_mcp(mcp::tool_text_result(id, &format!("permission verdict `{behavior}` sent")));
    }

    /// Relays a Claude Code permission request to the local session (DESIGN.md §12/§14). The verdict
    /// is returned by the `submit_permission` tool.
    fn relay_permission(&self, request_id: &str, tool_name: &str, description: &str) {
        let mut meta = std::collections::BTreeMap::new();
        meta.insert("kind".to_owned(), "permission_request".to_owned());
        meta.insert("request_id".to_owned(), request_id.to_owned());
        let content = format!(
            "Claude Code requests approval to run `{tool_name}`: {description}\nAnswer with the submit_permission tool: {{\"request_id\": \"{request_id}\", \"decision\": \"allow\"|\"deny\"}}."
        );
        self.send_mcp(mcp::channel_notification(&content, &meta));
    }

    // -----------------------------------------------------------------------
    // Server → bridge.
    // -----------------------------------------------------------------------

    fn handle_inbound(&mut self, server: &str, frame: ProtocolMessage) {
        match frame {
            ProtocolMessage::ChannelMsg { channel, from, payload } => self.inject(server, Some(channel), from, payload),
            ProtocolMessage::Whisper { from, payload, .. } => self.inject(server, None, from, payload),
            ProtocolMessage::Joined { channel } => self.resolve_pending(server, &format!("joined {channel}")),
            ProtocolMessage::ChannelList { channels } => self.resolve_pending(server, &format_channels(&channels)),
            ProtocolMessage::Presence { channel, sessions } => self.resolve_pending(server, &format_presence(channel.as_deref(), &sessions)),
            ProtocolMessage::Ack { detail } => self.resolve_pending(server, detail.as_deref().unwrap_or("ok")),
            ProtocolMessage::InviteToken { token } => self.resolve_pending(server, &format!("invite token: {token}")),
            ProtocolMessage::Error(error) => self.resolve_error(server, &error),
            // Keepalive acks and any handshake frames (consumed by the client) are ignored here.
            _ => {}
        }
    }

    fn inject(&self, server: &str, channel: Option<String>, from: SessionPath, payload: Payload) {
        let body = match payload {
            Payload::Plain(text) => text,
            Payload::Encrypted(_) => "<end-to-end-encrypted payload — not supported in v1>".to_owned(),
        };
        let scope = channel.as_ref().map_or(Scope::Whisper, |c| Scope::Channel(c.clone()));

        match policy::inbound_delivery(&self.config, server, &scope) {
            Delivery::Drop => {}
            Delivery::Inject(level) => self.sink.deliver(&Injection {
                server: server.to_owned(),
                channel,
                from,
                level,
                body,
            }),
        }
    }

    fn resolve_pending(&mut self, server: &str, text: &str) {
        if let Some(id) = self.pending.get_mut(server).and_then(VecDeque::pop_front) {
            self.send_mcp(mcp::tool_text_result(&id, text));
        }
    }

    fn resolve_error(&mut self, server: &str, error: &ProtocolError) {
        if let Some(id) = self.pending.get_mut(server).and_then(VecDeque::pop_front) {
            self.send_mcp(mcp::tool_error_result(&id, &error.to_string()));
        } else {
            // A stray error (e.g. a whisper to an offline target) — surface it as a notice.
            let mut meta = std::collections::BTreeMap::new();
            meta.insert("server".to_owned(), server.to_owned());
            meta.insert("kind".to_owned(), "error".to_owned());
            self.send_mcp(mcp::channel_notification(&format!("Server `{server}` error: {error}"), &meta));
        }
    }

    // -----------------------------------------------------------------------
    // Tool set (emit tools gated on `>= converse`, DESIGN.md §9) + helpers.
    // -----------------------------------------------------------------------

    fn tools(&self) -> Vec<Tool> {
        let mut tools = vec![join_channel_tool(), list_channels_tool(), who_tool(), submit_permission_tool()];
        if self.any_emit_allowed() {
            tools.push(send_channel_tool());
            tools.push(whisper_tool());
        }
        tools
    }

    fn any_emit_allowed(&self) -> bool {
        let joined: Vec<(String, String)> = self
            .servers
            .iter()
            .flat_map(|(server, handle)| {
                handle
                    .joined
                    .lock()
                    .expect("joined mutex poisoned")
                    .iter()
                    .map(|channel| (server.clone(), channel.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();
        policy::any_emit_allowed(&self.config, joined.iter().map(|(server, channel)| (server.as_str(), channel.as_str())))
    }

    /// Resolves the target server: the `server` argument, or the sole connection if unambiguous.
    fn resolve_server(&self, id: &Value, args: &Value) -> Result<String, Value> {
        if let Some(server) = arg_str(args, "server") {
            if self.servers.contains_key(server) {
                return Ok(server.to_owned());
            }
            return Err(mcp::tool_error_result(id, &format!("not connected to server `{server}`")));
        }
        match self.servers.keys().next() {
            Some(only) if self.servers.len() == 1 => Ok(only.clone()),
            _ => Err(mcp::tool_error_result(id, "multiple servers connected; pass `server`")),
        }
    }

    fn our_path(&self, server: &str) -> SessionPath {
        self.servers.get(server).map_or_else(
            || SessionPath::new("unknown", "unknown", self.session.clone()),
            |handle| SessionPath::new(handle.registration.username.clone(), handle.registration.machine.clone(), self.session.clone()),
        )
    }

    fn set_override(&mut self, server: &str, channel: &str, level: PermissionLevel) {
        self.config.overrides.retain(|o| !(o.server == server && o.channel.as_deref() == Some(channel)));
        self.config.overrides.push(PermissionOverride {
            server: server.to_owned(),
            channel: Some(channel.to_owned()),
            level,
        });
    }

    fn send_mcp(&self, message: Value) {
        let _ = self.to_mcp.send(message);
    }

    fn send_to_server(&self, server: &str, frame: ProtocolMessage) {
        if let Some(handle) = self.servers.get(server) {
            let _ = handle.to_server.send(frame);
        }
    }
}

fn arg_str<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key).and_then(Value::as_str)
}

fn format_channels(channels: &[crate::protocol::ChannelInfo]) -> String {
    if channels.is_empty() {
        return "no channels visible".to_owned();
    }
    channels
        .iter()
        .map(|c| format!("{} ({}{})", c.name, c.visibility.as_str(), if c.member { ", member" } else { "" }))
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_presence(channel: Option<&str>, sessions: &[SessionPath]) -> String {
    let scope = channel.map_or_else(|| "server-wide".to_owned(), |c| format!("#{c}"));
    if sessions.is_empty() {
        return format!("{scope}: nobody online");
    }
    let who = sessions.iter().map(SessionPath::to_string).collect::<Vec<_>>().join(", ");
    format!("{scope}: {who}")
}

// --- Tool definitions ---------------------------------------------------------

fn join_channel_tool() -> Tool {
    Tool {
        name: "join_channel",
        description: "Join a channel on a server and subscribe this session to it.",
        input_schema: json!({
            "type": "object",
            "properties": {
                "server": { "type": "string", "description": "Server URL (optional if only one is connected)." },
                "channel": { "type": "string", "description": "Channel name to join." },
                "token": { "type": "string", "description": "Invite token, if the channel requires one." },
                "perm": { "type": "string", "enum": ["mute", "notify", "converse", "act"], "description": "Autonomy level for this channel." }
            },
            "required": ["channel"]
        }),
    }
}

fn list_channels_tool() -> Tool {
    Tool {
        name: "list_channels",
        description: "List the channels visible to you on a server.",
        input_schema: json!({
            "type": "object",
            "properties": { "server": { "type": "string", "description": "Server URL (optional if only one is connected)." } }
        }),
    }
}

fn who_tool() -> Tool {
    Tool {
        name: "who",
        description: "List who is present on a server, optionally scoped to a channel.",
        input_schema: json!({
            "type": "object",
            "properties": {
                "server": { "type": "string", "description": "Server URL (optional if only one is connected)." },
                "channel": { "type": "string", "description": "Restrict presence to this channel." }
            }
        }),
    }
}

fn submit_permission_tool() -> Tool {
    Tool {
        name: "submit_permission",
        description: "Answer a relayed Claude Code permission request (allow or deny).",
        input_schema: json!({
            "type": "object",
            "properties": {
                "request_id": { "type": "string", "description": "The request_id from the permission prompt." },
                "decision": { "type": "string", "enum": ["allow", "deny"], "description": "The verdict." }
            },
            "required": ["request_id", "decision"]
        }),
    }
}

fn send_channel_tool() -> Tool {
    Tool {
        name: "send_channel",
        description: "Send a message to a channel (allowed only at converse/act).",
        input_schema: json!({
            "type": "object",
            "properties": {
                "server": { "type": "string", "description": "Server URL (optional if only one is connected)." },
                "channel": { "type": "string", "description": "Channel to send to." },
                "text": { "type": "string", "description": "The message text." }
            },
            "required": ["channel", "text"]
        }),
    }
}

fn whisper_tool() -> Tool {
    Tool {
        name: "whisper",
        description: "Send a direct message to exactly one session path (allowed only at converse/act).",
        input_schema: json!({
            "type": "object",
            "properties": {
                "server": { "type": "string", "description": "Server URL (optional if only one is connected)." },
                "target": { "type": "string", "description": "The recipient's full user/machine/session path." },
                "text": { "type": "string", "description": "The message text." }
            },
            "required": ["target", "text"]
        }),
    }
}

#[cfg(test)]
mod tests;
