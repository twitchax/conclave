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
//! Split by responsibility: `policy` resolves the local autonomy level and gates emit;
//! `sink` frames a delivered message and pushes it to the session; `mcp` is the JSON-RPC
//! stdio peer toward Claude Code; `client` holds the outbound WS connections to central with
//! reconnect + re-subscribe. `BridgeCore` is the transport-free dispatcher those feed.

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
    base::{PermissionLevel, Res, SessionPath, Visibility, Void},
    identity::{Config, Identity, PermissionOverride, Scope, ServerRegistration},
    protocol::{AdminOp, Payload, ProtocolError, ProtocolMessage},
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
        tokio::spawn(client::run_link(registration.url.clone(), connect, inbound_tx.clone(), out_rx, Arc::clone(&shutdown)));
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

/// One entry in a server's in-order response queue. Every server-bound request that expects a
/// response enqueues one, so the FIFO stays correctly correlated (PRD-0008 T-001).
enum Pending {
    /// A deferred MCP tool call awaiting the server's response. `ok` overrides the success text
    /// (used by send/whisper, whose `Ack` carries no useful detail); `None` uses the frame's own
    /// rendered content (join / list / who / invite / admin).
    Tool { id: Value, ok: Option<String> },
    /// An internal re-subscribe `Join` issued on reconnect — its `Joined` ack is consumed silently.
    Resubscribe,
}

/// The transport-free bridge dispatcher: MCP events and inbound server frames in, MCP responses /
/// injections / outbound frames out. Everything I/O lives in [`run`]; this is unit-testable.
struct BridgeCore {
    config: Config,
    session: String,
    to_mcp: mpsc::UnboundedSender<Value>,
    sink: Box<dyn NotificationSink>,
    servers: HashMap<String, ServerHandle>,
    /// Per-server in-order queue of responses awaited from that server (tool calls + re-subscribes).
    pending: HashMap<String, VecDeque<Pending>>,
    /// Servers the session has been told are disconnected — so link state surfaces once per drop and
    /// a reconnect is announced (PRD-0008 T-003, #21).
    link_down_notified: HashSet<String>,
    /// Servers on which the authenticated user is an admin (from `ServerInfo`) — gates admin tools.
    admin_servers: HashSet<String>,
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
            link_down_notified: HashSet::new(),
            admin_servers: HashSet::new(),
        }
    }

    fn register_server(&mut self, registration: ServerRegistration, to_server: mpsc::UnboundedSender<ProtocolMessage>, joined: Arc<Mutex<HashSet<String>>>) {
        self.servers.insert(registration.url.clone(), ServerHandle { registration, to_server, joined });
    }

    /// The dispatcher loop: MCP events, inbound server frames, and Ctrl-C / stdin-EOF shutdown.
    async fn run(mut self, mut from_mcp: mpsc::UnboundedReceiver<FromMcp>, mut inbound: mpsc::UnboundedReceiver<(String, client::LinkEvent)>, shutdown: Arc<Notify>) -> Void {
        loop {
            tokio::select! {
                () = shutdown.notified() => break,
                _ = tokio::signal::ctrl_c() => break,
                event = from_mcp.recv() => match event {
                    Some(event) => self.handle_mcp(event),
                    None => break,
                },
                event = inbound.recv() => match event {
                    Some((server, event)) => self.handle_link_event(&server, event),
                    None => break,
                },
            }
        }
        shutdown.notify_waiters();
        Ok(())
    }

    /// Routes a link event: lifecycle (re-subscribe on `Up`, fail pending on `Down`) or a frame.
    fn handle_link_event(&mut self, server: &str, event: client::LinkEvent) {
        match event {
            client::LinkEvent::Up => self.link_up(server),
            client::LinkEvent::Down => self.link_down(server),
            client::LinkEvent::Frame(frame) => self.handle_inbound(server, frame),
        }
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
            "set_perm" => self.tool_set_perm(id, args),
            "create_channel" => self.tool_create_channel(id, args),
            "delete_channel" => self.tool_delete_channel(id, args),
            "set_visibility" => self.tool_set_visibility(id, args),
            "acl_add" => self.tool_acl(id, args, true),
            "acl_remove" => self.tool_acl(id, args, false),
            "invite_create" => self.tool_invite_create(id, args),
            "invite_revoke" => self.tool_invite_revoke(id, args),
            "kick" => self.tool_kick(id, args),
            "ban" => self.tool_ban(id, args),
            other => self.send_mcp(mcp::tool_error_result(id, &format!("unknown tool `{other}`"))),
        }
    }

    /// Pushes an admin op to the server, deferring its MCP result to the `Ack` / `InviteToken` /
    /// `Error` response. The server authorizes by role, so a non-admin call is refused server-side.
    /// Enqueues a deferred tool call awaiting `server`'s response. `ok` overrides the success text
    /// (`None` uses the response frame's own content).
    fn defer(&mut self, id: &Value, server: &str, ok: Option<String>) {
        self.pending.entry(server.to_owned()).or_default().push_back(Pending::Tool { id: id.clone(), ok });
    }

    fn defer_admin(&mut self, id: &Value, server: &str, op: AdminOp) {
        // Scope admin ops to servers that actually asserted admin for this user, so one server
        // claiming admin cannot confer admin power against another home (PRD-0008 T-007, #31).
        if !self.admin_servers.contains(server) {
            return self.send_mcp(mcp::tool_error_result(id, &format!("not an admin on `{server}`")));
        }
        self.defer(id, server, None);
        self.send_to_server(server, ProtocolMessage::Admin(op));
    }

    fn tool_create_channel(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let Some(name) = arg_str(args, "name") else {
            return self.send_mcp(mcp::tool_error_result(id, "`name` is required"));
        };
        let visibility = arg_str(args, "visibility").and_then(|v| v.parse().ok()).unwrap_or(Visibility::Public);
        self.defer_admin(id, &server, AdminOp::CreateChannel { name: name.to_owned(), visibility });
    }

    fn tool_delete_channel(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let Some(name) = arg_str(args, "name") else {
            return self.send_mcp(mcp::tool_error_result(id, "`name` is required"));
        };
        self.defer_admin(id, &server, AdminOp::DeleteChannel { name: name.to_owned() });
    }

    fn tool_set_visibility(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let (Some(name), Some(visibility)) = (arg_str(args, "name"), arg_str(args, "visibility").and_then(|v| v.parse::<Visibility>().ok())) else {
            return self.send_mcp(mcp::tool_error_result(id, "`name` and a valid `visibility` are required"));
        };
        self.defer_admin(id, &server, AdminOp::SetVisibility { name: name.to_owned(), visibility });
    }

    fn tool_acl(&mut self, id: &Value, args: &Value, add: bool) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let (Some(channel), Some(user)) = (arg_str(args, "channel"), arg_str(args, "user")) else {
            return self.send_mcp(mcp::tool_error_result(id, "`channel` and `user` are required"));
        };
        let op = if add {
            AdminOp::AclAdd {
                channel: channel.to_owned(),
                user: user.to_owned(),
            }
        } else {
            AdminOp::AclRemove {
                channel: channel.to_owned(),
                user: user.to_owned(),
            }
        };
        self.defer_admin(id, &server, op);
    }

    fn tool_invite_create(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let Some(channel) = arg_str(args, "channel") else {
            return self.send_mcp(mcp::tool_error_result(id, "`channel` is required"));
        };
        let uses = args.get("uses").and_then(Value::as_u64).and_then(|u| u32::try_from(u).ok());
        let expires_in_secs = args.get("expires_in_secs").and_then(Value::as_u64);
        self.defer_admin(
            id,
            &server,
            AdminOp::InviteCreate {
                channel: channel.to_owned(),
                uses,
                expires_in_secs,
            },
        );
    }

    fn tool_invite_revoke(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let Some(token) = arg_str(args, "token") else {
            return self.send_mcp(mcp::tool_error_result(id, "`token` is required"));
        };
        self.defer_admin(id, &server, AdminOp::InviteRevoke { token: token.to_owned() });
    }

    fn tool_kick(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let (Some(channel), Some(target)) = (arg_str(args, "channel"), arg_str(args, "target")) else {
            return self.send_mcp(mcp::tool_error_result(id, "`channel` and `target` are required"));
        };
        self.defer_admin(
            id,
            &server,
            AdminOp::Kick {
                channel: channel.to_owned(),
                target: target.to_owned(),
            },
        );
    }

    fn tool_ban(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let (Some(channel), Some(user)) = (arg_str(args, "channel"), arg_str(args, "user")) else {
            return self.send_mcp(mcp::tool_error_result(id, "`channel` and `user` are required"));
        };
        self.defer_admin(
            id,
            &server,
            AdminOp::Ban {
                channel: channel.to_owned(),
                user: user.to_owned(),
            },
        );
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
                Ok(level) => self.set_scope_override(&server, Some(channel.to_owned()), level),
                Err(err) => return self.send_mcp(mcp::tool_error_result(id, &err.to_string())),
            }
        }

        // Deferred: the result — and recording the channel as joined (done on the `Joined` ack, so a
        // rejected join isn't pre-recorded, PRD-0008 T-003 #20) — waits for the server to confirm.
        self.defer(id, &server, None);
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
        // Deferred until the server confirms (Ack) or rejects (Error), so the result reflects real
        // delivery and its Error correlates instead of stealing another call's slot (T-001).
        self.defer(id, &server, Some(format!("sent to {channel}")));
        self.send_to_server(
            &server,
            ProtocolMessage::ChannelMsg {
                channel: channel.to_owned(),
                from,
                payload: Payload::Plain(text.to_owned()),
            },
        );
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
        // Deferred until the server confirms/rejects — a whisper to an offline target now returns
        // that error to the caller instead of misrouting it to another pending call (T-001).
        self.defer(id, &server, Some("whisper sent".to_owned()));
        self.send_to_server(
            &server,
            ProtocolMessage::Whisper {
                from,
                target,
                payload: Payload::Plain(text.to_owned()),
            },
        );
    }

    fn tool_list(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        self.defer(id, &server, None);
        self.send_to_server(&server, ProtocolMessage::ListChannels);
    }

    fn tool_who(&mut self, id: &Value, args: &Value) {
        let server = match self.resolve_server(id, args) {
            Ok(server) => server,
            Err(error) => return self.send_mcp(error),
        };
        let channel = arg_str(args, "channel").map(str::to_owned);
        self.defer(id, &server, None);
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

    /// Changes the local autonomy level live (no reconnect); it applies to the next inbound message.
    fn tool_set_perm(&mut self, id: &Value, args: &Value) {
        let Some(level) = arg_str(args, "level").and_then(|level| level.parse::<PermissionLevel>().ok()) else {
            return self.send_mcp(mcp::tool_error_result(id, "`level` must be mute, notify, converse, or act"));
        };
        let whisper = args.get("whisper").and_then(Value::as_bool).unwrap_or(false);
        let channel = arg_str(args, "channel");

        // Channel / whisper scopes are per-server; with neither, set the machine-wide default.
        if channel.is_some() || whisper {
            let server = match self.resolve_server(id, args) {
                Ok(server) => server,
                Err(error) => return self.send_mcp(error),
            };
            let scope = if whisper { None } else { channel.map(str::to_owned) };
            self.set_scope_override(&server, scope, level);
        } else {
            self.config.default_permission = level;
        }
        self.send_mcp(mcp::tool_text_result(id, "permission updated"));
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
            ProtocolMessage::Joined { channel } => {
                // Record the subscription only now that the server has confirmed it (#20).
                if let Some(handle) = self.servers.get(server) {
                    handle.joined.lock().expect("joined mutex poisoned").insert(channel.clone());
                }
                self.resolve_pending(server, &format!("joined {channel}"));
            }
            ProtocolMessage::ChannelList { channels } => self.resolve_pending(server, &format_channels(&channels)),
            ProtocolMessage::Presence { channel, sessions } => self.resolve_pending(server, &format_presence(channel.as_deref(), &sessions)),
            ProtocolMessage::Ack { detail } => self.resolve_pending(server, detail.as_deref().unwrap_or("ok")),
            ProtocolMessage::InviteToken { token } => self.resolve_pending(server, &format!("invite token: {token}")),
            ProtocolMessage::Error(error) => self.resolve_error(server, &error),
            // The post-auth role signal gates the admin tools (DESIGN.md §7).
            ProtocolMessage::ServerInfo { admin } => {
                if admin {
                    self.admin_servers.insert(server.to_owned());
                } else {
                    self.admin_servers.remove(server);
                }
            }
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
        match self.pending.get_mut(server).and_then(VecDeque::pop_front) {
            Some(Pending::Tool { id, ok }) => self.send_mcp(mcp::tool_text_result(&id, ok.as_deref().unwrap_or(text))),
            // A re-subscribe `Joined` ack (or an orphan success) — consume it, don't answer a tool.
            Some(Pending::Resubscribe) | None => {}
        }
    }

    fn resolve_error(&mut self, server: &str, error: &ProtocolError) {
        match self.pending.get_mut(server).and_then(VecDeque::pop_front) {
            Some(Pending::Tool { id, .. }) => self.send_mcp(mcp::tool_error_result(&id, &error.to_string())),
            // A re-subscribe `Join` that failed (e.g. the channel was deleted) — consume silently.
            Some(Pending::Resubscribe) => {}
            // A stray error with nothing pending — surface it as a notice.
            None => self.notify(server, "error", &format!("Server `{server}` error: {error}")),
        }
    }

    /// Surfaces a system notice (link state, stray errors) into the session as a channel notification.
    fn notify(&self, server: &str, kind: &str, text: &str) {
        let mut meta = std::collections::BTreeMap::new();
        meta.insert("server".to_owned(), server.to_owned());
        meta.insert("kind".to_owned(), kind.to_owned());
        self.send_mcp(mcp::channel_notification(text, &meta));
    }

    /// On a fresh connection, announce a reconnect (if the session was told we dropped) and
    /// re-subscribe every joined channel, enqueuing a silent `Resubscribe` per `Join` so its
    /// `Joined` ack never resolves an unrelated tool call (PRD-0008 T-001/T-003).
    fn link_up(&mut self, server: &str) {
        if self.link_down_notified.remove(server) {
            self.notify(server, "link", &format!("Reconnected to `{server}`."));
        }
        let Some(handle) = self.servers.get(server) else { return };
        let channels: Vec<String> = handle.joined.lock().expect("joined mutex poisoned").iter().cloned().collect();
        for channel in channels {
            self.pending.entry(server.to_owned()).or_default().push_back(Pending::Resubscribe);
            self.send_to_server(server, ProtocolMessage::Join { channel, token: None });
        }
    }

    /// On a link drop, fail every pending tool call for `server` (so a deferred call never hangs),
    /// then surface the disconnect to the session once (PRD-0008 T-002/T-003). Re-subscribe entries
    /// are simply dropped.
    fn link_down(&mut self, server: &str) {
        if let Some(queue) = self.pending.remove(server) {
            for entry in queue {
                if let Pending::Tool { id, .. } = entry {
                    self.send_mcp(mcp::tool_error_result(&id, &format!("connection to `{server}` lost; retry")));
                }
            }
        }
        if self.link_down_notified.insert(server.to_owned()) {
            self.notify(server, "link", &format!("Disconnected from `{server}` — reconnecting."));
        }
    }

    // -----------------------------------------------------------------------
    // Tool set (emit tools gated on `>= converse`, DESIGN.md §9) + helpers.
    // -----------------------------------------------------------------------

    fn tools(&self) -> Vec<Tool> {
        let mut tools = vec![join_channel_tool(), list_channels_tool(), who_tool(), submit_permission_tool(), set_perm_tool()];
        if self.any_emit_allowed() {
            tools.push(send_channel_tool());
            tools.push(whisper_tool());
        }
        // Admin tools are offered only when the user is an admin on some connected server (§7).
        if !self.admin_servers.is_empty() {
            tools.extend(admin_tools());
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

    /// Sets a local permission override for a `(server, scope)`, replacing any prior one. `channel`
    /// is `Some(name)` for a channel scope or `None` for the whisper scope.
    fn set_scope_override(&mut self, server: &str, channel: Option<String>, level: PermissionLevel) {
        self.config.overrides.retain(|o| !(o.server == server && o.channel == channel));
        self.config.overrides.push(PermissionOverride {
            server: server.to_owned(),
            channel,
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

fn set_perm_tool() -> Tool {
    Tool {
        name: "set_perm",
        description: "Set your autonomy level live (mute/notify/converse/act) for a channel, the whisper scope, or the machine default. Takes effect on the next inbound message — no reconnect.",
        input_schema: json!({
            "type": "object",
            "properties": {
                "server": { "type": "string", "description": "Server URL (optional if only one is connected)." },
                "channel": { "type": "string", "description": "Channel to scope to (omit with `whisper` for the whisper scope, or omit both for the machine default)." },
                "whisper": { "type": "boolean", "description": "Apply to the whisper scope instead of a channel." },
                "level": { "type": "string", "enum": ["mute", "notify", "converse", "act"] }
            },
            "required": ["level"]
        }),
    }
}

/// Admin / moderation tools — offered only to admin users, authorized again by role server-side (§7).
fn admin_tools() -> Vec<Tool> {
    let server = json!({ "type": "string", "description": "Server URL (optional if only one is connected)." });
    vec![
        Tool {
            name: "create_channel",
            description: "Admin: create a channel (visibility public/unlisted/private; default public).",
            input_schema: json!({
                "type": "object",
                "properties": { "server": server, "name": { "type": "string" }, "visibility": { "type": "string", "enum": ["public", "unlisted", "private"] } },
                "required": ["name"]
            }),
        },
        Tool {
            name: "delete_channel",
            description: "Admin: delete a channel.",
            input_schema: json!({ "type": "object", "properties": { "server": server, "name": { "type": "string" } }, "required": ["name"] }),
        },
        Tool {
            name: "set_visibility",
            description: "Admin: change a channel's visibility (public/unlisted/private).",
            input_schema: json!({
                "type": "object",
                "properties": { "server": server, "name": { "type": "string" }, "visibility": { "type": "string", "enum": ["public", "unlisted", "private"] } },
                "required": ["name", "visibility"]
            }),
        },
        Tool {
            name: "acl_add",
            description: "Admin: add a user to a channel's access-control list.",
            input_schema: json!({ "type": "object", "properties": { "server": server, "channel": { "type": "string" }, "user": { "type": "string" } }, "required": ["channel", "user"] }),
        },
        Tool {
            name: "acl_remove",
            description: "Admin: remove a user from a channel's access-control list.",
            input_schema: json!({ "type": "object", "properties": { "server": server, "channel": { "type": "string" }, "user": { "type": "string" } }, "required": ["channel", "user"] }),
        },
        Tool {
            name: "invite_create",
            description: "Admin: mint an invite token for a channel (optional uses / expires_in_secs).",
            input_schema: json!({
                "type": "object",
                "properties": { "server": server, "channel": { "type": "string" }, "uses": { "type": "integer" }, "expires_in_secs": { "type": "integer" } },
                "required": ["channel"]
            }),
        },
        Tool {
            name: "invite_revoke",
            description: "Admin: revoke an invite token.",
            input_schema: json!({ "type": "object", "properties": { "server": server, "token": { "type": "string" } }, "required": ["token"] }),
        },
        Tool {
            name: "kick",
            description: "Admin: remove a session path or user from a channel.",
            input_schema: json!({ "type": "object", "properties": { "server": server, "channel": { "type": "string" }, "target": { "type": "string" } }, "required": ["channel", "target"] }),
        },
        Tool {
            name: "ban",
            description: "Admin: ban a user from a channel (drops them and blocks rejoin).",
            input_schema: json!({ "type": "object", "properties": { "server": server, "channel": { "type": "string" }, "user": { "type": "string" } }, "required": ["channel", "user"] }),
        },
    ]
}

#[cfg(test)]
mod tests;
