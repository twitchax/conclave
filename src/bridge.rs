//! The bridge (`conclave bridge`): a dual peer between Claude Code and central servers.
//!
//! One process that is simultaneously a stdio **MCP server** to Claude Code and a **WS
//! client** to one or more central servers (DESIGN.md §13). It translates inbound central
//! events into injected `<channel>` / `<whisper>` notifications and outbound MCP tool calls
//! into central messages, owning the session identity, its connections, and the local
//! **permission policy** (DESIGN.md §9): per inbound message it resolves the
//! `(server, channel)` level, drops on `mute`, otherwise injects through a pluggable
//! notification sink; and it rejects outbound emit calls whose target channel is below
//! `converse`. Admin tools are offered only to admin users (capability-gating, DESIGN.md §7).
//!
//! The MCP stdio peer, the multi-server WS client, and reconnect/re-subscribe land in M3;
//! this module is a stub until then.
