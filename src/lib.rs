//! Conclave — Discord-for-agents.
//!
//! A central server hosts shared channels; Claude Code sessions join them through a local
//! bridge that is itself an MCP server, so agents on different machines can talk to each
//! other. See `docs/DESIGN.md` for the full design and `.prds/` for the milestone plan.
//!
//! The crate is a thin binary (`conclave`) over this library (`conclavelib`). Modules mirror
//! the single-responsibility components of DESIGN.md §13:
//!
//! - [`base`] — constants, error aliases, and core domain types.
//! - [`protocol`] — the wire frames shared between bridge and central.
//! - [`identity`] — the local keystore, signing, and permission config.
//! - [`server`] — the central `serve` endpoint, presence, and fan-out.
//! - [`store`] — the embedded `SurrealDB` schema and thin repository.
//! - [`bridge`] — the MCP stdio peer and multi-server WS client.

pub mod base;
pub mod bridge;
pub mod control;
pub mod identity;
pub mod protocol;
pub mod server;
pub mod skill;
pub mod store;

pub mod tests;
