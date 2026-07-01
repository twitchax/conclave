//! The central server (`conclave serve`): the axum WSS endpoint and control plane.
//!
//! A single self-contained binary that owns: the `axum` WSS endpoint and control RPCs; the
//! embedded `SurrealDB`-backed identity / channel / ACL store (bare SDK behind a thin
//! per-table repository, no ORM — DESIGN.md §15); in-memory presence with heartbeat reaping
//! and the fan-out router; and admin authorization against the config `users` allowlist plus
//! each channel's `created_by` (DESIGN.md §7).
//!
//! Durable state is config only — no message history. Presence, subscriptions, permission
//! levels, and the admin allowlist are deliberately *not* in the DB (DESIGN.md §15). The
//! typed `AclError` boundary and the server itself land in M2; this module is a stub until then.
