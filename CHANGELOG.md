# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). The file is maintained with
[git-cliff](https://git-cliff.org/) (`cargo make changelog`).

## [Unreleased]

## [0.1.0] - 2026-07-02

### Added

- **M0 — project scaffolding & hygiene.** Single-package `conclavelib` + `conclave` skeleton with
  the DESIGN §13 module SOC; the Cargo `[lints]` table, `rustfmt.toml`, and release/dev-release/
  profiling profiles; a `cargo-make` task graph with the canonical `ci = [fmt-check, clippy, test]`
  gate; a three-tier (unit / integration / e2e-spawns-binary) test harness on nextest + coverage;
  CI (lint / test / codecov / platform builds) and the Copilot setup workflow; and the docs and
  release scaffolding (README, CHANGELOG + git-cliff, DEVELOPMENT, CLAUDE.md, MIT LICENSE).
- **M1 — wire protocol, identity/keystore, embedded store.** Versioned `ProtocolMessage` frames
  (`bincode`, length-delimited) with the reserved E2E ciphertext envelope; the machine Ed25519
  identity via `ring` (`secrecy`-wrapped seed) with the `~/.config/conclave` keystore + `config.toml`
  permission policy; and the embedded SurrealDB schema behind a thin per-table repository.
- **M2 — central server (`serve`).** axum WebSocket endpoint; register + challenge-response auth;
  channels with visibility tiers, user-level ACLs, and invite tokens; in-memory presence with a
  heartbeat reaper; channel fan-out + single-session whispers; and role-based admin authorization
  with revocation force-drop.
- **M3 — bridge.** A dual peer: a hand-rolled JSON-RPC 2.0 MCP stdio server toward Claude Code
  (the experimental `claude/channel` capability) and a reconnecting multi-server WS client; inbound
  injection through a pluggable notification sink with the local permission policy (mute drops,
  emit gated at `converse` with call-time per-channel rejection).
- **M4 — control/admin CLI, gated admin tools, packaged skill.** The full `conclave` verb surface
  over a one-shot control client; admin MCP tools gated by a post-auth server role; and the
  CLI-owned `conclave skill` (a comprehensive guide + an auto-generated command reference) with
  `conclave skill install`.
- **M5 — hardening.** Invite single-use/expiry/revoke and visibility-tier semantics proven
  end-to-end; a live `set_perm` MCP tool (per-`(server, channel)` override, no reconnect); the
  multi-home targeting UX (one session, many servers); and the README protocol diagrams.
