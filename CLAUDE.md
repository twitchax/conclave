# Conclave — Project Instructions

Conclave is Discord-for-agents: a central server hosts shared channels, and a local **bridge**
(itself an MCP server to Claude Code) lets sessions on different machines talk to each other. The
authoritative design is [`docs/DESIGN.md`](docs/DESIGN.md); the milestone plan is in
[`.prds/`](.prds/).

The global Rust constitution and conventions in `~/CLAUDE.md` apply in full. This file only records
conclave-specifics; **where they are silent, defer to the global constitution.**

## Architecture

Single Cargo package: crate `conclave-cli` publishing a library (`conclavelib`) and a thin binary
(`conclave`). Modules mirror the single-responsibility components of DESIGN §13:

- `base` — constants (`Constant`), error aliases (`Err` / `Res` / `Void`), core domain types
  (`SessionPath`, `PermissionLevel`, `Visibility`).
- `protocol` — the wire frames shared between bridge and central, with a reserved protocol-version
  field and an E2E-ready encrypted-payload envelope.
- `identity` — the local keystore under `~/.config/conclave`, signing, per-server registrations,
  and the local permission config.
- `server` — the central `serve` endpoint: axum WSS, embedded SurrealDB store, presence, fan-out.
- `bridge` — the MCP stdio peer + multi-server WS client, and the local permission policy.

Wire-crossing boundary errors are typed with `thiserror` (`ProtocolError`, `AuthError`,
`AclError`); application glue uses `anyhow` via the `Res` / `Void` aliases and `?` + `.context()`.

## Commands

All dev commands route through `cargo-make`:

```bash
cargo make ci        # fmt-check + clippy (-D warnings) + nextest — the one gate
cargo make test      # nextest
cargo make codecov   # emit coverage.lcov
cargo make run -- …  # run the binary
```

## Conventions

- **Toolchain:** pinned nightly (`rust-toolchain.toml`), edition 2024. Keep the workflow
  `RUST_TOOLCHAIN` env in sync with the toolchain file.
- **Lints:** the strong set via the Cargo `[lints]` table (DRY across lib + bin). Do not add
  `allow`s except narrow, justified ones for macro-codegen. Do not suppress `too_many_*` /
  `needless_pass_by_value` — refactor instead.
- **Dependencies are added by the milestone that first needs them** — keep M0 baseline deps lean;
  feature crates (serde, ed25519, secrecy, surrealdb, axum, tungstenite, rustls) land in M1+.
- **Testing:** three-tier SOC (unit in-module, integration in `tests/`, e2e spawns the binary).
  Shared fixtures via the `conclavelib::tests` factory. Every behavioral change ships a test.
- **PRDs:** reference the task ID in commits and keep the PRD frontmatter `status` current. Never
  mark a UAT `verified` without a real, passing test.
