# Development

Contributor guide for conclave. The global Rust constitution (`~/CLAUDE.md`) applies on top of
everything here; project-specific conventions live in [`docs/DESIGN.md`](docs/DESIGN.md) §22.

## Toolchain

- **Pinned nightly** via [`rust-toolchain.toml`](rust-toolchain.toml) (`nightly-2025-12-22`),
  **edition 2024**. `rustup` installs it automatically on the first build inside the repo. Nothing
  in the stack requires nightly except the `#[coverage(off)]` attribute; dropping to stable later
  is trivial.
- Components `rustfmt` and `clippy` are pinned alongside the channel.

## Commands

Everything routes through [`cargo-make`](https://github.com/sagiegurari/cargo-make) — it is the
sole entry point for dev commands.

```bash
cargo make ci             # Canonical gate: fmt-check + clippy (-D warnings) + nextest
cargo make fmt            # Format the tree
cargo make fmt-check      # Verify formatting (no writes)
cargo make clippy         # Lint with -D warnings
cargo make test           # Run the suite via nextest
cargo make test-cargo     # Fallback: plain `cargo test`
cargo make codecov        # Emit coverage.lcov
cargo make codecov-html   # Emit an HTML coverage report
cargo make build          # Debug build
cargo make build-release  # Optimized build
cargo make run -- --help  # Run the binary
cargo make changelog      # Regenerate CHANGELOG.md via git-cliff
```

First-time setup installs the binstall-provided tools:

```bash
cargo make tools          # cargo-nextest + cargo-llvm-cov (via cargo binstall)
```

## Lints & formatting

Lints live in the Cargo `[lints]` table so they apply DRY across the lib and bin:
`deny(unused, clippy::unwrap_used, clippy::correctness, clippy::complexity, clippy::pedantic)`.
Tests relax `clippy::unwrap_used`. Suppressing `too_many_arguments`, `too_many_lines`, or
`needless_pass_by_value` is prohibited — extract a struct / helper / borrow instead. The only
sanctioned `allow`s are narrow, justified ones for macro-codegen false positives.

Formatting is `rustfmt` with [`rustfmt.toml`](rustfmt.toml) (`max_width = 200`, …). CI runs
`fmt --check`.

## Test layout (SOC)

Three tiers, mirroring `docs/DESIGN.md` §17 / §22:

- **Unit** — in-module `#[cfg(test)] mod tests`. Shared fixtures come from the always-compiled
  `conclavelib::tests` factory (duplex transports, path/key fixtures) so unit and out-of-crate
  suites build them from one place.
- **Integration** — `tests/*.rs`, one bounded subsystem per file, preferring in-memory
  `tokio::io::duplex()` over real sockets.
- **E2E** — `tests/e2e.rs` spawns the real binary via `env!("CARGO_BIN_EXE_conclave")` inside a
  `tempfile::TempDir`. E2E test names are prefixed `e2e_` so they can be selected with
  `cargo nextest run -E 'test(/e2e_/)'`.

Flakiness controls for socket/timing-sensitive tests live in
[`.config/nextest.toml`](.config/nextest.toml): `retries = 2` and a serialized `network-heavy`
test group. **Every behavioral change ships a test — no exceptions.**

## Bridge ↔ Claude Code (`claude/channel`)

The bridge is an MCP server that injects inbound traffic into Claude Code via the experimental
`claude/channel` capability (DESIGN.md §4). The wire shape is validated against the installed CC in
CI with a **mock MCP client** (`test(/bridge_inject/)`) and a two-bridge e2e (`test(/e2e_channel/)`).
The **live-CC** check is manual and kept out of CI because Claude Code gates the capability behind a
development-channel flag (a normally-registered MCP server has `claude/channel` stripped):

1. Stand up a server and register + enroll this machine (M4 adds the `register` verb; until then use
   the e2e's provisioning path as a reference).
2. Launch Claude Code with the bridge loaded as a **development channel** so the capability survives:

   ```bash
   claude --dangerously-load-development-channels \
     'server:conclave=conclave bridge --server wss://your.server --as my-session'
   ```

   > `--dangerously-load-development-channels` is for local channel development only. The alternative
   > is the `allowedChannelPlugins` managed-settings allowlist. Without one of these, CC strips
   > `claude/channel` and no injection occurs.
3. From another session/machine, send a channel message; confirm it surfaces in the CC session as a
   `<channel …>` tag, and that `send_channel` / `whisper` replies flow back.

If the capability shape ever drifts, the mock-client tests in `src/bridge/mcp.rs` are the canonical
record of the expected frames; update them alongside the validation.

## Milestones & PRDs

Work is planned as PRDs in [`.prds/`](.prds/) (M0 = PRD-0001 … M5 = PRD-0006). Reference the task
ID in commits (e.g. `PRD-0001 T-004: cargo-make task graph`) and keep the task `status` in the PRD
frontmatter current as you go. Do not mark a UAT `verified` unless a real test exists and passes.

## Release

Versioning is SemVer; the changelog is Keep-a-Changelog via git-cliff. `cargo make release-bump`
performs the version bump + tag (`cargo release --no-publish`); publishing to crates.io is a
separate, manual step.
