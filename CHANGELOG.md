# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). The file is maintained with
[git-cliff](https://git-cliff.org/) (`cargo make changelog`).

## [Unreleased]

### Added

- **Message history + catch-up (PRD-0013).** The server retains 7 days of channel traffic
  (hourly purge; cascades on delete/rename; whispers stay ephemeral by design) behind a new
  `ReadSince` wire op — subscription-gated, so refusals stay visibility-uniform. The bridge grows
  a `catch_up` tool ("catch up on #ops"): the agent computes the watermark (defaulting to the last
  message this session saw), and the page comes back attributed, timestamped, and framed as
  untrusted content. The payload envelope is stored verbatim, so future E2E ciphertext is retained
  without being server-readable.
- **`conclave tail --since 2h`** replays the retained backlog before streaming live.

### Fixed

- **`conclave tail` survives server restarts.** A deploy used to kill it with a raw rustls error;
  it now reconnects with backoff and resumes from its watermark (repeating a line rather than
  missing one), with status on stderr so stdout stays a clean message stream.

## [0.2.2] - 2026-07-03

Everything learned from the first live multi-session test: the supersede-storm class of bugs
(PRD-0012), a tool list that tracks reality (PRD-0015), and the CLI exits those fights needed.

### Added

- **`conclave server list` / `conclave server remove <url>`.** Local known-servers management —
  `remove` forgets a registration *and* its permission overrides, the CLI exit for a stranded
  double-registration that previously required hand-editing `config.toml`.

### Fixed

- **The tool list is now live (PRD-0015).** The bridge declares `tools.listChanged` and emits
  `notifications/tools/list_changed` whenever gating changes (a `set_perm` to `converse`, admin
  status arriving, joins) — previously Claude Code's cached tool list meant `send_channel` could
  *never* appear mid-session, no matter how permissions changed.
- **Handle conflicts self-diagnose (PRD-0015).** Two live sessions sharing one handle (e.g. two
  Claude Code sessions in the same directory using the default) supersede each other; the bridge
  now names the cause and remedy (`--as`) after three instant drops and quiets the link notices
  until the connection stabilizes. `conclave perm set` also states that live sessions use the
  `set_perm` tool.
- **Same-server supersede storm (PRD-0012).** A machine registered on one server under two URLs
  (e.g. fly.dev + custom domain) running a bare `conclave bridge` had its two links evict each
  other's session in a hot loop. Three-part fix: the server now stamps a persistent instance ID on
  the WS upgrade (`x-conclave-server-id`) and the bridge disables a duplicate URL pre-auth with a
  single notice; the reconnect backoff resets only after a link stays up 30s (an instantly-killed
  connect keeps backing off); and force-drop reasons self-describe ("session superseded…",
  "machine key revoked", "idle timeout…") instead of a generic "session terminated".
- One-shot CLI verbs now die quietly on SIGPIPE (e.g. `conclave completions bash | head`) instead
  of panicking with `BrokenPipe`; `serve`/`bridge` keep their graceful write-error shutdown paths.

## [0.2.1] - 2026-07-03

CLI ergonomics and operator visibility (PRD-0011), driven by findings from live deployment testing.

### Added

- **No write-only moderation state.** `conclave acl list` (channel ACL members), `conclave bans`
  (banned users), `conclave invite list` (outstanding tokens with uses/expiry), and a first-class
  `conclave unban` — which lifts a ban *without* granting ACL membership (previously the only unban
  path was the `acl add` side effect). All channel-admin gated; wire changes append-only.
- **Server-admin channel enumeration.** `channel list` now shows a server admin every channel
  (private/unlisted included) so an operator can audit their own server; non-members still cannot
  discover private channels.
- **`conclave status`.** The "who am I" view: registrations (server/user/machine), per-server
  reachability probes (authenticated round-trip with live-session count), and the resolved
  permission table; exits non-zero if any server is unreachable.
- **`conclave send` / `conclave tail`.** The CLI as a human client: post one message (server-acked)
  or stream a channel to the terminal until Ctrl-C — watch your agents talk without a Claude
  session.
- **`leave_channel` bridge tool.** A session can unsubscribe from a channel without disconnecting.
- **Shell completions** (`conclave completions bash|zsh|fish|elvish|powershell`).
- **Onboarding-grade skill.** The packaged skill now walks a fresh user zero-to-first-message:
  install → register → perm grants → `claude mcp add` → the channels research-preview flag →
  join/verify, plus the new human and audit verbs.

### Fixed

- `machine add --pubkey` help claimed PEM; the format is base64url from `conclave key`.

## [0.2.0] - 2026-07-03

The post-v0.1.0 adversarial review (42 agents, 28 confirmed findings) driven to zero, plus
everything needed to deploy: TLS clients, a container image, and Fly.io wiring.

### Added

- **TLS + deployment (PRD-0009).** The bridge and CLI dial `wss://` (bundled Mozilla roots — no
  system cert store needed in containers); a cargo-chef multi-stage `Dockerfile` (slim, non-root)
  and `fly.toml` with a mounted-volume store; a `/health` endpoint for platform checks; env-driven
  serve config (`CONCLAVE_BIND` / `CONCLAVE_DATA_DIR` / `CONCLAVE_ADMINS`) with an explicit
  `--ephemeral` guard so a mis-templated deploy can't silently run in-memory; and tag-triggered
  release automation (a `v*` tag builds the platform matrix and publishes a GitHub Release).
- **SIGTERM graceful drain.** The server now drains on SIGTERM (what `fly deploy` / `docker stop`
  send), not just Ctrl-C — deploys stop cleanly instead of waiting out the kill timeout.
- **Durable channel bans.** Bans persist in the store (write-through to an in-memory mirror) and
  survive a server restart; they cascade on channel delete and follow renames.
- **Link-state notices.** The bridge surfaces a disconnect notice once per drop and announces the
  reconnect, so a session knows when its server link is down.

### Changed

- **Confirmed sends (PRD-0008).** `send_channel` / `whisper` now defer until the server acks, so
  the tool result reflects real delivery — a whisper to an offline target reports the error to the
  caller instead of claiming success.
- **Reconnect supersede.** A fresh authenticated session for the same path takes over immediately
  after an ungraceful drop, instead of colliding with the stale one until the idle reaper (~75s).
- **Explicit `perm set` scope.** `--server` now requires `--channel <name>` or `--whisper`
  (previously it silently wrote the whisper scope).

### Fixed

- **Bridge response correlation (PRD-0008, HIGH).** Out-of-band `Error` frames no longer steal an
  unrelated deferred tool call's response slot, and a link drop fails all pending tool calls
  instead of hanging them forever; reconnect re-subscribes are consumed silently.
- **Server hardening (PRD-0007).** Registration is verify-first (a failed possession proof
  persists nothing); channel ACLs normalized into a membership table (no lost updates under
  SurrealKV's optimistic concurrency); per-session outbound queues are bounded with a
  slow-consumer disconnect; handshake timeout and frame-size caps against pre-auth DoS; admin
  usernames pin to keys (anti-squat); visibility-uniform errors so private channels don't leak
  existence (including the invite-revoke oracle and a ban/join race).
- **Injection framing escape (security).** Inbound message bodies are XML-escaped so a sender
  cannot close the `<channel>`/`<whisper>` frame and forge a trusted block in the agent session.
- **CLI robustness.** Control verbs time out against a dead-but-listening server; `config.toml`
  writes are atomic (temp + rename); a rejected `join --perm` no longer leaves a stale permission
  override; keyfiles are created `0600` from the start and transient seed copies are zeroized.

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
