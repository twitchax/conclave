//! Local identity and keystore: the per-machine keypair, signing, and on-disk state.
//!
//! Owns everything under `~/.config/conclave` (`Constant::CONFIG_DIR_NAME`): the machine's
//! own Ed25519 keypair (private key wrapped in `secrecy`), the per-server registrations
//! (username + machine name), the known-servers list, and the local permission config
//! (default + per-`(server, channel)` / whisper overrides, DESIGN.md §9).
//!
//! Auth is challenge-response — the machine signs a server-issued challenge and the server
//! resolves the pubkey to a `(user, machine)` (DESIGN.md §5). The typed `AuthError` boundary
//! and keypair generation / signing land in M1; this module is a stub until then.
