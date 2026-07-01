//! The local permission policy: how an inbound message drives the agent, and whether the bridge
//! may emit (DESIGN.md §9).
//!
//! Levels are resolved per `(server, scope)` from the local [`Config`] (default + overrides, M1);
//! this module turns a resolved [`PermissionLevel`] into the two things it controls — inbound
//! delivery (`mute` drops) and outbound emit capability (`converse`/`act` allow). Enforcement is by
//! capability: `mute` never injects, and a `send`/`whisper` below `converse` is rejected at call
//! time. The tool list is session-global, so the emit tools are *offered* iff **some** joined
//! channel is `>= converse`; the per-channel gate is the call-time check ([`emit_allowed`]).

use crate::{
    base::PermissionLevel,
    identity::{Config, Scope},
};

/// What to do with an inbound message after resolving its `(server, scope)` level (DESIGN.md §9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Delivery {
    /// `mute`: drop the message entirely; the recipient stays present but is not pinged.
    Drop,
    /// Deliver, injecting with the surrounding prompt for this level (`notify`/`converse`/`act`).
    Inject(PermissionLevel),
}

/// Resolves how an inbound message on `(server, scope)` is delivered locally: `mute` drops,
/// everything else injects at its level.
pub(crate) fn inbound_delivery(config: &Config, server: &str, scope: &Scope) -> Delivery {
    match config.resolve_permission(server, scope) {
        PermissionLevel::Mute => Delivery::Drop,
        level => Delivery::Inject(level),
    }
}

/// Whether the bridge may emit (`send`/`whisper`) on `(server, scope)` — `converse` and above (§9).
pub(crate) fn emit_allowed(config: &Config, server: &str, scope: &Scope) -> bool {
    config.resolve_permission(server, scope).may_emit()
}

/// Whether **any** joined `(server, channel)` resolves `>= converse`, so the session-global emit
/// tools are offered at all (per-channel enforcement is then the call-time [`emit_allowed`] check).
pub(crate) fn any_emit_allowed<'a>(config: &Config, joined: impl IntoIterator<Item = (&'a str, &'a str)>) -> bool {
    joined
        .into_iter()
        .any(|(server, channel)| config.resolve_permission(server, &Scope::Channel(channel.to_owned())).may_emit())
}

#[cfg(test)]
mod tests {
    // Tests relax `unwrap_used` (house convention; DESIGN.md §22).
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::identity::PermissionOverride;
    use pretty_assertions::assert_eq;

    fn config_with(default: PermissionLevel, overrides: Vec<PermissionOverride>) -> Config {
        Config {
            default_permission: default,
            servers: vec![],
            overrides,
        }
    }

    fn override_for(server: &str, channel: Option<&str>, level: PermissionLevel) -> PermissionOverride {
        PermissionOverride {
            server: server.to_owned(),
            channel: channel.map(str::to_owned),
            level,
        }
    }

    #[test]
    fn bridge_perm_mute_drops_inbound() {
        let config = config_with(PermissionLevel::Notify, vec![override_for("s1", Some("ops"), PermissionLevel::Mute)]);
        assert_eq!(inbound_delivery(&config, "s1", &Scope::Channel("ops".to_owned())), Delivery::Drop);
    }

    #[test]
    fn bridge_perm_notify_injects_read_only() {
        let config = config_with(PermissionLevel::Notify, vec![]);
        // No override → the machine default (notify) → inject read-only.
        assert_eq!(inbound_delivery(&config, "s1", &Scope::Channel("ops".to_owned())), Delivery::Inject(PermissionLevel::Notify));
        assert_eq!(inbound_delivery(&config, "s1", &Scope::Whisper), Delivery::Inject(PermissionLevel::Notify));
    }

    #[test]
    fn bridge_perm_emit_requires_converse_or_above() {
        let config = config_with(
            PermissionLevel::Notify,
            vec![override_for("s1", Some("ops"), PermissionLevel::Converse), override_for("s1", Some("act-chan"), PermissionLevel::Act)],
        );
        // Below converse → no emit.
        assert!(!emit_allowed(&config, "s1", &Scope::Channel("public".to_owned())));
        // Converse / act → emit allowed.
        assert!(emit_allowed(&config, "s1", &Scope::Channel("ops".to_owned())));
        assert!(emit_allowed(&config, "s1", &Scope::Channel("act-chan".to_owned())));
    }

    #[test]
    fn bridge_perm_emit_tools_offered_when_any_channel_is_converse() {
        let config = config_with(PermissionLevel::Notify, vec![override_for("s1", Some("ops"), PermissionLevel::Converse)]);

        // A session in only notify channels does not get the emit tools...
        assert!(!any_emit_allowed(&config, [("s1", "public"), ("s1", "lobby")]));
        // ...but one converse channel anywhere exposes them (per-channel gate is call-time).
        assert!(any_emit_allowed(&config, [("s1", "public"), ("s1", "ops")]));
    }
}
