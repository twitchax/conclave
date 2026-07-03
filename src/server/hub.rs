//! The transport-agnostic server core: durable store + in-memory presence, subscriptions,
//! bans, and the fan-out router (DESIGN.md §13, §14).
//!
//! The [`Hub`] owns the single source of runtime truth. Durable identity / channel / ban state
//! lives in the embedded [`Store`]; live presence and channel subscriptions are in-memory maps
//! keyed by full [`SessionPath`] (never persisted, DESIGN.md §15), and bans are write-through:
//! durable in the store, mirrored in memory for lock-guarded checks. Each live session registers
//! an outbound frame channel so the router can push channel fan-out and whispers to it. Every method
//! is transport-free — the session driver (see [`super::session`]) and the axum adapter (see
//! [`super::wss`]) both drive the same `Hub`, and the unit tests drive it directly.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
    time::Duration,
};

use tokio::{
    sync::{Notify, mpsc},
    time::Instant,
};

use crate::{
    base::{SessionPath, Visibility},
    identity::{self, AuthError},
    protocol::{self, AdminOp, ChannelInfo, HistoryMessage, MachineInfo, Payload, ProtocolError, ProtocolMessage},
    store::{ChannelRecord, Store},
};

use super::AclError;

/// Upper bound on one [`ProtocolMessage::History`] page (PRD-0013): the client re-asks with the
/// last row's `ts_ms` to page through a larger backlog.
const HISTORY_PAGE_CAP: usize = 500;

/// The outbound half of a live session: the router pushes frames here and the session's writer
/// task drains them to the transport.
type Outbound = mpsc::Sender<ProtocolMessage>;

/// A hub method's result: the response frame to return to the caller, or the wire error to surface.
type Reply = Result<ProtocolMessage, ProtocolError>;

/// Per-live-session runtime state (in-memory only, DESIGN.md §15).
struct SessionEntry {
    /// The resolved account this session authenticated as.
    user: String,
    /// The enrolled machine this session authenticated with.
    machine: String,
    /// Outbound frame sink to this session's writer task.
    outbound: Outbound,
    /// Fires when the session must be force-dropped (revocation / reaping, DESIGN.md §16).
    kill: Arc<Kill>,
    /// The channels this session is currently subscribed to.
    channels: HashSet<String>,
    /// Last inbound activity, for the heartbeat reaper (DESIGN.md §10).
    last_seen: Instant,
}

/// A session's force-drop signal, carrying *why* it fired so the termination frame self-describes
/// (a superseded session must not read like an auth failure, PRD-0012 T-002).
pub(crate) struct Kill {
    notify: Notify,
    reason: OnceLock<&'static str>,
}

impl Kill {
    fn new() -> Self {
        Self {
            notify: Notify::new(),
            reason: OnceLock::new(),
        }
    }

    /// Fires the kill, recording the reason (the first recorded reason wins).
    pub(crate) fn fire(&self, reason: &'static str) {
        let _ = self.reason.set(reason);
        self.notify.notify_one();
    }

    /// Waits for the kill to fire.
    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }

    /// The recorded reason, generic if none was set.
    pub(crate) fn reason(&self) -> &'static str {
        self.reason.get().copied().unwrap_or("session terminated")
    }
}

/// The in-memory runtime state, guarded by a single mutex (short, await-free critical sections).
#[derive(Default)]
struct HubState {
    /// Live sessions keyed by full participant path (presence truth).
    sessions: HashMap<SessionPath, SessionEntry>,
    /// Channel → subscribed session paths (the fan-out index).
    subscriptions: HashMap<String, HashSet<SessionPath>>,
    /// Channel → banned usernames — the in-memory mirror of the store's durable `ban` table,
    /// loaded at startup and written through on ban/unban (so checks stay lock-guarded, #30).
    bans: HashMap<String, HashSet<String>>,
}

/// The central relay's runtime: the embedded store, the server-admin allowlist, and live state.
pub(crate) struct Hub {
    store: Store,
    admins: super::AdminAllowlist,
    /// The store's persistent instance ID, stamped on every WS upgrade response so a bridge can
    /// recognize the same server reached under two URLs (PRD-0012 T-003).
    instance_id: String,
    state: Mutex<HubState>,
}

impl Hub {
    /// Builds a shared hub over `store`, with `admins` as the server-wide admin allowlist (§7),
    /// loading the persisted channel bans into the in-memory view (so `is_banned` stays a
    /// lock-guarded, await-free check — see `subscribe`, #30) before serving.
    ///
    /// # Errors
    ///
    /// Returns an error if the persisted bans cannot be loaded.
    pub(crate) async fn new(store: Store, admins: super::AdminAllowlist) -> crate::base::Res<Arc<Self>> {
        let mut bans: HashMap<String, HashSet<String>> = HashMap::new();
        for (channel, user) in store.list_bans().await? {
            bans.entry(channel).or_default().insert(user);
        }
        let instance_id = store.instance_id().await?;
        Ok(Arc::new(Self {
            store,
            admins,
            instance_id,
            state: Mutex::new(HubState { bans, ..HubState::default() }),
        }))
    }

    fn state(&self) -> MutexGuard<'_, HubState> {
        self.state.lock().expect("hub state mutex poisoned")
    }

    /// Whether `user` is a server-wide admin (on the serve-config allowlist, DESIGN.md §7).
    pub(crate) fn is_admin(&self, user: &str) -> bool {
        self.admins.contains_key(user)
    }

    /// The server's persistent instance ID (see the `instance_id` field).
    pub(crate) fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// The machines enrolled under `user` (for `machine list`, DESIGN.md §5.1).
    pub(crate) async fn list_machines(&self, user: &str) -> Result<Vec<MachineInfo>, ProtocolError> {
        let machines = self.store.list_machines(user).await.map_err(internal)?;
        Ok(machines
            .into_iter()
            .map(|m| MachineInfo {
                name: m.name,
                pubkey: m.pubkey,
                added_at: m.added_at,
            })
            .collect())
    }

    /// The registered usernames — server-admin only (for `user list`, DESIGN.md §7).
    pub(crate) async fn list_users(&self, caller: &str) -> Result<Vec<String>, ProtocolError> {
        if !self.is_admin(caller) {
            return Err(AclError::NotAdmin.into());
        }
        let users = self.store.list_users().await.map_err(internal)?;
        Ok(users.into_iter().map(|u| u.username).collect())
    }

    // -----------------------------------------------------------------------
    // Registration & authentication (the handshake, DESIGN.md §5).
    // -----------------------------------------------------------------------

    /// Claims a username and enrolls the calling machine as its first key (self-authorizing, §5.1).
    #[tracing::instrument(level = "debug", skip(self, pubkey))]
    pub(crate) async fn register(&self, username: &str, machine: &str, pubkey: &[u8]) -> Result<(), ProtocolError> {
        let pubkey_b64 = identity::encode_key(pubkey);

        // A pinned admin username may be claimed only by its bound key, so an admin name cannot be
        // squatted first-come on a fresh deploy (anti-squat, PRD-0007 T-002 / DESIGN.md §7).
        if let Some(Some(bound)) = self.admins.get(username)
            && &pubkey_b64 != bound
        {
            return Err(AuthError::Reserved(username.to_owned()).into());
        }

        if self.store.get_user(username).await.map_err(internal)?.is_some() {
            return Err(AuthError::UsernameTaken(username.to_owned()).into());
        }
        if self.store.get_machine_by_pubkey(&pubkey_b64).await.map_err(internal)?.is_some() {
            return Err(AuthError::Malformed("public key is already enrolled".to_owned()).into());
        }

        self.store.create_user(username).await.map_err(internal)?;
        self.store.create_machine(username, machine, &pubkey_b64).await.map_err(internal)?;
        Ok(())
    }

    /// Resolves an enrolled public key to its `(user, machine)`; a removed key is unknown (§5.1).
    pub(crate) async fn resolve(&self, pubkey: &[u8]) -> Result<(String, String), ProtocolError> {
        let pubkey_b64 = identity::encode_key(pubkey);
        let machine = self
            .store
            .get_machine_by_pubkey(&pubkey_b64)
            .await
            .map_err(internal)?
            .ok_or_else(|| ProtocolError::from(AuthError::UnknownKey))?;
        Ok((machine.user, machine.name))
    }

    /// Registers a live session and returns its kill handle, superseding any prior session on the
    /// same path so a half-open reconnect takes over immediately (§5, #16).
    pub(crate) fn attach(&self, path: &SessionPath, user: &str, machine: &str, outbound: Outbound) -> Arc<Kill> {
        let mut st = self.state();
        // A fresh authenticated session for this path supersedes any prior one, so a reconnect after
        // an ungraceful (half-open) drop takes over immediately instead of waiting out the idle
        // reaper (#16). Possession is already proven at auth, so only the identity holder can take
        // over its own handle; a true duplicate (same handle, two live processes) is last-writer-wins.
        if let Some(existing) = st.sessions.get(path) {
            existing.kill.fire("session superseded by a newer connection for the same session path");
            Self::take_session(&mut st, path);
            tracing::info!(%path, "session superseded by a newer connection");
        }
        tracing::info!(%path, user, machine, "session established");

        let kill = Arc::new(Kill::new());
        st.sessions.insert(
            path.clone(),
            SessionEntry {
                user: user.to_owned(),
                machine: machine.to_owned(),
                outbound,
                kill: Arc::clone(&kill),
                channels: HashSet::new(),
                last_seen: Instant::now(),
            },
        );
        kill
    }

    /// Removes a session's presence and subscriptions (on clean disconnect); idempotent. Guarded by
    /// session identity so a session that was superseded (#16) does not evict its replacement.
    pub(crate) fn detach(&self, path: &SessionPath, kill: &Arc<Kill>) {
        let mut st = self.state();
        if st.sessions.get(path).is_some_and(|e| Arc::ptr_eq(&e.kill, kill)) {
            Self::take_session(&mut st, path);
            tracing::debug!(%path, "session detached");
        }
    }

    /// Refreshes a session's heartbeat timestamp on any inbound activity (DESIGN.md §10).
    pub(crate) fn touch(&self, path: &SessionPath) {
        if let Some(entry) = self.state().sessions.get_mut(path) {
            entry.last_seen = Instant::now();
        }
    }

    /// Force-drops every session idle for at least `timeout`, returning how many were reaped (§10).
    pub(crate) fn reap_idle(&self, timeout: Duration) -> usize {
        let now = Instant::now();
        let mut st = self.state();
        let stale: Vec<SessionPath> = st
            .sessions
            .iter()
            .filter(|(_, e)| now.saturating_duration_since(e.last_seen) >= timeout)
            .map(|(p, _)| p.clone())
            .collect();

        for path in &stale {
            Self::kill_locked(&mut st, path, "idle timeout: no heartbeat");
        }
        stale.len()
    }

    // -----------------------------------------------------------------------
    // Membership: join / leave / discovery (DESIGN.md §6).
    // -----------------------------------------------------------------------

    /// Joins a channel — authorized by visibility, ACL membership, or a redeemed invite token (§6).
    #[tracing::instrument(level = "debug", skip(self, token), fields(path = %path))]
    pub(crate) async fn join(&self, user: &str, path: &SessionPath, channel: &str, token: Option<&str>) -> Result<(), ProtocolError> {
        let record = self
            .store
            .get_channel(channel)
            .await
            .map_err(internal)?
            .ok_or_else(|| ProtocolError::NotFound(format!("channel `{channel}`")))?;

        if self.is_banned(channel, user) {
            return Err(AclError::ChannelPrivate(channel.to_owned()).into());
        }

        let already_member = self.store.is_channel_member(channel, user).await.map_err(internal)?;
        if !already_member {
            match parse_visibility(&record.visibility) {
                // Public and unlisted are open to anyone who reaches them (unlisted needs the name).
                Visibility::Public | Visibility::Unlisted => {}
                // Private requires an ACL entry (absent here) or a valid invite that grants one.
                Visibility::Private => {
                    // Denials on a private channel return the same `not found` as an absent channel,
                    // so its existence never leaks to a non-member (finding #12).
                    let token = token.ok_or_else(|| ProtocolError::from(AclError::ChannelNotFound(channel.to_owned())))?;
                    self.redeem_invite(channel, token).await?;
                    self.store.add_channel_member(channel, user).await.map_err(internal)?;
                }
            }
        }

        // The subscribe re-checks the ban atomically; a ban that raced this join wins (#30).
        if !self.subscribe(path, channel) {
            return Err(AclError::ChannelPrivate(channel.to_owned()).into());
        }
        Ok(())
    }

    /// Unsubscribes a session from a channel (it stays present for its other channels).
    pub(crate) fn leave(&self, path: &SessionPath, channel: &str) {
        self.unsubscribe_session(path, channel);
    }

    /// The channels visible to `user`: every public channel plus any private/unlisted one they
    /// belong to. Private and unlisted names never leak to non-members (DESIGN.md §6).
    pub(crate) async fn list_channels(&self, user: &str) -> Result<Vec<ChannelInfo>, ProtocolError> {
        let channels = self.store.list_channels().await.map_err(internal)?;
        let memberships: HashSet<String> = self.store.list_user_memberships(user).await.map_err(internal)?.into_iter().collect();
        // Server admins enumerate everything (operator visibility — an admin must be able to audit
        // channels they are not a member of); everyone else sees public + their memberships.
        let admin = self.is_admin(user);
        let infos = channels
            .into_iter()
            .filter_map(|c| {
                let visibility = parse_visibility(&c.visibility);
                let member = memberships.contains(&c.name);
                let visible = admin || matches!(visibility, Visibility::Public) || member;
                visible.then_some(ChannelInfo { name: c.name, visibility, member })
            })
            .collect();
        Ok(infos)
    }

    /// Presence: server-wide when `channel` is `None`, else membership-gated to that channel (§6).
    pub(crate) async fn who(&self, user: &str, channel: Option<&str>) -> Result<Vec<SessionPath>, ProtocolError> {
        let Some(channel) = channel else {
            return Ok(self.present_paths());
        };

        // Presence is visible on a public channel to anyone — so a participant who holds no ACL
        // entry can still query it (finding #13) — and on a private/unlisted channel only to a
        // member or admin. Otherwise return the same `not found` as an absent channel, so the
        // existence of a private/unlisted channel never leaks (finding #12).
        let not_found = || ProtocolError::from(AclError::ChannelNotFound(channel.to_owned()));
        let record = self.store.get_channel(channel).await.map_err(internal)?.ok_or_else(not_found)?;
        let allowed = match parse_visibility(&record.visibility) {
            Visibility::Public => true,
            Visibility::Unlisted | Visibility::Private => self.store.is_channel_member(channel, user).await.map_err(internal)? || self.is_admin(user),
        };
        if !allowed {
            return Err(not_found());
        }

        let st = self.state();
        Ok(st.subscriptions.get(channel).map(|subs| subs.iter().cloned().collect()).unwrap_or_default())
    }

    // -----------------------------------------------------------------------
    // Routing: channel fan-out and whispers (DESIGN.md §8, §14).
    // -----------------------------------------------------------------------

    /// Fans a channel message out to every *other* subscribed session; the sender must be a member.
    /// The message is retained for catch-up before delivery (PRD-0013): a failed history write
    /// logs and still delivers — availability over completeness.
    #[tracing::instrument(level = "debug", skip(self, payload), fields(from = %from))]
    pub(crate) async fn post(&self, from: &SessionPath, channel: &str, payload: Payload) -> Result<(), ProtocolError> {
        let targets: Vec<(Arc<Kill>, Outbound)> = {
            let st = self.state();
            let subs = st.subscriptions.get(channel).ok_or_else(|| ProtocolError::from(AclError::NotMember(channel.to_owned())))?;
            if !subs.contains(from) {
                return Err(AclError::NotMember(channel.to_owned()).into());
            }
            subs.iter()
                .filter(|p| *p != from)
                .filter_map(|p| st.sessions.get(p).map(|e| (Arc::clone(&e.kill), e.outbound.clone())))
                .collect()
        };

        // Retain the envelope verbatim, server-stamped (the read-since watermark unit).
        match protocol::encode_payload(&payload) {
            Ok(bytes) => {
                let ts_ms = chrono::Utc::now().timestamp_millis();
                if let Err(err) = self.store.append_message(channel, &from.to_string(), &bytes, ts_ms).await {
                    tracing::warn!(%channel, error = %err, "failed to retain channel history; delivering live only");
                }
            }
            Err(err) => tracing::warn!(%channel, error = %err, "failed to encode payload for retention; delivering live only"),
        }

        let msg = ProtocolMessage::ChannelMsg {
            channel: channel.to_owned(),
            from: from.clone(),
            payload,
        };
        for (kill, tx) in targets {
            // A full (slow consumer) or closed queue: force-drop it rather than grow memory (#14);
            // a reconnect re-subscribes with fresh state.
            if tx.try_send(msg.clone()).is_err() {
                kill.fire("slow consumer: outbound queue overflowed");
            }
        }
        Ok(())
    }

    /// Reads a channel's retained history strictly after `since_ms` (PRD-0013 T-002). The caller
    /// must be subscribed — the same check (and the same error) as posting, so a refusal never
    /// reveals whether a private channel exists.
    #[tracing::instrument(level = "debug", skip(self), fields(caller = %caller))]
    pub(crate) async fn read_since(&self, caller: &SessionPath, channel: &str, since_ms: i64) -> Result<ProtocolMessage, ProtocolError> {
        {
            let st = self.state();
            let subs = st.subscriptions.get(channel).ok_or_else(|| ProtocolError::from(AclError::NotMember(channel.to_owned())))?;
            if !subs.contains(caller) {
                return Err(AclError::NotMember(channel.to_owned()).into());
            }
        }

        let rows = self.store.read_messages_since(channel, since_ms, HISTORY_PAGE_CAP).await.map_err(internal)?;
        let messages = rows
            .into_iter()
            .filter_map(|row| {
                // Undecodable rows (wire evolution beyond this build) are skipped, not fatal.
                let payload = protocol::decode_payload(&row.payload).ok()?;
                let from = row.from.parse::<SessionPath>().ok()?;
                Some(HistoryMessage { from, ts_ms: row.ts_ms, payload })
            })
            .collect();
        Ok(ProtocolMessage::History { channel: channel.to_owned(), messages })
    }

    /// Delivers a whisper to exactly one live session path, erroring if it is not online (§8).
    #[tracing::instrument(level = "debug", skip(self, payload), fields(from = %from, target = %target))]
    pub(crate) fn whisper(&self, from: &SessionPath, target: &SessionPath, payload: Payload) -> Result<(), ProtocolError> {
        let target_entry = self.state().sessions.get(target).map(|e| (Arc::clone(&e.kill), e.outbound.clone()));
        let Some((kill, outbound)) = target_entry else {
            return Err(ProtocolError::NotFound(format!("session `{target}` is not online")));
        };

        let msg = ProtocolMessage::Whisper {
            from: from.clone(),
            target: target.clone(),
            payload,
        };
        // Force-drop a slow/closed consumer rather than grow its queue unbounded (#14).
        if outbound.try_send(msg).is_err() {
            kill.fire("slow consumer: outbound queue overflowed");
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Admin & moderation (DESIGN.md §7); revocation force-drops live state (§16).
    // -----------------------------------------------------------------------

    /// Authorizes and applies an admin / control operation, returning the response frame.
    #[tracing::instrument(level = "debug", skip(self, op), fields(op = op.name()))]
    pub(crate) async fn admin(&self, user: &str, op: AdminOp) -> Reply {
        match op {
            // Any authenticated user may create a channel and becomes its channel-admin (§7).
            AdminOp::CreateChannel { name, visibility } => {
                self.store.create_channel(&name, visibility, user).await.map_err(internal)?;
                Ok(ack(name))
            }
            AdminOp::DeleteChannel { name } => {
                self.authorize_channel_admin(&name, user).await?;
                self.store.delete_channel(&name).await.map_err(internal)?;
                self.drop_channel(&name);
                Ok(ack(name))
            }
            AdminOp::RenameChannel { name, new_name } => {
                self.authorize_channel_admin(&name, user).await?;
                self.store.rename_channel(&name, &new_name).await.map_err(internal)?;
                self.rename_channel_subscriptions(&name, &new_name);
                Ok(ack(new_name))
            }
            AdminOp::SetVisibility { name, visibility } => {
                self.authorize_channel_admin(&name, user).await?;
                self.store.set_channel_visibility(&name, visibility).await.map_err(internal)?;
                Ok(ack(name))
            }
            AdminOp::AclAdd { channel, user: target } => {
                self.authorize_channel_admin(&channel, user).await?;
                self.store.add_channel_member(&channel, &target).await.map_err(internal)?;
                self.remove_ban(&channel, &target).await?;
                Ok(ack(target))
            }
            AdminOp::AclRemove { channel, user: target } => {
                self.authorize_channel_admin(&channel, user).await?;
                self.store.remove_channel_member(&channel, &target).await.map_err(internal)?;
                self.unsubscribe_user(&target, &channel);
                Ok(ack(target))
            }
            // Channel-admin membership audit: who is on this channel's ACL (not live presence).
            AdminOp::AclList { channel } => {
                self.authorize_channel_admin(&channel, user).await?;
                let users = self.store.list_channel_members(&channel).await.map_err(internal)?;
                Ok(ProtocolMessage::UserList { users })
            }
            // First-class unban (previously only an acl-add side effect); grants no membership.
            AdminOp::Unban { channel, user: target } => {
                self.authorize_channel_admin(&channel, user).await?;
                self.remove_ban(&channel, &target).await?;
                Ok(ack(target))
            }
            // Channel-admin ban audit — durable bans must never be write-only state.
            AdminOp::BanList { channel } => {
                self.authorize_channel_admin(&channel, user).await?;
                let users = self.store.list_channel_bans(&channel).await.map_err(internal)?;
                Ok(ProtocolMessage::UserList { users })
            }
            // Channel-admin invite audit — outstanding tokens with uses/expiry (PRD-0011).
            AdminOp::InviteList { channel } => self.list_channel_invites(user, &channel).await,
            AdminOp::InviteCreate { channel, uses, expires_in_secs } => self.create_invite(user, &channel, uses, expires_in_secs).await,
            AdminOp::InviteRevoke { token } => {
                // Uniform ack whether or not the token exists, so a non-admin cannot use revoke as an
                // existence oracle for tokens (#29): only the channel's admin actually deletes.
                if let Some(invite) = self.store.get_invite(&token).await.map_err(internal)?
                    && self.is_channel_admin(&invite.channel, user).await
                {
                    self.store.delete_invite(&token).await.map_err(internal)?;
                }
                Ok(ack(token))
            }
            AdminOp::Kick { channel, target } => {
                self.authorize_channel_admin(&channel, user).await?;
                if let Ok(path) = target.parse::<SessionPath>() {
                    self.unsubscribe_session(&path, &channel);
                } else {
                    self.unsubscribe_user(&target, &channel);
                }
                Ok(ack(target))
            }
            AdminOp::Ban { channel, user: target } => {
                self.authorize_channel_admin(&channel, user).await?;
                self.store.remove_channel_member(&channel, &target).await.map_err(internal)?;
                self.add_ban(&channel, &target).await?;
                self.unsubscribe_user(&target, &channel);
                Ok(ack(target))
            }
            AdminOp::UserRemove { username } => {
                if !self.is_admin(user) {
                    return Err(AclError::NotAdmin.into());
                }
                self.remove_user(&username).await?;
                Ok(ack(username))
            }
            // The lost-laptop kill switch: a user revokes their own machine key (§5.1).
            AdminOp::MachineRemove { name } => {
                self.store.delete_machine(user, &name).await.map_err(internal)?;
                self.force_drop_machine(user, &name);
                Ok(ack(name))
            }
            // Self-service enrollment: adds a machine key to the *caller's own* account (`user`) —
            // never another user's, so there is no cross-account key-planting path (#8). The key
            // confers no access until its holder proves possession at first-connect auth (the
            // challenge signature is verified against this pubkey), so enrolling a key one does not
            // hold grants nothing. Enrollment-time proof would add no security here: a party able to
            // enroll their own key can equally sign an enrollment challenge with it.
            AdminOp::MachineAdd { name, pubkey } => {
                let pubkey_b64 = identity::encode_key(&pubkey);
                self.store.create_machine(user, &name, &pubkey_b64).await.map_err(internal)?;
                Ok(ack(name))
            }
        }
    }

    /// Mints an invite token for a channel (channel-admin; bounded uses / expiry).
    async fn create_invite(&self, user: &str, channel: &str, max_uses: Option<u32>, lifetime_secs: Option<u64>) -> Reply {
        self.authorize_channel_admin(channel, user).await?;
        let token = identity::generate_token().map_err(internal)?;
        let expires_at = match lifetime_secs {
            Some(secs) => Some(invite_expiry(secs)?),
            None => None,
        };
        self.store.create_invite(channel, &token, max_uses.map(i64::from), expires_at, user).await.map_err(internal)?;
        Ok(ProtocolMessage::InviteToken { token })
    }

    /// The channel-admin invite audit: outstanding tokens with uses/expiry (PRD-0011).
    async fn list_channel_invites(&self, user: &str, channel: &str) -> Reply {
        self.authorize_channel_admin(channel, user).await?;
        let invites = self
            .store
            .list_invites(channel)
            .await
            .map_err(internal)?
            .into_iter()
            .map(|i| crate::protocol::InviteInfo {
                token: i.token,
                uses_remaining: i.uses_remaining,
                expires_at: i.expires_at,
            })
            .collect();
        Ok(ProtocolMessage::InviteList { invites })
    }

    /// Erases a user entirely: their created channels (cascading memberships + invites + bans),
    /// their memberships elsewhere, their machines, and the user row — so re-registering the freed
    /// name cannot inherit channel-admin rights or private access (finding #6). Live sessions drop.
    async fn remove_user(&self, username: &str) -> Result<(), ProtocolError> {
        for channel in self.store.list_channels_created_by(username).await.map_err(internal)? {
            self.store.delete_channel(&channel).await.map_err(internal)?;
            self.drop_channel(&channel);
        }
        self.store.delete_user_memberships(username).await.map_err(internal)?;
        for machine in self.store.list_machines(username).await.map_err(internal)? {
            self.store.delete_machine(username, &machine.name).await.map_err(internal)?;
        }
        self.store.delete_user(username).await.map_err(internal)?;
        self.force_drop_user(username);
        Ok(())
    }

    async fn authorize_channel_admin(&self, channel: &str, user: &str) -> Result<ChannelRecord, ProtocolError> {
        let record = self
            .store
            .get_channel(channel)
            .await
            .map_err(internal)?
            .ok_or_else(|| ProtocolError::from(AclError::ChannelNotFound(channel.to_owned())))?;
        if record.created_by == user || self.is_admin(user) {
            Ok(record)
        } else {
            Err(AclError::NotAdmin.into())
        }
    }

    /// Whether `user` administers `channel` (its creator or a server admin) — a boolean check that
    /// never errors, so a caller can gate silently without leaking the channel's existence (#29).
    async fn is_channel_admin(&self, channel: &str, user: &str) -> bool {
        match self.store.get_channel(channel).await {
            Ok(Some(record)) => record.created_by == user || self.is_admin(user),
            _ => false,
        }
    }

    async fn redeem_invite(&self, channel: &str, token: &str) -> Result<(), ProtocolError> {
        // An invalid / wrong-channel / expired / spent token is refused as `not found`, matching an
        // absent channel so a private channel's existence never leaks (finding #12).
        let invite = self
            .store
            .get_invite(token)
            .await
            .map_err(internal)?
            .filter(|inv| inv.channel == channel)
            .ok_or_else(|| ProtocolError::from(AclError::ChannelNotFound(channel.to_owned())))?;

        if invite.expires_at.as_deref().is_some_and(is_expired) {
            self.store.delete_invite(token).await.map_err(internal)?;
            return Err(AclError::ChannelNotFound(channel.to_owned()).into());
        }

        // Unlimited tokens (`None`) grant access without consuming a use; a limited token atomically
        // consumes one, so concurrent redeemers of the last use are mutually exclusive (T-003).
        if invite.uses_remaining.is_some() && !self.store.try_consume_invite_use(token).await.map_err(internal)? {
            return Err(AclError::ChannelNotFound(channel.to_owned()).into());
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // In-memory presence / subscription mutation (all lock-guarded, await-free).
    // -----------------------------------------------------------------------

    /// Subscribes a session to a channel; returns `false` (no-op) if the user is banned or the
    /// session is gone. The ban is re-checked here under the same lock as the insert, so a ban that
    /// lands between join's pre-check and this call cannot be bypassed (#30 TOCTOU).
    fn subscribe(&self, path: &SessionPath, channel: &str) -> bool {
        let mut st = self.state();
        if st.bans.get(channel).is_some_and(|banned| banned.contains(&path.user)) {
            return false;
        }
        let Some(entry) = st.sessions.get_mut(path) else {
            return false;
        };
        entry.channels.insert(channel.to_owned());
        st.subscriptions.entry(channel.to_owned()).or_default().insert(path.clone());
        true
    }

    fn unsubscribe_session(&self, path: &SessionPath, channel: &str) {
        let mut st = self.state();
        if let Some(subs) = st.subscriptions.get_mut(channel) {
            subs.remove(path);
            if subs.is_empty() {
                st.subscriptions.remove(channel);
            }
        }
        if let Some(entry) = st.sessions.get_mut(path) {
            entry.channels.remove(channel);
        }
    }

    fn unsubscribe_user(&self, user: &str, channel: &str) {
        let mut st = self.state();
        let Some(subs) = st.subscriptions.get(channel) else {
            return;
        };
        let paths: Vec<SessionPath> = subs.iter().filter(|p| st.sessions.get(*p).is_some_and(|e| e.user == user)).cloned().collect();
        for path in paths {
            if let Some(subs) = st.subscriptions.get_mut(channel) {
                subs.remove(&path);
                if subs.is_empty() {
                    st.subscriptions.remove(channel);
                }
            }
            if let Some(entry) = st.sessions.get_mut(&path) {
                entry.channels.remove(channel);
            }
        }
    }

    fn drop_channel(&self, channel: &str) {
        let mut st = self.state();
        if let Some(subs) = st.subscriptions.remove(channel) {
            for path in subs {
                if let Some(entry) = st.sessions.get_mut(&path) {
                    entry.channels.remove(channel);
                }
            }
        }
        st.bans.remove(channel);
    }

    fn rename_channel_subscriptions(&self, old: &str, new: &str) {
        let mut st = self.state();
        if let Some(subs) = st.subscriptions.remove(old) {
            for path in &subs {
                if let Some(entry) = st.sessions.get_mut(path) {
                    entry.channels.remove(old);
                    entry.channels.insert(new.to_owned());
                }
            }
            st.subscriptions.insert(new.to_owned(), subs);
        }
        if let Some(banned) = st.bans.remove(old) {
            st.bans.insert(new.to_owned(), banned);
        }
    }

    fn force_drop_user(&self, user: &str) {
        let mut st = self.state();
        let paths: Vec<SessionPath> = st.sessions.iter().filter(|(_, e)| e.user == user).map(|(p, _)| p.clone()).collect();
        for path in &paths {
            Self::kill_locked(&mut st, path, "user removed from this server");
        }
    }

    fn force_drop_machine(&self, user: &str, machine: &str) {
        let mut st = self.state();
        let paths: Vec<SessionPath> = st.sessions.iter().filter(|(_, e)| e.user == user && e.machine == machine).map(|(p, _)| p.clone()).collect();
        for path in &paths {
            Self::kill_locked(&mut st, path, "machine key revoked");
        }
    }

    fn is_banned(&self, channel: &str, user: &str) -> bool {
        self.state().bans.get(channel).is_some_and(|banned| banned.contains(user))
    }

    /// Records a ban durably, then mirrors it in memory (write-through: the store survives a
    /// restart, the in-memory set serves the lock-guarded checks).
    async fn add_ban(&self, channel: &str, user: &str) -> Result<(), ProtocolError> {
        self.store.add_ban(channel, user).await.map_err(internal)?;
        self.state().bans.entry(channel.to_owned()).or_default().insert(user.to_owned());
        Ok(())
    }

    /// Lifts a ban durably, then in memory (the unban path: `AclAdd` re-admits a banned user).
    async fn remove_ban(&self, channel: &str, user: &str) -> Result<(), ProtocolError> {
        self.store.remove_ban(channel, user).await.map_err(internal)?;
        if let Some(banned) = self.state().bans.get_mut(channel) {
            banned.remove(user);
        }
        Ok(())
    }

    /// Removes a session and its subscriptions, returning the entry (shared by detach and kill).
    fn take_session(st: &mut HubState, path: &SessionPath) -> Option<SessionEntry> {
        let entry = st.sessions.remove(path)?;
        for channel in &entry.channels {
            if let Some(subs) = st.subscriptions.get_mut(channel) {
                subs.remove(path);
                if subs.is_empty() {
                    st.subscriptions.remove(channel);
                }
            }
        }
        Some(entry)
    }

    /// Force-drops a session: removes it and signals its driver to shut the transport (§16).
    fn kill_locked(st: &mut HubState, path: &SessionPath, reason: &'static str) {
        if let Some(entry) = Self::take_session(st, path) {
            entry.kill.fire(reason);
        }
    }

    // -----------------------------------------------------------------------
    // Read-only views (used by the session driver and the tests).
    // -----------------------------------------------------------------------

    /// Every currently-present session path (server-wide presence).
    pub(crate) fn present_paths(&self) -> Vec<SessionPath> {
        self.state().sessions.keys().cloned().collect()
    }

    /// Whether a given session path is currently present (used by the tests).
    #[cfg(test)]
    pub(crate) fn is_present(&self, path: &SessionPath) -> bool {
        self.state().sessions.contains_key(path)
    }

    /// The session paths currently subscribed to a channel (used by the tests).
    #[cfg(test)]
    pub(crate) fn subscribers(&self, channel: &str) -> Vec<SessionPath> {
        self.state().subscriptions.get(channel).map(|subs| subs.iter().cloned().collect()).unwrap_or_default()
    }
}

/// Wraps any error as an opaque wire [`ProtocolError::Internal`] (store / I/O failures).
fn internal<E: Display>(err: E) -> ProtocolError {
    ProtocolError::Internal(err.to_string())
}

/// A successful control-op acknowledgement carrying the affected name.
fn ack(detail: impl Into<String>) -> ProtocolMessage {
    ProtocolMessage::Ack { detail: Some(detail.into()) }
}

/// Parses a stored visibility token, defaulting to the most-restrictive-safe `public` listing
/// behavior only if the datum is somehow unknown (our own writes are always valid).
fn parse_visibility(token: &str) -> Visibility {
    token.parse().unwrap_or(Visibility::Public)
}

/// Whether an RFC 3339 expiry has passed; a malformed timestamp fails closed (treated as expired).
fn is_expired(rfc3339: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(rfc3339).map_or(true, |dt| dt.with_timezone(&chrono::Utc) <= chrono::Utc::now())
}

/// An RFC 3339 expiry `secs` seconds from now, computed with checked arithmetic so an absurd
/// duration (from the wire or the CLI's `--expires-in`) returns an error instead of overflow-
/// panicking (PRD-0007 T-005, finding #10).
fn invite_expiry(secs: u64) -> Result<String, ProtocolError> {
    let too_far = || ProtocolError::MalformedFrame("invite expiry is too far in the future".to_owned());
    let secs = i64::try_from(secs).map_err(|_| too_far())?;
    let delta = chrono::TimeDelta::try_seconds(secs).ok_or_else(too_far)?;
    let expiry = chrono::Utc::now().checked_add_signed(delta).ok_or_else(too_far)?;
    Ok(expiry.to_rfc3339())
}

#[cfg(test)]
mod tests {
    // Tests relax `unwrap_used` (house convention; DESIGN.md §22).
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::store::Store;

    async fn hub_with_private_channel(token_uses: Option<i64>, expires_at: Option<String>) -> Arc<Hub> {
        let store = Store::open_in_memory().await.unwrap();
        store.create_channel("ops", Visibility::Private, "aaron").await.unwrap();
        store.create_invite("ops", "tok", token_uses, expires_at, "aaron").await.unwrap();
        Hub::new(store, HashMap::new()).await.unwrap()
    }

    fn attach_session(hub: &Arc<Hub>, user: &str) -> SessionPath {
        let path = SessionPath::new(user, "machine", "session");
        // The outbound receiver is unused (these tests don't fan out); attach only needs the sender.
        let (tx, _rx) = mpsc::channel(super::super::session::OUTBOUND_CAPACITY);
        hub.attach(&path, user, "machine", tx);
        path
    }

    /// A shared in-memory sink usable as a `tracing` writer (PRD-0014 uat-001).
    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Buf {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(data);
            Ok(data.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl tracing_subscriber::fmt::MakeWriter<'_> for Buf {
        type Writer = Buf;

        fn make_writer(&self) -> Buf {
            self.clone()
        }
    }

    #[tokio::test]
    async fn hub_request_paths_emit_spans_with_path_and_channel_fields() {
        // Capture everything the request paths emit (PRD-0014 T-001). Single-threaded test
        // runtime, so the scoped default subscriber holds across awaits.
        let buf = Buf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NEW)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let store = Store::open_in_memory().await.unwrap();
        store.create_channel("ops", Visibility::Public, "aaron").await.unwrap();
        let hub = Hub::new(store, HashMap::new()).await.unwrap();

        let aaron = attach_session(&hub, "aaron");
        hub.join("aaron", &aaron, "ops", None).await.unwrap();
        hub.post(&aaron, "ops", Payload::Plain("observable".to_owned())).await.unwrap();
        hub.read_since(&aaron, "ops", 0).await.unwrap();

        let output = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        // Lifecycle is an event carrying the full session path…
        assert!(
            output.contains("session established") && output.contains("aaron/machine/session"),
            "attach must log establishment with the path: {output}"
        );
        // …and each request path is a span carrying the caller and channel fields.
        for span in ["join", "post", "read_since"] {
            assert!(output.contains(span), "the `{span}` path must be instrumented: {output}");
        }
        assert!(output.contains("ops"), "spans must carry the channel: {output}");
        // Message bodies must NOT be logged (privacy: telemetry never contains content).
        assert!(!output.contains("observable"), "payload bodies must never reach telemetry: {output}");
    }

    #[tokio::test]
    async fn fanout_drops_a_slow_consumer_instead_of_growing_its_queue() {
        let store = Store::open_in_memory().await.unwrap();
        store.create_channel("ops", Visibility::Public, "aaron").await.unwrap();
        let hub = Hub::new(store, HashMap::new()).await.unwrap();

        // aaron posts; bob is a slow consumer with a tiny, never-drained outbound queue.
        let aaron = attach_session(&hub, "aaron");
        hub.join("aaron", &aaron, "ops", None).await.unwrap();

        let bob = SessionPath::new("bob", "machine", "session");
        let (b_tx, _b_rx) = mpsc::channel(1);
        let b_kill = hub.attach(&bob, "bob", "machine", b_tx);
        hub.join("bob", &bob, "ops", None).await.unwrap();

        // Fill bob's 1-slot queue, then overflow it — the second post cannot enqueue.
        hub.post(&aaron, "ops", Payload::Plain("one".to_owned())).await.unwrap();
        hub.post(&aaron, "ops", Payload::Plain("two".to_owned())).await.unwrap();

        // The slow consumer is force-dropped (its kill fires) rather than buffering without bound.
        assert!(
            tokio::time::timeout(Duration::from_millis(500), b_kill.notified()).await.is_ok(),
            "a consumer that fills its bounded queue must be force-dropped",
        );
    }

    #[tokio::test]
    async fn invite_revoke_gives_a_uniform_ack_and_no_delete_for_non_admins() {
        let hub = hub_with_private_channel(None, None).await; // channel `ops` (private), invite `tok`, admin `aaron`.

        // A non-admin gets the same ack whether the token exists or not — no existence oracle (#29).
        let existing = hub.admin("mallory", AdminOp::InviteRevoke { token: "tok".to_owned() }).await.unwrap();
        let absent = hub.admin("mallory", AdminOp::InviteRevoke { token: "ghost".to_owned() }).await.unwrap();
        assert!(
            matches!(existing, ProtocolMessage::Ack { .. }),
            "revoking an existing token as a non-admin must ack, not error: {existing:?}"
        );
        assert!(matches!(absent, ProtocolMessage::Ack { .. }), "revoking an absent token must ack identically: {absent:?}");

        // The non-admin did not actually delete the token: a legitimate redeemer still gets in.
        let carol = attach_session(&hub, "carol");
        assert!(hub.join("carol", &carol, "ops", Some("tok")).await.is_ok(), "a non-admin revoke must not delete the token");
    }

    #[tokio::test]
    async fn list_channels_shows_a_server_admin_everything() {
        let store = Store::open_in_memory().await.unwrap();
        // alice's private channel; neither root (server admin) nor bob is a member.
        store.create_channel("secret", Visibility::Private, "alice").await.unwrap();
        let hub = Hub::new(store, HashMap::from([("root".to_owned(), None)])).await.unwrap();

        // The server admin can enumerate every channel (operator visibility)...
        let admin_view: Vec<String> = hub.list_channels("root").await.unwrap().into_iter().map(|c| c.name).collect();
        assert!(admin_view.contains(&"secret".to_owned()), "a server admin must see private channels: {admin_view:?}");

        // ...while a regular non-member still cannot discover it.
        let bob_view: Vec<String> = hub.list_channels("bob").await.unwrap().into_iter().map(|c| c.name).collect();
        assert!(!bob_view.contains(&"secret".to_owned()), "a private channel must stay hidden from non-members: {bob_view:?}");
    }

    #[tokio::test]
    async fn acl_list_returns_members_to_channel_admins_only() {
        let store = Store::open_in_memory().await.unwrap();
        store.create_channel("ops", Visibility::Private, "aaron").await.unwrap();
        let hub = Hub::new(store, HashMap::new()).await.unwrap();

        hub.admin(
            "aaron",
            AdminOp::AclAdd {
                channel: "ops".to_owned(),
                user: "david".to_owned(),
            },
        )
        .await
        .unwrap();

        // The channel admin (creator) lists the membership — the creator is a member from creation.
        match hub.admin("aaron", AdminOp::AclList { channel: "ops".to_owned() }).await.unwrap() {
            ProtocolMessage::UserList { mut users } => {
                users.sort();
                assert_eq!(users, vec!["aaron".to_owned(), "david".to_owned()]);
            }
            other => panic!("expected a UserList, got {other:?}"),
        }

        // A non-admin is refused.
        assert!(
            hub.admin("mallory", AdminOp::AclList { channel: "ops".to_owned() }).await.is_err(),
            "a non-admin must not list a channel's members",
        );
    }

    #[tokio::test]
    async fn invite_list_shows_outstanding_tokens_to_channel_admins_only() {
        let hub = hub_with_private_channel(Some(2), None).await; // `ops` with invite `tok` by aaron.

        // The channel admin sees the outstanding token with its remaining uses.
        match hub.admin("aaron", AdminOp::InviteList { channel: "ops".to_owned() }).await.unwrap() {
            ProtocolMessage::InviteList { invites } => {
                assert_eq!(invites.len(), 1);
                assert_eq!(invites[0].token, "tok");
                assert_eq!(invites[0].uses_remaining, Some(2));
            }
            other => panic!("expected an InviteList, got {other:?}"),
        }

        // A non-admin is refused.
        assert!(hub.admin("mallory", AdminOp::InviteList { channel: "ops".to_owned() }).await.is_err());
    }

    #[tokio::test]
    async fn ban_visibility_list_and_unban_are_channel_admin_gated() {
        let store = Store::open_in_memory().await.unwrap();
        store.create_channel("ops", Visibility::Public, "aaron").await.unwrap();
        let hub = Hub::new(store.clone(), HashMap::new()).await.unwrap();

        hub.admin(
            "aaron",
            AdminOp::Ban {
                channel: "ops".to_owned(),
                user: "bob".to_owned(),
            },
        )
        .await
        .unwrap();

        // The channel admin lists the bans...
        match hub.admin("aaron", AdminOp::BanList { channel: "ops".to_owned() }).await.unwrap() {
            ProtocolMessage::UserList { users } => assert_eq!(users, vec!["bob".to_owned()]),
            other => panic!("expected a UserList of bans, got {other:?}"),
        }
        // ...a non-admin is refused for both verbs...
        assert!(hub.admin("mallory", AdminOp::BanList { channel: "ops".to_owned() }).await.is_err());
        assert!(
            hub.admin(
                "mallory",
                AdminOp::Unban {
                    channel: "ops".to_owned(),
                    user: "bob".to_owned(),
                }
            )
            .await
            .is_err()
        );

        // ...and unban lifts the ban durably WITHOUT granting ACL membership.
        hub.admin(
            "aaron",
            AdminOp::Unban {
                channel: "ops".to_owned(),
                user: "bob".to_owned(),
            },
        )
        .await
        .unwrap();
        match hub.admin("aaron", AdminOp::BanList { channel: "ops".to_owned() }).await.unwrap() {
            ProtocolMessage::UserList { users } => assert!(users.is_empty(), "unban must lift the ban: {users:?}"),
            other => panic!("expected a UserList, got {other:?}"),
        }
        assert!(store.list_bans().await.unwrap().is_empty(), "unban must be durable");
        assert!(!store.is_channel_member("ops", "bob").await.unwrap(), "unban must not grant ACL membership");

        // The unbanned user can join the public channel again.
        let bob = attach_session(&hub, "bob");
        assert!(hub.join("bob", &bob, "ops", None).await.is_ok(), "an unbanned user may rejoin");
    }

    #[tokio::test]
    async fn bans_survive_a_server_restart() {
        let store = Store::open_in_memory().await.unwrap();
        store.create_channel("ops", Visibility::Public, "aaron").await.unwrap();

        // First server lifetime: aaron (the channel admin) bans bob.
        let hub = Hub::new(store.clone(), HashMap::new()).await.unwrap();
        let ack = hub
            .admin(
                "aaron",
                AdminOp::Ban {
                    channel: "ops".to_owned(),
                    user: "bob".to_owned(),
                },
            )
            .await
            .unwrap();
        assert!(matches!(ack, ProtocolMessage::Ack { .. }));
        drop(hub);

        // Second lifetime over the same durable store: the ban still holds.
        let hub = Hub::new(store, HashMap::new()).await.unwrap();
        let bob = attach_session(&hub, "bob");
        assert!(hub.join("bob", &bob, "ops", None).await.is_err(), "a persisted ban must survive a server restart");
    }

    #[tokio::test]
    async fn subscribe_re_checks_the_ban_atomically() {
        let store = Store::open_in_memory().await.unwrap();
        store.create_channel("ops", Visibility::Public, "aaron").await.unwrap();
        let hub = Hub::new(store, HashMap::new()).await.unwrap();
        let bob = attach_session(&hub, "bob");

        // A ban landing after join's early check (the TOCTOU window) is still enforced by the
        // re-check inside `subscribe`, under the same lock as the insert (#30).
        hub.add_ban("ops", "bob").await.unwrap();
        assert!(!hub.subscribe(&bob, "ops"), "subscribe must refuse a banned user");
        assert!(!hub.subscribers("ops").contains(&bob), "a banned user must not end up subscribed");
    }

    #[tokio::test]
    async fn invite_single_use_is_consumed_after_one_redeem() {
        let hub = hub_with_private_channel(Some(1), None).await;

        let david = attach_session(&hub, "david");
        hub.join("david", &david, "ops", Some("tok")).await.unwrap();
        assert!(hub.subscribers("ops").contains(&david), "redeeming a valid invite must subscribe + add to the ACL");

        // The spent single-use token cannot be redeemed again.
        let carol = attach_session(&hub, "carol");
        assert!(hub.join("carol", &carol, "ops", Some("tok")).await.is_err(), "a spent single-use invite must be refused");
    }

    #[tokio::test]
    async fn invite_multi_use_allows_several_then_exhausts() {
        let hub = hub_with_private_channel(Some(2), None).await;

        for user in ["david", "carol"] {
            let path = attach_session(&hub, user);
            hub.join(user, &path, "ops", Some("tok")).await.unwrap();
        }
        // The third redemption exhausts the token.
        let evan = attach_session(&hub, "evan");
        assert!(hub.join("evan", &evan, "ops", Some("tok")).await.is_err(), "an exhausted invite must be refused");
    }

    #[tokio::test]
    async fn invite_expiry_refuses_an_expired_token() {
        let hub = hub_with_private_channel(None, Some("2000-01-01T00:00:00+00:00".to_owned())).await;

        let david = attach_session(&hub, "david");
        assert!(hub.join("david", &david, "ops", Some("tok")).await.is_err(), "an expired token must be refused");
    }

    #[tokio::test]
    async fn invite_revoked_token_is_refused() {
        let hub = hub_with_private_channel(None, None).await;

        // The channel admin revokes the token.
        hub.admin("aaron", AdminOp::InviteRevoke { token: "tok".to_owned() }).await.unwrap();

        let david = attach_session(&hub, "david");
        assert!(hub.join("david", &david, "ops", Some("tok")).await.is_err(), "a revoked token must be refused");
    }

    #[tokio::test]
    async fn invite_wrong_channel_token_is_refused() {
        let hub = hub_with_private_channel(None, None).await;
        // A second private channel with no invite of its own.
        // (The `tok` invite is bound to `ops`, so it must not open `secret`.)
        let david = attach_session(&hub, "david");
        assert!(hub.join("david", &david, "ops", Some("nope")).await.is_err(), "an unknown token must be refused");
    }

    // PRD-0007 T-003 — concurrent redemption must not double-spend a single-use invite, and
    // concurrent joins must not clobber the ACL (finding #2).

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invite_single_use_admits_exactly_one_under_concurrent_redeem() {
        const RACERS: usize = 16;
        let hub = hub_with_private_channel(Some(1), None).await;

        let mut tasks = Vec::with_capacity(RACERS);
        for i in 0..RACERS {
            let user = format!("user{i}");
            let path = attach_session(&hub, &user);
            let hub = Arc::clone(&hub);
            tasks.push(tokio::spawn(async move { hub.join(&user, &path, "ops", Some("tok")).await.is_ok() }));
        }

        let mut admitted = 0;
        for task in tasks {
            if task.await.unwrap() {
                admitted += 1;
            }
        }
        assert_eq!(admitted, 1, "a single-use invite must admit exactly one redeemer under concurrency, admitted {admitted}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_joins_on_unlimited_invite_preserve_every_acl_entry() {
        const JOINERS: usize = 12;
        let hub = hub_with_private_channel(None, None).await;

        let mut tasks = Vec::with_capacity(JOINERS);
        for i in 0..JOINERS {
            let user = format!("user{i}");
            let path = attach_session(&hub, &user);
            let hub = Arc::clone(&hub);
            tasks.push(tokio::spawn(async move { hub.join(&user, &path, "ops", Some("tok")).await }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }

        // Membership must carry the creator plus every joiner — normalized rows, no lost writes.
        let members = hub.store.list_channel_members("ops").await.unwrap();
        assert_eq!(members.len(), JOINERS + 1, "concurrent joins must not lose a membership, got {members:?}");
    }

    // PRD-0007 T-004 — removing a user purges their memberships and created channels, so the freed
    // username cannot be re-registered to inherit private access or channel-admin rights (finding #6).

    #[tokio::test]
    async fn user_remove_purges_memberships_and_created_channels() {
        let store = Store::open_in_memory().await.unwrap();
        store.create_user("victim").await.unwrap();
        // The victim owns one channel and is a member of another they did not create.
        store.create_channel("victim-ops", Visibility::Private, "victim").await.unwrap();
        store.create_channel("lobby", Visibility::Public, "aaron").await.unwrap();
        store.add_channel_member("lobby", "victim").await.unwrap();
        let hub = Hub::new(store, HashMap::from([("root".to_owned(), None)])).await.unwrap();

        hub.admin("root", AdminOp::UserRemove { username: "victim".to_owned() }).await.unwrap();

        assert!(hub.store.get_channel("victim-ops").await.unwrap().is_none(), "a removed user's created channels must be deleted");
        assert!(!hub.store.is_channel_member("lobby", "victim").await.unwrap(), "a removed user's memberships must be purged");
    }

    // PRD-0007 T-005 — a wildly large invite expiry must return an error, not overflow-panic
    // (finding #10, reachable from the wire and the CLI's --expires-in).

    #[tokio::test]
    async fn invite_create_with_absurd_expiry_errors_instead_of_panicking() {
        let store = Store::open_in_memory().await.unwrap();
        store.create_channel("ops", Visibility::Private, "aaron").await.unwrap();
        let hub = Hub::new(store, HashMap::new()).await.unwrap();

        let result = hub
            .admin(
                "aaron",
                AdminOp::InviteCreate {
                    channel: "ops".to_owned(),
                    uses: None,
                    expires_in_secs: Some(u64::MAX),
                },
            )
            .await;
        assert!(result.is_err(), "an absurd expiry must return an error, got {result:?}");
    }
}
