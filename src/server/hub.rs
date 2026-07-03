//! The transport-agnostic server core: durable store + in-memory presence, subscriptions,
//! bans, and the fan-out router (DESIGN.md §13, §14).
//!
//! The [`Hub`] owns the single source of runtime truth. Durable identity / channel state lives in
//! the embedded [`Store`]; live presence, channel subscriptions, and channel bans are in-memory
//! maps keyed by full [`SessionPath`] (never persisted, DESIGN.md §15). Each live session registers
//! an outbound frame channel so the router can push channel fan-out and whispers to it. Every method
//! is transport-free — the session driver (see [`super::session`]) and the axum adapter (see
//! [`super::wss`]) both drive the same `Hub`, and the unit tests drive it directly.

use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use tokio::{
    sync::{Notify, mpsc},
    time::Instant,
};

use crate::{
    base::{SessionPath, Visibility},
    identity::{self, AuthError},
    protocol::{AdminOp, ChannelInfo, MachineInfo, Payload, ProtocolError, ProtocolMessage},
    store::{ChannelRecord, Store},
};

use super::AclError;

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
    kill: Arc<Notify>,
    /// The channels this session is currently subscribed to.
    channels: HashSet<String>,
    /// Last inbound activity, for the heartbeat reaper (DESIGN.md §10).
    last_seen: Instant,
}

/// The in-memory runtime state, guarded by a single mutex (short, await-free critical sections).
#[derive(Default)]
struct HubState {
    /// Live sessions keyed by full participant path (presence truth).
    sessions: HashMap<SessionPath, SessionEntry>,
    /// Channel → subscribed session paths (the fan-out index).
    subscriptions: HashMap<String, HashSet<SessionPath>>,
    /// Channel → banned usernames (in-memory in v1; resets on restart).
    bans: HashMap<String, HashSet<String>>,
}

/// The central relay's runtime: the embedded store, the server-admin allowlist, and live state.
pub(crate) struct Hub {
    store: Store,
    admins: super::AdminAllowlist,
    state: Mutex<HubState>,
}

impl Hub {
    /// Builds a shared hub over `store`, with `admins` as the server-wide admin allowlist (§7).
    pub(crate) fn new(store: Store, admins: super::AdminAllowlist) -> Arc<Self> {
        Arc::new(Self {
            store,
            admins,
            state: Mutex::new(HubState::default()),
        })
    }

    fn state(&self) -> MutexGuard<'_, HubState> {
        self.state.lock().expect("hub state mutex poisoned")
    }

    /// Whether `user` is a server-wide admin (on the serve-config allowlist, DESIGN.md §7).
    pub(crate) fn is_admin(&self, user: &str) -> bool {
        self.admins.contains_key(user)
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

    /// Registers a live session and returns its kill handle, rejecting a duplicate live path (§5).
    pub(crate) fn attach(&self, path: &SessionPath, user: &str, machine: &str, outbound: Outbound) -> Result<Arc<Notify>, ProtocolError> {
        let mut st = self.state();
        if st.sessions.contains_key(path) {
            return Err(AuthError::HandleCollision(path.session.clone()).into());
        }

        let kill = Arc::new(Notify::new());
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
        Ok(kill)
    }

    /// Removes a session's presence and subscriptions (on clean disconnect); idempotent.
    pub(crate) fn detach(&self, path: &SessionPath) {
        let mut st = self.state();
        Self::take_session(&mut st, path);
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
            Self::kill_locked(&mut st, path);
        }
        stale.len()
    }

    // -----------------------------------------------------------------------
    // Membership: join / leave / discovery (DESIGN.md §6).
    // -----------------------------------------------------------------------

    /// Joins a channel — authorized by visibility, ACL membership, or a redeemed invite token (§6).
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

        self.subscribe(path, channel);
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
        let infos = channels
            .into_iter()
            .filter_map(|c| {
                let visibility = parse_visibility(&c.visibility);
                let member = memberships.contains(&c.name);
                let visible = matches!(visibility, Visibility::Public) || member;
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
    pub(crate) fn post(&self, from: &SessionPath, channel: &str, payload: Payload) -> Result<(), ProtocolError> {
        let targets: Vec<(Arc<Notify>, Outbound)> = {
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

        let msg = ProtocolMessage::ChannelMsg {
            channel: channel.to_owned(),
            from: from.clone(),
            payload,
        };
        for (kill, tx) in targets {
            // A full (slow consumer) or closed queue: force-drop it rather than grow memory (#14);
            // a reconnect re-subscribes with fresh state.
            if tx.try_send(msg.clone()).is_err() {
                kill.notify_one();
            }
        }
        Ok(())
    }

    /// Delivers a whisper to exactly one live session path, erroring if it is not online (§8).
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
            kill.notify_one();
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Admin & moderation (DESIGN.md §7); revocation force-drops live state (§16).
    // -----------------------------------------------------------------------

    /// Authorizes and applies an admin / control operation, returning the response frame.
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
                self.remove_ban(&channel, &target);
                Ok(ack(target))
            }
            AdminOp::AclRemove { channel, user: target } => {
                self.authorize_channel_admin(&channel, user).await?;
                self.store.remove_channel_member(&channel, &target).await.map_err(internal)?;
                self.unsubscribe_user(&target, &channel);
                Ok(ack(target))
            }
            AdminOp::InviteCreate { channel, uses, expires_in_secs } => {
                self.authorize_channel_admin(&channel, user).await?;
                let token = identity::generate_token().map_err(internal)?;
                let expires_at = match expires_in_secs {
                    Some(secs) => Some(invite_expiry(secs)?),
                    None => None,
                };
                self.store.create_invite(&channel, &token, uses.map(i64::from), expires_at, user).await.map_err(internal)?;
                Ok(ProtocolMessage::InviteToken { token })
            }
            AdminOp::InviteRevoke { token } => {
                if let Some(invite) = self.store.get_invite(&token).await.map_err(internal)? {
                    self.authorize_channel_admin(&invite.channel, user).await?;
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
                self.add_ban(&channel, &target);
                self.unsubscribe_user(&target, &channel);
                Ok(ack(target))
            }
            AdminOp::UserRemove { username } => {
                if !self.is_admin(user) {
                    return Err(AclError::NotAdmin.into());
                }
                // Delete the channels this user created (cascading their memberships + invites) and
                // purge the user's memberships elsewhere, so re-registering the freed name cannot
                // inherit channel-admin rights or private access (finding #6).
                for channel in self.store.list_channels_created_by(&username).await.map_err(internal)? {
                    self.store.delete_channel(&channel).await.map_err(internal)?;
                    self.drop_channel(&channel);
                }
                self.store.delete_user_memberships(&username).await.map_err(internal)?;
                for machine in self.store.list_machines(&username).await.map_err(internal)? {
                    self.store.delete_machine(&username, &machine.name).await.map_err(internal)?;
                }
                self.store.delete_user(&username).await.map_err(internal)?;
                self.force_drop_user(&username);
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

    fn subscribe(&self, path: &SessionPath, channel: &str) {
        let mut st = self.state();
        let Some(entry) = st.sessions.get_mut(path) else {
            return;
        };
        entry.channels.insert(channel.to_owned());
        st.subscriptions.entry(channel.to_owned()).or_default().insert(path.clone());
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
            Self::kill_locked(&mut st, path);
        }
    }

    fn force_drop_machine(&self, user: &str, machine: &str) {
        let mut st = self.state();
        let paths: Vec<SessionPath> = st.sessions.iter().filter(|(_, e)| e.user == user && e.machine == machine).map(|(p, _)| p.clone()).collect();
        for path in &paths {
            Self::kill_locked(&mut st, path);
        }
    }

    fn is_banned(&self, channel: &str, user: &str) -> bool {
        self.state().bans.get(channel).is_some_and(|banned| banned.contains(user))
    }

    fn add_ban(&self, channel: &str, user: &str) {
        self.state().bans.entry(channel.to_owned()).or_default().insert(user.to_owned());
    }

    fn remove_ban(&self, channel: &str, user: &str) {
        if let Some(banned) = self.state().bans.get_mut(channel) {
            banned.remove(user);
        }
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
    fn kill_locked(st: &mut HubState, path: &SessionPath) {
        if let Some(entry) = Self::take_session(st, path) {
            entry.kill.notify_one();
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
        Hub::new(store, HashMap::new())
    }

    fn attach_session(hub: &Arc<Hub>, user: &str) -> SessionPath {
        let path = SessionPath::new(user, "machine", "session");
        // The outbound receiver is unused (these tests don't fan out); attach only needs the sender.
        let (tx, _rx) = mpsc::channel(super::super::session::OUTBOUND_CAPACITY);
        hub.attach(&path, user, "machine", tx).unwrap();
        path
    }

    #[tokio::test]
    async fn fanout_drops_a_slow_consumer_instead_of_growing_its_queue() {
        let store = Store::open_in_memory().await.unwrap();
        store.create_channel("ops", Visibility::Public, "aaron").await.unwrap();
        let hub = Hub::new(store, HashMap::new());

        // aaron posts; bob is a slow consumer with a tiny, never-drained outbound queue.
        let aaron = attach_session(&hub, "aaron");
        hub.join("aaron", &aaron, "ops", None).await.unwrap();

        let bob = SessionPath::new("bob", "machine", "session");
        let (b_tx, _b_rx) = mpsc::channel(1);
        let b_kill = hub.attach(&bob, "bob", "machine", b_tx).unwrap();
        hub.join("bob", &bob, "ops", None).await.unwrap();

        // Fill bob's 1-slot queue, then overflow it — the second post cannot enqueue.
        hub.post(&aaron, "ops", Payload::Plain("one".to_owned())).unwrap();
        hub.post(&aaron, "ops", Payload::Plain("two".to_owned())).unwrap();

        // The slow consumer is force-dropped (its kill fires) rather than buffering without bound.
        assert!(
            tokio::time::timeout(Duration::from_millis(500), b_kill.notified()).await.is_ok(),
            "a consumer that fills its bounded queue must be force-dropped",
        );
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
        let hub = Hub::new(store, HashMap::from([("root".to_owned(), None)]));

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
        let hub = Hub::new(store, HashMap::new());

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
