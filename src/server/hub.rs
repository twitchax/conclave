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
    protocol::{AdminOp, ChannelInfo, Payload, ProtocolError, ProtocolMessage},
    store::{ChannelRecord, Store},
};

use super::AclError;

/// The outbound half of a live session: the router pushes frames here and the session's writer
/// task drains them to the transport.
type Outbound = mpsc::UnboundedSender<ProtocolMessage>;

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
    admins: HashSet<String>,
    state: Mutex<HubState>,
}

impl Hub {
    /// Builds a shared hub over `store`, with `admins` as the server-wide admin allowlist (§7).
    pub(crate) fn new(store: Store, admins: HashSet<String>) -> Arc<Self> {
        Arc::new(Self {
            store,
            admins,
            state: Mutex::new(HubState::default()),
        })
    }

    fn state(&self) -> MutexGuard<'_, HubState> {
        self.state.lock().expect("hub state mutex poisoned")
    }

    fn is_admin(&self, user: &str) -> bool {
        self.admins.contains(user)
    }

    // -----------------------------------------------------------------------
    // Registration & authentication (the handshake, DESIGN.md §5).
    // -----------------------------------------------------------------------

    /// Claims a username and enrolls the calling machine as its first key (self-authorizing, §5.1).
    pub(crate) async fn register(&self, username: &str, machine: &str, pubkey: &[u8]) -> Result<(), ProtocolError> {
        if self.store.get_user(username).await.map_err(internal)?.is_some() {
            return Err(AuthError::UsernameTaken(username.to_owned()).into());
        }

        let pubkey_b64 = identity::encode_key(pubkey);
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
        let mut record = self
            .store
            .get_channel(channel)
            .await
            .map_err(internal)?
            .ok_or_else(|| ProtocolError::NotFound(format!("channel `{channel}`")))?;

        if self.is_banned(channel, user) {
            return Err(AclError::ChannelPrivate(channel.to_owned()).into());
        }

        let already_member = record.acl.iter().any(|u| u == user);
        if !already_member {
            match parse_visibility(&record.visibility) {
                // Public and unlisted are open to anyone who reaches them (unlisted needs the name).
                Visibility::Public | Visibility::Unlisted => {}
                // Private requires an ACL entry (absent here) or a valid invite that grants one.
                Visibility::Private => {
                    let token = token.ok_or_else(|| ProtocolError::from(AclError::ChannelPrivate(channel.to_owned())))?;
                    self.redeem_invite(channel, token).await?;
                    record.acl.push(user.to_owned());
                    self.store.set_channel_acl(channel, &record.acl).await.map_err(internal)?;
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
        let infos = channels
            .into_iter()
            .filter_map(|c| {
                let visibility = parse_visibility(&c.visibility);
                let member = c.acl.iter().any(|u| u == user);
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

        let record = self
            .store
            .get_channel(channel)
            .await
            .map_err(internal)?
            .ok_or_else(|| ProtocolError::from(AclError::ChannelNotFound(channel.to_owned())))?;
        if !record.acl.iter().any(|u| u == user) && !self.is_admin(user) {
            return Err(AclError::NotMember(channel.to_owned()).into());
        }

        let st = self.state();
        Ok(st.subscriptions.get(channel).map(|subs| subs.iter().cloned().collect()).unwrap_or_default())
    }

    // -----------------------------------------------------------------------
    // Routing: channel fan-out and whispers (DESIGN.md §8, §14).
    // -----------------------------------------------------------------------

    /// Fans a channel message out to every *other* subscribed session; the sender must be a member.
    pub(crate) fn post(&self, from: &SessionPath, channel: &str, payload: Payload) -> Result<(), ProtocolError> {
        let targets: Vec<Outbound> = {
            let st = self.state();
            let subs = st.subscriptions.get(channel).ok_or_else(|| ProtocolError::from(AclError::NotMember(channel.to_owned())))?;
            if !subs.contains(from) {
                return Err(AclError::NotMember(channel.to_owned()).into());
            }
            subs.iter().filter(|p| *p != from).filter_map(|p| st.sessions.get(p).map(|e| e.outbound.clone())).collect()
        };

        let msg = ProtocolMessage::ChannelMsg {
            channel: channel.to_owned(),
            from: from.clone(),
            payload,
        };
        for tx in targets {
            let _ = tx.send(msg.clone());
        }
        Ok(())
    }

    /// Delivers a whisper to exactly one live session path, erroring if it is not online (§8).
    pub(crate) fn whisper(&self, from: &SessionPath, target: &SessionPath, payload: Payload) -> Result<(), ProtocolError> {
        let outbound = self.state().sessions.get(target).map(|e| e.outbound.clone());
        let Some(outbound) = outbound else {
            return Err(ProtocolError::NotFound(format!("session `{target}` is not online")));
        };

        let msg = ProtocolMessage::Whisper {
            from: from.clone(),
            target: target.clone(),
            payload,
        };
        let _ = outbound.send(msg);
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
                let mut record = self.authorize_channel_admin(&channel, user).await?;
                if !record.acl.iter().any(|u| u == &target) {
                    record.acl.push(target.clone());
                    self.store.set_channel_acl(&channel, &record.acl).await.map_err(internal)?;
                }
                self.remove_ban(&channel, &target);
                Ok(ack(target))
            }
            AdminOp::AclRemove { channel, user: target } => {
                let mut record = self.authorize_channel_admin(&channel, user).await?;
                record.acl.retain(|u| u != &target);
                self.store.set_channel_acl(&channel, &record.acl).await.map_err(internal)?;
                self.unsubscribe_user(&target, &channel);
                Ok(ack(target))
            }
            AdminOp::InviteCreate { channel, uses, expires_in_secs } => {
                self.authorize_channel_admin(&channel, user).await?;
                let token = identity::generate_token().map_err(internal)?;
                let expires_at = expires_in_secs.map(|secs| {
                    let secs = i64::try_from(secs).unwrap_or(i64::MAX);
                    (chrono::Utc::now() + chrono::Duration::seconds(secs)).to_rfc3339()
                });
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
                let mut record = self.authorize_channel_admin(&channel, user).await?;
                record.acl.retain(|u| u != &target);
                self.store.set_channel_acl(&channel, &record.acl).await.map_err(internal)?;
                self.add_ban(&channel, &target);
                self.unsubscribe_user(&target, &channel);
                Ok(ack(target))
            }
            AdminOp::UserRemove { username } => {
                if !self.is_admin(user) {
                    return Err(AclError::NotAdmin.into());
                }
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
            // Self-service enrollment of a new machine key; it proves possession on first connect.
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
        let invite = self
            .store
            .get_invite(token)
            .await
            .map_err(internal)?
            .filter(|inv| inv.channel == channel)
            .ok_or_else(|| ProtocolError::from(AclError::ChannelPrivate(channel.to_owned())))?;

        if invite.expires_at.as_deref().is_some_and(is_expired) {
            self.store.delete_invite(token).await.map_err(internal)?;
            return Err(AclError::ChannelPrivate(channel.to_owned()).into());
        }

        match invite.uses_remaining {
            Some(remaining) if remaining <= 1 => self.store.delete_invite(token).await.map_err(internal)?,
            Some(remaining) => self.store.set_invite_uses(token, remaining - 1).await.map_err(internal)?,
            None => {}
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
