//! Embedded `SurrealDB` store: durable config only, behind a thin per-table repository.
//!
//! The store is `SurrealDB` **embedded** — the official SDK with a local KV backend, so
//! `conclave serve` stays a single self-contained binary with a data directory and no external DB
//! process (DESIGN.md §15). [`Store::open`] uses the pure-Rust `SurrealKV` backend for persistence;
//! [`Store::open_in_memory`] backs hermetic tests.
//!
//! There is no ORM: storage records are the SDK's own typed layer (`SurrealValue`), and this module
//! maps between them and the domain types. Only durable config lives here (`user`, `machine`,
//! `channel`, `invite`, with the uniqueness constraints from DESIGN.md §15); presence,
//! subscriptions, permission levels, and the admin allowlist are deliberately not persisted.

use std::path::Path;

use anyhow::Context as _;
use surrealdb::{
    Surreal,
    engine::local::{Db, Mem, SurrealKv},
    types::{SurrealValue, Value},
};

use crate::base::{Res, Visibility, Void};

/// The single namespace / database the embedded store uses.
const NAMESPACE: &str = "conclave";
const DATABASE: &str = "conclave";

/// Schema definition run at open: the uniqueness constraints from DESIGN.md §15. `IF NOT EXISTS`
/// keeps re-opening a persistent store idempotent.
const SCHEMA: &str = "\
DEFINE INDEX IF NOT EXISTS user_username ON user FIELDS username UNIQUE;
DEFINE INDEX IF NOT EXISTS machine_pubkey ON machine FIELDS pubkey UNIQUE;
DEFINE INDEX IF NOT EXISTS machine_user_name ON machine FIELDS user, name UNIQUE;
DEFINE INDEX IF NOT EXISTS channel_name ON channel FIELDS name UNIQUE;
DEFINE INDEX IF NOT EXISTS invite_token ON invite FIELDS token UNIQUE;
DEFINE INDEX IF NOT EXISTS membership_channel_user ON membership FIELDS channel, user UNIQUE;
";

/// A registered account (`username` unique per server, DESIGN.md §15).
#[derive(Debug, Clone, PartialEq, Eq, SurrealValue)]
pub struct UserRecord {
    /// The account name.
    pub username: String,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
}

/// An enrolled machine keypair under a user (`pubkey` globally unique; `name` unique within the
/// user, DESIGN.md §5, §15).
#[derive(Debug, Clone, PartialEq, Eq, SurrealValue)]
pub struct MachineRecord {
    /// The owning username.
    pub user: String,
    /// The machine name (unique within the user).
    pub name: String,
    /// The machine's public key, base64-encoded (globally unique).
    pub pubkey: String,
    /// RFC 3339 enrollment timestamp.
    pub added_at: String,
}

/// A channel (`name` unique, DESIGN.md §6, §15). Membership (the ACL) is normalized into the
/// `membership` table rather than an embedded array, so concurrent joins insert distinct records
/// instead of contending on one row (PRD-0007 T-003).
#[derive(Debug, Clone, PartialEq, Eq, SurrealValue)]
pub struct ChannelRecord {
    /// The channel name.
    pub name: String,
    /// The visibility tier token (see [`Visibility::as_str`]).
    pub visibility: String,
    /// The creating (and administering) user.
    pub created_by: String,
    /// RFC 3339 creation timestamp.
    pub created_at: String,
}

/// An invite token for a channel (`token` unique, DESIGN.md §6, §15).
#[derive(Debug, Clone, PartialEq, Eq, SurrealValue)]
pub struct InviteRecord {
    /// The channel the token grants access to.
    pub channel: String,
    /// The opaque token string.
    pub token: String,
    /// Remaining redemptions, or unlimited if absent.
    pub uses_remaining: Option<i64>,
    /// RFC 3339 expiry, or non-expiring if absent.
    pub expires_at: Option<String>,
    /// The creating user.
    pub created_by: String,
}

// Query variable bindings (SurrealDB 3.x binds a `SurrealValue` object of variables).

#[derive(SurrealValue)]
struct ByUsername {
    username: String,
}

#[derive(SurrealValue)]
struct ByPubkey {
    pubkey: String,
}

#[derive(SurrealValue)]
struct ByUser {
    user: String,
}

#[derive(SurrealValue)]
struct ByName {
    name: String,
}

#[derive(SurrealValue)]
struct ByToken {
    // `token` is a protected variable name in SurrealQL, so bind under `tok`.
    tok: String,
}

#[derive(SurrealValue)]
struct ByUserAndName {
    user: String,
    name: String,
}

#[derive(SurrealValue)]
struct SetVisibility {
    name: String,
    visibility: String,
}

#[derive(SurrealValue)]
struct Rename {
    old: String,
    new: String,
}

#[derive(SurrealValue)]
struct SetUses {
    // `token` is a protected variable name in SurrealQL, so bind under `tok`.
    tok: String,
    uses: i64,
}

#[derive(SurrealValue)]
struct Membership {
    channel: String,
    user: String,
}

#[derive(SurrealValue)]
struct ByChannel {
    channel: String,
}

/// A bounded cap on optimistic-concurrency retries: high enough to clear realistic contention on a
/// single channel record, low enough that a genuinely stuck write still surfaces (DESIGN.md §15).
const MAX_WRITE_ATTEMPTS: usize = 64;

/// Whether a `SurrealDB` error is an optimistic-concurrency write-write conflict (`SurrealKV`
/// surfaces `TransactionWriteConflict`; `SurrealDB` maps it to `TransactionConflict`, both rendering
/// with "conflict"). These are expected under concurrent load and must be retried per `SurrealDB`'s
/// optimistic-concurrency contract — the loser of a same-key write re-applies its statement — rather
/// than serialized behind an application lock (DESIGN.md §15).
fn is_write_conflict(err: &surrealdb::Error) -> bool {
    err.to_string().to_lowercase().contains("conflict")
}

/// The embedded store: a thin typed repository over an embedded `SurrealDB` instance.
pub struct Store {
    db: Surreal<Db>,
}

impl Store {
    /// Opens (or creates) a persistent store rooted at `path` using the `SurrealKV` backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot be opened or the schema cannot be applied.
    pub async fn open(path: &Path) -> Res<Self> {
        let db = Surreal::new::<SurrealKv>(path.to_string_lossy().as_ref()).await.context("failed to open the embedded store")?;
        Self::init(db).await
    }

    /// Opens an ephemeral in-memory store (for tests).
    ///
    /// # Errors
    ///
    /// Returns an error if the in-memory backend cannot be initialized.
    pub async fn open_in_memory() -> Res<Self> {
        let db = Surreal::new::<Mem>(()).await.context("failed to open the in-memory store")?;
        Self::init(db).await
    }

    async fn init(db: Surreal<Db>) -> Res<Self> {
        db.use_ns(NAMESPACE).use_db(DATABASE).await.context("failed to select namespace/database")?;
        db.query(SCHEMA).await.context("failed to apply schema")?.check().context("schema application reported an error")?;
        Ok(Self { db })
    }

    async fn insert<T: SurrealValue>(&self, table: &str, record: T) -> Void {
        let _created: Option<Value> = self.db.create(table.to_owned()).content(record).await.with_context(|| format!("failed to insert into `{table}`"))?;
        Ok(())
    }

    /// Creates a user, enforcing the unique-username constraint.
    ///
    /// # Errors
    ///
    /// Returns an error if the username is already taken or the write fails.
    pub async fn create_user(&self, username: &str) -> Res<UserRecord> {
        let record = UserRecord {
            username: username.to_owned(),
            created_at: now_rfc3339(),
        };
        self.insert("user", record.clone()).await?;
        Ok(record)
    }

    /// Fetches a user by username.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_user(&self, username: &str) -> Res<Option<UserRecord>> {
        let mut response = self
            .db
            .query("SELECT * OMIT id FROM user WHERE username = $username")
            .bind(ByUsername { username: username.to_owned() })
            .await
            .context("failed to query user")?;
        let rows: Vec<UserRecord> = response.take(0).context("failed to decode user rows")?;
        Ok(rows.into_iter().next())
    }

    /// Enrolls a machine, enforcing the globally-unique pubkey and per-user-unique name constraints.
    ///
    /// # Errors
    ///
    /// Returns an error if the pubkey is already enrolled, the name collides within the user, or the
    /// write fails.
    pub async fn create_machine(&self, user: &str, name: &str, pubkey_base64: &str) -> Res<MachineRecord> {
        let record = MachineRecord {
            user: user.to_owned(),
            name: name.to_owned(),
            pubkey: pubkey_base64.to_owned(),
            added_at: now_rfc3339(),
        };
        self.insert("machine", record.clone()).await?;
        Ok(record)
    }

    /// Fetches a machine by its base64 public key.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_machine_by_pubkey(&self, pubkey_base64: &str) -> Res<Option<MachineRecord>> {
        let mut response = self
            .db
            .query("SELECT * OMIT id FROM machine WHERE pubkey = $pubkey")
            .bind(ByPubkey { pubkey: pubkey_base64.to_owned() })
            .await
            .context("failed to query machine")?;
        let rows: Vec<MachineRecord> = response.take(0).context("failed to decode machine rows")?;
        Ok(rows.into_iter().next())
    }

    /// Lists the machines enrolled under a user.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn list_machines(&self, user: &str) -> Res<Vec<MachineRecord>> {
        let mut response = self
            .db
            .query("SELECT * OMIT id FROM machine WHERE user = $user")
            .bind(ByUser { user: user.to_owned() })
            .await
            .context("failed to list machines")?;
        response.take(0).context("failed to decode machine rows")
    }

    /// Revokes a machine by `(user, name)`.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete fails.
    pub async fn delete_machine(&self, user: &str, name: &str) -> Void {
        self.db
            .query("DELETE machine WHERE user = $user AND name = $name")
            .bind(ByUserAndName {
                user: user.to_owned(),
                name: name.to_owned(),
            })
            .await
            .context("failed to delete machine")?
            .check()
            .context("machine delete reported an error")?;
        Ok(())
    }

    /// Creates a channel, enforcing the unique-name constraint.
    ///
    /// # Errors
    ///
    /// Returns an error if the name is already taken or the write fails.
    pub async fn create_channel(&self, name: &str, visibility: Visibility, created_by: &str) -> Res<ChannelRecord> {
        let record = ChannelRecord {
            name: name.to_owned(),
            visibility: visibility.as_str().to_owned(),
            created_by: created_by.to_owned(),
            created_at: now_rfc3339(),
        };
        self.insert("channel", record.clone()).await?;
        // The creator is the channel's first member (it also administers via `created_by`).
        self.add_channel_member(name, created_by).await?;
        Ok(record)
    }

    /// Fetches a channel by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_channel(&self, name: &str) -> Res<Option<ChannelRecord>> {
        let mut response = self
            .db
            .query("SELECT * OMIT id FROM channel WHERE name = $name")
            .bind(ByName { name: name.to_owned() })
            .await
            .context("failed to query channel")?;
        let rows: Vec<ChannelRecord> = response.take(0).context("failed to decode channel rows")?;
        Ok(rows.into_iter().next())
    }

    /// Creates an invite token, enforcing the unique-token constraint.
    ///
    /// # Errors
    ///
    /// Returns an error if the token already exists or the write fails.
    pub async fn create_invite(&self, channel: &str, token: &str, uses_remaining: Option<i64>, expires_at: Option<String>, created_by: &str) -> Res<InviteRecord> {
        let record = InviteRecord {
            channel: channel.to_owned(),
            token: token.to_owned(),
            uses_remaining,
            expires_at,
            created_by: created_by.to_owned(),
        };
        self.insert("invite", record.clone()).await?;
        Ok(record)
    }

    /// Fetches an invite by token.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_invite(&self, token: &str) -> Res<Option<InviteRecord>> {
        let mut response = self
            .db
            .query("SELECT * OMIT id FROM invite WHERE token = $tok")
            .bind(ByToken { tok: token.to_owned() })
            .await
            .context("failed to query invite")?;
        let rows: Vec<InviteRecord> = response.take(0).context("failed to decode invite rows")?;
        Ok(rows.into_iter().next())
    }

    /// Lists every channel; the caller applies visibility / membership gating (DESIGN.md §6).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn list_channels(&self) -> Res<Vec<ChannelRecord>> {
        let mut response = self.db.query("SELECT * OMIT id FROM channel").await.context("failed to list channels")?;
        response.take(0).context("failed to decode channel rows")
    }

    /// Adds `user` to a channel's membership (its ACL), idempotently. Each membership is its own
    /// record under the unique `(channel, user)` index, so concurrent adds of different users write
    /// distinct keys and never contend on a shared row (PRD-0007 T-003); a conflict on the same pair
    /// is retried per `SurrealDB`'s optimistic-concurrency contract.
    ///
    /// # Errors
    ///
    /// Returns an error if the write keeps conflicting past the retry cap or otherwise fails.
    pub async fn add_channel_member(&self, channel: &str, user: &str) -> Void {
        for attempt in 0..MAX_WRITE_ATTEMPTS {
            let outcome = self
                .db
                .query("INSERT INTO membership { channel: $channel, user: $user } ON DUPLICATE KEY UPDATE channel = $channel")
                .bind(Membership {
                    channel: channel.to_owned(),
                    user: user.to_owned(),
                })
                .await
                .and_then(surrealdb::IndexedResults::check);
            match outcome {
                Ok(_) => return Ok(()),
                Err(e) if is_write_conflict(&e) && attempt + 1 < MAX_WRITE_ATTEMPTS => tokio::task::yield_now().await,
                Err(e) => return Err(anyhow::Error::new(e).context("failed to add channel member")),
            }
        }
        anyhow::bail!("adding a channel member exhausted {MAX_WRITE_ATTEMPTS} write-conflict retries")
    }

    /// Removes `user` from a channel's membership; idempotent (removing a non-member is a no-op).
    ///
    /// # Errors
    ///
    /// Returns an error if the delete keeps conflicting past the retry cap or otherwise fails.
    pub async fn remove_channel_member(&self, channel: &str, user: &str) -> Void {
        for attempt in 0..MAX_WRITE_ATTEMPTS {
            let outcome = self
                .db
                .query("DELETE membership WHERE channel = $channel AND user = $user")
                .bind(Membership {
                    channel: channel.to_owned(),
                    user: user.to_owned(),
                })
                .await
                .and_then(surrealdb::IndexedResults::check);
            match outcome {
                Ok(_) => return Ok(()),
                Err(e) if is_write_conflict(&e) && attempt + 1 < MAX_WRITE_ATTEMPTS => tokio::task::yield_now().await,
                Err(e) => return Err(anyhow::Error::new(e).context("failed to remove channel member")),
            }
        }
        anyhow::bail!("removing a channel member exhausted {MAX_WRITE_ATTEMPTS} write-conflict retries")
    }

    /// Whether `user` is a member of `channel`.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn is_channel_member(&self, channel: &str, user: &str) -> Res<bool> {
        let mut response = self
            .db
            .query("SELECT VALUE user FROM membership WHERE channel = $channel AND user = $user")
            .bind(Membership {
                channel: channel.to_owned(),
                user: user.to_owned(),
            })
            .await
            .context("failed to query membership")?;
        let rows: Vec<String> = response.take(0).context("failed to decode membership rows")?;
        Ok(!rows.is_empty())
    }

    /// The channels `user` is a member of (for discovery gating, DESIGN.md §6).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn list_user_memberships(&self, user: &str) -> Res<Vec<String>> {
        let mut response = self
            .db
            .query("SELECT VALUE channel FROM membership WHERE user = $user")
            .bind(ByUser { user: user.to_owned() })
            .await
            .context("failed to list user memberships")?;
        response.take(0).context("failed to decode membership channels")
    }

    /// The members of a channel (its ACL users).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn list_channel_members(&self, channel: &str) -> Res<Vec<String>> {
        let mut response = self
            .db
            .query("SELECT VALUE user FROM membership WHERE channel = $channel")
            .bind(ByChannel { channel: channel.to_owned() })
            .await
            .context("failed to list channel members")?;
        response.take(0).context("failed to decode membership users")
    }

    /// The names of the channels created (and administered) by `user`.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn list_channels_created_by(&self, user: &str) -> Res<Vec<String>> {
        let mut response = self
            .db
            .query("SELECT VALUE name FROM channel WHERE created_by = $user")
            .bind(ByUser { user: user.to_owned() })
            .await
            .context("failed to list created channels")?;
        response.take(0).context("failed to decode channel names")
    }

    /// Removes every membership held by `user` (used when a user is removed, DESIGN.md §7).
    ///
    /// # Errors
    ///
    /// Returns an error if the delete fails.
    pub async fn delete_user_memberships(&self, user: &str) -> Void {
        self.db
            .query("DELETE membership WHERE user = $user")
            .bind(ByUser { user: user.to_owned() })
            .await
            .context("failed to delete user memberships")?
            .check()
            .context("user membership delete reported an error")?;
        Ok(())
    }

    /// Changes a channel's visibility tier.
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn set_channel_visibility(&self, name: &str, visibility: Visibility) -> Void {
        self.db
            .query("UPDATE channel SET visibility = $visibility WHERE name = $name")
            .bind(SetVisibility {
                name: name.to_owned(),
                visibility: visibility.as_str().to_owned(),
            })
            .await
            .context("failed to update channel visibility")?
            .check()
            .context("channel visibility update reported an error")?;
        Ok(())
    }

    /// Renames a channel, enforcing the unique-name constraint on the new name.
    ///
    /// # Errors
    ///
    /// Returns an error if the new name is already taken or the update fails.
    pub async fn rename_channel(&self, old: &str, new: &str) -> Void {
        self.db
            .query("UPDATE channel SET name = $new WHERE name = $old")
            .bind(Rename { old: old.to_owned(), new: new.to_owned() })
            .await
            .context("failed to rename channel")?
            .check()
            .context("channel rename reported an error")?;
        // Keep memberships and invites attached to the renamed channel (PRD-0007 T-004).
        self.db
            .query("UPDATE membership SET channel = $new WHERE channel = $old")
            .bind(Rename { old: old.to_owned(), new: new.to_owned() })
            .await
            .context("failed to migrate channel memberships")?
            .check()
            .context("membership rename reported an error")?;
        self.db
            .query("UPDATE invite SET channel = $new WHERE channel = $old")
            .bind(Rename { old: old.to_owned(), new: new.to_owned() })
            .await
            .context("failed to migrate channel invites")?
            .check()
            .context("invite rename reported an error")?;
        Ok(())
    }

    /// Deletes a channel.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete fails.
    pub async fn delete_channel(&self, name: &str) -> Void {
        self.db
            .query("DELETE channel WHERE name = $name")
            .bind(ByName { name: name.to_owned() })
            .await
            .context("failed to delete channel")?
            .check()
            .context("channel delete reported an error")?;
        // Drop the channel's memberships and invites so a future same-named channel cannot inherit
        // them (invite cascade — PRD-0007 T-004, finding #5).
        self.db
            .query("DELETE membership WHERE channel = $channel")
            .bind(ByChannel { channel: name.to_owned() })
            .await
            .context("failed to delete channel memberships")?
            .check()
            .context("membership delete reported an error")?;
        self.db
            .query("DELETE invite WHERE channel = $channel")
            .bind(ByChannel { channel: name.to_owned() })
            .await
            .context("failed to delete channel invites")?
            .check()
            .context("invite delete reported an error")?;
        Ok(())
    }

    /// Sets an invite's remaining redemptions (used when redeeming a limited-use token).
    ///
    /// # Errors
    ///
    /// Returns an error if the update fails.
    pub async fn set_invite_uses(&self, token: &str, uses_remaining: i64) -> Void {
        self.db
            .query("UPDATE invite SET uses_remaining = $uses WHERE token = $tok")
            .bind(SetUses {
                tok: token.to_owned(),
                uses: uses_remaining,
            })
            .await
            .context("failed to update invite uses")?
            .check()
            .context("invite uses update reported an error")?;
        Ok(())
    }

    /// Atomically consumes one redemption of a limited-use invite: decrements `uses_remaining` only
    /// while it is positive, returning whether a use was claimed. The guarded single-statement update
    /// (retried on an optimistic-concurrency conflict) makes concurrent redeemers of the last use
    /// mutually exclusive, so a single-use token admits exactly one (PRD-0007 T-003). The caller
    /// handles unlimited (`None`) tokens and expiry; an exhausted token is deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if the update keeps conflicting past the retry cap or otherwise fails.
    pub async fn try_consume_invite_use(&self, token: &str) -> Res<bool> {
        for attempt in 0..MAX_WRITE_ATTEMPTS {
            let outcome = self
                .db
                .query("UPDATE invite SET uses_remaining = uses_remaining - 1 WHERE token = $tok AND uses_remaining > 0 RETURN VALUE uses_remaining")
                .bind(ByToken { tok: token.to_owned() })
                .await
                .and_then(surrealdb::IndexedResults::check);
            match outcome {
                Ok(mut response) => {
                    let remaining: Vec<i64> = response.take(0).context("failed to decode invite uses")?;
                    return match remaining.into_iter().next() {
                        // No positive-use row matched — the token was already spent (or removed).
                        None => Ok(false),
                        // This redemption took the last use; delete the spent token.
                        Some(0) => {
                            self.delete_invite(token).await?;
                            Ok(true)
                        }
                        // A use was consumed with more remaining.
                        Some(_) => Ok(true),
                    };
                }
                Err(e) if is_write_conflict(&e) && attempt + 1 < MAX_WRITE_ATTEMPTS => tokio::task::yield_now().await,
                Err(e) => return Err(anyhow::Error::new(e).context("failed to consume invite use")),
            }
        }
        anyhow::bail!("consuming an invite use exhausted {MAX_WRITE_ATTEMPTS} write-conflict retries")
    }

    /// Deletes an invite token (on revoke or when an exhausted token is redeemed).
    ///
    /// # Errors
    ///
    /// Returns an error if the delete fails.
    pub async fn delete_invite(&self, token: &str) -> Void {
        self.db
            .query("DELETE invite WHERE token = $tok")
            .bind(ByToken { tok: token.to_owned() })
            .await
            .context("failed to delete invite")?
            .check()
            .context("invite delete reported an error")?;
        Ok(())
    }

    /// Lists every registered user (server-admin `user list`).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn list_users(&self) -> Res<Vec<UserRecord>> {
        let mut response = self.db.query("SELECT * OMIT id FROM user").await.context("failed to list users")?;
        response.take(0).context("failed to decode user rows")
    }

    /// Deletes a user (server-admin `user remove`); the caller also revokes the user's machines.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete fails.
    pub async fn delete_user(&self, username: &str) -> Void {
        self.db
            .query("DELETE user WHERE username = $username")
            .bind(ByUsername { username: username.to_owned() })
            .await
            .context("failed to delete user")?
            .check()
            .context("user delete reported an error")?;
        Ok(())
    }
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339()
}

#[cfg(test)]
mod tests {
    // Tests relax `unwrap_used` (house convention; DESIGN.md §22).
    #![allow(clippy::unwrap_used)]

    use super::*;
    use pretty_assertions::assert_eq;

    async fn store() -> Store {
        Store::open_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn user_create_and_fetch_round_trip() {
        let store = store().await;
        let created = store.create_user("aaron").await.unwrap();

        assert_eq!(store.get_user("aaron").await.unwrap(), Some(created));
        assert_eq!(store.get_user("nobody").await.unwrap(), None);
    }

    #[tokio::test]
    async fn duplicate_username_is_rejected() {
        let store = store().await;
        store.create_user("aaron").await.unwrap();

        assert!(store.create_user("aaron").await.is_err(), "the unique-username constraint must reject a duplicate");
    }

    #[tokio::test]
    async fn machine_pubkey_is_globally_unique() {
        let store = store().await;
        store.create_machine("aaron", "workstation", "PUBKEY-A").await.unwrap();

        // Same pubkey under a different user/name must still be rejected.
        assert!(store.create_machine("david", "desktop", "PUBKEY-A").await.is_err());
    }

    #[tokio::test]
    async fn machine_name_is_unique_within_a_user_but_not_across_users() {
        let store = store().await;
        store.create_machine("aaron", "workstation", "PUBKEY-A").await.unwrap();

        // Same name, same user, different key -> rejected.
        assert!(store.create_machine("aaron", "workstation", "PUBKEY-B").await.is_err());
        // Same name under a different user -> allowed.
        store.create_machine("david", "workstation", "PUBKEY-C").await.unwrap();
    }

    #[tokio::test]
    async fn machines_list_and_delete_for_a_user() {
        let store = store().await;
        store.create_machine("aaron", "workstation", "PUBKEY-A").await.unwrap();
        store.create_machine("aaron", "sno-box", "PUBKEY-B").await.unwrap();

        assert_eq!(store.list_machines("aaron").await.unwrap().len(), 2);

        store.delete_machine("aaron", "sno-box").await.unwrap();
        let remaining = store.list_machines("aaron").await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].name, "workstation");
    }

    #[tokio::test]
    async fn channel_create_fetch_and_unique_name() {
        let store = store().await;
        let created = store.create_channel("ops", Visibility::Private, "aaron").await.unwrap();

        assert_eq!(created.visibility, "private");
        assert_eq!(store.get_channel("ops").await.unwrap(), Some(created));
        assert!(store.create_channel("ops", Visibility::Public, "david").await.is_err());
    }

    #[tokio::test]
    async fn invite_create_fetch_and_unique_token() {
        let store = store().await;
        let created = store.create_invite("ops", "tok-123", Some(5), None, "aaron").await.unwrap();

        assert_eq!(store.get_invite("tok-123").await.unwrap(), Some(created));
        assert!(store.create_invite("ops", "tok-123", None, None, "aaron").await.is_err());
    }

    #[tokio::test]
    async fn channel_membership_add_remove_and_list() {
        let store = store().await;
        store.create_channel("ops", Visibility::Private, "aaron").await.unwrap();
        // The creator is seeded as the first member.
        assert!(store.is_channel_member("ops", "aaron").await.unwrap());

        store.add_channel_member("ops", "david").await.unwrap();
        // Idempotent: re-adding an existing member is a no-op, not a duplicate or an error.
        store.add_channel_member("ops", "david").await.unwrap();
        assert!(store.is_channel_member("ops", "david").await.unwrap());

        let mut members = store.list_channel_members("ops").await.unwrap();
        members.sort();
        assert_eq!(members, vec!["aaron".to_owned(), "david".to_owned()]);
        assert_eq!(store.list_user_memberships("david").await.unwrap(), vec!["ops".to_owned()]);

        store.remove_channel_member("ops", "david").await.unwrap();
        assert!(!store.is_channel_member("ops", "david").await.unwrap());
    }

    #[tokio::test]
    async fn channel_memberships_follow_delete_and_rename() {
        let store = store().await;
        store.create_channel("ops", Visibility::Private, "aaron").await.unwrap();
        store.add_channel_member("ops", "david").await.unwrap();

        // Rename migrates memberships to the new channel name.
        store.rename_channel("ops", "operations").await.unwrap();
        assert!(store.is_channel_member("operations", "david").await.unwrap());
        assert!(!store.is_channel_member("ops", "david").await.unwrap());

        // Deleting a channel drops its memberships, so a future same-named channel cannot inherit them.
        store.delete_channel("operations").await.unwrap();
        assert!(store.list_channel_members("operations").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn channel_visibility_can_be_changed() {
        let store = store().await;
        store.create_channel("ops", Visibility::Private, "aaron").await.unwrap();

        store.set_channel_visibility("ops", Visibility::Public).await.unwrap();

        assert_eq!(store.get_channel("ops").await.unwrap().unwrap().visibility, "public");
    }

    #[tokio::test]
    async fn channel_rename_moves_the_record_and_respects_uniqueness() {
        let store = store().await;
        store.create_channel("ops", Visibility::Private, "aaron").await.unwrap();
        store.create_channel("taken", Visibility::Public, "aaron").await.unwrap();

        store.rename_channel("ops", "operations").await.unwrap();
        assert!(store.get_channel("ops").await.unwrap().is_none());
        assert!(store.get_channel("operations").await.unwrap().is_some());

        // Renaming onto an existing name is rejected by the unique index.
        assert!(store.rename_channel("operations", "taken").await.is_err());
    }

    #[tokio::test]
    async fn channel_can_be_deleted_and_listed() {
        let store = store().await;
        store.create_channel("ops", Visibility::Private, "aaron").await.unwrap();
        store.create_channel("lobby", Visibility::Public, "aaron").await.unwrap();

        assert_eq!(store.list_channels().await.unwrap().len(), 2);

        store.delete_channel("ops").await.unwrap();
        let remaining = store.list_channels().await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].name, "lobby");
    }

    #[tokio::test]
    async fn invite_uses_can_be_decremented_and_revoked() {
        let store = store().await;
        store.create_invite("ops", "tok-123", Some(5), None, "aaron").await.unwrap();

        store.set_invite_uses("tok-123", 4).await.unwrap();
        assert_eq!(store.get_invite("tok-123").await.unwrap().unwrap().uses_remaining, Some(4));

        store.delete_invite("tok-123").await.unwrap();
        assert!(store.get_invite("tok-123").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn invites_are_dropped_when_the_channel_is_deleted() {
        let store = store().await;
        store.create_channel("ops", Visibility::Private, "aaron").await.unwrap();
        store.create_invite("ops", "tok", Some(5), None, "aaron").await.unwrap();

        store.delete_channel("ops").await.unwrap();
        assert!(
            store.get_invite("tok").await.unwrap().is_none(),
            "deleting a channel must drop its invites so a future same-named channel cannot honor them"
        );
    }

    #[tokio::test]
    async fn invites_follow_a_channel_rename() {
        let store = store().await;
        store.create_channel("ops", Visibility::Private, "aaron").await.unwrap();
        store.create_invite("ops", "tok", None, None, "aaron").await.unwrap();

        store.rename_channel("ops", "operations").await.unwrap();
        assert_eq!(store.get_invite("tok").await.unwrap().unwrap().channel, "operations", "an invite must follow its renamed channel");
    }

    #[tokio::test]
    async fn users_can_be_listed_and_deleted() {
        let store = store().await;
        store.create_user("aaron").await.unwrap();
        store.create_user("david").await.unwrap();

        assert_eq!(store.list_users().await.unwrap().len(), 2);

        store.delete_user("david").await.unwrap();
        let remaining = store.list_users().await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].username, "aaron");
    }
}
