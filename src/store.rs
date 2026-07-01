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

/// A channel (`name` unique, DESIGN.md §6, §15).
#[derive(Debug, Clone, PartialEq, Eq, SurrealValue)]
pub struct ChannelRecord {
    /// The channel name.
    pub name: String,
    /// The visibility tier token (see [`Visibility::as_str`]).
    pub visibility: String,
    /// The user-level access-control list.
    pub acl: Vec<String>,
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
            acl: vec![created_by.to_owned()],
            created_by: created_by.to_owned(),
            created_at: now_rfc3339(),
        };
        self.insert("channel", record.clone()).await?;
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
}
