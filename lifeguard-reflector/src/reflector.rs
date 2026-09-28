//! The reflector worker: leader loop, notification processing, active-set refresh.

use crate::config::{is_safe_identifier, ReflectorConfig};
use crate::leader::LeaderLock;
use crate::ReflectorError;
use redis::Commands;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// The cache key prefix shared with the ORM.
///
/// Must match `lifeguard::cache::keys::CACHE_KEY_PREFIX`; both crates' tests
/// assert the exact literal `lifeguard:model:<table>:<id>` to catch drift.
pub const CACHE_KEY_PREFIX: &str = "lifeguard:model";

/// Default TTL applied to refreshed keys, in seconds.
///
/// Must match `lifeguard::cache::keys::DEFAULT_CACHE_TTL_SECS`; the config
/// default in [`ReflectorConfig`] uses the same value.
pub const DEFAULT_TTL_SECS: u64 = 3600;

/// How long a follower sleeps before retrying leader acquisition.
const FOLLOWER_RETRY_SLEEP: Duration = Duration::from_secs(5);

/// Initial backoff after a failed PostgreSQL connection; capped at [`MAX_PG_BACKOFF`].
const INITIAL_PG_BACKOFF: Duration = Duration::from_millis(500);
const MAX_PG_BACKOFF: Duration = Duration::from_secs(30);

/// Primary-key metadata for one table, resolved from `pg_catalog`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PkInfo {
    /// Unquoted primary key column name.
    pub column: String,
    /// PostgreSQL type name (`int8`, `uuid`, ...), used to cast the payload id.
    pub typname: String,
}

/// Mutable per-leader-session state.
#[derive(Debug, Default)]
pub struct ReflectorState {
    pk_info: HashMap<String, PkInfo>,
    warned_tables: HashSet<String>,
}

/// The reflector worker.
pub struct Reflector {
    config: ReflectorConfig,
    leader: LeaderLock,
    redis: redis::Client,
}

impl Reflector {
    /// Builds a reflector from validated configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the Redis client cannot be constructed.
    pub fn new(config: ReflectorConfig) -> Result<Self, ReflectorError> {
        let redis = redis::Client::open(config.redis_url.as_str())?;
        let leader = LeaderLock::new(redis.clone(), config.lock_key.clone(), config.lock_ttl_ms)?;
        Ok(Self {
            config,
            leader,
            redis,
        })
    }

    /// Runs the reflector forever: leader election, LISTEN, refresh loop.
    ///
    /// Followers sleep and retry acquisition. As leader, the loop reconnects
    /// to PostgreSQL with capped exponential backoff and re-issues `LISTEN`
    /// on every (re)connection. Returns only on a fatal error.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial leader lock acquire fails permanently
    /// (Redis down).
    pub fn run(&mut self) -> Result<(), ReflectorError> {
        loop {
            match self.leader.try_acquire() {
                Ok(true) => {}
                Ok(false) => {
                    log::debug!("not the leader; retrying in {FOLLOWER_RETRY_SLEEP:?}");
                    std::thread::sleep(FOLLOWER_RETRY_SLEEP);
                    continue;
                }
                Err(e) => {
                    log::error!("leader acquire failed: {e}; retrying");
                    std::thread::sleep(FOLLOWER_RETRY_SLEEP);
                    continue;
                }
            }

            log::info!(
                "acquired leader lock {:?} as {:?}",
                self.leader.key(),
                self.leader.instance_id()
            );

            let mut backoff = INITIAL_PG_BACKOFF;
            loop {
                match may_postgres::connect(&self.config.pg_url) {
                    Ok(client) => {
                        if let Err(e) = Self::listen_all(&client, &self.config.channels) {
                            log::error!("LISTEN setup failed: {e}");
                        } else {
                            log::info!(
                                "listening on {:?}; polling every {}ms",
                                self.config.channels,
                                self.config.poll_interval_ms
                            );
                            let mut state = ReflectorState::default();
                            self.leader_session(&client, &mut state);
                        }
                        // Lock lost or session ended; drop the connection and
                        // re-enter the outer loop to re-acquire.
                        break;
                    }
                    Err(e) => {
                        log::error!(
                            "could not connect to PostgreSQL: {e}; backing off {backoff:?}"
                        );
                        std::thread::sleep(backoff);
                        backoff = std::cmp::min(backoff * 2, MAX_PG_BACKOFF);
                    }
                }
            }
        }
    }

    /// Runs one leader session: renew lock + drain notifications until the
    /// lock is lost.
    fn leader_session(&mut self, client: &may_postgres::Client, state: &mut ReflectorState) {
        let poll = Duration::from_millis(self.config.poll_interval_ms);
        loop {
            match self.leader.renew() {
                Ok(true) => {}
                Ok(false) => {
                    log::warn!("leader lock lost; returning to election");
                    return;
                }
                Err(e) => {
                    log::error!("leader renew failed: {e}");
                    std::thread::sleep(FOLLOWER_RETRY_SLEEP);
                    continue;
                }
            }

            match Self::run_tick(client, &self.config, state, &self.redis) {
                Ok(handled) => {
                    if handled > 0 {
                        log::debug!("refreshed {handled} key(s)");
                    }
                }
                Err(e) => log::error!("tick failed: {e}"),
            }
            std::thread::sleep(poll);
        }
    }

    /// Issues `LISTEN` for every configured channel on the connection.
    ///
    /// Channel names are validated at config load
    /// ([`ReflectorConfig::from_toml_str`]); they are still embedded via the
    /// identifier check here as defense in depth.
    ///
    /// # Errors
    ///
    /// Returns a PostgreSQL error if a LISTEN fails.
    pub fn listen_all(
        client: &may_postgres::Client,
        channels: &[String],
    ) -> Result<(), ReflectorError> {
        for channel in channels {
            debug_assert!(is_safe_identifier(channel));
            client.batch_execute(&format!("LISTEN {channel}"))?;
        }
        Ok(())
    }

    /// One polling tick: drive notification delivery, drain the queue, and
    /// refresh (or delete) the cached key for every notification whose model
    /// key currently exists in Redis.
    ///
    /// Returns the number of keys refreshed or deleted.
    ///
    /// # Errors
    ///
    /// Returns an error on PostgreSQL or Redis connectivity failures. Errors
    /// for individual notifications (malformed payloads, unsupported types)
    /// are logged and skipped, not propagated.
    pub fn run_tick(
        client: &may_postgres::Client,
        config: &ReflectorConfig,
        state: &mut ReflectorState,
        redis: &redis::Client,
    ) -> Result<usize, ReflectorError> {
        // Any round trip lets the connection surface pending notifications.
        client.batch_execute("SELECT 1")?;

        let mut handled = 0usize;
        while let Some(notification) = client.notifications().pop() {
            let payload = notification.payload().to_string();
            let channel = notification.channel().to_string();
            match Self::handle_notification(client, config, state, redis, &channel, &payload) {
                Ok(Some(())) => handled += 1,
                Ok(None) => {}
                Err(e) => log::warn!("skipping notification on {channel}: {e}"),
            }
        }
        Ok(handled)
    }

    /// Processes one notification: parses `{"table", "op", "id"}`, checks the
    /// model key in Redis, and refreshes or deletes it.
    ///
    /// Returns `Ok(Some(()))` when a key was refreshed/deleted, `Ok(None)`
    /// when the key was inactive or the notification was not actionable
    /// (logged).
    ///
    /// # Errors
    ///
    /// Returns an error on malformed payloads and on PostgreSQL/Redis
    /// failures (connectivity, catalog misses).
    fn handle_notification(
        client: &may_postgres::Client,
        config: &ReflectorConfig,
        state: &mut ReflectorState,
        redis: &redis::Client,
        channel: &str,
        payload: &str,
    ) -> Result<Option<()>, ReflectorError> {
        let parsed: serde_json::Value = serde_json::from_str(payload)
            .map_err(|e| ReflectorError::Config(format!("payload not JSON: {e}")))?;

        let table = parsed
            .get("table")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        if !is_safe_identifier(table) {
            log::warn!("ignoring notification on {channel}: invalid table name {table:?}");
            return Ok(None);
        }

        // The trigger renders the id as a JSON number for numeric keys and a
        // JSON string for uuid/text keys; normalize both to a string.
        let id_str = match parsed.get("id") {
            Some(serde_json::Value::Number(n)) => n.to_string(),
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(_) | None => {
                log::warn!("ignoring notification on {channel}: missing or untypable id");
                return Ok(None);
            }
        };

        let key = format!("{CACHE_KEY_PREFIX}:{table}:{id_str}");

        // Active-set rule: only keys that already exist in Redis are touched.
        // Keys that were never cached (or expired) are ignored.
        let mut conn = redis.get_connection()?;
        let exists: bool = conn.exists(&key)?;
        if !exists {
            return Ok(None);
        }

        let Some(pk) = resolve_pk(client, state, table) else {
            // Composite PKs / unresolvable tables are logged once per table.
            return Ok(None);
        };

        // The payload id is a string, but the bind parameter must serialize
        // as the PK column's type: the server infers `$1`'s type from the
        // column at prepare time, and a String bound as int8 fails client
        // side. No CAST in the SQL — `WHERE "id" = $1` keeps the PK index
        // usable.
        let Some(param) = parse_pk_param(&pk.typname, &id_str) else {
            log::debug!(
                "table {table}: cannot bind id {id_str:?} as {}; leaving cached value",
                pk.typname
            );
            return Ok(None);
        };

        // table/column names are validated identifiers and double-quoted.
        let sql = format!("SELECT * FROM \"{table}\" WHERE \"{}\" = $1", pk.column);
        let row = client.query_opt(sql.as_str(), &[param.as_to_sql()])?;

        match row {
            None => {
                // The row is gone (DELETE): evict the key rather than let a
                // stale value live until TTL.
                conn.del::<_, ()>(&key)?;
                log::debug!("row deleted → evicted {key}");
            }
            Some(row) => {
                let Some(json) = row_to_json(&row) else {
                    log::debug!(
                        "row for {table} has unsupported column types; leaving cached value"
                    );
                    return Ok(None);
                };
                conn.set_ex::<&str, &str, ()>(&key, &json.to_string(), config.ttl_seconds)?;
                log::debug!("refreshed {key}");
            }
        }
        Ok(Some(()))
    }
}

/// A typed primary-key value for a refresh bind parameter.
enum PkParam {
    I16(i16),
    I32(i32),
    I64(i64),
    Text(String),
    Uuid(uuid::Uuid),
}

impl PkParam {
    fn as_to_sql(&self) -> &dyn may_postgres::types::ToSql {
        match self {
            Self::I16(v) => v,
            Self::I32(v) => v,
            Self::I64(v) => v,
            Self::Text(v) => v,
            Self::Uuid(v) => v,
        }
    }
}

/// Parses the payload id string into the Rust value matching the PK column's
/// PostgreSQL type name (from the catalog), so the bind parameter serializes
/// correctly.
///
/// Returns `None` for unsupported types and unparseable ids; the caller then
/// leaves the existing cached value untouched.
fn parse_pk_param(typname: &str, id_str: &str) -> Option<PkParam> {
    match typname {
        "int2" => id_str.parse::<i16>().ok().map(PkParam::I16),
        "int4" => id_str.parse::<i32>().ok().map(PkParam::I32),
        "int8" => id_str.parse::<i64>().ok().map(PkParam::I64),
        "uuid" => id_str.parse::<uuid::Uuid>().ok().map(PkParam::Uuid),
        "text" | "varchar" | "bpchar" | "name" => Some(PkParam::Text(id_str.to_string())),
        _ => {
            log::debug!("unsupported pk type {typname:?}; leaving cached value");
            None
        }
    }
}

/// Resolves (and memoizes) the single-column primary key of `table` from the
/// PostgreSQL catalogs, resolved through the reflector connection's
/// `search_path` (unqualified names only — the ORM cache key is unqualified
/// too).
///
/// Returns `None` (logging once per table) for composite primary keys and
/// tables that `to_regclass` cannot resolve on this connection.
fn resolve_pk(
    client: &may_postgres::Client,
    state: &mut ReflectorState,
    table: &str,
) -> Option<PkInfo> {
    if let Some(info) = state.pk_info.get(table) {
        return Some(info.clone());
    }

    let rows = client
        .query(
            "SELECT a.attname, t.typname
               FROM pg_index i
               JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
               JOIN pg_type t ON t.oid = a.atttypid
              WHERE i.indrelid = to_regclass($1) AND i.indisprimary
              ORDER BY array_position(i.indkey, a.attnum)",
            &[&table],
        )
        .ok()?;

    let info = match rows.len() {
        1 => {
            let column: String = rows[0].get(0);
            let typname: String = rows[0].get(1);
            if !is_safe_identifier(&column) || !is_safe_identifier(&typname) {
                log::warn!("table {table}: unexpected pk column/type names; skipping");
                return None;
            }
            PkInfo { column, typname }
        }
        0 => {
            if state.warned_tables.insert(table.to_string()) {
                log::warn!(
                    "table {table}: not resolvable on this connection's search_path (or no primary key); skipping"
                );
            }
            return None;
        }
        _ => {
            if state.warned_tables.insert(table.to_string()) {
                log::warn!("table {table}: composite primary keys are not supported; skipping");
            }
            return None;
        }
    };

    state.pk_info.insert(table.to_string(), info.clone());
    Some(info)
}

/// Serializes a row as a JSON object keyed by column name, matching the shape
/// `serde_json::to_string(model)` produces in the ORM (model field names equal
/// column names via `#[serde(rename = "<column>")]`).
///
/// `NULL` values serialize as JSON `null`, which deserializes into `Option`
/// fields. Returns `None` when any column has a type outside the supported
/// subset; the caller then leaves the existing cached value untouched.
#[must_use]
pub fn row_to_json(row: &may_postgres::Row) -> Option<serde_json::Value> {
    use may_postgres::types::Type;

    let mut map = serde_json::Map::new();
    for (idx, col) in row.columns().iter().enumerate() {
        let t = col.type_();
        let value = if *t == Type::INT2 {
            row.try_get::<_, Option<i16>>(idx)
                .ok()?
                .map_or(serde_json::Value::Null, |v| serde_json::Value::from(v))
        } else if *t == Type::INT4 {
            row.try_get::<_, Option<i32>>(idx)
                .ok()?
                .map_or(serde_json::Value::Null, |v| serde_json::Value::from(v))
        } else if *t == Type::INT8 {
            row.try_get::<_, Option<i64>>(idx)
                .ok()?
                .map_or(serde_json::Value::Null, |v| serde_json::Value::from(v))
        } else if *t == Type::FLOAT4 {
            row.try_get::<_, Option<f32>>(idx)
                .ok()?
                .map_or(serde_json::Value::Null, |v| {
                    serde_json::Number::from_f64(f64::from(v))
                        .map_or(serde_json::Value::Null, serde_json::Value::Number)
                })
        } else if *t == Type::FLOAT8 {
            row.try_get::<_, Option<f64>>(idx)
                .ok()?
                .map_or(serde_json::Value::Null, |v| {
                    serde_json::Number::from_f64(v)
                        .map_or(serde_json::Value::Null, serde_json::Value::Number)
                })
        } else if *t == Type::BOOL {
            row.try_get::<_, Option<bool>>(idx)
                .ok()?
                .map_or(serde_json::Value::Null, |v| serde_json::Value::from(v))
        } else if *t == Type::TEXT || *t == Type::VARCHAR || *t == Type::BPCHAR || *t == Type::NAME
        {
            row.try_get::<_, Option<String>>(idx)
                .ok()?
                .map_or(serde_json::Value::Null, |v| serde_json::Value::from(v))
        } else if *t == Type::UUID {
            row.try_get::<_, Option<uuid::Uuid>>(idx)
                .ok()?
                .map_or(serde_json::Value::Null, |v| {
                    serde_json::Value::from(v.to_string())
                })
        } else if *t == Type::TIMESTAMPTZ {
            row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx)
                .ok()?
                .map_or(serde_json::Value::Null, |v| {
                    serde_json::Value::from(v.to_rfc3339())
                })
        } else if *t == Type::TIMESTAMP {
            row.try_get::<_, Option<chrono::NaiveDateTime>>(idx)
                .ok()?
                .map_or(serde_json::Value::Null, |v| {
                    serde_json::Value::from(v.to_string())
                })
        } else if *t == Type::DATE {
            row.try_get::<_, Option<chrono::NaiveDate>>(idx)
                .ok()?
                .map_or(serde_json::Value::Null, |v| {
                    serde_json::Value::from(v.to_string())
                })
        } else if *t == Type::JSON || *t == Type::JSONB {
            row.try_get::<_, Option<serde_json::Value>>(idx)
                .ok()?
                .unwrap_or(serde_json::Value::Null)
        } else {
            log::debug!("unsupported column type {} for column {}", t, col.name());
            return None;
        };
        map.insert(col.name().to_string(), value);
    }
    Some(serde_json::Value::Object(map))
}
