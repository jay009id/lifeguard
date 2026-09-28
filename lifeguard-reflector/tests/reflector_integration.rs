//! Integration tests for `lifeguard-reflector` against a real PostgreSQL +
//! Redis. Skips unless both `TEST_DATABASE_URL` and `TEST_REDIS_URL` are set.
//!
//! The trigger SQL here mirrors what `lifeguard-migrate`'s
//! `generate_notify_sql` emits for `#[notify(channel = "...")]`; the payload
//! contract (`{"table", "op", "id"}`) is asserted end-to-end in the
//! `lifeguard` crate's `notify_trigger` tests. These tests prove the
//! reflector side: leader election, active-set refresh, DELETE eviction, and
//! malformed-payload robustness.

use lifeguard_reflector::{LeaderLock, Reflector, ReflectorConfig, ReflectorState};
use redis::Commands;
use std::sync::Mutex;
use std::time::Duration;

const CHANNEL: &str = "reflector_test_events";
const TABLE: &str = "reflector_test_items";
const KEY: &str = "lifeguard:model:reflector_test_items:5";

/// Fixed table + channel → serialize these tests among themselves.
static REFLECTOR_TESTS: Mutex<()> = Mutex::new(());

fn serialised() -> std::sync::MutexGuard<'static, ()> {
    REFLECTOR_TESTS.lock().unwrap_or_else(|e| e.into_inner())
}

fn pg_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

fn redis_url() -> Option<String> {
    std::env::var("TEST_REDIS_URL")
        .or_else(|_| std::env::var("REDIS_URL"))
        .ok()
        .filter(|s| !s.trim().is_empty())
}

fn skip_without_infra() -> Option<(String, String)> {
    match (pg_url(), redis_url()) {
        (Some(pg), Some(redis)) => Some((pg, redis)),
        _ => {
            eprintln!("skipping: TEST_DATABASE_URL and TEST_REDIS_URL must both be set");
            None
        }
    }
}

fn test_config(pg: &str, redis: &str) -> ReflectorConfig {
    let toml_str =
        format!("pg_url = \"{pg}\"\nredis_url = \"{redis}\"\nchannels = [\"{CHANNEL}\"]\n");
    ReflectorConfig::from_toml_str(&toml_str).expect("valid test config")
}

/// Creates the table and installs a `pg_notify` trigger mirroring
/// `generate_notify_sql` output for `#[notify(channel = "...")]` entities.
fn setup_table(pg: &str) {
    let client = may_postgres::connect(pg).expect("connect for DDL");
    let ddl = format!(
        r#"
DROP TABLE IF EXISTS {TABLE} CASCADE;
CREATE TABLE {TABLE} (id BIGINT PRIMARY KEY, note TEXT NOT NULL);

CREATE OR REPLACE FUNCTION lifeguard_notify_{TABLE}() RETURNS trigger
LANGUAGE plpgsql
AS $lifeguard_notify$
BEGIN
    PERFORM pg_notify(
        '{CHANNEL}',
        json_build_object(
            'table', TG_TABLE_NAME,
            'op', TG_OP,
            'id', (CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END)
        )::text
    );
    RETURN NULL;
END;
$lifeguard_notify$;

DROP TRIGGER IF EXISTS lifeguard_notify_{TABLE} ON {TABLE};
CREATE TRIGGER lifeguard_notify_{TABLE}
    AFTER INSERT OR UPDATE OR DELETE ON {TABLE}
    FOR EACH ROW
    EXECUTE FUNCTION lifeguard_notify_{TABLE}();
"#
    );
    client
        .batch_execute(&ddl)
        .unwrap_or_else(|e| panic!("apply DDL: {e}\n---\n{ddl}"));
}

fn redis_client(url: &str) -> redis::Client {
    redis::Client::open(url).expect("redis client")
}

/// Runs `run_tick` until `done` returns true or the tick budget is spent.
/// `run_tick` drives notification delivery itself (`SELECT 1` round trip).
fn run_until(
    client: &may_postgres::Client,
    config: &ReflectorConfig,
    redis: &redis::Client,
    mut done: impl FnMut() -> bool,
) {
    let mut state = ReflectorState::default();
    for _ in 0..100 {
        if done() {
            return;
        }
        Reflector::run_tick(client, config, &mut state, redis).expect("run_tick");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn leader_lock_acquire_renew_and_expiry() {
    let Some((_, redis)) = skip_without_infra() else {
        return;
    };
    let _guard = serialised();

    let client = redis_client(&redis);
    let lock_key = format!("lifeguard:reflector:test:lock:{}", std::process::id());
    let ttl_ms = 500u64;

    let lock_a = LeaderLock::new(client.clone(), lock_key.clone(), ttl_ms).expect("lock a");
    let lock_b = LeaderLock::new(client, lock_key.clone(), ttl_ms).expect("lock b");
    assert_ne!(lock_a.instance_id(), lock_b.instance_id());

    assert!(
        lock_a.try_acquire().expect("acquire a"),
        "first acquire wins"
    );
    assert!(
        !lock_b.try_acquire().expect("acquire b"),
        "second acquire fails"
    );

    let mut conn = redis_client(&redis).get_connection().expect("conn");
    let pttl: i64 = redis::cmd("PTTL")
        .arg(&lock_key)
        .query(&mut conn)
        .expect("pttl");
    assert!(pttl > 0 && pttl <= ttl_ms as i64, "ttl set: {pttl}");

    assert!(lock_a.renew().expect("renew"), "owner can renew");
    let pttl: i64 = redis::cmd("PTTL")
        .arg(&lock_key)
        .query(&mut conn)
        .expect("pttl");
    assert!(pttl > 0, "renew extends ttl");

    // Simulate expiry: another actor deletes the lock.
    let _: () = conn.del(&lock_key).expect("del");
    assert!(
        !lock_a.renew().expect("renew after loss"),
        "stolen lock cannot renew"
    );

    // Re-acquire succeeds for either instance after the key is gone.
    assert!(lock_b.try_acquire().expect("re-acquire b"));
}

#[test]
fn notification_refreshes_prewarmed_key() {
    let Some((pg, redis)) = skip_without_infra() else {
        return;
    };
    let _guard = serialised();
    setup_table(&pg);

    let mut conn = redis_client(&redis).get_connection().expect("conn");
    let _: Result<(), _> = conn.del(KEY);
    let _: () = conn
        .set_ex(KEY, r#"{"id":5,"note":"stale"}"#, 3600)
        .expect("prewarm");

    // LISTEN must precede the write: notifications are not retroactive.
    let config = test_config(&pg, &redis);
    let redis_client = redis_client(&redis);
    let listener = may_postgres::connect(&pg).expect("listener");
    Reflector::listen_all(&listener, &config.channels).expect("listen");

    // Write through raw SQL so only the trigger (not the ORM) is involved.
    let writer = may_postgres::connect(&pg).expect("writer");
    writer
        .execute(
            "INSERT INTO reflector_test_items (id, note) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note",
            &[&5i64, &"fresh".to_string()],
        )
        .expect("write");

    run_until(&listener, &config, &redis_client, || {
        conn.get::<_, Option<String>>(KEY)
            .ok()
            .flatten()
            .is_some_and(|v| v.contains("fresh"))
    });

    let cached: String = conn.get(KEY).expect("key present");
    let parsed: serde_json::Value =
        serde_json::from_str(&cached).unwrap_or_else(|e| panic!("refreshed value not JSON: {e}"));
    assert_eq!(parsed["id"], 5, "column-name-keyed JSON shape: {cached}");
    assert_eq!(
        parsed["note"], "fresh",
        "column-name-keyed JSON shape: {cached}"
    );
}

#[test]
fn notification_for_absent_key_is_ignored() {
    let Some((pg, redis)) = skip_without_infra() else {
        return;
    };
    let _guard = serialised();
    setup_table(&pg);

    let mut conn = redis_client(&redis).get_connection().expect("conn");
    let _: Result<(), _> = conn.del(KEY);

    // LISTEN must precede the write: notifications are not retroactive.
    let config = test_config(&pg, &redis);
    let redis_client = redis_client(&redis);
    let listener = may_postgres::connect(&pg).expect("listener");
    Reflector::listen_all(&listener, &config.channels).expect("listen");

    let writer = may_postgres::connect(&pg).expect("writer");
    writer
        .execute(
            "INSERT INTO reflector_test_items (id, note) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note",
            &[&5i64, &"fresh".to_string()],
        )
        .expect("write");

    // Drain whatever the write produced; the key must never be created.
    run_until(&listener, &config, &redis_client, || false);
    std::thread::sleep(Duration::from_millis(50));
    run_until(&listener, &config, &redis_client, || false);

    let exists: bool = conn.exists(KEY).expect("exists");
    assert!(
        !exists,
        "inactive keys must not be created by the reflector"
    );
}

#[test]
fn delete_refreshes_to_del() {
    let Some((pg, redis)) = skip_without_infra() else {
        return;
    };
    let _guard = serialised();
    setup_table(&pg);

    let mut conn = redis_client(&redis).get_connection().expect("conn");
    let _: Result<(), _> = conn.del(KEY);
    let _: () = conn
        .set_ex(KEY, r#"{"id":5,"note":"stale"}"#, 3600)
        .expect("prewarm");

    // LISTEN must precede the writes: notifications are not retroactive.
    let config = test_config(&pg, &redis);
    let redis_client = redis_client(&redis);
    let listener = may_postgres::connect(&pg).expect("listener");
    Reflector::listen_all(&listener, &config.channels).expect("listen");

    let writer = may_postgres::connect(&pg).expect("writer");
    writer
        .execute(
            "INSERT INTO reflector_test_items (id, note) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET note = EXCLUDED.note",
            &[&5i64, &"to be deleted".to_string()],
        )
        .expect("insert");
    writer
        .execute("DELETE FROM reflector_test_items WHERE id = $1", &[&5i64])
        .expect("delete");

    run_until(&listener, &config, &redis_client, || {
        !conn.exists::<_, bool>(KEY).unwrap_or(true)
    });

    let exists: bool = conn.exists(KEY).expect("exists");
    assert!(!exists, "a deleted row must evict its cached key");
}

#[test]
fn malformed_payload_does_not_crash_the_tick() {
    let Some((pg, redis)) = skip_without_infra() else {
        return;
    };
    let _guard = serialised();
    setup_table(&pg);

    let config = test_config(&pg, &redis);
    let redis_client = redis_client(&redis);
    let listener = may_postgres::connect(&pg).expect("listener");
    Reflector::listen_all(&listener, &config.channels).expect("listen");

    // NOTIFY must come from another session.
    let notifier = may_postgres::connect(&pg).expect("notifier");
    notifier
        .execute(
            "SELECT pg_notify($1, $2)",
            &[&CHANNEL.to_string(), &"not-json".to_string()],
        )
        .expect("notify malformed");
    notifier
        .execute(
            "SELECT pg_notify($1, $2)",
            &[
                &CHANNEL.to_string(),
                &r#"{"table":"evil; DROP TABLE x","op":"INSERT","id":1}"#.to_string(),
            ],
        )
        .expect("notify hostile table");

    let mut state = ReflectorState::default();
    let handled = Reflector::run_tick(&listener, &config, &mut state, &redis_client)
        .expect("tick survives malformed payloads");
    assert_eq!(handled, 0);
}
