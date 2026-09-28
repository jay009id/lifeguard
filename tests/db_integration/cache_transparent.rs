//! Transparent cache-aside integration tests: `find_by_id` GET → miss →
//! Postgres → populate, and explicit invalidation on insert/update/delete.
//!
//! These tests prove the routing with a real Redis, not just that a trait
//! method exists:
//!
//! - a miss **populates** the exact key `lifeguard:model:<table>:<id>` with
//!   the model's `serde_json` form (the cross-crate contract the reflector
//!   depends on);
//! - a hit is **served from Redis**: an out-of-band SQL update is invisible
//!   until the key is dropped;
//! - the write path **invalidates** (never write-through): after
//!   insert/update/delete the key must be gone.
//!
//! Redis is provisioned by the test context (`TEST_REDIS_URL`, or a
//! testcontainers instance); a test skips itself if the Redis connection
//! fails. Keys are namespaced by the fixed table names and deleted
//! individually — never `FLUSHALL` (CI shares one Redis).

use lifeguard::query::manager::ModelManager;
use lifeguard::{ActiveModelTrait, LifeExecutor, MayPostgresExecutor, RedisCacheProvider};
use lifeguard_derive::{LifeModel, LifeRecord};
use redis::Commands;
use std::sync::Arc;

/// These tests share fixed table names (and therefore cache keys), so they
/// run one at a time like `notify_trigger` does.
static CACHE_TESTS: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serialised() -> std::sync::MutexGuard<'static, ()> {
    CACHE_TESTS.lock().unwrap_or_else(|e| e.into_inner())
}

/// Redis connection for raw assertions; `None` (test skips) when Redis is
/// unreachable.
fn redis_conn() -> Option<redis::Connection> {
    let ctx = crate::context::get_test_context();
    let client = redis::Client::open(ctx.redis_url.as_str()).ok()?;
    client.get_connection().ok()
}

/// Executor wired with the transparent Redis cache provider.
fn cached_executor() -> lifeguard::CachedLifeExecutor<MayPostgresExecutor> {
    let ctx = crate::context::get_test_context();
    let client = may_postgres::connect(&ctx.pg_url).expect("connect to primary");
    let provider = Arc::new(RedisCacheProvider::new(&ctx.redis_url).expect("redis provider"));
    MayPostgresExecutor::new(client).with_cache_provider(provider)
}

pub mod cache_user {
    use super::*;

    #[derive(LifeModel, LifeRecord)]
    #[table_name = "cache_test_users"]
    pub struct CacheUser {
        #[primary_key]
        pub id: i64,

        pub name: String,
        pub status: String,
    }

    /// The default `find_by_id` cache-aside path is the thing under test.
    impl lifeguard::query::manager::ModelManager for Entity {}
}
pub use cache_user::CacheUserRecord;

pub mod cache_uuid_item {
    use super::*;

    /// UUID-primary-key entity. The ORM read path never populates keys for
    /// these (`find_by_id` is i64-only), but the reflector does — so the
    /// write path must still invalidate them.
    #[derive(LifeModel, LifeRecord)]
    #[table_name = "cache_test_uuid_items"]
    pub struct CacheUuidItem {
        #[primary_key]
        #[column_type = "UUID"]
        pub id: uuid::Uuid,

        pub note: String,
    }

    impl lifeguard::query::manager::ModelManager for Entity {}
}
pub use cache_uuid_item::CacheUuidItemRecord;

const USER_KEY_1: &str = "lifeguard:model:cache_test_users:1";
const USER_KEY_2: &str = "lifeguard:model:cache_test_users:2";

/// Drops and recreates both tables from the generated DDL.
fn setup_tables(exec: &MayPostgresExecutor) {
    let ctx = crate::context::get_test_context();
    let client = may_postgres::connect(&ctx.pg_url).expect("connect for DDL");
    client
        .batch_execute("DROP TABLE IF EXISTS cache_test_users CASCADE")
        .expect("drop users");
    client
        .batch_execute("DROP TABLE IF EXISTS cache_test_uuid_items CASCADE")
        .expect("drop uuid items");

    let users_sql =
        lifeguard_migrate::sql_generator::generate_create_table_sql::<cache_user::Entity>(
            cache_user::Entity::table_definition(),
        )
        .expect("generate users DDL");
    client
        .batch_execute(&users_sql)
        .unwrap_or_else(|e| panic!("apply users DDL: {e}\n---\n{users_sql}"));

    let uuid_sql = lifeguard_migrate::sql_generator::generate_create_table_sql::<
        cache_uuid_item::Entity,
    >(cache_uuid_item::Entity::table_definition())
    .expect("generate uuid DDL");
    client
        .batch_execute(&uuid_sql)
        .unwrap_or_else(|e| panic!("apply uuid DDL: {e}\n---\n{uuid_sql}"));

    let _ = exec;
}

/// Deletes every key these tests touch, for a clean slate.
fn cleanup_keys(conn: &mut redis::Connection) {
    for key in [USER_KEY_1, USER_KEY_2] {
        let _: Result<(), _> = conn.del(key);
    }
}

#[test]
fn find_by_id_miss_populates_redis() {
    let _guard = serialised();
    let exec = cached_executor();
    setup_tables(&exec);

    let Some(mut conn) = redis_conn() else {
        eprintln!("skipping: Redis unavailable");
        return;
    };
    cleanup_keys(&mut conn);

    exec.execute(
        "INSERT INTO cache_test_users (id, name, status) VALUES ($1, $2, $3)",
        &[&1i64, &"Ada".to_string(), &"PENDING".to_string()],
    )
    .expect("raw insert");

    let model = cache_user::Entity::find_by_id(&exec, 1)
        .expect("find_by_id")
        .expect("row exists");

    // The miss must have populated the exact contract key with the model's
    // serde_json form.
    let cached: String = conn.get(USER_KEY_1).expect("key populated by read");
    let parsed: serde_json::Value =
        serde_json::from_str(&cached).unwrap_or_else(|e| panic!("cached value not JSON: {e}"));
    assert_eq!(parsed["id"], 1);
    assert_eq!(parsed["name"], "Ada");
    assert_eq!(parsed["status"], "PENDING");
    assert_eq!(model.name, "Ada");
    assert_eq!(model.status, "PENDING");
}

#[test]
fn hit_serves_from_redis_and_staleness_is_real() {
    let _guard = serialised();
    let exec = cached_executor();
    setup_tables(&exec);

    let Some(mut conn) = redis_conn() else {
        eprintln!("skipping: Redis unavailable");
        return;
    };
    cleanup_keys(&mut conn);

    exec.execute(
        "INSERT INTO cache_test_users (id, name, status) VALUES ($1, $2, $3)",
        &[&1i64, &"Ada".to_string(), &"PENDING".to_string()],
    )
    .expect("raw insert");

    // Populate the cache through the read path.
    let model = cache_user::Entity::find_by_id(&exec, 1).expect("find_by_id");
    assert_eq!(model.expect("row exists").status, "PENDING");
    let _: bool = conn.exists(USER_KEY_1).expect("exists");

    // Out-of-band write: bypasses the ORM, so nothing invalidates the key.
    exec.execute(
        "UPDATE cache_test_users SET status = $2 WHERE id = $1",
        &[&1i64, &"SENT".to_string()],
    )
    .expect("raw update");

    // Still the OLD value: this read was served by Redis, not Postgres.
    let stale = cache_user::Entity::find_by_id(&exec, 1)
        .expect("find_by_id")
        .expect("row exists");
    assert_eq!(stale.status, "PENDING", "read must come from the cache");

    // Dropping the key makes the next read see the fresh row.
    let _: () = conn.del(USER_KEY_1).expect("del");
    let fresh = cache_user::Entity::find_by_id(&exec, 1)
        .expect("find_by_id")
        .expect("row exists");
    assert_eq!(fresh.status, "SENT");
}

#[test]
fn insert_does_not_write_through_but_find_by_id_populates() {
    let _guard = serialised();
    let exec = cached_executor();
    setup_tables(&exec);

    let Some(mut conn) = redis_conn() else {
        eprintln!("skipping: Redis unavailable");
        return;
    };
    cleanup_keys(&mut conn);

    let mut record = CacheUserRecord::new();
    record.set_id(1);
    record.set_name("Grace".to_string());
    record.set_status("PENDING".to_string());
    record.insert(&exec).expect("insert");

    // Explicit invalidation, not write-through: the key must NOT exist yet.
    let exists: bool = conn.exists(USER_KEY_1).expect("exists");
    assert!(!exists, "insert must not write through to the cache");

    // The read path populates on miss.
    let model = cache_user::Entity::find_by_id(&exec, 1)
        .expect("find_by_id")
        .expect("row exists");
    assert_eq!(model.name, "Grace");
    let exists: bool = conn.exists(USER_KEY_1).expect("exists");
    assert!(exists, "find_by_id miss must populate the cache");
}

#[test]
fn update_invalidates_the_cached_key() {
    let _guard = serialised();
    let exec = cached_executor();
    setup_tables(&exec);

    let Some(mut conn) = redis_conn() else {
        eprintln!("skipping: Redis unavailable");
        return;
    };
    cleanup_keys(&mut conn);

    let mut record = CacheUserRecord::new();
    record.set_id(1);
    record.set_name("Ada".to_string());
    record.set_status("PENDING".to_string());
    let model = record.insert(&exec).expect("insert");

    cache_user::Entity::find_by_id(&exec, 1).expect("populate");
    let exists: bool = conn.exists(USER_KEY_1).expect("exists");
    assert!(exists);

    let mut update_record = CacheUserRecord::from_model(&model);
    update_record.set_status("SENT".to_string());
    update_record.update(&exec).expect("update");

    let exists: bool = conn.exists(USER_KEY_1).expect("exists");
    assert!(!exists, "update must invalidate the cached key");
}

#[test]
fn delete_invalidates_the_cached_key() {
    let _guard = serialised();
    let exec = cached_executor();
    setup_tables(&exec);

    let Some(mut conn) = redis_conn() else {
        eprintln!("skipping: Redis unavailable");
        return;
    };
    cleanup_keys(&mut conn);

    let mut record = CacheUserRecord::new();
    record.set_id(1);
    record.set_name("Ada".to_string());
    record.set_status("PENDING".to_string());
    let model = record.insert(&exec).expect("insert");

    cache_user::Entity::find_by_id(&exec, 1).expect("populate");
    let exists: bool = conn.exists(USER_KEY_1).expect("exists");
    assert!(exists);

    let delete_record = CacheUserRecord::from_model(&model);
    delete_record.delete(&exec).expect("delete");

    let exists: bool = conn.exists(USER_KEY_1).expect("exists");
    assert!(!exists, "delete must invalidate the cached key");
}

#[test]
fn uuid_pk_update_and_delete_invalidate_reflector_seeded_key() {
    let _guard = serialised();
    let exec = cached_executor();
    setup_tables(&exec);

    let Some(mut conn) = redis_conn() else {
        eprintln!("skipping: Redis unavailable");
        return;
    };
    cleanup_keys(&mut conn);

    let id = uuid::Uuid::new_v4();
    let key = format!("lifeguard:model:cache_test_uuid_items:{id}");

    let mut record = CacheUuidItemRecord::new();
    record.set_id(id);
    record.set_note("stale".to_string());
    let model = record.insert(&exec).expect("insert");

    // Simulate a reflector-populated key (the ORM read path never populates
    // UUID-PK keys on its own).
    let _: () = conn
        .set_ex(&key, "{\"id\":\"stale\",\"note\":\"stale\"}", 3600)
        .expect("seed");

    let mut update_record = CacheUuidItemRecord::from_model(&model);
    update_record.set_note("fresh".to_string());
    update_record.update(&exec).expect("update");

    let exists: bool = conn.exists(&key).expect("exists");
    assert!(
        !exists,
        "update must invalidate UUID-PK keys (derive PK fix)"
    );

    // Re-seed and prove the delete path invalidates too.
    let _: () = conn
        .set_ex(&key, "{\"id\":\"stale\",\"note\":\"stale\"}", 3600)
        .expect("reseed");
    let delete_record = CacheUuidItemRecord::from_model(&model);
    delete_record.delete(&exec).expect("delete");
    let exists: bool = conn.exists(&key).expect("exists");
    assert!(
        !exists,
        "delete must invalidate UUID-PK keys (derive PK fix)"
    );
}

#[test]
fn decorator_wraps_any_executor_equivalently() {
    let _guard = serialised();
    let mut db =
        lifeguard::test_helpers::TestDatabase::with_url(&crate::context::get_test_context().pg_url);
    let _client = db.connect().expect("connect");
    let exec = db.executor().expect("executor");
    setup_tables(&exec);

    let Some(mut conn) = redis_conn() else {
        eprintln!("skipping: Redis unavailable");
        return;
    };
    cleanup_keys(&mut conn);

    let ctx = crate::context::get_test_context();
    let provider = Arc::new(RedisCacheProvider::new(&ctx.redis_url).expect("redis provider"));

    // Construct the decorator directly instead of via the builder.
    let wrapped = lifeguard::CachedLifeExecutor::new(exec, provider);
    assert!(wrapped.cache_provider().is_some(), "provider exposed");

    // Delegation still works end-to-end.
    wrapped
        .execute(
            "INSERT INTO cache_test_users (id, name, status) VALUES ($1, $2, $3)",
            &[&2i64, &"Dee".to_string(), &"PENDING".to_string()],
        )
        .expect("insert via decorator");
    let model = cache_user::Entity::find_by_id(&wrapped, 2)
        .expect("find_by_id")
        .expect("row exists");
    assert_eq!(model.name, "Dee");
}
