# Transparent cache routing: enabling conditions & proof runbook

Result of the commit-range review `e7679aa9..34a8e193` (transparent Redis
cache-aside, reflector, integration suites). Documents **when** the Redis cache
is active and **how to prove** the routing contract: writes → primary, reads →
Redis → (miss) → replica → (fallback) → primary.

Related: [TESTING_CACHE_REFLECTOR.md](TESTING_CACHE_REFLECTOR.md) (test layers
and commands), [TEST_INFRASTRUCTURE.md](TEST_INFRASTRUCTURE.md).

## 1. Is Redis ever enabled automatically?

**No.** There is no auto-enable, env var, or runtime flag in the library. The
cache activates only through explicit opt-in:

| Component | Activation condition | Where |
|---|---|---|
| Library read/write path | Executor exposes a `CacheProvider`; default is `None` | `src/executor.rs:251` (`LifeExecutor::cache_provider`) |
| `MayPostgresExecutor` | `MayPostgresExecutor::with_cache_provider(Arc::new(RedisCacheProvider::new(url)))` → returns `CachedLifeExecutor` | `src/executor.rs:554-560` |
| Any executor (incl. pooled) | `CachedLifeExecutor::new(exec, provider)` decorator | `src/executor.rs:316-353` |
| Unconfigured | `DefaultCacheProvider`: `get` always `Miss`, `set`/`invalidate` no-op | `src/cache/provider.rs:56-73` |
| `lifeguard-reflector` process | `--config <toml>` (or `REFLECTOR_*` env) with `pg_url` (primary, required), `redis_url` (required), `channels`; also takes Redlock leader lock `lifeguard:reflector:leader` | `lifeguard-reflector/src/config.rs`, `reflector.example.toml` |

Failure behaviour: cache errors **fail open** — a broken Redis silently falls
through to the database; a value that fails to deserialize is repopulated
(`src/query/manager.rs:217-223`).

## 2. The routing contract

```
write (insert/update/delete) → ALWAYS primary tier → invalidate Redis key
read  find_by_id            → Redis GET (lifeguard:model:<table>:<id>)
                                ├─ Hit → return JSON; Postgres never touched
                                └─ Miss → SELECT via executor
                                            ├─ PooledLifeExecutor: replica tier if
                                            │  configured AND WAL-lag healthy
                                            └─ else primary tier
                                          → SET key in Redis (TTL 3600)
```

Per-key chain: `Redis miss → DB (replica tier by default when healthy) → populate Redis`.
The primary fallback is **tier-level** (no replica tier / lagging / unreachable),
not per-row — physical replication keeps primary and replica data identical.

Key constants: `DEFAULT_CACHE_TTL_SECS = 3600`, `CACHE_KEY_PREFIX = "lifeguard:model"`
(`src/cache/keys.rs:17-25`). The write path changed in this range from
write-through to **explicit invalidation** (`lifeguard-derive/src/macros/life_record.rs`,
commit `e7679aa`) — the read path repopulates on miss; the reflector refreshes warm keys.

## 3. Proof matrix

### Automated

| Claim | Test | File:line |
|---|---|---|
| Miss populates contract key with model JSON | `find_by_id_miss_populates_redis` | `tests/db_integration/cache_transparent.rs:130` |
| Hit served from Redis (out-of-band SQL UPDATE invisible until key DEL) | `hit_serves_from_redis_and_staleness_is_real` | `tests/db_integration/cache_transparent.rs:164` |
| Insert does not write through; miss populates | `insert_does_not_write_through_but_find_by_id_populates` | `tests/db_integration/cache_transparent.rs:208` |
| Update invalidates key | `update_invalidates_the_cached_key` | `tests/db_integration/cache_transparent.rs:239` |
| Delete invalidates key | `delete_invalidates_the_cached_key` | `tests/db_integration/cache_transparent.rs:269` |
| UUID-PK keys (reflector-seeded) invalidated | `uuid_pk_update_and_delete_invalidate_reflector_seeded_key` | `tests/db_integration/cache_transparent.rs:298` |
| Writes always hit primary tier (`pg_is_in_recovery() = false`) | `pooled_pool_construct_write_read_with_replica` | `tests/db_integration/pool_read_replica.rs:154` |
| Default reads hit replica tier when healthy (`= true`) | `pooled_read_preference_primary_forces_primary_tier` | `tests/db_integration/pool_read_replica.rs:321` |
| `ReadPreference::Primary` forces primary tier | same test | `tests/db_integration/pool_read_replica.rs:387` |
| Replica lagging → reads fall back to primary | `pooled_read_falls_back_to_primary_when_replica_lagging` | `tests/db_integration/pool_read_replica.rs:431` — **`#[ignore]`, flaky Toxiproxy timing** |

Run everything (local stack `postgres-replication-docker`: primary `35432`,
replicas `35433`/`35434`, Redis `36379`):

```bash
export TEST_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:35432/postgres?options=-c%20search_path%3Dlifeguard"
export TEST_REPLICA_URL="postgres://postgres:postgres@127.0.0.1:35433/postgres?options=-c%20search_path%3Dlifeguard"
export TEST_REDIS_URL="redis://127.0.0.1:36379"
./scripts/test-cache-reflector.sh            # unit + cache-aside + reflector
./scripts/test-cache-reflector.sh --full     # + full db suite (CI parity)
```

### Manual smoke

1. **Read → Redis**: `redis-cli -p 36379 MONITOR` in one shell, call `find_by_id`
   twice → observe `GET` (miss) → `SET` → `GET` (hit); or
   `redis-cli GET lifeguard:model:<table>:<id>`.
2. **Read really served by Redis**: populate via `find_by_id`, then run a raw SQL
   `UPDATE` (no ORM, so no invalidation) → next read returns the stale value until
   the key is `DEL`ed.
3. **Miss → replica**: wrap a `PooledLifeExecutor` (replica `35433`, lag monitor
   healthy) in `CachedLifeExecutor`, read a cold key → the fallback `SELECT`
   appears in `35433`'s `pg_stat_activity`/logs; confirm the tier with
   `SELECT pg_is_in_recovery()` on the pooled connection.
4. **Write → primary**: any write through the pooled executor → the statement
   appears on `35432`, never on `35433`.

## 4. Caveats

- Only entities with an integer PK column named `id` are served by the
  `find_by_id` cache path (i64 signature). UUID-PK keys are populated only by
  the reflector, and invalidated by the write path.
- Cached values bypass RLS/session-context filtering. Do not combine a cache
  provider with session-context executors for RLS-protected entities.
- The reflector reads from the **primary** (`pg_url`), never from replicas.
- `cache_transparent` tests use a `MayPostgresExecutor` connected directly to
  the primary — they prove the Redis legs, not the replica leg.

## 5. Known gaps / continuation checklist

- [ ] **No test combines `CachedLifeExecutor` wrapping `PooledLifeExecutor` with a
      replica tier** — the "miss → replica" leg is only covered indirectly by
      `pool_read_replica` + `cache_transparent` separately. Add an integration
      test: cold key → cache miss → read lands on replica tier → key populated.
- [ ] De-flake and re-enable
      `pooled_read_falls_back_to_primary_when_replica_lagging` (`WalLagMonitor`
      does not flip to lagging within the poll window after Toxiproxy disable).
- [ ] Optional: assert the reflector's NOTIFY → refresh path also invalidates
      UUID-PK keys end-to-end against the replica tier (reflector is
      primary-only by design today).
