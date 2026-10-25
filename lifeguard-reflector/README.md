# Lifeguard Reflector

> **Cache coherence microservice for the Lifeguard ORM**

[![License](https://img.shields.io/badge/license-Proprietary-red.svg)](LICENSE)

**Lifeguard Reflector** keeps shared Redis cache entries coherent with
PostgreSQL for every Lifeguard application sharing that cache, without adding
work to the read hot path.

## Architecture

Two independent paths keep Redis coherent. Exact routing semantics and the
proof matrix live in
[`docs/CACHE_ROUTING_PROOF.md`](../docs/CACHE_ROUTING_PROOF.md).

**System architecture.** Every app shares the same Redis through the ORM; one
reflector instance holds the Redlock lease and does all the coherence work
while standbys sleep and retry. Reads may use a physical replica; the
reflector itself stays primary-only:

```mermaid
graph TB
    subgraph Apps["Lifeguard apps (ORM)"]
        A1[App 1]
        A2[App 2]
        A3[App N]
    end

    subgraph Reflectors["Reflector instances"]
        Leader[Leader<br/>holds Redlock]
        S1[Standby]
        S2[Standby]
    end

    subgraph Data["Data tier"]
        PG[(PostgreSQL primary)]
        Replica[(PostgreSQL replica)]
        Redis[(Redis)]
    end

    A1 -->|read/write via ORM| PG & Redis & Replica
    A2 -->|read/write via ORM| PG & Redis & Replica
    A3 -->|read/write via ORM| PG & Redis & Replica

    PG -->|streaming replication| Replica
    PG -->|pg_notify payload| Leader
    Leader -->|re-read warm rows| PG
    Leader -->|SETEX / DEL warm keys| Redis
    Leader -. Redlock lease (SET NX PX) .-> Redis
    S1 -. acquire attempt .-> Redis
    S2 -. acquire attempt .-> Redis

    style Leader fill:#fff4e1
    style Redis fill:#ffe1e1
    style PG fill:#e1ffe1
    style Replica fill:#e1f5ff
```

- Standbys talk only to Redis (lock acquire attempts) — they hold no
  PostgreSQL connection until they become leader.
- The `pg_notify` trigger runs on the primary (writes happen there), so
  notifications flow primary → leader; the leader re-reads warm rows from the
  primary.
- There is deliberately no apps → reflector edge: apps never talk to the
  reflector. They coordinate only through shared state — app writes fire the
  `pg_notify` trigger the leader LISTENs, and both sides touch the same Redis
  keys. Apps behave identically with zero, one, or many reflector instances.

**Write + refresh.** Writes always land on the PostgreSQL primary; the ORM
write path invalidates the key immediately (no write-through), and the
`pg_notify` trigger lets the reflector refresh any key that still exists in
Redis:

```mermaid
sequenceDiagram
    participant App as Lifeguard app
    participant PG as PostgreSQL primary
    participant Refl as Reflector (Redlock leader)
    participant Redis as Redis

    App->>PG: INSERT / UPDATE / DELETE
    App->>Redis: invalidate key (explicit, no write-through)
    PG->>PG: trigger pg_notify(channel, '{"table","op","id"}')
    PG-->>Refl: NOTIFY payload (LISTEN)
    Refl->>Redis: EXISTS lifeguard:model:table:id
    alt key absent (never cached / TTL expired)
        Refl->>Refl: ignore — nothing stale to fix
    else key exists (warm)
        Refl->>PG: SELECT * FROM "table" WHERE "pk" = $1
        alt row present
            PG-->>Refl: row
            Refl->>Redis: SETEX key, ttl_seconds
        else row deleted
            Refl->>Redis: DEL key
        end
    end
```

- The reflector connects to the **primary only** — never to replicas.
- Leader election is single-node Redlock; extra instances are hot standbys
  that sleep and retry. There is no Raft consensus group.
- Only warm keys are touched (TTL active set): a key that was never cached or
  already expired is ignored, and a deleted row evicts the key.

**Read.** The ORM's `find_by_id` is cache-aside — Redis first, database only
on miss, populate on the way out:

```mermaid
graph TB
    subgraph Apps["Lifeguard apps (ORM)"]
        App[find_by_id + write path]
    end

    subgraph Data["Data tier"]
        PG[(PostgreSQL primary)]
        Replica[(PostgreSQL replica)]
        Redis[(Redis)]
    end

    Refl[Reflector<br/>Redlock leader]

    App -->|write: INSERT/UPDATE/DELETE| PG
    App -->|write: invalidate key| Redis
    App -->|read: GET lifeguard:model:table:id| Redis
    App -->|populate on miss, TTL 3600| Redis
    App -->|miss: SELECT when replica tier healthy| Replica
    App -->|miss: SELECT otherwise| PG

    PG -->|pg_notify payload| Refl
    Refl -->|re-read warm row| PG
    Refl -->|SETEX / DEL warm keys| Redis

    style Refl fill:#fff4e1
    style Redis fill:#ffe1e1
    style PG fill:#e1ffe1
    style Replica fill:#e1f5ff
```

- Only entities with an integer `id` column are served by this path (the i64
  `find_by_id` signature); UUID-PK keys are populated by the reflector and
  invalidated by the write path.
- The replica tier exists only for `PooledLifeExecutor` reads; the fallback to
  primary is tier-level (no replica configured / WAL-lag unhealthy), never
  per-row.
- Cache errors fail open: a broken Redis silently falls through to the
  database.

## What it does

1. **Leader election** — a single-node Redlock (`SET NX PX` acquire, Lua-CAS
   renewal) ensures only one reflector instance works at a time. Followers
   sleep and retry.
2. **LISTEN/NOTIFY** — entities with `#[notify(channel = "...")]` install a
   `pg_notify` trigger (SQL generated by `lifeguard-migrate`). The reflector
   LISTENs the configured channels and receives `{"table", "op", "id"}`
   payloads on every insert/update/delete.
3. **TTL active-set refresh** — on each notification it checks whether the
   model key `lifeguard:model:<table>:<id>` exists in Redis:
   - **exists** → re-read the row from the primary and `SETEX` it with
     `ttl_seconds` (kept in sync with the ORM's `DEFAULT_CACHE_TTL_SECS`);
   - **row deleted** → evict the key;
   - **absent** → ignore (the key was never cached, or its TTL expired —
     nothing stale to fix).

The ORM side (Lifeguard crate) performs the read-through: `ModelManager::find_by_id`
GETs the key first, populates on miss, and invalidates on insert/update/delete.

## Configuration

```toml
# reflector.toml
pg_url = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
redis_url = "redis://127.0.0.1:6379"

# Channels to LISTEN — must match the entities' #[notify(channel = "...")].
channels = ["lifeguard_updates"]

ttl_seconds = 3600            # keep in sync with the ORM's default cache TTL
lock_key = "lifeguard:reflector:leader"
lock_ttl_ms = 10000           # leader lock TTL
poll_interval_ms = 100        # notification poll interval
log_level = "info"
```

Every field can be overridden from the environment (`REFLECTOR_PG_URL`,
`REFLECTOR_REDIS_URL`, `REFLECTOR_CHANNELS` (comma-separated),
`REFLECTOR_TTL_SECONDS`, `REFLECTOR_LOCK_KEY`, `REFLECTOR_LOCK_TTL_MS`,
`REFLECTOR_POLL_INTERVAL_MS`, `REFLECTOR_LOG_LEVEL`). The config path itself
defaults to `reflector.toml` and can be changed with `--config` or
`REFLECTOR_CONFIG`.

```bash
cargo run -p lifeguard-reflector -- --config reflector.toml
```

## Operational notes

- The reflector connection's `search_path` must resolve the same unqualified
  table names the ORM uses (the trigger payload carries `TG_TABLE_NAME`).
- Single-column primary keys only (matches the `#[notify]` trigger generator);
  composite keys are logged once per table and skipped.
- Supported PK types for refresh: `int2`, `int4`, `int8`, `uuid`, `text`,
  `varchar`, `bpchar`, `name`. Other types leave the existing cached value
  untouched.
- The refresh query binds the id as a typed parameter (`WHERE "id" = $1`) so
  the PK index stays usable.

## Testing

Integration tests (`tests/reflector_integration.rs`) run against a real
PostgreSQL + Redis and skip unless both `TEST_DATABASE_URL` and
`TEST_REDIS_URL` are set. They cover leader election/renewal/expiry, refresh
of a prewarmed key, ignore-inactive-key, DELETE eviction, and malformed
payload robustness:

```bash
export TEST_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5432/postgres"
export TEST_REDIS_URL="redis://127.0.0.1:6379"
cargo nextest run -p lifeguard-reflector
```

The key format and payload contract are asserted end-to-end in the `lifeguard`
crate's `tests/db_integration/cache_transparent.rs` and
`tests/db_integration/notify_trigger.rs`.
