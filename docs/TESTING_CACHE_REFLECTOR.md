# Testing the transparent cache & LifeReflector

> Routing semantics (when Redis activates, write→primary / read→Redis→replica
> →primary contract) and the proof matrix: see
> [CACHE_ROUTING_PROOF.md](CACHE_ROUTING_PROOF.md).

Runbook for every test layer covering the transparent Redis cache-aside path
(`find_by_id`) and the `lifeguard-reflector` microservice. One-command
shortcut: [`scripts/test-cache-reflector.sh`](../scripts/test-cache-reflector.sh)
(run it from the repo root).

## Test layers

| Layer | Location | Requires | Proves |
|---|---|---|---|
| Key/PK helpers (unit) | `src/cache/keys.rs` | — | exact key format `lifeguard:model:<table>:<id>`, PK rendering for every supported type |
| `CachedLifeExecutor` (unit) | `src/executor.rs` (`cached_executor_tests`) | — | delegation + `cache_provider()` override |
| Reflector config (unit) | `lifeguard-reflector/src/config.rs` | — | TOML roundtrip, validation, `REFLECTOR_*` env overrides |
| Cache-aside (integration) | `tests/db_integration/cache_transparent.rs` | Postgres + Redis | miss populates the contract key with the model's JSON; hits are served from Redis (an out-of-band SQL update stays invisible until the key is dropped); insert/update/delete invalidate; UUID PKs invalidate a reflector-seeded key; decorator parity with the builder |
| Reflector (integration) | `lifeguard-reflector/tests/reflector_integration.rs` | Postgres + Redis | Redlock acquire/renew/expiry; NOTIFY → refresh of a prewarmed key; inactive keys ignored; DELETE eviction; malformed payloads do not crash the tick |

## Prerequisites

- Rust toolchain + `cargo nextest` (repo pins nextest; see
  [TEST_INFRASTRUCTURE.md](TEST_INFRASTRUCTURE.md)).
- A running PostgreSQL and Redis. Either:
  - **Local stack** — the sibling `postgres-replication-docker` checkout
    (primary `35432`, replicas `35433`/`35434`, Redis `36379`), or
  - **CI Compose** — [`.github/docker/docker-compose.yml`](../.github/docker/docker-compose.yml)
    (primary `6543`, Toxiproxy replica `6547`, Redis `6545`), or
  - **testcontainers** — when `TEST_DATABASE_URL` is unset, the
    `db_integration_suite` provisions disposable Postgres + Redis containers.

## Environment variables

| Variable | Role |
|---|---|
| `TEST_DATABASE_URL` | Primary Postgres for the db suite and reflector tests. |
| `TEST_REPLICA_URL` | Optional streaming standby (enables `pool_read_replica`; unused by the cache tests). |
| `TEST_REDIS_URL` | Redis for the cache tests and the reflector tests. |

Skip behavior:

- `cache_transparent` tests skip themselves when Redis is unreachable.
- `reflector_integration` skips entirely unless **both** `TEST_DATABASE_URL`
  and `TEST_REDIS_URL` are set.
- The reflector tests are serialized by the `lifeguard-shared-reflector`
  nextest test group (they share one table + NOTIFY channel + Redis key).

## Quick start (local stack)

```bash
# 1) Start the stack (if not already running)
cd /path/to/postgres-replication-docker && docker compose up -d

# 2) Run everything for the cache/reflector work
cd /path/to/lifeguard
export TEST_DATABASE_URL="postgres://postgres:postgres@127.0.0.1:35432/postgres?options=-c%20search_path%3Dlifeguard"
export TEST_REPLICA_URL="postgres://postgres:postgres@127.0.0.1:35433/postgres?options=-c%20search_path%3Dlifeguard"
export TEST_REDIS_URL="redis://127.0.0.1:36379"

./scripts/test-cache-reflector.sh            # unit + cache + reflector
./scripts/test-cache-reflector.sh --full     # + full db suite + workspace (CI parity)
```

The `lifeguard` schema must exist on the primary (create once with
`CREATE SCHEMA IF NOT EXISTS lifeguard;`) or drop the
`?options=-c%20search_path%3Dlifeguard` suffix from the URLs.

## Running each layer manually

```bash
# Unit tests (no infrastructure)
cargo test -p lifeguard --lib cache::
cargo test -p lifeguard --lib cached_executor
cargo test -p lifeguard-reflector --lib

# Cache-aside integration tests (7 tests, db suite filter)
cargo nextest run -p lifeguard --all-features --profile db-serial \
  --config-file .config/nextest.toml \
  -E 'binary(db_integration_suite) and test(cache_transparent)'

# Reflector integration tests (5 tests)
cargo nextest run -p lifeguard-reflector --test reflector_integration

# Regression: full db_integration_suite (serial; the derive write-path change
# touches insert/update/delete for every entity test)
cargo nextest run -p lifeguard --all-features --profile db-serial \
  --config-file .config/nextest.toml -E 'binary(db_integration_suite)'

# CI parity: whole workspace (includes the reflector crate's tests)
cargo nextest run --workspace --all-features --fail-fast --retries 1 \
  --exclude lifeguard-integration-tests -E 'not binary(db_integration_suite)'
```

## Manual smoke test of the reflector binary

```bash
cargo run -p lifeguard-reflector -- \
  --config lifeguard-reflector/reflector.example.toml
# (set REFLECTOR_PG_URL / REFLECTOR_REDIS_URL / REFLECTOR_CHANNELS to match
# your stack and the entities' #[notify(channel = "...")])

# In another shell: UPDATE/DELETE a row in a table with a #[notify] trigger,
# then watch the Redis key refresh (UPDATE) or disappear (DELETE):
#   redis-cli GET lifeguard:model:<table>:<id>
```

## CI

The `test` job already runs the db suite and workspace nextest with
`TEST_DATABASE_URL`, `TEST_REPLICA_URL`, and `TEST_REDIS_URL` pointed at the
Compose stack, so `cache_transparent` and `reflector_integration` run there
automatically (see `.github/workflows/ci.yaml` and
[TEST_INFRASTRUCTURE.md](TEST_INFRASTRUCTURE.md)).
