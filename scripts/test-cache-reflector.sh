#!/bin/bash
# Cache & reflector test runner for Lifeguard.
#
# Runs every test layer for the transparent Redis cache-aside path and the
# lifeguard-reflector service, and (with --full) the regression suites.
# Full instructions: docs/TESTING_CACHE_REFLECTOR.md
#
# Defaults target the local postgres-replication-docker stack (primary 35432,
# replica 35433, Redis 36379). Override per environment variable:
#   LG_TEST_PG_URL / LG_TEST_REPLICA_URL / LG_TEST_REDIS_URL
# or set the canonical TEST_DATABASE_URL / TEST_REPLICA_URL / TEST_REDIS_URL
# directly (they win).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

FULL=0
for arg in "$@"; do
  case "$arg" in
    --full) FULL=1 ;;
    -h | --help)
      sed -n '2,13p' "$0"
      exit 0
      ;;
    *)
      echo "unknown argument: $arg (use --full or --help)" >&2
      exit 2
      ;;
  esac
done

export TEST_DATABASE_URL="${TEST_DATABASE_URL:-${LG_TEST_PG_URL:-postgres://postgres:postgres@127.0.0.1:35432/postgres?options=-c%20search_path%3Dlifeguard}}"
export TEST_REPLICA_URL="${TEST_REPLICA_URL:-${LG_TEST_REPLICA_URL:-postgres://postgres:postgres@127.0.0.1:35433/postgres?options=-c%20search_path%3Dlifeguard}}"
export TEST_REDIS_URL="${TEST_REDIS_URL:-${LG_TEST_REDIS_URL:-redis://127.0.0.1:36379}}"

echo "TEST_DATABASE_URL=$TEST_DATABASE_URL"
echo "TEST_REPLICA_URL=$TEST_REPLICA_URL"
echo "TEST_REDIS_URL=$TEST_REDIS_URL"

echo "==> Unit tests: cache helpers"
cargo test -p lifeguard --lib cache::

echo "==> Unit tests: CachedLifeExecutor decorator"
cargo test -p lifeguard --lib cached_executor

echo "==> Unit tests: reflector config"
cargo test -p lifeguard-reflector --lib

echo "==> Integration: cache-aside (db suite, cache_transparent module)"
cargo nextest run -p lifeguard --all-features --profile db-serial \
  --config-file .config/nextest.toml \
  -E 'binary(db_integration_suite) and test(cache_transparent)'

echo "==> Integration: reflector (env-gated)"
cargo nextest run -p lifeguard-reflector --test reflector_integration

if [ "$FULL" -eq 1 ]; then
  echo "==> Regression: full db_integration_suite (serial)"
  cargo nextest run -p lifeguard --all-features --profile db-serial \
    --config-file .config/nextest.toml -E 'binary(db_integration_suite)'

  echo "==> Regression: whole workspace (CI parity)"
  cargo nextest run --workspace --all-features --fail-fast --retries 1 \
    --exclude lifeguard-integration-tests -E 'not binary(db_integration_suite)'
fi

echo "All cache & reflector tests passed."
