# Story 04: Redis Integration for Transparent Caching

## Description

Integrate Redis into Lifeguard's read path. LifeModel queries should transparently check Redis first, falling back to database on cache miss.

## Acceptance Criteria

- [x] LifeModel queries check Redis first
- [x] Cache hit: return data from Redis (sub-millisecond)
- [x] Cache miss: read from database, populate Redis
- [x] TTL-based expiration (configurable per model)
- [x] Cache key format: `lifeguard:model:<table>:<id>`
- [x] Unit tests demonstrate transparent caching

**Shipped** for `ModelManager::find_by_id` (cache-aside GET → miss → Postgres → `SETEX`),
activated by `CachedLifeExecutor` / `MayPostgresExecutor::with_cache_provider` +
`RedisCacheProvider`. Tests: `tests/db_integration/cache_transparent.rs`. Deviations from
the sketch below: TTL is a global default (`lifeguard::cache::keys::DEFAULT_CACHE_TTL_SECS`),
not per-model; the write path performs **explicit invalidation** (DEL on
insert/update/delete) rather than NOTIFY-per-write — the reflector refreshes warm keys
asynchronously on top of that. Other read APIs (`find`, `find_related`, raw SQL) do not
consult the cache yet; only integer primary keys named `id` are served.

## Technical Details

- Use `redis` crate for Redis client
- Read path:
  1. Check Redis: `GET lifeguard:model:<table>:<id>`
  2. If hit: deserialize and return
  3. If miss: read from database, serialize and cache
- Write path: LifeRecord triggers NOTIFY (LifeReflector handles cache refresh)
- TTL configuration: per-model or global default
- Serialization: use `serde` (JSON or MessagePack)

## Dependencies

- Epic 04: v1 Release
- Redis (external service)

## Notes

- This is the "transparent" part - application code doesn't change
- Cache hit rate should be 99%+ for Pricewhisperers scale
- Consider cache warming strategies

