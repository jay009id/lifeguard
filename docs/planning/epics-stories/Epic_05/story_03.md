# Story 03: LifeReflector - Redis Cache Coherence

## Description

Implement Redis cache coherence in LifeReflector. When a database write occurs, LifeReflector should refresh the corresponding Redis cache entry.

## Acceptance Criteria

- [x] LifeReflector checks Redis for cache key existence
- [x] If key exists, LifeReflector fetches fresh data from PostgreSQL
- [x] LifeReflector updates Redis with fresh data (TTL-based)
- [x] If key doesn't exist, LifeReflector ignores (inactive item)
- [x] TTL-based active set (only active items cached)
- [x] Unit tests demonstrate cache coherence

**Shipped** in the `lifeguard-reflector` crate (Redlock leader election, LISTEN/NOTIFY
consumption, active-set refresh, DELETE eviction). Tests:
`lifeguard-reflector/tests/reflector_integration.rs` (env-gated). Deviation from the
sketch below: the row lookup binds the id as a typed parameter matching the PK column's
type (`WHERE "id" = $1`), not a `CAST($1 AS ...)` — the CAST made the server infer the
wrong parameter type at prepare time.

## Technical Details

- Redis key format: `lifeguard:model:<table>:<id>`
- On NOTIFY:
  1. Parse notification (table, id)
  2. Check Redis: `EXISTS lifeguard:model:<table>:<id>`
  3. If exists: `SELECT * FROM table WHERE id = $1` (from primary)
  4. Update Redis: `SETEX key TTL serialized_data`
  5. If not exists: ignore (TTL expired, item inactive)
- Use TTL to manage active set (inactive items expire)

## Dependencies

- Story 02: LifeReflector - PostgreSQL LISTEN/NOTIFY Integration
- Redis (external service)

## Notes

- This prevents cache stampedes
- TTL-based active set is key to scalability
- Only active users' data stays cached

