//! Cache Coherence Architecture
//!
//! Exposes the transparent cache layer for `LifeModel` reads and
//! `LifeRecord` writes: the [`CacheProvider`] abstraction, its default and
//! Redis implementations, and the shared key helpers.
//!
//! The read path (`ModelManager::find_by_id`) performs cache-aside
//! GET → miss → Postgres → SET; the generated write path invalidates keys on
//! insert/update/delete. Cross-service cache refresh after commits is the
//! job of the [`lifeguard-reflector`](../../lifeguard-reflector/) microservice.

pub mod keys;
pub mod provider;
pub mod redis_provider;

pub use keys::{cache_key_pk_string, model_cache_key, CACHE_KEY_PREFIX, DEFAULT_CACHE_TTL_SECS};
pub use provider::{CacheError, CacheProvider, CachedResult, DefaultCacheProvider};
pub use redis_provider::RedisCacheProvider;
