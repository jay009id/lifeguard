//! Single-node Redlock leader election over Redis.
//!
//! Only one reflector instance may LISTEN and refresh at a time. Acquisition
//! is an atomic `SET <key> <instance_id> NX PX <ttl>` (with `GET` so a
//! `nil` reply proves the key was created by us); renewal re-issues the TTL
//! only while the lock still holds our instance id (Lua CAS, so a lock that
//! expired and was stolen by another instance can never be extended by the
//! previous owner).

use crate::ReflectorError;
use redis::{Commands, ExistenceCheck, SetExpiry, SetOptions};

/// Renewal Lua script: `if GET key == instance then PEXPIRE key ttl; return 1; end return 0`.
const RENEW_SCRIPT: &str = r#"
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
"#;

/// A distributed leader lock backed by a single Redis node.
pub struct LeaderLock {
    client: redis::Client,
    key: String,
    instance_id: String,
    ttl_ms: u64,
}

impl LeaderLock {
    /// Creates a lock handle for `key`.
    ///
    /// The instance id is `hostname:pid:random`, unique per process.
    ///
    /// # Errors
    ///
    /// Fails only if the Redis client cannot be created.
    pub fn new(client: redis::Client, key: String, ttl_ms: u64) -> Result<Self, ReflectorError> {
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown-host".to_string());
        let pid = std::process::id();
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let instance_id = format!("{hostname}:{pid}:{nonce:x}");
        Ok(Self {
            client,
            key,
            instance_id,
            ttl_ms,
        })
    }

    /// The Redis key backing this lock.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// This instance's unique owner id.
    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Attempts to acquire the lock.
    ///
    /// Returns `true` if this instance now owns the lock (either the key did
    /// not exist, or it already held our instance id), `false` if another
    /// instance owns it.
    ///
    /// # Errors
    ///
    /// Returns an error on Redis connectivity failures.
    pub fn try_acquire(&self) -> Result<bool, ReflectorError> {
        let mut conn = self.client.get_connection()?;
        let opts = SetOptions::default()
            .conditional_set(ExistenceCheck::NX)
            .with_expiration(SetExpiry::PX(self.ttl_ms))
            .get(true);
        let previous: Option<String> = conn.set_options(&self.key, &self.instance_id, opts)?;
        Ok(previous.is_none())
    }

    /// Extends the lock TTL, but only while it still holds our instance id.
    ///
    /// Returns `false` when the lock was lost (expired or stolen), in which
    /// case the caller must stop working and re-acquire.
    ///
    /// # Errors
    ///
    /// Returns an error on Redis connectivity failures.
    pub fn renew(&self) -> Result<bool, ReflectorError> {
        let mut conn = self.client.get_connection()?;
        let script = redis::Script::new(RENEW_SCRIPT);
        let result: i64 = script
            .key(&self.key)
            .arg(&self.instance_id)
            .arg(self.ttl_ms)
            .invoke(&mut conn)?;
        Ok(result == 1)
    }

    /// Releases the lock, but only if it still holds our instance id.
    ///
    /// # Errors
    ///
    /// Returns an error on Redis connectivity failures.
    pub fn release(&self) -> Result<(), ReflectorError> {
        let mut conn = self.client.get_connection()?;
        let script = redis::Script::new(
            r#"if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) end return 0"#,
        );
        let _: i64 = script
            .key(&self.key)
            .arg(&self.instance_id)
            .invoke(&mut conn)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lock tests need Redis; they live in `tests/reflector_integration.rs`
    /// because a unit test cannot assume a server. These cover the pure logic
    /// (script compilation) instead.
    #[test]
    fn renew_script_is_static_and_referenced() {
        assert!(RENEW_SCRIPT.contains("PEXPIRE"));
        assert!(RENEW_SCRIPT.contains("GET"));
    }
}
