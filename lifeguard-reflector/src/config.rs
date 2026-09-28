//! Reflector configuration: TOML file plus `REFLECTOR_*` environment overrides.

use serde::Deserialize;

/// TTL applied to refreshed cache keys, in seconds.
///
/// Keep in sync with `lifeguard::cache::keys::DEFAULT_CACHE_TTL_SECS` so
/// reflected keys live exactly as long as ORM-populated ones.
pub const DEFAULT_TTL_SECS: u64 = 3600;

/// Default lock key used for leader election.
pub const DEFAULT_LOCK_KEY: &str = "lifeguard:reflector:leader";

/// Reflector configuration, TOML-decodable.
///
/// ```toml
/// pg_url = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
/// redis_url = "redis://127.0.0.1:6379"
/// channels = ["lifeguard_updates"]
/// ttl_seconds = 3600
/// lock_key = "lifeguard:reflector:leader"
/// lock_ttl_ms = 10000
/// poll_interval_ms = 100
/// log_level = "info"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ReflectorConfig {
    /// PostgreSQL connection URL (primary). Required.
    pub pg_url: String,
    /// Redis connection URL. Required.
    pub redis_url: String,
    /// Notification channels to `LISTEN` on. Must match the channels declared
    /// by `#[notify(channel = "...")]` on entities. Required (non-empty).
    pub channels: Vec<String>,
    /// TTL for refreshed cache keys, in seconds.
    pub ttl_seconds: u64,
    /// Redis key holding the leader lock.
    pub lock_key: String,
    /// Leader lock TTL in milliseconds.
    pub lock_ttl_ms: u64,
    /// Sleep between notification polls, in milliseconds.
    pub poll_interval_ms: u64,
    /// `env_logger` level (`error`, `warn`, `info`, `debug`, `trace`).
    pub log_level: String,
}

impl Default for ReflectorConfig {
    fn default() -> Self {
        Self {
            pg_url: String::new(),
            redis_url: String::new(),
            channels: Vec::new(),
            ttl_seconds: DEFAULT_TTL_SECS,
            lock_key: DEFAULT_LOCK_KEY.to_string(),
            lock_ttl_ms: 10_000,
            poll_interval_ms: 100,
            log_level: "info".to_string(),
        }
    }
}

/// Validates that an identifier-ish config value (channel, table fragments)
/// contains only `[A-Za-z_][A-Za-z0-9_]*` characters before it is embedded in
/// SQL.
pub fn is_safe_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl ReflectorConfig {
    /// Parses TOML and validates the result.
    ///
    /// # Errors
    ///
    /// Returns [`ReflectorError::Config`] when the TOML is invalid or the
    /// required fields are missing/invalid.
    pub fn from_toml_str(toml_str: &str) -> Result<Self, crate::ReflectorError> {
        let config: Self =
            toml::from_str(toml_str).map_err(|e| crate::ReflectorError::Config(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    /// Loads configuration from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or parsed, or when the
    /// contents fail validation.
    pub fn from_file(path: &std::path::Path) -> Result<Self, crate::ReflectorError> {
        let toml_str = std::fs::read_to_string(path)?;
        Self::from_toml_str(&toml_str)
    }

    /// Applies `REFLECTOR_*` environment variable overrides in place.
    ///
    /// | Variable | Field |
    /// |---|---|
    /// | `REFLECTOR_PG_URL` | `pg_url` |
    /// | `REFLECTOR_REDIS_URL` | `redis_url` |
    /// | `REFLECTOR_CHANNELS` (comma-separated) | `channels` |
    /// | `REFLECTOR_TTL_SECONDS` | `ttl_seconds` |
    /// | `REFLECTOR_LOCK_KEY` | `lock_key` |
    /// | `REFLECTOR_LOCK_TTL_MS` | `lock_ttl_ms` |
    /// | `REFLECTOR_POLL_INTERVAL_MS` | `poll_interval_ms` |
    /// | `REFLECTOR_LOG_LEVEL` | `log_level` |
    pub fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("REFLECTOR_PG_URL") {
            if !v.trim().is_empty() {
                self.pg_url = v;
            }
        }
        if let Ok(v) = std::env::var("REFLECTOR_REDIS_URL") {
            if !v.trim().is_empty() {
                self.redis_url = v;
            }
        }
        if let Ok(v) = std::env::var("REFLECTOR_CHANNELS") {
            let channels: Vec<String> = v
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            if !channels.is_empty() {
                self.channels = channels;
            }
        }
        if let Ok(v) = std::env::var("REFLECTOR_TTL_SECONDS") {
            if let Ok(n) = v.trim().parse::<u64>() {
                self.ttl_seconds = n;
            }
        }
        if let Ok(v) = std::env::var("REFLECTOR_LOCK_KEY") {
            if !v.trim().is_empty() {
                self.lock_key = v;
            }
        }
        if let Ok(v) = std::env::var("REFLECTOR_LOCK_TTL_MS") {
            if let Ok(n) = v.trim().parse::<u64>() {
                self.lock_ttl_ms = n;
            }
        }
        if let Ok(v) = std::env::var("REFLECTOR_POLL_INTERVAL_MS") {
            if let Ok(n) = v.trim().parse::<u64>() {
                self.poll_interval_ms = n;
            }
        }
        if let Ok(v) = std::env::var("REFLECTOR_LOG_LEVEL") {
            if !v.trim().is_empty() {
                self.log_level = v;
            }
        }
    }

    /// Consuming form of [`apply_env_overrides`](Self::apply_env_overrides).
    #[must_use]
    pub fn with_env_overrides(mut self) -> Self {
        self.apply_env_overrides();
        self
    }

    fn validate(&self) -> Result<(), crate::ReflectorError> {
        if self.pg_url.trim().is_empty() {
            return Err(crate::ReflectorError::Config(
                "pg_url is required".to_string(),
            ));
        }
        if self.redis_url.trim().is_empty() {
            return Err(crate::ReflectorError::Config(
                "redis_url is required".to_string(),
            ));
        }
        if self.channels.is_empty() {
            return Err(crate::ReflectorError::Config(
                "at least one channel is required".to_string(),
            ));
        }
        for channel in &self.channels {
            if !is_safe_identifier(channel) {
                return Err(crate::ReflectorError::Config(format!(
                    "invalid channel name {channel:?}: expected [A-Za-z_][A-Za-z0-9_]*"
                )));
            }
        }
        if self.ttl_seconds == 0 {
            return Err(crate::ReflectorError::Config(
                "ttl_seconds must be > 0".to_string(),
            ));
        }
        if self.lock_ttl_ms == 0 {
            return Err(crate::ReflectorError::Config(
                "lock_ttl_ms must be > 0".to_string(),
            ));
        }
        if self.poll_interval_ms == 0 {
            return Err(crate::ReflectorError::Config(
                "poll_interval_ms must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL_TOML: &str = r#"
pg_url = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
redis_url = "redis://127.0.0.1:6379"
channels = ["lifeguard_updates", "users_changes"]
"#;

    #[test]
    fn toml_roundtrip_defaults() {
        let config = ReflectorConfig::from_toml_str(MINIMAL_TOML).expect("valid toml");
        assert_eq!(
            config.pg_url,
            "postgres://postgres:postgres@127.0.0.1:5432/postgres"
        );
        assert_eq!(config.redis_url, "redis://127.0.0.1:6379");
        assert_eq!(config.channels, vec!["lifeguard_updates", "users_changes"]);
        assert_eq!(config.ttl_seconds, 3600);
        assert_eq!(config.lock_key, "lifeguard:reflector:leader");
        assert_eq!(config.lock_ttl_ms, 10_000);
        assert_eq!(config.poll_interval_ms, 100);
        assert_eq!(config.log_level, "info");
    }

    #[test]
    fn toml_explicit_fields_win() {
        let toml_str = r#"
pg_url = "postgres://x"
redis_url = "redis://y"
channels = ["c"]
ttl_seconds = 60
lock_key = "custom:lock"
lock_ttl_ms = 500
poll_interval_ms = 50
log_level = "debug"
"#;
        let config = ReflectorConfig::from_toml_str(toml_str).expect("valid toml");
        assert_eq!(config.ttl_seconds, 60);
        assert_eq!(config.lock_key, "custom:lock");
        assert_eq!(config.lock_ttl_ms, 500);
        assert_eq!(config.poll_interval_ms, 50);
        assert_eq!(config.log_level, "debug");
    }

    #[test]
    fn missing_required_fields_rejected() {
        assert!(ReflectorConfig::from_toml_str("pg_url = \"postgres://x\"\n").is_err());
        assert!(ReflectorConfig::from_toml_str(
            "pg_url = \"postgres://x\"\nredis_url = \"redis://y\"\nchannels = []\n"
        )
        .is_err());
    }

    #[test]
    fn invalid_channel_rejected() {
        assert!(ReflectorConfig::from_toml_str(
            "pg_url = \"postgres://x\"\nredis_url = \"redis://y\"\nchannels = [\"evil; DROP TABLE x\"]\n"
        )
        .is_err());
    }

    #[test]
    fn zero_positive_fields_rejected() {
        assert!(ReflectorConfig::from_toml_str(
            "pg_url = \"postgres://x\"\nredis_url = \"redis://y\"\nchannels = [\"c\"]\nttl_seconds = 0\n"
        )
        .is_err());
    }

    #[test]
    fn env_overrides_apply() {
        // Env mutation is process-global; this test only runs among unit
        // tests in this binary, none of which touch REFLECTOR_* concurrently.
        std::env::set_var("REFLECTOR_TTL_SECONDS", "42");
        std::env::set_var("REFLECTOR_CHANNELS", " a, b ,c ");
        std::env::set_var("REFLECTOR_PG_URL", "postgres://env");
        let mut config = ReflectorConfig::from_toml_str(MINIMAL_TOML).expect("valid toml");
        config.apply_env_overrides();
        assert_eq!(config.ttl_seconds, 42);
        assert_eq!(config.channels, vec!["a", "b", "c"]);
        assert_eq!(config.pg_url, "postgres://env");
        // Non-numeric override is ignored, not fatal.
        std::env::set_var("REFLECTOR_TTL_SECONDS", "not-a-number");
        config.apply_env_overrides();
        assert_eq!(config.ttl_seconds, 42);
        std::env::remove_var("REFLECTOR_TTL_SECONDS");
        std::env::remove_var("REFLECTOR_CHANNELS");
        std::env::remove_var("REFLECTOR_PG_URL");
    }
}
