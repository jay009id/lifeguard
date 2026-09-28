//! Cache key helpers shared by the ORM read path, the generated write path,
//! and the `lifeguard-reflector` service.
//!
//! The key format is a cross-crate contract:
//!
//! ```text
//! lifeguard:model:<table>:<primary-key>
//! ```
//!
//! `lifeguard-reflector` builds the same keys from `#[notify]` trigger
//! payloads; both crates' tests assert the exact literal to catch drift.

/// Default TTL for cached models, in seconds.
///
/// `lifeguard-reflector`'s `ttl_seconds` should be configured to match this
/// so reflected keys live exactly as long as the ORM-populated ones.
pub const DEFAULT_CACHE_TTL_SECS: u64 = 3600;

/// The prefix shared by every model cache key (`lifeguard:model`).
pub const CACHE_KEY_PREFIX: &str = "lifeguard:model";

/// Builds the canonical model cache key: `lifeguard:model:<table>:<pk>`.
#[must_use]
pub fn model_cache_key(table: &str, pk: &str) -> String {
    format!("{CACHE_KEY_PREFIX}:{table}:{pk}")
}

/// Renders a primary key value for use in a cache key.
///
/// Returns `None` for types with no unambiguous string form (`Json`, arrays,
/// bytes, floats) or for `NULL` values. The caller then skips cache handling
/// for that key rather than guessing.
#[must_use]
pub fn cache_key_pk_string(value: &sea_query::Value) -> Option<String> {
    match value {
        sea_query::Value::Bool(Some(v)) => Some(v.to_string()),
        sea_query::Value::TinyInt(Some(v)) => Some(v.to_string()),
        sea_query::Value::SmallInt(Some(v)) => Some(v.to_string()),
        sea_query::Value::Int(Some(v)) => Some(v.to_string()),
        sea_query::Value::BigInt(Some(v)) => Some(v.to_string()),
        sea_query::Value::TinyUnsigned(Some(v)) => Some(v.to_string()),
        sea_query::Value::SmallUnsigned(Some(v)) => Some(v.to_string()),
        sea_query::Value::Unsigned(Some(v)) => Some(v.to_string()),
        sea_query::Value::BigUnsigned(Some(v)) => Some(v.to_string()),
        sea_query::Value::String(Some(v)) => Some(v.clone()),
        sea_query::Value::Char(Some(v)) => Some(v.to_string()),
        sea_query::Value::Uuid(Some(v)) => Some(v.to_string()),
        sea_query::Value::Decimal(Some(v)) => Some(v.to_string()),
        sea_query::Value::ChronoDate(Some(v)) => Some(v.to_string()),
        sea_query::Value::ChronoDateTimeUtc(Some(v)) => Some(v.to_rfc3339()),
        // Everything else either has no stable string form or is NULL.
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn model_cache_key_format_exact() {
        assert_eq!(model_cache_key("users", "42"), "lifeguard:model:users:42");
        assert_eq!(
            model_cache_key("order_items", "7f40d003-3a4b-4c0a-9b1e-123456789abc"),
            "lifeguard:model:order_items:7f40d003-3a4b-4c0a-9b1e-123456789abc"
        );
    }

    #[test]
    fn pk_string_covers_integer_variants() {
        use sea_query::Value;
        assert_eq!(
            cache_key_pk_string(&Value::Bool(Some(true))),
            Some("true".to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::TinyInt(Some(-8))),
            Some("-8".to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::SmallInt(Some(-16))),
            Some("-16".to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::Int(Some(32))),
            Some("32".to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::BigInt(Some(64))),
            Some("64".to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::TinyUnsigned(Some(8))),
            Some("8".to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::SmallUnsigned(Some(16))),
            Some("16".to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::Unsigned(Some(32))),
            Some("32".to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::BigUnsigned(Some(64))),
            Some("64".to_string())
        );
    }

    #[test]
    fn pk_string_uuid_string_char_decimal() {
        use sea_query::Value;
        let uuid = "7f40d003-3a4b-4c0a-9b1e-123456789abc";
        assert_eq!(
            cache_key_pk_string(&Value::Uuid(Some(uuid.parse().unwrap()))),
            Some(uuid.to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::String(Some("abc-123".to_string()))),
            Some("abc-123".to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::Char(Some('x'))),
            Some("x".to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::Decimal(Some("12.50".parse().unwrap()))),
            Some("12.50".to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::ChronoDate(Some(
                chrono::NaiveDate::from_ymd_opt(2026, 9, 28).unwrap()
            ))),
            Some("2026-09-28".to_string())
        );
        assert_eq!(
            cache_key_pk_string(&Value::ChronoDateTimeUtc(Some(
                chrono::DateTime::parse_from_rfc3339("2026-09-28T12:00:00Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc)
            ))),
            Some("2026-09-28T12:00:00+00:00".to_string())
        );
    }

    #[test]
    fn pk_string_none_for_null_inner() {
        use sea_query::Value;
        assert_eq!(cache_key_pk_string(&Value::BigInt(None)), None);
        assert_eq!(cache_key_pk_string(&Value::String(None)), None);
        assert_eq!(cache_key_pk_string(&Value::Uuid(None)), None);
    }

    #[test]
    fn pk_string_none_for_unsupported() {
        use sea_query::Value;
        assert_eq!(cache_key_pk_string(&Value::Float(Some(1.5))), None);
        assert_eq!(cache_key_pk_string(&Value::Double(Some(1.5))), None);
        assert_eq!(
            cache_key_pk_string(&Value::Bytes(Some(vec![1, 2, 3]))),
            None
        );
        assert_eq!(
            cache_key_pk_string(&Value::Json(Some(Box::new(serde_json::json!({"a": 1}))))),
            None
        );
        assert_eq!(
            cache_key_pk_string(&Value::ChronoDateTime(Some(
                chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc()
            ))),
            None
        );
    }
}
