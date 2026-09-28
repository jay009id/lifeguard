//! Lifeguard Reflector CLI — starts the cache coherence worker.
//!
//! Configuration comes from a TOML file (`--config`, default
//! `reflector.toml`, overridable via `REFLECTOR_CONFIG`) plus `REFLECTOR_*`
//! environment variable overrides. See
//! [`lifeguard_reflector::ReflectorConfig`].

// Same pedantic convention as the `lifeguard` crate.
#![allow(clippy::pedantic)]

use clap::Parser;
use lifeguard_reflector::{Reflector, ReflectorConfig};

#[derive(Parser)]
#[command(name = "lifeguard-reflector")]
#[command(about = "Cache coherence microservice for the Lifeguard ORM")]
#[command(version)]
struct Cli {
    /// Path to the TOML config file (env: REFLECTOR_CONFIG)
    #[arg(long, default_value = "reflector.toml")]
    config: std::path::PathBuf,
}

fn main() {
    let cli = Cli::parse();

    let config_path = std::env::var("REFLECTOR_CONFIG")
        .map(std::path::PathBuf::from)
        .unwrap_or(cli.config);

    let config = match ReflectorConfig::from_file(&config_path) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("failed to load {}: {e}", config_path.display());
            std::process::exit(2);
        }
    }
    .with_env_overrides();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(&config.log_level))
        .init();

    log::info!(
        "starting lifeguard-reflector (pg={}, redis={}, channels={:?}, ttl={}s)",
        redact_url(&config.pg_url),
        redact_url(&config.redis_url),
        config.channels,
        config.ttl_seconds
    );

    let mut reflector = match Reflector::new(config) {
        Ok(reflector) => reflector,
        Err(e) => {
            log::error!("failed to initialize reflector: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = reflector.run() {
        log::error!("reflector stopped: {e}");
        std::process::exit(1);
    }
}

/// Redacts userinfo from a URL for logging.
fn redact_url(url: &str) -> String {
    if let Some(at) = url.rfind('@') {
        if url[..at].contains("://") {
            let scheme_end = url.find("://").map_or(0, |i| i + 3);
            return format!("{}***{}", &url[..scheme_end], &url[at..]);
        }
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::redact_url;

    #[test]
    fn redacts_userinfo() {
        assert_eq!(
            redact_url("postgres://user:secret@127.0.0.1:5432/db"),
            "postgres://***@127.0.0.1:5432/db"
        );
        assert_eq!(
            redact_url("redis://127.0.0.1:6379"),
            "redis://127.0.0.1:6379"
        );
    }
}
