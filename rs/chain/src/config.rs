//! The plugin's config channel. avalanchego's subprocess runtime forwards
//! only `GRPC_*` / `GODEBUG*` to a plugin, so the `EPOCHDB_*` variables the
//! store reads (rs/store `casfs.rs`, `db.rs`) never reach `epochdb-rs` under
//! a host. The chain config JSON (Initialize's `config_bytes`) carries them
//! instead: every key below is set into this process's environment before
//! the store opens, so the store keeps its one env-reading code path and the
//! bench / serve tools keep working from a plain environment.
//!
//! The rule is mechanical: `EPOCHDB_S3_ENDPOINT` <-> `s3-endpoint` (strip the
//! prefix, lower case, `_` -> `-`). `cmd/epochdb-host` applies the same rule
//! forward to translate its own `EPOCHDB_*` environment into these bytes.
//! `roll-budget-mb` is the engine's own key and is read straight from the
//! JSON by `node_engine`.

/// config key -> environment variable, the single list. Values may be JSON
/// strings, numbers or booleans (`true` -> `1`, `false` -> `0`).
pub const KEYS: &[(&str, &str)] = &[
    ("s3-endpoint", "EPOCHDB_S3_ENDPOINT"),
    ("s3-region", "EPOCHDB_S3_REGION"),
    ("s3-bucket", "EPOCHDB_S3_BUCKET"),
    ("s3-prefix", "EPOCHDB_S3_PREFIX"),
    ("s3-access-key", "EPOCHDB_S3_ACCESS_KEY"),
    ("s3-secret-key", "EPOCHDB_S3_SECRET_KEY"),
    ("cache-dir", "EPOCHDB_CACHE_DIR"),
    ("cache-min-free", "EPOCHDB_CACHE_MIN_FREE"),
    ("cache-max-age", "EPOCHDB_CACHE_MAX_AGE"),
    ("terminal-txs", "EPOCHDB_TERMINAL_TXS"),
    ("window-max-bytes", "EPOCHDB_WINDOW_MAX_BYTES"),
    ("new-chain", "EPOCHDB_NEW_CHAIN"),
];

/// Keys whose values are never printed.
pub const SECRET: &[&str] = &["s3-access-key", "s3-secret-key"];

fn scalar(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(if *b { "1" } else { "0" }.into()),
        _ => None,
    }
}

/// Sets the environment from the config bytes; returns the keys applied.
/// A key present in the JSON wins over an inherited variable; absent keys
/// leave the environment alone (the fallback for the tools).
pub fn apply(config_bytes: &[u8]) -> Vec<&'static str> {
    let conf: serde_json::Value = serde_json::from_slice(config_bytes).unwrap_or(serde_json::Value::Null);
    let mut applied = Vec::new();
    for (key, var) in KEYS {
        if let Some(v) = conf.get(key).and_then(scalar) {
            std::env::set_var(var, v);
            applied.push(*key);
        }
    }
    applied
}

/// The config JSON with secret values replaced, for a log line.
pub fn redacted(config_bytes: &[u8]) -> String {
    match serde_json::from_slice::<serde_json::Value>(config_bytes) {
        Ok(mut v) => {
            if let Some(m) = v.as_object_mut() {
                for k in SECRET {
                    if m.contains_key(*k) {
                        m.insert((*k).into(), "<redacted>".into());
                    }
                }
            }
            v.to_string()
        }
        Err(_) => format!("<{} bytes, not JSON>", config_bytes.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_follow_the_rule_and_apply_sets_env() {
        let _g = crate::ENV_LOCK.lock().unwrap();
        for (k, v) in KEYS {
            assert_eq!(format!("EPOCHDB_{}", k.to_uppercase().replace('-', "_")), *v, "{k}");
        }
        let cfg = br#"{"state-sync-enabled":false,"s3-endpoint":"http://minio:9000","terminal-txs":800,"new-chain":true,"s3-secret-key":"hunter2"}"#;
        std::env::remove_var("EPOCHDB_S3_ENDPOINT");
        let applied = apply(cfg);
        assert_eq!(applied, ["s3-endpoint", "s3-secret-key", "terminal-txs", "new-chain"]);
        assert_eq!(std::env::var("EPOCHDB_S3_ENDPOINT").unwrap(), "http://minio:9000");
        assert_eq!(std::env::var("EPOCHDB_TERMINAL_TXS").unwrap(), "800");
        assert_eq!(std::env::var("EPOCHDB_NEW_CHAIN").unwrap(), "1");
        let r = redacted(cfg);
        assert!(!r.contains("hunter2") && r.contains("<redacted>") && r.contains("minio"), "{r}");
        assert_eq!(redacted(b"nope"), "<4 bytes, not JSON>");
        for (_, var) in KEYS {
            std::env::remove_var(var);
        }
    }
}
