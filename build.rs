use chrono::{TimeZone, Utc};
use std::env;

fn main() {
    let now = match env::var("SOURCE_DATE_EPOCH") {
        Ok(val) => {
            let timestamp = val.parse::<i64>().unwrap_or(0);
            Utc.timestamp_opt(timestamp, 0).unwrap()
        }
        Err(_) => Utc::now(),
    };

    let formatted_date = now.format("%b %d %Y %H:%M:%S").to_string();

    println!("cargo:rustc-env=BUILD_DATE={}", formatted_date);

    let arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();

    println!("cargo:rustc-env=BUILD_ARCH={}", arch);

    let mut enabled_features: Vec<String> = env::vars()
        .filter_map(|(key, _)| {
            key.strip_prefix("CARGO_FEATURE_")
                .map(|f| f.to_lowercase().replace('_', "-"))
        })
        .collect();

    enabled_features.sort();

    let features_str = enabled_features
        .iter()
        .filter(|f| *f != "default")
        .map(|f| format!(" [+{}", f))
        .collect::<Vec<_>>()
        .join("]")
        + if enabled_features.is_empty() {
            " "
        } else {
            "]"
        };

    let pkg_version = env::var("CARGO_PKG_VERSION").unwrap_or_default();

    let full_version = format!("{}{}", pkg_version, features_str);

    println!("cargo:rustc-env=BUILD_FEATURES={}", features_str);
    println!("cargo:rustc-env=BUILD_VERSION_FULL={}", full_version);
}
