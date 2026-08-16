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
}
