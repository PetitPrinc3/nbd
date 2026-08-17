use serde::Deserialize;

use std::net::{IpAddr, Ipv4Addr};

use std::fmt;

use tracing::warn;

#[derive(Deserialize)]
pub struct Config {
    pub provider: Vec<ProviderConfig>,
    pub kafka: ProducerConfig,
    pub nbd: NbdConfig,
}

#[derive(Deserialize)]
pub struct ProviderConfig {
    #[serde(default = "default_topic")]
    pub topic: String,
    pub group: IpAddr,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_message_size")]
    pub message_size: usize,
    pub interface: Option<Interface>,
}

fn default_topic() -> String {
    String::new()
}

fn default_port() -> u16 {
    0
}

fn default_message_size() -> usize {
    0
}

#[derive(Deserialize)]
pub struct ProducerConfig {
    pub broker: String,
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout: u64,
    #[serde(default = "default_message_timeout")]
    pub message_timeout: u64,
    #[serde(default = "default_retries")]
    pub message_retries: u16,
}

fn default_connection_timeout() -> u64 {
    warn!(
        "The `nbd.kafka.connection_timeout` parameter is unspecified and was replaced by a default value of `2000`ms."
    );
    2000
}

fn default_message_timeout() -> u64 {
    warn!(
        "The `nbd.kafka.message_timeout` parameter is unspecified and was replaced by a default value of `5000`ms."
    );
    5000
}

fn default_retries() -> u16 {
    warn!(
        "The `nbd.kafka.retries` parameter is unspecified and was replaced by a default value of `2`."
    );
    2
}

#[derive(Deserialize)]
pub struct NbdConfig {
    #[serde(default = "default_socket_buffer_size")]
    pub socket_buffer_size: usize,
    #[serde(default = "default_verbosity")]
    pub verbosity: VerbosityLevels,
}

fn default_socket_buffer_size() -> usize {
    warn!(
        "The `nbd.socket_buffer_size` parameter is unspecified and was replaced by a default value of `200ko`."
    );
    200 * 1024
}

fn default_verbosity() -> VerbosityLevels {
    warn!(
        "The `nbd.verbosity` parameter is unspecified and was replaced by a default value of `info`."
    );
    VerbosityLevels::Info
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VerbosityLevels {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl fmt::Display for VerbosityLevels {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            VerbosityLevels::Trace => write!(f, "trace"),
            VerbosityLevels::Debug => write!(f, "debug"),
            VerbosityLevels::Info => write!(f, "info"),
            VerbosityLevels::Warn => write!(f, "warn"),
            VerbosityLevels::Error => write!(f, "error"),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum Interface {
    V4(Ipv4Addr),
    V6(u32),
}
