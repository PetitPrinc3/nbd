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

impl Config {
    pub fn clone(&self) -> Config {
        Config {
            provider: self
                .provider
                .iter()
                .map(|provider| provider.clone())
                .collect(),
            kafka: self.kafka.clone(),
            nbd: self.nbd.clone(),
        }
    }
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

impl ProviderConfig {
    pub fn clone(&self) -> ProviderConfig {
        ProviderConfig {
            topic: self.topic.clone(),
            group: self.group,
            port: self.port,
            message_size: self.message_size,
            interface: match self.interface {
                Some(Interface::V4(interface)) => Some(Interface::V4(interface)),
                Some(Interface::V6(interface)) => Some(Interface::V6(interface)),
                None => None,
            },
        }
    }
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
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    #[serde(default = "default_retries")]
    pub retries: u16,
}

impl ProducerConfig {
    pub fn clone(&self) -> ProducerConfig {
        ProducerConfig {
            broker: self.broker.clone(),
            timeout: self.timeout,
            retries: self.retries,
        }
    }
}

fn default_timeout() -> u64 {
    warn!(
        "The `nbd.kafka.timeout` parameter is unspecified and was replaced by a default value of `6000`ms."
    );
    6000
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

impl NbdConfig {
    pub fn clone(&self) -> NbdConfig {
        NbdConfig {
            socket_buffer_size: self.socket_buffer_size,
            verbosity: match self.verbosity {
                VerbosityLevels::Trace => VerbosityLevels::Trace,
                VerbosityLevels::Debug => VerbosityLevels::Debug,
                VerbosityLevels::Info => VerbosityLevels::Info,
                VerbosityLevels::Warn => VerbosityLevels::Warn,
                VerbosityLevels::Error => VerbosityLevels::Error,
            },
        }
    }
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
