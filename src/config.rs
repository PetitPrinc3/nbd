use serde::Deserialize;
use std::net::{IpAddr, Ipv4Addr};

#[derive(Deserialize)]
pub struct Config {
    pub provider: Vec<ProviderConfig>,
    pub kafka: ProducerConfig,
    pub nbd: NbdConfig,
}

#[derive(Deserialize)]
pub struct ProviderConfig {
    pub topic: String,
    pub group: IpAddr,
    pub port: u16,
    pub message_size: usize,
    pub interface: Ipv4Addr,
}

#[derive(Deserialize)]
pub struct ProducerConfig {
    pub broker: String,
    pub timeout: u64,
    pub retries: u16,
}

#[derive(Deserialize)]
pub struct NbdConfig {
    pub network_buffer_size: usize,
    pub verbosity: String,
}
