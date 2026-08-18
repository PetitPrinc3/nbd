use serde::Deserialize;

use std::cmp;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs};
use std::path::PathBuf;

use tracing::{debug, error, info, warn};

use crate::errors::NbdError;

#[derive(Deserialize)]
pub struct RawConfig {
    pub provider: Vec<RawProviderConfig>,
    pub kafka: RawProducerConfig,
    pub nbd: Option<RawNbdConfig>,
}

impl RawConfig {
    pub fn from_path(path: &PathBuf) -> Result<RawConfig, NbdError> {
        match std::fs::read_to_string(path) {
            Ok(content) => {
                let raw_config: RawConfig = toml::from_str(&content)?;
                Ok(raw_config)
            }
            Err(e) => Err(NbdError::Config(format!(
                "An error occured while parsing the configuration file : {}",
                e
            ))),
        }
    }
}

#[derive(Deserialize)]
pub struct RawProviderConfig {
    pub topic: Option<String>,
    pub group: IpAddr,
    pub port: Option<u16>,
    pub message_size: Option<usize>,
    pub interface: Option<Interface>,
}

#[derive(Deserialize)]
pub struct RawProducerConfig {
    pub broker: String,
    pub connection_timeout: Option<u64>,
    pub message_timeout: Option<u64>,
    pub message_retries: Option<u16>,
}

#[derive(Deserialize, Default)]
pub struct RawNbdConfig {
    pub parallel_senders: Option<u32>,
    pub parallel_receivers: Option<u32>,
    pub socket_buffer_size: Option<usize>,
    pub verbosity: Option<VerbosityLevels>,
}

pub struct Config {
    pub provider: Vec<ProviderConfig>,
    pub kafka: ProducerConfig,
    pub nbd: NbdConfig,
}

impl TryFrom<RawConfig> for Config {
    type Error = NbdError;
    fn try_from(raw_config: RawConfig) -> Result<Self, Self::Error> {
        let kafka: ProducerConfig = ProducerConfig::from(raw_config.kafka);
        let nbd: NbdConfig = match raw_config.nbd {
            Some(raw_nbd_config) => NbdConfig::try_from(raw_nbd_config)?,
            None => {
                warn!(
                    "The `nbd` section is completely missing and will be replaced by default values."
                );

                NbdConfig::try_from(RawNbdConfig::default())?
            }
        };
        let provider: Vec<ProviderConfig> = raw_config
            .provider
            .into_iter()
            .enumerate()
            .map(|(idx, r)| ProviderConfig::try_from_raw(r, idx, nbd.socket_buffer_size))
            .collect::<Result<Vec<ProviderConfig>, _>>()?;

        Ok(Config {
            nbd,
            kafka,
            provider,
        })
    }
}

pub struct ProviderConfig {
    pub topic: String,
    pub group: IpAddr,
    pub port: u16,
    pub message_size: usize,
    pub interface: Interface,
}

impl ProviderConfig {
    fn try_from_raw(
        raw_provider_config: RawProviderConfig,
        idx: usize,
        socket_buffer_size: usize,
    ) -> Result<ProviderConfig, NbdError> {
        let mut provider_config = ProviderConfig {
            topic: String::new(),
            group: raw_provider_config.group,
            port: 0,
            message_size: 0,
            interface: Interface::V4(Ipv4Addr::UNSPECIFIED),
        };

        match raw_provider_config.topic {
            Some(topic) => {
                let mut valid_topic = topic.clone();

                if topic.len() > 254 || topic.is_empty() {
                    error!(
                        "The `providers.{}.topic` parameter's size is invalid as it should be between 1 and 254 chars. It will be replaced by a default value of `nbd-connector`.",
                        idx
                    );
                    valid_topic = String::from("nbd-connector");
                } else {
                    debug!("The `providers.{}.topic` parameter has a valid size.", idx);
                }

                if !topic
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '_')
                {
                    error!(
                        "The `providers.{}.topic` parameter is invalid as it should only contain alphanumeric characters or '.-_'. It will be replaced by a default value of `nbd-connector`.",
                        idx
                    );
                    valid_topic = String::from("nbd-connector");
                } else {
                    debug!(
                        "The `providers.{}.topic` parameter contains only valid characters.",
                        idx
                    )
                }

                provider_config.topic = valid_topic;
            }
            None => {
                warn!(
                    "The `providers.{}.topic` parameter is unspecified and will be replaced by a default value of `nbd-connector`.",
                    idx
                );
                provider_config.topic = String::from("nbd-connector");
            }
        }

        match raw_provider_config.port {
            Some(port) => {
                if port == 0 {
                    let default_port: u16 = match u16::try_from(20000 + &idx) {
                        Ok(p) => p,
                        Err(_) => {
                            error!(
                                "The `providers.{}.port` parameter cannot be zero and the software failed to determine a default value.",
                                &idx
                            );
                            Err(NbdError::Config(format!(
                                "The `providers.{}.port` parameter cannot be zero and the software failed to determine a default value.",
                                idx
                            )))
                        }?,
                    };

                    warn!(
                        "The `providers.{}.port` parameter cannot be zero and will be replaced by a default value of `{}`.",
                        &idx, default_port,
                    );
                    provider_config.port = default_port;
                } else if port < 1024 {
                    info!(
                        "While the `{}.port` parameter is specified, using a port lower than 1023 is not recommended (see https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers#Well-known_ports)",
                        &idx
                    );
                } else if port > 49151 {
                    info!(
                        "While the `{}.port` parameter is specified, using a port higher than 49152 is not recommended (see https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers#Dynamic,_private_or_ephemeral_ports)",
                        &idx
                    );
                } else {
                    debug!("The `{}.port` parameter is well configured.", &idx);
                }
            }
            None => {
                let default_port: u16 = match u16::try_from(20000 + &idx) {
                    Ok(p) => p,
                    Err(_) => {
                        error!(
                            "The `providers.{}.port` parameter is unspecified and the software failed to determine a default value.",
                            &idx
                        );
                        Err(NbdError::Config(format!(
                            "The `providers.{}.port` parameter is unspecified and the software failed to determine a default value.",
                            idx
                        )))
                    }?,
                };

                warn!(
                    "The `providers.{}.port` parameter is unspecified and will be replaced by a default value of `{}`.",
                    &idx, default_port,
                );
                provider_config.port = default_port;
            }
        }

        match raw_provider_config.message_size {
            Some(value) => {
                if value == 0 {
                    let default_message_size = cmp::min(1500, socket_buffer_size);
                    warn!(
                        "The `providers.{}.message_size` parameter is unspecified and will be replaced by a default value of `{}`.",
                        &idx, default_message_size,
                    );
                    provider_config.message_size = default_message_size;
                } else if value > socket_buffer_size {
                    warn!(
                        "The `providers.{}.message_size` parameter is bigger than the `nbd.socket_buffer_size` parameter ({} > {}).",
                        &idx, value, socket_buffer_size,
                    );
                    warn!(
                        "This is most likely a missconfiguration and will cause data loss when receiving packets longer than the `nbd.socket_buffer_size`."
                    );
                } else {
                    debug!(
                        "The `providers.{}.message_size` parameter is configured correctly.",
                        &idx,
                    );
                };
            }
            None => {
                let default_message_size = cmp::min(1500, socket_buffer_size);
                warn!(
                    "The `providers.{}.message_size` parameter is unspecified and will be replaced by a default value of `{}`.",
                    &idx, default_message_size,
                );
                provider_config.message_size = default_message_size;
            }
        }

        if provider_config.group.is_ipv4() {
            match raw_provider_config.interface {
                Some(Interface::V4(interface)) => {
                    provider_config.interface = Interface::V4(interface);
                }
                Some(Interface::V6(interface)) => {
                    warn!(
                        "Impossible use of an Ipv6 interface ({}) for an Ipv4 group ({}). The default interface will be used instead (0.0.0.0).",
                        interface, provider_config.group
                    );
                }
                None => {
                    info!(
                        "The default Ipv4 interface (0.0.0.0) will be used for group {} as none was specified.",
                        provider_config.group
                    );
                }
            }
        } else {
            match raw_provider_config.interface {
                Some(Interface::V6(interface)) => {
                    provider_config.interface = Interface::V6(interface);
                }
                Some(Interface::V4(interface)) => {
                    warn!(
                        "Impossible use of an Ipv4 interface ({}) for an Ipv6 group ({}). The default interface will be used instead (0).",
                        interface, provider_config.group
                    );
                    provider_config.interface = Interface::V6(0);
                }
                None => {
                    info!(
                        "The default Ipv6 interface (0) will be used for group {} as none was specified.",
                        provider_config.group
                    );
                    provider_config.interface = Interface::V6(0);
                }
            }
        }

        Ok(provider_config)
    }
}

pub struct ProducerConfig {
    pub broker: String,
    pub connection_timeout: u64,
    pub message_timeout: u64,
    pub message_retries: u16,
}

impl From<RawProducerConfig> for ProducerConfig {
    fn from(raw_producer_config: RawProducerConfig) -> Self {
        let mut producer_config = ProducerConfig {
            broker: raw_producer_config.broker,
            connection_timeout: 0,
            message_timeout: 0,
            message_retries: 0,
        };

        match producer_config.broker.to_socket_addrs() {
            Ok(_) => {
                debug!("Valid ip/port combination.");
            }
            Err(_) => {
                error!(
                    "The submitted `kafka.broker` parameter is not a valid <ip>:<port> combination."
                );
                warn!(
                    "If you are using a hostname instead of an ip, this error can mean that the specified hostname doesn't resolve."
                );
            }
        };

        match raw_producer_config.connection_timeout {
            Some(value) => {
                if value < 500 {
                    info!(
                        "The `kafka.connection_timeout` parameter is low ({}ms). It is recommended to increase it to at least 500ms, depending on your infrastructure and network reliability.",
                        value
                    );
                } else {
                    debug!("The `kafka.connection_timeout` parameter is configured correctly.");
                };

                producer_config.connection_timeout = value;
            }
            None => {
                warn!(
                    "The `nbd.kafka.connection_timeout` parameter is unspecified and was replaced by a default value of `2000`ms."
                );
                producer_config.connection_timeout = 2_000;
            }
        }

        match raw_producer_config.message_timeout {
            Some(value) => {
                if value < 100 {
                    info!(
                        "The `kafka.message_timeout` parameter is low ({}ms). It is recommended to increase it to at least 100ms, depending on your infrastructure and network reliability.",
                        value
                    );
                } else {
                    debug!("The `kafka.message_timeout` parameter is configured correctly.");
                };

                producer_config.message_timeout = value;
            }
            None => {
                warn!(
                    "The `nbd.kafka.message_timeout` parameter is unspecified and was replaced by a default value of `5000`ms."
                );
                producer_config.message_timeout = 5_000;
            }
        }

        match raw_producer_config.message_retries {
            Some(value) => {
                if value < 1 {
                    warn!(
                        "The `kafka.message_retries` parameter cannot be 0. It was increased to the default value of `2`."
                    );
                    producer_config.message_retries = 2;
                } else {
                    debug!("The `kafka.message_retries` parameter is configured correctly.");
                    producer_config.message_retries = value;
                };
            }
            None => {
                warn!(
                    "The `nbd.kafka.retries` parameter is unspecified and was replaced by a default value of `2`."
                );
                producer_config.message_retries = 2;
            }
        }

        producer_config
    }
}

pub struct NbdConfig {
    pub parallel_senders: u32,
    pub parallel_receivers: u32,
    pub socket_buffer_size: usize,
    pub verbosity: VerbosityLevels,
}

impl TryFrom<RawNbdConfig> for NbdConfig {
    type Error = NbdError;
    fn try_from(raw_nbd_config: RawNbdConfig) -> Result<Self, Self::Error> {
        let mut nbd_config = NbdConfig {
            parallel_senders: 0,
            parallel_receivers: 0,
            socket_buffer_size: 0,
            verbosity: VerbosityLevels::Info,
        };

        match raw_nbd_config.parallel_senders {
            Some(value) => {
                if value == 0 {
                    error!(
                        "The `nbd.parallel_senders` parameter can't be 0 and should at least be 1. A default value of 1000 is recommended."
                    );
                    return Err(NbdError::Config(String::from(
                        "The `nbd.parallel_senders` parameter can't be 0.",
                    )));
                } else if value < 100 {
                    warn!(
                        "The `nbd.parallel_senders` parameter is low ({}). It is recommended to increase it to at least 100, depending on your infrastructure and server capabilities.",
                        value
                    )
                };

                nbd_config.parallel_senders = value;
            }
            None => {
                warn!(
                    "The `nbd.parallel_senders` parameter is unspecified and was replaced by a default value of `1000`."
                );
                nbd_config.parallel_senders = 1000;
            }
        }

        match raw_nbd_config.parallel_receivers {
            Some(value) => {
                if value == 0 {
                    error!(
                        "The `nbd.parallel_receivers` parameter can't be 0 and should at least be 1. A default value of 1000 is recommended."
                    );
                    return Err(NbdError::Config(String::from(
                        "The `nbd.parallel_receivers` parameter can't be 0.",
                    )));
                } else if value < 1000 {
                    warn!(
                        "The `nbd.parallel_receivers` parameter is low ({}). It is recommended to increase it to at least 1000, depending on your infrastructure and server capabilities.",
                        value
                    )
                };

                nbd_config.parallel_receivers = value;
            }
            None => {
                warn!(
                    "The `nbd.parallel_receivers` parameter is unspecified and was replaced by a default value of `10 000`."
                );
                nbd_config.parallel_receivers = 10_000;
            }
        }

        if nbd_config.parallel_receivers <= nbd_config.parallel_senders {
            warn!(
                "The `nbd.parallel_receivers` parameter is lower than the `config.nbd.parallel_senders` which doesn't make sense  ({} <= {}). The software should be able to receive messages faster than it sends them as to be able to back pressure the network traffic.",
                nbd_config.parallel_receivers, nbd_config.parallel_senders,
            )
        };

        match raw_nbd_config.socket_buffer_size {
            Some(value) => {
                if value == 0 {
                    error!("The submitted nbd.socket_buffer_size cannot be zero.");
                    warn!(
                        "Please increase the parameter's value to a realistic size based on the expected network traffic (a minimum of 100ko is recommended)."
                    );
                    return Err(NbdError::Config(format!(
                        "The submitted nbd.socket_buffer_size ({}) cannot be zero.",
                        value
                    )));
                } else if value < 100 * 1024 {
                    info!(
                        "Your selected socket buffer size seems low. Be advised that the software will silently drop packets as soon as the buffer is filled."
                    );
                }

                nbd_config.socket_buffer_size = value;
            }
            None => {
                warn!(
                    "The `nbd.socket_buffer_size` parameter is unspecified and was replaced by a default value of `200ko`."
                );
                nbd_config.socket_buffer_size = 200 * 1024;
            }
        }

        match raw_nbd_config.verbosity {
            Some(value) => {
                nbd_config.verbosity = value;
            }
            None => {
                warn!(
                    "The `nbd.verbosity` parameter is unspecified and was replaced by a default value of `info`."
                );
                nbd_config.verbosity = VerbosityLevels::Info;
            }
        }

        Ok(nbd_config)
    }
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

#[derive(Deserialize, Clone)]
#[serde(untagged)]
pub enum Interface {
    V4(Ipv4Addr),
    V6(u32),
}
