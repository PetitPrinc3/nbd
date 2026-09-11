//! Two-stage TOML configuration parsing and validation.
//!
//! This module implements a "parse, don't validate" approach:
//!
//! 1. **Stage 1 — Deserialization**: Raw TOML is deserialized into permissive intermediate
//!    structs ([`RawConfig`], [`RawProviderConfig`], etc.) where most fields are `Option<T>`.
//!
//! 2. **Stage 2 — Validation & Transformation**: The raw structs are converted into
//!    validated, strongly-typed runtime structs ([`Config`], [`ProviderConfig`], etc.)
//!    via `TryFrom` implementations. Missing fields receive sensible defaults with
//!    diagnostic logging; invalid values produce descriptive [`NbdError::Config`] errors.
//!
//! ## Validation rules
//!
//! - Multicast group addresses are verified via `IpAddr::is_multicast()`
//! - IPv4/IPv6 family consistency between group and interface is enforced
//! - Kafka topic names are validated against `[a-zA-Z0-9._-]{1,254}`
//! - Port numbers warn on well-known (< 1024) and ephemeral (> 49151) ranges
//! - Buffer sizes are cross-checked against `socket_buffer_size` and UDP max (65,507)
//! - Kafka broker addresses are validated via DNS resolution
//! - Metrics listener interfaces are probed via ephemeral UDP socket binding

use serde::Deserialize;

use std::cmp;
use std::fmt;
#[cfg(feature = "metrics-exporter")]
use std::io::ErrorKind;
#[cfg(feature = "metrics-exporter")]
use std::net::UdpSocket;
use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs};
use std::path::PathBuf;

use tracing::{debug, error, info, warn};

use crate::errors::NbdError;

/// Raw, unvalidated configuration deserialized directly from a TOML file.
///
/// All optional fields mirror the TOML structure. Convert to [`Config`] via
/// `Config::try_from(raw_config)` to obtain validated, defaulted values.
#[derive(Deserialize)]
pub struct RawConfig {
    pub provider: Vec<RawProviderConfig>,
    pub kafka: RawProducerConfig,
    pub nbd: Option<RawNbdConfig>,
    #[cfg(feature = "metrics-exporter")]
    pub metrics: Option<RawMetricsConfig>,
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

/// Raw `[[provider]]` section from the TOML configuration.
#[derive(Deserialize)]
pub struct RawProviderConfig {
    pub topic: Option<String>,
    pub group: IpAddr,
    pub port: Option<u16>,
    pub message_size: Option<usize>,
    pub interface: Option<Interface>,
}

/// Raw `[kafka]` section from the TOML configuration.
#[derive(Deserialize)]
pub struct RawProducerConfig {
    pub broker: String,
    pub connection_timeout: Option<u64>,
    pub message_timeout: Option<u64>,
    pub message_retries: Option<u16>,
}

/// Raw `[nbd]` section from the TOML configuration.
#[derive(Deserialize, Default)]
pub struct RawNbdConfig {
    pub socket_buffer_size: Option<usize>,
    pub verbosity: Option<VerbosityLevels>,
}

/// Raw `[metrics]` section from the TOML configuration (requires `metrics-exporter` feature).
#[cfg(feature = "metrics-exporter")]
#[derive(Deserialize, Default)]
pub struct RawMetricsConfig {
    pub interface: Option<IpAddr>,
    pub port: Option<u16>,
}

/// Validated, strongly-typed runtime configuration.
///
/// Constructed from [`RawConfig`] via `Config::try_from()`. All fields have been validated
/// and missing values replaced with documented defaults.
pub struct Config {
    pub provider: Vec<ProviderConfig>,
    pub kafka: ProducerConfig,
    pub nbd: NbdConfig,
    #[cfg(feature = "metrics-exporter")]
    pub metrics: MetricsConfig,
}

impl TryFrom<RawConfig> for Config {
    type Error = NbdError;
    fn try_from(raw_config: RawConfig) -> Result<Self, Self::Error> {
        let kafka: ProducerConfig = ProducerConfig::from(raw_config.kafka);
        let nbd: NbdConfig = match raw_config.nbd {
            Some(raw_nbd_config) => NbdConfig::try_from(raw_nbd_config)?,
            None => {
                warn!(
                    "The `nbd` section is completely missing and was replaced by default values."
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

        #[cfg(feature = "metrics-exporter")]
        let metrics: MetricsConfig = match raw_config.metrics {
            Some(raw_metrics_config) => MetricsConfig::try_from(raw_metrics_config)?,
            None => {
                warn!(
                    "The `metrics` section is completely missing and was replaced by default values."
                );

                MetricsConfig::try_from(RawMetricsConfig::default())?
            }
        };

        Ok(Config {
            nbd,
            kafka,
            provider,
            #[cfg(feature = "metrics-exporter")]
            metrics,
        })
    }
}

/// Validated configuration for a single multicast provider (UDP listener).
///
/// Each provider represents one multicast group to subscribe to and one Kafka topic
/// to deliver messages to.
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

        if provider_config.group.is_multicast() {
            debug!(
                "The `providers.{}.group` parameter ({}) is a valid multicast address.",
                idx, provider_config.group,
            )
        } else {
            error!(
                "The `providers.{}.group` parameter ({}) is not a valid multicast address.",
                idx, provider_config.group,
            );
            return Err(NbdError::Config(format!(
                "The `providers.{}.group` parameter ({}) is not a valid multicast address.",
                idx, provider_config.group,
            )));
        }

        if provider_config.group.is_ipv4() {
            match raw_provider_config.interface {
                Some(Interface::V4(interface)) => {
                    provider_config.interface = Interface::V4(interface);
                }
                Some(Interface::V6(interface)) => {
                    warn!(
                        "Impossible use of an Ipv6 interface ({}) for an Ipv4 group ({}). The default interface was used instead (0.0.0.0).",
                        interface, provider_config.group
                    );
                }
                None => {
                    info!(
                        "The default Ipv4 interface (0.0.0.0) was used for group {} as none was specified.",
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
                        "Impossible use of an Ipv4 interface ({}) for an Ipv6 group ({}). The default interface was used instead (0).",
                        interface, provider_config.group
                    );
                    provider_config.interface = Interface::V6(0);
                }
                None => {
                    info!(
                        "The default Ipv6 interface (0) was used for group {} as none was specified.",
                        provider_config.group
                    );
                    provider_config.interface = Interface::V6(0);
                }
            }
        }

        match raw_provider_config.topic {
            Some(topic) => {
                let mut valid_topic = topic.clone();

                if topic.len() > 254 || topic.is_empty() {
                    error!(
                        "The `providers.{}.topic` parameter's size is invalid as it should be between 1 and 254 chars. It was replaced by a default value of `nbd-connector`.",
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
                        "The `providers.{}.topic` parameter is invalid as it should only contain alphanumeric characters or '.-_'. It was replaced by a default value of `nbd-connector`.",
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
                    "The `providers.{}.topic` parameter is unspecified and was replaced by a default value of `nbd-connector`.",
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
                        "The `providers.{}.port` parameter cannot be zero and was replaced by a default value of `{}`.",
                        &idx, default_port,
                    );
                    provider_config.port = default_port;
                } else if port < 1024 {
                    info!(
                        "While the `providers.{}.port` parameter is specified, using a port lower than 1023 is not recommended (see https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers#Well-known_ports)",
                        &idx
                    );
                    provider_config.port = port;
                } else if port > 49151 {
                    info!(
                        "While the `providers.{}.port` parameter is specified, using a port higher than 49152 is not recommended (see https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers#Dynamic,_private_or_ephemeral_ports)",
                        &idx
                    );
                    provider_config.port = port;
                } else {
                    debug!(
                        "The `providers.{}.port` parameter is well configured.",
                        &idx
                    );
                    provider_config.port = port;
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
                    "The `providers.{}.port` parameter is unspecified and was replaced by a default value of `{}`.",
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
                        "The `providers.{}.message_size` parameter is unspecified and was replaced by a default value of `{}`.",
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
                    provider_config.message_size = value;
                } else if value > 65_507 {
                    warn!(
                        "The `providers.{}.message_size` parameter is bigger than the maximum UDP datagram size ({} > {}) and was replaced by {}. (see https://en.wikipedia.org/wiki/User_Datagram_Protocol#UDP_datagram_structure)",
                        &idx, value, 65_507, 65_507,
                    );
                    warn!(
                        "This is most likely a missconfiguration and may cause issues if the total allocated space is bigger than the available memory. A default value of 1 500 is recommended."
                    );
                } else {
                    debug!(
                        "The `providers.{}.message_size` parameter is configured correctly.",
                        &idx,
                    );
                    provider_config.message_size = value;
                };
            }
            None => {
                let default_message_size = cmp::min(1500, socket_buffer_size);
                warn!(
                    "The `providers.{}.message_size` parameter is unspecified and was replaced by a default value of `{}`.",
                    &idx, default_message_size,
                );
                provider_config.message_size = default_message_size;
            }
        }

        Ok(provider_config)
    }
}

/// Validated Kafka producer configuration.
///
/// Controls broker connection, delivery timeouts, and retry behavior.
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
                debug!("The `kafka.broker` parameter is a valid ip/port combination.");
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
                if value < 10 {
                    info!(
                        "The `kafka.message_timeout` parameter is low ({}ms). It is recommended to increase it to at least 10ms, depending on your infrastructure and network reliability.",
                        value
                    );
                } else {
                    debug!("The `kafka.message_timeout` parameter is configured correctly.");
                };

                producer_config.message_timeout = value;
            }
            None => {
                warn!(
                    "The `nbd.kafka.message_timeout` parameter is unspecified and was replaced by a default value of `100`ms."
                );
                producer_config.message_timeout = 100;
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

/// Validated daemon-level configuration (socket buffers, logging).
pub struct NbdConfig {
    pub socket_buffer_size: usize,
    pub verbosity: VerbosityLevels,
}

impl TryFrom<RawNbdConfig> for NbdConfig {
    type Error = NbdError;
    fn try_from(raw_nbd_config: RawNbdConfig) -> Result<Self, Self::Error> {
        let mut nbd_config = NbdConfig {
            socket_buffer_size: 0,
            verbosity: VerbosityLevels::Info,
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

/// Validated Prometheus metrics exporter configuration (HTTP listener).
#[cfg(feature = "metrics-exporter")]
pub struct MetricsConfig {
    pub interface: IpAddr,
    pub port: u16,
}

#[cfg(feature = "metrics-exporter")]
impl TryFrom<RawMetricsConfig> for MetricsConfig {
    type Error = NbdError;
    fn try_from(raw_metrics_config: RawMetricsConfig) -> Result<Self, Self::Error> {
        let mut metrics_config = MetricsConfig {
            interface: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 9000,
        };

        match raw_metrics_config.interface {
            Some(interface) => match UdpSocket::bind((interface, 0)) {
                Ok(_) => {
                    debug!("The `metrics.interface` parameter is a valid ip address.");
                    metrics_config.interface = interface;
                }
                Err(e) => {
                    if e.kind() == ErrorKind::AddrNotAvailable {
                        warn!(
                            "The `metrics.interface` parameter doesn't belong to the host's interfaces. It was replaced by the Ipv4 localhost interface (127.0.0.1)."
                        );
                    } else {
                        error!(
                            "An error occured while testing the `metrics.interface` parameter : {}",
                            e
                        );
                        return Err(NbdError::Config(format!(
                            "An error occured while testing the `metrics.interface` parameter : {}",
                            e
                        )));
                    }
                }
            },
            None => {
                warn!(
                    "The `metrics.interface` parameter is not set. It was be replaced by the Ipv4 loopback interface (127.0.0.1)."
                );
            }
        }

        match raw_metrics_config.port {
            Some(port) => {
                debug!("The `metrics.port` parameter is a valid ip port.");
                metrics_config.port = port;
            }
            None => {
                warn!(
                    "The `metrics.port` parameter is not set. It was replaced by the a default value (9000)."
                );
            }
        }

        Ok(metrics_config)
    }
}

/// Log verbosity level for the daemon.
///
/// Deserialized from lowercase strings in TOML (e.g. `"info"`, `"debug"`).
/// Controls the `tracing` filter level at runtime via dynamic reloading.
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

/// Network interface binding for multicast group membership.
///
/// Uses serde untagged deserialization so the TOML value is automatically
/// parsed as either an IPv4 address string or an IPv6 interface index integer.
///
/// - [`V4`](Interface::V4) — IPv4 address of the local interface (e.g. `"192.168.1.10"` or `"0.0.0.0"` for any).
/// - [`V6`](Interface::V6) — IPv6 scope/interface index (e.g. `0` for any, or `2` for a specific NIC).
#[derive(Deserialize, Clone)]
#[serde(untagged)]
pub enum Interface {
    V4(Ipv4Addr),
    V6(u32),
}
