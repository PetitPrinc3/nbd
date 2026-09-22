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
//! - `kafka.acks` is validated against the set `{all, -1, 0, 1}`
//! - `kafka.compression` is validated against `{none, lz4, gzip, snappy, zstd}`
//! - `kafka.parallel_requests` is capped at 5 when `kafka.idempotence = true`
//! - `kafka.idempotence` is automatically forced to `false` when `kafka.acks` ∉ `{all, -1}`
//! - TLS cert/key files are validated for existence and paired correctly (feature `kafka-tls`)
//! - Kerberos keytab files are validated for existence (feature `kafka-auth-gssapi`)
//! - OAuth token endpoint URLs warn if not using HTTPS (feature `kafka-auth-oauth`)
//! - `security.protocol` is automatically derived from `tls`/`auth` presence:
//!   `plaintext` | `ssl` | `sasl_plaintext` | `sasl_ssl`
//!
//! ## Secret handling
//!
//! Secrets (passwords, OAuth tokens, TLS key passwords) are typed as [`SecretSource`], which
//! resolves values from a TOML literal, a shell environment variable (`{ env = "VAR" }`), or a
//! file (`{ file = "/path" }`). Resolved values are wrapped in [`secrecy::SecretString`], which
//! prevents accidental `Debug`/`Display` logging of secret material.
//!
//! File permission enforcement ([`check_secrets`]) runs before [`Config::try_from`]: on Unix,
//! any file containing a literal secret or referenced as a secrets file must have permissions
//! `0o600` (owner read/write only); NBD returns [`NbdError::Config`] and refuses to start
//! otherwise. On non-Unix platforms the check emits a warning instead.

use serde::Deserialize;

use secrecy::{ExposeSecret, SecretString};

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
#[serde(deny_unknown_fields)]
pub struct RawProducerConfig {
    pub broker: String,
    pub connection_timeout: Option<u64>,
    pub message_timeout: Option<u64>,
    pub message_retries: Option<u16>,
    pub compression: Option<String>,
    pub idempotence: Option<bool>,
    pub linger: Option<u16>,
    pub acks: Option<String>,
    pub queue_size: Option<u32>,
    pub parallel_requests: Option<u16>,
    pub tls: Option<TlsConfig>,
    pub auth: Option<RawAuthConfig>,
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
        let kafka: ProducerConfig = ProducerConfig::try_from(raw_config.kafka)?;
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

        let default_message_size = cmp::min(1500, socket_buffer_size);

        match raw_provider_config.message_size {
            Some(value) => {
                if value == 0 {
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
                    provider_config.message_size = default_message_size;
                } else {
                    debug!(
                        "The `providers.{}.message_size` parameter is configured correctly.",
                        &idx,
                    );
                    provider_config.message_size = value;
                };
            }
            None => {
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
/// Controls broker connection, delivery behavior, producer tuning, and
/// optional transport security and authentication.
///
/// The `security_protocol` field is derived automatically:
/// - `plaintext` — neither TLS nor auth configured
/// - `ssl` — TLS only (feature `kafka-tls`)
/// - `sasl_plaintext` — auth only, no TLS
/// - `sasl_ssl` — TLS + auth (feature `kafka-tls` + `kafka-auth-*`)
///
/// Secret fields (`tls.key_password`, `auth` credentials) are stored as
/// [`secrecy::SecretString`] and are explicitly dropped after the Kafka
/// producer is created to minimise secret lifetime in memory.
pub struct ProducerConfig {
    pub broker: String,
    pub connection_timeout: u64,
    pub message_timeout: u64,
    pub message_retries: u16,
    pub compression: String,
    pub idempotence: bool,
    pub linger: u16,
    pub acks: String,
    pub queue_size: u32,
    pub parallel_requests: u16,
    pub security_protocol: String,
    pub tls: Option<TlsConfig>,
    pub auth: Option<AuthConfig>,
}

impl TryFrom<RawProducerConfig> for ProducerConfig {
    type Error = NbdError;
    fn try_from(raw_producer_config: RawProducerConfig) -> Result<Self, Self::Error> {
        let mut producer_config = ProducerConfig {
            broker: raw_producer_config.broker,
            connection_timeout: 0,
            message_timeout: 0,
            message_retries: 2,
            compression: String::from("lz4"),
            idempotence: true,
            linger: 1,
            acks: String::from("all"),
            queue_size: 100000,
            parallel_requests: 5,
            security_protocol: String::from("plaintext"),
            tls: None,
            auth: None,
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
        };

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
                    "The `kafka.message_timeout` parameter is unspecified and was replaced by a default value of `100`ms."
                );
                producer_config.message_timeout = 100;
            }
        };

        match raw_producer_config.message_retries {
            Some(value) => {
                if value < 1 {
                    warn!(
                        "The `kafka.message_retries` parameter cannot be 0. It was increased to the default value of `2`."
                    );
                } else {
                    debug!("The `kafka.message_retries` parameter is configured correctly.");
                    producer_config.message_retries = value;
                };
            }
            None => {
                warn!(
                    "The `kafka.retries` parameter is unspecified and was replaced by a default value of `2`."
                );
            }
        };

        match raw_producer_config.acks {
            Some(value) => {
                if ["-1", "1", "0", "all"].contains(&value.as_str()) {
                    debug!("The `kafka.acks` parameter is configured correctly.");
                    producer_config.acks = value;
                } else {
                    warn!(
                        "The `kafka.acks` parameter is invalid. Possible values are : 'all', '-1', '0' and '1'. It was replaced by a default value of `all`."
                    );
                }
            }
            None => {
                warn!(
                    "The `kafka.acks` parameter is unspecified and was replaced by a default value of `all`."
                );
            }
        };

        match raw_producer_config.idempotence {
            Some(value) => {
                if value && (producer_config.acks != "all" && producer_config.acks != "-1") {
                    warn!(
                        "The `kafka.idempotence` parameter is et to true while the acks value is {} which is incompatible. `kafka.idempotence` was automatically set to false.",
                        producer_config.acks,
                    );
                    producer_config.idempotence = false;
                } else {
                    debug!("The `kafka.idempotence` parameter is configured correctly.");
                    producer_config.idempotence = value;
                }
            }
            None => {
                if producer_config.acks == "all" || producer_config.acks == "-1" {
                    warn!(
                        "The `kafka.idempotence` parameter is unspecified and was replaced by a default value of `true`."
                    );
                } else {
                    warn!(
                        "The `kafka.idempotence` parameter is unspecified and was replaced by a default value of `false` because `kafka.acks` is : {}.",
                        &producer_config.acks,
                    );
                    producer_config.idempotence = false;
                }
            }
        };

        match raw_producer_config.linger {
            Some(value) => {
                debug!("The `kafka.linger` parameter is configured correctly.");
                producer_config.linger = value;
            }
            None => {
                warn!(
                    "The `kafka.linger` parameter is unspecified and was replaced by a default value of `1`ms."
                );
            }
        };

        match raw_producer_config.compression {
            Some(value) => {
                if ["none", "lz4", "gzip", "snappy", "zstd"].contains(&value.as_str()) {
                    debug!("The `kafka.compression` parameter is configured correctly.");
                    producer_config.compression = value;
                } else {
                    warn!(
                        "The `kafka.compression` parameter is invalid. Possible values are : 'none', 'lz4', 'gzip', 'snappy' and 'zstd'. It was replaced by a default value of `lz4`."
                    );
                }
            }
            None => {
                warn!(
                    "The `kafka.compression` parameter is unspecified and was replaced by a default value of `lz4`."
                );
            }
        };

        match raw_producer_config.queue_size {
            Some(value) => {
                debug!("The `kafka.queue_size` parameter is configured correctly.");
                producer_config.queue_size = value;
            }
            None => {
                warn!(
                    "The `kafka.queue_size` parameter is unspecified and was replaced by a default value of `100 000`."
                );
            }
        };

        match raw_producer_config.parallel_requests {
            Some(value) => {
                if producer_config.idempotence {
                    if value <= 5 {
                        debug!("The `kafka.parallel_requests` parameter is configured correctly.");
                        producer_config.parallel_requests = value;
                    } else {
                        warn!(
                            "The `kafka.parallel_requests` parameter is set to {} while idempotence is enabled which requires parallel_requests to be lower than 5. It was replaced by a default value of `5`.",
                            value
                        );
                    }
                } else {
                    debug!("The `kafka.parallel_requests` parameter is configured correctly.");
                    producer_config.parallel_requests = value;
                }
            }
            None => {
                warn!(
                    "The `kafka.parallel_requests` parameter is unspecified and was replaced by a default value of `5`."
                );
            }
        };

        match raw_producer_config.tls {
            #[cfg(not(feature = "kafka-tls"))]
            Some(_tls_config) => {
                error!(
                    "The `kafka.tls` section is present but this nbd version doesn't support it. Either compile with the `kafka-tls` feature or remove this section from your configuration file."
                );
                return Err(NbdError::Config(String::from(
                    "The `kafka.tls` section is present but this nbd version doesn't support it. Either compile with the `kafka-tls` feature or remove this section from your configuration file.",
                )));
            }
            #[cfg(feature = "kafka-tls")]
            Some(mut tls_config) => {
                if tls_config.cert_file.is_some() != tls_config.key_file.is_some() {
                    error!("Incomplete tls certificate/key combination.");
                    return Err(NbdError::Config(format!(
                        "Incomplete tls certificate/key combination : key ({}) / cert ({})",
                        tls_config.key_file.is_some(),
                        tls_config.cert_file.is_some()
                    )));
                }

                if !tls_config.ca_file.is_file() {
                    error!(
                        "Certificate authority file {} doesn't exist.",
                        tls_config.ca_file.display()
                    );
                    return Err(NbdError::Config(format!(
                        "Certificate authority file {} doesn't exist.",
                        tls_config.ca_file.display()
                    )));
                }

                if let Some(path) = &tls_config.cert_file
                    && !path.is_file()
                {
                    error!("Certificate file {} doesn't exist.", path.display());
                    return Err(NbdError::Config(format!(
                        "Certificate file {} doesn't exist.",
                        path.display()
                    )));
                }

                if let Some(path) = &tls_config.key_file
                    && !path.is_file()
                {
                    error!("Key file {} doesn't exist.", path.display());
                    return Err(NbdError::Config(format!(
                        "Key file {} doesn't exist.",
                        path.display()
                    )));
                }

                if let Some(ref key_password) = tls_config.key_password
                    && let Ok(ref key_secret) = key_password.resolve()
                {
                    if key_secret.expose_secret().is_empty() {
                        return Err(NbdError::Config(String::from(
                            "The `kafka.tls.key_password` parameters cannot be empty.",
                        )));
                    } else {
                        tls_config.key_password = Some(SecretSource::Literal(key_secret.clone()));
                    }
                }

                if tls_config.key_password.is_some() && tls_config.key_file.is_none() {
                    warn!(
                        "The `kafka.tls.key_password` parameter is set while the `kafka.tls.key_file` is not configured."
                    );
                }

                debug!("The `kafka.tls` parameter is configured correctly.");
                producer_config.tls = Some(tls_config);
            }
            None => {
                debug!("The `kafka.tls` parameter is not configured.");
            }
        };

        match raw_producer_config.auth {
            Some(RawAuthConfig::RawCredentials(value)) => {
                debug!("The `kafka.auth` section is configured.");

                let resolved_user = match value.username.resolve() {
                    Ok(username) => username,
                    Err(e) => {
                        error!("{}", e);
                        return Err(e);
                    }
                };
                let resolved_pass = match value.password.resolve() {
                    Ok(password) => password,
                    Err(e) => {
                        error!("{}", e);
                        return Err(e);
                    }
                };

                if resolved_user.expose_secret().is_empty()
                    || resolved_pass.expose_secret().is_empty()
                {
                    return Err(NbdError::Config(String::from(
                        "The `kafka.auth.username` and `kafka.auth.password` parameters cannot be empty.",
                    )));
                }

                let mut producer_authconfig = Credentials {
                    username: resolved_user,
                    password: resolved_pass,
                    mechanism: SecurityMechanism::Plain,
                };

                match value.mechanism {
                    Some(m) => {
                        producer_authconfig.mechanism = m;
                    }
                    None => {
                        warn!(
                            "The `kafka.auth.mechanism` parameter is not configured. A default value of `PLAIN` will be used. This is not a recommended behavior. "
                        );
                    }
                }

                producer_config.auth = Some(AuthConfig::Credentials(producer_authconfig));
            }
            #[cfg(feature = "kafka-auth-gssapi")]
            Some(RawAuthConfig::Kerberos(value)) => {
                debug!("The `kafka.auth.gssapi` section is configured.");

                if !value.gssapi.keytab.is_file() {
                    error!(
                        "Keytab file {} doesn't exist.",
                        value.gssapi.keytab.display()
                    );
                    return Err(NbdError::Config(format!(
                        "Keytab file {} doesn't exist.",
                        value.gssapi.keytab.display()
                    )));
                }

                producer_config.auth = Some(AuthConfig::Kerberos(Kerberos {
                    principal: value.gssapi.principal,
                    keytab: value.gssapi.keytab,
                    service_name: value.gssapi.service_name,
                }));
            }
            #[cfg(feature = "kafka-auth-oauth")]
            Some(RawAuthConfig::OAuth(mut value)) => {
                debug!("The `kafka.auth.oauth` section is configured.");

                if !value.oauth.token_endpoint_url.starts_with("https://") {
                    warn!(
                        "The `kafka.auth.oauth.token_endpoint_url` parameter isn't secured (not https). This is not recommended behavior."
                    );
                }

                if let Ok(client_secret) = value.oauth.client_secret.resolve() {
                    if client_secret.expose_secret().is_empty() || value.oauth.client_id.is_empty()
                    {
                        return Err(NbdError::Config(String::from(
                            "The `kafka.auth.oauth.client_id` and `kafka.auth.oauth.client_secret` parameters cannot be empty.",
                        )));
                    } else {
                        value.oauth.client_secret = SecretSource::Literal(client_secret);
                    }
                }

                producer_config.auth = Some(AuthConfig::OAuth(OAuth {
                    token_endpoint_url: value.oauth.token_endpoint_url,
                    client_id: value.oauth.client_id,
                    client_secret: value.oauth.client_secret,
                    scope: value.oauth.scope,
                    extensions: value.oauth.extensions,
                }));
            }
            None => {
                debug!("The `kafka.auth` section is not configured.");
            }
        };

        producer_config.security_protocol = match (
            producer_config.tls.is_some(),
            producer_config.auth.is_some(),
        ) {
            (true, true) => String::from("sasl_ssl"),
            (true, false) => String::from("ssl"),
            (false, true) => {
                warn!(
                    "The `kafka.tls` section is not configured while `kafka.auth` is configured. Plaintext secrets could be intercepted on the network. This is not recommended behavior."
                );
                String::from("sasl_plaintext")
            }
            (false, false) => String::from("plaintext"),
        };

        Ok(producer_config)
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

#[derive(Debug, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum RawAuthConfig {
    RawCredentials(RawCredentials),
    #[cfg(feature = "kafka-auth-gssapi")]
    Kerberos(KerberosConfigWrapper),
    #[cfg(feature = "kafka-auth-oauth")]
    OAuth(OAuthConfigWrapper),
}

pub enum AuthConfig {
    Credentials(Credentials),
    #[cfg(feature = "kafka-auth-gssapi")]
    Kerberos(Kerberos),
    #[cfg(feature = "kafka-auth-oauth")]
    OAuth(OAuth),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    pub ca_file: PathBuf,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub key_password: Option<SecretSource>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawCredentials {
    pub username: SecretSource,
    pub password: SecretSource,
    pub mechanism: Option<SecurityMechanism>,
}

#[derive(Debug)]
pub struct Credentials {
    pub username: SecretString,
    pub password: SecretString,
    pub mechanism: SecurityMechanism,
}

#[derive(Debug, Deserialize)]
pub enum SecurityMechanism {
    #[serde(rename = "PLAIN")]
    Plain,
    #[cfg(feature = "kafka-tls")]
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    #[cfg(feature = "kafka-tls")]
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

impl SecurityMechanism {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            #[cfg(feature = "kafka-tls")]
            Self::ScramSha256 => "SCRAM-SHA-256",
            #[cfg(feature = "kafka-tls")]
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

#[cfg(feature = "kafka-auth-gssapi")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KerberosConfigWrapper {
    gssapi: Kerberos,
}

#[cfg(feature = "kafka-auth-oauth")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthConfigWrapper {
    oauth: OAuth,
}

#[cfg(feature = "kafka-auth-gssapi")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Kerberos {
    pub principal: String,
    pub keytab: PathBuf,
    pub service_name: String,
}

#[cfg(feature = "kafka-auth-oauth")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuth {
    pub token_endpoint_url: String,
    pub client_id: String,
    pub client_secret: SecretSource,
    pub scope: Option<String>,
    pub extensions: Option<String>,
}

#[cfg(unix)]
fn check_file_permissions(path: &std::path::Path) -> Result<(), NbdError> {
    use std::os::unix::fs::PermissionsExt;

    let mode = std::fs::metadata(path)
        .map_err(|e| NbdError::Config(format!("Cannot read {} : {e}", path.display())))?
        .permissions()
        .mode();

    if mode & 0o077 != 0 {
        return Err(NbdError::Config(format!(
            "Invalid permissions on {} while containing secrets ({:04o}).",
            path.display(),
            mode & 0o777
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_file_permissions(path: &std::path::Path) -> Result<(), NbdError> {
    warn!(
        "Cannot verify the permissions of {} on this platform. Make sure it is only readable by intended users.",
        path.display()
    );
    Ok(())
}

/// Validates file permissions for all secret-bearing paths in the Kafka configuration.
///
/// Must be called **before** [`Config::try_from`]. On Unix, any file that either
/// *contains* a literal secret (triggering a check on `config_path` itself) or is
/// *referenced* as a secrets file must have permissions `0o600` (owner read/write only).
/// Returns [`NbdError::Config`] and prevents startup if any file is group- or
/// world-readable. On non-Unix platforms the check emits a warning instead.
pub fn check_secrets(
    kafka_config: &RawProducerConfig,
    config_path: &std::path::Path,
) -> Result<(), NbdError> {
    match &kafka_config.auth {
        Some(RawAuthConfig::RawCredentials(creds)) => {
            if creds.password.is_literal() {
                check_file_permissions(config_path)?;
            }

            if let Some(secrets_file) = creds.password.file_path() {
                check_file_permissions(secrets_file)?;
            }
        }

        #[cfg(feature = "kafka-auth-oauth")]
        Some(RawAuthConfig::OAuth(oauth)) => {
            if oauth.oauth.client_secret.is_literal() {
                check_file_permissions(config_path)?;
            }

            if let Some(secrets_file) = oauth.oauth.client_secret.file_path() {
                check_file_permissions(secrets_file)?;
            }
        }

        #[cfg(feature = "kafka-auth-gssapi")]
        Some(RawAuthConfig::Kerberos(value)) => {
            check_file_permissions(&value.gssapi.keytab)?;
        }

        None => {}
    }

    #[cfg(feature = "kafka-tls")]
    if let Some(ref tls_config) = kafka_config.tls {
        if let Some(key_password) = &tls_config.key_password {
            if key_password.is_literal() {
                check_file_permissions(config_path)?;
            }

            if let Some(secrets_file) = key_password.file_path() {
                check_file_permissions(secrets_file)?;
            }
        }

        if let Some(key_file) = &tls_config.key_file {
            check_file_permissions(key_file)?;
        }
    }

    Ok(())
}

/// A TOML-deserialisable secret value accepted in three forms:
///
/// ```toml
/// # Inline literal — config file must be chmod 600 on Unix
/// password = "my-secret"
///
/// # Shell environment variable resolved at startup
/// password = { env = "MY_SECRET_VAR" }
///
/// # Path to a secrets file — file must be chmod 600 on Unix
/// password = { file = "/run/secrets/my_secret" }
/// ```
///
/// Resolved values are wrapped in [`secrecy::SecretString`], which prevents
/// accidental `Debug`/`Display` formatting of secret material at compile time.
/// Use [`SecretSource::resolve`] to obtain the underlying [`secrecy::SecretString`].
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum SecretSource {
    Env { env: String },
    File { file: PathBuf },
    Literal(SecretString),
}

impl SecretSource {
    pub fn resolve(&self) -> Result<SecretString, NbdError> {
        match self {
            SecretSource::Literal(value) => Ok(value.clone()),
            SecretSource::Env { env: var } => {
                let val = std::env::var(var).map_err(|e| {
                    NbdError::Config(format!(
                        "Unable to read secret from variable {} : {}",
                        var, e
                    ))
                })?;
                Ok(SecretString::from(val))
            }
            SecretSource::File { file: path } => match std::fs::read_to_string(path) {
                Ok(value) => Ok(SecretString::from(value.trim())),
                Err(e) => Err(NbdError::Config(format!(
                    "Unable to read secret from file {} : {}",
                    path.display(),
                    e
                ))),
            },
        }
    }

    pub fn is_literal(&self) -> bool {
        matches!(self, SecretSource::Literal(_))
    }

    pub fn file_path(&self) -> Option<&std::path::Path> {
        if let SecretSource::File { file: path } = self {
            return Some(path);
        }
        None
    }
}
