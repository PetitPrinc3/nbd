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
//! - Kafka topic names are checked against `[a-zA-Z0-9._-]{1,254}`; a missing, empty, too long or
//!   invalid topic is **replaced by `nbd-connector`** and reported with an error log, it does not
//!   stop the daemon
//! - Port numbers warn on well-known (< 1024) and ephemeral (> 49151) ranges
//! - `message_size` is capped at the maximum UDP datagram size (65,507 bytes), which bounds the
//!   receive buffer allocated for each provider; `0` is replaced by the default, and a value bigger
//!   than `socket_buffer_size` triggers a warning (the value is kept)
//! - Kafka broker addresses are resolved via DNS; a failure is logged but does not stop the
//!   daemon, the connectivity check made at startup is what catches an unreachable broker
//! - Metrics listener interfaces are probed via ephemeral UDP socket binding
//! - `kafka.acks` is validated against the set `{all, -1, 0, 1}`
//! - `kafka.compression` is validated against `{none, lz4, gzip, snappy, zstd}`
//! - `kafka.recovery_timeout` defaults to `600 000` ms (10 minutes), `0` is replaced by that default
//!   and values below `10 000` ms trigger a warning
//! - `kafka.parallel_requests` is capped at 5 when `kafka.idempotence = true`
//! - `kafka.idempotence` is automatically forced to `false` when `kafka.acks` ∉ `{all, -1}`
//! - TLS CA, certificate and key files are validated for existence, and the certificate and the key
//!   must come as a pair (feature `kafka-tls`)
//! - Kerberos keytab files are validated for existence (feature `kafka-auth-gssapi`)
//! - OAuth token endpoint URLs warn if not using HTTPS (feature `kafka-auth-oauth`)
//! - `security.protocol` is automatically derived from `tls`/`auth` presence:
//!   `plaintext` | `ssl` | `sasl_plaintext` | `sasl_ssl`
//!
//! ## Unknown keys
//!
//! `[kafka]` and all its sub-sections (`[kafka.tls]`, `[kafka.auth]`, ...) reject unknown keys: a
//! typo is a configuration error. The other sections (`[nbd]`, `[metrics]`, `[[provider]]`) and the
//! top level of the file **ignore them**, so a misspelled key there is silently replaced by its
//! default value. The `provider` key is required: a file without any `[[provider]]` section is
//! refused.
//!
//! ## Secret handling
//!
//! Secrets (passwords, OAuth tokens, TLS key passwords) are typed as [`SecretSource`], which
//! resolves values from a TOML literal, a shell environment variable (`{ env = "VAR" }`), or a
//! file (`{ file = "/path" }`). Resolved values are wrapped in [`secrecy::SecretString`], which
//! prevents accidental `Debug`/`Display` logging of secret material.
//!
//! [`secrecy::SecretString`] is zeroized when dropped, and the secrets are also erased explicitly
//! as soon as they are no longer needed, so that erasure does not depend on `Drop` alone: the text
//! of the configuration file is zeroized once parsed ([`RawConfig::from_path`]), the literals of
//! the raw configuration are erased once resolved, and the validated [`ProducerConfig`] is erased
//! by [`ProducerConfig::erase_secrets`] once the Kafka producer is built. Copies owned by
//! dependencies cannot be erased from here, see the documentation of those methods.
//!
//! File permission enforcement ([`check_secrets`]) runs before [`Config::try_from`]: on Unix,
//! any file containing a literal secret or referenced as a secrets file must have permissions
//! `0o600` (owner read/write only); NBD returns [`NbdError::Config`] and refuses to start
//! otherwise. On non-Unix platforms the check emits a warning instead. The check covers the
//! `password`, `key_password` and `client_secret` sources, the TLS `key_file` and the Kerberos
//! `keytab`; the `username` sources and the public files (`ca_file`, `cert_file`) are not checked.
//!
//! ## Security posture
//!
//! [`ProducerConfig::posture`] extracts the part of the Kafka configuration that defines the
//! security of the connection (broker, TLS files, SASL mechanism) into a [`SecurityPosture`], which
//! holds no secret. The Kafka producer recovery compares the posture of the configuration it just
//! re-read with the one in force, and refuses a downgrade. See [`SecurityPosture::check_against`].

use serde::Deserialize;

use secrecy::zeroize::Zeroize;
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
    /// The `[[provider]]` sections, one per multicast stream. At least one is required.
    pub provider: Vec<RawProviderConfig>,
    /// The `[kafka]` section. Required.
    pub kafka: RawProducerConfig,
    /// The `[nbd]` section. When it is absent, every parameter takes its default value.
    pub nbd: Option<RawNbdConfig>,
    /// The `[metrics]` section. When it is absent, every parameter takes its default value.
    #[cfg(feature = "metrics-exporter")]
    pub metrics: Option<RawMetricsConfig>,
}

impl RawConfig {
    /// Reads and parses a TOML configuration file.
    ///
    /// The text read from the file is zeroized as soon as it has been parsed, successfully or not.
    /// A parse error is returned as [`NbdError::Toml`]: its message shows the offending line of the
    /// file, which may hold a literal secret, and the error itself keeps a copy of the whole file
    /// text that is not zeroized.
    ///
    /// # Errors
    ///
    /// - [`NbdError::Config`] when the file cannot be read;
    /// - [`NbdError::Toml`] when its content is not valid.
    pub fn from_path(path: &PathBuf) -> Result<RawConfig, NbdError> {
        match std::fs::read_to_string(path) {
            Ok(mut content) => {
                let raw_config = toml::from_str(&content);
                content.zeroize();
                Ok(raw_config?)
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
    /// Kafka topic of the stream. Defaults to `nbd-connector`, which also replaces an invalid name.
    pub topic: Option<String>,
    /// Multicast group to join. Required, and must be a multicast address.
    pub group: IpAddr,
    /// UDP port to bind. Unset or `0` is replaced by `20000` plus the index of the provider.
    pub port: Option<u16>,
    /// Expected maximum datagram size, in bytes. See [`ProviderConfig::message_size`].
    pub message_size: Option<usize>,
    /// Local interface to join the group on. Defaults to any interface (`0.0.0.0` or `0`).
    pub interface: Option<Interface>,
}

/// Raw `[kafka]` section from the TOML configuration.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawProducerConfig {
    /// Address of the Kafka broker, as `<ip>:<port>` or `<hostname>:<port>`. Required.
    pub broker: String,
    /// Timeout, in milliseconds, of the connectivity check made at startup. Default `2000`.
    pub connection_timeout: Option<u64>,
    /// Maximum time, in milliseconds, spent trying to recreate a faulted Kafka producer.
    /// Unset or `0` is replaced by the default, see [`ProducerConfig::recovery_timeout`].
    pub recovery_timeout: Option<u64>,
    /// Delivery timeout of a message, in milliseconds (`message.timeout.ms`). Default `100`.
    pub message_timeout: Option<u64>,
    /// Number of delivery retries (`message.send.max.retries`). `0` is replaced by `2`.
    pub message_retries: Option<u16>,
    /// Compression codec (`compression.type`): `none`, `lz4` (default), `gzip`, `snappy` or `zstd`.
    pub compression: Option<String>,
    /// Idempotent producer (`enable.idempotence`). Default `true`, forced to `false` when `acks` is
    /// neither `all` nor `-1`.
    pub idempotence: Option<bool>,
    /// Batching delay, in milliseconds (`linger.ms`). Default `1`.
    pub linger: Option<u16>,
    /// Acknowledgement level (`acks`): `all` (default), `-1`, `1` or `0`.
    pub acks: Option<String>,
    /// Maximum number of messages in the producer queue (`queue.buffering.max.messages`).
    /// Default `100000`.
    pub queue_size: Option<u32>,
    /// Maximum number of in-flight requests per connection
    /// (`max.in.flight.requests.per.connection`). Default `5`, which is also the maximum when
    /// idempotence is enabled.
    pub parallel_requests: Option<u16>,
    /// The `[kafka.tls]` section. Requires the `kafka-tls` feature.
    pub tls: Option<TlsConfig>,
    /// The `[kafka.auth]` section.
    pub auth: Option<RawAuthConfig>,
}

/// Raw `[nbd]` section from the TOML configuration.
#[derive(Deserialize, Default)]
pub struct RawNbdConfig {
    /// Receive buffer of each multicast socket, in bytes. Default `204800`. `0` is refused.
    pub socket_buffer_size: Option<usize>,
    /// Log level. Default `info`.
    pub verbosity: Option<VerbosityLevels>,
}

/// Raw `[metrics]` section from the TOML configuration (requires `metrics-exporter` feature).
#[cfg(feature = "metrics-exporter")]
#[derive(Deserialize, Default)]
pub struct RawMetricsConfig {
    /// Interface the HTTP listener binds to. Default `127.0.0.1`.
    pub interface: Option<IpAddr>,
    /// Port of the HTTP listener. Default `9000`.
    pub port: Option<u16>,
}

/// Validated, strongly-typed runtime configuration.
///
/// Constructed from [`RawConfig`] via `Config::try_from()`. All fields have been validated
/// and missing values replaced with documented defaults.
pub struct Config {
    /// The multicast streams to ingest, in the order of the `[[provider]]` sections.
    pub provider: Vec<ProviderConfig>,
    /// The Kafka producer configuration.
    pub kafka: ProducerConfig,
    /// The daemon-level configuration.
    pub nbd: NbdConfig,
    /// The metrics exporter configuration.
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
    /// Kafka topic the datagrams of this stream are delivered to.
    pub topic: String,
    /// Multicast group to join.
    pub group: IpAddr,
    /// UDP port to bind.
    pub port: u16,
    /// Expected maximum datagram size, in bytes. Never `0`, and at most 65,507 (the maximum UDP
    /// payload). It sizes the receive buffer allocated for this provider, which holds 100 times
    /// this value.
    pub message_size: usize,
    /// Local interface the group is joined on, of the same IP family as the group.
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
                    let default_port: u16 = match u16::try_from(20000_usize.saturating_add(idx)) {
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
                let default_port: u16 = match u16::try_from(20000_usize.saturating_add(idx)) {
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
            Some(mut value) => {
                let mut is_correct = true;

                if value == 0 {
                    warn!(
                        "The `providers.{}.message_size` parameter is unspecified and was replaced by a default value of `{}`.",
                        &idx, default_message_size,
                    );
                    provider_config.message_size = default_message_size;
                    is_correct = false;
                } else if value > 65_507 {
                    warn!(
                        "The `providers.{}.message_size` parameter is bigger than the maximum UDP datagram size ({} > {}) and was replaced by {}. (see https://en.wikipedia.org/wiki/User_Datagram_Protocol#UDP_datagram_structure)",
                        &idx, value, 65_507, 65_507,
                    );
                    warn!(
                        "This is most likely a missconfiguration and may cause issues if the total allocated space is bigger than the available memory. A default value of 1 500 is recommended."
                    );
                    provider_config.message_size = 65_507;
                    value = 65_507;
                    is_correct = false;
                }

                if value > socket_buffer_size {
                    warn!(
                        "The `providers.{}.message_size` parameter is bigger than the `nbd.socket_buffer_size` parameter ({} > {}).",
                        &idx, value, socket_buffer_size,
                    );
                    warn!(
                        "This is most likely a missconfiguration and will cause data loss when receiving packets longer than the `nbd.socket_buffer_size`."
                    );
                    provider_config.message_size = value;
                    is_correct = false;
                }

                if is_correct {
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
/// [`secrecy::SecretString`], which is zeroized when dropped. They are also erased explicitly with
/// [`erase_secrets`](ProducerConfig::erase_secrets) once the Kafka producer is created, to
/// minimise secret lifetime in memory without relying on `Drop` alone.
///
/// The configuration erased at startup is not reused to recreate a producer after a fatal
/// error. The `[kafka]` section is instead parsed and validated again from the configuration
/// file, and its secrets resolved again, into a temporary `ProducerConfig` that is erased as soon
/// as the new producer is built. See [`producer_factory`](crate::producer_factory).
pub struct ProducerConfig {
    /// Address of the Kafka broker (`bootstrap.servers`).
    pub broker: String,
    /// Timeout, in milliseconds, of the connectivity check made at startup (`fetch_metadata`).
    /// It is not applied to the producer itself, and the check is not repeated after a recreation.
    pub connection_timeout: u64,
    /// Maximum time, in milliseconds, the supervisor keeps failing to recreate a faulted
    /// producer before giving up. The daemon then exits with [`NbdError::Recovery`]. Never `0`:
    /// the default is `600 000` (10 minutes).
    ///
    /// Building a producer does not wait for the broker, so this budget is consumed by failures of
    /// the rebuild itself (unreadable or invalid configuration, unresolvable secrets, rejected
    /// TLS/SASL material), not by an unreachable broker. See [`ManagedProducer`](crate::ManagedProducer).
    pub recovery_timeout: u64,
    /// Delivery timeout of a message, in milliseconds (`message.timeout.ms`).
    pub message_timeout: u64,
    /// Number of delivery retries (`message.send.max.retries`). Never `0`.
    pub message_retries: u16,
    /// Compression codec (`compression.type`).
    pub compression: String,
    /// Idempotent producer (`enable.idempotence`).
    pub idempotence: bool,
    /// Batching delay, in milliseconds (`linger.ms`).
    pub linger: u16,
    /// Acknowledgement level (`acks`).
    pub acks: String,
    /// Maximum number of messages in the producer queue (`queue.buffering.max.messages`).
    pub queue_size: u32,
    /// Maximum number of in-flight requests per connection
    /// (`max.in.flight.requests.per.connection`).
    pub parallel_requests: u16,
    /// `security.protocol`, derived from the presence of `tls` and `auth`: see the type
    /// documentation.
    pub security_protocol: String,
    /// TLS settings. Absent when the connection is not encrypted.
    pub tls: Option<TlsConfig>,
    /// SASL settings. Absent when the connection is not authenticated.
    pub auth: Option<AuthConfig>,
}

impl ProducerConfig {
    /// Overwrites the secrets held by this configuration with zeros.
    ///
    /// Covers the TLS key password, the SASL username and password, and the OAuth client secret.
    /// Secret sources that are not literals ([`SecretSource::Env`], [`SecretSource::File`]) only
    /// hold the name of a variable or a path, and are left untouched. A Kerberos configuration
    /// holds no secret value either: only a principal and the path of a keytab.
    ///
    /// **This is irreversible.** The erased fields stay in place, filled with zeros: a
    /// configuration on which this has been called must not be used to build a producer. Call it
    /// once [`build_producer`](crate::build_producer) has returned, whatever its result.
    ///
    /// Copies made elsewhere are out of reach: the ones `librdkafka` keeps for authentication and
    /// re-authentication, and the strings of the client configuration that
    /// [`build_producer`](crate::build_producer) fills, which are released without being zeroized.
    pub fn erase_secrets(&mut self) {
        if let Some(ref mut tls_config) = self.tls
            && let Some(key_password) = tls_config.key_password.as_mut()
        {
            key_password.erase();
        }

        (if let Some(ref mut auth) = self.auth {
            match auth {
                AuthConfig::Credentials(creds) => {
                    creds.username.zeroize();
                    creds.password.zeroize();
                }
                #[cfg(feature = "kafka-auth-gssapi")]
                AuthConfig::Kerberos(_) => {}
                #[cfg(feature = "kafka-auth-oauth")]
                AuthConfig::OAuth(creds) => {
                    creds.client_secret.erase();
                }
            }
        });
    }

    /// Extracts the [`SecurityPosture`] of this configuration: the broker, the TLS files and the
    /// SASL mechanism.
    ///
    /// The posture holds no secret, only paths and the name of the mechanism, so it can be kept
    /// and logged freely, and it stays valid after [`erase_secrets`](ProducerConfig::erase_secrets).
    pub fn posture(&self) -> SecurityPosture {
        SecurityPosture {
            target: self.broker.clone(),
            tls: self.tls.as_ref().map(SafeTlsConfig::from),
            sasl: match self.auth {
                Some(ref a) => Some(a.as_str()),
                None => None,
            },
        }
    }
}

impl TryFrom<RawProducerConfig> for ProducerConfig {
    type Error = NbdError;
    fn try_from(raw_producer_config: RawProducerConfig) -> Result<Self, Self::Error> {
        let mut producer_config = ProducerConfig {
            broker: raw_producer_config.broker,
            connection_timeout: 0,
            recovery_timeout: 0,
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
                    "The `kafka.connection_timeout` parameter is unspecified and was replaced by a default value of `2000`ms."
                );
                producer_config.connection_timeout = 2_000;
            }
        };

        match raw_producer_config.recovery_timeout {
            Some(0) => {
                warn!(
                    "The `kafka.recovery_timeout` parameter cannot be `0`. It was replaced by a default value of `600 000`ms (10 mins)."
                );
                producer_config.recovery_timeout = 600_000;
            }
            Some(value) => {
                if value < 10_000 {
                    warn!(
                        "The `kafka.recovery_timeout` parameter is low ({}ms). It is recommended to increase it to at least 10 seconds, depending on your infrastructure and network reliability.",
                        value
                    );
                } else {
                    debug!("The `kafka.recovery_timeout` parameter is configured correctly.");
                };

                producer_config.recovery_timeout = value;
            }
            None => {
                warn!(
                    "The `kafka.recovery_timeout` parameter is unspecified and was replaced by a default value of `600 000`ms (10 mins)."
                );
                producer_config.recovery_timeout = 600_000;
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

                if let Some(key_password) = tls_config.key_password.as_mut()
                    && let Ok(key_secret) = key_password.resolve()
                {
                    if key_secret.expose_secret().is_empty() {
                        return Err(NbdError::Config(String::from(
                            "The `kafka.tls.key_password` parameters cannot be empty.",
                        )));
                    } else {
                        key_password.erase();
                        tls_config.key_password = Some(SecretSource::Literal(key_secret));
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

                let mut resolved_user = match value.username.resolve() {
                    Ok(username) => username,
                    Err(e) => {
                        error!("{}", e);
                        return Err(e);
                    }
                };

                if let SecretSource::Literal(mut username) = value.username {
                    username.zeroize();
                }

                let mut resolved_pass = match value.password.resolve() {
                    Ok(password) => password,
                    Err(e) => {
                        error!("{}", e);
                        return Err(e);
                    }
                };

                if let SecretSource::Literal(mut password) = value.password {
                    password.zeroize();
                }

                if resolved_user.expose_secret().is_empty()
                    || resolved_pass.expose_secret().is_empty()
                {
                    resolved_user.zeroize();
                    resolved_pass.zeroize();

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
                        value.oauth.client_secret.erase();
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
    /// Receive buffer of each multicast socket, in bytes (`SO_RCVBUF`). Never `0`.
    pub socket_buffer_size: usize,
    /// Level of the log filter.
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
    /// Interface the HTTP listener binds to. An address that does not belong to the host is
    /// replaced by `127.0.0.1`.
    pub interface: IpAddr,
    /// Port of the HTTP listener.
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
    /// The most verbose level: everything that `Debug` logs, and what the dependencies trace.
    Trace,
    /// Debugging details, including one line for each message delivered to Kafka.
    Debug,
    /// Normal operation. The default.
    Info,
    /// Warnings and errors.
    Warn,
    /// Errors only.
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
    /// IPv4 address of the local interface.
    V4(Ipv4Addr),
    /// IPv6 interface index.
    V6(u32),
}

/// Raw `[kafka.auth]` section from the TOML configuration.
///
/// The three forms are mutually exclusive: credentials (`username`, `password` and an optional
/// `mechanism`), `[kafka.auth.gssapi]` or `[kafka.auth.oauth]`. The last two exist only with the
/// matching feature.
#[derive(Debug, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum RawAuthConfig {
    /// SASL username and password (`PLAIN`, or `SCRAM-*` with the `kafka-tls` feature).
    RawCredentials(RawCredentials),
    /// Kerberos, `[kafka.auth.gssapi]` (feature `kafka-auth-gssapi`).
    #[cfg(feature = "kafka-auth-gssapi")]
    Kerberos(KerberosConfigWrapper),
    /// OAuth 2.0 / OIDC, `[kafka.auth.oauth]` (feature `kafka-auth-oauth`).
    #[cfg(feature = "kafka-auth-oauth")]
    OAuth(OAuthConfigWrapper),
}

/// Validated SASL authentication settings, with their secrets resolved.
pub enum AuthConfig {
    /// SASL username and password.
    Credentials(Credentials),
    /// Kerberos (feature `kafka-auth-gssapi`).
    #[cfg(feature = "kafka-auth-gssapi")]
    Kerberos(Kerberos),
    /// OAuth 2.0 / OIDC (feature `kafka-auth-oauth`).
    #[cfg(feature = "kafka-auth-oauth")]
    OAuth(OAuth),
}

impl AuthConfig {
    /// Name of the SASL mechanism (`sasl.mechanism`): the one of the credentials, `GSSAPI` for
    /// Kerberos and `OAUTHBEARER` for OAuth.
    pub fn as_str(&self) -> &'static str {
        match self {
            AuthConfig::Credentials(c) => c.mechanism.as_str(),
            #[cfg(feature = "kafka-auth-gssapi")]
            AuthConfig::Kerberos(_) => "GSSAPI",
            #[cfg(feature = "kafka-auth-oauth")]
            AuthConfig::OAuth(_) => "OAUTHBEARER",
        }
    }
}

/// The `[kafka.tls]` section: TLS settings of the connection to the broker.
///
/// Used both as the raw and as the validated form. The files must exist, and the certificate and
/// the key must be given together.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// CA certificate used to verify the broker (`ssl.ca.location`). Required.
    pub ca_file: PathBuf,
    /// Client certificate for mutual TLS (`ssl.certificate.location`).
    pub cert_file: Option<PathBuf>,
    /// Client private key for mutual TLS (`ssl.key.location`).
    pub key_file: Option<PathBuf>,
    /// Password of the private key (`ssl.key.password`).
    pub key_password: Option<SecretSource>,
}

/// The part of a [`TlsConfig`] that holds no secret: the paths of the files.
///
/// Only the paths are kept, not the content of the files nor the key password, which is why a
/// [`SecurityPosture`] cannot tell that a file was replaced by another one at the same path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SafeTlsConfig {
    /// Path of the CA certificate.
    pub ca_file: PathBuf,
    /// Path of the client certificate, if any.
    pub cert_file: Option<PathBuf>,
    /// Path of the client private key, if any.
    pub key_file: Option<PathBuf>,
}

impl From<&TlsConfig> for SafeTlsConfig {
    fn from(tls_config: &TlsConfig) -> Self {
        SafeTlsConfig {
            ca_file: tls_config.ca_file.clone(),
            cert_file: tls_config.cert_file.clone(),
            key_file: tls_config.key_file.clone(),
        }
    }
}

/// Raw SASL username and password, as written in `[kafka.auth]`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawCredentials {
    /// SASL username. Must not resolve to an empty string.
    pub username: SecretSource,
    /// SASL password. Must not resolve to an empty string.
    pub password: SecretSource,
    /// SASL mechanism. Defaults to `PLAIN`, with a warning.
    pub mechanism: Option<SecurityMechanism>,
}

/// Validated SASL username and password, resolved and wrapped in [`SecretString`].
///
/// Cleared by [`ProducerConfig::erase_secrets`].
#[derive(Debug)]
pub struct Credentials {
    /// SASL username (`sasl.username`).
    pub username: SecretString,
    /// SASL password (`sasl.password`).
    pub password: SecretString,
    /// SASL mechanism (`sasl.mechanism`).
    pub mechanism: SecurityMechanism,
}

/// SASL mechanism of a username and password authentication.
///
/// Listed from the weakest to the strongest, which is also the order
/// [`SecurityPosture::check_against`] uses to tell a downgrade from an upgrade.
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub enum SecurityMechanism {
    /// `PLAIN`: the password is sent as is, which makes TLS essential.
    #[serde(rename = "PLAIN")]
    Plain,
    /// `SCRAM-SHA-256` (feature `kafka-tls`).
    #[cfg(feature = "kafka-tls")]
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    /// `SCRAM-SHA-512` (feature `kafka-tls`).
    #[cfg(feature = "kafka-tls")]
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

impl SecurityMechanism {
    /// Name of the mechanism, as `librdkafka` expects it in `sasl.mechanism`.
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

/// TOML wrapper of the `[kafka.auth.gssapi]` section.
#[cfg(feature = "kafka-auth-gssapi")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KerberosConfigWrapper {
    gssapi: Kerberos,
}

/// TOML wrapper of the `[kafka.auth.oauth]` section.
#[cfg(feature = "kafka-auth-oauth")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuthConfigWrapper {
    oauth: OAuth,
}

/// Kerberos (GSSAPI) settings. They hold no secret value: the secret is in the keytab file.
#[cfg(feature = "kafka-auth-gssapi")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Kerberos {
    /// Principal NBD authenticates as, as `<name>@<REALM>` (`sasl.kerberos.principal`).
    pub principal: String,
    /// Keytab file of the principal (`sasl.kerberos.keytab`). It must exist, and its permissions
    /// are checked by [`check_secrets`].
    pub keytab: PathBuf,
    /// Kerberos service name of the brokers, usually `kafka` (`sasl.kerberos.service.name`).
    pub service_name: String,
}

/// OAuth 2.0 / OIDC client credentials settings.
#[cfg(feature = "kafka-auth-oauth")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OAuth {
    /// Token endpoint of the identity provider (`sasl.oauthbearer.token.endpoint.url`). A URL that
    /// is not HTTPS is reported with a warning.
    pub token_endpoint_url: String,
    /// Client identifier (`sasl.oauthbearer.client.id`). Must not be empty.
    pub client_id: String,
    /// Client secret (`sasl.oauthbearer.client.secret`). Must not resolve to an empty string.
    pub client_secret: SecretSource,
    /// Scope requested with the token (`sasl.oauthbearer.scope`).
    pub scope: Option<String>,
    /// Extra SASL/OAUTHBEARER extensions, as comma-separated `key=value` pairs
    /// (`sasl.oauthbearer.extensions`).
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
#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum SecretSource {
    /// The secret is read from the environment variable `env`.
    Env {
        /// Name of the environment variable.
        env: String,
    },
    /// The secret is read from the file `file` (trailing whitespace is trimmed).
    File {
        /// Path of the file.
        file: PathBuf,
    },
    /// The secret is written in the configuration file itself.
    Literal(SecretString),
}

impl SecretSource {
    /// Resolves the secret value.
    ///
    /// A literal is cloned, the content of an environment variable is read, and a file is read and
    /// trimmed (the text read from the file is zeroized). The returned [`SecretString`] is a new
    /// copy, zeroized when dropped; erase it explicitly when it is no longer needed.
    ///
    /// # Errors
    ///
    /// Returns [`NbdError::Config`] when the environment variable is not set (or not valid
    /// Unicode), or when the file cannot be read.
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
                let secret = SecretString::from(val);
                Ok(secret)
            }
            SecretSource::File { file: path } => match std::fs::read_to_string(path) {
                Ok(mut value) => {
                    let secret = SecretString::from(value.trim());
                    value.zeroize();
                    Ok(secret)
                }
                Err(e) => Err(NbdError::Config(format!(
                    "Unable to read secret from file {} : {}",
                    path.display(),
                    e
                ))),
            },
        }
    }

    /// Whether the secret is written in the configuration file itself, which is then a
    /// secret-bearing file whose permissions must be checked.
    pub fn is_literal(&self) -> bool {
        matches!(self, SecretSource::Literal(_))
    }

    /// The path of the secret file, when the source is a [`File`](SecretSource::File).
    pub fn file_path(&self) -> Option<&std::path::Path> {
        if let SecretSource::File { file: path } = self {
            return Some(path);
        }
        None
    }

    /// Overwrites the secret with zeros when this source is a [`Literal`](SecretSource::Literal).
    ///
    /// Does nothing for [`Env`](SecretSource::Env) and [`File`](SecretSource::File), which only
    /// hold the name of a variable or a path.
    pub fn erase(&mut self) {
        match self {
            SecretSource::Literal(e) => e.zeroize(),
            SecretSource::Env { env: _ } => {}
            SecretSource::File { file: _ } => {}
        }
    }
}

/// The security-relevant part of a Kafka configuration: where it connects, with which TLS files
/// and with which SASL mechanism. It holds no secret.
///
/// Obtained with [`ProducerConfig::posture`]. The Kafka producer recovery (see
/// [`producer_factory`](crate::producer_factory)) keeps the posture of the running producer and
/// compares it, with [`check_against`](SecurityPosture::check_against), with the one of the
/// configuration it re-reads from the file before it rebuilds the producer. This turns a silent
/// downgrade of the connection, made by editing the file while the daemon runs, into a refusal.
///
/// # What a posture covers
///
/// - the broker address (`kafka.broker`);
/// - the **paths** of the TLS files (`kafka.tls.ca_file`, `cert_file`, `key_file`);
/// - the **name** of the SASL mechanism, or its absence.
///
/// # What it does not cover
///
/// - the content of the TLS files, nor the key password: a file replaced by another one at the same
///   path is not detected;
/// - the SASL credentials (username, password, client secret, principal, keytab): a change of
///   identity is not detected;
/// - everything else in `[kafka]` (acknowledgements, idempotence, compression, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityPosture {
    target: String,
    tls: Option<SafeTlsConfig>,
    sasl: Option<&'static str>,
}

impl SecurityPosture {
    /// Compares a `candidate` posture with this one, which is the reference, and refuses what
    /// weakens the connection.
    ///
    /// **Refused** (an error is logged for each, and [`NbdError::Config`] is returned listing them):
    ///
    /// - the `kafka.tls` section disappeared;
    /// - the CA file path changed;
    /// - the client certificate or the client key, which the reference had, disappeared;
    /// - the `kafka.auth` section disappeared;
    /// - the SASL mechanism is weaker than the reference's, in this order, from the strongest:
    ///   `GSSAPI` and `OAUTHBEARER` (same rank), `SCRAM-SHA-512`, `SCRAM-SHA-256`, `PLAIN`.
    ///
    /// **Accepted with a warning**: a `kafka.tls` section added or changed in a way that is not
    /// refused (including other certificate or key paths), a stronger SASL mechanism, and a
    /// different `kafka.broker`.
    ///
    /// **Accepted without any message**: everything a posture does not cover, such as a change of
    /// SASL credentials, and a switch between mechanisms of the same rank (`GSSAPI` and
    /// `OAUTHBEARER`).
    ///
    /// Note that the broker may change freely: the check protects the *level* of security of the
    /// connection, not its destination. With TLS configured, the new broker must still present a
    /// certificate issued by the same CA; without TLS, nothing verifies it.
    ///
    /// The reference is not modified: the caller decides to adopt the candidate as the new
    /// reference once it has been used successfully, as [`producer_factory`](crate::producer_factory)
    /// does.
    ///
    /// # Errors
    ///
    /// Returns [`NbdError::Config`] when at least one change is refused. Its message tells that the
    /// changes can be applied by restarting NBD, since a startup trusts the file and has no
    /// reference to compare with.
    pub fn check_against(&self, candidate: &Self) -> Result<(), NbdError> {
        let mut errors: Vec<String> = Vec::new();

        if self.tls != candidate.tls {
            if candidate.tls.is_none() {
                error!("The `kafka.tls` section is missing in the new configuration.");
                errors.push(String::from("missing `kafka.tls` section"))
            } else {
                warn!("The `kafka.tls` configuration was changed in the new configuration.");
            }
        }

        if let Some(ref config) = self.tls
            && let Some(ref candidate_config) = candidate.tls
        {
            if config.cert_file.is_some() && candidate_config.cert_file.is_none() {
                // Note : this doesn't prevent the file itself (its content) from being changed.
                error!(
                    "The certificate file for the TLS configuration was removed in the new configuration."
                );
                errors.push(String::from("missing certificate file"));
            }

            if config.key_file.is_some() && candidate_config.key_file.is_none() {
                // Note : this doesn't prevent the file itself (its content) from being changed.
                error!(
                    "The key file for the TLS configuration was removed in the new configuration."
                );
                errors.push(String::from("missing key file"));
            }

            if config.ca_file != candidate_config.ca_file {
                // Note : this doesn't prevent the file itself (its content) from being changed.
                error!(
                    "The CA file for the TLS configuration was changed in the new configuration."
                );
                errors.push(String::from("changed ca file"));
            }
        }

        if self.mechanism_rank() > candidate.mechanism_rank() {
            if candidate.mechanism_rank() < 0 {
                error!("The `kafka.auth` section is missing in the new configuration.");
                errors.push(String::from("missing `kafka.auth` section"));
            } else {
                error!(
                    "The `kafka.auth` section has a lower security mechanism ({} > {}).",
                    self.sasl.unwrap_or("NONE"),
                    candidate.sasl.unwrap_or("NONE")
                );
                errors.push(String::from("lower security mechanism"));
            }
        } else if self.mechanism_rank() < candidate.mechanism_rank() {
            warn!(
                "The `kafka.auth` section has a higher security mechanism ({} < {}).",
                self.sasl.unwrap_or("NONE"),
                candidate.sasl.unwrap_or("NONE")
            )
        }

        if self.target != candidate.target {
            warn!(
                "The `kafka.broker` parameter was changed to {} in the new configuration.",
                candidate.target
            );
        };

        match errors.is_empty() {
            true => Ok(()),
            false => {
                error!(
                    "Changes in the configuration file were refused. They can be manually re-applied by restarting NBD."
                );
                Err(NbdError::Config(format!(
                    "Configuration file refused for concerns : {}. These changes can be manually re-applied by restarting NBD.",
                    errors.join(", "),
                )))
            }
        }
    }

    fn mechanism_rank(&self) -> i16 {
        match self.sasl {
            Some("GSSAPI") => 3,
            Some("OAUTHBEARER") => 3,
            Some("SCRAM-SHA-512") => 2,
            Some("SCRAM-SHA-256") => 1,
            Some("PLAIN") => 0,
            None => -1,
            _ => i16::MAX,
        }
    }
}
