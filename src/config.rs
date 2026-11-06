//! Two-stage TOML configuration parsing and validation.
//!
//! This module implements a "parse, don't validate" approach:
//!
//! 1. **Stage 1 — Deserialization**: Raw TOML is deserialized into strict intermediate
//!    structs ([`RawConfig`], [`RawProviderConfig`], etc.) where most fields are `Option<T>`.
//!
//! 2. **Stage 2 — Validation & Transformation**: The raw structs are converted into
//!    validated, strongly-typed runtime structs ([`Config`], [`ProviderConfig`], etc.)
//!    via `TryFrom` implementations. A missing optional field receives a default with
//!    diagnostic logging; an invalid value is logged at `error` level and produces an
//!    [`NbdError::Config`]. **An invalid value is never replaced by a default.**
//!
//! ## Validation rules
//!
//! - Multicast group addresses are verified via `IpAddr::is_multicast()`, and two providers cannot
//!   share a group and a port
//! - Kafka topic names are required and checked against `[a-zA-Z0-9._-]{1,249}`, `.` and `..` being
//!   refused
//! - Ports are required and cannot be `0`. The well-known (< 1024) and ephemeral (> 49151) ranges
//!   are reported at `info` level
//! - Interfaces are required: a name that must exist on Linux, a non-zero index elsewhere, see
//!   [`Interface`]
//! - `nbd.socket_buffer_size` goes from `1` byte to 1 GiB. Whether the kernel grants it is checked
//!   when the sockets are created, not here
//! - Kafka broker addresses are resolved via DNS, and a failure is an error: `<ip>:<port>` or
//!   `<hostname>:<port>`, a single address
//! - `kafka.connection_timeout`, `kafka.recovery_timeout`, `kafka.message_retries`,
//!   `kafka.queue_size` and `kafka.queue_weight` cannot be `0`
//! - `kafka.message_timeout` goes from `1` to `300 000` ms, and values below `10` ms trigger a
//!   warning
//! - `kafka.recovery_timeout` defaults to `600 000` ms (10 minutes), and values below `10 000` ms
//!   trigger a warning
//! - `kafka.acks` is validated against the set `{all, -1, 0, 1}`
//! - `kafka.compression` is validated against `{none, lz4, gzip, snappy}`
//! - `kafka.idempotence` defaults to `true`, or to `false` when `kafka.acks` ∉ `{all, -1}`, in which
//!   case setting it to `true` is an error
//! - `kafka.parallel_requests` goes from `1` to `5` with idempotence, and must be at least `1`
//!   without it
//! - TLS CA, certificate and key files are validated for existence, and the certificate and the key
//!   must come as a pair (feature `kafka-tls`)
//! - Kerberos keytab files are validated for existence and must be absolute paths without shell
//!   metacharacters, and the principal is limited to a safe alphabet, since `librdkafka` passes
//!   them to `kinit` through a shell. The service name follows the same alphabet (feature
//!   `kafka-auth-gssapi`)
//! - `security.protocol` is automatically derived from `tls`/`auth` presence:
//!   `plaintext` | `ssl` | `sasl_plaintext` | `sasl_ssl`
//! - `kafka.auth` without `kafka.tls`, and an OAuth token endpoint URL that is not HTTPS (feature
//!   `kafka-auth-oauth`), are refused unless `kafka.allow_insecure_transport` is set
//! - Metrics listener interfaces are required, probed via ephemeral UDP socket binding, and refused
//!   unless they are loopback addresses or `metrics.allow_remote_metrics` is set
//!
//! What this validation cannot see fails later: a `kafka.linger` that is not lower than
//! `kafka.message_timeout` is refused by `librdkafka` when the client is created.
//!
//! ## Unknown keys
//!
//! Every section, and the top level of the file, rejects unknown keys: a typo is a configuration
//! error. That includes `[metrics]` when the `metrics-exporter` feature is disabled. The `provider`
//! and `kafka` keys are required.
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
//! File permission enforcement ([`check_secrets`]) runs before [`Config::try_from`]: on Unix, the
//! files that hold secrets must give no access to the group and the others, the other files of the
//! configuration must give them no write access, none of them can be a symbolic link, and their
//! parent directory must not be writable by the group and the others. NBD returns
//! [`NbdError::Config`] and refuses to start otherwise. On non-Unix platforms the check emits a
//! warning instead.
//!
//! ## Security posture
//!
//! [`ProducerConfig::posture`] extracts every non-secret setting of the Kafka configuration into a
//! [`SecurityPosture`]. The Kafka producer recovery compares the posture of the configuration it
//! just re-read with the one recorded at startup, and refuses any difference. See
//! [`SecurityPosture::check_against`].

use serde::Deserialize;

use secrecy::zeroize::Zeroize;
use secrecy::{ExposeSecret, SecretBox, SecretString};

use rdkafka::config::ClientConfig;

use std::collections::{BTreeMap, HashSet};
use std::fmt;
#[cfg(feature = "metrics-exporter")]
use std::io::ErrorKind;
#[cfg(feature = "metrics-exporter")]
use std::net::Ipv4Addr;
#[cfg(feature = "metrics-exporter")]
use std::net::UdpSocket;
use std::net::{IpAddr, ToSocketAddrs};
use std::path::PathBuf;

use tracing::{debug, error, info, warn};

use crate::errors::{NbdError, parse_toml_error};

/// Largest UDP payload a provider accepts, in bytes: the IPv6 limit, which covers the 65,507 bytes
/// of IPv4.
pub const MAX_UDP_PAYLOAD: usize = 65_527;

/// Headroom multiplier for initial buffer allocation.
///
/// The receive buffer is allocated as `MAX_UDP_PAYLOAD * BUFFER_RESERVATION_HEADROOM`
/// bytes (6.55 MB per provider) to reduce re-allocation frequency under sustained packet load.
pub const BUFFER_RESERVATION_HEADROOM: usize = 100;

/// Largest accepted `nbd.socket_buffer_size`, in bytes (1 GiB).
const MAX_SOCKET_BUFFER: usize = 1 << 30;

/// Upper bound of the `librdkafka` integer properties `queue.buffering.max.messages` and
/// `queue.buffering.max.kbytes`.
const INT_MAX: u32 = 2_147_483_647;

/// Minimum delay, in seconds, between two logged delivery failures for the same topic, and between
/// two logged socket errors for the same provider.
pub const LOG_THRESHOLD: u64 = 1;

/// Raw, unvalidated configuration deserialized directly from a TOML file.
///
/// All optional fields mirror the TOML structure. Convert to [`Config`] via
/// `Config::try_from(raw_config)` to obtain validated, defaulted values.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawConfig {
    /// The `[[provider]]` sections, one per multicast stream. At least one is required.
    pub provider: Vec<RawProviderConfig>,
    /// The `[kafka]` section. Required.
    pub kafka: RawProducerConfig,
    /// The `[nbd]` section. When it is absent, every parameter takes its default value.
    pub nbd: Option<RawNbdConfig>,
    /// The `[metrics]` section. When it is absent, the exporter is not started.
    #[cfg(feature = "metrics-exporter")]
    pub metrics: Option<RawMetricsConfig>,
}

impl RawConfig {
    /// Reads and parses a TOML configuration file.
    ///
    /// The text read from the file is zeroized as soon as it has been parsed, successfully or not.
    ///
    /// `safe_output` decides what a parse error reveals. With `true`, it is returned as an
    /// [`NbdError::Config`] that gives the line, the column and the message of the error, without
    /// the text of the file. With `false`, it is returned as [`NbdError::Toml`], whose message shows
    /// the offending line of the file, which may hold a literal secret. The daemon always uses
    /// `true`, and `--check-config-file` only uses `false` when its output is a terminal.
    ///
    /// In both cases, the message of a type error quotes the value it refused, and the error
    /// returned by the `toml` crate holds a copy of the whole file text that is not zeroized.
    ///
    /// # Errors
    ///
    /// - [`NbdError::Config`] when the file cannot be read, or when its content is not valid and
    ///   `safe_output` is `true`;
    /// - [`NbdError::Toml`] when its content is not valid and `safe_output` is `false`.
    pub fn from_path(path: &PathBuf, safe_output: bool) -> Result<RawConfig, NbdError> {
        match std::fs::read_to_string(path) {
            Ok(mut content) => {
                let raw_config = toml::from_str(&content);

                if safe_output && let Err(e) = raw_config {
                    return Err(parse_toml_error(content, e));
                }

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
#[serde(deny_unknown_fields)]
pub struct RawProviderConfig {
    /// Kafka topic of the stream. Required: 1 to 249 characters among `[a-zA-Z0-9._-]`.
    pub topic: Option<String>,
    /// Multicast group to join. Required, and must be a multicast address.
    pub group: IpAddr,
    /// UDP port to bind. Required, and cannot be `0`.
    pub port: Option<u16>,
    /// Local interface to receive the group on. Required, see [`Interface`].
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
    /// `0` is refused, see [`ProducerConfig::recovery_timeout`] for the default.
    pub recovery_timeout: Option<u64>,
    /// Delivery timeout of a message, in milliseconds (`message.timeout.ms`), from `1` to `300000`.
    /// Default `100`.
    pub message_timeout: Option<u64>,
    /// Number of delivery retries (`message.send.max.retries`). Default `2`, and `0` is refused.
    pub message_retries: Option<u16>,
    /// Compression codec (`compression.type`): `none` (default), `lz4`, `gzip` or `snappy`.
    pub compression: Option<String>,
    /// Idempotent producer (`enable.idempotence`). Defaults to `true` when `acks` is `all` or `-1`,
    /// and to `false` otherwise, in which case `true` is refused.
    pub idempotence: Option<bool>,
    /// Batching delay, in milliseconds (`linger.ms`). Default `1`.
    pub linger: Option<u16>,
    /// Acknowledgement level (`acks`): `all` (default), `-1`, `1` or `0`.
    pub acks: Option<String>,
    /// Maximum number of messages in the producer queue (`queue.buffering.max.messages`).
    /// Default `100000`.
    pub queue_size: Option<u32>,
    /// Maximum total size of the producer queue, in KiB (`queue.buffering.max.kbytes`).
    /// Default `100000`.
    pub queue_weight: Option<u32>,
    /// Maximum number of in-flight requests per connection
    /// (`max.in.flight.requests.per.connection`). Default `5`, which is also the maximum when
    /// idempotence is enabled.
    pub parallel_requests: Option<u16>,
    /// The `[kafka.tls]` section. Requires the `kafka-tls` feature.
    pub tls: Option<TlsConfig>,
    /// The `[kafka.auth]` section.
    pub auth: Option<RawAuthConfig>,
    /// Accepts `[kafka.auth]` without `[kafka.tls]`, and an OAuth token endpoint that is not HTTPS.
    /// Default `false`.
    pub allow_insecure_transport: Option<bool>,
}

/// Raw `[nbd]` section from the TOML configuration.
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RawNbdConfig {
    /// Receive buffer of each multicast socket, in bytes, from `1` to 1 GiB. Default `204800`.
    pub socket_buffer_size: Option<usize>,
    /// Log level. Default `info`.
    pub verbosity: Option<VerbosityLevels>,
}

/// Raw `[metrics]` section from the TOML configuration (requires `metrics-exporter` feature).
#[cfg(feature = "metrics-exporter")]
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RawMetricsConfig {
    /// Interface the HTTP listener binds to. Required: an address of the host, which must be a
    /// loopback address unless `allow_remote_metrics` is set.
    pub interface: Option<IpAddr>,
    /// Port of the HTTP listener. Default `9000`.
    pub port: Option<u16>,
    /// Accepts an `interface` that is not a loopback address. Default `false`.
    pub allow_remote_metrics: Option<bool>,
}

/// Validated, strongly-typed runtime configuration.
///
/// Constructed from [`RawConfig`] via `Config::try_from()`. All fields have been validated
/// and missing optional values replaced with documented defaults.
pub struct Config {
    /// The multicast streams to ingest, in the order of the `[[provider]]` sections.
    pub provider: Vec<ProviderConfig>,
    /// The Kafka producer configuration.
    pub kafka: ProducerConfig,
    /// The daemon-level configuration.
    pub nbd: NbdConfig,
    /// The metrics exporter configuration. `None` when the exporter must not be started.
    #[cfg(feature = "metrics-exporter")]
    pub metrics: Option<MetricsConfig>,
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
            .map(|(idx, r)| ProviderConfig::try_from_raw(r, idx))
            .collect::<Result<Vec<ProviderConfig>, _>>()?;

        if provider.is_empty() {
            error!("The `provider` section is empty and needs at least one provider.");
            return Err(NbdError::Config(String::from(
                "The `provider` section is empty and needs at least one provider.",
            )));
        }

        let mut seen: HashSet<(IpAddr, u16)> = HashSet::new();
        for (idx, p) in provider.iter().enumerate() {
            if !seen.insert((p.group, p.port)) {
                error!(
                    "The `provider.{}` group and port parameters are already used by another provider ({}:{}).",
                    idx, p.group, p.port
                );
                return Err(NbdError::Config(format!(
                    "Two providers use the same group/port combination ({}:{}).",
                    p.group, p.port
                )));
            }
        }

        #[cfg(feature = "metrics-exporter")]
        let metrics: Option<MetricsConfig> = match raw_config.metrics {
            Some(raw_metrics_config) => Some(MetricsConfig::try_from(raw_metrics_config)?),
            None => {
                warn!("The `metrics` section is not set. Prometheus metrics are disabled.");

                None
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
    /// Local interface the group is received on.
    pub interface: Interface,
}

impl ProviderConfig {
    fn try_from_raw(
        raw_provider_config: RawProviderConfig,
        idx: usize,
    ) -> Result<ProviderConfig, NbdError> {
        let mut provider_config = ProviderConfig {
            topic: String::new(),
            group: raw_provider_config.group,
            port: 0,
            interface: Interface(String::from("0")),
        };

        let mut errors: Vec<String> = Vec::new();

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
            errors.push(String::from("invalid multicast address"));
        }

        match raw_provider_config.interface {
            Some(interface) => match interface.index() {
                Ok(index) => {
                    if index == 0 {
                        error!(
                            "The `provider.{}.interface` is invalid as use of the default interface (0) is forbidden.",
                            idx
                        );
                        errors.push(String::from("forbidden interface value"));
                    } else {
                        debug!(
                            "The `provider.{}.interface` is correctly set to a valid interface.",
                            idx
                        );

                        provider_config.interface = interface;
                    }
                }
                Err(e) => {
                    error!(
                        "The `provider.{}.interface` parameter is invalid : {}",
                        idx, e
                    );
                    errors.push(String::from("invalid interface"));
                }
            },
            None => {
                error!("The `provider.{}.interface` parameter is missing.", idx);
                errors.push(String::from("missing interface"));
            }
        }

        match raw_provider_config.topic {
            Some(topic) => {
                if topic.len() > 249 || topic.is_empty() {
                    error!(
                        "The `providers.{}.topic` parameter's size is invalid as it should be between 1 and 249 chars.",
                        idx
                    );

                    errors.push(String::from("invalid topic length"));
                } else {
                    debug!("The `providers.{}.topic` parameter has a valid size.", idx);
                }

                if !topic
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '_')
                {
                    error!(
                        "The `providers.{}.topic` parameter is invalid as it should only contain alphanumeric characters or '.-_'.",
                        idx
                    );

                    errors.push(String::from("invalid chars in topic name"));
                } else {
                    if topic == "." || topic == ".." {
                        error!(
                            "The `providers.{}.topic` parameter is invalid as it should only contain alphanumeric characters or '.-_'.",
                            idx
                        );

                        errors.push(String::from("invalid chars in topic name"));
                    } else {
                        debug!(
                            "The `providers.{}.topic` parameter contains only valid characters.",
                            idx
                        )
                    }
                }

                provider_config.topic = topic;
            }
            None => {
                warn!(
                    "The `providers.{}.topic` parameter is missing. It should only contain between one and 249 alphanumeric characters or '.-_'.",
                    idx
                );

                errors.push(String::from("missing topic"));
            }
        }

        match raw_provider_config.port {
            Some(port) => {
                if port == 0 {
                    error!("The `providers.{}.port` parameter cannot be zero.", &idx);
                    errors.push(String::from("invalid port (0)"));
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
                error!("The `providers.{}.port` parameter is missing.", &idx);

                errors.push(String::from("missing port"));
            }
        }

        if errors.is_empty() {
            Ok(provider_config)
        } else {
            Err(NbdError::Config(format!(
                "Configuration validation for provider {} failed because of {} errors : {}",
                idx,
                errors.len(),
                errors.join(", "),
            )))
        }
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
/// - `sasl_plaintext` — auth only, no TLS (requires `allow_insecure_transport`)
/// - `sasl_ssl` — TLS + auth (feature `kafka-tls`, and `kafka-auth-*` for these mechanisms)
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
    /// the rebuild itself (unreadable or invalid configuration, unresolvable broker address or
    /// secrets, rejected TLS/SASL material), not by an unreachable broker. See
    /// [`ManagedProducer`](crate::ManagedProducer).
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
    /// Maximum total size of the producer queue, in KiB (`queue.buffering.max.kbytes`).
    pub queue_weight: u32,
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
    /// Whether `auth` is accepted without `tls`, and an OAuth token endpoint that is not HTTPS.
    pub allow_insecure_transport: bool,
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

    /// Extracts the [`SecurityPosture`] of this configuration: all its settings except the secrets.
    ///
    /// The posture holds no secret, so it can be kept and logged freely, and it stays valid after
    /// [`erase_secrets`](ProducerConfig::erase_secrets).
    pub fn posture(&self) -> SecurityPosture {
        let mut posture: BTreeMap<&'static str, String> = BTreeMap::new();

        posture.insert("broker", self.broker.clone());
        posture.insert("security_protocol", self.security_protocol.clone());
        posture.insert(
            "allow_insecure_transport",
            self.allow_insecure_transport.to_string(),
        );
        posture.insert("connection_timeout", self.connection_timeout.to_string());
        posture.insert("recovery_timeout", self.recovery_timeout.to_string());
        posture.insert("message_timeout", self.message_timeout.to_string());
        posture.insert("message_retries", self.message_retries.to_string());
        posture.insert("compression", self.compression.clone());
        posture.insert("idempotence", self.idempotence.to_string());
        posture.insert("linger", self.linger.to_string());
        posture.insert("acks", self.acks.clone());
        posture.insert("queue_size", self.queue_size.to_string());
        posture.insert("queue_weight", self.queue_weight.to_string());
        posture.insert("parallel_requests", self.parallel_requests.to_string());

        if let Some(ref config) = self.tls {
            posture.insert("tls.ca_file", config.ca_file.display().to_string());
            posture.insert(
                "tls.cert_file",
                match config.cert_file {
                    Some(ref path) => path.display().to_string(),
                    None => String::from(""),
                },
            );
            posture.insert(
                "tls.key_file",
                match config.key_file {
                    Some(ref path) => path.display().to_string(),
                    None => String::from(""),
                },
            );
        }

        if let Some(ref config) = self.auth {
            posture.insert("auth.mechanism", String::from(config.as_str()));

            match config {
                AuthConfig::Credentials(_) => {}
                #[cfg(feature = "kafka-auth-gssapi")]
                AuthConfig::Kerberos(krb) => {
                    posture.insert("auth.gssapi.principal", krb.principal.clone());
                    posture.insert("auth.gssapi.keytab", krb.keytab.display().to_string());
                    posture.insert("auth.gssapi.service_name", krb.service_name.clone());
                }
                #[cfg(feature = "kafka-auth-oauth")]
                AuthConfig::OAuth(oauth) => {
                    posture.insert(
                        "auth.oauth.token_endpoint_url",
                        oauth.token_endpoint_url.clone(),
                    );
                    posture.insert("auth.oauth.client_id", oauth.client_id.clone());
                    posture.insert(
                        "auth.oauth.scope",
                        match oauth.scope {
                            Some(ref scope) => scope.clone(),
                            None => String::from(""),
                        },
                    );
                    posture.insert(
                        "auth.oauth.extensions",
                        match oauth.extensions {
                            Some(ref extensions) => extensions.clone(),
                            None => String::from(""),
                        },
                    );
                }
            }
        }

        SecurityPosture { posture }
    }

    /// Translates this configuration into a `librdkafka` client configuration.
    ///
    /// Applies, in this order:
    ///
    /// 1. the TLS settings (`ssl.*`), when `tls` is set, with the verification of the broker
    ///    certificate and of its hostname enforced;
    /// 2. `security.protocol`;
    /// 3. the SASL settings, for the configured authentication mechanism;
    /// 4. the broker address and the producer tuning.
    ///
    /// Secrets are exposed only to fill the client configuration. The temporary copies this
    /// function creates (TLS key password, OAuth client secret) are zeroized right after use, but
    /// the strings of the returned [`ClientConfig`] are plain `String`s, released without being
    /// zeroized.
    ///
    /// # Errors
    ///
    /// Returns [`NbdError::Config`] when a secret cannot be resolved.
    pub fn configure(&self) -> Result<ClientConfig, NbdError> {
        let mut kafka_config = ClientConfig::new();

        // TLS config application
        if let Some(ref tls_config) = self.tls {
            kafka_config.set("ssl.ca.location", tls_config.ca_file.display().to_string());

            if let Some(cert_file) = &tls_config.cert_file {
                kafka_config.set("ssl.certificate.location", cert_file.display().to_string());
            }

            if let Some(key_file) = &tls_config.key_file {
                kafka_config.set("ssl.key.location", key_file.display().to_string());
            }

            if let Some(key_password) = &tls_config.key_password {
                let mut secret = key_password.resolve()?;
                kafka_config.set("ssl.key.password", secret.expose_secret());
                secret.zeroize();
            }

            kafka_config.set("enable.ssl.certificate.verification", "true");
            kafka_config.set("ssl.endpoint.identification.algorithm", "https");
        };

        // Authentication config application
        kafka_config.set("security.protocol", self.security_protocol.to_string());

        if let Some(ref auth_config) = self.auth {
            match auth_config {
                AuthConfig::Credentials(credentials) => {
                    kafka_config.set("sasl.mechanism", credentials.mechanism.as_str());
                    kafka_config.set("sasl.username", credentials.username.expose_secret());
                    kafka_config.set("sasl.password", credentials.password.expose_secret());
                }
                #[cfg(feature = "kafka-auth-gssapi")]
                AuthConfig::Kerberos(kerberos) => {
                    kafka_config.set("sasl.mechanism", "GSSAPI");
                    kafka_config.set("sasl.kerberos.principal", &kerberos.principal);
                    kafka_config.set(
                        "sasl.kerberos.keytab",
                        kerberos.keytab.display().to_string(),
                    );
                    kafka_config.set("sasl.kerberos.service.name", &kerberos.service_name);
                }
                #[cfg(feature = "kafka-auth-oauth")]
                AuthConfig::OAuth(oauth) => {
                    kafka_config.set("sasl.mechanism", "OAUTHBEARER");
                    kafka_config.set("sasl.oauthbearer.method", "oidc");
                    kafka_config.set("sasl.oauthbearer.client.id", &oauth.client_id);
                    let mut secret = oauth.client_secret.resolve()?;
                    kafka_config.set("sasl.oauthbearer.client.secret", secret.expose_secret());
                    secret.zeroize();
                    kafka_config.set(
                        "sasl.oauthbearer.token.endpoint.url",
                        &oauth.token_endpoint_url,
                    );
                    if let Some(scope) = &oauth.scope {
                        kafka_config.set("sasl.oauthbearer.scope", scope);
                    }
                    if let Some(extensions) = &oauth.extensions {
                        kafka_config.set("sasl.oauthbearer.extensions", extensions);
                    }
                }
            }
        };

        kafka_config
            .set("bootstrap.servers", &self.broker)
            .set("message.timeout.ms", self.message_timeout.to_string())
            .set("message.send.max.retries", self.message_retries.to_string())
            .set("compression.type", self.compression.to_string())
            .set(
                "max.in.flight.requests.per.connection",
                self.parallel_requests.to_string(),
            )
            .set("queue.buffering.max.messages", self.queue_size.to_string())
            .set("queue.buffering.max.kbytes", self.queue_weight.to_string())
            .set("enable.idempotence", self.idempotence.to_string())
            .set("linger.ms", self.linger.to_string())
            .set("acks", self.acks.to_string());

        Ok(kafka_config)
    }

    /// Checks that `librdkafka` accepts the settings of this configuration, without creating a
    /// client and without contacting the broker.
    ///
    /// This doesn't cover what `librdkafka` only verifies when a client is created, such as
    /// `linger.ms` being lower than `message.timeout.ms`, or the validity of the TLS material.
    ///
    /// # Errors
    ///
    /// Returns [`NbdError::Config`] when a secret cannot be resolved, and [`NbdError::Kafka`] when
    /// `librdkafka` rejects a setting.
    pub fn check(&self) -> Result<(), NbdError> {
        self.configure()?.create_native_config()?;
        Ok(())
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
            compression: String::from("none"),
            idempotence: true,
            linger: 1,
            acks: String::from("all"),
            queue_size: 100000,
            queue_weight: 100000,
            parallel_requests: 5,
            security_protocol: String::from("plaintext"),
            tls: None,
            auth: None,
            allow_insecure_transport: raw_producer_config
                .allow_insecure_transport
                .unwrap_or(false),
        };

        let mut errors: Vec<String> = Vec::new();

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

                errors.push(String::from("invalid broker"));
            }
        };

        match raw_producer_config.connection_timeout {
            Some(value) => {
                if value == 0 {
                    error!(
                        "The `kafka.connection_timeout` parameter cannot be set to 0. It is recommended to increase it to at least 500ms, depending on your infrastructure and network reliability."
                    );
                    errors.push(String::from("invalid connection timeout"));
                } else if value < 500 {
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
                error!("The `kafka.recovery_timeout` parameter cannot be `0`.");
                errors.push(String::from("invalid recovery timeout"));
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
                if value == 0 {
                    error!(
                        "The `kafka.message_timeout` parameter cannot be 0. It is recommended to set the timeout value between 10 and 300 000ms, depending on your infrastructure and network reliability.",
                    );
                    errors.push(String::from("invalid message timeout"));
                } else if value < 10 {
                    warn!(
                        "The `kafka.message_timeout` parameter is low ({}ms). It is recommended to increase it to at least 10ms, depending on your infrastructure and network reliability.",
                        value
                    );
                    producer_config.message_timeout = value;
                } else if value > 300_000 {
                    error!(
                        "The `kafka.message_timeout` parameter cannot be greater than 300 000. It is recommended to set the timeout value between 10 and 300 000ms, depending on your infrastructure and network reliability.",
                    );
                    errors.push(String::from("invalid message timeout"));
                } else {
                    debug!("The `kafka.message_timeout` parameter is configured correctly.");
                    producer_config.message_timeout = value;
                };
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
                    error!("The `kafka.message_retries` parameter cannot be 0.");
                    errors.push(String::from("invalid message retries"));
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
                    error!(
                        "The `kafka.acks` parameter is invalid. Possible values are : 'all', '-1', '0' and '1'."
                    );
                    errors.push(String::from("invalid acks value"));
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
                    error!(
                        "The `kafka.idempotence` parameter is et to true while the acks value is {} which is incompatible.",
                        producer_config.acks,
                    );
                    errors.push(String::from("impossible idempotence/acks combination"));
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
                if ["none", "lz4", "gzip", "snappy"].contains(&value.as_str()) {
                    debug!("The `kafka.compression` parameter is configured correctly.");
                    producer_config.compression = value;
                } else {
                    error!(
                        "The `kafka.compression` parameter is invalid. Possible values are : 'none', 'lz4', 'gzip', and 'snappy'."
                    );
                    errors.push(String::from("invalid compression value"));
                }
            }
            None => {
                warn!(
                    "The `kafka.compression` parameter is unspecified and was replaced by a default value of `none`."
                );
            }
        };

        match raw_producer_config.queue_size {
            Some(value) => {
                if value == 0 {
                    error!("The `kafka.queue_size` parameter cannot be 0.",);
                    errors.push(String::from("invalid queue size (0)"));
                } else if value > INT_MAX {
                    error!(
                        "The `kafka.queue_size` parameter excedes the maximum value of 2 147 483 647."
                    );
                    errors.push(String::from("invalid queue size"));
                } else {
                    debug!("The `kafka.queue_size` parameter is configured correctly.");
                    producer_config.queue_size = value;
                }
            }
            None => {
                warn!(
                    "The `kafka.queue_size` parameter is unspecified and was replaced by a default value of `100 000`."
                );
            }
        };

        match raw_producer_config.queue_weight {
            Some(value) => {
                if value == 0 {
                    error!("The `kafka.queue_weight` parameter cannot be 0.",);
                    errors.push(String::from("invalid queue weight (0)"));
                } else if value > INT_MAX {
                    error!(
                        "The `kafka.queue_weight` parameter excedes the maximum value of 2 147 483 647."
                    );
                    errors.push(String::from("invalid queue size"));
                } else {
                    debug!("The `kafka.queue_weight` parameter is configured correctly.");
                    producer_config.queue_weight = value;
                }
            }
            None => {
                warn!(
                    "The `kafka.queue_weight` parameter is unspecified and was replaced by a default value of `100 000`kbytes."
                );
            }
        };

        match raw_producer_config.parallel_requests {
            Some(value) => {
                if producer_config.idempotence {
                    if (1..=5).contains(&value) {
                        debug!("The `kafka.parallel_requests` parameter is configured correctly.");
                        producer_config.parallel_requests = value;
                    } else {
                        error!(
                            "The `kafka.parallel_requests` parameter is set to {} while idempotence is enabled which requires parallel_requests to be set between `1` and `5`.",
                            value
                        );
                        errors.push(String::from(
                            "invalid idempotence/parallel_requests combination",
                        ));
                    }
                } else if value >= 1 {
                    debug!("The `kafka.parallel_requests` parameter is configured correctly.");
                    producer_config.parallel_requests = value;
                } else {
                    error!(
                        "The `kafka.parallel_requests` parameter is invalid ({}). It needs to be at least `1`.",
                        value
                    );
                    errors.push(String::from(
                        "invalid idempotence/parallel_requests combination",
                    ));
                }
            }
            None => {
                warn!(
                    "The `kafka.parallel_requests` parameter is unspecified and was replaced by a default value of `5`."
                );
            }
        };

        let mut insecure_transport: bool = false;

        if raw_producer_config
            .allow_insecure_transport
            .unwrap_or(false)
        {
            warn!("Insecure transport is allowed (insecure TLS).");
            insecure_transport = true;
        }

        match raw_producer_config.tls {
            #[cfg(not(feature = "kafka-tls"))]
            Some(_tls_config) => {
                error!(
                    "The `kafka.tls` section is present but this nbd version doesn't support it. Either compile with the `kafka-tls` feature or remove this section from your configuration file."
                );

                errors.push(String::from("missing TLS feature"));
            }
            #[cfg(feature = "kafka-tls")]
            Some(mut tls_config) => {
                if tls_config.cert_file.is_some() != tls_config.key_file.is_some() {
                    error!(
                        "Incomplete tls certificate/key combination : key ({}) / cert ({})",
                        tls_config.key_file.is_some(),
                        tls_config.cert_file.is_some()
                    );

                    errors.push(String::from("incomplete tls certificate/key combination"));
                }

                if !tls_config.ca_file.is_file() {
                    error!(
                        "Certificate authority file {} doesn't exist.",
                        tls_config.ca_file.display()
                    );

                    errors.push(String::from("CA file doesn't exist."));
                }

                if let Some(path) = &tls_config.cert_file
                    && !path.is_file()
                {
                    error!("Certificate file {} doesn't exist.", path.display());

                    errors.push(String::from("certificate file doesn't exist"));
                }

                if let Some(path) = &tls_config.key_file
                    && !path.is_file()
                {
                    error!("Key file {} doesn't exist.", path.display());

                    errors.push(String::from("key file doesn't exist"));
                }

                if let Some(key_password) = tls_config.key_password.as_mut() {
                    if let Ok(key_secret) = key_password.resolve() {
                        if key_secret.expose_secret().is_empty() {
                            error!("The `kafka.tls.key_password` parameter cannot be empty.");
                            errors.push(String::from("empty key password"));
                        } else {
                            key_password.erase();
                            tls_config.key_password = Some(SecretSource::Literal(key_secret));
                        }
                    } else {
                        error!("The `kafka.tls.key_password` parameter failed to resolve.");
                        errors.push(String::from("key password failed to resolve"));
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
                        errors.push(String::from("username failed to resolve"));
                        SecretString::from("")
                    }
                };

                if let SecretSource::Literal(mut username) = value.username {
                    username.zeroize();
                }

                let mut resolved_pass = match value.password.resolve() {
                    Ok(password) => password,
                    Err(e) => {
                        error!("{}", e);
                        errors.push(String::from("password failed to resolve"));
                        SecretBox::<str>::new("".into())
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

                    error!(
                        "The `kafka.auth.username` and `kafka.auth.password` parameters cannot be empty."
                    );
                    errors.push(String::from("empty username or password"));
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
                        "The keytab file {} doesn't exist.",
                        value.gssapi.keytab.display()
                    );

                    errors.push(String::from("keytab file doesn't exist"));
                } else if !value.gssapi.keytab.is_absolute() {
                    error!("The keytab file needs to be an absolute path.");
                    errors.push(String::from("relative path for keytab file"));
                }

                if value
                    .gssapi
                    .keytab
                    .to_string_lossy()
                    .contains(['"', '$', '`', '\\', '\n'])
                {
                    error!(
                        "The `kafka.auth.gssapi.keytab` parameter contains invalid characters (`\"`, `$`, ```, `\\`, or a line break)"
                    );
                    errors.push(String::from("invalid keyfile path"));
                }

                if !check_krb_name(&value.gssapi.principal) {
                    error!(
                        "The `kafka.auth.gssapi.principal` parameter is invalid as it should only contain alphanumeric characters or `.-_/@` and cannot start with `-`",
                    );
                    errors.push(String::from("invalid Kerberos principal"));
                }

                if !check_krb_name(&value.gssapi.service_name) {
                    error!(
                        "The `kafka.auth.gssapi.service_name` parameter is invalid as it should only contain alphanumeric characters or `.-_/@` and cannot start with `-`",
                    );
                    errors.push(String::from("invalid service name"));
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

                if !value
                    .oauth
                    .token_endpoint_url
                    .to_lowercase()
                    .starts_with("https://")
                {
                    if insecure_transport {
                        warn!(
                            "The `kafka.auth.oauth.token_endpoint_url` parameter isn't secured (not https). This is not recommended behavior."
                        );
                    } else {
                        error!(
                            "The `kafka.auth.oauth.token_endpoint_url` parameter isn't secured (not https). This is forbidden by default (use `allow_insecure_transport = true` to apply anyway)."
                        );
                        errors.push(String::from("insecure endpoint (TLS)"));
                    }
                }

                if let Ok(client_secret) = value.oauth.client_secret.resolve() {
                    if client_secret.expose_secret().is_empty() || value.oauth.client_id.is_empty()
                    {
                        error!(
                            "The `kafka.auth.oauth.client_id` and `kafka.auth.oauth.client_secret` parameters cannot be empty."
                        );

                        errors.push(String::from("empty client_id or client_secret"));
                    } else {
                        value.oauth.client_secret.erase();
                        value.oauth.client_secret = SecretSource::Literal(client_secret);
                    }
                } else {
                    error!("The `kafka.auth.oauth.client_secret` parameters is missing.");
                    errors.push(String::from("missing client_secret"));
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
                if insecure_transport {
                    warn!(
                        "The `kafka.tls` section is not configured while `kafka.auth` is configured. Plaintext secrets could be intercepted on the network. This is not recommended behavior."
                    );
                } else {
                    error!(
                        "The `kafka.auth` section requires the `kafka.tls` section (or `kafka.allow_insecure_transport = true`)"
                    );
                    errors.push(String::from("insecure auth protocol"));
                }
                String::from("sasl_plaintext")
            }
            (false, false) => String::from("plaintext"),
        };

        if errors.is_empty() {
            Ok(producer_config)
        } else {
            Err(NbdError::Config(format!(
                "Producer configuration validation failed because of {} errors : {}",
                errors.len(),
                errors.join(", "),
            )))
        }
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

        let mut errors: Vec<String> = Vec::new();

        match raw_nbd_config.socket_buffer_size {
            Some(value) => {
                if value == 0 {
                    error!("The submitted nbd.socket_buffer_size cannot be zero.");
                    warn!(
                        "Please increase the parameter's value to a realistic size based on the expected network traffic (a minimum of 100ko is recommended)."
                    );

                    errors.push(String::from("invalid buffer size (0)"));
                } else if value < 100 * 1024 {
                    info!(
                        "Your selected socket buffer size seems low. Be advised that the software will silently drop packets as soon as the buffer is filled."
                    );
                } else if value > MAX_SOCKET_BUFFER {
                    error!(
                        "Your selected socket buffer size is too high and should be set between `1` and `{}`o (1 Gio).",
                        MAX_SOCKET_BUFFER
                    );
                    errors.push(String::from("invalid buffer size"));
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

        if errors.is_empty() {
            Ok(nbd_config)
        } else {
            Err(NbdError::Config(format!(
                "NBD's configuration validation failed because of {} errors : {}",
                errors.len(),
                errors.join(", "),
            )))
        }
    }
}

/// Validated Prometheus metrics exporter configuration (HTTP listener).
#[cfg(feature = "metrics-exporter")]
pub struct MetricsConfig {
    /// Interface the HTTP listener binds to: an address of the host.
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

        let mut errors: Vec<String> = Vec::new();

        match raw_metrics_config.interface {
            Some(interface) => match UdpSocket::bind((interface, 0)) {
                Ok(_) => {
                    if interface.is_loopback() {
                        debug!("The `metrics.interface` parameter is a valid ip address.");
                        metrics_config.interface = interface;
                    } else if raw_metrics_config.allow_remote_metrics.unwrap_or(false) {
                        debug!("The `metrics.interface` parameter is a valid ip address.");
                        warn!("The metrics are accessible remotely on {}.", interface);
                        metrics_config.interface = interface;
                    } else {
                        error!(
                            "Impossible use of a remote interface to publish metrics without setting `metrics.allow_remote_metrics` to true."
                        );
                        errors.push(String::from("impossible remote interface"));
                    }
                }
                Err(e) => {
                    if e.kind() == ErrorKind::AddrNotAvailable {
                        error!(
                            "The `metrics.interface` parameter doesn't belong to the host's interfaces."
                        );
                    } else {
                        error!(
                            "An error occured while testing the `metrics.interface` parameter : {}",
                            e
                        );
                    }
                    errors.push(String::from("invalid interface"));
                }
            },
            None => {
                error!("The `metrics.interface` parameter is not set.");
                errors.push(String::from("missing interface"));
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

        if errors.is_empty() {
            Ok(metrics_config)
        } else {
            Err(NbdError::Config(format!(
                "Prometheus metrics configuration validation failed because of {} errors : {}",
                errors.len(),
                errors.join(", "),
            )))
        }
    }
}

/// Log verbosity level for the daemon.
///
/// Deserialized from lowercase strings in TOML (e.g. `"info"`, `"debug"`).
/// Sets the level of the `tracing` filter once, at startup.
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

/// Network interface a provider receives its multicast group on.
///
/// Always a string in the TOML file, and built with [`Interface::try_from`]:
///
/// - on Linux, the name of the interface (e.g. `"eth0"`);
/// - on other systems, its numeric index (e.g. `"2"`).
///
/// The value is limited to 15 characters among `[a-zA-Z0-9._-]`. `.`, `..` and IP addresses are
/// refused. Whether the interface exists is checked by the validation of the configuration, with
/// [`index`](Interface::index). The default interface (index `0`) is refused there as well.
#[derive(Deserialize, Clone)]
#[serde(try_from = "String")]
pub struct Interface(String);

impl TryFrom<String> for Interface {
    type Error = NbdError;
    fn try_from(s: String) -> Result<Self, NbdError> {
        if s.is_empty() {
            return Err(NbdError::Config(String::from("Empty interface name")));
        }

        if s.len() > 15
            || s == "."
            || s == ".."
            || !s
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
        {
            return Err(NbdError::Config(String::from("Invalid interface value.")));
        }

        if s.parse::<IpAddr>().is_ok() {
            #[cfg(target_os = "linux")]
            return Err(NbdError::Config(String::from(
                "An interface needs to be designated by its name (eg: `eth0`) and not its address.",
            )));
            #[cfg(not(target_os = "linux"))]
            return Err(NbdError::Config(String::from(
                "An interface needs to be designated by its index (eg: `1`) and not its address.",
            )));
        }

        Ok(Interface(s))
    }
}

impl Interface {
    /// The interface as written in the configuration.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the index of the interface.
    ///
    /// On Linux, it is read from `/sys/class/net/<name>/ifindex`. On other systems, the value is the
    /// index.
    ///
    /// # Errors
    ///
    /// Returns [`NbdError::Socket`] when the interface doesn't exist or its index cannot be read.
    pub fn index(&self) -> Result<u32, NbdError> {
        #[cfg(target_os = "linux")]
        {
            let mut path = PathBuf::from("/sys/class/net");
            path.push(&self.0);
            path.push("ifindex");

            let raw = match std::fs::read_to_string(&path) {
                Ok(i) => i,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(NbdError::Socket(format!(
                        "Interface `{}` doesn't exist.",
                        self.0
                    )));
                }
                Err(_) => {
                    return Err(NbdError::Socket(format!(
                        "Unreadable device index for interface `{}`",
                        self.0
                    )));
                }
            };

            let n: u32 = raw.trim().parse().map_err(|_| {
                NbdError::Socket(format!("Invalid device index for interface {}", self.0))
            })?;

            Ok(n)
        }

        #[cfg(not(target_os = "linux"))]
        {
            let n: u32 = self.0.parse().map_err(|_| {
                NbdError::Socket(String::from("Failed to parse interface index. On non-linux machines, the interface value needs to be the index of the desired interface."))
            })?;

            Ok(n)
        }
    }
}

/// Whether a Kerberos name sticks to a safe alphabet: at most 255 characters among
/// `[a-zA-Z0-9._/@-]`, not starting with `-`. `librdkafka` passes the principal to `kinit` through
/// a shell.
#[cfg(feature = "kafka-auth-gssapi")]
fn check_krb_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && !name.starts_with("-")
        && name.chars().all(|c| {
            c.is_ascii_alphanumeric() || c == '-' || c == '/' || c == '@' || c == '_' || c == '.'
        })
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
/// Listed from the weakest to the strongest.
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
    /// Keytab file of the principal (`sasl.kerberos.keytab`). It must exist and be an absolute path
    /// without `"`, `$`, `` ` ``, `\` or a line break, and its permissions are checked by
    /// [`check_secrets`].
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
    /// is not HTTPS is refused, unless `kafka.allow_insecure_transport` is set.
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

/// Checks one file of the configuration: it must not be a symbolic link (the last component of the
/// path only), its immediate parent directory must not be writable by the group and the others, and
/// the file itself must give the group and the others no access if `is_secret`, no write access
/// otherwise.
///
/// The owner of the file is not checked.
#[cfg(unix)]
fn check_file_permissions(path: &std::path::Path, is_secret: bool) -> Result<(), NbdError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = std::fs::symlink_metadata(path)
        .map_err(|e| NbdError::Config(format!("Cannot read {} : {e}", path.display())))?;

    if metadata.file_type().is_symlink() {
        return Err(NbdError::Config(format!(
            "The use of symbolic links is prohibited for security reasons ({} is a symlink).",
            path.display(),
        )));
    }

    let max_perm = if is_secret { 0o077 } else { 0o022 };

    let mode = metadata.permissions().mode();

    if mode & max_perm != 0 {
        return Err(NbdError::Config(format!(
            "Invalid permissions on {} ({:04o}).",
            path.display(),
            mode & 0o777
        )));
    }

    let parent = match path.parent() {
        Some(folder) if !folder.as_os_str().is_empty() => folder,
        _ => std::path::Path::new("."),
    };

    let parent_metadata = std::fs::metadata(parent)
        .map_err(|e| NbdError::Config(format!("Cannot read {} : {e}", path.display())))?;

    let parent_mode = parent_metadata.permissions().mode();

    if parent_mode & 0o022 != 0 {
        return Err(NbdError::Config(format!(
            "Invalid permissions on {} which is the parent folder of {} while containing secrets ({:04o}).",
            parent.display(),
            path.display(),
            parent_mode & 0o777,
        )));
    }

    Ok(())
}

/// Permissions cannot be verified on this platform: emits a warning.
#[cfg(not(unix))]
fn check_file_permissions(path: &std::path::Path, _is_secret: bool) -> Result<(), NbdError> {
    warn!(
        "Cannot verify the permissions of {} on this platform. Make sure it is only readable by intended users.",
        path.display()
    );
    Ok(())
}

/// Validates the permissions of the files of the configuration.
///
/// Must be called **before** [`Config::try_from`]. On Unix:
///
/// - `config_path`, when it holds a literal `password`, `key_password` or `client_secret`, the
///   `{ file = ... }` sources of these three parameters, the TLS key and the Kerberos keytab must
///   give no access to the group and the others;
/// - `config_path` otherwise, the CA and the client certificate must give them no write access;
/// - none of them can be a symbolic link, and their parent directory must not be writable by the
///   group and the others.
///
/// The `{ file = ... }` source of a `username` is not checked, and neither is the owner of the
/// files. The TLS files are only checked with the `kafka-tls` feature.
///
/// Returns [`NbdError::Config`] and prevents startup on a violation. On non-Unix platforms the
/// check emits a warning instead.
pub fn check_secrets(
    kafka_config: &RawProducerConfig,
    config_path: &std::path::Path,
) -> Result<(), NbdError> {
    let mut is_secret: bool = false;

    match &kafka_config.auth {
        Some(RawAuthConfig::RawCredentials(creds)) => {
            if creds.password.is_literal() {
                is_secret = true;
            }

            if let Some(secrets_file) = creds.password.file_path() {
                check_file_permissions(secrets_file, true)?;
            }
        }

        #[cfg(feature = "kafka-auth-oauth")]
        Some(RawAuthConfig::OAuth(oauth)) => {
            if oauth.oauth.client_secret.is_literal() {
                is_secret = true;
            }

            if let Some(secrets_file) = oauth.oauth.client_secret.file_path() {
                check_file_permissions(secrets_file, true)?;
            }
        }

        #[cfg(feature = "kafka-auth-gssapi")]
        Some(RawAuthConfig::Kerberos(value)) => {
            check_file_permissions(&value.gssapi.keytab, true)?;
        }

        None => {}
    }

    #[cfg(feature = "kafka-tls")]
    if let Some(ref tls_config) = kafka_config.tls {
        if let Some(key_password) = &tls_config.key_password {
            if key_password.is_literal() {
                is_secret = true;
            }

            if let Some(secrets_file) = key_password.file_path() {
                check_file_permissions(secrets_file, true)?;
            }
        }

        check_file_permissions(&tls_config.ca_file, false)?;

        if let Some(ref path) = tls_config.cert_file {
            check_file_permissions(path, false)?;
        }

        if let Some(key_file) = &tls_config.key_file {
            check_file_permissions(key_file, true)?;
        }
    }

    check_file_permissions(config_path, is_secret)?;

    Ok(())
}

/// A TOML-deserialisable secret value accepted in three forms:
///
/// ```toml
/// # Inline literal — on Unix, the config file must give no access to the group and the others
/// password = "my-secret"
///
/// # Environment variable of the process, whose value cannot change until a restart
/// password = { env = "MY_SECRET_VAR" }
///
/// # Path to a secrets file, read again at every producer rebuild — on Unix, it must give no
/// # access to the group and the others, except for a `username`
/// password = { file = "/run/secrets/my_secret" }
/// ```
///
/// Resolved values are wrapped in [`secrecy::SecretString`]: formatting one with `Display` does not
/// compile, and `Debug` prints `[REDACTED]`.
/// Use [`SecretSource::resolve`] to obtain the underlying [`secrecy::SecretString`].
#[derive(Debug, Deserialize, Clone)]
#[serde(untagged, deny_unknown_fields)]
pub enum SecretSource {
    /// The secret is read from the environment variable `env`.
    Env {
        /// Name of the environment variable.
        env: String,
    },
    /// The secret is read from the file `file` (surrounding whitespace is trimmed).
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

/// The non-secret settings of a Kafka configuration, as recorded at startup.
///
/// Obtained with [`ProducerConfig::posture`]. The Kafka producer recovery (see
/// [`producer_factory`](crate::producer_factory)) keeps the posture of the startup configuration
/// and compares it, with [`check_against`](SecurityPosture::check_against), with the one of the
/// configuration it re-reads from the file before it rebuilds the producer. This turns a change of
/// the connection, made by editing the file while the daemon runs, into a refusal.
///
/// # What a posture covers
///
/// - the broker address and `security_protocol`, which reflects the presence of `[kafka.tls]` and
///   `[kafka.auth]`;
/// - `allow_insecure_transport`;
/// - the tuning: timeouts, retries, compression, idempotence, linger, acks, queue limits and
///   parallel requests;
/// - the **paths** of the TLS files (`kafka.tls.ca_file`, `cert_file`, `key_file`);
/// - the **name** of the SASL mechanism;
/// - the Kerberos principal, keytab path and service name, and the OAuth token endpoint, client
///   identifier, scope and extensions.
///
/// # What it does not cover
///
/// - the secrets: SASL username and password, OAuth client secret, TLS key password;
/// - the content of the TLS, keytab and secret files: a file replaced by another one at the same
///   path is not detected.
///
/// These are left out on purpose, so that they can be rotated without a restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityPosture {
    posture: BTreeMap<&'static str, String>,
}

impl SecurityPosture {
    /// Compares a `candidate` posture with this one, which is the reference, and refuses any
    /// difference, in either direction: an improvement is refused like a downgrade.
    ///
    /// When a setting differs, an error listing the names of these settings is logged, and
    /// [`NbdError::Config`] is returned. Their values are not logged. Everything a posture does not
    /// cover, such as a change of SASL credentials, is accepted without any message.
    ///
    /// The reference is not modified.
    ///
    /// # Errors
    ///
    /// Returns [`NbdError::Config`] when at least one setting differs. Its message tells that the
    /// changes can be applied by restarting NBD, since a startup trusts the file and has no
    /// reference to compare with.
    pub fn check_against(&self, candidate: &Self) -> Result<(), NbdError> {
        let mut errors: Vec<&str> = Vec::new();

        for setting in self.posture.keys().chain(candidate.posture.keys()) {
            if self.posture.get(setting) != candidate.posture.get(setting)
                && !errors.contains(setting)
            {
                errors.push(setting);
            }
        }

        match errors.is_empty() {
            true => Ok(()),
            false => {
                error!(
                    "Changes in the configuration file were refused ({}). They can be manually re-applied by restarting NBD.",
                    errors.join(", ")
                );
                Err(NbdError::Config(format!(
                    "Configuration file refused for security concerns : {}. These changes can be manually re-applied by restarting NBD.",
                    errors.join(", "),
                )))
            }
        }
    }
}
