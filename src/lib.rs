#![forbid(unsafe_code)]
#![deny(
    clippy::mem_forget,
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::todo,
    clippy::unimplemented
)]
//! # Nothing But Data
//!
//! A lightweight, high-performance and secure data pipeline for ingesting real-time
//! UDP multicast traffic and relaying it to an Apache Kafka broker.
//!
//! ## Architecture
//!
//! The library is organized around five core areas:
//!
//! - **Configuration** ([`Config`], [`RawConfig`]) — Two-stage TOML configuration parsing and validation.
//! - **Error handling** ([`NbdError`], [`UnwindShield`]) — Centralized error types via [`thiserror`].
//! - **Multicast ingestion** ([`Provider`]) — UDP multicast socket management and zero-copy async receive loop.
//! - **Message sinks** ([`MessageSink`]) — Trait abstraction with Kafka, benchmark, and iperf implementations.
//! - **Kafka resilience** ([`ManagedProducer`], [`RecoveryPolicy`]) — Self-healing Kafka producer: a supervisor
//!   replaces the `librdkafka` client when it raises a fatal error.
//!
//! ## Kafka resilience
//!
//! The production sink is a [`ManagedProducer`], not a bare `rdkafka` producer. Its
//! [`supervise`](ManagedProducer::supervise) task watches the underlying client and, when it
//! raises a fatal error, rebuilds it through [`producer_factory`] following a [`RecoveryPolicy`],
//! while the listeners keep using the same handle. If the producer cannot be rebuilt within
//! `kafka.recovery_timeout`, the supervisor returns [`NbdError::Recovery`] and the daemon exits.
//!
//! Rebuilding a producer re-reads the `[kafka]` section of the configuration file and re-resolves
//! its secrets, so the configuration file and the files it references must stay readable and
//! protected for the whole life of the daemon. A rebuild that would weaken the security of the
//! connection (TLS removed, CA replaced, weaker SASL mechanism, ...) is refused: the
//! [`SecurityPosture`] of the new configuration is compared with the one in force. A change of
//! broker, and anything a posture doesn't cover, is applied. [`ManagedProducer`] documents the
//! behavior in detail, including the security considerations. At shutdown,
//! [`ManagedProducer::close`] waits for the producer to be released and destroyed.
//!
//! ## Configuration
//!
//! [`RawConfig::from_path`] reads the TOML file, then `Config::try_from` validates it and fills in
//! the defaults (two stages: a permissive raw form, then a strongly-typed validated form). Most
//! invalid values are replaced by a default and reported in the logs rather than refused. For
//! instance, an invalid topic becomes `nbd-connector`. A few are errors: no `[[provider]]`, a
//! group that is not multicast, an empty secret, a missing TLS or Kerberos file. The `[kafka]`
//! section rejects unknown keys, the other sections ignore them. The README documents every
//! parameter and its default.
//!
//! ## Hardening
//!
//! Both the library and the daemon forbid `unsafe` code. They also deny, through `clippy`, the
//! constructs that can panic or overflow silently: `unwrap`, `expect`, `panic!`, `unreachable!`,
//! `todo!`, `unimplemented!`, unchecked arithmetic, and unchecked indexing and string slicing.
//! These lints are enforced by `cargo clippy` (run with `-D warnings` in CI), not by a plain
//! `cargo build`, and they don't apply to the crates of `tests/`.
//!
//! Fallible code returns [`NbdError`], which is checked at compile time to be `Send`, `Sync`,
//! `UnwindSafe` and `RefUnwindSafe` (see [`UnwindShield`]). Secrets are erased explicitly once
//! they have been used ([`ProducerConfig::erase_secrets`]), in addition to being zeroized when
//! dropped.
//!
//! ## Usage
//!
//! The main binary (`src/main.rs`) consumes this library to orchestrate the full daemon lifecycle.
//! Integration tests in `tests/` use the library directly with in-memory sinks ([`BenchSink`], [`IPerfSink`])
//! to validate the pipeline without requiring a Kafka broker.
//!
//! ## Example
//!
//! ```no_run
//! use nbd::{Config, NbdError, Provider, RawConfig};
//! use std::path::PathBuf;
//!
//! # fn main() -> Result<(), NbdError> {
//! let raw = RawConfig::from_path(&PathBuf::from("config.toml"))?;
//! let config = Config::try_from(raw)?;
//!
//! for provider_config in &config.provider {
//!     let mut provider = Provider::from(provider_config);
//!     // provider.subscribe(&config.nbd.socket_buffer_size)?;
//!     // provider.start_listener(sink, cancel_token).await?;
//! }
//! # Ok(())
//! # }
//! ```

mod config;
mod errors;
mod kafka;
mod providers;
mod sinks;

pub use config::{
    AuthConfig, Config, Credentials, Interface, ProducerConfig, ProviderConfig, RawConfig,
    RawCredentials, SecretSource, SecurityMechanism, SecurityPosture, check_secrets,
};

pub use errors::{NbdError, UnwindShield};
pub use kafka::{
    ManagedProducer, RecoveryPolicy, build_producer, producer_factory, wait_for_retired_producers,
};
pub use providers::Provider;
pub use sinks::{BenchSink, IPerfSink, MessageSink};
