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
//! - **Error handling** ([`NbdError`]) — Centralized error types via [`thiserror`].
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
//! protected for the whole life of the daemon. [`ManagedProducer`] documents the behavior in
//! detail, including the security considerations, and [`wait_for_retired_producers`] is used at
//! shutdown to wait for replaced producers to be destroyed.
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
//! use nbd::{RawConfig, Config, Provider};
//! use std::path::PathBuf;
//!
//! let raw = RawConfig::from_path(&PathBuf::from("config.toml")).unwrap();
//! let config = Config::try_from(raw).unwrap();
//!
//! for provider_config in &config.provider {
//!     let mut provider = Provider::from(provider_config);
//!     // provider.subscribe(&config.nbd.socket_buffer_size).unwrap();
//!     // provider.start_listener(sink, cancel_token).await.unwrap();
//! }
//! ```

mod config;
mod errors;
mod kafka;
mod providers;
mod sinks;

pub use config::{
    AuthConfig, Config, Credentials, Interface, ProducerConfig, ProviderConfig, RawConfig,
    RawCredentials, SecretSource, SecurityMechanism, check_secrets,
};

pub use errors::{NbdError, UnwindShield};
pub use kafka::{
    ManagedProducer, RecoveryPolicy, build_producer, producer_factory, wait_for_retired_producers,
};
pub use providers::Provider;
pub use sinks::{BenchSink, IPerfSink, MessageSink};
