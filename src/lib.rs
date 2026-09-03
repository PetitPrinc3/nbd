//! # Nothing But Data
//!
//! A lightweight, high-performance and secure data pipeline for ingesting real-time
//! UDP multicast traffic and relaying it to an Apache Kafka broker.
//!
//! ## Architecture
//!
//! The library is organized around four core modules:
//!
//! - **[`config`]** — Two-stage TOML configuration parsing and validation.
//! - **[`errors`]** — Centralized error types via [`thiserror`].
//! - **[`providers`]** — UDP multicast socket management and zero-copy async receive loop.
//! - **[`sinks`]** — [`MessageSink`] trait abstraction with Kafka, benchmark, and iperf implementations.
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
//! use nothing_but_data::{RawConfig, Config, Provider};
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
mod providers;
mod sinks;

pub use config::{Config, Interface, ProviderConfig, RawConfig};
pub use errors::NbdError;
pub use providers::Provider;
pub use sinks::{BenchSink, IPerfSink, MessageSink};
