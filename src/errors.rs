use thiserror::Error;

use std::error::Error;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::panic::{AssertUnwindSafe, RefUnwindSafe, UnwindSafe};

const _: () = {
    const fn require<T: Error + Display + Send + Sync + UnwindSafe + RefUnwindSafe + 'static>() {}
    require::<NbdError>();
};

/// Centralized error type for all NBD domain, networking, configuration, and runtime errors.
///
/// Follows ANSSI recommendation `LANG-ERRWRAP`: all sub-errors are wrapped into a dedicated
/// domain error type rather than being propagated as raw third-party errors.
#[derive(Error, Debug)]
pub enum NbdError {
    /// Metrics exporter initialization failure (only available with `metrics-exporter` feature).
    #[cfg(feature = "metrics-exporter")]
    #[error("{0}")]
    Setup(String),

    /// Socket creation or binding failure.
    #[error("Socket creation failed : {0}")]
    Socket(String),

    /// A spawned Tokio task panicked or was cancelled unexpectedly.
    #[error("A task failed : {0}")]
    TaskPanic(#[source] UnwindShield<tokio::task::JoinError>),

    /// Low-level I/O error (network, filesystem).
    #[error("Network error : {0}")]
    Network(#[source] UnwindShield<std::io::Error>),

    /// TOML deserialization error from the configuration file.
    #[error("Invalid TOML configuration: {0}")]
    Toml(#[from] toml::de::Error),

    /// Kafka client or delivery error from `librdkafka`.
    #[error("Kafka error : {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    /// Malformed or truncated UDP datagram that cannot be parsed.
    #[error("Invalid packet : {0}")]
    InvalidPacket(String),

    /// Semantic configuration error (invalid values, missing required fields).
    #[error("{0}")]
    Config(String),

    /// Graceful shutdown timeout — some tasks did not terminate in time.
    #[error("An error occured while terminating the process : {0}")]
    Termination(String),

    /// Backpressure error returned when a message sink buffer or transmission queue is full.
    #[error("{0}")]
    Backpressure(String),

    /// Kafka producer recovery failure.
    ///
    /// Returned in two situations:
    ///
    /// - the supervisor could not rebuild a faulted producer within the configured
    ///   `kafka.recovery_timeout` and gave up (the daemon then exits with an error);
    /// - a message was submitted while no producer was available.
    ///
    /// See [`ManagedProducer`](crate::ManagedProducer) for the recovery mechanism.
    #[error("Kafka producer recovery error : {0}")]
    Recovery(String),
}

impl From<std::io::Error> for NbdError {
    fn from(e: std::io::Error) -> Self {
        NbdError::Network(UnwindShield::new(e))
    }
}

impl From<tokio::task::JoinError> for NbdError {
    fn from(e: tokio::task::JoinError) -> Self {
        NbdError::TaskPanic(UnwindShield::new(e))
    }
}

pub struct UnwindShield<T>(std::panic::AssertUnwindSafe<T>);

impl<T> UnwindShield<T> {
    fn new(e: T) -> Self {
        UnwindShield(AssertUnwindSafe(e))
    }

    pub fn into_inner(self) -> T {
        self.0.0
    }
}

impl<T: std::fmt::Display> std::fmt::Display for UnwindShield<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0.0, f)
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for UnwindShield<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0.0, f)
    }
}

impl<T: Error> Error for UnwindShield<T> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Error::source(&self.0.0)
    }
}

impl<T: Deref> Deref for UnwindShield<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0.0
    }
}
