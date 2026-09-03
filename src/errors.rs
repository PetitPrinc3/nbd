use thiserror::Error;

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
    TaskPanic(#[from] tokio::task::JoinError),

    /// Low-level I/O error (network, filesystem).
    #[error("Network error : {0}")]
    Network(#[from] std::io::Error),

    /// TOML deserialization error from the configuration file.
    #[error("Toml error : {}", .0.message())]
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
}
