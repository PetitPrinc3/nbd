use thiserror::Error;

use std::error::Error;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use std::panic::{AssertUnwindSafe, RefUnwindSafe, UnwindSafe};

// Compile-time guarantee: `NbdError` can be shared across threads and can cross a panic boundary
// (`Send + Sync + UnwindSafe + RefUnwindSafe`). The build fails if a variant ever carries a type
// that breaks one of these properties. This is why `std::io::Error` and `tokio::task::JoinError`,
// which are not unwind safe (they hold `dyn Error + Send + Sync` and `dyn Any + Send` trait
// objects), are wrapped in an `UnwindShield`.
const _: () = {
    const fn require<T: Error + Display + Send + Sync + UnwindSafe + RefUnwindSafe + 'static>() {}
    require::<NbdError>();
};

/// Centralized error type for all NBD domain, networking, configuration, and runtime errors.
///
/// Follows ANSSI recommendation `LANG-ERRWRAP`: all sub-errors are wrapped into a dedicated
/// domain error type rather than being propagated as raw third-party errors.
///
/// # Guarantees
///
/// `NbdError` implements [`Error`], [`Display`], [`Send`], [`Sync`], [`UnwindSafe`] and
/// [`RefUnwindSafe`], and is `'static`. This is checked at compile time.
///
/// [`std::io::Error`] and [`tokio::task::JoinError`] are neither [`UnwindSafe`] nor
/// [`RefUnwindSafe`], so the [`Network`](NbdError::Network) and [`TaskPanic`](NbdError::TaskPanic)
/// variants hold them inside an [`UnwindShield`]. Build these two variants with
/// `NbdError::from(error)`, and get the original error back with [`UnwindShield::into_inner`].
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
    ///
    /// The [`JoinError`](tokio::task::JoinError) is held in an [`UnwindShield`]. Build this variant
    /// with `NbdError::from(join_error)`.
    #[error("A task failed : {0}")]
    TaskPanic(#[source] UnwindShield<tokio::task::JoinError>),

    /// Low-level I/O error (network, filesystem).
    ///
    /// The [`io::Error`](std::io::Error) is held in an [`UnwindShield`]. Build this variant with
    /// `NbdError::from(io_error)`, which is what the `?` operator does.
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

/// Wraps the error in an [`UnwindShield`] and returns it as [`NbdError::Network`].
impl From<std::io::Error> for NbdError {
    fn from(e: std::io::Error) -> Self {
        NbdError::Network(UnwindShield::new(e))
    }
}

/// Wraps the error in an [`UnwindShield`] and returns it as [`NbdError::TaskPanic`].
impl From<tokio::task::JoinError> for NbdError {
    fn from(e: tokio::task::JoinError) -> Self {
        NbdError::TaskPanic(UnwindShield::new(e))
    }
}

/// Wrapper that lets an error type that is not unwind safe live inside [`NbdError`].
///
/// [`std::io::Error`] and [`tokio::task::JoinError`] hold trait objects (`dyn Error + Send + Sync`
/// and `dyn Any + Send`) that are neither [`UnwindSafe`] nor [`RefUnwindSafe`], which would make
/// [`NbdError`] neither of them. `UnwindShield` wraps the error in [`AssertUnwindSafe`] so that
/// it is.
///
/// # What this does and does not guarantee
///
/// [`AssertUnwindSafe`] is a statement by the author, not a proof: it asserts that observing the
/// wrapped error after a panic cannot expose a broken invariant. It is a reasonable assertion
/// here because the wrapped errors are created once and then only read, through [`Display`],
/// [`Debug`] or [`Error::source`]. The wrapper doesn't make the inner type any safer otherwise.
///
/// The wrapper is transparent: [`Display`], [`Debug`] and [`Error::source`] are forwarded to the
/// inner error, so the message of an [`NbdError`] is the same as without the wrapper. [`Deref`] is
/// implemented, with the wrapped type as target, only when that type itself implements [`Deref`],
/// which is not the case of the two types held by [`NbdError`]. Use
/// [`into_inner`](UnwindShield::into_inner) to get the error back.
///
/// It cannot be built outside of this crate: obtain one by matching an [`NbdError`].
pub struct UnwindShield<T>(std::panic::AssertUnwindSafe<T>);

impl<T> UnwindShield<T> {
    fn new(e: T) -> Self {
        UnwindShield(AssertUnwindSafe(e))
    }

    /// Consumes the wrapper and returns the original error.
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
