//! Kafka producer construction and self-healing supervision.
//!
//! A `librdkafka` producer that has raised a *fatal* error can no longer be used and has to be
//! recreated (see [`Client::fatal_error`](rdkafka::client::Client::fatal_error)). In practice,
//! `librdkafka` raises such errors for idempotent producers (`kafka.idempotence = true`, which is
//! the default together with `kafka.acks = "all"`). This module lets the daemon survive such an
//! event: the listeners keep a stable handle on "the producer", while a background supervisor
//! swaps the underlying `librdkafka` client for a fresh one.
//!
//! ## Building blocks
//!
//! - [`build_producer`] translates a validated [`ProducerConfig`] (tuning, TLS, SASL) into a
//!   `librdkafka` [`FutureProducer`].
//! - [`ManagedProducer`] is a cheaply cloneable handle implementing [`MessageSink`]. The current
//!   producer lives in a lock-free [`ArcSwap`] cell, so [`submit`](MessageSink::submit) never
//!   waits for a swap. Every swap increments the *generation* of the producer
//!   ([`ManagedProducer::generation`]).
//! - [`ManagedProducer::supervise`] is the supervision loop. It is meant to run as one dedicated
//!   task for the whole life of the daemon.
//! - [`producer_factory`] builds the factory the supervisor calls to obtain a new producer. It
//!   **re-reads the configuration file** every time it is called.
//! - [`RecoveryPolicy`] tunes the supervisor: polling interval, back-off and give-up delay.
//! - [`wait_for_retired_producers`] lets the shutdown sequence wait until every replaced producer
//!   has actually been destroyed.
//!
//! ## Recovery lifecycle
//!
//! ```text
//!             fatal error                      factory succeeded
//! healthy ────────────────▶ faulted ───────────────────────────▶ healthy (generation + 1)
//!                              │
//!                              └─ factory keeps failing for `give_up_after`
//!                                   └─▶ NbdError::Recovery, the daemon exits with an error
//! ```
//!
//! The supervisor wakes up every [`RecoveryPolicy::poll_interval`], or immediately when
//! [`submit`](MessageSink::submit) notices that the producer is faulted, and checks whether the
//! current producer has raised a fatal error.
//!
//! The behavior while a producer is faulted, the meaning of a failed recreation, the exported
//! metrics, the security considerations and a complete example are documented on
//! [`ManagedProducer`].

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, Producer};

use bytes::Bytes;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{RecvTimeoutError, Sender, channel};
use std::sync::{Arc, OnceLock};

use std::time::{Duration, Instant};

use std::path::PathBuf;

use tokio::sync::Notify;
use tokio::task::spawn_blocking;
use tokio::time::{MissedTickBehavior, interval, sleep, sleep_until};
use tokio_util::sync::CancellationToken;

use arc_swap::ArcSwap;

use secrecy::ExposeSecret;

use tracing::{debug, error, info, warn};

use crate::sinks::submit_on;
use crate::{AuthConfig, MessageSink, NbdError, ProducerConfig, RawConfig, check_secrets};

/// Channel feeding the `nbd-kafka-reaper` thread, which destroys retired producers.
///
/// Initialized lazily by [`reaper`] and alive for the rest of the process: the thread is
/// intentionally never joined.
static REAPER: OnceLock<Sender<ReaperTargets>> = OnceLock::new();

/// Messages understood by the reaper thread. They are handled in order.
enum ReaperTargets {
    /// A retired producer to destroy.
    Target(FutureProducer),
    /// A marker acknowledged once every `Target` queued before it has been destroyed. See
    /// [`wait_for_retired_producers`].
    Completion(Sender<()>),
}

/// Returns the channel to the reaper thread, spawning the thread on first use.
///
/// Destroying a `librdkafka` client is a blocking operation (`rd_kafka_destroy`), so it is done
/// off the Tokio worker threads and off the hot path.
///
/// If the thread cannot be spawned, a warning is logged and the channel is returned with its
/// receiving end already gone: sending to it then fails, and the retired producer is destroyed
/// synchronously by the caller of [`retire`], which may block it.
fn reaper() -> &'static Sender<ReaperTargets> {
    REAPER.get_or_init(|| {
        let (tx, rx) = channel::<ReaperTargets>();
        match std::thread::Builder::new().name(String::from("nbd-kafka-reaper")).spawn(move || {
            for item in rx {
                match item {
                    ReaperTargets::Target(target) => {
                        let start = Instant::now();
                        drop(target);
                        debug!("Dropped producer in {}ms.", start.elapsed().as_millis())
                    }
                    ReaperTargets::Completion(sender) => {
                        let _ = sender.send(());
                    }
                }
            }
        }) {
            Ok(_) => {},
            Err(e) => {
                warn!("Failed to create the cleaner thread: {}. Expect increased latency and data loss in the event of a fatal failure from the Kafka broker.", e);
            }
        }
        tx
    })
}

/// Blocks until every producer retired so far has been destroyed.
///
/// Meant for the shutdown sequence, to make sure no `librdkafka` client is still being torn down
/// when the process exits. It queues a marker behind the retired producers on the reaper thread,
/// and waits for its acknowledgment.
///
/// **This function blocks the calling thread** for up to `timeout`.
///
/// The producer currently in use is only retired once the last [`ManagedProducer`] handle
/// referencing it is dropped. Stop the supervisor and drop every handle first, otherwise that
/// producer is not waited for.
///
/// Returns `Ok(())` immediately when no producer was ever managed, or when the reaper thread is
/// gone.
///
/// # Errors
///
/// Returns [`NbdError::Termination`] if the retired producers were not all destroyed within
/// `timeout`.
pub fn wait_for_retired_producers(timeout: Duration) -> Result<(), NbdError> {
    match REAPER.get() {
        Some(reaper_tx) => {
            let (tx, rx) = channel::<()>();
            let _ = reaper_tx.send(ReaperTargets::Completion(tx));

            match rx.recv_timeout(timeout) {
                Ok(_) => {
                    info!("Successfully terminated all Kafka producers.");
                    Ok(())
                }
                Err(RecvTimeoutError::Timeout) => Err(NbdError::Termination(String::from(
                    "Timed out while waiting for proper producer cancellation to occur.",
                ))),
                Err(RecvTimeoutError::Disconnected) => Ok(()),
            }
        }
        None => Ok(()),
    }
}

/// Builds a `librdkafka` producer from a validated [`ProducerConfig`].
///
/// Applies, in this order:
///
/// 1. the TLS settings (`ssl.*`), when `config.tls` is set;
/// 2. `security.protocol`, as derived by the configuration validation;
/// 3. the SASL settings, for the configured authentication mechanism
///    (credentials, Kerberos with `kafka-auth-gssapi`, OAuth 2.0 with `kafka-auth-oauth`);
/// 4. the broker address and the producer tuning (timeouts, retries, compression, idempotence,
///    queueing, batching and acknowledgments).
///
/// Secrets are resolved and exposed only to fill the `librdkafka` client configuration, which is
/// local to this function and dropped on return. `librdkafka` keeps its own copy for
/// authentication and re-authentication. The strings of that client configuration are plain
/// `String`s: they are released, but not zeroized.
///
/// Creating the producer **does not wait for the broker**: `librdkafka` connects in the
/// background, so success here does not prove that the broker is reachable. The daemon probes the
/// broker separately, once, at startup.
///
/// This is the function used both for the initial producer and, through [`producer_factory`],
/// for every recreation.
///
/// # Errors
///
/// Returns [`NbdError::Config`] when a secret cannot be resolved (missing environment variable,
/// unreadable secrets file), and [`NbdError::Kafka`] when `librdkafka` rejects the configuration
/// or fails to create the client (for instance, unusable TLS material).
pub fn build_producer(config: &ProducerConfig) -> Result<FutureProducer, NbdError> {
    let mut kafka_config = ClientConfig::new();

    // TLS config application
    if let Some(ref tls_config) = config.tls {
        kafka_config.set("ssl.ca.location", tls_config.ca_file.display().to_string());
        if let Some(cert_file) = &tls_config.cert_file {
            kafka_config.set("ssl.certificate.location", cert_file.display().to_string());
        }
        if let Some(key_file) = &tls_config.key_file {
            kafka_config.set("ssl.key.location", key_file.display().to_string());
        }
        if let Some(key_password) = &tls_config.key_password {
            kafka_config.set("ssl.key.password", key_password.resolve()?.expose_secret());
        }
    };

    // Authentication config application
    kafka_config.set("security.protocol", config.security_protocol.to_string());

    if let Some(ref auth_config) = config.auth {
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
                kafka_config.set(
                    "sasl.oauthbearer.client.secret",
                    oauth.client_secret.resolve()?.expose_secret(),
                );
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

    Ok(kafka_config
        .set("bootstrap.servers", &config.broker)
        .set("message.timeout.ms", config.message_timeout.to_string())
        .set(
            "message.send.max.retries",
            config.message_retries.to_string(),
        )
        .set("compression.type", config.compression.to_string())
        .set(
            "max.in.flight.requests.per.connection",
            config.parallel_requests.to_string(),
        )
        .set(
            "queue.buffering.max.messages",
            config.queue_size.to_string(),
        )
        .set("enable.idempotence", config.idempotence.to_string())
        .set("linger.ms", config.linger.to_string())
        .set("acks", config.acks.to_string())
        .create::<rdkafka::producer::FutureProducer>()?)
}

/// One generation of the `librdkafka` producer held by a [`ManagedProducer`].
///
/// A cell is published once and is never mutated afterwards, apart from its `faulted` flag:
/// replacing the producer means publishing a new cell, with `generation + 1`, in the
/// [`ProducerContainer`]. The previous cell is dropped as soon as its last reader releases it.
pub struct ProducerCell {
    /// The client. `None` only while the cell is being dropped, once the client has been handed
    /// over for destruction.
    producer: Option<FutureProducer>,
    /// Number of replacements since startup: `0` for the initial producer.
    generation: u64,
    /// Set the first time a submission observes a fatal error on this cell, so that the
    /// supervisor is notified only once per cell.
    faulted: AtomicBool,
}

impl ProducerCell {
    /// Returns the fatal error raised by this producer, formatted as `<text> (<code>)`, if any.
    ///
    /// This is how the supervisor detects a faulted producer, see
    /// [`Client::fatal_error`](rdkafka::client::Client::fatal_error).
    pub fn fatal_reason(&self) -> Option<String> {
        match self.producer {
            Some(ref producer) => producer
                .client()
                .fatal_error()
                .map(|(code, text)| format!("{} ({})", text, code)),
            None => None,
        }
    }
}

/// Hands the `librdkafka` client over to the reaper thread instead of destroying it in place.
///
/// This `Drop` implementation is needed because dropping a [`FutureProducer`] ends in
/// `rd_kafka_destroy`, which is a blocking operation. A cell is dropped on whichever thread
/// releases its last reference, typically a Tokio worker serving a listener or the supervisor,
/// where blocking would stall the ingestion path. Moving the client to the dedicated reaper
/// thread keeps that cost out of the hot path.
///
/// It does not panic: a failure to spawn the reaper thread is handled by [`reaper`], and the
/// result of the hand-over is ignored. It enforces no security property either: it only releases
/// a resource. Note that when it returns, the client is not necessarily destroyed yet; the
/// shutdown sequence waits for that explicitly, see [`wait_for_retired_producers`].
impl Drop for ProducerCell {
    fn drop(&mut self) {
        if let Some(producer) = self.producer.take() {
            retire(producer);
        }
    }
}

/// Sends a producer to the reaper thread, which destroys it.
///
/// If the reaper is unavailable, the send fails and the producer is dropped right here,
/// synchronously.
fn retire(producer: FutureProducer) {
    let _ = reaper().send(ReaperTargets::Target(producer));
}

/// State shared by every clone of a [`ManagedProducer`].
pub struct ProducerContainer {
    /// The current producer generation. Readers load it without locking, and only the supervisor
    /// replaces it.
    cell: ArcSwap<ProducerCell>,
    /// Wakes the supervisor up ahead of its next poll when a submission hits a fatal error.
    notifier: Notify,
    /// Fatal submission errors seen since the last recreation. Reported, then reset, when the
    /// supervisor installs a new producer.
    errors: AtomicU64,
}

/// A Kafka producer that repairs itself after a fatal error.
///
/// This is the production [`MessageSink`]. It is a cheaply cloneable handle: every clone refers
/// to the same underlying `librdkafka` producer, which [`supervise`](ManagedProducer::supervise)
/// replaces by a fresh one when it raises a fatal error. Listeners keep using their handle and
/// transparently pick up the new producer; reading the current producer never blocks, even
/// during a swap.
///
/// Each replacement increments the *generation* of the producer, see
/// [`generation`](ManagedProducer::generation). The replaced producer is destroyed by a
/// dedicated thread, see [`wait_for_retired_producers`] to wait for that at shutdown.
///
/// ## Behavior while a producer is faulted
///
/// - [`submit`](MessageSink::submit) fails with
///   `NbdError::Kafka(KafkaError::MessageProduction(RDKafkaErrorCode::Fatal))`. The datagram is
///   lost (a UDP multicast feed cannot be replayed) and counted by the listener as a sender error.
/// - Messages already in flight on the faulted producer may or may not be delivered: this is up
///   to `librdkafka`. NBD never flushes a producer before retiring it.
/// - As soon as the new producer is published, new submissions use it.
///
/// ## What triggers a recovery
///
/// The supervisor reacts to *fatal* errors reported by `librdkafka`, not to a broker that is
/// merely slow or unreachable: `librdkafka` reconnects on its own in those cases, and the
/// failures show up as delivery errors instead. `librdkafka` raises fatal errors for idempotent
/// producers, which is the default configuration (`kafka.idempotence = true` with
/// `kafka.acks = "all"`). Disabling idempotence, which `kafka.acks` other than `all`/`-1` does
/// implicitly, leaves the supervisor with little to detect.
///
/// ## What a failed recreation means
///
/// Building a producer does **not** wait for the broker, so a recreation does not fail because
/// the broker is unreachable, and, unlike at startup, the broker is not probed afterwards. A
/// recreation fails, and counts against `kafka.recovery_timeout`, when the factory fails:
/// unreadable or invalid configuration file, unresolvable secrets, a permission violation on a
/// secret-bearing file, or TLS/SASL material rejected by `librdkafka`.
///
/// ## Metrics
///
/// With the `metrics-exporter` feature, the following series are exported. [`new`] initializes
/// them at `0`.
///
/// | Metric                          | Type    | Meaning                                                     |
/// | ------------------------------- | ------- | ----------------------------------------------------------- |
/// | `nbd_errors_kafka_fatal_total`  | Counter | Fatal errors detected on a producer                         |
/// | `nbd_kafka_recreation_total`    | Counter | Successful producer recreations                             |
/// | `nbd_kafka_producer_generation` | Gauge   | Generation of the current producer (`0` is the initial one) |
///
/// [`new`]: ManagedProducer::new
///
/// ## Security considerations
///
/// Because [`producer_factory`] re-reads the `[kafka]` section and re-resolves its secrets
/// (environment variables, secret files) every time a producer is rebuilt:
///
/// - the configuration file and every file it references (CA, certificate, key, keytab, secret
///   files) must stay readable, and protected against unauthorized modification, for the whole
///   life of the daemon;
/// - a change made to the `[kafka]` section after startup (broker, TLS, SASL) takes effect at the
///   next recreation, without a restart. The other sections are not re-read;
/// - the file permission checks of [`check_secrets`] are enforced again;
/// - the configuration warnings are muted during the rebuild, so a weakened configuration (for
///   instance SASL credentials without TLS) is not reported by a warning;
/// - credentials briefly live again in the process memory while the new producer is built.
///
/// See `SECURITY.md` for the corresponding deployment recommendations.
///
/// ## Example
///
/// ```no_run
/// use std::path::PathBuf;
/// use std::time::Duration;
///
/// use nbd::{
///     Config, ManagedProducer, RawConfig, RecoveryPolicy, build_producer, producer_factory,
///     wait_for_retired_producers,
/// };
/// use tokio_util::sync::CancellationToken;
///
/// # async fn run() -> Result<(), nbd::NbdError> {
/// let path = PathBuf::from("config.toml");
/// let config = Config::try_from(RawConfig::from_path(&path)?)?;
///
/// // Build the first producer and wrap it.
/// let producer = ManagedProducer::new(build_producer(&config.kafka)?);
///
/// // Supervise it for the whole life of the daemon.
/// let policy = RecoveryPolicy::new(Duration::from_millis(config.kafka.recovery_timeout));
/// let token = CancellationToken::new();
/// let mut supervised = producer.clone();
/// let supervisor = tokio::spawn({
///     let token = token.clone();
///     async move {
///         supervised
///             .supervise(policy, producer_factory(path), token)
///             .await
///     }
/// });
///
/// // `producer` can now be handed to every provider as a `MessageSink`.
///
/// // Shutdown: stop the supervisor, drop every handle, wait for the retired producers.
/// token.cancel();
/// let _ = supervisor.await;
/// drop(producer);
/// wait_for_retired_producers(Duration::from_secs(5))?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ManagedProducer {
    container: Arc<ProducerContainer>,
}

impl ManagedProducer {
    /// Wraps an already created producer, as generation `0`.
    ///
    /// Starts the reaper thread that will destroy retired producers, and, with the
    /// `metrics-exporter` feature, registers the recovery metrics at `0`.
    ///
    /// The producer is not supervised until [`supervise`](ManagedProducer::supervise) runs.
    pub fn new(producer: FutureProducer) -> Self {
        reaper();

        #[cfg(feature = "metrics-exporter")]
        {
            metrics::counter!("nbd_kafka_recreation_total").increment(0);
            metrics::counter!("nbd_errors_kafka_fatal_total").increment(0);
            metrics::gauge!("nbd_kafka_producer_generation").set(0.0);
        }

        ManagedProducer {
            container: Arc::new(ProducerContainer {
                cell: ArcSwap::from_pointee(ProducerCell {
                    producer: Some(producer),
                    generation: 0,
                    faulted: AtomicBool::new(false),
                }),
                notifier: Notify::new(),
                errors: AtomicU64::new(0),
            }),
        }
    }

    /// Returns the generation of the current producer: `0` for the initial one, incremented at
    /// each successful recreation.
    pub fn generation(&self) -> u64 {
        self.container.cell.load().generation
    }

    /// Supervises the producer until cancelled, replacing it whenever it raises a fatal error.
    ///
    /// Run this once, as a dedicated task, for the whole life of the daemon. The task checks the
    /// current producer every [`RecoveryPolicy::poll_interval`], and straight away when a
    /// submission reports a fatal error.
    ///
    /// When a fatal error is detected:
    ///
    /// 1. the error is logged and counted;
    /// 2. unless the previous recreation is older than [`RecoveryPolicy::stability_threshold`],
    ///    the supervisor waits for the back-off delay of the current level;
    /// 3. it calls `factory` (on a blocking thread, since building a producer is not async) until
    ///    it succeeds, spacing the attempts with the back-off. Attempts stop once
    ///    [`RecoveryPolicy::give_up_after`] has elapsed since the first failed one;
    /// 4. the new producer is published with `generation + 1`, and the old one is retired.
    ///
    /// `factory` is typically the result of [`producer_factory`].
    ///
    /// Returns `Ok(())` when `token` is cancelled, including in the middle of a recreation.
    ///
    /// # Errors
    ///
    /// - [`NbdError::Recovery`] when the producer could not be recreated within
    ///   [`RecoveryPolicy::give_up_after`]. The caller is expected to terminate the daemon.
    /// - [`NbdError::TaskPanic`] when the blocking task running `factory` panicked.
    pub async fn supervise(
        &mut self,
        policy: RecoveryPolicy,
        factory: ProducerFactory,
        token: CancellationToken,
    ) -> Result<(), NbdError> {
        let mut tic = interval(policy.poll_interval);
        tic.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut stability_indicator: u32 = 0;
        let mut last_generation: Option<Instant> = None;

        loop {
            tokio::select! {
                _ = tic.tick() => {},
                _ = self.container.notifier.notified() => {},
                _ = token.cancelled() => {
                    info!("Producer supervisor was cancelled.");
                    return Ok(());
                }
            }

            let mut current_incident: Option<Instant> = None;

            let detected = Instant::now();

            let cell = self.container.cell.load_full();

            if let Some(reason) = cell.fatal_reason() {
                error!(
                    "The connection with the Kafka broker (generation {}) suffered a fatal error : {}",
                    cell.generation, reason
                );

                #[cfg(feature = "metrics-exporter")]
                metrics::counter!("nbd_errors_kafka_fatal_total").increment(1);

                if last_generation.is_none_or(|t| t.elapsed() > policy.stability_threshold) {
                    stability_indicator = 0;
                } else {
                    stability_indicator = stability_indicator.saturating_add(1);
                    let delay = policy.backoff_delay(stability_indicator);

                    warn!("Next attempt at recreating the connection in {:?}.", delay);

                    tokio::select! {
                        _ = token.cancelled() => {
                            warn!("Failed to recreate a connection with the Kafka broker (cancelled).");
                            return Ok(());
                        },
                        _ = sleep(delay) => {},
                    }
                }

                // Generate new producer
                let mut generation_errors: u32 = 0;
                let new_producer = loop {
                    let build = factory.clone();

                    let e = match spawn_blocking(move || build()).await {
                        Ok(Ok(producer)) => {
                            break producer;
                        }
                        Ok(Err(e)) => e,
                        Err(e) => return Err(NbdError::from(e)),
                    };

                    let since = current_incident.get_or_insert_with(Instant::now);

                    if since.elapsed() >= policy.give_up_after {
                        error!(
                            "Failed to recreate a connection with the Kafka broker. Giving up after {:?}. (policy `kafka.recovery_timeout`)",
                            policy.give_up_after
                        );

                        return Err(NbdError::Recovery(format!(
                            "Failed to recreate a connection with the Kafka broker : {e}. Giving up after {:?}. (policy `kafka.recovery_timeout`)",
                            policy.give_up_after
                        )));
                    }

                    generation_errors = generation_errors.saturating_add(1);

                    error!(
                        "An error occurred while recreating a connection with the Kafka broker (attempt {}) : {}",
                        generation_errors, e
                    );

                    let level_delay =
                        policy.backoff_delay(stability_indicator.saturating_add(generation_errors));

                    warn!(
                        "Next attempt at recreating a connection with the broker in {:?}",
                        level_delay
                    );

                    let capped_delay = since.checked_add(policy.give_up_after);

                    tokio::select! {
                        _= token.cancelled() => {
                            warn!("Failed to recreate a connection with the Kafka broker (cancelled).");
                            return Ok(());
                        },
                        _ = sleep(level_delay) => {},
                        _ = sleep_until(capped_delay.unwrap_or_else(Instant::now).into()), if capped_delay.is_some() => {},
                    }
                };

                // Swap broken producer with new one
                self.container.cell.swap(Arc::new(ProducerCell {
                    producer: Some(new_producer),
                    generation: cell.generation.saturating_add(1),
                    faulted: AtomicBool::new(false),
                }));

                drop(cell);

                // Reset error count
                let errors = self.container.errors.swap(0, Ordering::Relaxed);

                info!(
                    "Successfully recreated a connection with the Kafka broker in {:?} ({} errors).",
                    detected.elapsed(),
                    errors
                );

                #[cfg(feature = "metrics-exporter")]
                {
                    metrics::counter!("nbd_kafka_recreation_total").increment(1);
                    metrics::gauge!("nbd_kafka_producer_generation")
                        .set(self.container.cell.load().generation as f64);
                }

                // Store generation instant
                last_generation = Some(Instant::now());
            }
        }
    }
}

/// Submits the message through the current producer generation.
///
/// Same contract as the plain [`FutureProducer`] sink, plus the recovery handling:
///
/// - a full producer queue yields [`NbdError::Backpressure`];
/// - a fatal error yields `NbdError::Kafka(KafkaError::MessageProduction(RDKafkaErrorCode::Fatal))`.
///   The first submission to notice it on a given producer also wakes the supervisor up, and every
///   one of them is counted for the recovery report;
/// - [`NbdError::Recovery`] is returned if the cell holds no producer, a defensive case that
///   should not occur in normal operation.
impl MessageSink for ManagedProducer {
    type Transaction =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), NbdError>> + Send>>;

    fn submit(&self, topic: Arc<str>, payload: Bytes) -> Result<Self::Transaction, NbdError> {
        let cell = self.container.cell.load();

        match cell.producer {
            Some(ref producer) => match submit_on(producer, topic, payload) {
                Ok(result) => Ok(result),
                Err(NbdError::Kafka(rdkafka::error::KafkaError::MessageProduction(
                    rdkafka::error::RDKafkaErrorCode::Fatal,
                ))) => {
                    if !cell.faulted.swap(true, Ordering::Relaxed) {
                        self.container.notifier.notify_one();
                    }

                    self.container.errors.fetch_add(1, Ordering::Relaxed);

                    Err(NbdError::Kafka(
                        rdkafka::error::KafkaError::MessageProduction(
                            rdkafka::error::RDKafkaErrorCode::Fatal,
                        ),
                    ))
                }
                Err(e) => Err(e),
            },
            None => Err(NbdError::Recovery(String::from("no producer available."))),
        }
    }
}

/// Builds a fresh producer on demand. Shared between the supervisor and its blocking tasks.
pub type ProducerFactory = Arc<dyn Fn() -> Result<FutureProducer, NbdError> + Send + Sync>;

/// Creates the factory the supervisor uses to rebuild a faulted producer.
///
/// Each call of the factory goes through the same steps as the startup, **starting again from the
/// configuration file** at `config_path`:
///
/// 1. read and parse the file;
/// 2. enforce the permission checks on the files that carry secrets ([`check_secrets`]);
/// 3. validate the `[kafka]` section, with the log output muted;
/// 4. resolve the secrets again and call [`build_producer`].
///
/// As a consequence, an edit of the `[kafka]` section made while the daemon runs (new broker,
/// rotated credentials, other TLS files) is picked up at the next recreation, and the files it
/// references must still be readable. Only that section is used: nothing else is reloaded.
/// The broker is not probed, see [`build_producer`].
///
/// The validation of step 3 runs with `tracing` muted: its warnings (low timeouts, SASL
/// credentials without TLS, ...) were already emitted at startup and would only be noise at each
/// attempt, but they are therefore not emitted either for a configuration that was changed since.
///
/// # Errors
///
/// Each call of the returned factory fails with [`NbdError::Config`] when the file cannot be
/// read, is no longer valid TOML, violates the permission checks, or contains an invalid value, and
/// with the errors of [`build_producer`] otherwise. The message for an invalid TOML file gives the
/// line and column but does not repeat the offending text, which could contain a secret literal.
pub fn producer_factory(config_path: PathBuf) -> ProducerFactory {
    Arc::new(move || {
        let raw_config = match std::fs::read_to_string(&config_path) {
            Ok(content) => match toml::from_str::<RawConfig>(&content) {
                Ok(config) => config,
                Err(e) => {
                    let span = e.span();

                    match span {
                        Some(range) if let Some(before) = content.get(..range.start) => {
                            let span = before.split("\n");

                            let line = span.clone().count();
                            let col = match span.last() {
                                Some(line) => line.chars().count().saturating_add(1),
                                None => 0,
                            };

                            return Err(NbdError::Config(format!(
                                "The configuration file under {} is no longer valid (line {}, column {}). Run `nbd --check-config-file {}` to see the details.",
                                config_path.display(),
                                line,
                                col,
                                config_path.display(),
                            )));
                        }
                        _ => {
                            return Err(NbdError::Config(format!(
                                "The configuration file under {} is no longer valid. Run `nbd --check-config-file {}` to see the details.",
                                config_path.display(),
                                config_path.display(),
                            )));
                        }
                    }
                }
            },
            Err(e) => {
                return Err(NbdError::Config(format!(
                    "Failed to read the configuration file under {} : {}",
                    config_path.display(),
                    e
                )));
            }
        };

        check_secrets(&raw_config.kafka, &config_path)?;

        let config = tracing::subscriber::with_default(
            tracing::subscriber::NoSubscriber::default(),
            || ProducerConfig::try_from(raw_config.kafka),
        )?;

        build_producer(&config)
    })
}

/// Timing parameters of [`ManagedProducer::supervise`].
///
/// Only [`give_up_after`](RecoveryPolicy::give_up_after) is user-facing, through
/// `kafka.recovery_timeout`; [`RecoveryPolicy::new`] sets the other fields to fixed defaults.
///
/// The delay before the attempt of a given back-off level is
/// `backoff_base * 2^(level - 1)`, capped at `backoff_max`, and `0` for level `0`. With the
/// defaults: immediately, then `1s`, `2s`, `4s`, `8s`, ... up to `60s`.
#[derive(Debug, Clone, Copy)]
pub struct RecoveryPolicy {
    /// How long the supervisor keeps failing to recreate a producer before giving up and
    /// returning [`NbdError::Recovery`]. Measured from the first failed attempt of an incident.
    /// Configured by `kafka.recovery_timeout`.
    pub give_up_after: Duration,
    /// How often the supervisor checks the current producer for a fatal error, when no
    /// submission has woken it up before. `500ms` by default.
    pub poll_interval: Duration,
    /// A recreation older than this is considered stable: a new fatal error then restarts the
    /// back-off from level `0`. A fatal error within this delay raises the back-off level
    /// instead. `60s` by default.
    pub stability_threshold: Duration,
    /// First non-zero back-off delay. `1s` by default.
    pub backoff_base: Duration,
    /// Upper bound of the back-off delay. `60s` by default.
    pub backoff_max: Duration,
}

impl RecoveryPolicy {
    /// Creates a policy that gives up after `give_up_after`, with the default timings for
    /// everything else.
    pub fn new(give_up_after: Duration) -> Self {
        RecoveryPolicy {
            give_up_after,
            poll_interval: Duration::from_millis(500),
            stability_threshold: Duration::from_secs(60),
            backoff_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
        }
    }

    /// Back-off delay for a given level, see the [type documentation](RecoveryPolicy).
    ///
    /// The exponent is capped, and the multiplication saturates, so no level can overflow.
    fn backoff_delay(&self, level: u32) -> Duration {
        if level == 0 {
            Duration::ZERO
        } else {
            let exp = 1u32 << (level.saturating_sub(1)).min(16);
            self.backoff_base.saturating_mul(exp).min(self.backoff_max)
        }
    }
}
