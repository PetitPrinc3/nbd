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

static REAPER: OnceLock<Sender<ReaperTargets>> = OnceLock::new();

enum ReaperTargets {
    Target(FutureProducer),
    Completion(Sender<()>),
}

fn reaper() -> &'static Sender<ReaperTargets> {
    REAPER.get_or_init(|| {
        let (tx, rx) = channel::<ReaperTargets>();
        match std::thread::Builder::new().name(String::from("nbd-kafka-reaper")).spawn(move || {
            for item in rx {
                match item {
                    ReaperTargets::Target(target) => {
                        let start = Instant::now();
                        drop(target);
                        debug!("Dropped producer in {}ms;", start.elapsed().as_millis())
                    }
                    ReaperTargets::Completion(sender) => {
                        let _ = sender.send(());
                    }
                }
            }
        }) {
            Ok(_) => {},
            Err(e) => {
                warn!("Failed to create the cleaner thread: {}. Expect increased latency and data loss in the event of a fatal failure from the Kafka broker", e);
            }
        }
        tx
    })
}

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

pub struct ProducerCell {
    producer: Option<FutureProducer>,
    generation: u64,
    faulted: AtomicBool,
}

impl ProducerCell {
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

impl Drop for ProducerCell {
    fn drop(&mut self) {
        if let Some(producer) = self.producer.take() {
            retire(producer);
        }
    }
}

fn retire(producer: FutureProducer) {
    let _ = reaper().send(ReaperTargets::Target(producer));
}

pub struct ProducerContainer {
    cell: ArcSwap<ProducerCell>,
    notifier: Notify,
    errors: AtomicU64,
}

#[derive(Clone)]
pub struct ManagedProducer {
    container: Arc<ProducerContainer>,
}

impl ManagedProducer {
    pub fn new(producer: FutureProducer) -> Self {
        reaper();

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

    pub fn generation(&self) -> u64 {
        self.container.cell.load().generation
    }

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

                if last_generation.is_none_or(|t| t.elapsed() > policy.stability_threshold) {
                    stability_indicator = 0;
                } else {
                    stability_indicator = stability_indicator.saturating_add(1);
                    let delay = policy.get_delay(stability_indicator);

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
                        Err(e) => return Err(NbdError::TaskPanic(e)),
                    };

                    let since = current_incident.get_or_insert_with(Instant::now);

                    if since.elapsed() >= policy.give_up_after {
                        error!(
                            "Failed to recreate a connection with the Kafka broker. Giving up after {:?}. (policy)",
                            policy.give_up_after
                        );

                        return Err(NbdError::Recovery(format!(
                            "Failed to recreate a connection with the Kafka broker : {e}. Giving up after {:?}. (policy)",
                            policy.give_up_after
                        )));
                    }

                    generation_errors = generation_errors.saturating_add(1);

                    error!(
                        "An error occured while recreating a connection with the Kafka broker (attempt {}) : {}",
                        generation_errors, e
                    );
                    warn!(
                        "Next attempt at recreating a connection with the broker in {:?}",
                        policy.get_delay(stability_indicator.saturating_add(generation_errors))
                    );

                    tokio::select! {
                        _= token.cancelled() => {
                            warn!("Failed to recreate a connection with the Kafka broker (cancelled).");
                            return Ok(());
                        },
                        _ = sleep(policy.get_delay(stability_indicator.saturating_add(generation_errors))) => {}
                        _ = sleep_until((*since + policy.give_up_after).into()) => {}
                    }
                };

                // Swap broken producer with new one
                self.container.cell.swap(Arc::new(ProducerCell {
                    producer: Some(new_producer),
                    generation: cell.generation + 1,
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

                // Store generation instant
                last_generation = Some(Instant::now());
            }
        }
    }
}

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

pub type ProducerFactory = Arc<dyn Fn() -> Result<FutureProducer, NbdError> + Send + Sync>;

pub fn producer_factory(config_path: PathBuf) -> ProducerFactory {
    Arc::new(move || {
        let raw_config = match std::fs::read_to_string(&config_path) {
            Ok(content) => {
                let raw_config = match toml::from_str::<RawConfig>(&content) {
                    Ok(config) => config,
                    Err(e) => {
                        let span = e.span();

                        match span {
                            Some(range) => {
                                let error = content[0..range.start].split("\n");

                                let line = error.clone().count();
                                let col = match error.last() {
                                    Some(line) => line.len(),
                                    None => 0,
                                };

                                error!(
                                    "The configuration file under {} is no longer valid (line {}, column {}).",
                                    config_path.display(),
                                    line,
                                    col
                                );
                            }
                            None => {
                                error!(
                                    "The configuration file under {} is no longer valid.",
                                    config_path.display()
                                );
                            }
                        }

                        error!(
                            "Run `nbd --check-config-file {}` to see the details.",
                            config_path.display()
                        );

                        return Err(NbdError::Config(format!(
                            "The configuration file under {} is no longer valid.",
                            config_path.display()
                        )));
                    }
                };

                raw_config
            }
            Err(e) => {
                return Err(NbdError::Config(format!(
                    "An error occured while parsing the configuration file : {}",
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

#[derive(Debug, Clone, Copy)]
pub struct RecoveryPolicy {
    pub give_up_after: Duration,
    pub poll_interval: Duration,
    pub stability_threshold: Duration,
    pub backoff_base: Duration,
    pub backoff_max: Duration,
}

impl RecoveryPolicy {
    pub fn new(give_up_after: Duration) -> Self {
        RecoveryPolicy {
            give_up_after,
            poll_interval: Duration::from_millis(500),
            stability_threshold: Duration::from_secs(60),
            backoff_base: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
        }
    }

    fn get_delay(&self, level: u32) -> Duration {
        if level == 0 {
            Duration::ZERO
        } else {
            let exp = 1u32 << (level - 1).min(16);
            self.backoff_base.saturating_mul(exp).min(self.backoff_max)
        }
    }
}
