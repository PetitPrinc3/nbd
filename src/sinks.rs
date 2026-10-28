//! Message delivery abstractions and implementations.
//!
//! This module defines the [`MessageSink`] trait, which decouples the UDP multicast
//! receiver ([`Provider`](crate::Provider)) from the message destination.
//! This enables zero-cost in-memory benchmarking without a Kafka broker.
//!
//! ## Implementations
//!
//! - [`ManagedProducer`](crate::ManagedProducer) — Production sink delivering messages to Kafka.
//!   It wraps a [`FutureProducer`] and replaces it when the producer raises a fatal error.
//! - [`FutureProducer`] — The plain Kafka sink that [`ManagedProducer`](crate::ManagedProducer)
//!   builds on. It does not recover from a fatal error.
//! - [`BenchSink`] — In-memory sink for internal latency benchmarking with nanosecond timestamps.
//! - [`IPerfSink`] — Mock sink parsing standard iperf UDP packets for packet loss and latency analysis.

use bytes::Bytes;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use tracing::{debug, warn};

use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;

use arc_swap::ArcSwap;

use crate::errors::NbdError;

const UNIX_EPOCH: SystemTime = SystemTime::UNIX_EPOCH;

/// Time reference of the delivery failure log throttling, set when the first failure happens.
static LOG_CLOCK: OnceLock<Instant> = OnceLock::new();
/// Throttle state of the delivery failure logs: for each topic, the second (counted from
/// [`LOG_CLOCK`]) at which a failure was last logged.
///
/// Copy-on-write map behind an [`ArcSwap`], so that the usual path is a lock-free read. It holds
/// one entry per topic, so its size is bounded by the topics of the configured providers.
static LOG_LOCKS: LazyLock<ArcSwap<HashMap<Arc<str>, Arc<AtomicU64>>>> =
    LazyLock::new(|| ArcSwap::from_pointee(HashMap::new()));
/// Serializes the insertion of a new topic in [`LOG_LOCKS`].
static LOG_WRITER: Mutex<()> = Mutex::new(());
/// Minimum delay, in seconds, between two logged delivery failures for the same topic.
const LOG_THRESHOLD: u64 = 1;

/// Trait abstracting the non-blocking submission and asynchronous delivery confirmation of a message.
///
/// Implementations must be `Clone + Send + 'static` so they can be shared across
/// concurrent async tasks in the [`Provider`](crate::Provider) receive loop.
///
/// ## Submission model
///
/// - [`submit`](MessageSink::submit) enqueues or processes the payload non-blockingly,
///   returning a [`Transaction`](MessageSink::Transaction) future representing the asynchronous
///   delivery confirmation, or returns [`NbdError::Backpressure`] if the sink queue is full.
/// - Awaiting the returned [`Transaction`](MessageSink::Transaction) future confirms delivery
///   or yields an error.
///
/// # Arguments
///
/// * `topic` — The Kafka topic (or logical channel) to deliver the message to.
/// * `payload` — The raw message bytes, produced via zero-copy `BytesMut::split_to().freeze()`.
pub trait MessageSink: Clone + Send + 'static {
    type Transaction: std::future::Future<Output = Result<(), NbdError>> + Send + 'static;

    fn submit(&self, topic: Arc<str>, payload: Bytes) -> Result<Self::Transaction, NbdError>;
}

/// Delivery confirmation future returned for a Kafka message.
pub(crate) type KafkaTransaction =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), NbdError>> + Send>>;

/// Plain Kafka [`MessageSink`] delivering messages to a broker through
/// [`FutureProducer`].
///
/// Submits messages non-blockingly using `send_result`. If the librdkafka producer queue is full,
/// immediately returns [`NbdError::Backpressure`]; any other error raised while enqueuing, such as
/// a fatal producer error, is returned as [`NbdError::Kafka`].
///
/// The returned [`Transaction`](MessageSink::Transaction) future resolves when the broker has
/// answered:
///
/// - on success, it yields `Ok(())` and, with the `metrics-exporter` feature, increments
///   `nbd_kafka_sent_total` for the topic;
/// - on a delivery failure, it yields [`NbdError::Kafka`] and, with the `metrics-exporter`
///   feature, increments `nbd_errors_kafka_transmission_total` for the topic. The matching
///   warning is logged at most once per second and per topic, so that a broker outage cannot
///   flood the logs: without the metrics exporter, the occurrences that are not logged are not
///   counted anywhere;
/// - if the delivery is canceled before any confirmation arrives (typically because the producer
///   was destroyed), it yields [`NbdError::Backpressure`] and logs a warning each time.
///
/// This sink does not recover from a fatal producer error. The daemon uses
/// [`ManagedProducer`](crate::ManagedProducer) instead, which builds on it.
impl MessageSink for FutureProducer {
    type Transaction =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), NbdError>> + Send>>;

    fn submit(&self, topic: Arc<str>, payload: Bytes) -> Result<Self::Transaction, NbdError> {
        submit_on(self, topic, payload)
    }
}

/// Enqueues a message on `producer` and returns the future that confirms its delivery.
///
/// Shared implementation of [`MessageSink::submit`] for the plain [`FutureProducer`] sink and for
/// [`ManagedProducer`](crate::ManagedProducer). The errors and the semantics of the returned
/// future are those documented on the `MessageSink` implementation of [`FutureProducer`].
///
/// The delivery failure warning is throttled to one per topic every [`LOG_THRESHOLD`] seconds,
/// see [`LOG_LOCKS`].
pub(crate) fn submit_on(
    producer: &FutureProducer,
    topic: Arc<str>,
    payload: Bytes,
) -> Result<KafkaTransaction, NbdError> {
    let record: FutureRecord<[u8], [u8]> = FutureRecord::to(&topic).payload(&payload);
    match producer.send_result(record) {
        Ok(delivery_future) => {
            Ok(Box::pin(async move {
                match delivery_future.await {
                    Ok(Ok(_offset_info)) => {
                        #[cfg(feature = "metrics-exporter")]
                        {
                            // This section is only used for testing by computing latencies for the end-to-end test.
                            //    if payload.len() > 12 {
                            //        let ts_rx = SystemTime::now();

                            //        //let pkt_idx = u32::from_be_bytes(payload[..4].try_into().unwrap());

                            //        let ts_tx =
                            //            u32::from_be_bytes(payload[4..8].try_into().unwrap())
                            //                as u64;
                            //        let ts_tx_micros = (ts_tx * 1_000_000
                            //            + (u32::from_be_bytes(payload[8..12].try_into().unwrap())
                            //                as u64))
                            //            as u128;
                            //        let ts_rx_micros =
                            //            ts_rx.duration_since(UNIX_EPOCH).unwrap().as_micros();
                            //        let latency =
                            //            Duration::from_micros((ts_rx_micros - ts_tx_micros) as u64);

                            //        metrics::histogram!("nbd_e2e_latency").record(latency);
                            //    };
                            metrics::counter!("nbd_kafka_sent_total", "topic" => topic.clone())
                                .increment(1);
                        }

                        debug!("Successfully sent some message on topic {} !", &topic);

                        Ok(())
                    }
                    Ok(Err((e, _msg))) => {
                        #[cfg(feature = "metrics-exporter")]
                        metrics::counter!("nbd_errors_kafka_transmission_total", "topic" => topic.clone())
                            .increment(1);

                        let elapsed: u64 = LOG_CLOCK.get_or_init(Instant::now).elapsed().as_secs();
                        let log_locks = LOG_LOCKS.load();

                        match log_locks.get(&topic) {
                            Some(seconds) => {
                                let resolved_seconds = seconds.load(Ordering::Relaxed);
                                if elapsed.saturating_sub(resolved_seconds) >= LOG_THRESHOLD
                                    && seconds
                                        .compare_exchange(
                                            resolved_seconds,
                                            elapsed,
                                            Ordering::Relaxed,
                                            Ordering::Relaxed,
                                        )
                                        .is_ok()
                                {
                                    warn!("Failed to send one message on topic {} : {}", &topic, e);
                                }
                            }
                            None => {
                                let is_new = {
                                    let _writer =
                                        LOG_WRITER.lock().unwrap_or_else(|e| e.into_inner());
                                    let current = LOG_LOCKS.load();
                                    if current.contains_key(&topic) {
                                        false
                                    } else {
                                        let mut copy = HashMap::clone(&current);
                                        copy.insert(
                                            topic.clone(),
                                            Arc::new(AtomicU64::new(elapsed)),
                                        );
                                        LOG_LOCKS.store(Arc::new(copy));
                                        true
                                    }
                                };

                                if is_new {
                                    warn!("Failed to send one message on topic {} : {}", &topic, e);
                                }
                            }
                        }

                        Err(NbdError::Kafka(e))
                    }
                    Err(_canceled) => {
                        warn!(
                            "Producer canceled before send confirmation for one message on topic {}.",
                            &topic
                        );

                        Err(NbdError::Backpressure(format!(
                            "Failed to send one message on topic {} because the producer was cancelled.",
                            topic
                        )))
                    }
                }
            }))
        }
        Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _record)) => {
            Err(NbdError::Backpressure(format!(
                "Failed to send one message on topic {} because the queue is full.",
                topic
            )))
        }
        Err((e, _record)) => Err(NbdError::Kafka(e)),
    }
}

/// In-memory [`MessageSink`] for benchmarking the internal pipeline latency.
///
/// Expects payloads with an 8-byte big-endian nanosecond timestamp in the first bytes,
/// representing the elapsed time since [`epoch`](BenchSink::epoch).
///
/// Uses lock-free atomic counters (`Relaxed` ordering) on the hot path for message/byte
/// counting, and a `Mutex<Vec<Duration>>` for latency recording.
#[derive(Clone)]
pub struct BenchSink {
    pub epoch: Instant,
    pub received_messages: Arc<std::sync::atomic::AtomicU64>,
    pub received_bytes: Arc<std::sync::atomic::AtomicUsize>,
    pub latencies: Arc<Mutex<Vec<Duration>>>,
}

impl Default for BenchSink {
    fn default() -> Self {
        Self {
            epoch: Instant::now(),
            received_messages: Arc::<std::sync::atomic::AtomicU64>::default(),
            received_bytes: Arc::<std::sync::atomic::AtomicUsize>::default(),
            latencies: Arc::from(Mutex::new(Vec::with_capacity(5_000_000))),
        }
    }
}

impl MessageSink for BenchSink {
    type Transaction = std::future::Ready<Result<(), NbdError>>;

    fn submit(&self, _topic: Arc<str>, payload: Bytes) -> Result<Self::Transaction, NbdError> {
        let ts_rx = Instant::now();

        if payload.len() < 8 {
            return Err(NbdError::InvalidPacket(format!(
                "packet len < 8 ({})",
                payload.len()
            )));
        }

        let ts_tx_ns = match payload.get(..8) {
            Some(payload) => match <[u8; 8]>::try_from(payload) {
                Ok(vec) => u64::from_be_bytes(vec),
                Err(_) => {
                    return Err(NbdError::InvalidPacket(String::from(
                        "Failed to convert first 8 bytes of packet to u64",
                    )));
                }
            },
            None => {
                return Err(NbdError::InvalidPacket(format!(
                    "Failed to get first 8 bytes of packet with len : {}",
                    payload.len()
                )));
            }
        };

        if let Some(ts) = self.epoch.checked_add(Duration::from_nanos(ts_tx_ns))
            && let Ok(mut latencies) = self.latencies.lock()
        {
            match ts_rx.checked_duration_since(ts) {
                Some(latency) => {
                    latencies.push(latency);
                }
                None => {
                    return Err(NbdError::InvalidPacket(String::from(
                        "Invalid timestamp in packet.",
                    )));
                }
            }
        } else {
            return Err(NbdError::InvalidPacket(String::from(
                "Failed to determine latency for one packet.",
            )));
        }

        self.received_messages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.received_bytes
            .fetch_add(payload.len(), std::sync::atomic::Ordering::Relaxed);

        Ok(std::future::ready(Ok(())))
    }
}

/// [`MessageSink`] for benchmarking with external [iperf](https://iperf.fr/) UDP streams.
///
/// Parses the standard iperf UDP packet header (12 bytes):
/// - Bytes `[0..4]`: `u32` packet sequence number (big-endian)
/// - Bytes `[4..8]`: `u32` transmit timestamp seconds (big-endian)
/// - Bytes `[8..12]`: `u32` transmit timestamp microseconds (big-endian)
///
/// Tracks sequence numbers for out-of-order and packet loss detection.
/// Rejects packets shorter than 12 bytes with [`NbdError::InvalidPacket`].
#[derive(Clone)]
pub struct IPerfSink {
    pub state: Arc<Mutex<IPerfSinkState>>,
    pub received_messages: Arc<std::sync::atomic::AtomicU64>,
    pub received_bytes: Arc<std::sync::atomic::AtomicUsize>,
}

#[derive(Clone)]
pub struct IPerfSinkState {
    pub latencies: Vec<Duration>,
    pub indexes: Vec<u32>,
}

impl Default for IPerfSink {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(IPerfSinkState {
                latencies: Vec::with_capacity(5_000_000),
                indexes: Vec::with_capacity(5_000_000),
            })),
            received_messages: Arc::<std::sync::atomic::AtomicU64>::default(),
            received_bytes: Arc::<std::sync::atomic::AtomicUsize>::default(),
        }
    }
}

impl MessageSink for IPerfSink {
    type Transaction = std::future::Ready<Result<(), NbdError>>;

    fn submit(&self, _topic: Arc<str>, payload: Bytes) -> Result<Self::Transaction, NbdError> {
        let ts_rx = SystemTime::now();

        if payload.len() < 12 {
            return Err(NbdError::InvalidPacket(format!(
                "packet len : {}",
                payload.len()
            )));
        }

        let pkt_idx = match payload.get(..4) {
            Some(payload) => match <[u8; 4]>::try_from(payload) {
                Ok(vec) => u32::from_be_bytes(vec),
                Err(_) => {
                    return Err(NbdError::InvalidPacket(String::from(
                        "Failed to convert first 4 bytes of packet to u32",
                    )));
                }
            },
            None => {
                return Err(NbdError::InvalidPacket(format!(
                    "Failed to get first 4 bytes of packet with len : {}",
                    payload.len()
                )));
            }
        };

        let ts_tx = match payload.get(4..8) {
            Some(payload) => u64::from(match <[u8; 4]>::try_from(payload) {
                Ok(vec) => u32::from_be_bytes(vec),
                Err(_) => {
                    return Err(NbdError::InvalidPacket(String::from(
                        "Failed to convert bytes 4 to 8 of packet to u32",
                    )));
                }
            }),
            None => {
                return Err(NbdError::InvalidPacket(format!(
                    "Failed to get bytes 4 to 8 of packet with len : {}",
                    payload.len()
                )));
            }
        };

        let ts_tx_micros = u128::from(ts_tx.saturating_mul(1_000_000).saturating_add(
            match payload.get(8..12) {
                Some(payload) => u64::from(match <[u8; 4]>::try_from(payload) {
                    Ok(vec) => u32::from_be_bytes(vec),
                    Err(_) => {
                        return Err(NbdError::InvalidPacket(String::from(
                            "Failed to convert bytes 8 to 12 of packet to u32",
                        )));
                    }
                }),
                None => {
                    return Err(NbdError::InvalidPacket(format!(
                        "Failed to get bytes 8 to 12 of packet with len : {}",
                        payload.len()
                    )));
                }
            },
        ));

        if let Ok(ts_rx_micros) = ts_rx.duration_since(UNIX_EPOCH)
            && let Ok(mut state) = self.state.lock()
        {
            if let Ok(latency) =
                u64::try_from(match ts_rx_micros.as_micros().checked_sub(ts_tx_micros) {
                    Some(duration) => duration,
                    None => {
                        return Err(NbdError::InvalidPacket(String::from(
                            "Failed to compute latency based on packet data.",
                        )));
                    }
                })
            {
                state.latencies.push(Duration::from_micros(latency));
            }
            state.indexes.push(pkt_idx);
        } else {
            return Err(NbdError::InvalidPacket(String::from(
                "Failed to determine latency for one packet.",
            )));
        }

        self.received_messages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.received_bytes
            .fetch_add(payload.len(), std::sync::atomic::Ordering::Relaxed);

        Ok(std::future::ready(Ok(())))
    }
}
