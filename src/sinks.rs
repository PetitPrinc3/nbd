//! Message delivery abstractions and implementations.
//!
//! This module defines the [`MessageSink`] trait, which decouples the UDP multicast
//! receiver ([`Provider`](crate::Provider)) from the message destination.
//! This enables zero-cost in-memory benchmarking without a Kafka broker.
//!
//! ## Implementations
//!
//! - [`FutureProducer`](rdkafka::producer::FutureProducer) — Production sink delivering messages to Kafka.
//! - [`BenchSink`] — In-memory sink for internal latency benchmarking with nanosecond timestamps.
//! - [`IPerfSink`] — Mock sink parsing standard iperf UDP packets for packet loss and latency analysis.

use bytes::Bytes;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use tracing::{debug, warn};

use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;

use crate::errors::NbdError;

const UNIX_EPOCH: SystemTime = SystemTime::UNIX_EPOCH;

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

/// Production [`MessageSink`] implementation that delivers messages to a Kafka broker
/// via `rdkafka::FutureProducer`.
///
/// Submits messages non-blockingly using `send_result`. If the librdkafka producer queue is full,
/// immediately returns [`NbdError::Backpressure`]. When the `metrics-exporter` feature is enabled,
/// end-to-end latency is computed in the transaction future by extracting the transmit timestamp
/// from payload bytes `[4..12]` (iperf-compatible header: `u32` seconds + `u32` microseconds)
/// and recording the delta in the `nbd_e2e_latency` Prometheus histogram.
impl MessageSink for FutureProducer {
    type Transaction =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), NbdError>> + Send>>;

    fn submit(&self, topic: Arc<str>, payload: Bytes) -> Result<Self::Transaction, NbdError> {
        let record: FutureRecord<[u8], [u8]> = FutureRecord::to(&topic).payload(&payload);
        match self.send_result(record) {
            Ok(delivery_future) => {
                Ok(Box::pin(async move {
                    match delivery_future.await {
                        Ok(Ok(_offset_info)) => {
                            // This section is only used for testing by computing latencies for the end-to-end test.
                            //#[cfg(feature = "metrics-exporter")]
                            //{
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
                            //    metrics::counter!("nbd_kafka_sent_total", "topic" => topic.clone())
                            //        .increment(1);
                            //}

                            debug!("Successfully sent some message on topic {} !", &topic);

                            Ok(())
                        }
                        Ok(Err((e, _msg))) => {
                            #[cfg(feature = "metrics-exporter")]
                            metrics::counter!("nbd_errors_kafka_total", "topic" => topic.clone())
                                .increment(1);

                            warn!("Failed to send one message on topic {} : {}", &topic, e);
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
        let ts_tx_ns = u64::from_be_bytes(payload[..8].try_into().unwrap());
        let ts_tx = self.epoch + Duration::from_nanos(ts_tx_ns);

        self.received_messages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.received_bytes
            .fetch_add(payload.len(), std::sync::atomic::Ordering::Relaxed);
        self.latencies
            .lock()
            .unwrap()
            .push(ts_rx.duration_since(ts_tx));

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

        let pkt_idx = u32::from_be_bytes(payload[..4].try_into().unwrap());

        let ts_tx = u32::from_be_bytes(payload[4..8].try_into().unwrap()) as u64;
        let ts_tx_micros = (ts_tx * 1_000_000
            + (u32::from_be_bytes(payload[8..12].try_into().unwrap()) as u64))
            as u128;

        let ts_rx_micros = ts_rx.duration_since(UNIX_EPOCH).unwrap().as_micros();

        if let Ok(mut state) = self.state.lock() {
            state
                .latencies
                .push(Duration::from_micros((ts_rx_micros - ts_tx_micros) as u64));
            state.indexes.push(pkt_idx);
        }

        self.received_messages
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.received_bytes
            .fetch_add(payload.len(), std::sync::atomic::Ordering::Relaxed);

        Ok(std::future::ready(Ok(())))
    }
}
