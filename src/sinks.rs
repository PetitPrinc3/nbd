use bytes::Bytes;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;

use crate::errors::NbdError;

const UNIX_EPOCH: SystemTime = SystemTime::UNIX_EPOCH;

pub trait MessageSink: Clone + Send + 'static {
    fn send(
        &self,
        topic: Arc<str>,
        payload: Bytes,
    ) -> impl std::future::Future<Output = Result<(), NbdError>> + Send;
}

/// This is the MessageSink used to send messages to the Kafka broker.
impl MessageSink for FutureProducer {
    async fn send(&self, topic: Arc<str>, payload: Bytes) -> Result<(), NbdError> {
        let record: FutureRecord<[u8], [u8]> = FutureRecord::to(&topic).payload(&payload);
        FutureProducer::send(self, record, Timeout::After(Duration::from_millis(100)))
            .await
            .map(|_| {
                #[cfg(feature = "metrics-exporter")]
                {
                    if payload.len() > 12 {
                        let ts_rx = SystemTime::now();

                        //let pkt_idx = u32::from_be_bytes(payload[..4].try_into().unwrap());

                        let ts_tx = u32::from_be_bytes(payload[4..8].try_into().unwrap()) as u64;
                        let ts_tx_micros = (ts_tx * 1_000_000
                            + (u32::from_be_bytes(payload[8..12].try_into().unwrap()) as u64))
                            as u128;
                        let ts_rx_micros = ts_rx.duration_since(UNIX_EPOCH).unwrap().as_micros();
                        let latency = Duration::from_micros((ts_rx_micros - ts_tx_micros) as u64);

                        metrics::histogram!("nbd_e2e_latency").record(latency);
                    };
                }
            })
            .map_err(|(e, _)| NbdError::Kafka(e))
    }
}

/// This is the MessageSink used for bencharking the software's internal latencies.
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
    async fn send(&self, _topic: Arc<str>, payload: Bytes) -> Result<(), NbdError> {
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

        Ok(())
    }
}

/// This is the MessageSink used to benchmark the software's latency using iperf
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
    async fn send(&self, _topic: Arc<str>, payload: Bytes) -> Result<(), NbdError> {
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

        Ok(())
    }
}
