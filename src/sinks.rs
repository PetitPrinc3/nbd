use bytes::Bytes;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;

use crate::errors::NbdError;

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
            .map(|_| ())
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
