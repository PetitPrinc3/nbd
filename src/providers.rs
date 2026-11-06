//! Core UDP multicast ingestion engine.
//!
//! This module configures OS-level multicast sockets (IGMP/MLD) using [`socket2`],
//! binds to specified network interfaces, and runs an asynchronous receive loop,
//! without any payload copy in NBD, that streams incoming UDP packets to any
//! [`MessageSink`] implementation.
//!
//! ## Zero-copy buffer strategy
//!
//! The receive loop uses [`bytes::BytesMut`] as its buffer. When a datagram arrives,
//! the consumed bytes are split off via `split_to(len).freeze()`, producing a
//! reference-counted, immutable [`Bytes`](bytes::Bytes) handle — **no payload memory
//! is copied**. The buffer is re-allocated only when remaining capacity drops below
//! the largest UDP payload ([`MAX_UDP_PAYLOAD`]).
//!
//! This covers NBD's own path. The kernel still copies the datagram into the buffer, and
//! the Kafka sinks hand it to `librdkafka`, which copies it into its queue.
//!
//! ## Concurrency & Backpressure
//!
//! Incoming UDP datagrams are immediately submitted to the [`MessageSink`] via
//! [`submit`](MessageSink::submit). If the sink's internal transmission queue is full,
//! [`NbdError::Backpressure`] is returned and tracked via metrics without blocking
//! the receive loop. In-flight delivery confirmations are tracked in a [`FuturesUnordered`]
//! collection and drained in batches using `now_or_never()`.
//!
//! ## Graceful shutdown
//!
//! On [`CancellationToken`](tokio_util::sync::CancellationToken) cancellation, the
//! listener stops reading and only exits once the deliveries already submitted to the sink have
//! completed or failed. Datagrams that are still waiting in the socket buffer of the operating
//! system at that moment are not read.

#[cfg(not(target_os = "linux"))]
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
#[cfg(target_os = "linux")]
use std::net::{IpAddr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use std::time::{Duration, Instant};

use socket2::{Domain, InterfaceIndexOrAddress, Protocol, SockAddr, Socket, Type};

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};

use tokio::net::UdpSocket;

use bytes::BytesMut;

use tracing::{debug, error, info, warn};

use crate::config::{
    BUFFER_RESERVATION_HEADROOM, Interface, LOG_THRESHOLD, MAX_UDP_PAYLOAD, ProviderConfig,
};
use crate::errors::NbdError;
use crate::sinks::MessageSink;

/// A UDP multicast listener bound to a specific group and network interface.
///
/// ## Lifecycle
///
/// 1. Create from configuration: `Provider::from(&ProviderConfig)`
/// 2. Subscribe to multicast group: [`subscribe()`](Provider::subscribe) — creates the socket
///    ([`create_socket()`](Provider::create_socket)) and joins the multicast group
///    ([`join()`](Provider::join)). The daemon calls these two separately, so that it only joins
///    the groups once the Kafka broker has answered.
/// 3. Start receiving: [`start_listener()`](Provider::start_listener) — runs the async receive
///    loop, dispatching payloads to the given [`MessageSink`].
pub struct Provider {
    /// The multicast group this provider listens to.
    pub group: IpAddr,
    /// `<group>:<port>`, the label of the metrics of this provider (feature `metrics-exporter`).
    pub group_label: Arc<str>,
    topic: Arc<str>,
    port: u16,
    interface: Interface,
    socket: Option<Socket>,
}

impl From<&ProviderConfig> for Provider {
    /// Generate a provider from configuration data
    fn from(config: &ProviderConfig) -> Provider {
        Provider {
            group: config.group,
            group_label: Arc::<str>::from(format!("{}:{}", config.group, config.port)),
            topic: Arc::from(config.topic.as_str()),
            port: config.port,
            interface: config.interface.clone(),
            socket: None,
        }
    }
}

impl Provider {
    /// Creates the listening socket, without joining the group.
    ///
    /// Sets the size of the receive buffer to `max_buffer_size` bytes, and enables address reuse
    /// and non-blocking mode. On Linux, it disables `IP_MULTICAST_ALL`, binds the socket to the
    /// configured interface (`SO_BINDTODEVICE`) and to the group address, so that the socket only
    /// receives this group on this interface. On other systems, it binds to the wildcard address.
    ///
    /// # Errors
    ///
    /// Returns [`NbdError::Config`] when the kernel grants a smaller receive buffer than requested
    /// (on Linux, `net.core.rmem_max` is the limit), [`NbdError::Socket`] when the binding fails or
    /// the interface cannot be found, and [`NbdError::Network`] when the socket cannot be created
    /// or configured.
    pub fn create_socket(&mut self, max_buffer_size: &usize) -> Result<(), NbdError> {
        let domain = if self.group.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };

        let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

        socket.set_recv_buffer_size(*max_buffer_size)?;

        let received = socket.recv_buffer_size()?;

        #[cfg(target_os = "linux")]
        let granted = received / 2;
        #[cfg(not(target_os = "linux"))]
        let granted = received;

        if granted < *max_buffer_size {
            error!(
                "The kernel granted a smaller buffer size than requested : {} < {}. Raise `net.core.rmem_max` accordingly (see PRODUCTION.md).",
                granted, max_buffer_size
            );
            return Err(NbdError::Config(format!(
                "Received a smaller buffer than requested ({} < {})",
                granted, max_buffer_size
            )));
        }

        socket.set_reuse_address(true)?;

        // #[cfg(target_os = "linux")]
        // socket.set_reuse_port(true)?;

        socket.set_nonblocking(true)?;

        if self.group.is_ipv6() {
            socket.set_only_v6(true)?;
        }

        #[cfg(target_os = "linux")]
        {
            if self.group.is_ipv4() {
                socket.set_multicast_all_v4(false)?;
            } else {
                socket.set_multicast_all_v6(false)?;
            }

            socket.bind_device(Some(self.interface.as_str().as_bytes()))?;
        }

        #[cfg(target_os = "linux")]
        let bind_addr = match self.group {
            IpAddr::V4(maddr_v4) => SocketAddr::V4(SocketAddrV4::new(maddr_v4, self.port)),
            IpAddr::V6(maddr_v6) => SocketAddr::V6(SocketAddrV6::new(
                maddr_v6,
                self.port,
                0,
                self.interface.index()?,
            )),
        };

        #[cfg(not(target_os = "linux"))]
        let bind_addr = match self.group {
            IpAddr::V4(_) => SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, self.port)),
            IpAddr::V6(_) => {
                SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, self.port, 0, 0))
            }
        };

        socket.bind(&SockAddr::from(bind_addr)).map_err(|e| {
            NbdError::Socket(format!("Socket binding to {} failed : {}", bind_addr, e))
        })?;

        self.socket = Some(socket);
        Ok(())
    }

    /// Joins the multicast group on the configured interface, with an IGMP join for an IPv4 group
    /// and an MLD join for an IPv6 group.
    ///
    /// # Errors
    ///
    /// Returns [`NbdError::Socket`] when the socket doesn't exist or the interface cannot be found,
    /// and [`NbdError::Network`] when the group cannot be joined.
    pub fn join(&self) -> Result<(), NbdError> {
        let socket = self.socket()?;
        let idx = self.interface.index()?;

        match self.group {
            IpAddr::V4(maddr_v4) => {
                socket.join_multicast_v4_n(&maddr_v4, &InterfaceIndexOrAddress::Index(idx))?;
            }
            IpAddr::V6(maddr_v6) => {
                socket.join_multicast_v6(&maddr_v6, idx)?;
            }
        }

        info!(
            "Successfully subscribed to {} on {} !",
            &self.group,
            self.interface.as_str()
        );

        Ok(())
    }

    /// Returns the listening socket.
    ///
    /// # Errors
    ///
    /// Returns [`NbdError::Socket`] when the socket doesn't exist: before
    /// [`create_socket`](Provider::create_socket), and once
    /// [`start_listener`](Provider::start_listener) has taken it.
    pub fn socket(&self) -> Result<&Socket, NbdError> {
        self.socket
            .as_ref()
            .ok_or_else(|| NbdError::Socket(String::from("The socket doesn't exist yet.")))
    }

    /// Subscribes to the provider's multicast group: [`create_socket`](Provider::create_socket),
    /// then [`join`](Provider::join).
    ///
    /// # Errors
    ///
    /// Returns the errors of these two functions.
    pub fn subscribe(&mut self, max_buffer_size: &usize) -> Result<(), NbdError> {
        self.create_socket(max_buffer_size)?;
        self.join()
    }

    /// Start the asynchronous receive loop, dispatching UDP datagrams to the given sink.
    ///
    /// The loop uses `tokio::select!` with three branches:
    /// - **Cancellation**: exits only when `in_flight` is empty (graceful drain).
    /// - **Receive**: reads into `BytesMut`, splits via zero-copy `freeze()`, and submits
    ///   non-blockingly via [`sink.submit()`](MessageSink::submit). On success, the delivery
    ///   confirmation future is pushed into `in_flight`. If the queue is full ([`NbdError::Backpressure`]),
    ///   it records backpressure telemetry without blocking packet reception. Empty datagrams are
    ///   counted and not submitted.
    /// - **Completion**: polls and drains completed delivery confirmations from `in_flight`
    ///   in batches via `now_or_never()`.
    ///
    /// # Errors
    ///
    /// Returns [`NbdError::Socket`] if the socket has not been created (call
    /// [`subscribe`](Provider::subscribe) first), and [`NbdError::Network`] if it cannot be handed
    /// over to the Tokio runtime. An error while receiving a datagram is not fatal: it is counted,
    /// logged at most once per second, and the loop goes on.
    pub async fn start_listener<S: MessageSink>(
        &mut self,
        sink: S,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> Result<(), NbdError> {
        let udp_socket: std::net::UdpSocket = match self.socket.take() {
            Some(socket) => Ok(socket.into()),
            None => Err(NbdError::Socket(String::from(
                "Failed to convert socket to UDP socket because socket does not exist.",
            ))),
        }?;

        let listener = UdpSocket::from_std(udp_socket)?;

        let mut last_error_log = Instant::now();

        let mut buf =
            BytesMut::with_capacity(MAX_UDP_PAYLOAD.saturating_mul(BUFFER_RESERVATION_HEADROOM));

        let mut in_flight = FuturesUnordered::new();

        loop {
            tokio::select! {
                    _ = cancel_token.cancelled(), if in_flight.is_empty() => {
                        warn!("Listener on {} was cancelled.", self.group);
                        break Ok(());
                    }
                    stat = listener.recv_buf(&mut buf), if !cancel_token.is_cancelled() => {
                        match stat {
                            Ok(len) => {
                                if len == 0 {
                                    debug!("Received an empty packet on {}.", self.group);

                                    #[cfg(feature = "metrics-exporter")]
                                    metrics::counter!("nbd_udp_empty_packets_total", "group" => self.group_label.clone()).increment(1);

                                    continue
                                };

                                #[cfg(feature = "metrics-exporter")]
                                metrics::counter!("nbd_udp_packets_total", "group" => self.group_label.clone()).increment(1);
                                #[cfg(feature = "metrics-exporter")]
                                metrics::counter!("nbd_udp_bytes_total", "group" => self.group_label.clone()).increment(len as u64);

                                let msg_buffer = buf.split_to(len).freeze();
                                let msg_topic = Arc::clone(&self.topic);

                                match sink.submit(msg_topic, msg_buffer) {
                                    Ok(confirmation) => in_flight.push(confirmation),
                                    Err(NbdError::Backpressure(_)) => {
                                        #[cfg(feature = "metrics-exporter")]
                                        metrics::counter!("nbd_errors_full_queue_total", "group" => self.group_label.clone()).increment(1);
                                    }
                                    Err(_) => {
                                        #[cfg(feature = "metrics-exporter")]
                                        metrics::counter!("nbd_errors_sender_total", "group" => self.group_label.clone()).increment(1);
                                    }
                                }

                                if buf.capacity() < MAX_UDP_PAYLOAD {
                                    buf.reserve(MAX_UDP_PAYLOAD.saturating_mul(BUFFER_RESERVATION_HEADROOM));
                                };

                            }
                            Err(e) => {
                                #[cfg(feature = "metrics-exporter")]
                                metrics::counter!("nbd_errors_listeners_total", "group" => self.group_label.clone()).increment(1);

                                if last_error_log.elapsed() > Duration::from_secs(LOG_THRESHOLD) {
                                    warn!("A network error occured while listening into {} : {}", &self.topic, e);
                                    last_error_log = Instant::now();
                                }
                            }
                        };
                    }
                    Some(_) = in_flight.next() => {
                        while let Some(Some(_next)) = in_flight.next().now_or_never() {}
                    }
            };
        }
    }
}
