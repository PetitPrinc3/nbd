//! Core UDP multicast ingestion engine.
//!
//! This module configures OS-level multicast sockets (IGMP/MLD) using [`socket2`],
//! binds to specified network interfaces, and runs a zero-copy asynchronous receive
//! loop that streams incoming UDP packets to any [`MessageSink`] implementation.
//!
//! ## Zero-copy buffer strategy
//!
//! The receive loop uses [`bytes::BytesMut`] as its buffer. When a datagram arrives,
//! the consumed bytes are split off via `split_to(len).freeze()`, producing a
//! reference-counted, immutable [`Bytes`](bytes::Bytes) handle — **no payload memory
//! is copied**. The buffer is re-allocated only when remaining capacity drops below
//! the configured `message_size`.
//!
//! ## Bounded concurrency
//!
//! In-flight sink deliveries are tracked in a [`FuturesUnordered`] collection. The
//! receive branch of the `tokio::select!` loop is disabled when `in_flight.len() >=
//! parallel_senders`, applying backpressure to prevent unbounded task accumulation.
//!
//! ## Graceful shutdown
//!
//! On [`CancellationToken`](tokio_util::sync::CancellationToken) cancellation, the
//! listener only exits once all in-flight deliveries have completed, ensuring no
//! messages are silently dropped.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use futures::StreamExt;
use futures::stream::FuturesUnordered;

use tokio::net::UdpSocket;

use bytes::BytesMut;

use tracing::{debug, info, warn};

use crate::config::{Interface, ProviderConfig};
use crate::errors::NbdError;
use crate::sinks::MessageSink;

/// Headroom multiplier for initial buffer allocation.
///
/// The receive buffer is allocated as `message_size * BUFFER_RESERVATION_HEADROOM`
/// bytes to reduce re-allocation frequency under sustained packet load.
const BUFFER_RESERVATION_HEADROOM: usize = 100;

/// A UDP multicast listener bound to a specific group and network interface.
///
/// ## Lifecycle
///
/// 1. Create from configuration: `Provider::from(&ProviderConfig)`
/// 2. Subscribe to multicast group: [`subscribe()`](Provider::subscribe) — creates the socket,
///    sets options (`reuse_address`, `nonblocking`, buffer size), and joins the multicast group.
/// 3. Start receiving: [`start_listener()`](Provider::start_listener) — runs the async receive
///    loop, dispatching payloads to the given [`MessageSink`].
pub struct Provider {
    pub group: IpAddr,
    #[cfg(feature = "metrics-exporter")]
    pub group_label: Arc<str>,
    topic: Arc<str>,
    port: u16,
    interface: Interface,
    buff_size: usize,
    parallel_senders: usize,
    socket: Option<Socket>,
}

impl From<&ProviderConfig> for Provider {
    /// Generate a provider from configuration data
    fn from(config: &ProviderConfig) -> Provider {
        Provider {
            group: config.group,
            #[cfg(feature = "metrics-exporter")]
            group_label: Arc::<str>::from(config.group.to_string()),
            topic: Arc::from(config.topic.as_str()),
            port: config.port,
            interface: config.interface.clone(),
            buff_size: config.message_size,
            parallel_senders: config.parallel_senders,
            socket: None,
        }
    }
}

impl Provider {
    /// Create the listening socket
    fn create_socket(&mut self, max_buffer_size: &usize) -> Result<(), NbdError> {
        let domain = if self.group.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        };

        let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;

        socket.set_recv_buffer_size(*max_buffer_size)?;

        self.socket = Some(socket);

        Ok(())
    }

    /// Get the listening socket
    pub fn get_socket(&self) -> Result<&Socket, NbdError> {
        self.socket
            .as_ref()
            .ok_or_else(|| NbdError::Socket(String::from("The socket doesn't exist yet.")))
    }

    /// Subscribe to the provider's multicast group using IGMP join requests on the specified interface.
    pub fn subscribe(&mut self, max_buffer_size: &usize) -> Result<(), NbdError> {
        self.create_socket(max_buffer_size)?;

        match self.get_socket() {
            Ok(socket) => {
                socket.set_reuse_address(true)?;
                #[cfg(unix)]
                socket.set_reuse_port(true)?;
                socket.set_nonblocking(true)?;
                let binding_addr = if self.group.is_ipv4() {
                    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), self.port)
                } else {
                    SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), self.port)
                };
                match socket.bind(&SockAddr::from(binding_addr)) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(NbdError::Socket(format!("Binding failed : {}", e))),
                }?;
                Ok(())
            }
            Err(e) => Err(e),
        }?;

        match self.group {
            IpAddr::V4(ref maddr_v4) => match self.interface {
                Interface::V4(ref interface) => {
                    self.get_socket()?.join_multicast_v4(maddr_v4, interface)?;
                }
                Interface::V6(ref interface) => {
                    warn!(
                        "An unexpected error occured because the requested interface ({}) is Ipv6 while the requested group is Ipv4 ({}). Defaulting to the default Ipv4 interface (0.0.0.0)",
                        interface, maddr_v4
                    );
                    self.get_socket()?
                        .join_multicast_v4(maddr_v4, &Ipv4Addr::UNSPECIFIED)?;
                }
            },
            IpAddr::V6(ref maddr_v6) => match self.interface {
                Interface::V6(interface) => {
                    self.get_socket()?.join_multicast_v6(maddr_v6, interface)?;
                    self.get_socket()?.set_only_v6(true)?;
                }
                Interface::V4(ref interface) => {
                    warn!(
                        "An unexpected error occured because the requested interface ({}) is Ipv4 while the requested group is Ipv6 ({}). Defaulting to the default Ipv6 interface (0)",
                        interface, maddr_v6
                    );
                    self.get_socket()?.join_multicast_v6(maddr_v6, 0)?;
                    self.get_socket()?.set_only_v6(true)?;
                }
            },
        };

        info!("Successfully subscribed to {} !", &self.group);

        Ok(())
    }

    /// Start the asynchronous receive loop, dispatching UDP datagrams to the given sink.
    ///
    /// The loop uses `tokio::select!` with three branches:
    /// - **Cancellation**: exits only when `in_flight` is empty (graceful drain).
    /// - **Receive**: reads into `BytesMut`, splits via zero-copy `freeze()`, and pushes
    ///   a sink delivery future into `in_flight`. Disabled when `in_flight.len() >= parallel_senders`
    ///   to apply backpressure.
    /// - **Completion**: polls `in_flight` for completed deliveries, logging success/failure
    ///   and updating Prometheus counters when the `metrics-exporter` feature is active.
    ///
    /// # Errors
    ///
    /// Returns `NbdError::Socket` if the socket has not been created (call [`subscribe`](Provider::subscribe) first).
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

        let mut buf = BytesMut::with_capacity(self.buff_size * BUFFER_RESERVATION_HEADROOM);

        let mut in_flight = FuturesUnordered::new();

        loop {
            tokio::select! {
                    _ = cancel_token.cancelled(), if in_flight.is_empty() => {
                        warn!("Listener on {} was cancelled.", self.group);
                        break Ok(());
                    }
                    stat = listener.recv_buf(&mut buf), if !cancel_token.is_cancelled() && in_flight.len() < self.parallel_senders => {
                        match stat {
                            Ok(len) => {
                                if len == 0 {
                                    debug!("Received an empty packet on {}.", self.group);

                                    #[cfg(feature = "metrics-exporter")]
                                    metrics::counter!("nbd_empty_packets_total", "group" => self.group_label.clone()).increment(1);

                                    continue
                                };

                                #[cfg(feature = "metrics-exporter")]
                                metrics::counter!("nbd_udp_packets_total", "group" => self.group_label.clone()).increment(1);
                                #[cfg(feature = "metrics-exporter")]
                                metrics::counter!("nbd_udp_bytes_total", "group" => self.group_label.clone()).increment(len as u64);

                                let msg_buffer = buf.split_to(len).freeze();
                                let msg_topic = Arc::clone(&self.topic);
                                let msg_sink = sink.clone();

                                in_flight.push(async move {
                                    msg_sink.send(msg_topic, msg_buffer).await
                                });

                                if buf.capacity() < self.buff_size {
                                    buf.reserve(self.buff_size * BUFFER_RESERVATION_HEADROOM);
                                };
                            }
                            Err(e) => {
                                #[cfg(feature = "metrics-exporter")]
                                metrics::counter!("nbd_errors_listeners_total", "group" => self.group_label.clone()).increment(1);

                                warn!(
                                    "A network error occured while listeninto {} : {}",
                                    &self.group, e
                                );
                            }
                        };
                    }
                    Some(result) = in_flight.next() => {
                        match result {
                            Ok(_) => {
                                #[cfg(feature = "metrics-exporter")]
                                metrics::counter!("nbd_kafka_sent_total", "topic" => self.topic.clone()).increment(1);

                                debug!(
                                    "Successfully sent some message on topic {} !",
                                    self.topic
                                );
                            }
                            Err(e) => {
                                #[cfg(feature = "metrics-exporter")]
                                metrics::counter!("nbd_errors_kafka_total", "topic" => self.topic.clone()).increment(1);

                                warn!(
                                    "Failed to send one message on topic {} : {}",
                                    self.topic, e
                                );
                             }
                        }
                    }
            };
        }
    }
}
