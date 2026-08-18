use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use tokio::net::UdpSocket;
use tokio::sync::mpsc;

use bytes::BytesMut;

use tracing::{info, warn};

use crate::config::{Interface, ProviderConfig};
use crate::errors::NbdError;
use crate::message::Message;

pub struct Provider {
    pub group: IpAddr,
    #[cfg(feature = "metrics-exporter")]
    pub group_label: Arc<str>,
    topic: Arc<str>,
    port: u16,
    interface: Interface,
    buff_size: usize,
    socket: Option<Socket>,
}

impl Provider {
    pub fn from_config(config: &ProviderConfig) -> Provider {
        Provider {
            group: config.group,
            #[cfg(feature = "metrics-exporter")]
            group_label: Arc::<str>::from(config.group.to_string()),
            topic: Arc::from(config.topic.as_str()),
            port: config.port,
            interface: config.interface.clone(),
            buff_size: config.message_size,
            socket: None,
        }
    }

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

    pub fn get_socket(&self) -> Result<&Socket, NbdError> {
        self.socket
            .as_ref()
            .ok_or_else(|| NbdError::Socket(String::from("The socket doesn't exist yet.")))
    }

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

    pub async fn start_listener(
        &mut self,
        tx: mpsc::Sender<Message>,
        cancel_token: tokio_util::sync::CancellationToken,
    ) -> Result<(), NbdError> {
        let udp_socket: std::net::UdpSocket = match self.socket.take() {
            Some(socket) => Ok(socket.into()),
            None => Err(NbdError::Socket(String::from(
                "Failed to convert socket to UDP socket because socket does not exist.",
            ))),
        }?;
        let listener = UdpSocket::from_std(udp_socket)?;

        let mut buf = BytesMut::with_capacity(self.buff_size);

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    warn!("Listener on {} was cancelled.", self.group);
                    break Ok(());
                }
                stat = listener.recv_buf(&mut buf) => {
                    match stat {
                        Ok(len) => {
                            #[cfg(feature = "metrics-exporter")]
                            metrics::counter!("nbd_udp_packets_total", "group" => self.group_label.clone()).increment(1);
                            #[cfg(feature = "metrics-exporter")]
                            metrics::counter!("nbd_udp_bytes_total", "group" => self.group_label.clone()).increment(len as u64);

                            let current_buffer = buf.split_to(len).freeze();
                            buf.reserve(self.buff_size);

                            match Message::from_bytes(current_buffer, Arc::clone(&self.topic)) {
                                Ok(message) => {
                                    tx.send(message).await?;
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to create message from bytes from {} : {}",
                                        &self.group, e
                                    );
                                }
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
            };
        }
    }
}
