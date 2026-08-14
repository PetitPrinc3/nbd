use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use tokio::net::UdpSocket;

use bytes::BytesMut;

use tracing::{info, warn};

use crate::config::ProviderConfig;
use crate::errors::NbdError;
use crate::message::Message;

use tokio::sync::mpsc;

pub struct Provider {
    pub group: IpAddr,
    topic: String,
    port: u16,
    interface: Ipv4Addr,
    buff_size: usize,
    socket: Option<Socket>,
}

impl Provider {
    pub fn from_config(config: &ProviderConfig) -> Provider {
        Provider {
            topic: config.topic.clone(),
            group: config.group,
            port: config.port,
            interface: config.interface,
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

        match self.group {
            IpAddr::V4(ref maddr_v4) => {
                // join to the multicast address, with all interfaces
                self.get_socket()?
                    .join_multicast_v4(maddr_v4, &self.interface)?;
            }
            IpAddr::V6(ref maddr_v6) => {
                // join to the multicast address, with all interfaces (ipv6 uses indexes not addresses)
                self.get_socket()?.join_multicast_v6(maddr_v6, 0)?;
                self.get_socket()?.set_only_v6(true)?;
            }
        };

        match self.get_socket() {
            Ok(socket) => {
                socket.set_reuse_address(true)?;
                #[cfg(unix)]
                socket.set_reuse_port(true)?;
                socket.set_nonblocking(true)?;
                match socket.bind(&SockAddr::from(SocketAddr::new(
                    IpAddr::V4(self.interface),
                    self.port,
                ))) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(NbdError::Socket(format!("Binding failed : {}", e))),
                }?;
                Ok(())
            }
            Err(e) => Err(e),
        }?;

        info!("Successfully subscribed to {} !", &self.group);

        Ok(())
    }

    pub async fn start_listener(&mut self, tx: mpsc::Sender<Message>) -> Result<(), NbdError> {
        let udp_socket: std::net::UdpSocket = match self.socket.take() {
            Some(socket) => Ok(socket.into()),
            None => Err(NbdError::Socket(String::from(
                "Failed to convert socket to UDP socket because socket does not exist.",
            ))),
        }?;
        let listener = UdpSocket::from_std(udp_socket)?;

        let mut buf = BytesMut::with_capacity(self.buff_size);

        loop {
            match listener.recv_buf(&mut buf).await {
                Ok(len) => {
                    let current_buffer = buf.split_to(len).freeze();
                    buf.reserve(self.buff_size);
                    match Message::from_bytes(current_buffer, self.topic.clone()) {
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
                    warn!(
                        "A network error occured while listenin to {} : {}",
                        &self.group, e
                    );
                }
            };
        }
    }
}
