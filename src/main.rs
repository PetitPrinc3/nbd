#![forbid(unsafe_code)]
//No Bullshit Daemon

//use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use bytes::{Bytes, BytesMut};

use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;

use serde::Deserialize;

use thiserror::Error;

use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), NbdError> {
    let contenu = std::fs::read_to_string("config.toml")?;
    let config: Config = toml::from_str(&contenu)?;

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.nbd.verbosity));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let mut tasks = JoinSet::new();
    let (tx, mut rx) = mpsc::channel::<Message>(1000);

    for candidate in config.provider {
        let task_tx = tx.clone();
        tasks.spawn(async move {
            let mut provider: Provider = Provider::from_config(&candidate);
            match provider.subscribe(&config.nbd.network_buffer_size) {
                Ok(()) => {
                    match provider.start_listener(task_tx).await {
                        Ok(_) => {}
                        Err(e) => {
                            error!(
                                "Something went wrong while listening to {} : {}",
                                provider.group, e
                            );
                        }
                    };
                }
                Err(e) => {
                    error!(
                        "Something went wrong while subscribing to {} : {}",
                        provider.group, e
                    );
                }
            };
        });
    }

    //while tasks.join_next().await.is_some() {
    // essayer de redémarrer les producers, mais je n'ai pas compris comment ça s'orchestrait avec la consumer_task
    //};

    let consumer_task = tokio::spawn(async move {
        info!(
            "Connecting to the Kafka broker at {} ...",
            config.kafka.broker
        );

        (match ClientConfig::new()
            .set("bootstrap.servers", &config.kafka.broker)
            .set("message.timeout.ms", config.kafka.timeout.to_string())
            .set("message.send.max.retries", config.kafka.retries.to_string())
            .set("compression.type", "lz4")
            .set("acks", "1")
            .create::<rdkafka::producer::FutureProducer>()
        {
            Ok(producer) => {
                info!("Successfully connected to the Kafka broker.");
                while let Some(message) = rx.recv().await {
                    //println!("Got message @ {} on topic {} : {} ", message.timestamp, message.group, message.payload);
                    let record: FutureRecord<[u8], [u8]> =
                        FutureRecord::to(&message.topic).payload(message.payload.as_ref());

                    match producer
                        .send(record, Timeout::After(Duration::from_millis(100)))
                        .await
                    {
                        Ok(_) => {
                            match String::from_utf8(message.timestamp.to_vec()) {
                                Ok(ts) => {
                                    debug!("Successfully sent some message @ {} !", ts);
                                }
                                Err(_) => {
                                    debug!("Successfully sent some message !");
                                }
                            };
                        }
                        Err((e, _)) => {
                            warn!(
                                "Failed to send one message on topic {} : {}",
                                &message.topic, e
                            )
                        }
                    };
                }
                Ok(())
            }
            Err(e) => Err(NbdError::Kafka(e)),
        }) as Result<(), NbdError>
    });

    consumer_task.await?
}

struct Provider {
    topic: String,
    group: IpAddr,
    port: u16,
    interface: Ipv4Addr,
    buff_size: usize,
    socket: Option<Socket>,
}

impl Provider {
    fn from_config(config: &ProviderConfig) -> Provider {
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

    fn get_socket(&self) -> Result<&Socket, NbdError> {
        self.socket
            .as_ref()
            .ok_or_else(|| NbdError::Socket(String::from("The socket doesn't exist yet.")))
    }

    fn subscribe(&mut self, max_buffer_size: &usize) -> Result<(), NbdError> {
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

    async fn start_listener(&mut self, tx: mpsc::Sender<Message>) -> Result<(), NbdError> {
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
                        "A network error occured while listening to {} : {}",
                        &self.group, e
                    );
                }
            };
        }
    }
}

#[derive(Deserialize)]
struct Config {
    provider: Vec<ProviderConfig>,
    kafka: ProducerConfig,
    nbd: NbdConfig,
}

#[derive(Deserialize)]
struct ProviderConfig {
    topic: String,
    group: IpAddr,
    port: u16,
    message_size: usize,
    interface: Ipv4Addr,
}

#[derive(Deserialize)]
struct ProducerConfig {
    broker: String,
    timeout: u64,
    retries: u16,
}

#[derive(Deserialize)]
struct NbdConfig {
    network_buffer_size: usize,
    verbosity: String,
}

#[derive(Debug)]
struct Message {
    topic: String,
    timestamp: Bytes,
    payload: Bytes,
}

impl Message {
    fn from_bytes(data: Bytes, topic: String) -> Result<Self, NbdError> {
        if data.len() < 16 {
            Err(NbdError::InvalidPacket(String::from(
                "Received packet is too short (less than 16 bytes).",
            )))
        } else {
            Ok(Self {
                topic: topic,
                timestamp: data.slice(0..8),
                payload: data.clone(),
            })
        }
    }
}

#[derive(Error, Debug)]
enum NbdError {
    #[error("Socket creation failed : {0}")]
    Socket(String),

    #[error("A task failed : {0}")]
    TaskPanic(#[from] tokio::task::JoinError),

    #[error("Network error : {0}")]
    Network(#[from] std::io::Error),

    #[error("Message transmission error : {0}")]
    Transmission(#[from] tokio::sync::mpsc::error::SendError<Message>),

    #[error("Configuration error : {0}")]
    Configuration(#[from] toml::de::Error),

    #[error("Kafka error : {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("Invalid packet : {0}")]
    InvalidPacket(String),
}
