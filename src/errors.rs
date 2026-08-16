use thiserror::Error;

use crate::message::Message;

#[derive(Error, Debug)]
pub enum NbdError {
    #[error("Socket creation failed : {0}")]
    Socket(String),

    #[error("A task failed : {0}")]
    TaskPanic(#[from] tokio::task::JoinError),

    #[error("Network error : {0}")]
    Network(#[from] std::io::Error),

    #[error("Message transmission error : {0}")]
    Transmission(#[from] tokio::sync::mpsc::error::SendError<Message>),

    #[error("Toml error : {}", .0.message())]
    Toml(#[from] toml::de::Error),

    #[error("Kafka error : {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("Invalid packet : {0}")]
    InvalidPacket(String),

    #[error("{0}")]
    Config(String),
}
