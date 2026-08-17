use bytes::Bytes;

use crate::errors::NbdError;

#[derive(Debug)]
pub struct Message {
    pub topic: String,
    pub payload: Bytes,
}

impl Message {
    pub fn from_bytes(data: Bytes, topic: String) -> Result<Self, NbdError> {
        if data.is_empty() {
            Err(NbdError::InvalidPacket(String::from(
                "Received packet is empty.",
            )))
        } else {
            Ok(Self {
                topic,
                payload: data.clone(),
            })
        }
    }
}
