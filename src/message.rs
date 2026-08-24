use bytes::Bytes;

use std::sync::Arc;

use crate::errors::NbdError;

#[derive(Debug)]
pub struct Message {
    pub topic: Arc<str>,
    pub payload: Bytes,
}

impl Message {
    pub fn from_bytes(data: Bytes, topic: Arc<str>) -> Result<Self, NbdError> {
        if data.is_empty() {
            Err(NbdError::InvalidPacket(String::from(
                "Received packet is empty.",
            )))
        } else {
            Ok(Self {
                topic,
                payload: data,
            })
        }
    }
}

#[cfg(test)]
/// Message tests
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;

    #[test]
    /// Tests message creation with an empty payload (error expected)
    fn reject_empty_packet() {
        let topic = Arc::from("empty-packet-test");
        let test = Message::from_bytes(Bytes::new(), topic);
        assert!(test.is_err());
    }

    #[test]
    /// Tests message creation with a simple payload
    fn accept_valid_packet() {
        let topic = Arc::from("valid-packet-test");
        let data = Bytes::from_static(b"my vaild packet test !");
        let test = Message::from_bytes(data.clone(), topic);
        assert!(test.is_ok());
        assert_eq!(test.unwrap().payload, data);
    }

    #[test]
    /// Tests message creation with a payload bigger than the maximum UDP packet possible (65_535)
    fn do_not_panic() {
        let topic = Arc::from("panic-packet-test");
        let data = Bytes::from(vec![0xFFu8; 70_000]);
        let test = Message::from_bytes(data.clone(), topic);
        assert!(test.is_ok());
        assert_eq!(test.unwrap().payload, data);
    }
}
