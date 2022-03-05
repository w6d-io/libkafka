pub mod consumer;
pub mod error;
pub mod producer;

use std::collections::HashMap;

pub use consumer::KafkaConsumer;
pub use producer::KafkaProducer;

#[derive(Debug, PartialEq)]
pub struct KafkaMessage {
    pub headers: Option<HashMap<String, String>>,
    pub key: Option<String>,
    pub message: String,
}
