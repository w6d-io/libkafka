pub mod consumer;
pub mod error;
pub mod producer;
pub mod utils;

use std::collections::HashMap;

pub use consumer::KafkaConsumer;
pub use producer::KafkaProducer;
pub use rdkafka::message;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct KafkaMessage {
    pub headers: Option<HashMap<String, String>>,
    pub key: Option<String>,
    pub payload: String,
}
