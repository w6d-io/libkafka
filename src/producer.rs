use rdkafka::{
    config::ClientConfig,
    producer::{BaseProducer, BaseRecord, Producer},
};
use std::time::Duration;

use crate::error::KafkaError;

pub struct KafkaProducer {
    producer: BaseProducer,
    topic: String,
}

impl KafkaProducer {
    pub fn new(broker: &str, topic_name: &str) -> Result<KafkaProducer, KafkaError> {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("message.timeout.ms", "5000")
            .create()?;
        Ok(KafkaProducer {
            producer,
            topic: topic_name.to_owned(),
        })
    }

    pub fn produce(&self, message: &str) -> Result<(), KafkaError> {
        let delivery_status = self
            .producer
            .send(BaseRecord::to(&self.topic).key("").payload(message));
        if let Err((e, _)) = delivery_status {
            return Err(KafkaError::DeliveryError(format!("{}", e)));
        };
        self.producer.flush(Duration::from_secs(10));
        Ok(())
    }
}
