use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, BaseConsumer};
use rdkafka::message::Message;

use crate::error::KafkaError;

pub struct KafkaConsumer {
    consumer: BaseConsumer,
}

#[cfg(test)]
impl std::fmt::Debug for KafkaConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Point")
         .field("consumer", &"BaseConsumer".to_owned())
         .finish()
    }
}

impl KafkaConsumer {
    pub fn new(broker: &str, topic_name: &str) -> Result<KafkaConsumer, KafkaError> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("session.timeout.ms", "6000")
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .set("group.id", &format!("{}_ID", topic_name))
            .create()?;

        consumer.subscribe(&[topic_name])?;
        Ok(KafkaConsumer{consumer: consumer})
    }

    pub fn consume(&mut self) -> Result<String, KafkaError> {
        let message = self.consumer.poll(None);
        match message {
            Some(Ok(message)) => {
                let msg = match message.payload_view::<str>() {
                    None => "".to_owned(),
                    Some(Ok(s)) => s.to_owned(),
                    Some(Err(e)) => format!("<invalid utf-8> {}", e),
                };
                Ok(msg)
            },
            Some(Err(e)) => {
                Err(KafkaError::StreamError(format!("{}", e)))
            },
            None => {
                Err(KafkaError::EmptyMsgError)
            }
        }
    }
}
