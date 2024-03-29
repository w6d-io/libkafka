use std::{collections::HashMap, time::Duration};

#[cfg(any(feature = "kafka_debug", test))]
use rdkafka::message::{OwnedMessage, Timestamp};
use rdkafka::config::{ClientConfig, FromClientConfig};





#[allow(unused_imports)]
use crate::{
    error::{LibKafkaError, Result},
    utils::extract_message,
    KafkaMessage,
};

pub use rdkafka::consumer::{BaseConsumer, Consumer};

#[derive(Debug, Clone)]
pub struct KafkaConsumer<T: Consumer> {
    consumer_type: T,
}

///Generate a simple config.
pub fn default_config(broker: &str, group_id: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_owned(), broker.to_owned()),
        ("session.timeout.ms".to_owned(), "6000".to_owned()),
        ("enable.partition.eof".to_owned(), "false".to_owned()),
        ("enable.auto.commit".to_owned(), "true".to_owned()),
        ("auto.offset.reset".to_owned(), "earliest".to_owned()),
        ("group.id".to_owned(), format!("{group_id}_ID")),
    ])
}

impl<T: Consumer + FromClientConfig> KafkaConsumer<T> {
    /// Initialise a new consumer with the given config and subscribe it to the given topics.
    #[allow(unused_variables)]
    pub fn new<S: AsRef<str>>(config: &HashMap<String, String>, topics_name: &[S]) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        for (opt, val) in config.iter() {
            client_config.set(opt, val);
        }
        let consumer: T = client_config.create()?;
        let topic_slice: Vec<&str> = topics_name.iter().map(|v| v.as_ref()).collect();
        consumer.subscribe(&topic_slice)?;
        Ok(KafkaConsumer {
            consumer_type: consumer,
        })
    }
}

impl KafkaConsumer<BaseConsumer> {
    ///Extract a message from a BaseConsume.
    ///If the timeout is none this function block until a message is received.
    #[allow(unused_variables)]
    pub fn consume(&self, timeout: Option<Duration>) -> Result<Option<KafkaMessage>> {
        #[cfg(not(any(feature = "kafka_debug", test)))]
        let message = match self.consumer_type.poll(timeout) {
            Some(Ok(p)) => p,
            Some(Err(e)) => return Err(LibKafkaError::RDKafkaError(e)),
            None => return Ok(None),
        };
        #[cfg(any(feature = "kafka_debug", test))]
        let message = OwnedMessage::new(
            Some("debug".as_bytes().to_vec()),
            None,
            "debug".to_owned(),
            Timestamp::NotAvailable,
            1,
            1,
            None,
        );
        let ret = extract_message(message)?;
        Ok(Some(ret))
    }
}

#[cfg(any(feature = "async", test))]
use rdkafka::consumer::{MessageStream, DefaultConsumerContext};
#[cfg(any(feature = "async", test))]
pub use rdkafka::consumer::StreamConsumer;

#[cfg(any(feature = "async", test))]
impl KafkaConsumer<StreamConsumer> {
    ///Extract a message from a StreamConsumer.
    ///This function block until a message is received.
    ///If debug_kafka feature is enabled only return a debug message,
    ///only use this for testing purpose.
    pub async fn consume(&self) -> Result<KafkaMessage> {
        #[cfg(not(any(feature = "kafka_debug", test)))]
        let message = self.consumer_type.recv().await?;
        #[cfg(any(feature = "kafka_debug", test))]
        let message = OwnedMessage::new(
            Some("debug".as_bytes().to_vec()),
            None,
            "debug".to_owned(),
            Timestamp::NotAvailable,
            1,
            1,
            None,
        );
        extract_message(message)
    }

    /// Constructs a stream that yields messages from this consumer.
    /// To use this stream it is recommended to use a library that implements stream utilities
    /// like futures or tokio_stream.
    pub fn stream(&self) -> MessageStream<'_, DefaultConsumerContext> {
        self.consumer_type.stream()
    }
}

#[cfg(test)]
mod consumer_test {
    use rdkafka::consumer::StreamConsumer;

    use super::*;

    #[test]
    fn test_base_consumer_new() {
        KafkaConsumer::<BaseConsumer>::new(&default_config("test", "test"), &["test"]).unwrap();
    }

    #[test]
    fn test_base_consumer_consume() {
        let expected = KafkaMessage {
            headers: None,
            key: None,
            payload: "debug".to_owned(),
        };
        let consumer: KafkaConsumer<BaseConsumer> =
            KafkaConsumer::new(&default_config("test", "test"), &["test"]).unwrap();
        let message = consumer.consume(Some(Duration::from_millis(0))).unwrap();
        assert_eq!(message.unwrap(), expected)
    }

    #[tokio::test]
    async fn test_stream_consumer_new() {
        KafkaConsumer::<StreamConsumer>::new(&default_config("test", "test"), &["test"]).unwrap();
    }

    #[tokio::test]
    async fn test_stream_consumer_consume() {
        let expected = KafkaMessage {
            headers: None,
            key: None,
            payload: "debug".to_owned(),
        };
        let consumer: KafkaConsumer<StreamConsumer> =
            KafkaConsumer::new(&default_config("test", "test"), &["test"]).unwrap();
        let message = consumer.consume().await.unwrap();
        assert_eq!(message, expected)
    }
    #[tokio::test]
    async fn test_stream_consumer_stream() {
        let consumer: KafkaConsumer<StreamConsumer> =
            KafkaConsumer::new(&default_config("test", "test"), &["test"]).unwrap();
        consumer.stream();
    }
}
