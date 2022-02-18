use std::{collections::HashMap, time::Duration};

#[cfg(any(feature = "kafka_debug", test))]
use rdkafka::message::{OwnedMessage, Timestamp};
use rdkafka::{
    config::{ClientConfig, FromClientConfig},
    consumer::Consumer,
    message::Message,
};

use crate::error::{KafkaError, Result};

pub use rdkafka::consumer::BaseConsumer;
#[cfg(any(feature = "async", test))]
pub use rdkafka::consumer::StreamConsumer;
#[cfg(any(feature = "async", test))]
use rdkafka::consumer::MessageStream;

#[derive(Debug)]
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
    pub fn new(config: HashMap<String, String>, topic_name: &[&str]) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        for (opt, val) in config.iter() {
            client_config.set(opt, val);
        }
        let consumer: T = client_config.create()?;
        consumer.subscribe(topic_name)?;
        Ok(KafkaConsumer {
            consumer_type: consumer,
        })
    }
}

impl KafkaConsumer<BaseConsumer> {
    ///Extract a message frome a BaseConsume.
    ///If the timeout is none this function block until a message is received.
    pub fn consume(&self, timeout: Option<Duration>) -> Result<Option<String>> {
        let payload = match self.consumer_type.poll(timeout) {
            Some(Ok(p)) => p,
            Some(Err(e)) => return Err(KafkaError::RDKafkaError(e)),
            None => return Ok(None),
        };

        let msg = match payload.payload_view::<str>() {
            None => return Err(KafkaError::EmptyMsgError),
            Some(Ok(s)) => s.to_owned(),
            Some(Err(e)) => return Err(KafkaError::Utf8FormatError(e)),
        };
        Ok(Some(msg))
    }
}

#[cfg(any(feature = "async", test))]
impl KafkaConsumer<StreamConsumer> {
    ///Extract a message frome a StreamConsumer.
    ///This function block until a message is received.
    ///If debug_kafka feature is enabled only return a debug message,
    ///only use this for testing purpose.
    pub async fn consume(&self) -> Result<String> {
        #[cfg(not(any(feature = "kafka_debug", test)))]
        let payload = self.consumer_type.recv().await?;
        #[cfg(any(feature = "kafka_debug", test))]
        let payload = OwnedMessage::new(
            Some("debug".as_bytes().to_vec()),
            None,
            "debug".to_owned(),
            Timestamp::NotAvailable,
            1,
            1,
            None,
        );
        let msg = match payload.payload_view::<str>() {
            None => return Err(KafkaError::EmptyMsgError),
            Some(Ok(s)) => s.to_owned(),
            Some(Err(e)) => return Err(KafkaError::Utf8FormatError(e)),
        };
        Ok(msg)
    }

    ///Constructs a stream that yields messages from this consumer.
    ///To use this stream it is recomended to use a library that implements stream utilities
    ///like futures or tokio_stream.
    pub fn stream(&self) -> MessageStream<'_> {
        self.consumer_type.stream()
    }
}

#[cfg(test)]
mod consumer_test {
    use super::*;
    use rdkafka::consumer::StreamConsumer;

    #[test]
    fn test_base_consumer_new() {
        KafkaConsumer::<BaseConsumer>::new(default_config("test", "test"), &["test"]).unwrap();
    }

    #[test]
    fn test_base_consumer_consume() {
        let consumer =
            KafkaConsumer::<BaseConsumer>::new(default_config("test", "test"), &["test"]).unwrap();
        let msg = consumer.consume(Some(Duration::from_millis(0))).unwrap();
        assert_eq!(msg, None);
    }

    #[tokio::test]
    async fn test_stream_consumer_new() {
        KafkaConsumer::<StreamConsumer>::new(default_config("test", "test"), &["test"]).unwrap();
    }

    #[tokio::test]
    async fn test_stream_consumer_consume() {
        let consumer =
            KafkaConsumer::<StreamConsumer>::new(default_config("test", "test"), &["test"])
                .unwrap();
        let msg = consumer.consume().await.unwrap();
        assert_eq!(msg, "debug")
    }
    #[tokio::test]
    async fn test_stream_consumer_stream(){
        let consumer = KafkaConsumer::<StreamConsumer>::new(default_config("test", "test"), &["test"])
            .unwrap();
        consumer.stream();
    }
}
