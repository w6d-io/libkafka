use std::{collections::HashMap, time::Duration};

#[cfg(any(feature = "kafka_debug", test))]
use rdkafka::message::{OwnedMessage, Timestamp};
use rdkafka::{
    config::{ClientConfig, FromClientConfig},
    consumer::Consumer,
    message::{Headers, Message},
};

use crate::{
    error::{KafkaError, Result},
    KafkaMessage,
};

pub use rdkafka::consumer::BaseConsumer;


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

///convert kafka message headers to a hashmap
fn headers_to_map<T: Headers>(headers: &T) -> Result<HashMap<String, String>> {
    let size = headers.count();
    let mut map = HashMap::with_capacity(size);
    for i in 0..size {
        if let Some((k, v)) = headers.get_as::<str>(i) {
            map.insert(k.to_owned(), v?.to_owned());
        }
    }
    Ok(map)
}

impl<T: Consumer + FromClientConfig> KafkaConsumer<T> {
    /// Initialise a new consumer with the given config and subscribe it to the given topics.
    #[allow(unused_variables)]
    pub fn new(config: &HashMap<String, String>, topics_name: &[&str]) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        for (opt, val) in config.iter() {
            client_config.set(opt, val);
        }
        let consumer: T = client_config.create()?;
        consumer.subscribe(topics_name)?;
        Ok(KafkaConsumer {
            consumer_type: consumer,
        })
    }
}

impl KafkaConsumer<BaseConsumer> {
    ///Extract a message frome a BaseConsume.
    ///If the timeout is none this function block until a message is received.
    #[allow(unused_variables)]
    pub fn consume(&self, timeout: Option<Duration>) -> Result<Option<KafkaMessage>> {
        #[cfg(not(any(feature = "kafka_debug", test)))]
        let payload = match self.consumer_type.poll(timeout) {
            Some(Ok(p)) => p,
            Some(Err(e)) => return Err(KafkaError::RDKafkaError(e)),
            None => return Ok(None),
        };
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

        let message = match payload.payload_view::<str>() {
            None => return Err(KafkaError::EmptyMsgError),
            Some(Ok(s)) => s.to_owned(),
            Some(Err(e)) => return Err(KafkaError::Utf8FormatError(e)),
        };
        let key = match payload.key_view::<str>() {
            None => None,
            Some(Ok(k)) => Some(k.to_owned()),
            Some(Err(e)) => return Err(KafkaError::Utf8FormatError(e)),
        };
        let headers = match payload.headers() {
            None => None,
            Some(h) => Some(headers_to_map(h)?),
        };
        Ok(Some(KafkaMessage {
            message,
            headers,
            key,
        }))
    }
}

#[cfg(any(feature = "async", test))]
use rdkafka::consumer::MessageStream;
#[cfg(any(feature = "async", test))]
pub use rdkafka::consumer::StreamConsumer;

#[cfg(any(feature = "async", test))]
impl KafkaConsumer<StreamConsumer> {
    ///Extract a message frome a StreamConsumer.
    ///This function block until a message is received.
    ///If debug_kafka feature is enabled only return a debug message,
    ///only use this for testing purpose.
    pub async fn consume(&self) -> Result<KafkaMessage> {
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
        let message = match payload.payload_view::<str>() {
            None => return Err(KafkaError::EmptyMsgError),
            Some(Ok(s)) => s.to_owned(),
            Some(Err(e)) => return Err(KafkaError::Utf8FormatError(e)),
        };
        let key = match payload.key_view::<str>() {
            None => None,
            Some(Ok(k)) => Some(k.to_owned()),
            Some(Err(e)) => return Err(KafkaError::Utf8FormatError(e)),
        };
        let headers = match payload.headers() {
            None => None,
            Some(h) => Some(headers_to_map(h)?),
        };
        Ok(KafkaMessage {
            message,
            headers,
            key,
        })
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
    use rdkafka::{consumer::StreamConsumer, message::OwnedHeaders};

    use super::*;

    #[test]
    fn test_headers_to_map() {
        let headers = OwnedHeaders::new()
            .add("test1", "test1")
            .add("test2", "test2");

        let expected = HashMap::from([
            ("test1".to_owned(), "test1".to_owned()),
            ("test2".to_owned(), "test2".to_owned()),
        ]);
        let map = headers_to_map(&headers).unwrap();
        assert_eq!(map, expected);
    }

    #[test]
    fn test_base_consumer_new() {
        KafkaConsumer::<BaseConsumer>::new(&default_config("test", "test"), &["test"]).unwrap();
    }

    #[test]
    fn test_base_consumer_consume() {
        let expected = KafkaMessage {
            headers: None,
            key: None,
            message: "debug".to_owned(),
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
            message: "debug".to_owned(),
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
