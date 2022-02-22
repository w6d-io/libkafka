use std::{collections::HashMap, time::Duration};

use rdkafka::{
    config::{ClientConfig, FromClientConfig},
    message::OwnedHeaders,
    producer::{BaseRecord, DefaultProducerContext, Producer},
};

use crate::{
    error::{KafkaError, Result},
    KafkaMessage,
};

pub use rdkafka::producer::{BaseProducer, ThreadedProducer};

pub type DefaultThreadedProducer = ThreadedProducer<DefaultProducerContext>;

pub struct KafkaProducer<T: Producer> {
    producer_type: T,
    topic: String,
}

///Generate a simple config.
pub fn default_config(broker: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_owned(), broker.to_owned()),
        ("session.timeout.ms".to_owned(), "5000".to_owned()),
    ])
}

///convert hasmap to kafka message headers
fn map_to_header(map: HashMap<String, String>) -> OwnedHeaders {
    map.iter()
        .fold(OwnedHeaders::new(), |headers, (k, v)| headers.add(k, v))
}

impl<T> KafkaProducer<T>
where
    T: Producer + FromClientConfig,
{
    pub fn new(config: &HashMap<String, String>, topic_name: &str) -> Result<KafkaProducer<T>> {
        let mut client_config = ClientConfig::new();
        for (opt, val) in config.iter() {
            client_config.set(opt, val);
        }
        let producer: T = client_config.create()?;
        Ok(KafkaProducer {
            producer_type: producer,
            topic: topic_name.to_owned(),
        })
    }

    ///Flushes any pending messages.
    ///This method should be called before termination to ensure delivery of
    ///all enqueued messages. It will call poll() internally.
    pub fn flush(&self, timeout: Option<Duration>) {
        self.producer_type.flush(timeout);
    }
}

impl KafkaProducer<DefaultThreadedProducer> {
    ///Put a new message in the producer memory buffer.
    ///As this producer is threaded it is automaticaly polled.
    pub fn produce(&self, message: KafkaMessage) -> Result<()> {
        let mut payload: BaseRecord<str, str> =
            BaseRecord::to(&self.topic).payload(&message.message);
        if let Some(key) = &message.key {
            payload = payload.key(key);
        }
        if let Some(headers) = message.headers {
            payload = payload.headers(map_to_header(headers));
        }
        let delivery_status = self.producer_type.send(payload);
        if let Err((e, _)) = delivery_status {
            return Err(KafkaError::DeliveryError(format!("{}", e)));
        };
        Ok(())
    }
}

impl KafkaProducer<BaseProducer> {
    ///Put a new message in the producer memory buffer.
    ///poll must be called to send the message.
    pub fn produce(&self, message: KafkaMessage) -> Result<()> {
        let mut payload: BaseRecord<str, str> =
            BaseRecord::to(&self.topic).payload(&message.message);
        if let Some(key) = &message.key {
            payload = payload.key(key);
        }
        if let Some(headers) = message.headers {
            payload = payload.headers(map_to_header(headers));
        }
        let delivery_status = self.producer_type.send(payload);
        if let Err((e, _)) = delivery_status {
            return Err(KafkaError::DeliveryError(format!("{}", e)));
        };
        Ok(())
    }

    ///Polls the producer, returning the number of events served.
    ///Regular calls to poll are required to process the events and execute
    ///the message delivery callbacks.
    pub fn poll(&self, timeout: Option<Duration>) -> i32 {
        self.producer_type.poll(timeout)
    }
}

#[cfg(test)]
mod producer_test {
    use super::*;
    #[test]
    fn test_base_consumer_new() {
        KafkaProducer::<BaseProducer>::new(&default_config("test"), "test").unwrap();
    }
    #[test]
    fn test_base_consumer_produce() {
        let message = KafkaMessage {
            message: "test".to_owned(),
            key: None,
            headers: None,
        };
        let producer: KafkaProducer<BaseProducer> =
            KafkaProducer::new(&default_config("test"), "test").unwrap();
        producer.produce(message).unwrap();
        producer.poll(Some(Duration::from_secs(0)));
    }
    #[test]
    fn test_threaded_consumer_new() {
        KafkaProducer::<DefaultThreadedProducer>::new(&default_config("test"), "test").unwrap();
    }

    #[test]
    fn test_threaded_consumer_produce() {
        let message = KafkaMessage {
            message: "test".to_owned(),
            key: None,
            headers: None,
        };
        let producer: KafkaProducer<DefaultThreadedProducer> =
            KafkaProducer::new(&default_config("test"), "test").unwrap();
        producer.produce(message).unwrap();
    }
}
