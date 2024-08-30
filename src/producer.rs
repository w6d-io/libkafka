use std::{collections::HashMap, marker::PhantomData, time::Duration};

use rdkafka::{
    config::{ClientConfig, FromClientConfig},
    error::KafkaError,
    producer::{BaseRecord, DefaultProducerContext, Producer, ProducerContext},
};

use crate::{
    error::{LibKafkaError, Result},
    utils::map_to_header,
    KafkaMessage,
};

pub use rdkafka::producer::{BaseProducer, FutureProducer, ThreadedProducer};

pub type DefaultThreadedProducer = ThreadedProducer<DefaultProducerContext>;

///Struct contains the producer data.
#[derive(Clone)]
pub struct KafkaProducer<T, C = DefaultProducerContext>
where
    C: ProducerContext,
    T: Producer<C>,
{
    _producer_context: PhantomData<C>,
    producer_type: T,
    topic: String,
}

///Generate a simple config.
pub fn default_config(broker: &str) -> HashMap<String, String> {
    HashMap::from([
        ("bootstrap.servers".to_owned(), broker.to_owned()),
    ])
}

fn generate_base_message<'a>(data: &'a KafkaMessage, topic: &'a str) -> BaseRecord<'a, str, str> {
    let mut message: BaseRecord<str, str> = BaseRecord::to(topic).payload(&data.payload);
    if let Some(key) = &data.key {
        message = message.key(key);
    }
    if let Some(headers) = &data.headers {
        message = message.headers(map_to_header(headers));
    }
    message
}

impl<C, T> KafkaProducer<T, C>
where
    T: Producer<C> + FromClientConfig,
    C: ProducerContext,
{
    ///Create a new producer from the given config and topic name.
    pub fn new(config: &HashMap<String, String>, topic_name: &str) -> Result<KafkaProducer<T, C>> {
        let mut client_config = ClientConfig::new();
        for (opt, val) in config.iter() {
            client_config.set(opt, val);
        }
        let producer: T = client_config.create()?;
        Ok(KafkaProducer {
            _producer_context: PhantomData,
            producer_type: producer,
            topic: topic_name.to_owned(),
        })
    }

    ///Flushes any pending messages.
    ///This method should be called before termination to ensure delivery of
    ///all enqueued messages. It will call poll() internally.
    pub fn flush(&self, timeout: Option<Duration>) -> Result<(), KafkaError> {
        self.producer_type.flush(timeout)
    }
}

impl KafkaProducer<DefaultThreadedProducer> {
    /// Put a new message in the producer memory buffer.
    /// As this producer is threaded it is automatically polled.
    pub fn produce(&self, data: KafkaMessage) -> Result<()> {
        let message = generate_base_message(&data, &self.topic);
        let delivery_status = self.producer_type.send(message);
        if let Err((e, _)) = delivery_status {
            return Err(LibKafkaError::RDKafkaError(e));
        };
        Ok(())
    }
}

impl KafkaProducer<BaseProducer> {
    /// Put a new message in the producer memory buffer.
    /// poll must be called to send the message.
    pub fn produce(&self, data: KafkaMessage) -> Result<()> {
        let status = self
            .producer_type
            .send(generate_base_message(&data, &self.topic));
        if let Err((e, _)) = status {
            return Err(LibKafkaError::RDKafkaError(e));
        };
        Ok(())
    }

    ///Polls the producer, returning the number of events served.
    ///Regular calls to poll are required to process the events and execute
    ///the message delivery callbacks.
    pub fn poll(&self, timeout: Option<Duration>) -> () {
        self.producer_type.poll(timeout)
    }
}
#[cfg(any(feature = "async", test))]
pub mod future_producer {
    use super::*;
    use rdkafka::{
        client::DefaultClientContext,
        producer::{future_producer::FutureProducerContext, FutureProducer, FutureRecord},
    };

    pub type DefaultFutureContext = FutureProducerContext<DefaultClientContext>;

    fn generate_future_message<'a>(
        data: &'a KafkaMessage,
        topic: &'a str,
    ) -> FutureRecord<'a, str, str> {
        let mut message: FutureRecord<str, str> = FutureRecord::to(topic).payload(&data.payload);
        if let Some(key) = &data.key {
            message = message.key(key);
        }
        if let Some(headers) = &data.headers {
            message = message.headers(map_to_header(headers));
        }
        message
    }

    impl KafkaProducer<FutureProducer, FutureProducerContext<DefaultClientContext>> {
        ///Put a new message in the producer memory buffer.
        ///As this producer is threaded it is automatically polled.
        pub async fn produce(&self, data: KafkaMessage, timeout: Option<Duration>) -> Result<()> {
            let message = generate_future_message(&data, &self.topic);
            let delivery_status = self.producer_type.send(message, timeout).await;
            if let Err((e, _)) = delivery_status {
                return Err(LibKafkaError::RDKafkaError(e));
            };
            Ok(())
        }
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
            payload: "test".to_owned(),
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
            payload: "test".to_owned(),
            key: None,
            headers: None,
        };
        let producer: KafkaProducer<DefaultThreadedProducer> =
            KafkaProducer::new(&default_config("test"), "test").unwrap();
        producer.produce(message).unwrap();
    }
}
