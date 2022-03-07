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

///Struct containin the producer data.
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

///Convert hasmap to kafka message headers.
fn map_to_header(map: HashMap<String, String>) -> OwnedHeaders {
    map.iter()
        .fold(OwnedHeaders::new(), |headers, (k, v)| headers.add(k, v))
}

impl<T> KafkaProducer<T>
where
    T: Producer + FromClientConfig,
{
    ///Create a new producer from the given config and topic name.
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
            BaseRecord::to(&self.topic).payload(&message.payload);
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
            BaseRecord::to(&self.topic).payload(&message.payload);
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
/* #[cfg(any(feature = "async", test))]
pub mod future_producer {
    use super::*;
    use rdkafka::{
        client::Client,
        message::ToBytes,
        producer::{
            future_producer::{FutureProducerContext, OwnedDeliveryResult},
            FutureProducer, FutureRecord,
        },
        util::Timeout,
    };

    pub struct DefaultFutureProducer(FutureProducer<FutureProducerContext<DefaultProducerContext>>);

    impl DefaultFutureProducer {
        pub async fn send<K, P, T>(
            &self,
            record: FutureRecord<'_, K, P>,
            queue_timeout: T,
        ) -> OwnedDeliveryResult
        where
            K: ToBytes + ?Sized,
            P: ToBytes + ?Sized,
            T: Into<Timeout>,
        {
            self.0.send(record, queue_timeout).await
        }
    }

    impl<T> Producer for DefaultFutureProducer where rdkafka::util::Timeout: std::convert::From<T> {
        fn client(&self) -> &Client<DefaultProducerContext> {
            self.0.client()
        }
        fn in_flight_count(&self) -> i32 {
            self.0.in_flight_count()
        }
        fn flush(&self, timeout: T)
        {
            self.0.flush(timeout)
        }

        fn init_transactions(
            &self,
            timeout: T,
        ) -> rdkafka::error::KafkaResult<()> {
            self.0.init_transactions(timeout)
        }

        fn begin_transaction(&self) -> rdkafka::error::KafkaResult<()> {
            self.0.begin_transaction()
        }

        fn send_offsets_to_transaction(
            &self,
            offsets: &rdkafka::TopicPartitionList,
            cgm: &rdkafka::consumer::ConsumerGroupMetadata,
            timeout: T,
        ) -> rdkafka::error::KafkaResult<()> {
            self.0.send_offsets_to_transaction(offsets, cgm, timeout)
        }

        fn commit_transaction(
            &self,
            timeout: T,
        ) -> rdkafka::error::KafkaResult<()> {
            self.0.commit_transaction(timeout)
        }

        fn abort_transaction(
            &self,
            timeout: T,
        ) -> rdkafka::error::KafkaResult<()> {
            self.0.abort_transaction(timeout)
        }

        fn context(&self) -> &std::sync::Arc<FutureProducerContext<DefaultProducerContext>> {
            self.client().context()
        }
    }

    impl DefaultFutureProducer {
        ///Put a new message in the producer memory buffer.
        ///As this producer is threaded it is automaticaly polled.
        pub async fn produce(
            &self,
            message: KafkaMessage,
            timeout: Option<Duration>,
        ) -> Result<()> {
            let mut payload: FutureRecord<str, str> =
                FutureRecord::to(&self.topic).payload(&message.message);
            if let Some(key) = &message.key {
                payload = payload.key(key);
            }
            if let Some(headers) = message.headers {
                payload = payload.headers(map_to_header(headers));
            }
            let delivery_status = self.producer_type.send(payload, timeout).await;
            if let Err((e, _)) = delivery_status {
                return Err(KafkaError::DeliveryError(format!("{}", e)));
            };
            Ok(())
        }
    }
} */

#[cfg(test)]
mod producer_test {
    use rdkafka::message::Headers;

    use super::*;

    #[test]
    fn test_map_to_header() {
        let expected = OwnedHeaders::new()
            .add("test", "test")
            .add("test2", "test2");
        let input = HashMap::from([
            ("test".to_owned(), "test".to_owned()),
            ("test2".to_owned(), "test2".to_owned()),
        ]);
        let headers = map_to_header(input);
        let mut map = HashMap::new();
        let mut expected_map = HashMap::new();
        for i in 0..headers.count() {
            if let Some((k, v)) = headers.get_as::<str>(i) {
                map.insert(k, v.unwrap());
            }
            if let Some((k, v)) = expected.get_as::<str>(i) {
                expected_map.insert(k, v.unwrap());
            }
        }
        assert_eq!(map, expected_map);
    }

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
