/* use kafka::{KafkaConsumer, KafkaProducer};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

fn get_random_string(len: usize) -> String {
    let vec = thread_rng().sample_iter(&Alphanumeric).take(len).collect();
    String::from_utf8(vec).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_produce_consume() {
        let message = get_random_string(12);

        // test create producer, consumer
        let maybe_producer = KafkaProducer::new("localhost:9092", "LIBKAFKA_TEST_TOPIC");
        let maybe_consumer = KafkaConsumer::new("localhost:9092", "LIBKAFKA_TEST_TOPIC");
        assert_eq!(maybe_producer.is_ok(), true);
        assert_eq!(maybe_consumer.is_ok(), true);
        let producer = maybe_producer.unwrap();
        let mut consumer = maybe_consumer.unwrap();

        // test produce
        assert_eq!(Ok(()), producer.produce(&message));
        assert_eq!(Ok(()), producer.produce(&message));

        // test consume
        assert_eq!(Ok(message.to_owned()), consumer.consume());
        assert_eq!(Ok(message), consumer.consume());
    }
} */
