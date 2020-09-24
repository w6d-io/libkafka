use kafka::{produce, KafkaConsumer};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;

fn get_random_string(len: usize) -> String {
    thread_rng().sample_iter(&Alphanumeric).take(len).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_produce_consume() {
        let message = get_random_string(12);
        assert_eq!(Ok(()), produce("LIBKAFKA_TEST_TOPIC", &message));
        assert_eq!(Ok(()), produce("LIBKAFKA_TEST_TOPIC", &message));

        let maybe_consumer = KafkaConsumer::new("LIBKAFKA_TEST_TOPIC");
        assert_eq!(maybe_consumer.is_ok(), true);
        let mut consumer = maybe_consumer.unwrap();

        assert_eq!(Ok(message.to_owned()), consumer.consume());
        assert_eq!(Ok(message.to_owned()), consumer.consume());
    }
}
