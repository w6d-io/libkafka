use kafka::{produce, consume};
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
        assert_eq!(Ok(message), consume("LIBKAFKA_TEST_TOPIC"));
    }
}
