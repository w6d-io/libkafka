extern crate libkafka;
use libkafka::{produce, consume};

fn main() {
    produce("KAFKA_TOPIC", "hello world");
    consume("KAFKA_TOPIC");
}
