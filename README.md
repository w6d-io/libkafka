# libkafka

Easy as pie rust lib for Kafka.

<!-- ![](./libkafka.jpg) -->

See usage examples in `./tests` directory.

## Build for rust

Add this to your `Cargo.toml`:

```toml
[dependencies]
libkafka = { git = "https://github.com/w6d-io/libkafka" branch = "develop"}
```

Add this to your `.cargo/config`

```toml
[net]
git-fetch-with-cli = true
```
todo: update exemple 
```rust
use kafka::{KafkaProducer, KafkaConsumer};

let broker = "localhost:9092";

let producer = KafkaProducer::new(broker, "KAFKA_TOPIC").unwrap();
producer.produce(broker, "KAFKA_TOPIC", "message").unwrap();

let mut consumer = KafkaConsumer::new(broker, "KAFKA_TOPIC").unwrap();
let message = consumer.consume().unwrap();
```
## Refs

* [rdkafka](https://github.com/fede1024/rust-rdkafka)
    * [rdkafka smol](https://github.com/fede1024/rust-rdkafka/blob/master/examples/smol_runtime.rs)
    * [poll](https://docs.rs/rdkafka/0.24.0/rdkafka/consumer/base_consumer/struct.BaseConsumer.html#method.poll)
    * [base producer](https://docs.rs/rdkafka/0.24.0/rdkafka/producer/base_producer/struct.BaseProducer.html)
* [PYO3 rust python bindings](https://github.com/PyO3/pyo3)
    * [GIL](https://pyo3.rs/v0.10.1/parallelism.html)
* [error management](https://nick.groenen.me/posts/rust-error-handling/)
    * [thiserror](https://github.com/dtolnay/thiserror)
* [local kafka](https://kafka.apache.org/quickstart)
* [smol](https://github.com/stjepang/smol)
