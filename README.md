# libkafka

Easy as pie rust lib for Kafka with python bindings.

<!-- ![](./libkafka.jpg) -->

See usage examples in `./tests` directory.

## Build for rust

Add this to your `Cargo.toml`:

```toml
[dependencies]
libkafka = { git = "https://gitlab.w6d.io/w6d/ia/ml/libkafka.git" }
```

Add this to your `.cargo/config`

```toml
[net]
git-fetch-with-cli = true
```

```bash
cargo build --release
ls ./target/release/libkafka.rlib
```

```rust
use kafka::{KafkaProducer, KafkaConsumer};

let broker = "localhost:9092";

let producer = KafkaProducer::new(broker, "KAFKA_TOPIC").unwrap();
producer.produce(broker, "KAFKA_TOPIC", "message").unwrap();

let mut consumer = KafkaConsumer::new(broker, "KAFKA_TOPIC").unwrap();
let message = consumer.consume().unwrap();
```

## Build for python

Build and rename from `libkafka.dylib` to `kafka.so` (renaming is important)
```bash
cargo build --release --features "python"
cp ./target/release/libkafka.dylib kafka.so
```

Then simply copy the `kafka.so` file to the root of your python project and simply :

```python
from kafka import Producer, Consumer

broker = "localhost:9092"

producer = Producer(broker, "KAFKA_TOPIC")
producer.produce("message")

consumer = Consumer(broker, "KAFKA_TOPIC")
message = consumer.consume()
```

## Refs

* [rdkafka](https://github.com/fede1024/rust-rdkafka)
    * [rdkafka smol](https://github.com/fede1024/rust-rdkafka/blob/master/examples/smol_runtime.rs)
    * [poll](https://docs.rs/rdkafka/0.24.0/rdkafka/consumer/base_consumer/struct.BaseConsumer.html#method.poll)
    * [base producer](https://docs.rs/rdkafka/0.24.0/rdkafka/producer/base_producer/struct.BaseProducer.html)
* [PYO3 rust python bindings](https://github.com/PyO3/pyo3)
* [error management](https://nick.groenen.me/posts/rust-error-handling/)
    * [thiserror](https://github.com/dtolnay/thiserror)
* [local kafka](https://kafka.apache.org/quickstart)
* [smol](https://github.com/stjepang/smol)
