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
use kafka::{produce, KafkaConsumer};

produce("KAFKA_TOPIC", "hello world");

let consumer = KafkaConsumer::new("KAFKA_TOPIC").unwrap();
let msg = consumer.consume().unwrap();
```

## Build for python

Build and rename from `libkafka.dylib` to `kafka.so` (renaming is important)
```bash
cargo build --release --features "python"
cp ./target/release/libkafka.dylib kafka.so
```

Then simply copy the `kafka.so` file to the root of your python project and simply :

```python
from kafka import produce, Consumer

produce("KAFKA_TOPIC", "hello world")

consumer = Consumer("KAFKA_TOPIC")
msg = consumer.consume()
```

## Refs

* [rdkafka](https://github.com/fede1024/rust-rdkafka)
    * [rdkafka smol](https://github.com/fede1024/rust-rdkafka/blob/master/examples/smol_runtime.rs)
    * [poll](https://docs.rs/rdkafka/0.24.0/rdkafka/consumer/base_consumer/struct.BaseConsumer.html#method.poll)
* [smol](https://github.com/stjepang/smol)
* [PYO3 rust python bindings](https://github.com/PyO3/pyo3)
* [local kafka](https://kafka.apache.org/quickstart)
