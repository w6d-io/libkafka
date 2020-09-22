extern crate rdkafka;

use std::future::Future;
use std::time::{Duration, Instant};

use futures::future::{self, FutureExt};
use futures::stream::StreamExt;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::util::AsyncRuntime;

pub struct SmolRuntime;
impl AsyncRuntime for SmolRuntime {
    type Delay = future::Map<smol::Timer, fn(Instant)>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        smol::spawn(task).detach()
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        smol::Timer::after(duration).map(|_| ())
    }
}

pub fn consume(topic_name: &str) -> Result<String, String> {
    smol::block_on(async {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .set("group.id", "rust-rdkafka-smol-runtime-example")
            .create()
            .expect("Consumer creation failed");
        consumer.subscribe(&[topic_name]).unwrap();

        let mut stream =
            consumer.start_with_runtime::<SmolRuntime>(Duration::from_millis(100), false);
        let message = stream.next().await;
        match message {
            Some(Ok(message)) => {
                let msg = match message.payload_view::<str>() {
                    None => "".to_owned(),
                    Some(Ok(s)) => s.to_owned(),
                    Some(Err(e)) => format!("<invalid utf-8> {}", e),
                };
                Ok(msg)
            },
            Some(Err(e)) => {
                Err(format!("an error occurred while streaming kafka messages: {}", e))
            },
            None => {
                Err("Consumer unexpectedly returned no messages".to_owned())
            }
        }
    })
}
