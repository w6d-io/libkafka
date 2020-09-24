extern crate rdkafka;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, BaseConsumer};
use rdkafka::message::Message;

pub struct KafkaConsumer {
    consumer: BaseConsumer,
}

#[cfg(test)]
impl std::fmt::Debug for KafkaConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Point")
         .field("consumer", &"BaseConsumer".to_owned())
         .finish()
    }
}

impl KafkaConsumer {
    pub fn new(topic_name: &str) -> Result<KafkaConsumer, String> {
        let c = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .set("group.id", &format!("{}_ID", topic_name))
            .create();

        let consumer: BaseConsumer;
        match c {
            Ok(con) => {consumer = con;},
            Err(e) => {return Err(format!("unable to create producer: {}", e));},
        }
        match consumer.subscribe(&[topic_name]) {
            Err(e) => {return Err(format!("unable to create consumer: {}", e))}
            _ => {}
        }
        Ok(KafkaConsumer{consumer: consumer})
    }

    pub fn consume(&mut self) -> Result<String, String> {
        smol::block_on(async {
            let message = self.consumer.poll(None);
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
}
