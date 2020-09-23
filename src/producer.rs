extern crate rdkafka;

use std::time::{Duration};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::runtime::SmolRuntime;

pub fn produce(topic_name: &str, message: &str) -> Result<(), String> {
    smol::block_on(async {
        let p = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .set("message.timeout.ms", "5000")
            .create();
        let producer: FutureProducer;
        match p {
            Ok(pro) => {producer = pro;},
            Err(e) => {return Err(format!("unable to create producer: {}", e));},
        }

        let delivery_status = producer
            .send_with_runtime::<SmolRuntime, Vec<u8>, _, _>(
                FutureRecord::to(topic_name).payload(message),
                Duration::from_secs(0),
            )
            .await;
        if let Err((e, _)) = delivery_status {
            println!("unable to send message: {}", e);
            return Err(format!("unable to send message: {}", e))
        };
        Ok(())
    })
}

// .set("group.id", &format!("{}_ID", topic_name))
//     .set("bootstrap.servers", "kafka.kafka:9092")
//     .set("enable.partition.eof", "false")
//     .set("session.timeout.ms", "6000")
//     .set("enable.auto.commit", "true")
//     .set("statistics.interval.ms", "2000")
//     // .set_log_level(RDKafkaLogLevel::Debug)
//     .set("auto.offset.reset", "earliest")
//     .create_with_context(context)
//     .expect("Consumer creation failed");
