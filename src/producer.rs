extern crate rdkafka;

use std::time::{Duration};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::runtime::SmolRuntime;

pub fn produce(broker: &str, topic_name: &str, message: &str) -> Result<(), String> {
    smol::block_on(async {
        let p = ClientConfig::new()
            .set("bootstrap.servers", broker)
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
