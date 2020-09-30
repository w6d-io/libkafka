use std::time::{Duration};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use crate::runtime::SmolRuntime;
use crate::error::KafkaError;

pub fn produce(broker: &str, topic_name: &str, message: &str) -> Result<(), KafkaError> {
    smol::block_on(async {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("message.timeout.ms", "5000")
            .create()?;

        let delivery_status = producer
            .send_with_runtime::<SmolRuntime, Vec<u8>, _, _>(
                FutureRecord::to(topic_name).payload(message),
                Duration::from_secs(0),
            )
            .await;
        if let Err((e, _)) = delivery_status {
            return Err(KafkaError::DeliveryError(format!("{}", e)));
        };
        Ok(())
    })
}
