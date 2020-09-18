// use futures::StreamExt;
//
// use rdkafka::client::ClientContext;
// use rdkafka::config::{ClientConfig};
// // use rdkafka::config::{RDKafkaLogLevel};
// use rdkafka::consumer::stream_consumer::StreamConsumer;
// use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
// use rdkafka::error::KafkaResult;
// use rdkafka::message::{Headers, Message};
// use rdkafka::topic_partition_list::TopicPartitionList;
//
// // A context can be used to change the behavior of producers and consumers by adding callbacks
// // that will be executed by librdkafka.
// // This particular context sets up custom callbacks to log rebalancing events.
// struct CustomContext;
//
// impl ClientContext for CustomContext {}
//
// impl ConsumerContext for CustomContext {
//     fn pre_rebalance(&self, rebalance: &Rebalance) {
//         println!("Pre rebalance {:?}", rebalance);
//     }
//
//     fn post_rebalance(&self, rebalance: &Rebalance) {
//         println!("Post rebalance {:?}", rebalance);
//     }
//
//     fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
//         println!("Committing offsets: {:?}", result);
//     }
// }
//
// // A type alias with your custom consumer can be created for convenience.
// type LoggingConsumer = StreamConsumer<CustomContext>;
//
// pub async fn consume(topic_name: &str) {
//     let context = CustomContext;
//
//     let consumer: LoggingConsumer = ClientConfig::new()
//         .set("group.id", &format!("{}_ID", topic_name))
//         .set("bootstrap.servers", "kafka.kafka:9092")
//         .set("enable.partition.eof", "false")
//         .set("session.timeout.ms", "6000")
//         .set("enable.auto.commit", "true")
//         .set("statistics.interval.ms", "2000")
//         // .set_log_level(RDKafkaLogLevel::Debug)
//         .set("auto.offset.reset", "earliest")
//         .create_with_context(context)
//         .expect("Consumer creation failed");
//
//     println!("Subscribing to topic...");
//     consumer
//         .subscribe(&[topic_name])
//         .expect("Can't subscribe to specified topics");
//
//     // consumer.start() returns a stream. The stream can be used to chain together expensive steps,
//     // such as complex computations on a thread pool or asynchronous IO.
//     let mut message_stream = consumer.start();
//
//     println!("Now entering the GREAT stream loop...");
//     while let Some(message) = message_stream.next().await {
//         match message {
//             Err(e) => println!("Kafka error: {}", e),
//             Ok(m) => {
//                 let payload = match m.payload_view::<str>() {
//                     None => "",
//                     Some(Ok(s)) => s,
//                     Some(Err(e)) => {
//                         println!("Error while deserializing message payload: {:?}", e);
//                         ""
//                     }
//                 };
//                 println!("key: '{:?}', payload: '{}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
//                       m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());
//                 if let Some(headers) = m.headers() {
//                     for i in 0..headers.count() {
//                         let header = headers.get(i).unwrap();
//                         println!("  Header {:#?}: {:?}", header.0, header.1);
//                     }
//                 }
//                 consumer.commit_message(&m, CommitMode::Async).unwrap();
//             }
//         };
//     }
//     println!("Now leaving the GREAT stream loop...");
// }
