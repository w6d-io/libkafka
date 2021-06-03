use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum KafkaError {
    #[error("an error occurred while streaming kafka messages: `{0}`")]
    StreamError(String),
    #[error("consumer unexpectedly returned no messages")]
    EmptyMsgError,
    #[error("unable to send message: `{0}`")]
    DeliveryError(String),
    #[error(transparent)]
    KafkaError(#[from] rdkafka::error::KafkaError),
}
