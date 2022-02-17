use std::result;

use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum KafkaError {
    #[error("invalid utf8 encoding: {0}")]
    Utf8FormatError(#[from] std::str::Utf8Error),
    #[error("consumer unexpectedly returned an empty message")]
    EmptyMsgError,
    #[error("unable to send message: `{0}`")]
    DeliveryError(String),
    #[error(transparent)]
    RDKafkaError(#[from] rdkafka::error::KafkaError),
}

pub type Result<T, E = KafkaError> = result::Result<T, E>;
