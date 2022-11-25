use std::result;
pub use rdkafka::error::KafkaError;

use thiserror::Error;


#[derive(Error, Debug, PartialEq, Eq)]
pub enum LibKafkaError {
    #[error("invalid utf8 encoding: {0}")]
    Utf8FormatError(#[from] std::str::Utf8Error),
    #[error("consumer unexpectedly returned an empty message")]
    EmptyMsgError,
    #[error("unable to send message: `{0}`")]
    DeliveryError(String),
    #[error(transparent)]
    RDKafkaError(#[from] rdkafka::error::KafkaError),
}

#[cfg(not(anyhow))]
pub type Result<T, E = LibKafkaError> = result::Result<T, E>;

#[cfg(anyhow)]
pub type Result  = anyhow::Result<T>;
