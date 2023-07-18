use std::result;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum LibKafkaError {
    #[error("invalid utf8 encoding: {0}")]
    Utf8FormatError(#[from] std::str::Utf8Error),
    #[error("consumer unexpectedly returned an empty message")]
    EmptyMsgError,
    #[error(transparent)]
    RDKafkaError(#[from] rdkafka::error::KafkaError),
}

#[cfg(not(anyhow))]
pub type Result<T, E = LibKafkaError> = result::Result<T, E>;

#[cfg(anyhow)]
pub type Result = anyhow::Result<T>;
