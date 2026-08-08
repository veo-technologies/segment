//! Errors which may arise from this crate.

use std::fmt;

use reqwest::{StatusCode, header::HeaderMap};
use thiserror::Error;

/// A message rejected locally before it was sent to Segment.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("invalid `{field}`: {message}")]
pub struct ValidationError {
    pub field: String,
    pub message: String,
}

impl ValidationError {
    pub(crate) fn new(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}

/// A non-successful response returned by Segment's Tracking API.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub body: String,
    pub headers: HeaderMap,
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.body.is_empty() {
            write!(f, "Segment API returned {}", self.status)
        } else {
            write!(f, "Segment API returned {}: {}", self.status, self.body)
        }
    }
}

impl std::error::Error for ApiError {}

/// An enum of errors this crate may produce.
#[derive(Error, Debug)]
pub enum Error {
    /// The given message is too large to be sent to Segment's API.
    #[error("message too large")]
    MessageTooLarge,
    /// The batch envelope is too large to contain even a single message.
    #[error("batch envelope too large")]
    BatchTooLarge,
    /// The message does not satisfy Segment's Tracking API contract.
    #[error(transparent)]
    ValidationError(#[from] ValidationError),
    /// Segment returned a non-successful HTTP response.
    #[error(transparent)]
    ApiError(Box<ApiError>),
    #[error("Deserialize error")]
    DeserializeError(#[from] serde_json::Error),
    #[error("Network error")]
    NetworkError(#[from] reqwest::Error),
}

impl From<ApiError> for Error {
    fn from(error: ApiError) -> Self {
        Self::ApiError(Box::new(error))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
