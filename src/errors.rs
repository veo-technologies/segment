//! Errors which may arise from this crate.

use std::{fmt, time::Duration};

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

impl ApiError {
    /// Whether Segment recommends retrying this response.
    pub fn is_retryable(&self) -> bool {
        let status = self.status.as_u16();
        matches!(status, 408 | 410 | 429 | 460)
            || (500..600).contains(&status) && !matches!(status, 501 | 505 | 511)
    }

    /// Delay requested by Segment through `Retry-After` or
    /// `X-RateLimit-Reset`, when expressed as seconds.
    pub fn retry_after(&self) -> Option<Duration> {
        ["retry-after", "x-ratelimit-reset"]
            .into_iter()
            .find_map(|name| self.headers.get(name))
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs)
    }
}

/// An enum of errors this crate may produce.
#[derive(Error, Debug)]
pub enum Error {
    /// The given message is too large to be sent to Segment's API.
    #[error("message too large")]
    MessageTooLarge,
    /// The batch envelope is too large to contain even a single message.
    #[error("batch envelope too large")]
    BatchTooLarge,
    /// The background delivery queue has reached its configured capacity.
    #[error("analytics queue is full")]
    QueueFull,
    /// The analytics client is closing or has already closed.
    #[error("analytics client is closed")]
    ClientClosed,
    /// The background delivery worker stopped unexpectedly.
    #[error("analytics delivery worker stopped")]
    WorkerStopped,
    /// No Tokio runtime was active when the background client was built.
    #[error("a Tokio runtime is required to build an Analytics client")]
    RuntimeUnavailable,
    /// A flush did not finish within the configured timeout.
    #[error("analytics flush timed out after {0:?}")]
    FlushTimeout(std::time::Duration),
    /// A configured middleware rejected the message.
    #[error("analytics middleware rejected the message: {0}")]
    Middleware(String),
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
