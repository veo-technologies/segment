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
    /// `X-RateLimit-Reset`.
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after_at(jiff::Timestamp::now())
    }

    pub(crate) fn retry_after_at(&self, now: jiff::Timestamp) -> Option<Duration> {
        if let Some(value) = self
            .headers
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
        {
            if let Ok(secs) = value.parse::<u64>() {
                return Some(Duration::from_secs(secs));
            }
            if let Ok(system_time) = httpdate::parse_http_date(value) {
                let target = jiff::Timestamp::try_from(system_time).ok()?;
                if target <= now {
                    return Some(Duration::ZERO);
                }
                return Duration::try_from(target.duration_since(now)).ok();
            }
        }

        if let Some(value) = self
            .headers
            .get("x-ratelimit-reset")
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
        {
            let secs = parse_rate_limit_seconds(value)?;
            if secs >= 1_000_000_000 {
                let target = jiff::Timestamp::from_second(i64::try_from(secs).ok()?).ok()?;
                if target <= now {
                    return Some(Duration::ZERO);
                }
                return Duration::try_from(target.duration_since(now)).ok();
            }
            return Some(Duration::from_secs(secs));
        }

        None
    }
}

fn parse_rate_limit_seconds(value: &str) -> Option<u64> {
    if let Ok(secs) = value.parse::<u64>() {
        return Some(secs);
    }
    let (whole, frac) = value.split_once('.')?;
    if !whole.is_empty() && frac.chars().all(|c| c.is_ascii_digit()) {
        return whole.parse::<u64>().ok();
    }
    None
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
    #[error("Compression error")]
    CompressionError(#[from] std::io::Error),
}

impl Error {
    pub(crate) fn retry_metadata(&self) -> (Option<StatusCode>, bool) {
        match self {
            Self::ApiError(err) => (Some(err.status), err.is_retryable()),
            Self::NetworkError(_) => (None, true),
            _ => (None, false),
        }
    }
}

impl From<ApiError> for Error {
    fn from(error: ApiError) -> Self {
        Self::ApiError(Box::new(error))
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

    use super::*;

    fn api_error_with_headers(headers: &[(&str, &str)]) -> ApiError {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.insert(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }
        ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            body: String::new(),
            headers: map,
        }
    }

    #[test]
    fn retry_after_parses_delta_seconds() {
        let err = api_error_with_headers(&[("Retry-After", " 42 ")]);
        let now = jiff::Timestamp::from_second(1_700_000_000).unwrap();
        assert_eq!(err.retry_after_at(now), Some(Duration::from_secs(42)));
    }

    #[test]
    fn retry_after_parses_http_date() {
        let now = jiff::Timestamp::from_second(1_700_000_000).unwrap();
        // 1_700_000_000 = Tue, 14 Nov 2023 22:13:20 GMT
        let future = api_error_with_headers(&[("Retry-After", "Tue, 14 Nov 2023 22:14:20 GMT")]);
        assert_eq!(future.retry_after_at(now), Some(Duration::from_secs(60)));

        let past = api_error_with_headers(&[("Retry-After", "Tue, 14 Nov 2023 22:12:20 GMT")]);
        assert_eq!(past.retry_after_at(now), Some(Duration::ZERO));
    }

    #[test]
    fn retry_after_parses_relative_ratelimit_reset() {
        let now = jiff::Timestamp::from_second(1_700_000_000).unwrap();
        let err = api_error_with_headers(&[("X-RateLimit-Reset", "15")]);
        assert_eq!(err.retry_after_at(now), Some(Duration::from_secs(15)));

        let float_err = api_error_with_headers(&[("X-RateLimit-Reset", "15.75")]);
        assert_eq!(float_err.retry_after_at(now), Some(Duration::from_secs(15)));
    }

    #[test]
    fn retry_after_parses_unix_epoch_ratelimit_reset() {
        let now = jiff::Timestamp::from_second(1_700_000_000).unwrap();
        let future = api_error_with_headers(&[("X-RateLimit-Reset", "1700000045")]);
        assert_eq!(future.retry_after_at(now), Some(Duration::from_secs(45)));

        let past = api_error_with_headers(&[("X-RateLimit-Reset", "1699999900")]);
        assert_eq!(past.retry_after_at(now), Some(Duration::ZERO));
    }
}
