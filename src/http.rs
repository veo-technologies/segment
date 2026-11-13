//! Low-level HTTP bindings to the Segment tracking API.

use std::time::{Duration, Instant};

#[cfg(feature = "gzip")]
use std::io::Write as _;

use serde::Serialize;
use serde_json::{Map, Value};

use crate::ApiError;
use crate::Client;
use crate::Message;
use crate::Result;
use crate::ValidationError;
use crate::message::BatchMessage;

/// The default Segment Tracking API host.
pub const DEFAULT_HOST: &str = "https://api.segment.io";

/// The EU1 Segment Tracking API host.
pub const EU1_HOST: &str = "https://events.eu1.segmentapis.com";

#[cfg(feature = "gzip")]
const GZIP_THRESHOLD_BYTES: usize = 1024;

/// Controls retries for transient Tracking API failures.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Number of retries after the initial request.
    pub max_retries: u32,
    /// Initial exponential-backoff delay.
    pub initial_backoff: Duration,
    /// Maximum exponential-backoff delay.
    pub max_backoff: Duration,
    /// Maximum delay accepted from a rate-limit response.
    pub max_retry_after: Duration,
    /// Maximum time a call may spend retrying.
    pub max_elapsed_time: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(60),
            max_retry_after: Duration::from_secs(300),
            max_elapsed_time: Duration::from_secs(300),
        }
    }
}

impl RetryPolicy {
    /// Disable automatic retries.
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            ..Self::default()
        }
    }

    fn delay(&self, retry: u32, error: Option<&ApiError>) -> Duration {
        if let Some(retry_after) = error.and_then(ApiError::retry_after) {
            return retry_after.min(self.max_retry_after);
        }

        let multiplier = 1_u32.checked_shl(retry.min(31)).unwrap_or(u32::MAX);
        let backoff = self
            .initial_backoff
            .saturating_mul(multiplier)
            .min(self.max_backoff);
        jitter(backoff)
    }
}

/// A client which sends messages to the Segment tracking API.
///
/// `HttpClient` implements [`Client`]; see that trait for more on how to send
/// events to Segment.
#[derive(Clone, Debug)]
pub struct HttpClient {
    client: reqwest::Client,
    host: String,
    retry_policy: RetryPolicy,
    #[cfg(feature = "gzip")]
    gzip: bool,
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::new(10, 0))
                .timeout(Duration::from_secs(15))
                .build()
                .expect("failed to build default reqwest client"),
            host: DEFAULT_HOST.to_owned(),
            retry_policy: RetryPolicy::default(),
            #[cfg(feature = "gzip")]
            gzip: true,
        }
    }
}

/// Builder for configuring an [`HttpClient`].
#[derive(Clone, Debug)]
pub struct HttpClientBuilder {
    host: String,
    connect_timeout: Duration,
    timeout: Duration,
    retry_policy: RetryPolicy,
    #[cfg(feature = "gzip")]
    gzip: bool,
    client: Option<reqwest::Client>,
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self {
            host: DEFAULT_HOST.to_owned(),
            connect_timeout: Duration::from_secs(10),
            timeout: Duration::from_secs(15),
            retry_policy: RetryPolicy::default(),
            #[cfg(feature = "gzip")]
            gzip: true,
            client: None,
        }
    }
}

impl HttpClientBuilder {
    /// Set the Segment Tracking API host, stripping any trailing slashes.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into().trim_end_matches('/').to_owned();
        self
    }

    /// Configure the client to send events to Segment's EU1 region ([`EU1_HOST`]).
    pub fn eu1(mut self) -> Self {
        self.host = EU1_HOST.to_owned();
        self
    }

    /// Set the TCP/TLS connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the total request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Configure how transient network and API failures are retried.
    pub fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Enable or disable gzip compression for JSON payloads >= 1024 bytes.
    #[cfg(feature = "gzip")]
    pub fn gzip(mut self, enabled: bool) -> Self {
        self.gzip = enabled;
        self
    }

    /// Reuse an existing [`reqwest::Client`].
    pub fn client(mut self, client: reqwest::Client) -> Self {
        self.client = Some(client);
        self
    }

    /// Validate configuration and construct an [`HttpClient`].
    pub fn build(self) -> Result<HttpClient> {
        if self.host.is_empty() || reqwest::Url::parse(&self.host).is_err() {
            return Err(ValidationError::new("host", "must be a valid URL").into());
        }

        let client = match self.client {
            Some(client) => client,
            None => reqwest::Client::builder()
                .connect_timeout(self.connect_timeout)
                .timeout(self.timeout)
                .build()?,
        };

        Ok(HttpClient {
            client,
            host: self.host,
            retry_policy: self.retry_policy,
            #[cfg(feature = "gzip")]
            gzip: self.gzip,
        })
    }
}

impl HttpClient {
    /// Create an [`HttpClientBuilder`] with default settings.
    pub fn builder() -> HttpClientBuilder {
        HttpClientBuilder::default()
    }

    /// Construct a new `HttpClient` from a `reqwest::Client` and a Segment API
    /// scheme and host.
    ///
    /// If you don't care to re-use an existing `reqwest::Client`, you can use
    /// the `Default::default` value, which will send events to
    /// `https://api.segment.io`.
    pub fn new(client: reqwest::Client, host: impl Into<String>) -> HttpClient {
        HttpClient {
            client,
            host: host.into().trim_end_matches('/').to_owned(),
            retry_policy: RetryPolicy::default(),
            #[cfg(feature = "gzip")]
            gzip: true,
        }
    }

    /// Configure how transient network and API failures are retried.
    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    fn encode_body(&self, json_bytes: Vec<u8>) -> Result<(Vec<u8>, bool)> {
        #[cfg(feature = "gzip")]
        if self.gzip && json_bytes.len() >= GZIP_THRESHOLD_BYTES {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            encoder.write_all(&json_bytes)?;
            return Ok((encoder.finish()?, true));
        }

        Ok((json_bytes, false))
    }
}

#[derive(Serialize)]
struct WireBatch<'a> {
    batch: &'a [BatchMessage],
    #[serde(rename = "sentAt")]
    sent_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    context: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    integrations: Option<&'a Value>,
    #[serde(flatten)]
    extra: &'a Map<String, Value>,
}

impl Client for HttpClient {
    async fn send<'a>(&'a self, write_key: &'a str, msg: &'a Message) -> Result<()> {
        msg.validate()?;

        let path = match msg {
            Message::Identify(_) => "/v1/identify",
            Message::Track(_) => "/v1/track",
            Message::Page(_) => "/v1/page",
            Message::Screen(_) => "/v1/screen",
            Message::Group(_) => "/v1/group",
            Message::Alias(_) => "/v1/alias",
            Message::Batch(_) => "/v1/batch",
        };

        let single_payload = match msg {
            Message::Batch(_) => None,
            other => {
                let mut prepared = other.clone();
                prepared.prepare()?;
                let json = serde_json::to_vec(&prepared)?;
                Some(self.encode_body(json)?)
            }
        };

        let prepared_batch_storage;
        let batch_envelope = match msg {
            Message::Batch(batch) => {
                let items: &[BatchMessage] = if batch.batch.iter().all(BatchMessage::is_prepared) {
                    &batch.batch
                } else {
                    let mut cloned = batch.batch.clone();
                    for item in &mut cloned {
                        item.prepare()?;
                    }
                    prepared_batch_storage = cloned;
                    &prepared_batch_storage
                };
                Some((items, batch))
            }
            _ => None,
        };

        let started = Instant::now();
        let mut retry = 0;
        loop {
            let (body, is_gzipped) = match (&single_payload, batch_envelope) {
                (Some((body, gzipped)), None) => (body.clone(), *gzipped),
                (None, Some((items, batch))) => {
                    let wire = WireBatch {
                        batch: items,
                        sent_at: format!("{:.9}", jiff::Timestamp::now()),
                        context: batch.context.as_ref(),
                        integrations: batch.integrations.as_ref(),
                        extra: &batch.extra,
                    };
                    let json = serde_json::to_vec(&wire)?;
                    self.encode_body(json)?
                }
                _ => unreachable!("message is either single or batch"),
            };

            let mut request = self
                .client
                .post(format!("{}{}", self.host, path))
                .basic_auth(write_key, Some(""))
                .header(
                    "User-Agent",
                    concat!("segment-rust/", env!("CARGO_PKG_VERSION")),
                )
                .header("Content-Type", "application/json")
                .header("X-Retry-Count", retry);
            if is_gzipped {
                request = request.header("Content-Encoding", "gzip");
            }

            let result = request.body(body).send().await;

            let error = match result {
                Ok(response)
                    if !response.status().is_client_error()
                        && !response.status().is_server_error() =>
                {
                    return Ok(());
                }
                Ok(response) => {
                    let status = response.status();
                    let headers = response.headers().clone();
                    match response.text().await {
                        Ok(body) => ErrorOrApi::Api(ApiError {
                            status,
                            body,
                            headers,
                        }),
                        Err(error) => ErrorOrApi::Network(error),
                    }
                }
                Err(error) => ErrorOrApi::Network(error),
            };

            let retryable = match &error {
                ErrorOrApi::Api(error) => error.is_retryable(),
                ErrorOrApi::Network(_) => true,
            };
            if !retryable || retry == self.retry_policy.max_retries {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    path,
                    retry,
                    retryable,
                    "giving up on Segment Tracking API request"
                );
                return Err(error.into_error());
            }

            let api_error = match &error {
                ErrorOrApi::Api(error) => Some(error),
                ErrorOrApi::Network(_) => None,
            };
            let delay = self.retry_policy.delay(retry, api_error);
            if started.elapsed().saturating_add(delay) > self.retry_policy.max_elapsed_time {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    path,
                    retry,
                    "giving up on Segment Tracking API request after max_elapsed_time"
                );
                return Err(error.into_error());
            }

            #[cfg(feature = "tracing")]
            tracing::debug!(
                path,
                retry,
                delay_ms = delay.as_millis() as u64,
                "retrying Segment Tracking API request"
            );

            tokio::time::sleep(delay).await;
            retry += 1;
        }
    }
}
enum ErrorOrApi {
    Api(ApiError),
    Network(reqwest::Error),
}

impl ErrorOrApi {
    fn into_error(self) -> crate::Error {
        match self {
            Self::Api(error) => error.into(),
            Self::Network(error) => error.into(),
        }
    }
}

fn jitter(duration: Duration) -> Duration {
    if duration.is_zero() {
        return duration;
    }

    let bytes = uuid::Uuid::new_v4().into_bytes();
    let random = u64::from_le_bytes(bytes[..8].try_into().expect("UUID has 16 bytes"));
    let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
    let half = nanos / 2;
    Duration::from_nanos(half + random % (nanos - half + 1))
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::mpsc::{self, Receiver},
        thread,
        time::Duration,
    };

    use reqwest::StatusCode;
    use serde_json::{Value, json};

    use super::*;
    use crate::{
        Error,
        message::{Track, User},
    };

    fn serve_once(
        status: &str,
        response_headers: &[(&str, &str)],
        response_body: &str,
    ) -> (String, Receiver<String>) {
        serve_responses(vec![TestResponse {
            status: status.to_owned(),
            headers: response_headers
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
            body: response_body.to_owned(),
        }])
    }

    struct TestResponse {
        status: String,
        headers: Vec<(String, String)>,
        body: String,
    }

    fn serve_responses(responses: Vec<TestResponse>) -> (String, Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test server should bind");
        let address = listener
            .local_addr()
            .expect("test server should have an address");
        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            for response in responses {
                let (mut stream, _) = listener.accept().expect("test request should connect");
                let body = read_request_body(&mut stream);
                sender.send(body).expect("request body should be observed");

                let mut wire_response = format!(
                    "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n",
                    response.status,
                    response.body.len()
                );
                for (name, value) in response.headers {
                    wire_response.push_str(&format!("{name}: {value}\r\n"));
                }
                wire_response.push_str("\r\n");
                wire_response.push_str(&response.body);
                stream
                    .write_all(wire_response.as_bytes())
                    .expect("response should be writable");
            }
        });

        (format!("http://{address}"), receiver)
    }

    fn read_request_body(stream: &mut TcpStream) -> String {
        read_captured_request(stream).body
    }

    struct CapturedRequest {
        headers: String,
        body: String,
    }

    fn read_captured_request(stream: &mut TcpStream) -> CapturedRequest {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout should be set");

        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        let (header_end, content_length, is_gzipped) = loop {
            let read = stream
                .read(&mut buffer)
                .expect("request should be readable");
            assert_ne!(read, 0, "request ended before headers were complete");
            request.extend_from_slice(&buffer[..read]);

            if let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                let header_end = header_end + 4;
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                let is_gzipped = headers.lines().any(|line| {
                    line.split_once(':').is_some_and(|(name, value)| {
                        name.eq_ignore_ascii_case("content-encoding")
                            && value.trim().eq_ignore_ascii_case("gzip")
                    })
                });
                break (header_end, content_length, is_gzipped);
            }
        };

        while request.len() < header_end + content_length {
            let read = stream
                .read(&mut buffer)
                .expect("request body should be readable");
            assert_ne!(read, 0, "request body ended early");
            request.extend_from_slice(&buffer[..read]);
        }

        let raw_body = &request[header_end..header_end + content_length];
        let body = if is_gzipped {
            let mut decoder = flate2::read::GzDecoder::new(raw_body);
            let mut decoded = String::new();
            decoder
                .read_to_string(&mut decoded)
                .expect("gzipped request body should decompress to UTF-8");
            decoded
        } else {
            String::from_utf8(raw_body.to_vec()).expect("request body should be UTF-8")
        };

        CapturedRequest {
            headers: String::from_utf8_lossy(&request[..header_end]).into_owned(),
            body,
        }
    }

    fn track() -> Track {
        Track {
            user: User::UserId {
                user_id: "user".to_owned(),
            },
            event: "Example".to_owned(),
            properties: json!({}),
            ..Default::default()
        }
    }

    fn immediate_retries(max_retries: u32) -> RetryPolicy {
        RetryPolicy {
            max_retries,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            max_retry_after: Duration::ZERO,
            max_elapsed_time: Duration::from_secs(5),
        }
    }

    #[tokio::test]
    async fn send_adds_message_id_and_library_context() {
        let (host, request_body) = serve_once("200 OK", &[], "{}");
        let client =
            HttpClient::new(reqwest::Client::new(), host).with_retry_policy(RetryPolicy::none());

        client
            .send("write-key", &track().into())
            .await
            .expect("request should succeed");

        let body: Value = serde_json::from_str(
            &request_body
                .recv_timeout(Duration::from_secs(5))
                .expect("request body should be captured"),
        )
        .expect("request should contain JSON");
        assert!(uuid::Uuid::parse_str(body["messageId"].as_str().unwrap()).is_ok());
        assert_eq!(body["context"]["library"]["name"], "segment-rust");
        assert_eq!(
            body["context"]["library"]["version"],
            env!("CARGO_PKG_VERSION")
        );
    }

    #[tokio::test]
    async fn api_errors_preserve_status_body_and_headers() {
        let (host, _request_body) = serve_once(
            "429 Too Many Requests",
            &[("Retry-After", "12")],
            r#"{"code":"rate_limited"}"#,
        );
        let client =
            HttpClient::new(reqwest::Client::new(), host).with_retry_policy(RetryPolicy::none());

        let error = client
            .send("write-key", &track().into())
            .await
            .expect_err("request should fail");
        let Error::ApiError(error) = error else {
            panic!("expected a structured API error");
        };
        assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(error.body, r#"{"code":"rate_limited"}"#);
        assert_eq!(error.headers["retry-after"], "12");
    }

    #[tokio::test]
    async fn retries_transient_and_rate_limited_responses_with_stable_ids() {
        let (host, request_bodies) = serve_responses(vec![
            TestResponse {
                status: "500 Internal Server Error".to_owned(),
                headers: vec![],
                body: "server error".to_owned(),
            },
            TestResponse {
                status: "429 Too Many Requests".to_owned(),
                headers: vec![("Retry-After".to_owned(), "60".to_owned())],
                body: "rate limited".to_owned(),
            },
            TestResponse {
                status: "200 OK".to_owned(),
                headers: vec![],
                body: "{}".to_owned(),
            },
        ]);
        let client =
            HttpClient::new(reqwest::Client::new(), host).with_retry_policy(immediate_retries(3));

        client
            .send("write-key", &track().into())
            .await
            .expect("third attempt should succeed");

        let message_ids = (0..3)
            .map(|_| {
                let body = request_bodies
                    .recv_timeout(Duration::from_secs(5))
                    .expect("request body should be captured");
                serde_json::from_str::<Value>(&body)
                    .expect("request should contain JSON")["messageId"]
                    .as_str()
                    .expect("message ID should be present")
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert!(message_ids.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn retry_classification_and_headers_follow_segment_contract() {
        let rate_limited = ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            body: String::new(),
            headers: [("Retry-After", "23")]
                .into_iter()
                .map(|(name, value)| {
                    (
                        reqwest::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                        reqwest::header::HeaderValue::from_str(value).unwrap(),
                    )
                })
                .collect(),
        };
        assert!(rate_limited.is_retryable());
        assert_eq!(rate_limited.retry_after(), Some(Duration::from_secs(23)));

        let bad_request = ApiError {
            status: StatusCode::BAD_REQUEST,
            body: String::new(),
            headers: Default::default(),
        };
        assert!(!bad_request.is_retryable());
    }

    #[test]
    fn builder_normalizes_host_and_supports_eu1() {
        let client = HttpClient::new(reqwest::Client::new(), "https://example.com///");
        assert_eq!(client.host, "https://example.com");

        let built = HttpClient::builder()
            .host("https://custom.segment.local/")
            .build()
            .expect("valid host should build");
        assert_eq!(built.host, "https://custom.segment.local");

        let eu1 = HttpClient::builder()
            .eu1()
            .build()
            .expect("eu1 host should build");
        assert_eq!(eu1.host, EU1_HOST);

        let invalid = HttpClient::builder()
            .host("not a valid url")
            .build()
            .expect_err("invalid host should fail");
        assert!(matches!(invalid, Error::ValidationError(_)));
    }

    #[cfg(feature = "gzip")]
    #[tokio::test]
    async fn gzip_compresses_large_payloads_and_skips_small_payloads() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test server should bind");
        let address = listener.local_addr().unwrap();
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let captured = read_captured_request(&mut stream);
                tx.send(captured).unwrap();
                stream
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                    )
                    .unwrap();
            }
        });

        let client = HttpClient::builder()
            .host(format!("http://{address}"))
            .retry_policy(RetryPolicy::none())
            .build()
            .unwrap();

        // Small payload (< 1024 bytes) should not be gzipped.
        client
            .send("write-key", &track().into())
            .await
            .expect("small request should succeed");
        let small = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            !small
                .headers
                .to_ascii_lowercase()
                .contains("content-encoding: gzip")
        );

        // Large payload (>= 1024 bytes) should be gzipped and decompress cleanly.
        let mut large_track = track();
        large_track.properties = json!({ "blob": "x".repeat(2048) });
        client
            .send("write-key", &large_track.into())
            .await
            .expect("large request should succeed");
        let large = rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            large
                .headers
                .to_ascii_lowercase()
                .contains("content-encoding: gzip")
        );
        let parsed: Value = serde_json::from_str(&large.body).unwrap();
        assert_eq!(parsed["properties"]["blob"], "x".repeat(2048));
    }

    #[test]
    fn retry_policy_delay_handles_unix_epoch_ratelimit_reset() {
        let future_epoch = (jiff::Timestamp::now().as_second() + 120).to_string();
        let rate_limited = ApiError {
            status: StatusCode::TOO_MANY_REQUESTS,
            body: String::new(),
            headers: [(
                reqwest::header::HeaderName::from_static("x-ratelimit-reset"),
                reqwest::header::HeaderValue::from_str(&future_epoch).unwrap(),
            )]
            .into_iter()
            .collect(),
        };
        let policy = RetryPolicy {
            max_retry_after: Duration::from_secs(300),
            ..RetryPolicy::default()
        };
        let delay = policy.delay(0, Some(&rate_limited));
        assert!(delay >= Duration::from_secs(115) && delay <= Duration::from_secs(121));
    }
}
