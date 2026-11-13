#![doc = include_str!("../README.md")]

use std::{fmt, time::Duration};

use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};

/// Version of Segment's OpenAPI document used to generate this crate.
pub const OPENAPI_VERSION: &str = "74.0.1";

/// Default US Public API endpoint.
pub const DEFAULT_BASE_URL: &str = "https://api.segmentapis.com";

/// EU Public API endpoint.
pub const EU_BASE_URL: &str = "https://eu1.api.segmentapis.com";

mod generated {
    #![allow(rustdoc::bare_urls)]

    include!(concat!(env!("OUT_DIR"), "/segment_public_api.rs"));
}

pub use generated::*;

/// Errors raised while configuring a Public API client.
#[derive(Debug, thiserror::Error)]
pub enum ConfigurationError {
    #[error("the Segment Public API token must not be empty")]
    EmptyToken,
    #[error("the Segment Public API token cannot be represented as an HTTP header")]
    InvalidToken(#[source] reqwest::header::InvalidHeaderValue),
    #[error("invalid Segment Public API base URL: {0}")]
    InvalidBaseUrl(String),
    #[error("could not build the Segment Public API HTTP client")]
    HttpClient(#[source] reqwest::Error),
}

/// Builder that always configures Public API bearer authentication.
pub struct AuthenticatedClientBuilder {
    token: String,
    base_url: String,
    request_timeout: Duration,
    connect_timeout: Duration,
}

impl fmt::Debug for AuthenticatedClientBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthenticatedClientBuilder")
            .field("token", &"[redacted]")
            .field("base_url", &self.base_url)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .finish()
    }
}

impl AuthenticatedClientBuilder {
    /// Start configuring a client with a Segment Public API token.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            base_url: DEFAULT_BASE_URL.to_owned(),
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
        }
    }

    /// Override the API endpoint, primarily for regional endpoints or tests.
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// Target Segment's EU Public API endpoint.
    pub fn eu1(mut self) -> Self {
        self.base_url = EU_BASE_URL.to_owned();
        self
    }

    /// Set the timeout for a complete API request.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set the TCP connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Build the generated client with a redacted bearer credential.
    pub fn build(self) -> Result<Client, ConfigurationError> {
        let token = self.token.trim();
        if token.is_empty() {
            return Err(ConfigurationError::EmptyToken);
        }

        let base_url = self.base_url.trim_end_matches('/');
        reqwest::Url::parse(base_url)
            .map_err(|error| ConfigurationError::InvalidBaseUrl(error.to_string()))?;
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(ConfigurationError::InvalidToken)?;
        authorization.set_sensitive(true);
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, authorization);

        let http = reqwest::Client::builder()
            .default_headers(headers)
            .user_agent(concat!(
                "segment-public-api-rust/",
                env!("CARGO_PKG_VERSION")
            ))
            .timeout(self.request_timeout)
            .connect_timeout(self.connect_timeout)
            .build()
            .map_err(ConfigurationError::HttpClient)?;

        Ok(Client::new_with_client(base_url, http))
    }
}

impl Client {
    /// Configure an authenticated client for Segment's default US endpoint.
    pub fn with_token(token: impl Into<String>) -> Result<Self, ConfigurationError> {
        AuthenticatedClientBuilder::new(token).build()
    }

    /// Start configuring an authenticated client.
    pub fn authenticated_builder(token: impl Into<String>) -> AuthenticatedClientBuilder {
        AuthenticatedClientBuilder::new(token)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    use super::*;

    #[test]
    fn authentication_builder_validates_and_redacts_tokens() {
        assert!(matches!(
            Client::with_token("  "),
            Err(ConfigurationError::EmptyToken)
        ));
        assert!(matches!(
            Client::with_token("bad\ntoken"),
            Err(ConfigurationError::InvalidToken(_))
        ));

        let builder = Client::authenticated_builder("super-secret");
        let debug = format!("{builder:?}");
        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("super-secret"));
    }

    #[test]
    fn authentication_builder_validates_base_urls() {
        assert!(matches!(
            Client::authenticated_builder("token")
                .base_url("not a URL")
                .build(),
            Err(ConfigurationError::InvalidBaseUrl(_))
        ));
    }

    #[test]
    fn authentication_builder_supports_eu1_and_strips_trailing_slash() {
        let eu_builder = Client::authenticated_builder("token").eu1();
        assert_eq!(eu_builder.base_url, EU_BASE_URL);
        assert!(eu_builder.build().is_ok());

        let client = Client::authenticated_builder("token")
            .base_url("https://eu1.api.segmentapis.com/")
            .build()
            .expect("trailing slash should be normalized");
        assert_eq!(client.baseurl(), EU_BASE_URL);
    }

    #[tokio::test]
    async fn generated_request_uses_bearer_auth_and_api_version() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test server should bind");
        let address = listener
            .local_addr()
            .expect("test server should have an address");
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("test server should accept");
            let mut request = [0_u8; 8_192];
            let bytes = stream
                .read(&mut request)
                .expect("request should be readable");
            let request = String::from_utf8_lossy(&request[..bytes]).to_ascii_lowercase();
            assert!(request.starts_with("get / http/1.1\r\n"));
            assert!(request.contains("authorization: bearer test-token\r\n"));
            assert!(request.contains("api-version: 74.0.1\r\n"));

            let body =
                r#"{"data":{"workspace":{"id":"workspace-id","name":"Test","slug":"test"}}}"#;
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("response should be writable");
        });

        let client = Client::authenticated_builder("test-token")
            .base_url(format!("http://{address}"))
            .build()
            .expect("authenticated client should build");
        let response = client
            .get_workspace()
            .await
            .expect("workspace response should deserialize")
            .into_inner();
        assert_eq!(
            response
                .data
                .expect("response should contain data")
                .workspace
                .id,
            "workspace-id"
        );
        server.join().expect("test server should finish");
    }
}
