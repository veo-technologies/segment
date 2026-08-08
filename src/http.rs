//! Low-level HTTP bindings to the Segment tracking API.

use crate::ApiError;
use crate::Client;
use crate::Message;
use crate::Result;
use std::time::Duration;

/// A client which synchronously sends single messages to the Segment tracking
/// API.
///
/// `HttpClient` implements [`Client`]; see that trait for more on how to send
/// events to Segment.
#[derive(Clone, Debug)]
pub struct HttpClient {
    client: reqwest::Client,
    host: String,
}

impl Default for HttpClient {
    fn default() -> Self {
        HttpClient {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::new(10, 0))
                .build()
                .expect("failed to build default reqwest client"),
            host: "https://api.segment.io".to_owned(),
        }
    }
}

impl HttpClient {
    /// Construct a new `HttpClient` from a `reqwest::Client` and a Segment API
    /// scheme and host.
    ///
    /// If you don't care to re-use an existing `reqwest::Client`, you can use
    /// the `Default::default` value, which will send events to
    /// `https://api.segment.io`.
    pub fn new(client: reqwest::Client, host: impl Into<String>) -> HttpClient {
        HttpClient {
            client,
            host: host.into(),
        }
    }
}

impl Client for HttpClient {
    async fn send<'a>(&'a self, write_key: &'a str, mut msg: Message) -> Result<()> {
        msg.prepare()?;

        let path = match &msg {
            Message::Identify(_) => "/v1/identify",
            Message::Track(_) => "/v1/track",
            Message::Page(_) => "/v1/page",
            Message::Screen(_) => "/v1/screen",
            Message::Group(_) => "/v1/group",
            Message::Alias(_) => "/v1/alias",
            Message::Batch(_) => "/v1/batch",
        };

        let response = self
            .client
            .post(format!("{}{}", self.host, path))
            .basic_auth(write_key, Some(""))
            .json(&msg)
            .send()
            .await?;

        if response.status().is_client_error() || response.status().is_server_error() {
            let status = response.status();
            let headers = response.headers().clone();
            let body = response.text().await?;
            return Err(ApiError {
                status,
                body,
                headers,
            }
            .into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
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
        let listener = TcpListener::bind("127.0.0.1:0").expect("test server should bind");
        let address = listener
            .local_addr()
            .expect("test server should have an address");
        let (sender, receiver) = mpsc::channel();
        let status = status.to_owned();
        let response_headers = response_headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect::<Vec<_>>();
        let response_body = response_body.to_owned();

        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("test request should connect");
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("read timeout should be set");

            let mut request = Vec::new();
            let mut buffer = [0_u8; 4096];
            let (header_end, content_length) = loop {
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
                    break (header_end, content_length);
                }
            };

            while request.len() < header_end + content_length {
                let read = stream
                    .read(&mut buffer)
                    .expect("request body should be readable");
                assert_ne!(read, 0, "request body ended early");
                request.extend_from_slice(&buffer[..read]);
            }

            sender
                .send(
                    String::from_utf8(request[header_end..header_end + content_length].to_vec())
                        .expect("request body should be UTF-8"),
                )
                .expect("request body should be observed");

            let mut response = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n",
                response_body.len()
            );
            for (name, value) in response_headers {
                response.push_str(&format!("{name}: {value}\r\n"));
            }
            response.push_str("\r\n");
            response.push_str(&response_body);
            stream
                .write_all(response.as_bytes())
                .expect("response should be writable");
        });

        (format!("http://{address}"), receiver)
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

    #[tokio::test]
    async fn send_adds_message_id_and_library_context() {
        let (host, request_body) = serve_once("200 OK", &[], "{}");
        let client = HttpClient::new(reqwest::Client::new(), host);

        client
            .send("write-key", track().into())
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
        let client = HttpClient::new(reqwest::Client::new(), host);

        let error = client
            .send("write-key", track().into())
            .await
            .expect_err("request should fail");
        let Error::ApiError(error) = error else {
            panic!("expected a structured API error");
        };
        assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(error.body, r#"{"code":"rate_limited"}"#);
        assert_eq!(error.headers["retry-after"], "12");
    }
}
