//! Utilities for batching up messages.
//! When a batch is full it is automatically sent over the network

use std::{collections::VecDeque, fmt};

use serde_json::Map;

use crate::{
    Error, ValidationError,
    batcher::{Batcher, MAX_BATCH_SIZE, PreparedBatchMessage},
    client::Client,
    errors::Result,
    http::HttpClient,
    message::{Batch, BatchMessage, Message},
};

/// A batcher can accept messages into an internal buffer, and report when
/// messages must be flushed.
///
/// The recommended usage pattern looks something like this:
///
/// ```no_run
/// use segment::{AutoBatcher, Batcher, HttpClient};
/// use segment::message::{Track, User};
/// use serde_json::json;
///
/// #[tokio::main(flavor = "current_thread")]
/// async fn main() -> segment::Result<()> {
///     let client = HttpClient::default();
///     let batcher = Batcher::new(None);
///     let mut batcher = AutoBatcher::new(client, batcher, "your_write_key");
///
///     for i in 0..100 {
///         let msg = Track {
///             user: User::UserId { user_id: format!("user-{}", i) },
///             event: "Example".to_owned(),
///             properties: json!({ "foo": "bar" }),
///             ..Default::default()
///         };
///
///         batcher.push(msg).await?;
///     }
///
///     batcher.flush().await?;
///     Ok(())
/// }
/// ```
///
/// Batcher will attempt to fit messages into maximally-sized batches, thus
/// reducing the number of round trips required with Segment's tracking API.
/// However, if you produce messages infrequently, this may significantly delay
/// the sending of messages to Segment.
///
/// If this delay is a concern, it is recommended that you periodically flush
/// the batcher on your own by calling [Self::flush].
#[derive(Clone)]
pub struct AutoBatcher<C = HttpClient> {
    client: C,
    batcher: Batcher,
    pending: VecDeque<PreparedBatchMessage>,
    key: String,
}

impl<C: fmt::Debug> fmt::Debug for AutoBatcher<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AutoBatcher")
            .field("client", &self.client)
            .field("batcher", &self.batcher)
            .field("pending_len", &self.pending.len())
            .field("key", &"[redacted]")
            .finish()
    }
}

impl<C> AutoBatcher<C>
where
    C: Client,
{
    /// Construct a new, empty batcher.
    ///
    /// ```
    /// use segment::{AutoBatcher, Batcher, HttpClient};
    ///
    /// let client = HttpClient::default();
    /// let batcher = Batcher::new(None);
    /// let mut batcher = AutoBatcher::new(client, batcher, "your_write_key");
    /// ```
    pub fn new(client: C, batcher: Batcher, key: impl Into<String>) -> Self {
        Self {
            batcher,
            client,
            pending: VecDeque::new(),
            key: key.into(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.batcher.len() + self.pending.len()
    }

    /// Push a message into the batcher.
    /// If the batcher is full, send it and create a new batcher with the message.
    ///
    /// Returns an error if the message is too large to be sent to Segment's
    /// API.
    ///
    /// ```no_run
    /// use serde_json::json;
    /// use segment::{AutoBatcher, Batcher, HttpClient};
    /// use segment::message::{Track, User};
    ///
    /// #[tokio::main(flavor = "current_thread")]
    /// async fn main() -> segment::Result<()> {
    ///     let client = HttpClient::default();
    ///     let batcher = Batcher::new(None);
    ///     let mut batcher = AutoBatcher::new(client, batcher, "your_write_key");
    ///
    ///     let msg = Track {
    ///         user: User::UserId { user_id: String::from("user") },
    ///         event: "Example".to_owned(),
    ///         properties: json!({ "foo": "bar" }),
    ///         ..Default::default()
    ///     };
    ///
    ///     batcher.push(msg).await?;
    ///     batcher.flush().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn push(&mut self, msg: impl Into<BatchMessage>) -> Result<()> {
        if self
            .batcher
            .context
            .as_ref()
            .is_some_and(|value| !value.is_object())
        {
            return Err(ValidationError::new("context", "must be a JSON object").into());
        }

        let prepared = PreparedBatchMessage::new(msg.into(), self.batcher.auto_timestamp)?;
        self.push_prepared(prepared).await
    }

    pub(crate) async fn push_prepared(&mut self, prepared: PreparedBatchMessage) -> Result<()> {
        if !self.pending.is_empty() {
            if self.batcher.empty_batch_size + prepared.serialized_size > MAX_BATCH_SIZE {
                return Err(Error::BatchTooLarge);
            }
            self.pending.push_back(prepared);
            return self.flush().await;
        }

        if let Some(prepared) = self.batcher.push_prepared(prepared)? {
            if let Err(error) = self.flush_current_batch().await {
                self.pending.push_back(prepared);
                return Err(error);
            }

            self.batcher.push_prepared(prepared)?;
        }

        Ok(())
    }

    /// Send all the message currently contained in the batcher, full or empty.
    ///
    /// Returns an error if the message is too large to be sent to Segment's
    /// API.
    /// ```no_run
    /// use serde_json::json;
    /// use segment::{AutoBatcher, Batcher, HttpClient};
    /// use segment::message::{Track, User};
    ///
    /// #[tokio::main(flavor = "current_thread")]
    /// async fn main() -> segment::Result<()> {
    ///     let client = HttpClient::default();
    ///     let batcher = Batcher::new(None);
    ///     let mut batcher = AutoBatcher::new(client, batcher, "your_write_key");
    ///
    ///     let msg = Track {
    ///         user: User::UserId { user_id: String::from("user") },
    ///         event: "Example".to_owned(),
    ///         properties: json!({ "foo": "bar" }),
    ///         ..Default::default()
    ///     };
    ///
    ///     batcher.push(msg).await?;
    ///     batcher.flush().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn flush(&mut self) -> Result<()> {
        self.flush_current_batch().await?;

        while let Some(prepared) = self.pending.pop_front() {
            if let Some(prepared) = self
                .batcher
                .push_prepared(prepared)
                .expect("pending messages were validated before being queued")
            {
                self.pending.push_front(prepared);
                self.flush_current_batch().await?;
            }
        }

        self.flush_current_batch().await
    }

    async fn flush_current_batch(&mut self) -> Result<()> {
        if self.batcher.is_empty() {
            return Ok(());
        }

        let (buffered, byte_count) = self.batcher.take_with_size();

        let message = Message::Batch(Batch {
            batch: buffered,
            sent_at: Some(jiff::Timestamp::now()),
            context: self.batcher.context.clone(),
            integrations: None,
            extra: Map::default(),
        });

        match self.client.send(&self.key, &message).await {
            Ok(()) => Ok(()),
            Err(error) => {
                if let Message::Batch(batch) = message {
                    self.batcher.restore_with_size(batch.batch, byte_count);
                }
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    use serde_json::json;

    use super::*;
    use crate::message::{Track, User};

    #[derive(Clone, Debug)]
    struct RecordingClient {
        sent: Arc<Mutex<Vec<(String, Message)>>>,
    }

    impl Client for RecordingClient {
        fn send<'a>(
            &'a self,
            write_key: &'a str,
            msg: &'a Message,
        ) -> impl Future<Output = Result<()>> + Send + 'a {
            self.sent
                .lock()
                .expect("recording client lock was poisoned")
                .push((write_key.to_owned(), msg.clone()));
            std::future::ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn flush_uses_custom_client() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let client = RecordingClient {
            sent: Arc::clone(&sent),
        };
        let mut batcher = AutoBatcher::new(client, Batcher::new(None), "write-key");

        batcher
            .push(Track {
                user: User::UserId {
                    user_id: "user".to_owned(),
                },
                event: "Example".to_owned(),
                properties: json!({ "foo": "bar" }),
                ..Default::default()
            })
            .await
            .expect("message should be accepted");
        batcher.flush().await.expect("batch should flush");

        let sent = sent.lock().expect("recording client lock was poisoned");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "write-key");
        let Message::Batch(batch) = &sent[0].1 else {
            panic!("expected a batch message");
        };
        assert_eq!(batch.batch.len(), 1);
    }

    #[derive(Clone, Debug)]
    struct FailOnceClient {
        failed: Arc<AtomicBool>,
        sent: Arc<Mutex<Vec<Message>>>,
    }

    impl Client for FailOnceClient {
        fn send<'a>(
            &'a self,
            _write_key: &'a str,
            msg: &'a Message,
        ) -> impl Future<Output = Result<()>> + Send + 'a {
            let result = if self.failed.swap(true, Ordering::SeqCst) {
                self.sent
                    .lock()
                    .expect("recording client lock was poisoned")
                    .push(msg.clone());
                Ok(())
            } else {
                Err(crate::Error::MessageTooLarge)
            };
            std::future::ready(result)
        }
    }

    #[tokio::test]
    async fn failed_flush_retains_messages_for_a_retry() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let client = FailOnceClient {
            failed: Arc::new(AtomicBool::new(false)),
            sent: Arc::clone(&sent),
        };
        let mut batcher = AutoBatcher::new(client, Batcher::new(None), "write-key");

        batcher
            .push(Track {
                user: User::UserId {
                    user_id: "user".to_owned(),
                },
                event: "Example".to_owned(),
                properties: json!({}),
                ..Default::default()
            })
            .await
            .expect("message should be queued");

        batcher
            .flush()
            .await
            .expect_err("the first delivery should fail");
        assert_eq!(batcher.batcher.buf.len(), 1);

        batcher.flush().await.expect("the retry should succeed");
        assert!(batcher.batcher.is_empty());
        assert_eq!(sent.lock().expect("lock should not be poisoned").len(), 1);
    }

    #[tokio::test]
    async fn failed_rollover_retains_the_overflowing_message() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let client = FailOnceClient {
            failed: Arc::new(AtomicBool::new(false)),
            sent: Arc::clone(&sent),
        };
        let mut batcher = AutoBatcher::new(client, Batcher::new(None), "write-key");

        let message = Track {
            user: User::UserId {
                user_id: "user".to_owned(),
            },
            event: "Example".to_owned(),
            properties: json!({ "padding": "a".repeat(30_000) }),
            ..Default::default()
        };

        let mut accepted = 0;
        loop {
            match batcher.push(message.clone()).await {
                Ok(()) => accepted += 1,
                Err(crate::Error::MessageTooLarge) => break,
                Err(error) => panic!("unexpected error: {error}"),
            }
        }

        assert_eq!(batcher.pending.len(), 1);
        assert!(!batcher.batcher.is_empty());

        batcher
            .flush()
            .await
            .expect("retry should deliver everything");
        let sent = sent.lock().expect("lock should not be poisoned");
        let delivered = sent
            .iter()
            .map(|message| match message {
                Message::Batch(batch) => batch.batch.len(),
                _ => panic!("expected batch message"),
            })
            .sum::<usize>();
        assert_eq!(delivered, accepted + 1);
    }

    #[test]
    fn debug_redacts_write_key() {
        let client = RecordingClient {
            sent: Arc::new(Mutex::new(Vec::new())),
        };
        let batcher = AutoBatcher::new(client, Batcher::new(None), "secret-write-key");
        let debug = format!("{batcher:?}");
        assert!(debug.contains("[redacted]"));
        assert!(!debug.contains("secret-write-key"));
    }
}
