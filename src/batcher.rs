//! Utilities for batching up messages.

use crate::message::{Batch, BatchMessage, Message};
use crate::{Error, Result};
use jiff::Timestamp;
use serde_json::{Map, Value};

const MAX_MESSAGE_SIZE: usize = 1024 * 32;
const MAX_BATCH_SIZE: usize = 500_000;
const MAX_BATCH_MESSAGES: usize = 2_500;

/// A batcher can accept messages into an internal buffer, and report when
/// messages must be flushed.
///
/// The recommended usage pattern looks something like this:
///
/// ```no_run
/// use segment::{Batcher, Client, HttpClient};
/// use segment::message::{Track, User};
/// use serde_json::json;
///
/// #[tokio::main(flavor = "current_thread")]
/// async fn main() -> segment::Result<()> {
///     let mut batcher = Batcher::new(None);
///     let client = HttpClient::default();
///     let write_key = "your_write_key";
///
///     for i in 0..100 {
///         let msg = Track {
///             user: User::UserId { user_id: format!("user-{}", i) },
///             event: "Example".to_owned(),
///             properties: json!({ "foo": "bar" }),
///             ..Default::default()
///         };
///
///         // Batcher returns back ownership of a message if the internal buffer
///         // would overflow.
///         //
///         // When this occurs, we flush the batcher, create a new batcher, and add
///         // the message into the new batcher.
///         if let Some(msg) = batcher.push(msg)? {
///             client.send(write_key, batcher.into_message()).await?;
///             batcher = Batcher::new(None);
///             batcher.push(msg)?;
///         }
///     }
///
///     if !batcher.is_empty() {
///         client.send(write_key, batcher.into_message()).await?;
///     }
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
/// the batcher on your own by calling `into_message`.
///
/// By default if the message you push in the batcher does not contain any
/// timestamp, the timestamp at the time of the push will be automatically
/// added to your message.
/// You can disable this behaviour with the [`Self::without_auto_timestamp`] method
/// though.
#[derive(Clone, Debug)]
pub struct Batcher {
    pub(crate) buf: Vec<BatchMessage>,
    /// Exact serialized size of the batch envelope and its buffered messages.
    pub(crate) byte_count: usize,
    pub(crate) empty_batch_size: usize,
    pub(crate) context: Option<Value>,
    pub(crate) auto_timestamp: bool,
}

impl Batcher {
    /// Construct a new, empty batcher.
    ///
    /// Optionally, you may specify a `context` that should be set on every
    /// batch returned by `into_message`.
    pub fn new(context: Option<Value>) -> Self {
        let empty_batch_size = serialized_batch_size(&[], context.as_ref());
        Self {
            buf: Vec::new(),
            byte_count: empty_batch_size,
            empty_batch_size,
            context,
            auto_timestamp: true,
        }
    }

    pub fn without_auto_timestamp(&mut self) {
        self.auto_timestamp = false;
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Push a message into the batcher.
    ///
    /// Returns `Ok(None)` if the message was accepted and is now owned by the
    /// batcher.
    ///
    /// Returns `Ok(Some(msg))` if the message was rejected because the current
    /// batch would be oversized if this message were accepted. The given
    /// message is returned back, and it is recommended that you flush the
    /// current batch before attempting to push `msg` in again.
    ///
    /// Returns an error if the message is too large to be sent to Segment's
    /// API.
    pub fn push(&mut self, msg: impl Into<BatchMessage>) -> Result<Option<BatchMessage>> {
        let mut msg: BatchMessage = msg.into();
        let timestamp = msg.timestamp_mut();
        if self.auto_timestamp && timestamp.is_none() {
            *timestamp = Some(Timestamp::now());
        }
        let size = serde_json::to_vec(&msg)?.len();
        if size > MAX_MESSAGE_SIZE {
            return Err(Error::MessageTooLarge);
        }

        let separator_size = usize::from(!self.buf.is_empty());
        let byte_count = self.byte_count + separator_size + size;
        if self.buf.len() == MAX_BATCH_MESSAGES || byte_count > MAX_BATCH_SIZE {
            if self.buf.is_empty() {
                return Err(Error::BatchTooLarge);
            }
            return Ok(Some(msg));
        }

        self.byte_count = byte_count;
        self.buf.push(msg);
        Ok(None)
    }

    pub(crate) fn take(&mut self) -> Vec<BatchMessage> {
        self.byte_count = self.empty_batch_size;
        std::mem::take(&mut self.buf)
    }

    pub(crate) fn restore(&mut self, messages: Vec<BatchMessage>) {
        self.byte_count = serialized_batch_size(&messages, self.context.as_ref());
        self.buf = messages;
    }

    /// Consumes this batcher and converts it into a message that can be sent to
    /// Segment.
    pub fn into_message(self) -> Message {
        Message::Batch(Batch {
            batch: self.buf,
            context: self.context,
            integrations: None,
            extra: Map::default(),
        })
    }
}

fn serialized_batch_size(messages: &[BatchMessage], context: Option<&Value>) -> usize {
    serde_json::to_vec(&Batch {
        batch: messages.to_vec(),
        context: context.cloned(),
        integrations: None,
        extra: Map::default(),
    })
    .expect("Segment messages backed by serde_json::Value must serialize")
    .len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Track, User};
    use serde_json::json;

    #[test]
    fn test_push_and_into() {
        let batch_msg = BatchMessage::Track(Track {
            ..Default::default()
        });

        let context = json!({
            "foo": "bar",
        });

        let mut batcher = Batcher::new(Some(context.clone()));
        batcher.without_auto_timestamp();
        let result = batcher.push(batch_msg.clone());
        assert_eq!(None, result.expect("message should be accepted"));

        let batch = batcher.into_message();
        let inner_batch = match batch {
            Message::Batch(b) => b,
            _ => panic!("invalid message type"),
        };
        assert_eq!(context, inner_batch.context.expect("context should be set"));
        assert_eq!(1, inner_batch.batch.len());

        assert_eq!(inner_batch.batch, vec![batch_msg]);
    }

    #[test]
    fn test_push_adds_timestamp() {
        let mut batcher = Batcher::new(None);
        batcher
            .push(Track::default())
            .expect("message should be accepted");

        let BatchMessage::Track(track) = &batcher.buf[0] else {
            panic!("pushed message should be a track event");
        };
        assert!(track.timestamp.is_some());
    }

    #[test]
    fn test_bad_message_size() {
        let batch_msg = Track {
            user: User::UserId {
                user_id: String::from_utf8(vec![b'a'; 1024 * 33])
                    .expect("test data should be valid UTF-8"),
            },
            ..Default::default()
        };

        let mut batcher = Batcher::new(None);
        let result = batcher.push(batch_msg);

        let err = result.expect_err("message should be too large");
        assert!(err.to_string().contains("message too large"));
    }

    #[test]
    fn test_max_buffer() {
        let batch_msg = Track {
            user: User::UserId {
                user_id: String::from_utf8(vec![b'a'; 1024 * 30])
                    .expect("test data should be valid UTF-8"),
            },
            ..Default::default()
        };

        let mut batcher = Batcher::new(None);
        batcher.without_auto_timestamp();
        let mut result = Ok(None);
        for _i in 0..20 {
            result = batcher.push(batch_msg.clone());
            if result
                .as_ref()
                .expect("message should not be individually too large")
                .is_some()
            {
                break;
            }
        }

        let msg = result
            .expect("message should not be individually too large")
            .expect("message should be returned when the batch is full");
        assert_eq!(BatchMessage::from(batch_msg), msg);
    }

    #[test]
    fn rejected_message_does_not_change_byte_count() {
        let batch_msg = Track {
            user: User::UserId {
                user_id: String::from_utf8(vec![b'a'; 1024 * 30])
                    .expect("test data should be valid UTF-8"),
            },
            ..Default::default()
        };

        let mut batcher = Batcher::new(None);
        batcher.without_auto_timestamp();

        loop {
            let byte_count = batcher.byte_count;
            let result = batcher
                .push(batch_msg.clone())
                .expect("message should not be individually too large");
            if result.is_some() {
                assert_eq!(batcher.byte_count, byte_count);
                break;
            }
        }
    }

    #[test]
    fn byte_count_includes_the_complete_batch_envelope() {
        let context = json!({ "source": "batch-test" });
        let mut batcher = Batcher::new(Some(context));
        batcher.without_auto_timestamp();
        batcher
            .push(Track {
                user: User::UserId {
                    user_id: "user".to_owned(),
                },
                event: "Example".to_owned(),
                properties: json!({ "key": "value" }),
                ..Default::default()
            })
            .expect("message should fit");

        let actual_size = serde_json::to_vec(&batcher.clone().into_message())
            .expect("batch should serialize")
            .len();
        assert_eq!(batcher.byte_count, actual_size);
    }

    #[test]
    fn oversized_batch_context_rejects_the_first_message() {
        let context = json!({ "padding": "a".repeat(MAX_BATCH_SIZE) });
        let mut batcher = Batcher::new(Some(context));

        let error = batcher
            .push(Track::default())
            .expect_err("an oversized envelope can never be flushed successfully");

        assert!(matches!(error, Error::BatchTooLarge));
        assert!(batcher.is_empty());
    }

    #[test]
    fn batch_message_count_is_limited() {
        let mut batcher = Batcher::new(None);
        batcher.without_auto_timestamp();

        for _ in 0..MAX_BATCH_MESSAGES {
            assert_eq!(
                batcher
                    .push(Track::default())
                    .expect("small message should be valid"),
                None
            );
        }

        assert!(
            batcher
                .push(Track::default())
                .expect("small message should be valid")
                .is_some()
        );
        assert_eq!(batcher.buf.len(), MAX_BATCH_MESSAGES);
    }
}
