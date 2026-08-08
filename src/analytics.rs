//! High-level asynchronous Segment client with bounded background batching.

use std::{sync::Arc, time::Duration};

use jiff::Timestamp;
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::{
    AutoBatcher, Batcher, Client, Error, HttpClient, Result, ValidationError,
    batcher::{MAX_BATCH_SIZE, MAX_MESSAGE_SIZE},
    message::{Alias, BatchMessage, Group, Identify, Page, Screen, Track},
};

const DEFAULT_FLUSH_AT: usize = 20;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_MAX_QUEUE_SIZE: usize = 10_000;
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

/// What to do when the bounded background queue is full.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Wait until the worker has room. This is lossless and provides
    /// backpressure to producers.
    #[default]
    Block,
    /// Reject the new message immediately with [`Error::QueueFull`].
    Reject,
}

/// Builder for a high-level [`Analytics`] client.
#[derive(Debug)]
pub struct AnalyticsBuilder<C = HttpClient> {
    client: C,
    write_key: String,
    flush_at: usize,
    flush_interval: Duration,
    max_queue_size: usize,
    overflow_policy: OverflowPolicy,
    batch_context: Option<Value>,
    auto_timestamp: bool,
}

impl AnalyticsBuilder<HttpClient> {
    /// Start configuring a client using the default HTTP transport.
    pub fn new(write_key: impl Into<String>) -> Self {
        Self::with_client(write_key, HttpClient::default())
    }
}

impl<C> AnalyticsBuilder<C> {
    /// Start configuring a client using a custom transport.
    pub fn with_client(write_key: impl Into<String>, client: C) -> Self {
        Self {
            client,
            write_key: write_key.into(),
            flush_at: DEFAULT_FLUSH_AT,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            max_queue_size: DEFAULT_MAX_QUEUE_SIZE,
            overflow_policy: OverflowPolicy::Block,
            batch_context: None,
            auto_timestamp: true,
        }
    }

    /// Flush after this many accepted messages.
    pub fn flush_at(mut self, flush_at: usize) -> Self {
        self.flush_at = flush_at;
        self
    }

    /// Flush queued messages at least this often.
    pub fn flush_interval(mut self, flush_interval: Duration) -> Self {
        self.flush_interval = flush_interval;
        self
    }

    /// Bound the number of commands waiting for the delivery worker.
    pub fn max_queue_size(mut self, max_queue_size: usize) -> Self {
        self.max_queue_size = max_queue_size;
        self
    }

    /// Choose whether producers wait or fail when the queue is full.
    pub fn overflow_policy(mut self, overflow_policy: OverflowPolicy) -> Self {
        self.overflow_policy = overflow_policy;
        self
    }

    /// Context merged by Segment into every event in a batch.
    pub fn batch_context(mut self, context: Value) -> Self {
        self.batch_context = Some(context);
        self
    }

    /// Control whether missing event timestamps are filled at enqueue time.
    pub fn auto_timestamp(mut self, enabled: bool) -> Self {
        self.auto_timestamp = enabled;
        self
    }

    /// Spawn the background delivery worker on the active Tokio runtime.
    pub fn build(self) -> Result<Analytics>
    where
        C: Client + Send + Sync + 'static,
    {
        if self.write_key.trim().is_empty() {
            return Err(ValidationError::new("writeKey", "must not be empty").into());
        }
        if self.flush_at == 0 {
            return Err(ValidationError::new("flushAt", "must be greater than zero").into());
        }
        if self.flush_interval.is_zero() {
            return Err(ValidationError::new("flushInterval", "must be greater than zero").into());
        }
        if self.max_queue_size == 0 {
            return Err(ValidationError::new("maxQueueSize", "must be greater than zero").into());
        }
        if self
            .batch_context
            .as_ref()
            .is_some_and(|context| !context.is_object())
        {
            return Err(ValidationError::new("context", "must be a JSON object").into());
        }

        let mut batcher = Batcher::new(self.batch_context);
        if batcher.empty_batch_size >= MAX_BATCH_SIZE {
            return Err(Error::BatchTooLarge);
        }
        if !self.auto_timestamp {
            batcher.without_auto_timestamp();
        }
        let empty_batch_size = batcher.empty_batch_size;

        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| Error::RuntimeUnavailable)?;
        let (sender, receiver) = mpsc::channel(self.max_queue_size);
        let worker = AutoBatcher::new(self.client, batcher, self.write_key);
        runtime.spawn(run_worker(
            receiver,
            worker,
            self.flush_at,
            self.flush_interval,
        ));

        Ok(Analytics {
            inner: Arc::new(Inner {
                sender,
                state: Mutex::new(State::Open),
                overflow_policy: self.overflow_policy,
                auto_timestamp: self.auto_timestamp,
                empty_batch_size,
            }),
        })
    }
}

/// A cloneable Segment client that queues and batches messages in the
/// background.
#[derive(Debug)]
pub struct Analytics {
    inner: Arc<Inner>,
}

impl Clone for Analytics {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Analytics {
    /// Configure a client using the default HTTP transport.
    pub fn builder(write_key: impl Into<String>) -> AnalyticsBuilder<HttpClient> {
        AnalyticsBuilder::new(write_key)
    }
    /// Configure a client using a custom transport.
    pub fn builder_with_client<C>(write_key: impl Into<String>, client: C) -> AnalyticsBuilder<C> {
        AnalyticsBuilder::with_client(write_key, client)
    }

    /// Queue a message for background delivery.
    ///
    /// Success acknowledges acceptance into the local queue, not delivery to
    /// Segment. Call [`Self::flush`] when a delivery acknowledgement is needed.
    pub async fn enqueue(&self, message: impl Into<BatchMessage>) -> Result<()> {
        let mut message = message.into();
        if self.inner.auto_timestamp && message.timestamp_mut().is_none() {
            *message.timestamp_mut() = Some(Timestamp::now());
        }
        message.prepare()?;
        let message_size = serde_json::to_vec(&message)?.len();
        if message_size > MAX_MESSAGE_SIZE {
            return Err(Error::MessageTooLarge);
        }
        if self.inner.empty_batch_size + message_size > MAX_BATCH_SIZE {
            return Err(Error::BatchTooLarge);
        }

        let state = self.inner.state.lock().await;
        if *state != State::Open {
            return Err(Error::ClientClosed);
        }

        match self.inner.overflow_policy {
            OverflowPolicy::Block => self
                .inner
                .sender
                .send(Command::Event(Box::new(message)))
                .await
                .map_err(|_| Error::WorkerStopped),
            OverflowPolicy::Reject => self
                .inner
                .sender
                .try_send(Command::Event(Box::new(message)))
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => Error::QueueFull,
                    mpsc::error::TrySendError::Closed(_) => Error::WorkerStopped,
                }),
        }
    }

    pub async fn identify(&self, message: Identify) -> Result<()> {
        self.enqueue(message).await
    }

    pub async fn track(&self, message: Track) -> Result<()> {
        self.enqueue(message).await
    }

    pub async fn page(&self, message: Page) -> Result<()> {
        self.enqueue(message).await
    }

    pub async fn screen(&self, message: Screen) -> Result<()> {
        self.enqueue(message).await
    }

    pub async fn group(&self, message: Group) -> Result<()> {
        self.enqueue(message).await
    }

    pub async fn alias(&self, message: Alias) -> Result<()> {
        self.enqueue(message).await
    }

    /// Deliver every message accepted before this call.
    pub async fn flush(&self) -> Result<()> {
        self.flush_inner(None).await
    }

    /// Deliver every message accepted before this call, bounded by a timeout.
    pub async fn flush_with_timeout(&self, timeout: Duration) -> Result<()> {
        self.flush_inner(Some(timeout)).await
    }

    /// Stop accepting messages and drain the queue.
    pub async fn close_and_flush(&self) -> Result<()> {
        self.close_and_flush_with_timeout(DEFAULT_CLOSE_TIMEOUT)
            .await
    }

    /// Stop accepting messages and drain the queue within `timeout`.
    ///
    /// A failed or timed-out drain reopens the client so the caller can retry.
    pub async fn close_and_flush_with_timeout(&self, timeout: Duration) -> Result<()> {
        let receiver = {
            let mut state = self.inner.state.lock().await;
            if *state != State::Open {
                return Err(Error::ClientClosed);
            }
            *state = State::Closing;
            let (sender, receiver) = oneshot::channel();
            if self
                .inner
                .sender
                .send(Command::Flush(sender))
                .await
                .is_err()
            {
                *state = State::Open;
                return Err(Error::WorkerStopped);
            }
            receiver
        };

        let result = match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Error::WorkerStopped),
            Err(_) => Err(Error::FlushTimeout(timeout)),
        };

        if let Err(error) = result {
            *self.inner.state.lock().await = State::Open;
            return Err(error);
        }

        let (sender, receiver) = oneshot::channel();
        self.inner
            .sender
            .send(Command::Shutdown(sender))
            .await
            .map_err(|_| Error::WorkerStopped)?;
        receiver.await.map_err(|_| Error::WorkerStopped)?;
        *self.inner.state.lock().await = State::Closed;
        Ok(())
    }

    async fn flush_inner(&self, timeout: Option<Duration>) -> Result<()> {
        let receiver = {
            let state = self.inner.state.lock().await;
            if *state != State::Open {
                return Err(Error::ClientClosed);
            }
            let (sender, receiver) = oneshot::channel();
            self.inner
                .sender
                .send(Command::Flush(sender))
                .await
                .map_err(|_| Error::WorkerStopped)?;
            receiver
        };

        match timeout {
            Some(timeout) => match tokio::time::timeout(timeout, receiver).await {
                Ok(result) => result.map_err(|_| Error::WorkerStopped)?,
                Err(_) => Err(Error::FlushTimeout(timeout)),
            },
            None => receiver.await.map_err(|_| Error::WorkerStopped)?,
        }
    }
}

#[derive(Debug)]
struct Inner {
    sender: mpsc::Sender<Command>,
    state: Mutex<State>,
    overflow_policy: OverflowPolicy,
    auto_timestamp: bool,
    empty_batch_size: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Open,
    Closing,
    Closed,
}

enum Command {
    Event(Box<BatchMessage>),
    Flush(oneshot::Sender<Result<()>>),
    Shutdown(oneshot::Sender<()>),
}

async fn run_worker<C>(
    mut receiver: mpsc::Receiver<Command>,
    mut batcher: AutoBatcher<C>,
    flush_at: usize,
    flush_interval: Duration,
) where
    C: Client + Send + Sync + 'static,
{
    let timer = tokio::time::sleep(flush_interval);
    tokio::pin!(timer);
    let mut accepted_since_flush = 0;

    loop {
        tokio::select! {
            command = receiver.recv() => match command {
                Some(Command::Event(message)) => {
                    accepted_since_flush += 1;
                    let result = batcher.push(*message).await;
                    if result.is_ok()
                        && accepted_since_flush >= flush_at
                        && batcher.flush().await.is_ok()
                    {
                        accepted_since_flush = 0;
                    }
                }
                Some(Command::Flush(sender)) => {
                    let result = batcher.flush().await;
                    if result.is_ok() {
                        accepted_since_flush = 0;
                    }
                    let _ = sender.send(result);
                }
                Some(Command::Shutdown(sender)) => {
                    let _ = sender.send(());
                    break;
                }
                None => {
                    let _ = batcher.flush().await;
                    break;
                }
            },
            () = &mut timer => {
                if batcher.flush().await.is_ok() {
                    accepted_since_flush = 0;
                }
                timer.as_mut().reset(tokio::time::Instant::now() + flush_interval);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    };

    use serde_json::json;
    use tokio::sync::Notify;

    use super::*;
    use crate::{Message, message::User};

    #[derive(Clone, Debug)]
    struct RecordingClient {
        sent: Arc<StdMutex<Vec<Message>>>,
        delivered: Arc<Notify>,
    }

    impl Client for RecordingClient {
        fn send<'a>(
            &'a self,
            _write_key: &'a str,
            message: Message,
        ) -> impl std::future::Future<Output = Result<()>> + Send + 'a {
            self.sent
                .lock()
                .expect("recording client lock was poisoned")
                .push(message);
            self.delivered.notify_one();
            std::future::ready(Ok(()))
        }
    }

    #[derive(Clone, Debug)]
    struct BlockingOnceClient {
        sent: Arc<StdMutex<Vec<Message>>>,
        blocked: Arc<AtomicBool>,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Client for BlockingOnceClient {
        async fn send<'a>(&'a self, _write_key: &'a str, message: Message) -> Result<()> {
            self.sent
                .lock()
                .expect("recording client lock was poisoned")
                .push(message);
            if !self.blocked.swap(true, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(())
        }
    }

    fn track(event: &str) -> Track {
        Track {
            user: User::UserId {
                user_id: "user".to_owned(),
            },
            event: event.to_owned(),
            properties: json!({}),
            ..Default::default()
        }
    }

    fn delivered_message_count(messages: &[Message]) -> usize {
        messages
            .iter()
            .map(|message| match message {
                Message::Batch(batch) => batch.batch.len(),
                _ => panic!("background client should send batches"),
            })
            .sum()
    }

    #[tokio::test]
    async fn count_flush_delivers_and_close_rejects_late_events() {
        let sent = Arc::new(StdMutex::new(Vec::new()));
        let client = RecordingClient {
            sent: Arc::clone(&sent),
            delivered: Arc::new(Notify::new()),
        };
        let analytics = Analytics::builder_with_client("write-key", client)
            .flush_at(2)
            .flush_interval(Duration::from_secs(60))
            .build()
            .expect("client should build");

        analytics
            .track(track("One"))
            .await
            .expect("event should queue");
        analytics
            .track(track("Two"))
            .await
            .expect("event should queue");
        analytics.flush().await.expect("flush should succeed");

        assert_eq!(
            delivered_message_count(&sent.lock().expect("lock should not be poisoned")),
            2
        );

        analytics
            .close_and_flush()
            .await
            .expect("close should succeed");
        assert!(matches!(
            analytics.track(track("Late")).await,
            Err(Error::ClientClosed)
        ));
    }

    #[tokio::test]
    async fn interval_flush_delivers_low_volume_events() {
        let sent = Arc::new(StdMutex::new(Vec::new()));
        let delivered = Arc::new(Notify::new());
        let client = RecordingClient {
            sent: Arc::clone(&sent),
            delivered: Arc::clone(&delivered),
        };
        let analytics = Analytics::builder_with_client("write-key", client)
            .flush_at(100)
            .flush_interval(Duration::from_millis(10))
            .build()
            .expect("client should build");

        analytics
            .track(track("Interval"))
            .await
            .expect("event should queue");
        tokio::time::timeout(Duration::from_secs(1), delivered.notified())
            .await
            .expect("timer should flush the event");

        assert_eq!(
            delivered_message_count(&sent.lock().expect("lock should not be poisoned")),
            1
        );
        analytics
            .close_and_flush()
            .await
            .expect("close should succeed");
    }

    #[tokio::test]
    async fn reject_policy_reports_a_full_queue() {
        let client = BlockingOnceClient {
            sent: Arc::new(StdMutex::new(Vec::new())),
            blocked: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        };
        let entered = Arc::clone(&client.entered);
        let release = Arc::clone(&client.release);
        let analytics = Analytics::builder_with_client("write-key", client)
            .flush_at(1)
            .flush_interval(Duration::from_secs(60))
            .max_queue_size(1)
            .overflow_policy(OverflowPolicy::Reject)
            .build()
            .expect("client should build");

        analytics
            .track(track("One"))
            .await
            .expect("event should queue");
        entered.notified().await;
        analytics
            .track(track("Two"))
            .await
            .expect("event should queue");
        assert!(matches!(
            analytics.track(track("Three")).await,
            Err(Error::QueueFull)
        ));

        release.notify_one();
        analytics
            .close_and_flush()
            .await
            .expect("close should drain accepted events");
    }

    #[tokio::test]
    async fn timed_out_close_reopens_the_client_for_retry() {
        let sent = Arc::new(StdMutex::new(Vec::new()));
        let client = BlockingOnceClient {
            sent: Arc::clone(&sent),
            blocked: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        };
        let release = Arc::clone(&client.release);
        let analytics = Analytics::builder_with_client("write-key", client)
            .flush_at(100)
            .flush_interval(Duration::from_secs(60))
            .build()
            .expect("client should build");

        analytics
            .track(track("One"))
            .await
            .expect("event should queue");
        assert!(matches!(
            analytics
                .close_and_flush_with_timeout(Duration::from_millis(10))
                .await,
            Err(Error::FlushTimeout(_))
        ));

        release.notify_one();
        analytics.flush().await.expect("retry should succeed");
        analytics
            .close_and_flush()
            .await
            .expect("client should close after retry");
        assert_eq!(
            delivered_message_count(&sent.lock().expect("lock should not be poisoned")),
            1
        );
    }
}
