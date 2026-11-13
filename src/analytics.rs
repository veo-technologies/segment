//! High-level asynchronous Segment client with bounded background batching.

use std::{fmt, sync::Arc, time::Duration};

use serde_json::Value;
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};

use crate::{
    AutoBatcher, Batcher, Client, Error, HttpClient, Result, ValidationError,
    batcher::{MAX_BATCH_SIZE, PreparedBatchMessage},
    message::{Alias, BatchMessage, Group, Identify, Page, Screen, Track},
};

const DEFAULT_FLUSH_AT: usize = 20;
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_MAX_QUEUE_SIZE: usize = 10_000;
const DEFAULT_EVENT_BUFFER_SIZE: usize = 1_024;
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

/// Why the delivery worker attempted to flush buffered messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlushReason {
    /// The serialized Tracking API batch reached its capacity.
    Capacity,
    /// The configured message-count threshold was reached.
    MessageCount,
    /// The configured flush interval elapsed.
    Interval,
    /// The application explicitly requested a flush.
    Manual,
    /// The application is draining the client during shutdown.
    Shutdown,
}

/// A non-blocking lifecycle event emitted by [`Analytics`].
///
/// Subscribe with [`Analytics::subscribe`]. Slow subscribers can lag and lose
/// events, but they never delay event delivery to Segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnalyticsEvent {
    /// A message was accepted into the bounded worker queue.
    Enqueued {
        message_id: String,
        message_type: &'static str,
    },
    /// A middleware intentionally filtered a message out.
    Dropped { message_type: &'static str },
    /// One or more messages were delivered successfully.
    Delivered {
        message_count: usize,
        reason: FlushReason,
    },
    /// A delivery failed. The loss-safe batch remains available for a retry.
    DeliveryFailed {
        message_count: usize,
        reason: FlushReason,
        status: Option<reqwest::StatusCode>,
        retryable: bool,
        error: String,
    },
    /// A message was rejected by the configured queue overflow policy.
    QueueFull { message_type: &'static str },
    /// The background client drained and stopped.
    Closed,
}

/// An ordered message transformation applied before validation and enqueueing.
///
/// Returning `Ok(None)` intentionally drops a message. Returning an error
/// rejects it without changing the queue. Middleware runs in the producer task,
/// so expensive work should be performed before calling [`Analytics::enqueue`].
pub trait Middleware: Send + Sync {
    fn process(&self, message: BatchMessage) -> Result<Option<BatchMessage>>;
}

impl<F> Middleware for F
where
    F: Fn(BatchMessage) -> Result<Option<BatchMessage>> + Send + Sync,
{
    fn process(&self, message: BatchMessage) -> Result<Option<BatchMessage>> {
        self(message)
    }
}

/// Builder for a high-level [`Analytics`] client.
pub struct AnalyticsBuilder<C = HttpClient> {
    client: C,
    write_key: String,
    flush_at: usize,
    flush_interval: Duration,
    max_queue_size: usize,
    overflow_policy: OverflowPolicy,
    batch_context: Option<Value>,
    auto_timestamp: bool,
    middleware: Vec<Arc<dyn Middleware>>,
    event_buffer_size: usize,
}

impl<C: fmt::Debug> fmt::Debug for AnalyticsBuilder<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnalyticsBuilder")
            .field("client", &self.client)
            .field("write_key", &"[redacted]")
            .field("flush_at", &self.flush_at)
            .field("flush_interval", &self.flush_interval)
            .field("max_queue_size", &self.max_queue_size)
            .field("overflow_policy", &self.overflow_policy)
            .field("batch_context", &self.batch_context)
            .field("auto_timestamp", &self.auto_timestamp)
            .field("middleware_count", &self.middleware.len())
            .field("event_buffer_size", &self.event_buffer_size)
            .finish()
    }
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
            middleware: Vec::new(),
            event_buffer_size: DEFAULT_EVENT_BUFFER_SIZE,
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

    /// Add an ordered message enrichment or filtering step.
    pub fn middleware(mut self, middleware: impl Middleware + 'static) -> Self {
        self.middleware.push(Arc::new(middleware));
        self
    }

    /// Bound each lifecycle-event subscriber's in-memory buffer.
    pub fn event_buffer_size(mut self, event_buffer_size: usize) -> Self {
        self.event_buffer_size = event_buffer_size;
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
        if self.event_buffer_size == 0 {
            return Err(
                ValidationError::new("eventBufferSize", "must be greater than zero").into(),
            );
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
        let (events, _) = broadcast::channel(self.event_buffer_size);
        let worker = AutoBatcher::new(self.client, batcher, self.write_key);
        runtime.spawn(run_worker(
            receiver,
            worker,
            self.flush_at,
            self.flush_interval,
            events.clone(),
        ));

        Ok(Analytics {
            inner: Arc::new(Inner {
                sender,
                state: Mutex::new(State::Open),
                overflow_policy: self.overflow_policy,
                auto_timestamp: self.auto_timestamp,
                empty_batch_size,
                middleware: self.middleware,
                events,
            }),
        })
    }
}

/// A cloneable Segment client that queues and batches messages in the
/// background.
pub struct Analytics {
    inner: Arc<Inner>,
}

impl fmt::Debug for Analytics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Analytics").finish_non_exhaustive()
    }
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

    /// Subscribe to delivery lifecycle events.
    ///
    /// The returned broadcast receiver reports [`broadcast::error::RecvError::Lagged`]
    /// if the subscriber cannot keep up with the configured event buffer.
    pub fn subscribe(&self) -> broadcast::Receiver<AnalyticsEvent> {
        self.inner.events.subscribe()
    }

    /// Queue a message for background delivery.
    ///
    /// Success acknowledges acceptance into the local queue, not delivery to
    /// Segment. Call [`Self::flush`] when a delivery acknowledgement is needed.
    pub async fn enqueue(&self, message: impl Into<BatchMessage>) -> Result<()> {
        let mut message = message.into();
        for middleware in &self.inner.middleware {
            let message_type = message.message_type();
            message = match middleware.process(message)? {
                Some(message) => message,
                None => {
                    publish(&self.inner.events, AnalyticsEvent::Dropped { message_type });
                    return Ok(());
                }
            };
        }
        let prepared = PreparedBatchMessage::new(message, self.inner.auto_timestamp)?;
        if self.inner.empty_batch_size + prepared.serialized_size > MAX_BATCH_SIZE {
            return Err(Error::BatchTooLarge);
        }
        let message_id = prepared
            .message
            .message_id()
            .expect("message preparation generates an ID")
            .to_owned();
        let message_type = prepared.message.message_type();

        let state = self.inner.state.lock().await;
        if *state != State::Open {
            return Err(Error::ClientClosed);
        }

        let result = match self.inner.overflow_policy {
            OverflowPolicy::Block => self
                .inner
                .sender
                .send(Command::Event(Box::new(prepared)))
                .await
                .map_err(|_| Error::WorkerStopped),
            OverflowPolicy::Reject => match self
                .inner
                .sender
                .try_send(Command::Event(Box::new(prepared)))
            {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Full(_)) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(message_type, "analytics queue full, rejecting message");
                    publish(
                        &self.inner.events,
                        AnalyticsEvent::QueueFull { message_type },
                    );
                    Err(Error::QueueFull)
                }
                Err(mpsc::error::TrySendError::Closed(_)) => Err(Error::WorkerStopped),
            },
        };
        if result.is_ok() {
            publish(
                &self.inner.events,
                AnalyticsEvent::Enqueued {
                    message_id,
                    message_type,
                },
            );
        }
        result
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
                .send(Command::Flush {
                    sender,
                    reason: FlushReason::Shutdown,
                })
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
        publish(&self.inner.events, AnalyticsEvent::Closed);
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
                .send(Command::Flush {
                    sender,
                    reason: FlushReason::Manual,
                })
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

struct Inner {
    sender: mpsc::Sender<Command>,
    state: Mutex<State>,
    overflow_policy: OverflowPolicy,
    auto_timestamp: bool,
    empty_batch_size: usize,
    middleware: Vec<Arc<dyn Middleware>>,
    events: broadcast::Sender<AnalyticsEvent>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Open,
    Closing,
    Closed,
}

enum Command {
    Event(Box<PreparedBatchMessage>),
    Flush {
        sender: oneshot::Sender<Result<()>>,
        reason: FlushReason,
    },
    Shutdown(oneshot::Sender<()>),
}

async fn run_worker<C>(
    mut receiver: mpsc::Receiver<Command>,
    mut batcher: AutoBatcher<C>,
    flush_at: usize,
    flush_interval: Duration,
    events: broadcast::Sender<AnalyticsEvent>,
) where
    C: Client + Send + Sync + 'static,
{
    let timer = tokio::time::sleep(flush_interval);
    tokio::pin!(timer);
    let mut accepted_since_flush = 0;

    loop {
        tokio::select! {
            command = receiver.recv() => match command {
                Some(Command::Event(prepared)) => {
                    accepted_since_flush += 1;
                    let result = push_observed(&mut batcher, *prepared, &events).await;
                    if result.is_ok()
                        && accepted_since_flush >= flush_at
                        && flush_observed(&mut batcher, FlushReason::MessageCount, &events).await.is_ok()
                    {
                        accepted_since_flush = 0;
                    }
                }
                Some(Command::Flush { sender, reason }) => {
                    let result = flush_observed(&mut batcher, reason, &events).await;
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
                    let _ = flush_observed(&mut batcher, FlushReason::Shutdown, &events).await;
                    break;
                }
            },
            () = &mut timer => {
                if flush_observed(&mut batcher, FlushReason::Interval, &events).await.is_ok() {
                    accepted_since_flush = 0;
                }
                timer.as_mut().reset(tokio::time::Instant::now() + flush_interval);
            }
        }
    }
}

async fn push_observed<C>(
    batcher: &mut AutoBatcher<C>,
    prepared: PreparedBatchMessage,
    events: &broadcast::Sender<AnalyticsEvent>,
) -> Result<()>
where
    C: Client,
{
    let before = batcher.len();
    match batcher.push_prepared(prepared).await {
        Ok(()) => {
            let after = batcher.len();
            if after < before + 1 {
                let message_count = before + 1 - after;
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    message_count,
                    reason = ?FlushReason::Capacity,
                    "delivered segment batch"
                );
                publish(
                    events,
                    AnalyticsEvent::Delivered {
                        message_count,
                        reason: FlushReason::Capacity,
                    },
                );
            }
            Ok(())
        }
        Err(error) => {
            let message_count = batcher.len();
            let (status, retryable) = error.retry_metadata();
            #[cfg(feature = "tracing")]
            tracing::warn!(
                message_count,
                reason = ?FlushReason::Capacity,
                ?status,
                retryable,
                %error,
                "segment batch delivery failed"
            );
            publish(
                events,
                AnalyticsEvent::DeliveryFailed {
                    message_count,
                    reason: FlushReason::Capacity,
                    status,
                    retryable,
                    error: error.to_string(),
                },
            );
            Err(error)
        }
    }
}

async fn flush_observed<C>(
    batcher: &mut AutoBatcher<C>,
    reason: FlushReason,
    events: &broadcast::Sender<AnalyticsEvent>,
) -> Result<()>
where
    C: Client,
{
    let message_count = batcher.len();
    if message_count == 0 {
        return Ok(());
    }

    match batcher.flush().await {
        Ok(()) => {
            #[cfg(feature = "tracing")]
            tracing::debug!(message_count, ?reason, "delivered segment batch");
            publish(
                events,
                AnalyticsEvent::Delivered {
                    message_count,
                    reason,
                },
            );
            Ok(())
        }
        Err(error) => {
            let message_count = batcher.len();
            let (status, retryable) = error.retry_metadata();
            #[cfg(feature = "tracing")]
            tracing::warn!(
                message_count,
                ?reason,
                ?status,
                retryable,
                %error,
                "segment batch delivery failed"
            );
            publish(
                events,
                AnalyticsEvent::DeliveryFailed {
                    message_count,
                    reason,
                    status,
                    retryable,
                    error: error.to_string(),
                },
            );
            Err(error)
        }
    }
}

fn publish(events: &broadcast::Sender<AnalyticsEvent>, event: AnalyticsEvent) {
    let _ = events.send(event);
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
            message: &'a Message,
        ) -> impl Future<Output = Result<()>> + Send + 'a {
            self.sent
                .lock()
                .expect("recording client lock was poisoned")
                .push(message.clone());
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

    #[derive(Clone, Debug)]
    struct FailOnceClient {
        sent: Arc<StdMutex<Vec<Message>>>,
        failed: Arc<AtomicBool>,
    }

    impl Client for FailOnceClient {
        fn send<'a>(
            &'a self,
            _write_key: &'a str,
            message: &'a Message,
        ) -> impl Future<Output = Result<()>> + Send + 'a {
            if !self.failed.swap(true, Ordering::SeqCst) {
                return std::future::ready(Err(Error::Middleware(
                    "simulated delivery failure".to_owned(),
                )));
            }
            self.sent
                .lock()
                .expect("recording client lock was poisoned")
                .push(message.clone());
            std::future::ready(Ok(()))
        }
    }

    impl Client for BlockingOnceClient {
        async fn send<'a>(&'a self, _write_key: &'a str, message: &'a Message) -> Result<()> {
            self.sent
                .lock()
                .expect("recording client lock was poisoned")
                .push(message.clone());
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

    fn received_events(receiver: &mut broadcast::Receiver<AnalyticsEvent>) -> Vec<AnalyticsEvent> {
        let mut events = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            events.push(event);
        }
        events
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
        let mut events = analytics.subscribe();

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
        assert!(received_events(&mut events).iter().any(|event| matches!(
            event,
            AnalyticsEvent::QueueFull {
                message_type: "track"
            }
        )));

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

    #[tokio::test]
    async fn middleware_enriches_in_order_and_can_drop_messages() {
        let sent = Arc::new(StdMutex::new(Vec::new()));
        let client = RecordingClient {
            sent: Arc::clone(&sent),
            delivered: Arc::new(Notify::new()),
        };
        let analytics = Analytics::builder_with_client("write-key", client)
            .flush_at(100)
            .flush_interval(Duration::from_secs(60))
            .middleware(
                |mut message: BatchMessage| -> Result<Option<BatchMessage>> {
                    if let BatchMessage::Track(track) = &mut message {
                        track
                            .properties
                            .as_object_mut()
                            .expect("test properties should be an object")
                            .insert("enriched".to_owned(), json!(true));
                    }
                    Ok(Some(message))
                },
            )
            .middleware(|message: BatchMessage| -> Result<Option<BatchMessage>> {
                if matches!(
                    &message,
                    BatchMessage::Track(Track { event, properties, .. })
                        if event == "Drop" && properties["enriched"] == json!(true)
                ) {
                    Ok(None)
                } else {
                    Ok(Some(message))
                }
            })
            .build()
            .expect("client should build");
        let mut events = analytics.subscribe();

        analytics
            .track(track("Keep"))
            .await
            .expect("kept event should queue");
        analytics
            .track(track("Drop"))
            .await
            .expect("dropped event should be acknowledged");
        analytics.flush().await.expect("flush should succeed");
        analytics
            .close_and_flush()
            .await
            .expect("close should succeed");

        let sent = sent.lock().expect("lock should not be poisoned");
        assert_eq!(delivered_message_count(&sent), 1);
        let Message::Batch(batch) = &sent[0] else {
            panic!("background client should send batches");
        };
        let BatchMessage::Track(track) = &batch.batch[0] else {
            panic!("test should deliver a track message");
        };
        assert_eq!(track.properties["enriched"], json!(true));

        let events = received_events(&mut events);
        assert!(events.iter().any(|event| matches!(
            event,
            AnalyticsEvent::Dropped {
                message_type: "track"
            }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            AnalyticsEvent::Delivered {
                message_count: 1,
                reason: FlushReason::Manual
            }
        )));
        assert!(events.contains(&AnalyticsEvent::Closed));
    }

    #[tokio::test]
    async fn delivery_events_report_failure_and_loss_safe_retry() {
        let sent = Arc::new(StdMutex::new(Vec::new()));
        let client = FailOnceClient {
            sent: Arc::clone(&sent),
            failed: Arc::new(AtomicBool::new(false)),
        };
        let analytics = Analytics::builder_with_client("write-key", client)
            .flush_at(100)
            .flush_interval(Duration::from_secs(60))
            .build()
            .expect("client should build");
        let mut events = analytics.subscribe();

        analytics
            .track(track("Retry"))
            .await
            .expect("event should queue");
        assert!(matches!(analytics.flush().await, Err(Error::Middleware(_))));
        analytics
            .flush()
            .await
            .expect("retained batch should retry");
        analytics
            .close_and_flush()
            .await
            .expect("close should succeed");

        assert_eq!(
            delivered_message_count(&sent.lock().expect("lock should not be poisoned")),
            1
        );
        let events = received_events(&mut events);
        assert!(events.iter().any(|event| matches!(
            event,
            AnalyticsEvent::DeliveryFailed {
                message_count: 1,
                reason: FlushReason::Manual,
                ..
            }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            AnalyticsEvent::Delivered {
                message_count: 1,
                reason: FlushReason::Manual
            }
        )));
    }

    #[derive(Clone, Debug)]
    struct RateLimitedOnceClient {
        sent: Arc<StdMutex<Vec<Message>>>,
        failed: Arc<AtomicBool>,
    }

    impl Client for RateLimitedOnceClient {
        fn send<'a>(
            &'a self,
            _write_key: &'a str,
            message: &'a Message,
        ) -> impl Future<Output = Result<()>> + Send + 'a {
            if !self.failed.swap(true, Ordering::SeqCst) {
                return std::future::ready(Err(Error::from(crate::errors::ApiError {
                    status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                    body: "rate limited".to_owned(),
                    headers: reqwest::header::HeaderMap::new(),
                })));
            }
            self.sent
                .lock()
                .expect("recording client lock was poisoned")
                .push(message.clone());
            std::future::ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn delivery_failed_captures_api_error_status_and_retryability() {
        let sent = Arc::new(StdMutex::new(Vec::new()));
        let client = RateLimitedOnceClient {
            sent: Arc::clone(&sent),
            failed: Arc::new(AtomicBool::new(false)),
        };
        let analytics = Analytics::builder_with_client("write-key", client)
            .flush_at(100)
            .flush_interval(Duration::from_secs(60))
            .build()
            .expect("client should build");
        let mut events = analytics.subscribe();

        analytics
            .track(track("RateLimited"))
            .await
            .expect("event should queue");
        assert!(matches!(analytics.flush().await, Err(Error::ApiError(_))));
        analytics
            .close_and_flush()
            .await
            .expect("close should succeed");

        let events = received_events(&mut events);
        assert!(events.iter().any(|event| matches!(
            event,
            AnalyticsEvent::DeliveryFailed {
                message_count: 1,
                reason: FlushReason::Manual,
                status: Some(reqwest::StatusCode::TOO_MANY_REQUESTS),
                retryable: true,
                ..
            }
        )));
    }
}
