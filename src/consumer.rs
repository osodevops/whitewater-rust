//! A permanent, supervised consumer bound to a Feed's named Reader —
//! the durable-Reader equivalent of Kafka's `while (true) poll()` loop
//! with the plumbing removed:
//!
//! ```ignore
//! let mut consumer = feed.consumer("enricher");
//! while let Some(message) = consumer.next().await? {
//!     process(&message).await;
//! }
//! // or a handler, completing only on close/stop:
//! feed.consumer("enricher").run(|msg| async move {
//!     process(msg).await?; Ok(())
//! }).await?;
//! ```
//!
//! Semantics:
//! - The named Reader's acknowledged cursor is durable server-side — a
//!   restart (app or session) resumes where it stopped; no offset
//!   management.
//! - Each fetched page is delivered record-by-record; its delivered
//!   cursor acks only after every record delivered — a handler error or
//!   an abandoned consumer leaves the page unacked so it redelivers
//!   (at-least-once). Poison pills are the app's job — route to a DLQ
//!   feed.
//! - Transient failures retry with exponential backoff inside the
//!   supervisor; a fenced session re-opens and resumes. Startup
//!   refusals (misconfiguration, denied auth) and decode/protocol
//!   faults surface on `next`/`run` — fail fast, never loop.
//! - `close` (or `ConsumerHandle::stop`) finishes the in-flight page,
//!   acks, closes the session — the SIGTERM path.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::sleep;

use crate::error::SdkError;
use crate::feed::{Feed, FeedMessage, FeedReader};

/// Pause between polls when a fetch returns an empty page.
const EMPTY_POLL: Duration = Duration::from_millis(250);
/// In-session retry delay for transient fetch/open failures.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Where a Reader starts consuming a Feed on first declaration — the
/// WCL `START AT` operand. Ignored once the Reader exists: it always
/// resumes from durable acknowledged progress.
#[derive(Clone, Debug)]
pub enum ReaderStart {
    /// From the oldest retained record.
    Beginning,
    /// From records appended after creation.
    Now,
    /// From an opaque Cursor previously returned by the Riverbed.
    Cursor(String),
    /// From a signed Unix epoch nanosecond timestamp.
    TimestampNs(i64),
}

impl ReaderStart {
    pub(crate) fn wcl(&self) -> String {
        match self {
            ReaderStart::Now => "NOW".to_owned(),
            ReaderStart::Cursor(cursor) => {
                format!("CURSOR '{}'", cursor.replace('\'', "''"))
            }
            ReaderStart::TimestampNs(nanos) => format!("TIMESTAMP {nanos}"),
            ReaderStart::Beginning => "BEGINNING".to_owned(),
        }
    }
}

/// `true` keeps the page going; `false` ends the run cleanly (receiver
/// dropped, close asked).
type DeliverOutcome = bool;

/// Optional visibility sink for supervised failures.
type ErrorSink = Option<Arc<dyn Fn(&SdkError) + Send + Sync>>;

/// The supervise loop's fixed configuration.
struct Config {
    reader: String,
    start: ReaderStart,
    capacity: u64,
    limit: u64,
    initial_backoff: Duration,
    max_backoff: Duration,
    on_error: ErrorSink,
}

/// The per-record delivery sink the supervise loop drives.
type Deliver<K, V> = Box<
    dyn FnMut(
            FeedMessage<K, V>,
        ) -> Pin<Box<dyn Future<Output = Result<DeliverOutcome, SdkError>> + Send>>
        + Send,
>;

/// One supervised session's exit classification.
enum SessionEnd {
    /// Clean stop requested — supervise returns `Ok(())`.
    Stop,
    /// A supervised failure — backoff, re-open, redeliver. Reported to
    /// `on_error` for visibility.
    Retry(SdkError),
    /// Fail fast — surfaced to `next`/`run` with no restart.
    Terminal(SdkError),
}

impl SessionEnd {
    /// Every session-path failure lands here: transient errors retry,
    /// fencing/expiry re-opens; `Refused` and local faults (codec,
    /// protocol, incompatible) terminate — reopening never fixes them.
    fn classify(error: SdkError) -> Self {
        if error.retryable() {
            return SessionEnd::Retry(error);
        }
        match error {
            SdkError::StaleEpoch
            | SdkError::Conflict(_)
            | SdkError::LeaseBusy
            | SdkError::LeaseStale
            | SdkError::LeaseCapacity
            | SdkError::Sequence => SessionEnd::Retry(error),
            error => SessionEnd::Terminal(error),
        }
    }
}

/// A handle that stops a running [`FeedConsumer`] from another task —
/// clone freely; `stop` equals [`FeedConsumer::close`] without the join.
#[derive(Clone)]
pub struct ConsumerHandle {
    stop: watch::Sender<bool>,
}

impl ConsumerHandle {
    /// Signal graceful stop: the in-flight page finishes and acks, the
    /// session releases, `next`/`run` returns.
    pub fn stop(&self) {
        let _ = self.stop.send(true);
    }
}

/// A permanent, supervised consumer — see the module docs. Build via
/// [`Feed::consumer`]; one consume lifetime per instance.
pub struct FeedConsumer<K, V> {
    feed: Feed<K, V>,
    reader: String,
    start: ReaderStart,
    capacity: u64,
    limit: u64,
    initial_backoff: Duration,
    max_backoff: Duration,
    on_error: ErrorSink,
    rx: Option<mpsc::Receiver<Result<FeedMessage<K, V>, SdkError>>>,
    supervise: Option<JoinHandle<Result<(), SdkError>>>,
    stop: watch::Sender<bool>,
    started: bool,
}

impl<K, V> FeedConsumer<K, V>
where
    K: Send + 'static,
    V: Send + 'static,
{
    pub(crate) fn new(feed: Feed<K, V>, reader: &str) -> Self {
        let (stop, _) = watch::channel(false);
        Self {
            feed,
            reader: reader.to_owned(),
            start: ReaderStart::Beginning,
            capacity: 128,
            limit: 128,
            initial_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(30),
            on_error: None,
            rx: None,
            supervise: None,
            stop,
            started: false,
        }
    }

    /// Where the Reader starts on first declaration (default
    /// `Beginning`); ignored once the Reader exists — it resumes from
    /// durable progress.
    pub fn start_at(mut self, start: ReaderStart) -> Self {
        self.start = start;
        self
    }

    /// Records fetched per page (default 128).
    pub fn capacity(mut self, capacity: u64) -> Self {
        self.capacity = capacity.max(1);
        self.limit = self.capacity;
        self
    }

    /// Restart backoff bounds (default 250ms → 30s).
    pub fn backoff(mut self, initial: Duration, max: Duration) -> Self {
        self.initial_backoff = initial;
        self.max_backoff = max;
        self
    }

    /// Error sink for visibility — plumbing failures and handler
    /// errors alike. The loop continues regardless.
    pub fn on_error(mut self, handler: impl Fn(&SdkError) + Send + Sync + 'static) -> Self {
        self.on_error = Some(Arc::new(handler));
        self
    }

    /// A handle that stops this consumer from another task (signal
    /// handlers, shutdown watchers).
    pub fn handle(&self) -> ConsumerHandle {
        ConsumerHandle {
            stop: self.stop.clone(),
        }
    }

    /// Pull the next decoded record: `Ok(Some)` per record, `Ok(None)`
    /// after a clean close, `Err` once for a terminal failure — startup
    /// refusal, decode fault. The supervise task starts lazily here.
    ///
    /// ```ignore
    /// while let Some(message) = consumer.next().await? {
    ///     process(&message).await;
    /// }
    /// ```
    pub async fn next(&mut self) -> Result<Option<FeedMessage<K, V>>, SdkError> {
        if !self.started {
            self.start_channel_mode()?;
        }
        if let Some(rx) = self.rx.as_mut() {
            match rx.recv().await {
                Some(Ok(message)) => return Ok(Some(message)),
                Some(Err(error)) => return Err(error),
                None => {}
            }
        }
        // The channel closed — the supervise ended: surface its terminal
        // error once, then keep returning Ok(None).
        self.join().await?;
        Ok(None)
    }

    /// Deliver decoded records to `handler` one at a time until close —
    /// `Err` only for startup refusals or a terminal session failure.
    /// A handler `Err` leaves the page unacked so it redelivers; poison
    /// pills are the app's job (route to a DLQ feed).
    pub async fn run<F, Fut>(&mut self, mut handler: F) -> Result<(), SdkError>
    where
        F: FnMut(FeedMessage<K, V>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), SdkError>> + Send + 'static,
    {
        self.claim()?;
        let deliver: Deliver<K, V> = Box::new(move |message| {
            let delivery = handler(message);
            Box::pin(async move { delivery.await.map(|()| true) })
        });
        self.spawn(deliver);
        self.join().await
    }

    /// Graceful stop: the in-flight page finishes and acks, the session
    /// releases, `next`/`run` returns. Safe any number of times.
    pub async fn close(&mut self) -> Result<(), SdkError> {
        let _ = self.stop.send(true);
        self.join().await
    }

    /// Await the supervise task's terminal state — `Ok(())` on a clean
    /// stop, `Err` for a terminal failure.
    async fn join(&mut self) -> Result<(), SdkError> {
        let Some(task) = self.supervise.take() else {
            return Ok(());
        };
        match task.await {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(SdkError::Transport(format!("consumer task: {error}"))),
        }
    }

    fn claim(&mut self) -> Result<(), SdkError> {
        if self.started {
            return Err(SdkError::Protocol(format!(
                "consumer '{}' already started — one supervise lifetime per instance",
                self.reader
            )));
        }
        Ok(())
    }

    fn spawn(&mut self, deliver: Deliver<K, V>) {
        self.started = true;
        let config = Config {
            reader: self.reader.clone(),
            start: self.start.clone(),
            capacity: self.capacity,
            limit: self.limit,
            initial_backoff: self.initial_backoff,
            max_backoff: self.max_backoff,
            on_error: self.on_error.clone(),
        };
        self.supervise = Some(tokio::spawn(supervise(
            self.feed.clone(),
            config,
            self.stop.subscribe(),
            deliver,
        )));
    }

    fn start_channel_mode(&mut self) -> Result<(), SdkError> {
        self.claim()?;
        let (tx, rx) = mpsc::channel::<Result<FeedMessage<K, V>, SdkError>>(self.capacity as usize);
        self.rx = Some(rx);
        self.spawn(Box::new(move |message| {
            let tx = tx.clone();
            Box::pin(async move {
                // A dropped receiver is a clean stop, not an error.
                Ok(tx.send(Ok(message)).await.is_ok())
            })
        }));
        Ok(())
    }
}

impl<K, V> Drop for FeedConsumer<K, V> {
    fn drop(&mut self) {
        // Signal stop so a spawned supervise closes the session rather
        // than leaking it until the server fences the epoch.
        let _ = self.stop.send(true);
    }
}

/// One restart-supervised session loop.
async fn supervise<K, V>(
    feed: Feed<K, V>,
    config: Config,
    mut stop: watch::Receiver<bool>,
    mut deliver: Deliver<K, V>,
) -> Result<(), SdkError>
where
    K: Send,
    V: Send,
{
    let mut backoff = config.initial_backoff;
    loop {
        match run_session(
            &feed,
            &config.reader,
            &config.start,
            config.capacity,
            config.limit,
            &mut stop,
            &mut deliver,
        )
        .await
        {
            SessionEnd::Stop => return Ok(()),
            SessionEnd::Terminal(error) => return Err(error),
            SessionEnd::Retry(error) => {
                if let Some(sink) = &config.on_error {
                    sink(&error);
                }
                tokio::select! {
                    _ = stop.changed() => return Ok(()),
                    _ = sleep(backoff) => {}
                }
                backoff = backoff.saturating_mul(2).min(config.max_backoff);
            }
        }
    }
}

/// One declared-Reader session: declare if missing + open with retry,
/// then fetch → deliver → ack until stop, a delivery outcome, or a
/// session failure ends it. Session cleanup is best-effort.
async fn run_session<K, V>(
    feed: &Feed<K, V>,
    reader: &str,
    start: &ReaderStart,
    capacity: u64,
    limit: u64,
    stop: &mut watch::Receiver<bool>,
    deliver: &mut Deliver<K, V>,
) -> SessionEnd
where
    K: Send,
    V: Send,
{
    let mut reader = loop {
        if *stop.borrow() {
            return SessionEnd::Stop;
        }
        match feed.open_reader(reader, capacity, &start.wcl()).await {
            Ok(reader) => break reader,
            Err(error) if error.retryable() => {
                tokio::select! {
                    _ = stop.changed() => return SessionEnd::Stop,
                    _ = sleep(RETRY_DELAY) => continue,
                }
            }
            Err(error) => return SessionEnd::classify(error),
        }
    };
    let end = poll_loop(&mut reader, limit, stop, deliver).await;
    let _ = reader.close().await;
    end
}

/// The fetch → deliver → ack core of one open session.
async fn poll_loop<K, V>(
    reader: &mut FeedReader<K, V>,
    limit: u64,
    stop: &mut watch::Receiver<bool>,
    deliver: &mut Deliver<K, V>,
) -> SessionEnd
where
    K: Send,
    V: Send,
{
    loop {
        if *stop.borrow() {
            return SessionEnd::Stop;
        }
        let page = match reader.fetch(limit).await {
            Ok(page) => page,
            Err(error) if error.retryable() => {
                tokio::select! {
                    _ = stop.changed() => return SessionEnd::Stop,
                    _ = sleep(RETRY_DELAY) => continue,
                }
            }
            Err(error) => return SessionEnd::classify(error),
        };
        if page.records.is_empty() {
            tokio::select! {
                _ = stop.changed() => return SessionEnd::Stop,
                _ = sleep(EMPTY_POLL) => continue,
            }
        }
        // A page being delivered runs to completion — finishing beats
        // mid-page abandonment (at-least-once would replay it anyway).
        for message in page.records {
            match deliver(message).await {
                Ok(true) => {}
                Ok(false) => return SessionEnd::Stop,
                // Handler errors always supervise: the page stays
                // unacked and redelivers on the next session.
                Err(error) => return SessionEnd::Retry(error),
            }
        }
        if let Err(error) = reader.ack().await {
            return SessionEnd::classify(error);
        }
    }
}
