//! Session-bound Writers.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::client::Client;
use crate::error::SdkError;
use crate::model::{decode_append_outcome, metadata_base64, AppendOutcome, STANDARD};

/// A declared Writer bound to a session epoch. `append` carries a
/// caller `request_id`: identical retries dedupe server-side, so the
/// safe pattern after an ambiguous failure is the identical call.
#[derive(Clone)]
pub struct Writer {
    pub(crate) client: Client,
    pub(crate) name: String,
    pub(crate) session_epoch: u64,
    /// Sticky endpoint: session state is Node-local.
    pub(crate) pinned: std::sync::Arc<std::sync::Mutex<String>>,
    /// Opt-in binary append transport (`FSA1` frames); JSON is the
    /// default wire contract.
    pub(crate) binary_appends: bool,
}

impl Writer {
    pub fn session_epoch(&self) -> u64 {
        self.session_epoch
    }

    /// The declared Writer name this session appends under.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Reopen the session after `StaleEpoch` — the server supersedes the
    /// old epoch; continuing with it can never succeed.
    pub async fn reopen(&mut self) -> Result<(), SdkError> {
        *self = self.client.writer(&self.name).await?;
        Ok(())
    }

    /// Append one record. `request_id` is the idempotency identity: an
    /// identical retry returns the original committed outcome
    /// (`deduplicated: true`), a fresh identity may commit twice.
    pub async fn append(
        &self,
        request_id: Uuid,
        key: &[u8],
        payload: &[u8],
        event_time_ns: i64,
        metadata: BTreeMap<String, Vec<u8>>,
    ) -> Result<AppendOutcome, SdkError> {
        let body = self
            .client
            .session_call(
                &self.pinned,
                "POST",
                "/v1/writers/append",
                Some(&json!({
                    "request_id": request_id.to_string(),
                    "writer": self.name,
                    "session_epoch": self.session_epoch,
                    "event_time_ns": event_time_ns.to_string(),
                    "key_base64": STANDARD.encode(key),
                    "payload_base64": STANDARD.encode(payload),
                    "metadata_base64": metadata_base64(&metadata),
                    "use_compression": self.client.use_compression,
                })),
            )
            .await?;
        decode_append_outcome(&body)
    }

    /// Opt into binary `FSA1` append frames on `append_batch`: the
    /// request body ships `application/vnd.whitewater.frames` instead
    /// of JSON+base64 — ~25% less wire and no serde pass either side.
    /// Appends stay idempotent: the request_ids ride inside the frames.
    /// JSON remains the default; unsupported Transports answer
    /// [`SdkError::Refused`].
    pub fn binary_appends(&self) -> Self {
        let mut writer = self.clone();
        writer.binary_appends = true;
        writer
    }

    /// Append a batch as one request; per-record `request_id`s keep the
    /// same retry contract as `append`.
    pub async fn append_batch(
        &self,
        records: &[(Uuid, Vec<u8>, Vec<u8>, i64)],
    ) -> Result<Vec<AppendOutcome>, SdkError> {
        if self.binary_appends {
            return self.append_batch_binary(records).await;
        }
        let items: Vec<serde_json::Value> = records
            .iter()
            .map(|(request_id, key, payload, event_time_ns)| {
                json!({
                    "request_id": request_id.to_string(),
                    "writer": self.name,
                    "session_epoch": self.session_epoch,
                    "event_time_ns": event_time_ns.to_string(),
                    "key_base64": STANDARD.encode(key),
                    "payload_base64": STANDARD.encode(payload),
                })
            })
            .collect();
        let body = self
            .client
            .session_call(
                &self.pinned,
                "POST",
                "/v1/writers/append-batch",
                Some(&json!({ "records": items })),
            )
            .await?;
        let outcomes = body
            .get("results")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .or_else(|| body.as_array().cloned())
            .unwrap_or_default();
        outcomes.iter().map(decode_append_outcome).collect()
    }

    /// `FSA1` wire form of [`Writer::append_batch`] — same request/response
    /// contract, frames instead of JSON.
    async fn append_batch_binary(
        &self,
        records: &[(Uuid, Vec<u8>, Vec<u8>, i64)],
    ) -> Result<Vec<AppendOutcome>, SdkError> {
        let frames: Vec<crate::frame::AppendFrame<'_>> = records
            .iter()
            .map(
                |(request_id, key, payload, event_time_ns)| crate::frame::AppendFrame {
                    writer: &self.name,
                    request_id: *request_id,
                    session_epoch: self.session_epoch,
                    event_time_ns: Some(*event_time_ns),
                    use_compression: false,
                    key,
                    payload,
                    metadata: &NO_METADATA,
                },
            )
            .collect();
        let body = self
            .client
            .session_call_binary_body(
                &self.pinned,
                "/v1/writers/append-batch",
                &crate::frame::encode_append_page(&frames),
            )
            .await?;
        let outcomes = body
            .get("results")
            .and_then(Value::as_array)
            .ok_or_else(|| SdkError::Protocol("append-batch returned no results".to_owned()))?;
        outcomes.iter().map(decode_append_outcome).collect()
    }
}

static NO_METADATA: std::sync::LazyLock<BTreeMap<String, Vec<u8>>> =
    std::sync::LazyLock::new(BTreeMap::new);

/// Request-side knobs for [`Writer::batched`] / [`Writer::autotuned`].
pub use crate::autotune::{AutoTune, Batching};

/// A batch-forming Writer facade: `append` queues the record and
/// resolves when its batch commits — same signature as
/// [`Writer::append`], so callers keep one call per record while the
/// pump coalesces load into `append-batch` rounds behind them.
/// [`Batching`] pins linger/cap explicitly; [`AutoTune`] self-adjusts
/// (idle sends immediately, busy accumulates one measured RTT).
#[derive(Clone)]
pub struct PumpedWriter {
    tx: tokio::sync::mpsc::UnboundedSender<Op>,
    /// Live tuner state under `autotuned`; absent under `batched`
    /// where the cap is fixed.
    auto: Option<Arc<std::sync::Mutex<crate::autotune::AutoTuner>>>,
    fixed_cap: usize,
}

struct Op {
    request_id: Uuid,
    key: Vec<u8>,
    payload: Vec<u8>,
    event_time_ns: i64,
    done: tokio::sync::oneshot::Sender<Result<AppendOutcome, SdkError>>,
    enqueued: std::time::Instant,
}

enum Tuner {
    Fixed(crate::autotune::BatchTuner),
    Auto(Arc<std::sync::Mutex<crate::autotune::AutoTuner>>),
}

impl Tuner {
    fn evaluate(
        &self,
        queue: usize,
        oldest_age: Duration,
        in_flight: usize,
    ) -> crate::autotune::Flush {
        match self {
            Self::Fixed(tuner) => tuner.evaluate(queue, oldest_age, in_flight),
            Self::Auto(tuner) => tuner
                .lock()
                .expect("tuner")
                .evaluate(queue, oldest_age, in_flight),
        }
    }

    fn cap(&self) -> usize {
        match self {
            Self::Fixed(tuner) => tuner.cap(),
            Self::Auto(tuner) => tuner.lock().expect("tuner").batch_cap(),
        }
    }

    fn observe(
        &self,
        sent: usize,
        rtt: Duration,
        recommended: Option<u64>,
        retry_after_ms: Option<u64>,
    ) {
        if let Self::Auto(tuner) = self {
            tuner
                .lock()
                .expect("tuner")
                .observe(sent, rtt, recommended, retry_after_ms);
        }
    }
}

fn spawn_pump(writer: Writer, mut rx: tokio::sync::mpsc::UnboundedReceiver<Op>, tuner: Tuner) {
    let in_flight = Arc::new(AtomicUsize::new(0));
    let tuner = Arc::new(tuner);
    tokio::spawn(async move {
        let mut queue: std::collections::VecDeque<Op> = std::collections::VecDeque::new();
        let mut closed = false;
        while !(closed && queue.is_empty()) {
            while let Ok(op) = rx.try_recv() {
                if op.request_id.is_nil() {
                    closed = true;
                } else {
                    queue.push_back(op);
                }
            }
            let oldest_age = queue
                .front()
                .map(|op| op.enqueued.elapsed())
                .unwrap_or_default();
            match tuner.evaluate(queue.len(), oldest_age, in_flight.load(Ordering::Relaxed)) {
                crate::autotune::Flush::Now => {
                    let take = tuner.cap().min(queue.len());
                    let mut batch = Vec::with_capacity(take);
                    let mut senders = Vec::with_capacity(take);
                    for op in queue.drain(..take) {
                        senders.push(op.done);
                        batch.push((op.request_id, op.key, op.payload, op.event_time_ns));
                    }
                    in_flight.fetch_add(1, Ordering::Relaxed);
                    let writer = writer.clone();
                    let in_flight = in_flight.clone();
                    let tuner = tuner.clone();
                    tokio::spawn(async move {
                        let started = std::time::Instant::now();
                        // Identical retries share request_ids — the same
                        // dedup contract single appends rely on.
                        let mut outcome = writer.append_batch(&batch).await;
                        for _ in 0..4 {
                            if outcome.is_ok() {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            outcome = writer.append_batch(&batch).await;
                        }
                        let rtt = started.elapsed();
                        in_flight.fetch_sub(1, Ordering::Relaxed);
                        match outcome {
                            Ok(outcomes) => {
                                let first = outcomes.first();
                                tuner.observe(
                                    batch.len(),
                                    rtt,
                                    first.and_then(|outcome| outcome.recommended_batch_count),
                                    first.and_then(|outcome| outcome.retry_after_ms),
                                );
                                for (done, outcome) in senders.into_iter().zip(outcomes) {
                                    let _ = done.send(Ok(outcome));
                                }
                            }
                            Err(error) => {
                                for done in senders {
                                    let _ = done.send(Err(clone_error(&error)));
                                }
                            }
                        }
                    });
                }
                crate::autotune::Flush::Wait(wait) => {
                    // Wake on the linger deadline, a new record, or a
                    // close barrier — whichever lands first.
                    let wait = wait.min(Duration::from_millis(500));
                    tokio::select! {
                        op = rx.recv() => match op {
                            Some(op) if op.request_id.is_nil() => closed = true,
                            Some(op) => queue.push_back(op),
                            None => closed = true,
                        },
                        _ = tokio::time::sleep(wait) => {},
                    }
                }
            }
        }
        while in_flight.load(Ordering::Relaxed) > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
}

/// Rebuild an `SdkError` for fan-out — some variants hold structured
/// detail so the type is intentionally not `Clone`.
fn clone_error(error: &SdkError) -> SdkError {
    match error {
        SdkError::StaleEpoch => SdkError::StaleEpoch,
        SdkError::Sequence => SdkError::Sequence,
        SdkError::LeaseBusy => SdkError::LeaseBusy,
        SdkError::LeaseStale => SdkError::LeaseStale,
        SdkError::LeaseCapacity => SdkError::LeaseCapacity,
        SdkError::IndexBehind => SdkError::IndexBehind,
        SdkError::Conflict(detail) => SdkError::Conflict(detail.clone()),
        SdkError::Ambiguous(detail) => SdkError::Ambiguous(detail.clone()),
        SdkError::Unavailable(detail) => SdkError::Unavailable(detail.clone()),
        SdkError::Refused(detail) => SdkError::Refused(detail.clone()),
        SdkError::Transport(detail) => SdkError::Transport(detail.clone()),
        SdkError::Protocol(detail) => SdkError::Protocol(detail.clone()),
        SdkError::Codec(detail) => SdkError::Codec(detail.clone()),
        SdkError::IncompatibleServer { expected, reported } => SdkError::IncompatibleServer {
            expected: *expected,
            reported: reported.clone(),
        },
    }
}

impl PumpedWriter {
    /// Queue one record; resolves with its [`AppendOutcome`] once the
    /// containing batch commits — identical signature and retry
    /// contract to [`Writer::append`].
    pub async fn append(
        &self,
        request_id: Uuid,
        key: &[u8],
        payload: &[u8],
        event_time_ns: i64,
    ) -> Result<AppendOutcome, SdkError> {
        let (done, wait) = tokio::sync::oneshot::channel();
        self.tx
            .send(Op {
                request_id,
                key: key.to_vec(),
                payload: payload.to_vec(),
                event_time_ns,
                done,
                enqueued: std::time::Instant::now(),
            })
            .map_err(|_| SdkError::Transport("batch pump stopped".to_owned()))?;
        wait.await
            .map_err(|_| SdkError::Transport("batch pump dropped ack".to_owned()))?
    }

    /// Current batch cap — adaptive under `autotuned`, fixed under
    /// `batched`.
    pub fn batch_cap(&self) -> usize {
        self.auto
            .as_ref()
            .map(|tuner| tuner.lock().expect("tuner").batch_cap())
            .unwrap_or(self.fixed_cap)
    }

    /// Outstanding append-batch rounds the tuner currently allows —
    /// sheds under server pressure, recovers on fast unpressured
    /// rounds. Always `batching.max_in_flight` under `batched`.
    pub fn in_flight_limit(&self) -> Option<usize> {
        self.auto
            .as_ref()
            .map(|tuner| tuner.lock().expect("tuner").in_flight_limit())
    }

    /// Drain the queue and wait for every in-flight batch — call before
    /// dropping when outstanding acks matter.
    pub async fn flush(&self) -> Result<(), SdkError> {
        // A nil request_id is the close barrier: the pump drains the
        // queue behind it, then exits once in-flight resolves.
        self.tx
            .send(Op {
                request_id: Uuid::nil(),
                key: Vec::new(),
                payload: Vec::new(),
                event_time_ns: 0,
                done: tokio::sync::oneshot::channel().0,
                enqueued: std::time::Instant::now(),
            })
            .map_err(|_| SdkError::Transport("batch pump stopped".to_owned()))
    }
}

impl Writer {
    /// Fixed-linger batching — the `linger.ms` equivalent: records wait
    /// up to `batching.linger` for the batch to fill to
    /// `batching.max_batch`, with `batching.max_in_flight` concurrent
    /// rounds.
    pub fn batched(&self, batching: Batching) -> PumpedWriter {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        spawn_pump(
            self.clone(),
            rx,
            Tuner::Fixed(crate::autotune::BatchTuner::new(batching)),
        );
        PumpedWriter {
            tx,
            auto: None,
            fixed_cap: batching.max_batch,
        }
    }

    /// Self-adjusting batching (Nagle): an idle Writer sends at once —
    /// a trickle pays zero added latency — while a busy Writer
    /// accumulates roughly one measured round trip of records.
    /// High-pressure `recommended_batch_count` feedback sheds
    /// outstanding-round concurrency (never batch size — smaller rounds
    /// amplify the per-round cost driving that pressure), and
    /// `retry_after_ms` paces the accumulation window.
    pub fn autotuned(&self, tune: AutoTune) -> PumpedWriter {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let auto = Arc::new(std::sync::Mutex::new(crate::autotune::AutoTuner::new(tune)));
        spawn_pump(self.clone(), rx, Tuner::Auto(auto.clone()));
        PumpedWriter {
            tx,
            auto: Some(auto),
            fixed_cap: 0,
        }
    }
}
