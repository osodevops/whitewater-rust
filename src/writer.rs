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

    /// The Node base URL this Writer's session is currently pinned to.
    pub fn pinned_base(&self) -> String {
        self.pinned.lock().expect("pinned base").clone()
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
    /// same retry contract as `append`. Records carry no Metadata —
    /// [`Writer::append_items`] is the form that ships it.
    pub async fn append_batch(
        &self,
        records: &[(Uuid, Vec<u8>, Vec<u8>, i64)],
    ) -> Result<Vec<AppendOutcome>, SdkError> {
        let items: Vec<AppendItem> = records
            .iter()
            .map(|(request_id, key, payload, event_time_ns)| AppendItem {
                request_id: *request_id,
                key: key.clone(),
                payload: payload.clone(),
                event_time_ns: *event_time_ns,
                metadata: BTreeMap::new(),
            })
            .collect();
        self.append_items(&items).await
    }

    /// Append a batch with per-record Metadata — the same wire contract
    /// as [`Writer::append_batch`], on both the JSON and `FSA1` paths.
    /// When the server refuses the body as too large
    /// ([`SdkError::PayloadTooLarge`]) the batch splits in half and each
    /// half retries in order, so oversized runs still commit.
    pub async fn append_items(&self, items: &[AppendItem]) -> Result<Vec<AppendOutcome>, SdkError> {
        match self.append_items_once(items).await {
            Err(SdkError::PayloadTooLarge(_)) if items.len() > 1 => {
                let half = items.len() / 2;
                // Boxed recursion: halving bounds the depth at log2(n).
                let mut outcomes = Box::pin(self.append_items(&items[..half])).await?;
                outcomes.extend(Box::pin(self.append_items(&items[half..])).await?);
                Ok(outcomes)
            }
            other => other,
        }
    }

    async fn append_items_once(
        &self,
        items: &[AppendItem],
    ) -> Result<Vec<AppendOutcome>, SdkError> {
        if self.binary_appends {
            return self.append_batch_binary(items).await;
        }
        let records: Vec<serde_json::Value> = items
            .iter()
            .map(|item| {
                json!({
                    "request_id": item.request_id.to_string(),
                    "writer": self.name,
                    "session_epoch": self.session_epoch,
                    "event_time_ns": item.event_time_ns.to_string(),
                    "key_base64": STANDARD.encode(&item.key),
                    "payload_base64": STANDARD.encode(&item.payload),
                    "metadata_base64": metadata_base64(&item.metadata),
                    "use_compression": self.client.use_compression,
                })
            })
            .collect();
        let body = self
            .client
            .session_call(
                &self.pinned,
                "POST",
                "/v1/writers/append-batch",
                Some(&json!({ "records": records })),
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

    /// `FSA1` wire form of [`Writer::append_items`] — same
    /// request/response contract, frames instead of JSON.
    async fn append_batch_binary(
        &self,
        items: &[AppendItem],
    ) -> Result<Vec<AppendOutcome>, SdkError> {
        let frames: Vec<crate::frame::AppendFrame<'_>> = items
            .iter()
            .map(|item| crate::frame::AppendFrame {
                writer: &self.name,
                request_id: item.request_id,
                session_epoch: self.session_epoch,
                event_time_ns: Some(item.event_time_ns),
                use_compression: self.client.use_compression,
                key: &item.key,
                payload: &item.payload,
                metadata: &item.metadata,
            })
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

/// One record in a [`Writer::append_items`] batch: the same fields
/// [`Writer::append`] carries, including per-record Metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendItem {
    pub request_id: Uuid,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub event_time_ns: i64,
    pub metadata: BTreeMap<String, Vec<u8>>,
}

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
    // Keys already committed to a spawned batch: no second batch may
    // carry the same Key while one is in flight (per-Key order). The
    // pump owns formation; completion tasks decrement on the way out.
    let in_flight_keys = Arc::new(std::sync::Mutex::new(std::collections::HashMap::<
        Vec<u8>,
        usize,
    >::new()));
    let completed = Arc::new(tokio::sync::Notify::new());
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
                    let cap = tuner.cap();
                    let mut batch_ops = Vec::with_capacity(cap.min(queue.len()));
                    // Front-to-back: take ops whose Key is not in flight;
                    // an op skipped for capacity or a blocked Key stays
                    // queued in order, and later ops with a blocked Key
                    // hit the same flying entry — no overtaking.
                    {
                        let mut flying = in_flight_keys.lock().expect("in-flight keys");
                        // Takeable iff the key was not in flight when the
                        // scan started AND it has not been skipped earlier
                        // in this scan. Keys claimed by THIS batch keep
                        // queue order inside the batch — duplicates are
                        // fine — and each new key is claimed exactly once.
                        let mut newly_claimed = std::collections::HashSet::new();
                        let mut skipped = std::collections::HashSet::new();
                        let mut kept = std::collections::VecDeque::with_capacity(queue.len());
                        for op in std::mem::take(&mut queue) {
                            if batch_ops.len() < cap
                                && !skipped.contains(&op.key)
                                && (newly_claimed.contains(&op.key)
                                    || !flying.contains_key(&op.key))
                            {
                                newly_claimed.insert(op.key.clone());
                                batch_ops.push(op);
                            } else {
                                skipped.insert(op.key.clone());
                                kept.push_back(op);
                            }
                        }
                        for key in newly_claimed {
                            *flying.entry(key).or_insert(0) += 1;
                        }
                        queue = kept;
                    }
                    if batch_ops.is_empty() {
                        // Everything queued is key-blocked: wait for a
                        // completion or new work instead of spinning.
                        tokio::select! {
                            op = rx.recv() => match op {
                                Some(op) if op.request_id.is_nil() => closed = true,
                                Some(op) => queue.push_back(op),
                                None => closed = true,
                            },
                            _ = completed.notified() => {},
                        }
                        continue;
                    }
                    let mut batch = Vec::with_capacity(batch_ops.len());
                    let mut senders = Vec::with_capacity(batch_ops.len());
                    for op in batch_ops {
                        senders.push(op.done);
                        batch.push((op.request_id, op.key, op.payload, op.event_time_ns));
                    }
                    in_flight.fetch_add(1, Ordering::Relaxed);
                    let writer = writer.clone();
                    let in_flight = in_flight.clone();
                    let tuner = tuner.clone();
                    let in_flight_keys = in_flight_keys.clone();
                    let completed = completed.clone();
                    let batch_keys: Vec<Vec<u8>> = {
                        let mut seen = std::collections::HashSet::new();
                        batch
                            .iter()
                            .filter(|&(_, key, _, _)| seen.insert(key.clone()))
                            .map(|(_, key, _, _)| key.clone())
                            .collect()
                    };
                    tokio::spawn(async move {
                        let started = std::time::Instant::now();
                        // Identical retries share request_ids — the same
                        // dedup contract single appends rely on.
                        let mut outcome = writer.append_batch(&batch).await;
                        for _ in 0..4 {
                            // Only uncertain/transient outcomes earn the
                            // identical-request_id retry; authoritative
                            // refusals (stale epoch, too large, protocol)
                            // surface to every waiter immediately.
                            if !matches!(
                                outcome,
                                Err(SdkError::Transport(_)
                                    | SdkError::Unavailable(_)
                                    | SdkError::Ambiguous(_))
                            ) {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            outcome = writer.append_batch(&batch).await;
                        }
                        let rtt = started.elapsed();
                        in_flight.fetch_sub(1, Ordering::Relaxed);
                        {
                            let mut flying = in_flight_keys.lock().expect("in-flight keys");
                            for key in batch_keys {
                                if let Some(count) = flying.get_mut(&key) {
                                    *count -= 1;
                                    if *count == 0 {
                                        flying.remove(&key);
                                    }
                                }
                            }
                        }
                        completed.notify_one();
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
                        _ = completed.notified() => {},
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
        SdkError::PayloadTooLarge(detail) => SdkError::PayloadTooLarge(detail.clone()),
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
