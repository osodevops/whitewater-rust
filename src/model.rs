//! Wire model types shared across the client surface. Nanosecond fields
//! are `i64` in Rust and decimal strings on the wire — never floats.

use std::collections::BTreeMap;

use base64::Engine;
use serde_json::Value;
use uuid::Uuid;

use crate::error::SdkError;

pub(crate) use base64::engine::general_purpose::STANDARD;

/// One record an application appends or receives. `event_time_ns` and
/// `ingest_time_ns` are signed nanoseconds; `metadata` is record
/// Metadata — never transport headers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub message_id: Option<Uuid>,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
    pub metadata: BTreeMap<String, Vec<u8>>,
    pub event_time_ns: i64,
    pub ingest_time_ns: i64,
    /// Opaque per-record Cursor. Never parsed or compared by the SDK;
    /// acknowledge a page's `delivered_cursor` instead.
    pub cursor: Option<String>,
}

/// What one append committed: the durable identity plus placement-free
/// feedback the caller may act on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendOutcome {
    pub message_id: Uuid,
    pub cursor: String,
    pub deduplicated: bool,
    /// Sustained dominant-Key share: spread or shed load for the Key;
    /// a strictly ordered hot Key is never parallelized.
    pub hot_key: bool,
    pub recommended_batch_count: Option<u64>,
    pub retry_after_ms: Option<u64>,
}

/// Enforced transport bounds the server advertises on `/health` — SDKs
/// negotiate capacity before a request hits a 4xx refusal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportLimits {
    pub max_frame_bytes: u64,
    pub max_committed_read_bytes: u64,
    pub max_logical_read_frames: u64,
    pub max_logical_read_ranges: u64,
    pub member_fetch_max_limit: u64,
    pub member_max_lease_ticks: u64,
}

/// A page of delivered records plus the cursors that describe progress.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchPage {
    pub records: Vec<Record>,
    /// Advances when this page lands; ack this, never a record cursor.
    pub delivered_cursor: Option<String>,
    /// The durable restart point after the last ack.
    pub acknowledged_cursor: Option<String>,
    pub recommended_capacity: Option<u64>,
    pub retry_after_ms: Option<u64>,
}

/// Options for `Client::temporary_fetch`: `new_only` waits for fresh
/// history, `tail` follows live delivery, `after`/`after_event_time_ns`
/// bound where the page starts.
#[derive(Clone, Debug, Default)]
pub struct TemporaryFetch {
    /// Opaque Cursor to resume after; `None` starts per `tail`/`new_only`.
    pub after: Option<String>,
    pub limit: u64,
    pub new_only: bool,
    pub tail: bool,
    /// Bounded server-side wait for records on a live tail.
    pub wait_ms: Option<u64>,
    /// Event-time lower bound (signed epoch nanoseconds).
    pub after_event_time_ns: Option<i64>,
}

/// A page returned by `Client::temporary_fetch`.
#[derive(Clone, Debug)]
pub struct TemporaryPage {
    pub records: Vec<Record>,
    /// Opaque continuation for the next page; `None` at the tip.
    pub next_cursor: Option<String>,
}

pub(crate) fn urlencode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

pub(crate) fn metadata_base64(metadata: &BTreeMap<String, Vec<u8>>) -> Value {
    metadata
        .iter()
        .map(|(name, value)| (name.clone(), Value::String(STANDARD.encode(value))))
        .collect::<serde_json::Map<String, Value>>()
        .into()
}

pub(crate) fn decode_append_outcome(body: &Value) -> Result<AppendOutcome, SdkError> {
    let message_id = body
        .get("message_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .ok_or_else(|| SdkError::Protocol("append returned no message_id".to_owned()))?;
    let cursor = body
        .get("cursor")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let feedback = body.get("feedback").cloned().unwrap_or(Value::Null);
    Ok(AppendOutcome {
        message_id,
        cursor,
        deduplicated: body
            .get("deduplicated")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        hot_key: feedback
            .get("hot_key")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        recommended_batch_count: feedback
            .get("recommended_batch_count")
            .and_then(Value::as_u64),
        retry_after_ms: feedback.get("retry_after_ms").and_then(Value::as_u64),
    })
}

pub(crate) fn decode_record(value: &Value) -> Result<Record, SdkError> {
    let decode = |field: &str| -> Result<Vec<u8>, SdkError> {
        value
            .get(field)
            .and_then(Value::as_str)
            .and_then(|encoded| STANDARD.decode(encoded).ok())
            .ok_or_else(|| SdkError::Protocol(format!("record missing {field}")))
    };
    let nanos = |field: &str| -> i64 {
        value
            .get(field)
            .and_then(|raw| match raw {
                Value::String(text) => text.parse::<i64>().ok(),
                Value::Number(number) => number.as_i64(),
                _ => None,
            })
            .unwrap_or(0)
    };
    let metadata = value
        .get("metadata_base64")
        .and_then(Value::as_object)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|(name, encoded)| {
                    encoded
                        .as_str()
                        .and_then(|raw| STANDARD.decode(raw).ok())
                        .map(|bytes| (name.clone(), bytes))
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Record {
        message_id: value
            .get("message_id")
            .and_then(Value::as_str)
            .and_then(|raw| Uuid::parse_str(raw).ok()),
        key: decode("key_base64")?,
        payload: decode("payload_base64")?,
        metadata,
        event_time_ns: nanos("event_time_ns"),
        ingest_time_ns: nanos("ingest_time_ns"),
        cursor: value
            .get("cursor")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

/// Current Unix epoch nanoseconds for caller-omitted event times.
pub(crate) fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| {
            duration
                .as_nanos()
                .min(i64::MAX as u128)
                .try_into()
                .unwrap_or(i64::MAX)
        })
        .unwrap_or_default()
}
