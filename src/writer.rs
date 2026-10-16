//! Session-bound Writers.

use std::collections::BTreeMap;

use base64::Engine;
use serde_json::json;
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
            .call(
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

    /// Append a batch as one request; per-record `request_id`s keep the
    /// same retry contract as `append`.
    pub async fn append_batch(
        &self,
        records: &[(Uuid, Vec<u8>, Vec<u8>, i64)],
    ) -> Result<Vec<AppendOutcome>, SdkError> {
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
            .call(
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
}
