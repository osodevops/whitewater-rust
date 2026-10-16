//! Declared StateStores: byte-level rows, freshness evidence, and
//! idempotent mutations. The codec-bound typed facade is `crate::Store`.

use base64::Engine;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::client::Client;
use crate::error::SdkError;
use crate::model::STANDARD;
use crate::typed::{wire_typed, TypedValue};

/// Outcome of one committed StateStore mutation.
#[derive(Clone, Debug)]
pub struct MutationOutcome {
    pub store_id: Uuid,
    pub sequence: u64,
    /// Identical `mutation_id` replay — the row did not commit twice.
    pub deduplicated: bool,
}

/// One row as served by lookup/get_by/iterate.
#[derive(Clone, Debug)]
pub struct StoreRow {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

/// Freshness + identity envelope every StateStore read reports.
#[derive(Clone, Debug)]
pub struct ReadFreshness {
    /// Highest journal sequence the serving replica applied.
    pub applied_seq: u64,
    /// Highest committed sequence on the serving replica.
    pub committed_seq: u64,
    /// `applied_seq < committed_seq`: the replica is still applying.
    pub behind: bool,
    /// Node identity that served — diagnostics, never routing input.
    pub served_by: String,
}

#[derive(Clone, Debug)]
pub struct LookupOutcome {
    pub row: Option<StoreRow>,
    pub freshness: ReadFreshness,
}

#[derive(Clone, Debug)]
pub struct GetByPage {
    pub rows: Vec<StoreRow>,
    pub freshness: ReadFreshness,
}

#[derive(Clone, Debug)]
pub struct IteratePage {
    pub rows: Vec<StoreRow>,
    /// Continuation key for the next page; `None` at the end.
    pub next_key: Option<Vec<u8>>,
    /// Journal position the page was pinned to.
    pub pinned_seq: u64,
    pub served_by: String,
}

/// Outcome of `sync`/`rebuild`: records applied this pass plus whether
/// the store reached the committed tip of its source Feed.
#[derive(Clone, Debug)]
pub struct SyncOutcome {
    pub applied: u64,
    pub frontier: Option<String>,
    pub complete: bool,
}

fn decode_store_row(value: &Value, key_field: &str) -> Result<StoreRow, SdkError> {
    let decode = |field: &str| -> Result<Vec<u8>, SdkError> {
        value
            .get(field)
            .and_then(Value::as_str)
            .and_then(|raw| STANDARD.decode(raw).ok())
            .ok_or_else(|| SdkError::Protocol(format!("store row missing {field}")))
    };
    Ok(StoreRow {
        key: decode(key_field)?,
        payload: decode("payload_base64")?,
    })
}

fn decode_freshness(body: &Value) -> ReadFreshness {
    ReadFreshness {
        applied_seq: body.get("applied_seq").and_then(Value::as_u64).unwrap_or(0),
        committed_seq: body
            .get("committed_seq")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        behind: body.get("behind").and_then(Value::as_bool).unwrap_or(false),
        served_by: body
            .get("served_by")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    }
}

fn decode_mutation_outcome(body: &Value) -> Result<MutationOutcome, SdkError> {
    Ok(MutationOutcome {
        store_id: body
            .get("store_id")
            .and_then(Value::as_str)
            .and_then(|raw| Uuid::parse_str(raw).ok())
            .ok_or_else(|| SdkError::Protocol("mutation returned no store_id".to_owned()))?,
        sequence: body.get("sequence").and_then(Value::as_u64).unwrap_or(0),
        deduplicated: body
            .get("deduplicated")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn decode_sync_outcome(body: &Value) -> SyncOutcome {
    SyncOutcome {
        applied: body.get("applied").and_then(Value::as_u64).unwrap_or(0),
        frontier: body
            .get("frontier")
            .and_then(Value::as_str)
            .map(str::to_owned),
        complete: body
            .get("complete")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }
}

/// A declared StateStore bound by name. Manual stores accept
/// `put_row`/`delete_row`/`clear` under caller `mutation_id`s (identical
/// retries dedupe); feed-sourced stores refuse mutation server-side and
/// are maintained by `sync`/`rebuild`.
#[derive(Clone)]
pub struct StateStore {
    pub(crate) client: Client,
    pub(crate) name: String,
}

impl StateStore {
    /// Put one row with optional named-key postings. `mutation_id` is the
    /// idempotency identity: an identical retry dedupes, a different
    /// payload under the same id conflicts.
    pub async fn put_row(
        &self,
        mutation_id: Uuid,
        key: &[u8],
        payload: &[u8],
        named: &[(String, Vec<TypedValue>)],
    ) -> Result<MutationOutcome, SdkError> {
        let named_wire: Vec<Value> = named
            .iter()
            .map(|(name, values)| {
                json!({
                    "name": name,
                    "values": values.iter().map(wire_typed).collect::<Vec<_>>(),
                })
            })
            .collect();
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/mutate",
                Some(&json!({
                    "store": self.name,
                    "mutation_id": mutation_id.to_string(),
                    "op": {
                        "kind": "put_row",
                        "key_base64": STANDARD.encode(key),
                        "payload_base64": STANDARD.encode(payload),
                        "named": named_wire,
                    },
                })),
            )
            .await?;
        decode_mutation_outcome(&body)
    }

    /// Delete one row by primary key under a `mutation_id` identity.
    pub async fn delete_row(
        &self,
        mutation_id: Uuid,
        key: &[u8],
    ) -> Result<MutationOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/mutate",
                Some(&json!({
                    "store": self.name,
                    "mutation_id": mutation_id.to_string(),
                    "op": {
                        "kind": "delete_row",
                        "key_base64": STANDARD.encode(key),
                    },
                })),
            )
            .await?;
        decode_mutation_outcome(&body)
    }

    /// Empty the store under a `mutation_id` identity — replay dedupes.
    pub async fn clear(&self, mutation_id: Uuid) -> Result<MutationOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/clear",
                Some(&json!({
                    "store": self.name,
                    "mutation_id": mutation_id.to_string(),
                })),
            )
            .await?;
        decode_mutation_outcome(&body)
    }

    /// Point lookup by primary key. `strict` bounded-waits for the
    /// replica's applied sequence to reach its committed snapshot;
    /// unreachable freshness answers `IndexBehind`.
    pub async fn lookup(&self, key: &[u8], strict: bool) -> Result<LookupOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/lookup",
                Some(&json!({
                    "store": self.name,
                    "key_base64": STANDARD.encode(key),
                    "strict": strict,
                })),
            )
            .await?;
        let row = match body.get("found").and_then(Value::as_bool) {
            Some(true) => Some(StoreRow {
                key: key.to_vec(),
                payload: body
                    .get("payload_base64")
                    .and_then(Value::as_str)
                    .and_then(|raw| STANDARD.decode(raw).ok())
                    .ok_or_else(|| {
                        SdkError::Protocol("lookup found a row without payload".to_owned())
                    })?,
            }),
            _ => None,
        };
        Ok(LookupOutcome {
            row,
            freshness: decode_freshness(&body),
        })
    }

    /// Rows whose declared `named_key` posting equals one of `values`.
    /// Bounded by `limit`; `strict` carries the same freshness contract
    /// as `lookup`.
    pub async fn get_by(
        &self,
        named_key: &str,
        values: &[TypedValue],
        limit: Option<u64>,
        strict: bool,
    ) -> Result<GetByPage, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/get-by",
                Some(&json!({
                    "store": self.name,
                    "named_key": named_key,
                    "values": values.iter().map(wire_typed).collect::<Vec<_>>(),
                    "limit": limit,
                    "strict": strict,
                })),
            )
            .await?;
        let rows = body
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|row| decode_store_row(row, "key_base64"))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(GetByPage {
            rows,
            freshness: decode_freshness(&body),
        })
    }

    /// Primary-key ordered page; `after_key` continues from a prior
    /// page's `next_key`, `None` starts at the first row.
    pub async fn iterate(
        &self,
        after_key: Option<&[u8]>,
        limit: Option<u64>,
    ) -> Result<IteratePage, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/iterate",
                Some(&json!({
                    "store": self.name,
                    "after_key_base64": after_key.map(|key| STANDARD.encode(key)),
                    "limit": limit,
                })),
            )
            .await?;
        let rows = body
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|row| decode_store_row(row, "key_base64"))
            .collect::<Result<Vec<_>, _>>()?;
        let next_key = body
            .get("next_key_base64")
            .and_then(Value::as_str)
            .and_then(|raw| STANDARD.decode(raw).ok());
        Ok(IteratePage {
            rows,
            next_key,
            pinned_seq: body.get("pinned_seq").and_then(Value::as_u64).unwrap_or(0),
            served_by: body
                .get("served_by")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        })
    }

    /// Drive one catch-up pass on a feed-sourced store; `complete` means
    /// the committed tip was reached.
    pub async fn sync(&self) -> Result<SyncOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/sync",
                Some(&json!({ "store": self.name })),
            )
            .await?;
        Ok(decode_sync_outcome(&body))
    }

    /// Rebuild a feed-sourced store from retained history; `token` is the
    /// caller's retry identity for the pass.
    pub async fn rebuild(&self, token: Option<Uuid>) -> Result<SyncOutcome, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/state-stores/rebuild",
                Some(&json!({
                    "store": self.name,
                    "token": token.map(|value| value.to_string()),
                })),
            )
            .await?;
        Ok(decode_sync_outcome(&body))
    }
}
