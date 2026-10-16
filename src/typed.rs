//! Typed Index/StateStore field values and Index query shapes.

use serde_json::{json, Value};

use crate::error::SdkError;
use crate::model::STANDARD;
use base64::Engine;

/// One typed Index/StateStore field value — the wire contract shared by
/// `query_index`, `state_store.get_by`, and named-key mutations. Bytes
/// travel base64 like every binary field on the API.
#[derive(Clone, Debug, PartialEq)]
pub enum TypedValue {
    Null,
    Bool(bool),
    I64(i64),
    Text(String),
    Bytes(Vec<u8>),
}

pub(crate) fn wire_typed(value: &TypedValue) -> Value {
    match value {
        TypedValue::Null => json!({"kind": "null"}),
        TypedValue::Bool(value) => json!({"kind": "bool", "value": value}),
        TypedValue::I64(value) => json!({"kind": "i64", "value": value}),
        TypedValue::Text(value) => json!({"kind": "text", "value": value}),
        TypedValue::Bytes(value) => {
            json!({"kind": "bytes", "value_base64": STANDARD.encode(value)})
        }
    }
}

/// Inclusive/exclusive bound for an Index range query.
#[derive(Clone, Debug)]
pub struct IndexBound {
    pub value: TypedValue,
    pub inclusive: bool,
}

/// The query shape `Client::query_index` lowers onto the wire.
#[derive(Clone, Debug)]
pub enum IndexQuery {
    /// Every declared field, exact match.
    Exact(Vec<TypedValue>),
    /// Leading fields exact, plus an optional text prefix on the next.
    Prefix {
        leading: Vec<TypedValue>,
        partial_text: Option<String>,
    },
    /// Leading fields exact, the next field between `lower`/`upper`.
    Range {
        leading: Vec<TypedValue>,
        lower: IndexBound,
        upper: IndexBound,
    },
}

/// A page returned by `Client::query_index`.
#[derive(Clone, Debug)]
pub struct IndexPage {
    /// Decoded `(key, payload)` posting rows.
    pub rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// Active generation that served — changes on rebuild.
    pub generation: u64,
    pub applied_cursor: Option<String>,
    pub target_cursor: Option<String>,
    /// Serving Node still materializing the activation frontier.
    pub behind: bool,
    /// The page hit a server bound; narrow the query to continue.
    pub truncated: bool,
    /// Node identity that served — diagnostics, never routing input.
    pub served_by: String,
}

pub(crate) fn decode_index_page(body: &Value) -> Result<IndexPage, SdkError> {
    let rows = body
        .get("rows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|row| {
            let decode = |field: &str| -> Result<Vec<u8>, SdkError> {
                row.get(field)
                    .and_then(Value::as_str)
                    .and_then(|raw| STANDARD.decode(raw).ok())
                    .ok_or_else(|| SdkError::Protocol(format!("index row missing {field}")))
            };
            Ok((decode("key_base64")?, decode("payload_base64")?))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(IndexPage {
        rows,
        generation: body.get("generation").and_then(Value::as_u64).unwrap_or(0),
        applied_cursor: body
            .get("applied_cursor")
            .and_then(Value::as_str)
            .map(str::to_owned),
        target_cursor: body
            .get("target_cursor")
            .and_then(Value::as_str)
            .map(str::to_owned),
        behind: body.get("behind").and_then(Value::as_bool).unwrap_or(false),
        truncated: body
            .get("truncated")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        served_by: body
            .get("served_by")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
    })
}
