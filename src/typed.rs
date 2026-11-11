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

/// How a committed record projects to a typed field value — the
/// `IndexFieldSpec` the wire carries. Declared on StateStore named keys
/// (feed-sourced stores) and on Pipe enrich/window operations; applied
/// by server-side replay and drive. Manual stores leave it unset and
/// take values on each `put_row` instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Extractor {
    /// The source record's application Key.
    RecordKey,
    /// A named record Metadata entry.
    Metadata(String),
    /// A JSON-path projection over the record payload.
    Json(String),
}

impl Extractor {
    /// The source record's application Key.
    pub fn record_key() -> Self {
        Self::RecordKey
    }

    /// A named record Metadata entry.
    pub fn metadata(name: impl Into<String>) -> Result<Self, SdkError> {
        let name = name.into();
        if name.trim().is_empty() || name.len() > 256 {
            return Err(SdkError::Refused(
                "metadata extractor name must be 1..=256 bytes".to_owned(),
            ));
        }
        Ok(Self::Metadata(name))
    }

    /// A `json` version-1 path projection over the record payload.
    pub fn json(field: impl Into<String>) -> Result<Self, SdkError> {
        let field = field.into();
        if field.trim().is_empty() || field.len() > 512 {
            return Err(SdkError::Refused(
                "json extractor field must be 1..=512 bytes".to_owned(),
            ));
        }
        Ok(Self::Json(field))
    }

    /// The wire `IndexFieldSpec` object the catalog stores.
    pub fn to_wire(&self) -> Value {
        match self {
            Self::RecordKey => json!({ "kind": "key" }),
            Self::Metadata(name) => json!({ "kind": "metadata", "name": name }),
            Self::Json(field) => json!({
                "kind": "projection",
                "extractor": "json",
                "version": 1,
                "field": field,
            }),
        }
    }

    /// The field-spec bounds the server enforces on declarations;
    /// enum-literal construction re-checks here so a malformed spec
    /// still fails before it reaches the wire.
    pub(crate) fn validate(&self) -> Result<(), SdkError> {
        let invalid = || SdkError::Refused(format!("invalid field spec {}", self.to_wire()));
        match self {
            Self::RecordKey => Ok(()),
            Self::Metadata(name) if !name.is_empty() && name.len() <= 256 => Ok(()),
            Self::Json(field) if !field.is_empty() && field.len() <= 512 => Ok(()),
            _ => Err(invalid()),
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

impl From<&str> for TypedValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for TypedValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<i64> for TypedValue {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<bool> for TypedValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<Vec<u8>> for TypedValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}
