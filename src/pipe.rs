//! Declared Pipes: lifecycle operations over the shared-frontier
//! compute contract.

use serde_json::{json, Value};
use uuid::Uuid;

use crate::client::Client;
use crate::error::SdkError;

/// The catalog row `DESCRIBE PIPE`/`SHOW PIPES` returns for a declared
/// Pipe. `operation` stays an opaque [`Value`] — Forward, Window, and
/// Enrich variants evolve server-side without an SDK type bump.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipeInfo {
    pub name: String,
    /// `declared` while driven, `paused` under `PAUSE PIPE`.
    pub stage: String,
    pub subscription_id: Uuid,
    pub operation: Value,
    pub output_feed_id: Option<Uuid>,
    pub output_store_id: Option<Uuid>,
    pub status: String,
    pub created_at_ns: i64,
}

fn pipe_info(value: &Value) -> Option<PipeInfo> {
    let uuid = |field: &str| {
        value
            .get(field)
            .and_then(Value::as_str)
            .and_then(|raw| raw.parse::<Uuid>().ok())
    };
    Some(PipeInfo {
        name: value.get("name")?.as_str()?.to_owned(),
        stage: value.get("stage")?.as_str()?.to_owned(),
        subscription_id: uuid("subscription_id")?,
        operation: value.get("operation").cloned().unwrap_or(Value::Null),
        output_feed_id: uuid("output_feed_id"),
        output_store_id: uuid("output_store_id"),
        status: value.get("status")?.as_str()?.to_owned(),
        created_at_ns: value.get("created_at_ns")?.as_i64()?,
    })
}

pub(crate) fn decode_pipes(rows: &[Value]) -> Vec<PipeInfo> {
    rows.iter().filter_map(pipe_info).collect()
}

/// A declared Pipe's lifecycle surface. Pausing is cooperative — an
/// in-flight effect finishes and the frontier stays put so redelivery
/// stays replay-safe — and resume restarts driving from the same
/// durable frontier.
pub struct Pipe {
    client: Client,
    name: String,
}

impl Pipe {
    pub(crate) fn new(client: Client, name: String) -> Self {
        Self { client, name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// `DESCRIBE PIPE`: the live catalog row including stage and the
    /// opaque operation definition.
    pub async fn describe(&self) -> Result<PipeInfo, SdkError> {
        let results = self
            .client
            .commands(vec![json!({
                "command": "describe",
                "kind": "pipe",
                "name": self.name,
            })])
            .await?;
        let data = results
            .first()
            .and_then(|result| result.get("data"))
            .cloned()
            .unwrap_or(Value::Null);
        decode_pipes(&[data])
            .into_iter()
            .next()
            .ok_or_else(|| SdkError::Protocol("describe pipe returned no definition".to_owned()))
    }

    /// `PAUSE PIPE`: halts every drive cooperatively; idempotent.
    pub async fn pause(&self) -> Result<(), SdkError> {
        self.lifecycle("pause_pipe").await
    }

    /// `RESUME PIPE`: restarts driving from the durable frontier.
    pub async fn resume(&self) -> Result<(), SdkError> {
        self.lifecycle("resume_pipe").await
    }

    /// `DROP PIPE`: retires the declaration and its compute lease.
    pub async fn drop(self) -> Result<(), SdkError> {
        self.client
            .commands(vec![json!({
                "command": "drop",
                "kind": "pipe",
                "name": self.name,
            })])
            .await
            .map(|_| ())
    }

    async fn lifecycle(&self, command: &str) -> Result<(), SdkError> {
        self.client
            .commands(vec![json!({ "command": command, "name": self.name })])
            .await
            .map(|_| ())
    }
}
