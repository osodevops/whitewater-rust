//! Session-bound Readers.

use serde_json::json;
use uuid::Uuid;

use crate::client::Client;
use crate::error::SdkError;
use crate::model::{decode_record, FetchPage};

/// A declared Reader bound to a session epoch. Delivery is
/// at-least-once; `ack` persists the page's `delivered_cursor` — never a
/// record's own cursor, which may not cover other ranges.
#[derive(Clone)]
pub struct Reader {
    pub(crate) client: Client,
    pub(crate) name: String,
    pub(crate) session_epoch: u64,
    pub(crate) delivered_cursor: Option<String>,
    pub(crate) acknowledged_cursor: Option<String>,
}

impl Reader {
    pub fn session_epoch(&self) -> u64 {
        self.session_epoch
    }

    /// The declared Reader name this session fetches under.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn acknowledged_cursor(&self) -> Option<&str> {
        self.acknowledged_cursor.as_deref()
    }

    /// Fetch one bounded page. The page's `delivered_cursor` is the ack
    /// target; an ambiguous failure makes the identical retry safe.
    pub async fn fetch(&mut self, limit: u64) -> Result<FetchPage, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/readers/fetch",
                Some(&json!({
                    "request_id": Uuid::new_v4().to_string(),
                    "reader": self.name,
                    "session_epoch": self.session_epoch,
                    "limit": limit,
                })),
            )
            .await?;
        let records = body
            .get("records")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(decode_record)
            .collect::<Result<Vec<_>, _>>()?;
        self.delivered_cursor = body
            .get("delivered_cursor")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| self.delivered_cursor.clone());
        self.acknowledged_cursor = body
            .get("acknowledged_cursor")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or_else(|| self.acknowledged_cursor.clone());
        Ok(FetchPage {
            records,
            delivered_cursor: self.delivered_cursor.clone(),
            acknowledged_cursor: self.acknowledged_cursor.clone(),
            recommended_capacity: body
                .get("recommended_capacity")
                .and_then(serde_json::Value::as_u64),
            retry_after_ms: body
                .get("retry_after_ms")
                .and_then(serde_json::Value::as_u64),
        })
    }

    /// Acknowledge the latest fetched page's `delivered_cursor`. Passing
    /// `None` acks nothing — the durable point stays put.
    pub async fn ack(&mut self) -> Result<(), SdkError> {
        let Some(cursor) = self.delivered_cursor.clone() else {
            return Ok(());
        };
        let body = self
            .client
            .call(
                "POST",
                "/v1/readers/ack",
                Some(&json!({
                    "request_id": Uuid::new_v4().to_string(),
                    "reader": self.name,
                    "session_epoch": self.session_epoch,
                    "cursor": cursor,
                })),
            )
            .await?;
        self.acknowledged_cursor = body
            .get("acknowledged_cursor")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .or(Some(cursor));
        Ok(())
    }

    /// Close the session; the acknowledged cursor stays the durable
    /// restart point for the next `open`.
    pub async fn close(self) -> Result<(), SdkError> {
        self.client
            .call(
                "POST",
                "/v1/readers/close",
                Some(&json!({
                    "request_id": Uuid::new_v4().to_string(),
                    "reader": self.name,
                    "session_epoch": self.session_epoch,
                })),
            )
            .await?;
        Ok(())
    }
}
