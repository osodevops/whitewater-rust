//! Session-bound Readers.

use serde_json::{json, Value};
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
    /// Sticky endpoint: session state (pacing, cursors) is Node-local.
    pub(crate) pinned: std::sync::Arc<std::sync::Mutex<String>>,
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
        self.fetch_inner(limit, None).await
    }

    /// Acknowledge the previous page's `delivered_cursor` (when present
    /// and unacknowledged) and fetch the next page in one round-trip.
    pub async fn fetch_and_ack(&mut self, limit: u64) -> Result<FetchPage, SdkError> {
        let ack_cursor = self
            .delivered_cursor
            .clone()
            .filter(|delivered| self.acknowledged_cursor.as_ref() != Some(delivered));
        self.fetch_inner(limit, ack_cursor).await
    }

    async fn fetch_inner(
        &mut self,
        limit: u64,
        ack_cursor: Option<String>,
    ) -> Result<FetchPage, SdkError> {
        let mut payload = json!({
            "request_id": Uuid::new_v4().to_string(),
            "reader": self.name,
            "session_epoch": self.session_epoch,
            "limit": limit,
        });
        if let Some(ack_cursor) = ack_cursor {
            payload["ack_cursor"] = json!(ack_cursor);
        }
        // Binary first: the page's `next_cursor` slot carries
        // `delivered_cursor` and the pacing fields ride `x-whitewater-*`
        // response headers, so a bulk drain never pays the server-side
        // decode + base64 + JSON pass.
        match self
            .client
            .session_call_frame_page(&self.pinned, "/v1/readers/fetch", Some(&payload))
            .await
        {
            Ok(reply) if (200..300).contains(&reply.status) => {
                if let Some((records, next)) = reply.page {
                    let header = |name: &str| {
                        reply
                            .headers
                            .iter()
                            .find(|(key, _)| key.eq_ignore_ascii_case(name))
                            .map(|(_, value)| value.clone())
                    };
                    self.delivered_cursor = next.or_else(|| self.delivered_cursor.clone());
                    self.acknowledged_cursor = header("x-whitewater-acknowledged-cursor")
                        .or_else(|| self.acknowledged_cursor.clone());
                    return Ok(FetchPage {
                        records,
                        delivered_cursor: self.delivered_cursor.clone(),
                        acknowledged_cursor: self.acknowledged_cursor.clone(),
                        recommended_capacity: header("x-whitewater-recommended-capacity")
                            .and_then(|value| value.parse::<u64>().ok()),
                        retry_after_ms: header("x-whitewater-retry-after-ms")
                            .and_then(|value| value.parse::<u64>().ok()),
                    });
                }
                // The Node ignored the Accept and answered JSON.
                let body: serde_json::Value =
                    serde_json::from_slice(&reply.body).unwrap_or(Value::Null);
                return self.page_from_json(body);
            }
            Ok(reply) => {
                let body: serde_json::Value =
                    serde_json::from_slice(&reply.body).unwrap_or(Value::Null);
                return Err(crate::error::map_error(reply.status, &body));
            }
            // Scripted transports without binary support fall through to
            // the JSON call.
            Err(SdkError::Refused(_)) => {}
            Err(error) => return Err(error),
        }
        let body = self
            .client
            .session_call(&self.pinned, "POST", "/v1/readers/fetch", Some(&payload))
            .await?;
        self.page_from_json(body)
    }

    /// Assemble the `FetchPage` from the JSON response shape — shared by
    /// the JSON fallback and the binary arm's JSON-body reply.
    fn page_from_json(&mut self, body: serde_json::Value) -> Result<FetchPage, SdkError> {
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
            .session_call(
                &self.pinned,
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
            .session_call(
                &self.pinned,
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

/// Adaptive-limit Reader: pages grow while they return full (a drain
/// favors fewer, larger requests) — the read-side sibling of
/// [`crate::PumpedWriter`]'s batch tuning. Tail pacing stays with the
/// server's `retry_after_ms`, which [`FeedConsumer`](crate::FeedConsumer)
/// pumps already honor.
pub struct AutoReader {
    inner: Reader,
    tuner: crate::autotune::ReadTuner,
    limit: u64,
}

impl Reader {
    /// Fetch with a self-tuned page limit instead of a fixed capacity.
    pub fn autotuned(self) -> AutoReader {
        let tuner = crate::autotune::ReadTuner::default();
        let limit = tuner.limit();
        AutoReader {
            inner: self,
            tuner,
            limit,
        }
    }
}

impl AutoReader {
    /// Fetch one page at the tuner's current limit, then fold the
    /// page's fill into the next limit decision.
    pub async fn fetch(&mut self) -> Result<FetchPage, SdkError> {
        let page = self.inner.fetch(self.limit).await?;
        let (limit, _) = self.tuner.next(page.records.len());
        self.limit = limit;
        Ok(page)
    }

    /// Piggyback acknowledgement of the previous page onto the next
    /// adaptive fetch — halves RPCs per page in steady-state drains.
    pub async fn fetch_and_ack(&mut self) -> Result<FetchPage, SdkError> {
        let page = self.inner.fetch_and_ack(self.limit).await?;
        let (limit, _) = self.tuner.next(page.records.len());
        self.limit = limit;
        Ok(page)
    }

    /// Acknowledge the latest fetched page — same contract as
    /// [`Reader::ack`].
    pub async fn ack(&mut self) -> Result<(), SdkError> {
        self.inner.ack().await
    }

    /// Current adaptive page limit — diagnostics surface this.
    pub fn limit(&self) -> u64 {
        self.limit
    }
}
