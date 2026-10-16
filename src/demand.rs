//! Demand observability: per-resource attribution counters and
//! per-domain rollups served by `/v1/admin/demand/*`. Counter names are
//! a server-owned vocabulary that evolves without an SDK bump, so rows
//! keep their fields verbatim and expose typed accessors for the stable
//! identity columns — the same contract as `PipeInfo::operation`.

use serde_json::Value;

use crate::client::Client;
use crate::error::SdkError;

/// One demand row for a declared resource (Feed, Reader, Subscription,
/// Index, or Pipe). `fields` carries the server's counters verbatim.
#[derive(Clone, Debug)]
pub struct DemandRow {
    /// The resource's declared name.
    pub name: String,
    /// Owning Domain when the server attributes one.
    pub domain: Option<String>,
    /// Server-side counters verbatim (`appended_records`, `drives`,
    /// `queries`, …) — new counters arrive without an SDK update.
    pub fields: Value,
}

impl DemandRow {
    /// One counter as `u64` when present.
    pub fn counter(&self, field: &str) -> Option<u64> {
        self.fields.get(field).and_then(Value::as_u64)
    }
}

/// A demand snapshot for one resource kind: per-resource rows plus the
/// per-domain rollup map (domain name → aggregated counters).
#[derive(Clone, Debug)]
pub struct DemandReport {
    pub rows: Vec<DemandRow>,
    /// `{"<domain>": {<counters>}}` — opaque rollup rows keyed by Domain.
    pub domains: Value,
}

/// Evidence that one Feed Key dominates its traffic window. A strictly
/// ordered hot Key is never parallelized — the honest operator signal
/// is to spread or shed load for the Key, not add capacity.
#[derive(Clone, Debug)]
pub struct HotKey {
    /// The evidence object verbatim — includes any range placement
    /// fields the server reports; those are diagnostics, not a public
    /// placement contract.
    pub evidence: Value,
}

impl HotKey {
    /// The Feed carrying the dominant Key, when reported.
    pub fn feed(&self) -> Option<&str> {
        self.evidence.get("feed").and_then(Value::as_str)
    }

    /// Dominant-Key share of the window in basis points.
    pub fn share_bps(&self) -> Option<u64> {
        self.evidence.get("share_bps").and_then(Value::as_u64)
    }

    /// Records attributed to the Key inside the sampled window.
    pub fn window_records(&self) -> Option<u64> {
        self.evidence.get("window_records").and_then(Value::as_u64)
    }

    /// Bytes attributed to the Key inside the sampled window.
    pub fn window_bytes(&self) -> Option<u64> {
        self.evidence.get("window_bytes").and_then(Value::as_u64)
    }
}

fn decode_report(body: &Value, resource_key: &str, name_field: &str) -> DemandReport {
    let rows = body
        .get(resource_key)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|row| DemandRow {
            name: row
                .get(name_field)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            domain: row.get("domain").and_then(Value::as_str).map(str::to_owned),
            fields: row.clone(),
        })
        .collect();
    DemandReport {
        rows,
        domains: body.get("domains").cloned().unwrap_or(Value::Null),
    }
}

impl Client {
    /// `GET /v1/admin/demand/feeds` — per-Feed append/read/replication/
    /// movement counters plus the per-Domain rollup.
    pub async fn demand_feeds(&self) -> Result<DemandReport, SdkError> {
        let body = self.call("GET", "/v1/admin/demand/feeds", None).await?;
        Ok(decode_report(&body, "feeds", "feed"))
    }

    /// `GET /v1/admin/demand/readers` — per-Reader request/fetch/ack
    /// counters plus the per-Domain rollup.
    pub async fn demand_readers(&self) -> Result<DemandReport, SdkError> {
        let body = self.call("GET", "/v1/admin/demand/readers", None).await?;
        Ok(decode_report(&body, "readers", "reader"))
    }

    /// `GET /v1/admin/demand/subscriptions` — per-Subscription request,
    /// join/claim/fetch/ack, and member counters plus rollup.
    pub async fn demand_subscriptions(&self) -> Result<DemandReport, SdkError> {
        let body = self
            .call("GET", "/v1/admin/demand/subscriptions", None)
            .await?;
        Ok(decode_report(&body, "subscriptions", "subscription"))
    }

    /// `GET /v1/admin/demand/indexes` — per-Index query counters plus
    /// the per-Domain rollup.
    pub async fn demand_indexes(&self) -> Result<DemandReport, SdkError> {
        let body = self.call("GET", "/v1/admin/demand/indexes", None).await?;
        Ok(decode_report(&body, "indexes", "index"))
    }

    /// `GET /v1/admin/demand/pipes` — per-Pipe drive/apply/failure
    /// counters plus the per-Domain rollup.
    pub async fn demand_pipes(&self) -> Result<DemandReport, SdkError> {
        let body = self.call("GET", "/v1/admin/demand/pipes", None).await?;
        Ok(decode_report(&body, "pipes", "pipe"))
    }

    /// `GET /v1/admin/demand/hot-keys` — dominant-Key evidence across
    /// live ranges.
    pub async fn hot_keys(&self) -> Result<Vec<HotKey>, SdkError> {
        let body = self.call("GET", "/v1/admin/demand/hot-keys", None).await?;
        Ok(body
            .get("hot_keys")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|evidence| HotKey { evidence })
            .collect())
    }
}
