//! Subscription members: shared-frontier work claiming.

use serde_json::{json, Value};
use uuid::Uuid;

use crate::client::Client;
use crate::error::SdkError;
use crate::model::{decode_record, Record};

/// One claimed unit of Subscription work a member holds a lease on.
/// `claim` grants the lease; records arrive through `fetch`.
#[derive(Clone, Debug)]
pub struct ClaimedWork {
    pub work_id: Uuid,
    pub lease_epoch: u64,
    pub expires_at_tick: Option<u64>,
}

/// The bounded page `member.fetch` delivers under a live lease. `cursor`
/// and `positions` are the opaque progress evidence `ack` commits back —
/// carry them through `Member::ack`, never reconstruct them.
#[derive(Clone, Debug)]
pub struct WorkPage {
    pub records: Vec<Record>,
    pub cursor: Option<String>,
    /// Opaque per-range progress evidence; internal detail passed back
    /// to `ack` verbatim so no physical layout crosses the SDK contract.
    pub positions: Value,
    /// The delivering claim's lease epoch — the durable per-work
    /// delivery-attempt counter retry budgets count against.
    pub delivery_attempt: u64,
    /// Unix epoch nanos when the earliest delayed head becomes eligible.
    pub next_delivery_ns: Option<i64>,
}

/// A cooperating Subscription member: joins under a generated identity,
/// claims bounded work, and acks committed progress through quorum
/// leases — the shared-frontier consumer-group contract.
#[derive(Clone)]
pub struct Member {
    client: Client,
    subscription: String,
    member_id: Uuid,
    member_epoch: u64,
}

impl Member {
    /// Constructs a member handle without joining — for conformance and
    /// fencing tests that must present an arbitrary member identity.
    #[doc(hidden)]
    pub fn for_testing(
        client: Client,
        subscription: &str,
        member_id: Uuid,
        member_epoch: u64,
    ) -> Self {
        Self {
            client,
            subscription: subscription.to_owned(),
            member_id,
            member_epoch,
        }
    }

    pub fn member_id(&self) -> Uuid {
        self.member_id
    }

    pub fn member_epoch(&self) -> u64 {
        self.member_epoch
    }

    /// Claim one work item up to the member/Subscription caps. A busy
    /// Subscription answers `LeaseBusy`; capacity pressure answers
    /// `LeaseCapacity` — both are typed, retry-safe signals.
    pub async fn claim(&self) -> Result<ClaimedWork, SdkError> {
        let work_id = Uuid::new_v4();
        let body = self
            .client
            .call(
                "POST",
                "/v1/subscriptions/members/claim",
                Some(&json!({
                    "subscription": self.subscription,
                    "request_id": Uuid::new_v4().to_string(),
                    "member_id": self.member_id.to_string(),
                    "member_epoch": self.member_epoch,
                    "work_id": work_id.to_string(),
                })),
            )
            .await?;
        let lease = body.get("lease").ok_or_else(|| {
            SdkError::Protocol("claim succeeded without a granted lease".to_owned())
        })?;
        Ok(ClaimedWork {
            work_id: lease
                .get("work_id")
                .and_then(Value::as_str)
                .and_then(|raw| Uuid::parse_str(raw).ok())
                .unwrap_or(work_id),
            lease_epoch: lease
                .get("lease_epoch")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            expires_at_tick: lease.get("expires_at_tick").and_then(Value::as_u64),
        })
    }

    /// Fetch the bounded page of records bound to a claimed work item.
    /// The returned `cursor`/`positions` are the ack evidence — commit
    /// them with `Member::ack` after processing the page.
    pub async fn fetch(&self, work: &ClaimedWork, limit: u64) -> Result<WorkPage, SdkError> {
        let body = self
            .client
            .call(
                "POST",
                "/v1/subscriptions/members/fetch",
                Some(&json!({
                    "subscription": self.subscription,
                    "request_id": Uuid::new_v4().to_string(),
                    "member_id": self.member_id.to_string(),
                    "member_epoch": self.member_epoch,
                    "work_id": work.work_id.to_string(),
                    "lease_epoch": work.lease_epoch,
                    "limit": limit,
                })),
            )
            .await?;
        let records = body
            .get("records")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(decode_record)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(WorkPage {
            records,
            cursor: body
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
            positions: body.get("positions").cloned().unwrap_or(Value::Null),
            delivery_attempt: body
                .get("delivery_attempt")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            next_delivery_ns: body
                .get("next_delivery_ns")
                .and_then(Value::as_str)
                .and_then(|raw| raw.parse::<i64>().ok()),
        })
    }

    /// Commit the claimed work's progress under the lease epoch, carrying
    /// the fetch page's `cursor`/`positions` evidence verbatim; stale
    /// leases fence as `LeaseStale`/`StaleEpoch`, never silent loss.
    pub async fn ack(&self, work: &ClaimedWork, page: &WorkPage) -> Result<(), SdkError> {
        self.client
            .call(
                "POST",
                "/v1/subscriptions/members/ack",
                Some(&json!({
                    "subscription": self.subscription,
                    "request_id": Uuid::new_v4().to_string(),
                    "member_id": self.member_id.to_string(),
                    "member_epoch": self.member_epoch,
                    "work_id": work.work_id.to_string(),
                    "lease_epoch": work.lease_epoch,
                    "cursor": page.cursor,
                    "positions": page.positions,
                })),
            )
            .await?;
        Ok(())
    }

    /// Renew a held lease; expiry without renew is `LeaseStale` fencing.
    pub async fn renew(&self, work: &ClaimedWork, lease_ticks: u64) -> Result<(), SdkError> {
        self.client
            .call(
                "POST",
                "/v1/subscriptions/members/renew",
                Some(&json!({
                    "subscription": self.subscription,
                    "request_id": Uuid::new_v4().to_string(),
                    "member_id": self.member_id.to_string(),
                    "member_epoch": self.member_epoch,
                    "work_id": work.work_id.to_string(),
                    "lease_epoch": work.lease_epoch,
                    "lease_ticks": lease_ticks,
                })),
            )
            .await?;
        Ok(())
    }

    /// Release a claim without acking: the work redelivers to another
    /// member under a fresh lease.
    pub async fn release(&self, work: &ClaimedWork) -> Result<(), SdkError> {
        self.client
            .call(
                "POST",
                "/v1/subscriptions/members/release",
                Some(&json!({
                    "subscription": self.subscription,
                    "request_id": Uuid::new_v4().to_string(),
                    "member_id": self.member_id.to_string(),
                    "member_epoch": self.member_epoch,
                    "work_id": work.work_id.to_string(),
                    "lease_epoch": work.lease_epoch,
                })),
            )
            .await?;
        Ok(())
    }
}
