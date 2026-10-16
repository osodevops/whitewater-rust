//! Typed failure surface. `(status, code)` on the wire maps here; the
//! `error` text is never branched on.

use serde_json::Value;

/// Typed failure surfaced to the application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SdkError {
    /// Writer/Reader/member epoch superseded: reopen the session.
    StaleEpoch,
    /// Sequence gap on the raw feed-append path.
    Sequence,
    /// Subscription work item claimed elsewhere; claim different work.
    LeaseBusy,
    /// Work lease expired or superseded.
    LeaseStale,
    /// Member or Subscription work cap reached; back off.
    LeaseCapacity,
    /// Generic fencing conflict without a wire `code`.
    Conflict(String),
    /// 5xx after an uncertain outcome: retry the identical request.
    Ambiguous(String),
    /// Quorum/replica unavailable: safe to retry the identical request.
    Unavailable(String),
    /// `index_behind`: strict freshness could not be reached inside the
    /// bounded wait; the read may be retried.
    IndexBehind,
    /// Refused before commit (syntax, validation, authentication).
    Refused(String),
    /// Server reports a different protocol major.
    IncompatibleServer {
        expected: u64,
        reported: Option<String>,
    },
    /// Transport-level failure before a status line arrived.
    Transport(String),
    /// A reply that violates the wire schema.
    Protocol(String),
    /// A bound [`crate::Codec`] failed to encode or decode a value.
    Codec(String),
}

impl SdkError {
    /// Whether an identical-`request_id` retry can settle the outcome.
    pub fn retryable(&self) -> bool {
        matches!(
            self,
            SdkError::Ambiguous(_)
                | SdkError::Unavailable(_)
                | SdkError::Transport(_)
                | SdkError::IndexBehind
        )
    }
}

impl std::fmt::Display for SdkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SdkError::StaleEpoch => write!(f, "session epoch superseded"),
            SdkError::Sequence => write!(f, "sequence gap"),
            SdkError::LeaseBusy => write!(f, "work claimed elsewhere"),
            SdkError::LeaseStale => write!(f, "work lease expired or superseded"),
            SdkError::LeaseCapacity => write!(f, "subscription work cap reached"),
            SdkError::Conflict(detail) => write!(f, "conflict: {detail}"),
            SdkError::Ambiguous(detail) => write!(f, "ambiguous outcome: {detail}"),
            SdkError::Unavailable(detail) => write!(f, "unavailable: {detail}"),
            SdkError::IndexBehind => {
                write!(f, "index has not reached the requested freshness")
            }
            SdkError::Refused(detail) => write!(f, "refused: {detail}"),
            SdkError::IncompatibleServer { expected, reported } => write!(
                f,
                "server protocol major {reported:?} incompatible with {expected}"
            ),
            SdkError::Transport(detail) => write!(f, "transport: {detail}"),
            SdkError::Protocol(detail) => write!(f, "protocol violation: {detail}"),
            SdkError::Codec(detail) => write!(f, "codec: {detail}"),
        }
    }
}

impl std::error::Error for SdkError {}

pub(crate) fn map_error(status: u16, body: &Value) -> SdkError {
    let detail = body
        .get("error")
        .and_then(Value::as_str)
        .unwrap_or("no detail")
        .to_owned();
    let code = body.get("code").and_then(Value::as_str);
    match (status, code) {
        (409, Some("stale_epoch")) => SdkError::StaleEpoch,
        (409, Some("sequence")) => SdkError::Sequence,
        (409, Some("lease.busy")) => SdkError::LeaseBusy,
        (409, Some("lease.stale_lease")) => SdkError::LeaseStale,
        (409, Some("lease.capacity")) => SdkError::LeaseCapacity,
        (409, Some("index_behind")) => SdkError::IndexBehind,
        (409, _) => SdkError::Conflict(detail),
        (503, _) => SdkError::Unavailable(detail),
        _ if status >= 500 => SdkError::Ambiguous(detail),
        _ => SdkError::Refused(format!("{status}: {detail}")),
    }
}
