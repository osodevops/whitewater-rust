//! Declared Pipe operation shapes — `forward`, `enrich`, and `window`
//! — plus the wire bounds the server enforces. Build with
//! [`PipeOperation::forward`], [`PipeOperation::enrich`], or
//! [`PipeOperation::window`], then pass to the matching
//! `Client::define_*_pipe`.

use std::time::Duration;

use serde_json::{json, Value};

use crate::error::SdkError;
use crate::typed::Extractor;

/// The exact nanosecond count a `Duration` carries — the wire encoding.
/// A `Duration` beyond u64 nanoseconds (~584 years) saturates at the
/// wire maximum; the server bounds every declared duration far below
/// that ceiling.
fn duration_ns(value: Duration) -> u64 {
    u64::try_from(value.as_nanos()).unwrap_or(u64::MAX)
}

/// Bound on distinct active group Keys a `window` Pipe keeps in
/// pane/sentinel state — the server enforces the same ceiling.
pub const WINDOW_MAX_KEY_LIMIT: u64 = 512;

/// Bound on accounted pane-state bytes a `window` Pipe keeps — the
/// server enforces the same ceiling.
pub const WINDOW_MAX_BYTE_LIMIT: u64 = 256 * 1024;

/// What a `window` Pipe adds to each pane's accumulator: `count`
/// records, `sum` an extracted integer, or `average` tracking
/// count+sum (readers divide).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowAggregation {
    /// Count records per (group Key, pane).
    Count,
    /// Sum one extracted integer value per pane.
    Sum(Extractor),
    /// Track count and integer sum per pane; readers divide.
    Average(Extractor),
}

impl WindowAggregation {
    /// Count records per (group Key, pane).
    pub fn count() -> Self {
        Self::Count
    }

    /// Sum one extracted integer value per pane.
    pub fn sum(value: Extractor) -> Self {
        Self::Sum(value)
    }

    /// Track count and integer sum per pane; readers divide.
    pub fn average(value: Extractor) -> Self {
        Self::Average(value)
    }

    /// The wire aggregation object.
    pub fn to_wire(&self) -> Value {
        match self {
            Self::Count => json!({ "aggregation": "count" }),
            Self::Sum(value) => {
                json!({ "aggregation": "sum", "value": value.to_wire() })
            }
            Self::Average(value) => {
                json!({ "aggregation": "average", "value": value.to_wire() })
            }
        }
    }
}

/// Which record clock a `window` Pipe buckets into panes. Both clocks
/// are signed 64-bit Unix epoch nanoseconds on the record itself.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WindowTime {
    /// `event_time_ns` — the application-stamped occurrence clock (default).
    #[default]
    Event,
    /// `ingest_time_ns` — the durable-arrival clock.
    Ingest,
}

impl WindowTime {
    /// The wire value (`"event"`/`"ingest"`).
    pub fn to_wire(&self) -> Value {
        match self {
            Self::Event => json!("event"),
            Self::Ingest => json!("ingest"),
        }
    }
}

/// How fresh an `enrich` Pipe lookup row must be before it is journaled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EnrichFreshness {
    /// Serve the replica's applied state (default; the resolution is
    /// journaled either way, so replayed outputs are byte-identical).
    #[default]
    Latest,
    /// Require applied to cover committed at read entry; lag yields a
    /// retryable drive failure.
    Strict,
}

impl EnrichFreshness {
    /// The wire value (`"latest"`/`"strict"`).
    pub fn to_wire(&self) -> Value {
        match self {
            Self::Latest => json!("latest"),
            Self::Strict => json!("strict"),
        }
    }
}

/// What an `enrich` Pipe lookup miss does to the output record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EnrichMiss {
    /// Forward the record with `enrich.matched = false` (default).
    #[default]
    Forward,
    /// Consume without emitting; the input frontier still commits.
    Drop,
    /// Route the miss to the declared quarantine Feed.
    Quarantine,
}

impl EnrichMiss {
    /// The wire value (`"forward"`/`"drop"`/`"quarantine"`).
    pub fn to_wire(&self) -> Value {
        match self {
            Self::Forward => json!("forward"),
            Self::Drop => json!("drop"),
            Self::Quarantine => json!("quarantine"),
        }
    }
}

/// The declaration a `window` [`PipeOperation`] accumulates: fixed
/// `window`-wide panes of a managed StateStore view plus the
/// lateness, retention, and cost bounds the drive enforces. Built by
/// [`PipeOperation::window`]; chain the option setters, then pass to
/// `Client::define_window_pipe`.
///
/// A pane only provably closes under a declared lateness bound —
/// `max_late` or `watermark`, which are alternative models —
/// so `emit_feed` on the define requires one of them, `grace`
/// requires `watermark`, and `expire` must exceed `grace` and
/// `max_late` when they are declared. Lateness bounds are
/// meaningless under the ingest clock; the server — and
/// `Client::define_window_pipe` — refuse those combinations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WindowOperation {
    window_ns: u64,
    key: Extractor,
    aggregation: WindowAggregation,
    time: WindowTime,
    max_late_ns: Option<u64>,
    watermark_ns: Option<u64>,
    grace_ns: Option<u64>,
    expire_ns: Option<u64>,
    key_limit: Option<u64>,
    byte_limit: Option<u64>,
}

impl WindowOperation {
    /// Which record clock assigns panes; [`WindowTime::Event`] default.
    pub fn time(mut self, time: WindowTime) -> Self {
        self.time = time;
        self
    }

    /// Ingest-clock lateness bound: a record arriving further behind
    /// its `event_time_ns` quarantines into a countable `late` row.
    /// Mutually exclusive with `watermark`; event clock only.
    pub fn max_late(mut self, bound: Duration) -> Self {
        self.max_late_ns = Some(duration_ns(bound));
        self
    }

    /// Event-time lateness model: panes close once the durable horizon
    /// `max(event_time) - watermark` passes their end. Mutually
    /// exclusive with `max_late`; event clock only.
    pub fn watermark(mut self, bound: Duration) -> Self {
        self.watermark_ns = Some(duration_ns(bound));
        self
    }

    /// Revision bound — requires `watermark`: an emitted pane still
    /// accepts revising events while `horizon <= pane_end + grace`.
    pub fn grace(mut self, bound: Duration) -> Self {
        self.grace_ns = Some(duration_ns(bound));
        self
    }

    /// Pane retention bound: a pane row deletes once
    /// `pane_end + expire` falls behind the close clock. Must exceed
    /// `grace` and `max_late` when they are declared.
    pub fn expire(mut self, bound: Duration) -> Self {
        self.expire_ns = Some(duration_ns(bound));
        self
    }

    /// Bound on distinct active group Keys in pane/sentinel state.
    pub fn key_limit(mut self, limit: u64) -> Self {
        self.key_limit = Some(limit);
        self
    }

    /// Bound on accounted pane-state bytes.
    pub fn byte_limit(mut self, limit: u64) -> Self {
        self.byte_limit = Some(limit);
        self
    }

    /// The wire `PipeOperation` object.
    pub fn to_wire(&self) -> Value {
        let mut wire = json!({
            "operation": "window",
            "window_ns": self.window_ns,
            "key": self.key.to_wire(),
            "aggregation": self.aggregation.to_wire(),
            "time": self.time.to_wire(),
        });
        let object = wire.as_object_mut().expect("window wire is an object");
        for (name, bound) in [
            ("max_late_ns", self.max_late_ns),
            ("watermark_ns", self.watermark_ns),
            ("grace_ns", self.grace_ns),
            ("expire_ns", self.expire_ns),
            ("key_limit", self.key_limit),
            ("byte_limit", self.byte_limit),
        ] {
            if let Some(bound) = bound {
                object.insert(name.to_owned(), json!(bound));
            }
        }
        wire
    }

    /// The bounds the server can check without catalog state — mirrored
    /// locally so a malformed declaration fails before the round-trip.
    pub(crate) fn validate(&self, emit_feed: bool) -> Result<(), SdkError> {
        fn refused(message: &str) -> SdkError {
            SdkError::Refused(message.to_owned())
        }
        if self.window_ns == 0 {
            return Err(refused("window_ns must be a non-zero duration"));
        }
        self.key.validate()?;
        if let WindowAggregation::Sum(value) | WindowAggregation::Average(value) = &self.aggregation
        {
            value.validate()?;
        }
        let ingest = matches!(self.time, WindowTime::Ingest);
        if let Some(bound) = self.max_late_ns {
            if bound == 0 {
                return Err(refused("max_late_ns requires a non-zero duration"));
            }
            if ingest {
                return Err(refused("max_late_ns is meaningless under the ingest clock"));
            }
        }
        if self.max_late_ns.is_some() && self.watermark_ns.is_some() {
            return Err(refused(
                "max_late_ns and watermark_ns are alternative lateness models — declare exactly one",
            ));
        }
        if let Some(bound) = self.watermark_ns {
            if bound == 0 {
                return Err(refused("watermark_ns requires a non-zero duration"));
            }
            if ingest {
                return Err(refused(
                    "watermark_ns is meaningless under the ingest clock",
                ));
            }
        }
        if let Some(bound) = self.grace_ns {
            if bound == 0 {
                return Err(refused("grace_ns requires a non-zero duration"));
            }
            if self.watermark_ns.is_none() {
                return Err(refused("grace_ns requires watermark_ns"));
            }
        }
        if let Some(expire) = self.expire_ns {
            if expire == 0 {
                return Err(refused("expire_ns requires a non-zero duration"));
            }
            if let Some(grace) = self.grace_ns {
                if expire <= grace {
                    return Err(refused("expire_ns must exceed grace_ns"));
                }
            }
            if let Some(late) = self.max_late_ns {
                if expire <= late {
                    return Err(refused("expire_ns must exceed max_late_ns"));
                }
            }
        }
        if matches!(self.key_limit, Some(0)) {
            return Err(refused("key_limit requires a non-zero bound"));
        }
        if self
            .key_limit
            .map(|limit| limit > WINDOW_MAX_KEY_LIMIT)
            .unwrap_or(false)
        {
            return Err(refused("key_limit accepts at most 512 active Keys"));
        }
        if matches!(self.byte_limit, Some(0)) {
            return Err(refused("byte_limit requires a non-zero bound"));
        }
        if self
            .byte_limit
            .map(|limit| limit > WINDOW_MAX_BYTE_LIMIT)
            .unwrap_or(false)
        {
            return Err(refused("byte_limit accepts at most 256 KiB"));
        }
        // Without a lateness bound no pane ever provably closes — the
        // server refuses `EMIT TO FEED` the same way.
        if emit_feed && self.max_late_ns.is_none() && self.watermark_ns.is_none() {
            return Err(refused("emit_feed requires max_late_ns or watermark_ns"));
        }
        Ok(())
    }
}

/// The processing step a Pipe applies to each delivered input record.
/// Build with [`PipeOperation::forward`], [`PipeOperation::enrich`], or
/// [`PipeOperation::window`]; the wire form is internally tagged on
/// `operation` — see [`PipeOperation::to_wire`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PipeOperation {
    /// Copy each delivered record to the output Feed unchanged.
    Forward,
    /// Point-lookup enrichment: resolve one StateStore row by the
    /// projected `key` and copy its payload into output Metadata
    /// (`enrich.payload`, `enrich.matched`). The resolution rides inside
    /// the effect journal, so replayed outputs are byte-identical.
    Enrich {
        /// Which record value addresses the lookup row's key.
        key: Extractor,
        /// How fresh the lookup row must be; [`EnrichFreshness::Latest`]
        /// is the safe default.
        freshness: EnrichFreshness,
        /// Optional declared named secondary key to resolve through
        /// instead of the store's primary key.
        named: Option<String>,
        /// What a lookup miss does to the output record.
        on_miss: EnrichMiss,
    },
    /// Rolling-window accumulation — see [`WindowOperation`].
    Window(WindowOperation),
}

impl PipeOperation {
    /// Copy each delivered record to the output Feed unchanged.
    pub fn forward() -> Self {
        Self::Forward
    }

    /// Point-lookup enrichment against a StateStore: resolve one row by
    /// the projected `key` and copy its payload into output Metadata
    /// (`enrich.payload`, `enrich.matched`). `named` resolves through a
    /// declared named secondary key instead of the primary key.
    pub fn enrich(
        key: Extractor,
        freshness: EnrichFreshness,
        named: Option<&str>,
        on_miss: EnrichMiss,
    ) -> Self {
        Self::Enrich {
            key,
            freshness,
            named: named.map(str::to_owned),
            on_miss,
        }
    }

    /// Enrich with defaults: latest freshness, forward-on-miss.
    pub fn enrich_default(key: Extractor) -> Self {
        Self::enrich(key, EnrichFreshness::Latest, None, EnrichMiss::Forward)
    }

    /// Accumulate each delivered record into fixed `window`-wide
    /// panes of a managed StateStore view, grouped by `key` under
    /// `aggregation`. Returns the [`WindowOperation`] declaration —
    /// chain the lateness/retention/limit setters on it before passing
    /// it to `Client::define_window_pipe`.
    pub fn window(
        window: Duration,
        key: Extractor,
        aggregation: WindowAggregation,
    ) -> Result<WindowOperation, SdkError> {
        let window_ns = duration_ns(window);
        if window_ns == 0 {
            return Err(SdkError::Refused(
                "window must be a non-zero duration".to_owned(),
            ));
        }
        Ok(WindowOperation {
            window_ns,
            key,
            aggregation,
            time: WindowTime::Event,
            max_late_ns: None,
            watermark_ns: None,
            grace_ns: None,
            expire_ns: None,
            key_limit: None,
            byte_limit: None,
        })
    }

    /// The wire `PipeOperation` object — internally tagged on
    /// `operation`.
    pub fn to_wire(&self) -> Value {
        match self {
            Self::Forward => json!({ "operation": "forward" }),
            Self::Enrich {
                key,
                freshness,
                named,
                on_miss,
            } => {
                let mut wire = json!({
                    "operation": "enrich",
                    "key": key.to_wire(),
                    "freshness": freshness.to_wire(),
                    "on_miss": on_miss.to_wire(),
                });
                if let Some(named) = named {
                    wire["named"] = json!(named);
                }
                wire
            }
            Self::Window(window) => window.to_wire(),
        }
    }
}

impl From<WindowOperation> for PipeOperation {
    fn from(window: WindowOperation) -> Self {
        Self::Window(window)
    }
}
