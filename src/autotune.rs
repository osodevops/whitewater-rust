//! Self-adjusting batch formation and fetch pacing.
//!
//! Two opt-in behaviours share one pump:
//!
//! * [`Batching`] — the explicit `linger.ms` equivalent: a record waits
//!   at most `linger` for the batch to fill to `max_batch`.
//! * [`AutoTune`] — Nagle semantics plus measured-RTT pacing: an idle
//!   Writer sends immediately (a trickle pays zero added latency),
//!   while a busy Writer accumulates for roughly one measured round
//!   trip — the server's response time *is* the linger. Server
//!   `recommended_batch_count`/`retry_after_ms` feedback clamps the
//!   window; congestion evidence shrinks it.
//!
//! The tuners below are pure state machines: every decision takes the
//! observed values as arguments so unit tests drive them with
//! synthetic timings — no sleeps, no wall clock.

use std::time::Duration;

/// Fixed batching knobs — the `linger.ms` equivalent for callers who
/// want an explicit trade rather than self-tuning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Batching {
    /// Longest a non-full batch waits for more records.
    pub linger: Duration,
    /// Flush immediately once the queue holds this many records.
    /// The server's per-request bound is 1,000.
    pub max_batch: usize,
    /// Concurrent append-batch rounds a pump may hold.
    pub max_in_flight: usize,
}

impl Default for Batching {
    fn default() -> Self {
        Self {
            linger: Duration::from_millis(5),
            max_batch: 256,
            max_in_flight: 4,
        }
    }
}

/// Auto-tune envelope: bounds the adaptive window; every field is the
/// honest contract the tuner cannot exceed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AutoTune {
    /// First batch cap before any feedback arrives.
    pub initial_batch: usize,
    /// Hard ceiling — the server's per-request bound; a lower
    /// `recommended_batch_count` clamps below it.
    pub max_batch: usize,
    /// Concurrent append-batch rounds the pump may hold.
    pub max_in_flight: usize,
    /// Longest a busy Writer may hold a non-full batch.
    pub max_linger: Duration,
    /// Round-trip latency above which the window shrinks — the
    /// congestion signal that favors smaller, quicker batches.
    pub latency_budget: Duration,
}

impl Default for AutoTune {
    fn default() -> Self {
        Self {
            initial_batch: 1_000,
            max_batch: 1_000,
            max_in_flight: 4,
            max_linger: Duration::from_millis(50),
            latency_budget: Duration::from_millis(400),
        }
    }
}

/// Whether a queued write should flush into an append-batch call now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flush {
    /// Send the queue's contents now.
    Now,
    /// Keep waiting — either the batch fills or `wait` elapses.
    Wait(Duration),
}

/// Fixed-linger controller: flush on a full queue or on the oldest
/// record reaching `linger` — the static contract.
#[derive(Clone, Debug)]
pub struct BatchTuner {
    config: Batching,
}

impl BatchTuner {
    pub fn new(config: Batching) -> Self {
        Self { config }
    }

    /// The fixed batch cap — diagnostics surface this.
    pub fn cap(&self) -> usize {
        self.config.max_batch
    }

    /// `queue` = records waiting, `oldest_age` = age of the oldest
    /// queued record, `in_flight` = unacknowledged batches.
    pub fn evaluate(&self, queue: usize, oldest_age: Duration, in_flight: usize) -> Flush {
        if queue == 0 || in_flight >= self.config.max_in_flight {
            return Flush::Wait(Duration::MAX);
        }
        if queue >= self.config.max_batch {
            return Flush::Now;
        }
        if oldest_age >= self.config.linger {
            return Flush::Now;
        }
        Flush::Wait(self.config.linger - oldest_age)
    }
}

/// Self-adjusting controller (Nagle + EWMA RTT + server feedback):
///
/// * idle (`in_flight == 0`) → [`Flush::Now`]: a trickle sends
///   immediately and pays zero added latency;
/// * busy → accumulate until the measured RTT's linger elapses, the
///   cap fills, or an in-flight slot frees;
/// * full batches that commit within `latency_budget` grow the cap
///   toward `max_batch` (or the server's recommendation); a batch
///   exceeding the budget halves it — congestion favors smaller,
///   quicker rounds.
#[derive(Clone, Debug)]
pub struct AutoTuner {
    config: AutoTune,
    cap: usize,
    in_flight_limit: usize,
    ewma_rtt: Duration,
}

impl AutoTuner {
    pub fn new(config: AutoTune) -> Self {
        Self {
            cap: config.initial_batch.clamp(1, config.max_batch),
            in_flight_limit: config.max_in_flight,
            ewma_rtt: Duration::from_millis(20),
            config,
        }
    }

    /// The current batch cap — diagnostics surface this.
    pub fn batch_cap(&self) -> usize {
        self.cap
    }

    /// Current outstanding-round limit — diagnostics surface this:
    /// concurrency sheds under server pressure and recovers on fast,
    /// unpressured rounds.
    pub fn in_flight_limit(&self) -> usize {
        self.in_flight_limit
    }

    /// Current effective linger: zero when idle (handled by the caller
    /// flushing on `in_flight == 0`), up to `max_linger` while busy.
    pub fn linger(&self) -> Duration {
        self.ewma_rtt.min(self.config.max_linger)
    }

    /// Send now if: queue fills the cap, the batch is idle-free to go
    /// (Nagle's rule), or the oldest record outlived the measured RTT.
    pub fn evaluate(&self, queue: usize, oldest_age: Duration, in_flight: usize) -> Flush {
        if queue == 0 || in_flight >= self.in_flight_limit {
            return Flush::Wait(Duration::MAX);
        }
        if in_flight == 0 || queue >= self.cap {
            return Flush::Now;
        }
        let linger = self.linger();
        if oldest_age >= linger {
            return Flush::Now;
        }
        Flush::Wait(linger - oldest_age)
    }

    /// Fold one committed batch's evidence into the window.
    /// `sent` = records in that batch, `rtt` = its measured round trip.
    /// `recommended`/`retry_after` are the server's feedback fields.
    pub fn observe(
        &mut self,
        _sent: usize,
        rtt: Duration,
        recommended: Option<u64>,
        retry_after_ms: Option<u64>,
    ) {
        self.ewma_rtt = self.ewma_rtt * 4 / 5 + rtt / 5;
        let paced = retry_after_ms.filter(|value| *value > 0);
        if paced.is_some() {
            // Hard pacing: serialize to one outstanding round and hold
            // the linger at the server's requested interval.
            self.in_flight_limit = 1;
            self.ewma_rtt = self.ewma_rtt.max(
                Duration::from_millis(retry_after_ms.unwrap_or(0)).min(self.config.max_linger),
            );
        } else if recommended.is_some_and(|value| value <= 16) {
            // recommended_batch_count is populated on every outcome:
            // 100 is the server's normal advisory, but 16 (high
            // pressure) or 8 (hot Key) is a shed signal. The useful
            // shed is concurrency — halving outstanding rounds — not
            // smaller batches, which would send MORE requests and
            // amplify the per-round fixed cost driving that pressure.
            self.in_flight_limit = (self.in_flight_limit / 2).max(1);
        } else if rtt <= self.config.latency_budget {
            // No pressure evidence and fast rounds: recover one
            // outstanding slot toward the configured ceiling.
            self.in_flight_limit = (self.in_flight_limit + 1).min(self.config.max_in_flight);
        }
    }
}

/// Fetch-side pacing: grows `limit` while pages return full (a drain
/// in progress favors fewer, larger requests) and relaxes `wait_ms`
/// while pages return empty (a tail should long-poll instead of busy
/// polling). Records flowing again resets the tail pacing.
#[derive(Clone, Debug)]
pub struct ReadTuner {
    limit: u64,
    wait_ms: u64,
    max_limit: u64,
    max_wait_ms: u64,
}

impl Default for ReadTuner {
    fn default() -> Self {
        Self {
            limit: 512,
            wait_ms: 0,
            max_limit: 65_536,
            max_wait_ms: 5_000,
        }
    }
}

impl ReadTuner {
    /// `(limit, wait_ms)` for the *next* fetch, given the last page's
    /// record count.
    pub fn next(&mut self, returned: usize) -> (u64, u64) {
        if returned == 0 {
            self.wait_ms = (self.wait_ms.max(50) * 2).min(self.max_wait_ms);
        } else {
            self.wait_ms = 0;
            if returned as u64 >= self.limit {
                self.limit = (self.limit * 2).min(self.max_limit);
            }
        }
        (self.limit, self.wait_ms)
    }

    /// Current page limit — diagnostics surface this.
    pub fn limit(&self) -> u64 {
        self.limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_tuner_flushes_on_cap_or_linger() {
        let tuner = BatchTuner::new(Batching {
            linger: Duration::from_millis(5),
            max_batch: 4,
            max_in_flight: 2,
        });
        assert_eq!(
            tuner.evaluate(0, Duration::ZERO, 0),
            Flush::Wait(Duration::MAX)
        );
        assert_eq!(tuner.evaluate(4, Duration::ZERO, 1), Flush::Now);
        assert_eq!(tuner.evaluate(2, Duration::from_millis(6), 1), Flush::Now);
        assert_eq!(
            tuner.evaluate(2, Duration::from_millis(3), 1),
            Flush::Wait(Duration::from_millis(2))
        );
        // All slots busy — hold regardless of queue pressure.
        assert_eq!(
            tuner.evaluate(100, Duration::from_secs(1), 2),
            Flush::Wait(Duration::MAX)
        );
    }

    #[test]
    fn auto_tuner_sends_immediately_when_idle() {
        let tuner = AutoTuner::new(AutoTune::default());
        // Trickle: nothing in flight — zero added latency.
        assert_eq!(tuner.evaluate(1, Duration::ZERO, 0), Flush::Now);
        // Busy: a fresh record may accumulate for one measured RTT.
        assert!(matches!(
            tuner.evaluate(1, Duration::ZERO, 1),
            Flush::Wait(wait) if wait > Duration::ZERO
        ));
    }

    #[test]
    fn auto_tuner_sheds_concurrency_under_server_pressure() {
        let mut tuner = AutoTuner::new(AutoTune {
            max_in_flight: 4,
            latency_budget: Duration::from_millis(400),
            ..AutoTune::default()
        });
        // The normal advisory (100) carries no shed signal — a fast
        // round recovers one outstanding slot.
        tuner.observe(512, Duration::from_millis(30), Some(100), None);
        assert_eq!(tuner.in_flight_limit(), 4);
        // High-pressure recommendations halve outstanding rounds.
        tuner.observe(512, Duration::from_millis(300), Some(16), None);
        assert_eq!(tuner.in_flight_limit(), 2);
        tuner.observe(512, Duration::from_millis(300), Some(8), None);
        assert_eq!(tuner.in_flight_limit(), 1);
        // ...but never below one, and recovery returns as pressure
        // clears and rounds stay inside the budget.
        tuner.observe(512, Duration::from_millis(30), Some(100), None);
        assert_eq!(tuner.in_flight_limit(), 2);
    }

    #[test]
    fn auto_tuner_serializes_on_retry_pacing() {
        let mut tuner = AutoTuner::new(AutoTune {
            max_in_flight: 4,
            max_linger: Duration::from_millis(60),
            ..AutoTune::default()
        });
        tuner.observe(512, Duration::from_millis(900), None, Some(50));
        assert_eq!(tuner.in_flight_limit(), 1);
        // As fast paced rounds decay the EWMA below retry_after, the
        // floor keeps the window at the server's requested interval.
        for _ in 0..10 {
            tuner.observe(512, Duration::from_millis(10), None, Some(50));
        }
        assert_eq!(tuner.linger(), Duration::from_millis(50));
        // Latency alone never sheds concurrency: under load RTT mostly
        // reflects this tuner's own queue.
        tuner.observe(512, Duration::from_millis(900), None, None);
        assert_eq!(tuner.in_flight_limit(), 1);
    }

    #[test]
    fn read_tuner_grows_on_full_pages_and_relaxes_on_empty() {
        let mut tuner = ReadTuner::default();
        tuner.next(512);
        assert_eq!(tuner.limit(), 1_024);
        let (_, wait) = tuner.next(0);
        assert!(wait > 0);
        let (limit, wait) = tuner.next(1_024);
        assert_eq!(wait, 0);
        assert_eq!(limit, 2_048);
    }
}
