//! Internal chdig observability counters, rendered into the status bar when toggled with `!`.
//!
//! Metrics are recorded unconditionally — the cost is two atomic ops per worker event plus a
//! lock-and-push on a ~256-entry ring buffer. Display is gated on a toggle flag: when off
//! the refresh thread sleeps and does not ping the event loop, so there is no UI cost either.
//!
//! Picks:
//! - Nearest-rank percentile over a fixed-size [`Histogram`] (O(N log N) per snapshot,
//!   N≤256). Simpler than an online estimator (t-digest, HDR histogram) and accurate enough
//!   for a status bar at a few Hz.
//! - Event-loop latency is measured as a `cb_sink` round-trip, not frame render time.
//!   Cursive does not expose per-frame hooks; round-trip drift is the quantity the user
//!   actually perceives as "responsiveness". Tracked as a histogram (not a single latest
//!   value) so transient spikes don't get hidden behind whatever the most recent ping saw.
//! - [`InFlightGuard`] is an RAII guard so early returns and panics in the worker cannot
//!   leak the counter.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use cursive::CbSink;

const SAMPLES_CAPACITY: usize = 256;

/// Fixed-capacity ring-buffer histogram over `Duration` samples. Thread-safe via an
/// internal `Mutex` — contention is negligible at the rates we record (≤ a few Hz).
pub struct Histogram {
    samples: Mutex<VecDeque<Duration>>,
}

impl Histogram {
    fn new() -> Self {
        Histogram {
            samples: Mutex::new(VecDeque::with_capacity(SAMPLES_CAPACITY)),
        }
    }

    pub fn record(&self, d: Duration) {
        let mut s = self.samples.lock().unwrap();
        if s.len() == SAMPLES_CAPACITY {
            s.pop_front();
        }
        s.push_back(d);
    }

    /// Nearest-rank (p50, p90, p99). Returns zeros on an empty histogram.
    pub fn percentiles(&self) -> (Duration, Duration, Duration) {
        let s = self.samples.lock().unwrap();
        if s.is_empty() {
            return (Duration::ZERO, Duration::ZERO, Duration::ZERO);
        }
        let mut v: Vec<Duration> = s.iter().copied().collect();
        v.sort_unstable();
        (percentile(&v, 50), percentile(&v, 90), percentile(&v, 99))
    }
}

pub struct DebugMetrics {
    shown: AtomicBool,
    in_flight: AtomicU64,
    /// `cb_sink` round-trip latency — proxy for "how responsive does chdig feel".
    ui_lag: Histogram,
    /// Per-worker-event processing duration (a worker event is one ClickHouse query /
    /// action chdig issued).
    event: Histogram,
}

#[must_use = "Drop decrements the in-flight counter; hold this for the duration of work"]
pub struct InFlightGuard(Arc<DebugMetrics>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl DebugMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(DebugMetrics {
            shown: AtomicBool::new(false),
            in_flight: AtomicU64::new(0),
            ui_lag: Histogram::new(),
            event: Histogram::new(),
        })
    }

    pub fn is_shown(&self) -> bool {
        self.shown.load(Ordering::Relaxed)
    }

    /// Flips visibility and returns the new state.
    pub fn toggle_shown(&self) -> bool {
        !self.shown.fetch_xor(true, Ordering::Relaxed)
    }

    pub fn track_in_flight(self: &Arc<Self>) -> InFlightGuard {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        InFlightGuard(Arc::clone(self))
    }

    pub fn record_event(&self, d: Duration) {
        self.event.record(d);
    }

    pub fn record_ui_lag(&self, d: Duration) {
        self.ui_lag.record(d);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let (lag_p50, lag_p90, lag_p99) = self.ui_lag.percentiles();
        let (evt_p50, evt_p90, evt_p99) = self.event.percentiles();
        MetricsSnapshot {
            in_flight: self.in_flight.load(Ordering::Relaxed),
            lag_p50,
            lag_p90,
            lag_p99,
            evt_p50,
            evt_p90,
            evt_p99,
        }
    }

    /// Spawn a background thread that, *while visibility is on*, probes event-loop lag
    /// via a `cb_sink` round-trip and pushes the latest snapshot into the status bar.
    /// When visibility is off the thread sleeps, so the hidden cost is just a dormant
    /// thread (no cb_sink traffic, no redraws). Exits when the sink is closed.
    pub fn spawn_refresh(self: &Arc<Self>, cb_sink: CbSink, interval: Duration) {
        let metrics = Arc::clone(self);
        thread::Builder::new()
            .name("chdig-debug-metrics".into())
            .spawn(move || refresh_loop(metrics, cb_sink, interval))
            .expect("spawn chdig-debug-metrics");
    }
}

fn refresh_loop(metrics: Arc<DebugMetrics>, cb_sink: CbSink, interval: Duration) {
    loop {
        thread::sleep(interval);
        if !metrics.is_shown() {
            continue;
        }
        let sent_at = Instant::now();
        let metrics = Arc::clone(&metrics);
        let send_result = cb_sink.send(Box::new(move |siv: &mut cursive::Cursive| {
            metrics.record_ui_lag(sent_at.elapsed());
            let text = metrics.snapshot().to_string();
            crate::view::Navigation::set_statusbar_debug(siv, text);
        }));
        if send_result.is_err() {
            break;
        }
    }
}

#[derive(Default, Clone, Copy)]
pub struct MetricsSnapshot {
    pub in_flight: u64,
    pub lag_p50: Duration,
    pub lag_p90: Duration,
    pub lag_p99: Duration,
    pub evt_p50: Duration,
    pub evt_p90: Duration,
    pub evt_p99: Duration,
}

impl fmt::Display for MetricsSnapshot {
    /// Status-bar line; written to be readable without a legend:
    ///   * `UI lag`   – cb_sink round-trip percentiles (event loop responsiveness)
    ///   * `Active`   – worker events currently being processed
    ///   * `Event`    – worker-event processing-time percentiles (one per ClickHouse query)
    ///
    /// All triples are `p50/p90/p99`, nearest-rank over the last [`SAMPLES_CAPACITY`]
    /// samples of each kind.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "UI lag p50/p90/p99: {}/{}/{} ms  Active: {}  Event p50/p90/p99: {}/{}/{} ms",
            self.lag_p50.as_millis(),
            self.lag_p90.as_millis(),
            self.lag_p99.as_millis(),
            self.in_flight,
            self.evt_p50.as_millis(),
            self.evt_p90.as_millis(),
            self.evt_p99.as_millis(),
        )
    }
}

/// Nearest-rank percentile; q ∈ 0..=100. Undefined on an empty slice — callers must guard.
fn percentile<T: Copy>(sorted: &[T], q: u32) -> T {
    debug_assert!(q <= 100);
    debug_assert!(!sorted.is_empty());
    let rank = (q as usize * sorted.len()).div_ceil(100).max(1);
    sorted[rank - 1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_integer_ranks() {
        let v: Vec<u64> = (1..=10).collect();
        assert_eq!(percentile(&v, 50), 5);
        assert_eq!(percentile(&v, 90), 9);
        assert_eq!(percentile(&v, 99), 10);
        assert_eq!(percentile(&v, 100), 10);
    }

    #[test]
    fn percentile_single_element() {
        assert_eq!(percentile(&[42u64], 50), 42);
        assert_eq!(percentile(&[42u64], 99), 42);
    }

    #[test]
    fn histogram_caps_at_capacity() {
        let h = Histogram::new();
        // Feed monotonic samples well past capacity and assert that the p99 reflects
        // only the most recent SAMPLES_CAPACITY values (earliest ones were evicted).
        let total = SAMPLES_CAPACITY + 50;
        for i in 0..total {
            h.record(Duration::from_millis(i as u64));
        }
        let (_p50, _p90, p99) = h.percentiles();
        // Oldest retained = total - SAMPLES_CAPACITY = 50; newest = total - 1 = 305.
        // Nearest-rank p99: rank = ceil(99 * 256 / 100) = 254; value = 50 + (254-1) = 303.
        assert_eq!(p99, Duration::from_millis(303));
    }

    #[test]
    fn histogram_empty_returns_zero() {
        let h = Histogram::new();
        assert_eq!(
            h.percentiles(),
            (Duration::ZERO, Duration::ZERO, Duration::ZERO)
        );
    }

    #[test]
    fn ui_lag_and_event_are_independent() {
        let m = DebugMetrics::new();
        m.record_ui_lag(Duration::from_millis(5));
        m.record_event(Duration::from_millis(500));
        let s = m.snapshot();
        assert_eq!(s.lag_p50, Duration::from_millis(5));
        assert_eq!(s.evt_p50, Duration::from_millis(500));
    }

    #[test]
    fn in_flight_guard_is_raii() {
        let m = DebugMetrics::new();
        assert_eq!(m.snapshot().in_flight, 0);
        let g1 = m.track_in_flight();
        let g2 = m.track_in_flight();
        assert_eq!(m.snapshot().in_flight, 2);
        drop(g1);
        assert_eq!(m.snapshot().in_flight, 1);
        drop(g2);
        assert_eq!(m.snapshot().in_flight, 0);
    }

    #[test]
    fn toggle_shown_returns_new_state() {
        let m = DebugMetrics::new();
        assert!(!m.is_shown());
        assert!(m.toggle_shown());
        assert!(m.is_shown());
        assert!(!m.toggle_shown());
        assert!(!m.is_shown());
    }

    #[test]
    fn display_format_is_readable() {
        let s = MetricsSnapshot {
            in_flight: 3,
            lag_p50: Duration::from_millis(1),
            lag_p90: Duration::from_millis(4),
            lag_p99: Duration::from_millis(12),
            evt_p50: Duration::from_millis(12),
            evt_p90: Duration::from_millis(87),
            evt_p99: Duration::from_millis(420),
        };
        let rendered = s.to_string();
        assert!(rendered.contains("UI lag p50/p90/p99: 1/4/12 ms"));
        assert!(rendered.contains("Active: 3"));
        assert!(rendered.contains("Event p50/p90/p99: 12/87/420 ms"));
    }
}
