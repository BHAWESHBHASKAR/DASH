//! Lock-free fixed-bucket histograms.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::exposition::MetricsWriter;

/// Request latency buckets in seconds (0.5 ms .. 10 s).
pub const LATENCY_SECONDS_BUCKETS: &[f64] = &[
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// `fsync` / write latency buckets in seconds (50 us .. 2.5 s).
pub const FSYNC_SECONDS_BUCKETS: &[f64] = &[
    0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
    2.5,
];

/// Buckets for slow background operations in seconds (10 ms .. 10 min):
/// checkpoints, vector index save and load.
pub const SLOW_OPERATION_SECONDS_BUCKETS: &[f64] = &[
    0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0,
];

/// Buckets for batch sizes (entries per batch).
pub const BATCH_SIZE_BUCKETS: &[f64] = &[
    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0,
];

/// A fixed-bucket histogram that can be shared between threads and observed
/// without locks. Buckets hold non-cumulative counts; the last slot is the
/// overflow (`+Inf`) bucket. Rendering derives `_count` and the `+Inf` bucket
/// from the same bucket snapshot, so a scrape racing an observation still
/// produces a consistent (cumulative, `+Inf == _count`) family.
#[derive(Debug)]
pub struct Histogram {
    bounds: &'static [f64],
    buckets: Box<[AtomicU64]>,
    sum_bits: AtomicU64,
}

/// A point-in-time copy of a [`Histogram`].
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSnapshot {
    pub bounds: &'static [f64],
    /// Non-cumulative counts, `bounds.len() + 1` entries.
    pub buckets: Vec<u64>,
    pub sum: f64,
}

impl HistogramSnapshot {
    pub fn count(&self) -> u64 {
        self.buckets.iter().sum()
    }
}

impl Histogram {
    /// `bounds` must be strictly increasing and finite.
    pub fn new(bounds: &'static [f64]) -> Self {
        debug_assert!(bounds.windows(2).all(|w| w[0] < w[1]));
        debug_assert!(bounds.iter().all(|b| b.is_finite()));
        let buckets = (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect();
        Self {
            bounds,
            buckets,
            sum_bits: AtomicU64::new(0f64.to_bits()),
        }
    }

    /// Record one observation. Negative and NaN values are clamped to 0.
    pub fn observe(&self, value: f64) {
        let value = if value.is_nan() || value < 0.0 {
            0.0
        } else {
            value
        };
        let idx = self
            .bounds
            .iter()
            .position(|bound| value <= *bound)
            .unwrap_or(self.bounds.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        let mut current = self.sum_bits.load(Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(current) + value).to_bits();
            match self.sum_bits.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Record a duration in seconds.
    pub fn observe_duration(&self, elapsed: Duration) {
        self.observe(elapsed.as_secs_f64());
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            bounds: self.bounds,
            buckets: self
                .buckets
                .iter()
                .map(|b| b.load(Ordering::Relaxed))
                .collect(),
            sum: f64::from_bits(self.sum_bits.load(Ordering::Relaxed)),
        }
    }

    pub fn count(&self) -> u64 {
        self.snapshot().count()
    }

    /// Render the whole family (header plus one series without labels).
    pub fn render(&self, w: &mut MetricsWriter, name: &str, help: &str) {
        w.histogram(name, help, &[], &self.snapshot());
    }
}

/// A histogram owned by a single writer (for example under a mutex).
#[derive(Debug, Clone, PartialEq)]
pub struct LocalHistogram {
    bounds: &'static [f64],
    buckets: Vec<u64>,
    sum: f64,
}

impl LocalHistogram {
    pub fn new(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            buckets: vec![0; bounds.len() + 1],
            sum: 0.0,
        }
    }

    pub fn observe(&mut self, value: f64) {
        let value = if value.is_nan() || value < 0.0 {
            0.0
        } else {
            value
        };
        let idx = self
            .bounds
            .iter()
            .position(|bound| value <= *bound)
            .unwrap_or(self.bounds.len());
        self.buckets[idx] += 1;
        self.sum += value;
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            bounds: self.bounds,
            buckets: self.buckets.clone(),
            sum: self.sum,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observations_land_in_the_first_bucket_whose_bound_is_not_exceeded() {
        let h = Histogram::new(&[1.0, 2.0, 5.0]);
        for v in [0.5, 1.0, 1.5, 5.0, 7.0, -3.0, f64::NAN] {
            h.observe(v);
        }
        let snap = h.snapshot();
        assert_eq!(snap.buckets, vec![4, 1, 1, 1]);
        assert_eq!(snap.count(), 7);
        assert!((snap.sum - 15.0).abs() < 1e-9);
    }

    #[test]
    fn concurrent_observations_are_all_counted() {
        let h = std::sync::Arc::new(Histogram::new(LATENCY_SECONDS_BUCKETS));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let h = std::sync::Arc::clone(&h);
                std::thread::spawn(move || {
                    for i in 0..1000 {
                        h.observe(f64::from(i) / 1000.0);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(h.count(), 4000);
        let expected: f64 = 4.0 * (0..1000).map(|i| f64::from(i) / 1000.0).sum::<f64>();
        assert!((h.snapshot().sum - expected).abs() < 1e-6);
    }

    #[test]
    fn local_histogram_matches_the_shared_one() {
        let shared = Histogram::new(BATCH_SIZE_BUCKETS);
        let mut local = LocalHistogram::new(BATCH_SIZE_BUCKETS);
        for v in [1.0, 3.0, 100.0, 4096.0] {
            shared.observe(v);
            local.observe(v);
        }
        assert_eq!(shared.snapshot(), local.snapshot());
    }
}
