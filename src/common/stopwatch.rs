/// Stupid and simple implementation of stopwatch.
use std::time::{Duration, Instant};

pub struct Stopwatch {
    start_time: Instant,
}

impl Stopwatch {
    pub fn start_new() -> Stopwatch {
        Stopwatch {
            start_time: Instant::now(),
        }
    }

    pub fn elapsed_ms(&self) -> u64 {
        return self.elapsed().as_millis() as u64;
    }

    pub fn elapsed(&self) -> Duration {
        return self.start_time.elapsed();
    }
}
