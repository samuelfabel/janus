//! Pluggable clock for TTL deadlines (testable lazy expire).

use std::time::Instant;

/// Source of "now" for expiry checks.
pub trait Clock: Send {
    /// Current monotonic instant.
    fn now(&self) -> Instant;
}

/// Production clock: [`Instant::now`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Test clock with a controllable instant.
#[derive(Debug, Clone)]
pub struct FakeClock {
    now: Instant,
}

impl FakeClock {
    /// Starts at `Instant::now()` (anchor only; advance via [`Self::set`] / [`Self::advance`]).
    pub fn new() -> Self {
        FakeClock {
            now: Instant::now(),
        }
    }

    /// Jump to an absolute instant.
    pub fn set(&mut self, now: Instant) {
        self.now = now;
    }

    /// Advance by `duration`.
    pub fn advance(&mut self, duration: std::time::Duration) {
        self.now += duration;
    }

    /// Current fake instant (also available via [`Clock::now`]).
    pub fn instant(&self) -> Instant {
        self.now
    }
}

impl Default for FakeClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Instant {
        self.now
    }
}
