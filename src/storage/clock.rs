//! Pluggable clock for TTL deadlines (testable lazy expire).

use std::sync::{Arc, Mutex};
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
///
/// [`Clone`] shares the underlying instant so a handle kept outside a
/// [`crate::storage::MemoryStorageEngine`] (or behind `Box<dyn StorageEngine>`)
/// can still [`Self::advance`] time.
#[derive(Debug, Clone)]
pub struct FakeClock {
    now: Arc<Mutex<Instant>>,
}

impl FakeClock {
    /// Starts at `Instant::now()` (anchor only; advance via [`Self::set`] / [`Self::advance`]).
    pub fn new() -> Self {
        FakeClock {
            now: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Jump to an absolute instant.
    pub fn set(&self, now: Instant) {
        *self.now.lock().unwrap_or_else(|p| p.into_inner()) = now;
    }

    /// Advance by `duration`.
    pub fn advance(&self, duration: std::time::Duration) {
        *self.now.lock().unwrap_or_else(|p| p.into_inner()) += duration;
    }

    /// Current fake instant (also available via [`Clock::now`]).
    pub fn instant(&self) -> Instant {
        *self.now.lock().unwrap_or_else(|p| p.into_inner())
    }
}

impl Default for FakeClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap_or_else(|p| p.into_inner())
    }
}
