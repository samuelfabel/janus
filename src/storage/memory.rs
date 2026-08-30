//! In-memory [`StorageEngine`] backed by a [`HashMap`], with optional TTL.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::storage::{
    clock::{Clock, SystemClock},
    engine::{StorageEngine, Ttl},
};

struct Entry {
    value: Vec<u8>,
    deadline: Option<Instant>,
}

/// HashMap-backed storage engine with lazy key expiry.
pub struct MemoryStorageEngine<C: Clock = SystemClock> {
    map: HashMap<Vec<u8>, Entry>,
    clock: C,
}

impl MemoryStorageEngine<SystemClock> {
    /// Creates an empty in-memory engine using [`SystemClock`].
    pub fn new() -> Self {
        MemoryStorageEngine::with_clock(SystemClock)
    }
}

impl<C: Clock> MemoryStorageEngine<C> {
    /// Creates an empty engine with an injected clock (tests).
    pub fn with_clock(clock: C) -> Self {
        MemoryStorageEngine {
            map: HashMap::new(),
            clock,
        }
    }

    /// Shared clock handle for tests that advance time.
    pub fn clock_mut(&mut self) -> &mut C {
        &mut self.clock
    }

    fn purge_if_expired(&mut self, key: &[u8]) -> bool {
        let now = self.clock.now();
        let expired = matches!(
            self.map.get(key),
            Some(Entry {
                deadline: Some(deadline),
                ..
            }) if *deadline <= now
        );
        if expired {
            self.map.remove(key);
            true
        } else {
            false
        }
    }
}

impl Default for MemoryStorageEngine<SystemClock> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: Clock> StorageEngine for MemoryStorageEngine<C> {
    fn get(&mut self, key: &[u8]) -> Option<&[u8]> {
        self.purge_if_expired(key);
        self.map.get(key).map(|e| e.value.as_slice())
    }

    fn set(&mut self, key: &[u8], value: &[u8]) {
        self.map.insert(
            key.to_vec(),
            Entry {
                value: value.to_vec(),
                deadline: None,
            },
        );
    }

    fn delete(&mut self, key: &[u8]) -> bool {
        if self.purge_if_expired(key) {
            return false;
        }
        self.map.remove(key).is_some()
    }

    fn expire_at(&mut self, key: &[u8], deadline: Instant) -> bool {
        if self.purge_if_expired(key) {
            return false;
        }
        match self.map.get_mut(key) {
            Some(entry) => {
                entry.deadline = Some(deadline);
                true
            }
            None => false,
        }
    }

    fn ttl(&mut self, key: &[u8]) -> Ttl {
        if self.purge_if_expired(key) {
            return Ttl::Missing;
        }
        match self.map.get(key) {
            None => Ttl::Missing,
            Some(Entry {
                deadline: None, ..
            }) => Ttl::NoExpiry,
            Some(Entry {
                deadline: Some(deadline),
                ..
            }) => {
                let now = self.clock.now();
                if *deadline <= now {
                    // Should have been purged; treat as missing.
                    self.map.remove(key);
                    Ttl::Missing
                } else {
                    Ttl::Remaining(deadline.saturating_duration_since(now))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{clock::FakeClock, engine::StorageEngine};

    const KEY: &[u8] = b"key";
    const VALUE: &[u8] = b"value1";
    const VALUE2: &[u8] = b"value2";

    #[test]
    fn set_then_get_returns_value() {
        let mut engine = MemoryStorageEngine::new();
        engine.set(KEY, VALUE);
        assert_eq!(engine.get(KEY), Some(VALUE));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let mut engine = MemoryStorageEngine::new();
        assert_eq!(engine.get(KEY), None);
    }

    #[test]
    fn set_overwrites_previous_value() {
        let mut engine = MemoryStorageEngine::new();
        engine.set(KEY, VALUE);
        engine.set(KEY, VALUE2);
        assert_eq!(engine.get(KEY), Some(VALUE2));
    }

    #[test]
    fn delete_existing_key_returns_true_and_removes() {
        let mut engine = MemoryStorageEngine::new();
        engine.set(KEY, VALUE);
        assert!(engine.delete(KEY));
        assert_eq!(engine.get(KEY), None);
    }

    #[test]
    fn delete_missing_key_returns_false() {
        let mut engine = MemoryStorageEngine::new();
        assert!(!engine.delete(KEY));
    }

    #[test]
    fn empty_key_and_empty_value_are_allowed() {
        let mut engine = MemoryStorageEngine::new();
        engine.set(b"", b"");
        assert_eq!(engine.get(b""), Some(b"".as_slice()));
        assert!(engine.delete(b""));
        assert_eq!(engine.get(b""), None);
    }

    #[test]
    fn binary_opaque_bytes_roundtrip() {
        let mut engine = MemoryStorageEngine::new();
        let key = b"\x00\xffkey";
        let value = b"val\r\n\x00ue";
        engine.set(key, value);
        assert_eq!(engine.get(key), Some(value.as_slice()));
    }

    #[test]
    fn expire_at_future_get_hits() {
        let mut engine = MemoryStorageEngine::with_clock(FakeClock::new());
        let start = engine.clock_mut().instant();
        engine.set(KEY, VALUE);
        assert!(engine.expire_at(KEY, start + Duration::from_secs(10)));
        assert_eq!(engine.get(KEY), Some(VALUE));
    }

    #[test]
    fn expire_at_past_get_misses_and_purges() {
        let mut engine = MemoryStorageEngine::with_clock(FakeClock::new());
        let start = engine.clock_mut().instant();
        engine.set(KEY, VALUE);
        assert!(engine.expire_at(KEY, start + Duration::from_secs(1)));
        engine.clock_mut().advance(Duration::from_secs(2));
        assert_eq!(engine.get(KEY), None);
        // Second get still miss (purged).
        assert_eq!(engine.get(KEY), None);
    }

    #[test]
    fn ttl_no_expiry_remaining_missing() {
        let mut engine = MemoryStorageEngine::with_clock(FakeClock::new());
        let start = engine.clock_mut().instant();
        assert_eq!(engine.ttl(KEY), Ttl::Missing);

        engine.set(KEY, VALUE);
        assert_eq!(engine.ttl(KEY), Ttl::NoExpiry);

        assert!(engine.expire_at(KEY, start + Duration::from_secs(5)));
        match engine.ttl(KEY) {
            Ttl::Remaining(d) => assert!(d <= Duration::from_secs(5) && d > Duration::ZERO),
            other => panic!("unexpected {other:?}"),
        }

        engine.clock_mut().advance(Duration::from_secs(6));
        assert_eq!(engine.ttl(KEY), Ttl::Missing);
    }

    #[test]
    fn expire_at_missing_returns_false() {
        let mut engine = MemoryStorageEngine::with_clock(FakeClock::new());
        let now = engine.clock_mut().instant();
        assert!(!engine.expire_at(KEY, now + Duration::from_secs(1)));
    }

    #[test]
    fn set_clears_previous_ttl() {
        let mut engine = MemoryStorageEngine::with_clock(FakeClock::new());
        let start = engine.clock_mut().instant();
        engine.set(KEY, VALUE);
        assert!(engine.expire_at(KEY, start + Duration::from_secs(1)));
        engine.set(KEY, VALUE2);
        assert_eq!(engine.ttl(KEY), Ttl::NoExpiry);
        engine.clock_mut().advance(Duration::from_secs(5));
        assert_eq!(engine.get(KEY), Some(VALUE2));
    }

    #[test]
    fn delete_expired_returns_false() {
        let mut engine = MemoryStorageEngine::with_clock(FakeClock::new());
        let start = engine.clock_mut().instant();
        engine.set(KEY, VALUE);
        assert!(engine.expire_at(KEY, start + Duration::from_millis(1)));
        engine.clock_mut().advance(Duration::from_secs(1));
        assert!(!engine.delete(KEY));
    }
}
