//! Local slot ownership for the Kernel (no network endpoints).

use std::collections::HashSet;

use super::SLOT_COUNT;

/// Set of slots this Kernel owns.
///
/// Composition injects ownership; the Kernel never stores host:port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotOwnership {
    kind: OwnershipKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OwnershipKind {
    /// All [`SLOT_COUNT`] slots (single-node default).
    All,
    /// Explicit subset (partial shard).
    Set(HashSet<u16>),
}

impl SlotOwnership {
    /// Own every slot (`0..SLOT_COUNT`).
    pub fn all() -> Self {
        Self {
            kind: OwnershipKind::All,
        }
    }

    /// Own only the slots in `slots` (duplicates ignored; out-of-range ignored).
    pub fn from_slots(slots: impl IntoIterator<Item = u16>) -> Self {
        let set: HashSet<u16> = slots.into_iter().filter(|&s| s < SLOT_COUNT).collect();
        Self {
            kind: OwnershipKind::Set(set),
        }
    }

    /// `true` when this node owns `slot`.
    pub fn contains(&self, slot: u16) -> bool {
        match &self.kind {
            OwnershipKind::All => slot < SLOT_COUNT,
            OwnershipKind::Set(set) => set.contains(&slot),
        }
    }

    /// `true` when ownership covers every Redis Cluster slot.
    pub fn is_full(&self) -> bool {
        match &self.kind {
            OwnershipKind::All => true,
            OwnershipKind::Set(set) => set.len() == SLOT_COUNT as usize,
        }
    }
}

impl Default for SlotOwnership {
    fn default() -> Self {
        Self::all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_owns_every_slot() {
        let ownership = SlotOwnership::all();
        assert!(ownership.is_full());
        assert!(ownership.contains(0));
        assert!(ownership.contains(12182));
        assert!(ownership.contains(SLOT_COUNT - 1));
        assert!(!ownership.contains(SLOT_COUNT));
    }

    #[test]
    fn partial_owns_only_listed_slots() {
        let ownership = SlotOwnership::from_slots([12182u16, 5061]);
        assert!(!ownership.is_full());
        assert!(ownership.contains(12182));
        assert!(ownership.contains(5061));
        assert!(!ownership.contains(0));
    }
}
