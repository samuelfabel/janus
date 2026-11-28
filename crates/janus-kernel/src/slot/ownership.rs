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

    /// Add `slot` to the owned set (no-op when already full / already present).
    pub fn insert(&mut self, slot: u16) {
        if slot >= SLOT_COUNT {
            return;
        }
        match &mut self.kind {
            OwnershipKind::All => {}
            OwnershipKind::Set(set) => {
                set.insert(slot);
                if set.len() == SLOT_COUNT as usize {
                    self.kind = OwnershipKind::All;
                }
            }
        }
    }

    /// Remove `slot` from ownership.
    ///
    /// Removing from a full map materializes an explicit set of the remaining
    /// slots (rare control-plane path during resharding).
    pub fn remove(&mut self, slot: u16) {
        if slot >= SLOT_COUNT {
            return;
        }
        match &mut self.kind {
            OwnershipKind::All => {
                let mut set: HashSet<u16> = (0..SLOT_COUNT).collect();
                set.remove(&slot);
                self.kind = OwnershipKind::Set(set);
            }
            OwnershipKind::Set(set) => {
                set.remove(&slot);
            }
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

    #[test]
    fn insert_and_remove_update_set() {
        let mut ownership = SlotOwnership::from_slots([5061u16]);
        ownership.insert(12182);
        assert!(ownership.contains(12182));
        ownership.remove(5061);
        assert!(!ownership.contains(5061));
        assert!(ownership.contains(12182));
    }
}
