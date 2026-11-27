mod kernel;
pub mod replication;
pub mod slot;

#[cfg(test)]
mod storage_tests;

pub use kernel::{
    apply_bootstrap_snapshot, apply_replication_record, is_write, lock_kernel, ERR_CROSSSLOT,
    ERR_SLOT_NOT_OWNED, Kernel, Role,
};
pub use slot::{slot, SLOT_COUNT, SlotOwnership};
