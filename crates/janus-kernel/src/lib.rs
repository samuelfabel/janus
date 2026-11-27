mod kernel;
pub mod replication;
pub mod slot;

#[cfg(test)]
mod storage_tests;

pub use kernel::{
    Kernel, Role, apply_bootstrap_snapshot, apply_replication_record, is_write, lock_kernel,
};
pub use slot::{SLOT_COUNT, SlotOwnership, slot};
