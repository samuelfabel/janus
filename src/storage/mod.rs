pub mod btree;
pub mod clock;
pub mod engine;
pub mod memory;
pub mod snapshot;
pub mod store;
pub mod wal;

pub use btree::BTreeStorageEngine;
pub use clock::{Clock, FakeClock, SystemClock};
pub use engine::{StorageEngine, Ttl};
pub use memory::MemoryStorageEngine;
pub use snapshot::{SnapshotEntry, SnapshotError, decode, encode};
pub use store::{BootError, FileSnapshotStore, SnapshotStore, boot_load};
pub use wal::{
    WalError, WalRecord, WalWriter, boot_wal, replay as replay_wal, unix_now_secs,
};

/// Composition-root factory: default production storage plugin (`MemoryStorageEngine`).
///
/// Callers inject the returned `Box<dyn StorageEngine>` into [`crate::kernel::kernel::Kernel`].
/// Tests may substitute [`BTreeStorageEngine`] (or another implementor) the same way.
pub fn build_storage() -> Box<dyn StorageEngine> {
    Box::new(MemoryStorageEngine::new())
}

#[cfg(test)]
mod factory_tests {
    use super::*;
    use crate::{command::types::Command, kernel::kernel::Kernel, response::types::Response};

    #[test]
    fn build_storage_default_supports_set_get() {
        let mut kernel = Kernel::from_boxed(build_storage());
        assert_eq!(
            kernel.execute(&Command::Set {
                key: b"k",
                value: b"v",
            }),
            Response::Empty
        );
        assert_eq!(
            kernel.execute(&Command::Get { key: b"k" }),
            Response::Value(Some(b"v".to_vec()))
        );
    }
}
