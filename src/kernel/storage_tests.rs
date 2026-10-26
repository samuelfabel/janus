//! Kernel smoke tests over storage plugins injected behind `Box<dyn StorageEngine>`.

use janus_storage::{BTreeStorageEngine, build_storage};
use janus_types::{Command, Response};

use crate::kernel::kernel::Kernel;

const KEY: &[u8] = b"key";
const VALUE: &[u8] = b"value1";

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

#[test]
fn kernel_set_get_delete_with_btree_behind_box() {
    let mut kernel = Kernel::new(BTreeStorageEngine::new());
    assert_eq!(
        kernel.execute(&Command::Set {
            key: KEY,
            value: VALUE,
        }),
        Response::Empty
    );
    assert_eq!(
        kernel.execute(&Command::Get { key: KEY }),
        Response::Value(Some(VALUE.to_vec()))
    );
    assert_eq!(
        kernel.execute(&Command::Delete { key: KEY }),
        Response::Deleted(true)
    );
    assert_eq!(
        kernel.execute(&Command::Get { key: KEY }),
        Response::Value(None)
    );
}
