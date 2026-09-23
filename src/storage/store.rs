//! Snapshot persistence sink (file or disabled).

use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

use crate::storage::engine::StorageEngine;
use crate::storage::snapshot::{self, SnapshotError};

/// Persists and loads raw snapshot bytes (path owned by the store, not by SAVE).
pub trait SnapshotStore: Send {
    /// Atomically replace the snapshot file contents when possible.
    fn save(&self, bytes: &[u8]) -> io::Result<()>;

    /// `Ok(None)` when the snapshot file is absent.
    fn load(&self) -> io::Result<Option<Vec<u8>>>;
}

/// File-backed snapshot store (`--dbfile` / `JANUS_DBFILE`).
#[derive(Debug, Clone)]
pub struct FileSnapshotStore {
    path: PathBuf,
}

impl FileSnapshotStore {
    /// Creates a store rooted at `path`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        FileSnapshotStore { path: path.into() }
    }

    /// Snapshot file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl SnapshotStore for FileSnapshotStore {
    fn save(&self, bytes: &[u8]) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let tmp = self.path.with_extension("tmp");
        {
            let mut file = fs::File::create(&tmp)?;
            file.write_all(bytes)?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    fn load(&self) -> io::Result<Option<Vec<u8>>> {
        match fs::read(&self.path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err),
        }
    }
}

/// Load snapshot bytes into `engine` when the store has a file.
///
/// Missing file → no-op. Corrupt / invalid snapshot → hard error.
pub fn boot_load(
    engine: &mut (impl StorageEngine + ?Sized),
    store: &impl SnapshotStore,
) -> Result<(), BootError> {
    match store.load().map_err(BootError::Io)? {
        None => Ok(()),
        Some(bytes) => {
            let entries = snapshot::decode(&bytes).map_err(BootError::Corrupt)?;
            engine.import_snapshot(&entries);
            Ok(())
        }
    }
}

/// Failure while applying a snapshot at process start.
#[derive(Debug)]
pub enum BootError {
    Io(io::Error),
    Corrupt(SnapshotError),
}

impl std::fmt::Display for BootError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BootError::Io(err) => write!(f, "snapshot i/o: {err}"),
            BootError::Corrupt(err) => write!(f, "corrupt snapshot: {err}"),
        }
    }
}

impl std::error::Error for BootError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BootError::Io(err) => Some(err),
            BootError::Corrupt(err) => Some(err),
        }
    }
}

impl From<BootError> for io::Error {
    fn from(value: BootError) -> Self {
        match value {
            BootError::Io(err) => err,
            BootError::Corrupt(err) => io::Error::new(io::ErrorKind::InvalidData, err.to_string()),
        }
    }
}
