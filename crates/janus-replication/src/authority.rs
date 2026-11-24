//! Minimal config authority: publish the current primary RESP endpoint.
//!
//! Lives outside the Kernel (control plane ≠ data plane). No gossip, Raft, or
//! Sentinel protocol — ops and clients read the published endpoint and re-point.

use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

/// Publishes and exposes the current primary client endpoint.
#[derive(Debug)]
pub struct ConfigAuthority {
    primary: Mutex<Option<String>>,
    path: Option<PathBuf>,
}

impl ConfigAuthority {
    /// In-memory authority (tests / in-process supervisor).
    pub fn in_memory() -> Self {
        Self {
            primary: Mutex::new(None),
            path: None,
        }
    }

    /// File-backed authority: one line containing the RESP endpoint.
    ///
    /// Loads an existing file when present; missing file starts empty.
    pub fn file(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        let primary = if path.exists() {
            let raw = fs::read_to_string(&path)?;
            let line = raw.lines().next().unwrap_or("").trim();
            if line.is_empty() {
                None
            } else {
                Some(line.to_string())
            }
        } else {
            None
        };
        Ok(Self {
            primary: Mutex::new(primary),
            path: Some(path),
        })
    }

    /// Publish `endpoint` as the current primary (memory + optional file).
    pub fn publish_primary(&self, endpoint: impl Into<String>) -> io::Result<()> {
        let endpoint = endpoint.into();
        {
            let mut guard = self.primary.lock().expect("authority mutex");
            *guard = Some(endpoint.clone());
        }
        if let Some(path) = &self.path {
            write_endpoint_file(path, &endpoint)?;
        }
        Ok(())
    }

    /// Current primary RESP endpoint, if published.
    pub fn current_primary(&self) -> Option<String> {
        self.primary.lock().expect("authority mutex").clone()
    }

    /// Path of the optional on-disk authority file.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }
}

fn write_endpoint_file(path: &Path, endpoint: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let tmp = path.with_extension("tmp");
    {
        let mut file = fs::File::create(&tmp)?;
        writeln!(file, "{endpoint}")?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn memory_authority_publishes_endpoint() {
        let auth = ConfigAuthority::in_memory();
        assert!(auth.current_primary().is_none());
        auth.publish_primary("127.0.0.1:6380").expect("publish");
        assert_eq!(auth.current_primary().as_deref(), Some("127.0.0.1:6380"));
    }

    #[test]
    fn file_authority_round_trips() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("janus-authority-{nanos}.txt"));
        let auth = ConfigAuthority::file(&path).expect("open");
        auth.publish_primary("127.0.0.1:7000").expect("publish");
        drop(auth);
        let reloaded = ConfigAuthority::file(&path).expect("reload");
        assert_eq!(reloaded.current_primary().as_deref(), Some("127.0.0.1:7000"));
        let _ = fs::remove_file(path);
    }
}
