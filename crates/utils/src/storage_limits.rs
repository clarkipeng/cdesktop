use std::{io, path::Path};

const DEFAULT_MAX_TRANSCRIPT_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_MAX_FORK_BYTES: u64 = 128 * 1024 * 1024;
const DEFAULT_MIN_FREE_DISK_BYTES: u64 = 10 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
pub struct WriterLimits {
    pub transcript_bytes: u64,
    pub fork_bytes: u64,
    pub free_disk_bytes: u64,
}

impl WriterLimits {
    pub fn from_env() -> Self {
        Self {
            transcript_bytes: configured_limit(
                "CDESKTOP_MAX_TRANSCRIPT_BYTES",
                DEFAULT_MAX_TRANSCRIPT_BYTES,
            ),
            fork_bytes: configured_limit("CDESKTOP_MAX_FORK_BYTES", DEFAULT_MAX_FORK_BYTES),
            free_disk_bytes: configured_limit(
                "CDESKTOP_MIN_FREE_DISK_BYTES",
                DEFAULT_MIN_FREE_DISK_BYTES,
            ),
        }
    }
}

pub fn ensure_launch_allowed(path: &Path) -> io::Result<()> {
    ensure_free_disk(path, 0, WriterLimits::from_env())
}

pub fn ensure_transcript_write_allowed(path: &Path, incoming_bytes: u64) -> io::Result<()> {
    let limits = WriterLimits::from_env();
    let current_bytes = match std::fs::metadata(path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error),
    };
    if exceeds_limit(current_bytes, incoming_bytes, limits.transcript_bytes) {
        return Err(io::Error::other(format!(
            "transcript byte limit exceeded: {} + {} > {}",
            current_bytes, incoming_bytes, limits.transcript_bytes
        )));
    }
    ensure_free_disk(path, incoming_bytes, limits)
}

pub fn ensure_fork_allowed(source: &Path) -> io::Result<()> {
    let limits = WriterLimits::from_env();
    let source_bytes = std::fs::metadata(source)?.len();
    if source_bytes > limits.fork_bytes {
        return Err(io::Error::other(format!(
            "fork byte limit exceeded: {} > {}",
            source_bytes, limits.fork_bytes
        )));
    }
    ensure_free_disk(source, source_bytes, limits)
}

fn ensure_free_disk(path: &Path, reserved_bytes: u64, limits: WriterLimits) -> io::Result<()> {
    let existing = nearest_existing_ancestor(path).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("no existing ancestor for {}", path.display()),
        )
    })?;
    let available = fs2::available_space(existing)?;
    let required = limits.free_disk_bytes.saturating_add(reserved_bytes);
    if available < required {
        return Err(io::Error::other(format!(
            "free disk byte limit reached: {} available, {} required",
            available, required
        )));
    }
    Ok(())
}

fn nearest_existing_ancestor(path: &Path) -> Option<&Path> {
    let mut candidate = Some(path);
    while let Some(current) = candidate {
        if current.exists() {
            return Some(current);
        }
        candidate = current.parent();
    }
    None
}

fn configured_limit(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn exceeds_limit(current: u64, additional: u64, limit: u64) -> bool {
    current.saturating_add(additional) > limit
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_existing_ancestor_handles_future_files() {
        let root = std::env::temp_dir();
        let future = root.join("cdesktop-storage-limit-test").join("file.jsonl");
        assert_eq!(nearest_existing_ancestor(&future), Some(root.as_path()));
    }

    #[test]
    fn transcript_limit_allows_the_boundary_and_rejects_overflow() {
        assert!(!exceeds_limit(90, 10, 100));
        assert!(exceeds_limit(90, 11, 100));
        assert!(exceeds_limit(u64::MAX, 1, u64::MAX - 1));
    }
}
