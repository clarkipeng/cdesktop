//! Native file publication: durable contents and directory entries before a
//! database reference can acknowledge them. Used by log and artifact owners.

#[cfg(any(not(windows), test))]
use std::fs;
use std::{io, path::Path};

/// Make each new parent entry durable, including a newly created session tree.
/// Syncing only the leaf directory can still lose its unsynced ancestors.
pub fn create_dir_all(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() || path.is_dir() {
        return Ok(());
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        create_dir_all(parent)?;
    }
    #[cfg(not(windows))]
    {
        match fs::create_dir(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => {}
            Err(error) => return Err(error),
        }
        sync_parent(path)
    }
    #[cfg(windows)]
    {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let staging = tempfile::tempdir_in(parent)?;
        match move_write_through(staging.path(), path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// Existing destinations are never replaced. A failure after publication leaves
/// the destination in place for verification/recovery, never speculative deletion.
pub fn publish_noclobber(temp: tempfile::NamedTempFile, destination: &Path) -> io::Result<()> {
    temp.as_file().sync_all()?;
    #[cfg(not(windows))]
    {
        temp.persist_noclobber(destination)
            .map_err(|error| error.error)?;
        sync_parent(destination)
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Storage::FileSystem::{FILE_ATTRIBUTE_NORMAL, SetFileAttributesW};
        // tempfile marks Windows staging files temporary. Clear that flag
        // before the write-through rename, matching tempfile's persist contract.
        let encoded = wide_path(temp.path())?;
        if unsafe { SetFileAttributesW(encoded.as_ptr(), FILE_ATTRIBUTE_NORMAL) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let path = temp.into_temp_path();
        move_write_through(&path, destination)
    }
}

/// Re-establish publication durability after a lost/failed acknowledgement.
/// Readability alone is insufficient to authorize deletion of another source.
pub fn confirm_publication(path: &Path) -> io::Result<()> {
    #[cfg(not(windows))]
    {
        fs::File::open(path)?.sync_all()?;
        sync_parent(path)
    }
    #[cfg(windows)]
    {
        let _ = path;
        // MoveFileExW WRITE_THROUGH confirms a new successful publication. We
        // have no verified equivalent for an already-existing directory entry;
        // callers must retain redundant originals instead of guessing.
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "cannot reconfirm existing Windows publication durability",
        ))
    }
}

#[cfg(not(windows))]
fn sync_parent(path: &Path) -> io::Result<()> {
    fs::File::open(
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?
    .sync_all()
}

#[cfg(windows)]
fn wide_path(path: &Path) -> io::Result<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt;
    let mut encoded: Vec<u16> = path.as_os_str().encode_wide().collect();
    if encoded.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "NUL in native publication path",
        ));
    }
    encoded.push(0);
    Ok(encoded)
}

#[cfg(windows)]
fn move_write_through(source: &Path, destination: &Path) -> io::Result<()> {
    use windows_sys::Win32::Storage::FileSystem::{MOVEFILE_WRITE_THROUGH, MoveFileExW};
    let source = wide_path(source)?;
    let destination = wide_path(destination)?;
    // No REPLACE_EXISTING or COPY_ALLOWED: one native-volume publication,
    // synchronously persisted, with an existing destination left untouched.
    if unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn publish_preserves_existing_bytes_and_builds_nested_owner_directories() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("session/processes");
        create_dir_all(&directory).unwrap();
        let destination = directory.join("owner");
        let mut first = tempfile::NamedTempFile::new_in(&directory).unwrap();
        first.write_all(b"first").unwrap();
        publish_noclobber(first, &destination).unwrap();
        #[cfg(not(windows))]
        confirm_publication(&destination).unwrap();
        let mut retry = tempfile::NamedTempFile::new_in(&directory).unwrap();
        retry.write_all(b"different").unwrap();
        assert!(publish_noclobber(retry, &destination).is_err());
        assert_eq!(fs::read(destination).unwrap(), b"first");
    }
}
