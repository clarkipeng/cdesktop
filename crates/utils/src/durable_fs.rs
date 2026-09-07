//! Native file publication: durable contents and directory entries before a
//! database reference can acknowledge them. Used by log and artifact owners.

#[cfg(any(not(windows), test))]
use std::fs;
use std::{io, path::Path};

/// Separate readable coverage from a confirmed filesystem acknowledgement.
/// Unverified data must not advance a durable cursor or replace another source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationDurability {
    Confirmed,
    Unverified,
}

impl PublicationDurability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::Unverified => "unverified",
        }
    }
}

/// Verify the value first, then confirm its owner and reachable directory entries.
/// The owner is native, append-only and never replaced. A writer may still be
/// open: the reader's barrier covers bytes it observed before the barrier.
pub fn read_confirmed<T>(
    path: &Path,
    read: impl FnOnce(&Path) -> io::Result<T>,
) -> io::Result<(T, PublicationDurability)> {
    read_confirmed_with(path, read, confirm_publication)
}

fn read_confirmed_with<T>(
    path: &Path,
    read: impl FnOnce(&Path) -> io::Result<T>,
    confirm: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<(T, PublicationDurability)> {
    let value = read(path)?;
    let durability = match confirm(path) {
        Ok(()) => PublicationDurability::Confirmed,
        Err(error) if error.kind() == io::ErrorKind::Unsupported => {
            PublicationDurability::Unverified
        }
        Err(error) => return Err(error),
    };
    Ok((value, durability))
}

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
        sync_parent_chain(destination)
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
        sync_parent_chain(path)
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

/// An existing directory is not proof that its entry survived a previous
/// creator's crash. Confirm the entire reachable chain, including intermediate
/// symlink targets, before acknowledging publication. Native paths must remain
/// quiescent; this is not protection against an external directory rename.
#[cfg(not(windows))]
fn sync_parent_chain(path: &Path) -> io::Result<()> {
    sync_parent_chain_with(path, |directory| fs::File::open(directory)?.sync_all())
}

#[cfg(not(windows))]
fn sync_parent_chain_with(
    path: &Path,
    mut sync: impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let mut pending = vec![std::path::absolute(path)?];
    let mut visited = std::collections::HashSet::new();
    while let Some(path) = pending.pop() {
        for entry in path.ancestors() {
            if !visited.insert(entry.to_owned()) {
                continue;
            }
            if let Some(parent) = entry.parent() {
                sync(parent)?;
                if fs::symlink_metadata(entry)?.file_type().is_symlink() {
                    // canonicalize alone would hide intermediate symlinks and
                    // leave their own parent entries unconfirmed.
                    pending.push(parent.join(fs::read_link(entry)?));
                }
            }
        }
    }
    Ok(())
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
    #[cfg(not(windows))]
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn read_acknowledgement_confirms_after_read_and_never_hides_io_failure() {
        let read_finished = std::cell::Cell::new(false);
        let read = |_: &Path| {
            read_finished.set(true);
            Ok(b"observed bytes")
        };
        let confirm = |_: &Path| {
            assert!(
                read_finished.get(),
                "barrier must follow the observed value"
            );
            Ok(())
        };
        let (_, durability) = read_confirmed_with(Path::new("owner"), read, confirm).unwrap();
        assert_eq!(durability, PublicationDurability::Confirmed);

        let (bytes, durability) = read_confirmed_with(
            Path::new("owner"),
            |_| Ok(b"still readable"),
            |_| Err(io::ErrorKind::Unsupported.into()),
        )
        .unwrap();
        assert_eq!(bytes, b"still readable");
        assert_eq!(durability, PublicationDurability::Unverified);
        assert!(
            read_confirmed_with(
                Path::new("owner"),
                |_| Ok(b"not acknowledged"),
                |_| Err(io::ErrorKind::PermissionDenied.into()),
            )
            .is_err()
        );
        assert!(
            read_confirmed_with::<()>(
                Path::new("owner"),
                |_| Err(io::ErrorKind::InvalidData.into()),
                |_| panic!("do not confirm a failed read"),
            )
            .is_err()
        );
    }

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

    #[cfg(not(windows))]
    #[test]
    fn confirmation_visits_existing_ancestors_and_propagates_their_errors() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("legacy/session/processes");
        fs::create_dir_all(&directory).unwrap();
        let owner = directory.join("owner");
        fs::write(&owner, b"already published").unwrap();
        let mut visited = Vec::new();
        sync_parent_chain_with(&owner, |directory| {
            visited.push(directory.to_owned());
            Ok(())
        })
        .unwrap();
        assert!(visited.starts_with(&[
            directory,
            root.path().join("legacy/session"),
            root.path().join("legacy"),
            root.path().to_owned(),
        ]));
        assert!(visited.contains(&PathBuf::from("/")));
        let error = sync_parent_chain_with(&owner, |directory| {
            if directory == root.path().join("legacy") {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "ancestor refused sync",
                ))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[cfg(unix)]
    #[test]
    fn confirmation_includes_intermediate_symlink_targets() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        for name in ["visible", "intermediate", "target"] {
            fs::create_dir(root.join(name)).unwrap();
        }
        fs::write(root.join("target/owner"), b"original").unwrap();
        symlink("../target", root.join("intermediate/link")).unwrap();
        symlink("../intermediate/link", root.join("visible/link")).unwrap();
        let owner = root.join("visible/link/owner");
        let mut visited = Vec::new();
        sync_parent_chain_with(&owner, |directory| {
            visited.push(directory.canonicalize()?);
            Ok(())
        })
        .unwrap();
        for name in ["visible", "intermediate", "target"] {
            assert!(visited.contains(&root.join(name)), "missing {name}");
        }
        confirm_publication(&owner).unwrap();
    }
}
