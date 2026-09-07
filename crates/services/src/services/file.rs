use std::{
    fs,
    path::{Path, PathBuf},
};

use bytes::Bytes;
use db::models::{
    execution_artifact::ExecutionArtifact,
    file::{CreateFile, File},
};
use futures::{Stream, StreamExt};
use mime_guess::MimeGuess;
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum FileError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("File too large: {0} bytes (max: {1} bytes)")]
    TooLarge(u64, u64),

    #[error("File not found")]
    NotFound,

    #[error("Failed to build response: {0}")]
    ResponseBuildError(String),
}

/// Sanitize filename for filesystem safety:
/// - Lowercase
/// - Spaces → underscores
/// - Remove special characters (keep alphanumeric and underscores)
/// - Truncate if too long
fn sanitize_filename(name: &str) -> String {
    let stem = Path::new(name)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("file");

    let clean: String = stem
        .to_lowercase()
        .chars()
        .map(|c| if c.is_whitespace() { '_' } else { c })
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect();

    // Truncate to reasonable length to avoid filesystem limits
    let max_len = 50;
    if clean.len() > max_len {
        clean[..max_len].to_string()
    } else if clean.is_empty() {
        "file".to_string()
    } else {
        clean
    }
}

#[derive(Clone)]
pub struct FileService {
    cache_dir: PathBuf,
    legacy_cache_dir: PathBuf,
    pool: SqlitePool,
    max_size_bytes: u64,
}

impl FileService {
    pub fn new(pool: SqlitePool) -> Result<Self, FileError> {
        let cache_dir = utils::cache_dir().join("attachments");
        let legacy_cache_dir = utils::cache_dir().join("images");
        fs::create_dir_all(&cache_dir)?;
        Ok(Self {
            cache_dir,
            legacy_cache_dir,
            pool,
            max_size_bytes: 20 * 1024 * 1024, // 20MB default
        })
    }

    pub async fn store_file(
        &self,
        data: &[u8],
        original_filename: &str,
    ) -> Result<File, FileError> {
        let file_size = data.len() as u64;

        if file_size > self.max_size_bytes {
            return Err(FileError::TooLarge(file_size, self.max_size_bytes));
        }

        let hash = format!("{:x}", Sha256::digest(data));

        let extension = Path::new(original_filename)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("bin");

        let mime_type = MimeGuess::from_path(original_filename)
            .first_raw()
            .map(str::to_string)
            .or_else(|| {
                MimeGuess::from_ext(extension)
                    .first_raw()
                    .map(str::to_string)
            });

        let existing_file = File::find_by_hash(&self.pool, &hash).await?;

        if let Some(existing) = existing_file {
            tracing::debug!("Reusing existing file record with hash {}", hash);
            return Ok(existing);
        }

        let clean_name = sanitize_filename(original_filename);
        let new_filename = format!("{}_{}.{}", Uuid::new_v4(), clean_name, extension);
        let cached_path = self.cache_dir.join(&new_filename);
        fs::write(&cached_path, data)?;

        let file = File::create(
            &self.pool,
            &CreateFile {
                file_path: new_filename,
                original_name: original_filename.to_string(),
                mime_type,
                size_bytes: file_size as i64,
                hash,
            },
        )
        .await?;
        Ok(file)
    }

    /// Retains arbitrary-sized input without materialising it in memory. The
    /// final attachment stays content-addressed; callers record every use as a
    /// separate occurrence through `retain_execution_artifact`.
    pub async fn store_stream<S, E>(
        &self,
        mut stream: S,
        original_filename: &str,
        max_size_bytes: Option<u64>,
    ) -> Result<File, FileError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let staging_name = format!("{}.upload", Uuid::new_v4());
        let staging_path = self.cache_dir.join(&staging_name);
        let mut output = tokio::fs::File::create(&staging_path).await?;
        let mut hash = Sha256::new();
        let mut size = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|error| FileError::Io(std::io::Error::other(error.to_string())))?;
            size = size.saturating_add(chunk.len() as u64);
            if let Some(max) = max_size_bytes
                && size > max
            {
                let _ = tokio::fs::remove_file(&staging_path).await;
                return Err(FileError::TooLarge(size, max));
            }
            hash.update(&chunk);
            output.write_all(&chunk).await?;
        }
        output.sync_all().await?;
        drop(output);

        let hash = format!("{:x}", hash.finalize());
        if let Some(existing) = File::find_by_hash(&self.pool, &hash).await? {
            let _ = tokio::fs::remove_file(staging_path).await;
            return Ok(existing);
        }
        let extension = Path::new(original_filename)
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("bin");
        let filename = format!(
            "{}_{}.{}",
            Uuid::new_v4(),
            sanitize_filename(original_filename),
            extension
        );
        tokio::fs::rename(&staging_path, self.cache_dir.join(&filename)).await?;
        File::create(
            &self.pool,
            &CreateFile {
                file_path: filename,
                original_name: original_filename.to_owned(),
                mime_type: MimeGuess::from_path(original_filename)
                    .first_raw()
                    .map(str::to_owned),
                size_bytes: size as i64,
                hash,
            },
        )
        .await
        .map_err(FileError::Database)
    }

    pub async fn retain_execution_artifact(
        &self,
        execution_id: Uuid,
        original_path: &str,
        producer_ref: Option<&str>,
        file: &File,
    ) -> Result<ExecutionArtifact, FileError> {
        ExecutionArtifact::create(
            &self.pool,
            execution_id,
            file.id,
            original_path,
            producer_ref,
        )
        .await
        .map_err(FileError::Database)
    }

    pub async fn get_execution_artifact(
        &self,
        occurrence_id: Uuid,
    ) -> Result<Option<ExecutionArtifact>, FileError> {
        ExecutionArtifact::find_by_id(&self.pool, occurrence_id)
            .await
            .map_err(FileError::Database)
    }

    pub async fn delete_orphaned_files(&self) -> Result<(), FileError> {
        let orphaned_files = File::find_orphaned_files(&self.pool).await?;
        if orphaned_files.is_empty() {
            tracing::debug!("No orphaned files found during cleanup");
            return Ok(());
        }

        tracing::debug!("Found {} orphaned files to clean up", orphaned_files.len());
        let mut deleted_count = 0;
        let mut failed_count = 0;

        for file in orphaned_files {
            match self.delete_file(file.id).await {
                Ok(_) => {
                    deleted_count += 1;
                    tracing::debug!("Deleted orphaned file: {}", file.id);
                }
                Err(e) => {
                    failed_count += 1;
                    tracing::error!("Failed to delete orphaned file {}: {}", file.id, e);
                }
            }
        }

        tracing::info!(
            "File cleanup completed: {} deleted, {} failed",
            deleted_count,
            failed_count
        );

        Ok(())
    }

    pub fn get_absolute_path(&self, file: &File) -> PathBuf {
        self.resolve_cached_path(&file.file_path)
            .unwrap_or_else(|| self.cache_dir.join(&file.file_path))
    }

    pub async fn get_file(&self, id: Uuid) -> Result<Option<File>, FileError> {
        Ok(File::find_by_id(&self.pool, id).await?)
    }

    pub async fn delete_file(&self, id: Uuid) -> Result<(), FileError> {
        if let Some(file) = File::find_by_id(&self.pool, id).await? {
            let file_path = self.cache_dir.join(&file.file_path);
            if file_path.exists() {
                fs::remove_file(file_path)?;
            }

            let legacy_file_path = self.legacy_cache_dir.join(&file.file_path);
            if legacy_file_path.exists() {
                fs::remove_file(legacy_file_path)?;
            }

            File::delete(&self.pool, id).await?;
        }

        Ok(())
    }

    pub async fn copy_files_by_workspace_to_worktree(
        &self,
        worktree_path: &Path,
        workspace_id: Uuid,
        agent_working_dir: Option<&str>,
    ) -> Result<(), FileError> {
        let files = File::find_by_workspace_id(&self.pool, workspace_id).await?;
        let target_path = match agent_working_dir {
            Some(dir) if !dir.is_empty() => worktree_path.join(dir),
            _ => worktree_path.to_path_buf(),
        };
        self.copy_files(&target_path, files)
    }

    pub async fn copy_files_by_ids_to_worktree(
        &self,
        worktree_path: &Path,
        file_ids: &[Uuid],
    ) -> Result<(), FileError> {
        let mut files = Vec::new();
        for id in file_ids {
            if let Some(file) = File::find_by_id(&self.pool, *id).await? {
                files.push(file);
            }
        }
        self.copy_files(worktree_path, files)
    }

    /// Copy files to the worktree. Skips files that already exist at target.
    fn copy_files(&self, worktree_path: &Path, files: Vec<File>) -> Result<(), FileError> {
        if files.is_empty() {
            return Ok(());
        }

        let attachments_dir = worktree_path.join(utils::path::CDESKTOP_ATTACHMENTS_DIR);

        // Fast path: check if all files exist before doing anything
        let all_exist = files
            .iter()
            .all(|file| attachments_dir.join(&file.file_path).exists());
        if all_exist {
            return Ok(());
        }

        std::fs::create_dir_all(&attachments_dir)?;

        // Create .gitignore to ignore all files in this directory
        let gitignore_path = attachments_dir.join(".gitignore");
        if !gitignore_path.exists() {
            std::fs::write(&gitignore_path, "*\n")?;
        }

        for file in files {
            let src = self
                .resolve_cached_path(&file.file_path)
                .unwrap_or_else(|| self.cache_dir.join(&file.file_path));
            let dst = attachments_dir.join(&file.file_path);

            if dst.exists() {
                continue;
            }

            if src.exists() {
                if let Err(e) = std::fs::copy(&src, &dst) {
                    tracing::error!("Failed to copy {}: {}", file.file_path, e);
                } else {
                    tracing::debug!("Copied {}", file.file_path);
                }
            } else {
                tracing::warn!("Missing cache file: {}", src.display());
            }
        }

        Ok(())
    }

    fn resolve_cached_path(&self, file_path: &str) -> Option<PathBuf> {
        let primary = self.cache_dir.join(file_path);
        if primary.exists() {
            return Some(primary);
        }

        let legacy = self.legacy_cache_dir.join(file_path);
        if legacy.exists() {
            tracing::info!(
                "Using legacy attachment cache path for {}: {}",
                file_path,
                legacy.display()
            );
            return Some(legacy);
        }

        None
    }
}
