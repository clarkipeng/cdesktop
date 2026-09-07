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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

    #[error("artifact publication key was already used for different evidence")]
    PublicationConflict,

    #[error("File is retained by a workspace or execution occurrence")]
    Retained,

    #[error("Insufficient free space to retain evidence safely")]
    StorageUnavailable,

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
        clean.chars().take(max_len).collect()
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
    free_disk_reserve_bytes: u64,
}

/// Identity of one logical publication, independent of deduplicated bytes.
pub struct ArtifactPublication<'a> {
    pub execution_id: Uuid,
    pub original_path: &'a str,
    pub producer_ref: Option<&'a str>,
    pub publication_key: &'a str,
}

struct StagedUpload {
    // Drop removes incomplete uploads on errors or cancelled requests.
    temp: tempfile::NamedTempFile,
    data: CreateFile,
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
            free_disk_reserve_bytes: utils::execution_logs::free_disk_reserve_bytes(),
        })
    }

    pub async fn store_file(
        &self,
        data: &[u8],
        original_filename: &str,
    ) -> Result<File, FileError> {
        self.store_stream(
            futures::stream::iter([Ok::<_, std::io::Error>(Bytes::copy_from_slice(data))]),
            original_filename,
            Some(self.max_size_bytes),
        )
        .await
    }

    /// Retains arbitrary-sized input without materialising it in memory. The
    /// final attachment stays content-addressed. Referenced execution evidence
    /// uses publish_execution_artifact, which commits its retention atomically.
    pub async fn store_stream<S, E>(
        &self,
        stream: S,
        original_filename: &str,
        max_size_bytes: Option<u64>,
    ) -> Result<File, FileError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let staged = self
            .stage_upload(stream, original_filename, max_size_bytes)
            .await?;
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let file = self.publish_upload(&mut tx, staged).await?;
        tx.commit().await?;
        Ok(file)
    }

    async fn stage_upload<S, E>(
        &self,
        mut stream: S,
        original_filename: &str,
        max_size_bytes: Option<u64>,
    ) -> Result<StagedUpload, FileError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let temp = tempfile::Builder::new()
            .prefix(".upload-")
            .tempfile_in(&self.cache_dir)?;
        let mut output = tokio::fs::File::from_std(temp.reopen()?);
        let mut hash = Sha256::new();
        let mut size = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|error| FileError::Io(std::io::Error::other(error.to_string())))?;
            if fs2::available_space(&self.cache_dir)?
                < self
                    .free_disk_reserve_bytes
                    .saturating_add(chunk.len() as u64)
            {
                return Err(FileError::StorageUnavailable);
            }
            size = size.saturating_add(chunk.len() as u64);
            if let Some(max) = max_size_bytes
                && size > max
            {
                return Err(FileError::TooLarge(size, max));
            }
            hash.update(&chunk);
            output.write_all(&chunk).await?;
        }
        output.sync_all().await?;
        drop(output);

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
        Ok(StagedUpload {
            temp,
            data: CreateFile {
                file_path: filename,
                original_name: original_filename.to_owned(),
                mime_type: MimeGuess::from_path(original_filename)
                    .first_raw()
                    .map(str::to_owned),
                size_bytes: i64::try_from(size).map_err(std::io::Error::other)?,
                hash: format!("{:x}", hash.finalize()),
            },
        })
    }

    async fn publish_upload(
        &self,
        conn: &mut sqlx::SqliteConnection,
        staged: StagedUpload,
    ) -> Result<File, FileError> {
        if let Some(existing) = File::find_by_hash(&mut *conn, &staged.data.hash).await? {
            // Never acknowledge retention of missing or corrupt original bytes.
            self.verify_cached_file(&existing).await?;
            return Ok(existing);
        }
        let cache_dir = self.cache_dir.clone();
        let data = tokio::task::spawn_blocking(move || -> Result<CreateFile, std::io::Error> {
            // A unique physical generation prevents GC of an old row from
            // unlinking a later equal-byte publication. Publish durable bytes
            // before the DB reference; uncertain commits must not delete them.
            staged
                .temp
                .persist_noclobber(cache_dir.join(&staged.data.file_path))
                .map_err(|error| error.error)?;
            fs::File::open(cache_dir)?.sync_all()?;
            Ok(staged.data)
        })
        .await
        .map_err(std::io::Error::other)??;
        Ok(File::create(conn, &data).await?)
    }

    async fn verify_cached_file(&self, file: &File) -> Result<(), FileError> {
        let mut input = tokio::fs::File::open(self.get_absolute_path(file)).await?;
        let mut buffer = [0; 64 * 1024];
        let mut hash = Sha256::new();
        let mut size = 0u64;
        loop {
            let read = input.read(&mut buffer).await?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
            size += read as u64;
        }
        if size != file.size_bytes as u64 || format!("{:x}", hash.finalize()) != file.hash {
            return Err(std::io::Error::other("cached artifact hash mismatch").into());
        }
        Ok(())
    }

    pub async fn publish_execution_artifact<S, E>(
        &self,
        stream: S,
        original_filename: &str,
        publication: ArtifactPublication<'_>,
    ) -> Result<ExecutionArtifact, FileError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let staged = self.stage_upload(stream, original_filename, None).await?;
        // Upload first, then serialize publication with GC and other writers.
        // No committed attachment can exist without its retained occurrence.
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(artifact) = ExecutionArtifact::find_by_publication(
            &mut tx,
            publication.execution_id,
            publication.publication_key,
        )
        .await?
        {
            let file = File::find_by_id(&mut *tx, artifact.attachment_id)
                .await?
                .ok_or(FileError::NotFound)?;
            if file.hash != staged.data.hash
                || artifact.original_path != publication.original_path
                || artifact.original_name.as_deref() != Some(original_filename)
                || artifact.producer_ref.as_deref() != publication.producer_ref
            {
                return Err(FileError::PublicationConflict);
            }
            // Reuse the normal blob verification without publishing any new
            // bytes or occurrence. The staging file drops on every replay.
            self.publish_upload(&mut tx, staged).await?;
            tx.commit().await?;
            return Ok(artifact);
        }
        let file = self.publish_upload(&mut tx, staged).await?;
        let artifact = ExecutionArtifact::create(
            &mut tx,
            publication.execution_id,
            file.id,
            publication.original_path,
            original_filename,
            publication.producer_ref,
            publication.publication_key,
        )
        .await
        .map_err(FileError::Database)?;
        tx.commit().await?;
        Ok(artifact)
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
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let file = File::delete_unreferenced(&mut *tx, id).await?;
        tx.commit().await?;
        if let Some(file) = file {
            let file_path = self.cache_dir.join(&file.file_path);
            if file_path.exists() {
                fs::remove_file(file_path)?;
            }

            let legacy_file_path = self.legacy_cache_dir.join(&file.file_path);
            if legacy_file_path.exists() {
                fs::remove_file(legacy_file_path)?;
            }
        } else if File::find_by_id(&self.pool, id).await?.is_some() {
            return Err(FileError::Retained);
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

#[cfg(test)]
mod evidence_tests {
    use super::*;

    async fn fixture() -> (tempfile::TempDir, FileService, Uuid) {
        let dir = tempfile::tempdir().unwrap();
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::new()
                    .filename(dir.path().join("fixture.sqlite"))
                    .create_if_missing(true)
                    .foreign_keys(true)
                    .busy_timeout(std::time::Duration::from_secs(5)),
            )
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE execution_processes (id BLOB PRIMARY KEY);
            CREATE TABLE attachments (
                id BLOB PRIMARY KEY, file_path TEXT NOT NULL, original_name TEXT NOT NULL,
                mime_type TEXT, size_bytes INTEGER NOT NULL, hash TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
            CREATE TABLE workspace_attachments (
                workspace_id BLOB, attachment_id BLOB REFERENCES attachments(id));",
        )
        .execute(&pool)
        .await
        .unwrap();
        // Exercise the shipped occurrence schema, including its real FKs and
        // durable publication-key uniqueness, not a permissive test substitute.
        sqlx::raw_sql(include_str!(
            "../../../db/migrations/20260906000000_add_execution_artifacts.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::raw_sql(include_str!(
            "../../../db/migrations/20260906000001_add_execution_artifact_publication_key.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::raw_sql(include_str!(
            "../../../db/migrations/20260906000002_add_execution_artifact_original_name.sql"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let execution_id = Uuid::new_v4();
        sqlx::query("INSERT INTO execution_processes (id) VALUES (?)")
            .bind(execution_id)
            .execute(&pool)
            .await
            .unwrap();
        let cache_dir = dir.path().join("attachments");
        fs::create_dir(&cache_dir).unwrap();
        let service = FileService {
            cache_dir,
            legacy_cache_dir: dir.path().join("images"),
            pool,
            max_size_bytes: 20 * 1024 * 1024,
            free_disk_reserve_bytes: 0,
        };
        (dir, service, execution_id)
    }

    async fn publish(
        service: &FileService,
        execution_id: Uuid,
        key: &str,
        bytes: &'static [u8],
    ) -> Result<ExecutionArtifact, FileError> {
        service
            .publish_execution_artifact(
                futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(bytes))]),
                "checkpoint.md",
                ArtifactPublication {
                    execution_id,
                    original_path: ".context/checkpoint.md",
                    producer_ref: Some("task/checkpoint"),
                    publication_key: key,
                },
            )
            .await
    }

    #[tokio::test]
    async fn lost_response_replays_occurrence_and_equal_bytes_keep_distinct_occurrences() {
        // An uncertain network response must not create a second fact, while
        // distinct publications of equal content still retain both facts.
        let (_dir, service, execution) = fixture().await;
        let first = publish(&service, execution, "one", b"exact bytes\n")
            .await
            .unwrap();
        let replay = publish(&service, execution, "one", b"exact bytes\n")
            .await
            .unwrap();
        let second = publish(&service, execution, "two", b"exact bytes\n")
            .await
            .unwrap();
        assert_eq!(first.id, replay.id);
        assert_eq!(first.original_name.as_deref(), Some("checkpoint.md"));
        assert_eq!(first.captured_at, replay.captured_at);
        assert_ne!(first.id, second.id);
        assert_eq!(first.attachment_id, second.attachment_id);
        assert!(matches!(
            publish(&service, execution, "one", b"changed").await,
            Err(FileError::PublicationConflict)
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM attachments")
                .fetch_one(&service.pool)
                .await
                .unwrap(),
            1
        );
        service.delete_orphaned_files().await.unwrap();
        let file = service
            .get_file(first.attachment_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            fs::read(service.get_absolute_path(&file)).unwrap(),
            b"exact bytes\n"
        );
    }

    #[tokio::test]
    async fn gc_rechecks_a_snapshot_after_an_occurrence_retains_the_blob() {
        // The old GC selected an orphan, then deleted its bytes even if a
        // publication acquired a reference between selection and deletion.
        let (_dir, service, execution) = fixture().await;
        let file = service
            .store_file(b"retained", "checkpoint.md")
            .await
            .unwrap();
        let selected = File::find_orphaned_files(&service.pool).await.unwrap();
        assert_eq!(selected[0].id, file.id);
        let occurrence = publish(&service, execution, "retain", b"retained")
            .await
            .unwrap();
        assert_eq!(occurrence.attachment_id, file.id);
        assert!(matches!(
            service.delete_file(selected[0].id).await,
            Err(FileError::Retained)
        ));
        assert_eq!(
            fs::read(service.get_absolute_path(&file)).unwrap(),
            b"retained"
        );
    }

    #[tokio::test]
    async fn concurrent_publications_and_gc_cannot_lose_retained_bytes() {
        // BEGIN IMMEDIATE and the deleting statement's reference predicate
        // fence independent pooled writers, not just clones of a Rust mutex.
        let (_dir, service, execution) = fixture().await;
        let (first, second, cleanup) = tokio::join!(
            publish(&service, execution, "concurrent-a", b"shared"),
            publish(&service, execution, "concurrent-b", b"shared"),
            service.delete_orphaned_files(),
        );
        cleanup.unwrap();
        let first = first.unwrap();
        let second = second.unwrap();
        assert_ne!(first.id, second.id);
        assert_eq!(first.attachment_id, second.attachment_id);
        service.delete_orphaned_files().await.unwrap();
        let file = service
            .get_file(first.attachment_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            fs::read(service.get_absolute_path(&file)).unwrap(),
            b"shared"
        );
    }

    #[tokio::test]
    async fn failed_occurrence_rolls_back_attachment_publication() {
        // A failure after durable blob publication but before retention must
        // never expose a committed unretained attachment as successful evidence.
        let (_dir, service, _) = fixture().await;
        assert!(
            publish(&service, Uuid::new_v4(), "missing-execution", b"bytes")
                .await
                .is_err()
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM attachments")
                .fetch_one(&service.pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_artifacts")
                .fetch_one(&service.pool)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn interrupted_upload_drops_its_staging_file() {
        // Stream errors leave neither a DB reference nor an accumulating
        // partial upload on disk. No production cache root is touched.
        let (_dir, service, _) = fixture().await;
        let stream = futures::stream::iter([
            Ok(Bytes::from_static(b"partial")),
            Err(std::io::Error::other("disconnected")),
        ]);
        assert!(
            service
                .store_stream(stream, "partial.md", None)
                .await
                .is_err()
        );
        assert_eq!(fs::read_dir(&service.cache_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn artifact_upload_preserves_the_shared_disk_reserve() {
        // Disabling the HTTP size cap must not let artifact uploads consume
        // space reserved for recording/control. Refusal leaves no staged file.
        let (_dir, mut service, execution) = fixture().await;
        service.free_disk_reserve_bytes = u64::MAX;
        assert!(matches!(
            publish(&service, execution, "no-space", b"bytes").await,
            Err(FileError::StorageUnavailable)
        ));
        assert_eq!(fs::read_dir(&service.cache_dir).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn each_occurrence_keeps_its_name_and_replay_rejects_corrupt_cached_bytes() {
        // A blob's first filename is not the filename of every occurrence;
        // equal-length corruption must not masquerade as a verified replay.
        let (_dir, service, execution) = fixture().await;
        let first = publish(&service, execution, "first-name", b"data")
            .await
            .unwrap();
        let second = service
            .publish_execution_artifact(
                futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"data"))]),
                "another.txt",
                ArtifactPublication {
                    execution_id: execution,
                    original_path: "another.txt",
                    producer_ref: None,
                    publication_key: "second-name",
                },
            )
            .await
            .unwrap();
        assert_eq!(first.attachment_id, second.attachment_id);
        assert_eq!(first.original_name.as_deref(), Some("checkpoint.md"));
        assert_eq!(second.original_name.as_deref(), Some("another.txt"));
        let file = service
            .get_file(first.attachment_id)
            .await
            .unwrap()
            .unwrap();
        fs::write(service.get_absolute_path(&file), b"oops").unwrap();
        assert!(matches!(
            publish(&service, execution, "first-name", b"data").await,
            Err(FileError::Io(_))
        ));
    }
}
