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
use utils::durable_fs::PublicationDurability;
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

#[derive(Debug, serde::Serialize)]
pub struct ArtifactReceipt {
    #[serde(flatten)]
    pub occurrence: ExecutionArtifact,
    pub sha256: String,
    pub size_bytes: i64,
    /// Only Confirmed authorises removing another source. Readable metadata
    /// alone does not prove a durable reference or durable blob publication.
    pub durability: PublicationDurability,
}

struct StagedUpload {
    // Drop removes incomplete uploads on errors or cancelled requests.
    temp: tempfile::NamedTempFile,
    data: CreateFile,
}

/// SQL preparation precedes filesystem publication. No reference write may be
/// deferred until after publish_upload; only the transaction commit follows it.
struct PreparedUpload {
    file: File,
    staged: Option<StagedUpload>,
}

impl FileService {
    pub fn new(pool: SqlitePool) -> Result<Self, FileError> {
        let cache_dir = utils::cache_dir().join("attachments");
        let legacy_cache_dir = utils::cache_dir().join("images");
        utils::durable_fs::create_dir_all(&cache_dir)?;
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
        let prepared = Self::prepare_upload(&mut tx, staged).await?;
        let (file, _) = self.publish_upload(prepared).await?;
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
        // sync_all waits for buffered writes but retains their errors for the
        // next write/flush. Observe that error before publishing the upload.
        output.flush().await?;
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

    async fn prepare_upload(
        conn: &mut sqlx::SqliteConnection,
        staged: StagedUpload,
    ) -> Result<PreparedUpload, FileError> {
        if let Some(file) = File::find_by_hash(&mut *conn, &staged.data.hash).await? {
            return Ok(PreparedUpload { file, staged: None });
        }
        Ok(PreparedUpload {
            file: File::create(conn, &staged.data).await?,
            staged: Some(staged),
        })
    }

    async fn publish_upload(
        &self,
        prepared: PreparedUpload,
    ) -> Result<(File, PublicationDurability), FileError> {
        let PreparedUpload { file, staged } = prepared;
        let durability = if let Some(staged) = staged {
            let destination = self.cache_dir.join(&file.file_path);
            tokio::task::spawn_blocking(move || {
                // A unique physical generation prevents stale GC unlinking a
                // later equal-byte publication. The prepared SQL is uncommitted;
                // durable bytes still precede COMMIT. Keep uncertain outcomes.
                utils::durable_fs::publish_noclobber(staged.temp, &destination)
            })
            .await
            .map_err(std::io::Error::other)??
        } else {
            // Never acknowledge missing or corrupt original bytes on dedup.
            self.verify_cached_file(&file).await?
        };
        Ok((file, durability))
    }

    async fn verify_cached_file(&self, file: &File) -> Result<PublicationDurability, FileError> {
        let path = self.get_absolute_path(file);
        let expected_hash = file.hash.clone();
        let expected_size = file.size_bytes;
        let (_, durability) = tokio::task::spawn_blocking(move || {
            utils::durable_fs::read_confirmed(&path, |path| {
                let mut input = fs::File::open(path)?;
                let mut buffer = [0; 64 * 1024];
                let mut hash = Sha256::new();
                let mut size = 0u64;
                loop {
                    let read = std::io::Read::read(&mut input, &mut buffer)?;
                    if read == 0 {
                        break;
                    }
                    hash.update(&buffer[..read]);
                    size += read as u64;
                }
                if size != expected_size as u64 || format!("{:x}", hash.finalize()) != expected_hash
                {
                    return Err(std::io::Error::other("cached artifact hash mismatch"));
                }
                Ok(())
            })
        })
        .await
        .map_err(std::io::Error::other)??;
        Ok(durability)
    }

    async fn artifact_receipt(
        &self,
        occurrence: ExecutionArtifact,
        file: File,
        mut durability: PublicationDurability,
        confirm: impl FnOnce(&Path) -> std::io::Result<()> + Send + 'static,
    ) -> Result<ArtifactReceipt, FileError> {
        if !occurrence.durable_commit {
            durability = PublicationDurability::Unverified;
        } else if durability == PublicationDurability::Confirmed {
            // Read the actual native filename through SQLite. Never open/close
            // its DB file behind SQLite's back; that could release its locks.
            let filename: String =
                sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name='main'")
                    .fetch_one(&self.pool)
                    .await?;
            durability = if filename.is_empty() {
                PublicationDurability::Unverified
            } else {
                tokio::task::spawn_blocking(move || {
                    PublicationDurability::from_confirmation(confirm(Path::new(&filename)))
                })
                .await
                .map_err(std::io::Error::other)??
            };
        }
        Ok(ArtifactReceipt {
            occurrence,
            sha256: file.hash,
            size_bytes: file.size_bytes,
            durability,
        })
    }

    pub async fn publish_execution_artifact<S, E>(
        &self,
        stream: S,
        original_filename: &str,
        publication: ArtifactPublication<'_>,
    ) -> Result<ArtifactReceipt, FileError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        self.publish_execution_artifact_with_confirmation(
            stream,
            original_filename,
            publication,
            utils::durable_fs::confirm_directory_entries,
        )
        .await
    }

    async fn publish_execution_artifact_with_confirmation<S, E>(
        &self,
        stream: S,
        original_filename: &str,
        publication: ArtifactPublication<'_>,
        confirm: impl FnOnce(&Path) -> std::io::Result<()> + Send + 'static,
    ) -> Result<ArtifactReceipt, FileError>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        let staged = self.stage_upload(stream, original_filename, None).await?;
        // Stage first, then serialize SQL preparation and publication with GC.
        // Known reference refusal must happen before publishing a blob, while
        // no committed attachment can exist without its durable retained bytes.
        let mut tx = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        if let Some(mut artifact) = ExecutionArtifact::find_by_publication(
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
            // Prepare any policy upgrade before verifying the existing bytes;
            // verification failure rolls it back. Never publish another blob.
            artifact.upgrade_commit_policy(&mut tx).await?;
            let durability = self.verify_cached_file(&file).await?;
            drop(staged);
            tx.commit().await?;
            return self
                .artifact_receipt(artifact, file, durability, confirm)
                .await;
        }
        let prepared = Self::prepare_upload(&mut tx, staged).await?;
        let artifact = ExecutionArtifact::create(
            &mut tx,
            publication.execution_id,
            prepared.file.id,
            publication.original_path,
            original_filename,
            publication.producer_ref,
            publication.publication_key,
        )
        .await
        .map_err(FileError::Database)?;
        let (file, durability) = self.publish_upload(prepared).await?;
        tx.commit().await?;
        self.artifact_receipt(artifact, file, durability, confirm)
            .await
    }

    pub async fn get_execution_artifact_receipt(
        &self,
        execution_id: Uuid,
        occurrence_id: Uuid,
    ) -> Result<Option<ArtifactReceipt>, FileError> {
        let Some(occurrence) = self
            .get_execution_artifact(occurrence_id)
            .await?
            .filter(|occurrence| occurrence.execution_id == execution_id)
        else {
            return Ok(None);
        };
        let file = File::find_by_id(&self.pool, occurrence.attachment_id)
            .await?
            .ok_or(FileError::NotFound)?;
        let durability = self.verify_cached_file(&file).await?;
        Ok(Some(
            self.artifact_receipt(
                occurrence,
                file,
                durability,
                utils::durable_fs::confirm_directory_entries,
            )
            .await?,
        ))
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
        fixture_with_commit_migration(true).await
    }

    async fn fixture_with_commit_migration(
        include_commit_marker: bool,
    ) -> (tempfile::TempDir, FileService, Uuid) {
        let dir = tempfile::tempdir().unwrap();
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(
                db::connection_options(&dir.path().join("fixture.sqlite"))
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
        if include_commit_marker {
            sqlx::raw_sql(include_str!(
                "../../../db/migrations/20260906000003_add_artifact_durable_commit.sql"
            ))
            .execute(&pool)
            .await
            .unwrap();
        }
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
        publish_receipt(service, execution_id, key, bytes)
            .await
            .map(|receipt| receipt.occurrence)
    }

    async fn publish_receipt(
        service: &FileService,
        execution_id: Uuid,
        key: &str,
        bytes: &'static [u8],
    ) -> Result<ArtifactReceipt, FileError> {
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
    async fn receipts_bind_verified_bytes_and_scope_without_exposing_the_commit_marker() {
        let (_root, service, execution) = fixture().await;
        let receipt = publish_receipt(&service, execution, "receipt", b"verified\n")
            .await
            .unwrap();
        assert!(receipt.occurrence.durable_commit);
        assert_eq!(
            receipt.sha256,
            format!("{:x}", Sha256::digest(b"verified\n"))
        );
        assert_eq!(receipt.size_bytes, 9);
        assert_eq!(
            receipt.durability,
            if cfg!(windows) {
                PublicationDurability::Unverified
            } else {
                PublicationDurability::Confirmed
            }
        );
        let retrieved = service
            .get_execution_artifact_receipt(execution, receipt.occurrence.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(&receipt).unwrap(),
            serde_json::to_value(&retrieved).unwrap()
        );
        assert!(
            serde_json::to_value(&receipt)
                .unwrap()
                .get("durable_commit")
                .is_none()
        );
        assert!(
            service
                .get_execution_artifact_receipt(Uuid::new_v4(), receipt.occurrence.id)
                .await
                .unwrap()
                .is_none()
        );
        let file = service
            .get_file(receipt.occurrence.attachment_id)
            .await
            .unwrap()
            .unwrap();
        fs::write(service.get_absolute_path(&file), b"wrong bytes").unwrap();
        assert!(
            service
                .get_execution_artifact_receipt(execution, receipt.occurrence.id)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn migration_keeps_old_references_unverified_until_a_keyed_replay_writes_the_upgrade() {
        let (_root, service, execution) = fixture_with_commit_migration(false).await;
        let file = service
            .store_file(b"old source", "checkpoint.md")
            .await
            .unwrap();
        let occurrence_id = Uuid::new_v4();
        sqlx::query("INSERT INTO execution_artifacts (id,execution_id,attachment_id,original_path,original_name,producer_ref,publication_key,captured_at) VALUES (?,?,?,'.context/checkpoint.md','checkpoint.md','task/checkpoint','old','2020-01-01 00:00:00')")
            .bind(occurrence_id).bind(execution).bind(file.id).execute(&service.pool).await.unwrap();
        sqlx::raw_sql(include_str!(
            "../../../db/migrations/20260906000003_add_artifact_durable_commit.sql"
        ))
        .execute(&service.pool)
        .await
        .unwrap();
        let old = service
            .get_execution_artifact_receipt(execution, occurrence_id)
            .await
            .unwrap()
            .unwrap();
        assert!(!old.occurrence.durable_commit);
        assert_eq!(old.durability, PublicationDurability::Unverified);
        assert!(
            !service
                .get_execution_artifact(occurrence_id)
                .await
                .unwrap()
                .unwrap()
                .durable_commit,
            "GET must not upgrade historical provenance"
        );
        let cached_path = service.get_absolute_path(&file);
        fs::write(&cached_path, b"corrupt old source").unwrap();
        assert!(
            publish_receipt(&service, execution, "old", b"old source")
                .await
                .is_err()
        );
        assert!(
            !service
                .get_execution_artifact(occurrence_id)
                .await
                .unwrap()
                .unwrap()
                .durable_commit,
            "a prepared upgrade must roll back when byte verification fails"
        );
        fs::write(cached_path, b"old source").unwrap();
        let replay = publish_receipt(&service, execution, "old", b"old source")
            .await
            .unwrap();
        assert_eq!(replay.occurrence.id, occurrence_id);
        assert_eq!(replay.occurrence.captured_at, old.occurrence.captured_at);
        assert!(replay.occurrence.durable_commit);
        assert_eq!(
            replay.occurrence.attachment_id,
            old.occurrence.attachment_id
        );
        assert_eq!(
            replay.durability,
            if cfg!(windows) {
                PublicationDurability::Unverified
            } else {
                PublicationDurability::Confirmed
            }
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_artifacts")
                .fetch_one(&service.pool)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn a_weak_writing_connection_cannot_publish_a_durable_reference() {
        let (root, mut service, execution) = fixture().await;
        service.pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                db::connection_options(&root.path().join("fixture.sqlite"))
                    .synchronous(sqlx::sqlite::SqliteSynchronous::Full),
            )
            .await
            .unwrap();
        assert!(matches!(
            publish_receipt(&service, execution, "weak", b"retained candidate").await,
            Err(FileError::Database(sqlx::Error::Protocol(_)))
        ));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_artifacts")
                .fetch_one(&service.pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM attachments")
                .fetch_one(&service.pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(fs::read_dir(&service.cache_dir).unwrap().count(), 0);
        assert!(matches!(
            publish_receipt(&service, execution, "weak", b"retained candidate").await,
            Err(FileError::Database(sqlx::Error::Protocol(_)))
        ));
        assert_eq!(fs::read_dir(&service.cache_dir).unwrap().count(), 0);
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn failed_or_unsupported_receipt_barriers_preserve_committed_references_for_replay() {
        let (_root, service, execution) = fixture().await;
        // Inject only the final directory barrier, after the real reference
        // commit, on both first publication and replay. Blob bytes stay owned.
        let mut occurrence_id = None;
        for failure in [
            std::io::ErrorKind::PermissionDenied,
            std::io::ErrorKind::Unsupported,
        ] {
            let pool = service.pool.clone();
            let result = service
                .publish_execution_artifact_with_confirmation(
                    futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(
                        b"receipt retry",
                    ))]),
                    "checkpoint.md",
                    ArtifactPublication {
                        execution_id: execution,
                        original_path: ".context/checkpoint.md",
                        producer_ref: Some("task/checkpoint"),
                        publication_key: "retry",
                    },
                    move |_: &Path| {
                        // A separate SQLite connection must see the committed
                        // reference before any final barrier can acknowledge it.
                        let committed = tokio::runtime::Handle::current().block_on(
                            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_artifacts WHERE publication_key='retry' AND durable_commit=1").fetch_one(&pool)
                        ).unwrap();
                        assert_eq!(committed, 1);
                        Err(failure.into())
                    },
                )
                .await;
            if failure == std::io::ErrorKind::PermissionDenied {
                assert!(matches!(result, Err(FileError::Io(_))));
            } else {
                assert_eq!(
                    result.unwrap().durability,
                    PublicationDurability::Unverified
                );
            }
            let id: Uuid = sqlx::query_scalar(
                "SELECT id FROM execution_artifacts WHERE publication_key='retry'",
            )
            .fetch_one(&service.pool)
            .await
            .unwrap();
            assert!(occurrence_id.is_none_or(|previous| previous == id));
            occurrence_id = Some(id);
            let artifact = service.get_execution_artifact(id).await.unwrap().unwrap();
            assert!(artifact.durable_commit);
            let file = service
                .get_file(artifact.attachment_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                fs::read(service.get_absolute_path(&file)).unwrap(),
                b"receipt retry"
            );
        }
        let replay = publish_receipt(&service, execution, "retry", b"receipt retry")
            .await
            .unwrap();
        assert_eq!(Some(replay.occurrence.id), occurrence_id);
        assert_eq!(replay.durability, PublicationDurability::Confirmed);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM execution_artifacts")
                .fetch_one(&service.pool)
                .await
                .unwrap(),
            1
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
        // Refused SQL preparation must leave neither retained rows nor a
        // published blob generation that a retry could leak again.
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
        assert_eq!(fs::read_dir(&service.cache_dir).unwrap().count(), 0);
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
        assert_eq!(first.attachment_id, second.occurrence.attachment_id);
        assert_eq!(first.original_name.as_deref(), Some("checkpoint.md"));
        assert_eq!(
            second.occurrence.original_name.as_deref(),
            Some("another.txt")
        );
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
