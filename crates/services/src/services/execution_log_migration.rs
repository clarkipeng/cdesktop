//! Explicit, resumable migration of one terminal execution. Originals survive
//! any unverified publication. No default database, startup job, or global prune.

use std::{fs::Metadata, path::Path};

use anyhow::{Context, Result, ensure};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::{SqliteConnection, SqlitePool};
use tokio::io::AsyncReadExt;
use utils::execution_logs::{
    CaptureOutcome, ExecutionLogWriter, LegacySqlRow, LogAppend, execution_log_sha256,
    free_disk_reserve_bytes, legacy_process_log_file_path_in_root, lock_execution_log,
    process_log_file_path_in_root, scan_owner, validate_execution_log_json,
};
use uuid::Uuid;

const COPY_BYTES: usize = 64 * 1024;

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum LegacySource {
    Absent,
    Pruned,
    Retained { reason: String },
}

#[derive(Debug, Serialize)]
pub struct MigrationReport {
    pub execution_id: Uuid,
    pub published: bool,
    pub owner_sha256: Option<String>,
    pub sql: LegacySource,
    pub plain_file: LegacySource,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq)]
enum Fault {
    BeforePublish,
    AfterPublish,
    BeforePrune,
    ConfirmPublication,
}

pub async fn migrate_execution_logs(
    pool: &SqlitePool,
    root: &Path,
    execution_id: Uuid,
) -> Result<MigrationReport> {
    migrate_inner(
        pool,
        root,
        execution_id,
        free_disk_reserve_bytes(),
        #[cfg(test)]
        None,
    )
    .await
}

async fn terminal_session(connection: &mut SqliteConnection, id: Uuid) -> Result<Uuid> {
    let (session, status): (Uuid, String) =
        sqlx::query_as("SELECT session_id, status FROM execution_processes WHERE id = ?1")
            .bind(id)
            .fetch_one(connection)
            .await?;
    ensure!(
        matches!(status.as_str(), "completed" | "failed" | "killed"),
        "execution is not terminal"
    );
    Ok(session)
}

async fn migrate_inner(
    pool: &SqlitePool,
    root: &Path,
    execution_id: Uuid,
    reserve: u64,
    #[cfg(test)] fault: Option<Fault>,
) -> Result<MigrationReport> {
    let session = terminal_session(&mut *pool.acquire().await?, execution_id).await?;
    let owner = process_log_file_path_in_root(root, session, execution_id);
    let plain = legacy_process_log_file_path_in_root(root, session, execution_id);
    let lock_path = owner.clone();
    let _lease = tokio::task::spawn_blocking(move || lock_execution_log(&lock_path)).await??;
    ensure!(
        terminal_session(&mut *pool.acquire().await?, execution_id).await? == session,
        "execution changed"
    );
    let mut report = MigrationReport {
        execution_id,
        published: false,
        owner_sha256: None,
        sql: LegacySource::Absent,
        plain_file: LegacySource::Absent,
    };

    if !tokio::fs::try_exists(&owner).await? {
        // Staging sidecars/locks share a disposable directory. Only the verified
        // owner is published; recovery never requires a sidecar.
        let staging = tempfile::tempdir_in(owner.parent().context("owner parent")?)?;
        let temp = tempfile::NamedTempFile::new_in(staging.path())?;
        let mut writer =
            ExecutionLogWriter::with_free_disk_reserve(temp.path().to_owned(), reserve).await?;
        // Do not hold a database transaction across compression and fsync.
        // Terminal native SQL sources are immutable; fresh verification below
        // protects pruning even if an external editor changed them meanwhile.
        let sql = copy_sql(&mut *pool.acquire().await?, execution_id, Some(&mut writer)).await?;
        let expected_hash = if sql.rows > 0 {
            sql.raw_hash
        } else if tokio::fs::try_exists(&plain).await? {
            copy_plain(&plain, Some(&mut writer)).await?.0
        } else {
            return Ok(report);
        };
        writer.finish(CaptureOutcome::LegacyUnknown).await?;
        drop(writer);
        let summary = scan_owner(temp.path()).await?;
        ensure!(
            sql.rows == 0 || summary.legacy_sql_fingerprint.as_deref() == Some(&sql.fingerprint),
            "SQL provenance verification failed"
        );
        ensure!(
            execution_log_sha256(temp.path()).await? == expected_hash,
            "legacy conversion hash mismatch"
        );
        validate_execution_log_json(temp.path()).await?;
        #[cfg(test)]
        ensure!(
            fault != Some(Fault::BeforePublish),
            "injected before publication"
        );
        let destination = owner.clone();
        tokio::task::spawn_blocking(move || {
            utils::durable_fs::publish_noclobber(temp, &destination)
        })
        .await??;
        report.published = true;
        #[cfg(test)]
        ensure!(
            fault != Some(Fault::AfterPublish),
            "injected lost publication response"
        );
    }

    // Existing files get exactly the same verification as newly published ones.
    let summary = scan_owner(&owner)
        .await
        .context("verify compressed owner")?;
    ensure!(
        summary.outcome.is_some(),
        "unsealed owner; originals retained"
    );
    validate_execution_log_json(&owner).await?;
    let owner_hash = execution_log_sha256(&owner).await?;
    report.owner_sha256 = Some(
        owner_hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect(),
    );

    // A successful file rename alone does not confirm pre-existing ancestor
    // entries. Every prune requires the same complete publication barrier.
    let path = owner.clone();
    let durability = tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        if fault == Some(Fault::ConfirmPublication) {
            return Err(std::io::Error::other("injected ancestor sync failure"));
        }
        utils::durable_fs::confirm_publication(&path)
    })
    .await?;
    let durability_error = durability
        .err()
        .map(|error| format!("publication durability unavailable: {error}"));

    #[cfg(test)]
    ensure!(fault != Some(Fault::BeforePrune), "injected before prune");
    // This transaction spans only fresh source verification and deletion, not
    // compression/publication. It can still take time for very large SQL logs.
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    ensure!(
        terminal_session(&mut tx, execution_id).await? == session,
        "execution changed"
    );
    let current = copy_sql(&mut tx, execution_id, None).await?;
    if current.rows > 0 {
        report.sql = if let Some(reason) = &durability_error {
            LegacySource::Retained {
                reason: reason.clone(),
            }
        } else if current.raw_hash == owner_hash
            && summary.legacy_sql_fingerprint.as_deref() == Some(&current.fingerprint)
        {
            sqlx::query("DELETE FROM execution_process_logs WHERE execution_id = ?1")
                .bind(execution_id)
                .execute(&mut *tx)
                .await?;
            LegacySource::Pruned
        } else {
            LegacySource::Retained {
                reason: "owner bytes or original SQL metadata differ".into(),
            }
        };
    }
    tx.commit().await?;

    if tokio::fs::try_exists(&plain).await? {
        let (hash, observed) = copy_plain(&plain, None).await?;
        // Native terminal sources are quiescent under the lease. Check identity
        // and bytes again; this is not a lock against arbitrary external editors.
        let current = tokio::fs::metadata(&plain).await?;
        report.plain_file = if let Some(reason) = &durability_error {
            LegacySource::Retained {
                reason: reason.clone(),
            }
        } else if hash == owner_hash && same_file_state(&observed, &current)? {
            tokio::fs::remove_file(&plain).await?;
            LegacySource::Pruned
        } else {
            LegacySource::Retained {
                reason: "plain source differs or changed during verification".into(),
            }
        };
    }
    Ok(report)
}

struct SqlEvidence {
    rows: u64,
    raw_hash: [u8; 32],
    fingerprint: String,
}

/// Keyset reads keep memory bounded and work with a one-connection pool. The
/// rowid tie-breaker is supported by both shipped legacy SQL table generations.
async fn copy_sql(
    connection: &mut SqliteConnection,
    execution: Uuid,
    mut writer: Option<&mut ExecutionLogWriter>,
) -> Result<SqlEvidence> {
    let mut previous: Option<(String, i64)> = None;
    let mut raw = Sha256::new();
    let mut evidence = Sha256::new();
    let mut rows = 0;
    loop {
        let row: Option<(i64, String, i64, i64, i64)> = sqlx::query_as(
            "SELECT rowid, substr(inserted_at, 1, 1025), byte_size, length(CAST(logs AS BLOB)), length(CAST(inserted_at AS BLOB))
             FROM execution_process_logs WHERE execution_id = ?1 AND (?2 IS NULL OR (inserted_at, rowid) > (?2, ?3))
             ORDER BY inserted_at, rowid LIMIT 1",
        ).bind(execution).bind(previous.as_ref().map(|value| &value.0))
            .bind(previous.as_ref().map(|value| value.1)).fetch_optional(&mut *connection).await?;
        let Some((row_id, inserted_at, reported_byte_size, byte_length, timestamp_length)) = row
        else {
            break;
        };
        ensure!(
            timestamp_length <= 1024,
            "legacy insertion timestamp exceeds owner metadata capacity"
        );
        let source = LegacySqlRow {
            row_id,
            inserted_at,
            reported_byte_size,
            original_bytes: byte_length.try_into()?,
        };
        source.hash_header(&mut evidence)?;
        let mut offset = 0;
        loop {
            let bytes: Vec<u8> = sqlx::query_scalar(
                "SELECT substr(CAST(logs AS BLOB), ?3, ?4) FROM execution_process_logs WHERE execution_id = ?1 AND rowid = ?2",
            ).bind(execution).bind(row_id).bind(i64::try_from(offset)? + 1).bind(COPY_BYTES as i64)
                .fetch_one(&mut *connection).await?;
            ensure!(
                bytes.len() as u64 == (source.original_bytes - offset).min(COPY_BYTES as u64),
                "legacy SQL row changed during copy"
            );
            raw.update(&bytes);
            evidence.update(&bytes);
            if let Some(writer) = writer.as_deref_mut() {
                ensure!(
                    writer.append_legacy_bytes(&bytes, Some(&source)).await? == LogAppend::Written,
                    "recording storage unavailable"
                );
            }
            offset += bytes.len() as u64;
            if offset == source.original_bytes {
                break;
            }
        }
        rows += 1;
        previous = Some((source.inserted_at, source.row_id));
    }
    Ok(SqlEvidence {
        rows,
        raw_hash: raw.finalize().into(),
        fingerprint: format!("{:x}", evidence.finalize()),
    })
}

async fn copy_plain(
    path: &Path,
    mut writer: Option<&mut ExecutionLogWriter>,
) -> Result<([u8; 32], Metadata)> {
    let mut file = tokio::fs::File::open(path).await?;
    let before = file.metadata().await?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0; COPY_BYTES];
    loop {
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
        if let Some(writer) = writer.as_deref_mut() {
            ensure!(
                writer.append_legacy_bytes(&buffer[..read], None).await? == LogAppend::Written,
                "recording storage unavailable"
            );
        }
    }
    ensure!(
        same_file_state(&before, &file.metadata().await?)?,
        "plain source changed during copy"
    );
    Ok((hash.finalize().into(), before))
}

fn same_file_state(left: &Metadata, right: &Metadata) -> Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if left.dev() != right.dev() || left.ino() != right.ino() {
            return Ok(false);
        }
    }
    Ok(left.len() == right.len()
        && left.modified()? == right.modified()?
        && left.created().ok() == right.created().ok())
}

#[cfg(test)]
mod tests {
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;

    struct Fixture {
        root: tempfile::TempDir,
        pool: SqlitePool,
        session: Uuid,
        execution: Uuid,
    }

    impl Fixture {
        async fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            // Exercise the real shipped schema and foreign keys, never a default
            // native database or permissive reconstruction of its tables.
            sqlx::migrate!("../db/migrations").run(&pool).await.unwrap();
            let workspace = Uuid::new_v4();
            let session = Uuid::new_v4();
            let execution = Uuid::new_v4();
            sqlx::query("INSERT INTO workspaces(id, branch) VALUES (?1, 'fixture')")
                .bind(workspace)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO sessions(id, workspace_id) VALUES (?1, ?2)")
                .bind(session)
                .bind(workspace)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO execution_processes(id, session_id, status) VALUES (?1, ?2, 'completed')").bind(execution).bind(session).execute(&pool).await.unwrap();
            Self {
                root,
                pool,
                session,
                execution,
            }
        }

        fn owner(&self) -> std::path::PathBuf {
            process_log_file_path_in_root(self.root.path(), self.session, self.execution)
        }

        fn plain(&self) -> std::path::PathBuf {
            legacy_process_log_file_path_in_root(self.root.path(), self.session, self.execution)
        }

        async fn sql(&self, bytes: &str) {
            sqlx::query("INSERT INTO execution_process_logs(execution_id, logs, byte_size, inserted_at) VALUES (?1, ?2, ?3, '2025-07-29 12:34:56.789')")
                .bind(self.execution).bind(bytes).bind(bytes.len() as i64).execute(&self.pool).await.unwrap();
        }

        async fn write_plain(&self, bytes: &str) {
            tokio::fs::create_dir_all(self.plain().parent().unwrap())
                .await
                .unwrap();
            tokio::fs::write(self.plain(), bytes).await.unwrap();
        }

        async fn count(&self) -> i64 {
            sqlx::query_scalar(
                "SELECT count(*) FROM execution_process_logs WHERE execution_id = ?1",
            )
            .bind(self.execution)
            .fetch_one(&self.pool)
            .await
            .unwrap()
        }

        async fn migrate(&self) -> Result<MigrationReport> {
            migrate_inner(&self.pool, self.root.path(), self.execution, 0, None).await
        }

        async fn owner_with(&self, bytes: &str) {
            let mut writer = ExecutionLogWriter::with_free_disk_reserve(self.owner(), 0)
                .await
                .unwrap();
            writer
                .append_legacy_bytes(bytes.as_bytes(), None)
                .await
                .unwrap();
            writer.finish(CaptureOutcome::LegacyUnknown).await.unwrap();
        }
    }

    fn assert_verified_source(source: &LegacySource) {
        if cfg!(windows) {
            assert!(
                matches!(source, LegacySource::Retained { reason } if reason.contains("publication durability unavailable"))
            );
        } else {
            assert_eq!(*source, LegacySource::Pruned);
        }
    }

    #[tokio::test]
    async fn failed_ancestor_confirmation_retains_every_original_on_publish_and_replay() {
        let fixture = Fixture::new().await;
        let bytes = "{\"Stdout\":\"retained\"}\n";
        fixture.sql(bytes).await;
        fixture.write_plain(bytes).await;
        for newly_published in [true, false] {
            let report = migrate_inner(
                &fixture.pool,
                fixture.root.path(),
                fixture.execution,
                0,
                Some(Fault::ConfirmPublication),
            )
            .await
            .unwrap();
            assert_eq!(report.published, newly_published);
            for source in [&report.sql, &report.plain_file] {
                assert!(
                    matches!(source, LegacySource::Retained { reason } if reason.contains("injected ancestor sync failure"))
                );
            }
            assert_eq!(fixture.count().await, 1);
            assert_eq!(
                tokio::fs::read_to_string(fixture.plain()).await.unwrap(),
                bytes
            );
            assert!(fixture.owner().exists());
        }
        let retry = fixture.migrate().await.unwrap();
        assert_verified_source(&retry.sql);
        assert_verified_source(&retry.plain_file);
    }

    #[tokio::test]
    async fn sql_rows_and_insertion_metadata_survive_roundtrip_without_global_prune() {
        let fixture = Fixture::new().await;
        let large = format!("{{\"Stdout\":\"{}\"}}", "界".repeat(COPY_BYTES));
        fixture.sql("").await;
        fixture.sql(&large).await;
        fixture.sql("{\"Stderr\":\"second\"}\n").await;
        let bytes = format!("{large}{{\"Stderr\":\"second\"}}\n");
        fixture.write_plain(&bytes).await;
        let original = copy_sql(
            &mut fixture.pool.acquire().await.unwrap(),
            fixture.execution,
            None,
        )
        .await
        .unwrap();
        let other = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO execution_processes(id, session_id, status) VALUES (?1, ?2, 'completed')",
        )
        .bind(other)
        .bind(fixture.session)
        .execute(&fixture.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO execution_process_logs(execution_id, logs, byte_size) VALUES (?1, 'other evidence', 14)").bind(other).execute(&fixture.pool).await.unwrap();

        let report = fixture.migrate().await.unwrap();
        assert!(report.published);
        assert_verified_source(&report.sql);
        assert_verified_source(&report.plain_file);
        assert_eq!(fixture.count().await, if cfg!(windows) { 3 } else { 0 });
        assert_eq!(fixture.plain().exists(), cfg!(windows));
        let summary = scan_owner(&fixture.owner()).await.unwrap();
        assert_eq!(summary.legacy_sql_fingerprint, Some(original.fingerprint));
        assert_eq!(summary.outcome, Some(CaptureOutcome::LegacyUnknown));
        assert!(summary.legacy_capture_metadata);
        assert_eq!(
            execution_log_sha256(&fixture.owner()).await.unwrap(),
            original.raw_hash
        );
        let remaining: String =
            sqlx::query_scalar("SELECT logs FROM execution_process_logs WHERE execution_id = ?1")
                .bind(other)
                .fetch_one(&fixture.pool)
                .await
                .unwrap();
        assert_eq!(remaining, "other evidence");
        let replay = fixture.migrate().await.unwrap();
        assert!(!replay.published);
        assert_eq!(replay.owner_sha256, report.owner_sha256);
        if cfg!(windows) {
            assert_verified_source(&replay.sql);
        } else {
            assert_eq!(replay.sql, LegacySource::Absent);
        }
    }

    #[tokio::test]
    async fn original_single_row_sql_schema_is_supported() {
        let fixture = Fixture::new().await;
        sqlx::query("DROP TABLE execution_process_logs")
            .execute(&fixture.pool)
            .await
            .unwrap();
        sqlx::raw_sql(include_str!(
            "../../../db/migrations/20250729162941_create_execution_process_logs.sql"
        ))
        .execute(&fixture.pool)
        .await
        .unwrap();
        fixture.sql("{\"Stdout\":\"old row\"}\n").await;
        assert_verified_source(&fixture.migrate().await.unwrap().sql);
    }

    #[tokio::test]
    async fn interruption_at_publication_boundaries_preserves_originals_and_replays() {
        for fault in [
            Fault::BeforePublish,
            Fault::AfterPublish,
            Fault::BeforePrune,
        ] {
            let fixture = Fixture::new().await;
            let bytes = "{\"Stdout\":\"retained\"}\n";
            fixture.sql(bytes).await;
            fixture.write_plain(bytes).await;
            let failure = migrate_inner(
                &fixture.pool,
                fixture.root.path(),
                fixture.execution,
                0,
                Some(fault),
            )
            .await;
            assert!(failure.is_err());
            assert_eq!(fixture.count().await, 1);
            assert_eq!(
                tokio::fs::read_to_string(fixture.plain()).await.unwrap(),
                bytes
            );
            assert_eq!(fixture.owner().exists(), fault != Fault::BeforePublish);
            if fixture.owner().exists() {
                // A corrupt disposable sidecar never authorizes or prevents prune.
                tokio::fs::write(
                    utils::execution_logs::process_log_frame_index_path(&fixture.owner()),
                    b"bad index",
                )
                .await
                .unwrap();
            }
            let retry = fixture.migrate().await.unwrap();
            assert_verified_source(&retry.sql);
            assert_verified_source(&retry.plain_file);
        }
    }

    #[tokio::test]
    async fn active_execution_or_native_writer_cannot_be_migrated() {
        let fixture = Fixture::new().await;
        fixture.sql("{\"Stdout\":\"keep\"}\n").await;
        sqlx::query("UPDATE execution_processes SET status = 'running' WHERE id = ?1")
            .bind(fixture.execution)
            .execute(&fixture.pool)
            .await
            .unwrap();
        assert!(fixture.migrate().await.is_err());
        assert!(!fixture.owner().exists());
        sqlx::query("UPDATE execution_processes SET status = 'completed' WHERE id = ?1")
            .bind(fixture.execution)
            .execute(&fixture.pool)
            .await
            .unwrap();
        let lease = lock_execution_log(&fixture.owner()).unwrap();
        assert!(fixture.migrate().await.is_err());
        assert_eq!(fixture.count().await, 1);
        drop(lease);
        assert_verified_source(&fixture.migrate().await.unwrap().sql);
    }

    #[tokio::test]
    async fn invalid_sources_and_corrupt_owners_are_never_pruned() {
        let fixture = Fixture::new().await;
        fixture.sql("{broken JSON").await;
        assert!(fixture.migrate().await.is_err());
        assert!(!fixture.owner().exists());
        assert_eq!(fixture.count().await, 1);
        tokio::fs::write(fixture.owner(), b"corrupt compressed owner")
            .await
            .unwrap();
        assert!(fixture.migrate().await.is_err());
        assert_eq!(
            tokio::fs::read(fixture.owner()).await.unwrap(),
            b"corrupt compressed owner"
        );
        assert_eq!(fixture.count().await, 1);
    }

    #[tokio::test]
    async fn different_owner_or_missing_sql_provenance_retains_original_rows() {
        for owner in [
            "{\"Stdout\":\"different\"}\n",
            "{\"Stdout\":\"original\"}\n",
        ] {
            let fixture = Fixture::new().await;
            let bytes = "{\"Stdout\":\"original\"}\n";
            fixture.sql(bytes).await;
            fixture.write_plain(bytes).await;
            fixture.owner_with(owner).await;
            let report = fixture.migrate().await.unwrap();
            assert!(matches!(report.sql, LegacySource::Retained { .. }));
            assert_eq!(fixture.count().await, 1);
            assert_eq!(fixture.plain().exists(), cfg!(windows) || owner != bytes);
        }
    }

    #[tokio::test]
    async fn changed_sql_metadata_and_mismatching_plain_copy_remain_readable() {
        let fixture = Fixture::new().await;
        fixture.sql("{\"Stdout\":\"original\"}\n").await;
        fixture
            .write_plain("{\"Stdout\":\"different copy\"}\n")
            .await;
        assert!(
            migrate_inner(
                &fixture.pool,
                fixture.root.path(),
                fixture.execution,
                0,
                Some(Fault::AfterPublish)
            )
            .await
            .is_err()
        );
        sqlx::query("UPDATE execution_process_logs SET inserted_at = '2025-07-30', byte_size = 999 WHERE execution_id = ?1").bind(fixture.execution).execute(&fixture.pool).await.unwrap();
        let report = fixture.migrate().await.unwrap();
        assert!(matches!(report.sql, LegacySource::Retained { .. }));
        assert!(matches!(report.plain_file, LegacySource::Retained { .. }));
        assert_eq!(fixture.count().await, 1);
        assert!(fixture.plain().exists());
    }

    #[tokio::test]
    async fn plain_only_storage_refusal_and_absence_are_distinct() {
        let fixture = Fixture::new().await;
        let absent = fixture.migrate().await.unwrap();
        assert!(!absent.published);
        assert!(absent.owner_sha256.is_none());
        assert!(!fixture.owner().exists());
        fixture.write_plain("{\"Stdout\":\"plain only\"}\n").await;
        assert!(
            migrate_inner(
                &fixture.pool,
                fixture.root.path(),
                fixture.execution,
                u64::MAX,
                None
            )
            .await
            .is_err()
        );
        assert!(fixture.plain().exists());
        assert!(!fixture.owner().exists());
        let report = fixture.migrate().await.unwrap();
        assert_eq!(report.sql, LegacySource::Absent);
        assert_verified_source(&report.plain_file);
        assert_eq!(
            scan_owner(&fixture.owner()).await.unwrap().outcome,
            Some(CaptureOutcome::LegacyUnknown)
        );
    }
}
