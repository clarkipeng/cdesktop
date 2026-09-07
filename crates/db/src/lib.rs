use std::{path::Path, sync::Arc};

use sqlx::{
    Error, Pool, Sqlite, SqlitePool,
    migrate::MigrateError,
    sqlite::{
        SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqlitePoolOptions,
        SqliteSynchronous,
    },
};
use utils::assets::asset_dir;

pub mod models;
pub mod provider_catalog;
pub mod provider_payloads;

/// One native commit policy for both pool constructors and explicit-path fixtures.
/// DELETE/FULL alone can lose the last commit after power loss. EXTRA confirms
/// journal removal; fullfsync requests the stronger macOS storage barrier.
pub fn connection_options(path: &Path) -> SqliteConnectOptions {
    SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Delete)
        .synchronous(SqliteSynchronous::Extra)
        .pragma("fullfsync", "ON")
}

/// Verify the actual writing connection, not merely the pool's configured defaults.
/// This is a prerequisite for an artifact reference's durable-commit marker.
async fn require_durable_commit_policy(conn: &mut SqliteConnection) -> Result<(), Error> {
    let (journal, synchronous, fullfsync): (String, i64, i64) = sqlx::query_as(
        "SELECT journal_mode, synchronous, fullfsync FROM pragma_journal_mode(), pragma_synchronous(), pragma_fullfsync()",
    ).fetch_one(conn).await?;
    if journal != "delete" || synchronous != 3 || fullfsync != 1 {
        return Err(Error::Protocol(
            "artifact retention requires DELETE/EXTRA/fullfsync on its writing connection".into(),
        ));
    }
    Ok(())
}

async fn run_migrations(pool: &Pool<Sqlite>) -> Result<(), Error> {
    use std::collections::HashSet;

    let migrator = sqlx::migrate!("./migrations");
    let mut processed_versions: HashSet<i64> = HashSet::new();

    loop {
        match migrator.run(pool).await {
            Ok(()) => return Ok(()),
            Err(MigrateError::VersionMismatch(version)) => {
                if cfg!(debug_assertions) {
                    // return the error in debug mode to catch migration issues early
                    return Err(sqlx::Error::Migrate(Box::new(
                        MigrateError::VersionMismatch(version),
                    )));
                }

                if !cfg!(windows) {
                    // On non-Windows platforms, we do not attempt to auto-fix checksum mismatches
                    return Err(sqlx::Error::Migrate(Box::new(
                        MigrateError::VersionMismatch(version),
                    )));
                }

                // Guard against infinite loop
                if !processed_versions.insert(version) {
                    return Err(sqlx::Error::Migrate(Box::new(
                        MigrateError::VersionMismatch(version),
                    )));
                }

                // On Windows, there can be checksum mismatches due to line ending differences
                // or other platform-specific issues. Update the stored checksum and retry.
                tracing::warn!(
                    "Migration version {} has checksum mismatch, updating stored checksum (likely platform-specific difference)",
                    version
                );

                // Find the migration with the mismatched version and get its current checksum
                if let Some(migration) = migrator.iter().find(|m| m.version == version) {
                    // Update the checksum in _sqlx_migrations to match the current file
                    sqlx::query("UPDATE _sqlx_migrations SET checksum = ? WHERE version = ?")
                        .bind(&*migration.checksum)
                        .bind(version)
                        .execute(pool)
                        .await?;
                } else {
                    // Migration not found in current set, can't fix
                    return Err(sqlx::Error::Migrate(Box::new(
                        MigrateError::VersionMismatch(version),
                    )));
                }
            }
            Err(e) => return Err(e.into()),
        }
    }
}

#[derive(Clone)]
pub struct DBService {
    pub pool: Pool<Sqlite>,
}

impl DBService {
    pub async fn new() -> Result<DBService, Error> {
        let options = connection_options(&asset_dir().join("db.v2.sqlite"));
        let pool = SqlitePool::connect_with(options).await?;
        run_migrations(&pool).await?;
        Ok(DBService { pool })
    }

    pub async fn new_with_after_connect<F>(after_connect: F) -> Result<DBService, Error>
    where
        F: for<'a> Fn(
                &'a mut SqliteConnection,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>,
            > + Send
            + Sync
            + 'static,
    {
        let pool = Self::create_pool(Some(Arc::new(after_connect))).await?;
        Ok(DBService { pool })
    }

    async fn create_pool<F>(after_connect: Option<Arc<F>>) -> Result<Pool<Sqlite>, Error>
    where
        F: for<'a> Fn(
                &'a mut SqliteConnection,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>,
            > + Send
            + Sync
            + 'static,
    {
        let options = connection_options(&asset_dir().join("db.v2.sqlite"));

        let pool = if let Some(hook) = after_connect {
            SqlitePoolOptions::new()
                .after_connect(move |conn, _meta| {
                    let hook = hook.clone();
                    Box::pin(async move {
                        hook(conn).await?;
                        Ok(())
                    })
                })
                .connect_with(options)
                .await?
        } else {
            SqlitePool::connect_with(options).await?
        };

        run_migrations(&pool).await?;
        Ok(pool)
    }
}

#[cfg(test)]
mod durability_tests {
    use super::*;

    #[tokio::test]
    async fn artifact_commit_policy_is_verified_on_each_actual_connection() {
        let root = tempfile::tempdir().unwrap();
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect_with(connection_options(&root.path().join("policy.sqlite")))
            .await
            .unwrap();
        let mut first = pool.acquire().await.unwrap();
        let mut second = pool.acquire().await.unwrap();
        require_durable_commit_policy(&mut first).await.unwrap();
        require_durable_commit_policy(&mut second).await.unwrap();
        sqlx::query("PRAGMA synchronous=FULL")
            .execute(&mut *second)
            .await
            .unwrap();
        assert!(require_durable_commit_policy(&mut second).await.is_err());
        require_durable_commit_policy(&mut first).await.unwrap();
        sqlx::query("PRAGMA synchronous=EXTRA")
            .execute(&mut *second)
            .await
            .unwrap();
        sqlx::query("PRAGMA fullfsync=OFF")
            .execute(&mut *second)
            .await
            .unwrap();
        assert!(require_durable_commit_policy(&mut second).await.is_err());
    }
}
