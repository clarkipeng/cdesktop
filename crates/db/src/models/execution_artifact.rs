use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqliteConnection, SqlitePool};
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ExecutionArtifact {
    pub id: Uuid,
    pub execution_id: Uuid,
    pub attachment_id: Uuid,
    pub original_path: String,
    pub original_name: Option<String>,
    pub producer_ref: Option<String>,
    pub publication_key: Option<String>,
    pub captured_at: DateTime<Utc>,
    /// Internal reference-commit provenance, not a complete retention receipt.
    /// Blob and directory barriers must still succeed before acknowledging it.
    #[serde(skip)]
    pub durable_commit: bool,
}

impl ExecutionArtifact {
    // Keep result columns explicit. After ALTER TABLE on another connection,
    // SQLite can reprepare SELECT * at step-time with a wider result than
    // SQLx 0.8's cached column metadata, panicking instead of returning the row.
    pub async fn create(
        conn: &mut SqliteConnection,
        execution_id: Uuid,
        attachment_id: Uuid,
        original_path: &str,
        original_name: &str,
        producer_ref: Option<&str>,
        publication_key: &str,
    ) -> Result<Self, sqlx::Error> {
        crate::require_durable_commit_policy(conn).await?;
        sqlx::query_as::<_, Self>(
            "INSERT INTO execution_artifacts
             (id, execution_id, attachment_id, original_path, original_name, producer_ref, publication_key, durable_commit)
             VALUES (?, ?, ?, ?, ?, ?, ?, 1)
             RETURNING id, execution_id, attachment_id, original_path, original_name,
                       producer_ref, publication_key, captured_at, durable_commit",
        )
        .bind(Uuid::new_v4())
        .bind(execution_id)
        .bind(attachment_id)
        .bind(original_path)
        .bind(original_name)
        .bind(producer_ref)
        .bind(publication_key)
        .fetch_one(conn)
        .await
    }

    /// A keyed publication replay may upgrade an earlier unverified reference
    /// with a real write, without changing its occurrence identity or capture facts.
    pub async fn upgrade_commit_policy(
        &mut self,
        conn: &mut SqliteConnection,
    ) -> Result<(), sqlx::Error> {
        if !self.durable_commit {
            crate::require_durable_commit_policy(conn).await?;
            self.durable_commit = sqlx::query_scalar("UPDATE execution_artifacts SET durable_commit=1 WHERE id=? RETURNING durable_commit")
                .bind(self.id)
                .fetch_one(conn)
                .await?;
        }
        Ok(())
    }

    pub async fn find_by_publication(
        conn: &mut SqliteConnection,
        execution_id: Uuid,
        publication_key: &str,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as::<_, Self>(
            "SELECT id, execution_id, attachment_id, original_path, original_name,
                    producer_ref, publication_key, captured_at, durable_commit
             FROM execution_artifacts WHERE execution_id = ? AND publication_key = ?",
        )
        .bind(execution_id)
        .bind(publication_key)
        .fetch_optional(conn)
        .await
    }

    pub async fn find_by_id(pool: &SqlitePool, id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as::<_, Self>(
            "SELECT id, execution_id, attachment_id, original_path, original_name,
                    producer_ref, publication_key, captured_at, durable_commit
             FROM execution_artifacts WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(pool)
        .await
    }
}
