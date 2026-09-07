use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use uuid::Uuid;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct ExecutionArtifact {
    pub id: Uuid,
    pub execution_id: Uuid,
    pub attachment_id: Uuid,
    pub original_path: String,
    pub producer_ref: Option<String>,
    pub publication_key: Option<String>,
    pub captured_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionArtifactPublication {
    Created,
    Replayed,
}

impl ExecutionArtifact {
    pub async fn create_or_replay(
        pool: &SqlitePool,
        execution_id: Uuid,
        attachment_id: Uuid,
        original_path: &str,
        producer_ref: Option<&str>,
        publication_key: &str,
    ) -> Result<(Self, ExecutionArtifactPublication), sqlx::Error> {
        let id = Uuid::new_v4();
        if let Some(created) = sqlx::query_as::<_, Self>(
            "INSERT INTO execution_artifacts (id, execution_id, attachment_id, original_path, producer_ref, publication_key)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT(execution_id, publication_key) DO NOTHING
             RETURNING id, execution_id, attachment_id, original_path, producer_ref, publication_key, captured_at",
        )
        .bind(id)
        .bind(execution_id)
        .bind(attachment_id)
        .bind(original_path)
        .bind(producer_ref)
        .bind(publication_key)
        .fetch_optional(pool)
        .await?
        {
            return Ok((created, ExecutionArtifactPublication::Created));
        }

        let existing = sqlx::query_as::<_, Self>(
            "SELECT id, execution_id, attachment_id, original_path, producer_ref, publication_key, captured_at
             FROM execution_artifacts WHERE execution_id = ? AND publication_key = ?",
        )
        .bind(execution_id)
        .bind(publication_key)
        .fetch_one(pool)
        .await?;
        Ok((existing, ExecutionArtifactPublication::Replayed))
    }

    pub async fn find_by_id(pool: &SqlitePool, id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as::<_, Self>("SELECT id, execution_id, attachment_id, original_path, producer_ref, publication_key, captured_at FROM execution_artifacts WHERE id = ?")
            .bind(id)
            .fetch_optional(pool)
            .await
    }
}

#[cfg(test)]
mod tests {
    use sqlx::SqlitePool;

    use super::*;

    async fn pool() -> SqlitePool {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE execution_artifacts (
                id TEXT PRIMARY KEY NOT NULL,
                execution_id TEXT NOT NULL,
                attachment_id TEXT NOT NULL,
                original_path TEXT NOT NULL,
                producer_ref TEXT,
                publication_key TEXT,
                captured_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE UNIQUE INDEX idx_execution_artifacts_publication_key
            ON execution_artifacts(execution_id, publication_key);",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    #[tokio::test]
    async fn publication_key_replays_one_occurrence_but_preserves_distinct_ones() {
        // The database, rather than a caller-side retry guess, owns replay.
        let pool = pool().await;
        let execution_id = Uuid::new_v4();
        let attachment_id = Uuid::new_v4();
        let (first, state) = ExecutionArtifact::create_or_replay(
            &pool,
            execution_id,
            attachment_id,
            "checkpoint.md",
            Some("task/checkpoint"),
            "checkpoint-42",
        )
        .await
        .unwrap();
        assert_eq!(state, ExecutionArtifactPublication::Created);
        let (replay, state) = ExecutionArtifact::create_or_replay(
            &pool,
            execution_id,
            attachment_id,
            "checkpoint.md",
            Some("task/checkpoint"),
            "checkpoint-42",
        )
        .await
        .unwrap();
        assert_eq!(state, ExecutionArtifactPublication::Replayed);
        assert_eq!(replay.id, first.id);

        let (second, state) = ExecutionArtifact::create_or_replay(
            &pool,
            execution_id,
            attachment_id,
            "checkpoint.md",
            Some("task/checkpoint"),
            "checkpoint-43",
        )
        .await
        .unwrap();
        assert_eq!(state, ExecutionArtifactPublication::Created);
        assert_ne!(second.id, first.id);
    }
}
