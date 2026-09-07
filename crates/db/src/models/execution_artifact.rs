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
    pub captured_at: DateTime<Utc>,
}

impl ExecutionArtifact {
    pub async fn create(
        pool: &SqlitePool,
        execution_id: Uuid,
        attachment_id: Uuid,
        original_path: &str,
        producer_ref: Option<&str>,
    ) -> Result<Self, sqlx::Error> {
        let id = Uuid::new_v4();
        sqlx::query_as::<_, Self>(
            "INSERT INTO execution_artifacts (id, execution_id, attachment_id, original_path, producer_ref)
             VALUES (?, ?, ?, ?, ?)
             RETURNING id, execution_id, attachment_id, original_path, producer_ref, captured_at",
        )
        .bind(id)
        .bind(execution_id)
        .bind(attachment_id)
        .bind(original_path)
        .bind(producer_ref)
        .fetch_one(pool)
        .await
    }

    pub async fn find_by_id(pool: &SqlitePool, id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as::<_, Self>("SELECT id, execution_id, attachment_id, original_path, producer_ref, captured_at FROM execution_artifacts WHERE id = ?")
            .bind(id)
            .fetch_optional(pool)
            .await
    }
}
