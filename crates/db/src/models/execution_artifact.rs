use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO execution_artifacts (id, execution_id, attachment_id, original_path, producer_ref) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(Uuid::new_v4())
        .bind(execution_id)
        .bind(attachment_id)
        .bind(original_path)
        .bind(producer_ref)
        .execute(pool)
        .await?;
        Ok(())
    }
}
