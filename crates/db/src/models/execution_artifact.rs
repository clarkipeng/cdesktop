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
}

impl ExecutionArtifact {
    pub async fn create(
        conn: &mut SqliteConnection,
        execution_id: Uuid,
        attachment_id: Uuid,
        original_path: &str,
        original_name: &str,
        producer_ref: Option<&str>,
        publication_key: &str,
    ) -> Result<Self, sqlx::Error> {
        sqlx::query_as::<_, Self>(
            "INSERT INTO execution_artifacts
             (id, execution_id, attachment_id, original_path, original_name, producer_ref, publication_key)
             VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING *",
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

    pub async fn find_by_publication(
        conn: &mut SqliteConnection,
        execution_id: Uuid,
        publication_key: &str,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as::<_, Self>(
            "SELECT * FROM execution_artifacts WHERE execution_id = ? AND publication_key = ?",
        )
        .bind(execution_id)
        .bind(publication_key)
        .fetch_optional(conn)
        .await
    }

    pub async fn find_by_id(pool: &SqlitePool, id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as::<_, Self>("SELECT * FROM execution_artifacts WHERE id = ?")
            .bind(id)
            .fetch_optional(pool)
            .await
    }
}
