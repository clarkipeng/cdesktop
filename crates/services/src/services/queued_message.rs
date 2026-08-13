use chrono::{DateTime, SecondsFormat, Utc};
use db::models::scratch::DraftFollowUpData;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use thiserror::Error;
use ts_rs::TS;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum QueuedMessageError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Timestamp(#[from] chrono::ParseError),
}

/// Represents a queued follow-up message for a session
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct QueuedMessage {
    /// The session this message is queued for
    pub session_id: Uuid,
    /// The follow-up data (message + variant)
    pub data: DraftFollowUpData,
    /// Timestamp when the message was queued
    pub queued_at: DateTime<Utc>,
}

/// Status of the queue for a session (for frontend display)
#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum QueueStatus {
    /// No message queued
    Empty,
    /// Message is queued and waiting for execution to complete
    Queued { message: QueuedMessage },
}

/// Durable service for managing one queued follow-up message per session.
#[derive(Clone)]
pub struct QueuedMessageService {
    pool: SqlitePool,
}

impl QueuedMessageService {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Return interrupted claims to the pending state after a backend restart.
    pub async fn recover_interrupted_claims(&self) -> Result<(), QueuedMessageError> {
        sqlx::query("UPDATE queued_messages SET state = 'queued' WHERE state = 'starting'")
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Queue a message for a session. Replaces any existing queued message.
    pub async fn queue_message(
        &self,
        session_id: Uuid,
        data: DraftFollowUpData,
    ) -> Result<QueuedMessage, QueuedMessageError> {
        let queued = QueuedMessage {
            session_id,
            data,
            queued_at: Utc::now(),
        };
        let data = serde_json::to_string(&queued.data)?;
        let queued_at = format_timestamp(queued.queued_at);

        sqlx::query(
            "INSERT INTO queued_messages (session_id, data, queued_at, state) VALUES (?, ?, ?, 'queued') \
             ON CONFLICT(session_id) DO UPDATE SET data = excluded.data, queued_at = excluded.queued_at, state = 'queued'",
        )
        .bind(session_id)
        .bind(data)
        .bind(queued_at)
        .execute(&self.pool)
        .await?;

        Ok(queued)
    }

    /// Cancel/remove a queued message for a session
    pub async fn cancel_queued(&self, session_id: Uuid) -> Result<(), QueuedMessageError> {
        sqlx::query("DELETE FROM queued_messages WHERE session_id = ?")
            .bind(session_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Get the queued message for a session (if any)
    pub async fn get_queued(
        &self,
        session_id: Uuid,
    ) -> Result<Option<QueuedMessage>, QueuedMessageError> {
        let row = sqlx::query("SELECT data, queued_at FROM queued_messages WHERE session_id = ?")
            .bind(session_id)
            .fetch_optional(&self.pool)
            .await?;

        row.map(|row| decode_row(session_id, &row)).transpose()
    }

    /// Atomically claim the pending message before starting its follow-up.
    pub async fn claim_queued(
        &self,
        session_id: Uuid,
    ) -> Result<Option<QueuedMessage>, QueuedMessageError> {
        let row = sqlx::query(
            "UPDATE queued_messages SET state = 'starting' \
             WHERE session_id = ? AND state = 'queued' \
             RETURNING data, queued_at",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| decode_row(session_id, &row)).transpose()
    }

    /// Delete a claim only after its follow-up process has started successfully.
    pub async fn complete_claim(&self, message: &QueuedMessage) -> Result<(), QueuedMessageError> {
        sqlx::query(
            "DELETE FROM queued_messages WHERE session_id = ? AND queued_at = ? AND state = 'starting'",
        )
        .bind(message.session_id)
        .bind(format_timestamp(message.queued_at))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Make a failed start available for another explicit or automatic attempt.
    pub async fn retry_claim(&self, message: &QueuedMessage) -> Result<(), QueuedMessageError> {
        sqlx::query(
            "UPDATE queued_messages SET state = 'queued' \
             WHERE session_id = ? AND queued_at = ? AND state = 'starting'",
        )
        .bind(message.session_id)
        .bind(format_timestamp(message.queued_at))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Check if a session has a queued message
    pub async fn has_queued(&self, session_id: Uuid) -> Result<bool, QueuedMessageError> {
        let exists: i64 =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM queued_messages WHERE session_id = ?)")
                .bind(session_id)
                .fetch_one(&self.pool)
                .await?;
        Ok(exists != 0)
    }

    /// Get queue status for frontend display
    pub async fn get_status(&self, session_id: Uuid) -> Result<QueueStatus, QueuedMessageError> {
        Ok(match self.get_queued(session_id).await? {
            Some(msg) => QueueStatus::Queued { message: msg },
            None => QueueStatus::Empty,
        })
    }
}

fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn decode_row(
    session_id: Uuid,
    row: &sqlx::sqlite::SqliteRow,
) -> Result<QueuedMessage, QueuedMessageError> {
    let data: String = row.try_get("data")?;
    let queued_at: String = row.try_get("queued_at")?;
    Ok(QueuedMessage {
        session_id,
        data: serde_json::from_str(&data)?,
        queued_at: DateTime::parse_from_rfc3339(&queued_at)?.with_timezone(&Utc),
    })
}

#[cfg(test)]
mod tests {
    use executors::{executors::BaseCodingAgent, profile::ExecutorConfig};
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;

    async fn service() -> QueuedMessageService {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE queued_messages (\
                session_id BLOB PRIMARY KEY NOT NULL,\
                data TEXT NOT NULL,\
                queued_at TEXT NOT NULL,\
                state TEXT NOT NULL DEFAULT 'queued' CHECK (state IN ('queued', 'starting'))\
            )",
        )
        .execute(&pool)
        .await
        .unwrap();
        QueuedMessageService::new(pool)
    }

    fn follow_up(message: &str) -> DraftFollowUpData {
        DraftFollowUpData {
            message: message.to_string(),
            executor_config: ExecutorConfig::new(BaseCodingAgent::ClaudeCode),
        }
    }

    #[tokio::test]
    async fn queue_survives_service_recreation() {
        let service = service().await;
        let session_id = Uuid::new_v4();
        service
            .queue_message(session_id, follow_up("persist me"))
            .await
            .unwrap();

        let restarted = QueuedMessageService::new(service.pool.clone());
        let queued = restarted.get_queued(session_id).await.unwrap().unwrap();

        assert_eq!(queued.data.message, "persist me");
    }

    #[tokio::test]
    async fn failed_start_can_retry_the_same_claim() {
        let service = service().await;
        let session_id = Uuid::new_v4();
        service
            .queue_message(session_id, follow_up("retry me"))
            .await
            .unwrap();

        let claimed = service.claim_queued(session_id).await.unwrap().unwrap();
        assert!(service.claim_queued(session_id).await.unwrap().is_none());

        service.retry_claim(&claimed).await.unwrap();
        let retried = service.claim_queued(session_id).await.unwrap().unwrap();
        assert_eq!(retried.data.message, "retry me");

        service.complete_claim(&retried).await.unwrap();
        assert!(!service.has_queued(session_id).await.unwrap());
    }

    #[tokio::test]
    async fn restart_recovers_an_interrupted_claim() {
        let service = service().await;
        let session_id = Uuid::new_v4();
        service
            .queue_message(session_id, follow_up("recover me"))
            .await
            .unwrap();
        service.claim_queued(session_id).await.unwrap().unwrap();

        let restarted = QueuedMessageService::new(service.pool.clone());
        restarted.recover_interrupted_claims().await.unwrap();

        let recovered = restarted.claim_queued(session_id).await.unwrap().unwrap();
        assert_eq!(recovered.data.message, "recover me");
    }
}
