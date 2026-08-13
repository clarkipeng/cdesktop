use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, SqlitePool, Type, types::Json};
use ts_rs::TS;
use uuid::Uuid;

#[derive(Debug, Clone, Type, Serialize, Deserialize, PartialEq, TS)]
#[sqlx(type_name = "session_command_intent", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
#[ts(use_ts_enum)]
pub enum SessionCommandIntent {
    Continue,
    Replace,
}

#[derive(Debug, Clone, Type, Serialize, Deserialize, PartialEq, TS)]
#[sqlx(type_name = "session_command_state", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
#[ts(use_ts_enum)]
pub enum SessionCommandState {
    Pending,
    Claimed,
    Done,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize, TS)]
pub struct SessionCommand {
    pub id: Uuid,
    pub session_id: Uuid,
    pub dedupe_key: Option<String>,
    pub intent: SessionCommandIntent,
    pub body: String,
    #[ts(type = "unknown")]
    pub config: Option<Json<Value>>,
    pub state: SessionCommandState,
    pub execution_process_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

pub struct NewSessionCommand {
    pub session_id: Uuid,
    pub dedupe_key: Option<String>,
    pub intent: SessionCommandIntent,
    pub body: String,
    pub config: Option<Value>,
}

impl SessionCommand {
    pub async fn enqueue(
        pool: &SqlitePool,
        command: NewSessionCommand,
    ) -> Result<Self, sqlx::Error> {
        let id = Uuid::new_v4();
        let config = command.config.map(Json);
        let inserted = sqlx::query_as::<_, Self>(
            r#"INSERT INTO session_commands (
                   id, session_id, dedupe_key, intent, body, config
               ) VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
               RETURNING *"#,
        )
        .bind(id)
        .bind(command.session_id)
        .bind(command.dedupe_key.as_deref())
        .bind(command.intent)
        .bind(command.body)
        .bind(config)
        .fetch_optional(pool)
        .await?;

        if let Some(inserted) = inserted {
            return Ok(inserted);
        }

        sqlx::query_as::<_, Self>("SELECT * FROM session_commands WHERE dedupe_key = ?")
            .bind(command.dedupe_key)
            .fetch_one(pool)
            .await
    }

    pub async fn pending(pool: &SqlitePool, session_id: Uuid) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as::<_, Self>(
            "SELECT * FROM session_commands \
             WHERE session_id = ? AND state = 'pending' \
             ORDER BY rowid",
        )
        .bind(session_id)
        .fetch_all(pool)
        .await
    }

    pub async fn claim_pending(
        pool: &SqlitePool,
        session_id: Uuid,
        execution_process_id: Uuid,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let mut transaction = pool.begin().await?;
        sqlx::query(
            r#"UPDATE session_commands
               SET state = 'claimed', execution_process_id = ?
               WHERE id IN (
                   SELECT id FROM session_commands
                   WHERE session_id = ? AND state = 'pending'
               )"#,
        )
        .bind(execution_process_id)
        .bind(session_id)
        .execute(&mut *transaction)
        .await?;
        let claimed = sqlx::query_as::<_, Self>(
            r#"SELECT * FROM session_commands
               WHERE execution_process_id = ? AND state = 'claimed'
               ORDER BY rowid"#,
        )
        .bind(execution_process_id)
        .fetch_all(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(claimed)
    }
}

#[cfg(test)]
mod tests {
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;

    async fn pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            r#"CREATE TABLE session_commands (
                id BLOB PRIMARY KEY NOT NULL,
                session_id BLOB NOT NULL,
                dedupe_key TEXT,
                intent TEXT NOT NULL,
                body TEXT NOT NULL,
                config TEXT,
                state TEXT NOT NULL DEFAULT 'pending',
                execution_process_id BLOB,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
                finished_at TEXT
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE UNIQUE INDEX dedupe ON session_commands(dedupe_key) \
             WHERE dedupe_key IS NOT NULL",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    fn command(session_id: Uuid, body: &str, dedupe_key: Option<&str>) -> NewSessionCommand {
        NewSessionCommand {
            session_id,
            dedupe_key: dedupe_key.map(str::to_owned),
            intent: SessionCommandIntent::Continue,
            body: body.to_owned(),
            config: None,
        }
    }

    #[tokio::test]
    async fn enqueue_is_append_only_and_idempotent_when_keyed() {
        let pool = pool().await;
        let session_id = Uuid::new_v4();
        let first = SessionCommand::enqueue(&pool, command(session_id, "first", Some("a")))
            .await
            .unwrap();
        let duplicate =
            SessionCommand::enqueue(&pool, command(session_id, "ignored duplicate", Some("a")))
                .await
                .unwrap();
        SessionCommand::enqueue(&pool, command(session_id, "second", None))
            .await
            .unwrap();

        assert_eq!(duplicate.id, first.id);
        assert_eq!(
            SessionCommand::pending(&pool, session_id)
                .await
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn claim_batches_pending_commands_in_order() {
        let pool = pool().await;
        let session_id = Uuid::new_v4();
        SessionCommand::enqueue(&pool, command(session_id, "first", None))
            .await
            .unwrap();
        SessionCommand::enqueue(&pool, command(session_id, "second", None))
            .await
            .unwrap();

        let execution_id = Uuid::new_v4();
        let claimed = SessionCommand::claim_pending(&pool, session_id, execution_id)
            .await
            .unwrap();

        assert_eq!(
            claimed
                .iter()
                .map(|item| item.body.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert!(
            claimed
                .iter()
                .all(|item| item.execution_process_id == Some(execution_id))
        );
        assert!(
            SessionCommand::pending(&pool, session_id)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
