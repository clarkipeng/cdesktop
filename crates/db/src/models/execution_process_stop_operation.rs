use sqlx::{FromRow, SqlitePool};
use uuid::Uuid;

/// The durable, caller-visible outcome of a keyed stop request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopExecutionOutcome {
    Accepted,
    Rejected,
}

impl StopExecutionOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
        }
    }

    fn parse(value: &str) -> Result<Self, sqlx::Error> {
        match value {
            "accepted" => Ok(Self::Accepted),
            "rejected" => Ok(Self::Rejected),
            _ => Err(sqlx::Error::Protocol(format!(
                "invalid execution process stop outcome: {value}"
            ))),
        }
    }
}

#[derive(Debug, FromRow)]
struct StopExecutionOperationRow {
    outcome: Option<String>,
}

/// A replay-safe state for one `(execution_process_id, dedupe_key)` pair.
pub enum StopExecutionOperationState {
    New,
    Pending,
    Complete(StopExecutionOutcome),
}

pub struct StopExecutionOperation;

impl StopExecutionOperation {
    /// Creates the durable intent before the stop is invoked. Completed rows
    /// are never overwritten, so every replay observes its original result.
    pub async fn begin(
        pool: &SqlitePool,
        execution_process_id: Uuid,
        dedupe_key: &str,
    ) -> Result<StopExecutionOperationState, sqlx::Error> {
        let inserted = sqlx::query(
            "INSERT INTO execution_process_stop_operations (execution_process_id, dedupe_key) \
             VALUES (?, ?) ON CONFLICT(execution_process_id, dedupe_key) DO NOTHING",
        )
        .bind(execution_process_id)
        .bind(dedupe_key)
        .execute(pool)
        .await?
        .rows_affected()
            != 0;

        if inserted {
            return Ok(StopExecutionOperationState::New);
        }

        Self::state(pool, execution_process_id, dedupe_key).await
    }

    pub async fn state(
        pool: &SqlitePool,
        execution_process_id: Uuid,
        dedupe_key: &str,
    ) -> Result<StopExecutionOperationState, sqlx::Error> {
        let row = sqlx::query_as::<_, StopExecutionOperationRow>(
            "SELECT outcome FROM execution_process_stop_operations \
             WHERE execution_process_id = ? AND dedupe_key = ?",
        )
        .bind(execution_process_id)
        .bind(dedupe_key)
        .fetch_one(pool)
        .await?;

        match row.outcome {
            Some(outcome) => Ok(StopExecutionOperationState::Complete(
                StopExecutionOutcome::parse(&outcome)?,
            )),
            None => Ok(StopExecutionOperationState::Pending),
        }
    }

    /// First completion wins. This deliberately preserves a definitive reject
    /// instead of turning a replay into a transport retry.
    pub async fn complete(
        pool: &SqlitePool,
        execution_process_id: Uuid,
        dedupe_key: &str,
        outcome: StopExecutionOutcome,
    ) -> Result<StopExecutionOutcome, sqlx::Error> {
        sqlx::query(
            "UPDATE execution_process_stop_operations \
             SET outcome = ?, completed_at = datetime('now', 'subsec') \
             WHERE execution_process_id = ? AND dedupe_key = ? AND outcome IS NULL",
        )
        .bind(outcome.as_str())
        .bind(execution_process_id)
        .bind(dedupe_key)
        .execute(pool)
        .await?;

        match Self::state(pool, execution_process_id, dedupe_key).await? {
            StopExecutionOperationState::Complete(outcome) => Ok(outcome),
            StopExecutionOperationState::New | StopExecutionOperationState::Pending => Err(
                sqlx::Error::Protocol("execution process stop operation was not completed".into()),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use sqlx::{
        SqlitePool,
        sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    };

    use super::*;

    async fn pool(url: &str) -> SqlitePool {
        let options = SqliteConnectOptions::from_str(url)
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE execution_process_stop_operations (\
                execution_process_id BLOB NOT NULL, dedupe_key TEXT NOT NULL, \
                outcome TEXT, created_at TEXT, completed_at TEXT, \
                PRIMARY KEY (execution_process_id, dedupe_key))",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    #[tokio::test]
    async fn accepted_stop_replays_after_a_lost_response_without_another_stop() {
        let pool = pool("sqlite::memory:").await;
        let process_id = Uuid::new_v4();
        let mut stop_calls = 0;

        assert!(matches!(
            StopExecutionOperation::begin(&pool, process_id, "stall-1")
                .await
                .unwrap(),
            StopExecutionOperationState::New
        ));
        stop_calls += 1; // the first response is deliberately "lost"
        StopExecutionOperation::complete(
            &pool,
            process_id,
            "stall-1",
            StopExecutionOutcome::Accepted,
        )
        .await
        .unwrap();

        assert!(matches!(
            StopExecutionOperation::begin(&pool, process_id, "stall-1")
                .await
                .unwrap(),
            StopExecutionOperationState::Complete(StopExecutionOutcome::Accepted)
        ));
        assert_eq!(stop_calls, 1);
    }

    #[tokio::test]
    async fn completed_stop_replays_after_server_restart() {
        let path = std::env::temp_dir().join(format!("cdesktop-stop-{}.sqlite", Uuid::new_v4()));
        let url = format!("sqlite://{}", path.display());
        let process_id = Uuid::new_v4();
        let first_pool = pool(&url).await;
        StopExecutionOperation::begin(&first_pool, process_id, "restart")
            .await
            .unwrap();
        StopExecutionOperation::complete(
            &first_pool,
            process_id,
            "restart",
            StopExecutionOutcome::Accepted,
        )
        .await
        .unwrap();
        first_pool.close().await;

        let second_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::from_str(&url).unwrap())
            .await
            .unwrap();
        assert!(matches!(
            StopExecutionOperation::begin(&second_pool, process_id, "restart")
                .await
                .unwrap(),
            StopExecutionOperationState::Complete(StopExecutionOutcome::Accepted)
        ));
        second_pool.close().await;
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn rejected_stop_replays_as_the_same_definitive_outcome() {
        let pool = pool("sqlite::memory:").await;
        let process_id = Uuid::new_v4();
        StopExecutionOperation::begin(&pool, process_id, "rejected")
            .await
            .unwrap();
        StopExecutionOperation::complete(
            &pool,
            process_id,
            "rejected",
            StopExecutionOutcome::Rejected,
        )
        .await
        .unwrap();

        assert!(matches!(
            StopExecutionOperation::begin(&pool, process_id, "rejected")
                .await
                .unwrap(),
            StopExecutionOperationState::Complete(StopExecutionOutcome::Rejected)
        ));
    }

    #[tokio::test]
    async fn dedupe_key_is_scoped_to_an_execution_process() {
        let pool = pool("sqlite::memory:").await;
        let key = "same-key";
        assert!(matches!(
            StopExecutionOperation::begin(&pool, Uuid::new_v4(), key)
                .await
                .unwrap(),
            StopExecutionOperationState::New
        ));
        assert!(matches!(
            StopExecutionOperation::begin(&pool, Uuid::new_v4(), key)
                .await
                .unwrap(),
            StopExecutionOperationState::New
        ));
    }
}
