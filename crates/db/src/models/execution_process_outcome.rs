use chrono::{DateTime, Utc};
use executors::outcome::{ExecutionOutcomeClass, NormalizedExecutionOutcome};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool, types::Json};
use ts_rs::TS;
use uuid::Uuid;

use super::execution_process::ExecutionProcessStatus;

/// Durable normalized outcome of one execution attempt. Written exactly once
/// (primary-key guarded) by the completion winner; see
/// `ExecutionProcess::complete_running_attempt`. Contains only the safe
/// structured fields of the outcome contract — never provider text,
/// credentials, or headers.
#[derive(Debug, Clone, FromRow, Serialize, Deserialize, TS)]
pub struct ExecutionProcessOutcome {
    pub execution_process_id: Uuid,
    #[ts(type = "NormalizedExecutionOutcome")]
    pub outcome: Json<NormalizedExecutionOutcome>,
    pub created_at: DateTime<Utc>,
}

impl ExecutionProcessOutcome {
    /// Record the outcome for an attempt. Idempotent: the first writer wins,
    /// later writes are ignored, so a stale duplicate completion can never
    /// overwrite the authoritative classification.
    pub async fn record(
        pool: &SqlitePool,
        execution_process_id: Uuid,
        outcome: &NormalizedExecutionOutcome,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "INSERT OR IGNORE INTO execution_process_outcomes (execution_process_id, outcome) \
             VALUES (?, ?)",
        )
        .bind(execution_process_id)
        .bind(Json(outcome))
        .execute(pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn find_by_execution_process_id(
        pool: &SqlitePool,
        execution_process_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM execution_process_outcomes WHERE execution_process_id = ?")
            .bind(execution_process_id)
            .fetch_optional(pool)
            .await
    }

    /// Effective outcome for an attempt: the stored adapter classification
    /// when present, otherwise derived from the terminal status so callers
    /// always observe a normalized class without a second write path.
    /// Running and successfully completed attempts have no failure outcome.
    pub fn effective(
        stored: Option<&NormalizedExecutionOutcome>,
        status: &ExecutionProcessStatus,
    ) -> Option<NormalizedExecutionOutcome> {
        match status {
            ExecutionProcessStatus::Running | ExecutionProcessStatus::Completed => None,
            ExecutionProcessStatus::Killed => Some(NormalizedExecutionOutcome::new(
                ExecutionOutcomeClass::UserStopped,
            )),
            ExecutionProcessStatus::Failed => Some(stored.cloned().unwrap_or_else(|| {
                NormalizedExecutionOutcome::new(ExecutionOutcomeClass::Unknown)
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use executors::outcome::OutcomeBindingScope;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;

    async fn pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            r#"CREATE TABLE execution_process_outcomes (
                execution_process_id BLOB PRIMARY KEY NOT NULL,
                outcome TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'subsec'))
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    #[tokio::test]
    async fn record_is_first_writer_wins() {
        let pool = pool().await;
        let id = Uuid::new_v4();
        let first = NormalizedExecutionOutcome::new(ExecutionOutcomeClass::QuotaExhausted)
            .with_provider_code("usage_limit_exceeded");
        let second = NormalizedExecutionOutcome::new(ExecutionOutcomeClass::Unknown);

        assert!(
            ExecutionProcessOutcome::record(&pool, id, &first)
                .await
                .unwrap()
        );
        assert!(
            !ExecutionProcessOutcome::record(&pool, id, &second)
                .await
                .unwrap()
        );

        let stored = ExecutionProcessOutcome::find_by_execution_process_id(&pool, id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.outcome.0.class,
            ExecutionOutcomeClass::QuotaExhausted
        );
        assert_eq!(
            stored.outcome.0.provider_code.as_deref(),
            Some("usage_limit_exceeded")
        );
        assert_eq!(
            stored.outcome.0.binding_scope,
            Some(OutcomeBindingScope::Account)
        );
    }

    #[test]
    fn effective_outcome_derives_from_terminal_status() {
        let stored = NormalizedExecutionOutcome::new(ExecutionOutcomeClass::AuthExpired);

        assert_eq!(
            ExecutionProcessOutcome::effective(None, &ExecutionProcessStatus::Running),
            None
        );
        assert_eq!(
            ExecutionProcessOutcome::effective(Some(&stored), &ExecutionProcessStatus::Completed),
            None
        );
        assert_eq!(
            ExecutionProcessOutcome::effective(None, &ExecutionProcessStatus::Killed)
                .unwrap()
                .class,
            ExecutionOutcomeClass::UserStopped
        );
        assert_eq!(
            ExecutionProcessOutcome::effective(Some(&stored), &ExecutionProcessStatus::Failed)
                .unwrap()
                .class,
            ExecutionOutcomeClass::AuthExpired
        );
        assert_eq!(
            ExecutionProcessOutcome::effective(None, &ExecutionProcessStatus::Failed)
                .unwrap()
                .class,
            ExecutionOutcomeClass::Unknown
        );
    }
}
