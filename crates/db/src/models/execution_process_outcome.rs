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

    /// Lists the normalized outcomes recorded for executions in one session.
    /// The outcome record deliberately contains only the display-safe
    /// normalized contract, never the execution command configuration.
    pub async fn find_by_session_id(
        pool: &SqlitePool,
        session_id: Uuid,
    ) -> Result<Vec<Self>, sqlx::Error> {
        sqlx::query_as(
            "SELECT epo.* FROM execution_process_outcomes epo \
             JOIN execution_processes ep ON ep.id = epo.execution_process_id \
             WHERE ep.session_id = ? ORDER BY epo.created_at DESC",
        )
        .bind(session_id)
        .fetch_all(pool)
        .await
    }

    /// A terminal process is not proof that its detached tools were cleaned.
    pub async fn unconfirmed_cleanup_for_workspace(
        pool: &SqlitePool,
        workspace_id: Uuid,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT epo.execution_process_id FROM execution_process_outcomes epo \
             JOIN execution_processes ep ON ep.id = epo.execution_process_id \
             JOIN sessions s ON s.id = ep.session_id \
             WHERE s.workspace_id = ? AND json_extract(epo.outcome, '$.cleanup_confirmed') = 0 \
             LIMIT 1",
        )
        .bind(workspace_id)
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
            ExecutionProcessStatus::Killed => {
                let mut outcome =
                    NormalizedExecutionOutcome::new(ExecutionOutcomeClass::UserStopped);
                outcome.cleanup_confirmed = stored.and_then(|value| value.cleanup_confirmed);
                Some(outcome)
            }
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

    async fn outcomes_pool() -> SqlitePool {
        let pool = pool().await;
        sqlx::query(
            "CREATE TABLE execution_processes (id BLOB PRIMARY KEY NOT NULL, session_id BLOB NOT NULL)",
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

    #[tokio::test]
    async fn unconfirmed_cleanup_survives_reopen_and_cannot_be_overwritten_by_retry() {
        let root = tempfile::tempdir().unwrap();
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(root.path().join("outcomes.sqlite"))
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .connect_with(options.clone())
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE sessions (id BLOB PRIMARY KEY, workspace_id BLOB); \
             CREATE TABLE execution_processes (id BLOB PRIMARY KEY, session_id BLOB); \
             CREATE TABLE execution_process_outcomes (execution_process_id BLOB PRIMARY KEY, \
             outcome TEXT, created_at TEXT DEFAULT (datetime('now', 'subsec')));",
        )
        .execute(&pool)
        .await
        .unwrap();
        let workspaces = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let processes = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        for (index, confirmed) in [Some(false), Some(true), None].into_iter().enumerate() {
            let session = Uuid::new_v4();
            sqlx::query("INSERT INTO sessions VALUES (?, ?)")
                .bind(session)
                .bind(workspaces[index])
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO execution_processes VALUES (?, ?)")
                .bind(processes[index])
                .bind(session)
                .execute(&pool)
                .await
                .unwrap();
            let mut outcome = NormalizedExecutionOutcome::new(ExecutionOutcomeClass::UserStopped);
            outcome.cleanup_confirmed = confirmed;
            ExecutionProcessOutcome::record(&pool, processes[index], &outcome)
                .await
                .unwrap();
        }
        pool.close().await;
        let pool = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .unwrap();
        assert_eq!(
            ExecutionProcessOutcome::unconfirmed_cleanup_for_workspace(&pool, workspaces[0])
                .await
                .unwrap(),
            Some(processes[0])
        );
        for workspace in &workspaces[1..] {
            assert_eq!(
                ExecutionProcessOutcome::unconfirmed_cleanup_for_workspace(&pool, *workspace)
                    .await
                    .unwrap(),
                None
            );
        }
        let retry = NormalizedExecutionOutcome::new(ExecutionOutcomeClass::UserStopped);
        assert!(
            !ExecutionProcessOutcome::record(&pool, processes[0], &retry)
                .await
                .unwrap()
        );
        let stored = ExecutionProcessOutcome::find_by_execution_process_id(&pool, processes[0])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.outcome.0.cleanup_confirmed, Some(false));
        assert_eq!(
            ExecutionProcessOutcome::effective(
                Some(&stored.outcome.0),
                &ExecutionProcessStatus::Killed
            )
            .unwrap()
            .cleanup_confirmed,
            Some(false)
        );
    }

    #[tokio::test]
    async fn lists_only_outcomes_for_the_requested_session() {
        let pool = outcomes_pool().await;
        let session_id = Uuid::new_v4();
        let other_session_id = Uuid::new_v4();
        let execution_process_id = Uuid::new_v4();
        let other_execution_process_id = Uuid::new_v4();

        for (id, session_id) in [
            (execution_process_id, session_id),
            (other_execution_process_id, other_session_id),
        ] {
            sqlx::query("INSERT INTO execution_processes (id, session_id) VALUES (?, ?)")
                .bind(id)
                .bind(session_id)
                .execute(&pool)
                .await
                .unwrap();
        }

        ExecutionProcessOutcome::record(
            &pool,
            execution_process_id,
            &NormalizedExecutionOutcome::new(ExecutionOutcomeClass::QuotaExhausted),
        )
        .await
        .unwrap();
        ExecutionProcessOutcome::record(
            &pool,
            other_execution_process_id,
            &NormalizedExecutionOutcome::new(ExecutionOutcomeClass::AuthExpired),
        )
        .await
        .unwrap();

        let outcomes = ExecutionProcessOutcome::find_by_session_id(&pool, session_id)
            .await
            .unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].execution_process_id, execution_process_id);

        assert!(
            ExecutionProcessOutcome::find_by_session_id(&pool, Uuid::new_v4())
                .await
                .unwrap()
                .is_empty()
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
