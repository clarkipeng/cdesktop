use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{FromRow, SqlitePool, types::Json};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow)]
pub struct TaskLaunch {
    pub id: Uuid,
    pub contract_version: i64,
    pub task_id: String,
    pub incarnation_generation: i64,
    pub attempt_id: String,
    pub idempotency_key: String,
    pub launch: Json<Value>,
    pub phase: String,
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub owner_instance_id: Uuid,
    pub effect_created: bool,
    pub history_ref: Option<String>,
    pub outcome: Option<Json<Value>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug)]
pub struct NewTaskLaunch<'a> {
    pub task_id: &'a str,
    pub incarnation_generation: i64,
    pub attempt_id: &'a str,
    pub idempotency_key: &'a str,
    pub launch: &'a Value,
    pub workspace_id: Uuid,
    pub session_id: Uuid,
    pub owner_instance_id: Uuid,
}

#[derive(Debug, Error)]
pub enum TaskLaunchError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("The idempotency key or attempt identity belongs to different launch parameters")]
    Conflict,
    #[error("The task incarnation generation is stale")]
    StaleGeneration,
}

impl TaskLaunch {
    /// Durably authorizes native identities before any workspace side effect.
    /// Concurrent callers either own the single inserted row or read it back.
    pub async fn begin(
        pool: &SqlitePool,
        launch: NewTaskLaunch<'_>,
    ) -> Result<(Self, bool), TaskLaunchError> {
        let inserted = sqlx::query(
            r#"INSERT INTO task_launches (
                   id, contract_version, task_id, incarnation_generation,
                   attempt_id, idempotency_key, launch, workspace_id,
                   session_id, owner_instance_id
               )
               SELECT ?, 1, ?, ?, ?, ?, ?, ?, ?, ?
               WHERE NOT EXISTS (
                   SELECT 1 FROM task_launches
                   WHERE task_id = ? AND incarnation_generation > ?
               )
               ON CONFLICT DO NOTHING"#,
        )
        .bind(Uuid::new_v4())
        .bind(launch.task_id)
        .bind(launch.incarnation_generation)
        .bind(launch.attempt_id)
        .bind(launch.idempotency_key)
        .bind(Json(launch.launch))
        .bind(launch.workspace_id)
        .bind(launch.session_id)
        .bind(launch.owner_instance_id)
        .bind(launch.task_id)
        .bind(launch.incarnation_generation)
        .execute(pool)
        .await?
        .rows_affected()
            == 1;

        if inserted {
            let row = Self::find_by_key(pool, launch.idempotency_key)
                .await?
                .ok_or(sqlx::Error::RowNotFound)?;
            return Ok((row, true));
        }

        if let Some(existing) = Self::find_by_key(pool, launch.idempotency_key).await? {
            existing.matches(&launch)?;
            return Ok((existing, false));
        }

        let same_attempt = sqlx::query_as::<_, Self>(
            "SELECT * FROM task_launches WHERE task_id = ? \
             AND incarnation_generation = ? AND attempt_id = ?",
        )
        .bind(launch.task_id)
        .bind(launch.incarnation_generation)
        .bind(launch.attempt_id)
        .fetch_optional(pool)
        .await?;
        if same_attempt.is_some() {
            return Err(TaskLaunchError::Conflict);
        }

        let newer_exists: i64 = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM task_launches \
             WHERE task_id = ? AND incarnation_generation > ?)",
        )
        .bind(launch.task_id)
        .bind(launch.incarnation_generation)
        .fetch_one(pool)
        .await?;
        if newer_exists != 0 {
            return Err(TaskLaunchError::StaleGeneration);
        }

        Err(TaskLaunchError::Conflict)
    }

    pub async fn find_by_key(
        pool: &SqlitePool,
        idempotency_key: &str,
    ) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as("SELECT * FROM task_launches WHERE idempotency_key = ?")
            .bind(idempotency_key)
            .fetch_optional(pool)
            .await
    }

    pub async fn mark_active(
        pool: &SqlitePool,
        idempotency_key: &str,
        owner_instance_id: Uuid,
    ) -> Result<Self, sqlx::Error> {
        sqlx::query(
            "UPDATE task_launches SET phase = 'active', effect_created = 1, \
             updated_at = datetime('now', 'subsec') \
             WHERE idempotency_key = ? AND owner_instance_id = ? AND phase = 'pending'",
        )
        .bind(idempotency_key)
        .bind(owner_instance_id)
        .execute(pool)
        .await?;
        Self::find_by_key(pool, idempotency_key)
            .await?
            .ok_or(sqlx::Error::RowNotFound)
    }

    pub async fn reconcile_active(
        pool: &SqlitePool,
        idempotency_key: &str,
    ) -> Result<Self, sqlx::Error> {
        sqlx::query(
            "UPDATE task_launches SET phase = 'active', effect_created = 1, \
             updated_at = datetime('now', 'subsec') \
             WHERE idempotency_key = ? AND phase = 'pending'",
        )
        .bind(idempotency_key)
        .execute(pool)
        .await?;
        Self::find_by_key(pool, idempotency_key)
            .await?
            .ok_or(sqlx::Error::RowNotFound)
    }

    pub async fn mark_outcome(
        pool: &SqlitePool,
        idempotency_key: &str,
        phase: &str,
        outcome: &Value,
        effect_created: bool,
    ) -> Result<Self, sqlx::Error> {
        sqlx::query(
            "UPDATE task_launches SET phase = ?, outcome = ?, effect_created = ?, \
             updated_at = datetime('now', 'subsec') \
             WHERE idempotency_key = ? AND phase IN ('pending', 'active')",
        )
        .bind(phase)
        .bind(Json(outcome))
        .bind(effect_created)
        .bind(idempotency_key)
        .execute(pool)
        .await?;
        Self::find_by_key(pool, idempotency_key)
            .await?
            .ok_or(sqlx::Error::RowNotFound)
    }

    fn matches(&self, requested: &NewTaskLaunch<'_>) -> Result<(), TaskLaunchError> {
        if self.task_id != requested.task_id
            || self.incarnation_generation != requested.incarnation_generation
            || self.attempt_id != requested.attempt_id
            || self.idempotency_key != requested.idempotency_key
            || self.launch.0 != *requested.launch
        {
            return Err(TaskLaunchError::Conflict);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use tokio::sync::Barrier;

    use super::*;

    async fn pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(
                SqliteConnectOptions::from_str("sqlite::memory:")
                    .unwrap()
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        sqlx::query(
            r#"CREATE TABLE task_launches (
                id BLOB PRIMARY KEY NOT NULL,
                contract_version INTEGER NOT NULL,
                task_id TEXT NOT NULL,
                incarnation_generation INTEGER NOT NULL,
                attempt_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                launch TEXT NOT NULL,
                phase TEXT NOT NULL DEFAULT 'pending',
                workspace_id BLOB NOT NULL,
                session_id BLOB NOT NULL,
                owner_instance_id BLOB NOT NULL,
                effect_created INTEGER NOT NULL DEFAULT 0,
                history_ref TEXT,
                outcome TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
                UNIQUE (task_id, incarnation_generation, attempt_id)
            )"#,
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    fn request<'a>(
        key: &'a str,
        generation: i64,
        launch: &'a Value,
        owner: Uuid,
    ) -> NewTaskLaunch<'a> {
        NewTaskLaunch {
            task_id: "task-a",
            incarnation_generation: generation,
            attempt_id: "attempt-a",
            idempotency_key: key,
            launch,
            workspace_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            owner_instance_id: owner,
        }
    }

    #[tokio::test]
    async fn duplicate_key_returns_the_original_native_identity() {
        let pool = pool().await;
        let owner = Uuid::new_v4();
        let launch = serde_json::json!({"name": "worker"});
        let (first, inserted) = TaskLaunch::begin(&pool, request("key-a", 1, &launch, owner))
            .await
            .unwrap();
        assert!(inserted);

        let (second, inserted) = TaskLaunch::begin(
            &pool,
            NewTaskLaunch {
                workspace_id: Uuid::new_v4(),
                session_id: Uuid::new_v4(),
                ..request("key-a", 1, &launch, owner)
            },
        )
        .await
        .unwrap();

        assert!(!inserted);
        assert_eq!(second.workspace_id, first.workspace_id);
        assert_eq!(second.session_id, first.session_id);
    }

    #[tokio::test]
    async fn same_key_with_different_parameters_is_a_conflict() {
        let pool = pool().await;
        let owner = Uuid::new_v4();
        let first = serde_json::json!({"name": "first"});
        TaskLaunch::begin(&pool, request("key-a", 1, &first, owner))
            .await
            .unwrap();
        let changed = serde_json::json!({"name": "changed"});

        assert!(matches!(
            TaskLaunch::begin(&pool, request("key-a", 1, &changed, owner)).await,
            Err(TaskLaunchError::Conflict)
        ));
    }

    #[tokio::test]
    async fn older_generation_is_rejected_after_newer_generation() {
        let pool = pool().await;
        let owner = Uuid::new_v4();
        let launch = serde_json::json!({"name": "worker"});
        TaskLaunch::begin(&pool, request("new", 2, &launch, owner))
            .await
            .unwrap();

        assert!(matches!(
            TaskLaunch::begin(&pool, request("old", 1, &launch, owner)).await,
            Err(TaskLaunchError::StaleGeneration)
        ));
    }

    #[tokio::test]
    async fn concurrent_duplicates_authorize_one_effect() {
        let pool = Arc::new(pool().await);
        let barrier = Arc::new(Barrier::new(3));
        let owner = Uuid::new_v4();
        let mut handles = Vec::new();
        for _ in 0..2 {
            let pool = pool.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                let launch = serde_json::json!({"name": "worker"});
                barrier.wait().await;
                TaskLaunch::begin(&pool, request("same", 1, &launch, owner))
                    .await
                    .unwrap()
            }));
        }
        barrier.wait().await;
        let first = handles.remove(0).await.unwrap();
        let second = handles.remove(0).await.unwrap();
        assert_ne!(first.1, second.1);
        assert_eq!(first.0.workspace_id, second.0.workspace_id);
        assert_eq!(first.0.session_id, second.0.session_id);
    }

    #[tokio::test]
    async fn crash_reconciliation_preserves_the_reserved_identity() {
        let pool = pool().await;
        let owner = Uuid::new_v4();
        let launch = serde_json::json!({"name": "worker"});
        let (reserved, _) = TaskLaunch::begin(&pool, request("crash", 1, &launch, owner))
            .await
            .unwrap();

        let active = TaskLaunch::reconcile_active(&pool, "crash").await.unwrap();

        assert_eq!(active.workspace_id, reserved.workspace_id);
        assert_eq!(active.session_id, reserved.session_id);
        assert_eq!(active.phase, "active");
        assert!(active.effect_created);
    }

    #[tokio::test]
    async fn terminal_outcome_is_not_reopened_by_a_late_replay() {
        let pool = pool().await;
        let owner = Uuid::new_v4();
        let launch = serde_json::json!({"name": "worker"});
        TaskLaunch::begin(&pool, request("lost", 1, &launch, owner))
            .await
            .unwrap();
        TaskLaunch::mark_outcome(
            &pool,
            "lost",
            "terminal",
            &serde_json::json!({"kind": "lost"}),
            false,
        )
        .await
        .unwrap();

        let replay = TaskLaunch::reconcile_active(&pool, "lost").await.unwrap();

        assert_eq!(replay.phase, "terminal");
        assert!(!replay.effect_created);
        assert_eq!(replay.outcome.unwrap().0["kind"], "lost");
    }
}
