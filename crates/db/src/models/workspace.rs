use std::path::PathBuf;

use chrono::{DateTime, Utc};
use executors::actions::{ExecutorAction, ExecutorActionType};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use thiserror::Error;
use ts_rs::TS;
use uuid::Uuid;

/// Maximum length for auto-generated workspace names (derived from first user prompt)
const WORKSPACE_NAME_MAX_LEN: usize = 60;

use super::{
    execution_process::ExecutorActionField,
    repo::Repo,
    session::Session,
    workspace_repo::{RepoWithTargetBranch, WorkspaceRepo},
};

#[derive(Debug, Error)]
pub enum WorkspaceError {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error("Workspace not found")]
    WorkspaceNotFound,
    #[error("Validation error: {0}")]
    ValidationError(String),
    #[error("Branch not found: {0}")]
    BranchNotFound(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct ContainerInfo {
    pub workspace_id: Uuid,
}

#[derive(Debug)]
struct WorkspaceContainerRefRow {
    id: Uuid,
    container_ref: String,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    TS,
    sqlx::Type,
    strum_macros::Display,
    strum_macros::EnumString,
)]
#[serde(rename_all = "lowercase")]
#[sqlx(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum WorkspaceSource {
    User,
    Routine,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize, TS)]
pub struct Workspace {
    pub id: Uuid,
    pub task_id: Option<Uuid>,
    pub container_ref: Option<String>,
    pub branch: String,
    pub setup_completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub archived: bool,
    /// Derived from `pin_order IS NOT NULL`. Always populated by SELECTs.
    pub pinned: bool,
    /// Position in the pinned list (0 = top). NULL for unpinned workspaces.
    pub pin_order: Option<i64>,
    pub name: Option<String>,
    pub worktree_deleted: bool,
    pub use_worktree: bool,
    pub source: WorkspaceSource,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub struct WorkspaceWithStatus {
    #[serde(flatten)]
    #[ts(flatten)]
    pub workspace: Workspace,
    pub is_running: bool,
    pub is_errored: bool,
}

impl std::ops::Deref for WorkspaceWithStatus {
    type Target = Workspace;
    fn deref(&self) -> &Self::Target {
        &self.workspace
    }
}

#[derive(Debug, Deserialize, TS)]
pub struct CreateFollowUpAttempt {
    pub prompt: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceContext {
    pub workspace: Workspace,
    pub workspace_repos: Vec<RepoWithTargetBranch>,
    pub orchestrator_session_id: Option<Uuid>,
}

#[derive(Debug, Deserialize, TS)]
pub struct CreateWorkspace {
    pub branch: String,
    pub name: Option<String>,
    pub use_worktree: bool,
}

impl Workspace {
    /// Where this workspace's attached `repo` lives on disk.
    ///
    /// In worktree mode, this is `<container_ref>/<repo.name>`. Returns `None`
    /// in worktree mode when the workspace has not yet been backfilled (lazy
    /// `start_container`). In direct mode (`use_worktree = false`), this is
    /// always `repo.path`.
    pub fn execution_dir(&self, repo: &Repo) -> Option<PathBuf> {
        if self.use_worktree {
            let container_ref = self.container_ref.as_deref()?;
            Some(PathBuf::from(container_ref).join(&repo.name))
        } else {
            Some(repo.path.clone())
        }
    }

    /// Fetch all workspaces. Newest first.
    pub async fn fetch_all(pool: &SqlitePool) -> Result<Vec<Self>, WorkspaceError> {
        let workspaces = sqlx::query_as!(
            Workspace,
            r#"SELECT id AS "id!: Uuid",
                          task_id AS "task_id: Uuid",
                          container_ref,
                          branch,
                          setup_completed_at AS "setup_completed_at: DateTime<Utc>",
                          created_at AS "created_at!: DateTime<Utc>",
                          updated_at AS "updated_at!: DateTime<Utc>",
                          archived AS "archived!: bool",
                          (pin_order IS NOT NULL) AS "pinned!: bool",
                          pin_order AS "pin_order: i64",
                          name,
                          worktree_deleted AS "worktree_deleted!: bool",
                          use_worktree AS "use_worktree!: bool",
                          source AS "source!: WorkspaceSource"
                   FROM workspaces
                   ORDER BY created_at DESC"#
        )
        .fetch_all(pool)
        .await
        .map_err(WorkspaceError::Database)?;

        Ok(workspaces)
    }

    /// Load full workspace context by workspace ID.
    pub async fn load_context(
        pool: &SqlitePool,
        workspace_id: Uuid,
    ) -> Result<WorkspaceContext, WorkspaceError> {
        let workspace = Workspace::find_by_id(pool, workspace_id)
            .await?
            .ok_or(WorkspaceError::WorkspaceNotFound)?;

        let workspace_repos =
            WorkspaceRepo::find_repos_with_target_branch_for_workspace(pool, workspace_id).await?;
        let orchestrator_session_id = Session::find_first_by_workspace_id(pool, workspace_id)
            .await?
            .map(|session| session.id);

        Ok(WorkspaceContext {
            workspace,
            workspace_repos,
            orchestrator_session_id,
        })
    }

    /// Update container reference
    pub async fn update_container_ref(
        pool: &SqlitePool,
        workspace_id: Uuid,
        container_ref: &str,
    ) -> Result<(), sqlx::Error> {
        let now = Utc::now();
        sqlx::query!(
            "UPDATE workspaces SET container_ref = $1, updated_at = $2 WHERE id = $3",
            container_ref,
            now,
            workspace_id
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn mark_worktree_deleted(
        pool: &SqlitePool,
        workspace_id: Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "UPDATE workspaces SET worktree_deleted = TRUE, updated_at = datetime('now') WHERE id = ?",
            workspace_id
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn clear_worktree_deleted(
        pool: &SqlitePool,
        workspace_id: Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "UPDATE workspaces SET worktree_deleted = FALSE, updated_at = datetime('now') WHERE id = ?",
            workspace_id
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Update the workspace's updated_at timestamp to prevent cleanup.
    /// Call this when the workspace is accessed (e.g., opened in editor).
    pub async fn touch(pool: &SqlitePool, workspace_id: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "UPDATE workspaces SET updated_at = datetime('now', 'subsec') WHERE id = ?",
            workspace_id
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn find_by_id(pool: &SqlitePool, id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as!(
            Workspace,
            r#"SELECT  id                AS "id!: Uuid",
                       task_id           AS "task_id: Uuid",
                       container_ref,
                       branch,
                       setup_completed_at AS "setup_completed_at: DateTime<Utc>",
                       created_at        AS "created_at!: DateTime<Utc>",
                       updated_at        AS "updated_at!: DateTime<Utc>",
                       archived          AS "archived!: bool",
                       (pin_order IS NOT NULL) AS "pinned!: bool",
                       pin_order         AS "pin_order: i64",
                       name,
                       worktree_deleted  AS "worktree_deleted!: bool",
                       use_worktree      AS "use_worktree!: bool",
                       source            AS "source!: WorkspaceSource"
               FROM    workspaces
               WHERE   id = $1"#,
            id
        )
        .fetch_optional(pool)
        .await
    }

    pub async fn find_by_rowid(pool: &SqlitePool, rowid: i64) -> Result<Option<Self>, sqlx::Error> {
        sqlx::query_as!(
            Workspace,
            r#"SELECT  id                AS "id!: Uuid",
                       task_id           AS "task_id: Uuid",
                       container_ref,
                       branch,
                       setup_completed_at AS "setup_completed_at: DateTime<Utc>",
                       created_at        AS "created_at!: DateTime<Utc>",
                       updated_at        AS "updated_at!: DateTime<Utc>",
                       archived          AS "archived!: bool",
                       (pin_order IS NOT NULL) AS "pinned!: bool",
                       pin_order         AS "pin_order: i64",
                       name,
                       worktree_deleted  AS "worktree_deleted!: bool",
                       use_worktree      AS "use_worktree!: bool",
                       source            AS "source!: WorkspaceSource"
               FROM    workspaces
               WHERE   rowid = $1"#,
            rowid
        )
        .fetch_optional(pool)
        .await
    }

    pub async fn container_ref_exists(
        pool: &SqlitePool,
        container_ref: &str,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query!(
            r#"SELECT EXISTS(SELECT 1 FROM workspaces WHERE container_ref = ?) as "exists!: bool""#,
            container_ref
        )
        .fetch_one(pool)
        .await?;

        Ok(result.exists)
    }

    /// Find workspaces that are expired and eligible for cleanup.
    /// Uses accelerated cleanup (1 hour) for archived workspaces.
    /// Uses standard cleanup (72 hours) for non-archived workspaces.
    ///
    /// Expiry is measured from the later of the workspace's own `updated_at`
    /// and its most recent completed execution, expressed as an aggregate over
    /// a `UNION ALL` rather than a scalar `max()`. The scalar form returns NULL
    /// as soon as one argument is NULL, so a workspace that never completed an
    /// execution compared against NULL and could never expire - it kept its
    /// worktree forever. `updated_at` is always present, so the aggregate form
    /// cannot produce NULL and no workspace can hide behind an empty execution
    /// history. This mirrors [`Self::find_idle_for_auto_archive`].
    ///
    /// Only worktree-backed workspaces are selected: a `use_worktree = FALSE`
    /// workspace owns no managed directory, so there is nothing to reclaim and
    /// nothing to mark. `container_ref` is deliberately not required, because
    /// `WorkspaceManager::workspace_dir_for` derives the directory from the
    /// workspace id and name when the column is unset.
    pub async fn find_expired_for_cleanup(
        pool: &SqlitePool,
    ) -> Result<Vec<Workspace>, sqlx::Error> {
        sqlx::query_as!(
            Workspace,
            r#"
            SELECT
                w.id as "id!: Uuid",
                w.task_id as "task_id: Uuid",
                w.container_ref,
                w.branch as "branch!",
                w.setup_completed_at as "setup_completed_at: DateTime<Utc>",
                w.created_at as "created_at!: DateTime<Utc>",
                w.updated_at as "updated_at!: DateTime<Utc>",
                w.archived as "archived!: bool",
                (w.pin_order IS NOT NULL) as "pinned!: bool",
                w.pin_order as "pin_order: i64",
                w.name,
                w.worktree_deleted as "worktree_deleted!: bool",
                w.use_worktree as "use_worktree!: bool",
                w.source as "source!: WorkspaceSource"
            FROM workspaces w
            WHERE w.use_worktree = TRUE
                AND w.worktree_deleted = FALSE
                AND NOT EXISTS (
                    SELECT 1
                    FROM sessions s
                    JOIN execution_processes ep ON ep.session_id = s.id
                    WHERE s.workspace_id = w.id
                      AND ep.completed_at IS NULL
                )
                AND datetime('now',
                    CASE
                        WHEN w.archived = 1
                        THEN '-1 hours'
                        ELSE '-72 hours'
                    END
                ) > (
                    SELECT MAX(activity)
                    FROM (
                        SELECT datetime(w.updated_at) AS activity
                        UNION ALL
                        SELECT datetime(ep.completed_at)
                        FROM sessions s
                        JOIN execution_processes ep ON ep.session_id = s.id
                        WHERE s.workspace_id = w.id
                          AND ep.completed_at IS NOT NULL
                    )
                )
            ORDER BY w.updated_at ASC
            "#
        )
        .fetch_all(pool)
        .await
    }

    /// Workspaces eligible for automatic archiving: idle beyond `idle_days`
    /// with nothing running and no pin holding them open.
    ///
    /// Idleness is measured from the later of the workspace's own
    /// `updated_at` and its most recent completed execution. A workspace that
    /// never ran anything still has `updated_at`, so it cannot hide from the
    /// sweep behind an empty execution history.
    pub async fn find_idle_for_auto_archive(
        pool: &SqlitePool,
        idle_days: u32,
    ) -> Result<Vec<Uuid>, sqlx::Error> {
        let cutoff = format!("-{idle_days} days");
        sqlx::query_scalar!(
            r#"
            SELECT w.id AS "id!: Uuid"
            FROM workspaces w
            WHERE w.archived = FALSE
              AND w.pin_order IS NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM sessions s
                  JOIN execution_processes ep ON ep.session_id = s.id
                  WHERE s.workspace_id = w.id
                    AND ep.completed_at IS NULL
              )
              AND datetime('now', $1) > (
                  SELECT MAX(activity)
                  FROM (
                      SELECT datetime(w.updated_at) AS activity
                      UNION ALL
                      SELECT datetime(ep.completed_at)
                      FROM sessions s
                      JOIN execution_processes ep ON ep.session_id = s.id
                      WHERE s.workspace_id = w.id
                        AND ep.completed_at IS NOT NULL
                  )
              )
            ORDER BY w.updated_at ASC
            "#,
            cutoff
        )
        .fetch_all(pool)
        .await
    }

    pub async fn create(
        pool: &SqlitePool,
        data: &CreateWorkspace,
        id: Uuid,
    ) -> Result<Self, WorkspaceError> {
        Ok(sqlx::query_as!(
            Workspace,
            r#"INSERT INTO workspaces (id, task_id, container_ref, branch, setup_completed_at, name, use_worktree)
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               RETURNING id as "id!: Uuid", task_id as "task_id: Uuid", container_ref, branch, setup_completed_at as "setup_completed_at: DateTime<Utc>", created_at as "created_at!: DateTime<Utc>", updated_at as "updated_at!: DateTime<Utc>", archived as "archived!: bool", (pin_order IS NOT NULL) as "pinned!: bool", pin_order as "pin_order: i64", name, worktree_deleted as "worktree_deleted!: bool", use_worktree as "use_worktree!: bool", source as "source!: WorkspaceSource""#,
            id,
            Option::<Uuid>::None,
            Option::<String>::None,
            data.branch,
            Option::<DateTime<Utc>>::None,
            data.name,
            data.use_worktree
        )
        .fetch_one(pool)
        .await?)
    }

    pub async fn set_source(
        pool: &SqlitePool,
        workspace_id: Uuid,
        source: WorkspaceSource,
    ) -> Result<(), sqlx::Error> {
        let source_str = source.to_string();
        sqlx::query!(
            "UPDATE workspaces SET source = $1, updated_at = datetime('now','subsec') WHERE id = $2",
            source_str,
            workspace_id
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn update_branch_name(
        pool: &SqlitePool,
        workspace_id: Uuid,
        new_branch_name: &str,
    ) -> Result<(), WorkspaceError> {
        sqlx::query!(
            "UPDATE workspaces SET branch = $1, updated_at = datetime('now') WHERE id = $2",
            new_branch_name,
            workspace_id,
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Find workspace by path using container-ref path containment.
    /// Used by clients that may open a repo subfolder rather than the workspace root.
    pub async fn resolve_container_ref_by_prefix(
        pool: &SqlitePool,
        path: &str,
    ) -> Result<ContainerInfo, sqlx::Error> {
        let workspaces = sqlx::query_as!(
            WorkspaceContainerRefRow,
            r#"SELECT id as "id!: Uuid",
                      container_ref as "container_ref!"
               FROM workspaces
               WHERE container_ref IS NOT NULL"#,
        )
        .fetch_all(pool)
        .await?;

        Self::best_matching_container_ref(
            path,
            workspaces
                .iter()
                .map(|ws| (ws.id, ws.container_ref.as_str())),
        )
        .map(|workspace_id| ContainerInfo { workspace_id })
        .ok_or(sqlx::Error::RowNotFound)
    }

    fn best_matching_container_ref<'a>(
        path: &str,
        candidates: impl Iterator<Item = (Uuid, &'a str)>,
    ) -> Option<Uuid> {
        let path = std::path::Path::new(path);

        candidates
            .filter(|(_, container_ref)| {
                let container_ref = std::path::Path::new(container_ref);
                path.starts_with(container_ref) || container_ref.starts_with(path)
            })
            .max_by_key(|(_, container_ref)| {
                std::path::Path::new(container_ref).components().count()
            })
            .map(|(workspace_id, _)| workspace_id)
    }

    pub async fn set_archived(
        pool: &SqlitePool,
        workspace_id: Uuid,
        archived: bool,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "UPDATE workspaces SET archived = $1, updated_at = datetime('now', 'subsec') WHERE id = $2",
            archived,
            workspace_id
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Update workspace fields. Only non-None values will be updated.
    /// For `name`, pass `Some("")` to clear the name, `Some("foo")` to set it, or `None` to leave unchanged.
    /// For `pinned`: `Some(true)` appends to the end of the pinned list (pin_order = MAX+1) if
    /// not already pinned; `Some(false)` clears pin_order and renumbers remaining pinned rows.
    pub async fn update(
        pool: &SqlitePool,
        workspace_id: Uuid,
        archived: Option<bool>,
        pinned: Option<bool>,
        name: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        // Convert empty string to None for name field (to store as NULL)
        let name_value = name.filter(|s| !s.is_empty());
        let name_provided = name.is_some();

        let mut tx = pool.begin().await?;

        sqlx::query!(
            r#"UPDATE workspaces SET
                archived = COALESCE($1, archived),
                name = CASE WHEN $2 THEN $3 ELSE name END,
                updated_at = datetime('now', 'subsec')
            WHERE id = $4"#,
            archived,
            name_provided,
            name_value,
            workspace_id
        )
        .execute(&mut *tx)
        .await?;

        if let Some(should_pin) = pinned {
            if should_pin {
                // Pin: append to end if not already pinned.
                sqlx::query!(
                    r#"UPDATE workspaces
                       SET pin_order = COALESCE(
                           (SELECT MAX(pin_order) + 1 FROM workspaces WHERE pin_order IS NOT NULL),
                           0
                       )
                       WHERE id = $1 AND pin_order IS NULL"#,
                    workspace_id
                )
                .execute(&mut *tx)
                .await?;
            } else {
                // Unpin: clear pin_order, then close the gap by renumbering remaining pinned rows.
                sqlx::query!(
                    "UPDATE workspaces SET pin_order = NULL WHERE id = $1",
                    workspace_id
                )
                .execute(&mut *tx)
                .await?;
                Self::compact_pin_order(&mut tx).await?;
            }
        }

        tx.commit().await
    }

    /// Renumber pinned workspaces to close gaps (pin_order = 0..N-1 by current order).
    async fn compact_pin_order(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"UPDATE workspaces
               SET pin_order = sub.rn - 1
               FROM (
                   SELECT id, ROW_NUMBER() OVER (ORDER BY pin_order ASC) AS rn
                   FROM workspaces
                   WHERE pin_order IS NOT NULL
               ) sub
               WHERE workspaces.id = sub.id"#
        )
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    /// Atomically set the pinned set to exactly `ordered_ids` in the given order.
    /// Workspaces not in the list have their pin_order cleared. Workspaces in the
    /// list get pin_order = their position in the list. Unknown ids are skipped
    /// silently; pin_order is compacted afterwards to keep the sequence contiguous.
    pub async fn reorder_pins(pool: &SqlitePool, ordered_ids: &[Uuid]) -> Result<(), sqlx::Error> {
        let mut tx = pool.begin().await?;

        sqlx::query!("UPDATE workspaces SET pin_order = NULL WHERE pin_order IS NOT NULL")
            .execute(&mut *tx)
            .await?;

        for (idx, id) in ordered_ids.iter().enumerate() {
            let position = idx as i64;
            sqlx::query!(
                "UPDATE workspaces SET pin_order = $1 WHERE id = $2",
                position,
                id
            )
            .execute(&mut *tx)
            .await?;
        }

        // Re-compact: if any input ids didn't match a row, the sequence has gaps.
        Self::compact_pin_order(&mut tx).await?;

        tx.commit().await
    }

    pub async fn get_first_user_message(
        pool: &SqlitePool,
        workspace_id: Uuid,
    ) -> Result<Option<String>, sqlx::Error> {
        let actions = sqlx::query_scalar!(
            r#"SELECT ep.executor_action as "executor_action!: sqlx::types::Json<ExecutorActionField>"
               FROM sessions s
               JOIN execution_processes ep ON ep.session_id = s.id
               WHERE s.workspace_id = $1
               ORDER BY s.created_at ASC, ep.created_at ASC"#,
            workspace_id
        )
        .fetch_all(pool)
        .await?;

        for action in actions {
            if let ExecutorActionField::ExecutorAction(action) = action.0
                && let Some(prompt) = Self::extract_first_prompt_from_executor_action(&action)
            {
                return Ok(Some(prompt));
            }
        }

        Ok(None)
    }

    fn extract_first_prompt_from_executor_action(action: &ExecutorAction) -> Option<String> {
        let mut current = Some(action);
        while let Some(action) = current {
            match action.typ() {
                ExecutorActionType::CodingAgentInitialRequest(request) => {
                    return Some(request.prompt.clone());
                }
                ExecutorActionType::CodingAgentFollowUpRequest(request) => {
                    return Some(request.prompt.clone());
                }
                ExecutorActionType::ReviewRequest(request) => {
                    return Some(request.prompt.clone());
                }
                ExecutorActionType::ScriptRequest(_) => {
                    current = action.next_action();
                }
            }
        }
        None
    }

    pub fn truncate_to_name(prompt: &str, max_len: usize) -> String {
        let trimmed = prompt.trim();
        if trimmed.chars().count() <= max_len {
            trimmed.to_string()
        } else {
            let truncated: String = trimmed.chars().take(max_len).collect();
            if let Some(last_space) = truncated.rfind(' ') {
                format!("{}...", &truncated[..last_space])
            } else {
                format!("{}...", truncated)
            }
        }
    }

    pub async fn find_all_with_status(
        pool: &SqlitePool,
        archived: Option<bool>,
        limit: Option<i64>,
    ) -> Result<Vec<WorkspaceWithStatus>, sqlx::Error> {
        // Fetch all workspaces with status (uses cached SQLx query)
        let records = sqlx::query!(
            r#"SELECT
                w.id AS "id!: Uuid",
                w.task_id AS "task_id: Uuid",
                w.container_ref,
                w.branch,
                w.setup_completed_at AS "setup_completed_at: DateTime<Utc>",
                w.created_at AS "created_at!: DateTime<Utc>",
                w.updated_at AS "updated_at!: DateTime<Utc>",
                w.archived AS "archived!: bool",
                (w.pin_order IS NOT NULL) AS "pinned!: bool",
                w.pin_order AS "pin_order: i64",
                w.name,
                w.worktree_deleted AS "worktree_deleted!: bool",
                w.use_worktree AS "use_worktree!: bool",
                w.source AS "source!: WorkspaceSource",

                CASE WHEN EXISTS (
                    SELECT 1
                    FROM sessions s
                    JOIN execution_processes ep ON ep.session_id = s.id
                    WHERE s.workspace_id = w.id
                      AND ep.status = 'running'
                      AND ep.run_reason IN ('setupscript','cleanupscript','codingagent')
                    LIMIT 1
                ) THEN 1 ELSE 0 END AS "is_running!: i64",

                CASE WHEN (
                    SELECT ep.status
                    FROM sessions s
                    JOIN execution_processes ep ON ep.session_id = s.id
                    WHERE s.workspace_id = w.id
                      AND ep.run_reason IN ('setupscript','cleanupscript','codingagent')
                    ORDER BY ep.created_at DESC
                    LIMIT 1
                ) IN ('failed','killed') THEN 1 ELSE 0 END AS "is_errored!: i64"

            FROM workspaces w
            WHERE w.source = 'user'
            ORDER BY w.updated_at DESC"#
        )
        .fetch_all(pool)
        .await?;

        let mut workspaces: Vec<WorkspaceWithStatus> = records
            .into_iter()
            .map(|rec| WorkspaceWithStatus {
                workspace: Workspace {
                    id: rec.id,
                    task_id: rec.task_id,
                    container_ref: rec.container_ref,
                    branch: rec.branch,
                    setup_completed_at: rec.setup_completed_at,
                    created_at: rec.created_at,
                    updated_at: rec.updated_at,
                    archived: rec.archived,
                    pinned: rec.pinned,
                    pin_order: rec.pin_order,
                    name: rec.name,
                    worktree_deleted: rec.worktree_deleted,
                    use_worktree: rec.use_worktree,
                    source: rec.source,
                },
                is_running: rec.is_running != 0,
                is_errored: rec.is_errored != 0,
            })
            // Apply archived filter if provided
            .filter(|ws| archived.is_none_or(|a| ws.workspace.archived == a))
            .collect();

        // Apply limit if provided (already sorted by updated_at DESC from query)
        if let Some(lim) = limit {
            workspaces.truncate(lim as usize);
        }

        for ws in &mut workspaces {
            if ws.workspace.name.is_none()
                && let Some(prompt) = Self::get_first_user_message(pool, ws.workspace.id).await?
            {
                let name = Self::truncate_to_name(&prompt, WORKSPACE_NAME_MAX_LEN);
                Self::update(pool, ws.workspace.id, None, None, Some(&name)).await?;
                ws.workspace.name = Some(name);
            }
        }

        Ok(workspaces)
    }

    /// Delete a workspace by ID
    pub async fn delete(pool: &SqlitePool, id: Uuid) -> Result<u64, sqlx::Error> {
        let result = sqlx::query!("DELETE FROM workspaces WHERE id = $1", id)
            .execute(pool)
            .await?;
        Ok(result.rows_affected())
    }

    /// Count total workspaces across all projects
    pub async fn find_by_id_with_status(
        pool: &SqlitePool,
        id: Uuid,
    ) -> Result<Option<WorkspaceWithStatus>, sqlx::Error> {
        let rec = sqlx::query!(
            r#"SELECT
                w.id AS "id!: Uuid",
                w.task_id AS "task_id: Uuid",
                w.container_ref,
                w.branch,
                w.setup_completed_at AS "setup_completed_at: DateTime<Utc>",
                w.created_at AS "created_at!: DateTime<Utc>",
                w.updated_at AS "updated_at!: DateTime<Utc>",
                w.archived AS "archived!: bool",
                (w.pin_order IS NOT NULL) AS "pinned!: bool",
                w.pin_order AS "pin_order: i64",
                w.name,
                w.worktree_deleted AS "worktree_deleted!: bool",
                w.use_worktree AS "use_worktree!: bool",
                w.source AS "source!: WorkspaceSource",

                CASE WHEN EXISTS (
                    SELECT 1
                    FROM sessions s
                    JOIN execution_processes ep ON ep.session_id = s.id
                    WHERE s.workspace_id = w.id
                      AND ep.status = 'running'
                      AND ep.run_reason IN ('setupscript','cleanupscript','codingagent')
                    LIMIT 1
                ) THEN 1 ELSE 0 END AS "is_running!: i64",

                CASE WHEN (
                    SELECT ep.status
                    FROM sessions s
                    JOIN execution_processes ep ON ep.session_id = s.id
                    WHERE s.workspace_id = w.id
                      AND ep.run_reason IN ('setupscript','cleanupscript','codingagent')
                    ORDER BY ep.created_at DESC
                    LIMIT 1
                ) IN ('failed','killed') THEN 1 ELSE 0 END AS "is_errored!: i64"

            FROM workspaces w
            WHERE w.id = $1"#,
            id
        )
        .fetch_optional(pool)
        .await?;

        let Some(rec) = rec else {
            return Ok(None);
        };

        let mut ws = WorkspaceWithStatus {
            workspace: Workspace {
                id: rec.id,
                task_id: rec.task_id,
                container_ref: rec.container_ref,
                branch: rec.branch,
                setup_completed_at: rec.setup_completed_at,
                created_at: rec.created_at,
                updated_at: rec.updated_at,
                archived: rec.archived,
                pinned: rec.pinned,
                pin_order: rec.pin_order,
                name: rec.name,
                worktree_deleted: rec.worktree_deleted,
                use_worktree: rec.use_worktree,
                source: rec.source,
            },
            is_running: rec.is_running != 0,
            is_errored: rec.is_errored != 0,
        };

        if ws.workspace.name.is_none()
            && let Some(prompt) = Self::get_first_user_message(pool, ws.workspace.id).await?
        {
            let name = Self::truncate_to_name(&prompt, WORKSPACE_NAME_MAX_LEN);
            Self::update(pool, ws.workspace.id, None, None, Some(&name)).await?;
            ws.workspace.name = Some(name);
        }

        Ok(Some(ws))
    }
}

#[cfg(test)]
mod tests {
    use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};
    use uuid::Uuid;

    use super::Workspace;

    async fn migrated_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    /// Insert a workspace whose last own activity was `age_days` ago.
    async fn workspace_idle_for(pool: &SqlitePool, age_days: i64, pinned: bool) -> Uuid {
        let id = Uuid::new_v4();
        let pin_order = pinned.then_some(0i64);
        sqlx::query(
            "INSERT INTO workspaces (id, branch, name, updated_at, pin_order)
             VALUES (?, 'main', 'ws', datetime('now', ?), ?)",
        )
        .bind(id)
        .bind(format!("-{age_days} days"))
        .bind(pin_order)
        .execute(pool)
        .await
        .unwrap();
        id
    }

    /// Attach one execution process. `completed_days_ago` of `None` leaves it
    /// in flight, which is the state a workspace awaiting approval is in.
    async fn add_execution(pool: &SqlitePool, workspace_id: Uuid, completed_days_ago: Option<i64>) {
        let session_id = Uuid::new_v4();
        sqlx::query("INSERT INTO sessions (id, workspace_id) VALUES (?, ?)")
            .bind(session_id)
            .bind(workspace_id)
            .execute(pool)
            .await
            .unwrap();

        let (status, completed_offset) = match completed_days_ago {
            Some(days) => ("completed", Some(format!("-{days} days"))),
            None => ("running", None),
        };
        // Number every placeholder: mixing `?` with `?3` renumbers the
        // anonymous ones and silently binds the wrong column.
        sqlx::query(
            "INSERT INTO execution_processes (id, session_id, status, completed_at)
             VALUES (?1, ?2, ?3, CASE WHEN ?4 IS NULL THEN NULL ELSE datetime('now', ?4) END)",
        )
        .bind(Uuid::new_v4())
        .bind(session_id)
        .bind(status)
        .bind(completed_offset)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn auto_archive_retires_only_idle_unpinned_finished_workspaces() {
        let pool = migrated_pool().await;

        // Idle for 30 days with no execution history at all. 400+ workspaces
        // accumulated this way over a week of fleet use.
        let never_ran = workspace_idle_for(&pool, 30, false).await;

        // Idle for 30 days, last execution finished 30 days ago.
        let long_finished = workspace_idle_for(&pool, 30, false).await;
        add_execution(&pool, long_finished, Some(30)).await;

        // Pinned workspaces are held open deliberately.
        let _pinned = workspace_idle_for(&pool, 30, true).await;

        // Still running. A workspace blocked on an approval is in this state:
        // the process that raised the approval has not completed.
        let running = workspace_idle_for(&pool, 30, false).await;
        add_execution(&pool, running, None).await;

        // Touched recently.
        let _fresh = workspace_idle_for(&pool, 1, false).await;

        // Old workspace record, but its last run finished an hour ago.
        let recently_ran = workspace_idle_for(&pool, 30, false).await;
        add_execution(&pool, recently_ran, Some(0)).await;

        // Already archived.
        let archived = workspace_idle_for(&pool, 30, false).await;
        sqlx::query("UPDATE workspaces SET archived = TRUE WHERE id = ?")
            .bind(archived)
            .execute(&pool)
            .await
            .unwrap();

        let mut idle = Workspace::find_idle_for_auto_archive(&pool, 7)
            .await
            .unwrap();
        idle.sort();

        let mut expected = vec![never_ran, long_finished];
        expected.sort();
        assert_eq!(idle, expected);
    }

    #[tokio::test]
    async fn auto_archive_threshold_bounds_the_sweep() {
        let pool = migrated_pool().await;
        let five_days = workspace_idle_for(&pool, 5, false).await;

        assert!(
            Workspace::find_idle_for_auto_archive(&pool, 7)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            Workspace::find_idle_for_auto_archive(&pool, 3)
                .await
                .unwrap(),
            vec![five_days]
        );
    }

    /// Insert a workspace whose own last activity was `age_hours` ago.
    async fn workspace_aged_hours(pool: &SqlitePool, age_hours: i64, archived: bool) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO workspaces (id, branch, name, container_ref, updated_at, archived)
             VALUES (?1, 'main', 'ws', '/tmp/ws', datetime('now', ?2), ?3)",
        )
        .bind(id)
        .bind(format!("-{age_hours} hours"))
        .bind(archived)
        .execute(pool)
        .await
        .unwrap();
        id
    }

    async fn expired_ids(pool: &SqlitePool) -> Vec<Uuid> {
        let mut ids: Vec<Uuid> = Workspace::find_expired_for_cleanup(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|w| w.id)
            .collect();
        ids.sort();
        ids
    }

    /// The regression that let 82GB accumulate: a workspace that never
    /// completed an execution was compared against a NULL activity timestamp,
    /// so it never expired and kept its worktree forever.
    #[tokio::test]
    async fn cleanup_reclaims_archived_workspaces_that_never_ran_anything() {
        let pool = migrated_pool().await;
        let never_ran = workspace_aged_hours(&pool, 24, true).await;

        assert_eq!(expired_ids(&pool).await, vec![never_ran]);
    }

    #[tokio::test]
    async fn cleanup_respects_the_archived_and_live_retention_windows() {
        let pool = migrated_pool().await;

        // Archived: eligible after 1 hour.
        let archived_expired = workspace_aged_hours(&pool, 2, true).await;
        let _archived_fresh = workspace_aged_hours(&pool, 0, true).await;

        // Live: eligible after 72 hours.
        let live_expired = workspace_aged_hours(&pool, 80, false).await;
        let _live_fresh = workspace_aged_hours(&pool, 70, false).await;

        let mut expected = vec![archived_expired, live_expired];
        expected.sort();
        assert_eq!(expired_ids(&pool).await, expected);
    }

    /// Expiry is measured in UTC on both sides. Comparing a UTC column against
    /// `datetime('now','localtime')` shifted every window by the machine's UTC
    /// offset, which delayed cleanup west of UTC and, worse, expired live
    /// worktrees hours early east of it.
    #[tokio::test]
    async fn cleanup_windows_do_not_shift_with_the_local_timezone() {
        let pool = migrated_pool().await;

        // Just inside the live window. A positive UTC offset would drag this
        // across the threshold and delete a worktree that is still in use.
        let _almost_expired = workspace_aged_hours(&pool, 71, false).await;
        // Just inside the archived window.
        let _almost_expired_archived = workspace_aged_hours(&pool, 0, true).await;

        assert!(expired_ids(&pool).await.is_empty());
    }

    #[tokio::test]
    async fn cleanup_never_selects_a_workspace_with_an_open_execution() {
        let pool = migrated_pool().await;
        let running = workspace_aged_hours(&pool, 500, true).await;
        add_execution(&pool, running, None).await;

        assert!(expired_ids(&pool).await.is_empty());
    }

    #[tokio::test]
    async fn cleanup_measures_expiry_from_the_last_completed_execution() {
        let pool = migrated_pool().await;

        // Old record, but it finished running moments ago.
        let recently_ran = workspace_aged_hours(&pool, 500, true).await;
        add_execution(&pool, recently_ran, Some(0)).await;

        assert!(expired_ids(&pool).await.is_empty());
    }

    /// Sweeping twice back to back is a no-op the second time: the flag set by
    /// the first pass removes the workspace from the selection.
    #[tokio::test]
    async fn cleanup_selection_is_idempotent_once_the_worktree_is_marked() {
        let pool = migrated_pool().await;
        let expired = workspace_aged_hours(&pool, 24, true).await;
        assert_eq!(expired_ids(&pool).await, vec![expired]);

        Workspace::mark_worktree_deleted(&pool, expired)
            .await
            .unwrap();

        assert!(expired_ids(&pool).await.is_empty());
    }

    /// A worktree-disabled workspace runs in the user's real repository, so it
    /// owns nothing to reclaim and must never be offered to the sweep.
    #[tokio::test]
    async fn cleanup_never_selects_a_worktree_disabled_workspace() {
        let pool = migrated_pool().await;
        let direct = workspace_aged_hours(&pool, 500, true).await;
        sqlx::query("UPDATE workspaces SET use_worktree = FALSE WHERE id = ?")
            .bind(direct)
            .execute(&pool)
            .await
            .unwrap();

        assert!(expired_ids(&pool).await.is_empty());
    }

    /// `workspace_dir_for` derives a directory from the workspace id and name
    /// when `container_ref` is unset, so requiring the column would strand
    /// those directories permanently.
    #[tokio::test]
    async fn cleanup_selects_workspaces_with_no_container_ref() {
        let pool = migrated_pool().await;
        let no_ref = workspace_aged_hours(&pool, 24, true).await;
        sqlx::query("UPDATE workspaces SET container_ref = NULL WHERE id = ?")
            .bind(no_ref)
            .execute(&pool)
            .await
            .unwrap();

        assert_eq!(expired_ids(&pool).await, vec![no_ref]);
    }

    #[test]
    fn best_matching_container_ref_prefers_deepest_match() {
        let broad_id = Uuid::new_v4();
        let exact_id = Uuid::new_v4();
        let selected = Workspace::best_matching_container_ref(
            "/tmp/ws/repo/packages/app",
            [(broad_id, "/tmp"), (exact_id, "/tmp/ws")].into_iter(),
        );

        assert_eq!(selected, Some(exact_id));
    }

    #[test]
    fn best_matching_container_ref_supports_parent_request_path() {
        let workspace_id = Uuid::new_v4();
        let selected = Workspace::best_matching_container_ref(
            "/tmp/ws/repo",
            [(workspace_id, "/tmp/ws/repo/packages/app")].into_iter(),
        );

        assert_eq!(selected, Some(workspace_id));
    }

    #[test]
    fn best_matching_container_ref_ignores_unrelated_paths() {
        let workspace_id = Uuid::new_v4();
        let selected = Workspace::best_matching_container_ref(
            "/tmp/other/path",
            [(workspace_id, "/tmp/ws")].into_iter(),
        );

        assert_eq!(selected, None);
    }
}
