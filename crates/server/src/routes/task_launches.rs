use std::sync::LazyLock;

use axum::{
    Json, Router,
    extract::{Query, State},
    response::Json as ResponseJson,
    routing::{get, post},
};
use chrono::Utc;
use db::models::{
    execution_process::{ExecutionProcess, ExecutionProcessRunReason, ExecutionProcessStatus},
    execution_process_outcome::ExecutionProcessOutcome,
    requests::CreateAndStartWorkspaceRequest,
    session::Session,
    task_launch::{NewTaskLaunch, TaskLaunch, TaskLaunchError},
    workspace::Workspace,
};
use deployment::Deployment;
use executors::outcome::ExecutionOutcomeClass;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utils::{assets::asset_dir, response::ApiResponse, storage_limits::ensure_launch_allowed};
use uuid::Uuid;

use crate::{
    DeploymentImpl, error::ApiError,
    routes::workspaces::create::create_and_start_workspace_with_ids,
};

const CONTRACT_VERSION: i64 = 1;
static TASK_LAUNCH_INSTANCE_ID: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);

pub fn router() -> Router<DeploymentImpl> {
    Router::new()
        .route("/task-launches", post(create_or_return))
        .route("/task-launches/by-key", get(get_by_key))
}

#[derive(Debug, Deserialize)]
struct TaskLaunchRequest {
    contract_version: i64,
    task_id: String,
    incarnation_generation: i64,
    attempt_id: String,
    idempotency_key: String,
    launch: CreateAndStartWorkspaceRequest,
}

#[derive(Debug, Deserialize)]
struct TaskLaunchQuery {
    idempotency_key: String,
}

#[derive(Debug, Serialize)]
struct TaskLaunchResponse {
    contract_version: i64,
    task_id: String,
    incarnation_generation: i64,
    attempt_id: String,
    idempotency_key: String,
    phase: String,
    effect: &'static str,
    workspace_id: Option<Uuid>,
    session_id: Option<Uuid>,
    outcome: Option<Value>,
    history_ref: Option<String>,
}

async fn create_or_return(
    State(deployment): State<DeploymentImpl>,
    Json(request): Json<TaskLaunchRequest>,
) -> Result<ResponseJson<ApiResponse<TaskLaunchResponse>>, ApiError> {
    validate_request(&request)?;
    let launch_value = serde_json::to_value(&request.launch)
        .map_err(|error| ApiError::BadRequest(error.to_string()))?;
    let owner_instance_id = *TASK_LAUNCH_INSTANCE_ID;
    let (record, inserted) = TaskLaunch::begin(
        &deployment.db().pool,
        NewTaskLaunch {
            task_id: &request.task_id,
            incarnation_generation: request.incarnation_generation,
            attempt_id: &request.attempt_id,
            idempotency_key: &request.idempotency_key,
            launch: &launch_value,
            workspace_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            owner_instance_id,
        },
    )
    .await
    .map_err(map_task_launch_error)?;

    if !inserted {
        let record = reconcile_record(&deployment, record).await?;
        if record.phase == "pending" && record.owner_instance_id == owner_instance_id {
            return Err(ApiError::TooEarly(
                "Task launch is still being created by this cdesktop instance".to_string(),
            ));
        }
        return Ok(ResponseJson(ApiResponse::success(to_response(
            record, false,
        ))));
    }

    if let Err(error) = ensure_launch_allowed(&asset_dir()) {
        tracing::warn!(
            task_id = %record.task_id,
            idempotency_key = %record.idempotency_key,
            "Task launch refused before write: {error}"
        );
        let record = TaskLaunch::mark_outcome(
            &deployment.db().pool,
            &record.idempotency_key,
            "refused",
            &json!({"kind": "storage_refused", "refused_before_write": true}),
            false,
        )
        .await?;
        return Ok(ResponseJson(ApiResponse::success(to_response(
            record, true,
        ))));
    }

    match create_and_start_workspace_with_ids(
        &deployment,
        request.launch,
        record.workspace_id,
        record.session_id,
    )
    .await
    {
        Ok(_) => {
            let record = TaskLaunch::mark_active(
                &deployment.db().pool,
                &record.idempotency_key,
                owner_instance_id,
            )
            .await?;
            Ok(ResponseJson(ApiResponse::success(to_response(
                record, true,
            ))))
        }
        Err(error) => {
            tracing::error!(
                task_id = %record.task_id,
                idempotency_key = %record.idempotency_key,
                "Task launch failed after durable reservation: {error}"
            );
            let effect_created = native_effect_exists(&deployment, &record).await?;
            let record = TaskLaunch::mark_outcome(
                &deployment.db().pool,
                &record.idempotency_key,
                "terminal",
                &json!({"kind": "failed"}),
                effect_created,
            )
            .await?;
            Ok(ResponseJson(ApiResponse::success(to_response(
                record, true,
            ))))
        }
    }
}

async fn get_by_key(
    State(deployment): State<DeploymentImpl>,
    Query(query): Query<TaskLaunchQuery>,
) -> Result<ResponseJson<ApiResponse<TaskLaunchResponse>>, ApiError> {
    if query.idempotency_key.trim().is_empty() {
        return Err(ApiError::BadRequest(
            "idempotency_key must not be empty".to_string(),
        ));
    }
    let record = TaskLaunch::find_by_key(&deployment.db().pool, &query.idempotency_key)
        .await?
        .ok_or_else(|| ApiError::NotFound("Task launch not found".to_string()))?;
    let record = reconcile_record(&deployment, record).await?;
    Ok(ResponseJson(ApiResponse::success(to_response(
        record, false,
    ))))
}

fn validate_request(request: &TaskLaunchRequest) -> Result<(), ApiError> {
    if request.contract_version != CONTRACT_VERSION {
        return Err(ApiError::BadRequest(
            "Unsupported task launch contract version".to_string(),
        ));
    }
    if request.incarnation_generation < 0 {
        return Err(ApiError::BadRequest(
            "incarnation_generation must be non-negative".to_string(),
        ));
    }
    if [
        request.task_id.as_str(),
        request.attempt_id.as_str(),
        request.idempotency_key.as_str(),
    ]
    .iter()
    .any(|value| value.trim().is_empty())
    {
        return Err(ApiError::BadRequest(
            "Task launch identity fields must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn map_task_launch_error(error: TaskLaunchError) -> ApiError {
    match error {
        TaskLaunchError::Database(error) => ApiError::Database(error),
        TaskLaunchError::Conflict => ApiError::Conflict(
            "Task launch key or attempt belongs to different parameters".to_string(),
        ),
        TaskLaunchError::StaleGeneration => {
            ApiError::Conflict("Task launch generation is stale".to_string())
        }
    }
}

async fn reconcile_record(
    deployment: &DeploymentImpl,
    record: TaskLaunch,
) -> Result<TaskLaunch, ApiError> {
    if !matches!(record.phase.as_str(), "pending" | "active") {
        return Ok(record);
    }

    let pool = &deployment.db().pool;
    let workspace = Workspace::find_by_id(pool, record.workspace_id).await?;
    let session = Session::find_by_id(pool, record.session_id).await?;
    if let (Some(workspace), Some(session)) = (&workspace, &session)
        && session.workspace_id != workspace.id
    {
        return Ok(TaskLaunch::mark_outcome(
            pool,
            &record.idempotency_key,
            "terminal",
            &json!({"kind": "lost"}),
            true,
        )
        .await?);
    }
    let processes = ExecutionProcess::find_by_session_id(pool, record.session_id, true).await?;

    if let Some(process) = processes
        .iter()
        .rev()
        .find(|process| matches!(process.run_reason, ExecutionProcessRunReason::CodingAgent))
    {
        if matches!(process.status, ExecutionProcessStatus::Running) {
            if record.phase == "pending" {
                return Ok(TaskLaunch::reconcile_active(pool, &record.idempotency_key).await?);
            }
            return Ok(record);
        }

        let outcome = task_outcome(pool, process).await?;
        return Ok(TaskLaunch::mark_outcome(
            pool,
            &record.idempotency_key,
            "terminal",
            &outcome,
            true,
        )
        .await?);
    }

    if processes
        .iter()
        .any(|process| matches!(process.status, ExecutionProcessStatus::Running))
    {
        if record.phase == "pending" {
            return Ok(TaskLaunch::reconcile_active(pool, &record.idempotency_key).await?);
        }
        return Ok(record);
    }

    if !processes.is_empty() {
        return Ok(TaskLaunch::reconcile_active(pool, &record.idempotency_key).await?);
    }

    if record.owner_instance_id == *TASK_LAUNCH_INSTANCE_ID && record.phase == "pending" {
        return Ok(record);
    }

    TaskLaunch::mark_outcome(
        pool,
        &record.idempotency_key,
        "terminal",
        &json!({"kind": "lost"}),
        workspace.is_some() || session.is_some(),
    )
    .await
    .map_err(ApiError::from)
}

async fn native_effect_exists(
    deployment: &DeploymentImpl,
    record: &TaskLaunch,
) -> Result<bool, ApiError> {
    let pool = &deployment.db().pool;
    Ok(Workspace::find_by_id(pool, record.workspace_id)
        .await?
        .is_some()
        || Session::find_by_id(pool, record.session_id)
            .await?
            .is_some())
}

async fn task_outcome(
    pool: &sqlx::SqlitePool,
    process: &ExecutionProcess,
) -> Result<Value, ApiError> {
    if matches!(process.status, ExecutionProcessStatus::Completed) {
        return Ok(json!({"kind": "completed"}));
    }
    let normalized = ExecutionProcessOutcome::find_by_execution_process_id(pool, process.id)
        .await?
        .map(|record| record.outcome.0);
    if matches!(
        normalized.as_ref().map(|outcome| outcome.class),
        Some(ExecutionOutcomeClass::QuotaExhausted)
    ) {
        let retry_at = normalized.as_ref().and_then(|outcome| {
            outcome
                .resets_at
                .map(|value| value.timestamp() as f64)
                .or_else(|| {
                    outcome.retry_after_seconds.map(|seconds| {
                        (Utc::now() + chrono::Duration::seconds(seconds)).timestamp() as f64
                    })
                })
        });
        return Ok(json!({"kind": "quota_exhausted", "retry_at": retry_at}));
    }
    Ok(json!({"kind": "failed"}))
}

fn to_response(record: TaskLaunch, created_by_request: bool) -> TaskLaunchResponse {
    let effect = if created_by_request && record.effect_created {
        "created"
    } else if record.effect_created {
        "existing"
    } else {
        "none"
    };
    let expose_ids = record.effect_created;
    TaskLaunchResponse {
        contract_version: record.contract_version,
        task_id: record.task_id,
        incarnation_generation: record.incarnation_generation,
        attempt_id: record.attempt_id,
        idempotency_key: record.idempotency_key,
        phase: record.phase,
        effect,
        workspace_id: expose_ids.then_some(record.workspace_id),
        session_id: expose_ids.then_some(record.session_id),
        outcome: record.outcome.map(|value| value.0),
        history_ref: record.history_ref,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use sqlx::types::Json;

    use super::*;

    fn record(phase: &str, effect_created: bool) -> TaskLaunch {
        TaskLaunch {
            id: Uuid::new_v4(),
            contract_version: 1,
            task_id: "task-a".to_string(),
            incarnation_generation: 1,
            attempt_id: "attempt-a".to_string(),
            idempotency_key: "task-launch:task-a:1:attempt-a".to_string(),
            launch: Json(json!({"prompt": "work"})),
            phase: phase.to_string(),
            workspace_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            owner_instance_id: Uuid::new_v4(),
            effect_created,
            history_ref: None,
            outcome: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn replay_reports_existing_native_identity() {
        let record = record("active", true);
        let workspace_id = record.workspace_id;
        let session_id = record.session_id;

        let response = to_response(record, false);

        assert_eq!(response.effect, "existing");
        assert_eq!(response.workspace_id, Some(workspace_id));
        assert_eq!(response.session_id, Some(session_id));
    }

    #[test]
    fn refusal_does_not_expose_an_uncreated_identity() {
        let response = to_response(record("refused", false), true);

        assert_eq!(response.effect, "none");
        assert_eq!(response.workspace_id, None);
        assert_eq!(response.session_id, None);
    }
}
