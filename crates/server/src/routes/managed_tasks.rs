use std::{future::Future, sync::LazyLock};

use axum::{
    Json, Router,
    extract::{Path, State},
    response::Json as ResponseJson,
    routing::put,
};
use db::models::{
    managed_task_effect::{
        FinishManagedTaskEffect, ManagedTaskEffect, ManagedTaskEffectError, NewManagedTaskEffect,
    },
    requests::CreateAndStartWorkspaceRequest,
    session::{Session, SessionError},
    workspace::{Workspace, WorkspaceError},
};
use deployment::Deployment;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ts_rs::TS;
use utils::response::ApiResponse;
use uuid::Uuid;

use crate::{
    DeploymentImpl,
    error::ApiError,
    routes::{
        teammates::{SpawnSource, SpawnTeammateRequest, spawn_teammate_with_id},
        workspaces::create::create_and_start_workspace_with_ids,
    },
};

static INSTANCE_ID: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);

pub fn router() -> Router<DeploymentImpl> {
    Router::new().route(
        "/managed-tasks/{task_id}/epochs/{epoch}",
        put(create_or_return).get(get_effect),
    )
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ManagedLaunch {
    Workspace {
        request: CreateAndStartWorkspaceRequest,
    },
    Session {
        workspace_id: Uuid,
        caller_session_id: Uuid,
        request: SpawnTeammateRequest,
    },
}

impl ManagedLaunch {
    fn kind(&self) -> &'static str {
        match self {
            Self::Workspace { .. } => "workspace",
            Self::Session { .. } => "session",
        }
    }

    fn workspace_id(&self) -> Uuid {
        match self {
            Self::Workspace { .. } => Uuid::new_v4(),
            Self::Session { workspace_id, .. } => *workspace_id,
        }
    }
}

#[derive(Debug, Serialize, TS)]
pub struct ManagedTaskEffectResponse {
    state: String,
    workspace_id: Option<Uuid>,
    session_id: Option<Uuid>,
    reason: Option<String>,
    retryable: bool,
    #[ts(optional)]
    retry_after_seconds: Option<u64>,
    created: bool,
}

struct ReservedManagedLaunch {
    record: ManagedTaskEffect,
    inserted: bool,
    launch_result: Option<Result<(), ApiError>>,
}

/// The reservation is committed before the native launch closure runs. A
/// duplicate `(task_id, epoch)` therefore returns the IDs of the first native
/// effect without invoking the launcher again.
async fn reserve_and_launch<F, Fut>(
    pool: &sqlx::SqlitePool,
    effect: NewManagedTaskEffect<'_>,
    launch: F,
) -> Result<ReservedManagedLaunch, ApiError>
where
    F: FnOnce(Uuid, Uuid) -> Fut,
    Fut: Future<Output = Result<(), ApiError>>,
{
    let (record, inserted) = ManagedTaskEffect::begin(pool, effect)
        .await
        .map_err(map_effect_error)?;
    let launch_result = inserted.then(|| launch(record.workspace_id, record.session_id));
    let launch_result = match launch_result {
        Some(launch) => Some(launch.await),
        None => None,
    };
    Ok(ReservedManagedLaunch {
        record,
        inserted,
        launch_result,
    })
}

async fn create_or_return(
    State(deployment): State<DeploymentImpl>,
    Path((task_id, epoch)): Path<(Uuid, i64)>,
    Json(launch): Json<ManagedLaunch>,
) -> Result<ResponseJson<ApiResponse<ManagedTaskEffectResponse>>, ApiError> {
    if epoch < 1 {
        return Err(ApiError::BadRequest("Task epoch must be positive".into()));
    }
    let request_hash = request_hash(&launch)?;
    let kind = launch.kind();
    let workspace_id = launch.workspace_id();
    let session_id = Uuid::new_v4();
    let deployment_for_launch = deployment.clone();
    let reserved = reserve_and_launch(
        &deployment.db().pool,
        NewManagedTaskEffect {
            task_id,
            epoch,
            request_hash: &request_hash,
            kind,
            workspace_id,
            session_id,
            owner_instance_id: *INSTANCE_ID,
            lease_id: Uuid::new_v4(),
        },
        |workspace_id, session_id| async move {
            match launch {
                ManagedLaunch::Workspace { request } => {
                    create_and_start_workspace_with_ids(
                        &deployment_for_launch,
                        request,
                        workspace_id,
                        session_id,
                    )
                    .await?;
                }
                ManagedLaunch::Session {
                    workspace_id: requested_workspace_id,
                    caller_session_id,
                    request,
                } => {
                    let workspace = Workspace::find_by_id(
                        &deployment_for_launch.db().pool,
                        requested_workspace_id,
                    )
                    .await?
                    .ok_or(WorkspaceError::WorkspaceNotFound)?;
                    let caller =
                        Session::find_by_id(&deployment_for_launch.db().pool, caller_session_id)
                            .await?
                            .ok_or(SessionError::NotFound)?;
                    if caller.workspace_id != workspace.id || workspace.id != workspace_id {
                        return Err(ApiError::Conflict(
                            "Caller session belongs to another workspace".into(),
                        ));
                    }
                    spawn_teammate_with_id(
                        &deployment_for_launch,
                        &workspace,
                        Some(&caller),
                        request,
                        SpawnSource::SessionCli,
                        session_id,
                    )
                    .await?;
                }
            }
            Ok(())
        },
    )
    .await?;
    let record = reserved.record;
    let inserted = reserved.inserted;

    if !inserted {
        let record = reconcile(&deployment, record).await?;
        if record.state == "pending" && record.owner_instance_id == *INSTANCE_ID {
            return Err(ApiError::TooEarly(
                "Managed task launch is still running".into(),
            ));
        }
        return Ok(ResponseJson(ApiResponse::success(to_response(
            record, false,
        ))));
    }

    let launch_result = reserved
        .launch_result
        .expect("the inserting request always invokes the native launcher");

    let record = match launch_result {
        Ok(()) => {
            ManagedTaskEffect::finish(
                &deployment.db().pool,
                FinishManagedTaskEffect {
                    task_id,
                    epoch,
                    owner_instance_id: *INSTANCE_ID,
                    lease_id: record.lease_id,
                    state: "active",
                    effect_created: true,
                    reason: None,
                    retryable: false,
                    retry_after_seconds: None,
                },
            )
            .await?
        }
        Err(error) => {
            tracing::error!(%task_id, epoch, "managed task launch failed: {error}");
            let current = ManagedTaskEffect::find(&deployment.db().pool, task_id, epoch)
                .await?
                .ok_or_else(|| ApiError::BadRequest("Managed task effect not found".into()))?;
            let effect_created = native_effect_exists(&deployment, &current).await?;
            finish_launch_failure(&deployment.db().pool, record, &error, effect_created).await?
        }
    };

    Ok(ResponseJson(ApiResponse::success(to_response(
        record, true,
    ))))
}

async fn get_effect(
    State(deployment): State<DeploymentImpl>,
    Path((task_id, epoch)): Path<(Uuid, i64)>,
) -> Result<ResponseJson<ApiResponse<ManagedTaskEffectResponse>>, ApiError> {
    let record = ManagedTaskEffect::find(&deployment.db().pool, task_id, epoch)
        .await?
        .ok_or_else(|| ApiError::BadRequest("Managed task effect not found".into()))?;
    let record = reconcile(&deployment, record).await?;
    Ok(ResponseJson(ApiResponse::success(to_response(
        record, false,
    ))))
}

async fn reconcile(
    deployment: &DeploymentImpl,
    record: ManagedTaskEffect,
) -> Result<ManagedTaskEffect, ApiError> {
    // A pending effect belongs exclusively to its owner and lease. Another
    // live server may only report that retryable pending state; it must never
    // infer a restart from a concurrent request and publish a false terminal.
    let _ = deployment;
    Ok(record)
}

async fn native_effect_exists(
    deployment: &DeploymentImpl,
    record: &ManagedTaskEffect,
) -> Result<bool, ApiError> {
    let session = Session::find_by_id(&deployment.db().pool, record.session_id).await?;
    if session.is_none_or(|session| session.workspace_id != record.workspace_id) {
        return Ok(false);
    }
    Ok(
        !db::models::execution_process::ExecutionProcess::find_by_session_id(
            &deployment.db().pool,
            record.session_id,
            true,
        )
        .await?
        .is_empty(),
    )
}

fn request_hash(launch: &ManagedLaunch) -> Result<String, ApiError> {
    let bytes =
        serde_json::to_vec(launch).map_err(|error| ApiError::BadRequest(error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn map_effect_error(error: ManagedTaskEffectError) -> ApiError {
    match error {
        ManagedTaskEffectError::Database(error) => ApiError::Database(error),
        ManagedTaskEffectError::Conflict => ApiError::Conflict(
            "The task epoch already belongs to different launch parameters".into(),
        ),
        ManagedTaskEffectError::StaleEpoch => ApiError::Conflict("The task epoch is stale".into()),
    }
}

fn to_response(record: ManagedTaskEffect, created_by_request: bool) -> ManagedTaskEffectResponse {
    let visible = record.effect_created;
    ManagedTaskEffectResponse {
        state: record.state,
        workspace_id: visible.then_some(record.workspace_id),
        session_id: visible.then_some(record.session_id),
        reason: record.reason,
        retryable: record.retryable,
        retry_after_seconds: record
            .retry_after_seconds
            .and_then(|seconds| u64::try_from(seconds).ok()),
        created: created_by_request && visible,
    }
}

struct ManagedLaunchFailure {
    reason: String,
    retryable: bool,
    retry_after_seconds: Option<u64>,
}

fn managed_launch_failure(error: &ApiError) -> ManagedLaunchFailure {
    match error {
        ApiError::HostAdmission(refusal) => {
            let refusal = refusal.refusal();
            ManagedLaunchFailure {
                reason: refusal.safe_message,
                retryable: true,
                retry_after_seconds: u64::try_from(refusal.retry_after_seconds).ok(),
            }
        }
        ApiError::MaintenanceDrain(drain) => ManagedLaunchFailure {
            reason: error.to_string(),
            retryable: true,
            retry_after_seconds: Some(drain.retry_after_seconds()),
        },
        _ => ManagedLaunchFailure {
            reason: error.to_string(),
            retryable: false,
            retry_after_seconds: None,
        },
    }
}

async fn finish_launch_failure(
    pool: &sqlx::SqlitePool,
    record: ManagedTaskEffect,
    error: &ApiError,
    effect_created: bool,
) -> Result<ManagedTaskEffect, sqlx::Error> {
    let outcome = managed_launch_failure(error);
    ManagedTaskEffect::finish(
        pool,
        FinishManagedTaskEffect {
            task_id: record.task_id,
            epoch: record.epoch,
            owner_instance_id: record.owner_instance_id,
            lease_id: record.lease_id,
            state: if effect_created {
                "active"
            } else if outcome.retryable {
                "pending"
            } else {
                "lost"
            },
            effect_created,
            reason: (!effect_created).then_some(outcome.reason.as_str()),
            retryable: !effect_created && outcome.retryable,
            retry_after_seconds: (!effect_created)
                .then_some(outcome.retry_after_seconds)
                .flatten()
                .and_then(|seconds| i64::try_from(seconds).ok()),
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;

    async fn pool() -> sqlx::SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::migrate!("../db/migrations").run(&pool).await.unwrap();
        pool
    }

    fn effect(task_id: Uuid, session_id: Uuid) -> NewManagedTaskEffect<'static> {
        NewManagedTaskEffect {
            task_id,
            epoch: 1,
            request_hash: "same-request",
            kind: "session",
            workspace_id: Uuid::new_v4(),
            session_id,
            owner_instance_id: Uuid::new_v4(),
            lease_id: Uuid::new_v4(),
        }
    }

    #[test]
    fn request_hash_is_stable_and_parameter_sensitive() {
        let request = SpawnTeammateRequest {
            name: "reviewer".into(),
            prompt: Some("audit".into()),
            executor_config: None,
            selected_provider_id: None,
        };
        let first = ManagedLaunch::Session {
            workspace_id: Uuid::nil(),
            caller_session_id: Uuid::nil(),
            request,
        };
        assert_eq!(request_hash(&first).unwrap(), request_hash(&first).unwrap());

        let second = ManagedLaunch::Session {
            workspace_id: Uuid::nil(),
            caller_session_id: Uuid::nil(),
            request: SpawnTeammateRequest {
                name: "reviewer".into(),
                prompt: Some("different".into()),
                executor_config: None,
                selected_provider_id: None,
            },
        };
        assert_ne!(
            request_hash(&first).unwrap(),
            request_hash(&second).unwrap()
        );
    }

    #[tokio::test]
    async fn route_launch_seam_retries_without_creating_a_second_native_session() {
        let pool = pool().await;
        let task_id = Uuid::new_v4();
        let launches = Arc::new(AtomicUsize::new(0));

        let first = reserve_and_launch(&pool, effect(task_id, Uuid::new_v4()), {
            let launches = launches.clone();
            move |_, _| async move {
                launches.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();
        assert!(first.inserted);
        let first_session_id = first.record.session_id;
        ManagedTaskEffect::finish(
            &pool,
            FinishManagedTaskEffect {
                task_id,
                epoch: 1,
                owner_instance_id: first.record.owner_instance_id,
                lease_id: first.record.lease_id,
                state: "active",
                effect_created: true,
                reason: None,
                retryable: false,
                retry_after_seconds: None,
            },
        )
        .await
        .unwrap();

        let retry = reserve_and_launch(&pool, effect(task_id, Uuid::new_v4()), {
            let launches = launches.clone();
            move |_, _| async move {
                launches.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();

        assert!(!retry.inserted);
        assert!(retry.launch_result.is_none());
        assert_eq!(retry.record.session_id, first_session_id);
        assert_eq!(launches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn admission_refusal_leaves_the_effect_pending_with_the_retry_contract() {
        // A host-full refusal created no native session, but the reservation is
        // still the authoritative effect for this task epoch and can be retried.
        let pool = pool().await;
        let task_id = Uuid::new_v4();
        let (record, inserted) = ManagedTaskEffect::begin(&pool, effect(task_id, Uuid::new_v4()))
            .await
            .unwrap();
        assert!(inserted);
        let refusal = services::services::host_admission::AdmissionError::ProcessExhausted {
            available: 2_810,
            reserve: 3_002,
            culprit: Some("2,212 dead children of FwUpdateManagerd (pid 693)".into()),
        };
        let expected_reason = refusal.refusal().safe_message;

        let finished =
            finish_launch_failure(&pool, record, &ApiError::HostAdmission(refusal), false)
                .await
                .unwrap();

        assert_eq!(finished.state, "pending");
        assert_eq!(finished.reason.as_deref(), Some(expected_reason.as_str()));
        assert!(finished.retryable);
        assert_eq!(finished.retry_after_seconds, Some(30));
    }

    #[tokio::test]
    async fn retry_adopts_refusal_reservation_and_fences_in_flight_launch() {
        let pool = pool().await;
        let task_id = Uuid::new_v4();
        let launches = Arc::new(AtomicUsize::new(0));
        let (record, inserted) = ManagedTaskEffect::begin(&pool, effect(task_id, Uuid::new_v4()))
            .await
            .unwrap();
        assert!(inserted);
        let refusal = services::services::host_admission::AdmissionError::ProcessExhausted {
            available: 2_810,
            reserve: 3_002,
            culprit: Some("2,212 dead children of FwUpdateManagerd (pid 693)".into()),
        };
        let finished =
            finish_launch_failure(&pool, record, &ApiError::HostAdmission(refusal), false)
                .await
                .unwrap();
        let original_session_id = finished.session_id;
        let (launch_started_tx, launch_started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let retry = {
            let launches = launches.clone();
            let retry_pool = pool.clone();
            tokio::spawn(async move {
                reserve_and_launch(
                    &retry_pool,
                    effect(task_id, Uuid::new_v4()),
                    move |_, _| async move {
                        launches.fetch_add(1, Ordering::SeqCst);
                        let _ = launch_started_tx.send(());
                        let _ = release_rx.await;
                        Ok(())
                    },
                )
                .await
                .unwrap()
            })
        };
        launch_started_rx.await.unwrap();

        let concurrent = reserve_and_launch(&pool, effect(task_id, Uuid::new_v4()), {
            let launches = launches.clone();
            move |_, _| async move {
                launches.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .await
        .unwrap();
        assert!(!concurrent.inserted);
        assert!(concurrent.launch_result.is_none());
        assert_eq!(concurrent.record.session_id, original_session_id);

        release_tx.send(()).unwrap();
        let retry = retry.await.unwrap();
        assert!(retry.inserted);
        assert!(retry.launch_result.unwrap().is_ok());
        assert_eq!(retry.record.session_id, original_session_id);
        assert_eq!(launches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn definitive_failure_is_lost_with_its_real_reason() {
        let pool = pool().await;
        let task_id = Uuid::new_v4();
        let (record, inserted) = ManagedTaskEffect::begin(&pool, effect(task_id, Uuid::new_v4()))
            .await
            .unwrap();
        assert!(inserted);

        let finished = finish_launch_failure(
            &pool,
            record,
            &ApiError::BadRequest("requested workspace does not exist".into()),
            false,
        )
        .await
        .unwrap();

        assert_eq!(finished.state, "lost");
        assert_eq!(
            finished.reason.as_deref(),
            Some("Bad request: requested workspace does not exist")
        );
        assert!(!finished.retryable);
        assert_eq!(finished.retry_after_seconds, None);
    }
}
