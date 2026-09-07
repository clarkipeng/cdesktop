use std::sync::LazyLock;

use anyhow;
use axum::{
    Extension, Router,
    extract::{DefaultBodyLimit, Multipart, Path, Query, State, ws::Message},
    http::header,
    middleware::from_fn_with_state,
    response::{IntoResponse, Json as ResponseJson},
    routing::{get, post},
};
use db::models::{
    execution_process::{ExecutionProcess, ExecutionProcessStatus, RunningExecutionInfo},
    execution_process_repo_state::ExecutionProcessRepoState,
    execution_process_stop_operation::{
        StopExecutionOperation, StopExecutionOperationState, StopExecutionOutcome,
    },
};
use deployment::Deployment;
use futures_util::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use services::services::container::ContainerService;
use utils::{log_msg::LogMsg, response::ApiResponse};
use uuid::Uuid;

use crate::{
    DeploymentImpl,
    error::ApiError,
    middleware::{
        load_execution_process_middleware,
        signed_ws::{MaybeSignedWebSocket, SignedWsUpgrade},
    },
};

/// Identifies the server process that owns a pending keyed stop. A different
/// value after restart can reconcile, but never re-execute, an orphaned stop.
static STOP_OPERATION_INSTANCE_ID: LazyLock<Uuid> = LazyLock::new(Uuid::new_v4);

#[derive(Debug, Deserialize)]
struct SessionExecutionProcessQuery {
    pub session_id: Uuid,
    /// If true, include soft-deleted (dropped) processes in results/stream
    #[serde(default)]
    pub show_soft_deleted: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct StopExecutionProcessRequest {
    /// Caller-owned, deterministic key used to replay a lost stop response.
    #[serde(default)]
    dedupe_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawLogRangeQuery {
    start: u64,
    end: u64,
}

#[derive(Debug, Deserialize)]
struct ArtifactUploadQuery {
    /// Original producer-relative path, if it differs from the uploaded name.
    original_path: Option<String>,
    producer_ref: Option<String>,
    publication_key: String,
}

#[derive(Deserialize)]
struct ArtifactPath {
    occurrence_id: Uuid,
}

#[derive(Debug, Serialize)]
struct NormalizedLogSnapshot {
    entries: Vec<serde_json::Value>,
    patch_count: usize,
    skipped_patch_count: usize,
    complete: bool,
    incomplete_reason: Option<String>,
}

fn apply_normalized_message(
    document: &mut serde_json::Value,
    message: LogMsg,
    patch_count: &mut usize,
    skipped_patch_count: &mut usize,
) -> bool {
    match message {
        LogMsg::JsonPatch(patch) => {
            if json_patch::patch(document, &patch).is_ok() {
                *patch_count += 1;
            } else {
                *skipped_patch_count += 1;
            }
            false
        }
        LogMsg::Finished => true,
        _ => false,
    }
}

async fn get_execution_process_by_id(
    Extension(execution_process): Extension<ExecutionProcess>,
    State(_deployment): State<DeploymentImpl>,
) -> Result<ResponseJson<ApiResponse<ExecutionProcess>>, ApiError> {
    Ok(ResponseJson(ApiResponse::success(execution_process)))
}

async fn list_execution_processes_by_session(
    State(deployment): State<DeploymentImpl>,
    Query(query): Query<SessionExecutionProcessQuery>,
) -> Result<ResponseJson<ApiResponse<Vec<ExecutionProcess>>>, ApiError> {
    let processes = ExecutionProcess::find_by_session_id(
        &deployment.db().pool,
        query.session_id,
        query.show_soft_deleted.unwrap_or(false),
    )
    .await?;
    Ok(ResponseJson(ApiResponse::success(processes)))
}

async fn list_running_execution_processes(
    State(deployment): State<DeploymentImpl>,
) -> Result<ResponseJson<ApiResponse<Vec<RunningExecutionInfo>>>, ApiError> {
    let processes = ExecutionProcess::find_running_info(&deployment.db().pool).await?;
    Ok(ResponseJson(ApiResponse::success(processes)))
}

async fn get_normalized_log_snapshot(
    Extension(execution_process): Extension<ExecutionProcess>,
    State(deployment): State<DeploymentImpl>,
) -> Result<ResponseJson<ApiResponse<NormalizedLogSnapshot>>, ApiError> {
    // A live normalizer may still be processing the last raw message when its
    // process ends. Only historical replay awaits all normalizers and can
    // establish a complete bounded view; a live snapshot remains provisional.
    let live_view = deployment
        .container()
        .get_msg_store_by_id(&execution_process.id)
        .await
        .is_some();
    let Some(mut stream) = deployment
        .container()
        .stream_normalized_logs(&execution_process.id)
        .await
    else {
        return Err(ApiError::BadRequest(
            "normalized logs are unavailable for this execution".into(),
        ));
    };

    let mut document = serde_json::json!({ "entries": [] });
    let mut patch_count = 0;
    let mut skipped_patch_count = 0;
    let mut complete = false;
    let mut incomplete_reason = live_view
        .then(|| "live normalized view is provisional; read again after finalization".to_owned());
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(250);
    while patch_count < 100_000 {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let next = tokio::time::timeout(
            remaining.min(std::time::Duration::from_millis(50)),
            stream.next(),
        )
        .await;
        let Ok(Some(message)) = next else {
            break;
        };
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                incomplete_reason = Some(error.to_string());
                continue;
            }
        };
        if apply_normalized_message(
            &mut document,
            message,
            &mut patch_count,
            &mut skipped_patch_count,
        ) {
            complete = incomplete_reason.is_none() && skipped_patch_count == 0;
            break;
        }
    }

    let entries = document
        .get_mut("entries")
        .and_then(serde_json::Value::as_array_mut)
        .map(std::mem::take)
        .unwrap_or_default();
    Ok(ResponseJson(ApiResponse::success(NormalizedLogSnapshot {
        entries,
        patch_count,
        skipped_patch_count,
        complete,
        incomplete_reason,
    })))
}

/// Returns exact producer bytes for a bounded decompressed range. The range
/// namespace is `(execution_process.id, [start, end))`; it remains stable when
/// Zstd frames or the rebuildable sidecar change.
async fn get_raw_log_range(
    Extension(execution_process): Extension<ExecutionProcess>,
    Query(query): Query<RawLogRangeQuery>,
) -> Result<axum::response::Response, ApiError> {
    if query.end < query.start
        || query.end.saturating_sub(query.start)
            > utils::execution_logs::MAX_EXECUTION_LOG_RANGE_BYTES
    {
        return Err(ApiError::BadRequest(
            "invalid or oversized execution log range".into(),
        ));
    }
    let path = utils::execution_logs::process_log_file_path(
        execution_process.session_id,
        execution_process.id,
    );
    let bytes =
        utils::execution_logs::read_execution_log_range_bytes(&path, query.start, query.end)
            .await?;
    axum::response::Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .header(
            "x-cdesktop-source-range",
            format!("[{}, {})", query.start, query.start + bytes.len() as u64),
        )
        .header("x-cdesktop-source-id", execution_process.id.to_string())
        .body(axum::body::Body::from(bytes))
        .map_err(|error| ApiError::BadRequest(error.to_string()))
}

/// Capture completion is an owner fact, not a guess from the execution's exit
/// status or a short range read. Missing/corrupt owners return errors.
async fn get_raw_log_status(
    Extension(process): Extension<ExecutionProcess>,
) -> Result<ResponseJson<ApiResponse<utils::execution_logs::OwnerSummary>>, ApiError> {
    let path = utils::execution_logs::process_log_file_path(process.session_id, process.id);
    let status = utils::execution_logs::scan_owner(&path).await?;
    Ok(ResponseJson(ApiResponse::success(status)))
}

async fn migrate_raw_log(
    Extension(process): Extension<ExecutionProcess>,
    State(deployment): State<DeploymentImpl>,
) -> Result<
    ResponseJson<ApiResponse<services::services::execution_log_migration::MigrationReport>>,
    ApiError,
> {
    if process.status == ExecutionProcessStatus::Running
        || deployment
            .container()
            .get_msg_store_by_id(&process.id)
            .await
            .is_some()
    {
        return Err(ApiError::Conflict(
            "execution capture is still active".into(),
        ));
    }
    let report = services::services::execution_log_migration::migrate_execution_logs(
        &deployment.db().pool,
        &utils::assets::asset_dir(),
        process.id,
    )
    .await
    .map_err(services::services::container::ContainerError::Other)?;
    Ok(ResponseJson(ApiResponse::success(report)))
}

/// Durable producer entry point for checkpoints and reports. Artifact bytes
/// are deduplicated by their attachment hash, while every upload produces an
/// occurrence row owned by this execution.
async fn upload_execution_artifact(
    Extension(execution_process): Extension<ExecutionProcess>,
    State(deployment): State<DeploymentImpl>,
    Query(query): Query<ArtifactUploadQuery>,
    mut multipart: Multipart,
) -> Result<ResponseJson<ApiResponse<db::models::execution_artifact::ExecutionArtifact>>, ApiError>
{
    while let Some(field) = multipart.next_field().await? {
        if field.name() != Some("artifact") {
            continue;
        }
        let filename = field.file_name().unwrap_or("artifact.bin").to_owned();
        if query.publication_key.trim().is_empty() {
            return Err(ApiError::BadRequest("publication_key is required".into()));
        }
        let original_path = query.original_path.as_deref().unwrap_or(&filename);
        let occurrence = deployment
            .file()
            .publish_execution_artifact(
                field.into_stream(),
                &filename,
                services::services::file::ArtifactPublication {
                    execution_id: execution_process.id,
                    original_path,
                    producer_ref: query.producer_ref.as_deref(),
                    publication_key: &query.publication_key,
                },
            )
            .await
            .map_err(|error| match error {
                services::services::file::FileError::PublicationConflict => ApiError::Conflict(
                    "artifact publication key already refers to different evidence".into(),
                ),
                other => other.into(),
            })?;
        return Ok(ResponseJson(ApiResponse::success(occurrence)));
    }
    Err(ApiError::File(
        services::services::file::FileError::NotFound,
    ))
}

async fn get_execution_artifact(
    Extension(execution_process): Extension<ExecutionProcess>,
    State(deployment): State<DeploymentImpl>,
    Path(path): Path<ArtifactPath>,
) -> Result<ResponseJson<ApiResponse<db::models::execution_artifact::ExecutionArtifact>>, ApiError>
{
    let occurrence = deployment
        .file()
        .get_execution_artifact(path.occurrence_id)
        .await?
        .filter(|artifact| artifact.execution_id == execution_process.id)
        .ok_or_else(|| ApiError::File(services::services::file::FileError::NotFound))?;
    Ok(ResponseJson(ApiResponse::success(occurrence)))
}

async fn get_execution_artifact_file(
    Extension(execution_process): Extension<ExecutionProcess>,
    State(deployment): State<DeploymentImpl>,
    Path(path): Path<ArtifactPath>,
) -> Result<axum::response::Response, ApiError> {
    let occurrence = deployment
        .file()
        .get_execution_artifact(path.occurrence_id)
        .await?
        .filter(|artifact| artifact.execution_id == execution_process.id)
        .ok_or_else(|| ApiError::File(services::services::file::FileError::NotFound))?;
    super::attachments::serve_file(Path(occurrence.attachment_id), State(deployment)).await
}

async fn stream_raw_logs_ws(
    ws: SignedWsUpgrade,
    State(deployment): State<DeploymentImpl>,
    Path(exec_id): Path<Uuid>,
) -> impl IntoResponse {
    // Always accept the WebSocket upgrade — handle "not found" inside the
    // connection by sending `finished` and closing cleanly, instead of
    // rejecting with HTTP 404 which the browser surfaces as an opaque
    // connection failure.
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = handle_raw_logs_ws(socket, deployment, exec_id).await {
            tracing::warn!("raw logs WS closed: {}", e);
        }
    })
}

async fn handle_raw_logs_ws(
    mut socket: MaybeSignedWebSocket,
    deployment: DeploymentImpl,
    exec_id: Uuid,
) -> anyhow::Result<()> {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use executors::logs::utils::patch::ConversationPatch;
    use utils::log_msg::LogMsg;

    // Get the raw stream — if not found, send finished and close cleanly
    let raw_stream = match deployment.container().stream_raw_logs(&exec_id).await {
        Some(stream) => stream,
        None => {
            // No logs available: send finished so the client gets a clean
            // close instead of retrying endlessly.
            let _ = socket
                .send(LogMsg::Finished.to_ws_message_unchecked())
                .await;
            let _ = socket.close().await;
            return Ok(());
        }
    };

    let counter = Arc::new(AtomicUsize::new(0));
    let mut stream = raw_stream.map_ok({
        let counter = counter.clone();
        move |m| match m {
            LogMsg::Stdout(content) => {
                let index = counter.fetch_add(1, Ordering::SeqCst);
                let patch = ConversationPatch::add_stdout(index, content);
                LogMsg::JsonPatch(patch).to_ws_message_unchecked()
            }
            LogMsg::Stderr(content) => {
                let index = counter.fetch_add(1, Ordering::SeqCst);
                let patch = ConversationPatch::add_stderr(index, content);
                LogMsg::JsonPatch(patch).to_ws_message_unchecked()
            }
            LogMsg::Finished => LogMsg::Finished.to_ws_message_unchecked(),
            _ => unreachable!("Raw stream should only have Stdout/Stderr/Finished"),
        }
    });

    loop {
        tokio::select! {
            item = stream.next() => {
                match item {
                    Some(Ok(msg)) => {
                        if socket.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("stream error: {}", e);
                        break;
                    }
                    None => break,
                }
            }
            inbound = socket.recv() => {
                match inbound {
                    Ok(Some(Message::Close(_))) => break,
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }
    }
    // Send a proper close frame so the client sees code 1000 (normal closure)
    // instead of an abnormal TCP drop that triggers reconnection attempts.
    let _ = socket.close().await;
    Ok(())
}

async fn stream_normalized_logs_ws(
    ws: SignedWsUpgrade,
    State(deployment): State<DeploymentImpl>,
    Path(exec_id): Path<Uuid>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        let stream = deployment
            .container()
            .stream_normalized_logs(&exec_id)
            .await;

        match stream {
            Some(stream) => {
                let stream = stream.err_into::<anyhow::Error>().into_stream();
                if let Err(e) = handle_normalized_logs_ws(socket, stream).await {
                    tracing::warn!("normalized logs WS closed: {}", e);
                }
            }
            None => {
                // No logs available: send finished and close cleanly
                let mut socket = socket;
                let _ = socket
                    .send(utils::log_msg::LogMsg::Finished.to_ws_message_unchecked())
                    .await;
                let _ = socket.close().await;
            }
        }
    })
}

async fn handle_normalized_logs_ws(
    mut socket: MaybeSignedWebSocket,
    stream: impl futures_util::Stream<Item = anyhow::Result<LogMsg>> + Unpin + Send + 'static,
) -> anyhow::Result<()> {
    let mut stream = stream.map_ok(|msg| msg.to_ws_message_unchecked());
    loop {
        tokio::select! {
            item = stream.next() => {
                match item {
                    Some(Ok(msg)) => {
                        if socket.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("stream error: {}", e);
                        break;
                    }
                    None => break,
                }
            }
            inbound = socket.recv() => {
                match inbound {
                    Ok(Some(Message::Close(_))) => break,
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }
    }
    let _ = socket.close().await;
    Ok(())
}

async fn stop_execution_process(
    Extension(execution_process): Extension<ExecutionProcess>,
    State(deployment): State<DeploymentImpl>,
    payload: Option<axum::Json<StopExecutionProcessRequest>>,
) -> Result<ResponseJson<ApiResponse<()>>, ApiError> {
    let Some(dedupe_key) = payload.and_then(|axum::Json(request)| request.dedupe_key) else {
        deployment
            .container()
            .stop_execution(&execution_process, ExecutionProcessStatus::Killed)
            .await?;

        return Ok(ResponseJson(ApiResponse::success(())));
    };
    if dedupe_key.is_empty() {
        return Err(ApiError::BadRequest("dedupe_key must not be empty".into()));
    }

    let pool = &deployment.db().pool;
    let instance_id = *STOP_OPERATION_INSTANCE_ID;
    let state =
        StopExecutionOperation::begin(pool, execution_process.id, &dedupe_key, instance_id).await?;
    match state {
        StopExecutionOperationState::Complete(outcome) => return stop_outcome_response(outcome),
        StopExecutionOperationState::Owner => {}
        // Terminal execution status is only written after cancellation/kill
        // succeeds. Therefore an orphaned pending request may be accepted
        // only when that durable side-effect boundary was crossed.
        StopExecutionOperationState::Pending {
            owned_by_current_instance: true,
        } => {
            // 425 is deliberately distinct from the durable 409 rejection:
            // retry this exact key until the owner publishes its outcome.
            return Err(ApiError::TooEarly(
                "The original stop request is still in progress; retry the same dedupe_key.".into(),
            ));
        }
        StopExecutionOperationState::Pending {
            owned_by_current_instance: false,
        } => {
            let outcome = orphaned_stop_outcome();
            let outcome = StopExecutionOperation::complete(
                pool,
                execution_process.id,
                &dedupe_key,
                outcome,
                instance_id,
            )
            .await?;
            return stop_outcome_response(outcome);
        }
    }

    let outcome = match deployment
        .container()
        .stop_execution(&execution_process, ExecutionProcessStatus::Killed)
        .await
    {
        Ok(()) => StopExecutionOutcome::Accepted,
        Err(error) => {
            tracing::warn!(
                execution_process_id = %execution_process.id,
                "keyed stop request rejected: {error}"
            );
            StopExecutionOutcome::Rejected
        }
    };
    let outcome = StopExecutionOperation::complete(
        pool,
        execution_process.id,
        &dedupe_key,
        outcome,
        instance_id,
    )
    .await?;
    stop_outcome_response(outcome)
}

fn stop_outcome_response(
    outcome: StopExecutionOutcome,
) -> Result<ResponseJson<ApiResponse<()>>, ApiError> {
    match outcome {
        StopExecutionOutcome::Accepted => Ok(ResponseJson(ApiResponse::success(()))),
        StopExecutionOutcome::Rejected => Err(ApiError::Conflict(
            "The original stop request was rejected.".into(),
        )),
        StopExecutionOutcome::Interrupted => Err(ApiError::StopInterrupted(
            "The original stop owner ended before its result was durably known; reconcile this key without issuing another stop."
                .into(),
        )),
    }
}

fn orphaned_stop_outcome() -> StopExecutionOutcome {
    // A terminal execution row can come from the independent exit monitor,
    // not this stop operation. Without a durable process-controller identity,
    // it cannot prove this key performed the side effect. Preserve safety by
    // recording a distinct terminal interruption rather than inferring either
    // acceptance or rejection.
    StopExecutionOutcome::Interrupted
}

async fn stream_execution_processes_by_session_ws(
    ws: SignedWsUpgrade,
    State(deployment): State<DeploymentImpl>,
    Query(query): Query<SessionExecutionProcessQuery>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = handle_execution_processes_by_session_ws(
            socket,
            deployment,
            query.session_id,
            query.show_soft_deleted.unwrap_or(false),
        )
        .await
        {
            tracing::warn!("execution processes by session WS closed: {}", e);
        }
    })
}

async fn handle_execution_processes_by_session_ws(
    mut socket: MaybeSignedWebSocket,
    deployment: DeploymentImpl,
    session_id: uuid::Uuid,
    show_soft_deleted: bool,
) -> anyhow::Result<()> {
    // Get the raw stream and convert LogMsg to WebSocket messages
    let mut stream = deployment
        .events()
        .stream_execution_processes_for_session_raw(session_id, show_soft_deleted)
        .await?
        .map_ok(|msg| msg.to_ws_message_unchecked());

    loop {
        tokio::select! {
            item = stream.next() => {
                match item {
                    Some(Ok(msg)) => {
                        if socket.send(msg).await.is_err() {
                            break;
                        }
                    }
                    Some(Err(e)) => {
                        tracing::error!("stream error: {}", e);
                        let _ = socket.close_for_refresh().await;
                        break;
                    }
                    None => break,
                }
            }
            inbound = socket.recv() => {
                match inbound {
                    Ok(Some(Message::Close(_))) => break,
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }
    }
    Ok(())
}

async fn get_execution_process_repo_states(
    Extension(execution_process): Extension<ExecutionProcess>,
    State(deployment): State<DeploymentImpl>,
) -> Result<ResponseJson<ApiResponse<Vec<ExecutionProcessRepoState>>>, ApiError> {
    let pool = &deployment.db().pool;
    let repo_states =
        ExecutionProcessRepoState::find_by_execution_process_id(pool, execution_process.id).await?;
    Ok(ResponseJson(ApiResponse::success(repo_states)))
}

fn execution_routes() -> Router<DeploymentImpl> {
    Router::new()
        .route("/", get(get_execution_process_by_id))
        .route("/stop", post(stop_execution_process))
        .route("/repo-states", get(get_execution_process_repo_states))
        .route("/normalized-snapshot", get(get_normalized_log_snapshot))
        .route("/raw-log", get(get_raw_log_range))
        .route("/raw-log/status", get(get_raw_log_status))
        .route("/raw-log/migrate", post(migrate_raw_log))
        .route(
            "/artifacts",
            post(upload_execution_artifact).layer(DefaultBodyLimit::disable()),
        )
        .route("/artifacts/{occurrence_id}", get(get_execution_artifact))
        .route(
            "/artifacts/{occurrence_id}/file",
            get(get_execution_artifact_file),
        )
        .route("/raw-logs/ws", get(stream_raw_logs_ws))
        .route("/normalized-logs/ws", get(stream_normalized_logs_ws))
}

pub(super) fn router(deployment: &DeploymentImpl) -> Router<DeploymentImpl> {
    let workspace_id_router = execution_routes().layer(from_fn_with_state(
        deployment.clone(),
        load_execution_process_middleware,
    ));

    let workspaces_router = Router::new()
        .route("/", get(list_execution_processes_by_session))
        .route("/running", get(list_running_execution_processes))
        .route(
            "/stream/session/ws",
            get(stream_execution_processes_by_session_ws),
        )
        .nest("/{id}", workspace_id_router);

    Router::new().nest("/execution-processes", workspaces_router)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_routes_have_no_overlapping_handlers() {
        // Axum rejects duplicate method/path registration only at construction,
        // so compilation alone cannot prove the native API can start.
        let _ = execution_routes();
    }

    #[tokio::test]
    async fn nested_artifact_paths_extract_parent_and_occurrence_by_name() {
        // A scalar Path<Uuid> fails when the nested route carries two IDs.
        // Exercise the real extractor types through Axum, without a deployment
        // or production DB, on a temporary loopback listener.
        let app = Router::new().route(
            "/{id}/artifacts/{occurrence_id}",
            get(
                |Path(_parent): Path<crate::middleware::ExecutionProcessPath>,
                 Path(path): Path<ArtifactPath>| async move {
                    path.occurrence_id.to_string()
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let parent = Uuid::new_v4();
        let occurrence = Uuid::new_v4();
        let response = reqwest::get(format!("http://{address}/{parent}/artifacts/{occurrence}"))
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), occurrence.to_string());
        let invalid = reqwest::get(format!("http://{address}/invalid/artifacts/{occurrence}"))
            .await
            .unwrap();
        assert_eq!(invalid.status(), axum::http::StatusCode::BAD_REQUEST);
        server.abort();
        let _ = server.await;
    }

    #[test]
    fn normalized_snapshot_coalesces_streaming_replacements() {
        let mut document = serde_json::json!({ "entries": [] });
        let mut applied = 0;
        let mut skipped = 0;
        for patch in [
            serde_json::json!([{
                "op": "add",
                "path": "/entries/0",
                "value": { "content": "hel" }
            }]),
            serde_json::json!([{
                "op": "replace",
                "path": "/entries/0",
                "value": { "content": "hello" }
            }]),
        ] {
            let message =
                LogMsg::JsonPatch(serde_json::from_value(patch).expect("valid JSON patch fixture"));
            assert!(!apply_normalized_message(
                &mut document,
                message,
                &mut applied,
                &mut skipped,
            ));
        }

        assert_eq!(applied, 2);
        assert_eq!(skipped, 0);
        assert_eq!(document["entries"][0]["content"], "hello");
        assert!(apply_normalized_message(
            &mut document,
            LogMsg::Finished,
            &mut applied,
            &mut skipped,
        ));
    }

    #[test]
    fn orphaned_intent_never_infers_acceptance_from_natural_exit_status() {
        for _natural_status in [
            ExecutionProcessStatus::Running,
            ExecutionProcessStatus::Completed,
            ExecutionProcessStatus::Failed,
        ] {
            assert_eq!(orphaned_stop_outcome(), StopExecutionOutcome::Interrupted);
        }
    }

    #[test]
    fn keyed_stop_outcomes_keep_rejection_and_interruption_distinct() {
        assert!(matches!(
            stop_outcome_response(StopExecutionOutcome::Rejected),
            Err(ApiError::Conflict(_))
        ));
        assert!(matches!(
            stop_outcome_response(StopExecutionOutcome::Interrupted),
            Err(ApiError::StopInterrupted(_))
        ));
        assert!(stop_outcome_response(StopExecutionOutcome::Accepted).is_ok());
    }

    #[test]
    fn omitted_dedupe_key_preserves_the_legacy_stop_request() {
        let request: StopExecutionProcessRequest =
            serde_json::from_value(serde_json::json!({})).expect("empty stop request is valid");
        assert!(request.dedupe_key.is_none());
    }
}
