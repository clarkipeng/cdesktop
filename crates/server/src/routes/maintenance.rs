use std::path::{Path, PathBuf};

use axum::{Json, Router, response::Json as ResponseJson, routing::get};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use services::services::maintenance::{drain_remaining_millis, set_drain as set_drain_seconds};
use utils::response::ApiResponse;

use crate::DeploymentImpl;

#[derive(Debug, Deserialize)]
struct DrainRequest {
    seconds: u64,
}

#[derive(Debug, Serialize)]
struct DrainStatus {
    draining: bool,
    remaining_millis: u64,
}

fn status() -> DrainStatus {
    let remaining_millis = drain_remaining_millis();
    DrainStatus {
        draining: remaining_millis > 0,
        remaining_millis,
    }
}

async fn get_drain() -> ResponseJson<ApiResponse<DrainStatus>> {
    ResponseJson(ApiResponse::success(status()))
}

async fn set_drain(Json(request): Json<DrainRequest>) -> ResponseJson<ApiResponse<DrainStatus>> {
    set_drain_seconds(request.seconds).await;
    ResponseJson(ApiResponse::success(status()))
}

#[derive(Debug, Serialize)]
struct UpdateStatus {
    managed: bool,
    status: String,
    pending_version: Option<String>,
    active_version: Option<String>,
    updated_at: Option<f64>,
}

fn update_state_path() -> PathBuf {
    std::env::var_os("SIGHTMESH_UPDATE_STATE")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".local/state/sightmesh/update.json")))
        .unwrap_or_else(|| PathBuf::from(".local/state/sightmesh/update.json"))
}

fn version(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_object)
        .and_then(|release| release.get("version"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn read_update_status(path: &Path) -> UpdateStatus {
    let Ok(contents) = std::fs::read_to_string(path) else {
        return UpdateStatus {
            managed: false,
            status: "unmanaged".to_string(),
            pending_version: None,
            active_version: None,
            updated_at: None,
        };
    };
    let Ok(value) = serde_json::from_str::<Value>(&contents) else {
        return UpdateStatus {
            managed: true,
            status: "unavailable".to_string(),
            pending_version: None,
            active_version: None,
            updated_at: None,
        };
    };
    UpdateStatus {
        managed: true,
        status: value
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unavailable")
            .to_string(),
        pending_version: version(&value, "pending"),
        active_version: version(&value, "active"),
        updated_at: value.get("updated_at").and_then(Value::as_f64),
    }
}

async fn get_update() -> ResponseJson<ApiResponse<UpdateStatus>> {
    ResponseJson(ApiResponse::success(read_update_status(
        &update_state_path(),
    )))
}

pub fn router() -> Router<DeploymentImpl> {
    Router::new()
        .route("/maintenance/drain", get(get_drain).post(set_drain))
        .route("/maintenance/update", get(get_update))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drain_is_bounded_and_can_be_released() {
        let _owner = services::services::maintenance::test_drain_owner().await;
        let _ = set_drain(Json(DrainRequest { seconds: 300 })).await;
        assert!(drain_remaining_millis() <= 30 * 1000);
        assert!(drain_remaining_millis() > 0);

        let _ = set_drain(Json(DrainRequest { seconds: 0 })).await;
        assert_eq!(drain_remaining_millis(), 0);
    }

    #[test]
    fn update_status_is_redacted_to_display_fields() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("update.json");
        std::fs::write(
            &path,
            r#"{
                "status":"waiting-for-idle",
                "pending":{"version":"0.2.4","source":"secret","executable":"/private/path"},
                "active":{"version":"0.2.3"},
                "updated_at":123.0,
                "last_error":"private details"
            }"#,
        )
        .unwrap();

        let status = read_update_status(&path);

        assert!(status.managed);
        assert_eq!(status.status, "waiting-for-idle");
        assert_eq!(status.pending_version.as_deref(), Some("0.2.4"));
        assert_eq!(status.active_version.as_deref(), Some("0.2.3"));
    }
}
