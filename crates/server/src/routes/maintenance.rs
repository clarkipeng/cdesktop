use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    response::Json as ResponseJson,
    routing::get,
};
use serde::{Deserialize, Serialize};
use utils::response::ApiResponse;

use crate::DeploymentImpl;

const MAX_DRAIN_SECONDS: u64 = 30;
static DRAIN_UNTIL_MILLIS: AtomicU64 = AtomicU64::new(0);

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub fn drain_remaining_millis() -> u64 {
    DRAIN_UNTIL_MILLIS
        .load(Ordering::Acquire)
        .saturating_sub(now_millis())
}

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
    let seconds = request.seconds.min(MAX_DRAIN_SECONDS);
    let deadline = if seconds == 0 {
        0
    } else {
        now_millis().saturating_add(seconds.saturating_mul(1000))
    };
    DRAIN_UNTIL_MILLIS.store(deadline, Ordering::Release);
    ResponseJson(ApiResponse::success(status()))
}

pub fn router() -> Router<DeploymentImpl> {
    Router::new().route("/maintenance/drain", get(get_drain).post(set_drain))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drain_is_bounded_and_can_be_released() {
        let _ = set_drain(Json(DrainRequest { seconds: 300 })).await;
        assert!(drain_remaining_millis() <= MAX_DRAIN_SECONDS * 1000);
        assert!(drain_remaining_millis() > 0);

        let _ = set_drain(Json(DrainRequest { seconds: 0 })).await;
        assert_eq!(drain_remaining_millis(), 0);
    }
}
