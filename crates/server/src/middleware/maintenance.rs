use axum::{
    Json,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use services::services::maintenance::DrainError;
use utils::response::ApiResponse;

/// Preserve the HTTP retry contract at the edge. Admission itself belongs to
/// the shared execution-start boundary, not to route or method classification.
pub fn drain_refusal_response(error: &DrainError) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, error.retry_after_seconds().to_string())],
        Json(ApiResponse::<()>::error(
            "cdesktop is draining for a verified local update; retry shortly",
        )),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use services::services::maintenance::{admit_execution_start, set_drain};

    use super::*;

    #[tokio::test]
    async fn externally_reachable_start_gets_retryable_service_unavailable() {
        set_drain(30).await;
        let error = admit_execution_start().await.unwrap_err();
        let response = drain_refusal_response(&error);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().contains_key(header::RETRY_AFTER));
        set_drain(0).await;
    }
}
