use axum::{
    body::Body,
    extract::Request,
    http::{Method, StatusCode, header},
    middleware::Next,
    response::Response,
};

use crate::routes::maintenance::drain_remaining_millis;

fn starts_execution(path: &str) -> bool {
    path == "/workspaces/start"
        || path == "/workspaces/from-pr"
        || path.ends_with("/follow-up")
        || path.ends_with("/commands/dispatch")
        || path.ends_with("/review")
        || path.ends_with("/teammates")
        || path.ends_with("/setup")
        || path.contains("/execution/dev-server/start")
        || path.contains("/execution/cleanup")
        || path.contains("/execution/archive")
}

fn should_reject(method: &Method, path: &str, remaining: u64) -> bool {
    *method == Method::POST && starts_execution(path) && remaining > 0
}

pub async fn reject_mutations_while_draining(request: Request, next: Next) -> Response {
    let remaining = drain_remaining_millis();
    if should_reject(request.method(), request.uri().path(), remaining) {
        let retry_after = remaining.div_ceil(1000).max(1).to_string();
        return Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(header::RETRY_AFTER, retry_after)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"success":false,"message":"cdesktop is draining for a verified local update; retry shortly"}"#,
            ))
            .unwrap_or_else(|_| Response::new(Body::empty()));
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_rejects_starts_but_allows_stops_and_other_mutations() {
        assert!(should_reject(&Method::POST, "/sessions/1/follow-up", 1000));
        assert!(should_reject(&Method::POST, "/workspaces/start", 1000));
        assert!(!should_reject(
            &Method::POST,
            "/execution-processes/1/stop",
            1000
        ));
        assert!(!should_reject(
            &Method::POST,
            "/workspaces/1/execution/stop",
            1000
        ));
        assert!(!should_reject(&Method::DELETE, "/workspaces/1", 1000));
        assert!(!should_reject(&Method::GET, "/workspaces", 1000));
        assert!(!should_reject(&Method::POST, "/maintenance/drain", 1000));
        assert!(!should_reject(&Method::POST, "/sessions/1/follow-up", 0));
    }
}
