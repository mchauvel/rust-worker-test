use std::time::Instant;

use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
use serde::Serialize;
use std::sync::Arc;

use crate::status::RunnerHealth;

#[derive(Clone)]
struct ApiState {
    instance_id: String,
    started_at: Instant,
    health: Arc<RunnerHealth>,
}

#[derive(Serialize)]
struct RunnerStatus {
    scope: &'static str,
    instance_id: String,
    process_status: &'static str,
    runner_status: &'static str,
    active_tasks: usize,
    uptime_seconds: u64,
}

pub fn router(health: Arc<RunnerHealth>) -> Router {
    let state = ApiState {
        instance_id: format!("process-{}", std::process::id()),
        started_at: Instant::now(),
        health,
    };

    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .route("/api/v1/runners/self", get(runner_status))
        .with_state(state)
}

async fn liveness() -> StatusCode {
    StatusCode::OK
}

async fn readiness(State(state): State<ApiState>) -> StatusCode {
    if state.health.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn runner_status(State(state): State<ApiState>) -> Json<RunnerStatus> {
    Json(RunnerStatus {
        scope: "local",
        instance_id: state.instance_id,
        process_status: "running",
        runner_status: state.health.as_str(),
        active_tasks: state.health.active_tasks(),
        uptime_seconds: state.started_at.elapsed().as_secs(),
    })
}
