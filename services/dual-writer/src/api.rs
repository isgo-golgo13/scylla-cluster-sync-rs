use std::sync::Arc;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use prometheus::{Encoder, TextEncoder};
use serde_json::json;
use tower_http::{compression::CompressionLayer, cors::CorsLayer, trace::TraceLayer};
use tracing::info;

use scylla_sync_shared::types::{WriteRequest, WriteResponse};
use crate::writer::DualWriter;
use crate::config::DualWriterConfig;

pub async fn start_server(writer: Arc<DualWriter>, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/write", post(handle_write))
        .route("/health", get(handle_health))
        .route("/metrics", get(handle_metrics))
        .route("/stats", get(handle_stats))
        .route("/config", post(handle_config_update))
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .with_state(writer);
    
    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("API server listening on {}", addr);
    
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handle_write(
    State(writer): State<Arc<DualWriter>>,
    Json(request): Json<WriteRequest>,
) -> impl IntoResponse {
    match writer.write(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(e) => {
            let error_response = WriteResponse {
                success: false,
                request_id: uuid::Uuid::new_v4(),
                write_timestamp: 0,
                latency_ms: 0.0,
                error: Some(e.to_string()),
            };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error_response)).into_response()
        }
    }
}

async fn handle_health() -> impl IntoResponse {
    Json(json!({
        "status": "healthy",
        "service": "dual-writer",
        "timestamp": chrono::Utc::now(),
    }))
}

async fn handle_metrics() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

async fn handle_stats(State(writer): State<Arc<DualWriter>>) -> impl IntoResponse {
    let stats = writer.get_stats();
    Json(json!({
        "total_writes": stats.total_writes,
        "successful_writes": stats.successful_writes,
        "failed_writes": stats.failed_writes,
        "retried_writes": stats.retried_writes,
        "success_rate": if stats.total_writes > 0 {
            (stats.successful_writes as f64 / stats.total_writes as f64) * 100.0
        } else {
            100.0
        }
    }))
}

async fn handle_config_update(
    State(writer): State<Arc<DualWriter>>,
    Json(new_config): Json<DualWriterConfig>,
) -> impl IntoResponse {
    writer.update_config(new_config).await;
    StatusCode::OK
}