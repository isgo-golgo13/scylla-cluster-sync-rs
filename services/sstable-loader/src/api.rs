use std::sync::Arc;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use prometheus::{Encoder, TextEncoder};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower_http::{compression::CompressionLayer, cors::CorsLayer, trace::TraceLayer};
use tracing::info;

use crate::loader::SSTableLoader;

pub async fn start_server(loader: Arc<SSTableLoader>, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/start", post(handle_start))
        .route("/pause", post(handle_pause))
        .route("/resume", post(handle_resume))
        .route("/stop", post(handle_stop))
        .route("/progress", get(handle_progress))
        .route("/status", get(handle_status))
        .route("/health", get(handle_health))
        .route("/metrics", get(handle_metrics))
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .with_state(loader);
    
    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("SSTable-Loader API server listening on {}", addr);
    
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Deserialize)]
struct StartRequest {
    tables: Option<Vec<String>>,
}

async fn handle_start(
    State(loader): State<Arc<SSTableLoader>>,
    Json(request): Json<StartRequest>,
) -> impl IntoResponse {
    if loader.is_running() {
        return (StatusCode::CONFLICT, Json(json!({
            "success": false,
            "error": "Migration already running",
        }))).into_response();
    }
    
    let loader_clone = loader.clone();
    let tables = request.tables.clone();
    
    tokio::spawn(async move {
        if let Err(e) = loader_clone.start_migration(tables).await {
            tracing::error!("Migration failed: {}", e);
        }
    });
    
    (StatusCode::OK, Json(json!({
        "success": true,
        "message": "Migration started",
    }))).into_response()
}

async fn handle_pause(
    State(loader): State<Arc<SSTableLoader>>,
) -> impl IntoResponse {
    loader.pause();
    Json(json!({
        "success": true,
        "message": "Migration paused",
    }))
}

async fn handle_resume(
    State(loader): State<Arc<SSTableLoader>>,
) -> impl IntoResponse {
    loader.resume();
    Json(json!({
        "success": true,
        "message": "Migration resumed",
    }))
}

async fn handle_stop(
    State(loader): State<Arc<SSTableLoader>>,
) -> impl IntoResponse {
    loader.stop();
    Json(json!({
        "success": true,
        "message": "Migration stopped",
    }))
}

async fn handle_progress(
    State(loader): State<Arc<SSTableLoader>>,
) -> impl IntoResponse {
    let (total, migrated, failed, progress, throughput) = loader.get_stats();
    
    Json(json!({
        "total_rows": total,
        "migrated_rows": migrated,
        "failed_rows": failed,
        "progress_percent": progress,
        "throughput_rows_per_sec": throughput,
        "is_running": loader.is_running(),
        "is_paused": loader.is_paused(),
    }))
}

async fn handle_status(
    State(loader): State<Arc<SSTableLoader>>,
) -> impl IntoResponse {
    let (total, migrated, failed, progress, throughput) = loader.get_stats();
    
    Json(json!({
        "service": "sstable-loader",
        "status": if loader.is_running() {
            if loader.is_paused() { "paused" } else { "running" }
        } else {
            "stopped"
        },
        "stats": {
            "total_rows": total,
            "migrated_rows": migrated,
            "failed_rows": failed,
            "progress_percent": progress,
            "throughput_rows_per_sec": throughput,
        },
        "timestamp": chrono::Utc::now(),
    }))
}

async fn handle_health() -> impl IntoResponse {
    Json(json!({
        "status": "healthy",
        "service": "sstable-loader",
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