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
use uuid::Uuid;

use crate::reader::DualReader;

pub async fn start_server(reader: Arc<DualReader>, port: u16) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/validate", post(handle_validate))
        .route("/validate/:table", post(handle_validate_table))
        .route("/discrepancies", get(handle_get_discrepancies))
        .route("/discrepancies/:table", get(handle_get_table_discrepancies))
        .route("/discrepancies/clear", post(handle_clear_discrepancies))
        .route("/reconcile/:id", post(handle_reconcile))
        .route("/health", get(handle_health))
        .route("/status", get(handle_status))
        .route("/metrics", get(handle_metrics))
        .layer(TraceLayer::new_for_http())
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .with_state(reader);
    
    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("Dual-Reader API server listening on {}", addr);
    
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Deserialize)]
struct ValidateRequest {
    tables: Option<Vec<String>>,
    sample_rate: Option<f64>,
}

async fn handle_validate(
    State(reader): State<Arc<DualReader>>,
    Json(request): Json<ValidateRequest>,
) -> impl IntoResponse {
    match reader.validate_all_tables().await {
        Ok(results) => (StatusCode::OK, Json(json!({
            "success": true,
            "results": results,
        }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
            "success": false,
            "error": e.to_string(),
        }))).into_response(),
    }
}

async fn handle_validate_table(
    State(reader): State<Arc<DualReader>>,
    axum::extract::Path(table): axum::extract::Path<String>,
) -> impl IntoResponse {
    match reader.validate_table(&table).await {
        Ok(result) => (StatusCode::OK, Json(result)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
            "success": false,
            "error": e.to_string(),
        }))).into_response(),
    }
}

async fn handle_get_discrepancies(
    State(reader): State<Arc<DualReader>>,
) -> impl IntoResponse {
    let discrepancies = reader.get_discrepancies();
    Json(json!({
        "total": discrepancies.len(),
        "discrepancies": discrepancies,
    }))
}

async fn handle_get_table_discrepancies(
    State(reader): State<Arc<DualReader>>,
    axum::extract::Path(table): axum::extract::Path<String>,
) -> impl IntoResponse {
    let discrepancies = reader.get_discrepancies_for_table(&table);
    Json(json!({
        "table": table,
        "total": discrepancies.len(),
        "discrepancies": discrepancies,
    }))
}

async fn handle_clear_discrepancies(
    State(reader): State<Arc<DualReader>>,
) -> impl IntoResponse {
    reader.clear_discrepancies();
    Json(json!({
        "success": true,
        "message": "Discrepancies cleared",
    }))
}

async fn handle_reconcile(
    State(reader): State<Arc<DualReader>>,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
) -> impl IntoResponse {
    match reader.reconcile_discrepancy(id).await {
        Ok(_) => (StatusCode::OK, Json(json!({
            "success": true,
            "message": format!("Discrepancy {} reconciled", id),
        }))).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({
            "success": false,
            "error": e.to_string(),
        }))).into_response(),
    }
}

async fn handle_health(
    State(reader): State<Arc<DualReader>>,
) -> impl IntoResponse {
    match reader.health_check().await {
        Ok(_) => Json(json!({
            "status": "healthy",
            "service": "dual-reader",
            "timestamp": chrono::Utc::now(),
        })),
        Err(e) => Json(json!({
            "status": "unhealthy",
            "error": e.to_string(),
        })),
    }
}

async fn handle_status(
    State(reader): State<Arc<DualReader>>,
) -> impl IntoResponse {
    let discrepancies = reader.get_discrepancies();
    Json(json!({
        "service": "dual-reader",
        "status": "running",
        "total_discrepancies": discrepancies.len(),
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