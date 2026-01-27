// services/sstable-loader/src/api.rs
//
// REST API for SSTable-Loader with IndexManager integration
//

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, error};

use crate::loader::SSTableLoader;
use crate::index_manager::IndexManager;

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    pub loader: Arc<SSTableLoader>,
    pub index_manager: Option<Arc<IndexManager>>,
}

/// Health check response
#[derive(Serialize)]
struct HealthResponse {
    status: String,
    service: String,
    index_manager_enabled: bool,
    index_count: usize,
}

/// Migration start request
#[derive(Deserialize)]
struct MigrationRequest {
    #[serde(default)]
    keyspace_filter: Option<Vec<String>>,
    #[serde(default)]
    drop_indexes_first: bool,
    #[serde(default)]
    rebuild_indexes_after: bool,
}

/// Migration response
#[derive(Serialize)]
struct MigrationResponse {
    status: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats: Option<serde_json::Value>,
}

/// Index operation response
#[derive(Serialize)]
struct IndexResponse {
    status: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    indexes_affected: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<Vec<String>>,
}

pub async fn start_server(
    loader: Arc<SSTableLoader>,
    index_manager: Option<Arc<IndexManager>>,
    port: u16,
) -> anyhow::Result<()> {
    let state = AppState {
        loader,
        index_manager,
    };
    
    let app = Router::new()
        // Health & Status
        .route("/health", get(health_check))
        .route("/status", get(migration_status))
        
        // Migration endpoints
        .route("/start", post(start_migration))
        .route("/stop", post(stop_migration))
        .route("/migrate/:keyspace/:table", post(migrate_single_table))
        .route("/discover/:keyspace", get(discover_tables))
        
        // Index management endpoints
        .route("/indexes/drop", post(drop_indexes))
        .route("/indexes/rebuild", post(rebuild_indexes))
        .route("/indexes/verify", get(verify_indexes))
        .route("/indexes/status", get(index_status))
        
        // Keyspace-specific index operations
        .route("/indexes/drop/:keyspace", post(drop_keyspace_indexes))
        .route("/indexes/rebuild/:keyspace", post(rebuild_keyspace_indexes))
        
        .with_state(state);
    
    let addr = format!("0.0.0.0:{}", port);
    info!("SSTable-Loader API listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    
    Ok(())
}

/// GET /health - Health check
async fn health_check(State(state): State<AppState>) -> Json<HealthResponse> {
    let (enabled, count) = match &state.index_manager {
        Some(mgr) => (true, mgr.index_count()),
        None => (false, 0),
    };
    
    Json(HealthResponse {
        status: "healthy".to_string(),
        service: "sstable-loader".to_string(),
        index_manager_enabled: enabled,
        index_count: count,
    })
}

/// GET /status - Migration status
async fn migration_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    let stats = state.loader.get_stats().await;
    Json(serde_json::json!({
        "status": "ok",
        "migration": stats,
        "index_manager": state.index_manager.as_ref().map(|m| {
            serde_json::json!({
                "enabled": true,
                "index_count": m.index_count()
            })
        })
    }))
}

/// POST /start - Start migration
async fn start_migration(
    State(state): State<AppState>,
    Json(request): Json<MigrationRequest>,
) -> Json<MigrationResponse> {
    info!("Migration start requested");
    
    // Phase 1: Drop indexes if requested
    if request.drop_indexes_first {
        if let Some(ref idx_mgr) = state.index_manager {
            info!("Dropping indexes before migration...");
            if let Err(e) = idx_mgr.drop_all().await {
                return Json(MigrationResponse {
                    status: "error".to_string(),
                    message: format!("Failed to drop indexes: {}", e),
                    stats: None,
                });
            }
            info!("Indexes dropped successfully");
        }
    }
    
    // Phase 2: Run migration
    match state.loader.start_migration(request.keyspace_filter).await {
        Ok(stats) => {
            info!("Migration completed successfully");
            
            // Phase 3: Rebuild indexes if requested
            if request.rebuild_indexes_after {
                if let Some(ref idx_mgr) = state.index_manager {
                    info!("Rebuilding indexes after migration...");
                    if let Err(e) = idx_mgr.rebuild_all().await {
                        return Json(MigrationResponse {
                            status: "partial".to_string(),
                            message: format!("Migration succeeded but index rebuild failed: {}", e),
                            stats: Some(serde_json::to_value(stats).unwrap_or_default()),
                        });
                    }
                    info!("Indexes rebuilt successfully");
                }
            }
            
            Json(MigrationResponse {
                status: "success".to_string(),
                message: "Migration completed".to_string(),
                stats: Some(serde_json::to_value(stats).unwrap_or_default()),
            })
        }
        Err(e) => {
            error!("Migration failed: {}", e);
            Json(MigrationResponse {
                status: "error".to_string(),
                message: format!("Migration failed: {}", e),
                stats: None,
            })
        }
    }
}

/// POST /stop - Stop migration
async fn stop_migration(State(state): State<AppState>) -> Json<MigrationResponse> {
    match state.loader.stop_migration().await {
        Ok(_) => Json(MigrationResponse {
            status: "success".to_string(),
            message: "Migration stopped".to_string(),
            stats: None,
        }),
        Err(e) => Json(MigrationResponse {
            status: "error".to_string(),
            message: format!("Failed to stop migration: {}", e),
            stats: None,
        }),
    }
}

/// POST /indexes/drop - Drop all indexes
async fn drop_indexes(State(state): State<AppState>) -> Json<IndexResponse> {
    match &state.index_manager {
        Some(idx_mgr) => {
            info!("Dropping all {} indexes", idx_mgr.index_count());
            match idx_mgr.drop_all().await {
                Ok(dropped) => Json(IndexResponse {
                    status: "success".to_string(),
                    message: format!("Dropped {} indexes", dropped.len()),
                    indexes_affected: Some(dropped.len()),
                    details: Some(dropped),
                }),
                Err(e) => Json(IndexResponse {
                    status: "error".to_string(),
                    message: format!("Failed to drop indexes: {}", e),
                    indexes_affected: None,
                    details: None,
                }),
            }
        }
        None => Json(IndexResponse {
            status: "skipped".to_string(),
            message: "Index management is disabled".to_string(),
            indexes_affected: None,
            details: None,
        }),
    }
}

/// POST /indexes/rebuild - Rebuild all indexes
async fn rebuild_indexes(State(state): State<AppState>) -> Json<IndexResponse> {
    match &state.index_manager {
        Some(idx_mgr) => {
            let count = idx_mgr.index_count();
            info!("Rebuilding all {} indexes", count);
            match idx_mgr.rebuild_all().await {
                Ok(_) => Json(IndexResponse {
                    status: "success".to_string(),
                    message: format!("Rebuilt {} indexes", count),
                    indexes_affected: Some(count),
                    details: None,
                }),
                Err(e) => Json(IndexResponse {
                    status: "error".to_string(),
                    message: format!("Failed to rebuild indexes: {}", e),
                    indexes_affected: None,
                    details: None,
                }),
            }
        }
        None => Json(IndexResponse {
            status: "skipped".to_string(),
            message: "Index management is disabled".to_string(),
            indexes_affected: None,
            details: None,
        }),
    }
}

/// GET /indexes/verify - Verify all indexes exist
async fn verify_indexes(State(state): State<AppState>) -> Json<IndexResponse> {
    match &state.index_manager {
        Some(idx_mgr) => {
            info!("Verifying {} indexes", idx_mgr.index_count());
            match idx_mgr.verify_all().await {
                Ok(true) => Json(IndexResponse {
                    status: "success".to_string(),
                    message: "All indexes verified".to_string(),
                    indexes_affected: Some(idx_mgr.index_count()),
                    details: None,
                }),
                Ok(false) => Json(IndexResponse {
                    status: "warning".to_string(),
                    message: "Some indexes are missing".to_string(),
                    indexes_affected: None,
                    details: None,
                }),
                Err(e) => Json(IndexResponse {
                    status: "error".to_string(),
                    message: format!("Verification failed: {}", e),
                    indexes_affected: None,
                    details: None,
                }),
            }
        }
        None => Json(IndexResponse {
            status: "skipped".to_string(),
            message: "Index management is disabled".to_string(),
            indexes_affected: None,
            details: None,
        }),
    }
}

/// GET /indexes/status - Get index manager status
async fn index_status(State(state): State<AppState>) -> Json<serde_json::Value> {
    match &state.index_manager {
        Some(idx_mgr) => Json(serde_json::json!({
            "enabled": true,
            "index_count": idx_mgr.index_count(),
            "status": "ready"
        })),
        None => Json(serde_json::json!({
            "enabled": false,
            "index_count": 0,
            "status": "disabled"
        })),
    }
}

/// POST /indexes/drop/:keyspace - Drop indexes for specific keyspace
async fn drop_keyspace_indexes(
    State(state): State<AppState>,
    axum::extract::Path(keyspace): axum::extract::Path<String>,
) -> Json<IndexResponse> {
    match &state.index_manager {
        Some(idx_mgr) => {
            info!("Dropping indexes for keyspace: {}", keyspace);
            match idx_mgr.drop_keyspace(&keyspace).await {
                Ok(dropped) => Json(IndexResponse {
                    status: "success".to_string(),
                    message: format!("Dropped {} indexes in keyspace {}", dropped.len(), keyspace),
                    indexes_affected: Some(dropped.len()),
                    details: Some(dropped),
                }),
                Err(e) => Json(IndexResponse {
                    status: "error".to_string(),
                    message: format!("Failed to drop indexes: {}", e),
                    indexes_affected: None,
                    details: None,
                }),
            }
        }
        None => Json(IndexResponse {
            status: "skipped".to_string(),
            message: "Index management is disabled".to_string(),
            indexes_affected: None,
            details: None,
        }),
    }
}

/// POST /indexes/rebuild/:keyspace - Rebuild indexes for specific keyspace
async fn rebuild_keyspace_indexes(
    State(state): State<AppState>,
    axum::extract::Path(keyspace): axum::extract::Path<String>,
) -> Json<IndexResponse> {
    match &state.index_manager {
        Some(idx_mgr) => {
            info!("Rebuilding indexes for keyspace: {}", keyspace);
            match idx_mgr.rebuild_keyspace(&keyspace).await {
                Ok(_) => Json(IndexResponse {
                    status: "success".to_string(),
                    message: format!("Rebuilt indexes in keyspace {}", keyspace),
                    indexes_affected: None,
                    details: None,
                }),
                Err(e) => Json(IndexResponse {
                    status: "error".to_string(),
                    message: format!("Failed to rebuild indexes: {}", e),
                    indexes_affected: None,
                    details: None,
                }),
            }
        }
        None => Json(IndexResponse {
            status: "skipped".to_string(),
            message: "Index management is disabled".to_string(),
            indexes_affected: None,
            details: None,
        }),
    }
}

/// POST /migrate/:keyspace/:table - Migrate a single table
async fn migrate_single_table(
    State(state): State<AppState>,
    axum::extract::Path((keyspace, table)): axum::extract::Path<(String, String)>,
) -> Json<MigrationResponse> {
    info!("Single table migration requested: {}.{}", keyspace, table);
    
    match state.loader.migrate_single_table(&keyspace, &table).await {
        Ok(stats) => {
            info!("Single table migration completed: {}.{}", keyspace, table);
            Json(MigrationResponse {
                status: "success".to_string(),
                message: format!("Migration of {}.{} completed", keyspace, table),
                stats: Some(serde_json::to_value(stats).unwrap_or_default()),
            })
        }
        Err(e) => {
            error!("Single table migration failed: {}", e);
            Json(MigrationResponse {
                status: "error".to_string(),
                message: format!("Migration failed: {}", e),
                stats: None,
            })
        }
    }
}

/// GET /discover/:keyspace - Discover all tables in a keyspace
async fn discover_tables(
    State(state): State<AppState>,
    axum::extract::Path(keyspace): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    info!("Discovering tables in keyspace: {}", keyspace);
    
    let query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?";
    
    match state.loader.get_source_connection()
        .get_session()
        .query_unpaged(query, (&keyspace,))
        .await
    {
        Ok(result) => {
            let tables: Vec<String> = result.rows
                .unwrap_or_default()
                .iter()
                .filter_map(|row| {
                    row.columns.first()
                        .and_then(|col| col.as_ref())
                        .and_then(|val| {
                            match val {
                                scylla::frame::response::result::CqlValue::Text(s) => Some(s.clone()),
                                scylla::frame::response::result::CqlValue::Ascii(s) => Some(s.clone()),
                                _ => None,
                            }
                        })
                })
                .collect();
            
            info!("Discovered {} tables in keyspace {}", tables.len(), keyspace);
            
            Json(serde_json::json!({
                "status": "success",
                "keyspace": keyspace,
                "table_count": tables.len(),
                "tables": tables
            }))
        }
        Err(e) => {
            error!("Failed to discover tables: {}", e);
            Json(serde_json::json!({
                "status": "error",
                "message": format!("Failed to discover tables: {}", e)
            }))
        }
    }
}