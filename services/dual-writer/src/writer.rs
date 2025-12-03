use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use dashmap::DashMap;
use parking_lot::Mutex;
use tracing::{info, warn, error, debug};
use uuid::Uuid;

use svckit::{
    types::WriteRequest,
    errors::SyncError,
    database::{ScyllaConnection, DatabaseConnection, QueryBuilder},
    metrics,
};
use crate::config::{DualWriterConfig, WriteMode};
use crate::filter::FilterGovernor;  // <-- NEW: Import FilterGovernor

pub struct DualWriter {
    source_conn: Arc<ScyllaConnection>,
    target_conn: Arc<ScyllaConnection>,
    config: Arc<RwLock<DualWriterConfig>>,
    failed_writes: Arc<DashMap<Uuid, FailedWrite>>,
    stats: Arc<Mutex<WriterStats>>,
    filter_governor: Arc<FilterGovernor>,  // <-- NEW: FilterGovernor field
}

#[derive(Debug, Clone)]
struct FailedWrite {
    request: WriteRequest,
    error: String,
    attempts: u32,
    first_attempt: SystemTime,
}

#[derive(Debug, Default, Clone)]
pub struct WriterStats {
    pub total_writes: u64,
    pub successful_writes: u64,
    pub failed_writes: u64,
    pub retried_writes: u64,
}

pub struct WriteResponse {
    pub success: bool,
    pub request_id: Uuid,
    pub write_timestamp: i64,
    pub latency_ms: f64,
    pub error: Option<String>,
}

impl DualWriter {
    pub async fn new(config: DualWriterConfig, filter_governor: Arc<FilterGovernor>) -> Result<Self, SyncError> {  // <-- NEW: Add filter parameter
        let source_conn = Arc::new(ScyllaConnection::new(&config.source).await?);
        let target_conn = Arc::new(ScyllaConnection::new(&config.target).await?);
        
        info!("Initialized connections to source and target ScyllaDB clusters");
        
        Ok(Self {
            source_conn,
            target_conn,
            config: Arc::new(RwLock::new(config)),
            failed_writes: Arc::new(DashMap::new()),
            stats: Arc::new(Mutex::new(WriterStats::default())),
            filter_governor,  // <-- NEW: Store filter
        })
    }
    
    pub async fn write(&self, mut request: WriteRequest) -> Result<WriteResponse, SyncError> {
        let start = Instant::now();
        let config = self.config.read().await;
        let mode = config.writer.mode.clone();
        drop(config);
        
        // Generate timestamp if not provided
        if request.timestamp.is_none() {
            request.timestamp = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as i64
            );
        }
        
        let result = match mode {
            WriteMode::SourceOnly => self.write_source_only(&request).await,
            WriteMode::DualAsync => self.write_dual_async(&request).await,
            WriteMode::DualSync => self.write_dual_sync(&request).await,
            WriteMode::TargetOnly => self.write_target_only(&request).await,
        };
        
        // Update stats
        {
            let mut stats = self.stats.lock();
            stats.total_writes += 1;
            if result.is_ok() {
                stats.successful_writes += 1;
            } else {
                stats.failed_writes += 1;
            }
        }
        
        // Record metrics
        let duration = start.elapsed().as_secs_f64();
        metrics::record_operation("write", &mode.to_string(), result.is_ok(), duration);
        
        result
    }
    
    async fn write_source_only(&self, request: &WriteRequest) -> Result<WriteResponse, SyncError> {
        let query = QueryBuilder::build_insert_query(request);
        let start = Instant::now();
        
        self.source_conn.execute(&query, request.values.clone()).await?;
        
        Ok(WriteResponse {
            success: true,
            request_id: request.request_id,
            write_timestamp: request.timestamp.unwrap_or(0),
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
            error: None,
        })
    }
    
    async fn write_dual_async(&self, request: &WriteRequest) -> Result<WriteResponse, SyncError> {
        let query = QueryBuilder::build_insert_query(request);
        let start = Instant::now();
        
        // Write to source first (primary)
        self.source_conn.execute(&query, request.values.clone()).await?;
        let primary_latency = start.elapsed().as_secs_f64() * 1000.0;
        
        // <-- NEW: Check filter before shadow write
        if !self.filter_governor.should_write_to_target(request).await {
            info!("Skipping target write - tenant/table is blacklisted: {}.{}", 
                  request.keyspace, request.table);
            return Ok(WriteResponse {
                success: true,
                request_id: request.request_id,
                write_timestamp: request.timestamp.unwrap_or(0),
                latency_ms: primary_latency,
                error: None,
            });
        }
        
        // Shadow write to target (async, non-blocking)
        let target_conn = self.target_conn.clone();
        let request_clone = request.clone();
        let query_clone = query.clone();
        let failed_writes = self.failed_writes.clone();
        let config = self.config.read().await.clone();
        
        tokio::spawn(async move {
            let timeout = Duration::from_millis(config.writer.shadow_timeout_ms);
            let result = tokio::time::timeout(
                timeout,
                target_conn.execute(&query_clone, request_clone.values.clone())
            ).await;
            
            match result {
                Ok(Ok(_)) => {
                    info!("Shadow write successful for request {}", request_clone.request_id);
                }
                Ok(Err(e)) => {
                    warn!("Shadow write failed for request {}: {:?}", request_clone.request_id, e);
                    failed_writes.insert(
                        request_clone.request_id,
                        FailedWrite {
                            request: request_clone,
                            error: e.to_string(),
                            attempts: 1,
                            first_attempt: SystemTime::now(),
                        }
                    );
                }
                Err(_timeout_err) => {
                    warn!("Shadow write timed out for request {}", request_clone.request_id);
                    failed_writes.insert(
                        request_clone.request_id,
                        FailedWrite {
                            request: request_clone.clone(),
                            error: "Timeout".to_string(),
                            attempts: 1,
                            first_attempt: SystemTime::now(),
                        }
                    );
                }
            }
        });
        
        Ok(WriteResponse {
            success: true,
            request_id: request.request_id,
            write_timestamp: request.timestamp.unwrap_or(0),
            latency_ms: primary_latency,
            error: None,
        })
    }
    
    async fn write_dual_sync(&self, request: &WriteRequest) -> Result<WriteResponse, SyncError> {
        let query = QueryBuilder::build_insert_query(request);
        let start = Instant::now();
        
        // <-- NEW: Check filter before target write
        let should_write_target = self.filter_governor.should_write_to_target(request).await;
        
        if !should_write_target {
            // Only write to source
            self.source_conn.execute(&query, request.values.clone()).await?;
            info!("Skipping target write - tenant/table is blacklisted: {}.{}", 
                  request.keyspace, request.table);
        } else {
            // Execute both writes in parallel
            let source_future = self.source_conn.execute(&query, request.values.clone());
            let target_future = self.target_conn.execute(&query, request.values.clone());
            
            let (source_result, target_result) = tokio::join!(source_future, target_future);
            
            // Both must succeed
            source_result?;
            target_result?;
        }
        
        Ok(WriteResponse {
            success: true,
            request_id: request.request_id,
            write_timestamp: request.timestamp.unwrap_or(0),
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
            error: None,
        })
    }
    
    async fn write_target_only(&self, request: &WriteRequest) -> Result<WriteResponse, SyncError> {
        let query = QueryBuilder::build_insert_query(request);
        let start = Instant::now();
        
        self.target_conn.execute(&query, request.values.clone()).await?;
        
        Ok(WriteResponse {
            success: true,
            request_id: request.request_id,
            write_timestamp: request.timestamp.unwrap_or(0),
            latency_ms: start.elapsed().as_secs_f64() * 1000.0,
            error: None,
        })
    }
    
    pub async fn retry_failed_writes_loop(&self) {
        loop {
            let config = self.config.read().await;
            let interval = Duration::from_secs(config.writer.retry_interval_secs);
            let max_attempts = config.writer.max_retry_attempts;
            drop(config);
            
            tokio::time::sleep(interval).await;
            
            let mut to_remove = Vec::new();
            let mut to_update = Vec::new();
            
            for entry in self.failed_writes.iter() {
                let id = *entry.key();
                let write = entry.value().clone();
                
                if write.attempts >= max_attempts {
                    error!("Abandoning write {} after {} attempts", id, write.attempts);
                    to_remove.push(id);
                    continue;
                }
                
                // <-- NEW: Check filter before retry
                if !self.filter_governor.should_write_to_target(&write.request).await {
                    info!("Removing failed write {} - now blacklisted", id);
                    to_remove.push(id);
                    continue;
                }
                
                let query = QueryBuilder::build_insert_query(&write.request);
                match self.target_conn.execute(&query, write.request.values.clone()).await {
                    Ok(_) => {
                        info!("Retry successful for write {}", id);
                        to_remove.push(id);
                        
                        let mut stats = self.stats.lock();
                        stats.retried_writes += 1;
                    }
                    Err(e) => {
                        warn!("Retry {} failed for write {}: {}", write.attempts + 1, id, e);
                        to_update.push((id, FailedWrite {
                            attempts: write.attempts + 1,
                            error: e.to_string(),
                            ..write
                        }));
                    }
                }
            }
            
            for id in to_remove {
                self.failed_writes.remove(&id);
            }
            
            for (id, updated_write) in to_update {
                self.failed_writes.insert(id, updated_write);
            }
        }
    }
    
    pub async fn update_config(&self, new_config: DualWriterConfig) {
        let mut config = self.config.write().await;
        *config = new_config;
        info!("Configuration updated");
    }
    
    pub fn get_stats(&self) -> WriterStats {
        self.stats.lock().clone()
    }
    
    /// Query source cluster (for SELECT queries)
    pub async fn query_source(&self, query: &str, values: Vec<serde_json::Value>) -> Result<Vec<Vec<serde_json::Value>>, SyncError> {
        debug!("Executing SELECT on source cluster: {}", query);
        
        // Execute query on source cluster
        let result = self.source_conn.execute(query, values).await?;
        
        // For now, return empty rows (proper row parsing would go here)
        Ok(vec![])
    }
}

impl std::fmt::Display for WriteMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteMode::SourceOnly => write!(f, "source_only"),
            WriteMode::DualAsync => write!(f, "dual_async"),
            WriteMode::DualSync => write!(f, "dual_sync"),
            WriteMode::TargetOnly => write!(f, "target_only"),
        }
    }
}