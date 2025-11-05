use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use dashmap::DashMap;
use parking_lot::Mutex;
use tracing::{info, warn, error};
use uuid::Uuid;

use scylla_sync_shared::{
    types::{WriteRequest, WriteResponse},
    errors::SyncError,
    database::{ScyllaConnection, DatabaseConnection, QueryBuilder},
    metrics,
};
use crate::config::{DualWriterConfig, WriteMode};

pub struct DualWriter {
    source_conn: Arc<ScyllaConnection>,
    target_conn: Arc<ScyllaConnection>,
    config: Arc<RwLock<DualWriterConfig>>,
    failed_writes: Arc<DashMap<Uuid, FailedWrite>>,
    stats: Arc<Mutex<WriterStats>>,
}

#[derive(Debug, Clone)]
struct FailedWrite {
    request: WriteRequest,
    error: String,
    attempts: u32,
    first_attempt: SystemTime,
}

#[derive(Debug, Default)]
struct WriterStats {
    total_writes: u64,
    successful_writes: u64,
    failed_writes: u64,
    retried_writes: u64,
}

impl DualWriter {
    pub async fn new(config: DualWriterConfig) -> Result<Self, SyncError> {
        let source_conn = Arc::new(ScyllaConnection::new(&config.source).await?);
        let target_conn = Arc::new(ScyllaConnection::new(&config.target).await?);
        
        info!("Initialized connections to source and target ScyllaDB clusters");
        
        Ok(Self {
            source_conn,
            target_conn,
            config: Arc::new(RwLock::new(config)),
            failed_writes: Arc::new(DashMap::new()),
            stats: Arc::new(Mutex::new(WriterStats::default())),
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
        let mut stats = self.stats.lock();
        stats.total_writes += 1;
        if result.is_ok() {
            stats.successful_writes += 1;
        } else {
            stats.failed_writes += 1;
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
                Ok(Err(e)) | Err(_) => {
                    warn!("Shadow write failed for request {}: {:?}", request_clone.request_id, e);
                    failed_writes.insert(
                        request_clone.request_id,
                        FailedWrite {
                            request: request_clone,
                            error: format!("{:?}", e),
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
        
        // Execute both writes in parallel
        let source_future = self.source_conn.execute(&query, request.values.clone());
        let target_future = self.target_conn.execute(&query, request.values.clone());
        
        let (source_result, target_result) = tokio::join!(source_future, target_future);
        
        // Both must succeed
        source_result?;
        target_result?;
        
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
            
            for entry in self.failed_writes.iter() {
                let (id, mut write) = entry.pair();
                
                if write.attempts >= max_attempts {
                    error!("Abandoning write {} after {} attempts", id, write.attempts);
                    to_remove.push(*id);
                    continue;
                }
                
                let query = QueryBuilder::build_insert_query(&write.request);
                match self.target_conn.execute(&query, write.request.values.clone()).await {
                    Ok(_) => {
                        info!("Retry successful for write {}", id);
                        to_remove.push(*id);
                        
                        let mut stats = self.stats.lock();
                        stats.retried_writes += 1;
                    }
                    Err(e) => {
                        write.attempts += 1;
                        write.error = e.to_string();
                        warn!("Retry {} failed for write {}: {}", write.attempts, id, e);
                    }
                }
            }
            
            for id in to_remove {
                self.failed_writes.remove(&id);
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