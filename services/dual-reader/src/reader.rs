use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use dashmap::DashMap;
use tracing::{info, warn, error};
use uuid::Uuid;

use scylla_sync_shared::{
    types::{ValidationResult, Discrepancy, DiscrepancyType, RowData},
    errors::SyncError,
    database::ScyllaConnection,
    metrics,
};
use crate::config::DualReaderConfig;
use crate::validator::Validator;

pub struct DualReader {
    source_conn: Arc<ScyllaConnection>,
    target_conn: Arc<ScyllaConnection>,
    config: Arc<RwLock<DualReaderConfig>>,
    discrepancies: Arc<DashMap<Uuid, Discrepancy>>,
    validator: Arc<Validator>,
}

impl DualReader {
    pub async fn new(config: DualReaderConfig) -> Result<Self, SyncError> {
        let source_conn = Arc::new(ScyllaConnection::new(&config.source).await?);
        let target_conn = Arc::new(ScyllaConnection::new(&config.target).await?);
        
        info!("Initialized connections to source and target clusters for validation");
        
        let validator = Arc::new(Validator::new(
            source_conn.clone(),
            target_conn.clone(),
        ));
        
        Ok(Self {
            source_conn,
            target_conn,
            config: Arc::new(RwLock::new(config)),
            discrepancies: Arc::new(DashMap::new()),
            validator,
        })
    }
    
    pub async fn validate_table(&self, table: &str) -> Result<ValidationResult, SyncError> {
        info!("Starting validation for table: {}", table);
        let start = std::time::Instant::now();
        
        let config = self.config.read().await;
        let sample_rate = config.reader.sample_rate;
        let batch_size = config.reader.batch_size;
        drop(config);
        
        // Perform validation
        let result = self.validator.validate_table(
            table,
            sample_rate,
            batch_size,
        ).await?;
        
        // Store discrepancies
        for discrepancy in &result.discrepancies {
            self.discrepancies.insert(discrepancy.id, discrepancy.clone());
        }
        
        let duration = start.elapsed();
        info!(
            "Validation complete for table {}: {}/{} rows matched ({:.2}%) in {:?}",
            table,
            result.rows_matched,
            result.rows_checked,
            result.consistency_percentage,
            duration
        );
        
        // Record metrics
        metrics::record_operation(
            "validation",
            table,
            result.discrepancies.is_empty(),
            duration.as_secs_f64()
        );
        
        Ok(result)
    }
    
    pub async fn validate_all_tables(&self) -> Result<Vec<ValidationResult>, SyncError> {
        let config = self.config.read().await;
        let tables = config.reader.tables.clone();
        drop(config);
        
        let mut results = Vec::new();
        
        for table in tables {
            match self.validate_table(&table).await {
                Ok(result) => results.push(result),
                Err(e) => {
                    error!("Failed to validate table {}: {}", table, e);
                }
            }
        }
        
        Ok(results)
    }
    
    pub async fn continuous_validation_loop(&self) {
        loop {
            let config = self.config.read().await;
            let interval = Duration::from_secs(config.reader.validation_interval_secs);
            drop(config);
            
            info!("Starting continuous validation cycle");
            
            match self.validate_all_tables().await {
                Ok(results) => {
                    let total_discrepancies: usize = results.iter()
                        .map(|r| r.discrepancies.len())
                        .sum();
                    
                    info!(
                        "Validation cycle complete: {} tables validated, {} total discrepancies found",
                        results.len(),
                        total_discrepancies
                    );
                }
                Err(e) => {
                    error!("Validation cycle failed: {}", e);
                }
            }
            
            tokio::time::sleep(interval).await;
        }
    }
    
    pub fn get_discrepancies(&self) -> Vec<Discrepancy> {
        self.discrepancies
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }
    
    pub fn get_discrepancies_for_table(&self, table: &str) -> Vec<Discrepancy> {
        self.discrepancies
            .iter()
            .filter(|entry| entry.value().table == table)
            .map(|entry| entry.value().clone())
            .collect()
    }
    
    pub fn clear_discrepancies(&self) {
        self.discrepancies.clear();
        info!("Cleared all discrepancies");
    }
    
    pub async fn reconcile_discrepancy(&self, discrepancy_id: Uuid) -> Result<(), SyncError> {
        let discrepancy = self.discrepancies
            .get(&discrepancy_id)
            .ok_or_else(|| SyncError::ValidationError("Discrepancy not found".to_string()))?
            .clone();
        
        info!("Reconciling discrepancy: {:?}", discrepancy.discrepancy_type);
        
        match discrepancy.discrepancy_type {
            DiscrepancyType::MissingInTarget => {
                // Copy from source to target
                if let Some(source_data) = &discrepancy.source_value {
                    self.validator.copy_row_to_target(&discrepancy.table, source_data).await?;
                }
            }
            DiscrepancyType::MissingInSource => {
                // This shouldn't happen in a migration scenario
                warn!("Row exists in target but not source - unexpected in migration");
            }
            DiscrepancyType::DataMismatch => {
                // Copy from source to target (source is authoritative)
                if let Some(source_data) = &discrepancy.source_value {
                    self.validator.copy_row_to_target(&discrepancy.table, source_data).await?;
                }
            }
            _ => {
                info!("No reconciliation needed for {:?}", discrepancy.discrepancy_type);
            }
        }
        
        // Remove from discrepancies list
        self.discrepancies.remove(&discrepancy_id);
        
        Ok(())
    }
    
    pub async fn health_check(&self) -> Result<(), SyncError> {
        // Check both connections
        self.source_conn.get_session()
            .query("SELECT now() FROM system.local", &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Source health check failed: {}", e)))?;
        
        self.target_conn.get_session()
            .query("SELECT now() FROM system.local", &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Target health check failed: {}", e)))?;
        
        Ok(())
    }
}