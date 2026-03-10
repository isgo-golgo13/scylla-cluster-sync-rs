use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use dashmap::DashMap;
use tracing::{info, warn, error};
use uuid::Uuid;
use chrono::Utc;

use svckit::{
    types::{ValidationResult, Discrepancy},
    errors::SyncError,
    database::ScyllaConnection,
    metrics,
};
use crate::config::{DualReaderConfig, ReconciliationMode};
use crate::validator::Validator;
use crate::reconciliation::{
    Reconciliator, ReconciliationStrategy, 
    SourceAuthoritativeStrategy, NewestTimestampStrategy, ManualReviewStrategy
};
use crate::filter::{DualReaderFilterGovernor, ReadDecision};

pub struct DualReader {
    source_conn: Arc<ScyllaConnection>,
    target_conn: Arc<ScyllaConnection>,
    config: Arc<RwLock<DualReaderConfig>>,
    discrepancies: Arc<DashMap<Uuid, Discrepancy>>,
    validator: Arc<Validator>,
    reconciliator: Arc<RwLock<Reconciliator>>,
    /// Whitelist-based filter governor (Iconik client spec)
    filter: Arc<DualReaderFilterGovernor>,
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
        
        let strategy: Box<dyn ReconciliationStrategy> = match config.reader.reconciliation_mode {
            ReconciliationMode::SourceWins => Box::new(SourceAuthoritativeStrategy),
            ReconciliationMode::NewestWins => Box::new(NewestTimestampStrategy),
            ReconciliationMode::Manual => Box::new(ManualReviewStrategy),
        };
        
        let reconciliator = Reconciliator::new(
            strategy,
            source_conn.clone(),
            target_conn.clone(),
        );

        // Build filter governor from config
        let filter = Arc::new(DualReaderFilterGovernor::new(&config.filter));
        if filter.is_enabled() {
            info!("Dual-reader filtering enabled — selective comparison active");
        } else {
            info!("Dual-reader filtering disabled — all tables compared");
        }
        
        Ok(Self {
            source_conn,
            target_conn,
            config: Arc::new(RwLock::new(config)),
            discrepancies: Arc::new(DashMap::new()),
            validator,
            reconciliator: Arc::new(RwLock::new(reconciliator)),
            filter,
        })
    }
    
    /// Validate a table — applies table whitelist filter before proceeding.
    ///
    /// If the table is not in the compare_tables whitelist (when filtering is
    /// enabled), this is a no-op: returns an empty ValidationResult and logs
    /// a source-only notice. No dual read is performed.
    pub async fn validate_table(&self, table: &str) -> Result<ValidationResult, SyncError> {
        // Apply filter — table-level check (no domain context at this call site)
        let decision = self.filter.decide(table, None).await;
        
        if let ReadDecision::SourceOnly { reason } = decision {
            warn!("⊘ Skipping comparison for '{}': {} — source only", table, reason);
            return Ok(ValidationResult {
                table: table.to_string(),
                rows_checked: 0,
                rows_matched: 0,
                discrepancies: vec![],
                consistency_percentage: 100.0,
                validation_time: Utc::now(),
                source_only: true,
            });
        }

        info!("Starting validation for table: {}", table);
        let start = std::time::Instant::now();
        
        let config = self.config.read().await;
        let sample_rate = config.reader.sample_rate;
        let batch_size = config.reader.batch_size;
        drop(config);
        
        let result = self.validator.validate_table(
            table,
            sample_rate,
            batch_size,
        ).await?;
        
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
        
        metrics::record_operation(
            "validation",
            table,
            result.discrepancies.is_empty(),
            duration.as_secs_f64()
        );
        
        Ok(result)
    }

    /// Validate a table for a specific system_domain_id.
    ///
    /// Applies the full AND-gate filter: both table AND domain must be in their
    /// respective whitelists for a dual read to proceed.
    ///
    /// This is the primary entry point for domain-scoped validation matching
    /// the Iconik client filtering spec.
    pub async fn validate_table_for_domain(
        &self,
        table: &str,
        domain_id: &str,
    ) -> Result<ValidationResult, SyncError> {
        let decision = self.filter.decide(table, Some(domain_id)).await;

        if let ReadDecision::SourceOnly { reason } = decision {
            warn!(
                "⊘ Skipping comparison for '{}' domain '{}': {} — source only",
                table, domain_id, reason
            );
            return Ok(ValidationResult {
                table: table.to_string(),
                rows_checked: 0,
                rows_matched: 0,
                discrepancies: vec![],
                consistency_percentage: 100.0,
                validation_time: Utc::now(),
                source_only: true,
            });
        }

        info!("Starting domain-scoped validation: table={} domain={}", table, domain_id);
        let start = std::time::Instant::now();

        let config = self.config.read().await;
        let sample_rate = config.reader.sample_rate;
        let batch_size = config.reader.batch_size;
        drop(config);

        let result = self.validator.validate_table(
            table,
            sample_rate,
            batch_size,
        ).await?;

        for discrepancy in &result.discrepancies {
            self.discrepancies.insert(discrepancy.id, discrepancy.clone());
        }

        let duration = start.elapsed();
        info!(
            "Domain validation complete: table={} domain={} {}/{} rows matched ({:.2}%) in {:?}",
            table, domain_id,
            result.rows_matched,
            result.rows_checked,
            result.consistency_percentage,
            duration
        );

        metrics::record_operation(
            "validation_domain",
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
                    let source_only_count = results.iter()
                        .filter(|r| r.source_only)
                        .count();
                    
                    info!(
                        "Validation cycle complete: {} tables, {} compared, {} source-only, {} discrepancies",
                        results.len(),
                        results.len() - source_only_count,
                        source_only_count,
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
        
        let reconciliator = self.reconciliator.read().await;
        let result = reconciliator.reconcile(&discrepancy).await?;
        
        if result.success {
            self.discrepancies.remove(&discrepancy_id);
            info!("Reconciliation successful: {:?}", result.action);
        }
        
        Ok(())
    }
    
    pub async fn set_reconciliation_strategy(
        &self,
        strategy: Box<dyn ReconciliationStrategy>
    ) {
        let mut reconciliator = self.reconciliator.write().await;
        reconciliator.set_strategy(strategy);
    }
    
    pub async fn health_check(&self) -> Result<(), SyncError> {
        self.source_conn.get_session()
            .query_unpaged("SELECT now() FROM system.local", &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Source health check failed: {}", e)))?;
        
        self.target_conn.get_session()
            .query_unpaged("SELECT now() FROM system.local", &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Target health check failed: {}", e)))?;
        
        Ok(())
    }

    /// Get filter statistics (for API/status endpoint)
    pub fn get_filter_stats(&self) -> crate::filter::FilterStatsSummary {
        self.filter.get_stats()
    }

    /// Whether filtering is currently active
    pub fn is_filter_enabled(&self) -> bool {
        self.filter.is_enabled()
    }
}

