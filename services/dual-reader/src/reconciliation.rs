use async_trait::async_trait;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};

use svckit::{
    types::{Discrepancy, DiscrepancyType, RowData},
    errors::SyncError,
    database::ScyllaConnection,
};

#[async_trait]
pub trait ReconciliationStrategy: Send + Sync {
    async fn reconcile(
        &self,
        discrepancy: &Discrepancy,
        source: &ScyllaConnection,
        target: &ScyllaConnection,
    ) -> Result<ReconciliationResult, SyncError>;
    
    fn name(&self) -> &str;
}

pub struct SourceAuthoritativeStrategy;

#[async_trait]
impl ReconciliationStrategy for SourceAuthoritativeStrategy {
    async fn reconcile(
        &self,
        discrepancy: &Discrepancy,
        _source: &ScyllaConnection,
        target: &ScyllaConnection,
    ) -> Result<ReconciliationResult, SyncError> {
        match discrepancy.discrepancy_type {
            DiscrepancyType::MissingInTarget => {
                if let Some(source_data) = &discrepancy.source_value {
                    copy_row_to_target(target, &discrepancy.table, source_data).await?;
                    Ok(ReconciliationResult {
                        success: true,
                        action: ReconciliationAction::CopiedToTarget,
                        rows_affected: 1,
                    })
                } else {
                    Ok(ReconciliationResult::no_action())
                }
            }
            DiscrepancyType::DataMismatch => {
                if let Some(source_data) = &discrepancy.source_value {
                    copy_row_to_target(target, &discrepancy.table, source_data).await?;
                    Ok(ReconciliationResult {
                        success: true,
                        action: ReconciliationAction::CopiedToTarget,
                        rows_affected: 1,
                    })
                } else {
                    Ok(ReconciliationResult::no_action())
                }
            }
            DiscrepancyType::MissingInSource => {
                warn!("Row exists in target but not source - unexpected in migration");
                Ok(ReconciliationResult::no_action())
            }
            _ => Ok(ReconciliationResult::no_action()),
        }
    }
    
    fn name(&self) -> &str {
        "SourceAuthoritative"
    }
}

pub struct NewestTimestampStrategy;

#[async_trait]
impl ReconciliationStrategy for NewestTimestampStrategy {
    async fn reconcile(
        &self,
        discrepancy: &Discrepancy,
        source: &ScyllaConnection,
        target: &ScyllaConnection,
    ) -> Result<ReconciliationResult, SyncError> {
        match discrepancy.discrepancy_type {
            DiscrepancyType::TimestampMismatch => {
                if let (Some(src), Some(tgt)) = (&discrepancy.source_value, &discrepancy.target_value) {
                    if src.writetime.unwrap_or(0) > tgt.writetime.unwrap_or(0) {
                        copy_row_to_target(target, &discrepancy.table, src).await?;
                        Ok(ReconciliationResult {
                            success: true,
                            action: ReconciliationAction::CopiedToTarget,
                            rows_affected: 1,
                        })
                    } else {
                        copy_row_to_target(source, &discrepancy.table, tgt).await?;
                        Ok(ReconciliationResult {
                            success: true,
                            action: ReconciliationAction::CopiedToSource,
                            rows_affected: 1,
                        })
                    }
                } else {
                    Ok(ReconciliationResult::no_action())
                }
            }
            _ => Ok(ReconciliationResult::no_action()),
        }
    }
    
    fn name(&self) -> &str {
        "NewestTimestamp"
    }
}

pub struct ManualReviewStrategy;

#[async_trait]
impl ReconciliationStrategy for ManualReviewStrategy {
    async fn reconcile(
        &self,
        discrepancy: &Discrepancy,
        _source: &ScyllaConnection,
        _target: &ScyllaConnection,
    ) -> Result<ReconciliationResult, SyncError> {
        warn!("Discrepancy flagged for manual review: {:?}", discrepancy.id);
        Ok(ReconciliationResult {
            success: true,
            action: ReconciliationAction::ManualReviewRequired,
            rows_affected: 0,
        })
    }
    
    fn name(&self) -> &str {
        "ManualReview"
    }
}

pub struct Reconciliator {
    strategy: Box<dyn ReconciliationStrategy>,
    source: Arc<ScyllaConnection>,
    target: Arc<ScyllaConnection>,
}

impl Reconciliator {
    pub fn new(
        strategy: Box<dyn ReconciliationStrategy>,
        source: Arc<ScyllaConnection>,
        target: Arc<ScyllaConnection>,
    ) -> Self {
        info!("Reconciliator initialized with strategy: {}", strategy.name());
        Self { strategy, source, target }
    }
    
    pub fn set_strategy(&mut self, strategy: Box<dyn ReconciliationStrategy>) {
        info!("Switching reconciliation strategy to: {}", strategy.name());
        self.strategy = strategy;
    }
    
    pub async fn reconcile(
        &self,
        discrepancy: &Discrepancy,
    ) -> Result<ReconciliationResult, SyncError> {
        let start = Instant::now();
        
        let result = self.strategy
            .reconcile(discrepancy, &self.source, &self.target)
            .await?;
        
        info!(
            "Reconciliation completed: {:?} in {:?}",
            result.action,
            start.elapsed()
        );
        
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct ReconciliationResult {
    pub success: bool,
    pub action: ReconciliationAction,
    pub rows_affected: u64,
}

impl ReconciliationResult {
    pub fn no_action() -> Self {
        Self {
            success: true,
            action: ReconciliationAction::NoActionNeeded,
            rows_affected: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReconciliationAction {
    CopiedToTarget,
    CopiedToSource,
    ManualReviewRequired,
    NoActionNeeded,
}

async fn copy_row_to_target(
    target: &ScyllaConnection,
    table: &str,
    _row_data: &RowData,
) -> Result<(), SyncError> {
    let query = format!("INSERT INTO {} JSON ?", table);
    target.get_session()
        .query_unpaged(query.as_str(), &[])
        .await
        .map_err(|e| SyncError::DatabaseError(format!("Failed to copy row: {}", e)))?;
    Ok(())
}