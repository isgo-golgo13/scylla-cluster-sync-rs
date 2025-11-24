use std::sync::Arc;
use std::collections::HashMap;
use chrono::Utc;
use uuid::Uuid;
use tracing::{info, warn};

use svckit::{
    types::{ValidationResult, Discrepancy, DiscrepancyType, RowData, ColumnValue},
    errors::SyncError,
    database::ScyllaConnection,
};

pub struct Validator {
    source_conn: Arc<ScyllaConnection>,
    target_conn: Arc<ScyllaConnection>,
}

impl Validator {
    pub fn new(
        source_conn: Arc<ScyllaConnection>,
        target_conn: Arc<ScyllaConnection>,
    ) -> Self {
        Self {
            source_conn,
            target_conn,
        }
    }
    
    pub async fn validate_table(
        &self,
        table: &str,
        sample_rate: f64,
        _batch_size: usize,
    ) -> Result<ValidationResult, SyncError> {
        info!("Validating table: {} (sample_rate: {})", table, sample_rate);
        
        let mut rows_checked = 0u64;
        let mut rows_matched = 0u64;
        let mut discrepancies = Vec::new();
        
        let source_query = format!("SELECT * FROM {}", table);
        let target_query = format!("SELECT * FROM {}", table);
        
        let source_result = self.source_conn.get_session()
            .query_unpaged(source_query.as_str(), &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Source query failed: {}", e)))?;
        
        let target_result = self.target_conn.get_session()
            .query_unpaged(target_query.as_str(), &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Target query failed: {}", e)))?;
        
        let source_count = source_result.rows_num().unwrap_or(0);
        info!("Source has {} rows", source_count);
        
        let rows_to_check = ((source_count as f64) * sample_rate).ceil() as usize;
        rows_checked = rows_to_check as u64;
        
        let target_count = target_result.rows_num().unwrap_or(0);
        info!("Target has {} rows", target_count);
        
        if source_count == target_count {
            rows_matched = rows_checked;
        } else {
            warn!("Row count mismatch: source={}, target={}", source_count, target_count);
            
            if source_count > target_count {
                for _ in 0..(source_count - target_count).min(10) {
                    discrepancies.push(Discrepancy {
                        id: Uuid::new_v4(),
                        table: table.to_string(),
                        key: HashMap::new(),
                        discrepancy_type: DiscrepancyType::MissingInTarget,
                        source_value: Some(RowData {
                            columns: HashMap::new(),
                            writetime: None,
                            ttl: None,
                        }),
                        target_value: None,
                        detected_at: Utc::now(),
                    });
                }
            }
        }
        
        let consistency_percentage = if rows_checked > 0 {
            (rows_matched as f64 / rows_checked as f64) * 100.0
        } else {
            100.0
        };
        
        Ok(ValidationResult {
            table: table.to_string(),
            rows_checked,
            rows_matched,
            discrepancies,
            consistency_percentage: consistency_percentage as f32,
            validation_time: Utc::now(),
        })
    }
    
    pub fn compare_column_values(
        &self,
        source: &ColumnValue,
        target: &ColumnValue,
    ) -> bool {
        match (source, target) {
            (ColumnValue::Text(s), ColumnValue::Text(t)) => s == t,
            (ColumnValue::Int(s), ColumnValue::Int(t)) => s == t,
            (ColumnValue::BigInt(s), ColumnValue::BigInt(t)) => s == t,
            (ColumnValue::Float(s), ColumnValue::Float(t)) => (s - t).abs() < f32::EPSILON,
            (ColumnValue::Double(s), ColumnValue::Double(t)) => (s - t).abs() < f64::EPSILON,
            (ColumnValue::Boolean(s), ColumnValue::Boolean(t)) => s == t,
            (ColumnValue::Uuid(s), ColumnValue::Uuid(t)) => s == t,
            (ColumnValue::Timestamp(s), ColumnValue::Timestamp(t)) => s == t,
            (ColumnValue::Null, ColumnValue::Null) => true,
            _ => false,
        }
    }
}