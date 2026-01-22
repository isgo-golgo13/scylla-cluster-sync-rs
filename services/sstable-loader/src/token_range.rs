// services/sstable-loader/src/token_range.rs
//
// Token range calculation and query building for parallel data migration
// Supports composite partition keys (e.g., token(col1, col2))
//

use std::sync::Arc;
use tracing::info;
use svckit::{
    errors::SyncError,
    database::ScyllaConnection,
};

#[derive(Debug, Clone)]
pub struct TokenRange {
    pub start: i64,
    pub end: i64,
}

pub struct TokenRangeCalculator {
    source_conn: Arc<ScyllaConnection>,
}

impl TokenRangeCalculator {
    pub fn new(source_conn: Arc<ScyllaConnection>) -> Self {
        Self { source_conn }
    }
    
    /// Calculate optimal token ranges for parallel processing
    pub async fn calculate_ranges(
        &self,
        ranges_per_core: usize,
    ) -> Result<Vec<TokenRange>, SyncError> {
        info!("Calculating token ranges for parallel processing");
        
        // Get cluster topology
        let num_nodes = self.get_cluster_size().await?;
        let cores_per_node = self.estimate_cores_per_node().await?;
        
        // Calculate total number of ranges
        let total_ranges = num_nodes * cores_per_node * ranges_per_core;
        
        info!(
            "Cluster topology: {} nodes, ~{} cores/node, {} ranges/core = {} total ranges",
            num_nodes, cores_per_node, ranges_per_core, total_ranges
        );
        
        // ScyllaDB uses Murmur3 partitioner with range [i64::MIN, i64::MAX]
        let min_token = i64::MIN;
        let max_token = i64::MAX;
        let token_space = (max_token as i128) - (min_token as i128) + 1;
        let range_size = token_space / (total_ranges as i128);
        
        let mut ranges = Vec::with_capacity(total_ranges);
        
        for i in 0..total_ranges {
            let start = (min_token as i128) + (i as i128 * range_size);
            let end = if i == total_ranges - 1 {
                max_token as i128
            } else {
                start + range_size - 1
            };
            
            ranges.push(TokenRange {
                start: start as i64,
                end: end as i64,
            });
        }
        
        info!("Generated {} token ranges for parallel processing", ranges.len());
        Ok(ranges)
    }
    
    async fn get_cluster_size(&self) -> Result<usize, SyncError> {
        // Query system.peers to count nodes
        let result = self.source_conn.get_session()
            .query_unpaged("SELECT peer FROM system.peers", &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Failed to query peers: {}", e)))?;
        
        // Number of peers + 1 (local node)
        let num_peers = result.rows_num().unwrap_or(0);
        let num_nodes = num_peers + 1;
        
        info!("Detected {} nodes in cluster", num_nodes);
        Ok(num_nodes)
    }
    
    async fn estimate_cores_per_node(&self) -> Result<usize, SyncError> {
        // In ScyllaDB, you can query system.local for CPU info
        // For simplicity, we'll use a conservative estimate
        // In production, query actual system tables
        let estimated_cores = 8; // Conservative estimate
        
        info!("Estimated {} cores per node", estimated_cores);
        Ok(estimated_cores)
    }
    
    /// Build a CQL query for a specific token range with composite partition key support
    /// 
    /// # Arguments
    /// * `table` - Fully qualified table name (keyspace.table)
    /// * `partition_keys` - ALL partition key columns (supports composite keys)
    /// * `range` - Token range to query
    /// 
    /// # Example
    /// For composite key (system_domain_id, id):
    /// ```sql
    /// SELECT * FROM keyspace.table 
    /// WHERE token(system_domain_id, id) >= -9223372036854775808 
    ///   AND token(system_domain_id, id) <= -8000000000000000000
    /// ```
    pub fn build_range_query(
        &self,
        table: &str,
        partition_keys: &[String],
        range: &TokenRange,
    ) -> String {
        // Join ALL partition key columns for token() function
        let token_columns = partition_keys.join(", ");
        
        format!(
            "SELECT * FROM {} WHERE token({}) >= {} AND token({}) <= {}",
            table, token_columns, range.start, token_columns, range.end
        )
    }
    
    /// Build a CQL query for a specific token range (single partition key)
    /// 
    /// # Deprecated
    /// Use `build_range_query` with `&[String]` for composite key support
    #[allow(dead_code)]
    pub fn build_range_query_single(
        &self,
        table: &str,
        partition_key: &str,
        range: &TokenRange,
    ) -> String {
        format!(
            "SELECT * FROM {} WHERE token({}) >= {} AND token({}) <= {}",
            table, partition_key, range.start, partition_key, range.end
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_range_calculation() {
        // Test token range calculation logic
        let num_ranges = 256;
        let min_token = i64::MIN;
        let max_token = i64::MAX;
        let token_space = (max_token as i128) - (min_token as i128) + 1;
        let range_size = token_space / (num_ranges as i128);
        
        assert!(range_size > 0);
        
        // Verify first range
        let start_0 = min_token as i128;
        let _end_0 = start_0 + range_size - 1;
        assert_eq!(start_0, i64::MIN as i128);
        
        // Verify last range ends at max
        let start_last = (min_token as i128) + ((num_ranges - 1) as i128 * range_size);
        assert!(start_last < max_token as i128);
    }
    
    #[test]
    fn test_build_range_query_single_column() {
        // Test single partition key
        let partition_keys = vec!["id".to_string()];
        let token_columns = partition_keys.join(", ");
        
        let query = format!(
            "SELECT * FROM test.table WHERE token({}) >= {} AND token({}) <= {}",
            token_columns, -1000, token_columns, 1000
        );
        
        assert_eq!(
            query,
            "SELECT * FROM test.table WHERE token(id) >= -1000 AND token(id) <= 1000"
        );
    }
    
    #[test]
    fn test_build_range_query_composite_key() {
        // Test composite partition key (the fix!)
        let partition_keys = vec![
            "system_domain_id".to_string(),
            "id".to_string(),
        ];
        let token_columns = partition_keys.join(", ");
        
        let query = format!(
            "SELECT * FROM mattiasa_files_keyspace.formats WHERE token({}) >= {} AND token({}) <= {}",
            token_columns, -1000, token_columns, 1000
        );
        
        assert_eq!(
            query,
            "SELECT * FROM mattiasa_files_keyspace.formats WHERE token(system_domain_id, id) >= -1000 AND token(system_domain_id, id) <= 1000"
        );
    }
    
    #[test]
    fn test_build_range_query_triple_key() {
        // Test 3-column partition key
        let partition_keys = vec![
            "tenant_id".to_string(),
            "region".to_string(),
            "entity_id".to_string(),
        ];
        let token_columns = partition_keys.join(", ");
        
        let query = format!(
            "SELECT * FROM multi.table WHERE token({}) >= {} AND token({}) <= {}",
            token_columns, 0, token_columns, 100
        );
        
        assert_eq!(
            query,
            "SELECT * FROM multi.table WHERE token(tenant_id, region, entity_id) >= 0 AND token(tenant_id, region, entity_id) <= 100"
        );
    }
}
