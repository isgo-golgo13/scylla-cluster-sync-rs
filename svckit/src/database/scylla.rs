use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use scylla::{Session, SessionBuilder, QueryResult};
use scylla::transport::session::PoolSize;
use scylla::transport::execution_profile::ExecutionProfile;
use scylla::speculative_execution::SimpleSpeculativeExecutionPolicy;
use scylla::serialize::row::SerializeRow;
use tracing::info;

use crate::config::DatabaseConfig;
use crate::errors::SyncError;

/// Trait for database connections (ScyllaDB/Cassandra)
#[async_trait::async_trait]
pub trait DatabaseConnection: Send + Sync {
    async fn execute(&self, query: &str, values: Vec<serde_json::Value>) -> Result<(), SyncError>;
    async fn query(&self, query: &str, values: Vec<serde_json::Value>) -> Result<QueryResult, SyncError>;
    fn get_session(&self) -> &Session;
}

/// ScyllaDB connection wrapper
pub struct ScyllaConnection {
    session: Arc<Session>,
    config: DatabaseConfig,
}

impl ScyllaConnection {
    /// Create a new ScyllaDB connection
    pub async fn new(config: &DatabaseConfig) -> Result<Self, SyncError> {
        info!("Connecting to ScyllaDB cluster: {:?}", config.hosts);

        let contact_points: Vec<String> = config
            .hosts
            .iter()
            .map(|h| format!("{}:{}", h, config.port))
            .collect();

        let pool_size = NonZeroUsize::new(config.pool_size as usize)
            .unwrap_or(NonZeroUsize::new(4).unwrap());

        let mut session_builder = SessionBuilder::new()
            .known_nodes(&contact_points)
            .pool_size(PoolSize::PerShard(pool_size))
            .use_keyspace(&config.keyspace, true);

        // Add authentication if provided
        if let (Some(ref username), Some(ref password)) = (&config.username, &config.password) {
            session_builder = session_builder.user(username, password);
        }
        
        // Add speculative execution via ExecutionProfile if enabled (for GC pause resilience)
        if config.speculative_execution {
            info!("Speculative execution enabled (delay: {}ms)", config.speculative_delay_ms);
            let policy = SimpleSpeculativeExecutionPolicy {
                max_retry_count: 2,
                retry_interval: Duration::from_millis(config.speculative_delay_ms),
            };
            let profile = ExecutionProfile::builder()
                .speculative_execution_policy(Some(Arc::new(policy)))
                .build();
            let handle = profile.into_handle();
            session_builder = session_builder.default_execution_profile_handle(handle);
        }

        let session = session_builder
            .build()
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Failed to connect to ScyllaDB: {}", e)))?;

        info!("Successfully connected to ScyllaDB keyspace: {}", config.keyspace);

        Ok(Self {
            session: Arc::new(session),
            config: config.clone(),
        })
    }

    /// Get the underlying Scylla session
    pub fn get_session(&self) -> &Session {
        &self.session
    }

    /// Prepare a statement for efficient repeated execution
    pub async fn prepare(&self, query: &str) -> Result<scylla::prepared_statement::PreparedStatement, SyncError> {
        self.session
            .prepare(query)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Failed to prepare statement: {}", e)))
    }

    /// Execute a simple query without values
    pub async fn execute_simple(&self, query: &str) -> Result<QueryResult, SyncError> {
        self.session
            .query_unpaged(query, ())
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Query execution failed: {}", e)))
    }

    /// Execute a query with serializable values
    pub async fn execute_with_values<V: SerializeRow>(
        &self,
        query: &str,
        values: V,
    ) -> Result<QueryResult, SyncError> {
        self.session
            .query_unpaged(query, values)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Query execution failed: {}", e)))
    }

    /// Get token ranges for parallel processing
    pub async fn get_token_ranges(&self) -> Result<Vec<(i64, i64)>, SyncError> {
        // Query system tables to get actual token ranges
        let _result = self.execute_simple("SELECT tokens FROM system.local").await?;

        // For simplicity, generate equal-sized token ranges
        // In production, parse actual token ownership from cluster
        let num_ranges = self.estimate_optimal_ranges().await?;
        let min_token: i64 = i64::MIN;
        let max_token: i64 = i64::MAX;

        let range_size = (max_token as i128 - min_token as i128) / num_ranges as i128;
        let mut ranges = Vec::new();

        for i in 0..num_ranges {
            let start = min_token as i128 + (i as i128 * range_size);
            let end = if i == num_ranges - 1 {
                max_token as i128
            } else {
                start + range_size - 1
            };

            ranges.push((start as i64, end as i64));
        }

        info!("Generated {} token ranges for parallel processing", ranges.len());
        Ok(ranges)
    }

    /// Estimate optimal number of ranges based on cluster topology
    async fn estimate_optimal_ranges(&self) -> Result<usize, SyncError> {
        let result = self.execute_simple("SELECT peer FROM system.peers").await?;

        let num_peers = result.rows_num().unwrap_or(0);
        let num_nodes = num_peers + 1; // +1 for local node
        
        // Heuristic: 4 ranges per core, assume 8 cores per node
        let optimal_ranges = num_nodes * 8 * 4;
        
        info!("Estimated {} optimal ranges for {} nodes", optimal_ranges, num_nodes);
        Ok(optimal_ranges.max(256)) // Minimum 256 ranges
    }
}

#[async_trait::async_trait]
impl DatabaseConnection for ScyllaConnection {
    async fn execute(&self, query: &str, values: Vec<serde_json::Value>) -> Result<(), SyncError> {
        if values.is_empty() {
            self.execute_simple(query).await?;
        } else {
            // For complex values, serialize to JSON and use JSON insert
            let json_str = serde_json::to_string(&values)
                .map_err(|e| SyncError::DatabaseError(format!("JSON serialization failed: {}", e)))?;
            self.session
                .query_unpaged(query, (json_str,))
                .await
                .map_err(|e| SyncError::DatabaseError(format!("Query execution failed: {}", e)))?;
        }
        Ok(())
    }

    async fn query(&self, query: &str, values: Vec<serde_json::Value>) -> Result<QueryResult, SyncError> {
        if values.is_empty() {
            self.execute_simple(query).await
        } else {
            let json_str = serde_json::to_string(&values)
                .map_err(|e| SyncError::DatabaseError(format!("JSON serialization failed: {}", e)))?;
            self.session
                .query_unpaged(query, (json_str,))
                .await
                .map_err(|e| SyncError::DatabaseError(format!("Query execution failed: {}", e)))
        }
    }

    fn get_session(&self) -> &Session {
        &self.session
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nonzero_pool_size() {
        let pool_size = NonZeroUsize::new(4).unwrap();
        assert_eq!(pool_size.get(), 4);
    }
}