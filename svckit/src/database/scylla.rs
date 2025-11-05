use std::sync::Arc;
use scylla::{Session, SessionBuilder};
use scylla::transport::session::PoolSize;
use tracing::{info, error};

use crate::config::DatabaseConfig;
use crate::errors::SyncError;

/// Trait for database connections (ScyllaDB/Cassandra)
#[async_trait::async_trait]
pub trait DatabaseConnection: Send + Sync {
    async fn execute(&self, query: &str, values: Vec<serde_json::Value>) -> Result<(), SyncError>;
    async fn query(&self, query: &str, values: Vec<serde_json::Value>) -> Result<Vec<scylla::QueryResult>, SyncError>;
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

        let mut session_builder = SessionBuilder::new()
            .known_nodes(&contact_points)
            .pool_size(PoolSize::PerShard(config.pool_size as usize))
            .use_keyspace(&config.keyspace, true);

        // Add authentication if provided
        if let (Some(ref username), Some(ref password)) = (&config.username, &config.password) {
            session_builder = session_builder.user(username, password);
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

    /// Execute a batch of queries
    pub async fn batch_execute(
        &self,
        queries: Vec<(String, Vec<serde_json::Value>)>,
    ) -> Result<(), SyncError> {
        use scylla::batch::Batch;

        let mut batch = Batch::default();
        
        for (query, _) in &queries {
            batch.append_statement(query.as_str());
        }

        // Convert JSON values to Scylla values
        let values: Vec<Vec<scylla::frame::value::Value>> = queries
            .into_iter()
            .map(|(_, vals)| {
                vals.iter()
                    .map(Self::json_to_scylla_value)
                    .collect()
            })
            .collect();

        self.session
            .batch(&batch, values)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Batch execution failed: {}", e)))?;

        Ok(())
    }

    /// Get token ranges for parallel processing
    pub async fn get_token_ranges(&self) -> Result<Vec<(i64, i64)>, SyncError> {
        // Query system tables to get actual token ranges
        let query = "SELECT tokens FROM system.local";
        let result = self.session
            .query(query, &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Failed to query token ranges: {}", e)))?;

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
        let query = "SELECT peer FROM system.peers";
        let result = self.session
            .query(query, &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Failed to query peers: {}", e)))?;

        let num_nodes = result.rows.as_ref().map(|r| r.len()).unwrap_or(0) + 1; // +1 for local node
        
        // Heuristic: 4 ranges per core, assume 8 cores per node
        let optimal_ranges = num_nodes * 8 * 4;
        
        info!("Estimated {} optimal ranges for {} nodes", optimal_ranges, num_nodes);
        Ok(optimal_ranges.max(256)) // Minimum 256 ranges
    }

    /// Convert JSON value to Scylla CQL value
    fn json_to_scylla_value(value: &serde_json::Value) -> scylla::frame::value::Value {
        use scylla::frame::value::Value;
        
        match value {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Boolean(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::BigInt(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Double(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::Text(s.clone()),
            serde_json::Value::Array(_) => {
                // Simplified: arrays need proper type handling
                Value::Null
            }
            serde_json::Value::Object(_) => {
                // Simplified: objects need proper type handling
                Value::Null
            }
        }
    }

    /// Convert Scylla values to JSON
    fn scylla_value_to_json(value: &scylla::frame::value::Value) -> serde_json::Value {
        use scylla::frame::value::Value;
        
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Boolean(b) => serde_json::Value::Bool(*b),
            Value::BigInt(i) => serde_json::json!(i),
            Value::Int(i) => serde_json::json!(i),
            Value::Double(f) => serde_json::json!(f),
            Value::Float(f) => serde_json::json!(f),
            Value::Text(s) => serde_json::Value::String(s.clone()),
            _ => serde_json::Value::Null,
        }
    }
}

#[async_trait::async_trait]
impl DatabaseConnection for ScyllaConnection {
    async fn execute(&self, query: &str, values: Vec<serde_json::Value>) -> Result<(), SyncError> {
        let scylla_values: Vec<scylla::frame::value::Value> = values
            .iter()
            .map(Self::json_to_scylla_value)
            .collect();

        self.session
            .query(query, scylla_values)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Query execution failed: {}", e)))?;

        Ok(())
    }

    async fn query(&self, query: &str, values: Vec<serde_json::Value>) -> Result<Vec<scylla::QueryResult>, SyncError> {
        let scylla_values: Vec<scylla::frame::value::Value> = values
            .iter()
            .map(Self::json_to_scylla_value)
            .collect();

        let result = self.session
            .query(query, scylla_values)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Query failed: {}", e)))?;

        Ok(vec![result])
    }

    fn get_session(&self) -> &Session {
        &self.session
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_to_scylla_value() {
        let json_int = serde_json::json!(42);
        let val = ScyllaConnection::json_to_scylla_value(&json_int);
        assert!(matches!(val, scylla::frame::value::Value::BigInt(42)));
    }
}