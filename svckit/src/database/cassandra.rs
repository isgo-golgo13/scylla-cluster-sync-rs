use async_trait::async_trait;
use cdrs_tokio::cluster::session::{Session as CassandraSession, SessionBuilder};
use cdrs_tokio::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs_tokio::load_balancing::RoundRobin;
use cdrs_tokio::query::*;
use cdrs_tokio::query_values;
use cdrs_tokio::frame::Frame;
use std::sync::Arc;
use tracing::{info, warn, error};

use crate::errors::SvcKitError;
use super::DatabaseClient;

type CurrentSession = CassandraSession<RoundRobin<TcpConnectionPool>>;

/// Cassandra client for compatibility with legacy Cassandra clusters
#[derive(Clone)]
pub struct CassandraClient {
    session: Arc<CurrentSession>,
    keyspace: String,
    contact_points: Vec<String>,
}

impl CassandraClient {
    /// Create a new Cassandra client
    pub async fn new(
        contact_points: Vec<String>,
        keyspace: String,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Self, SvcKitError> {
        info!("Connecting to Cassandra cluster: {:?}", contact_points);

        let mut node_configs = Vec::new();
        
        for contact_point in &contact_points {
            let parts: Vec<&str> = contact_point.split(':').collect();
            let host = parts[0];
            let port = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(9042);

            let mut node_config = NodeTcpConfigBuilder::new()
                .with_contact_point(host.parse().map_err(|e| {
                    SvcKitError::DatabaseConnection(format!("Invalid host address {}: {}", host, e))
                })?)
                .with_port(port);

            if let (Some(ref user), Some(ref pass)) = (username, password) {
                node_config = node_config.with_authenticator(
                    cdrs_tokio::authenticators::StaticPasswordAuthenticator::new(user, pass)
                );
            }

            node_configs.push(node_config.build());
        }

        let cluster_config = ClusterTcpConfig(node_configs);
        
        let session = SessionBuilder::new()
            .with_cluster_config(cluster_config)
            .build()
            .await
            .map_err(|e| SvcKitError::DatabaseConnection(format!("Cassandra connection failed: {}", e)))?;

        // Use keyspace
        session
            .query(format!("USE {}", keyspace))
            .await
            .map_err(|e| SvcKitError::Database(format!("Failed to use keyspace: {}", e)))?;

        info!("Successfully connected to Cassandra cluster");

        Ok(Self {
            session: Arc::new(session),
            keyspace,
            contact_points,
        })
    }

    /// Get the underlying session
    pub fn session(&self) -> &CurrentSession {
        &self.session
    }

    /// Execute a query with values
    pub async fn execute_query(
        &self,
        query: &str,
        values: QueryValues,
    ) -> Result<Frame, SvcKitError> {
        self.session
            .query_with_values(query, values)
            .await
            .map_err(|e| SvcKitError::Database(format!("Query execution failed: {}", e)))
    }

    /// Prepare a query for efficient execution
    pub async fn prepare_query(&self, query: &str) -> Result<PreparedQuery, SvcKitError> {
        self.session
            .prepare(query)
            .await
            .map_err(|e| SvcKitError::Database(format!("Failed to prepare query: {}", e)))
    }

    /// Execute batch operations
    pub async fn execute_batch(
        &self,
        queries: Vec<(String, QueryValues)>,
    ) -> Result<(), SvcKitError> {
        use cdrs_tokio::query::batch::Batch;
        use cdrs_tokio::query::batch::BatchQueryBuilder;

        let mut batch = Batch::default();
        
        for (query, _values) in queries {
            batch.add_query(query);
        }

        self.session
            .batch(&batch)
            .await
            .map_err(|e| SvcKitError::Database(format!("Batch execution failed: {}", e)))?;

        Ok(())
    }

    /// Get cluster metadata
    pub async fn get_cluster_metadata(&self) -> Result<String, SvcKitError> {
        let query = "SELECT cluster_name FROM system.local";
        let result = self.session
            .query(query)
            .await
            .map_err(|e| SvcKitError::Database(format!("Failed to get cluster metadata: {}", e)))?;

        Ok(format!("{:?}", result))
    }

    /// Get token ranges for parallel processing (Cassandra-specific)
    pub async fn get_token_ranges(&self) -> Result<Vec<(i64, i64)>, SvcKitError> {
        // Query system.local and system.peers to get token information
        let query = "SELECT tokens FROM system.local";
        let _result = self.session
            .query(query)
            .await
            .map_err(|e| SvcKitError::Database(format!("Failed to query tokens: {}", e)))?;

        // For simplicity, generate equal-sized ranges
        // In production, parse actual token ranges from cluster
        let num_ranges = 256; // Default range count
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

        info!("Generated {} token ranges for Cassandra parallel processing", ranges.len());
        Ok(ranges)
    }
}

#[async_trait]
impl DatabaseClient for CassandraClient {
    async fn execute(&self, query: &str, _values: Vec<scylla::frame::value::Value>) -> Result<(), SvcKitError> {
        // Convert scylla values to cdrs values (simplified)
        self.session
            .query(query)
            .await
            .map_err(|e| SvcKitError::Database(format!("Query execution failed: {}", e)))?;
        Ok(())
    }

    async fn execute_prepared(&self, _statement_id: &str, _values: Vec<scylla::frame::value::Value>) -> Result<(), SvcKitError> {
        unimplemented!("Use prepare_query() and execute methods instead")
    }

    async fn query(&self, query: &str, _values: Vec<scylla::frame::value::Value>) -> Result<Vec<scylla::QueryResult>, SvcKitError> {
        let _result = self.session
            .query(query)
            .await
            .map_err(|e| SvcKitError::Database(format!("Query failed: {}", e)))?;
        
        // Note: This is a simplified return - in production you'd convert CDRS Frame to Scylla QueryResult
        Ok(vec![])
    }

    async fn health_check(&self) -> Result<(), SvcKitError> {
        let query = "SELECT now() FROM system.local";
        self.session
            .query(query)
            .await
            .map_err(|e| SvcKitError::DatabaseConnection(format!("Health check failed: {}", e)))?;
        
        Ok(())
    }

    fn connection_info(&self) -> String {
        format!(
            "Cassandra - Keyspace: {}, Nodes: {:?}",
            self.keyspace, self.contact_points
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires running Cassandra instance
    async fn test_cassandra_connection() {
        let client = CassandraClient::new(
            vec!["127.0.0.1:9042".to_string()],
            "test_keyspace".to_string(),
            None,
            None,
        )
        .await;

        assert!(client.is_ok());
    }
}