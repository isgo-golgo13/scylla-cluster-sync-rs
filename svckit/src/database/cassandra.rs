// svckit/src/database/cassandra.rs
//
// Cassandra client using cdrs-tokio driver
// For native Cassandra 4.x protocol support
//
// NOTE: For most use cases, the Scylla driver (scylla crate) works with 
// Cassandra 4.x since both use CQL protocol. This file provides native
// cdrs-tokio support for cases requiring Cassandra-specific features.

use async_trait::async_trait;
use std::sync::Arc;
use tracing::{info, warn};

use crate::errors::SyncError;

// cdrs-tokio imports
use cdrs_tokio::cluster::session::{Session as CdrsSession, SessionBuilder};
use cdrs_tokio::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs_tokio::load_balancing::RoundRobin;
use cdrs_tokio::query::*;
use cdrs_tokio::frame::Frame;

type CassandraSessionType = CdrsSession<RoundRobin<TcpConnectionPool>>;

/// Cassandra-specific database client trait
/// Separate from DatabaseConnection because cdrs-tokio has different types
#[async_trait]
pub trait CassandraDatabase: Send + Sync {
    async fn execute_cql(&self, query: &str) -> Result<Frame, SyncError>;
    async fn execute_cql_with_values(&self, query: &str, values: QueryValues) -> Result<Frame, SyncError>;
    async fn health_check(&self) -> Result<(), SyncError>;
    fn connection_info(&self) -> String;
}

/// Cassandra client for compatibility with legacy Cassandra clusters
/// Uses cdrs-tokio driver for native Cassandra protocol support
#[derive(Clone)]
pub struct CassandraClient {
    session: Arc<CassandraSessionType>,
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
    ) -> Result<Self, SyncError> {
        info!("Connecting to Cassandra cluster: {:?}", contact_points);

        let mut node_configs = Vec::new();
        
        for contact_point in &contact_points {
            let parts: Vec<&str> = contact_point.split(':').collect();
            let host = parts[0];
            let port = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(9042);

            let node_config = NodeTcpConfigBuilder::new()
                .with_contact_point(host.parse().map_err(|e| {
                    SyncError::DatabaseError(format!("Invalid host address {}: {}", host, e))
                })?)
                .with_port(port);

            // Note: Authentication handling would need cdrs-tokio authenticator setup
            // For now, we skip auth config - production would need proper handling
            if username.is_some() && password.is_some() {
                warn!("Cassandra authentication configured but not applied - use ScyllaConnection for auth support");
            }

            node_configs.push(node_config.build());
        }

        let cluster_config = ClusterTcpConfig(node_configs);
        
        let session = SessionBuilder::new()
            .with_cluster_config(cluster_config)
            .build()
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Cassandra connection failed: {}", e)))?;

        // Use keyspace
        session
            .query(format!("USE {}", keyspace))
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Failed to use keyspace: {}", e)))?;

        info!("Successfully connected to Cassandra cluster");

        Ok(Self {
            session: Arc::new(session),
            keyspace,
            contact_points,
        })
    }

    /// Get the underlying cdrs-tokio session
    pub fn session(&self) -> &CassandraSessionType {
        &self.session
    }
    
    /// Get keyspace name
    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }

    /// Execute a query with values
    pub async fn execute_query(
        &self,
        query: &str,
        values: QueryValues,
    ) -> Result<Frame, SyncError> {
        self.session
            .query_with_values(query, values)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Query execution failed: {}", e)))
    }

    /// Execute a simple query without values
    pub async fn execute_simple(&self, query: &str) -> Result<Frame, SyncError> {
        self.session
            .query(query)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Query execution failed: {}", e)))
    }

    /// Prepare a query for efficient execution
    pub async fn prepare_query(&self, query: &str) -> Result<PreparedQuery, SyncError> {
        self.session
            .prepare(query)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Failed to prepare query: {}", e)))
    }

    /// Execute batch operations
    pub async fn execute_batch(
        &self,
        queries: Vec<String>,
    ) -> Result<(), SyncError> {
        use cdrs_tokio::query::batch::Batch;

        let mut batch = Batch::default();
        
        for query in queries {
            batch.add_query(query);
        }

        self.session
            .batch(&batch)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Batch execution failed: {}", e)))?;

        Ok(())
    }

    /// Get cluster metadata
    pub async fn get_cluster_metadata(&self) -> Result<String, SyncError> {
        let query = "SELECT cluster_name FROM system.local";
        let result = self.session
            .query(query)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Failed to get cluster metadata: {}", e)))?;

        Ok(format!("{:?}", result))
    }

    /// Get token ranges for parallel processing (Cassandra-specific)
    pub async fn get_token_ranges(&self) -> Result<Vec<(i64, i64)>, SyncError> {
        // Query system.local and system.peers to get token information
        let query = "SELECT tokens FROM system.local";
        let _result = self.session
            .query(query)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Failed to query tokens: {}", e)))?;

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
impl CassandraDatabase for CassandraClient {
    async fn execute_cql(&self, query: &str) -> Result<Frame, SyncError> {
        self.execute_simple(query).await
    }

    async fn execute_cql_with_values(&self, query: &str, values: QueryValues) -> Result<Frame, SyncError> {
        self.execute_query(query, values).await
    }

    async fn health_check(&self) -> Result<(), SyncError> {
        let query = "SELECT now() FROM system.local";
        self.session
            .query(query)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Health check failed: {}", e)))?;
        
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
    
    #[tokio::test]
    #[ignore] // Requires running Cassandra instance
    async fn test_cassandra_health_check() {
        let client = CassandraClient::new(
            vec!["127.0.0.1:9042".to_string()],
            "system".to_string(),
            None,
            None,
        )
        .await
        .unwrap();

        let result = client.health_check().await;
        assert!(result.is_ok());
    }
}