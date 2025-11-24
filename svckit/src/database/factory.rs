// svckit/src/database/factory.rs
//
// Factory Pattern for runtime database driver selection
// Supports: ScyllaDB, Cassandra 4.x
//

use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::config::DatabaseConfig;
use crate::errors::SyncError;
use super::scylla::{ScyllaConnection, DatabaseConnection};  // <-- Import trait!

/// Supported database drivers
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseDriver {
    #[default]
    Scylla,
    Cassandra,
}

impl From<&str> for DatabaseDriver {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "cassandra" | "cassandra4" | "cass" => DatabaseDriver::Cassandra,
            "scylla" | "scylladb" => DatabaseDriver::Scylla,
            _ => DatabaseDriver::Scylla, // Default to Scylla
        }
    }
}

impl From<String> for DatabaseDriver {
    fn from(s: String) -> Self {
        DatabaseDriver::from(s.as_str())
    }
}

/// Unified database connection trait
#[async_trait]
pub trait UnifiedConnection: Send + Sync {
    async fn execute(&self, query: &str, values: Vec<serde_json::Value>) -> Result<(), SyncError>;
    async fn query(&self, query: &str) -> Result<scylla::QueryResult, SyncError>;
    async fn health_check(&self) -> Result<(), SyncError>;
    fn driver_name(&self) -> &str;
    fn get_session(&self) -> &scylla::Session;
}

/// Database connection factory
pub struct DatabaseFactory;

impl DatabaseFactory {
    /// Create a database connection based on driver type
    pub async fn create(
        driver: DatabaseDriver,
        config: &DatabaseConfig,
    ) -> Result<Arc<dyn UnifiedConnection>, SyncError> {
        info!("Creating database connection with driver: {:?}", driver);
        
        match driver {
            DatabaseDriver::Scylla => {
                let conn = ScyllaConnection::new(config).await?;
                Ok(Arc::new(ScyllaWrapper(conn)))
            }
            DatabaseDriver::Cassandra => {
                // Scylla driver is CQL-compatible with Cassandra 4.x
                // Both use the same CQL binary protocol
                info!("Using Scylla driver for Cassandra 4.x compatibility");
                let conn = ScyllaConnection::new(config).await?;
                Ok(Arc::new(CassandraWrapper(conn)))
            }
        }
    }
    
    /// Create from config (reads driver field from config)
    pub async fn create_from_config(
        config: &DatabaseConfig,
    ) -> Result<Arc<dyn UnifiedConnection>, SyncError> {
        let driver = DatabaseDriver::from(config.driver.as_str());
        Self::create(driver, config).await
    }
    
    /// Create from string driver name
    pub async fn create_from_str(
        driver_name: &str,
        config: &DatabaseConfig,
    ) -> Result<Arc<dyn UnifiedConnection>, SyncError> {
        let driver = DatabaseDriver::from(driver_name);
        Self::create(driver, config).await
    }
}

/// Wrapper for ScyllaConnection implementing UnifiedConnection
struct ScyllaWrapper(ScyllaConnection);

#[async_trait]
impl UnifiedConnection for ScyllaWrapper {
    async fn execute(&self, query: &str, values: Vec<serde_json::Value>) -> Result<(), SyncError> {
        DatabaseConnection::execute(&self.0, query, values).await
    }
    
    async fn query(&self, query: &str) -> Result<scylla::QueryResult, SyncError> {
        self.0.execute_simple(query).await
    }
    
    async fn health_check(&self) -> Result<(), SyncError> {
        self.0.execute_simple("SELECT now() FROM system.local").await?;
        Ok(())
    }
    
    fn driver_name(&self) -> &str {
        "scylla"
    }
    
    fn get_session(&self) -> &scylla::Session {
        self.0.get_session()
    }
}

/// Wrapper for Cassandra (uses Scylla driver for Cassandra 4.x compatibility)
struct CassandraWrapper(ScyllaConnection);

#[async_trait]
impl UnifiedConnection for CassandraWrapper {
    async fn execute(&self, query: &str, values: Vec<serde_json::Value>) -> Result<(), SyncError> {
        DatabaseConnection::execute(&self.0, query, values).await
    }
    
    async fn query(&self, query: &str) -> Result<scylla::QueryResult, SyncError> {
        self.0.execute_simple(query).await
    }
    
    async fn health_check(&self) -> Result<(), SyncError> {
        self.0.execute_simple("SELECT now() FROM system.local").await?;
        Ok(())
    }
    
    fn driver_name(&self) -> &str {
        "cassandra"
    }
    
    fn get_session(&self) -> &scylla::Session {
        self.0.get_session()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_driver_from_string() {
        assert_eq!(DatabaseDriver::from("cassandra"), DatabaseDriver::Cassandra);
        assert_eq!(DatabaseDriver::from("Cassandra4"), DatabaseDriver::Cassandra);
        assert_eq!(DatabaseDriver::from("scylla"), DatabaseDriver::Scylla);
        assert_eq!(DatabaseDriver::from("ScyllaDB"), DatabaseDriver::Scylla);
        assert_eq!(DatabaseDriver::from("unknown"), DatabaseDriver::Scylla); // Default
    }
    
    #[test]
    fn test_driver_from_owned_string() {
        assert_eq!(DatabaseDriver::from("cassandra".to_string()), DatabaseDriver::Cassandra);
        assert_eq!(DatabaseDriver::from("scylla".to_string()), DatabaseDriver::Scylla);
    }
}