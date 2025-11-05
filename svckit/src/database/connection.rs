use scylla::{Session, SessionBuilder};
use scylla::transport::session::PoolSize;
use std::sync::Arc;
use tracing::{info, error};

use crate::config::DatabaseConfig;
use crate::errors::SyncError;

/// ScyllaDB connection wrapper with session management
#[derive(Clone)]
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
            .map(|host| format!("{}:{}", host, config.port))
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
            .map_err(|e| {
                error!("Failed to connect to ScyllaDB: {}", e);
                SyncError::DatabaseError(format!("Connection failed: {}", e))
            })?;

        info!("Successfully connected to ScyllaDB cluster");

        Ok(Self {
            session: Arc::new(session),
            config: config.clone(),
        })
    }

    /// Get the underlying session
    pub fn get_session(&self) -> &Session {
        &self.session
    }

    /// Execute a query with values
    pub async fn execute(
        &self,
        query: &str,
        values: Vec<serde_json::Value>,
    ) -> Result<(), SyncError> {
        use scylla::frame::value::Value;

        // Convert JSON values to Scylla values
        let scylla_values: Vec<Value> = values
            .iter()
            .map(|v| json_to_scylla_value(v))
            .collect();

        self.session
            .query(query, scylla_values)
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Query execution failed: {}", e)))?;

        Ok(())
    }

    /// Health check
    pub async fn health_check(&self) -> Result<(), SyncError> {
        self.session
            .query("SELECT now() FROM system.local", &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Health check failed: {}", e)))?;
        Ok(())
    }

    /// Get cluster metadata
    pub async fn get_metadata(&self) -> Result<String, SyncError> {
        let result = self.session
            .query("SELECT cluster_name FROM system.local", &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Failed to get metadata: {}", e)))?;

        Ok(format!("{:?}", result))
    }
}

/// Convert JSON value to Scylla value
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
        serde_json::Value::Array(_) => Value::Null, // Simplified
        serde_json::Value::Object(_) => Value::Null, // Simplified
    }
}

pub trait DatabaseConnection: Send + Sync {
    fn execute(&self, query: &str, values: Vec<serde_json::Value>) -> impl std::future::Future<Output = Result<(), SyncError>> + Send;
}

impl DatabaseConnection for ScyllaConnection {
    async fn execute(&self, query: &str, values: Vec<serde_json::Value>) -> Result<(), SyncError> {
        self.execute(query, values).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_to_scylla_value() {
        let json = serde_json::json!(42);
        let value = json_to_scylla_value(&json);
        assert!(matches!(value, scylla::frame::value::Value::BigInt(42)));
    }
}