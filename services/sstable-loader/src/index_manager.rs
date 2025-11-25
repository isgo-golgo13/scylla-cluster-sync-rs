// services/sstable-loader/src/index_manager.rs
//
// IndexManager - Strategy Pattern for secondary index lifecycle
// Handles drop, rebuild, and verification of 60+ secondary indexes
//

use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn, error};
use serde::{Deserialize, Serialize};

use svckit::{
    errors::SyncError,
    database::ScyllaConnection,
};

/// Index information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub keyspace: String,
    pub table: String,
    pub index_name: String,
    pub column_name: String,
    pub index_type: IndexType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum IndexType {
    Secondary,
    Custom,
}

/// Index operation strategy
#[async_trait]
pub trait IndexStrategy: Send + Sync {
    async fn drop_indexes(&self, indexes: &[IndexInfo]) -> Result<Vec<String>, SyncError>;
    async fn rebuild_indexes(&self, indexes: &[IndexInfo]) -> Result<(), SyncError>;
    async fn verify_indexes(&self, indexes: &[IndexInfo]) -> Result<bool, SyncError>;
    fn name(&self) -> &str;
}

/// Standard index strategy - drop before load, rebuild after
pub struct StandardIndexStrategy {
    connection: Arc<ScyllaConnection>,
}

impl StandardIndexStrategy {
    pub fn new(connection: Arc<ScyllaConnection>) -> Self {
        Self { connection }
    }
}

#[async_trait]
impl IndexStrategy for StandardIndexStrategy {
    async fn drop_indexes(&self, indexes: &[IndexInfo]) -> Result<Vec<String>, SyncError> {
        info!("Dropping {} secondary indexes before SSTable load", indexes.len());
        let mut dropped = Vec::new();
        
        for index in indexes {
            let drop_query = format!(
                "DROP INDEX IF EXISTS {}.{}",
                index.keyspace,
                index.index_name
            );
            
            match self.connection.execute_simple(&drop_query).await {
                Ok(_) => {
                    info!("Dropped index: {}.{}", index.keyspace, index.index_name);
                    dropped.push(index.index_name.clone());
                }
                Err(e) => {
                    error!("Failed to drop index {}: {}", index.index_name, e);
                    // Continue with other indexes
                }
            }
        }
        
        info!("Dropped {}/{} indexes successfully", dropped.len(), indexes.len());
        Ok(dropped)
    }
    
    async fn rebuild_indexes(&self, indexes: &[IndexInfo]) -> Result<(), SyncError> {
        info!("Rebuilding {} secondary indexes after SSTable load", indexes.len());
        let start = Instant::now();
        
        for (i, index) in indexes.iter().enumerate() {
            let create_query = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})",
                index.index_name,
                index.keyspace,
                index.table,
                index.column_name
            );
            
            info!("Rebuilding index {}/{}: {}", i + 1, indexes.len(), index.index_name);
            
            match self.connection.execute_simple(&create_query).await {
                Ok(_) => {
                    info!("Rebuilt index: {}.{}", index.keyspace, index.index_name);
                }
                Err(e) => {
                    error!("Failed to rebuild index {}: {}", index.index_name, e);
                    return Err(SyncError::MigrationError(
                        format!("Index rebuild failed: {}", e)
                    ));
                }
            }
        }
        
        let duration = start.elapsed();
        info!(
            "Index rebuild complete: {} indexes in {:?} ({:.2} sec/index avg)",
            indexes.len(),
            duration,
            duration.as_secs_f64() / indexes.len() as f64
        );
        
        Ok(())
    }
    
    async fn verify_indexes(&self, indexes: &[IndexInfo]) -> Result<bool, SyncError> {
        info!("Verifying {} indexes", indexes.len());
        
        for index in indexes {
            let query = format!(
                "SELECT index_name FROM system_schema.indexes WHERE keyspace_name = '{}' AND table_name = '{}' AND index_name = '{}'",
                index.keyspace,
                index.table,
                index.index_name
            );
            
            let result = self.connection.execute_simple(&query).await?;
            
            if result.rows_num().unwrap_or(0) == 0 {
                warn!("Index not found: {}.{}", index.keyspace, index.index_name);
                return Ok(false);
            }
        }
        
        info!("All {} indexes verified", indexes.len());
        Ok(true)
    }
    
    fn name(&self) -> &str {
        "StandardIndexStrategy"
    }
}

/// Parallel index rebuild strategy - rebuilds indexes concurrently
pub struct ParallelIndexStrategy {
    connection: Arc<ScyllaConnection>,
    max_concurrent: usize,
}

impl ParallelIndexStrategy {
    pub fn new(connection: Arc<ScyllaConnection>, max_concurrent: usize) -> Self {
        Self {
            connection,
            max_concurrent,
        }
    }
}

#[async_trait]
impl IndexStrategy for ParallelIndexStrategy {
    async fn drop_indexes(&self, indexes: &[IndexInfo]) -> Result<Vec<String>, SyncError> {
        // Same as standard strategy
        let standard = StandardIndexStrategy::new(self.connection.clone());
        standard.drop_indexes(indexes).await
    }
    
    async fn rebuild_indexes(&self, indexes: &[IndexInfo]) -> Result<(), SyncError> {
        info!("Rebuilding {} indexes in parallel (max {} concurrent)", 
              indexes.len(), self.max_concurrent);
        
        let start = Instant::now();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(self.max_concurrent));
        let mut tasks = vec![];
        
        for index in indexes {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let connection = self.connection.clone();
            let index_clone = index.clone();
            
            let task = tokio::spawn(async move {
                let create_query = format!(
                    "CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})",
                    index_clone.index_name,
                    index_clone.keyspace,
                    index_clone.table,
                    index_clone.column_name
                );
                
                let result = connection.execute_simple(&create_query).await;
                drop(permit);
                
                match result {
                    Ok(_) => {
                        info!("Rebuilt index: {}", index_clone.index_name);
                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to rebuild index {}: {}", index_clone.index_name, e);
                        Err(e)
                    }
                }
            });
            
            tasks.push(task);
        }
        
        // Wait for all tasks
        let mut errors = 0;
        for task in tasks {
            if let Err(e) = task.await {
                error!("Index rebuild task failed: {}", e);
                errors += 1;
            }
        }
        
        if errors > 0 {
            return Err(SyncError::MigrationError(
                format!("{} index rebuilds failed", errors)
            ));
        }
        
        let duration = start.elapsed();
        info!("Parallel index rebuild complete: {} indexes in {:?}", indexes.len(), duration);
        
        Ok(())
    }
    
    async fn verify_indexes(&self, indexes: &[IndexInfo]) -> Result<bool, SyncError> {
        let standard = StandardIndexStrategy::new(self.connection.clone());
        standard.verify_indexes(indexes).await
    }
    
    fn name(&self) -> &str {
        "ParallelIndexStrategy"
    }
}

/// IndexManager - orchestrates index lifecycle
pub struct IndexManager {
    strategy: Box<dyn IndexStrategy>,
    indexes: Vec<IndexInfo>,
}

impl IndexManager {
    pub fn new(strategy: Box<dyn IndexStrategy>, indexes: Vec<IndexInfo>) -> Self {
        info!("IndexManager initialized with {} indexes using strategy: {}", 
              indexes.len(), strategy.name());
        Self { strategy, indexes }
    }
    
    /// Drop all indexes before migration
    pub async fn drop_all(&self) -> Result<Vec<String>, SyncError> {
        self.strategy.drop_indexes(&self.indexes).await
    }
    
    /// Rebuild all indexes after migration
    pub async fn rebuild_all(&self) -> Result<(), SyncError> {
        self.strategy.rebuild_indexes(&self.indexes).await
    }
    
    /// Verify all indexes exist
    pub async fn verify_all(&self) -> Result<bool, SyncError> {
        self.strategy.verify_indexes(&self.indexes).await
    }
    
    /// Get index count
    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }
}

/// Load indexes from configuration
pub fn load_indexes_from_config(config_path: &str) -> Result<Vec<IndexInfo>, SyncError> {
    use std::fs;
    
    let contents = fs::read_to_string(config_path)
        .map_err(|e| SyncError::ConfigError(format!("Failed to read index config: {}", e)))?;
    
    let indexes: Vec<IndexInfo> = serde_yaml::from_str(&contents)
        .map_err(|e| SyncError::ConfigError(format!("Failed to parse index config: {}", e)))?;
    
    info!("Loaded {} indexes from config: {}", indexes.len(), config_path);
    Ok(indexes)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_index_info_serialization() {
        let index = IndexInfo {
            keyspace: "files_keyspace".to_string(),
            table: "files".to_string(),
            index_name: "files_by_asset_id".to_string(),
            column_name: "asset_id".to_string(),
            index_type: IndexType::Secondary,
        };
        
        let yaml = serde_yaml::to_string(&index).unwrap();
        assert!(yaml.contains("files_keyspace"));
        assert!(yaml.contains("asset_id"));
    }
}