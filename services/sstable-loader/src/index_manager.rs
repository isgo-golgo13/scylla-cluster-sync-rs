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
use tokio::time::sleep;

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
                    info!("✓ Dropped index: {}.{}", index.keyspace, index.index_name);
                    dropped.push(index.index_name.clone());
                }
                Err(e) => {
                    warn!("✗ Failed to drop index {}.{}: {}", 
                        index.keyspace, index.index_name, e);
                }
            }
            
            // Small delay to avoid overwhelming cluster
            sleep(Duration::from_millis(100)).await;
        }
        
        info!("Successfully dropped {}/{} indexes", dropped.len(), indexes.len());
        Ok(dropped)
    }
    
    async fn rebuild_indexes(&self, indexes: &[IndexInfo]) -> Result<(), SyncError> {
        info!("Rebuilding {} secondary indexes after SSTable load", indexes.len());
        let start = Instant::now();
        
        for index in indexes {
            let create_query = format!(
                "CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})",
                index.index_name,
                index.keyspace,
                index.table,
                index.column_name
            );
            
            info!("Rebuilding index: {}.{} on column {}", 
                index.keyspace, index.index_name, index.column_name);
            
            match self.connection.execute_simple(&create_query).await {
                Ok(_) => {
                    info!("✓ Rebuilt index: {}.{}", index.keyspace, index.index_name);
                }
                Err(e) => {
                    error!("✗ Failed to rebuild index {}.{}: {}", 
                        index.keyspace, index.index_name, e);
                    return Err(SyncError::DatabaseError(format!(
                        "Failed to rebuild index {}: {}", index.index_name, e
                    )));
                }
            }
            
            // Small delay between index builds
            sleep(Duration::from_millis(200)).await;
        }
        
        let elapsed = start.elapsed();
        info!("All {} indexes rebuilt in {:?}", indexes.len(), elapsed);
        Ok(())
    }
    
    async fn verify_indexes(&self, indexes: &[IndexInfo]) -> Result<bool, SyncError> {
        info!("Verifying {} indexes exist", indexes.len());
        
        for index in indexes {
            let query = format!(
                "SELECT index_name FROM system_schema.indexes WHERE keyspace_name = '{}' AND table_name = '{}' AND index_name = '{}'",
                index.keyspace,
                index.table,
                index.index_name
            );
            
            match self.connection.execute_simple(&query).await {
                Ok(result) => {
                    // FIXED: Use rows_num() instead of is_empty()
                    let row_count = result.rows_num().unwrap_or(0);
                    if row_count == 0 {
                        warn!("✗ Index missing: {}.{}", index.keyspace, index.index_name);
                        return Ok(false);
                    }
                }
                Err(e) => {
                    error!("Failed to verify index {}.{}: {}", 
                        index.keyspace, index.index_name, e);
                    return Ok(false);
                }
            }
        }
        
        info!("✓ All {} indexes verified", indexes.len());
        Ok(true)
    }
    
    fn name(&self) -> &str {
        "StandardIndexStrategy"
    }
}

/// Parallel index strategy - rebuild indexes in parallel batches
pub struct ParallelIndexStrategy {
    connection: Arc<ScyllaConnection>,
    parallelism: usize,
}

impl ParallelIndexStrategy {
    pub fn new(connection: Arc<ScyllaConnection>, parallelism: usize) -> Self {
        Self { 
            connection,
            parallelism: parallelism.max(1),
        }
    }
}

#[async_trait]
impl IndexStrategy for ParallelIndexStrategy {
    async fn drop_indexes(&self, indexes: &[IndexInfo]) -> Result<Vec<String>, SyncError> {
        info!("Dropping {} indexes with parallelism={}", indexes.len(), self.parallelism);
        let mut dropped = Vec::new();
        
        for chunk in indexes.chunks(self.parallelism) {
            let mut tasks = Vec::new();
            
            for index in chunk {
                let conn = self.connection.clone();
                let index = index.clone();
                
                let task = tokio::spawn(async move {
                    let drop_query = format!(
                        "DROP INDEX IF EXISTS {}.{}",
                        index.keyspace,
                        index.index_name
                    );
                    
                    match conn.execute_simple(&drop_query).await {
                        Ok(_) => {
                            info!("✓ Dropped index: {}.{}", index.keyspace, index.index_name);
                            Ok(index.index_name)
                        }
                        Err(e) => {
                            warn!("✗ Failed to drop index {}.{}: {}", 
                                index.keyspace, index.index_name, e);
                            Err(e)
                        }
                    }
                });
                
                tasks.push(task);
            }
            
            // Wait for batch to complete
            for task in tasks {
                if let Ok(Ok(name)) = task.await {
                    dropped.push(name);
                }
            }
            
            // Small delay between batches
            sleep(Duration::from_millis(500)).await;
        }
        
        info!("Successfully dropped {}/{} indexes", dropped.len(), indexes.len());
        Ok(dropped)
    }
    
    async fn rebuild_indexes(&self, indexes: &[IndexInfo]) -> Result<(), SyncError> {
        info!("Rebuilding {} indexes with parallelism={}", indexes.len(), self.parallelism);
        let start = Instant::now();
        let mut failures = Vec::new();
        
        for chunk in indexes.chunks(self.parallelism) {
            let mut tasks = Vec::new();
            
            for index in chunk {
                let conn = self.connection.clone();
                let index = index.clone();
                
                let task = tokio::spawn(async move {
                    let create_query = format!(
                        "CREATE INDEX IF NOT EXISTS {} ON {}.{} ({})",
                        index.index_name,
                        index.keyspace,
                        index.table,
                        index.column_name
                    );
                    
                    info!("Rebuilding index: {}.{}", index.keyspace, index.index_name);
                    
                    match conn.execute_simple(&create_query).await {
                        Ok(_) => {
                            info!("✓ Rebuilt index: {}.{}", index.keyspace, index.index_name);
                            Ok(())
                        }
                        Err(e) => {
                            error!("✗ Failed to rebuild index {}.{}: {}", 
                                index.keyspace, index.index_name, e);
                            Err((index.index_name.clone(), e))
                        }
                    }
                });
                
                tasks.push(task);
            }
            
            // Wait for batch to complete
            for task in tasks {
                if let Ok(Err((name, e))) = task.await {
                    failures.push((name, e));
                }
            }
            
            // Small delay between batches
            sleep(Duration::from_secs(1)).await;
        }
        
        if !failures.is_empty() {
            error!("{} indexes failed to rebuild", failures.len());
            return Err(SyncError::DatabaseError(format!(
                "Failed to rebuild {} indexes", failures.len()
            )));
        }
        
        let elapsed = start.elapsed();
        info!("All {} indexes rebuilt in {:?}", indexes.len(), elapsed);
        Ok(())
    }
    
    async fn verify_indexes(&self, indexes: &[IndexInfo]) -> Result<bool, SyncError> {
        // Use standard verification (no parallelism needed for reads)
        let standard = StandardIndexStrategy::new(self.connection.clone());
        standard.verify_indexes(indexes).await
    }
    
    fn name(&self) -> &str {
        "ParallelIndexStrategy"
    }
}

/// IndexManager - Orchestrates index lifecycle
pub struct IndexManager {
    strategy: Box<dyn IndexStrategy>,
    indexes: Vec<IndexInfo>,
}

impl IndexManager {
    pub fn new(strategy: Box<dyn IndexStrategy>, indexes: Vec<IndexInfo>) -> Self {
        info!("IndexManager initialized with {} strategy, {} indexes", 
            strategy.name(), indexes.len());
        Self { strategy, indexes }
    }
    
    pub fn index_count(&self) -> usize {
        self.indexes.len()
    }
    
    pub async fn drop_all(&self) -> Result<Vec<String>, SyncError> {
        info!("Dropping all {} indexes", self.indexes.len());
        self.strategy.drop_indexes(&self.indexes).await
    }
    
    pub async fn rebuild_all(&self) -> Result<(), SyncError> {
        info!("Rebuilding all {} indexes", self.indexes.len());
        self.strategy.rebuild_indexes(&self.indexes).await
    }
    
    pub async fn verify_all(&self) -> Result<bool, SyncError> {
        info!("Verifying all {} indexes", self.indexes.len());
        self.strategy.verify_indexes(&self.indexes).await
    }
    
    pub async fn drop_keyspace(&self, keyspace: &str) -> Result<Vec<String>, SyncError> {
        let indexes: Vec<_> = self.indexes.iter()
            .filter(|idx| idx.keyspace == keyspace)
            .cloned()
            .collect();
        
        info!("Dropping {} indexes in keyspace {}", indexes.len(), keyspace);
        self.strategy.drop_indexes(&indexes).await
    }
    
    pub async fn rebuild_keyspace(&self, keyspace: &str) -> Result<(), SyncError> {
        let indexes: Vec<_> = self.indexes.iter()
            .filter(|idx| idx.keyspace == keyspace)
            .cloned()
            .collect();
        
        info!("Rebuilding {} indexes in keyspace {}", indexes.len(), keyspace);
        self.strategy.rebuild_indexes(&indexes).await
    }
}

/// Load index configuration from YAML file
pub fn load_indexes_from_config(path: &str) -> Result<Vec<IndexInfo>, SyncError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| SyncError::ConfigError(format!("Failed to read index config: {}", e)))?;
    
    let indexes: Vec<IndexInfo> = serde_yaml::from_str(&content)
        .map_err(|e| SyncError::ConfigError(format!("Failed to parse index config: {}", e)))?;
    
    info!("Loaded {} indexes from {}", indexes.len(), path);
    Ok(indexes)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_index_info_serialization() {
        let index = IndexInfo {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            index_name: "test_idx".to_string(),
            column_name: "test_col".to_string(),
            index_type: IndexType::Secondary,
        };
        
        let yaml = serde_yaml::to_string(&index).unwrap();
        assert!(yaml.contains("test_ks"));
        assert!(yaml.contains("test_idx"));
    }
    
    #[test]
    fn test_load_indexes_from_yaml() {
        let yaml = r#"
- keyspace: files_keyspace
  table: files
  index_name: files_by_asset_id
  column_name: asset_id
  index_type: secondary

- keyspace: assets_keyspace
  table: assets
  index_name: assets_by_status
  column_name: status
  index_type: secondary
"#;
        
        let indexes: Vec<IndexInfo> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(indexes.len(), 2);
        assert_eq!(indexes[0].keyspace, "files_keyspace");
        assert_eq!(indexes[1].table, "assets");
    }
}