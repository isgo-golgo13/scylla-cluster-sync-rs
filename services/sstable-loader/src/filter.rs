// services/sstable-loader/src/filter.rs
//
// FilterGovernor for SSTable-Loader
// Filters out blacklisted tenants and tables during bulk migration
//
// Supports:
// - Table-level blacklist: Skip entire tables
// - Tenant-level blacklist: Skip partitions/rows containing blacklisted tenants
//

use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

/// Filter configuration loaded from filter-rules.yaml
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FilterConfig {
    /// List of tenant IDs to exclude from migration
    #[serde(default)]
    pub tenant_blacklist: Vec<String>,
    
    /// List of tables to skip entirely (format: "keyspace.table")
    #[serde(default)]
    pub table_blacklist: Vec<String>,
    
    /// Column names that contain tenant IDs (for row-level filtering)
    /// e.g., ["system_domain_id", "tenant_id"]
    #[serde(default)]
    pub tenant_id_columns: Vec<String>,
}

/// Filter decision for a row or partition
#[derive(Debug, Clone, PartialEq)]
pub enum FilterDecision {
    /// Allow the data to be migrated
    Allow,
    /// Skip/drop the data (tenant blacklisted)
    SkipTenant(String),
    /// Skip/drop the data (table blacklisted)
    SkipTable(String),
}

/// FilterGovernor - Thread-safe filter rule engine
/// 
/// Determines whether tables, partitions, or rows should be migrated
/// based on configurable blacklist rules.
pub struct FilterGovernor {
    /// Set of blacklisted tenant IDs for O(1) lookup
    tenant_blacklist: Arc<RwLock<HashSet<String>>>,
    
    /// Set of blacklisted tables for O(1) lookup
    table_blacklist: Arc<RwLock<HashSet<String>>>,
    
    /// Column names that contain tenant IDs
    tenant_id_columns: Arc<RwLock<Vec<String>>>,
    
    /// Statistics
    stats: Arc<FilterStats>,
}

/// Filter statistics (thread-safe counters)
pub struct FilterStats {
    pub tables_skipped: std::sync::atomic::AtomicU64,
    pub partitions_skipped: std::sync::atomic::AtomicU64,
    pub rows_skipped: std::sync::atomic::AtomicU64,
    pub rows_allowed: std::sync::atomic::AtomicU64,
}

impl FilterStats {
    pub fn new() -> Self {
        Self {
            tables_skipped: std::sync::atomic::AtomicU64::new(0),
            partitions_skipped: std::sync::atomic::AtomicU64::new(0),
            rows_skipped: std::sync::atomic::AtomicU64::new(0),
            rows_allowed: std::sync::atomic::AtomicU64::new(0),
        }
    }
    
    pub fn get_summary(&self) -> FilterStatsSummary {
        use std::sync::atomic::Ordering;
        FilterStatsSummary {
            tables_skipped: self.tables_skipped.load(Ordering::Relaxed),
            partitions_skipped: self.partitions_skipped.load(Ordering::Relaxed),
            rows_skipped: self.rows_skipped.load(Ordering::Relaxed),
            rows_allowed: self.rows_allowed.load(Ordering::Relaxed),
        }
    }
}

impl Default for FilterStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable filter stats summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterStatsSummary {
    pub tables_skipped: u64,
    pub partitions_skipped: u64,
    pub rows_skipped: u64,
    pub rows_allowed: u64,
}

impl FilterGovernor {
    /// Create new FilterGovernor from configuration
    pub fn new(config: &FilterConfig) -> Self {
        let tenant_set: HashSet<String> = config.tenant_blacklist.iter().cloned().collect();
        let table_set: HashSet<String> = config.table_blacklist.iter().cloned().collect();
        
        info!(
            "FilterGovernor initialized: {} tenants blacklisted, {} tables blacklisted, {} tenant ID columns",
            tenant_set.len(),
            table_set.len(),
            config.tenant_id_columns.len()
        );
        
        if !tenant_set.is_empty() {
            debug!("Blacklisted tenants: {:?}", tenant_set);
        }
        if !table_set.is_empty() {
            debug!("Blacklisted tables: {:?}", table_set);
        }
        
        Self {
            tenant_blacklist: Arc::new(RwLock::new(tenant_set)),
            table_blacklist: Arc::new(RwLock::new(table_set)),
            tenant_id_columns: Arc::new(RwLock::new(config.tenant_id_columns.clone())),
            stats: Arc::new(FilterStats::new()),
        }
    }
    
    /// Create a no-op FilterGovernor (allows everything)
    pub fn allow_all() -> Self {
        Self {
            tenant_blacklist: Arc::new(RwLock::new(HashSet::new())),
            table_blacklist: Arc::new(RwLock::new(HashSet::new())),
            tenant_id_columns: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(FilterStats::new()),
        }
    }
    
    /// Check if a table should be skipped entirely
    pub async fn should_skip_table(&self, table_name: &str) -> bool {
        let blacklist = self.table_blacklist.read().await;
        let skip = blacklist.contains(table_name);
        
        if skip {
            use std::sync::atomic::Ordering;
            self.stats.tables_skipped.fetch_add(1, Ordering::Relaxed);
            info!("Skipping blacklisted table: {}", table_name);
        }
        
        skip
    }
    
    /// Check if a tenant ID is blacklisted
    pub async fn is_tenant_blacklisted(&self, tenant_id: &str) -> bool {
        let blacklist = self.tenant_blacklist.read().await;
        blacklist.contains(tenant_id)
    }
    
    /// Get the list of tenant ID column names
    pub async fn get_tenant_id_columns(&self) -> Vec<String> {
        let columns = self.tenant_id_columns.read().await;
        columns.clone()
    }
    
    /// Check if any value in the provided map matches a blacklisted tenant
    /// 
    /// # Arguments
    /// * `column_values` - Map of column_name -> value
    /// 
    /// # Returns
    /// * `FilterDecision::Allow` if no blacklisted tenant found
    /// * `FilterDecision::SkipTenant(id)` if blacklisted tenant found
    pub async fn check_row(&self, column_values: &std::collections::HashMap<String, String>) -> FilterDecision {
        let tenant_columns = self.tenant_id_columns.read().await;
        let blacklist = self.tenant_blacklist.read().await;
        
        for column in tenant_columns.iter() {
            if let Some(value) = column_values.get(column) {
                if blacklist.contains(value) {
                    use std::sync::atomic::Ordering;
                    self.stats.rows_skipped.fetch_add(1, Ordering::Relaxed);
                    debug!("Filtering row: column {} = {} (blacklisted)", column, value);
                    return FilterDecision::SkipTenant(value.clone());
                }
            }
        }
        
        use std::sync::atomic::Ordering;
        self.stats.rows_allowed.fetch_add(1, Ordering::Relaxed);
        FilterDecision::Allow
    }
    
    /// Check a single tenant ID value directly
    pub async fn check_tenant_id(&self, tenant_id: &str) -> FilterDecision {
        if self.is_tenant_blacklisted(tenant_id).await {
            use std::sync::atomic::Ordering;
            self.stats.rows_skipped.fetch_add(1, Ordering::Relaxed);
            FilterDecision::SkipTenant(tenant_id.to_string())
        } else {
            use std::sync::atomic::Ordering;
            self.stats.rows_allowed.fetch_add(1, Ordering::Relaxed);
            FilterDecision::Allow
        }
    }
    
    /// Record a skipped partition (for stats)
    pub fn record_partition_skipped(&self) {
        use std::sync::atomic::Ordering;
        self.stats.partitions_skipped.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Get filter statistics summary
    pub fn get_stats(&self) -> FilterStatsSummary {
        self.stats.get_summary()
    }
    
    /// Reload configuration (hot-reload support)
    pub async fn reload_config(&self, config: &FilterConfig) {
        {
            let mut tenant_blacklist = self.tenant_blacklist.write().await;
            *tenant_blacklist = config.tenant_blacklist.iter().cloned().collect();
        }
        {
            let mut table_blacklist = self.table_blacklist.write().await;
            *table_blacklist = config.table_blacklist.iter().cloned().collect();
        }
        {
            let mut tenant_columns = self.tenant_id_columns.write().await;
            *tenant_columns = config.tenant_id_columns.clone();
        }
        
        info!(
            "FilterGovernor reloaded: {} tenants, {} tables blacklisted",
            config.tenant_blacklist.len(),
            config.table_blacklist.len()
        );
    }
    
    /// Check if filtering is enabled (any rules configured)
    pub async fn is_enabled(&self) -> bool {
        let tenants = self.tenant_blacklist.read().await;
        let tables = self.table_blacklist.read().await;
        !tenants.is_empty() || !tables.is_empty()
    }
}

/// Load filter configuration from YAML file
pub fn load_filter_config(path: &str) -> anyhow::Result<FilterConfig> {
    use std::fs;
    
    let contents = fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("Failed to read filter config '{}': {}", path, e))?;
    
    #[derive(Deserialize)]
    struct FilterYaml {
        filters: FilterConfig,
    }
    
    let yaml: FilterYaml = serde_yaml::from_str(&contents)
        .map_err(|e| anyhow::anyhow!("Failed to parse filter config '{}': {}", path, e))?;
    
    info!(
        "Filter config loaded from '{}': {} tenants, {} tables blacklisted",
        path,
        yaml.filters.tenant_blacklist.len(),
        yaml.filters.table_blacklist.len()
    );
    
    Ok(yaml.filters)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_table_blacklist() {
        let config = FilterConfig {
            tenant_blacklist: vec![],
            table_blacklist: vec!["keyspace.secret_table".to_string()],
            tenant_id_columns: vec![],
        };
        
        let governor = FilterGovernor::new(&config);
        
        assert!(governor.should_skip_table("keyspace.secret_table").await);
        assert!(!governor.should_skip_table("keyspace.public_table").await);
    }
    
    #[tokio::test]
    async fn test_tenant_blacklist() {
        let config = FilterConfig {
            tenant_blacklist: vec!["tenant-123".to_string(), "tenant-456".to_string()],
            table_blacklist: vec![],
            tenant_id_columns: vec!["system_domain_id".to_string()],
        };
        
        let governor = FilterGovernor::new(&config);
        
        assert!(governor.is_tenant_blacklisted("tenant-123").await);
        assert!(governor.is_tenant_blacklisted("tenant-456").await);
        assert!(!governor.is_tenant_blacklisted("tenant-789").await);
    }
    
    #[tokio::test]
    async fn test_row_filtering() {
        let config = FilterConfig {
            tenant_blacklist: vec!["bad-tenant".to_string()],
            table_blacklist: vec![],
            tenant_id_columns: vec!["system_domain_id".to_string(), "tenant_id".to_string()],
        };
        
        let governor = FilterGovernor::new(&config);
        
        // Row with blacklisted tenant
        let mut row1 = std::collections::HashMap::new();
        row1.insert("system_domain_id".to_string(), "bad-tenant".to_string());
        row1.insert("data".to_string(), "some data".to_string());
        
        assert_eq!(
            governor.check_row(&row1).await,
            FilterDecision::SkipTenant("bad-tenant".to_string())
        );
        
        // Row with allowed tenant
        let mut row2 = std::collections::HashMap::new();
        row2.insert("system_domain_id".to_string(), "good-tenant".to_string());
        row2.insert("data".to_string(), "some data".to_string());
        
        assert_eq!(governor.check_row(&row2).await, FilterDecision::Allow);
    }
    
    #[tokio::test]
    async fn test_allow_all() {
        let governor = FilterGovernor::allow_all();
        
        assert!(!governor.should_skip_table("any.table").await);
        assert!(!governor.is_tenant_blacklisted("any-tenant").await);
        assert!(!governor.is_enabled().await);
    }
    
    #[tokio::test]
    async fn test_stats() {
        let config = FilterConfig {
            tenant_blacklist: vec!["blocked".to_string()],
            table_blacklist: vec![],
            tenant_id_columns: vec!["tenant".to_string()],
        };
        
        let governor = FilterGovernor::new(&config);
        
        // Check some rows
        let _ = governor.check_tenant_id("blocked").await;
        let _ = governor.check_tenant_id("allowed").await;
        let _ = governor.check_tenant_id("blocked").await;
        
        let stats = governor.get_stats();
        assert_eq!(stats.rows_skipped, 2);
        assert_eq!(stats.rows_allowed, 1);
    }
}
