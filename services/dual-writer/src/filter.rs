// services/dual-writer/src/filter.rs
//
// FilterGovernor - Strategy Pattern for tenant/table filtering
// Prevents blacklisted tenants and tables from getting written to target cluster
//

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

use svckit::types::WriteRequest;

/// Filter decision
#[derive(Debug, Clone, PartialEq)]
pub enum FilterDecision {
    Allow,
    Deny(DenyReason),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DenyReason {
    TenantBlacklisted(Uuid),
    TableBlacklisted(String),
}

/// Filter strategy trait
#[async_trait]
pub trait FilterStrategy: Send + Sync {
    async fn evaluate(&self, request: &WriteRequest) -> FilterDecision;
    fn name(&self) -> &str;
}

/// Tenant filter - blocks specific system_domain_id UUIDs
pub struct TenantFilter {
    blacklist: Arc<RwLock<HashSet<Uuid>>>,
}

impl TenantFilter {
    pub fn new(blacklist: Vec<Uuid>) -> Self {
        Self {
            blacklist: Arc::new(RwLock::new(blacklist.into_iter().collect())),
        }
    }
    
    pub async fn reload(&self, new_blacklist: Vec<Uuid>) {
        let mut blacklist = self.blacklist.write().await;
        *blacklist = new_blacklist.into_iter().collect();
        info!("Tenant blacklist reloaded: {} entries", blacklist.len());
    }
    
    /// Extract system_domain_id from WriteRequest
    /// Assumes first value in partition key is system_domain_id (UUID)
    fn extract_tenant_id(&self, request: &WriteRequest) -> Option<Uuid> {
        // Parse from request.values - first value should be system_domain_id
        if let Some(first_value) = request.values.first() {
            if let Some(uuid_str) = first_value.as_str() {
                return Uuid::parse_str(uuid_str).ok();
            }
        }
        None
    }
}

#[async_trait]
impl FilterStrategy for TenantFilter {
    async fn evaluate(&self, request: &WriteRequest) -> FilterDecision {
        if let Some(tenant_id) = self.extract_tenant_id(request) {
            let blacklist = self.blacklist.read().await;
            if blacklist.contains(&tenant_id) {
                warn!("Tenant {} is blacklisted - skipping target write", tenant_id);
                return FilterDecision::Deny(DenyReason::TenantBlacklisted(tenant_id));
            }
        }
        FilterDecision::Allow
    }
    
    fn name(&self) -> &str {
        "TenantFilter"
    }
}

/// Table filter - blocks specific keyspace.table combinations
pub struct TableFilter {
    blacklist: Arc<RwLock<HashSet<String>>>,
}

impl TableFilter {
    pub fn new(blacklist: Vec<String>) -> Self {
        Self {
            blacklist: Arc::new(RwLock::new(blacklist.into_iter().collect())),
        }
    }
    
    pub async fn reload(&self, new_blacklist: Vec<String>) {
        let mut blacklist = self.blacklist.write().await;
        *blacklist = new_blacklist.into_iter().collect();
        info!("Table blacklist reloaded: {} entries", blacklist.len());
    }
    
    fn matches_pattern(&self, pattern: &str, table_name: &str) -> bool {
        if pattern.starts_with("*.") {
            // Wildcard pattern: *.migrations matches any_keyspace.migrations
            let suffix = &pattern[2..];
            table_name.ends_with(suffix)
        } else {
            pattern == table_name
        }
    }
}

#[async_trait]
impl FilterStrategy for TableFilter {
    async fn evaluate(&self, request: &WriteRequest) -> FilterDecision {
        let table_name = format!("{}.{}", request.keyspace, request.table);
        let blacklist = self.blacklist.read().await;
        
        for pattern in blacklist.iter() {
            if self.matches_pattern(pattern, &table_name) {
                warn!("Table {} is blacklisted - skipping target write", table_name);
                return FilterDecision::Deny(DenyReason::TableBlacklisted(table_name));
            }
        }
        
        FilterDecision::Allow
    }
    
    fn name(&self) -> &str {
        "TableFilter"
    }
}

/// Composite filter - chains multiple filters (AND logic)
pub struct CompositeFilter {
    filters: Vec<Box<dyn FilterStrategy>>,
}

impl CompositeFilter {
    pub fn new(filters: Vec<Box<dyn FilterStrategy>>) -> Self {
        Self { filters }
    }
}

#[async_trait]
impl FilterStrategy for CompositeFilter {
    async fn evaluate(&self, request: &WriteRequest) -> FilterDecision {
        for filter in &self.filters {
            match filter.evaluate(request).await {
                FilterDecision::Deny(reason) => return FilterDecision::Deny(reason),
                FilterDecision::Allow => continue,
            }
        }
        FilterDecision::Allow
    }
    
    fn name(&self) -> &str {
        "CompositeFilter"
    }
}

/// FilterGovernor - Main filter orchestrator
pub struct FilterGovernor {
    strategy: Arc<RwLock<Box<dyn FilterStrategy>>>,
    tenant_filter: Arc<TenantFilter>,
    table_filter: Arc<TableFilter>,
}

impl FilterGovernor {
    pub fn new(config: &FilterConfig) -> Self {
        let tenant_filter = Arc::new(TenantFilter::new(config.tenant_blacklist.clone()));
        let table_filter = Arc::new(TableFilter::new(config.table_blacklist.clone()));
        
        let composite = CompositeFilter::new(vec![
            Box::new(TenantFilter::new(config.tenant_blacklist.clone())),
            Box::new(TableFilter::new(config.table_blacklist.clone())),
        ]);
        
        info!(
            "FilterGovernor initialized: {} tenants, {} tables blacklisted",
            config.tenant_blacklist.len(),
            config.table_blacklist.len()
        );
        
        Self {
            strategy: Arc::new(RwLock::new(Box::new(composite))),
            tenant_filter,
            table_filter,
        }
    }
    
    /// Evaluate if write should proceed to target
    pub async fn should_write_to_target(&self, request: &WriteRequest) -> bool {
        let strategy = self.strategy.read().await;
        matches!(strategy.evaluate(request).await, FilterDecision::Allow)
    }
    
    /// Reload filter configuration (hot-reload)
    pub async fn reload_config(&self, config: &FilterConfig) {
        self.tenant_filter.reload(config.tenant_blacklist.clone()).await;
        self.table_filter.reload(config.table_blacklist.clone()).await;
        
        info!("FilterGovernor configuration reloaded");
    }
}

/// Filter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterConfig {
    #[serde(default)]
    pub tenant_blacklist: Vec<Uuid>,
    
    #[serde(default)]
    pub table_blacklist: Vec<String>,
    
    #[serde(default = "default_reload_interval")]
    pub reload_interval_secs: u64,
}

fn default_reload_interval() -> u64 {
    60
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            tenant_blacklist: vec![],
            table_blacklist: vec![],
            reload_interval_secs: 60,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_tenant_filter() {
        let tenant_id = Uuid::new_v4();
        let filter = TenantFilter::new(vec![tenant_id]);
        
        let mut request = WriteRequest {
            keyspace: "test_ks".to_string(),
            table: "test_table".to_string(),
            query: "".to_string(),
            values: vec![serde_json::json!(tenant_id.to_string())],
            consistency: None,
            request_id: Uuid::new_v4(),
            timestamp: None,
        };
        
        let decision = filter.evaluate(&request).await;
        assert!(matches!(decision, FilterDecision::Deny(_)));
    }
    
    #[tokio::test]
    async fn test_table_filter() {
        let filter = TableFilter::new(vec!["stats_keyspace.user_audit".to_string()]);
        
        let request = WriteRequest {
            keyspace: "stats_keyspace".to_string(),
            table: "user_audit".to_string(),
            query: "".to_string(),
            values: vec![],
            consistency: None,
            request_id: Uuid::new_v4(),
            timestamp: None,
        };
        
        let decision = filter.evaluate(&request).await;
        assert!(matches!(decision, FilterDecision::Deny(_)));
    }
    
    #[tokio::test]
    async fn test_wildcard_pattern() {
        let filter = TableFilter::new(vec!["*.migrations".to_string()]);
        
        let request = WriteRequest {
            keyspace: "any_keyspace".to_string(),
            table: "migrations".to_string(),
            query: "".to_string(),
            values: vec![],
            consistency: None,
            request_id: Uuid::new_v4(),
            timestamp: None,
        };
        
        let decision = filter.evaluate(&request).await;
        assert!(matches!(decision, FilterDecision::Deny(_)));
    }
}