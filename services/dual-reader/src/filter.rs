// services/dual-reader/src/filter.rs
//
// DualReaderFilterGovernor — Whitelist-based read decision engine
//
// Implements the Iconik client filtering spec:
//
// AND gate — BOTH conditions must pass for a dual read:
//   1. table IN compare_tables whitelist
//   2. system_domain_id IN compare_system_domain_ids whitelist (when provided)
//
// If either condition fails → SOURCE ONLY (no comparison, no discrepancy logging)
// If both pass              → DUAL READ + compare + log discrepancies to JSONL
//
// This is the inverse of sstable-loader's FilterGovernor (blacklist).
// Here everything is a whitelist — only explicitly listed items are compared.
//
// Rationale: some customers already reside in AWS pre-migration. We only want
// to compare domains that have been migrated. Tables not in the compare list
// contain source-system-specific data that won't be migrated at all.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::config::DualReaderFilterConfig;

// =============================================================================
// Read Decision
// =============================================================================

/// The outcome of a filter check — what the reader should do for this request
#[derive(Debug, Clone, PartialEq)]
pub enum ReadDecision {
    /// Both table and domain are in their whitelists → dual read + compare
    DualRead,
    /// Table or domain not in whitelist → read source only, skip comparison
    SourceOnly { reason: String },
}

// =============================================================================
// Filter Statistics
// =============================================================================

pub struct FilterStats {
    /// Reads where filter passed → dual read performed
    pub dual_reads: AtomicU64,
    /// Reads routed to source only due to filter
    pub source_only_reads: AtomicU64,
    /// Requests skipped because table not in compare_tables
    pub tables_filtered: AtomicU64,
    /// Requests skipped because domain not in compare_system_domain_ids
    pub domains_filtered: AtomicU64,
}

impl FilterStats {
    fn new() -> Self {
        Self {
            dual_reads: AtomicU64::new(0),
            source_only_reads: AtomicU64::new(0),
            tables_filtered: AtomicU64::new(0),
            domains_filtered: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> FilterStatsSummary {
        FilterStatsSummary {
            dual_reads: self.dual_reads.load(Ordering::Relaxed),
            source_only_reads: self.source_only_reads.load(Ordering::Relaxed),
            tables_filtered: self.tables_filtered.load(Ordering::Relaxed),
            domains_filtered: self.domains_filtered.load(Ordering::Relaxed),
        }
    }
}

/// Serializable snapshot of filter stats for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterStatsSummary {
    pub dual_reads: u64,
    pub source_only_reads: u64,
    pub tables_filtered: u64,
    pub domains_filtered: u64,
}

// =============================================================================
// DualReaderFilterGovernor
// =============================================================================

/// Thread-safe whitelist filter engine for the dual-reader.
///
/// Determines whether a given (table, domain) combination should be:
///   - DualRead: read from both source + target and compare
///   - SourceOnly: read from source only, return result without comparison
pub struct DualReaderFilterGovernor {
    /// Whether filtering is active — when false, everything is DualRead
    enabled: bool,

    /// Whitelisted tables (keyspace.table format)
    /// Empty set with enabled=true means NO tables are compared
    compare_tables: Arc<RwLock<HashSet<String>>>,

    /// Whitelisted system_domain_ids
    /// Empty set with enabled=true means NO domains are compared
    compare_domains: Arc<RwLock<HashSet<String>>>,

    /// Filter statistics
    stats: Arc<FilterStats>,
}

impl DualReaderFilterGovernor {
    /// Create a new governor from config
    pub fn new(config: &DualReaderFilterConfig) -> Self {
        let table_set: HashSet<String> = config.compare_tables.iter().cloned().collect();
        let domain_set: HashSet<String> = config.compare_system_domain_ids.iter().cloned().collect();

        info!(
            "DualReaderFilterGovernor initialized: enabled={}, {} tables whitelisted, {} domains whitelisted",
            config.enabled,
            table_set.len(),
            domain_set.len()
        );

        if config.enabled {
            if !table_set.is_empty() {
                info!("Compare tables: {:?}", table_set);
            }
            if !domain_set.is_empty() {
                info!("Compare domains: {} entries", domain_set.len());
            }
        }

        Self {
            enabled: config.enabled,
            compare_tables: Arc::new(RwLock::new(table_set)),
            compare_domains: Arc::new(RwLock::new(domain_set)),
            stats: Arc::new(FilterStats::new()),
        }
    }

    /// Create a pass-through governor (dual-read everything).
    /// Used when filter config is absent or disabled.
    pub fn allow_all() -> Self {
        Self {
            enabled: false,
            compare_tables: Arc::new(RwLock::new(HashSet::new())),
            compare_domains: Arc::new(RwLock::new(HashSet::new())),
            stats: Arc::new(FilterStats::new()),
        }
    }

    /// Core decision function — AND gate.
    ///
    /// # Arguments
    /// * `table`     - Fully qualified table name (keyspace.table)
    /// * `domain_id` - Optional system_domain_id extracted from the query/context
    ///
    /// # Returns
    /// * `ReadDecision::DualRead`   — read from both clusters, compare, log discrepancies
    /// * `ReadDecision::SourceOnly` — read from source only, return result, skip comparison
    pub async fn decide(&self, table: &str, domain_id: Option<&str>) -> ReadDecision {
        // Filter disabled → always dual read, no checks needed
        if !self.enabled {
            self.stats.dual_reads.fetch_add(1, Ordering::Relaxed);
            return ReadDecision::DualRead;
        }

        // --- Step 1: Table whitelist check ---
        {
            let tables = self.compare_tables.read().await;
            // Non-empty whitelist: table must be present
            if !tables.is_empty() && !tables.contains(table) {
                self.stats.tables_filtered.fetch_add(1, Ordering::Relaxed);
                self.stats.source_only_reads.fetch_add(1, Ordering::Relaxed);
                debug!("Table '{}' not in compare_tables → source only", table);
                return ReadDecision::SourceOnly {
                    reason: format!("table '{}' not in compare_tables whitelist", table),
                };
            }
        }

        // --- Step 2: Domain whitelist check (only if domain_id provided) ---
        if let Some(domain) = domain_id {
            let domains = self.compare_domains.read().await;
            // Non-empty whitelist: domain must be present
            if !domains.is_empty() && !domains.contains(domain) {
                self.stats.domains_filtered.fetch_add(1, Ordering::Relaxed);
                self.stats.source_only_reads.fetch_add(1, Ordering::Relaxed);
                debug!("Domain '{}' not in compare_system_domain_ids → source only", domain);
                return ReadDecision::SourceOnly {
                    reason: format!("domain '{}' not in compare_system_domain_ids whitelist", domain),
                };
            }
        }

        // Both conditions passed → dual read
        self.stats.dual_reads.fetch_add(1, Ordering::Relaxed);
        ReadDecision::DualRead
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn get_stats(&self) -> FilterStatsSummary {
        self.stats.snapshot()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DualReaderFilterConfig;

    fn make_config(
        enabled: bool,
        tables: Vec<&str>,
        domains: Vec<&str>,
    ) -> DualReaderFilterConfig {
        DualReaderFilterConfig {
            enabled,
            compare_tables: tables.into_iter().map(String::from).collect(),
            compare_system_domain_ids: domains.into_iter().map(String::from).collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_disabled_always_dual_reads() {
        let cfg = make_config(false, vec!["ks.table_a"], vec!["domain-1"]);
        let g = DualReaderFilterGovernor::new(&cfg);
        // filter disabled — even unlisted table/domain should dual read
        assert_eq!(g.decide("ks.other_table", Some("unknown-domain")).await, ReadDecision::DualRead);
    }

    #[tokio::test]
    async fn test_allow_all_governor() {
        let g = DualReaderFilterGovernor::allow_all();
        assert_eq!(g.decide("ks.any_table", Some("any-domain")).await, ReadDecision::DualRead);
        assert!(!g.is_enabled());
    }

    #[tokio::test]
    async fn test_table_not_in_whitelist() {
        let cfg = make_config(true, vec!["ks.approved_table"], vec![]);
        let g = DualReaderFilterGovernor::new(&cfg);

        // Unlisted table → source only
        let decision = g.decide("ks.other_table", None).await;
        assert!(matches!(decision, ReadDecision::SourceOnly { .. }));

        // Listed table → dual read
        assert_eq!(g.decide("ks.approved_table", None).await, ReadDecision::DualRead);
    }

    #[tokio::test]
    async fn test_domain_not_in_whitelist() {
        let cfg = make_config(true, vec![], vec!["domain-abc-123"]);
        let g = DualReaderFilterGovernor::new(&cfg);

        // Listed domain → dual read (empty table whitelist = all tables pass)
        assert_eq!(g.decide("ks.any_table", Some("domain-abc-123")).await, ReadDecision::DualRead);

        // Unlisted domain → source only
        let decision = g.decide("ks.any_table", Some("domain-xyz-999")).await;
        assert!(matches!(decision, ReadDecision::SourceOnly { .. }));
    }

    #[tokio::test]
    async fn test_and_gate_both_must_pass() {
        let cfg = make_config(
            true,
            vec!["ks.assets"],
            vec!["domain-abc-123", "domain-def-456"],
        );
        let g = DualReaderFilterGovernor::new(&cfg);

        // Both pass → dual read
        assert_eq!(g.decide("ks.assets", Some("domain-abc-123")).await, ReadDecision::DualRead);

        // Table fails → source only
        let d1 = g.decide("ks.other", Some("domain-abc-123")).await;
        assert!(matches!(d1, ReadDecision::SourceOnly { .. }));

        // Domain fails → source only
        let d2 = g.decide("ks.assets", Some("domain-unknown")).await;
        assert!(matches!(d2, ReadDecision::SourceOnly { .. }));

        // Both fail → source only (table checked first)
        let d3 = g.decide("ks.other", Some("domain-unknown")).await;
        assert!(matches!(d3, ReadDecision::SourceOnly { .. }));
    }

    #[tokio::test]
    async fn test_no_domain_provided_skips_domain_check() {
        let cfg = make_config(true, vec!["ks.assets"], vec!["domain-abc-123"]);
        let g = DualReaderFilterGovernor::new(&cfg);

        // Table in whitelist, no domain provided → domain check skipped → dual read
        assert_eq!(g.decide("ks.assets", None).await, ReadDecision::DualRead);
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let cfg = make_config(true, vec!["ks.assets"], vec!["domain-abc"]);
        let g = DualReaderFilterGovernor::new(&cfg);

        let _ = g.decide("ks.assets", Some("domain-abc")).await;  // dual
        let _ = g.decide("ks.other", Some("domain-abc")).await;   // table filtered
        let _ = g.decide("ks.assets", Some("domain-xyz")).await;  // domain filtered
        let _ = g.decide("ks.assets", None).await;                // dual (no domain)

        let stats = g.get_stats();
        assert_eq!(stats.dual_reads, 2);
        assert_eq!(stats.source_only_reads, 2);
        assert_eq!(stats.tables_filtered, 1);
        assert_eq!(stats.domains_filtered, 1);
    }
}
