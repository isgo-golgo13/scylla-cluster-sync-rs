use anyhow::Result;
use config::{Config, File};
use serde::{Deserialize, Serialize};
use svckit::config::{DatabaseConfig, ObservabilityConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DualReaderConfig {
    pub source: DatabaseConfig,
    pub target: DatabaseConfig,
    pub reader: ReaderConfig,
    pub observability: ObservabilityConfig,
    /// Optional filter config for selective dual reads (Iconik client spec)
    #[serde(default)]
    pub filter: DualReaderFilterConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReaderConfig {
    pub tables: Vec<String>,
    pub validation_interval_secs: u64,
    pub sample_rate: f64,
    pub max_concurrent_reads: usize,
    pub batch_size: usize,
    pub max_discrepancies_to_report: usize,
    pub auto_reconcile: bool,
    pub reconciliation_mode: ReconciliationMode,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationMode {
    SourceWins,
    NewestWins,
    Manual,
}

impl Default for ReconciliationMode {
    fn default() -> Self {
        ReconciliationMode::SourceWins
    }
}

/// Iconik client filtering spec — selective dual reads based on table + domain whitelists.
///
/// Both conditions must pass (AND gate) for a dual read to occur.
/// If either fails → source only, no comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DualReaderFilterConfig {
    /// Master switch — set to true to enable filtering (default: false)
    #[serde(default)]
    pub enabled: bool,

    /// Tables to compare (whitelist), format: "keyspace.table"
    /// Tables NOT in this list are read from source only (no comparison)
    /// Empty list with enabled=true means NO tables are compared
    #[serde(default)]
    pub compare_tables: Vec<String>,

    /// system_domain_ids to compare (whitelist)
    /// Domains NOT in this list are read from source only
    /// Empty list with enabled=true means NO domains are compared
    #[serde(default)]
    pub compare_system_domain_ids: Vec<String>,

    /// Column name used to extract system_domain_id from query context
    #[serde(default = "default_domain_id_column")]
    pub system_domain_id_column: String,

    /// Write discrepancies to JSONL file (default: true)
    #[serde(default = "default_true")]
    pub log_discrepancies: bool,

    /// Path for the discrepancy JSONL log
    #[serde(default = "default_discrepancy_log_path")]
    pub discrepancy_log_path: String,
}

impl Default for DualReaderFilterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            compare_tables: vec![],
            compare_system_domain_ids: vec![],
            system_domain_id_column: default_domain_id_column(),
            log_discrepancies: true,
            discrepancy_log_path: default_discrepancy_log_path(),
        }
    }
}

fn default_domain_id_column() -> String {
    "system_domain_id".to_string()
}

fn default_true() -> bool {
    true
}

fn default_discrepancy_log_path() -> String {
    "discrepancies.jsonl".to_string()
}

pub fn load_config(path: &str) -> Result<DualReaderConfig> {
    let config = Config::builder()
        .add_source(File::with_name(path))
        .add_source(config::Environment::with_prefix("DUAL_READER"))
        .build()?;
    
    Ok(config.try_deserialize()?)
}