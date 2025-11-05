use anyhow::Result;
use config::{Config, File};
use serde::{Deserialize, Serialize};
use scylla_sync_shared::config::{DatabaseConfig, ObservabilityConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DualReaderConfig {
    pub source: DatabaseConfig,
    pub target: DatabaseConfig,
    pub reader: ReaderConfig,
    pub observability: ObservabilityConfig,
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

pub fn load_config(path: &str) -> Result<DualReaderConfig> {
    let config = Config::builder()
        .add_source(File::with_name(path))
        .add_source(config::Environment::with_prefix("DUAL_READER"))
        .build()?;
    
    Ok(config.try_deserialize()?)
}