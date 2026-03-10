use anyhow::Result;
use config::{Config, File};
use serde::{Deserialize, Serialize};
use svckit::config::{DatabaseConfig, ObservabilityConfig};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableLoaderConfig {
    pub source: DatabaseConfig,
    pub target: DatabaseConfig,
    pub loader: LoaderConfig,
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoaderConfig {
    pub tables: Vec<TableConfig>,
    pub num_ranges_per_core: usize,
    pub max_concurrent_loaders: usize,
    pub batch_size: usize,
    pub checkpoint_interval_secs: u64,
    pub checkpoint_file: String,
    pub prefetch_rows: usize,
    pub compression: bool,
    pub max_throughput_mbps: u64,
    pub max_retries: u32,
    pub retry_delay_secs: u64,
    pub skip_on_error: bool,
    /// Path to JSONL file for logging failed/corrupted rows (optional)
    #[serde(default = "default_failed_rows_file")]
    pub failed_rows_file: String,
    // -------------------------------------------------------------------------
    // Phase 2 optimization fields (only active with --features optimized-inserts)
    // -------------------------------------------------------------------------
    /// Number of rows per UNLOGGED BATCH insert (default: 100, range: 50-200)
    #[serde(default = "default_insert_batch_size")]
    pub insert_batch_size: usize,
    /// Max concurrent in-flight inserts within a token range (default: 32)
    #[serde(default = "default_insert_concurrency")]
    pub insert_concurrency: usize,
}

fn default_failed_rows_file() -> String {
    "failed_rows.jsonl".to_string()
}

fn default_insert_batch_size() -> usize {
    100
}

fn default_insert_concurrency() -> usize {
    32
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    pub name: String,
    /// Partition key columns - if empty, will be auto-discovered from schema
    #[serde(default)]
    pub partition_key: Vec<String>,
}

pub fn load_config(path: &str) -> Result<SSTableLoaderConfig> {
    let config = Config::builder()
        .add_source(File::with_name(path))
        .add_source(config::Environment::with_prefix("SSTABLE_LOADER"))
        .build()?;
    
    Ok(config.try_deserialize()?)
}
