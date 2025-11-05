use anyhow::Result;
use config::{Config, File};
use serde::{Deserialize, Serialize};
use scylla_sync_shared::config::{DatabaseConfig, ObservabilityConfig};

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableConfig {
    pub name: String,
    pub partition_key: Vec<String>,
}

pub fn load_config(path: &str) -> Result<SSTableLoaderConfig> {
    let config = Config::builder()
        .add_source(File::with_name(path))
        .add_source(config::Environment::with_prefix("SSTABLE_LOADER"))
        .build()?;
    
    Ok(config.try_deserialize()?)
}