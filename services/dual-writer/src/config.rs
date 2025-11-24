use anyhow::Result;
use config::{Config, File};
use serde::{Deserialize, Serialize};
use svckit::config::{DatabaseConfig, ObservabilityConfig};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DualWriterConfig {
    pub source: DatabaseConfig,
    pub target: DatabaseConfig,
    pub writer: WriterConfig,
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterConfig {
    pub mode: WriteMode,
    pub shadow_timeout_ms: u64,
    pub retry_interval_secs: u64,
    pub max_retry_attempts: u32,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WriteMode {
    SourceOnly,
    DualAsync,
    DualSync,
    TargetOnly,
}

pub fn load_config(path: &str) -> Result<DualWriterConfig> {
    let config = Config::builder()
        .add_source(File::with_name(path))
        .add_source(config::Environment::with_prefix("DUAL_WRITER"))
        .build()?;
    
    Ok(config.try_deserialize()?)
}