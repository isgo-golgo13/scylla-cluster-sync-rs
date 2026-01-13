use anyhow::Result;
use config::{Config, File};
use serde::{Deserialize, Serialize};
use svckit::config::{DatabaseConfig, ObservabilityConfig};
use std::net::SocketAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DualWriterConfig {
    pub source: DatabaseConfig,
    pub target: DatabaseConfig,
    pub writer: WriterConfig,
    pub observability: ObservabilityConfig,
}

impl DualWriterConfig {
    /// Get the source cluster address for CQL proxying
    pub fn source_cql_addr(&self) -> SocketAddr {
        let host = self.source.hosts.first()
            .map(|s| s.as_str())
            .unwrap_or("127.0.0.1");
        let port = self.source.port;
        
        format!("{}:{}", host, port)
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 9042)))
    }

    /// Get the target cluster address for dual-writing
    pub fn target_cql_addr(&self) -> SocketAddr {
        let host = self.target.hosts.first()
            .map(|s| s.as_str())
            .unwrap_or("127.0.0.1");
        let port = self.target.port;
        
        format!("{}:{}", host, port)
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 9042)))
    }
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