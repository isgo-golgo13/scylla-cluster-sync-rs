use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Database driver: "scylla" or "cassandra" (defaults to "scylla")
    #[serde(default = "default_driver")]
    pub driver: String,
    pub hosts: Vec<String>,
    #[serde(default = "default_port")]
    pub port: u16,
    pub keyspace: String,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default = "default_connection_timeout_ms")]
    pub connection_timeout_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}

fn default_driver() -> String {
    "scylla".to_string()
}

fn default_port() -> u16 {
    9042
}

fn default_connection_timeout_ms() -> u64 {
    5000
}

fn default_request_timeout_ms() -> u64 {
    10000
}

fn default_pool_size() -> u32 {
    4
}

impl DatabaseConfig {
    pub fn connection_timeout(&self) -> Duration {
        Duration::from_millis(self.connection_timeout_ms)
    }
    
    pub fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            driver: default_driver(),
            hosts: vec!["localhost".to_string()],
            port: default_port(),
            keyspace: "system".to_string(),
            username: None,
            password: None,
            connection_timeout_ms: default_connection_timeout_ms(),
            request_timeout_ms: default_request_timeout_ms(),
            pool_size: default_pool_size(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    #[serde(default = "default_metrics_port")]
    pub metrics_port: u16,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    pub jaeger_endpoint: Option<String>,
}

fn default_metrics_port() -> u16 {
    9090
}

fn default_log_level() -> String {
    "info".to_string()
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics_port: default_metrics_port(),
            log_level: default_log_level(),
            jaeger_endpoint: None,
        }
    }
}