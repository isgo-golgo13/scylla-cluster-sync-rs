use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub hosts: Vec<String>,
    pub port: u16,
    pub keyspace: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connection_timeout: Duration,
    pub request_timeout: Duration,
    pub pool_size: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            hosts: vec!["localhost".to_string()],
            port: 9042,
            keyspace: "system".to_string(),
            username: None,
            password: None,
            connection_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            pool_size: 4,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    pub metrics_port: u16,
    pub log_level: String,
    pub jaeger_endpoint: Option<String>,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            metrics_port: 9090,
            log_level: "info".to_string(),
            jaeger_endpoint: None,
        }
    }
}
