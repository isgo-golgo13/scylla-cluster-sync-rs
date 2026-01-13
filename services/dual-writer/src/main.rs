// services/dual-writer/src/main.rs

mod writer;
mod config;
mod cql_server;
mod filter;
mod health;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::filter::{FilterGovernor, FilterConfig};
use crate::cql_server::CqlServer;

#[derive(Parser, Debug)]
#[command(name = "dual-writer")]
#[command(about = "CQL dual-writer proxy for zero-downtime Cassandra/ScyllaDB migration")]
struct Args {
    #[arg(short, long, default_value = "config.yaml")]
    config: String,
    
    #[arg(long, default_value = "0.0.0.0:9042")]
    bind_addr: String,
    
    #[arg(long, default_value = "config/filter-rules.yaml")]
    filter_config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "dual_writer=info,cql_server=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    let args = Args::parse();
    info!("Starting CQL Dual-Writer Proxy");
    
    // Load configuration
    let config = config::load_config(&args.config)?;
    
    // Load filter configuration
    let filter_config = load_filter_config(&args.filter_config)?;
    let filter_governor = Arc::new(FilterGovernor::new(&filter_config));
    
    // Initialize dual writer with filter
    let writer = Arc::new(writer::DualWriter::new(config.clone(), filter_governor.clone()).await?);
    
    // Get source address for CQL proxying
    let source_addr = config.source_cql_addr();
    
    info!("Dual-writer initialized");
    info!("   Source: {:?} (proxy to {})", config.source.hosts, source_addr);
    info!("   Target: {:?}", config.target.hosts);
    info!("   Mode: {:?}", config.writer.mode);
    
    // Start background retry loop
    let writer_clone = writer.clone();
    tokio::spawn(async move {
        writer_clone.retry_failed_writes_loop().await;
    });
    
    // Start filter hot-reload task
    let filter_clone = filter_governor.clone();
    let filter_path = args.filter_config.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            if let Ok(new_config) = load_filter_config(&filter_path) {
                filter_clone.reload_config(&new_config).await;
            }
        }
    });
    
    // Parse bind address
    let bind_addr: SocketAddr = args.bind_addr.parse()?;
    
    // Start CQL server
    info!("🚀 Starting CQL proxy server on {}", bind_addr);
    info!("   Proxying reads to source: {}", source_addr);
    info!("   Intercepting writes for dual-write");
    info!("   Python app can now connect with cassandra-driver");
    
    let cql_server = CqlServer::new(writer, bind_addr, source_addr);
    cql_server.start().await?;
    
    Ok(())
}

fn load_filter_config(path: &str) -> Result<FilterConfig> {
    use std::fs;
    
    let contents = fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("Failed to read filter config: {}", e))?;
    
    #[derive(serde::Deserialize)]
    struct FilterYaml {
        filters: FilterConfig,
    }
    
    let yaml: FilterYaml = serde_yaml::from_str(&contents)
        .map_err(|e| anyhow::anyhow!("Failed to parse filter config: {}", e))?;
    
    info!("Filter config loaded: {} tenants, {} tables blacklisted",
          yaml.filters.tenant_blacklist.len(),
          yaml.filters.table_blacklist.len());
    
    Ok(yaml.filters)
}