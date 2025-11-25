mod writer;
mod config;
mod api;
mod health;
mod filter;  // <-- Added

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use filter::FilterConfig;  // <-- Added

#[derive(Parser, Debug)]
#[command(name = "dual-writer")]
#[command(about = "ScyllaDB dual-writer proxy for zero-downtime migration")]
struct Args {
    #[arg(short, long, default_value = "config/dual-writer.yaml")]
    config: String,
    
    #[arg(short, long, default_value = "config/filter-rules.yaml")]  // <-- Added
    filter_config: String,
    
    #[arg(short, long, default_value = "8080")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "dual_writer=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    let args = Args::parse();
    info!("Starting Dual-Writer service on port {}", args.port);
    
    // Load configuration
    let config = config::load_config(&args.config)?;
    
    // Load filter configuration  // <-- Added
    let filter_config = load_filter_config(&args.filter_config)?;
    info!(
        "FilterGovernor loaded: {} tenants, {} tables blacklisted",
        filter_config.tenant_blacklist.len(),
        filter_config.table_blacklist.len()
    );
    
    // Initialize dual writer with filter
    let writer = Arc::new(
        writer::DualWriter::new(config.clone(), filter_config.clone()).await?  // <-- Modified
    );
    
    // Start background tasks
    let writer_clone = writer.clone();
    tokio::spawn(async move {
        writer_clone.retry_failed_writes_loop().await;
    });
    
    // Start filter reload task  // <-- Added
    let writer_clone = writer.clone();
    let filter_path = args.filter_config.clone();
    tokio::spawn(async move {
        filter_reload_loop(writer_clone, filter_path).await;
    });
    
    // Start API server
    api::start_server(writer, args.port).await?;
    
    Ok(())
}

// Load filter config helper  // <-- Added
fn load_filter_config(path: &str) -> Result<FilterConfig> {
    use std::fs;
    
    let contents = fs::read_to_string(path)?;
    
    #[derive(serde::Deserialize)]
    struct FilterWrapper {
        filters: FilterConfig,
    }
    
    let wrapper: FilterWrapper = serde_yaml::from_str(&contents)?;
    Ok(wrapper.filters)
}

// Hot-reload filter configuration  // <-- Added
async fn filter_reload_loop(writer: Arc<writer::DualWriter>, config_path: String) {
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
        
        match load_filter_config(&config_path) {
            Ok(new_config) => {
                info!("Reloading filter configuration");
                writer.update_filter_config(new_config).await;
            }
            Err(e) => {
                tracing::warn!("Failed to reload filter config: {}", e);
            }
        }
    }
}