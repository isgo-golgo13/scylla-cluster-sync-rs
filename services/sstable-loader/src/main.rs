mod loader;
mod config;
mod api;
mod token_range;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "sstable-loader")]
#[command(about = "High-performance SSTable bulk loader for ScyllaDB cluster migration")]
struct Args {
    #[arg(short, long, default_value = "config/sstable-loader.yaml")]
    config: String,
    
    #[arg(short, long, default_value = "8081")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sstable_loader=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    let args = Args::parse();
    info!("Starting SSTable-Loader service on port {}", args.port);
    
    // Load configuration
    let config = config::load_config(&args.config)?;
    
    // Initialize loader
    let loader = Arc::new(loader::SSTableLoader::new(config.clone()).await?);
    
    info!("SSTable-Loader initialized and ready");
    
    // Start API server
    api::start_server(loader, args.port).await?;
    
    Ok(())
}