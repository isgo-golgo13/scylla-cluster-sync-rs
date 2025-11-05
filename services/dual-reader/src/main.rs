mod reader;
mod config;
mod api;
mod validator;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "dual-reader")]
#[command(about = "ScyllaDB dual-reader validation service for data consistency verification")]
struct Args {
    #[arg(short, long, default_value = "config/dual-reader.yaml")]
    config: String,
    
    #[arg(short, long, default_value = "8082")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "dual_reader=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    
    let args = Args::parse();
    info!("Starting Dual-Reader validation service on port {}", args.port);
    
    // Load configuration
    let config = config::load_config(&args.config)?;
    
    // Initialize dual reader
    let reader = Arc::new(reader::DualReader::new(config.clone()).await?);
    
    // Start background validation tasks
    let reader_clone = reader.clone();
    tokio::spawn(async move {
        reader_clone.continuous_validation_loop().await;
    });
    
    // Start API server
    api::start_server(reader, args.port).await?;
    
    Ok(())
}