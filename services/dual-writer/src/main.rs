mod writer;
mod config;
mod api;
mod health;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "dual-writer")]
#[command(about = "ScyllaDB dual-writer proxy for zero-downtime migration")]
struct Args {
    #[arg(short, long, default_value = "config.yaml")]
    config: String,
    
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
    
    // Initialize dual writer
    let writer = Arc::new(writer::DualWriter::new(config.clone()).await?);
    
    // Start background tasks
    let writer_clone = writer.clone();
    tokio::spawn(async move {
        writer_clone.retry_failed_writes_loop().await;
    });
    
    // Start API server
    api::start_server(writer, args.port).await?;
    
    Ok(())
}