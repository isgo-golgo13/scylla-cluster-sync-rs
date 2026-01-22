// services/sstable-loader/src/main.rs
//
// SSTable-Loader - High-performance bulk data migration service
// With IndexManager integration for 60+ secondary indexes
// With FilterGovernor integration for tenant/table filtering
//

mod loader;
mod config;
mod api;
mod token_range;
mod index_manager;
mod filter;

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use tracing::{info, warn, error};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use index_manager::{IndexManager, ParallelIndexStrategy, load_indexes_from_config};
use filter::{FilterGovernor, load_filter_config};

#[derive(Parser, Debug)]
#[command(name = "sstable-loader")]
#[command(about = "High-performance SSTable bulk loader for ScyllaDB cluster migration")]
struct Args {
    /// Path to main configuration file
    #[arg(short, long, default_value = "config/sstable-loader.yaml")]
    config: String,
    
    /// Path to secondary indexes configuration
    #[arg(short, long, default_value = "config/indexes.yaml")]
    indexes: String,
    
    /// Path to filter rules configuration (tenant/table blacklists)
    #[arg(short, long, default_value = "config/filter-rules.yaml")]
    filter_config: String,
    
    /// API server port
    #[arg(short, long, default_value = "8081")]
    port: u16,
    
    /// Skip index management (for testing or manual control)
    #[arg(long, default_value = "false")]
    skip_indexes: bool,
    
    /// Skip filter rules (migrate everything)
    #[arg(long, default_value = "false")]
    skip_filters: bool,
    
    /// Number of parallel index operations
    #[arg(long, default_value = "4")]
    index_parallelism: usize,
    
    /// Auto-start migration on boot (otherwise wait for API trigger)
    #[arg(long, default_value = "false")]
    auto_start: bool,
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
    info!("Configuration: {}", args.config);
    info!("Index config: {}", args.indexes);
    info!("Filter config: {}", args.filter_config);
    info!("Skip indexes: {}", args.skip_indexes);
    info!("Skip filters: {}", args.skip_filters);
    
    // Load main configuration
    let config = config::load_config(&args.config)?;
    
    // Initialize FilterGovernor
    let filter_governor: Arc<FilterGovernor> = if !args.skip_filters {
        match load_filter_config(&args.filter_config) {
            Ok(filter_config) => {
                info!(
                    "Filter rules loaded: {} tenants blacklisted, {} tables blacklisted",
                    filter_config.tenant_blacklist.len(),
                    filter_config.table_blacklist.len()
                );
                Arc::new(FilterGovernor::new(&filter_config))
            }
            Err(e) => {
                warn!("Failed to load filter config: {}. Proceeding without filtering.", e);
                Arc::new(FilterGovernor::allow_all())
            }
        }
    } else {
        info!("Filtering disabled (--skip-filters flag)");
        Arc::new(FilterGovernor::allow_all())
    };
    
    // Initialize loader with filter
    let loader = Arc::new(loader::SSTableLoader::new(config.clone(), filter_governor.clone()).await?);
    
    // Initialize IndexManager (if not skipped)
    let index_manager: Option<Arc<IndexManager>> = if !args.skip_indexes {
        match load_indexes_from_config(&args.indexes) {
            Ok(indexes) => {
                info!("Loaded {} indexes from configuration", indexes.len());
                
                // Create parallel index strategy
                let strategy = Box::new(ParallelIndexStrategy::new(
                    loader.get_target_connection(),
                    args.index_parallelism,
                ));
                
                Some(Arc::new(IndexManager::new(strategy, indexes)))
            }
            Err(e) => {
                warn!("Failed to load index config: {}. Proceeding without index management.", e);
                None
            }
        }
    } else {
        info!("Index management disabled (--skip-indexes flag)");
        None
    };
    
    // Log index manager status
    if let Some(ref idx_mgr) = index_manager {
        info!(
            "IndexManager ready: {} indexes configured, parallelism={}",
            idx_mgr.index_count(),
            args.index_parallelism
        );
    }
    
    info!("SSTable-Loader initialized and ready");
    
    // Auto-start migration if requested
    if args.auto_start {
        info!("Auto-start enabled - beginning migration sequence");
        
        // Phase 1: Drop indexes
        if let Some(ref idx_mgr) = index_manager {
            info!("Phase 1: Dropping {} indexes before data load...", idx_mgr.index_count());
            match idx_mgr.drop_all().await {
                Ok(dropped) => {
                    info!("Successfully dropped {} indexes", dropped.len());
                }
                Err(e) => {
                    error!("Failed to drop indexes: {}. Migration aborted.", e);
                    return Err(e.into());
                }
            }
        }
        
        // Phase 2: Run migration
        info!("Phase 2: Starting SSTable migration...");
        match loader.start_migration(None).await {
            Ok(stats) => {
                info!("Migration completed: {:?}", stats);
                
                // Log filter stats if filtering was active
                if filter_governor.is_enabled().await {
                    let filter_stats = loader.get_filter_stats();
                    info!(
                        "Filter summary: {} tables skipped, {} rows filtered",
                        filter_stats.tables_skipped,
                        filter_stats.rows_skipped
                    );
                }
            }
            Err(e) => {
                error!("Migration failed: {}. Index rebuild skipped.", e);
                return Err(e.into());
            }
        }
        
        // Phase 3: Rebuild indexes
        if let Some(ref idx_mgr) = index_manager {
            info!("Phase 3: Rebuilding {} indexes after data load...", idx_mgr.index_count());
            match idx_mgr.rebuild_all().await {
                Ok(_) => {
                    info!("All indexes rebuilt successfully");
                }
                Err(e) => {
                    error!("Failed to rebuild indexes: {}", e);
                    return Err(e.into());
                }
            }
            
            // Verify indexes
            info!("Phase 4: Verifying indexes...");
            match idx_mgr.verify_all().await {
                Ok(true) => info!("All indexes verified successfully"),
                Ok(false) => warn!("Some indexes failed verification - manual check required"),
                Err(e) => error!("Index verification failed: {}", e),
            }
        }
        
        info!("Migration sequence completed successfully");
    }
    
    // Start API server (handles manual migration triggers)
    api::start_server(loader, index_manager, args.port).await?;
    
    Ok(())
}
