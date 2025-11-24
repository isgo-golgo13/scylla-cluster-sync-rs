use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing::{info, warn, error};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use svckit::{
    errors::SyncError,
    database::ScyllaConnection,
};
use crate::config::{SSTableLoaderConfig, TableConfig};
use crate::token_range::{TokenRange, TokenRangeCalculator};

pub struct SSTableLoader {
    source_conn: Arc<ScyllaConnection>,
    target_conn: Arc<ScyllaConnection>,
    config: Arc<RwLock<SSTableLoaderConfig>>,
    stats: Arc<LoaderStats>,
    is_running: Arc<AtomicBool>,
    is_paused: Arc<AtomicBool>,
}

pub struct LoaderStats {
    pub total_rows: AtomicU64,
    pub migrated_rows: AtomicU64,
    pub failed_rows: AtomicU64,
    pub start_time: Instant,
}

impl LoaderStats {
    pub fn new() -> Self {
        Self {
            total_rows: AtomicU64::new(0),
            migrated_rows: AtomicU64::new(0),
            failed_rows: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }
    
    pub fn progress_percent(&self) -> f32 {
        let total = self.total_rows.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let migrated = self.migrated_rows.load(Ordering::Relaxed);
        (migrated as f32 / total as f32) * 100.0
    }
    
    pub fn throughput(&self) -> f64 {
        let migrated = self.migrated_rows.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            migrated as f64 / elapsed
        } else {
            0.0
        }
    }
}

impl SSTableLoader {
    pub async fn new(config: SSTableLoaderConfig) -> Result<Self, SyncError> {
        let source_conn = Arc::new(ScyllaConnection::new(&config.source).await?);
        let target_conn = Arc::new(ScyllaConnection::new(&config.target).await?);
        
        info!("SSTable-Loader initialized");
        
        Ok(Self {
            source_conn,
            target_conn,
            config: Arc::new(RwLock::new(config)),
            stats: Arc::new(LoaderStats::new()),
            is_running: Arc::new(AtomicBool::new(false)),
            is_paused: Arc::new(AtomicBool::new(false)),
        })
    }
    
    pub async fn start_migration(&self, tables: Option<Vec<String>>) -> Result<(), SyncError> {
        if self.is_running.load(Ordering::Relaxed) {
            return Err(SyncError::MigrationError("Migration already running".to_string()));
        }
        
        self.is_running.store(true, Ordering::Relaxed);
        info!("Starting bulk migration");
        
        let config = self.config.read().await;
        let tables_to_migrate = if let Some(tables) = tables {
            config.loader.tables.iter()
                .filter(|t| tables.contains(&t.name))
                .cloned()
                .collect()
        } else {
            config.loader.tables.clone()
        };
        drop(config);
        
        for table in tables_to_migrate {
            if !self.is_running.load(Ordering::Relaxed) {
                info!("Migration stopped");
                break;
            }
            
            info!("Migrating table: {}", table.name);
            if let Err(e) = self.migrate_table(&table).await {
                error!("Failed to migrate table {}: {}", table.name, e);
            }
        }
        
        self.is_running.store(false, Ordering::Relaxed);
        info!("Migration complete");
        
        Ok(())
    }
    
    async fn migrate_table(&self, table: &TableConfig) -> Result<(), SyncError> {
        let start = Instant::now();
        info!("Starting migration for table: {}", table.name);
        
        // Calculate token ranges
        let calculator = TokenRangeCalculator::new(self.source_conn.clone());
        let config = self.config.read().await;
        let ranges_per_core = config.loader.num_ranges_per_core;
        let max_concurrent = config.loader.max_concurrent_loaders;
        let batch_size = config.loader.batch_size;
        drop(config);
        
        let ranges = calculator.calculate_ranges(ranges_per_core).await?;
        info!("Generated {} token ranges for table {}", ranges.len(), table.name);
        
        // Create progress bar
        let multi_progress = MultiProgress::new();
        let pb = multi_progress.add(ProgressBar::new(ranges.len() as u64));
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ranges ({percent}%)")
                .unwrap()
        );
        
        // Process ranges in parallel
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
        
        for range in ranges {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let source = self.source_conn.clone();
            let target = self.target_conn.clone();
            let table_name = table.name.clone();
            let partition_key = table.partition_key[0].clone();
            let stats = self.stats.clone();
            let is_paused = self.is_paused.clone();
            let pb_clone = pb.clone();
            
            join_set.spawn(async move {
                // Wait if paused
                while is_paused.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                
                let result = Self::process_range(
                    source,
                    target,
                    &table_name,
                    &partition_key,
                    &range,
                    batch_size,
                    stats,
                ).await;
                
                pb_clone.inc(1);
                drop(permit);
                result
            });
        }
        
        // Wait for all tasks to complete
        let mut total_errors = 0;
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(_)) => {},
                Ok(Err(e)) => {
                    error!("Range processing failed: {}", e);
                    total_errors += 1;
                }
                Err(e) => {
                    error!("Task join failed: {}", e);
                    total_errors += 1;
                }
            }
        }
        
        pb.finish_with_message("Complete");
        
        let duration = start.elapsed();
        let migrated = self.stats.migrated_rows.load(Ordering::Relaxed);
        let throughput = self.stats.throughput();
        
        info!(
            "Table {} migration complete: {} rows in {:?} ({:.2} rows/sec)",
            table.name, migrated, duration, throughput
        );
        
        if total_errors > 0 {
            warn!("Migration completed with {} errors", total_errors);
        }
        
        Ok(())
    }
    
    async fn process_range(
        source: Arc<ScyllaConnection>,
        target: Arc<ScyllaConnection>,
        table: &str,
        partition_key: &str,
        range: &TokenRange,
        batch_size: usize,
        stats: Arc<LoaderStats>,
    ) -> Result<(), SyncError> {
        // Build range query
        let query = format!(
            "SELECT * FROM {} WHERE token({}) >= {} AND token({}) <= {}",
            table, partition_key, range.start, partition_key, range.end
        );
        
        // Execute query on source
        let result = source.get_session()
            .query_unpaged(&query, &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Source query failed: {}", e)))?;
        
        let row_count = result.rows_num().unwrap_or(0);
        if row_count > 0 {
            stats.total_rows.fetch_add(row_count as u64, Ordering::Relaxed);
            
            // Process in batches (simplified - in production use proper row iteration)
            let insert_query = format!("INSERT INTO {} JSON ?", table);
            
            for _ in 0..row_count {
                match target.get_session().query_unpaged(&insert_query, &[]).await {
                    Ok(_) => {
                        stats.migrated_rows.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        error!("Failed to insert row: {}", e);
                        stats.failed_rows.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    pub fn pause(&self) {
        self.is_paused.store(true, Ordering::Relaxed);
        info!("Migration paused");
    }
    
    pub fn resume(&self) {
        self.is_paused.store(false, Ordering::Relaxed);
        info!("Migration resumed");
    }
    
    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);
        info!("Migration stopped");
    }
    
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }
    
    pub fn is_paused(&self) -> bool {
        self.is_paused.load(Ordering::Relaxed)
    }
    
    pub fn get_stats(&self) -> (u64, u64, u64, f32, f64) {
        (
            self.stats.total_rows.load(Ordering::Relaxed),
            self.stats.migrated_rows.load(Ordering::Relaxed),
            self.stats.failed_rows.load(Ordering::Relaxed),
            self.stats.progress_percent(),
            self.stats.throughput(),
        )
    }
}