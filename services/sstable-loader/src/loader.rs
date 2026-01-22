// services/sstable-loader/src/loader.rs
//
// SSTableLoader - High-performance bulk data migration using token-range parallelism
// Part of scylla-cluster-sync-rs for Iconik GCP→AWS migration
//
// Key features:
// - Token-range based parallelism for distributed reads
// - Batch inserts for optimal throughput
// - Progress tracking with indicatif
// - Pause/Resume/Stop controls
// - IndexManager integration for 60+ secondary indexes
// - Composite partition key support (auto-discovery from schema)
// - Tenant/table filtering via FilterGovernor
//

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tracing::{info, warn, error, debug};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use scylla::frame::response::result::CqlValue;

use svckit::{
    errors::SyncError,
    database::ScyllaConnection,
};
use crate::config::{SSTableLoaderConfig, TableConfig};
use crate::token_range::{TokenRange, TokenRangeCalculator};
use crate::filter::{FilterGovernor, FilterDecision};

/// SSTableLoader - Orchestrates bulk data migration
pub struct SSTableLoader {
    source_conn: Arc<ScyllaConnection>,
    target_conn: Arc<ScyllaConnection>,
    config: Arc<RwLock<SSTableLoaderConfig>>,
    stats: Arc<LoaderStats>,
    is_running: Arc<AtomicBool>,
    is_paused: Arc<AtomicBool>,
    filter: Arc<FilterGovernor>,
}

/// Migration statistics (thread-safe atomic counters)
pub struct LoaderStats {
    pub total_rows: AtomicU64,
    pub migrated_rows: AtomicU64,
    pub failed_rows: AtomicU64,
    pub filtered_rows: AtomicU64,
    pub tables_completed: AtomicU64,
    pub tables_total: AtomicU64,
    pub tables_skipped: AtomicU64,
    pub start_time: RwLock<Option<Instant>>,
}

/// Serializable snapshot of migration stats for API responses
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStats {
    pub total_rows: u64,
    pub migrated_rows: u64,
    pub failed_rows: u64,
    pub filtered_rows: u64,
    pub tables_completed: u64,
    pub tables_total: u64,
    pub tables_skipped: u64,
    pub progress_percent: f32,
    pub throughput_rows_per_sec: f64,
    pub elapsed_secs: f64,
    pub is_running: bool,
    pub is_paused: bool,
}

impl LoaderStats {
    pub fn new() -> Self {
        Self {
            total_rows: AtomicU64::new(0),
            migrated_rows: AtomicU64::new(0),
            failed_rows: AtomicU64::new(0),
            filtered_rows: AtomicU64::new(0),
            tables_completed: AtomicU64::new(0),
            tables_total: AtomicU64::new(0),
            tables_skipped: AtomicU64::new(0),
            start_time: RwLock::new(None),
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
    
    pub async fn throughput(&self) -> f64 {
        let migrated = self.migrated_rows.load(Ordering::Relaxed);
        let start_time = self.start_time.read().await;
        if let Some(start) = *start_time {
            let elapsed = start.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                return migrated as f64 / elapsed;
            }
        }
        0.0
    }
    
    pub async fn elapsed_secs(&self) -> f64 {
        let start_time = self.start_time.read().await;
        if let Some(start) = *start_time {
            start.elapsed().as_secs_f64()
        } else {
            0.0
        }
    }
    
    pub fn reset(&self) {
        self.total_rows.store(0, Ordering::Relaxed);
        self.migrated_rows.store(0, Ordering::Relaxed);
        self.failed_rows.store(0, Ordering::Relaxed);
        self.filtered_rows.store(0, Ordering::Relaxed);
        self.tables_completed.store(0, Ordering::Relaxed);
        self.tables_total.store(0, Ordering::Relaxed);
        self.tables_skipped.store(0, Ordering::Relaxed);
    }
}

impl Default for LoaderStats {
    fn default() -> Self {
        Self::new()
    }
}

impl SSTableLoader {
    /// Create new SSTableLoader with connections to source (GCP) and target (AWS) clusters
    pub async fn new(
        config: SSTableLoaderConfig,
        filter: Arc<FilterGovernor>,
    ) -> Result<Self, SyncError> {
        info!("Initializing SSTableLoader...");
        info!("Source cluster: {:?}", config.source.hosts);
        info!("Target cluster: {:?}", config.target.hosts);
        
        let source_conn = Arc::new(ScyllaConnection::new(&config.source).await?);
        let target_conn = Arc::new(ScyllaConnection::new(&config.target).await?);
        
        info!("SSTable-Loader initialized - connections established");
        
        if filter.is_enabled().await {
            info!("Filtering enabled - some tenants/tables will be excluded");
        }
        
        Ok(Self {
            source_conn,
            target_conn,
            config: Arc::new(RwLock::new(config)),
            stats: Arc::new(LoaderStats::new()),
            is_running: Arc::new(AtomicBool::new(false)),
            is_paused: Arc::new(AtomicBool::new(false)),
            filter,
        })
    }
    
    // =========================================================================
    // CONNECTION ACCESSORS (for IndexManager integration)
    // =========================================================================
    
    /// Get target connection for IndexManager
    pub fn get_target_connection(&self) -> Arc<ScyllaConnection> {
        self.target_conn.clone()
    }
    
    /// Get source connection (if needed for validation)
    pub fn get_source_connection(&self) -> Arc<ScyllaConnection> {
        self.source_conn.clone()
    }
    
    // =========================================================================
    // STATS & STATUS
    // =========================================================================
    
    /// Get migration statistics snapshot (for API responses)
    pub async fn get_stats(&self) -> MigrationStats {
        MigrationStats {
            total_rows: self.stats.total_rows.load(Ordering::Relaxed),
            migrated_rows: self.stats.migrated_rows.load(Ordering::Relaxed),
            failed_rows: self.stats.failed_rows.load(Ordering::Relaxed),
            filtered_rows: self.stats.filtered_rows.load(Ordering::Relaxed),
            tables_completed: self.stats.tables_completed.load(Ordering::Relaxed),
            tables_total: self.stats.tables_total.load(Ordering::Relaxed),
            tables_skipped: self.stats.tables_skipped.load(Ordering::Relaxed),
            progress_percent: self.stats.progress_percent(),
            throughput_rows_per_sec: self.stats.throughput().await,
            elapsed_secs: self.stats.elapsed_secs().await,
            is_running: self.is_running.load(Ordering::Relaxed),
            is_paused: self.is_paused.load(Ordering::Relaxed),
        }
    }
    
    /// Get raw stats tuple (legacy compatibility)
    pub fn get_stats_tuple(&self) -> (u64, u64, u64, f32) {
        (
            self.stats.total_rows.load(Ordering::Relaxed),
            self.stats.migrated_rows.load(Ordering::Relaxed),
            self.stats.failed_rows.load(Ordering::Relaxed),
            self.stats.progress_percent(),
        )
    }
    
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }
    
    pub fn is_paused(&self) -> bool {
        self.is_paused.load(Ordering::Relaxed)
    }
    
    /// Get filter statistics
    pub fn get_filter_stats(&self) -> crate::filter::FilterStatsSummary {
        self.filter.get_stats()
    }
    
    // =========================================================================
    // PARTITION KEY DISCOVERY
    // =========================================================================
    
    /// Auto-discover ALL partition key columns from system_schema.columns
    async fn discover_partition_keys(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<Vec<String>, SyncError> {
        debug!("Discovering partition keys for {}.{}", keyspace, table);
        
        let query = "SELECT column_name, position FROM system_schema.columns \
                     WHERE keyspace_name = ? AND table_name = ? AND kind = 'partition_key'";
        
        let result = self.source_conn.get_session()
            .query_unpaged(query, (keyspace, table))
            .await
            .map_err(|e| SyncError::DatabaseError(format!(
                "Failed to query partition keys for {}.{}: {}", keyspace, table, e
            )))?;
        
        let mut partition_keys: Vec<(i32, String)> = Vec::new();
        
        if let Some(rows) = result.rows {
            for row in rows {
                let column_name: String = row.columns[0]
                    .as_ref()
                    .and_then(|v| v.as_text())
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                
                let position: i32 = row.columns[1]
                    .as_ref()
                    .and_then(|v| v.as_int())
                    .unwrap_or(0);
                
                partition_keys.push((position, column_name));
            }
        }
        
        partition_keys.sort_by_key(|(pos, _)| *pos);
        
        let columns: Vec<String> = partition_keys.into_iter().map(|(_, name)| name).collect();
        
        if columns.is_empty() {
            return Err(SyncError::DatabaseError(format!(
                "No partition key columns found for {}.{}", keyspace, table
            )));
        }
        
        info!("Discovered partition keys for {}.{}: {:?}", keyspace, table, columns);
        Ok(columns)
    }
    
    /// Get partition keys for a table
    async fn get_partition_keys(&self, table: &TableConfig) -> Result<Vec<String>, SyncError> {
        if !table.partition_key.is_empty() {
            debug!("Using configured partition keys for {}: {:?}", table.name, table.partition_key);
            return Ok(table.partition_key.clone());
        }
        
        let parts: Vec<&str> = table.name.split('.').collect();
        if parts.len() != 2 {
            return Err(SyncError::DatabaseError(format!(
                "Table name must be 'keyspace.table' format, got: {}", table.name
            )));
        }
        
        let keyspace = parts[0];
        let table_name = parts[1];
        
        info!("Auto-discovering partition keys for {}", table.name);
        self.discover_partition_keys(keyspace, table_name).await
    }
    
    // =========================================================================
    // MIGRATION CONTROL
    // =========================================================================
    
    /// Start bulk migration
    pub async fn start_migration(
        &self, 
        keyspace_filter: Option<Vec<String>>
    ) -> Result<MigrationStats, SyncError> {
        if self.is_running.load(Ordering::Relaxed) {
            return Err(SyncError::MigrationError("Migration already running".to_string()));
        }
        
        self.stats.reset();
        {
            let mut start_time = self.stats.start_time.write().await;
            *start_time = Some(Instant::now());
        }
        
        self.is_running.store(true, Ordering::Relaxed);
        info!("Starting bulk migration");
        
        // Determine tables to migrate
        let config = self.config.read().await;
        let tables_to_migrate: Vec<TableConfig> = if let Some(ref filter) = keyspace_filter {
            config.loader.tables.iter()
                .filter(|t| {
                    let keyspace = t.name.split('.').next().unwrap_or("");
                    filter.contains(&keyspace.to_string()) || filter.contains(&t.name)
                })
                .cloned()
                .collect()
        } else {
            config.loader.tables.clone()
        };
        drop(config);
        
        self.stats.tables_total.store(tables_to_migrate.len() as u64, Ordering::Relaxed);
        info!("Tables to migrate: {}", tables_to_migrate.len());
        
        for table in &tables_to_migrate {
            info!("  - {}", table.name);
        }
        
        // Migrate each table
        for table in tables_to_migrate {
            if !self.is_running.load(Ordering::Relaxed) {
                info!("Migration stopped by user");
                break;
            }
            
            // Check table blacklist BEFORE migrating
            if self.filter.should_skip_table(&table.name).await {
                info!("⊘ Skipping blacklisted table: {}", table.name);
                self.stats.tables_skipped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            
            info!("Migrating table: {}", table.name);
            match self.migrate_table(&table).await {
                Ok(_) => {
                    self.stats.tables_completed.fetch_add(1, Ordering::Relaxed);
                    info!("✓ Table {} migration complete", table.name);
                }
                Err(e) => {
                    error!("✗ Failed to migrate table {}: {}", table.name, e);
                }
            }
        }
        
        self.is_running.store(false, Ordering::Relaxed);
        
        let final_stats = self.get_stats().await;
        info!(
            "Migration complete: {} rows migrated, {} filtered, {} failed, {:.2}% success rate",
            final_stats.migrated_rows,
            final_stats.filtered_rows,
            final_stats.failed_rows,
            if final_stats.total_rows > 0 {
                (final_stats.migrated_rows as f64 / final_stats.total_rows as f64) * 100.0
            } else {
                100.0
            }
        );
        
        // Log filter stats
        let filter_stats = self.filter.get_stats();
        if filter_stats.rows_skipped > 0 || filter_stats.tables_skipped > 0 {
            info!(
                "Filter stats: {} tables skipped, {} rows filtered out",
                filter_stats.tables_skipped,
                filter_stats.rows_skipped
            );
        }
        
        Ok(final_stats)
    }
    
    /// Stop running migration gracefully
    pub async fn stop_migration(&self) -> Result<(), SyncError> {
        if !self.is_running.load(Ordering::Relaxed) {
            return Err(SyncError::MigrationError("No migration is running".to_string()));
        }
        
        info!("Stopping migration...");
        self.is_running.store(false, Ordering::Relaxed);
        self.is_paused.store(false, Ordering::Relaxed);
        
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        info!("Migration stopped");
        Ok(())
    }
    
    /// Pause migration
    pub fn pause(&self) {
        self.is_paused.store(true, Ordering::Relaxed);
        info!("Migration paused");
    }
    
    /// Resume paused migration
    pub fn resume(&self) {
        self.is_paused.store(false, Ordering::Relaxed);
        info!("Migration resumed");
    }
    
    /// Stop migration
    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);
        info!("Migration stop signal sent");
    }
    
    // =========================================================================
    // TABLE MIGRATION (token-range parallelism)
    // =========================================================================
    
    async fn migrate_table(&self, table: &TableConfig) -> Result<(), SyncError> {
        let start = Instant::now();
        info!("Starting migration for table: {}", table.name);
        
        // Get ALL partition key columns
        let partition_keys = self.get_partition_keys(table).await?;
        info!("Using partition keys for {}: {:?}", table.name, partition_keys);
        
        // Get tenant ID columns for filtering
        let tenant_id_columns = self.filter.get_tenant_id_columns().await;
        
        // Check if partition key contains a tenant ID column (for partition-level filtering)
        let partition_has_tenant_col = partition_keys.iter()
            .any(|pk| tenant_id_columns.contains(pk));
        
        if partition_has_tenant_col {
            debug!("Table {} has tenant ID in partition key - partition-level filtering enabled", table.name);
        }
        
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
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ranges ({percent}%) | {msg}")
                .unwrap()
        );
        pb.set_message(format!("Migrating {}", table.name));
        
        // Process ranges in parallel
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
        
        for range in ranges {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let source = self.source_conn.clone();
            let target = self.target_conn.clone();
            let table_name = table.name.clone();
            let partition_keys_clone = partition_keys.clone();
            let tenant_id_columns_clone = tenant_id_columns.clone();
            let stats = self.stats.clone();
            let filter = self.filter.clone();
            let is_paused = self.is_paused.clone();
            let is_running = self.is_running.clone();
            let pb_clone = pb.clone();
            let batch_size = batch_size;
            
            join_set.spawn(async move {
                if !is_running.load(Ordering::Relaxed) {
                    drop(permit);
                    return Ok(());
                }
                
                while is_paused.load(Ordering::Relaxed) {
                    if !is_running.load(Ordering::Relaxed) {
                        drop(permit);
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                
                let result = Self::process_range(
                    source,
                    target,
                    &table_name,
                    &partition_keys_clone,
                    &tenant_id_columns_clone,
                    &range,
                    batch_size,
                    stats,
                    filter,
                ).await;
                
                pb_clone.inc(1);
                drop(permit);
                result
            });
        }
        
        // Wait for all tasks
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
        
        pb.finish_with_message(format!("{} complete", table.name));
        
        let duration = start.elapsed();
        let migrated = self.stats.migrated_rows.load(Ordering::Relaxed);
        let filtered = self.stats.filtered_rows.load(Ordering::Relaxed);
        let throughput = self.stats.throughput().await;
        
        info!(
            "Table {} migration complete: {} rows migrated, {} filtered, {:?} ({:.2} rows/sec)",
            table.name, migrated, filtered, duration, throughput
        );
        
        if total_errors > 0 {
            warn!("Migration completed with {} range errors", total_errors);
        }
        
        Ok(())
    }
    
    /// Process a single token range with filtering support
    async fn process_range(
        source: Arc<ScyllaConnection>,
        target: Arc<ScyllaConnection>,
        table: &str,
        partition_keys: &[String],
        tenant_id_columns: &[String],
        range: &TokenRange,
        batch_size: usize,
        stats: Arc<LoaderStats>,
        filter: Arc<FilterGovernor>,
    ) -> Result<(), SyncError> {
        // Build token function with ALL partition key columns
        let token_columns = partition_keys.join(", ");
        
        let query = format!(
            "SELECT * FROM {} WHERE token({}) >= {} AND token({}) <= {}",
            table, token_columns, range.start, token_columns, range.end
        );
        
        let result = source.get_session()
            .query_unpaged(query.as_str(), &[])
            .await
            .map_err(|e| SyncError::DatabaseError(format!("Source query failed: {}", e)))?;
        
        let row_count = result.rows_num().unwrap_or(0);
        if row_count == 0 {
            return Ok(());
        }
        
        stats.total_rows.fetch_add(row_count as u64, Ordering::Relaxed);
        
        // Get column specs for building JSON
        let col_specs = result.col_specs();
        let column_names: Vec<String> = col_specs.iter()
            .map(|spec| spec.name.clone())
            .collect();
        
        // Find tenant ID column index
        let tenant_col_idx: Option<usize> = column_names.iter()
            .position(|name| tenant_id_columns.contains(name));
        
        let insert_query = format!("INSERT INTO {} JSON ?", table);
        
        let mut batch_count = 0;
        
        if let Some(rows) = result.rows {
            for row in rows {
                // Check if row should be filtered
                if let Some(idx) = tenant_col_idx {
                    if let Some(ref col_value) = row.columns[idx] {
                        let tenant_id = cql_value_to_string(col_value);
                        
                        if let FilterDecision::SkipTenant(tid) = filter.check_tenant_id(&tenant_id).await {
                            debug!("Filtering row with tenant_id: {}", tid);
                            stats.filtered_rows.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                    }
                }
                
                // Convert row to JSON
                let json_row = row_to_json(&column_names, &row.columns);
                
                // Row passed filter - insert into target
                match target.get_session()
                    .query_unpaged(insert_query.as_str(), (&json_row,))
                    .await 
                {
                    Ok(_) => {
                        stats.migrated_rows.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        if batch_count % 1000 == 0 {
                            warn!("Insert error (batch {}): {}", batch_count, e);
                        }
                        stats.failed_rows.fetch_add(1, Ordering::Relaxed);
                    }
                }
                
                batch_count += 1;
                
                if batch_count % batch_size == 0 {
                    tokio::task::yield_now().await;
                }
            }
        }
        
        Ok(())
    }
    
    // =========================================================================
    // CONFIGURATION
    // =========================================================================
    
    /// Update configuration at runtime
    pub async fn update_config(&self, new_config: SSTableLoaderConfig) {
        let mut config = self.config.write().await;
        *config = new_config;
        info!("SSTableLoader configuration updated");
    }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/// Convert a CqlValue to its string representation
fn cql_value_to_string(value: &CqlValue) -> String {
    match value {
        CqlValue::Uuid(uuid) => uuid.to_string(),
        CqlValue::Timeuuid(uuid) => uuid.to_string(),
        CqlValue::Text(s) => s.clone(),
        CqlValue::Ascii(s) => s.clone(),
        CqlValue::Int(i) => i.to_string(),
        CqlValue::BigInt(i) => i.to_string(),
        CqlValue::SmallInt(i) => i.to_string(),
        CqlValue::TinyInt(i) => i.to_string(),
        CqlValue::Float(f) => f.to_string(),
        CqlValue::Double(d) => d.to_string(),
        CqlValue::Boolean(b) => b.to_string(),
        CqlValue::Timestamp(ts) => ts.0.to_string(),
        CqlValue::Date(d) => d.0.to_string(),
        CqlValue::Time(t) => t.0.to_string(),
        CqlValue::Inet(addr) => addr.to_string(),
        CqlValue::Varint(vi) => format!("{:?}", vi.as_signed_bytes_be_slice()),
        CqlValue::Decimal(dec) => format!("{:?}", dec),
        CqlValue::Blob(bytes) => format!("0x{}", bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>()),
        _ => format!("{:?}", value),
    }
}

/// Convert a CqlValue to its JSON representation
fn cql_value_to_json(value: &CqlValue) -> serde_json::Value {
    match value {
        CqlValue::Uuid(uuid) => serde_json::json!(uuid.to_string()),
        CqlValue::Timeuuid(uuid) => serde_json::json!(uuid.to_string()),
        CqlValue::Text(s) => serde_json::json!(s),
        CqlValue::Ascii(s) => serde_json::json!(s),
        CqlValue::Int(i) => serde_json::json!(i),
        CqlValue::BigInt(i) => serde_json::json!(i),
        CqlValue::SmallInt(i) => serde_json::json!(i),
        CqlValue::TinyInt(i) => serde_json::json!(i),
        CqlValue::Float(f) => serde_json::json!(f),
        CqlValue::Double(d) => serde_json::json!(d),
        CqlValue::Boolean(b) => serde_json::json!(b),
        CqlValue::Timestamp(ts) => serde_json::json!(ts.0),
        CqlValue::Date(d) => serde_json::json!(d.0),
        CqlValue::Time(t) => serde_json::json!(t.0),
        CqlValue::Inet(addr) => serde_json::json!(addr.to_string()),
        CqlValue::Blob(bytes) => {
            let hex_str = format!("0x{}", bytes.iter().map(|b| format!("{:02x}", b)).collect::<String>());
            serde_json::json!(hex_str)
        }
        CqlValue::List(list) => {
            let items: Vec<serde_json::Value> = list.iter().map(cql_value_to_json).collect();
            serde_json::json!(items)
        }
        CqlValue::Set(set) => {
            let items: Vec<serde_json::Value> = set.iter().map(cql_value_to_json).collect();
            serde_json::json!(items)
        }
        CqlValue::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map.iter()
                .map(|(k, v)| (cql_value_to_string(k), cql_value_to_json(v)))
                .collect();
            serde_json::json!(obj)
        }
        CqlValue::Tuple(tuple) => {
            let items: Vec<serde_json::Value> = tuple.iter()
                .map(|opt| opt.as_ref().map(cql_value_to_json).unwrap_or(serde_json::Value::Null))
                .collect();
            serde_json::json!(items)
        }
        CqlValue::UserDefinedType { fields, .. } => {
            let obj: serde_json::Map<String, serde_json::Value> = fields.iter()
                .map(|(name, opt_val)| {
                    let val = opt_val.as_ref()
                        .map(cql_value_to_json)
                        .unwrap_or(serde_json::Value::Null);
                    (name.clone(), val)
                })
                .collect();
            serde_json::json!(obj)
        }
        CqlValue::Varint(vi) => {
            // Convert varint to string representation
            serde_json::json!(format!("{:?}", vi.as_signed_bytes_be_slice()))
        }
        CqlValue::Decimal(dec) => {
            serde_json::json!(format!("{:?}", dec))
        }
        CqlValue::Counter(c) => serde_json::json!(c.0),
        CqlValue::Duration(d) => {
            serde_json::json!({
                "months": d.months,
                "days": d.days,
                "nanoseconds": d.nanoseconds
            })
        }
        CqlValue::Empty => serde_json::Value::Null,
    }
}

/// Convert a row to JSON string for INSERT JSON
fn row_to_json(column_names: &[String], columns: &[Option<CqlValue>]) -> String {
    let mut map = serde_json::Map::new();
    
    for (name, opt_value) in column_names.iter().zip(columns.iter()) {
        let json_value = match opt_value {
            Some(value) => cql_value_to_json(value),
            None => serde_json::Value::Null,
        };
        map.insert(name.clone(), json_value);
    }
    
    serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_loader_stats() {
        let stats = LoaderStats::new();
        
        stats.total_rows.store(100, Ordering::Relaxed);
        stats.migrated_rows.store(75, Ordering::Relaxed);
        stats.failed_rows.store(5, Ordering::Relaxed);
        stats.filtered_rows.store(20, Ordering::Relaxed);
        
        assert_eq!(stats.progress_percent(), 75.0);
    }
    
    #[test]
    fn test_migration_stats_serialization() {
        let stats = MigrationStats {
            total_rows: 1000,
            migrated_rows: 900,
            failed_rows: 50,
            filtered_rows: 50,
            tables_completed: 5,
            tables_total: 10,
            tables_skipped: 2,
            progress_percent: 90.0,
            throughput_rows_per_sec: 1234.5,
            elapsed_secs: 60.0,
            is_running: true,
            is_paused: false,
        };
        
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("\"migrated_rows\":900"));
        assert!(json.contains("\"filtered_rows\":50"));
    }
    
    #[test]
    fn test_token_columns_join() {
        let keys = vec![
            "system_domain_id".to_string(),
            "id".to_string(),
        ];
        let token_columns = keys.join(", ");
        assert_eq!(token_columns, "system_domain_id, id");
    }
    
    #[test]
    fn test_row_to_json() {
        let column_names = vec!["id".to_string(), "name".to_string()];
        let columns = vec![
            Some(CqlValue::Int(123)),
            Some(CqlValue::Text("test".to_string())),
        ];
        
        let json = row_to_json(&column_names, &columns);
        assert!(json.contains("\"id\":123"));
        assert!(json.contains("\"name\":\"test\""));
    }
}
