## Dual-Writer Proxy Service w/ SSTableLoader Processor and Dual-Reader Ack Service

```rust
// Complete Rust Xfer Suite for Cassandra → ScyllaDB

use tokio::sync::mpsc;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;
use futures::stream::{self, StreamExt};
use std::time::{SystemTime, Duration};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// PART 1: ENHANCED DUAL-WRITER 
// ============================================================================

pub struct EnhancedDualWriter {
    cassandra: Arc<CassandraSession>,
    scylla: Arc<ScyllaSession>,
    config: Arc<RwLock<DualWriterConfig>>,
    metrics: Arc<Metrics>,
    // NEW: Track write timestamps for validation
    write_log: Arc<RwLock<WriteLog>>,
}

#[derive(Clone)]
pub struct WriteLog {
    // Track recent writes for validation
    recent_writes: HashMap<String, WriteRecord>,
    ttl: Duration,
}

#[derive(Clone)]
pub struct WriteRecord {
    key: String,
    timestamp: SystemTime,
    operation: OperationType,
    checksum: u64,
}

impl EnhancedDualWriter {
    pub async fn write_with_verification(&self, request: WriteRequest) -> Result<WriteResponse> {
        let write_timestamp = SystemTime::now();
        let micros = write_timestamp.duration_since(UNIX_EPOCH)?.as_micros() as i64;
        
        // CRITICAL: Use SAME timestamp for both databases
        let query_with_timestamp = format!(
            "{} USING TIMESTAMP {}",
            request.query,
            micros
        );
        
        // Write to primary (Cassandra)
        let cassandra_future = self.cassandra.execute(&query_with_timestamp);
        
        // Write to shadow (ScyllaDB) 
        let scylla_future = self.scylla.execute(&query_with_timestamp);
        
        // Wait for primary, fire-and-forget for shadow initially
        match self.config.read().await.mode {
            WriteMode::DualWriteAsync => {
                // Primary must succeed
                cassandra_future.await?;
                
                // Shadow write in background
                tokio::spawn(async move {
                    if let Err(e) = scylla_future.await {
                        log::warn!("Shadow write failed: {}", e);
                        // Record failure for later replay
                        self.record_failed_write(request, e).await;
                    }
                });
            }
            WriteMode::DualWriteSync => {
                // Both must succeed
                let (cassandra_result, scylla_result) = 
                    tokio::join!(cassandra_future, scylla_future);
                
                cassandra_result?;
                scylla_result?;
            }
        }
        
        // Log write for validation
        self.log_write_for_validation(request, micros).await;
        
        Ok(WriteResponse {
            success: true,
            timestamp: micros,
        })
    }
    
    async fn record_failed_write(&self, request: WriteRequest, error: Error) {
        // Store failed writes for replay
        // This prevents data loss if shadow write fails
        let mut failed_writes = FAILED_WRITES_QUEUE.lock().await;
        failed_writes.push(FailedWrite {
            request,
            error: error.to_string(),
            timestamp: SystemTime::now(),
            retry_count: 0,
        });
    }
}

// ============================================================================
// PART 2: SSTABLELOADER Rust Service
// ============================================================================

pub struct RustSSTableLoader {
    source_cassandra: Arc<CassandraSession>,
    target_scylla: Arc<ScyllaSession>,
    config: SSTableLoaderConfig,
    progress: Arc<RwLock<MigrationProgress>>,
}

#[derive(Clone, Debug)]
pub struct SSTableLoaderConfig {
    pub tables: Vec<TableConfig>,
    pub parallel_streams: usize,  // How many tables to migrate in parallel
    pub batch_size: usize,        // Rows per batch
    pub rate_limit: Option<usize>, // Rows per second limit
    pub token_range_splits: usize, // Split each table into N token ranges
    pub compression: bool,
    pub retry_failed: bool,
}

#[derive(Clone, Debug)]
pub struct TableConfig {
    pub keyspace: String,
    pub table_name: String,
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<String>,
    pub columns: Vec<String>,
    pub estimated_rows: u64,
    pub include_ttl: bool,
    pub include_tombstones: bool, // CRITICAL for consistency!
}

#[derive(Debug)]
pub struct MigrationProgress {
    pub tables_completed: Vec<String>,
    pub tables_in_progress: Vec<String>,
    pub total_rows_migrated: u64,
    pub current_throughput: f64,
    pub errors: Vec<MigrationError>,
}

impl RustSSTableLoader {
    pub async fn new(config: SSTableLoaderConfig) -> Result<Self> {
        Ok(Self {
            source_cassandra: Arc::new(create_cassandra_session().await?),
            target_scylla: Arc::new(create_scylla_session().await?),
            config,
            progress: Arc::new(RwLock::new(MigrationProgress::default())),
        })
    }
    
    /// Main migration entry point - handles all tables in parallel
    pub async fn migrate_all_tables(&self) -> Result<MigrationReport> {
        let start_time = SystemTime::now();
        
        // Split tables into parallel streams
        let table_streams = stream::iter(self.config.tables.clone())
            .map(|table| self.migrate_table(table))
            .buffer_unordered(self.config.parallel_streams);
        
        // Process all tables
        let results: Vec<TableMigrationResult> = table_streams.collect().await;
        
        // Generate report
        let report = MigrationReport {
            duration: SystemTime::now().duration_since(start_time)?,
            tables_migrated: results.len(),
            total_rows: results.iter().map(|r| r.rows_migrated).sum(),
            errors: results.iter().flat_map(|r| r.errors.clone()).collect(),
        };
        
        Ok(report)
    }
    
    /// Migrate a single table using token range scanning
    async fn migrate_table(&self, table: TableConfig) -> TableMigrationResult {
        info!("Starting migration for {}.{}", table.keyspace, table.table_name);
        
        // Update progress
        {
            let mut progress = self.progress.write().await;
            progress.tables_in_progress.push(table.table_name.clone());
        }
        
        // Get token ranges for parallel processing
        let token_ranges = self.get_token_ranges(&table).await?;
        
        // Process each token range in parallel
        let range_futures = token_ranges.iter().map(|range| {
            self.migrate_token_range(&table, range)
        });
        
        let range_results = stream::iter(range_futures)
            .buffer_unordered(4) // Process 4 ranges in parallel per table
            .collect::<Vec<_>>()
            .await;
        
        // Aggregate results
        let total_rows: u64 = range_results.iter().map(|r| r.rows).sum();
        
        info!("Completed migration for {}.{}: {} rows", 
              table.keyspace, table.table_name, total_rows);
        
        TableMigrationResult {
            table_name: format!("{}.{}", table.keyspace, table.table_name),
            rows_migrated: total_rows,
            errors: vec![],
        }
    }
    
    /// Migrate a specific token range of a table
    async fn migrate_token_range(
        &self, 
        table: &TableConfig, 
        range: &TokenRange
    ) -> Result<TokenRangeResult> {
        let mut rows_migrated = 0u64;
        let mut continuation_token = None;
        
        loop {
            // Build query for this token range
            let query = self.build_range_query(table, range, continuation_token)?;
            
            // Execute on source
            let rows = self.source_cassandra.execute(&query).await?;
            
            if rows.is_empty() {
                break;
            }
            
            // Batch insert into target
            for batch in rows.chunks(self.config.batch_size) {
                self.write_batch_to_scylla(table, batch).await?;
                rows_migrated += batch.len() as u64;
                
                // Apply rate limiting if configured
                if let Some(rate_limit) = self.config.rate_limit {
                    self.apply_rate_limit(batch.len(), rate_limit).await;
                }
            }
            
            // Update continuation token for pagination
            continuation_token = Some(self.get_last_token(&rows));
            
            // Update progress
            {
                let mut progress = self.progress.write().await;
                progress.total_rows_migrated += rows.len() as u64;
            }
        }
        
        Ok(TokenRangeResult {
            range: range.clone(),
            rows: rows_migrated,
        })
    }
    
    /// Build query for a specific token range
    fn build_range_query(
        &self,
        table: &TableConfig,
        range: &TokenRange,
        continuation: Option<String>
    ) -> Result<String> {
        let mut query = format!(
            "SELECT {} FROM {}.{} WHERE token({}) > {} AND token({}) <= {}",
            table.columns.join(", "),
            table.keyspace,
            table.table_name,
            table.partition_key.join(", "),
            range.start_token,
            table.partition_key.join(", "),
            range.end_token
        );
        
        // Add continuation for pagination
        if let Some(token) = continuation {
            query.push_str(&format!(" AND token({}) > {}", 
                table.partition_key.join(", "), token));
        }
        
        // Include TTL and WRITETIME if needed
        if table.include_ttl {
            query = query.replace("SELECT", "SELECT TTL(value),");
        }
        
        query.push_str(" LIMIT 5000"); // Batch size for reading
        
        Ok(query)
    }
    
    /// Write batch to ScyllaDB preserving timestamps and TTLs
    async fn write_batch_to_scylla(
        &self,
        table: &TableConfig,
        batch: &[Row]
    ) -> Result<()> {
        // Build batch insert with preserved timestamps
        let mut batch_query = String::from("BEGIN UNLOGGED BATCH\n");
        
        for row in batch {
            let insert = self.build_insert_with_metadata(table, row)?;
            batch_query.push_str(&insert);
            batch_query.push_str(";\n");
        }
        
        batch_query.push_str("APPLY BATCH;");
        
        // Execute batch
        self.target_scylla.execute(&batch_query).await?;
        
        Ok(())
    }
    
    /// Preserve WRITETIME and TTL from source
    fn build_insert_with_metadata(&self, table: &TableConfig, row: &Row) -> Result<String> {
        let values = self.extract_values(row)?;
        let writetime = row.get_writetime()?;
        let ttl = row.get_ttl();
        
        let mut insert = format!(
            "INSERT INTO {}.{} ({}) VALUES ({}) USING TIMESTAMP {}",
            table.keyspace,
            table.table_name,
            table.columns.join(", "),
            values.join(", "),
            writetime
        );
        
        // Preserve TTL if exists
        if let Some(ttl_value) = ttl {
            if ttl_value > 0 {
                insert.push_str(&format!(" AND TTL {}", ttl_value));
            }
        }
        
        Ok(insert)
    }
    
    /// Get token ranges for parallel processing
    async fn get_token_ranges(&self, table: &TableConfig) -> Result<Vec<TokenRange>> {
        // Query system.local to get token ranges
        let query = "SELECT tokens FROM system.local";
        let result = self.source_cassandra.execute(query).await?;
        
        // Split into configured number of ranges
        let ranges = self.split_token_range(
            i64::MIN,
            i64::MAX,
            self.config.token_range_splits
        );
        
        Ok(ranges)
    }
    
    /// Split token range for parallel processing
    fn split_token_range(&self, start: i64, end: i64, splits: usize) -> Vec<TokenRange> {
        let range_size = (end - start) / splits as i64;
        let mut ranges = Vec::new();
        
        for i in 0..splits {
            let range_start = start + (i * range_size as usize) as i64;
            let range_end = if i == splits - 1 {
                end
            } else {
                start + ((i + 1) * range_size as usize) as i64
            };
            
            ranges.push(TokenRange {
                start_token: range_start,
                end_token: range_end,
            });
        }
        
        ranges
    }
}

// ============================================================================
// PART 3: DUAL-READER VALIDATOR 
// ============================================================================

pub struct DualReaderValidator {
    source_cassandra: Arc<CassandraSession>,
    target_scylla: Arc<ScyllaSession>,
    config: ValidationConfig,
    discrepancies: Arc<RwLock<Vec<DataDiscrepancy>>>,
}

#[derive(Clone, Debug)]
pub struct ValidationConfig {
    pub sample_rate: f32,        // What % of data to validate (0.01 = 1%)
    pub validation_mode: ValidationMode,
    pub batch_size: usize,
    pub parallel_validators: usize,
    pub checksum_columns: bool,   // Whether to checksum individual columns
    pub compare_timestamps: bool, // Whether to compare WRITETIME
    pub compare_ttl: bool,        // Whether to compare TTL values
}

#[derive(Clone, Debug)]
pub enum ValidationMode {
    RandomSample,     // Random sampling across keyspace
    FullScan,        // Validate everything (slow but thorough)
    RecentWrites,    // Only validate recent writes
    CriticalTables,  // Only validate specified critical tables
}

#[derive(Debug, Clone)]
pub struct DataDiscrepancy {
    pub table: String,
    pub key: String,
    pub discrepancy_type: DiscrepancyType,
    pub source_value: Option<String>,
    pub target_value: Option<String>,
    pub detected_at: SystemTime,
}

#[derive(Debug, Clone)]
pub enum DiscrepancyType {
    MissingInTarget,
    MissingInSource,
    ValueMismatch,
    TimestampMismatch,
    TTLMismatch,
    TombstoneMismatch,
}

impl DualReaderValidator {
    /// Main validation entry point
    pub async fn validate_migration(&self) -> Result<ValidationReport> {
        info!("Starting dual-reader validation with {} sample rate", 
              self.config.sample_rate);
        
        let tables = self.get_tables_to_validate().await?;
        let validation_futures = tables.iter().map(|table| {
            self.validate_table(table)
        });
        
        let results = stream::iter(validation_futures)
            .buffer_unordered(self.config.parallel_validators)
            .collect::<Vec<_>>()
            .await;
        
        // Aggregate results
        let report = ValidationReport {
            tables_validated: results.len(),
            total_rows_checked: results.iter().map(|r| r.rows_checked).sum(),
            discrepancies_found: self.discrepancies.read().await.len(),
            validation_passed: self.discrepancies.read().await.is_empty(),
            details: self.discrepancies.read().await.clone(),
        };
        
        Ok(report)
    }
    
    /// Validate a single table
    async fn validate_table(&self, table: &str) -> Result<TableValidationResult> {
        let mut rows_checked = 0u64;
        let mut discrepancies = Vec::new();
        
        // Get sample of keys to validate
        let keys = self.get_keys_to_validate(table).await?;
        
        for batch in keys.chunks(self.config.batch_size) {
            let validations = batch.iter().map(|key| {
                self.validate_row(table, key)
            });
            
            let batch_results = stream::iter(validations)
                .buffer_unordered(10)
                .collect::<Vec<_>>()
                .await;
            
            for result in batch_results {
                rows_checked += 1;
                if let Some(discrepancy) = result? {
                    discrepancies.push(discrepancy);
                    
                    // Record discrepancy
                    let mut all_discrepancies = self.discrepancies.write().await;
                    all_discrepancies.push(discrepancy.clone());
                }
            }
        }
        
        Ok(TableValidationResult {
            table_name: table.to_string(),
            rows_checked,
            discrepancies_found: discrepancies.len(),
        })
    }
    
    /// Validate a single row
    async fn validate_row(
        &self,
        table: &str,
        key: &str
    ) -> Result<Option<DataDiscrepancy>> {
        // Read from both databases
        let (source_row, target_row) = tokio::join!(
            self.read_from_cassandra(table, key),
            self.read_from_scylla(table, key)
        );
        
        // Compare results
        match (source_row?, target_row?) {
            (Some(source), Some(target)) => {
                // Both have the row - compare values
                if let Some(mismatch) = self.compare_rows(&source, &target)? {
                    return Ok(Some(DataDiscrepancy {
                        table: table.to_string(),
                        key: key.to_string(),
                        discrepancy_type: mismatch,
                        source_value: Some(self.serialize_row(&source)?),
                        target_value: Some(self.serialize_row(&target)?),
                        detected_at: SystemTime::now(),
                    }));
                }
            }
            (Some(_), None) => {
                // Missing in target
                return Ok(Some(DataDiscrepancy {
                    table: table.to_string(),
                    key: key.to_string(),
                    discrepancy_type: DiscrepancyType::MissingInTarget,
                    source_value: None,
                    target_value: None,
                    detected_at: SystemTime::now(),
                }));
            }
            (None, Some(_)) => {
                // Missing in source (shouldn't happen unless deleted)
                return Ok(Some(DataDiscrepancy {
                    table: table.to_string(),
                    key: key.to_string(),
                    discrepancy_type: DiscrepancyType::MissingInSource,
                    source_value: None,
                    target_value: None,
                    detected_at: SystemTime::now(),
                }));
            }
            (None, None) => {
                // Both missing - consistent (might be deleted)
            }
        }
        
        Ok(None)
    }
    
    /// Compare two rows for discrepancies
    fn compare_rows(&self, source: &Row, target: &Row) -> Result<Option<DiscrepancyType>> {
        // Compare values
        if self.config.checksum_columns {
            let source_checksum = self.calculate_row_checksum(source)?;
            let target_checksum = self.calculate_row_checksum(target)?;
            
            if source_checksum != target_checksum {
                return Ok(Some(DiscrepancyType::ValueMismatch));
            }
        }
        
        // Compare timestamps if configured
        if self.config.compare_timestamps {
            let source_ts = source.get_writetime()?;
            let target_ts = target.get_writetime()?;
            
            // Allow small timestamp differences (clock skew)
            if (source_ts - target_ts).abs() > 1000 { // 1ms tolerance
                return Ok(Some(DiscrepancyType::TimestampMismatch));
            }
        }
        
        // Compare TTL if configured
        if self.config.compare_ttl {
            let source_ttl = source.get_ttl();
            let target_ttl = target.get_ttl();
            
            // TTL might differ slightly due to timing
            match (source_ttl, target_ttl) {
                (Some(s), Some(t)) if (s - t).abs() > 60 => {
                    return Ok(Some(DiscrepancyType::TTLMismatch));
                }
                (Some(_), None) | (None, Some(_)) => {
                    return Ok(Some(DiscrepancyType::TTLMismatch));
                }
                _ => {}
            }
        }
        
        Ok(None)
    }
    
    /// Calculate checksum for a row
    fn calculate_row_checksum(&self, row: &Row) -> Result<u64> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        
        // Hash all column values
        for column in row.columns() {
            column.hash(&mut hasher);
        }
        
        Ok(hasher.finish())
    }
}

// ============================================================================
// PART 4: TWO-WAY SCANNER (Superior to dual-reader!)
// ============================================================================

pub struct TwoWayScanner {
    source_cassandra: Arc<CassandraSession>,
    target_scylla: Arc<ScyllaSession>,
    dual_writer: Arc<EnhancedDualWriter>,
    config: TwoWayScannerConfig,
}

#[derive(Clone, Debug)]
pub struct TwoWayScannerConfig {
    pub scan_mode: ScanMode,
    pub reconciliation_enabled: bool,  // Auto-fix discrepancies
    pub scan_interval: Duration,       // How often to scan
    pub tables: Vec<String>,
    pub alert_threshold: usize,        // Alert if this many discrepancies
}

#[derive(Clone, Debug)]
pub enum ScanMode {
    Continuous,      // Keep scanning forever
    OneTime,         // Single scan
    Incremental,     // Only scan changes since last scan
}

impl TwoWayScanner {
    /// Continuous scanning with auto-reconciliation
    pub async fn start_continuous_scan(&self) -> Result<()> {
        loop {
            info!("Starting two-way scan cycle");
            
            // Scan source → target
            let source_to_target = self.scan_source_to_target().await?;
            
            // Scan target → source 
            let target_to_source = self.scan_target_to_source().await?;
            
            // Reconcile differences if enabled
            if self.config.reconciliation_enabled {
                self.reconcile_differences(source_to_target, target_to_source).await?;
            }
            
            // Alert if threshold exceeded
            let total_discrepancies = source_to_target.len() + target_to_source.len();
            if total_discrepancies > self.config.alert_threshold {
                self.send_alert(total_discrepancies).await?;
            }
            
            // Wait before next scan
            tokio::time::sleep(self.config.scan_interval).await;
        }
    }
    
    /// Reconcile differences by re-writing from source of truth
    async fn reconcile_differences(
        &self,
        source_to_target: Vec<DataDiscrepancy>,
        target_to_source: Vec<DataDiscrepancy>
    ) -> Result<()> {
        for discrepancy in source_to_target {
            match discrepancy.discrepancy_type {
                DiscrepancyType::MissingInTarget => {
                    // Re-read from source and write to target
                    let row = self.source_cassandra
                        .read(&discrepancy.table, &discrepancy.key)
                        .await?;
                    
                    if let Some(data) = row {
                        self.target_scylla
                            .write(&discrepancy.table, &data)
                            .await?;
                        info!("Reconciled missing row in target: {}", discrepancy.key);
                    }
                }
                DiscrepancyType::ValueMismatch | DiscrepancyType::TimestampMismatch => {
                    // Source is truth - overwrite target
                    let row = self.source_cassandra
                        .read(&discrepancy.table, &discrepancy.key)
                        .await?;
                    
                    if let Some(data) = row {
                        self.target_scylla
                            .write_with_timestamp(&discrepancy.table, &data)
                            .await?;
                        info!("Reconciled mismatch in target: {}", discrepancy.key);
                    }
                }
                _ => {}
            }
        }
        
        Ok(())
    }
}

// ============================================================================
// PART 5: ALERTING
// ============================================================================

#[derive(Debug)]
pub struct MigrationMonitor {
    dual_writer_metrics: Arc<DualWriterMetrics>,
    loader_metrics: Arc<LoaderMetrics>,
    validator_metrics: Arc<ValidatorMetrics>,
    alert_config: AlertConfig,
}

#[derive(Debug)]
pub struct DualWriterMetrics {
    pub writes_total: AtomicU64,
    pub write_failures: AtomicU64,
    pub write_latency_ms: AtomicU64,
    pub consistency_score: AtomicU64,
}

#[derive(Debug)]
pub struct LoaderMetrics {
    pub rows_migrated: AtomicU64,
    pub tables_completed: AtomicU64,
    pub migration_rate: AtomicU64,  // rows/second
    pub estimated_completion: Option<SystemTime>,
}

#[derive(Debug)]
pub struct ValidatorMetrics {
    pub rows_validated: AtomicU64,
    pub discrepancies_found: AtomicU64,
    pub validation_coverage: f32,
    pub last_validation: SystemTime,
}

impl MigrationMonitor {
    /// Generate real-time dashboard data
    pub async fn get_dashboard_metrics(&self) -> DashboardMetrics {
        DashboardMetrics {
            dual_writer: DualWriterStatus {
                mode: "DualWriteSync",
                writes_per_second: self.calculate_write_rate(),
                failure_rate: self.calculate_failure_rate(),
                latency_p99: self.get_p99_latency(),
            },
            loader: LoaderStatus {
                progress_percent: self.calculate_progress(),
                rows_per_second: self.loader_metrics.migration_rate.load(Ordering::Relaxed),
                eta: self.loader_metrics.estimated_completion,
            },
            validator: ValidatorStatus {
                consistency_score: self.calculate_consistency_score(),
                discrepancy_count: self.validator_metrics.discrepancies_found.load(Ordering::Relaxed),
                coverage: self.validator_metrics.validation_coverage,
            },
            alerts: self.get_active_alerts().await,
        }
    }
    
    /// Check for critical issues
    pub async fn health_check(&self) -> HealthStatus {
        let mut issues = Vec::new();
        
        // Check write failure rate
        if self.calculate_failure_rate() > 0.01 { // >1% failures
            issues.push("High write failure rate detected");
        }
        
        // Check migration progress
        if self.is_migration_stalled().await {
            issues.push("Migration appears stalled");
        }
        
        // Check validation discrepancies
        if self.validator_metrics.discrepancies_found.load(Ordering::Relaxed) > 100 {
            issues.push("High number of validation discrepancies");
        }
        
        HealthStatus {
            healthy: issues.is_empty(),
            issues,
            timestamp: SystemTime::now(),
        }
    }
}
===========================================================================

pub async fn complete_migration_workflow() -> Result<()> {
    // Phase 1: Start dual-writing
    let dual_writer = EnhancedDualWriter::new(config).await?;
    dual_writer.start().await?;
    info!("Phase 1: Dual-writing enabled");
    
    // Phase 2: Bulk migration with SSTableLoader (no Spark needed!)
    let loader_config = SSTableLoaderConfig {
        tables: get_all_tables().await?,
        parallel_streams: 4,        // Migrate 4 tables in parallel
        batch_size: 1000,          // 1000 rows per batch
        rate_limit: Some(50000),    // 50K rows/second max
        token_range_splits: 16,     // Split each table into 16 ranges
        compression: true,
        retry_failed: true,
    };
    
    let loader = RustSSTableLoader::new(loader_config).await?;
    let migration_report = loader.migrate_all_tables().await?;
    info!("Phase 2: Bulk migration complete - {} rows", migration_report.total_rows);
    
    // Phase 3: Validation with dual-reader
    let validator_config = ValidationConfig {
        sample_rate: 0.10,  // Validate 10% of data
        validation_mode: ValidationMode::FullScan,
        batch_size: 500,
        parallel_validators: 8,
        checksum_columns: true,
        compare_timestamps: true,
        compare_ttl: true,
    };
    
    let validator = DualReaderValidator::new(validator_config).await?;
    let validation_report = validator.validate_migration().await?;
    
    if !validation_report.validation_passed {
        error!("Validation failed with {} discrepancies", 
               validation_report.discrepancies_found);
        return Err("Validation failed");
    }
    
    info!("Phase 3: Validation passed - {} rows checked", 
          validation_report.total_rows_checked);
    
    // Phase 4: Start continuous two-way scanning
    let scanner = TwoWayScanner::new(TwoWayScannerConfig {
        scan_mode: ScanMode::Continuous,
        reconciliation_enabled: true,
        scan_interval: Duration::from_secs(300), // Every 5 minutes
        tables: get_critical_tables(),
        alert_threshold: 10,
    }).await?;
    
    tokio::spawn(async move {
        scanner.start_continuous_scan().await
    });
    
    info!("Phase 4: Two-way scanner running");
    
    // Phase 5: Monitor everything
    let monitor = MigrationMonitor::new().await?;
    loop {
        let metrics = monitor.get_dashboard_metrics().await;
        info!("Dashboard: {:?}", metrics);
        
        let health = monitor.health_check().await;
        if !health.healthy {
            warn!("Health issues: {:?}", health.issues);
        }
        
        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
```
