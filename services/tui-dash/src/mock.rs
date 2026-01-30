// services/tui-dash/src/mock.rs
//
// Mock data generator for demo mode

use rand::Rng;

use crate::state::{DashboardState, TableStatus};

pub struct MockDataGenerator {
    tick_count: u64,
    current_table_idx: usize,
    started: bool,
}

impl MockDataGenerator {
    pub fn new() -> Self {
        Self {
            tick_count: 0,
            current_table_idx: 0,
            started: false,
        }
    }
    
    pub fn update(&mut self, state: &mut DashboardState) {
        self.tick_count += 1;
        
        // Initialize on first tick
        if !self.started {
            self.initialize(state);
            self.started = true;
            return;
        }
        
        // Update elapsed time
        state.elapsed_secs += 0.1;
        state.update_elapsed();
        
        if state.is_paused || !state.is_running {
            return;
        }
        
        let mut rng = rand::thread_rng();
        
        // Simulate row migration
        let rows_per_tick = rng.gen_range(500..2500) as u64;
        state.migrated_rows = (state.migrated_rows + rows_per_tick).min(state.total_rows);
        
        // Occasionally add failed rows
        if rng.gen_ratio(1, 100) {
            let failed = rng.gen_range(1..10) as u64;
            state.failed_rows += failed;
            state.add_log("WARN", &format!("Insert retry failed for {} rows - LOCAL_QUORUM unavailable", failed));
        }
        
        // Update throughput with some variance
        let base_throughput = 15000.0;
        state.throughput = base_throughput + rng.gen_range(-2000.0..2000.0);
        
        // Update progress
        state.update_progress();
        
        // Update current table progress
        if self.current_table_idx < state.tables.len() {
            let table = &mut state.tables[self.current_table_idx];
            table.progress += rng.gen_range(0.5..2.0);
            table.rows_migrated = ((table.progress / 100.0) * table.rows_total as f64) as u64;
            
            if table.progress >= 100.0 {
                table.progress = 100.0;
                table.status = "DONE".to_string();
                table.rows_migrated = table.rows_total;
                let completed_table_name = table.name.clone();
                
                state.tables_completed += 1;
                state.ranges_completed += 128;
                
                state.add_log("INFO", &format!("Table {} migration completed", completed_table_name));
                
                self.current_table_idx += 1;
                
                // Start next table
                if self.current_table_idx < state.tables.len() {
                    let next_table_name = state.tables[self.current_table_idx].name.clone();
                    state.tables[self.current_table_idx].status = "RUNNING".to_string();
                    state.add_log("INFO", &format!("Starting migration: {}", next_table_name));
                }
            }
        }
        
        // Check if migration complete
        if state.migrated_rows >= state.total_rows || self.current_table_idx >= state.tables.len() {
            state.is_running = false;
            state.migrated_rows = state.total_rows;
            state.progress_percent = 100.0;
            state.add_log("INFO", "Migration completed successfully");
        }
        
        // Periodic activity logs
        if self.tick_count % 50 == 0 {
            let messages = [
                "Processing token range -9223372036854775808 to -8646911284551352321",
                "Batch insert: 1000 rows committed",
                "Token range completed, moving to next segment",
                "Connection pool: 8/8 active connections",
                "Memory usage: 1.2GB / 4GB allocated",
            ];
            let msg = messages[rng.gen_range(0..messages.len())];
            state.add_log("INFO", msg);
        }
        
        // Occasional warnings
        if self.tick_count % 200 == 0 && rng.gen_ratio(1, 3) {
            let warnings = [
                "Slow response from target node scylla-target-2.aws.example.com",
                "Retry attempt 1/3 for batch insert",
                "Speculative execution triggered - coordinator slow",
            ];
            let msg = warnings[rng.gen_range(0..warnings.len())];
            state.add_log("WARN", msg);
        }
    }
    
    fn initialize(&mut self, state: &mut DashboardState) {
        state.is_running = true;
        state.is_paused = false;
        
        // Setup tables
        let table_names = vec![
            ("acls_keyspace.group_acls", 3_487_274),
            ("acls_keyspace.user_acls", 12_450_000),
            ("assets_keyspace.assets", 8_234_567),
            ("assets_keyspace.asset_versions", 45_678_901),
            ("assets_keyspace.segments", 18_234_123),
            ("collections_keyspace.collections", 2_345_678),
            ("files_keyspace.files", 5_678_901),
            ("metadata_keyspace.custom_metadata", 15_890_234),
        ];
        
        state.tables_total = table_names.len() as u32;
        state.total_rows = table_names.iter().map(|(_, rows)| *rows as u64).sum();
        state.ranges_total = state.tables_total * 128;
        
        for (i, (name, rows)) in table_names.iter().enumerate() {
            state.tables.push(TableStatus {
                name: name.to_string(),
                status: if i == 0 { "RUNNING".to_string() } else { "PENDING".to_string() },
                progress: 0.0,
                rows_migrated: 0,
                rows_total: *rows as u64,
            });
        }
        
        // Initial logs
        state.add_log("INFO", "TUI Dashboard initialized - Demo Mode");
        state.add_log("INFO", &format!("Migration target: {} tables, {} total rows", state.tables_total, state.total_rows));
        state.add_log("INFO", "Connected to source cluster: cassandra-gcp-eu-1.example.com");
        state.add_log("INFO", "Connected to target cluster: scylla-aws-us-1.example.com");
        state.add_log("INFO", &format!("Starting migration: {}", table_names[0].0));
    }
}

impl Default for MockDataGenerator {
    fn default() -> Self {
        Self::new()
    }
}
