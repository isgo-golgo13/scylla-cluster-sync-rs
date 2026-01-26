// services/doctore-dash/src/mock.rs
//
// Doctore Dashboard - Mock Data Generator
// Simulates live migration data for UI development and demos
//

use leptos::*;
use gloo_timers::future::TimeoutFuture;
use rand::prelude::*;

use crate::state::{DoctoreState, MigrationStats, FilterStats, TableState, ServiceStatus};

/// Start mock data updates
pub fn start_mock_updates(state: DoctoreState) {
    // Initialize with demo tables
    initialize_mock_tables(&state);
    
    // Initialize services as healthy
    state.sync_status.set(ServiceStatus::Healthy);
    state.bulk_status.set(ServiceStatus::Healthy);
    state.verify_status.set(ServiceStatus::Healthy);
    
    // Log startup
    state.log("info", "🏛️ Doctore Dashboard initialized (mock mode)");
    state.log("info", "Connected to Doctore Bulk (sstable-loader)");
    state.log("info", "Connected to Doctore Sync (dual-writer)");
    state.log("info", "Connected to Doctore Verify (dual-reader)");
    
    // Spawn update loop
    spawn_local(mock_update_loop(state));
}

/// Initialize mock tables
fn initialize_mock_tables(state: &DoctoreState) {
    let tables = vec![
        TableState {
            name: "assets_keyspace.assets".to_string(),
            rows_total: 2_500_000,
            rows_migrated: 0,
            rows_failed: 0,
            status: "pending".to_string(),
        },
        TableState {
            name: "assets_keyspace.asset_versions".to_string(),
            rows_total: 8_750_000,
            rows_migrated: 0,
            rows_failed: 0,
            status: "pending".to_string(),
        },
        TableState {
            name: "files_keyspace.files".to_string(),
            rows_total: 15_000_000,
            rows_migrated: 0,
            rows_failed: 0,
            status: "pending".to_string(),
        },
        TableState {
            name: "files_keyspace.file_sets".to_string(),
            rows_total: 3_200_000,
            rows_migrated: 0,
            rows_failed: 0,
            status: "pending".to_string(),
        },
        TableState {
            name: "files_keyspace.formats".to_string(),
            rows_total: 450_000,
            rows_migrated: 0,
            rows_failed: 0,
            status: "pending".to_string(),
        },
        TableState {
            name: "metadata_keyspace.metadata".to_string(),
            rows_total: 12_000_000,
            rows_migrated: 0,
            rows_failed: 0,
            status: "pending".to_string(),
        },
    ];
    
    state.tables.set(tables);
}

/// Main mock update loop
async fn mock_update_loop(state: DoctoreState) {
    let mut rng = rand::thread_rng();
    let mut elapsed = 0.0_f64;
    let mut current_table_idx = 0;
    
    // Simulation state
    let mut total_rows: u64 = 0;
    let mut migrated_rows: u64 = 0;
    let mut failed_rows: u64 = 0;
    let mut filtered_rows: u64 = 0;
    let mut tables_completed: u64 = 0;
    
    // Calculate total rows across all tables
    let tables = state.tables.get_untracked();
    let tables_total = tables.len() as u64;
    for table in &tables {
        total_rows += table.rows_total;
    }
    
    // Start "migration"
    state.log("info", "Starting bulk migration...");
    
    loop {
        TimeoutFuture::new(1_000).await; // Update every second
        elapsed += 1.0;
        
        // Simulate throughput (5,000 - 25,000 rows/sec with variance)
        let base_throughput: f64 = 15_000.0 + rng.gen_range(-5_000.0..5_000.0);
        let throughput = base_throughput.max(1_000.0);
        let rows_this_tick = throughput as u64;
        
        // Update migrated count
        let remaining = total_rows - migrated_rows - failed_rows - filtered_rows;
        let actual_migrated = rows_this_tick.min(remaining);
        
        // Simulate occasional failures (0.001% rate)
        let new_failures = if rng.gen_bool(0.00001) { rng.gen_range(1..5) } else { 0 };
        failed_rows += new_failures;
        
        // Simulate filtering (0.5% rate)
        let new_filtered = (actual_migrated as f64 * 0.005) as u64;
        filtered_rows += new_filtered;
        
        migrated_rows += actual_migrated - new_filtered;
        
        // Update current table progress
        state.tables.update(|tables| {
            if current_table_idx < tables.len() {
                let table = &mut tables[current_table_idx];
                
                if table.status == "pending" {
                    table.status = "running".to_string();
                    // Can't call state.log here due to borrow checker, log outside
                }
                
                let table_remaining = table.rows_total - table.rows_migrated - table.rows_failed;
                let table_migrated = actual_migrated.min(table_remaining);
                table.rows_migrated += table_migrated;
                table.rows_failed += new_failures;
                
                // Check if table complete
                if table.rows_migrated + table.rows_failed >= table.rows_total {
                    table.status = "completed".to_string();
                }
            }
        });
        
        // Check if current table is complete, move to next
        let tables = state.tables.get_untracked();
        if current_table_idx < tables.len() {
            if tables[current_table_idx].status == "completed" {
                tables_completed += 1;
                state.log("info", &format!("✓ Table {} complete", tables[current_table_idx].name));
                current_table_idx += 1;
                
                if current_table_idx < tables.len() {
                    state.log("info", &format!("Starting table {}", tables[current_table_idx].name));
                }
            }
        }
        
        // Calculate progress
        let progress = if total_rows > 0 {
            (migrated_rows as f32 / total_rows as f32) * 100.0
        } else {
            0.0
        };
        
        // Determine if still running
        let is_running = migrated_rows + failed_rows + filtered_rows < total_rows;
        
        // Update migration stats
        let stats = MigrationStats {
            total_rows,
            migrated_rows,
            failed_rows,
            filtered_rows,
            tables_completed,
            tables_total,
            tables_skipped: 0,
            skipped_corrupted_ranges: 0,
            progress_percent: progress,
            throughput_rows_per_sec: throughput,
            elapsed_secs: elapsed,
            is_running,
            is_paused: false,
        };
        state.update_migration(stats);
        
        // Update filter stats
        let filter_stats = FilterStats {
            tables_skipped: 0,
            partitions_skipped: 0,
            rows_skipped: filtered_rows,
            rows_allowed: migrated_rows,
        };
        state.update_filter(filter_stats);
        
        // Add throughput point for chart
        state.add_throughput_point(elapsed, throughput);
        
        // Occasional log messages
        if elapsed as u64 % 10 == 0 {
            state.log("info", &format!(
                "Progress: {:.1}% ({} / {} rows) @ {:.0} rows/sec",
                progress, migrated_rows, total_rows, throughput
            ));
        }
        
        // Log failures
        if new_failures > 0 {
            state.log("warn", &format!("Insert failures: {} rows", new_failures));
        }
        
        // Check if complete
        if !is_running {
            state.log("info", &format!(
                "🏆 Migration complete! {} rows migrated, {} filtered, {} failed ({:.2}% success)",
                migrated_rows,
                filtered_rows,
                failed_rows,
                (migrated_rows as f64 / total_rows as f64) * 100.0
            ));
            break;
        }
    }
}

/// Generate sample migration stats (for static previews)
pub fn sample_migration_stats() -> MigrationStats {
    MigrationStats {
        total_rows: 42_000_000,
        migrated_rows: 31_500_000,
        failed_rows: 127,
        filtered_rows: 210_000,
        tables_completed: 4,
        tables_total: 6,
        tables_skipped: 1,
        skipped_corrupted_ranges: 0,
        progress_percent: 75.0,
        throughput_rows_per_sec: 15_432.5,
        elapsed_secs: 2042.5,
        is_running: true,
        is_paused: false,
    }
}

/// Generate sample filter stats (for static previews)
pub fn sample_filter_stats() -> FilterStats {
    FilterStats {
        tables_skipped: 1,
        partitions_skipped: 42,
        rows_skipped: 210_000,
        rows_allowed: 31_500_000,
    }
}
