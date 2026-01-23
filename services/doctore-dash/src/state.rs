// services/doctore-dash/src/state.rs
//
// Doctore Dashboard - Reactive State Management
//

use leptos::*;
use serde::{Deserialize, Serialize};

/// Migration statistics (mirrors sstable-loader MigrationStats)
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
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

/// Filter statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct FilterStats {
    pub tables_skipped: u64,
    pub partitions_skipped: u64,
    pub rows_skipped: u64,
    pub rows_allowed: u64,
}

/// Service health status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceStatus {
    Unknown,
    Healthy,
    Degraded,
    Offline,
}

impl Default for ServiceStatus {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Individual service state
#[derive(Debug, Clone, Default)]
pub struct ServiceState {
    pub name: String,
    pub status: ServiceStatus,
    pub stats: Option<MigrationStats>,
}

/// Table migration state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableState {
    pub name: String,
    pub rows_total: u64,
    pub rows_migrated: u64,
    pub rows_failed: u64,
    pub status: String, // "pending", "running", "completed", "failed"
}

/// Log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: String,
    pub level: String, // "info", "warn", "error"
    pub message: String,
}

/// Throughput data point for charting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThroughputPoint {
    pub time: f64,
    pub value: f64,
}

/// Main application state
/// All fields are RwSignal which is Copy, so DoctoreState is Copy
#[derive(Clone, Copy)]
pub struct DoctoreState {
    // Migration stats
    pub migration: RwSignal<MigrationStats>,
    
    // Filter stats
    pub filter: RwSignal<FilterStats>,
    
    // Service statuses
    pub sync_status: RwSignal<ServiceStatus>,
    pub bulk_status: RwSignal<ServiceStatus>,
    pub verify_status: RwSignal<ServiceStatus>,
    
    // Tables being migrated
    pub tables: RwSignal<Vec<TableState>>,
    
    // Throughput history (for chart)
    pub throughput_history: RwSignal<Vec<ThroughputPoint>>,
    
    // Event log
    pub logs: RwSignal<Vec<LogEntry>>,
    
    // Data source mode
    pub is_mock: RwSignal<bool>,
}

impl DoctoreState {
    pub fn new() -> Self {
        Self {
            migration: create_rw_signal(MigrationStats::default()),
            filter: create_rw_signal(FilterStats::default()),
            sync_status: create_rw_signal(ServiceStatus::Unknown),
            bulk_status: create_rw_signal(ServiceStatus::Unknown),
            verify_status: create_rw_signal(ServiceStatus::Unknown),
            tables: create_rw_signal(vec![]),
            throughput_history: create_rw_signal(vec![]),
            logs: create_rw_signal(vec![]),
            is_mock: create_rw_signal(true),
        }
    }
    
    /// Add a log entry
    pub fn log(&self, level: &str, message: &str) {
        let entry = LogEntry {
            timestamp: js_sys::Date::new_0().to_iso_string().as_string().unwrap_or_default(),
            level: level.to_string(),
            message: message.to_string(),
        };
        
        self.logs.update(|logs| {
            logs.push(entry);
            // Keep only last 100 entries
            if logs.len() > 100 {
                logs.remove(0);
            }
        });
    }
    
    /// Add throughput data point
    pub fn add_throughput_point(&self, time: f64, value: f64) {
        self.throughput_history.update(|history| {
            history.push(ThroughputPoint { time, value });
            // Keep only last 60 points (1 minute at 1/sec)
            if history.len() > 60 {
                history.remove(0);
            }
        });
    }
    
    /// Update migration stats
    pub fn update_migration(&self, stats: MigrationStats) {
        self.migration.set(stats);
    }
    
    /// Update filter stats
    pub fn update_filter(&self, stats: FilterStats) {
        self.filter.set(stats);
    }
}

impl Default for DoctoreState {
    fn default() -> Self {
        Self::new()
    }
}

/// Hook to get/create doctore state
pub fn use_doctore_state() -> DoctoreState {
    DoctoreState::new()
}
