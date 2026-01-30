// services/tui-dash/src/state.rs
//
// Dashboard state management

use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct DashboardState {
    // Migration stats
    pub total_rows: u64,
    pub migrated_rows: u64,
    pub failed_rows: u64,
    pub filtered_rows: u64,
    pub throughput: f64,
    pub progress_percent: f64,
    
    // Table stats
    pub tables_total: u32,
    pub tables_completed: u32,
    pub tables_skipped: u32,
    
    // Range stats
    pub ranges_total: u32,
    pub ranges_completed: u32,
    
    // Status
    pub is_running: bool,
    pub is_paused: bool,
    pub elapsed_secs: f64,
    pub elapsed_display: String,
    
    // Tables list
    pub tables: Vec<TableStatus>,
    
    // Activity log
    pub activity_log: Vec<LogEntry>,
    
    // UI state
    pub scroll_offset: usize,
}

#[derive(Debug, Clone)]
pub struct TableStatus {
    pub name: String,
    pub status: String,  // PENDING, RUNNING, DONE, FAILED
    pub progress: f64,
    pub rows_migrated: u64,
    pub rows_total: u64,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: DateTime<Local>,
    pub level: String,  // INFO, WARN, ERROR
    pub message: String,
}

impl DashboardState {
    pub fn new() -> Self {
        Self {
            total_rows: 0,
            migrated_rows: 0,
            failed_rows: 0,
            filtered_rows: 0,
            throughput: 0.0,
            progress_percent: 0.0,
            tables_total: 0,
            tables_completed: 0,
            tables_skipped: 0,
            ranges_total: 0,
            ranges_completed: 0,
            is_running: false,
            is_paused: false,
            elapsed_secs: 0.0,
            elapsed_display: "00:00:00".to_string(),
            tables: Vec::new(),
            activity_log: Vec::new(),
            scroll_offset: 0,
        }
    }
    
    pub fn toggle_pause(&mut self) {
        if self.is_running {
            self.is_paused = !self.is_paused;
            if self.is_paused {
                self.add_log("WARN", "Migration paused by user");
            } else {
                self.add_log("INFO", "Migration resumed");
            }
        }
    }
    
    pub fn reset(&mut self) {
        *self = Self::new();
        self.add_log("INFO", "Dashboard reset");
    }
    
    pub fn scroll_up(&mut self) {
        if self.scroll_offset > 0 {
            self.scroll_offset -= 1;
        }
    }
    
    pub fn scroll_down(&mut self) {
        self.scroll_offset += 1;
    }
    
    pub fn add_log(&mut self, level: &str, message: &str) {
        self.activity_log.push(LogEntry {
            timestamp: Local::now(),
            level: level.to_string(),
            message: message.to_string(),
        });
        
        // Keep last 100 entries
        if self.activity_log.len() > 100 {
            self.activity_log.remove(0);
        }
    }
    
    pub fn update_elapsed(&mut self) {
        let hours = (self.elapsed_secs / 3600.0) as u32;
        let minutes = ((self.elapsed_secs % 3600.0) / 60.0) as u32;
        let seconds = (self.elapsed_secs % 60.0) as u32;
        self.elapsed_display = format!("{:02}:{:02}:{:02}", hours, minutes, seconds);
    }
    
    pub fn update_progress(&mut self) {
        if self.total_rows > 0 {
            self.progress_percent = (self.migrated_rows as f64 / self.total_rows as f64) * 100.0;
        }
    }
}

// API response structure (matches sstable-loader /status endpoint)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStats {
    pub total_rows: u64,
    pub migrated_rows: u64,
    pub failed_rows: u64,
    pub filtered_rows: u64,
    pub tables_completed: u32,
    pub tables_total: u32,
    pub tables_skipped: u32,
    pub skipped_corrupted_ranges: u64,
    pub progress_percent: f64,
    pub throughput_rows_per_sec: f64,
    pub elapsed_secs: f64,
    pub is_running: bool,
    pub is_paused: bool,
}

impl Default for DashboardState {
    fn default() -> Self {
        Self::new()
    }
}
