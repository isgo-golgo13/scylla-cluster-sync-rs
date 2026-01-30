// services/tui-dash/src/api.rs
//
// API client for fetching live stats from sstable-loader

use std::time::Duration;
use anyhow::Result;
use serde::Deserialize;

use crate::state::DashboardState;

#[derive(Debug, Deserialize)]
pub struct ApiStatus {
    pub status: String,
    pub stats: Option<MigrationStats>,
}

#[derive(Debug, Deserialize)]
pub struct MigrationStats {
    pub total_rows: u64,
    pub migrated_rows: u64,
    pub failed_rows: u64,
    pub filtered_rows: u64,
    pub tables_completed: u32,
    pub tables_total: u32,
    pub tables_skipped: u32,
    pub skipped_corrupted_ranges: u64,
    pub progress_percent: f32,
    pub throughput_rows_per_sec: f64,
    pub elapsed_secs: f64,
    pub is_running: bool,
    pub is_paused: bool,
}

pub struct ApiClient {
    client: reqwest::blocking::Client,
    base_url: String,
    connected: bool,
    last_error: Option<String>,
}

impl ApiClient {
    pub fn new(base_url: &str) -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_millis(500))
            .build()
            .unwrap_or_default();
        
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            connected: false,
            last_error: None,
        }
    }
    
    pub fn fetch_status(&mut self, state: &mut DashboardState) {
        let url = format!("{}/status", self.base_url);
        
        match self.client.get(&url).send() {
            Ok(response) => {
                if response.status().is_success() {
                    match response.json::<ApiStatus>() {
                        Ok(api_status) => {
                            self.connected = true;
                            self.last_error = None;
                            self.update_state(state, api_status);
                        }
                        Err(e) => {
                            self.connected = false;
                            self.last_error = Some(format!("Parse error: {}", e));
                            if state.activity_log.is_empty() || 
                               !state.activity_log.last().map(|l| l.message.contains("Parse error")).unwrap_or(false) {
                                state.add_log("ERROR", &format!("Failed to parse API response: {}", e));
                            }
                        }
                    }
                } else {
                    self.connected = false;
                    self.last_error = Some(format!("HTTP {}", response.status()));
                }
            }
            Err(e) => {
                if self.connected {
                    state.add_log("WARN", &format!("Lost connection to sstable-loader: {}", e));
                }
                self.connected = false;
                self.last_error = Some(format!("Connection error: {}", e));
            }
        }
        
        // Update connection status in state
        if !self.connected && !state.activity_log.iter().any(|l| l.message.contains("Connecting to")) {
            state.add_log("INFO", &format!("Connecting to {}...", self.base_url));
        }
    }
    
    fn update_state(&mut self, state: &mut DashboardState, api_status: ApiStatus) {
        if let Some(stats) = api_status.stats {
            // First connection log
            if !state.is_running && stats.is_running {
                state.add_log("INFO", &format!("Connected to sstable-loader at {}", self.base_url));
            }
            
            // Detect state changes for logging
            if state.is_running && !stats.is_running && stats.migrated_rows > 0 {
                state.add_log("INFO", "Migration completed");
            }
            
            if !state.is_paused && stats.is_paused {
                state.add_log("WARN", "Migration paused");
            }
            
            if state.is_paused && !stats.is_paused && stats.is_running {
                state.add_log("INFO", "Migration resumed");
            }
            
            // Log failures
            if stats.failed_rows > state.failed_rows && stats.failed_rows > 0 {
                let new_failures = stats.failed_rows - state.failed_rows;
                if new_failures > 100 {
                    state.add_log("WARN", &format!("{} rows failed", new_failures));
                }
            }
            
            // Update stats
            state.total_rows = stats.total_rows;
            state.migrated_rows = stats.migrated_rows;
            state.failed_rows = stats.failed_rows;
            state.filtered_rows = stats.filtered_rows;
            state.throughput = stats.throughput_rows_per_sec;
            state.progress_percent = stats.progress_percent as f64;
            
            state.tables_total = stats.tables_total;
            state.tables_completed = stats.tables_completed;
            state.tables_skipped = stats.tables_skipped;
            
            state.is_running = stats.is_running;
            state.is_paused = stats.is_paused;
            state.elapsed_secs = stats.elapsed_secs;
            state.update_elapsed();
        }
    }
    
    pub fn is_connected(&self) -> bool {
        self.connected
    }
    
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }
}
