// services/doctore-dash/src/components/progress.rs
//
// Doctore Dashboard - Progress Donut Component
//

use leptos::*;
use crate::state::DoctoreState;

#[component]
pub fn ProgressDonut(state: DoctoreState) -> impl IntoView {
    // Reactive values from state
    let progress = move || state.migration.get().progress_percent;
    let migrated = move || state.migration.get().migrated_rows;
    let total = move || state.migration.get().total_rows;
    let is_running = move || state.migration.get().is_running;
    let is_paused = move || state.migration.get().is_paused;
    
    // SVG donut parameters
    let radius = 80.0;
    let stroke_width = 12.0;
    let circumference = 2.0 * std::f64::consts::PI * radius;
    
    // Calculate stroke dashoffset for progress
    let dash_offset = move || {
        let p = progress() as f64;
        circumference * (1.0 - p / 100.0)
    };
    
    // Status text
    let status_text = move || {
        if is_paused() {
            "PAUSED"
        } else if is_running() {
            "MIGRATING"
        } else if progress() >= 100.0 {
            "COMPLETE"
        } else {
            "READY"
        }
    };
    
    // Status class
    let status_class = move || {
        if is_paused() {
            "status-paused"
        } else if is_running() {
            "status-running"
        } else if progress() >= 100.0 {
            "status-complete"
        } else {
            "status-ready"
        }
    };
    
    view! {
        <div class="progress-donut">
            <svg class="donut-svg" viewBox="0 0 200 200">
                // Background circle
                <circle
                    class="donut-bg"
                    cx="100"
                    cy="100"
                    r=radius
                    fill="none"
                    stroke="#2D2D2D"
                    stroke-width=stroke_width
                />
                
                // Progress circle
                <circle
                    class="donut-progress"
                    cx="100"
                    cy="100"
                    r=radius
                    fill="none"
                    stroke="#DC143C"
                    stroke-width=stroke_width
                    stroke-linecap="round"
                    stroke-dasharray=circumference
                    stroke-dashoffset=dash_offset
                    transform="rotate(-90 100 100)"
                />
                
                // Center text
                <text x="100" y="90" class="donut-percent" text-anchor="middle">
                    {move || format!("{:.1}%", progress())}
                </text>
                <text x="100" y="115" class="donut-status" text-anchor="middle">
                    {status_text}
                </text>
            </svg>
            
            <div class="progress-details">
                <div class="progress-row">
                    <span class="progress-label">"Migrated"</span>
                    <span class="progress-value">{move || format_number(migrated())}</span>
                </div>
                <div class="progress-row">
                    <span class="progress-label">"Total"</span>
                    <span class="progress-value">{move || format_number(total())}</span>
                </div>
            </div>
        </div>
    }
}

/// Format large numbers with commas
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}
