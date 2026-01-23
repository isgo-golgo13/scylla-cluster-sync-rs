// services/doctore-dash/src/components/throughput.rs
//
// Doctore Dashboard - Throughput Chart Component
// Uses Charming (Rust ECharts wrapper) for visualization
//

use leptos::*;
use crate::state::DoctoreState;

#[component]
pub fn ThroughputChart(state: DoctoreState) -> impl IntoView {
    // Get throughput history reactively
    let history = move || state.throughput_history.get();
    let current = move || state.migration.get().throughput_rows_per_sec;
    
    // Calculate min/max for scale
    let max_throughput = move || {
        history()
            .iter()
            .map(|p| p.value)
            .fold(0.0_f64, |a, b| a.max(b))
            .max(1000.0) // Minimum scale
    };
    
    view! {
        <div class="throughput-chart">
            // Current throughput display
            <div class="throughput-current">
                <span class="throughput-value">{move || format!("{:.0}", current())}</span>
                <span class="throughput-unit">"rows/sec"</span>
            </div>
            
            // SVG Line chart
            <svg class="chart-svg" viewBox="0 0 400 150" preserveAspectRatio="none">
                // Grid lines
                <line x1="0" y1="37" x2="400" y2="37" class="grid-line" />
                <line x1="0" y1="75" x2="400" y2="75" class="grid-line" />
                <line x1="0" y1="112" x2="400" y2="112" class="grid-line" />
                
                // Chart path
                <path
                    class="chart-line"
                    d=move || generate_path(&history(), max_throughput())
                    fill="none"
                    stroke="#DC143C"
                    stroke-width="2"
                />
                
                // Area fill
                <path
                    class="chart-area"
                    d=move || generate_area(&history(), max_throughput())
                    fill="url(#gradient)"
                    opacity="0.3"
                />
                
                // Gradient definition
                <defs>
                    <linearGradient id="gradient" x1="0%" y1="0%" x2="0%" y2="100%">
                        <stop offset="0%" stop-color="#DC143C" stop-opacity="0.8" />
                        <stop offset="100%" stop-color="#DC143C" stop-opacity="0.0" />
                    </linearGradient>
                </defs>
            </svg>
            
            // Time axis labels
            <div class="chart-labels">
                <span>"-60s"</span>
                <span>"-30s"</span>
                <span>"now"</span>
            </div>
        </div>
    }
}

/// Generate SVG path for line chart
fn generate_path(history: &[crate::state::ThroughputPoint], max_val: f64) -> String {
    if history.is_empty() {
        return String::new();
    }
    
    let width = 400.0;
    let height = 140.0;
    let padding = 5.0;
    
    let points: Vec<String> = history
        .iter()
        .enumerate()
        .map(|(i, point)| {
            let x = (i as f64 / history.len().max(1) as f64) * width;
            let y = height - padding - ((point.value / max_val) * (height - padding * 2.0));
            format!("{:.1},{:.1}", x, y)
        })
        .collect();
    
    if points.is_empty() {
        return String::new();
    }
    
    format!("M {} L {}", points[0], points.join(" L "))
}

/// Generate SVG path for area fill
fn generate_area(history: &[crate::state::ThroughputPoint], max_val: f64) -> String {
    if history.is_empty() {
        return String::new();
    }
    
    let width = 400.0;
    let height = 140.0;
    let padding = 5.0;
    
    let mut points: Vec<String> = history
        .iter()
        .enumerate()
        .map(|(i, point)| {
            let x = (i as f64 / history.len().max(1) as f64) * width;
            let y = height - padding - ((point.value / max_val) * (height - padding * 2.0));
            format!("{:.1},{:.1}", x, y)
        })
        .collect();
    
    if points.is_empty() {
        return String::new();
    }
    
    // Close the area path
    let last_x = ((history.len() - 1) as f64 / history.len().max(1) as f64) * width;
    points.push(format!("{:.1},{:.1}", last_x, height));
    points.push(format!("0,{:.1}", height));
    
    format!("M {} L {} Z", points[0], points.join(" L "))
}
