// services/doctore-dash/src/components/stats.rs
//
// Doctore Dashboard - Statistics Cards Component
//

use leptos::*;
use crate::state::DoctoreState;

#[component]
pub fn StatsCards(state: DoctoreState) -> impl IntoView {
    let stats = move || state.migration.get();
    
    view! {
        <div class="stats-grid">
            <StatCard 
                label="Migrated"
                value=move || format_number(stats().migrated_rows)
                icon="✓"
                class_name="stat-success"
            />
            
            <StatCard 
                label="Failed"
                value=move || format_number(stats().failed_rows)
                icon="✗"
                class_name="stat-error"
            />
            
            <StatCard 
                label="Filtered"
                value=move || format_number(stats().filtered_rows)
                icon="⊘"
                class_name="stat-warning"
            />
            
            <StatCard 
                label="Throughput"
                value=move || format!("{:.0}/s", stats().throughput_rows_per_sec)
                icon="▶"
                class_name="stat-info"
            />
            
            <StatCard 
                label="Elapsed"
                value=move || format_duration(stats().elapsed_secs)
                icon="⏱"
                class_name="stat-neutral"
            />
            
            <StatCard 
                label="Tables"
                value=move || format!("{}/{}", stats().tables_completed, stats().tables_total)
                icon="▤"
                class_name="stat-neutral"
            />
        </div>
    }
}

#[component]
fn StatCard(
    label: &'static str,
    value: impl Fn() -> String + 'static,
    icon: &'static str,
    class_name: &'static str,
) -> impl IntoView {
    view! {
        <div class=format!("stat-card {}", class_name)>
            <div class="stat-icon">{icon}</div>
            <div class="stat-content">
                <span class="stat-value">{value}</span>
                <span class="stat-label">{label}</span>
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

/// Format duration in human-readable format
fn format_duration(secs: f64) -> String {
    let total_secs = secs as u64;
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    
    if hours > 0 {
        format!("{}h {}m {}s", hours, mins, secs)
    } else if mins > 0 {
        format!("{}m {}s", mins, secs)
    } else {
        format!("{}s", secs)
    }
}
