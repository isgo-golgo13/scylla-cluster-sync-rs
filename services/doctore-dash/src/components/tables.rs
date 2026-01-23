// services/doctore-dash/src/components/tables.rs
//
// Doctore Dashboard - Table Progress Component
//

use leptos::*;
use crate::state::DoctoreState;

#[component]
pub fn TableProgress(state: DoctoreState) -> impl IntoView {
    let tables = move || state.tables.get();
    
    view! {
        <div class="tables-list">
            <For
                each=tables
                key=|table| table.name.clone()
                children=move |table| {
                    let progress = if table.rows_total > 0 {
                        (table.rows_migrated as f64 / table.rows_total as f64) * 100.0
                    } else {
                        0.0
                    };
                    
                    let status_class = match table.status.as_str() {
                        "completed" => "table-completed",
                        "running" => "table-running",
                        "failed" => "table-failed",
                        _ => "table-pending",
                    };
                    
                    let status_icon = match table.status.as_str() {
                        "completed" => "✓",
                        "running" => "▶",
                        "failed" => "✗",
                        _ => "○",
                    };
                    
                    view! {
                        <div class=format!("table-row {}", status_class)>
                            <span class="table-status-icon">{status_icon}</span>
                            <span class="table-name">{table.name.clone()}</span>
                            <div class="table-progress-bar">
                                <div 
                                    class="table-progress-fill"
                                    style=format!("width: {}%", progress)
                                />
                            </div>
                            <span class="table-progress-text">
                                {format!("{:.1}%", progress)}
                            </span>
                            <span class="table-rows">
                                {format_compact(table.rows_migrated)}"/"
                                {format_compact(table.rows_total)}
                            </span>
                        </div>
                    }
                }
            />
        </div>
    }
}

/// Format number in compact form (e.g., 1.5M, 250K)
fn format_compact(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.0}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}
