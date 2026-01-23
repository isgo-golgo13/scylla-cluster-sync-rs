// services/doctore-dash/src/components/filter.rs
//
// Doctore Dashboard - Filter Statistics Component
//

use leptos::*;
use crate::state::DoctoreState;

#[component]
pub fn FilterStats(state: DoctoreState) -> impl IntoView {
    let filter = move || state.filter.get();
    let migration = move || state.migration.get();
    
    // Calculate filter rate
    let filter_rate = move || {
        let total = migration().migrated_rows + filter().rows_skipped;
        if total > 0 {
            (filter().rows_skipped as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    };
    
    view! {
        <div class="filter-stats">
            <div class="filter-summary">
                <div class="filter-rate">
                    <span class="filter-rate-value">{move || format!("{:.2}%", filter_rate())}</span>
                    <span class="filter-rate-label">"filtered"</span>
                </div>
            </div>
            
            <div class="filter-details">
                <FilterRow 
                    label="Tables Skipped"
                    value=move || filter().tables_skipped
                    icon="▤"
                />
                <FilterRow 
                    label="Partitions Skipped"
                    value=move || filter().partitions_skipped
                    icon="◫"
                />
                <FilterRow 
                    label="Rows Filtered"
                    value=move || filter().rows_skipped
                    icon="⊘"
                />
                <FilterRow 
                    label="Rows Allowed"
                    value=move || filter().rows_allowed
                    icon="✓"
                />
            </div>
        </div>
    }
}

#[component]
fn FilterRow(
    label: &'static str,
    value: impl Fn() -> u64 + 'static,
    icon: &'static str,
) -> impl IntoView {
    view! {
        <div class="filter-row">
            <span class="filter-icon">{icon}</span>
            <span class="filter-label">{label}</span>
            <span class="filter-value">{move || format_number(value())}</span>
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
