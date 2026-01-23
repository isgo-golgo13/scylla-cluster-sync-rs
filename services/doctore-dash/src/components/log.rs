// services/doctore-dash/src/components/log.rs
//
// Doctore Dashboard - Event Log Component
//

use leptos::*;
use crate::state::DoctoreState;

#[component]
pub fn ErrorLog(state: DoctoreState) -> impl IntoView {
    let logs = move || state.logs.get();
    
    view! {
        <div class="log-container">
            <div class="log-scroll">
                <For
                    each=move || logs().into_iter().rev().take(50).collect::<Vec<_>>()
                    key=|entry| format!("{}-{}", entry.timestamp, entry.message)
                    children=move |entry| {
                        let level_class = match entry.level.as_str() {
                            "error" => "log-error",
                            "warn" => "log-warn",
                            _ => "log-info",
                        };
                        
                        let level_icon = match entry.level.as_str() {
                            "error" => "✗",
                            "warn" => "⚠",
                            _ => "●",
                        };
                        
                        // Format timestamp to just time portion
                        let time = entry.timestamp
                            .split('T')
                            .nth(1)
                            .and_then(|t| t.split('.').next())
                            .unwrap_or(&entry.timestamp)
                            .to_string();
                        
                        view! {
                            <div class=format!("log-entry {}", level_class)>
                                <span class="log-time">{time}</span>
                                <span class="log-icon">{level_icon}</span>
                                <span class="log-message">{entry.message.clone()}</span>
                            </div>
                        }
                    }
                />
            </div>
        </div>
    }
}
