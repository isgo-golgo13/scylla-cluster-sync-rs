// services/doctore-dash/src/components/header.rs
//
// Doctore Dashboard - Header Component
//

use leptos::*;

#[component]
pub fn Header() -> impl IntoView {
    view! {
        <header class="header">
            <div class="header-brand">
                <span class="header-icon">"🏛️"</span>
                <h1 class="header-title">"DOCTORE"</h1>
                <span class="header-subtitle">"Database Migration Control"</span>
            </div>
            
            <div class="header-status">
                <ServiceIndicator name="Sync" status="healthy" />
                <ServiceIndicator name="Bulk" status="healthy" />
                <ServiceIndicator name="Verify" status="healthy" />
            </div>
            
            <div class="header-actions">
                <span class="connection-status connected">"● Connected"</span>
            </div>
        </header>
    }
}

#[component]
fn ServiceIndicator(
    name: &'static str,
    status: &'static str,
) -> impl IntoView {
    let status_class = match status {
        "healthy" => "status-healthy",
        "degraded" => "status-degraded",
        "offline" => "status-offline",
        _ => "status-unknown",
    };
    
    view! {
        <div class=format!("service-indicator {}", status_class)>
            <span class="service-dot"></span>
            <span class="service-name">{name}</span>
        </div>
    }
}
