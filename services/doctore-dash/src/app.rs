// services/doctore-dash/src/app.rs
//
// Doctore Dashboard - Main Application Component
//

use leptos::*;
use crate::components::{
    Header,
    ProgressDonut,
    ThroughputChart,
    StatsCards,
    TableProgress,
    FilterStats,
    Controls,
    ErrorLog,
};
use crate::state::{DoctoreState, use_doctore_state};
use crate::mock::start_mock_updates;

#[component]
pub fn App() -> impl IntoView {
    // Initialize reactive state
    let state = use_doctore_state();
    
    // Start mock data updates (in standalone mode)
    start_mock_updates(state.clone());
    
    view! {
        <div class="doctore-app">
            <Header />
            
            <main class="dashboard">
                <div class="dashboard-grid">
                    // Left column - Progress & Controls
                    <section class="panel progress-panel">
                        <h2 class="panel-title">"Migration Progress"</h2>
                        <ProgressDonut state=state.clone() />
                        <Controls state=state.clone() />
                    </section>
                    
                    // Center column - Charts
                    <section class="panel charts-panel">
                        <h2 class="panel-title">"Throughput"</h2>
                        <ThroughputChart state=state.clone() />
                    </section>
                    
                    // Right column - Stats
                    <section class="panel stats-panel">
                        <h2 class="panel-title">"Statistics"</h2>
                        <StatsCards state=state.clone() />
                    </section>
                </div>
                
                <div class="dashboard-grid bottom-row">
                    // Tables progress
                    <section class="panel tables-panel">
                        <h2 class="panel-title">"Tables"</h2>
                        <TableProgress state=state.clone() />
                    </section>
                    
                    // Filter stats
                    <section class="panel filter-panel">
                        <h2 class="panel-title">"Filter Status"</h2>
                        <FilterStats state=state.clone() />
                    </section>
                    
                    // Error log
                    <section class="panel log-panel">
                        <h2 class="panel-title">"Event Log"</h2>
                        <ErrorLog state=state.clone() />
                    </section>
                </div>
            </main>
            
            <footer class="footer">
                <span class="footer-brand">"🏛️ Doctore"</span>
                <span class="footer-tagline">"Train your data for the migration arena"</span>
                <span class="footer-company">"flarestick.io"</span>
            </footer>
        </div>
    }
}
