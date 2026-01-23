// services/doctore-dash/src/main.rs
//
// Doctore Dashboard - Real-time database migration control panel
// 🏛️ "Train your data for the migration arena"
//

mod app;
mod components;
mod mock;
mod state;

use leptos::*;

fn main() {
    // Better panic messages in browser console
    console_error_panic_hook::set_once();
    
    // Initialize logging
    let _ = console_log::init_with_level(log::Level::Debug);
    
    log::info!("🏛️ Doctore Dashboard starting...");
    
    // Mount Leptos app
    mount_to_body(|| {
        view! { <app::App /> }
    });
}
