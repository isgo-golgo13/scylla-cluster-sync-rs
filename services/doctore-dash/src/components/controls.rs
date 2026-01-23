// services/doctore-dash/src/components/controls.rs
//
// Doctore Dashboard - Control Buttons Component
//

use leptos::*;
use crate::state::DoctoreState;

#[component]
pub fn Controls(state: DoctoreState) -> impl IntoView {
    let is_running = move || state.migration.get().is_running;
    let is_paused = move || state.migration.get().is_paused;
    
    // Button handlers (mock mode - just log)
    let on_start = move |_| {
        state.log("info", "Start migration requested");
    };
    
    let on_pause = move |_| {
        state.log("info", "Pause migration requested");
    };
    
    let on_resume = move |_| {
        state.log("info", "Resume migration requested");
    };
    
    let on_stop = move |_| {
        state.log("warn", "Stop migration requested");
    };
    
    view! {
        <div class="controls">
            // Start button (shown when not running)
            <Show
                when=move || !is_running()
                fallback=|| view! {}
            >
                <button class="btn btn-start" on:click=on_start>
                    <span class="btn-icon">"▶"</span>
                    <span class="btn-text">"Start"</span>
                </button>
            </Show>
            
            // Pause/Resume button (shown when running)
            <Show
                when=move || is_running() && !is_paused()
                fallback=|| view! {}
            >
                <button class="btn btn-pause" on:click=on_pause>
                    <span class="btn-icon">"⏸"</span>
                    <span class="btn-text">"Pause"</span>
                </button>
            </Show>
            
            <Show
                when=move || is_running() && is_paused()
                fallback=|| view! {}
            >
                <button class="btn btn-resume" on:click=on_resume>
                    <span class="btn-icon">"▶"</span>
                    <span class="btn-text">"Resume"</span>
                </button>
            </Show>
            
            // Stop button (shown when running)
            <Show
                when=is_running
                fallback=|| view! {}
            >
                <button class="btn btn-stop" on:click=on_stop>
                    <span class="btn-icon">"⏹"</span>
                    <span class="btn-text">"Stop"</span>
                </button>
            </Show>
            
            // Config button (always shown)
            <button class="btn btn-config">
                <span class="btn-icon">"⚙"</span>
                <span class="btn-text">"Config"</span>
            </button>
        </div>
    }
}
