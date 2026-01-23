// services/doctore-dash/src/lib.rs
//
// Doctore Dashboard - Library exports
//

pub mod app;
pub mod components;
pub mod mock;
pub mod state;

use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn hydrate() {
    console_error_panic_hook::set_once();
    let _ = console_log::init_with_level(log::Level::Debug);
    
    leptos::mount_to_body(|| {
        leptos::view! { <app::App /> }
    });
}
