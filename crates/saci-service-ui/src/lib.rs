//! The `saci-service` live dashboard.
//!
//! A client-side-rendered Leptos app served from `/ui` by the service's own
//! control plane. It polls `/api/snapshot` at 1 Hz and draws every running
//! workflow as its own animated SVG, plus a traces browser and a log tail.
//!
//! There is no server-side rendering, no websocket and no SSE: the axum side
//! stays stateless, and one GET per second is cheaper than holding a stream
//! open per viewer.
//!
//! The JSON shapes are `crates/saci/src/inspector_wire.rs`, the same file
//! the server serializes from, compiled in here by path so the contract
//! cannot drift between the two halves. The `saci` crate itself does not
//! build for wasm32-unknown-unknown, so it is not a dependency.

mod api;
mod app;
mod components;
mod ui;
#[path = "../../saci/src/inspector_wire.rs"]
pub mod wire;

use leptos::mount::mount_to;
use wasm_bindgen::prelude::*;

pub use app::App;

/// Mount the dashboard into `#saci-app`.
///
/// `#[wasm_bindgen(start)]` runs this on module instantiation, so
/// `crates/saci/assets/ui/index.html` only has to `await init()`.
#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();

    let mount_point = leptos::prelude::document()
        .get_element_by_id("saci-app")
        .expect("index.html must contain #saci-app")
        .unchecked_into::<web_sys::HtmlElement>();

    mount_to(mount_point, App).forget();
}
