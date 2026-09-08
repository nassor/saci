//! `Separator`, horizontal only.

use leptos::prelude::*;

/// shadcn `Separator`, `orientation="horizontal"`.
#[component]
pub fn Separator() -> impl IntoView {
    view! {
        <div
            data-slot="separator"
            role="separator"
            class="my-3 h-px w-full shrink-0 bg-border"
        ></div>
    }
}
