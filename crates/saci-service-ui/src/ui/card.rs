//! `Card`, `CardHeader`, `CardTitle`, `CardContent`.

use leptos::prelude::*;

/// shadcn `Card`: the surface every panel on the dashboard sits on.
#[component]
pub fn Card(children: Children) -> impl IntoView {
    view! {
        <div
            data-slot="card"
            class="flex flex-col gap-6 rounded-xl border bg-card py-6 text-card-foreground shadow-sm"
        >
            {children()}
        </div>
    }
}

/// shadcn `CardHeader`.
#[component]
pub fn CardHeader(children: Children) -> impl IntoView {
    view! {
        <div
            data-slot="card-header"
            class="flex flex-col gap-1.5 px-6"
        >
            {children()}
        </div>
    }
}

/// shadcn `CardTitle`.
#[component]
pub fn CardTitle(children: Children) -> impl IntoView {
    view! {
        <div data-slot="card-title" class="font-semibold leading-none">
            {children()}
        </div>
    }
}

/// shadcn `CardContent`.
#[component]
pub fn CardContent(children: Children) -> impl IntoView {
    view! {
        <div data-slot="card-content" class="px-6">
            {children()}
        </div>
    }
}
