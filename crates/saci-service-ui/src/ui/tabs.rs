//! `Tabs`, as a bar plus one trigger per tab.
//!
//! shadcn's `Tabs` is a Radix context provider with a `TabsContent` per panel.
//! Leptos has no equivalent implicit context to port, and the dashboard's three
//! panels are wholly different components, so the selected value lives in the
//! caller's signal and the panel is chosen there. That keeps the trigger markup
//! and classes identical to shadcn's while dropping a provider the port would
//! only fake.

use leptos::prelude::*;

/// shadcn `TabsList`.
#[component]
pub fn TabBar(children: Children) -> impl IntoView {
    view! {
        <div
            data-slot="tabs-list"
            role="tablist"
            class="inline-flex h-9 w-fit items-center justify-center rounded-lg bg-muted p-[3px]"
        >
            {children()}
        </div>
    }
}

/// shadcn `TabsTrigger`.
#[component]
pub fn TabButton(
    /// Whether this tab is the selected one.
    #[prop(into)]
    active: Signal<bool>,
    /// Selects this tab.
    #[prop(into)]
    on_select: Callback<()>,
    children: Children,
) -> impl IntoView {
    let class = move || {
        let base = "inline-flex h-[calc(100%-1px)] flex-1 items-center justify-center gap-1.5 \
                    rounded-md border border-transparent px-3 py-1 text-sm font-medium \
                    whitespace-nowrap transition-colors outline-none \
                    focus-visible:ring-[3px] focus-visible:ring-ring/50";
        if active.get() {
            format!("{base} bg-background text-foreground shadow-sm")
        } else {
            format!("{base} text-muted-foreground hover:text-foreground")
        }
    };
    view! {
        <button
            data-slot="tabs-trigger"
            role="tab"
            type="button"
            aria-selected=move || active.get().to_string()
            class=class
            on:click=move |_| on_select.run(())
        >
            {children()}
        </button>
    }
}
