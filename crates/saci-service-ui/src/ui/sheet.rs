//! `Sheet`: the right-hand detail panel.

use leptos::prelude::*;

/// shadcn `Sheet`, `side="right"`, with its overlay.
///
/// Open state is the caller's signal: the graph owns which node is selected, and
/// duplicating that into the sheet would give two sources of truth for one
/// piece of UI state.
#[component]
pub fn Sheet(
    /// Whether the panel is showing.
    #[prop(into)]
    open: Signal<bool>,
    /// Panel heading.
    #[prop(into)]
    title: Signal<String>,
    /// Dismiss handler, run by the overlay and the close button.
    #[prop(into)]
    on_close: Callback<()>,
    children: ChildrenFn,
) -> impl IntoView {
    view! {
        <Show when=move || open.get()>
            <div
                data-slot="sheet-overlay"
                class="fixed inset-0 z-40 bg-black/40"
                on:click=move |_| on_close.run(())
            ></div>
            <div
                data-slot="sheet-content"
                class="fixed inset-y-0 right-0 z-50 flex w-full max-w-sm flex-col gap-4 border-l \
                       bg-card p-6 shadow-lg"
            >
                <div class="flex items-start justify-between gap-4">
                    <div data-slot="sheet-title" class="font-semibold">
                        {move || title.get()}
                    </div>
                    <button
                        type="button"
                        class="rounded-md px-2 text-sm text-muted-foreground hover:text-foreground"
                        on:click=move |_| on_close.run(())
                    >
                        "close"
                    </button>
                </div>
                <div class="flex-1 overflow-y-auto text-sm">{children()}</div>
            </div>
        </Show>
    }
}
