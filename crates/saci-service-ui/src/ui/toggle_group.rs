//! `ToggleGroup` and `ToggleItem`: a segmented single-choice control.

use leptos::prelude::*;

/// shadcn `ToggleGroup`, `variant="outline" size="sm"`.
///
/// One bordered strip; the items inside it collapse their shared borders, so
/// the group reads as one control rather than a row of buttons.
#[component]
pub fn ToggleGroup(children: Children) -> impl IntoView {
    view! {
        <div
            data-slot="toggle-group"
            role="group"
            class="flex h-8 w-fit items-center rounded-md border border-border \
                   [&>*:not(:first-child)]:border-l [&>*:not(:first-child)]:border-border \
                   [&>*:first-child]:rounded-l-md [&>*:last-child]:rounded-r-md"
        >
            {children()}
        </div>
    }
}

/// shadcn `ToggleGroupItem`.
#[component]
pub fn ToggleItem(
    /// Whether this item is the selected one.
    #[prop(into)]
    active: Signal<bool>,
    /// Selects this item.
    #[prop(into)]
    on_select: Callback<()>,
    children: Children,
) -> impl IntoView {
    let class = move || {
        let base = "inline-flex h-full min-w-8 items-center justify-center px-2.5 text-xs \
                    font-medium whitespace-nowrap transition-colors outline-none \
                    focus-visible:ring-[3px] focus-visible:ring-ring/40";
        if active.get() {
            format!("{base} bg-accent text-foreground")
        } else {
            format!("{base} text-muted-foreground hover:bg-accent/50 hover:text-foreground")
        }
    };
    view! {
        <button
            data-slot="toggle-group-item"
            type="button"
            aria-pressed=move || active.get().to_string()
            class=class
            on:click=move |_| on_select.run(())
        >
            {children()}
        </button>
    }
}
