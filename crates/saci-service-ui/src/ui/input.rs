//! `Input`, the single-line text field.

use leptos::prelude::*;

/// shadcn `Input`, `type="text"`.
///
/// The value lives in the caller's signal: every filter on the dashboard is
/// applied client-side over an already-fetched window, so the field is a
/// controlled input over the same signal the filter reads.
#[component]
pub fn Input(
    /// Placeholder text.
    placeholder: &'static str,
    /// Current value.
    #[prop(into)]
    value: Signal<String>,
    /// Fired on every keystroke.
    #[prop(into)]
    on_input: Callback<String>,
    /// Tailwind width class, e.g. `"w-64"`. A literal, because Tailwind cannot
    /// see a computed class name.
    #[prop(default = "w-56")]
    width: &'static str,
) -> impl IntoView {
    let class = format!(
        "h-8 rounded-md border border-border bg-background px-2.5 text-xs outline-none \
         placeholder:text-muted-foreground focus-visible:border-ring \
         focus-visible:ring-[3px] focus-visible:ring-ring/40 {width}"
    );
    view! {
        <input
            data-slot="input"
            type="text"
            class=class
            placeholder=placeholder
            prop:value=move || value.get()
            on:input=move |ev| on_input.run(event_target_value(&ev))
        />
    }
}
