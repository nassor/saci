//! `Select`, over a native `<select>`.

use leptos::prelude::*;

/// shadcn `Select`, rendered as a native `<select>`.
///
/// shadcn's own is a Radix listbox. A native control is what keeps keyboard
/// selection, type-ahead and the platform popup working while the WASM module
/// is busy laying out a thousand log rows, which is exactly when an operator
/// reaches for a filter.
#[component]
pub fn Select(
    /// `(value, label)` pairs, in display order. The first is the reset entry.
    #[prop(into)]
    options: Signal<Vec<(String, String)>>,
    /// Currently selected value.
    #[prop(into)]
    value: Signal<String>,
    /// Fired with the newly selected value.
    #[prop(into)]
    on_change: Callback<String>,
    /// Tailwind width class, e.g. `"w-40"`. A literal, because Tailwind cannot
    /// see a computed class name.
    #[prop(default = "w-40")]
    width: &'static str,
) -> impl IntoView {
    let class = format!(
        "h-8 rounded-md border border-border bg-background px-2 text-xs outline-none \
         focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/40 {width}"
    );
    view! {
        <select
            data-slot="select"
            class=class
            prop:value=move || value.get()
            on:change=move |ev| on_change.run(event_target_value(&ev))
        >
            {move || {
                let current = value.get();
                options
                    .get()
                    .into_iter()
                    .map(|(option, label)| {
                        let selected = option == current;
                        view! {
                            <option value=option selected=selected>
                                {label}
                            </option>
                        }
                    })
                    .collect_view()
            }}
        </select>
    }
}
