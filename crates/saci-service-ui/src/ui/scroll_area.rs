//! `ScrollArea`, native overflow.
//!
//! shadcn's version wraps Radix's custom scrollbar. The log tail wants a real
//! scrollbar that keeps working when the WASM module is busy, so this keeps the
//! same markup and classes over native overflow instead.

use leptos::prelude::*;

/// shadcn `ScrollArea`.
#[component]
pub fn ScrollArea(
    /// Tailwind height class, e.g. `"h-96"`. A literal, because Tailwind cannot
    /// see a computed class name.
    #[prop(default = "h-96")]
    height: &'static str,
    children: Children,
) -> impl IntoView {
    let class = format!("relative w-full overflow-y-auto overscroll-contain rounded-md {height}");
    view! {
        <div data-slot="scroll-area" class=class>
            {children()}
        </div>
    }
}
