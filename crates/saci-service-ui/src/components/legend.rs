//! The colour legend both SVG views share.
//!
//! The graph and the waterfall colour their shapes from the same three tokens,
//! so they explain them the same way: one swatch per plane, in words, next to
//! the drawing that uses it.

use leptos::prelude::*;

/// One legend entry: a swatch in a plane's colour, then what it means.
///
/// `colour` is a CSS value, normally `var(--data)`, `var(--boundary)` or
/// `var(--control)`, applied inline: it names one of a handful of SVG tokens
/// rather than a layout decision Tailwind should generate a utility for.
#[component]
pub fn Swatch(
    /// CSS colour for the swatch.
    colour: &'static str,
    /// What the colour means.
    label: &'static str,
) -> impl IntoView {
    view! {
        <span class="flex items-center gap-1.5">
            <span
                class="inline-block h-2 w-3.5 shrink-0 rounded-sm"
                style=format!("background: {colour}")
            ></span>
            {label}
        </span>
    }
}
