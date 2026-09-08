//! `Tooltip`, hover-only.
//!
//! shadcn's tooltip is Radix-positioned. There is no Radix here, so this uses
//! the same markup and classes, but the bubble is `position: fixed`, anchored
//! at the trigger's own `getBoundingClientRect()` rather than pinned to it
//! with `absolute` offsets. Plain CSS positioning breaks in two ways this
//! dashboard actually hits: a trigger inside a `rounded-lg overflow-hidden`
//! card (every node table's header row) clips an `absolute` bubble the
//! instant it is wider than the card, and a trigger hard against the
//! viewport edge (the `w-60` left rail) pushes a horizontally centred bubble
//! half off-screen. `position: fixed` is clipped by neither: its containing
//! block is the viewport itself unless an ancestor sets
//! `transform`/`filter`/`perspective`, none of which this layout uses.
//!
//! Centering and clamping happen in CSS, not Rust, and need only the
//! trigger's rect: the bubble is anchored at `left: 0` plus the trigger's own
//! `top`, then walked into place by `transform: translate(...)`. A percentage
//! inside `translate()` resolves against the *translated element's own*
//! border box, so `translateX(-50%)` centres the bubble on `left` without
//! Rust ever measuring the bubble's width, and `translateY(-100% - gap)`
//! raises it clear of the trigger by its own rendered height, wrapped text
//! included. `clamp()` bounds the horizontal translate between the viewport
//! margin and `100vw - margin - 100%` (that last `100%` is the bubble's own
//! width again), so a bubble too wide to sit centred on an edge trigger
//! slides in instead of running off-screen. The bubble itself wraps
//! (`whitespace-normal`) inside a viewport-relative `max-width`, so a long
//! phrase folds into a paragraph rather than staying one line wider than the
//! window. This still is not a collision-aware positioner: the bubble is
//! always placed above the trigger, never flipped below it.

use leptos::html;
use leptos::prelude::*;

/// Gap kept between the bubble and the left/right viewport edge, in pixels.
const VIEWPORT_MARGIN_PX: f64 = 8.0;
/// Gap between the trigger's top edge and the bubble's bottom edge.
const TRIGGER_GAP_PX: f64 = 4.0;

/// shadcn `Tooltip`: `children` is the trigger, `content` the bubble.
#[component]
pub fn Tooltip(
    /// The bubble's text, evaluated on every render.
    #[prop(into)]
    content: Signal<String>,
    children: Children,
) -> impl IntoView {
    let (open, set_open) = signal(false);
    let (style, set_style) = signal(String::new());
    let trigger_ref = NodeRef::<html::Span>::new();

    // Only the trigger is measured, and only on `pointerenter`: the bubble
    // mounts after `set_open` below, so nothing of its own would be laid out
    // yet to measure even if this needed it.
    let reposition = move || {
        let Some(trigger) = trigger_ref.get_untracked() else {
            return;
        };
        let rect = trigger.get_bounding_client_rect();
        let center_x = rect.left() + rect.width() / 2.0;
        let top = rect.top();
        set_style.set(format!(
            "left: 0; top: {top}px; transform: translate(\
             clamp({VIEWPORT_MARGIN_PX}px, calc({center_x}px - 50%), \
             calc(100vw - {VIEWPORT_MARGIN_PX}px - 100%)), \
             calc(-100% - {TRIGGER_GAP_PX}px));"
        ));
    };

    view! {
        <span
            data-slot="tooltip-trigger"
            class="inline-flex"
            node_ref=trigger_ref
            on:pointerenter=move |_| {
                reposition();
                set_open.set(true);
            }
            on:pointerleave=move |_| set_open.set(false)
        >
            {children()}
            <Show when=move || open.get()>
                <span
                    data-slot="tooltip-content"
                    role="tooltip"
                    class="pointer-events-none fixed z-50 w-max max-w-[min(24rem,calc(100vw-1rem))] \
                           rounded-md bg-foreground px-2 py-1 text-xs text-background shadow-md \
                           whitespace-normal"
                    style=move || style.get()
                >
                    {move || content.get()}
                </span>
            </Show>
        </span>
    }
}
