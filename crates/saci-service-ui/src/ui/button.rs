//! `Button`, in the two tones the dashboard needs.

use leptos::prelude::*;

/// Which shadcn button variant to render.
///
/// A closed enum rather than a class string parameter: Tailwind only sees whole
/// literals, so every variant's classes must appear literally in this file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ButtonTone {
    /// Outline. Everything that re-fetches, and every reversible action.
    Outline,
    /// Outline in the destructive colour. Stopping a workflow.
    Destructive,
}

impl ButtonTone {
    /// The variant's own classes, appended to the shared base.
    fn classes(self) -> &'static str {
        match self {
            Self::Outline => {
                "border-border bg-background hover:bg-accent hover:text-accent-foreground"
            }
            Self::Destructive => {
                "border-destructive/40 bg-background text-destructive hover:bg-destructive/10"
            }
        }
    }
}

/// shadcn `Button`, `size="sm"`.
///
/// `disabled` is bound to the attribute as well as the class list: a verb the
/// service would refuse is unclickable, not merely dimmed.
#[component]
pub fn Button(
    /// Click handler. Not called while `disabled`.
    #[prop(into)]
    on_click: Callback<()>,
    /// Visual tone.
    #[prop(default = ButtonTone::Outline)]
    tone: ButtonTone,
    /// Whether the button refuses input.
    #[prop(optional, into)]
    disabled: Signal<bool>,
    children: Children,
) -> impl IntoView {
    let class = format!(
        "inline-flex h-8 shrink-0 items-center justify-center gap-1.5 rounded-md border px-3 \
         text-xs font-medium whitespace-nowrap transition-colors outline-none \
         focus-visible:ring-[3px] focus-visible:ring-ring/50 disabled:pointer-events-none \
         disabled:opacity-50 {}",
        tone.classes()
    );
    view! {
        <button
            data-slot="button"
            type="button"
            class=class
            disabled=move || disabled.get()
            on:click=move |_| {
                if !disabled.get_untracked() {
                    on_click.run(());
                }
            }
        >
            {children()}
        </button>
    }
}
