//! `Badge`, in the four tones the dashboard needs.

use leptos::prelude::*;

/// Which shadcn badge variant to render.
///
/// A closed enum rather than a class string parameter: Tailwind only sees whole
/// literals, so every variant's classes must appear literally in this file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BadgeTone {
    /// Filled, brand colour. Ready, healthy, on.
    Primary,
    /// Filled, destructive colour. Errors, retries above zero.
    Destructive,
    /// Muted fill. Neutral facts.
    Secondary,
    /// Outline only. Labels and counts.
    Outline,
}

impl BadgeTone {
    /// The variant's own classes, appended to the shared base.
    fn classes(self) -> &'static str {
        match self {
            Self::Primary => "border-transparent bg-primary text-primary-foreground",
            Self::Destructive => "border-transparent bg-destructive text-white",
            Self::Secondary => "border-transparent bg-secondary text-secondary-foreground",
            Self::Outline => "border-border text-foreground",
        }
    }
}

/// shadcn `Badge`.
#[component]
pub fn Badge(
    /// Visual tone.
    #[prop(default = BadgeTone::Outline)]
    tone: BadgeTone,
    children: Children,
) -> impl IntoView {
    let class = format!(
        "inline-flex w-fit shrink-0 items-center justify-center gap-1 overflow-hidden \
         whitespace-nowrap rounded-full border px-2 py-0.5 text-xs font-medium {}",
        tone.classes()
    );
    view! {
        <span data-slot="badge" class=class>
            {children()}
        </span>
    }
}
