//! `Table` and its parts.

use leptos::prelude::*;

/// shadcn `Table`, including the overflow wrapper its root carries.
#[component]
pub fn Table(children: Children) -> impl IntoView {
    view! {
        <div data-slot="table-container" class="relative w-full overflow-x-auto">
            <table data-slot="table" class="w-full caption-bottom text-sm">
                {children()}
            </table>
        </div>
    }
}

/// shadcn `TableHeader`.
#[component]
pub fn TableHeader(children: Children) -> impl IntoView {
    view! {
        <thead data-slot="table-header" class="[&_tr]:border-b">
            {children()}
        </thead>
    }
}

/// shadcn `TableBody`.
#[component]
pub fn TableBody(children: Children) -> impl IntoView {
    view! {
        <tbody data-slot="table-body" class="[&_tr:last-child]:border-0">
            {children()}
        </tbody>
    }
}

/// shadcn `TableRow`.
#[component]
pub fn TableRow(
    /// Highlight the row as selected.
    #[prop(into, default = Signal::derive(|| false))]
    selected: Signal<bool>,
    /// Click handler, when the row is selectable.
    #[prop(into, optional)]
    on_click: Option<Callback<()>>,
    children: Children,
) -> impl IntoView {
    let clickable = on_click.is_some();
    let class = move || {
        let base = "border-b transition-colors hover:bg-muted/50";
        match (selected.get(), clickable) {
            (true, _) => format!("{base} bg-muted"),
            (false, true) => format!("{base} cursor-pointer"),
            (false, false) => base.to_string(),
        }
    };
    view! {
        <tr
            data-slot="table-row"
            class=class
            on:click=move |_| {
                if let Some(handler) = on_click {
                    handler.run(());
                }
            }
        >
            {children()}
        </tr>
    }
}

/// shadcn `TableHead`.
#[component]
pub fn TableHead(children: Children) -> impl IntoView {
    view! {
        <th
            data-slot="table-head"
            class="h-9 px-2 text-left align-middle text-xs font-medium text-muted-foreground \
                   whitespace-nowrap"
        >
            {children()}
        </th>
    }
}

/// shadcn `TableCell`.
#[component]
pub fn TableCell(children: Children) -> impl IntoView {
    view! {
        <td data-slot="table-cell" class="p-2 align-middle whitespace-nowrap">
            {children()}
        </td>
    }
}
