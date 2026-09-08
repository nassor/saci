//! The Logs tab: a tail over the inspector's retained event buffer.
//!
//! ## Why the window is fetched whole and filtered here
//!
//! `/api/logs` returns the newest records, at or above one level, and nothing
//! more: it has no substring or field predicate, and adding one would cost a
//! round trip per keystroke. The tab therefore holds one window of up to
//! [`LOG_LIMIT`] records and applies the text and scope filters over that in
//! the browser, so typing narrows the view without waiting for the network.
//!
//! The level chips are the one filter with a server side too. They re-request
//! at the chosen level, because a window filled with `debug` chatter can hold
//! no `error` at all, and no client-side predicate can recover a record the
//! response never carried.
//!
//! ## Why rendering is capped
//!
//! A thousand rows is four thousand DOM nodes, and the poll would rebuild all
//! of them. Only the newest [`RENDER_CAP`] matching records are drawn, and the
//! footer says so: silently dropping the rest would misrepresent the buffer.
//!
//! ## Why following pauses on expand
//!
//! A record has no id on the wire, so a row is addressed by its index into the
//! fetched window, and the next poll renumbers every one of them. Expanding a
//! row therefore stops the tail, which is also what an operator reading one
//! record wants. The poll keeps running while paused so the badge can say how
//! many records arrived.

use leptos::prelude::*;
use leptos::task::spawn_local;
use saci_inspector_wire::{LogRecord, Pair};

use crate::api;
use crate::ui::{
    Badge, BadgeTone, Button, Card, CardContent, CardHeader, CardTitle, Input, ScrollArea, Select,
    ToggleGroup, ToggleItem,
};

/// How many records one window holds. `MAX_LIMIT` in the service's
/// `inspector_api` caps a request at 1000.
const LOG_LIMIT: usize = 1000;

/// How many matching records are drawn at once.
const RENDER_CAP: usize = 400;

/// Poll period. Slower than the snapshot's 1 Hz: a log row costs four DOM
/// nodes, and under load every poll replaces the whole rendered window.
const POLL_MS: u64 = 1500;

/// How many field chips a collapsed row shows before the rest collapse behind
/// a count.
const INLINE_FIELDS: usize = 2;

/// The level filter's options, coarsest first. `None` is "everything the
/// buffer holds".
const LEVELS: [(&str, Option<&str>); 5] = [
    ("all", None),
    ("debug", Some("debug")),
    ("info", Some("info")),
    ("warn", Some("warn")),
    ("error", Some("error")),
];

/// Field keys that name the unit of work a record belongs to.
///
/// The scope filter offers one entry per distinct `key=value` pair the window
/// carries under one of these, which is how a viewer isolates one workflow's
/// or one node's records without typing a substring that also matches a
/// message.
const SCOPE_KEYS: [&str; 7] = [
    "workflow",
    "node",
    "source",
    "processor",
    "sink",
    "stage",
    "system",
];

/// Severity rank, coarsest first, for the client-side "at or above" test.
fn rank(level: &str) -> u8 {
    match level {
        "ERROR" => 0,
        "WARN" => 1,
        "INFO" => 2,
        "DEBUG" => 3,
        _ => 4,
    }
}

/// The chip class for one level.
pub(crate) fn level_class(level: &str) -> &'static str {
    match level {
        "ERROR" => "saci-lvl saci-lvl-error",
        "WARN" => "saci-lvl saci-lvl-warn",
        "INFO" => "saci-lvl saci-lvl-info",
        "DEBUG" => "saci-lvl saci-lvl-debug",
        _ => "saci-lvl saci-lvl-trace",
    }
}

/// Unix milliseconds as `HH:MM:SS.mmm` UTC.
///
/// UTC rather than local time: the record's own timestamp is the only clock
/// the wire carries, and a locale conversion would need a timezone database
/// the bundle does not ship.
pub(crate) fn format_clock(at_unix_ms: u64) -> String {
    let secs = at_unix_ms / 1000;
    let ms = at_unix_ms % 1000;
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}.{ms:03}")
}

/// The last two `::` segments of a module path, which is what distinguishes
/// two targets in practice. The full path stays in the row's `title` and in
/// the expanded detail.
pub(crate) fn short_target(target: &str) -> String {
    let mut parts = target.rsplit("::");
    match (parts.next(), parts.next()) {
        (Some(last), Some(before)) => format!("{before}::{last}"),
        (Some(last), None) => last.to_string(),
        _ => target.to_string(),
    }
}

/// The `key=value` scope this record belongs to, if it carries one.
fn scope_of(fields: &[Pair]) -> Option<String> {
    SCOPE_KEYS.iter().find_map(|key| {
        fields
            .iter()
            .find(|(name, _)| name == key)
            .map(|(name, value)| format!("{name}={value}"))
    })
}

/// Whether the record's message, target or fields contain `needle`, which the
/// caller has already lowercased.
fn matches_text(record: &LogRecord, needle: &str) -> bool {
    if needle.is_empty() {
        return true;
    }
    if record.message.to_lowercase().contains(needle)
        || record.target.to_lowercase().contains(needle)
    {
        return true;
    }
    record.fields.iter().any(|(key, value)| {
        key.to_lowercase().contains(needle) || value.to_lowercase().contains(needle)
    })
}

/// The Logs tab.
#[component]
pub fn LogsView() -> impl IntoView {
    let (records, set_records) = signal::<Vec<LogRecord>>(Vec::new());
    let (level, set_level) = signal::<Option<&'static str>>(None);
    let (query, set_query) = signal(String::new());
    let (scope, set_scope) = signal(String::new());
    let (following, set_following) = signal(true);
    let (pending, set_pending) = signal(0usize);
    let (expanded, set_expanded) = signal::<Option<usize>>(None);
    let (loaded, set_loaded) = signal(false);

    // One fetch, applied either to the view or to the pending counter. The
    // newest displayed timestamp is what decides how many of the fetched
    // records are new, so a paused tab can report arrivals without disturbing
    // what the viewer is reading.
    let load = move |replace: bool| {
        spawn_local(async move {
            let Ok(list) = api::logs(LOG_LIMIT, level.get_untracked()).await else {
                set_loaded.set(true);
                return;
            };
            if replace {
                set_records.set(list);
                set_pending.set(0);
                set_expanded.set(None);
            } else {
                let newest = records
                    .get_untracked()
                    .first()
                    .map_or(0, |record: &LogRecord| record.at_unix_ms);
                set_pending.set(
                    list.iter()
                        .filter(|record| record.at_unix_ms > newest)
                        .count(),
                );
            }
            set_loaded.set(true);
        });
    };
    load(true);

    let handle = set_interval_with_handle(
        move || {
            if !document().hidden() {
                load(following.get_untracked());
            }
        },
        std::time::Duration::from_millis(POLL_MS),
    )
    .expect("setInterval is available in every browser that can run WebAssembly");
    on_cleanup(move || handle.clear());

    // Recomputed only when the window or a filter changes, not on every render
    // of every row. The index is the row's identity, so it travels with the
    // record.
    let filtered = Memo::new(move |_| {
        let needle = query.get().trim().to_lowercase();
        let scope = scope.get();
        let floor = level.get().map(|name| rank(&name.to_uppercase()));
        records
            .get()
            .into_iter()
            .enumerate()
            .filter(|(_, record)| {
                floor.is_none_or(|floor| rank(&record.level) <= floor)
                    && (scope.is_empty() || scope_of(&record.fields).as_deref() == Some(&scope))
                    && matches_text(record, &needle)
            })
            .take(RENDER_CAP)
            .collect::<Vec<_>>()
    });

    // Every distinct scope the window carries, so the dropdown offers exactly
    // the workflows and nodes that are actually logging.
    let scopes = Memo::new(move |_| {
        let mut seen: Vec<String> = Vec::new();
        for record in records.get() {
            if let Some(scope) = scope_of(&record.fields)
                && !seen.contains(&scope)
            {
                seen.push(scope);
            }
        }
        seen.sort_unstable();
        let mut options = vec![(String::new(), "all scopes".to_string())];
        options.extend(seen.into_iter().map(|scope| (scope.clone(), scope)));
        options
    });

    let total = move || records.get().len();
    let shown = move || filtered.get().len();
    let capped = move || shown() >= RENDER_CAP;

    let clear_filters = move || {
        set_query.set(String::new());
        set_scope.set(String::new());
        set_level.set(None);
        load(true);
    };

    view! {
        <Card>
            <CardHeader>
                <div class="flex flex-wrap items-center justify-between gap-x-4 gap-y-3">
                    <div class="flex items-baseline gap-2">
                        <CardTitle>"Logs"</CardTitle>
                        <span class="font-mono text-xs text-muted-foreground tabular-nums">
                            {move || {
                                if capped() {
                                    format!("{} of {} retained", RENDER_CAP, total())
                                } else {
                                    format!("{} of {} retained", shown(), total())
                                }
                            }}
                        </span>
                    </div>
                    <div class="flex flex-wrap items-center gap-2">
                        <ToggleGroup>
                            {LEVELS
                                .into_iter()
                                .map(|(label, value)| {
                                    view! {
                                        <ToggleItem
                                            active=Signal::derive(move || level.get() == value)
                                            on_select=Callback::new(move |()| {
                                                set_level.set(value);
                                                load(following.get());
                                            })
                                        >
                                            {label}
                                        </ToggleItem>
                                    }
                                })
                                .collect_view()}
                        </ToggleGroup>
                        <Input
                            placeholder="filter message or field…"
                            value=Signal::derive(move || query.get())
                            on_input=Callback::new(move |value: String| set_query.set(value))
                            width="w-60"
                        />
                        <Select
                            options=Signal::derive(move || scopes.get())
                            value=Signal::derive(move || scope.get())
                            on_change=Callback::new(move |value: String| set_scope.set(value))
                            width="w-44"
                        />
                        <ToggleGroup>
                            <ToggleItem
                                active=Signal::derive(move || following.get())
                                on_select=Callback::new(move |()| {
                                    set_following.set(true);
                                    load(true);
                                })
                            >
                                "follow"
                            </ToggleItem>
                            <ToggleItem
                                active=Signal::derive(move || !following.get())
                                on_select=Callback::new(move |()| set_following.set(false))
                            >
                                "pause"
                            </ToggleItem>
                        </ToggleGroup>
                        <Show when=move || { !following.get() && pending.get() > 0 }>
                            <Badge tone=BadgeTone::Primary>
                                {move || {
                                    if pending.get() >= LOG_LIMIT {
                                        format!("{LOG_LIMIT}+ new")
                                    } else {
                                        format!("{} new", pending.get())
                                    }
                                }}
                            </Badge>
                        </Show>
                    </div>
                </div>
            </CardHeader>
            <CardContent>
                <Show
                    when=move || !filtered.get().is_empty()
                    fallback=move || {
                        view! {
                            <div class="flex flex-col items-start gap-2 rounded-md border border-dashed border-border px-4 py-8">
                                <p class="text-sm text-muted-foreground">
                                    {move || {
                                        if !loaded.get() {
                                            "Loading the event buffer…".to_string()
                                        } else if total() == 0 {
                                            "No events retained. The per-iteration tree is \
                                             recorded at debug, so `observability \
                                             log_level=\"info\"` leaves this empty."
                                                .to_string()
                                        } else {
                                            format!(
                                                "No record in the window of {} matches these filters.",
                                                total(),
                                            )
                                        }
                                    }}
                                </p>
                                <Show when=move || { loaded.get() && total() > 0 }>
                                    <Button on_click=Callback::new(move |()| clear_filters())>
                                        "clear filters"
                                    </Button>
                                </Show>
                            </div>
                        }
                    }
                >
                    <div class="overflow-hidden rounded-md border border-border">
                        <div class="grid grid-cols-[6.5rem_4.25rem_12rem_1fr] gap-x-3 border-b border-border bg-muted/60 px-3 py-1.5 text-[0.6875rem] font-medium text-muted-foreground">
                            <span>"time utc"</span>
                            <span>"level"</span>
                            <span>"target"</span>
                            <span>"message"</span>
                        </div>
                        <ScrollArea height="h-[calc(100vh-15rem)]">
                            <div>
                                {move || {
                                    filtered
                                        .get()
                                        .into_iter()
                                        .map(|(index, record)| {
                                            row(index, record, expanded, set_expanded, set_following)
                                        })
                                        .collect_view()
                                }}
                            </div>
                        </ScrollArea>
                    </div>
                    <Show when=capped>
                        <p class="mt-2 text-xs text-muted-foreground">
                            {move || {
                                format!(
                                    "Showing the newest {RENDER_CAP} matching records of the {} in \
                                     the window. Narrow the filter to reach older ones.",
                                    total(),
                                )
                            }}
                        </p>
                    </Show>
                </Show>
            </CardContent>
        </Card>
    }
}

/// One record: a dense row, plus its full detail when it is the expanded one.
fn row(
    index: usize,
    record: LogRecord,
    expanded: ReadSignal<Option<usize>>,
    set_expanded: WriteSignal<Option<usize>>,
    set_following: WriteSignal<bool>,
) -> AnyView {
    let is_open = move || expanded.get() == Some(index);
    let fields = record.fields.clone();
    let inline: Vec<Pair> = fields.iter().take(INLINE_FIELDS).cloned().collect();
    let hidden = fields.len().saturating_sub(INLINE_FIELDS);
    let message = record.message.clone();
    let target = record.target.to_string();

    view! {
        <div class="border-b border-border/60 last:border-0">
            <div
                class="grid cursor-pointer grid-cols-[6.5rem_4.25rem_12rem_1fr] items-baseline gap-x-3 px-3 py-1 font-mono text-xs hover:bg-muted/40"
                on:click=move |_| {
                    if is_open() {
                        set_expanded.set(None);
                    } else {
                        set_expanded.set(Some(index));
                        set_following.set(false);
                    }
                }
            >
                <span class="text-muted-foreground tabular-nums">
                    {format_clock(record.at_unix_ms)}
                </span>
                <span class=level_class(&record.level)>{record.level.to_string()}</span>
                <span class="truncate text-muted-foreground" title=target.clone()>
                    {short_target(&record.target)}
                </span>
                <div class="flex min-w-0 items-baseline gap-1.5">
                    <span class="truncate">{message.clone()}</span>
                    {inline
                        .into_iter()
                        .map(|(key, value)| {
                            view! {
                                <span class="saci-kv">
                                    <b>{key}</b>
                                    "="
                                    {value}
                                </span>
                            }
                        })
                        .collect_view()}
                    <Show when=move || { hidden > 0 }>
                        <span class="saci-kv">{format!("+{hidden}")}</span>
                    </Show>
                </div>
            </div>
            <Show when=is_open>
                {
                    let record = record.clone();
                    move || detail_block(&record)
                }
            </Show>
        </div>
    }
    .into_any()
}

/// The expanded record: full message, identity rows, every field.
fn detail_block(record: &LogRecord) -> AnyView {
    view! {
        <div class="border-t border-border/60 bg-muted/40 px-3 py-2">
            <p class="font-mono text-xs break-words whitespace-pre-wrap">
                {record.message.clone()}
            </p>
            <dl class="mt-2 grid grid-cols-[6rem_1fr] gap-x-3 gap-y-0.5 font-mono text-[0.6875rem]">
                {detail_rows(record)
                    .into_iter()
                    .map(|(key, value)| {
                        view! {
                            <>
                                <dt class="text-muted-foreground">{key}</dt>
                                <dd class="break-all">{value}</dd>
                            </>
                        }
                    })
                    .collect_view()}
            </dl>
            {(!record.fields.is_empty())
                .then(|| {
                    view! {
                        <div class="mt-2 flex flex-wrap gap-1">
                            {record
                                .fields
                                .clone()
                                .into_iter()
                                .map(|(key, value)| {
                                    view! {
                                        <span class="saci-kv">
                                            <b>{key}</b>
                                            "="
                                            {value}
                                        </span>
                                    }
                                })
                                .collect_view()}
                        </div>
                    }
                })}
        </div>
    }
    .into_any()
}

/// The identity rows of the expanded detail: everything about the record that
/// is not a structured field.
fn detail_rows(record: &LogRecord) -> Vec<(&'static str, String)> {
    let mut rows = vec![
        ("target", record.target.to_string()),
        ("level", record.level.to_string()),
        ("at", format!("{} utc", format_clock(record.at_unix_ms))),
    ];
    if let Some(span_id) = record.span_id {
        rows.push(("span", span_id.to_string()));
    }
    if let Some(trace_id) = record.trace_id {
        rows.push(("trace", trace_id.to_string()));
    }
    rows
}
