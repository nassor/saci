//! The Traces tab: a sortable trace list plus an SVG waterfall.
//!
//! ## What one trace is
//!
//! The host opens one `workflow.batch` root span per iteration, holding a
//! `source.drain` per source node, a `runtime.run` per processor node, and a
//! `sink.write` per sink node. Under `runtime.run` sits whatever the runtime
//! opens: `pipeline.run` and its stage and system spans for a native pipeline,
//! one `processor.batch` for a WASM processor or a native plugin. A processor's
//! own `pipeline.stage` and `system.execute` spans stay inside the guest,
//! because `host-io` has no span import, so a wasm processor or a native
//! plugin is one bar rather than a subtree.
//!
//! ## Why the list sorts client-side
//!
//! `/api/traces` returns the newest `limit` traces and takes no sort or filter
//! parameter. Sorting and filtering therefore run over the fetched window, so
//! ordering by duration reorders the same set instantly rather than asking for
//! a different one.
//!
//! ## What self time means here
//!
//! A span's self time is its duration minus the duration of its direct
//! children, floored at zero. The retention window can expire a child while
//! its parent survives, and a wasm or plugin guest opens spans the host never
//! sees, so a missing child inflates self time rather than being invented: the
//! number is exactly "time this span held that no retained child accounts
//! for".

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};

use leptos::prelude::*;
use leptos::task::spawn_local;
use saci_inspector_wire::{LogRecord, SpanRecord, Topology, TraceDetail, TraceSummary};

use crate::api;
use crate::components::Swatch;
use crate::components::logs::{format_clock, level_class, short_target};
use crate::ui::{
    Badge, BadgeTone, Button, Card, CardContent, CardHeader, CardTitle, Input, ScrollArea, Table,
    TableBody, TableCell, TableHead, TableHeader, TableRow, ToggleGroup, ToggleItem,
};

/// How many traces the list requests.
const TRACE_LIMIT: usize = 200;

/// How often the list refreshes. Only the list: the open trace's spans are a
/// separate fetch, so a refresh never disturbs the waterfall a viewer is
/// reading, and the newest trace is auto-selected only while nothing is.
const POLL_MS: u64 = 4000;

/// Waterfall geometry, in viewBox units.
const BAR_H: f64 = 16.0;
const BAR_GAP: f64 = 3.0;
/// Height of the time axis strip above the first bar.
const AXIS_H: f64 = 24.0;
/// Track width. The card scrolls rather than shrinking, so this is an authored
/// size and not a fit-to-viewport one.
const TRACK_W: f64 = 860.0;
/// Room to the right of the track for each bar's duration label.
const GUTTER_W: f64 = 132.0;
/// Narrowest bar drawn. A sub-millisecond span inside a 30 ms trace is under a
/// tenth of a unit wide, and an invisible bar cannot be hovered.
const MIN_BAR_W: f64 = 6.0;
/// Horizontal label indent per tree level.
const INDENT_W: f64 = 11.0;
/// Levels beyond which the label stops indenting, so it still fits the label
/// column.
const MAX_INDENT_LEVEL: usize = 6;
/// How many bars one waterfall draws.
const ROW_CAP: usize = 300;
/// Vertical gridlines, and therefore tick labels, across the track.
const TICKS: usize = 6;

/// Which column the list is ordered by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Sort {
    /// Newest first.
    Newest,
    /// Slowest first.
    Slowest,
}

/// The plane a span belongs to, which is what colours its bar.
///
/// The same three-way vocabulary the graph uses: connector IO is the data
/// plane, the host-to-guest call is the boundary, and the orchestration the
/// host does around them is the control plane.
fn span_colour(name: &str) -> &'static str {
    if name.starts_with("source") || name.starts_with("sink") {
        "var(--data)"
    } else if name.starts_with("runtime") || name.starts_with("processor") {
        "var(--boundary)"
    } else if name.starts_with("workflow") || name.starts_with("pipeline") {
        "var(--control)"
    } else {
        "var(--dgm-mute)"
    }
}

/// The Traces tab.
#[component]
pub fn TracesView(#[prop(into)] topology: Signal<Option<Topology>>) -> impl IntoView {
    let (traces, set_traces) = signal::<Vec<TraceSummary>>(Vec::new());
    let (selected, set_selected) = signal::<Option<u64>>(None);
    let (detail, set_detail) = signal::<Option<TraceDetail>>(None);
    let (loaded, set_loaded) = signal(false);
    let (sort, set_sort) = signal(Sort::Newest);
    let (query, set_query) = signal(String::new());

    let select = Callback::new(move |trace_id: u64| {
        set_selected.set(Some(trace_id));
        set_detail.set(None);
        spawn_local(async move {
            match api::trace(trace_id).await {
                Ok(value) => set_detail.set(Some(value)),
                Err(_) => set_detail.set(None),
            }
        });
    });

    // Landing on an empty pane says nothing about the running service, so the
    // newest trace opens itself. Only when nothing is selected: a refresh must
    // not pull the viewer off the trace they are reading.
    let refresh = move || {
        spawn_local(async move {
            if let Ok(list) = api::traces(TRACE_LIMIT).await {
                let newest = list.first().map(|summary| summary.trace_id);
                set_traces.set(list);
                if let (None, Some(trace_id)) = (selected.get_untracked(), newest) {
                    select.run(trace_id);
                }
            }
            set_loaded.set(true);
        });
    };
    refresh();

    let handle = set_interval_with_handle(
        move || {
            if !document().hidden() {
                refresh();
            }
        },
        std::time::Duration::from_millis(POLL_MS),
    )
    .expect("setInterval is available in every browser that can run WebAssembly");
    on_cleanup(move || handle.clear());

    let rows = Memo::new(move |_| {
        let needle = query.get().trim().to_lowercase();
        let mut list: Vec<TraceSummary> = traces
            .get()
            .into_iter()
            .filter(|summary| needle.is_empty() || summary.name.to_lowercase().contains(&needle))
            .collect();
        match sort.get() {
            Sort::Newest => list.sort_by_key(|a| Reverse(a.started_unix_ms)),
            Sort::Slowest => list.sort_by_key(|a| Reverse(a.duration_us)),
        }
        list
    });

    // The bar scale: the slowest trace in the filtered set is full width, so a
    // slow outlier is findable without reading every number.
    let slowest = Memo::new(move |_| {
        rows.get()
            .iter()
            .map(|summary| summary.duration_us)
            .max()
            .unwrap_or(1)
            .max(1)
    });

    // A wasm processor and a native plugin are each one bar: neither one's
    // inner spans reach the host. Said under the waterfall, where the missing
    // depth is visible, rather than in the empty state, which now only means no
    // iteration has finished.
    let one_bar_processor = move || {
        topology.get().is_some_and(|topo| {
            topo.workflows
                .iter()
                .flat_map(|w| w.nodes.iter())
                .any(|node| {
                    node.runtime
                        .as_ref()
                        .is_some_and(|rt| rt.kind == "wasm" || rt.kind == "plugin")
                })
        })
    };

    // Stacked, not side by side: the waterfall carries an authored width of
    // about 1150px, and any two-column split at a 1280px window clips its
    // track and its duration labels. `min-w-0` keeps that authored width
    // scrolling inside its own card instead of widening the page.
    view! {
        <div class="grid gap-4 [&>*]:min-w-0">
            <Card>
                <CardHeader>
                    <div class="flex flex-wrap items-center justify-between gap-x-4 gap-y-3">
                        <div class="flex items-baseline gap-2">
                            <CardTitle>"Traces"</CardTitle>
                            <span class="font-mono text-xs text-muted-foreground tabular-nums">
                                {move || format!("{} retained", rows.get().len())}
                            </span>
                        </div>
                        <div class="flex flex-wrap items-center gap-2">
                            <ToggleGroup>
                                <ToggleItem
                                    active=Signal::derive(move || sort.get() == Sort::Newest)
                                    on_select=Callback::new(move |()| set_sort.set(Sort::Newest))
                                >
                                    "newest"
                                </ToggleItem>
                                <ToggleItem
                                    active=Signal::derive(move || sort.get() == Sort::Slowest)
                                    on_select=Callback::new(move |()| set_sort.set(Sort::Slowest))
                                >
                                    "slowest"
                                </ToggleItem>
                            </ToggleGroup>
                            <Input
                                placeholder="root span…"
                                value=Signal::derive(move || query.get())
                                on_input=Callback::new(move |value: String| set_query.set(value))
                                width="w-40"
                            />
                            <Button on_click=Callback::new(move |()| {
                                set_selected.set(None);
                                refresh();
                            })>"refresh"</Button>
                        </div>
                    </div>
                </CardHeader>
                <CardContent>
                    <Show
                        when=move || !rows.get().is_empty()
                        fallback=move || {
                            view! {
                                <div class="rounded-md border border-dashed border-border px-4 py-8">
                                    <p class="text-sm text-muted-foreground">
                                        {move || {
                                            if !loaded.get() {
                                                "Loading traces…"
                                            } else if traces.get().is_empty() {
                                                "No traces retained yet. One trace appears per \
                                                 workflow iteration, and the whole tree is \
                                                 recorded at debug."
                                            } else {
                                                "No retained trace has a root span matching that \
                                                 name."
                                            }
                                        }}
                                    </p>
                                </div>
                            }
                        }
                    >
                        <ScrollArea height="h-[calc(100vh-34rem)] min-h-[16rem]">
                            <Table>
                                <TableHeader>
                                    <TableRow>
                                        <TableHead>"root span"</TableHead>
                                        <TableHead>"started utc"</TableHead>
                                        <TableHead>"duration"</TableHead>
                                        <TableHead>"spans"</TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {move || {
                                        let scale = slowest.get();
                                        rows.get()
                                            .into_iter()
                                            .map(|summary| {
                                                trace_row(summary, scale, selected, select)
                                            })
                                            .collect_view()
                                    }}
                                </TableBody>
                            </Table>
                        </ScrollArea>
                    </Show>
                </CardContent>
            </Card>

            <Card>
                <CardHeader>
                    <div class="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
                        <CardTitle>"Waterfall"</CardTitle>
                        <span class="font-mono text-xs text-muted-foreground">
                            {move || {
                                selected
                                    .get()
                                    .map_or_else(
                                        || "no trace selected".to_string(),
                                        |id| format!("trace {id}"),
                                    )
                            }}
                        </span>
                    </div>
                </CardHeader>
                <CardContent>
                    <Show
                        when=move || detail.get().is_some()
                        fallback=move || {
                            view! {
                                <div class="rounded-md border border-dashed border-border px-4 py-8">
                                    <p class="text-sm text-muted-foreground">
                                        {move || {
                                            if selected.get().is_some() {
                                                "Loading spans…"
                                            } else {
                                                "Select a trace to see its spans."
                                            }
                                        }}
                                    </p>
                                </div>
                            }
                        }
                    >
                        {move || detail.get().map(waterfall)}
                        <Show when=one_bar_processor>
                            <p class="mt-3 text-xs text-muted-foreground">
                                "A wasm processor or a native plugin is one bar: its \
                                 pipeline.stage and system.execute spans stay inside it, and \
                                 neither host-io nor the plugin ABI carries a span import."
                            </p>
                        </Show>
                    </Show>
                </CardContent>
            </Card>
        </div>
    }
}

/// One row of the trace list: identity, wall-clock start, a duration bar, and
/// the span count.
fn trace_row(
    summary: TraceSummary,
    slowest: u64,
    selected: ReadSignal<Option<u64>>,
    select: Callback<u64>,
) -> AnyView {
    let trace_id = summary.trace_id;
    let is_selected = Signal::derive(move || selected.get() == Some(trace_id));
    #[allow(clippy::cast_precision_loss, reason = "durations stay inside f64")]
    let fraction = (summary.duration_us as f64 / slowest as f64).clamp(0.02, 1.0);
    let error = summary.error;

    view! {
        <TableRow selected=is_selected on_click=Callback::new(move |()| select.run(trace_id))>
            <TableCell>
                <div class="flex items-center gap-2">
                    <span class="font-medium">{summary.name.to_string()}</span>
                    <Show when=move || error>
                        <Badge tone=BadgeTone::Destructive>"error"</Badge>
                    </Show>
                </div>
            </TableCell>
            <TableCell>
                <span class="font-mono text-xs text-muted-foreground tabular-nums">
                    {format_clock(summary.started_unix_ms)}
                </span>
            </TableCell>
            <TableCell>
                <div class="flex w-44 items-center gap-2">
                    <span class="w-16 shrink-0 text-right font-mono text-xs tabular-nums">
                        {format_micros(summary.duration_us)}
                    </span>
                    <span class="h-1.5 min-w-16 flex-1 rounded-full bg-border">
                        <span
                            class="block h-full rounded-full bg-primary"
                            style=format!("width: {:.1}%", fraction * 100.0)
                        ></span>
                    </span>
                </div>
            </TableCell>
            <TableCell>
                <span class="font-mono text-xs text-muted-foreground tabular-nums">
                    {summary.span_count.to_string()}
                </span>
            </TableCell>
        </TableRow>
    }
    .into_any()
}

/// One bar per span, x by offset from trace start, indent by tree depth.
///
/// Rows are laid out depth first, not by start time: `started_unix_ms` is
/// millisecond-resolution and one iteration is often shorter than that, so
/// sorting a five-level tree on it would interleave parents and children
/// arbitrarily.
fn waterfall(detail: TraceDetail) -> AnyView {
    let TraceDetail { spans, logs } = detail;
    if spans.is_empty() {
        return view! {
            <p class="text-sm text-muted-foreground">
                "This trace retained no spans: the buffer expired them while its logs survived."
            </p>
        }
        .into_any();
    }
    let ordered = depth_first(&spans);
    let self_us = self_times(&spans);
    let truncated = ordered.len().saturating_sub(ROW_CAP);

    let start = spans.iter().map(|s| s.started_unix_ms).min().unwrap_or(0);
    let span_end = |span: &SpanRecord| span.started_unix_ms + span.duration_us / 1000;
    let end = spans
        .iter()
        .map(span_end)
        .max()
        .unwrap_or(start)
        .max(start + 1);
    #[allow(clippy::cast_precision_loss, reason = "millisecond spans are small")]
    let total_ms = (end - start) as f64;

    // The label column widens with the longest name at its own indent, so a
    // deep `pipeline.stage` subtree is not clipped and a shallow trace wastes
    // no horizontal room.
    #[allow(clippy::cast_precision_loss, reason = "names are short")]
    let label_w = ordered
        .iter()
        .take(ROW_CAP)
        .map(|(span, level)| {
            let indent = level.min(&MAX_INDENT_LEVEL);
            #[allow(clippy::cast_precision_loss, reason = "the level is capped at 6")]
            let indent = *indent as f64 * INDENT_W;
            indent + span.name.chars().count() as f64 * 6.2 + 24.0
        })
        .fold(150.0_f64, f64::max)
        .min(320.0);
    let width = label_w + TRACK_W + GUTTER_W;
    #[allow(
        clippy::cast_precision_loss,
        reason = "the row count is capped at ROW_CAP"
    )]
    let height = AXIS_H + ordered.len().min(ROW_CAP) as f64 * (BAR_H + BAR_GAP) + 4.0;

    let axis: Vec<_> = (0..=TICKS)
        .map(|tick| {
            #[allow(clippy::cast_precision_loss, reason = "TICKS is 6")]
            let fraction = tick as f64 / TICKS as f64;
            let x = label_w + fraction * TRACK_W;
            let at_us = (total_ms * 1000.0 * fraction).round();
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "a trace duration is small and non-negative"
            )]
            let label = format_micros(at_us as u64);
            view! {
                <g>
                    <line
                        x1=format!("{x:.1}")
                        y1=format!("{AXIS_H}")
                        x2=format!("{x:.1}")
                        y2=format!("{height:.1}")
                        stroke="var(--dgm-edge)"
                        stroke-width="1"
                    />
                    <text
                        x=format!("{x:.1}")
                        y="14"
                        font-size="9"
                        text-anchor=if tick == 0 { "start" } else { "middle" }
                        fill="var(--muted-foreground)"
                        font-family="var(--font-mono)"
                    >
                        {label}
                    </text>
                </g>
            }
        })
        .collect();

    let bars: Vec<_> = ordered
        .iter()
        .take(ROW_CAP)
        .enumerate()
        .map(|(index, &(span, level))| {
            #[allow(clippy::cast_precision_loss, reason = "the row count is capped")]
            let row = index as f64;
            let y = AXIS_H + row * (BAR_H + BAR_GAP);
            #[allow(clippy::cast_precision_loss, reason = "millisecond offsets are small")]
            let offset_ms = (span.started_unix_ms - start) as f64;
            #[allow(clippy::cast_precision_loss, reason = "durations stay inside f64")]
            let width_ms = span.duration_us as f64 / 1000.0;
            let w = (width_ms / total_ms * TRACK_W).clamp(MIN_BAR_W, TRACK_W);
            // `started_unix_ms` is millisecond-resolution while the track is
            // scaled to the trace's own span, so the last child of a trace
            // shorter than a few milliseconds rounds onto the right edge. Clamp
            // the bar back inside the track instead of drawing it off-canvas.
            let x = (label_w + offset_ms / total_ms * TRACK_W).min(label_w + TRACK_W - w);
            #[allow(clippy::cast_precision_loss, reason = "the level is capped at 6")]
            let depth = level.min(MAX_INDENT_LEVEL) as f64 * INDENT_W;
            let colour = span_colour(&span.name);
            let own = self_us.get(&span.span_id).copied().unwrap_or(0);
            let hover = hover_text(span, own);

            view! {
                <g class="saci-bar">
                    <title>{hover}</title>
                    <rect
                        x="0"
                        y=format!("{y:.1}")
                        width=format!("{:.1}", label_w + TRACK_W + GUTTER_W)
                        height=format!("{BAR_H}")
                        fill="transparent"
                    />
                    <rect
                        x=format!("{:.1}", depth + 2.0)
                        y=format!("{:.1}", y + 3.0)
                        width="2"
                        height=format!("{:.1}", BAR_H - 6.0)
                        fill=colour
                    />
                    <text
                        x=format!("{:.1}", depth + 10.0)
                        y=format!("{:.1}", y + 11.5)
                        font-size="10.5"
                        fill="var(--foreground)"
                        font-family="var(--font-mono)"
                    >
                        {span.name.to_string()}
                    </text>
                    <rect
                        x=format!("{x:.1}")
                        y=format!("{:.1}", y + 2.0)
                        width=format!("{w:.1}")
                        height=format!("{:.1}", BAR_H - 4.0)
                        rx="2"
                        fill=colour
                        opacity="0.82"
                    />
                    <text
                        x=format!("{:.1}", label_w + TRACK_W + 8.0)
                        y=format!("{:.1}", y + 11.5)
                        font-size="10"
                        fill="var(--muted-foreground)"
                        font-family="var(--font-mono)"
                    >
                        {if own == span.duration_us {
                            format_micros(span.duration_us)
                        } else {
                            format!(
                                "{} · self {}",
                                format_micros(span.duration_us),
                                format_micros(own),
                            )
                        }}
                    </text>
                </g>
            }
        })
        .collect();

    view! {
        <div class="space-y-3">
            <div class="saci-canvas rounded-md border border-border bg-[var(--dgm-bg)] p-2">
                <svg
                    viewBox=format!("0 0 {width:.0} {height:.0}")
                    preserveAspectRatio="xMinYMin meet"
                    class="w-full"
                    style=format!("min-width: {width:.0}px")
                >
                    <line
                        x1="0"
                        y1=format!("{AXIS_H}")
                        x2=format!("{width:.0}")
                        y2=format!("{AXIS_H}")
                        stroke="var(--dgm-edge)"
                        stroke-width="1"
                    />
                    {axis}
                    {bars}
                </svg>
            </div>

            <div class="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
                <Swatch colour="var(--data)" label="source / sink io" />
                <Swatch colour="var(--boundary)" label="host to guest call" />
                <Swatch colour="var(--control)" label="workflow / pipeline" />
                <span>"self = duration no retained child accounts for"</span>
            </div>

            <Show when={
                let hidden = truncated;
                move || { hidden > 0 }
            }>
                <p class="text-xs text-muted-foreground">
                    {format!(
                        "Showing the first {ROW_CAP} spans of this trace; {truncated} deeper rows \
                         are not drawn.",
                    )}
                </p>
            </Show>

            <Show when={
                let has_logs = !logs.is_empty();
                move || has_logs
            }>
                <div>
                    <h3 class="mb-1.5 text-xs font-medium text-muted-foreground">
                        {format!("{} events inside this trace", logs.len())}
                    </h3>
                    <div class="overflow-hidden rounded-md border border-border">
                        {logs.clone().into_iter().map(trace_log_row).collect_view()}
                    </div>
                </div>
            </Show>
        </div>
    }
    .into_any()
}

/// One event emitted inside the open trace, in the Logs tab's row shape.
fn trace_log_row(record: LogRecord) -> AnyView {
    let fields = record
        .fields
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(" ");
    view! {
        <div class="grid grid-cols-[6.5rem_4.25rem_12rem_1fr] items-baseline gap-x-3 border-b border-border/60 px-3 py-1 font-mono text-xs last:border-0">
            <span class="text-muted-foreground tabular-nums">
                {format_clock(record.at_unix_ms)}
            </span>
            <span class=level_class(&record.level)>{record.level.to_string()}</span>
            <span class="truncate text-muted-foreground" title=record.target.to_string()>
                {short_target(&record.target)}
            </span>
            <div class="flex min-w-0 items-baseline gap-1.5">
                <span class="truncate">{record.message.clone()}</span>
                <Show when={
                    let any = !fields.is_empty();
                    move || any
                }>
                    <span class="saci-kv">{fields.clone()}</span>
                </Show>
            </div>
        </div>
    }
    .into_any()
}

/// The bar's hover text: identity, both durations, then every field.
fn hover_text(span: &SpanRecord, self_us: u64) -> String {
    let mut lines = vec![
        span.name.to_string(),
        format!("total {}", format_micros(span.duration_us)),
        format!("self  {}", format_micros(self_us)),
        format!("target {}", span.target),
    ];
    if span.fields.is_empty() {
        lines.push("no fields".to_string());
    } else {
        lines.extend(
            span.fields
                .iter()
                .map(|(key, value)| format!("{key}={value}")),
        );
    }
    lines.join("\n")
}

/// Each span's duration minus the duration of its direct children, floored at
/// zero.
///
/// Children are summed rather than unioned: the host opens a workflow's
/// `source.drain`, `runtime.run` and `sink.write` in sequence, so their spans
/// do not overlap and the sum is the time the parent spent inside them.
fn self_times(spans: &[SpanRecord]) -> HashMap<u64, u64> {
    let mut children_us: HashMap<u64, u64> = HashMap::new();
    for span in spans {
        if let Some(parent) = span.parent_id {
            *children_us.entry(parent).or_default() += span.duration_us;
        }
    }
    spans
        .iter()
        .map(|span| {
            let inside = children_us.get(&span.span_id).copied().unwrap_or(0);
            (span.span_id, span.duration_us.saturating_sub(inside))
        })
        .collect()
}

/// Order `spans` parent before child, returning each with its tree level.
///
/// A span whose `parent_id` is absent from `spans` is treated as a root: the
/// retention window can expire a parent while its children survive, and
/// dropping the orphans would hide work that ran.
fn depth_first(spans: &[SpanRecord]) -> Vec<(&SpanRecord, usize)> {
    let mut children: HashMap<Option<u64>, Vec<&SpanRecord>> = HashMap::new();
    let present: HashSet<u64> = spans.iter().map(|span| span.span_id).collect();
    for span in spans {
        let key = span.parent_id.filter(|id| present.contains(id));
        children.entry(key).or_default().push(span);
    }
    for group in children.values_mut() {
        group.sort_by_key(|span| (span.started_unix_ms, span.span_id));
    }

    // Explicit stack rather than recursion: the tree comes off the wire, so its
    // depth is not a local invariant.
    let mut ordered = Vec::with_capacity(spans.len());
    let mut stack: Vec<(&SpanRecord, usize)> = children
        .get(&None)
        .map(|roots| roots.iter().rev().map(|span| (*span, 0)).collect())
        .unwrap_or_default();
    while let Some((span, level)) = stack.pop() {
        ordered.push((span, level));
        if ordered.len() > spans.len() {
            // A parent cycle would otherwise loop forever. Cannot happen with
            // `tracing`'s ids, but the wire is not a proof.
            break;
        }
        if let Some(group) = children.get(&Some(span.span_id)) {
            stack.extend(group.iter().rev().map(|child| (*child, level + 1)));
        }
    }
    ordered
}

/// `1234` as `1.23ms`, `900` as `900µs`, `2_500_000` as `2.50s`.
pub fn format_micros(micros: u64) -> String {
    #[allow(clippy::cast_precision_loss, reason = "durations stay inside f64")]
    let us = micros as f64;
    if us >= 1_000_000.0 {
        format!("{:.2}s", us / 1_000_000.0)
    } else if us >= 1_000.0 {
        format!("{:.2}ms", us / 1_000.0)
    } else {
        format!("{us:.0}µs")
    }
}
