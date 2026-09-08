//! The Pipelines tab: the workflow topology as an animated SVG.
//!
//! ## Why the structure is built once
//!
//! The graph's shape comes from `/api/topology`, which is fixed for the
//! process lifetime; only the numbers change. The element tree is therefore
//! created once, and every live value is a reactive attribute closure inside
//! it. Re-rendering the whole `<svg>` each second would restart the dash
//! animations and the `<animateMotion>` particles on every poll, so the flow
//! would visibly stutter at exactly 1 Hz.
//!
//! ## Why the canvas is authored, not fitted
//!
//! Each `<svg>` carries its laid-out size in pixels and its card scrolls, the
//! same rule `docs/static/styles.css` applies to a page diagram. Fitting a
//! graph to the viewport instead trades one problem for a worse one: a
//! six-way fan-out is twice as tall as it is wide, and scaling it to the card
//! shrinks every label past reading size.
//!
//! ## Colour vocabulary
//!
//! The `--dgm-*` custom properties are the same ones `docs/static/styles.css`
//! uses, and they mean the same thing here. A source is the data plane
//! (`--dgm-hd-data`); a sink is the same plane on its write side, so it takes
//! the write tint (`--dgm-row-w`) that marks a write there; a processor box is
//! the host-to-WebAssembly boundary (`--dgm-hd-bnd`); and control-plane facts,
//! the windowing chip among them, are teal (`--dgm-hd-ctl`). Each box also
//! carries a solid accent bar in its plane's full-strength colour, so the
//! three kinds stay apart at a glance and in a screenshot. They are read
//! directly as CSS variables rather than through Tailwind's utility
//! generator, because they style a handful of SVG shapes rather than layout.
//!
//! ## Where a node box's number comes from
//!
//! Every node records its own metric series under its own declared id: a source
//! under [`SOURCE_ATTR`], a processor under [`PROCESSOR_ATTR`], a sink under
//! [`SINK_ATTR`]. Each box reads the copy carrying its own id, so its
//! throughput, latency, sparkline and retry badge describe that one node. The
//! unattributed copy of the same series is the process-wide sum over every
//! node — what a `/metrics` consumer reads — so no box shows it.
//! `saci_stage_duration_seconds` is the exception: the host's span metrics layer
//! records it with no attributes at all, so a native processor's box reads the
//! process-wide value.
//!
//! A sink box reads its own `saci_sink_rows_written_total`, the rows the
//! runner handed to that sink, and reports records per second. It never reads
//! `saci_sink_batches_written_total` as its headline number: a batch is
//! whatever row count the upstream stage handed over, so a batch rate says
//! nothing about records moved. That counter appears in the detail sheet as a
//! total, beside the same edge rate the server publishes.
//!
//! ## How the layout is chosen
//!
//! A workflow is a DAG, so a node's column is its depth: the longest path from
//! any entry node to it. The nodes of a `WorkflowTopology` arrive in
//! topological order, so one forward pass relaxing every edge settles every
//! depth. A `source -> sink` pass-through is two columns and a two-processor
//! chain is four, which is what makes every edge point forward.
//!
//! Within a column, nodes are ordered by the average row of the predecessors
//! that feed them, sweeping left to right. That is the one cheap pass of the
//! standard layered-graph heuristic, and it is what keeps a fan-in from
//! drawing as a braid: without it a column keeps topology order, and two
//! edges whose endpoints are ordered oppositely cross in the gap.
//!
//! Each workflow lays out on its own `viewBox`, so two workflows never share a
//! depth column: a node's column is its depth inside its own DAG.
//! Cross-workflow channel bridges are listed in their own card below the
//! workflow cards, because an edge between two independent `<svg>`s has no
//! shared coordinate space to draw in.

use std::collections::HashMap;

use leptos::prelude::*;
use leptos::task::spawn_local;
use saci_inspector_wire::{
    BridgeEdge, EdgeRate, LogRecord, PROCESSOR_ATTR, Pair, PointAt, SINK_ATTR, SOURCE_ATTR,
    SeriesKind, SeriesSummary, Snapshot, TopoEdge, TopoNode, Topology, WORKFLOW_ATTR, WindowInfo,
    WorkflowRunState, WorkflowStatus, WorkflowTopology,
};

use crate::api;
use crate::components::Swatch;
use crate::components::logs::{format_clock, level_class};
use crate::ui::{Badge, BadgeTone, Button, ButtonTone, Sheet, Tooltip};

/// Node box geometry, in viewBox units, which are CSS pixels here.
const NODE_W: f64 = 208.0;
const NODE_H: f64 = 88.0;
const NODE_GAP: f64 = 22.0;
/// Clear space between one column's right edge and the next column's left. The
/// branch label chip has to fit inside it, which is what keeps a chip from
/// overlapping a box.
const COL_GAP: f64 = 92.0;
const COL_PITCH: f64 = NODE_W + COL_GAP;
/// Margin around the whole graph.
const PAD: f64 = 20.0;
/// Height of a box's title strip.
const HEADER_H: f64 = 24.0;
/// The sparkline's own box, bottom right of a node.
pub(super) const SPARK_W: f64 = 92.0;
pub(super) const SPARK_H: f64 = 28.0;
/// How many records the node detail sheet scans for events naming its node,
/// and how many of the matches it lists.
const NODE_LOG_WINDOW: usize = 400;
const NODE_LOG_ROWS: usize = 12;

/// Widest branch chip. Narrower than [`COL_GAP`], so a chip drawn on an edge's
/// midpoint cannot reach either box.
const CHIP_MAX_W: f64 = 84.0;

/// One node with its resolved position.
#[derive(Clone)]
struct Placed {
    node: TopoNode,
    x: f64,
    y: f64,
}

impl Placed {
    fn centre_y(&self) -> f64 {
        self.y + NODE_H / 2.0
    }
}

/// The plane one node kind belongs to: its accent colour and its header tint.
fn plane(kind: &str) -> (&'static str, &'static str) {
    match kind {
        "processor" => ("var(--boundary)", "var(--dgm-hd-bnd)"),
        "sink" => ("var(--data)", "var(--dgm-row-w)"),
        _ => ("var(--data)", "var(--dgm-hd-data)"),
    }
}

/// The name a workflow or node shows, falling back to its id.
///
/// A name is optional on the wire: a config that named nothing leaves it
/// absent, and an id is always present and unique workflow-wide.
pub(super) fn display(name: &Option<String>, id: &str) -> String {
    match name {
        Some(name) if !name.trim().is_empty() => name.clone(),
        _ => id.to_string(),
    }
}

/// `text` shortened to `max` characters with a trailing ellipsis.
///
/// SVG text does not wrap or clip to a width, so a long node name has to be
/// cut before it is drawn. The full value stays in the box's hover title and
/// in its detail sheet.
fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    text.chars().take(max.saturating_sub(1)).collect::<String>() + "…"
}

/// Place every node in its depth column, left to right.
///
/// A node's column is the longest path from any entry node to it, so every edge
/// points forward: two chained processors sharing one column would draw as a
/// backwards hook. `nodes` arrives in topological order, so relaxing each
/// node's outgoing edges once, in that order, settles every depth. A node whose
/// id no edge names keeps depth zero and lands in the first column.
///
/// Returns the placed nodes and the canvas size, which grows with the deepest
/// column and the tallest one.
fn layout(nodes: &[TopoNode], edges: &[TopoEdge]) -> (Vec<Placed>, f64, f64) {
    let index: HashMap<&str, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, node)| (node.id.as_str(), i))
        .collect();
    let mut outgoing: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
    let mut incoming: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
    for edge in edges {
        if let (Some(&from), Some(&to)) =
            (index.get(edge.from.as_str()), index.get(edge.to.as_str()))
        {
            outgoing[from].push(to);
            incoming[to].push(from);
        }
    }
    let mut depth: Vec<usize> = vec![0; nodes.len()];
    for (from, targets) in outgoing.iter().enumerate() {
        for &to in targets {
            depth[to] = depth[to].max(depth[from] + 1);
        }
    }

    let columns = depth.iter().copied().max().map_or(0, |deepest| deepest + 1);
    let mut by_column: Vec<Vec<usize>> = vec![Vec::new(); columns];
    for (i, &at) in depth.iter().enumerate() {
        by_column[at].push(i);
    }

    // Order each column by the average row of its predecessors, sweeping left
    // to right so a column is ordered against rows that are already fixed.
    // `f64::MAX` keeps a node with no fed-from predecessor at the bottom
    // instead of pulling it to row zero, and the topology index breaks every
    // tie, so the order is the same on every poll.
    let mut row_of: Vec<f64> = vec![0.0; nodes.len()];
    for column in &mut by_column {
        for (row, &i) in column.iter().enumerate() {
            #[allow(clippy::cast_precision_loss, reason = "node counts are small")]
            let value = row as f64;
            row_of[i] = value;
        }
    }
    for column in by_column.iter_mut().skip(1) {
        let mut ordered: Vec<(f64, usize, usize)> = column
            .iter()
            .enumerate()
            .map(|(row, &i)| {
                let parents = &incoming[i];
                let barycentre = if parents.is_empty() {
                    f64::MAX
                } else {
                    #[allow(clippy::cast_precision_loss, reason = "node counts are small")]
                    let count = parents.len() as f64;
                    parents.iter().map(|&p| row_of[p]).sum::<f64>() / count
                };
                (barycentre, row, i)
            })
            .collect();
        ordered.sort_by(|a, b| {
            a.0.partial_cmp(&b.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.1.cmp(&b.1))
        });
        *column = ordered.iter().map(|&(_, _, i)| i).collect();
        for (row, &i) in column.iter().enumerate() {
            #[allow(clippy::cast_precision_loss, reason = "node counts are small")]
            let value = row as f64;
            row_of[i] = value;
        }
    }

    let column_height = |count: usize| {
        #[allow(clippy::cast_precision_loss, reason = "node counts are small")]
        let count = count.max(1) as f64;
        count * NODE_H + (count - 1.0) * NODE_GAP
    };
    let tallest = by_column
        .iter()
        .map(|column| column_height(column.len()))
        .fold(NODE_H, f64::max);
    let height = PAD * 2.0 + tallest;

    let mut placed = Vec::with_capacity(nodes.len());
    for (at, column) in by_column.iter().enumerate() {
        #[allow(clippy::cast_precision_loss, reason = "column counts are small")]
        let x = PAD + COL_PITCH * at as f64;
        let offset = PAD + (tallest - column_height(column.len())) / 2.0;
        for (row, &i) in column.iter().enumerate() {
            #[allow(clippy::cast_precision_loss, reason = "node counts are small")]
            let row = row as f64;
            placed.push(Placed {
                node: nodes[i].clone(),
                x,
                y: offset + row * (NODE_H + NODE_GAP),
            });
        }
    }
    #[allow(clippy::cast_precision_loss, reason = "column counts are small")]
    let last_x = PAD + COL_PITCH * columns.saturating_sub(1) as f64;
    let view_w = last_x + NODE_W + PAD;

    (placed, height, view_w)
}

/// Every distinct runtime kind the workflow's processors run under, in first
/// appearance order.
fn runtime_kinds(nodes: &[TopoNode]) -> Vec<String> {
    let mut kinds: Vec<String> = Vec::new();
    for kind in nodes
        .iter()
        .filter_map(|node| node.runtime.as_ref())
        .map(|runtime| &runtime.kind)
    {
        if !kinds.iter().any(|seen| seen == kind) {
            kinds.push(kind.clone());
        }
    }
    kinds
}

/// A cubic from the right edge of one box to the left edge of another.
///
/// Both control points sit on the column gap's midline, so the curve leaves
/// and arrives horizontally and never re-enters either box.
fn edge_path(from: &Placed, to: &Placed) -> String {
    let x1 = from.x + NODE_W;
    let y1 = from.centre_y();
    let x2 = to.x;
    let y2 = to.centre_y();
    let mid = f64::midpoint(x1, x2);
    format!("M {x1:.1} {y1:.1} C {mid:.1} {y1:.1}, {mid:.1} {y2:.1}, {x2:.1} {y2:.1}")
}

/// Look up one edge's live rate by its exact `(from, to)` pair.
fn edge_rate(snapshot: &Option<Snapshot>, from: &str, to: &str) -> Option<EdgeRate> {
    snapshot.as_ref().and_then(|s| {
        s.edges
            .iter()
            .find(|e| e.from == from && e.to == to)
            .cloned()
    })
}

/// One series' numbers as a node box reads them.
pub(super) struct Reading {
    /// Which instrument produced it, which decides whether its history is
    /// already a rate or has to be differentiated.
    kind: SeriesKind,
    /// Newest value: a counter's total, a histogram's sum.
    pub(super) value: f64,
    /// Histogram observation count; `0` for a counter or a gauge.
    count: u64,
    /// Per-second rate across the two newest samples.
    pub(super) rate_per_sec: f64,
    /// History over the polled window, oldest first, as the wire sent it.
    points: Vec<PointAt>,
}

impl Reading {
    /// The history in the same unit as [`Reading::rate_per_sec`].
    ///
    /// A counter's points are cumulative and a histogram's are a running sum,
    /// so plotting them raw draws a monotonic ramp for every node whatever it
    /// is doing. Differentiating puts the curve in the unit the box's own
    /// number is in, which is what makes it a trend. A counter reset shows as
    /// a negative step and is clamped to zero rather than drawn as a spike
    /// downwards.
    pub(super) fn rate_points(&self) -> Vec<PointAt> {
        if self.kind == SeriesKind::Gauge {
            return self.points.clone();
        }
        self.points
            .windows(2)
            .map(|pair| {
                let (before, after) = (pair[0], pair[1]);
                #[allow(
                    clippy::cast_precision_loss,
                    reason = "a window's millisecond span stays inside f64"
                )]
                let dt = after.t.saturating_sub(before.t) as f64 / 1000.0;
                let v = if dt > 0.0 {
                    ((after.v - before.v) / dt).max(0.0)
                } else {
                    0.0
                };
                PointAt { t: after.t, v }
            })
            .collect()
    }
}

/// The first series `want` accepts, read into a [`Reading`].
fn find_series(
    snapshot: &Option<Snapshot>,
    want: impl Fn(&SeriesSummary) -> bool,
) -> Option<Reading> {
    snapshot.as_ref().and_then(|snap| {
        snap.series
            .iter()
            .find(|series| want(series))
            .map(|series| Reading {
                kind: series.kind,
                value: series.value,
                count: series.count,
                rate_per_sec: series.rate_per_sec,
                points: series.points.clone(),
            })
    })
}

/// The value `attrs` carries under `key`, if it carries one.
fn attr<'a>(attrs: &'a [Pair], key: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

/// One unattributed series: the process-wide form, summed over every node.
///
/// Only read for a series no node or workflow attributes:
/// `saci_stage_duration_seconds`, which the host's span metrics layer records
/// without fields.
fn series(snapshot: &Option<Snapshot>, name: &str) -> Option<Reading> {
    find_series(snapshot, |series| {
        series.name == name && series.attrs.is_empty()
    })
}

/// The `name` series attributed to one node id, under that node kind's `key`,
/// and to nothing else.
///
/// The single-attribute test is what makes this the node's own number. A
/// processor's `saci_processor_rows_out_total` is recorded once per node and
/// again per branch, so a match on `processor="<id>"` alone would return
/// whichever branch copy the response happens to list first.
///
/// A miss means the node has recorded nothing yet, never that the number lives
/// somewhere else: the attributed copy appears with the node's first sample.
pub(super) fn attributed(
    snapshot: &Option<Snapshot>,
    name: &str,
    key: &str,
    id: &str,
) -> Option<Reading> {
    find_series(snapshot, |series| {
        series.name == name && series.attrs.len() == 1 && attr(&series.attrs, key) == Some(id)
    })
}

/// The sink's own throughput: rows written per second.
///
/// `saci_sink_rows_written_total` is recorded per sink, so this needs no
/// upstream series and no branch resolution: the number is the sink's own.
/// The host records it on the same write as
/// `saci_sink_batches_written_total`, so there is no state in which a sink
/// has batches but no rows, and a batch is whatever row count the upstream
/// handed over anyway. An absent series means the sink has written nothing.
fn sink_reading(snapshot: &Option<Snapshot>, node_id: &str) -> Option<Reading> {
    attributed(snapshot, "saci_sink_rows_written_total", SINK_ATTR, node_id)
}

/// One rule for every rate and count the dashboard shows: `0` for zero, `<1`
/// below one, a plain integer up to a thousand, then `k` and `M` with one
/// decimal. The unit is drawn separately, so the number carries no suffix.
///
/// One rule rather than a decimal below ten and an integer above it: two
/// formats in one table read as two different measurements.
pub(super) fn format_count(value: f64) -> String {
    if value <= 0.0 {
        "0".to_string()
    } else if value < 1.0 {
        "<1".to_string()
    } else if value >= 1_000_000.0 {
        format!("{:.1}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.1}k", value / 1_000.0)
    } else {
        format!("{value:.0}")
    }
}

/// `12034.5` as `12.0k rows/s`, for a tooltip or a list row.
fn format_rate(rate: f64, unit: &str) -> String {
    format!("{} {unit}/s", format_count(rate))
}

/// Seconds as `1.2ms` / `340µs` / `2.1s`.
fn format_seconds(secs: f64) -> String {
    if secs >= 1.0 {
        format!("{secs:.2}s")
    } else if secs >= 0.001 {
        format!("{:.1}ms", secs * 1000.0)
    } else {
        format!("{:.0}µs", secs * 1_000_000.0)
    }
}

/// One line describing the whole window geometry, for the detail sheet.
fn window_spec_line(window: &WindowInfo) -> String {
    let geometry = match window.kind.as_str() {
        "sliding" => format!(
            "{} / {}",
            format_ms(window.size_ms),
            format_ms(window.slide_ms)
        ),
        "session" => format!("gap {}", format_ms(window.gap_ms)),
        _ => format_ms(window.size_ms),
    };
    let offset = window
        .offset_ms
        .filter(|&offset| offset != 0)
        .map_or_else(String::new, |offset| format!(" · offset {offset}ms"));
    format!("{} {}{offset}", window.kind, geometry)
}

/// The chip text on a windowed processor box: `⟐30s`, `⟐30s/5s` or `⟐gap5s`.
fn window_chip(window: &WindowInfo) -> String {
    match window.kind.as_str() {
        "sliding" => format!(
            "⟐{}/{}",
            format_ms(window.size_ms),
            format_ms(window.slide_ms)
        ),
        "session" => format!("⟐gap{}", format_ms(window.gap_ms)),
        _ => format!("⟐{}", format_ms(window.size_ms)),
    }
}

/// Milliseconds as a compact duration: `30000` → `30s`, `1500` → `1.5s`,
/// `500` → `500ms`.
fn format_ms(ms: Option<i64>) -> String {
    let Some(ms) = ms else {
        return "?".to_string();
    };
    if ms >= 1000 {
        if ms % 1000 == 0 {
            format!("{}s", ms / 1000)
        } else {
            #[allow(clippy::cast_precision_loss, reason = "window sizes are small")]
            let secs = ms as f64 / 1000.0;
            format!("{secs:.1}s")
        }
    } else {
        format!("{ms}ms")
    }
}

/// Epoch seconds as UTC wall-clock time, for a watermark reading.
fn format_epoch_utc(secs: f64) -> String {
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "an epoch second is non-negative and inside u64"
    )]
    let secs = secs.max(0.0) as u64;
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02} UTC")
}

/// A sparkline over a series' history, as a `(line, area)` path pair.
///
/// The area is the same curve closed to the baseline, which is what makes a
/// rising series read as a trend rather than as a jitter line. The vertical
/// scale runs from zero rather than from the window's own minimum: a counter
/// rate that never leaves a narrow band would otherwise fill the box with
/// magnified noise.
///
/// Returns two empty strings for fewer than two points: a one-point line is a
/// dot that reads as noise rather than a trend.
pub(super) fn sparkline(points: &[PointAt]) -> (String, String) {
    if points.len() < 2 {
        return (String::new(), String::new());
    }
    let max = points
        .iter()
        .map(|p| p.v)
        .fold(f64::NEG_INFINITY, f64::max)
        .max(f64::EPSILON);
    #[allow(
        clippy::cast_precision_loss,
        reason = "point counts are bounded at MAX_POINTS"
    )]
    let last = (points.len() - 1) as f64;
    let mut line = String::with_capacity(points.len() * 14);
    for (i, point) in points.iter().enumerate() {
        #[allow(
            clippy::cast_precision_loss,
            reason = "point counts are bounded at MAX_POINTS"
        )]
        let index = i as f64;
        let x = index / last * SPARK_W;
        let y = SPARK_H - (point.v.max(0.0) / max) * (SPARK_H - 2.0) - 1.0;
        if i == 0 {
            line.push_str(&format!("M {x:.1} {y:.1}"));
        } else {
            line.push_str(&format!(" L {x:.1} {y:.1}"));
        }
    }
    let area = format!("{line} L {SPARK_W:.1} {SPARK_H:.1} L 0 {SPARK_H:.1} Z");
    (line, area)
}

/// Dash animation period: faster with throughput, static at zero.
///
/// `8s / rate` clamped to a quarter second keeps a busy edge readable instead of
/// blurring into a solid line.
fn dash_duration(rate: f64) -> String {
    if rate <= 0.0 {
        return "0s".to_string();
    }
    let secs = (8.0 / rate.max(1.0)).clamp(0.25, 8.0);
    format!("{secs:.2}s")
}

/// Stroke width grows with the order of magnitude of the rate.
fn stroke_width(rate: f64) -> f64 {
    if rate <= 0.0 {
        return 1.0;
    }
    (1.0 + rate.max(1.0).log10()).clamp(1.0, 4.0)
}

/// The Pipelines tab.
#[component]
pub fn PipelinesView(
    #[prop(into)] topology: Signal<Option<Topology>>,
    #[prop(into)] snapshot: Signal<Option<Snapshot>>,
    /// Every controllable workflow's lifecycle state, or `None` when the
    /// service mounts no control plane. `None` hides the controls entirely.
    #[prop(into)]
    workflows: Signal<Option<Vec<WorkflowStatus>>>,
    /// `(workflow id, verb)`, where the verb is one of `start`, `stop`,
    /// `pause`, `resume` or `restart`.
    #[prop(into)]
    on_control: Callback<(String, &'static str)>,
    /// The same five verbs, applied to every workflow at once.
    #[prop(into)]
    on_control_all: Callback<&'static str>,
) -> impl IntoView {
    let (selected, set_selected) = signal::<Option<TopoNode>>(None);
    let (node_logs, set_node_logs) = signal::<Vec<LogRecord>>(Vec::new());

    // The event list under a node has to be about that node, so the fetch is
    // a wide window filtered on the node's own id appearing as a field value.
    // The newest ten records process-wide would be whichever workflow is
    // iterating fastest, which is worse than showing none.
    let open_node = Callback::new(move |node: TopoNode| {
        set_node_logs.set(Vec::new());
        let node_id = node.id.clone();
        spawn_local(async move {
            if let Ok(records) = api::logs(NODE_LOG_WINDOW, None).await {
                set_node_logs.set(
                    records
                        .into_iter()
                        .filter(|record| {
                            record
                                .fields
                                .iter()
                                .any(|(_, value)| value.as_str() == node_id)
                        })
                        .take(NODE_LOG_ROWS)
                        .collect(),
                );
            }
        });
        set_selected.set(Some(node));
    });

    let graph = move || {
        topology.get().map(|topo| {
            if topo.workflows.is_empty() {
                // A default `Topology` has no workflows, which is what
                // `/api/topology` returns before `build_all` publishes one.
                return view! {
                    <div class="rounded-xl border border-dashed border-border px-4 py-8">
                        <p class="text-sm text-muted-foreground">
                            "No workflow is declared. `/api/topology` publishes one per workflow \
                             once the service has built them."
                        </p>
                    </div>
                }
                .into_any();
            }
            let sections: Vec<_> = topo
                .workflows
                .into_iter()
                .map(|workflow| workflow_view(workflow, snapshot, open_node, workflows, on_control))
                .collect();
            let bridges = (!topo.bridges.is_empty()).then(|| bridges_view(topo.bridges, snapshot));
            view! { <div class="space-y-4">{sections}{bridges}</div> }.into_any()
        })
    };

    let sheet_title = Signal::derive(move || {
        selected
            .get()
            .map_or_else(String::new, |node| display(&node.name, &node.id))
    });

    view! {
        <div class="space-y-4">
            {move || service_controls(workflows, on_control_all)}

            <Show
                when=move || topology.get().is_some()
                fallback=|| {
                    view! {
                        <div class="rounded-xl border border-dashed border-border px-4 py-8">
                            <p class="text-sm text-muted-foreground">"Loading topology…"</p>
                        </div>
                    }
                }
            >
                {graph}
            </Show>

            <div class="flex flex-wrap items-center gap-x-5 gap-y-1.5 rounded-xl border border-border bg-card px-4 py-3 text-xs text-muted-foreground">
                <span class="font-medium text-foreground">"legend"</span>
                <Swatch colour="var(--data)" label="data plane: source and sink" />
                <Swatch colour="var(--boundary)" label="boundary: host to WebAssembly" />
                <Swatch colour="var(--control)" label="control plane: windowing" />
                <span>"the bar marks the side where data crosses the process boundary"</span>
                <span>"dashes and particles run at the edge's measured rate"</span>
            </div>

            <Sheet
                open=Signal::derive(move || selected.get().is_some())
                title=sheet_title
                on_close=Callback::new(move |()| set_selected.set(None))
            >
                {move || {
                    selected
                        .get()
                        .map(|node| detail_view(node, snapshot, node_logs))
                }}
            </Sheet>
        </div>
    }
}

/// One workflow's card: its declared identity, its runtime badges, its own
/// run/error counters, its lifecycle state and controls, and its own
/// independently laid out graph.
fn workflow_view(
    workflow: WorkflowTopology,
    snapshot: Signal<Option<Snapshot>>,
    on_open: Callback<TopoNode>,
    workflows: Signal<Option<Vec<WorkflowStatus>>>,
    on_control: Callback<(String, &'static str)>,
) -> AnyView {
    let (placed, height, view_w) = layout(&workflow.nodes, &workflow.edges);

    let by_id: HashMap<String, Placed> = placed
        .iter()
        .map(|placed| (placed.node.id.clone(), placed.clone()))
        .collect();

    // Drawn from the topology's own edge list rather than rebuilt as a
    // star: a workflow is a DAG, holding processor-to-processor and
    // source-to-sink edges no fixed shape can express. A missing
    // endpoint is skipped rather than panicking, so a future node kind
    // degrades to "not drawn" instead of a crash.
    let edge_views: Vec<_> = workflow
        .edges
        .iter()
        .filter_map(|edge| {
            let from = by_id.get(&edge.from)?.clone();
            let to = by_id.get(&edge.to)?.clone();
            Some(edge_view(from, to, edge.branch.clone(), snapshot))
        })
        .collect();

    let counts = kind_counts(&workflow.nodes);
    let node_views: Vec<_> = placed
        .into_iter()
        .map(|placed| node_view(placed, snapshot, on_open, workflow.id.clone()))
        .collect();

    let panels = super::flow::flow_panels(&workflow, snapshot, on_open);
    let title = display(&workflow.name, &workflow.id);
    // The id only earns a line of its own when it is not already the
    // title, which is every workflow the config named.
    let id = (title != workflow.id).then(|| workflow.id.clone());
    let kinds = runtime_kinds(&workflow.nodes);

    // Derived signals rather than plain closures: `Signal<T>` is `Copy`, so
    // the badges' own attribute closures can each read them without moving
    // the workflow id into every one. The attributed form is this workflow's
    // own counter; the unattributed one would be the sum across every
    // workflow the process runs.
    let runs = {
        let id = workflow.id.clone();
        Signal::derive(move || {
            attributed(
                &snapshot.get(),
                "saci_workflow_runs_total",
                WORKFLOW_ATTR,
                &id,
            )
            .map_or(0.0, |reading| reading.value)
        })
    };
    let errors = {
        let id = workflow.id.clone();
        Signal::derive(move || {
            attributed(
                &snapshot.get(),
                "saci_workflow_errors_total",
                WORKFLOW_ATTR,
                &id,
            )
            .map_or(0.0, |reading| reading.value)
        })
    };

    // This workflow's lifecycle, when the service mounts a control plane. A
    // `None` there is not an error: a cluster node and `http { control #false }`
    // both serve no `/api/workflows`, and the controls are simply absent.
    let status = {
        let id = workflow.id.clone();
        Signal::derive(move || {
            workflows
                .get()
                .and_then(|list| list.into_iter().find(|entry| entry.id == id))
        })
    };
    let state_badge = move || {
        status.get().map(|entry| {
            let tone = state_tone(entry.state);
            let label = entry.state.as_str();
            match entry.error.clone() {
                Some(error) => view! {
                    <Tooltip content=Signal::derive(move || error.clone())>
                        <Badge tone=tone>{label}</Badge>
                    </Tooltip>
                }
                .into_any(),
                None => view! { <Badge tone=tone>{label}</Badge> }.into_any(),
            }
        })
    };
    // Runner starts, which `saci_workflow_runs_total` does not count: that
    // series counts passes, and a restart leaves it where it was.
    let starts = move || {
        status.get().map(|entry| {
            let label = if entry.runs == 1 {
                "1 start".to_string()
            } else {
                format!("{} starts", entry.runs)
            };
            view! { <Badge tone=BadgeTone::Outline>{label}</Badge> }
        })
    };
    let hold = {
        let id = workflow.id.clone();
        move || {
            let entry = status.get()?;
            let parked = matches!(
                entry.state,
                WorkflowRunState::Pausing | WorkflowRunState::Paused
            );
            let verb = if parked { "resume" } else { "pause" };
            Some(control_button(
                verb,
                ButtonTone::Outline,
                id.clone(),
                status,
                on_control,
            ))
        }
    };
    let halt = {
        let id = workflow.id.clone();
        move || {
            let entry = status.get()?;
            let down = matches!(
                entry.state,
                WorkflowRunState::Stopped | WorkflowRunState::Completed | WorkflowRunState::Failed
            );
            let (verb, tone) = if down {
                ("start", ButtonTone::Outline)
            } else {
                ("stop", ButtonTone::Destructive)
            };
            Some(control_button(verb, tone, id.clone(), status, on_control))
        }
    };
    let restart = {
        let id = workflow.id.clone();
        move || {
            status.get()?;
            Some(control_button(
                "restart",
                ButtonTone::Outline,
                id.clone(),
                status,
                on_control,
            ))
        }
    };

    view! {
        <section class="rounded-xl border border-border bg-card">
            <header class="flex flex-wrap items-center gap-x-3 gap-y-2 border-b border-border px-4 py-3">
                <h2 class="text-sm font-semibold tracking-tight">{title}</h2>
                {id
                    .map(|id| {
                        view! {
                            <span class="font-mono text-xs text-muted-foreground">{id}</span>
                        }
                    })}
                <span class="font-mono text-xs text-muted-foreground">{counts}</span>
                <div class="ml-auto flex flex-wrap items-center gap-1.5">
                    {kinds
                        .into_iter()
                        .map(|kind| {
                            view! { <Badge tone=BadgeTone::Outline>{kind}</Badge> }
                        })
                        .collect_view()}
                    <Badge tone=BadgeTone::Secondary>
                        {move || format!("{} runs", runs.get() as u64)}
                    </Badge>
                    <Show when=move || { errors.get() > 0.0 }>
                        <Badge tone=BadgeTone::Destructive>
                            {move || format!("{} errors", errors.get() as u64)}
                        </Badge>
                    </Show>
                    {state_badge}
                    {starts}
                    {hold}
                    {halt}
                    {restart}
                </div>
            </header>
            <div class="saci-canvas p-2">
                <svg
                    viewBox=format!("0 0 {view_w:.0} {height:.0}")
                    width=format!("{view_w:.0}")
                    height=format!("{height:.0}")
                    role="img"
                    class="mx-auto block"
                >
                    {edge_views}
                    {node_views}
                </svg>
            </div>
            {panels}
        </section>
    }
    .into_any()
}

/// The service-wide lifecycle bar: one row of verbs acting on every
/// controllable workflow at once.
///
/// Absent when the service mounts no control plane, and absent when it
/// declares no workflow. Which verb each button carries follows the whole
/// service: `resume all` once every workflow is parked, `start all` once
/// every one is down.
fn service_controls(
    workflows: Signal<Option<Vec<WorkflowStatus>>>,
    on_control_all: Callback<&'static str>,
) -> Option<AnyView> {
    let list = workflows.get()?;
    if list.is_empty() {
        return None;
    }

    let parked = list.iter().all(|entry| {
        matches!(
            entry.state,
            WorkflowRunState::Pausing | WorkflowRunState::Paused
        )
    });
    let down = list.iter().all(|entry| {
        matches!(
            entry.state,
            WorkflowRunState::Stopped | WorkflowRunState::Completed | WorkflowRunState::Failed
        )
    });
    let hold = if parked { "resume" } else { "pause" };
    let (halt, halt_tone) = if down {
        ("start", ButtonTone::Outline)
    } else {
        ("stop", ButtonTone::Destructive)
    };

    let running = list
        .iter()
        .filter(|entry| entry.state == WorkflowRunState::Running)
        .count();
    let summary = format!("{running} of {} running", list.len());

    Some(
        view! {
            <section class="flex flex-wrap items-center gap-x-3 gap-y-2 rounded-xl border border-border bg-card px-4 py-3">
                <h2 class="text-sm font-semibold tracking-tight">"All workflows"</h2>
                <span class="font-mono text-xs text-muted-foreground">{summary}</span>
                <div class="ml-auto flex flex-wrap items-center gap-1.5">
                    {service_button(hold, ButtonTone::Outline, &list, on_control_all)}
                    {service_button(halt, halt_tone, &list, on_control_all)}
                    {service_button("restart", ButtonTone::Outline, &list, on_control_all)}
                </div>
            </section>
        }
        .into_any(),
    )
}

/// One service-wide button, enabled while at least one workflow would take the
/// verb.
///
/// The service endpoint applies a verb per workflow and reports the ones it
/// could not reach, so a partial sweep is a success. The tooltip says how many
/// will be left alone, which is the number an operator would otherwise have to
/// work out from the cards.
fn service_button(
    verb: &'static str,
    tone: ButtonTone,
    list: &[WorkflowStatus],
    on_control_all: Callback<&'static str>,
) -> AnyView {
    let blocked = list
        .iter()
        .filter(|entry| !verb_allowed(entry, verb))
        .count();
    let enabled = blocked < list.len();
    let hint = if !enabled {
        format!("no workflow can {verb} from its current state")
    } else if blocked == 0 {
        format!("{verb} every workflow")
    } else {
        format!("{verb} every workflow; {blocked} would refuse and is left alone")
    };
    let label = format!("{verb} all");
    view! {
        <Tooltip content=Signal::derive(move || hint.clone())>
            <Button
                tone=tone
                disabled=Signal::derive(move || !enabled)
                on_click=Callback::new(move |()| on_control_all.run(verb))
            >
                {label}
            </Button>
        </Tooltip>
    }
    .into_any()
}

/// The badge tone one lifecycle state reads in.
///
/// Only two states earn a colour: `running` is the healthy steady state and
/// `failed` is the one an operator has to act on. Everything else, the three
/// transient states included, is an outline, so the card does not flash a
/// colour on every ordinary transition.
fn state_tone(state: WorkflowRunState) -> BadgeTone {
    match state {
        WorkflowRunState::Running => BadgeTone::Secondary,
        WorkflowRunState::Failed => BadgeTone::Destructive,
        _ => BadgeTone::Outline,
    }
}

/// Whether the service would act on `verb` from this workflow's state.
///
/// Mostly the table `LifecycleRegistry::command` applies, so a disabled button
/// is either a request the service would refuse with `409`, or one it would
/// answer with an unchanged status, which is not worth a round trip.
///
/// `Completed` is the one state where this is stricter than the registry. A
/// workflow that finished its own work returns from its supervisor, which is
/// what lets a one-shot service exit; the registry would still dispatch
/// `start` and `restart`, but there is no longer anything listening, so the
/// only possible answer is `503`. An enabled button that can only produce an
/// error chip is worse than a disabled one carrying the reason.
fn verb_allowed(status: &WorkflowStatus, verb: &str) -> bool {
    use WorkflowRunState as S;
    if matches!(verb, "start" | "stop" | "restart") && !status.restartable {
        return false;
    }
    matches!(
        (verb, status.state),
        ("start", S::Stopped | S::Failed)
            | ("stop", S::Running | S::Pausing | S::Paused)
            | ("pause", S::Running)
            | ("resume", S::Pausing | S::Paused)
            | ("restart", S::Running | S::Paused | S::Stopped | S::Failed)
    )
}

/// What a control button's tooltip says: why the verb is unavailable when it
/// is, and what it does otherwise.
fn verb_hint(status: &WorkflowStatus, verb: &str) -> String {
    if matches!(verb, "start" | "stop" | "restart") {
        if let Some(reason) = &status.restart_blocked_reason {
            return reason.clone();
        }
        if status.state == WorkflowRunState::Completed {
            return "this workflow finished its own work and its supervisor has exited, so it \
                    cannot be started again without restarting the service"
                .to_string();
        }
    }
    match verb {
        "start" => "build this workflow and run it".to_string(),
        "stop" => "drain the runner, finish every sink, and drop the built workflow".to_string(),
        "pause" => {
            "park the runner between passes; it admits nothing and keeps its state".to_string()
        }
        "resume" => "release the parked runner".to_string(),
        _ => "stop, then start".to_string(),
    }
}

/// One lifecycle button, disabled and explained from the workflow's own
/// published status.
fn control_button(
    verb: &'static str,
    tone: ButtonTone,
    workflow_id: String,
    status: Signal<Option<WorkflowStatus>>,
    on_control: Callback<(String, &'static str)>,
) -> AnyView {
    let disabled =
        Signal::derive(move || status.get().is_none_or(|entry| !verb_allowed(&entry, verb)));
    let hint = Signal::derive(move || {
        status
            .get()
            .map_or_else(String::new, |entry| verb_hint(&entry, verb))
    });
    view! {
        <Tooltip content=hint>
            <Button
                tone=tone
                disabled=disabled
                on_click=Callback::new(move |()| {
                    on_control.run((workflow_id.clone(), verb));
                })
            >
                {verb}
            </Button>
        </Tooltip>
    }
    .into_any()
}

/// `2 sources · 1 processor · 4 sinks`, the shape of the workflow in words.
fn kind_counts(nodes: &[TopoNode]) -> String {
    let count = |kind: &str| nodes.iter().filter(|node| node.kind == kind).count();
    let plural = |n: usize, word: &str| {
        if n == 1 {
            format!("{n} {word}")
        } else {
            format!("{n} {word}s")
        }
    };
    let mut parts = vec![plural(count("source"), "source")];
    let processors = count("processor");
    if processors > 0 {
        parts.push(plural(processors, "processor"));
    }
    parts.push(plural(count("sink"), "sink"));
    parts.join(" · ")
}

/// The channel bridges between workflows, listed rather than drawn: each
/// workflow renders its own `<svg>`, so an edge between two of them has no
/// shared coordinate space. Rated by the same `(from, to)` lookup as any edge.
///
/// `"idle"` rather than `0/s`: an omitted `EdgeRate` means neither side has
/// sampled yet, which is not the same as measured zero.
fn bridges_view(bridges: Vec<BridgeEdge>, snapshot: Signal<Option<Snapshot>>) -> AnyView {
    let rows: Vec<_> = bridges
        .into_iter()
        .map(|bridge| {
            let from = bridge.from.clone();
            let to = bridge.to.clone();
            let rate = move || {
                edge_rate(&snapshot.get(), &from, &to).map_or_else(
                    || "idle".to_string(),
                    |r| format_rate(r.rate_per_sec, &r.unit),
                )
            };
            view! {
                <li class="flex items-center gap-3 px-4 py-2 text-sm">
                    <Badge tone=BadgeTone::Secondary>{bridge.channel}</Badge>
                    <span class="font-mono text-xs text-muted-foreground">
                        {bridge.from} " → " {bridge.to}
                    </span>
                    <span class="ml-auto font-mono text-xs tabular-nums">{rate}</span>
                </li>
            }
        })
        .collect();

    view! {
        <section class="rounded-xl border border-border bg-card">
            <header class="border-b border-border px-4 py-3">
                <h2 class="text-sm font-semibold tracking-tight">"channel bridges"</h2>
                <p class="mt-0.5 text-xs text-muted-foreground">
                    "A ChannelSink and a ChannelSource meeting on one name. No config declares \
                     the pair, so it is listed rather than drawn."
                </p>
            </header>
            <ul class="divide-y divide-border">{rows}</ul>
        </section>
    }
    .into_any()
}

/// One edge: a flow line, two particles when it is moving, and a branch label
/// chip when the edge carries one.
fn edge_view(
    from: Placed,
    to: Placed,
    branch: Option<String>,
    snapshot: Signal<Option<Snapshot>>,
) -> AnyView {
    let path = edge_path(&from, &to);
    // Derived signals rather than plain closures: `Signal<T>` is `Copy`, so the
    // same rate can be read from several attribute closures, including the
    // per-particle ones, without cloning the node ids into each.
    let rate = {
        let from_id = from.node.id.clone();
        let to_id = to.node.id.clone();
        Signal::derive(move || {
            edge_rate(&snapshot.get(), &from_id, &to_id).map_or(0.0, |edge| edge.rate_per_sec)
        })
    };

    let tooltip = {
        let from_id = from.node.id.clone();
        let to_id = to.node.id.clone();
        let branch = branch.clone();
        Signal::derive(move || {
            let mut lines = vec![format!("{from_id} → {to_id}")];
            if let Some(branch) = &branch {
                lines.push(format!("branch: {branch}"));
            }
            lines.push(edge_rate(&snapshot.get(), &from_id, &to_id).map_or_else(
                || "not sampled".to_string(),
                |edge| format_rate(edge.rate_per_sec, &edge.unit),
            ));
            lines.join("\n")
        })
    };

    // A cubic whose control points share the gap's midline passes through the
    // midpoint of its endpoints, so the chip sits on the line it labels. It is
    // centred there rather than lifted above it: two branches of one processor
    // land on rows a box apart, so their chips separate by half that, while a
    // lifted chip would drift into the box above.
    let mid_x = f64::midpoint(from.x + NODE_W, to.x);
    let mid_y = f64::midpoint(from.centre_y(), to.centre_y());

    let particle_path = path.clone();
    view! {
        <g>
            <title>{move || tooltip.get()}</title>
            <path
                d=path.clone()
                fill="none"
                stroke="var(--dgm-edge)"
                stroke-width=move || format!("{:.1}", stroke_width(rate.get()))
                class="saci-edge"
            />
            <path
                d=path.clone()
                fill="none"
                stroke="var(--data)"
                stroke-width=move || format!("{:.1}", stroke_width(rate.get()))
                stroke-dasharray="6 12"
                stroke-linecap="round"
                class="saci-edge-flow"
                style=move || {
                    let duration = dash_duration(rate.get());
                    let opacity = if rate.get() > 0.0 { "0.65" } else { "0" };
                    format!("--saci-dash-dur: {duration}; opacity: {opacity}")
                }
            />
            <Show when=move || { rate.get() > 0.0 }>
                {(0..2)
                    .map(|i| {
                        let begin = format!("{}s", f64::from(i) * 0.8);
                        let particle_path = particle_path.clone();
                        view! {
                            <circle r="2.5" fill="var(--data)" class="saci-particle">
                                <animateMotion
                                    dur=move || dash_duration(rate.get() / 4.0)
                                    begin=begin
                                    repeatCount="indefinite"
                                    path=particle_path
                                />
                            </circle>
                        }
                    })
                    .collect_view()}
            </Show>
            {branch
                .map(|label| {
                    #[allow(clippy::cast_precision_loss, reason = "labels are short")]
                    let width = (12.0 + 6.0 * label.chars().count() as f64).min(CHIP_MAX_W);
                    let fitted = truncate(&label, 12);
                    view! {
                        <g>
                            <rect
                                x=format!("{:.1}", mid_x - width / 2.0)
                                y=format!("{:.1}", mid_y - 8.0)
                                width=format!("{width:.1}")
                                height="16"
                                rx="8"
                                fill="var(--dgm-blk)"
                                stroke="var(--dgm-edge)"
                            />
                            <text
                                x=format!("{mid_x:.1}")
                                y=format!("{:.1}", mid_y + 3.5)
                                font-size="9.5"
                                text-anchor="middle"
                                fill="var(--foreground)"
                                font-family="var(--font-mono)"
                            >
                                {fitted}
                            </text>
                        </g>
                    }
                })}
        </g>
    }
    .into_any()
}

/// One node box: name, type, the metric that describes its role, and a
/// sparkline of the same series.
fn node_view(
    placed: Placed,
    snapshot: Signal<Option<Snapshot>>,
    on_open: Callback<TopoNode>,
    workflow_id: String,
) -> AnyView {
    let node = placed.node.clone();
    let is_processor = node.kind == "processor";
    let (accent, header_fill) = plane(&node.kind);
    // Which side of the box the accent bar marks: the side data crosses the
    // process boundary on. A source takes it in from the left, a sink hands it
    // out to the right, and a processor's guest call is a boundary in both
    // directions.
    let (bar_left, bar_right) = match node.kind.as_str() {
        "processor" => (true, true),
        "sink" => (false, true),
        _ => (true, false),
    };
    // Each processor carries its own runtime, so a workflow mixing a wasm
    // processor with a plugin one still reads the right series per box. The
    // wasm host and the native plugin host both record the six
    // `saci_processor_*` series from the batch's run metrics; an in-process
    // native runtime reports no per-batch numbers and records none of them.
    let per_batch_series = node
        .runtime
        .as_ref()
        .is_some_and(|runtime| runtime.kind == "wasm" || runtime.kind == "plugin");
    let title = display(&node.name, &node.id);
    let subtitle = match &node.component {
        Some(component) => format!("{} · {component}", node.type_name),
        None => node.type_name.clone(),
    };

    // The live number differs by role: throughput on a connector, mean batch
    // latency on a processor. Every lookup carries this node's own id, so a box
    // never shows the process-wide sum over its siblings. The label names the
    // unit, so the number itself stays one short token.
    let measure = {
        let node_id = node.id.clone();
        let kind = node.kind.clone();
        move || {
            let snap = snapshot.get();
            match kind.as_str() {
                "source" => attributed(&snap, "saci_rows_processed_total", SOURCE_ATTR, &node_id)
                    .map(|reading| ("records/s", format_count(reading.rate_per_sec))),
                "sink" => sink_reading(&snap, &node_id)
                    .map(|reading| ("records/s", format_count(reading.rate_per_sec))),
                _ => {
                    // A native runtime records none of the six series, and
                    // `saci_stage_duration_seconds` comes from the host's span
                    // metrics layer with no attributes at all, so such a box
                    // reads the process-wide form.
                    let reading = if per_batch_series {
                        attributed(
                            &snap,
                            "saci_processor_batch_duration_seconds",
                            PROCESSOR_ATTR,
                            &node_id,
                        )
                    } else {
                        series(&snap, "saci_stage_duration_seconds")
                    };
                    reading.map(|reading| {
                        if reading.count == 0 {
                            ("mean batch", "—".to_string())
                        } else {
                            #[allow(
                                clippy::cast_precision_loss,
                                reason = "observation counts stay well inside f64"
                            )]
                            let mean = reading.value / reading.count as f64;
                            ("mean batch", format_seconds(mean))
                        }
                    })
                }
            }
            .unwrap_or(("no samples yet", "—".to_string()))
        }
    };
    let label = {
        let measure = measure.clone();
        move || measure().0
    };
    let value = {
        let measure = measure.clone();
        move || measure().1
    };

    let spark = {
        let node_id = node.id.clone();
        let kind = node.kind.clone();
        move || {
            let snap = snapshot.get();
            let reading = match kind.as_str() {
                "source" => attributed(&snap, "saci_rows_processed_total", SOURCE_ATTR, &node_id),
                "sink" => sink_reading(&snap, &node_id),
                _ if per_batch_series => attributed(
                    &snap,
                    "saci_processor_rows_in_total",
                    PROCESSOR_ATTR,
                    &node_id,
                ),
                // A native runtime records none of the six `saci_processor_*`
                // series, so its box traces its own workflow's iteration count
                // rather than nothing at all. The unattributed form would sum
                // every workflow the process runs.
                _ => attributed(
                    &snap,
                    "saci_workflow_runs_total",
                    WORKFLOW_ATTR,
                    &workflow_id,
                ),
            };
            reading.map_or_else(
                || (String::new(), String::new()),
                |reading| sparkline(&reading.rate_points()),
            )
        }
    };
    let spark_line = {
        let spark = spark.clone();
        move || spark().0
    };
    let spark_area = {
        let spark = spark.clone();
        move || spark().1
    };

    // A derived signal rather than a plain closure: `Signal<f64>` is `Copy`, so
    // the badge's own attribute closures can each read it without cloning this
    // node's id into every one of them.
    let retries = {
        let node_id = node.id.clone();
        Signal::derive(move || {
            attributed(
                &snapshot.get(),
                "saci_processor_retries_total",
                PROCESSOR_ATTR,
                &node_id,
            )
            .map_or(0.0, |reading| reading.value)
        })
    };

    let tooltip = {
        let node = node.clone();
        let title = title.clone();
        let measure = measure.clone();
        Signal::derive(move || {
            let (label, value) = measure();
            let mut lines = vec![
                title.clone(),
                node.type_name.clone(),
                format!("{value} {label}"),
            ];
            if let Some(component) = &node.component {
                lines.push(format!("component: {component}"));
            }
            for (key, value) in &node.detail {
                lines.push(format!("{key}: {value}"));
            }
            lines.join("\n")
        })
    };

    // The windowing chip: a teal tag in the header's right corner. A derived
    // signal so the `Show` gate and the chip body can both read it without
    // moving the declaration.
    let window = node.window.clone();
    let chip = Signal::derive(move || {
        window.as_ref().map(|w| {
            let label = window_chip(w);
            #[allow(clippy::cast_precision_loss, reason = "chip labels are short")]
            let width = (14.0 + 5.2 * label.chars().count() as f64).min(80.0);
            (label, width)
        })
    });
    // The name shares the header strip with up to two chips, so it is cut to
    // what is left rather than drawn over them.
    let name_chars = if node.window.is_some() { 16 } else { 24 };

    let x = placed.x;
    let y = placed.y;
    let click_node = node.clone();

    view! {
        <g
            transform=format!("translate({x:.1}, {y:.1})")
            class="saci-node cursor-pointer"
            on:click=move |_| on_open.run(click_node.clone())
        >
            <title>{move || tooltip.get()}</title>
            <rect
                class="saci-node-bg"
                width=format!("{NODE_W}")
                height=format!("{NODE_H}")
                rx="10"
                fill="var(--dgm-blk)"
                stroke="var(--dgm-edge)"
            />
            <path
                d=format!(
                    "M 10 0 L {} 0 A 10 10 0 0 1 {NODE_W} 10 L {NODE_W} {HEADER_H} L 0 {HEADER_H} L 0 10 A 10 10 0 0 1 10 0 Z",
                    NODE_W - 10.0,
                )
                fill=header_fill
            />
            {bar_left
                .then(|| {
                    view! {
                        <rect
                            x="0"
                            y="11"
                            width="4"
                            height=format!("{}", NODE_H - 22.0)
                            rx="2"
                            fill=accent
                        />
                    }
                })}
            {bar_right
                .then(|| {
                    view! {
                        <rect
                            x=format!("{}", NODE_W - 4.0)
                            y="11"
                            width="4"
                            height=format!("{}", NODE_H - 22.0)
                            rx="2"
                            fill=accent
                        />
                    }
                })}
            <text x="20" y="16.5" font-size="12" font-weight="600" fill="var(--foreground)">
                {truncate(&title, name_chars)}
            </text>
            <text x="20" y="41" font-size="9.5" fill="var(--muted-foreground)">
                {truncate(&subtitle, 30)}
            </text>
            <text
                x="20"
                y="59"
                font-size="8.5"
                letter-spacing="0.06em"
                fill="var(--muted-foreground)"
            >
                {move || label().to_uppercase()}
            </text>
            <text
                x="20"
                y="78"
                font-size="16"
                font-weight="600"
                fill="var(--foreground)"
                font-family="var(--font-mono)"
            >
                {value}
            </text>
            <g transform=format!("translate({:.1}, {:.1})", NODE_W - SPARK_W - 12.0, NODE_H - SPARK_H - 8.0)>
                <path d=spark_area fill=accent opacity="0.16" />
                <path
                    d=spark_line
                    fill="none"
                    stroke=accent
                    stroke-width="1.5"
                    stroke-linejoin="round"
                />
            </g>
            <Show when=move || { chip.get().is_some() }>
                <g transform=format!("translate({:.1}, 5)", NODE_W - 88.0)>
                    {move || {
                        chip.get()
                            .map(|(label, width)| {
                                view! {
                                    <>
                                        <rect
                                            width=format!("{width:.1}")
                                            height="14"
                                            rx="7"
                                            fill="var(--dgm-hd-ctl)"
                                        />
                                        <text
                                            x=format!("{:.1}", width / 2.0)
                                            y="10.5"
                                            text-anchor="middle"
                                            font-size="9"
                                            fill="var(--foreground)"
                                        >
                                            {label}
                                        </text>
                                    </>
                                }
                            })
                    }}
                </g>
            </Show>
            <Show when=move || { is_processor && retries.get() > 0.0 }>
                <g transform=format!("translate({:.1}, 5)", NODE_W - 44.0)>
                    <rect width="36" height="14" rx="7" fill="var(--destructive)" />
                    <text x="18" y="10.5" text-anchor="middle" font-size="9" fill="white">
                        {move || format!("↻{}", retries.get() as u64)}
                    </text>
                </g>
            </Show>
        </g>
    }
    .into_any()
}

/// The right-hand detail panel for one node: what it is, what it is doing, and
/// how it was declared, in that order.
fn detail_view(
    node: TopoNode,
    snapshot: Signal<Option<Snapshot>>,
    node_logs: ReadSignal<Vec<LogRecord>>,
) -> AnyView {
    let is_processor = node.kind == "processor";
    let detail = node.detail.clone();
    let node_id = node.id.clone();

    // Identity: what this node is, before any number. A processor's `detail`
    // already carries its version, stateful flag and artifact, so the runtime
    // rows here are the ones only `runtime` holds.
    let mut identity: Vec<(&str, String)> = vec![
        ("id", node.id.clone()),
        ("kind", node.kind.clone()),
        ("type", node.type_name.clone()),
    ];
    if let Some(component) = node.component.clone() {
        identity.push(("component", component));
    }
    if let Some(runtime) = node.runtime {
        identity.push(("runtime", runtime.kind));
        identity.push(("runtime name", runtime.name));
        if !runtime.schema_fingerprint.is_empty() {
            identity.push(("schema fingerprint", runtime.schema_fingerprint));
        }
        if !runtime.declared_components.is_empty() {
            identity.push(("components", runtime.declared_components.join(", ")));
        }
    }

    // Live numbers: this node's own throughput or latency, plus whatever its
    // role adds. Only the values carrying this processor's id: the
    // unattributed copy of `saci_processor_metric` sums every processor's guest
    // metric of the same name, which belongs on no single sheet.
    let live: Signal<Vec<(String, String)>> = {
        let node_id = node_id.clone();
        let kind = node.kind.clone();
        Signal::derive(move || {
            let snap = snapshot.get();
            let mut rows: Vec<(String, String)> = Vec::new();
            match kind.as_str() {
                "source" => {
                    if let Some(reading) =
                        attributed(&snap, "saci_rows_processed_total", SOURCE_ATTR, &node_id)
                    {
                        rows.push(("rows".to_string(), format_count(reading.value)));
                        rows.push((
                            "throughput".to_string(),
                            format_rate(reading.rate_per_sec, "records"),
                        ));
                    }
                    if let Some(reading) = attributed(
                        &snap,
                        "saci_source_batches_drained_total",
                        SOURCE_ATTR,
                        &node_id,
                    ) {
                        rows.push(("batches drained".to_string(), format_count(reading.value)));
                    }
                }
                "sink" => {
                    if let Some(reading) = sink_reading(&snap, &node_id) {
                        rows.push((
                            "throughput".to_string(),
                            format_rate(reading.rate_per_sec, "records"),
                        ));
                    }
                    if let Some(reading) = attributed(
                        &snap,
                        "saci_sink_batches_written_total",
                        SINK_ATTR,
                        &node_id,
                    ) {
                        rows.push(("batches written".to_string(), format_count(reading.value)));
                    }
                }
                _ => {
                    for (name, label) in [
                        ("saci_processor_rows_in_total", "rows in"),
                        ("saci_processor_rows_out_total", "rows out"),
                        ("saci_processor_systems_run_total", "systems run"),
                        ("saci_processor_retries_total", "retries"),
                    ] {
                        if let Some(reading) = attributed(&snap, name, PROCESSOR_ATTR, &node_id) {
                            rows.push((label.to_string(), format_count(reading.value)));
                        }
                    }
                    if let Some(reading) = attributed(
                        &snap,
                        "saci_processor_batch_duration_seconds",
                        PROCESSOR_ATTR,
                        &node_id,
                    ) && reading.count > 0
                    {
                        #[allow(
                            clippy::cast_precision_loss,
                            reason = "observation counts stay well inside f64"
                        )]
                        let mean = reading.value / reading.count as f64;
                        rows.push(("mean batch".to_string(), format_seconds(mean)));
                    }
                    rows.extend(
                        snap.as_ref()
                            .map(|snap| snap.series.as_slice())
                            .unwrap_or_default()
                            .iter()
                            .filter(|series| {
                                series.name == "saci_processor_metric"
                                    && attr(&series.attrs, PROCESSOR_ATTR) == Some(node_id.as_str())
                            })
                            .map(|series| {
                                let label = attr(&series.attrs, "metric")
                                    .map_or_else(|| "metric".to_string(), ToString::to_string);
                                (label, format!("{:.3} / {}", series.value, series.count))
                            }),
                    );
                }
            }
            rows
        })
    };

    // Configuration: how the node was declared. The window geometry first,
    // because it changes what every number above means, then the factory's own
    // detail pairs in the order the server produced them.
    let mut windowing: Vec<(String, String)> = Vec::new();
    if let Some(window) = node.window {
        windowing.push(("window".to_string(), window_spec_line(&window)));
        windowing.push(("time field".to_string(), window.time_field.clone()));
        if !window.key_fields.is_empty() {
            windowing.push(("key fields".to_string(), window.key_fields.join(", ")));
        }
        windowing.push((
            "allowed lateness".to_string(),
            format_ms(Some(window.allowed_lateness_ms)),
        ));
    }
    let has_windowing = !windowing.is_empty();
    let windowing_rows = Signal::derive(move || windowing.clone());
    // The live watermark belongs with the geometry that gives it meaning.
    let watermark = {
        let node_id = node_id.clone();
        Signal::derive(move || {
            attributed(
                &snapshot.get(),
                "saci_window_watermark_seconds",
                PROCESSOR_ATTR,
                &node_id,
            )
            .map_or_else(
                || "—".to_string(),
                |reading| format_epoch_utc(reading.value),
            )
        })
    };
    // Arrivals the node dropped whole, every row of them beyond the lateness
    // budget above. Nothing else on this sheet distinguishes that state from
    // an idle stream: the node keeps reporting batches and its sinks report
    // nothing. Absent until the counter exists, so a healthy node carries no
    // row for it.
    let dropped_arrivals = {
        let node_id = node_id.clone();
        Signal::derive(move || {
            attributed(
                &snapshot.get(),
                "saci_window_late_arrivals_total",
                PROCESSOR_ATTR,
                &node_id,
            )
            .filter(|reading| reading.value > 0.0)
            .map(|reading| format_count(reading.value))
        })
    };

    // The window declaration already has its own section, so the factory's
    // own `window` and `window.*` pairs are dropped rather than repeated.
    let configuration: Vec<(String, String)> = detail
        .iter()
        .filter(|(key, _)| key != "window" && !key.starts_with("window."))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();

    // Both lists are fixed for the sheet's lifetime, so each becomes one
    // `Copy` signal before the view: a `Signal::derive` built inside a `Show`
    // would move the vector into that closure and make it `FnOnce`, which is
    // not what `ChildrenFn` accepts.
    let identity_rows = Signal::derive(move || {
        identity
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect::<Vec<_>>()
    });
    let has_configuration = !configuration.is_empty();
    let configuration_rows = Signal::derive(move || configuration.clone());

    let span_stats = move || {
        snapshot
            .get()
            .map_or_else(Vec::new, |snap| snap.span_stats.clone())
    };

    view! {
        <div class="space-y-5">
            <Section label="identity">
                <Rows rows=identity_rows />
            </Section>

            <Show when=move || !live.get().is_empty()>
                <Section label="live">
                    <Rows rows=live />
                </Section>
            </Show>

            <Show when=move || { is_processor && !span_stats().is_empty() }>
                <Section label="stage and system latency">
                    <Rows rows=Signal::derive(move || {
                        span_stats()
                            .into_iter()
                            .map(|stat| {
                                (
                                    format!("{} {}", stat.span, stat.key),
                                    format!("p50 {}µs · p95 {}µs", stat.p50_us, stat.p95_us),
                                )
                            })
                            .collect()
                    }) />
                </Section>
            </Show>

            <Show when=move || has_windowing>
                <Section label="windowing">
                    <Rows rows=windowing_rows />
                    <dl class="mt-1 space-y-1 text-xs">
                        <div class="flex items-baseline justify-between gap-3">
                            <dt class="shrink-0 text-muted-foreground">"watermark"</dt>
                            <dd class="font-mono">{move || watermark.get()}</dd>
                        </div>
                        <Show when=move || dropped_arrivals.get().is_some()>
                            <div class="flex items-baseline justify-between gap-3">
                                <dt class="shrink-0 text-destructive">"dropped arrivals"</dt>
                                <dd class="font-mono text-destructive">
                                    {move || dropped_arrivals.get().unwrap_or_default()}
                                </dd>
                            </div>
                        </Show>
                    </dl>
                </Section>
            </Show>

            <Show when=move || has_configuration>
                <Section label="configuration">
                    <Rows rows=configuration_rows />
                </Section>
            </Show>

            <Show when=move || !node_logs.get().is_empty()>
                <Section label="recent events naming this node">
                    <div class="space-y-1">
                        {move || {
                            node_logs
                                .get()
                                .into_iter()
                                .map(|record| {
                                    view! {
                                        <div class="flex items-baseline gap-2 font-mono text-[0.6875rem]">
                                            <span class="text-muted-foreground tabular-nums">
                                                {format_clock(record.at_unix_ms)}
                                            </span>
                                            <span class=level_class(&record.level)>
                                                {record.level.to_string()}
                                            </span>
                                            <span class="min-w-0 truncate">
                                                {record.message.clone()}
                                            </span>
                                        </div>
                                    }
                                })
                                .collect_view()
                        }}
                    </div>
                </Section>
            </Show>
        </div>
    }
    .into_any()
}

/// One titled block in the detail sheet.
#[component]
fn Section(label: &'static str, children: Children) -> impl IntoView {
    view! {
        <div>
            <h3 class="mb-1.5 text-[0.6875rem] font-medium tracking-wide text-muted-foreground uppercase">
                {label}
            </h3>
            {children()}
        </div>
    }
}

/// A `key: value` list, keys left and values right, both monospace.
#[component]
fn Rows(#[prop(into)] rows: Signal<Vec<(String, String)>>) -> impl IntoView {
    view! {
        <dl class="space-y-1 text-xs">
            {move || {
                rows.get()
                    .into_iter()
                    .map(|(key, value)| {
                        let hover = value.clone();
                        view! {
                            <div class="flex items-baseline justify-between gap-3">
                                <dt class="shrink-0 text-muted-foreground">{key}</dt>
                                <dd class="min-w-0 truncate text-right font-mono" title=hover>
                                    {value}
                                </dd>
                            </div>
                        }
                    })
                    .collect_view()
            }}
        </dl>
    }
}
