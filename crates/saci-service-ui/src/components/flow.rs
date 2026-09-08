//! Admission and delivery panels: one table per side of a workflow, and the
//! workflow's throughput over time with its adaptive-backpressure decisions
//! marked on the same axis.
//!
//! ## Why these sit inside the workflow card
//!
//! Admission is per source and per workflow: the controller moves one
//! source's target, and the effect lands on the workflow that source feeds.
//! Putting the tables and the chart under that workflow's graph keeps the
//! cause next to the picture of the thing it governs, instead of a
//! process-wide panel a viewer has to re-attribute by hand.
//!
//! ## Why this element tree is rebuilt on every poll
//!
//! The graph's tree is built once because re-creating it would restart its
//! dash and `animateMotion` animations. Nothing here animates: the chart is
//! static paths whose point count changes with the window, and the marker
//! count changes with what the controller did. Rebuilding is therefore the
//! honest option, and it is what lets a decision that just landed appear
//! without a diffing scheme for a variable-length set of markers.
//!
//! ## Reading the numbers
//!
//! Every series is read under the node's own attribute key and nothing else,
//! the same rule the graph's boxes follow. A node that has recorded nothing
//! shows an em dash rather than a zero, because "not sampled" and "measured
//! zero" are different facts, and `saci_sink_pending_rows` is absent entirely
//! for a connector that reports none. `ChannelSink`, `PostgresSink` and
//! `S3Sink` report one; `FileSink`, `HttpSink`, Kafka, NATS and TCP do not.

use leptos::prelude::*;
use saci_inspector_wire::{
    FlowDecision, FlowDecisionKind, PointAt, SINK_ATTR, SOURCE_ATTR, Snapshot, TopoNode,
    WorkflowTopology,
};

use super::Swatch;
use super::graph::{Reading, attributed, display, format_count};
use crate::ui::{Card, CardContent, CardHeader, CardTitle, Tooltip};

/// Chart geometry, in viewBox units, which are CSS pixels here.
const CHART_W: f64 = 1120.0;
const PLOT_H: f64 = 168.0;
/// Left gutter for the throughput axis, right gutter for the target axis.
const PAD_L: f64 = 58.0;
const PAD_R: f64 = 62.0;
const PAD_T: f64 = 22.0;
const PAD_B: f64 = 24.0;
/// Vertical gridlines, and therefore time tick labels.
const TICKS: usize = 6;
/// How many time buckets the throughput line is summed into.
const BUCKETS: usize = 120;
/// Markers closer together than this many units collapse into one line, so a
/// burst of decisions is one hoverable group rather than a picket fence.
const MARKER_CLUSTER_W: f64 = 7.0;
/// Horizontal room one cluster's count label needs before the next one has to
/// step down a rung.
const LABEL_CLEAR_W: f64 = 16.0;

/// The panels under one workflow's graph.
pub(super) fn flow_panels(
    workflow: &WorkflowTopology,
    snapshot: Signal<Option<Snapshot>>,
    on_open: Callback<TopoNode>,
) -> AnyView {
    let sources: Vec<TopoNode> = workflow
        .nodes
        .iter()
        .filter(|node| node.kind == "source")
        .cloned()
        .collect();
    let sinks: Vec<TopoNode> = workflow
        .nodes
        .iter()
        .filter(|node| node.kind == "sink")
        .cloned()
        .collect();
    let workflow_id = workflow.id.clone();
    let source_ids: Vec<String> = sources.iter().map(|node| node.id.clone()).collect();

    view! {
        <div class="space-y-4 border-t border-border px-4 py-4">
            <div class="grid gap-4 2xl:grid-cols-2">
                {node_table("sources", sources, snapshot, on_open, true)}
                {node_table("sinks", sinks, snapshot, on_open, false)}
            </div>
            {throughput_card(workflow_id, source_ids, snapshot)}
        </div>
    }
    .into_any()
}

/// One side of the workflow as a table: one row per node, clickable into the
/// same detail sheet the graph box opens.
fn node_table(
    label: &'static str,
    nodes: Vec<TopoNode>,
    snapshot: Signal<Option<Snapshot>>,
    on_open: Callback<TopoNode>,
    is_source: bool,
) -> AnyView {
    if nodes.is_empty() {
        return ().into_any();
    }
    // A header whose basis is not obvious carries the explanation, rather than
    // leaving a reader to distrust a number they cannot account for.
    let heads: &[(&str, Option<&str>)] = if is_source {
        &[
            ("source", None),
            (
                "rows/s",
                Some(
                    "Rows admitted per second of wall-clock time, from this source's own \
                     saci_rows_processed_total.",
                ),
            ),
            ("batches/s", None),
            (
                "target",
                Some("Rows the controller admits per pass right now, saci_flow_target_rows."),
            ),
            (
                "smoothed",
                Some(
                    "saci_flow_throughput_rows_per_second: the controller's own smoothed rate, \
                     measured over consumer-chain time with the wait for input subtracted. It \
                     reads higher than the wall-clock rows/s beside it whenever the source spends \
                     part of the second waiting for work, and it is the number the controller \
                     optimises.",
                ),
            ),
            (
                "backoffs",
                Some(
                    "saci_flow_backoff_total: how many times a safety guard has divided this \
                     source's target. An em dash means the counter does not exist yet, which is a \
                     source that has never backed off.",
                ),
            ),
        ]
    } else {
        &[
            ("sink", None),
            (
                "rows/s",
                Some("Rows written per second, from this sink's own saci_sink_rows_written_total."),
            ),
            ("batches/s", None),
            (
                "backlog",
                Some(
                    "saci_sink_pending_rows: rows the connector holds but has not written. An em \
                     dash means the connector reports no backlog at all, which is not a backlog \
                     of zero.",
                ),
            ),
        ]
    };

    let rows: Vec<_> = nodes
        .into_iter()
        .map(|node| {
            let id = node.id.clone();
            let name = display(&node.name, &node.id);
            let cells: Vec<_> = measures(&id, is_source)
                .into_iter()
                .map(|measure| {
                    let id = id.clone();
                    view! {
                        <td class="px-2 py-1 text-right font-mono text-xs tabular-nums whitespace-nowrap">
                            {move || measure.read(&snapshot.get(), &id)}
                        </td>
                    }
                })
                .collect();
            let open = node.clone();
            view! {
                <tr
                    class="cursor-pointer border-b border-border/60 last:border-0 hover:bg-muted/40"
                    on:click=move |_| on_open.run(open.clone())
                >
                    <td class="px-2 py-1 text-xs font-medium whitespace-nowrap">{name}</td>
                    {cells}
                </tr>
            }
        })
        .collect();

    view! {
        <div class="overflow-hidden rounded-lg border border-border">
            <div class="border-b border-border bg-muted/50 px-3 py-1.5 text-[0.6875rem] font-medium tracking-wide text-muted-foreground uppercase">
                {label}
            </div>
            <table class="w-full">
                <thead>
                    <tr class="border-b border-border">
                        {heads
                            .iter()
                            .enumerate()
                            .map(|(index, (head, hint))| {
                                let align = if index == 0 {
                                    "px-2 py-1 text-left text-[0.6875rem] font-medium text-muted-foreground"
                                } else {
                                    "px-2 py-1 text-right text-[0.6875rem] font-medium text-muted-foreground"
                                };
                                let label = *head;
                                let hint = *hint;
                                view! {
                                    <th class=align>
                                        {hint
                                            .map_or_else(
                                                || label.into_any(),
                                                |hint| {
                                                    view! {
                                                        <Tooltip content=Signal::derive(move || {
                                                            hint.to_string()
                                                        })>
                                                            <span class="underline decoration-dotted">
                                                                {label}
                                                            </span>
                                                        </Tooltip>
                                                    }
                                                        .into_any()
                                                },
                                            )}
                                    </th>
                                }
                            })
                            .collect_view()}
                    </tr>
                </thead>
                <tbody>{rows}</tbody>
            </table>
        </div>
    }
    .into_any()
}

/// One numeric column of a node table.
#[derive(Clone, Copy)]
struct Measure {
    /// Series name to read.
    name: &'static str,
    /// Attribute key the node id is recorded under.
    key: &'static str,
    /// Whether to show the per-second rate rather than the latest value.
    rate: bool,
    /// Whether a rising history is adverse and earns a trend arrow.
    watch_trend: bool,
}

impl Measure {
    /// The cell's text: an em dash when the node has recorded nothing.
    fn read(self, snapshot: &Option<Snapshot>, id: &str) -> String {
        let Some(reading) = attributed(snapshot, self.name, self.key, id) else {
            return "—".to_string();
        };
        let value = if self.rate {
            reading.rate_per_sec
        } else {
            reading.value
        };
        let text = format_count(value);
        if self.watch_trend {
            format!("{text} {}", trend(&reading))
        } else {
            text
        }
    }
}

/// The columns each side shows, in header order.
fn measures(_id: &str, is_source: bool) -> Vec<Measure> {
    if is_source {
        vec![
            Measure {
                name: "saci_rows_processed_total",
                key: SOURCE_ATTR,
                rate: true,
                watch_trend: false,
            },
            Measure {
                name: "saci_source_batches_drained_total",
                key: SOURCE_ATTR,
                rate: true,
                watch_trend: false,
            },
            Measure {
                name: "saci_flow_target_rows",
                key: SOURCE_ATTR,
                rate: false,
                watch_trend: false,
            },
            Measure {
                name: "saci_flow_throughput_rows_per_second",
                key: SOURCE_ATTR,
                rate: false,
                watch_trend: false,
            },
            // A count, not a rate: how many times the guard has fired since
            // the process started is the number an operator acts on.
            Measure {
                name: "saci_flow_backoff_total",
                key: SOURCE_ATTR,
                rate: false,
                watch_trend: false,
            },
        ]
    } else {
        vec![
            Measure {
                name: "saci_sink_rows_written_total",
                key: SINK_ATTR,
                rate: true,
                watch_trend: false,
            },
            Measure {
                name: "saci_sink_batches_written_total",
                key: SINK_ATTR,
                rate: true,
                watch_trend: false,
            },
            // The one number worth watching the shape of: a backlog that only
            // grows is the sink losing the race with its upstream.
            Measure {
                name: "saci_sink_pending_rows",
                key: SINK_ATTR,
                rate: false,
                watch_trend: true,
            },
        ]
    }
}

/// A one-character trend over the reading's own history.
fn trend(reading: &Reading) -> &'static str {
    let points = reading.rate_points();
    let (Some(first), Some(last)) = (points.first(), points.last()) else {
        return " ";
    };
    let delta = last.v - first.v;
    let scale = first.v.abs().max(1.0);
    if delta / scale > 0.1 {
        "↑"
    } else if delta / scale < -0.1 {
        "↓"
    } else {
        "→"
    }
}

/// The workflow's throughput card.
fn throughput_card(
    workflow_id: String,
    source_ids: Vec<String>,
    snapshot: Signal<Option<Snapshot>>,
) -> AnyView {
    let count_id = workflow_id.clone();
    // The chart's element tree is rebuilt on every poll, so a tooltip cleared
    // on `pointerleave` would also blink out once a second. It is held in a
    // signal outside the chart instead and cleared when the pointer reaches
    // the plot background, which is the same gesture with a longer memory.
    let (hovered, set_hovered) = signal::<Option<(u64, Vec<String>)>>(None);
    view! {
        <Card>
            <CardHeader>
                <div class="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1">
                    <CardTitle>"Throughput and admission decisions"</CardTitle>
                    <span class="font-mono text-xs text-muted-foreground">
                        {move || {
                            let count = snapshot
                                .get()
                                .map_or(
                                    0,
                                    |snap| {
                                        snap.flow_decisions
                                            .iter()
                                            .filter(|decision| decision.workflow == count_id)
                                            .count()
                                    },
                                );
                            format!("{count} decisions in the window")
                        }}
                    </span>
                </div>
            </CardHeader>
            <CardContent>
                {move || chart(&workflow_id, &source_ids, &snapshot.get(), hovered, set_hovered)}
                <div class="mt-3 flex flex-wrap items-center gap-x-5 gap-y-1.5 text-xs text-muted-foreground">
                    <Swatch colour="var(--data)" label="rows/s admitted by this workflow" />
                    <Swatch colour="var(--dgm-mute)" label="per-source admission target" />
                    <Swatch colour="var(--control)" label="target grew" />
                    <Swatch colour="var(--boundary)" label="target shrank" />
                    <Swatch colour="var(--destructive)" label="backed off / held at floor" />
                </div>
            </CardContent>
        </Card>
    }
    .into_any()
}

/// The chart itself: throughput, per-source target steps, decision markers.
fn chart(
    workflow_id: &str,
    source_ids: &[String],
    snapshot: &Option<Snapshot>,
    hovered: ReadSignal<Option<(u64, Vec<String>)>>,
    set_hovered: WriteSignal<Option<(u64, Vec<String>)>>,
) -> AnyView {
    let rows: Vec<Vec<PointAt>> = source_ids
        .iter()
        .filter_map(|id| {
            attributed(snapshot, "saci_rows_processed_total", SOURCE_ATTR, id)
                .map(|reading| reading.rate_points())
        })
        .filter(|points| !points.is_empty())
        .collect();
    let targets: Vec<Vec<PointAt>> = source_ids
        .iter()
        .filter_map(|id| {
            attributed(snapshot, "saci_flow_target_rows", SOURCE_ATTR, id)
                .map(|reading| reading.rate_points())
        })
        .filter(|points| points.len() > 1)
        .collect();

    let Some((from, to)) = domain(&rows, &targets) else {
        return view! {
            <div class="rounded-md border border-dashed border-border px-4 py-8">
                <p class="text-sm text-muted-foreground">
                    "No admission history yet. The chart fills in as the metric exporter \
                     publishes this workflow's source samples."
                </p>
            </div>
        }
        .into_any();
    };

    // `flow_decisions` arrives in push order, which is per-runner arrival
    // order across workflows, so a later stamp can precede an earlier one.
    // Clustering neighbours on the x axis needs ascending time, and a stable
    // sort keeps two decisions stamped in the same millisecond in the order
    // the host recorded them.
    let mut decisions: Vec<&FlowDecision> = snapshot
        .as_ref()
        .map(|snap| snap.flow_decisions.as_slice())
        .unwrap_or_default()
        .iter()
        .filter(|decision| decision.workflow == workflow_id)
        .collect();
    decisions.sort_by_key(|decision| decision.at_unix_ms);

    let height = PAD_T + PLOT_H + PAD_B;
    let plot_w = CHART_W - PAD_L - PAD_R;
    #[allow(clippy::cast_precision_loss, reason = "a window span stays inside f64")]
    let span_ms = (to - from) as f64;
    let x_of = move |at: u64| {
        #[allow(clippy::cast_precision_loss, reason = "a window span stays inside f64")]
        let offset = at.saturating_sub(from) as f64;
        PAD_L + (offset / span_ms).clamp(0.0, 1.0) * plot_w
    };

    let series = bucketed(&rows, from, to);
    let peak = series
        .iter()
        .filter_map(|slot| *slot)
        .fold(0.0_f64, f64::max)
        .max(1.0);
    let peak_target = targets
        .iter()
        .flatten()
        .map(|point| point.v)
        .fold(0.0_f64, f64::max)
        .max(1.0);

    let (throughput, throughput_area) = line_paths(&series, peak);
    let target_paths: Vec<String> = targets
        .iter()
        .map(|points| step_path(points, peak_target, from, to))
        .collect();

    let axis: Vec<_> = (0..=TICKS)
        .map(|tick| {
            #[allow(clippy::cast_precision_loss, reason = "TICKS is small")]
            let fraction = tick as f64 / TICKS as f64;
            let x = PAD_L + fraction * plot_w;
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "a window offset is non-negative and small"
            )]
            let at = from + (span_ms * fraction) as u64;
            view! {
                <g>
                    <line
                        x1=format!("{x:.1}")
                        y1=format!("{PAD_T}")
                        x2=format!("{x:.1}")
                        y2=format!("{:.1}", PAD_T + PLOT_H)
                        stroke="var(--dgm-edge)"
                        stroke-width="1"
                    />
                    <text
                        x=format!("{x:.1}")
                        y=format!("{:.1}", PAD_T + PLOT_H + 14.0)
                        font-size="9"
                        text-anchor=if tick == 0 { "start" } else { "middle" }
                        fill="var(--muted-foreground)"
                        font-family="var(--font-mono)"
                    >
                        {clock(at)}
                    </text>
                </g>
            }
        })
        .collect();

    let markers = marker_views(&decisions, &x_of, height, set_hovered);

    view! {
        <div class="saci-canvas rounded-md border border-border bg-[var(--dgm-bg)] p-2">
            <svg
                viewBox=format!("0 0 {CHART_W:.0} {height:.0}")
                preserveAspectRatio="xMinYMin meet"
                class="w-full"
                style=format!("min-width: {CHART_W:.0}px")
            >
                <rect
                    x=format!("{PAD_L}")
                    y=format!("{PAD_T}")
                    width=format!("{plot_w:.1}")
                    height=format!("{PLOT_H}")
                    fill="transparent"
                    on:pointerenter=move |_| set_hovered.set(None)
                />
                {axis}
                <line
                    x1=format!("{PAD_L}")
                    y1=format!("{:.1}", PAD_T + PLOT_H)
                    x2=format!("{:.1}", CHART_W - PAD_R)
                    y2=format!("{:.1}", PAD_T + PLOT_H)
                    stroke="var(--dgm-edge)"
                    stroke-width="1"
                />
                <text
                    x=format!("{:.1}", PAD_L - 6.0)
                    y=format!("{:.1}", PAD_T + 8.0)
                    font-size="9"
                    text-anchor="end"
                    fill="var(--data)"
                    font-family="var(--font-mono)"
                >
                    {format_count(peak)}
                </text>
                <line
                    x1=format!("{PAD_L}")
                    y1=format!("{:.1}", PAD_T + PLOT_H / 2.0)
                    x2=format!("{:.1}", CHART_W - PAD_R)
                    y2=format!("{:.1}", PAD_T + PLOT_H / 2.0)
                    stroke="var(--dgm-edge)"
                    stroke-width="1"
                    stroke-dasharray="2 4"
                />
                <text
                    x=format!("{:.1}", PAD_L - 6.0)
                    y=format!("{:.1}", PAD_T + PLOT_H / 2.0 + 3.0)
                    font-size="9"
                    text-anchor="end"
                    fill="var(--muted-foreground)"
                    font-family="var(--font-mono)"
                >
                    {format_count(peak / 2.0)}
                </text>
                <text
                    x=format!("{:.1}", PAD_L - 6.0)
                    y=format!("{:.1}", PAD_T + PLOT_H)
                    font-size="9"
                    text-anchor="end"
                    fill="var(--muted-foreground)"
                    font-family="var(--font-mono)"
                >
                    "0"
                </text>
                <text
                    x=format!("{:.1}", CHART_W - PAD_R)
                    y=format!("{:.1}", PAD_T - 5.0)
                    font-size="9"
                    text-anchor="end"
                    fill="var(--dgm-mute)"
                    font-family="var(--font-mono)"
                >
                    {format!("target peak {} rows", format_count(peak_target))}
                </text>
                {target_paths
                    .into_iter()
                    .map(|path| {
                        view! {
                            <path
                                d=path
                                fill="none"
                                stroke="var(--dgm-mute)"
                                stroke-width="1"
                                stroke-dasharray="3 3"
                            />
                        }
                    })
                    .collect_view()}
                <path d=throughput_area fill="var(--data)" opacity="0.14" />
                <path
                    d=throughput
                    fill="none"
                    stroke="var(--data)"
                    stroke-width="2.2"
                    stroke-linejoin="round"
                    stroke-linecap="round"
                />
                {markers}
                {move || {
                    hovered.get().map(|(at, lines)| tooltip(x_of(at), &lines))
                }}
            </svg>
        </div>
    }
    .into_any()
}

/// One vertical line per decision, clustered when two land within
/// [`MARKER_CLUSTER_W`] units of each other.
fn marker_views(
    decisions: &[&FlowDecision],
    x_of: &impl Fn(u64) -> f64,
    height: f64,
    set_hovered: WriteSignal<Option<(u64, Vec<String>)>>,
) -> Vec<AnyView> {
    let mut clusters: Vec<(f64, u64, Vec<&FlowDecision>)> = Vec::new();
    for decision in decisions {
        let x = x_of(decision.at_unix_ms);
        match clusters.last_mut() {
            Some((at, _, group)) if (x - *at).abs() <= MARKER_CLUSTER_W => group.push(decision),
            _ => clusters.push((x, decision.at_unix_ms, vec![decision])),
        }
    }

    // Count labels stagger down whenever two clusters are close enough that
    // their labels would overlap, which happens well before the marker lines
    // themselves do.
    let mut last_label_x = f64::NEG_INFINITY;
    let mut rung = 0usize;
    let mut rungs: Vec<f64> = Vec::with_capacity(clusters.len());
    for (x, _, _) in &clusters {
        if x - last_label_x < LABEL_CLEAR_W {
            rung = (rung + 1) % 3;
        } else {
            rung = 0;
        }
        last_label_x = *x;
        #[allow(clippy::cast_precision_loss, reason = "three rungs")]
        rungs.push(rung as f64 * 10.0);
    }

    clusters
        .into_iter()
        .zip(rungs)
        .map(|((x, at, group), offset)| {
            // The adverse kind wins the colour of a mixed cluster: a back-off
            // hidden inside a group of ordinary search moves is the one an
            // operator must not miss.
            let colour = group
                .iter()
                .map(|decision| kind_colour(decision.kind))
                .find(|colour| *colour == "var(--destructive)")
                .unwrap_or_else(|| kind_colour(group[0].kind));
            let lines: Vec<String> = group
                .iter()
                .flat_map(|decision| {
                    [
                        format!(
                            "{} · {} · {} → {} rows",
                            decision.source,
                            kind_label(decision.kind),
                            decision.from_rows,
                            decision.to_rows,
                        ),
                        decision.reason.clone(),
                    ]
                })
                .collect();
            let hover = lines.join("\n");
            let pinned = lines.clone();
            let count = group.len();
            view! {
                <g
                    class="saci-bar cursor-pointer"
                    on:pointerenter=move |_| set_hovered.set(Some((at, pinned.clone())))
                >
                    <title>{hover}</title>
                    <rect
                        x=format!("{:.1}", x - 4.0)
                        y=format!("{PAD_T}")
                        width="8"
                        height=format!("{:.1}", PLOT_H)
                        fill="transparent"
                    />
                    <line
                        x1=format!("{x:.1}")
                        y1=format!("{PAD_T}")
                        x2=format!("{x:.1}")
                        y2=format!("{:.1}", height - PAD_B)
                        stroke=colour
                        stroke-width="1.2"
                        opacity="0.85"
                    />
                    <circle cx=format!("{x:.1}") cy=format!("{PAD_T}") r="3" fill=colour />
                    {(count > 1)
                        .then(|| {
                            view! {
                                <text
                                    x=format!("{:.1}", x + 5.0)
                                    y=format!("{:.1}", PAD_T + 3.0 + offset)
                                    font-size="8"
                                    fill=colour
                                    font-family="var(--font-mono)"
                                >
                                    {count.to_string()}
                                </text>
                            }
                        })}
                </g>
            }
            .into_any()
        })
        .collect()
}

/// The hovered marker's detail, drawn in the chart's own coordinate space so
/// it appears in the picture rather than as browser chrome.
fn tooltip(x: f64, lines: &[String]) -> AnyView {
    #[allow(clippy::cast_precision_loss, reason = "reason strings are short")]
    let widest = lines
        .iter()
        .map(|line| line.chars().count() as f64 * 5.6 + 14.0)
        .fold(140.0_f64, f64::max)
        .min(480.0);
    #[allow(clippy::cast_precision_loss, reason = "a cluster holds few decisions")]
    let height = 10.0 + lines.len() as f64 * 11.0;
    // Flip to the left of the marker when the box would leave the plot.
    let left = if x + 12.0 + widest > CHART_W - 4.0 {
        x - 12.0 - widest
    } else {
        x + 12.0
    };
    let top = PAD_T + 6.0;

    view! {
        <g pointer-events="none">
            <rect
                x=format!("{left:.1}")
                y=format!("{top:.1}")
                width=format!("{widest:.1}")
                height=format!("{height:.1}")
                rx="4"
                fill="var(--dgm-blk)"
                stroke="var(--dgm-edge)"
            />
            {lines
                .iter()
                .enumerate()
                .map(|(index, line)| {
                    #[allow(clippy::cast_precision_loss, reason = "few lines")]
                    let row = index as f64;
                    let bold = index % 2 == 0;
                    view! {
                        <text
                            x=format!("{:.1}", left + 6.0)
                            y=format!("{:.1}", top + 14.0 + row * 11.0)
                            font-size="9"
                            fill=if bold { "var(--foreground)" } else { "var(--muted-foreground)" }
                            font-family="var(--font-mono)"
                        >
                            {line.clone()}
                        </text>
                    }
                })
                .collect_view()}
        </g>
    }
    .into_any()
}

/// The plane colour one decision kind is drawn in.
fn kind_colour(kind: FlowDecisionKind) -> &'static str {
    match kind {
        FlowDecisionKind::Grew => "var(--control)",
        FlowDecisionKind::Shrank => "var(--boundary)",
        FlowDecisionKind::BackedOff | FlowDecisionKind::HeldAtFloor => "var(--destructive)",
    }
}

/// The decision kind in words, for a marker's hover text.
fn kind_label(kind: FlowDecisionKind) -> &'static str {
    match kind {
        FlowDecisionKind::Grew => "grew",
        FlowDecisionKind::Shrank => "shrank",
        FlowDecisionKind::BackedOff => "backed off",
        FlowDecisionKind::HeldAtFloor => "held at floor",
    }
}

/// The time domain every path and marker shares.
fn domain(rows: &[Vec<PointAt>], targets: &[Vec<PointAt>]) -> Option<(u64, u64)> {
    let times = rows
        .iter()
        .chain(targets.iter())
        .flatten()
        .map(|point| point.t);
    let from = times.clone().min()?;
    let to = times.max()?;
    (to > from).then_some((from, to))
}

/// Sum every source's rate into [`BUCKETS`] time buckets.
///
/// Bucketing by time rather than by index is what makes the sum meaningful:
/// the sources are exported on one interval but their decimated histories do
/// not carry identical timestamps, so adding `points[i]` across sources would
/// add samples taken at different moments. Within one bucket a source
/// contributes the mean of its own samples, so a source that happens to land
/// twice in a bucket does not count twice.
///
/// A source's buckets are then held forward between its own first and last
/// sample. `MAX_POINTS` decimates a window to at most 120 samples, so on a
/// bucket grid of the same order roughly every other bucket has no sample of
/// a given source; summing without the hold would leave a series of isolated
/// points with no segment between them, which draws nothing at all. Holding
/// the last known rate is the same reading the number above the chart gives.
/// Buckets before the first source started, and after the last one stopped,
/// stay `None` and break the line rather than reading as zero throughput.
fn bucketed(rows: &[Vec<PointAt>], from: u64, to: u64) -> Vec<Option<f64>> {
    #[allow(clippy::cast_precision_loss, reason = "a window span stays inside f64")]
    let span = (to - from) as f64;
    let slot_of = |at: u64| {
        #[allow(clippy::cast_precision_loss, reason = "a window span stays inside f64")]
        let offset = at.saturating_sub(from) as f64;
        #[allow(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "the fraction is clamped into 0..1 and BUCKETS is small"
        )]
        let slot = (((offset / span).clamp(0.0, 1.0)) * (BUCKETS - 1) as f64) as usize;
        slot
    };

    let mut totals: Vec<Option<f64>> = vec![None; BUCKETS];
    for points in rows {
        let mut own: Vec<Option<(f64, u32)>> = vec![None; BUCKETS];
        for point in points {
            let slot = slot_of(point.t);
            let entry = own[slot].get_or_insert((0.0, 0));
            entry.0 += point.v;
            entry.1 += 1;
        }
        let first = own.iter().position(Option::is_some);
        let last = own.iter().rposition(Option::is_some);
        let (Some(first), Some(last)) = (first, last) else {
            continue;
        };
        let mut held = 0.0;
        for slot in first..=last {
            if let Some((sum, count)) = own[slot] {
                held = sum / f64::from(count);
            }
            totals[slot] = Some(totals[slot].unwrap_or(0.0) + held);
        }
    }
    totals
}

/// A polyline over the bucketed series, plus the same curve closed to the
/// baseline, broken across unsampled buckets.
///
/// The area is what makes the throughput the dominant mark on a chart that
/// also carries a step line per source and a marker per decision.
fn line_paths(series: &[Option<f64>], peak: f64) -> (String, String) {
    let plot_w = CHART_W - PAD_L - PAD_R;
    let base = PAD_T + PLOT_H;
    let mut line = String::with_capacity(series.len() * 14);
    let mut area = String::with_capacity(series.len() * 16);
    let mut run: Vec<(f64, f64)> = Vec::new();

    let flush = |run: &mut Vec<(f64, f64)>, line: &mut String, area: &mut String| {
        if run.len() < 2 {
            run.clear();
            return;
        }
        for (index, (x, y)) in run.iter().enumerate() {
            let command = if index == 0 { "M" } else { "L" };
            line.push_str(&format!(" {command} {x:.1} {y:.1}"));
        }
        let (first_x, first_y) = run[0];
        area.push_str(&format!(
            " M {first_x:.1} {base:.1} L {first_x:.1} {first_y:.1}"
        ));
        for (x, y) in run.iter().skip(1) {
            area.push_str(&format!(" L {x:.1} {y:.1}"));
        }
        let (last_x, _) = run[run.len() - 1];
        area.push_str(&format!(" L {last_x:.1} {base:.1} Z"));
        run.clear();
    };

    for (slot, value) in series.iter().enumerate() {
        let Some(value) = value else {
            flush(&mut run, &mut line, &mut area);
            continue;
        };
        #[allow(clippy::cast_precision_loss, reason = "BUCKETS is small")]
        let fraction = slot as f64 / (BUCKETS - 1) as f64;
        let x = PAD_L + fraction * plot_w;
        let y = base - (value / peak).clamp(0.0, 1.0) * PLOT_H;
        run.push((x, y));
    }
    flush(&mut run, &mut line, &mut area);
    (line, area)
}

/// A step-after line for one source's admission target.
///
/// A step, not a slope: the target holds until a decision moves it, and a
/// straight line between two samples would draw a change that never happened.
fn step_path(points: &[PointAt], peak: f64, from: u64, to: u64) -> String {
    let plot_w = CHART_W - PAD_L - PAD_R;
    #[allow(clippy::cast_precision_loss, reason = "a window span stays inside f64")]
    let span = (to - from) as f64;
    let mut path = String::with_capacity(points.len() * 20);
    let mut last_y = 0.0;
    for (index, point) in points.iter().enumerate() {
        #[allow(clippy::cast_precision_loss, reason = "a window span stays inside f64")]
        let offset = point.t.saturating_sub(from) as f64;
        let x = PAD_L + (offset / span).clamp(0.0, 1.0) * plot_w;
        let y = PAD_T + PLOT_H - (point.v / peak).clamp(0.0, 1.0) * PLOT_H;
        if index == 0 {
            path.push_str(&format!("M {x:.1} {y:.1}"));
        } else {
            path.push_str(&format!(" L {x:.1} {last_y:.1} L {x:.1} {y:.1}"));
        }
        last_y = y;
    }
    path
}

/// Unix milliseconds as `HH:MM:SS` UTC, for a time axis label.
fn clock(at_unix_ms: u64) -> String {
    let secs = at_unix_ms / 1000;
    format!(
        "{:02}:{:02}:{:02}",
        (secs / 3600) % 24,
        (secs / 60) % 60,
        secs % 60
    )
}
