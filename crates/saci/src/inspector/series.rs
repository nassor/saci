//! Series summaries and per-edge rates derived from retained metric samples.
//!
//! [`Inspector::snapshot`](super::Inspector::snapshot) folds the samples in its
//! window into one [`SeriesSummary`] per `(name, attrs)` pair and rates every
//! topology edge from them.

use std::collections::HashMap;

use crate::inspector_wire::{
    EdgeRate, MAX_POINTS, MetricSample, PROCESSOR_ATTR, PointAt, SINK_ATTR, SOURCE_ATTR,
    SeriesKind, SeriesSummary, Topology,
};

/// Series the edge rates are read from.
const ROWS_PROCESSED: &str = "saci_source_rows_total";
const PROCESSOR_ROWS_OUT: &str = "saci_processor_rows_out_total";
const SINK_ROWS: &str = "saci_sink_rows_total";

/// How many metric export intervals one [`SeriesSummary::rate_per_sec`]
/// derivative spans, as a count of intervals rather than a duration: the
/// interval itself is configurable, and selecting samples by count is immune
/// to the few milliseconds of drift each export carries.
///
/// A runner admits rows throughout a pass and writes every sink at the end of
/// it, so a pass longer than one export interval lands the two halves of the
/// same flow in different samples: a derivative across the two newest samples
/// then reads a source and a sink of one workflow as if they moved different
/// amounts of data. Widening the span is the fix, and it trades
/// responsiveness for smoothing one for one. Two intervals is the least that
/// differs from the two-point form at all; at five, a step change in
/// throughput moves the number by a fifth on the next poll and is fully
/// registered five polls later.
///
/// The residual is quantisation, and it is arithmetic rather than a defect: a
/// window of `W` seconds holds `floor(W / P)` or `ceil(W / P)` bursts of a
/// writer with pass period `P`, and the two ends of one flow sit at different
/// phases of it, so their ratio is bounded by `floor / ceil` to
/// `ceil / floor`. At `W = 5 s` a one second pass reads within about
/// `0.8 .. 1.2`, and a three second pass only within `0.5 .. 2`. A workload
/// whose passes approach the window is not smoothed by it.
const RATE_WINDOW_INTERVALS: usize = 5;

/// Fold every sample in the window into one [`SeriesSummary`] per
/// `(name, attrs)` pair.
///
/// Series are keyed on their attributes as well as their name, because
/// `saci_processor_custom` carries a processor-chosen `metric` attribute and its
/// values are distinct series to the dashboard.
pub(super) fn summarize_series(samples: &[MetricSample]) -> Vec<SeriesSummary> {
    // Insertion-ordered accumulation, then one sort at the end: a HashMap of
    // vectors would reorder series between polls and make the UI list jump.
    let mut order: Vec<(String, Vec<(String, String)>)> = Vec::new();
    let mut index: HashMap<(String, Vec<(String, String)>), usize> = HashMap::new();
    let mut kinds: Vec<SeriesKind> = Vec::new();
    let mut counts: Vec<u64> = Vec::new();
    let mut points: Vec<Vec<PointAt>> = Vec::new();

    for sample in samples {
        for point in &sample.series {
            let key = (point.name.clone(), point.attrs.clone());
            let slot = match index.get(&key) {
                Some(slot) => *slot,
                None => {
                    let slot = order.len();
                    index.insert(key.clone(), slot);
                    order.push(key);
                    kinds.push(point.kind);
                    counts.push(point.count);
                    points.push(Vec::new());
                    slot
                }
            };
            counts[slot] = point.count;
            points[slot].push(PointAt {
                t: sample.at_unix_ms,
                v: point.value,
            });
        }
    }

    let mut out: Vec<SeriesSummary> = Vec::with_capacity(order.len());
    for (slot, (name, attrs)) in order.into_iter().enumerate() {
        let history = &points[slot];
        let Some(newest) = history.last() else {
            continue;
        };
        let kind = kinds[slot];
        let rate = match kind {
            SeriesKind::Gauge => newest.v,
            SeriesKind::Counter | SeriesKind::Histogram => rate_of(history),
        };
        out.push(SeriesSummary {
            name,
            kind,
            attrs,
            value: newest.v,
            count: counts[slot],
            rate_per_sec: rate,
            points: decimate(history),
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.attrs.cmp(&b.attrs)));
    out
}

/// First derivative over the newest [`RATE_WINDOW_INTERVALS`] export
/// intervals, in units per second.
///
/// A writer that moves its rows in one burst per pass and one that moves them
/// evenly report the same number for the same total over the same span. That
/// is the whole point: a runner counts a source's rows as it admits them and
/// a sink's rows when it writes the staged batch at the end of the pass, so a
/// derivative across the two newest samples reads the two ends of one flow as
/// different throughputs whenever a pass outruns the export interval. A pass
/// approaching the window's own width still reads bursty; the window bounds
/// how much is smoothed, it does not remove the burst.
///
/// A history shorter than the window contributes the span it has, and the
/// divisor is always the span the selected samples really cover, so an export
/// the reader delayed shortens no window and inflates no rate.
///
/// A cumulative counter that went backwards anywhere inside the window means
/// the process restarted or the SDK reset the series; reporting 0 is honest,
/// and a rate spanning the reset would be arbitrary.
fn rate_of(history: &[PointAt]) -> f64 {
    if history.len() < 2 {
        return 0.0;
    }
    // Selected by count, not by a `newest.t - N * interval` cutoff: an export
    // lands a few milliseconds late every interval, and that drift accumulates
    // until the cutoff falls after the Nth-oldest sample and drops it on every
    // poll, leaving a window one interval shorter than the constant says. N
    // intervals is N + 1 samples.
    let base = history.len().saturating_sub(RATE_WINDOW_INTERVALS + 1);
    let window = &history[base..];
    if window.windows(2).any(|pair| pair[1].v < pair[0].v) {
        return 0.0;
    }
    let oldest = window[0];
    let newest = history[history.len() - 1];
    let dt_ms = newest.t.saturating_sub(oldest.t);
    if dt_ms == 0 {
        return 0.0;
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "millisecond gaps are far below f64's exact integer range"
    )]
    let dt_secs = (dt_ms as f64) / 1000.0;
    (newest.v - oldest.v) / dt_secs
}

/// Thin `history` to at most [`MAX_POINTS`] entries, keeping the newest.
fn decimate(history: &[PointAt]) -> Vec<PointAt> {
    if history.len() <= MAX_POINTS {
        return history.to_vec();
    }
    let stride = history.len().div_ceil(MAX_POINTS);
    let mut out: Vec<PointAt> = history.iter().copied().step_by(stride).collect();
    let newest = history[history.len() - 1];
    if out.last().map(|p| p.t) != Some(newest.t) {
        out.push(newest);
    }
    out
}

/// The rate of one series carrying `key="value"`.
fn attributed_rate(series: &[SeriesSummary], name: &str, key: &str, value: &str) -> Option<f64> {
    series
        .iter()
        .find(|s| s.name == name && s.attrs.iter().any(|(k, v)| k == key && v == value))
        .map(|s| s.rate_per_sec)
}

/// The rate of one series carrying `key1="value1"` and `key2="value2"`.
fn attributed_rate_branch(
    series: &[SeriesSummary],
    name: &str,
    key1: &str,
    value1: &str,
    key2: &str,
    value2: &str,
) -> Option<f64> {
    series
        .iter()
        .find(|s| {
            s.name == name
                && s.attrs.iter().any(|(k, v)| k == key1 && v == value1)
                && s.attrs.iter().any(|(k, v)| k == key2 && v == value2)
        })
        .map(|s| s.rate_per_sec)
}

/// One sink's own throughput, in rows.
///
/// `SinkMetrics::write` records `saci_sink_batches_total` and
/// `saci_sink_rows_total` on the same write, and a zero-row write
/// still registers the rows series, so there is no state in which a sink has
/// batches but no rows. This reads the rows series alone: an absent one means
/// nothing has crossed to that sink yet.
fn sink_rows(series: &[SeriesSummary], sink: &str) -> Option<f64> {
    attributed_rate(series, SINK_ROWS, SINK_ATTR, sink)
}

/// Attach a throughput number to every topology edge.
///
/// Rated from whichever end of the edge measures what actually crossed it:
/// a source `from` reads `saci_source_rows_total` under `source="<from>"`,
/// a processor `from` reads `saci_processor_rows_out_total` under
/// `processor="<from>"`.
///
/// A processor-to-sink edge is the exception: it reads the **sink's** own
/// `saci_sink_rows_total` under `sink="<to>"`, and nothing else.
/// `saci_processor_rows_out_total` is not used there, because it is the row
/// count of the processor's whole output dataset, every component of it,
/// while a sink node takes one component: a processor that returns its input
/// alongside a reduced component (any windowing processor does) would rate
/// that edge at its input rate while the sink receives almost nothing. An
/// absent sink counter means nothing has crossed yet, so the edge is omitted.
/// The native-runtime path, which writes no `saci_processor_*` series at all,
/// therefore rates its sink edges exactly like the wasm and plugin paths.
///
/// `saci_sink_batches_total` rates nothing: a batch is whatever row
/// count the upstream handed over, so it moves with the admission target
/// rather than with the data, and it can never be the only sample a sink has
/// ([`sink_rows`]).
///
/// An edge whose attributed series has not been sampled yet is omitted rather
/// than reported as zero, so the output is not always one entry per
/// [`TopoEdge`](crate::inspector_wire::TopoEdge); a lookup by `(from, to)` is the
/// contract, not position. Every edge reads its own upstream node's own
/// attributed sample or is omitted.
///
/// A bridge gets its own arm, because a bridge's `from` is a `ChannelSink`
/// whose kind the per-edge match drops. It prefers the consuming
/// `ChannelSource`'s own `saci_source_rows_total`, then the producing
/// sink's own rows: the same flow, measured at whichever end has sampled.
///
/// A labelled edge is rated only from its own branch series, with no sink
/// fallback of any kind: an unchosen branch must read as absent rather than
/// as whatever its sink wrote.
pub(super) fn edge_rates(topology: &Topology, series: &[SeriesSummary]) -> Vec<EdgeRate> {
    let node_kind: HashMap<&str, &str> = topology
        .workflows
        .iter()
        .flat_map(|w| w.nodes.iter())
        .map(|node| (node.id.as_str(), node.kind.as_str()))
        .collect();

    let mut out = Vec::new();
    for edge in topology.workflows.iter().flat_map(|w| w.edges.iter()) {
        let (rate, unit) = match node_kind.get(edge.from.as_str()).copied() {
            Some("source") => {
                let Some(rate) = attributed_rate(series, ROWS_PROCESSED, SOURCE_ATTR, &edge.from)
                else {
                    continue;
                };
                (rate, "rows")
            }
            Some("processor") => {
                match &edge.branch {
                    Some(branch) => {
                        // A labelled edge is rated from its own branch
                        // series, never from its sink: an unchosen branch
                        // must read as absent, not as whatever the sink wrote.
                        let Some(rate) = attributed_rate_branch(
                            series,
                            PROCESSOR_ROWS_OUT,
                            PROCESSOR_ATTR,
                            &edge.from,
                            crate::inspector_wire::BRANCH_ATTR,
                            branch,
                        ) else {
                            continue;
                        };
                        (rate, "rows")
                    }
                    // A sink `to` is measured at the sink: those are the
                    // rows that crossed this edge, where the processor's
                    // rows-out counts its whole output dataset. Anything
                    // else downstream is another processor, which has no
                    // counter of what it received.
                    None if node_kind.get(edge.to.as_str()).copied() == Some("sink") => {
                        match sink_rows(series, &edge.to) {
                            Some(rate) => (rate, "rows"),
                            None => continue,
                        }
                    }
                    None => {
                        match attributed_rate(
                            series,
                            PROCESSOR_ROWS_OUT,
                            PROCESSOR_ATTR,
                            &edge.from,
                        ) {
                            Some(rate) => (rate, "rows"),
                            None => continue,
                        }
                    }
                }
            }
            _ => continue,
        };
        out.push(EdgeRate {
            from: edge.from.clone(),
            to: edge.to.clone(),
            rate_per_sec: rate,
            unit: unit.to_string(),
        });
    }

    for bridge in &topology.bridges {
        // Rows from the consuming `ChannelSource`'s own drain, which is the
        // same flow measured at the better end; the producing `ChannelSink`'s
        // own rows are the fallback before the consumer has sampled.
        let (rate, unit) = match attributed_rate(series, ROWS_PROCESSED, SOURCE_ATTR, &bridge.to)
            .or_else(|| sink_rows(series, &bridge.from))
        {
            Some(rate) => (rate, "rows"),
            None => continue,
        };
        out.push(EdgeRate {
            from: bridge.from.clone(),
            to: bridge.to.clone(),
            rate_per_sec: rate,
            unit: unit.to_string(),
        });
    }
    out
}

#[cfg(test)]
pub(super) mod fixtures {
    use crate::inspector_wire::{TopoEdge, TopoNode, Topology, WorkflowTopology};

    pub(in crate::inspector) fn topo_node(
        id: &str,
        kind: &str,
        type_name: &str,
        component: Option<&str>,
    ) -> TopoNode {
        TopoNode {
            id: id.to_string(),
            kind: kind.to_string(),
            name: None,
            type_name: type_name.to_string(),
            component: component.map(str::to_string),
            runtime: None,
            window: None,
            detail: Vec::new(),
        }
    }

    /// A one-processor workflow: every named source feeds `"p"`, which feeds
    /// every named sink.
    pub(in crate::inspector) fn topology_with(sources: &[&str], sinks: &[&str]) -> Topology {
        let mut nodes = vec![topo_node("p", "processor", "wasm", None)];
        let mut edges = Vec::new();
        for name in sources {
            nodes.push(topo_node(name, "source", "nats", Some("Order")));
            edges.push(TopoEdge {
                from: (*name).to_string(),
                to: "p".to_string(),
                branch: None,
            });
        }
        for name in sinks {
            nodes.push(topo_node(name, "sink", "postgresql", Some("Order")));
            edges.push(TopoEdge {
                from: "p".to_string(),
                to: (*name).to_string(),
                branch: None,
            });
        }
        Topology {
            version: 1,
            node_id: "1".to_string(),
            mode: "standalone".to_string(),
            workflows: vec![WorkflowTopology {
                id: "quickstart".to_string(),
                name: None,
                nodes,
                edges,
            }],
            bridges: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::{topo_node, topology_with};
    use super::*;
    use crate::inspector_wire::{SeriesPoint, TopoEdge, WorkflowTopology};

    fn counter_sample(at: u64, name: &str, value: f64) -> MetricSample {
        MetricSample {
            at_unix_ms: at,
            series: vec![SeriesPoint {
                name: name.to_string(),
                kind: SeriesKind::Counter,
                attrs: Vec::new(),
                value,
                count: 0,
            }],
        }
    }

    /// A counter sample carrying one `key="value"` attribute, as a
    /// `saci_processor_*`/`saci_source_rows_total`/`saci_sink_batches_total`
    /// series records it.
    fn attributed_counter_sample(
        at: u64,
        name: &str,
        key: &str,
        value_attr: &str,
        value: f64,
    ) -> MetricSample {
        MetricSample {
            at_unix_ms: at,
            series: vec![SeriesPoint {
                name: name.to_string(),
                kind: SeriesKind::Counter,
                attrs: vec![(key.to_string(), value_attr.to_string())],
                value,
                count: 0,
            }],
        }
    }

    /// A counter sample carrying two attributes, the shape a branch-attributed
    /// `saci_processor_rows_out_total{processor=...,branch=...}` series takes.
    fn branch_counter_sample(
        at: u64,
        name: &str,
        key1: &str,
        value1: &str,
        key2: &str,
        value2: &str,
        value: f64,
    ) -> MetricSample {
        MetricSample {
            at_unix_ms: at,
            series: vec![SeriesPoint {
                name: name.to_string(),
                kind: SeriesKind::Counter,
                attrs: vec![
                    (key1.to_string(), value1.to_string()),
                    (key2.to_string(), value2.to_string()),
                ],
                value,
                count: 0,
            }],
        }
    }

    /// A two-processor chain: `orders-in -> validate -> settle -> settlements`.
    fn two_processor_topology() -> Topology {
        let nodes = vec![
            topo_node("orders-in", "source", "nats", Some("Order")),
            topo_node("validate", "processor", "wasm", None),
            topo_node("settle", "processor", "wasm", None),
            topo_node("settlements", "sink", "postgresql", Some("Order")),
        ];
        let edges = vec![
            TopoEdge {
                from: "orders-in".to_string(),
                to: "validate".to_string(),
                branch: None,
            },
            TopoEdge {
                from: "validate".to_string(),
                to: "settle".to_string(),
                branch: None,
            },
            TopoEdge {
                from: "settle".to_string(),
                to: "settlements".to_string(),
                branch: None,
            },
        ];
        Topology {
            version: 1,
            node_id: "1".to_string(),
            mode: "standalone".to_string(),
            workflows: vec![WorkflowTopology {
                id: "quickstart".to_string(),
                name: None,
                nodes,
                edges,
            }],
            bridges: Vec::new(),
        }
    }

    #[test]
    fn counter_rate_is_the_derivative_over_the_samples_in_the_window() {
        let samples = vec![
            counter_sample(1_000, ROWS_PROCESSED, 100.0),
            counter_sample(2_000, ROWS_PROCESSED, 250.0),
        ];
        let series = summarize_series(&samples);
        assert_eq!(series.len(), 1);
        assert!((series[0].rate_per_sec - 150.0).abs() < 1e-9);
        assert!((series[0].value - 250.0).abs() < f64::EPSILON);
        assert_eq!(series[0].points.len(), 2);
    }

    #[test]
    fn counter_reset_reports_zero_not_a_negative_rate() {
        let samples = vec![
            counter_sample(1_000, ROWS_PROCESSED, 900.0),
            counter_sample(2_000, ROWS_PROCESSED, 5.0),
        ];
        let series = summarize_series(&samples);
        assert!((series[0].rate_per_sec - 0.0).abs() < f64::EPSILON);
    }

    /// A gauge has no rate, and a window is not a licence to average one: a
    /// rising history inside the window must still report the newest reading,
    /// not its mean and not its slope.
    #[test]
    fn gauge_rate_is_the_value_itself() {
        let samples: Vec<MetricSample> = [(1_000, 10.0), (2_000, 20.0), (3_000, 30.0)]
            .into_iter()
            .map(|(at, value)| MetricSample {
                at_unix_ms: at,
                series: vec![SeriesPoint {
                    name: "saci_uptime_seconds".to_string(),
                    kind: SeriesKind::Gauge,
                    attrs: Vec::new(),
                    value,
                    count: 0,
                }],
            })
            .collect();
        let series = summarize_series(&samples);
        assert!(
            (series[0].rate_per_sec - 30.0).abs() < f64::EPSILON,
            "expected the newest reading, got {}",
            series[0].rate_per_sec
        );
    }

    /// The contract the rate window exists for. A runner counts a source's
    /// rows as it admits them and a sink's rows when it writes the staged
    /// batch at the end of the pass, so one flow's two ends are a smooth
    /// writer and a bursty one. Carrying the same total over the same span,
    /// they must read as the same throughput; a derivative across the two
    /// newest samples reads the burst as 2.5x the drain here.
    #[test]
    fn a_bursty_writer_and_a_smooth_one_moving_the_same_rows_report_the_same_rate() {
        let at = [1_000, 2_000, 3_000, 4_000, 5_000, 6_000];
        let smooth = [0.0, 120.0, 240.0, 360.0, 480.0, 600.0];
        let bursty = [0.0, 0.0, 300.0, 300.0, 300.0, 600.0];

        let rate = |values: [f64; 6]| {
            let samples: Vec<MetricSample> = at
                .iter()
                .zip(values)
                .map(|(t, v)| counter_sample(*t, ROWS_PROCESSED, v))
                .collect();
            summarize_series(&samples)[0].rate_per_sec
        };

        let smooth_rate = rate(smooth);
        let bursty_rate = rate(bursty);
        assert!(
            (smooth_rate - 120.0).abs() < 1e-9,
            "600 rows over the 5s window is 120 rows/s, got {smooth_rate}"
        );
        assert!(
            (bursty_rate - smooth_rate).abs() < 1e-9,
            "the same 600 rows over the same span must read the same however they \
             were written: smooth {smooth_rate}, bursty {bursty_rate}"
        );
    }

    /// A window wide enough to hold several samples can straddle a restart
    /// whose endpoints still rise, which the endpoint comparison alone would
    /// report as a throughput.
    #[test]
    fn a_counter_reset_inside_the_window_reports_zero() {
        let samples = vec![
            counter_sample(1_000, ROWS_PROCESSED, 0.0),
            counter_sample(2_000, ROWS_PROCESSED, 100.0),
            counter_sample(3_000, ROWS_PROCESSED, 200.0),
            counter_sample(4_000, ROWS_PROCESSED, 50.0),
            counter_sample(5_000, ROWS_PROCESSED, 150.0),
        ];
        let series = summarize_series(&samples);
        assert!(
            (series[0].rate_per_sec - 0.0).abs() < f64::EPSILON,
            "the series restarted mid-window, got {}",
            series[0].rate_per_sec
        );
    }

    #[test]
    fn history_is_decimated_to_the_cap_and_keeps_the_newest_point() {
        let history: Vec<PointAt> = (0..1000u32)
            .map(|i| PointAt {
                t: 1000 + u64::from(i),
                v: f64::from(i),
            })
            .collect();
        let thinned = decimate(&history);
        assert!(thinned.len() <= MAX_POINTS + 1, "got {}", thinned.len());
        assert_eq!(thinned.last().expect("non-empty").t, 1999);
    }

    #[test]
    fn source_edge_reads_its_own_attributed_rate() {
        let topology = topology_with(&["orders-in"], &["settlements"]);
        let series = summarize_series(&[
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "orders-in", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "orders-in", 12.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let inbound = edges.iter().find(|e| e.to == "p").expect("source edge");
        assert_eq!(inbound.unit, "rows");
        assert!((inbound.rate_per_sec - 12.0).abs() < 1e-9);
    }

    #[test]
    fn two_sources_read_independent_attributed_rates() {
        let topology = topology_with(&["a", "b"], &["settlements"]);
        let series = summarize_series(&[
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "a", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "a", 20.0),
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "b", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "b", 5.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let a = edges.iter().find(|e| e.from == "a").expect("a edge");
        let b = edges.iter().find(|e| e.from == "b").expect("b edge");
        assert!((a.rate_per_sec - 20.0).abs() < 1e-9);
        assert!((b.rate_per_sec - 5.0).abs() < 1e-9);
    }

    /// A batch is whatever row count the upstream handed over, so a sink
    /// edge is rated in rows and the batches counter rates nothing. It can
    /// never be a sink's only sample either: `SinkMetrics::write`
    /// records both on the same write.
    #[test]
    fn sink_edge_reads_rows_and_ignores_the_sinks_batches() {
        let topology = topology_with(&["orders-in"], &["settlements"]);
        let series = summarize_series(&[
            attributed_counter_sample(
                1_000,
                "saci_sink_batches_total",
                SINK_ATTR,
                "settlements",
                0.0,
            ),
            attributed_counter_sample(
                2_000,
                "saci_sink_batches_total",
                SINK_ATTR,
                "settlements",
                3.0,
            ),
            attributed_counter_sample(1_000, SINK_ROWS, SINK_ATTR, "settlements", 0.0),
            attributed_counter_sample(2_000, SINK_ROWS, SINK_ATTR, "settlements", 1_500.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let outbound = edges.iter().find(|e| e.from == "p").expect("sink edge");
        assert_eq!(outbound.unit, "rows");
        assert!((outbound.rate_per_sec - 1_500.0).abs() < 1e-9);
    }

    /// A sink that has written nothing has neither counter, because
    /// `SinkMetrics::write` records both on the same write. The edge is
    /// then omitted rather than borrowing the upstream processor's rows-out,
    /// which would animate an edge into a sink that has never delivered.
    #[test]
    fn sink_edge_is_omitted_rather_than_reading_the_processors_rows_out() {
        let topology = topology_with(&["orders-in"], &["settlements"]);
        let series = summarize_series(&[
            attributed_counter_sample(1_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "p", 0.0),
            attributed_counter_sample(2_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "p", 40.0),
        ]);
        let edges = edge_rates(&topology, &series);
        assert!(
            edges.iter().all(|e| e.to != "settlements"),
            "nothing has crossed to the sink, so the edge carries no number: {edges:?}"
        );
    }

    /// The shape a windowing processor produces: it returns its input
    /// component alongside the reduced one, so `saci_processor_rows_out_total`
    /// tracks its input while the sink takes one component and receives
    /// almost nothing. The edge must read the sink's number, not the
    /// processor's, or it animates at input rate beside a sink card at zero.
    #[test]
    fn sink_edge_reads_the_sinks_own_rows_not_the_processors_rows_out() {
        let topology = topology_with(&["orders-in"], &["settlements"]);
        let series = summarize_series(&[
            attributed_counter_sample(1_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "p", 0.0),
            attributed_counter_sample(2_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "p", 5_000.0),
            attributed_counter_sample(1_000, SINK_ROWS, SINK_ATTR, "settlements", 0.0),
            attributed_counter_sample(2_000, SINK_ROWS, SINK_ATTR, "settlements", 8.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let outbound = edges.iter().find(|e| e.from == "p").expect("sink edge");
        assert_eq!(outbound.unit, "rows");
        assert!(
            (outbound.rate_per_sec - 8.0).abs() < 1e-9,
            "got {}",
            outbound.rate_per_sec
        );
    }

    /// A labelled edge reads its own branch series, and the same processor's
    /// unlabelled edge into a sink reads that sink's own rows. Neither takes
    /// the node-level `saci_processor_rows_out_total`, which counts every row
    /// of the whole output dataset.
    #[test]
    fn labelled_edge_reads_its_own_branch_series() {
        let topology = Topology {
            version: 1,
            node_id: "1".to_string(),
            mode: "standalone".to_string(),
            workflows: vec![WorkflowTopology {
                id: "w".to_string(),
                name: None,
                nodes: vec![
                    topo_node("validate", "processor", "wasm", None),
                    topo_node("out_high", "sink", "postgresql", Some("Order")),
                    topo_node("out_plain", "sink", "postgresql", Some("Order")),
                ],
                edges: vec![
                    TopoEdge {
                        from: "validate".to_string(),
                        to: "out_high".to_string(),
                        branch: Some("high".to_string()),
                    },
                    TopoEdge {
                        from: "validate".to_string(),
                        to: "out_plain".to_string(),
                        branch: None,
                    },
                ],
            }],
            bridges: Vec::new(),
        };
        let series = summarize_series(&[
            branch_counter_sample(
                1_000,
                PROCESSOR_ROWS_OUT,
                PROCESSOR_ATTR,
                "validate",
                crate::inspector_wire::BRANCH_ATTR,
                "high",
                0.0,
            ),
            branch_counter_sample(
                2_000,
                PROCESSOR_ROWS_OUT,
                PROCESSOR_ATTR,
                "validate",
                crate::inspector_wire::BRANCH_ATTR,
                "high",
                30.0,
            ),
            attributed_counter_sample(1_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "validate", 0.0),
            attributed_counter_sample(2_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "validate", 40.0),
            attributed_counter_sample(1_000, SINK_ROWS, SINK_ATTR, "out_plain", 0.0),
            attributed_counter_sample(2_000, SINK_ROWS, SINK_ATTR, "out_plain", 7.0),
        ]);
        let edges = edge_rates(&topology, &series);

        let high = edges
            .iter()
            .find(|e| e.to == "out_high")
            .expect("labelled edge");
        assert_eq!(high.unit, "rows");
        assert!((high.rate_per_sec - 30.0).abs() < 1e-9);

        let plain = edges
            .iter()
            .find(|e| e.to == "out_plain")
            .expect("unlabelled edge");
        assert!(
            (plain.rate_per_sec - 7.0).abs() < 1e-9,
            "the unlabelled sink edge reads its sink's 7 rows/s, not the node's 40: {plain:?}"
        );
    }

    /// An unchosen branch has no sample of its own, and the sink-batches
    /// fallback must not stand in for it: absent reads as absent, so the edge
    /// is omitted rather than showing whatever the sink wrote.
    #[test]
    fn labelled_edge_is_omitted_when_its_branch_has_no_sample() {
        let topology = Topology {
            version: 1,
            node_id: "1".to_string(),
            mode: "standalone".to_string(),
            workflows: vec![WorkflowTopology {
                id: "w".to_string(),
                name: None,
                nodes: vec![
                    topo_node("validate", "processor", "wasm", None),
                    topo_node("out_high", "sink", "postgresql", Some("Order")),
                ],
                edges: vec![TopoEdge {
                    from: "validate".to_string(),
                    to: "out_high".to_string(),
                    branch: Some("high".to_string()),
                }],
            }],
            bridges: Vec::new(),
        };
        let series = summarize_series(&[
            attributed_counter_sample(1_000, SINK_ROWS, SINK_ATTR, "out_high", 0.0),
            attributed_counter_sample(2_000, SINK_ROWS, SINK_ATTR, "out_high", 5.0),
        ]);
        let edges = edge_rates(&topology, &series);

        assert!(
            edges
                .iter()
                .all(|e| !(e.from == "validate" && e.to == "out_high")),
            "an unchosen branch must be omitted, not given the sink's rate: {edges:?}"
        );
    }

    /// Without a sample for the upstream processor's own rows-out there is
    /// nothing to rate the boundary with, and no other series may stand in
    /// for it.
    #[test]
    fn processor_edge_is_omitted_when_the_upstream_has_no_attributed_sample() {
        let topology = two_processor_topology();
        let series = summarize_series(&[
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "orders-in", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "orders-in", 12.0),
        ]);
        let edges = edge_rates(&topology, &series);

        let inbound = edges
            .iter()
            .find(|e| e.from == "orders-in" && e.to == "validate")
            .expect("source edge");
        assert!((inbound.rate_per_sec - 12.0).abs() < 1e-9);
        assert!(
            edges
                .iter()
                .all(|e| !(e.from == "validate" && e.to == "settle")),
            "the inter-processor edge must be omitted, not given a stand-in rate: {edges:?}"
        );
    }

    /// Rows leaving the upstream processor are the rows entering the
    /// downstream one.
    #[test]
    fn processor_edge_uses_the_upstream_processors_own_rows_out() {
        let topology = two_processor_topology();
        let series = summarize_series(&[
            attributed_counter_sample(1_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "validate", 0.0),
            attributed_counter_sample(2_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "validate", 30.0),
        ]);
        let edges = edge_rates(&topology, &series);

        let between = edges
            .iter()
            .find(|e| e.from == "validate" && e.to == "settle")
            .expect("inter-processor edge");
        assert!((between.rate_per_sec - 30.0).abs() < 1e-9);
        assert_eq!(between.unit, "rows");
    }

    /// Each processor's own attributed series counts only its own rows, so a
    /// chain must rate an inter-processor edge from its upstream node alone,
    /// never the process-wide sum both processors contribute to.
    #[test]
    fn processor_edge_uses_its_upstreams_own_rows_out_not_the_process_wide_sum() {
        let topology = two_processor_topology();
        let series = summarize_series(&[
            attributed_counter_sample(1_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "validate", 0.0),
            attributed_counter_sample(2_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "validate", 30.0),
            attributed_counter_sample(1_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "settle", 0.0),
            attributed_counter_sample(2_000, PROCESSOR_ROWS_OUT, PROCESSOR_ATTR, "settle", 20.0),
            counter_sample(1_000, PROCESSOR_ROWS_OUT, 0.0),
            counter_sample(2_000, PROCESSOR_ROWS_OUT, 50.0),
        ]);
        let edges = edge_rates(&topology, &series);

        let between = edges
            .iter()
            .find(|e| e.from == "validate" && e.to == "settle")
            .expect("inter-processor edge");
        assert!(
            (between.rate_per_sec - 30.0).abs() < 1e-9,
            "expected validate's own 30 rows/s, not the 50 the two processors sum to: {between:?}"
        );
        assert_eq!(between.unit, "rows");
        assert!(
            edges.iter().all(|e| e.to != "settlements"),
            "the sink has written nothing, so its edge carries no number: {edges:?}"
        );
    }

    /// A `producer` workflow whose `ChannelSink` bridges into a `consumer`
    /// workflow's `ChannelSource`: each workflow keeps its own two nodes and
    /// own edge, and the bridge is reported separately.
    fn bridged_topology() -> Topology {
        Topology {
            version: 1,
            node_id: "1".to_string(),
            mode: "standalone".to_string(),
            workflows: vec![
                WorkflowTopology {
                    id: "producer".to_string(),
                    name: None,
                    nodes: vec![
                        topo_node("orders_in", "source", "file", Some("Order")),
                        topo_node("bridge_out", "sink", "channel", Some("Order")),
                    ],
                    edges: vec![TopoEdge {
                        from: "orders_in".to_string(),
                        to: "bridge_out".to_string(),
                        branch: None,
                    }],
                },
                WorkflowTopology {
                    id: "consumer".to_string(),
                    name: None,
                    nodes: vec![
                        topo_node("bridge_in", "source", "channel", Some("Order")),
                        topo_node("orders_out", "sink", "file", Some("Order")),
                    ],
                    edges: vec![TopoEdge {
                        from: "bridge_in".to_string(),
                        to: "orders_out".to_string(),
                        branch: None,
                    }],
                },
            ],
            bridges: vec![crate::inspector_wire::BridgeEdge {
                channel: "bridge".to_string(),
                from: "bridge_out".to_string(),
                to: "bridge_in".to_string(),
            }],
        }
    }

    /// A bridge is rated from the consuming `ChannelSource`'s own rows
    /// first, like any source edge, even though the bridge's `from` endpoint
    /// is a sink: it is the same flow measured one step later.
    #[test]
    fn bridge_prefers_the_consumer_source_rows() {
        let topology = bridged_topology();
        let series = summarize_series(&[
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "bridge_in", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "bridge_in", 7.0),
            attributed_counter_sample(1_000, SINK_ROWS, SINK_ATTR, "bridge_out", 0.0),
            attributed_counter_sample(2_000, SINK_ROWS, SINK_ATTR, "bridge_out", 2.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let bridge = edges
            .iter()
            .find(|e| e.from == "bridge_out" && e.to == "bridge_in")
            .expect("bridge edge");
        assert_eq!(bridge.unit, "rows");
        assert!((bridge.rate_per_sec - 7.0).abs() < 1e-9);
    }

    /// Before the consuming source has drained anything, the bridge reads
    /// the producing `ChannelSink`'s own rows: the same flow, one step
    /// earlier, still in rows.
    #[test]
    fn bridge_falls_back_to_the_producer_sink_rows() {
        let topology = bridged_topology();
        let series = summarize_series(&[
            attributed_counter_sample(1_000, SINK_ROWS, SINK_ATTR, "bridge_out", 0.0),
            attributed_counter_sample(2_000, SINK_ROWS, SINK_ATTR, "bridge_out", 2.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let bridge = edges
            .iter()
            .find(|e| e.from == "bridge_out" && e.to == "bridge_in")
            .expect("bridge edge");
        assert_eq!(bridge.unit, "rows");
        assert!((bridge.rate_per_sec - 2.0).abs() < 1e-9);
    }

    /// A bridge with no sample on either side is omitted, exactly like an
    /// edge whose upstream node has not sampled yet.
    #[test]
    fn unsampled_bridge_is_omitted() {
        let topology = bridged_topology();
        let edges = edge_rates(&topology, &[]);
        assert!(
            edges
                .iter()
                .find(|e| e.from == "bridge_out" && e.to == "bridge_in")
                .is_none()
        );
    }

    /// Edges from two workflows are each rated from their own workflow's
    /// nodes: the loop walks every workflow, not one.
    #[test]
    fn edges_from_each_workflow_are_rated() {
        let topology = bridged_topology();
        let series = summarize_series(&[
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "orders_in", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "orders_in", 10.0),
            attributed_counter_sample(1_000, ROWS_PROCESSED, SOURCE_ATTR, "bridge_in", 0.0),
            attributed_counter_sample(2_000, ROWS_PROCESSED, SOURCE_ATTR, "bridge_in", 5.0),
        ]);
        let edges = edge_rates(&topology, &series);
        let producer = edges
            .iter()
            .find(|e| e.to == "bridge_out")
            .expect("producer edge");
        let consumer = edges
            .iter()
            .find(|e| e.to == "orders_out")
            .expect("consumer edge");
        assert!((producer.rate_per_sec - 10.0).abs() < 1e-9);
        assert!((consumer.rate_per_sec - 5.0).abs() < 1e-9);
    }
}
