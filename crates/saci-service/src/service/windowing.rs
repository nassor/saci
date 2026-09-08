//! Host-side watermark tracking for windowed processor nodes.
//!
//! A processor node whose config declares a `window` block receives the
//! merged rows of every inbound link; the windowing and merging *logic* lives
//! in the processor or plugin itself, but the event-time watermark is a host
//! concern: the host is the only side that sees every stream feeding the node,
//! and the dashboard needs one number per node. [`WindowTracker`] advances a
//! monotonic watermark from the `time_field` column of the node's merged
//! dataset, exposes it to an in-process runtime as a
//! `saci_core::windows::WindowWatermark` resource, and reports it as the
//! `saci_window_watermark_seconds` series.
//!
//! The watermark is the maximum event timestamp observed so far across all
//! inbound rows. That is the same rule `saci_core::windows::WatermarkState`
//! applies inside a native pipeline, so the host number and a guest's own
//! number cannot drift. Allowed lateness is not subtracted: it is the guest's
//! budget for re-firing, not a completeness threshold.
//!
//! ## Pass granularity, and what flow control does about it
//!
//! A watermark is derived from what a pass delivered, so every pass boundary
//! is an event-time observation point, and three things read those points.
//!
//! - **Lateness.** A row is late against the watermark the *previous* pass
//!   left ([`saci_core::windows::WatermarkState`]'s rule: classify, then
//!   advance). An extra boundary drawn inside a single arrival advances the
//!   watermark from the arrival's own earlier rows, so a row sitting further
//!   back in that same arrival than the lateness budget is dropped.
//! - **Firing.** A window fires once the watermark passes its end plus the
//!   allowed lateness. An extra boundary reaches that point sooner, so a
//!   window can close before a row belonging to it has been admitted, and
//!   that row then re-opens and re-fires the same group.
//! - **Fan-in.** Each source carries its own controller, so a pass can
//!   deliver a full credit from one source and nothing from a slower or idle
//!   peer. The merged watermark then runs ahead of the peer's stream.
//!
//! The first two are about boundaries the runner would *invent*, and a
//! source's admission credit is the outcome of a throughput measurement
//! rather than a property of the data: inventing boundaries from it would put
//! the measurement in the output. So a source on a path to a windowed node
//! (`reaches_windowed_node`) is held away from the credit twice over.
//!
//! - The runner never slices its arrival. The credit stops the drain pulling
//!   the *next* arrival; it never splits the one in hand.
//! - The runner never sends it
//!   [`request_batch_rows`](saci_core::io::Source::request_batch_rows). That
//!   hint steers the connector's own fetch size, and `KafkaSource` and
//!   `NatsSource` overwrite `batch_size` with it while `PostgresSource`
//!   overwrites `batch_rows`, so honouring it would move the arrival
//!   boundaries themselves. That is the same measurement one layer earlier.
//!   Such a connector keeps its declared fetch size, a number the operator
//!   wrote down.
//!
//! ## What that guarantees, and what it leaves
//!
//! Guaranteed: an arrival is never split, and its size is never chosen by a
//! throughput measurement. Every boundary a windowed node observes event time
//! at is an arrival boundary the connector produced from its own
//! configuration.
//!
//! Not guaranteed: which arrivals share a pass. `RunMode::Continuous` and
//! `RunMode::Interval` admit whole arrivals until the credit is spent, so the
//! credit decides how many of them merge into one iteration's dataset, and an
//! adaptive credit is a measurement. A drain-to-EOF iteration merges every
//! arrival it can take into one pass. So a windowed node's pass boundaries
//! still move with the credit, at arrival granularity. The credit is a live
//! number in those modes, and `saci_flow_target_rows` and
//! `saci_flow_throughput_rows_per_second` report what bounds a pass.
//!
//! `RunMode::Stream` gives one arrival its own item either way, so it has no
//! such residual, and the two suppressions above leave the credit deciding
//! nothing there at all. Such a source therefore carries no
//! [`FlowController`](super::flow::FlowController) in that mode: no samples,
//! no epoch experiment, and no `saci_flow_*` series, including
//! `saci_flow_backoff_total`. Its arrival size is whatever the connector's
//! declared `batch_size`/`batch_rows` produces, and
//! [`run_stream`](super::stream::run_stream) names each such source in one
//! startup line. A controller kept anyway would publish a target nothing
//! applies, which is the one question those series exist to answer.
//!
//! Two configurations take the measurement out of the batch modes' residual,
//! and an operator who needs a reproducible grouping declares one of them on
//! **every** source feeding the node:
//!
//! - `flow_control { rows N }` pins the credit. A pinned size moves for
//!   neither an epoch nor a guard
//!   ([`fixed_rows`](super::flow::FlowSettings::fixed_rows)), so a pass
//!   is the first whole arrivals reaching N rows.
//! - `flow_control { enabled #false }` drops the credit, so a pass is every
//!   arrival the drain could take.
//!
//! Fan-in is different again: it is skew between streams, not a boundary the
//! runner chose, and it exists with flow control off and with one pass per
//! arrival. It is ordinary watermark semantics, and `allowed_lateness_ms`
//! covers it. Disorder *across* a source's arrivals needs the same budget,
//! because merging is what the residual above still leaves to the credit.
//!
//! That leaves the interaction rule between windowing and flow control:
//!
//! > A windowed node's closed-window results do not depend on a throughput
//! > measurement dividing an arrival or sizing one, because the credit does
//! > neither. They do depend on how many whole arrivals a pass merges, which
//! > the credit still decides in `Continuous` and `Interval`; pin `rows` or
//! > disable flow control on every feeding source to fix that too. In
//! > `Stream` the credit decides nothing, so the source runs without a
//! > controller and reports no flow-control series. Event-time disorder
//! > across a source's arrivals, and event-time skew between fan-in peers,
//! > need `allowed_lateness_ms` to cover them; that is ordinary watermark
//! > semantics, which flow control does not change.
//!
//! `crates/saci-service/tests/flow_control.rs`'s `windowed` cases pin the
//! no-split half. The same input through the same windowed workflow emits the
//! same set of window rows under `flow_control { enabled #false }` and under a
//! `rows` pin far below the arrival size: with a row moved back inside one
//! arrival by three times the lateness budget, in `Continuous` and in
//! `Stream`; with across-arrival disorder inside the budget; and across a
//! fan-in pair. `crates/saci-service/tests/windowed_fetch_hint.rs` pins the
//! no-hint half, in both runners.
//!
//! Keeping an arrival whole means one arrival's worth of Arrow memory, not
//! one credit's worth, so a connector that hands over a very large batch is
//! held in full. `flow_control { max_chunk_bytes }` does not cap it, because
//! nothing is being cut, and no hint asks the connector to send less. Bound it
//! at the connector instead, with whatever batch-size or fetch-size key it
//! exposes.
//!
//! The accumulator itself is safe across passes. Both runners thread a
//! windowed node's state blob from one pass to the next whatever `store`
//! says, because a `window` block is the declaration that the node
//! accumulates over more than one pass; see
//! [`run_standalone`](super::standalone::run_standalone).

use saci_core::SaciResult;
use saci_core::dataset::Dataset;
use saci_core::error::SaciError;

use super::builder::BuiltEdge;
use super::config::WindowConfig;
#[cfg(feature = "tracing")]
use super::sampling::WINDOWING_TARGET;

/// For every node, whether any path out of it reaches a windowed node
/// (itself included).
///
/// `trackers[i]` is `Some` exactly for a node whose config declares a
/// `window` block, and `downstream[i]` holds that node's outbound edges.
/// `BuiltEdge::node` always exceeds the owning node's index, because
/// `BuiltService::nodes` is in topological order, so one reverse pass
/// resolves the whole reachability relation.
///
/// The runners read the source entries: a `true` source keeps its arrivals
/// whole and receives no
/// [`request_batch_rows`](saci_core::io::Source::request_batch_rows) hint,
/// because splitting an arrival or sizing one from the credit would both move
/// a pass boundary the windowed node observes event time at.
/// [`run_stream`](super::stream::run_stream) goes one step further and builds
/// no controller for it: with both suppressions in force and one arrival per
/// item, nothing in that loop would read the target. A source feeding both a
/// windowed and a non-windowed node is `true` as well. Both see the same
/// `RecordBatch` in the same pass, so there is no split that reaches only the
/// node that can take it.
pub(super) fn reaches_windowed_node(
    trackers: &[Option<WindowTracker>],
    downstream: &[Vec<BuiltEdge>],
) -> Vec<bool> {
    let mut reaches = vec![false; trackers.len()];
    for i in (0..trackers.len()).rev() {
        reaches[i] = trackers[i].is_some()
            || downstream[i].iter().any(|edge| {
                // One reverse pass resolves the whole relation only because
                // every edge points forward. A backward edge would read an
                // entry this pass has not computed yet and answer `false` for
                // a source that really does feed a windowed node, which the
                // runners would then slice.
                debug_assert!(
                    edge.node > i,
                    "BuiltService::nodes must be topologically ordered, but node {i} \
                     links back to {}",
                    edge.node
                );
                reaches.get(edge.node).copied().unwrap_or(false)
            });
    }
    reaches
}

/// What one arrival told a windowed node's watermark.
///
/// A monotonic watermark plus a lateness budget is a filter: an arrival whose
/// newest timestamp falls below `watermark - allowed_lateness_ms` carries
/// nothing the node's windowing logic can use, so it opens no window, closes
/// none, and the node emits nothing downstream. That is correct event-time
/// semantics and the runners do not change it, but it is also
/// indistinguishable from a dead pipeline at every point an operator looks:
/// the sources report throughput, the processor reports batches, and the sink
/// reports nothing at all. This is what the runners report it with.
///
/// Two producers reach the state and neither is a service fault: a stream
/// whose event time restarts behind where it stopped (a publisher restarted
/// against a still-running service, a replay rewound to an earlier offset),
/// and one row carrying a timestamp far in the future, which drags the
/// watermark past every real row that follows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatermarkAdvance {
    /// The arrival carried no readable timestamp, so it classifies nothing.
    /// An empty pass, or a node whose components do not carry the time field.
    Empty,
    /// At least one row sits inside the lateness budget, so the node can use
    /// the arrival. `recovered` is `true` on the first such arrival after a
    /// run of unusable ones, which is the pass that ends the report.
    Usable {
        /// Whether this arrival ended a run of unusable ones.
        recovered: bool,
    },
    /// Every row is older than `watermark - allowed_lateness_ms`: the node's
    /// windowing logic drops the whole arrival. `first` is `true` for the one
    /// arrival that opens the report, so a caller reports the episode once
    /// rather than once per pass.
    BeyondLateness {
        /// The newest event timestamp the arrival carried.
        newest_ms: i64,
        /// How far that timestamp sits behind the node's watermark.
        behind_ms: i64,
        /// Whether this arrival opened the report.
        first: bool,
    },
}

/// Consecutive unusable arrivals before [`WatermarkAdvance::BeyondLateness`]
/// reports `first`.
///
/// One lagging fan-in peer is not this condition. A pass delivers one
/// source's arrival, so a peer whose event time trails the merged watermark
/// by more than the budget alternates unusable arrivals with the leading
/// peer's usable ones, and a first-arrival trip would report on every one of
/// them. A stream whose whole event time moved back leaves no usable arrival
/// in between, so an unbroken run is what separates the two. Four passes is
/// milliseconds of a live stream.
///
/// `saci_window_late_arrivals_total` counts every unusable arrival either
/// way, so the lagging peer stays visible as a number.
const BEYOND_LATENESS_RUN_TO_REPORT: u32 = 4;

/// Monotonic event-time watermark for one windowed processor node.
///
/// The tracker owns the node's [`WindowConfig`] so a runner can build one per
/// windowed node up front and read the declaration back for the topology or
/// the resource it inserts. The watermark starts at `i64::MIN`, meaning
/// nothing observed yet, and only ever moves forward.
///
/// It also carries how long the current run of unusable arrivals is, so
/// [`advance_from`](Self::advance_from) can tell a caller which pass opened
/// the report and which one ended it. That state is per node and both runners
/// hold one tracker per windowed node, so it lives here rather than beside
/// the runner's other per-node vectors.
#[derive(Debug, Clone)]
pub struct WindowTracker {
    config: WindowConfig,
    watermark_ms: i64,
    /// Consecutive arrivals whose every row was beyond the lateness budget.
    beyond_lateness_run: u32,
    /// Whether the current run has already been reported as `first`.
    beyond_lateness_reported: bool,
}

impl WindowTracker {
    /// Create a tracker for `config`, before any data has been observed.
    pub fn new(config: WindowConfig) -> Self {
        Self {
            config,
            watermark_ms: i64::MIN,
            beyond_lateness_run: 0,
            beyond_lateness_reported: false,
        }
    }

    /// The declaration this tracker honours.
    pub fn config(&self) -> &WindowConfig {
        &self.config
    }

    /// The current watermark in milliseconds since the Unix epoch, or
    /// `i64::MIN` when no timestamp has been observed yet.
    pub fn watermark_ms(&self) -> i64 {
        self.watermark_ms
    }

    /// The current watermark as fractional epoch seconds.
    pub fn watermark_seconds(&self) -> f64 {
        self.watermark_ms as f64 / 1000.0
    }

    /// Whether any timestamp has been observed yet.
    pub fn has_watermark(&self) -> bool {
        self.watermark_ms != i64::MIN
    }

    /// Advance the watermark from every row of every component in `dataset`
    /// that carries the configured `time_field`, and classify the arrival
    /// against the node's lateness budget.
    ///
    /// The merged dataset holds one batch per component; a component whose
    /// schema lacks the time field is skipped (load-time validation requires
    /// every *delivered* component to carry it, but a processor may declare
    /// extra components of its own). Null timestamps are skipped, mirroring
    /// `WindowedSystem`'s null-timestamp handling.
    ///
    /// The returned [`WatermarkAdvance`] applies
    /// [`WatermarkState`](saci_core::windows::watermark::WatermarkState)'s own
    /// rule to the arrival's newest timestamp: below
    /// `watermark - allowed_lateness_ms`, the node's windowing logic has no
    /// row it can place in a window, so the whole arrival is dropped and
    /// nothing reaches the sinks behind it. Comparing the newest timestamp is
    /// enough because it is also the one that would have moved the watermark:
    /// an arrival that advanced it is on time by construction.
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` when a component's time column has a type
    /// the millisecond converter cannot read.
    pub fn advance_from(&mut self, dataset: &Dataset) -> SaciResult<WatermarkAdvance> {
        let time_field = self.config.time_field.as_str();
        let names: Vec<&'static str> = dataset.schemas().iter().map(|(name, _)| *name).collect();

        let watermark_before = self.watermark_ms;
        let mut newest: Option<i64> = None;
        for name in names {
            let Some(schema) = dataset.schemas().get(name) else {
                continue;
            };
            if !schema.fields().iter().any(|f| f.name() == time_field) {
                continue;
            }
            let batch = dataset
                .batch_for(name)
                .expect("registered component has a batch");
            let idx = schema.index_of(time_field).map_err(|e| {
                SaciError::generic(format!("WindowTracker: time field lookup: {e}"))
            })?;
            let col = batch.column(idx);
            let time_ms = saci_core::windows::time::to_ms_array(col)?;
            for value in time_ms.iter().flatten() {
                if value > self.watermark_ms {
                    self.watermark_ms = value;
                }
                if newest.is_none_or(|seen| value > seen) {
                    newest = Some(value);
                }
            }
        }

        let Some(newest) = newest else {
            // Nothing to classify: an empty arrival neither opens nor ends a
            // run of dropped ones.
            return Ok(WatermarkAdvance::Empty);
        };

        // The same three guards `WatermarkState::is_beyond_lateness` applies:
        // no watermark yet, and a budget at or above the watermark, are both
        // infinite tolerance.
        let threshold = if watermark_before == i64::MIN
            || self.config.allowed_lateness_ms >= watermark_before
        {
            None
        } else {
            Some(watermark_before - self.config.allowed_lateness_ms)
        };

        match threshold {
            Some(threshold) if newest < threshold => {
                self.beyond_lateness_run = self.beyond_lateness_run.saturating_add(1);
                let first = !self.beyond_lateness_reported
                    && self.beyond_lateness_run >= BEYOND_LATENESS_RUN_TO_REPORT;
                self.beyond_lateness_reported |= first;
                Ok(WatermarkAdvance::BeyondLateness {
                    newest_ms: newest,
                    behind_ms: watermark_before.saturating_sub(newest),
                    first,
                })
            }
            _ => {
                // One usable arrival breaks the run, so ordinary fan-in skew
                // never accumulates one, and a report always has a clearing
                // edge.
                self.beyond_lateness_run = 0;
                let recovered = std::mem::replace(&mut self.beyond_lateness_reported, false);
                Ok(WatermarkAdvance::Usable { recovered })
            }
        }
    }
}

/// Report one arrival's [`WatermarkAdvance`] for a windowed node.
///
/// Called by both runners on every pass that reaches a windowed processor,
/// which is why the reporting is edge-triggered: the counter takes every
/// dropped arrival, so an alert can see the whole episode, while the log
/// takes the pass that opened the run and the pass that ended it. A stream
/// whose event time has fallen behind is a condition, not an event, and one
/// line per pass at 50 000 rows a second is not a diagnosis.
///
/// The lines go to [`WINDOWING_TARGET`], which no `log_level` and no
/// `RUST_LOG` can silence, because this is the one condition where every
/// other number an operator can read says the service is healthy: the sources
/// report throughput, the processor reports batches, and the sinks behind the
/// node report nothing at all.
pub(super) fn report_watermark_advance(
    workflow_id: &str,
    processor_id: &str,
    tracker: &WindowTracker,
    advance: WatermarkAdvance,
) {
    match advance {
        WatermarkAdvance::Empty => {}
        WatermarkAdvance::Usable { recovered } => {
            if recovered {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: WINDOWING_TARGET,
                    workflow = %workflow_id,
                    processor = %processor_id,
                    watermark_ms = tracker.watermark_ms(),
                    "windowed node is accepting rows again: an arrival landed back inside its lateness budget. The watermark is unchanged unless that arrival advanced it"
                );
                #[cfg(not(feature = "tracing"))]
                {
                    let _ = (workflow_id, processor_id, tracker);
                }
            }
        }
        WatermarkAdvance::BeyondLateness {
            newest_ms,
            behind_ms,
            first,
        } => {
            crate::metrics::instruments().window_late_arrival(processor_id);
            if first {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: WINDOWING_TARGET,
                    workflow = %workflow_id,
                    processor = %processor_id,
                    newest_event_ms = newest_ms,
                    watermark_ms = tracker.watermark_ms(),
                    behind_ms,
                    allowed_lateness_ms = tracker.config().allowed_lateness_ms,
                    "windowed node is dropping every arrival: the inbound event time is behind its watermark by more than the allowed lateness, so no window can open or close and its sinks receive nothing. A producer restarted with rewound event time, a replay, or one far-future timestamp will do this; the watermark is monotonic and never rewinds with the stream"
                );
                #[cfg(not(feature = "tracing"))]
                {
                    let _ = (workflow_id, tracker, newest_ms, behind_ms);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float64Array, Int64Array, TimestampMillisecondArray};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use saci_core::component::Component;
    use saci_core::windows::WindowSpec;

    use super::*;

    #[derive(Serialize, Deserialize)]
    struct Trade {
        timestamp_ms: i64,
        price: f64,
    }
    impl Component for Trade {
        fn name() -> &'static str {
            "Trade"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("timestamp_ms", DataType::Int64, false),
                Field::new("price", DataType::Float64, false),
            ]))
        }
    }

    fn config() -> WindowConfig {
        WindowConfig {
            spec: WindowSpec::Tumbling {
                size_ms: 30_000,
                offset_ms: 0,
            },
            time_field: "timestamp_ms".to_string(),
            key_fields: Vec::new(),
            allowed_lateness_ms: 0,
        }
    }

    #[test]
    fn starts_without_a_watermark() {
        let tracker = WindowTracker::new(config());
        assert!(!tracker.has_watermark());
        assert_eq!(tracker.watermark_ms(), i64::MIN);
    }

    #[test]
    fn advances_from_the_time_column_and_is_monotonic() {
        let mut dataset = Dataset::new();
        dataset.register_component::<Trade>().unwrap();
        dataset
            .append::<Trade>(&[
                Trade {
                    timestamp_ms: 1_000,
                    price: 1.0,
                },
                Trade {
                    timestamp_ms: 3_000,
                    price: 2.0,
                },
            ])
            .unwrap();

        let mut tracker = WindowTracker::new(config());
        tracker.advance_from(&dataset).unwrap();
        assert_eq!(tracker.watermark_ms(), 3_000);

        // A later, smaller batch must not move the watermark backwards.
        dataset.clear();
        dataset
            .append::<Trade>(&[Trade {
                timestamp_ms: 500,
                price: 3.0,
            }])
            .unwrap();
        tracker.advance_from(&dataset).unwrap();
        assert_eq!(tracker.watermark_ms(), 3_000);

        dataset.clear();
        dataset
            .append::<Trade>(&[Trade {
                timestamp_ms: 4_500,
                price: 4.0,
            }])
            .unwrap();
        tracker.advance_from(&dataset).unwrap();
        assert_eq!(tracker.watermark_ms(), 4_500);
        assert!(tracker.has_watermark());
        assert!((tracker.watermark_seconds() - 4.5).abs() < 1e-9);
    }

    #[test]
    fn skips_components_without_the_time_field_and_null_timestamps() {
        let mut dataset = Dataset::new();
        dataset.register_component::<Trade>().unwrap();
        // A component with no time field: must be skipped, not an error.
        dataset.register_raw_component(
            "Audit",
            Arc::new(Schema::new(vec![Field::new("note", DataType::Utf8, false)])),
        );
        // A component whose time column carries nulls: the null's backing bits
        // must not advance the watermark.
        let null_schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp_ms",
            DataType::Int64,
            true,
        )]));
        dataset.register_raw_component("NullTrade", null_schema);

        dataset
            .append::<Trade>(&[Trade {
                timestamp_ms: 7_000,
                price: 1.0,
            }])
            .unwrap();
        let ts: ArrayRef = Arc::new(Int64Array::from(vec![Some(9_000i64), None]));
        let batch = arrow_array::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "timestamp_ms",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(ts) as ArrayRef],
        )
        .unwrap();
        dataset.append_record_batch("NullTrade", batch).unwrap();

        let mut tracker = WindowTracker::new(config());
        tracker.advance_from(&dataset).unwrap();
        assert_eq!(tracker.watermark_ms(), 9_000);
    }

    #[test]
    fn reads_arrow_timestamp_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "at",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            false,
        )]));
        let ts = TimestampMillisecondArray::from(vec![1_000, 2_000]);
        let mut dataset = Dataset::new();
        dataset.register_raw_component("Event", schema.clone());
        dataset
            .append_record_batch(
                "Event",
                arrow_array::RecordBatch::try_new(schema, vec![Arc::new(ts) as ArrayRef]).unwrap(),
            )
            .unwrap();

        let mut tracker = WindowTracker::new(WindowConfig {
            time_field: "at".to_string(),
            ..config()
        });
        tracker.advance_from(&dataset).unwrap();
        assert_eq!(tracker.watermark_ms(), 2_000);
    }

    #[test]
    fn rejects_an_unreadable_time_column_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp_ms",
            DataType::Float64,
            false,
        )]));
        let prices = Float64Array::from(vec![1.5, 2.5]);
        let mut dataset = Dataset::new();
        dataset.register_raw_component("BadTime", schema.clone());
        dataset
            .append_record_batch(
                "BadTime",
                arrow_array::RecordBatch::try_new(schema, vec![Arc::new(prices) as ArrayRef])
                    .unwrap(),
            )
            .unwrap();

        let mut tracker = WindowTracker::new(config());
        let err = tracker.advance_from(&dataset).unwrap_err();
        assert_eq!(err.category(), "generic");
    }

    /// One arrival of `Trade` rows at the given timestamps.
    fn arrival(timestamps: &[i64]) -> Dataset {
        let mut dataset = Dataset::new();
        dataset.register_component::<Trade>().unwrap();
        let rows: Vec<Trade> = timestamps
            .iter()
            .map(|&timestamp_ms| Trade {
                timestamp_ms,
                price: 1.0,
            })
            .collect();
        dataset.append::<Trade>(&rows).unwrap();
        dataset
    }

    /// The reported condition: an inbound stream whose event time restarted
    /// behind where it stopped. Every arrival after the rewind is beyond the
    /// budget, so the node's windowing logic drops all of it and its sinks
    /// receive nothing. Exactly one arrival of the run reports `first`, so a
    /// live stream produces one line rather than one per pass.
    #[test]
    fn an_arrival_wholly_behind_the_watermark_is_reported_once() {
        let mut tracker = WindowTracker::new(WindowConfig {
            allowed_lateness_ms: 5_000,
            ..config()
        });

        assert_eq!(
            tracker.advance_from(&arrival(&[100_000, 120_000])).unwrap(),
            WatermarkAdvance::Usable { recovered: false },
            "the first arrival sets the watermark"
        );

        // 2 000 < 120 000 - 5 000, so every arrival below is unusable. The
        // report waits for an unbroken run, which one lagging fan-in peer
        // never produces.
        let mut firsts = 0;
        for pass in 1..=6i64 {
            let advance = tracker.advance_from(&arrival(&[pass * 1_000])).unwrap();
            let WatermarkAdvance::BeyondLateness {
                newest_ms,
                behind_ms,
                first,
            } = advance
            else {
                panic!("pass {pass} must be unusable, got {advance:?}");
            };
            assert_eq!(newest_ms, pass * 1_000);
            assert_eq!(behind_ms, 120_000 - pass * 1_000);
            if first {
                firsts += 1;
                assert_eq!(
                    pass,
                    i64::from(BEYOND_LATENESS_RUN_TO_REPORT),
                    "the report opens on the pass that completes the run"
                );
            }
        }
        assert_eq!(firsts, 1, "one report per run, however long it lasts");
        assert_eq!(
            tracker.watermark_ms(),
            120_000,
            "a rewound stream never moves the watermark back"
        );

        assert_eq!(
            tracker.advance_from(&arrival(&[130_000])).unwrap(),
            WatermarkAdvance::Usable { recovered: true },
            "the pass that lands back inside the budget ends the run"
        );
        assert_eq!(
            tracker.advance_from(&arrival(&[140_000])).unwrap(),
            WatermarkAdvance::Usable { recovered: false },
            "and only that one pass reports the recovery"
        );
    }

    /// Ordinary fan-in skew is not the condition. A pass carries one source's
    /// arrival, so a peer trailing the merged watermark by more than the
    /// budget alternates with the leading peer's usable arrivals. Those
    /// arrivals are counted, never reported: no run of them accumulates.
    #[test]
    fn a_lagging_fan_in_peer_never_opens_a_report() {
        let mut tracker = WindowTracker::new(WindowConfig {
            allowed_lateness_ms: 5_000,
            ..config()
        });
        tracker.advance_from(&arrival(&[100_000])).unwrap();

        for pass in 0..20 {
            let leader = 100_000 + pass * 1_000;
            assert_eq!(
                tracker.advance_from(&arrival(&[leader])).unwrap(),
                WatermarkAdvance::Usable { recovered: false },
                "the leading peer keeps the watermark moving"
            );
            let advance = tracker.advance_from(&arrival(&[1_000])).unwrap();
            assert!(
                matches!(
                    advance,
                    WatermarkAdvance::BeyondLateness { first: false, .. }
                ),
                "the lagging peer's rows are dropped but never reported, got {advance:?}"
            );
        }
    }

    /// The lateness budget is what decides, so an arrival inside it is
    /// usable however far below the watermark it sits.
    #[test]
    fn an_arrival_inside_the_lateness_budget_is_usable() {
        let mut tracker = WindowTracker::new(WindowConfig {
            allowed_lateness_ms: 5_000,
            ..config()
        });
        tracker.advance_from(&arrival(&[100_000])).unwrap();
        assert_eq!(
            tracker.advance_from(&arrival(&[96_000])).unwrap(),
            WatermarkAdvance::Usable { recovered: false },
            "96 000 >= 100 000 - 5 000: late but acceptable, so the node can refire"
        );
    }

    /// An empty pass classifies nothing: it carries no timestamp, so it
    /// neither counts toward a run of dropped arrivals nor breaks one. An
    /// idle stream is therefore never reported as a rewound one, and an idle
    /// gap mid-run neither delays the report nor clears it.
    #[test]
    fn an_arrival_with_no_timestamp_neither_opens_nor_ends_a_run() {
        let mut tracker = WindowTracker::new(WindowConfig {
            allowed_lateness_ms: 0,
            ..config()
        });
        tracker.advance_from(&arrival(&[100_000])).unwrap();

        let mut empty = Dataset::new();
        empty.register_component::<Trade>().unwrap();

        // One short of the run, then an empty pass, then the arrival that
        // completes it: the report must still open, so the empty pass neither
        // counted nor reset.
        for pass in 1..i64::from(BEYOND_LATENESS_RUN_TO_REPORT) {
            let advance = tracker.advance_from(&arrival(&[pass * 1_000])).unwrap();
            assert!(
                matches!(
                    advance,
                    WatermarkAdvance::BeyondLateness { first: false, .. }
                ),
                "the run is still short of the threshold, got {advance:?}"
            );
        }
        assert_eq!(
            tracker.advance_from(&empty).unwrap(),
            WatermarkAdvance::Empty
        );
        assert!(
            matches!(
                tracker.advance_from(&arrival(&[8_000])).unwrap(),
                WatermarkAdvance::BeyondLateness { first: true, .. }
            ),
            "the empty pass must neither count toward the run nor reset it"
        );

        // And an empty pass inside an open report leaves it open.
        assert_eq!(
            tracker.advance_from(&empty).unwrap(),
            WatermarkAdvance::Empty
        );
        assert_eq!(
            tracker.advance_from(&arrival(&[9_000])).unwrap(),
            WatermarkAdvance::BeyondLateness {
                newest_ms: 9_000,
                behind_ms: 91_000,
                first: false,
            },
            "the empty pass must not have cleared the report"
        );
        assert_eq!(
            tracker.advance_from(&arrival(&[110_000])).unwrap(),
            WatermarkAdvance::Usable { recovered: true },
            "only a usable arrival clears it"
        );
    }

    /// With no watermark yet nothing can be late, which is the rule
    /// `WatermarkState::is_beyond_lateness` applies for `i64::MIN`.
    #[test]
    fn the_first_arrival_is_never_beyond_lateness() {
        let mut tracker = WindowTracker::new(config());
        assert_eq!(
            tracker.advance_from(&arrival(&[i64::MIN + 1])).unwrap(),
            WatermarkAdvance::Usable { recovered: false }
        );
    }

    /// One edge per `(from, to)` pair, in the topological shape
    /// `BuiltService::nodes` guarantees.
    fn edges(pairs: &[(usize, usize)], n: usize) -> Vec<Vec<BuiltEdge>> {
        let mut out = vec![Vec::new(); n];
        for &(from, to) in pairs {
            out[from].push(BuiltEdge {
                node: to,
                branch: None,
            });
        }
        out
    }

    /// A windowed node several links downstream still claims its source: the
    /// reverse pass has to carry reachability through the nodes between them,
    /// not just look one edge ahead.
    #[test]
    fn a_windowed_node_behind_two_processors_claims_the_source() {
        // source 0 -> processor 1 -> windowed processor 2 -> sink 3
        let trackers = vec![None, None, Some(WindowTracker::new(config())), None];
        let reaches = reaches_windowed_node(&trackers, &edges(&[(0, 1), (1, 2), (2, 3)], 4));
        assert_eq!(reaches, vec![true, true, true, false]);
    }

    /// A source that also feeds a plain sink is still a windowed source: the
    /// two downstream nodes share one `RecordBatch`, so there is no split
    /// that reaches only the one that could take it.
    #[test]
    fn a_source_fanning_out_to_both_kinds_counts_as_windowed() {
        // source 0 -> windowed processor 1, source 0 -> sink 2
        let trackers = vec![None, Some(WindowTracker::new(config())), None];
        let reaches = reaches_windowed_node(&trackers, &edges(&[(0, 1), (0, 2)], 3));
        assert_eq!(reaches, vec![true, true, false]);
    }

    /// A workflow with no `window` block anywhere claims nothing, which is
    /// what leaves the ordinary chunking path untouched.
    #[test]
    fn a_workflow_with_no_window_block_claims_no_source() {
        // source 0 -> processor 1 -> sink 2
        let trackers = vec![None, None, None];
        let reaches = reaches_windowed_node(&trackers, &edges(&[(0, 1), (1, 2)], 3));
        assert_eq!(reaches, vec![false, false, false]);
    }
}
