//! Adaptive flow control: how many rows a runner admits from one source per
//! pass, and how large the Arrow chunk handed downstream is.
//!
//! [`FlowController`] owns the adaptation state of exactly one source node. It
//! is pure host-side arithmetic: no IO, no clock reads, no connector
//! knowledge. The runner feeds it a [`FlowSample`] describing what happened
//! while the last admitted chunk was consumed, and reads
//! [`target_rows`](FlowController::target_rows) for the next one. A source
//! that ignores [`request_batch_rows`](saci_core::io::Source::request_batch_rows)
//! is still governed, because the runner reshapes whatever arrives with
//! zero-copy `RecordBatch::slice`. `Carry` is the one piece of Arrow this
//! module touches: both runners re-chunk, and both must weigh an arrival the
//! same way, so the buffer they share lives here rather than once per runner.
//!
//! ## Adjustment is an experiment, paced by an epoch
//!
//! Optimisation decisions land at the close of an adjustment epoch
//! (`adjust_interval_ms`, one minute by default), never per pass. Each epoch
//! runs two arms: the incumbent size, and one candidate a step away from it.
//! The arms alternate pass by pass rather than running as two blocks, so drift
//! in machine load, clock speed and upstream arrival rate falls on both arms
//! instead of being attributed to one. At close the arms' rows per second are
//! compared, the winner becomes the next epoch's incumbent, and the next
//! candidate steps further in the direction that just won; a loss flips the
//! direction, so the search can discover that smaller is faster for a pipeline
//! whose per-batch cost is dominated by cache behaviour rather than overhead.
//!
//! An epoch that closes with fewer than `min_samples_per_arm` usable samples
//! in either arm decides nothing and starts again. A workload that finishes
//! before its first epoch closes therefore runs at `start_rows`, which is the
//! right answer for a job too short to measure.
//!
//! A search that stops moving stops probing. Once `settle_after_epochs`
//! consecutive epochs close with enough evidence and leave the incumbent
//! where it is, the controller rests: a run of incumbent-only epochs, each a
//! normal epoch that simply never schedules the candidate, so every pass is
//! admitted at the incumbent. The run doubles with each further settled
//! epoch and stops at sixteen, so a converged source experiments in one
//! epoch out of seventeen and pays roughly 3% of its passes for the search
//! rather than half of them.
//!
//! Two things end a rest early: a guard trip, on the pass that saw it, and
//! an incumbent that starts missing `target_latency_ms`, at the epoch
//! boundary that measured it. The objective is checked at every boundary
//! that measured enough passes to judge it, so a stream workflow's objective
//! stays reactive instead of waiting out the run. Both reset the streak, and
//! so does an epoch that moves the incumbent, which only an experiment epoch
//! can do. An epoch that closes with too little evidence measured nothing
//! either way and leaves the streak alone. `settle_after_epochs 0` disables
//! resting, so the arms alternate for as long as the source runs.
//!
//! Safety is not paced. A pass error, a chunk projected past
//! `max_chunk_bytes`, or a sink backlog growing on consecutive passes divides
//! the target by `backoff_factor`, two by default, on the pass that observed
//! it, and abandons the epoch.
//!
//! ## Objective per run mode
//!
//! What the arms are judged on depends on the run mode, because the modes
//! promise different things:
//!
//! - **Stream mode: throughput subject to a latency objective.** A stream
//!   workflow exists to deliver items promptly, so `target_latency_ms` is a
//!   constraint on the size in effect, not only a veto on growth: a candidate
//!   breaching it cannot win however many rows per second it moved, and while
//!   the *incumbent* breaches it, a smaller candidate wins however many fewer
//!   it moved. The search therefore descends until the objective is met, or
//!   until `min_rows` proves it cannot be. The default there is 250 ms.
//! - **`Continuous` and `Interval`: throughput.** A batch pass carries no
//!   per-item latency contract, so rows per second alone decides.
//!   `target_latency_ms` defaults to 0 there, which disables the objective.
//!
//! The mode sets the *default* only. An explicitly configured
//! `target_latency_ms` is honoured verbatim in either mode, including a
//! non-zero value in batch mode and `0` in stream mode. An objective breached
//! even at `min_rows` cannot be met at any size the controller can choose, so
//! it is abandoned rather than obeyed at the cost of throughput, and the whole
//! rule turns off until a pass at the floor meets it again.
//!
//! A division is remembered as a congestion ceiling: while it stands, growth
//! is additive (an eighth of the ceiling) and stops at the size that failed,
//! and the next epoch's first candidate is a step *down*. What failed is the
//! larger of the arm under test and the incumbent, because a candidate
//! stepping down that trips a guard is no evidence against the size it was
//! being measured against. `min_rows` is a hard floor and also the most
//! conservative answer available, so adverse evidence there holds the target
//! and keeps the search state instead of thrashing it: for a pipeline with
//! fixed per-batch overhead, the floor is the worst throughput point, not the
//! safest one. It is still reported ([`FlowAdjustment::HeldAtFloor`]) and
//! still counted, because a source pinned against a wall is what an operator
//! needs to see.
//!
//! A step that cannot move, into a bound or into the ceiling, is turned
//! around rather than proposed: the arms only alternate while they differ, so
//! a candidate equal to the incumbent is an experiment that can never run.
//! Only `min_rows == max_rows` is stationary, and that range has one
//! admissible size.
//!
//! Time spent waiting for input is never consumption: the runners measure the
//! consumer chain alone, so a source with a one-second poll timeout neither
//! understates its throughput nor breaches a latency objective.
//!
//! ## Where it runs
//!
//! - `RunMode::Continuous` and `RunMode::Interval`: one sample per iteration
//!   per source. An iteration that exhausts its credit with the source still
//!   live, or that leaves a carry-over slice, is a backlogged iteration and
//!   skips run-mode pacing, so `interval_ms` stays the idle poll cadence
//!   rather than a throughput ceiling.
//! - `RunMode::Stream`: one sample per chunk, each chunk one workflow pass.
//! - `RunMode::OneShot` engages no controller. A single pass must drain
//!   everything by definition, so admitting a credit-sized prefix and exiting
//!   would silently drop the rest of the source.
//!
//! ## Where the choice is semantically visible
//!
//! For a node whose output is a function of its rows, how the runner grouped
//! them into passes is an implementation detail: a chunk is a prefix of the
//! same ordered rows, and the same rows produce the same output. A node whose
//! output is a function of the *pass* is not so lucky, and there are two
//! kinds. A windowed processor node is the one the runner can recognise and
//! protect, because a pass boundary is where it observes event time. The
//! other is a processor that derives a per-pass decision from the batch it
//! was handed: `examples/multi_workflow`'s router reads the first row's
//! `amount` and routes the whole batch on it. The runner cannot recognise
//! that from the node graph, so any controller-chosen boundary in such a
//! node's input changes which branch some rows take: a slice at the target
//! for a connector that ignores `request_batch_rows`, and an arrival sized
//! by the target for one that acts on it. What settles a pass differs by
//! case. A connector that ignores the hint is cut by the runner alone, so
//! `flow_control { rows N }` caps a pass at N rows and removes the
//! measurement dependence; a short arrival, and the tail of a longer one,
//! still make a shorter pass. One that acts on the hint resizes its own
//! fetch to it: `enabled #false` hands the connector back its configured
//! `batch_size`, widening the pass instead of settling it, and any N above
//! 1 still leaves a fetch window gathering toward N. Only `rows 1` is
//! structurally sufficient there, because a fetch of one cannot gather a
//! second message.
//!
//! So a source on a path to a windowed node is bounded but never sliced: the
//! credit stops the runner pulling the next arrival, and the arrival in hand
//! passes through whole. It is not sent `request_batch_rows` either, so the
//! credit does not size its arrivals at the connector.
//! [`windowing`](super::windowing) states the rule, what stays
//! measurement-dependent, and the memory one whole arrival costs.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;

use super::config::{RunMode, ServiceConfig, ServiceMode};

/// Weight of the newest measurement in the throughput EWMA.
///
/// The EWMA is reporting only: [`FlowController::throughput`] feeds the
/// `saci_flow_throughput_rows_per_second` gauge and nothing else, and no
/// adaptation decision reads it.
const EWMA_ALPHA: f64 = 0.3;

/// Fraction of a congestion ceiling one candidate step moves while that
/// ceiling stands, as its divisor: an eighth of the ceiling.
///
/// Additive steps below a known wall, rather than doubling into it.
const CEILING_INCREMENT_DIVISOR: usize = 8;

/// Consecutive passes on which a sink's backlog must strictly grow before it
/// counts as sink pressure.
///
/// Contract rather than tuning: the trend rule is what makes a sink's private
/// backlog number comparable at all, so it is not an operator knob.
const PRESSURE_TREND_PASSES: u32 = 2;

/// Longest run of incumbent-only epochs one settled stretch may reach.
///
/// Contract rather than tuning: it bounds how long a settled source can go
/// without re-measuring, so the search always comes back rather than
/// converging permanently on a size the machine has since outgrown.
const MAX_REST_EPOCHS: u32 = 16;

/// Default latency objective in stream mode, in milliseconds.
const STREAM_LATENCY_OBJECTIVE_MS: u64 = 250;

/// Resolved, concrete flow-control settings for one source node.
///
/// Produced by
/// [`FlowControlConfig::resolve`](super::config::FlowControlConfig::resolve).
/// The defaults are a working policy: the `flow_control` block exists to let
/// an operator pace, pin or disable adaptation, not because it has to be
/// filled in.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FlowSettings {
    /// Floor on the target, in rows.
    pub min_rows: usize,
    /// Ceiling on the target, in rows.
    pub max_rows: usize,
    /// Target the controller starts from, clamped into `[min_rows, max_rows]`.
    pub start_rows: usize,
    /// Ceiling on the projected Arrow memory of one chunk; `0` is unbounded.
    pub max_chunk_bytes: u64,
    /// Latency objective for one pass; `0` disables the objective.
    ///
    /// Defaults per run mode: 250 in stream mode, where per-item latency is
    /// the contract, and 0 in `Continuous`/`Interval`, where throughput is the
    /// only objective. An explicit value wins in either mode.
    pub target_latency_ms: u64,
    /// Length of one adjustment epoch, in milliseconds. Meaningless for a
    /// pinned or disabled controller, which never adjusts.
    pub adjust_interval_ms: u64,
    /// Multiplier from incumbent to candidate when stepping up, and its
    /// reciprocal when stepping down. Greater than 1.
    pub growth_factor: f64,
    /// Relative throughput gain a candidate must show to unseat the
    /// incumbent. In `0.0..1.0`.
    pub improve_threshold: f64,
    /// Usable samples each arm needs before an epoch may decide anything.
    pub min_samples_per_arm: u32,
    /// Consecutive deciding epochs that leave the incumbent alone before the
    /// controller rests on it. `0` never rests, so the arms alternate for as
    /// long as the source runs. A guard trip, an epoch that moves the
    /// incumbent, or an incumbent missing `target_latency_ms` starts the
    /// count again.
    pub settle_after_epochs: u32,
    /// Divisor applied to the target on a guard trip. Greater than 1.
    pub backoff_factor: f64,
    /// Passes held at the reduced size after a back-off before the search
    /// resumes. `0` resumes immediately.
    pub backoff_cooldown: u32,
    /// Pinned size. `Some(n)` holds the target at `n` and disables adaptation.
    pub fixed_rows: Option<usize>,
    /// Whether the runner applies an admission credit at all. `false` restores
    /// drain-to-EOF iterations and unconditional run-mode pacing.
    pub enabled: bool,
}

impl Default for FlowSettings {
    /// The batch-mode policy: throughput only, with no latency objective.
    ///
    /// This is what a programmatically assembled batch service gets. A stream
    /// service promises something else and carries a latency objective, so its
    /// policy lives in [`stream_defaults`](Self::stream_defaults) and reaches a
    /// direct [`run_stream`](super::stream::run_stream) call through
    /// [`FlowPlan::stream_default`].
    ///
    /// `start_rows` sits in the flat band of the per-row cost curve and
    /// `min_rows` two halvings below it. Per-row cost falls steeply until a few
    /// thousand rows and is flat from roughly four thousand to sixteen
    /// thousand, on both the native pipeline (`batch_vs_stream`) and the Arrow
    /// IPC round trip a WASM or plugin processor pays (`ipc_checkpoint`); a few
    /// hundred rows costs several times the per-row work of a few thousand. A
    /// cold start holds for a whole epoch, and for the entire life of a job
    /// shorter than one, so it begins where the curve is flat rather than on
    /// its slope.
    fn default() -> Self {
        Self {
            min_rows: 1_024,
            max_rows: 65_536,
            start_rows: 4_096,
            max_chunk_bytes: 8 * 1024 * 1024,
            target_latency_ms: 0,
            adjust_interval_ms: 60_000,
            growth_factor: 2.0,
            improve_threshold: 0.05,
            min_samples_per_arm: 4,
            settle_after_epochs: 3,
            backoff_factor: 2.0,
            backoff_cooldown: 4,
            fixed_rows: None,
            enabled: true,
        }
    }
}

impl FlowSettings {
    /// The stream-mode policy: the batch defaults plus the 250 ms per-pass
    /// latency objective.
    pub fn stream_defaults() -> Self {
        Self {
            target_latency_ms: STREAM_LATENCY_OBJECTIVE_MS,
            ..Self::default()
        }
    }
}

/// Whether the pass itself succeeded.
///
/// Sink backlog is reported as a number in [`FlowSample::sink_pending`], not as
/// an outcome: whether a given backlog is pressure is policy, and policy lives
/// in the controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowOutcome {
    /// The pass completed with no error.
    Ok,
    /// A processor or sink reported an error during the pass.
    Error,
}

/// One observation of a completed pass, fed back to [`FlowController::observe`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowSample {
    /// Rows admitted and consumed in the pass.
    pub rows: u64,
    /// Time the consumer chain spent on this pass. Never includes run-mode
    /// pacing or the wait for input.
    pub elapsed: Duration,
    /// Arrow memory of the admitted rows, as the proportional estimate
    /// `parent_bytes * admitted_rows / parent_rows`.
    ///
    /// Neither of the two directly available numbers is the answer: a
    /// zero-copy slice reports its parent's whole buffers, and the parent's
    /// own size covers rows this pass did not admit. The runner computes the
    /// proportion at the call site, where both counts are in hand.
    pub bytes: u64,
    /// Largest `pending_rows` reported by a sink this pass wrote, or `None`
    /// when no sink reports a backlog.
    pub sink_pending: Option<u64>,
    /// Whether the pass succeeded.
    pub outcome: FlowOutcome,
    /// When the pass finished. Epoch boundaries are derived from this, so the
    /// controller reads no clock of its own and a test can feed synthetic
    /// instants.
    pub at: Instant,
}

/// Why the controller moved, or held, the target.
///
/// Carried by every [`FlowAdjustment`] that decided something, so a runner can
/// report the evidence without re-deriving it from the controller's private
/// state. A cause carries numbers only: the controller reads no clock and
/// knows no node ids, so naming the source and stamping the time stays with
/// the runner.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FlowCause {
    /// The epoch's two arms were compared on rows per second and one won.
    Experiment {
        /// Rate of the arm that won.
        winner_rows_per_second: f64,
        /// Rate of the arm it unseated.
        loser_rows_per_second: f64,
    },
    /// The size in effect missed `target_latency_ms`, so the smaller arm won
    /// whatever it moved.
    LatencyObjective {
        /// Mean pass of the arm that was in effect, in milliseconds.
        mean_pass_ms: u64,
        /// The objective it missed, in milliseconds.
        objective_ms: u64,
    },
    /// The pass reported [`FlowOutcome::Error`].
    PassError,
    /// One chunk at the size under test projects past `max_chunk_bytes`.
    ChunkBytes {
        /// Arrow memory the next chunk is projected to weigh.
        projected_bytes: u64,
        /// The ceiling it passed.
        max_chunk_bytes: u64,
    },
    /// A sink's backlog grew on `PRESSURE_TREND_PASSES` consecutive passes.
    SinkBacklog {
        /// The newest backlog, in rows.
        pending_rows: u64,
    },
}

/// One decision's before and after, and the evidence behind it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FlowMove {
    /// Admission target before the decision, in rows.
    pub from_rows: usize,
    /// Admission target after it. Equal to `from_rows` for
    /// [`FlowAdjustment::HeldAtFloor`], which has nothing left to divide.
    pub to_rows: usize,
    /// What the decision was made on.
    pub cause: FlowCause,
}

/// What one [`FlowController::observe`] call did to the target.
///
/// Every variant but [`Held`](Self::Held) carries the decision, because those
/// are the passes an operator needs to see: the target moved, or a guard held
/// it against a wall.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FlowAdjustment {
    /// An epoch closed and committed a larger size.
    Grew(FlowMove),
    /// Nothing moved: mid-epoch, a candidate that lost, an epoch with too
    /// little evidence to decide, or an epoch that rested on a settled size.
    Held,
    /// An epoch closed and committed a smaller size.
    Shrank(FlowMove),
    /// A failure or a growing sink backlog divided the target immediately.
    BackedOff(FlowMove),
    /// A guard tripped at `min_rows`: the target held, because there is
    /// nothing left to divide.
    ///
    /// Distinct from [`Held`](Self::Held) because it is still adverse
    /// evidence, and a source stuck against a wall at the floor is exactly
    /// what an operator needs `saci_flow_backoff_total` to show. The policy is
    /// the same as `Held`: the target stays and the arms survive. The settle
    /// streak does not: a guard at the floor is adverse evidence, so a rest
    /// in progress ends and the search measures a candidate again.
    HeldAtFloor(FlowMove),
}

/// Which arm of the epoch's experiment a pass belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arm {
    Incumbent,
    Candidate,
}

/// Whether the controller is measuring a candidate or resting on a settled
/// incumbent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Experiment,
    /// Incumbent-only epochs still to run before the search resumes.
    Rest {
        remaining: u32,
    },
}

/// Which way the next candidate steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Up,
    Down,
}

impl Direction {
    fn flipped(self) -> Self {
        match self {
            Direction::Up => Direction::Down,
            Direction::Down => Direction::Up,
        }
    }
}

/// Rows, consumer time and sample count accumulated for one arm of an epoch.
#[derive(Debug, Default, Clone, Copy)]
struct ArmStats {
    rows: u64,
    elapsed: Duration,
    samples: u32,
}

impl ArmStats {
    fn add(&mut self, rows: u64, elapsed: Duration) {
        self.rows += rows;
        self.elapsed += elapsed;
        self.samples += 1;
    }

    /// Rows per second over the arm's passes, or `0.0` with no measured time.
    fn rate(&self) -> f64 {
        let seconds = self.elapsed.as_secs_f64();
        if seconds <= 0.0 {
            0.0
        } else {
            self.rows as f64 / seconds
        }
    }

    /// Mean time one pass of this arm took.
    fn mean_pass(&self) -> Duration {
        if self.samples == 0 {
            Duration::ZERO
        } else {
            self.elapsed / self.samples
        }
    }
}

/// Adaptive admission control for one source node.
///
/// Construct one per source, read [`target_rows`](Self::target_rows) before
/// each pass, and feed the result of that pass back through
/// [`observe`](Self::observe).
#[derive(Debug)]
pub struct FlowController {
    settings: FlowSettings,
    /// The size the epoch is defending, and the size it is testing.
    incumbent: usize,
    candidate: usize,
    /// Which arm the next pass runs at.
    arm: Arm,
    direction: Direction,
    /// Whether the candidate arm is scheduled at all, and how many
    /// incumbent-only epochs are left when it is not.
    phase: Phase,
    /// Consecutive deciding epochs that left the incumbent where it was.
    ///
    /// An epoch that moves the incumbent, and any guard trip, resets it. So
    /// does an epoch whose incumbent missed `target_latency_ms`: a size that
    /// is not meeting its objective is not a settled system, whichever arm
    /// won. An epoch with too little evidence measured nothing either way and
    /// leaves it alone.
    settled: u32,
    incumbent_stats: ArmStats,
    candidate_stats: ArmStats,
    /// Completion instant of the epoch's first measured pass; `None` between
    /// epochs.
    epoch_start: Option<Instant>,
    /// Smoothed rate, reported to metrics. No decision reads it.
    throughput: f64,
    /// The size that last failed, while it is still believed to be a wall.
    ceiling: Option<usize>,
    /// Last sink backlog seen, and how many consecutive passes it grew on.
    last_sink_pending: Option<u64>,
    pending_rises: u32,
    /// Set once the latency objective is breached at `min_rows`: no size the
    /// controller can choose meets it, so the objective is abandoned.
    latency_abandoned: bool,
    /// Transition of `latency_abandoned` on the last `observe`, for the runner
    /// to log. Taken by [`take_latency_notice`](Self::take_latency_notice).
    latency_notice: Option<bool>,
    cooldown: u32,
}

impl FlowController {
    /// A controller starting from `settings.start_rows`, or from
    /// `settings.fixed_rows` when one is pinned.
    ///
    /// A pinned size is taken verbatim rather than clamped: a config that pins
    /// `rows` may not also declare `min_rows`/`max_rows`/`start_rows`, so
    /// there is no operator-declared range to clamp it into.
    pub fn new(settings: FlowSettings) -> Self {
        let start = match settings.fixed_rows {
            Some(rows) => rows.max(1),
            None => clamp_rows(settings.start_rows, &settings),
        };
        let mut controller = Self {
            settings,
            incumbent: start,
            candidate: start,
            arm: Arm::Incumbent,
            direction: Direction::Up,
            phase: Phase::Experiment,
            settled: 0,
            incumbent_stats: ArmStats::default(),
            candidate_stats: ArmStats::default(),
            epoch_start: None,
            throughput: 0.0,
            ceiling: None,
            last_sink_pending: None,
            pending_rises: 0,
            latency_abandoned: false,
            latency_notice: None,
            cooldown: 0,
        };
        controller.propose_candidate();
        controller
    }

    /// Rows the runner may admit for the next pass: the arm currently under
    /// test, which is the incumbent on every pass of a rest epoch. Never
    /// zero.
    pub fn target_rows(&self) -> usize {
        if self.cooldown > 0 {
            return self.incumbent;
        }
        match self.arm {
            Arm::Incumbent => self.incumbent,
            Arm::Candidate => self.candidate,
        }
    }

    /// The size this controller is defending: the winner of the last epoch
    /// that decided anything.
    pub fn incumbent_rows(&self) -> usize {
        self.incumbent
    }

    /// Whether the runner applies an admission credit to this source.
    ///
    /// `false` means drain to EOF and pace unconditionally, exactly as a
    /// runner with no flow control at all.
    pub fn enabled(&self) -> bool {
        self.settings.enabled
    }

    /// Smoothed throughput in rows per second, or `0.0` before the first
    /// non-idle sample.
    pub fn throughput(&self) -> f64 {
        self.throughput
    }

    /// Whether the latency objective is currently abandoned as unachievable.
    pub fn latency_objective_abandoned(&self) -> bool {
        self.latency_abandoned
    }

    /// The change of [`latency_objective_abandoned`](Self::latency_objective_abandoned)
    /// caused by the last [`observe`](Self::observe), or `None` when it did not
    /// change. Consumed by the call, so the runner logs each transition once.
    pub fn take_latency_notice(&mut self) -> Option<bool> {
        self.latency_notice.take()
    }

    /// Fold one completed pass into the adaptation state.
    ///
    /// A sample with no rows or no measurable elapsed time is not evidence
    /// about capacity and moves nothing, though a failure or a growing sink
    /// backlog in such a pass still counts. A disabled or pinned controller
    /// keeps its throughput bookkeeping and leaves the target alone.
    pub fn observe(&mut self, sample: FlowSample) -> FlowAdjustment {
        self.latency_notice = None;
        let seconds = sample.elapsed.as_secs_f64();
        let measured = if sample.rows == 0 || seconds <= 0.0 {
            None
        } else {
            Some(sample.rows as f64 / seconds)
        };
        if let Some(rate) = measured {
            self.throughput = if self.throughput == 0.0 {
                rate
            } else {
                EWMA_ALPHA * rate + (1.0 - EWMA_ALPHA) * self.throughput
            };
        }

        if !self.is_adaptive() {
            return FlowAdjustment::Held;
        }

        // Tracked on every adaptive sample, so "grew on two consecutive
        // passes" means consecutive passes and not consecutive judgements.
        let pressure = self.track_sink_pressure(sample.sink_pending);
        // Which guard is named when several trip on one pass follows the order
        // they are evaluated in: an error is the strongest evidence a pass can
        // carry, and a backlog trend is weaker than a hard byte bound only
        // once it has already grown twice.
        let guard = if sample.outcome == FlowOutcome::Error {
            Some(FlowCause::PassError)
        } else if pressure {
            Some(FlowCause::SinkBacklog {
                pending_rows: sample.sink_pending.unwrap_or_default(),
            })
        } else {
            self.chunk_bytes_over_ceiling(&sample)
                .map(|projected_bytes| FlowCause::ChunkBytes {
                    projected_bytes,
                    max_chunk_bytes: self.settings.max_chunk_bytes,
                })
        };

        // The cooldown comes first: adverse evidence while cooling down
        // extends the cooldown rather than halving a second time.
        if self.cooldown > 0 {
            self.cooldown -= 1;
            if guard.is_some() {
                self.cooldown = self.settings.backoff_cooldown;
            }
            return FlowAdjustment::Held;
        }

        // Safety is immediate: a failure acts on the pass that saw it rather
        // than waiting up to a whole epoch.
        if let Some(cause) = guard {
            return self.back_off(cause);
        }

        self.track_latency_objective(&sample);

        if measured.is_none() {
            return FlowAdjustment::Held;
        }

        let arm = self.arm;
        match arm {
            Arm::Incumbent => self.incumbent_stats.add(sample.rows, sample.elapsed),
            Arm::Candidate => self.candidate_stats.add(sample.rows, sample.elapsed),
        }
        let epoch_start = *self.epoch_start.get_or_insert(sample.at);
        // Alternate, so load drift falls on both arms instead of one. A rest
        // epoch never schedules the candidate, so the arm stays on the
        // incumbent and every pass is admitted at it.
        if self.phase == Phase::Experiment && self.candidate != self.incumbent {
            self.arm = match arm {
                Arm::Incumbent => Arm::Candidate,
                Arm::Candidate => Arm::Incumbent,
            };
        }
        if sample.at.duration_since(epoch_start)
            < Duration::from_millis(self.settings.adjust_interval_ms.max(1))
        {
            return FlowAdjustment::Held;
        }
        match self.phase {
            Phase::Experiment => self.close_epoch(),
            Phase::Rest { remaining } => self.close_rest_epoch(remaining),
        }
    }

    /// Whether the target moves at all: a disabled or pinned controller only
    /// keeps throughput statistics.
    fn is_adaptive(&self) -> bool {
        self.settings.enabled && self.settings.fixed_rows.is_none()
    }

    /// The configured floor, never below one row.
    fn floor(&self) -> usize {
        self.settings.min_rows.max(1)
    }

    /// Decide the epoch: commit the winning arm, propose the next candidate,
    /// start a fresh epoch, and rest on the incumbent once the search has
    /// stopped moving.
    fn close_epoch(&mut self) -> FlowAdjustment {
        let incumbent = self.incumbent_stats;
        let candidate = self.candidate_stats;
        self.start_epoch();

        // Nowhere to step in either direction, which only a `min_rows ==
        // max_rows` range produces: there is one admissible size, so there is
        // no experiment to run and nothing to decide. Every other dead step
        // has already been turned around by `propose_candidate`.
        if self.candidate == self.incumbent {
            return FlowAdjustment::Held;
        }
        // Too little evidence: measure again rather than committing to noise.
        if incumbent.samples < self.settings.min_samples_per_arm
            || candidate.samples < self.settings.min_samples_per_arm
        {
            return FlowAdjustment::Held;
        }

        let incumbent_breaches = self.breaches_objective(&incumbent);
        let wins = if incumbent_breaches {
            // "Throughput subject to a latency objective": while the size in
            // effect misses the objective, no amount of rows per second
            // satisfies the contract, so a smaller size wins however many
            // fewer rows it moved. Descending ends either at a size that meets
            // the objective or at `min_rows`, where the latch below gives it
            // up as unreachable and this rule turns itself off.
            self.candidate < self.incumbent
        } else {
            // The size in effect meets the objective, so a candidate that
            // would break it cannot win however fast it is, and otherwise
            // rows per second decides.
            !self.breaches_objective(&candidate)
                && candidate.rate() > incumbent.rate() * (1.0 + self.settings.improve_threshold)
        };

        if !wins {
            if incumbent_breaches && self.candidate > self.incumbent {
                // The size in effect misses the objective and the arm just
                // tested was larger still: the answer is not further up,
                // whichever way the flip would have pointed.
                self.direction = Direction::Down;
            } else {
                // The step in this direction did not pay; try the other way.
                self.direction = self.direction.flipped();
            }
            self.settled = if incumbent_breaches {
                0
            } else {
                self.settled.saturating_add(1)
            };
            self.propose_candidate();
            self.rest_if_settled();
            return FlowAdjustment::Held;
        }

        let cause = if incumbent_breaches {
            FlowCause::LatencyObjective {
                mean_pass_ms: incumbent.mean_pass().as_millis() as u64,
                objective_ms: self.settings.target_latency_ms,
            }
        } else {
            FlowCause::Experiment {
                winner_rows_per_second: candidate.rate(),
                loser_rows_per_second: incumbent.rate(),
            }
        };

        let previous = self.incumbent;
        self.incumbent = self.candidate;
        self.settled = 0;
        // A clean win at the size that once failed is the evidence that the
        // wall moved.
        if self
            .ceiling
            .is_some_and(|ceiling| self.incumbent >= ceiling)
        {
            self.ceiling = None;
        }
        self.propose_candidate();
        let moved = FlowMove {
            from_rows: previous,
            to_rows: self.incumbent,
            cause,
        };
        if self.incumbent > previous {
            FlowAdjustment::Grew(moved)
        } else {
            FlowAdjustment::Shrank(moved)
        }
    }

    /// Rest on the incumbent once `settle_after_epochs` consecutive epochs
    /// have decided nothing.
    ///
    /// The run doubles with every further settled epoch and stops at
    /// [`MAX_REST_EPOCHS`], so a source that stays converged reaches one
    /// experiment epoch per seventeen: the probe cost of a settled search
    /// falls from about half of its passes to about 3% of them, while a
    /// source whose best size is still moving pays nothing.
    ///
    /// Called after [`propose_candidate`](Self::propose_candidate), never
    /// before: a rest keeps the candidate that epoch proposed. Exactly one
    /// path ends a rest without proposing one, the
    /// [`HeldAtFloor`](FlowAdjustment::HeldAtFloor) early return in
    /// [`back_off`](Self::back_off). The kept candidate is what lets that
    /// path resume the experiment. A rest entered
    /// with `candidate == incumbent` would be absorbing instead: the arms
    /// only alternate while they differ, and
    /// [`close_epoch`](Self::close_epoch) returns on that equality before it
    /// can propose another candidate, so nothing would ever schedule a
    /// candidate again.
    fn rest_if_settled(&mut self) {
        let after = self.settings.settle_after_epochs;
        if after == 0 || self.settled < after {
            return;
        }
        let doublings = (self.settled - after).min(MAX_REST_EPOCHS.ilog2());
        let remaining = (1u32 << doublings).min(MAX_REST_EPOCHS);
        self.phase = Phase::Rest { remaining };
    }

    /// Decide a rest epoch: nothing was measured against the incumbent, so
    /// the only question is whether to rest again.
    ///
    /// An incumbent that has started missing `target_latency_ms` ends the
    /// rest at once, because the world changed under the settled size and a
    /// stream workflow's objective cannot wait out [`MAX_REST_EPOCHS`] more
    /// epochs; the search resumes pointing down, so the first candidate it
    /// measures is already the smaller one.
    /// Otherwise the run counts down and the search resumes at zero.
    fn close_rest_epoch(&mut self, remaining: u32) -> FlowAdjustment {
        let incumbent = self.incumbent_stats;
        self.start_epoch();
        let breached = incumbent.samples >= self.settings.min_samples_per_arm
            && self.breaches_objective(&incumbent);
        if breached {
            self.settled = 0;
            // The size in effect misses the objective, so the answer is not
            // further up, whichever way the flip that entered the rest left
            // the search pointing.
            self.direction = Direction::Down;
        }
        if breached || remaining <= 1 {
            self.phase = Phase::Experiment;
            self.propose_candidate();
        } else {
            self.phase = Phase::Rest {
                remaining: remaining - 1,
            };
        }
        FlowAdjustment::Held
    }

    /// Whether one arm's mean pass misses the latency objective.
    ///
    /// Always `false` with no objective configured, and once the objective has
    /// been abandoned as unreachable, which is what makes the abandonment
    /// latch turn the whole rule off rather than only one of its halves.
    fn breaches_objective(&self, arm: &ArmStats) -> bool {
        self.settings.target_latency_ms > 0
            && !self.latency_abandoned
            && arm.mean_pass() > Duration::from_millis(self.settings.target_latency_ms)
    }

    /// Clear both arms and wait for the next measured pass to start the clock.
    fn start_epoch(&mut self) {
        self.incumbent_stats = ArmStats::default();
        self.candidate_stats = ArmStats::default();
        self.epoch_start = None;
        self.arm = Arm::Incumbent;
    }

    /// Set the candidate one step from the incumbent, in the current
    /// direction.
    ///
    /// A step that cannot move, into a bound or into the congestion ceiling, is
    /// turned around rather than left equal to the incumbent: the arms only
    /// alternate while they differ, so a candidate equal to the incumbent is
    /// an experiment that can never run, and nothing else would ever propose
    /// another one. `min_rows == max_rows` is the one range with nowhere to
    /// step either way, and it has a single admissible size by construction.
    fn propose_candidate(&mut self) {
        self.candidate = self.step_from_incumbent();
        if self.candidate == self.incumbent {
            self.direction = self.direction.flipped();
            self.candidate = self.step_from_incumbent();
        }
        self.arm = Arm::Incumbent;
    }

    /// One step from the incumbent in the current direction, clamped into the
    /// configured range. Multiplicative in open water, additive by a fraction
    /// of a congestion ceiling while one stands, and never past it.
    fn step_from_incumbent(&self) -> usize {
        let stepped = match self.direction {
            Direction::Up => match self.ceiling {
                Some(ceiling) => {
                    let increment = (ceiling / CEILING_INCREMENT_DIVISOR).max(1);
                    self.incumbent.saturating_add(increment).min(ceiling)
                }
                None => (self.incumbent as f64 * self.settings.growth_factor).round() as usize,
            },
            Direction::Down => match self.ceiling {
                Some(ceiling) => {
                    let decrement = (ceiling / CEILING_INCREMENT_DIVISOR).max(1);
                    self.incumbent.saturating_sub(decrement)
                }
                None => (self.incumbent as f64 / self.settings.growth_factor).round() as usize,
            },
        };
        clamp_rows(stepped.max(1), &self.settings)
    }

    /// The projected Arrow weight of the next chunk when it passes
    /// `max_chunk_bytes`, or `None` when it does not.
    ///
    /// Returns the projection rather than a flag so a back-off can name the
    /// number that tripped it.
    fn chunk_bytes_over_ceiling(&self, sample: &FlowSample) -> Option<u64> {
        if self.settings.max_chunk_bytes == 0 || sample.rows == 0 || sample.bytes == 0 {
            return None;
        }
        let bytes_per_row = sample.bytes as f64 / sample.rows as f64;
        let projected = bytes_per_row * self.target_rows() as f64;
        (projected > self.settings.max_chunk_bytes as f64).then_some(projected as u64)
    }

    /// Fold this pass's sink backlog into the trend, returning whether the
    /// backlog has now grown on [`PRESSURE_TREND_PASSES`] consecutive passes.
    ///
    /// An absolute backlog says nothing: `pending_rows` is measured against
    /// each sink's own flush policy, so a healthy buffering sink reports a
    /// large steady number. A backlog that only grows is a consumer losing
    /// ground whatever the number is.
    fn track_sink_pressure(&mut self, pending: Option<u64>) -> bool {
        let Some(pending) = pending else {
            self.last_sink_pending = None;
            self.pending_rises = 0;
            return false;
        };
        match self.last_sink_pending {
            Some(previous) if pending > previous => self.pending_rises += 1,
            _ => self.pending_rises = 0,
        }
        self.last_sink_pending = Some(pending);
        self.pending_rises >= PRESSURE_TREND_PASSES
    }

    /// Maintain the abandonment latch: a breach at the floor proves the
    /// objective is unreachable at every size the controller can pick, and a
    /// pass at the floor that meets it proves the opposite.
    fn track_latency_objective(&mut self, sample: &FlowSample) {
        if self.settings.target_latency_ms == 0
            || sample.rows == 0
            || self.target_rows() > self.floor()
        {
            return;
        }
        let breached = sample.elapsed > Duration::from_millis(self.settings.target_latency_ms);
        if breached != self.latency_abandoned {
            self.latency_abandoned = breached;
            self.latency_notice = Some(breached);
        }
    }

    /// Divide the size in effect and remember what failed as a congestion
    /// ceiling, so the climb back is additive instead of walking straight into
    /// the same wall. The epoch is abandoned.
    ///
    /// The arm under test may be the *candidate*, and a candidate stepping
    /// down says nothing about the incumbent it was measured against: taking
    /// the larger of the two is what stops one bad pass at a conservative size
    /// discarding a size that won an epoch and never failed, and stops the
    /// ceiling landing below it.
    ///
    /// At the floor there is nothing to divide: the target holds and the
    /// search state survives, because a chronically true guard would otherwise
    /// wipe it on every pass and pin the controller at `min_rows`, which is
    /// the worst throughput point for a pipeline with fixed per-batch
    /// overhead. That is still adverse evidence, so it is
    /// [`HeldAtFloor`](FlowAdjustment::HeldAtFloor) rather than
    /// [`Held`](FlowAdjustment::Held) and a runner still counts it.
    ///
    /// Adverse evidence means the system is not settled, whichever branch
    /// follows: the streak goes back to zero and a rest in progress ends, so
    /// the controller measures a candidate again rather than holding a size
    /// a guard has just complained about.
    fn back_off(&mut self, cause: FlowCause) -> FlowAdjustment {
        // The size that failed, not the arm that observed it, is the
        // meaningful "before": a candidate stepping down reports a smaller
        // target than the one being divided, and a decision reading
        // "500 rows to 500 rows" would describe no move at all.
        let failed = self.target_rows().max(self.incumbent);
        let previous_incumbent = self.incumbent;
        self.settled = 0;
        self.phase = Phase::Experiment;
        if failed <= self.floor() {
            return FlowAdjustment::HeldAtFloor(FlowMove {
                from_rows: failed,
                to_rows: failed,
                cause,
            });
        }
        self.ceiling = Some(match self.ceiling {
            Some(ceiling) => ceiling.min(failed),
            None => failed,
        });
        let reduced = (failed as f64 / self.settings.backoff_factor).floor() as usize;
        self.incumbent = clamp_rows(reduced.max(1), &self.settings);
        // A ceiling stands, so the next experiment asks whether even smaller
        // is faster before climbing back toward it.
        self.direction = Direction::Down;
        self.propose_candidate();
        self.start_epoch();
        self.cooldown = self.settings.backoff_cooldown;
        self.last_sink_pending = None;
        self.pending_rises = 0;
        let moved = FlowMove {
            from_rows: failed,
            to_rows: self.incumbent,
            cause,
        };
        // A candidate above the incumbent that fails, once halved and
        // clamped, can land back on the incumbent already in effect *when
        // that incumbent is already pinned at the floor*: nothing was
        // divided, and reporting `BackedOff` here would be the same no-op
        // state two different decision kinds depending on which arm
        // happened to trip it, and that is exactly what defeats
        // `FlowEpisode`'s `(kind, to_rows, cause)` dedup and turns one
        // chronic congestion into a `BackedOff`/`HeldAtFloor` picket fence
        // that never collapses. Gated on the floor: with no ceiling yet,
        // `growth_factor`/`backoff_factor` default to exact inverses, so a
        // candidate trip away from the floor (`candidate = 2 * incumbent`,
        // `reduced = failed / 2 = incumbent`) *also* clamps back to the
        // unchanged incumbent, and that is a real, still-searching
        // rejection rather than a wall the search cannot leave, so it must
        // stay `BackedOff`.
        if self.incumbent == previous_incumbent && self.incumbent <= self.floor() {
            FlowAdjustment::HeldAtFloor(moved)
        } else {
            FlowAdjustment::BackedOff(moved)
        }
    }
}

/// Clamp `rows` into `[min_rows, max_rows]`, never returning zero.
///
/// `max_rows >= min_rows >= 1` is a validated configuration invariant; the
/// `max` guards a controller built directly by an embedder.
fn clamp_rows(rows: usize, settings: &FlowSettings) -> usize {
    let min = settings.min_rows.max(1);
    let max = settings.max_rows.max(min);
    rows.clamp(min, max)
}

/// One source's unconsumed tail, and the Arrow weight of the arrival it was
/// cut from.
///
/// `bytes_per_row` is measured once, on the whole arrival, and carried with
/// the tail rather than recomputed from it. `RecordBatch::slice` is zero-copy,
/// so a slice reports its *parent's* buffers from
/// `get_array_memory_size`: dividing those by a shrinking tail inflates the
/// estimate on every later chunk, up to the whole arrival's weight for the
/// last one, which would project it past `max_chunk_bytes` and trip
/// [`chunk_bytes_over_ceiling`](FlowController::chunk_bytes_over_ceiling) on every
/// arrival larger than the ceiling, exactly the arrivals flow control exists
/// to cut up.
///
/// Both runners re-chunk and both weigh what they admit, so this is one type
/// rather than one per runner: the two cannot drift apart again.
#[derive(Debug)]
pub(super) struct Carry {
    /// Rows admitted before anything new is pulled from that source.
    pub(super) batch: RecordBatch,
    /// Arrow memory of one row of the arrival this tail came from.
    pub(super) bytes_per_row: f64,
}

impl Carry {
    /// Arrow memory of one row of `batch`, or `0.0` for an empty batch.
    ///
    /// Call it on a whole arrival, never on a chunk of one; see the type's own
    /// documentation for what a slice reports.
    pub(super) fn weigh(batch: &RecordBatch) -> f64 {
        let rows = batch.num_rows();
        if rows == 0 {
            0.0
        } else {
            batch.get_array_memory_size() as f64 / rows as f64
        }
    }
}

/// Resolved flow-control settings for every source node a config declares.
///
/// The runners take one of these instead of the whole [`ServiceConfig`], so a
/// programmatically assembled service gets the on-by-default policy from
/// [`Default`] with nothing to configure.
#[derive(Debug, Clone, Default)]
pub struct FlowPlan {
    fallback: FlowSettings,
    by_source: HashMap<String, FlowSettings>,
}

impl FlowPlan {
    /// One policy for every source, whatever its id. Test-only: the one caller
    /// is a runner test that needs settings a [`ServiceConfig`] cannot easily
    /// express, and a constructor belongs in the public surface only once an
    /// embedder asks for one.
    #[cfg(test)]
    pub fn uniform(settings: FlowSettings) -> Self {
        Self {
            fallback: settings,
            by_source: HashMap::new(),
        }
    }

    /// Layer each declared source's `flow_control` block over the top-level
    /// one, for every workflow in `config`.
    ///
    /// The run mode picks which hard defaults the layering ends at: stream
    /// mode carries a latency objective, the batch modes do not. Cluster mode
    /// builds no plan at all: it declares no source node, and
    /// [`ServiceConfig::validate`](super::config::ServiceConfig::validate)
    /// refuses a `flow_control` block there rather than resolving one that
    /// nothing would read.
    pub fn from_config(config: &ServiceConfig) -> Self {
        let defaults = match &config.mode {
            ServiceMode::Standalone { config: standalone }
                if standalone.run_mode == RunMode::Stream =>
            {
                FlowSettings::stream_defaults()
            }
            _ => FlowSettings::default(),
        };
        let fallback = config.flow_control.resolve(&Default::default(), defaults);
        let mut by_source = HashMap::new();
        for workflow in &config.workflows {
            for source in &workflow.sources {
                let settings = match &source.flow_control {
                    Some(local) => local.resolve(&config.flow_control, defaults),
                    None => fallback,
                };
                by_source.insert(source.id.clone(), settings);
            }
        }
        Self {
            fallback,
            by_source,
        }
    }

    /// The on-by-default policy for a stream service assembled in code:
    /// [`FlowSettings::stream_defaults`] for every source.
    ///
    /// [`Default`] is the batch policy, which carries no latency objective, so
    /// it is the wrong plan to hand [`run_stream`](super::stream::run_stream).
    /// The run mode is not recoverable from a [`BuiltService`](super::builder::BuiltService),
    /// so the caller names the policy instead: this constructor for a direct
    /// call, [`from_config`](Self::from_config) for a config-driven one, which
    /// resolves the same defaults from `run_mode`.
    pub fn stream_default() -> Self {
        Self {
            fallback: FlowSettings::stream_defaults(),
            by_source: HashMap::new(),
        }
    }

    /// The settings for source node `id`, or the top-level ones when the id is
    /// not a declared source.
    pub fn for_source(&self, id: &str) -> FlowSettings {
        self.by_source.get(id).copied().unwrap_or(self.fallback)
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;

    /// Which decision an adjustment is, with the numbers it carries dropped.
    ///
    /// Most tests here are about which way the controller moved, not by how
    /// much; the ones that are about the numbers read the [`FlowMove`]
    /// itself.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Decided {
        Grew,
        Held,
        Shrank,
        BackedOff,
        HeldAtFloor,
    }

    fn decided(adjustment: FlowAdjustment) -> Decided {
        match adjustment {
            FlowAdjustment::Grew(_) => Decided::Grew,
            FlowAdjustment::Held => Decided::Held,
            FlowAdjustment::Shrank(_) => Decided::Shrank,
            FlowAdjustment::BackedOff(_) => Decided::BackedOff,
            FlowAdjustment::HeldAtFloor(_) => Decided::HeldAtFloor,
        }
    }

    /// A one-minute epoch over a wide range, with no latency or byte
    /// objective, so a test drives one mechanism at a time.
    fn unbounded() -> FlowSettings {
        FlowSettings {
            min_rows: 100,
            max_rows: 100_000,
            start_rows: 1_000,
            max_chunk_bytes: 0,
            target_latency_ms: 0,
            ..FlowSettings::default()
        }
    }

    /// A driver that feeds passes with synthetic instants, so an epoch closes
    /// exactly when the test says it does.
    struct Driver {
        flow: FlowController,
        base: Instant,
        offset: Duration,
        /// Consumer time one pass takes at each arm size, as rows per second.
        rate_for: fn(usize) -> f64,
        last: FlowAdjustment,
        adjustments: Vec<FlowAdjustment>,
        targets: Vec<usize>,
        /// Latency-objective transitions, taken pass by pass exactly as a
        /// runner takes them, so a test can see a notice that a later pass
        /// would have cleared.
        notices: Vec<bool>,
    }

    impl Driver {
        fn new(settings: FlowSettings, rate_for: fn(usize) -> f64) -> Self {
            Self {
                flow: FlowController::new(settings),
                base: Instant::now(),
                offset: Duration::ZERO,
                rate_for,
                last: FlowAdjustment::Held,
                adjustments: Vec::new(),
                targets: Vec::new(),
                notices: Vec::new(),
            }
        }

        /// One pass at the controller's current target, advancing the clock by
        /// `gap` plus the pass's own consumer time.
        fn pass(&mut self, gap: Duration) -> FlowAdjustment {
            let target = self.flow.target_rows();
            let rows = target as u64;
            let seconds = rows as f64 / (self.rate_for)(target);
            let elapsed = Duration::from_secs_f64(seconds);
            self.offset += gap + elapsed;
            let sample = FlowSample {
                rows,
                elapsed,
                bytes: rows * 8,
                sink_pending: None,
                outcome: FlowOutcome::Ok,
                at: self.base + self.offset,
            };
            self.targets.push(target);
            self.last = self.flow.observe(sample);
            self.adjustments.push(self.last);
            if let Some(abandoned) = self.flow.take_latency_notice() {
                self.notices.push(abandoned);
            }
            self.last
        }

        /// `count` passes spread evenly over `window`.
        fn passes(&mut self, count: u32, window: Duration) -> FlowAdjustment {
            let gap = window / count;
            for _ in 0..count {
                self.pass(gap);
            }
            self.last
        }

        /// `samples` passes well inside the epoch, then one that crosses the
        /// boundary. The returned adjustment is the epoch's decision, so a
        /// test never has to guess which pass closed it.
        fn close_epoch(&mut self, samples: u32) -> FlowAdjustment {
            self.passes(samples, Duration::from_millis(1_000));
            self.pass(Duration::from_millis(
                self.flow.settings.adjust_interval_ms + 1,
            ))
        }

        fn target(&self) -> usize {
            self.flow.target_rows()
        }

        fn incumbent(&self) -> usize {
            self.flow.incumbent_rows()
        }
    }

    /// Bigger chunks are faster, the shape of a pipeline whose per-pass
    /// overhead dominates: rate rises with size and saturates.
    fn overhead_bound(rows: usize) -> f64 {
        // One pass costs 10 ms of fixed overhead plus 1 µs per row.
        let seconds = 0.010 + rows as f64 * 0.000_001;
        rows as f64 / seconds
    }

    /// Every size performs identically: no experiment can win.
    fn flat(_rows: usize) -> f64 {
        100_000.0
    }

    /// Smaller chunks are faster, the shape of a pipeline that falls out of
    /// cache: rate decays with size.
    fn cache_bound(rows: usize) -> f64 {
        1_000_000.0 / (1.0 + rows as f64 / 1_000.0)
    }

    #[test]
    fn a_faster_candidate_is_committed_at_epoch_close_and_not_before() {
        let mut driver = Driver::new(unbounded(), overhead_bound);
        assert_eq!(driver.target(), 1_000, "the first pass runs the incumbent");

        // Well inside the epoch: plenty of evidence, no decision.
        driver.passes(20, Duration::from_millis(30_000));
        assert!(
            driver
                .adjustments
                .iter()
                .all(|a| *a == FlowAdjustment::Held),
            "no adjustment may land inside an epoch: {:?}",
            driver.adjustments
        );
        assert_eq!(driver.incumbent(), 1_000);

        // Crossing the boundary closes the epoch and commits the winner.
        assert_eq!(
            decided(driver.pass(Duration::from_millis(45_000))),
            Decided::Grew
        );
        assert_eq!(driver.incumbent(), 2_000);
    }

    #[test]
    fn the_arms_alternate_pass_by_pass() {
        let mut driver = Driver::new(unbounded(), overhead_bound);
        driver.passes(8, Duration::from_millis(1_000));
        assert_eq!(
            driver.targets,
            vec![1_000, 2_000, 1_000, 2_000, 1_000, 2_000, 1_000, 2_000],
            "a block design would attribute load drift to one arm"
        );
    }

    /// The arms alternate pass by pass precisely so a workload that changes
    /// mid-epoch cannot decide the experiment. Throughput that halves halfway
    /// through an epoch falls on both arms equally, so the epoch still commits
    /// the arm that is genuinely faster.
    #[test]
    fn an_epoch_whose_throughput_halves_midway_still_commits_the_faster_arm() {
        // Twelve passes, the twelfth closing the epoch: strict alternation
        // gives each arm six, and switching regime at the halfway pass gives
        // each arm three fast passes and three slow ones.
        const PASSES: u32 = 12;

        let settings = unbounded();
        let mut flow = FlowController::new(settings);
        let base = Instant::now();
        let mut offset = Duration::ZERO;
        let mut adjustments = Vec::new();
        let mut sizes = Vec::new();

        for pass in 1..=PASSES {
            let target = flow.target_rows();
            let rows = target as u64;
            // The second half of the epoch runs at half the throughput of the
            // first, at every size: machine load, not chunk size.
            let factor = if pass * 2 <= PASSES { 1.0 } else { 0.5 };
            let elapsed = Duration::from_secs_f64(rows as f64 / (overhead_bound(target) * factor));
            // Every pass but the last sits well inside the epoch.
            offset += elapsed
                + if pass == PASSES {
                    Duration::from_millis(settings.adjust_interval_ms + 1)
                } else {
                    Duration::from_millis(1_000)
                };
            sizes.push(target);
            adjustments.push(flow.observe(FlowSample {
                rows,
                elapsed,
                bytes: rows * 8,
                sink_pending: None,
                outcome: FlowOutcome::Ok,
                at: base + offset,
            }));
        }

        assert_eq!(
            sizes,
            vec![
                1_000, 2_000, 1_000, 2_000, 1_000, 2_000, 1_000, 2_000, 1_000, 2_000, 1_000, 2_000
            ],
            "each arm must have carried three passes of each regime"
        );
        let (inside, closing) = adjustments.split_at(PASSES as usize - 1);
        assert!(
            inside.iter().all(|a| *a == FlowAdjustment::Held),
            "no decision may land inside the epoch: {adjustments:?}"
        );
        assert_eq!(
            closing.iter().copied().map(decided).collect::<Vec<_>>(),
            [Decided::Grew],
            "the larger arm is faster in both regimes, so it must win"
        );
        assert_eq!(flow.incumbent_rows(), 2_000);

        // The fixture is adversarial, not merely noisy: had the epoch run its
        // arms as blocks and the regime changed between them, the faster arm
        // would have measured slower than the incumbent and lost.
        assert!(
            overhead_bound(2_000) * 0.5 < overhead_bound(1_000),
            "a block-ordered epoch would have mismeasured the faster arm"
        );
    }

    #[test]
    fn a_slower_candidate_is_discarded_and_the_incumbent_survives() {
        let mut driver = Driver::new(unbounded(), cache_bound);
        // Growing hurts this pipeline, so the first epoch's up-candidate loses.
        assert_eq!(driver.close_epoch(16), FlowAdjustment::Held);
        assert_eq!(driver.incumbent(), 1_000, "the incumbent survives a loss");

        // A loss flips the direction, so the next epoch tests a smaller size,
        // which this pipeline does prefer.
        assert_eq!(decided(driver.close_epoch(16)), Decided::Shrank);
        assert_eq!(driver.incumbent(), 500);
    }

    #[test]
    fn an_epoch_with_too_few_samples_in_an_arm_decides_nothing() {
        let mut driver = Driver::new(unbounded(), overhead_bound);
        // One sample short of the per-arm minimum on the candidate arm, even
        // counting the pass that closes the epoch.
        let thin = 2 * unbounded().min_samples_per_arm - 2;
        assert_eq!(driver.close_epoch(thin), FlowAdjustment::Held);
        assert_eq!(driver.incumbent(), 1_000, "one thin epoch decides nothing");

        // The next epoch, with enough evidence, does decide.
        assert_eq!(
            decided(driver.close_epoch(4 * unbounded().min_samples_per_arm)),
            Decided::Grew
        );
        assert_eq!(driver.incumbent(), 2_000);
    }

    #[test]
    fn a_workload_shorter_than_one_epoch_stays_at_start_rows() {
        let mut driver = Driver::new(unbounded(), overhead_bound);
        driver.passes(50, Duration::from_millis(10_000));
        assert_eq!(driver.incumbent(), 1_000);
        assert!(
            !driver
                .adjustments
                .iter()
                .copied()
                .map(decided)
                .any(|a| a == Decided::Grew),
            "a job too short to measure must not adjust: {:?}",
            driver.adjustments
        );
    }

    #[test]
    fn a_flat_pipeline_keeps_its_incumbent_across_epochs() {
        let mut driver = Driver::new(unbounded(), flat);
        for _ in 0..4 {
            driver.close_epoch(16);
        }
        assert_eq!(
            driver.incumbent(),
            1_000,
            "with no measurable difference the hysteresis keeps the incumbent"
        );
    }

    /// Closes one epoch and reports whether it scheduled the candidate arm at
    /// all. A rest epoch admits the incumbent on every pass, so any other
    /// size in the window is the experiment running.
    fn probed(driver: &mut Driver, samples: u32) -> bool {
        let incumbent = driver.incumbent();
        let first = driver.targets.len();
        driver.close_epoch(samples);
        driver.targets[first..]
            .iter()
            .any(|rows| *rows != incumbent)
    }

    #[test]
    fn a_search_that_stops_moving_stops_scheduling_the_candidate() {
        let mut driver = Driver::new(unbounded(), flat);
        for _ in 0..unbounded().settle_after_epochs {
            assert_eq!(driver.close_epoch(16), FlowAdjustment::Held);
        }
        assert!(
            driver.targets.contains(&2_000),
            "the epochs that settled the search must have probed: {:?}",
            driver.targets
        );

        let settled = driver.targets.len();
        assert_eq!(driver.close_epoch(16), FlowAdjustment::Held);
        assert!(
            driver.targets[settled..].iter().all(|rows| *rows == 1_000),
            "every pass of a rest epoch is admitted at the incumbent: {:?}",
            &driver.targets[settled..]
        );
    }

    #[test]
    fn the_rest_run_lengthens_with_each_settled_epoch_and_stops_at_the_cap() {
        let mut driver = Driver::new(unbounded(), flat);
        let mut runs = Vec::new();
        let mut run = 0u32;
        for _ in 0..80 {
            if probed(&mut driver, 16) {
                if run > 0 {
                    runs.push(run);
                }
                run = 0;
            } else {
                run += 1;
            }
        }
        // One, two, four, eight, then the cap, whatever the cap is.
        let expected: Vec<u32> = (0..5).map(|k| (1u32 << k).min(MAX_REST_EPOCHS)).collect();
        assert!(
            runs.len() > expected.len(),
            "eighty epochs must contain more than {} runs: {runs:?}",
            expected.len()
        );
        assert!(
            runs.starts_with(&expected),
            "each further settled epoch doubles the run: {runs:?}"
        );
        assert!(
            runs[expected.len()..]
                .iter()
                .all(|length| *length == MAX_REST_EPOCHS),
            "the run stops growing at MAX_REST_EPOCHS: {runs:?}"
        );
    }

    /// What the reset owns is the *length* of the next rest run. Whether the
    /// epoch after a move probes at all is not evidence: the winning branch
    /// never rests, so that holds however long the streak was.
    #[test]
    fn an_epoch_that_moves_the_incumbent_resets_the_settled_streak() {
        let settings = FlowSettings {
            settle_after_epochs: 1,
            ..unbounded()
        };
        let mut driver = Driver::new(settings, flat);

        // Two settled epochs, each serving out the run it bought: the streak
        // reaches two, so an un-reset streak would be three after the epoch
        // that follows the move, and would buy four rest epochs.
        for _ in 0..5 {
            driver.close_epoch(16);
        }

        // The machine changes shape under the settled size, so the epoch that
        // catches it has something to win with.
        driver.rate_for = overhead_bound;
        assert_eq!(decided(driver.close_epoch(16)), Decided::Grew);

        // Flat again at the committed size, so the next epoch settles and the
        // run it buys is the shortest one: one rest epoch, then the arms
        // alternate again.
        driver.rate_for = flat;
        assert!(
            probed(&mut driver, 16),
            "the epoch after a move alternates its arms: {:?}",
            driver.targets
        );
        assert!(
            !probed(&mut driver, 16),
            "that epoch settled, so this one rests: {:?}",
            driver.targets
        );
        assert!(
            probed(&mut driver, 16),
            "a move resets the streak, so it buys one rest epoch and not the \
             four an un-reset streak of three would: {:?}",
            driver.targets
        );
    }

    #[test]
    fn a_guard_trip_while_resting_resumes_the_experiment() {
        let settings = FlowSettings {
            settle_after_epochs: 1,
            // The search resumes on the pass after the division, so no
            // cooldown stands between the back-off and the candidate arm.
            backoff_cooldown: 0,
            ..unbounded()
        };
        let mut driver = Driver::new(settings, flat);
        assert_eq!(driver.close_epoch(16), FlowAdjustment::Held);

        let resting = driver.targets.len();
        driver.passes(4, Duration::from_millis(1_000));
        assert!(
            driver.targets[resting..].iter().all(|rows| *rows == 1_000),
            "the rest epoch is under way: {:?}",
            &driver.targets[resting..]
        );

        let failing = FlowSample {
            rows: 1_000,
            elapsed: Duration::from_millis(10),
            bytes: 8_000,
            sink_pending: None,
            outcome: FlowOutcome::Error,
            at: driver.base + driver.offset,
        };
        assert_eq!(decided(driver.flow.observe(failing)), Decided::BackedOff);

        let tripped = driver.targets.len();
        driver.passes(4, Duration::from_millis(1_000));
        let incumbent = driver.incumbent();
        assert!(
            driver.targets[tripped..]
                .iter()
                .any(|rows| *rows != incumbent),
            "a guard trip ends the rest on the pass that saw it: {:?}",
            &driver.targets[tripped..]
        );
    }

    /// A guard trip at the floor takes the
    /// [`HeldAtFloor`](FlowAdjustment::HeldAtFloor) early return, which ends
    /// the rest without proposing a candidate, so the experiment can only
    /// resume on the one the epoch that entered the rest left behind.
    #[test]
    fn a_floor_guard_trip_while_resting_resumes_the_experiment() {
        let settings = FlowSettings {
            // The incumbent starts at the floor and a flat pipeline never
            // moves it, so a failing pass has nothing to divide.
            min_rows: 1_000,
            max_rows: 64_000,
            start_rows: 1_000,
            settle_after_epochs: 1,
            ..unbounded()
        };
        let mut driver = Driver::new(settings, flat);
        assert_eq!(driver.close_epoch(16), FlowAdjustment::Held);
        assert_eq!(driver.incumbent(), 1_000, "the search sits on the floor");

        let resting = driver.targets.len();
        driver.passes(4, Duration::from_millis(1_000));
        assert!(
            driver.targets[resting..].iter().all(|rows| *rows == 1_000),
            "the rest epoch is under way: {:?}",
            &driver.targets[resting..]
        );

        let failing = FlowSample {
            rows: 1_000,
            elapsed: Duration::from_millis(10),
            bytes: 8_000,
            sink_pending: None,
            outcome: FlowOutcome::Error,
            at: driver.base + driver.offset,
        };
        assert_eq!(decided(driver.flow.observe(failing)), Decided::HeldAtFloor);

        let tripped = driver.targets.len();
        driver.passes(4, Duration::from_millis(1_000));
        assert!(
            driver.targets[tripped..].iter().any(|rows| *rows > 1_000),
            "a guard at the floor ends the rest and the candidate arm runs \
             again: {:?}",
            &driver.targets[tripped..]
        );
    }

    /// Settles a stream-mode controller into a two-epoch rest run, spends one
    /// rest epoch on passes costing `pass` each, and reports the incumbent it
    /// settled on together with the first target the epoch after that one
    /// scheduled away from it. `None` means that epoch admitted the incumbent
    /// on every pass: the run was served out and no candidate was scheduled.
    ///
    /// The run is deliberately longer than one epoch, so serving it out and
    /// ending it early are distinguishable.
    fn resumes_after_a_rest_epoch_costing(pass: Duration) -> (usize, Option<usize>) {
        let settings = FlowSettings {
            target_latency_ms: 250,
            settle_after_epochs: 1,
            ..unbounded()
        };
        let mut driver = Driver::new(settings, flat);
        // Settle, rest once, settle again: the second settled epoch buys a
        // two-epoch run.
        for _ in 0..3 {
            driver.close_epoch(16);
        }

        let rows = driver.incumbent() as u64;
        for closing in [false; 16].into_iter().chain([true]) {
            let gap = if closing {
                Duration::from_millis(unbounded().adjust_interval_ms + 1)
            } else {
                Duration::from_millis(60)
            };
            driver.offset += gap + pass;
            driver.flow.observe(FlowSample {
                rows,
                elapsed: pass,
                bytes: rows * 8,
                sink_pending: None,
                outcome: FlowOutcome::Ok,
                at: driver.base + driver.offset,
            });
        }
        let incumbent = driver.incumbent();
        let first = driver.targets.len();
        driver.close_epoch(16);
        let candidate = driver.targets[first..]
            .iter()
            .copied()
            .find(|target| *target != incumbent);
        (incumbent, candidate)
    }

    #[test]
    fn an_incumbent_meeting_its_objective_serves_out_the_rest_run() {
        let (_, candidate) = resumes_after_a_rest_epoch_costing(Duration::from_millis(10));
        assert!(
            candidate.is_none(),
            "an incumbent meeting its objective serves the whole rest run \
             out, but a candidate of {candidate:?} ran"
        );
    }

    #[test]
    fn an_incumbent_breaching_the_objective_while_resting_ends_the_rest() {
        let (incumbent, candidate) = resumes_after_a_rest_epoch_costing(Duration::from_millis(400));
        let Some(rows) = candidate else {
            panic!(
                "an incumbent missing target_latency_ms resumes the search at \
                 the next boundary instead of waiting out the run"
            )
        };
        assert!(
            rows < incumbent,
            "an incumbent over its objective has no answer above it, so the \
             resumed search must measure a smaller candidate first, not \
             {rows} against an incumbent of {incumbent}"
        );
    }

    #[test]
    fn settle_after_epochs_zero_probes_for_as_long_as_the_source_runs() {
        let settings = FlowSettings {
            settle_after_epochs: 0,
            ..unbounded()
        };
        let mut driver = Driver::new(settings, flat);
        for epoch in 0..12 {
            assert!(
                probed(&mut driver, 16),
                "epoch {epoch} must still schedule a candidate"
            );
        }
    }

    #[test]
    fn a_committed_win_keeps_climbing_in_the_same_direction() {
        let mut driver = Driver::new(unbounded(), overhead_bound);
        driver.close_epoch(16);
        assert_eq!(driver.incumbent(), 2_000);
        driver.close_epoch(16);
        assert_eq!(
            driver.incumbent(),
            4_000,
            "the next candidate steps further"
        );
    }

    #[test]
    fn no_admitted_size_ever_leaves_the_configured_range() {
        let settings = FlowSettings {
            min_rows: 1_000,
            max_rows: 4_000,
            start_rows: 1_000,
            ..unbounded()
        };
        let mut driver = Driver::new(settings, overhead_bound);
        for _ in 0..8 {
            driver.close_epoch(16);
        }
        // Every size the runner was ever handed, not just the last one: a
        // controller that stopped experimenting would satisfy a check on the
        // final target alone. That it keeps experimenting at the bound is
        // `a_controller_at_max_rows_still_experiments_downward`.
        assert!(
            driver
                .targets
                .iter()
                .all(|rows| (1_000..=4_000).contains(rows)),
            "a target outside the configured range: {:?}",
            driver.targets
        );
    }

    /// The bound is not a dead end: a controller sitting on `max_rows` has
    /// only one direction left to test, and must test it. Otherwise a workload
    /// that changes shape after the climb is never noticed, because no arm but
    /// the incumbent is ever sampled again.
    #[test]
    fn a_controller_at_max_rows_still_experiments_downward() {
        let settings = FlowSettings {
            min_rows: 1_000,
            max_rows: 4_000,
            start_rows: 1_000,
            // Which direction the bound leaves open, not how often it is
            // tested: resting has its own tests.
            settle_after_epochs: 0,
            ..unbounded()
        };
        let mut driver = Driver::new(settings, overhead_bound);
        for _ in 0..8 {
            driver.close_epoch(16);
        }
        assert_eq!(driver.incumbent(), 4_000, "the climb reaches the ceiling");

        let after_climb = driver.targets.len();
        driver.passes(4, Duration::from_millis(1_000));
        assert!(
            driver.targets[after_climb..]
                .iter()
                .any(|rows| *rows < 4_000),
            "a controller at max_rows must still admit a smaller candidate: {:?}",
            &driver.targets[after_climb..]
        );
    }

    /// The mirror image, and the more expensive one: `min_rows` is the worst
    /// throughput point for a pipeline with fixed per-batch overhead, so a
    /// controller that reached it by winning must still be able to leave.
    #[test]
    fn a_controller_at_min_rows_still_experiments_upward() {
        let settings = FlowSettings {
            min_rows: 1_000,
            max_rows: 64_000,
            start_rows: 8_000,
            settle_after_epochs: 0,
            ..unbounded()
        };
        let mut driver = Driver::new(settings, cache_bound);
        for _ in 0..8 {
            driver.close_epoch(16);
        }
        assert_eq!(driver.incumbent(), 1_000, "the search reaches the floor");

        let after_descent = driver.targets.len();
        driver.passes(4, Duration::from_millis(1_000));
        assert!(
            driver.targets[after_descent..]
                .iter()
                .any(|rows| *rows > 1_000),
            "a controller at min_rows must still admit a larger candidate: {:?}",
            &driver.targets[after_descent..]
        );
    }

    /// A back-off whose halving lands exactly on the floor must not cost the
    /// source its search for the rest of the process: one transient failure is
    /// not evidence that `min_rows` is the right size forever.
    #[test]
    fn a_back_off_that_lands_on_min_rows_resumes_the_search() {
        let settings = FlowSettings {
            min_rows: 1_000,
            max_rows: 64_000,
            start_rows: 2_000,
            ..unbounded()
        };
        let mut flow = FlowController::new(settings);
        let base = Instant::now();
        let sample = |outcome, at| FlowSample {
            rows: 2_000,
            elapsed: Duration::from_millis(10),
            bytes: 16_000,
            sink_pending: None,
            outcome,
            at,
        };
        assert_eq!(
            decided(flow.observe(sample(FlowOutcome::Error, base))),
            Decided::BackedOff
        );
        assert_eq!(flow.incumbent_rows(), 1_000, "halved onto the floor");

        let mut targets = Vec::new();
        for pass in 1..=u64::from(settings.backoff_cooldown) + 4 {
            targets.push(flow.target_rows());
            flow.observe(sample(
                FlowOutcome::Ok,
                base + Duration::from_millis(pass * 10),
            ));
        }
        assert!(
            targets.iter().any(|rows| *rows > 1_000),
            "the search must resume once the cooldown expires: {targets:?}"
        );
    }

    /// A controller whose *candidate* arm is a step below its incumbent, which
    /// is what a guard trip must not mistake for the incumbent failing.
    fn tripped_on_a_smaller_candidate() -> FlowController {
        let settings = FlowSettings {
            min_rows: 1_024,
            max_rows: 65_536,
            start_rows: 4_096,
            ..unbounded()
        };
        // Growing hurts this pipeline, so the first epoch's up-candidate loses
        // and the search turns around: incumbent 4 096, candidate 2 048.
        let mut driver = Driver::new(settings, cache_bound);
        assert_eq!(driver.close_epoch(16), FlowAdjustment::Held);
        assert_eq!(driver.incumbent(), 4_096);
        driver.pass(Duration::from_millis(10));
        assert_eq!(driver.target(), 2_048, "the candidate arm steps down");

        let failing = FlowSample {
            rows: 2_048,
            elapsed: Duration::from_millis(10),
            bytes: 16_384,
            sink_pending: None,
            outcome: FlowOutcome::Error,
            at: driver.base + driver.offset,
        };
        assert_eq!(decided(driver.flow.observe(failing)), Decided::BackedOff);
        driver.flow
    }

    #[test]
    fn a_guard_trip_on_a_smaller_candidate_divides_the_incumbent_not_the_candidate() {
        let flow = tripped_on_a_smaller_candidate();
        assert_eq!(
            flow.incumbent_rows(),
            2_048,
            "the incumbent never failed: halve the 4 096 in effect, not the 2 048 under test"
        );
    }

    #[test]
    fn a_guard_trip_on_a_smaller_candidate_leaves_room_to_climb_back() {
        let mut driver = Driver {
            flow: tripped_on_a_smaller_candidate(),
            base: Instant::now(),
            offset: Duration::ZERO,
            rate_for: overhead_bound,
            last: FlowAdjustment::Held,
            adjustments: Vec::new(),
            targets: Vec::new(),
            notices: Vec::new(),
        };
        for _ in 0..3 {
            driver.close_epoch(16);
        }
        assert!(
            driver.incumbent() > 2_048,
            "the ceiling belongs at the size that failed, so the climb must pass \
             the candidate that tripped it, got {}",
            driver.incumbent()
        );
    }

    /// The additive step under a congestion ceiling is a fraction of that
    /// ceiling, not a whole `min_rows`: with the shipped floor those differ by
    /// a factor of two, and the coarser step is what walks a controller onto
    /// the floor it cannot leave.
    #[test]
    fn growth_under_a_congestion_ceiling_steps_by_an_eighth_of_it() {
        let settings = FlowSettings {
            min_rows: 1_024,
            max_rows: 65_536,
            start_rows: 4_096,
            ..unbounded()
        };
        let mut flow = FlowController::new(settings);
        let base = Instant::now();
        let sample = |outcome, at| FlowSample {
            rows: 4_096,
            elapsed: Duration::from_millis(10),
            bytes: 32_768,
            sink_pending: None,
            outcome,
            at,
        };
        assert_eq!(
            decided(flow.observe(sample(FlowOutcome::Error, base))),
            Decided::BackedOff
        );
        assert_eq!(flow.incumbent_rows(), 2_048);

        // The cooldown, then one pass on the incumbent arm, which hands the
        // next pass to the candidate.
        for pass in 1..=u64::from(settings.backoff_cooldown) + 1 {
            flow.observe(sample(
                FlowOutcome::Ok,
                base + Duration::from_millis(pass * 10),
            ));
        }
        assert_eq!(
            flow.target_rows(),
            1_536,
            "an eighth of the 4 096 ceiling below the 2 048 incumbent"
        );
    }

    #[test]
    fn a_guard_trip_backs_off_immediately_without_waiting_for_the_close() {
        let mut driver = Driver::new(unbounded(), overhead_bound);
        driver.passes(6, Duration::from_millis(1_000));
        assert_eq!(driver.incumbent(), 1_000, "still mid-epoch");

        let failing = FlowSample {
            rows: 1_000,
            elapsed: Duration::from_millis(10),
            bytes: 8_000,
            sink_pending: None,
            outcome: FlowOutcome::Error,
            at: driver.base + driver.offset,
        };
        assert_eq!(decided(driver.flow.observe(failing)), Decided::BackedOff);
        assert_eq!(
            driver.flow.incumbent_rows(),
            500,
            "safety is not paced by the epoch"
        );
    }

    #[test]
    fn a_steady_sink_backlog_is_never_pressure() {
        let mut flow = FlowController::new(unbounded());
        let base = Instant::now();
        let sample = FlowSample {
            rows: 1_000,
            elapsed: Duration::from_millis(10),
            bytes: 8_000,
            sink_pending: Some(90_000),
            outcome: FlowOutcome::Ok,
            at: base,
        };
        // A buffering sink reports a large constant backlog against its own
        // flush policy; that is not evidence about the chunk size.
        for _ in 0..10 {
            assert_ne!(decided(flow.observe(sample)), Decided::BackedOff);
        }
        assert_eq!(flow.incumbent_rows(), 1_000);
    }

    #[test]
    fn a_sink_backlog_growing_on_two_consecutive_passes_halves_the_target() {
        let mut flow = FlowController::new(unbounded());
        let base = Instant::now();
        let growing = |pending: u64| FlowSample {
            rows: 1_000,
            elapsed: Duration::from_millis(10),
            bytes: 8_000,
            sink_pending: Some(pending),
            outcome: FlowOutcome::Ok,
            at: base,
        };
        assert_ne!(
            decided(flow.observe(growing(10))),
            Decided::BackedOff,
            "the first report is only a baseline"
        );
        assert_ne!(
            decided(flow.observe(growing(20))),
            Decided::BackedOff,
            "one rise is not a trend"
        );
        assert_eq!(decided(flow.observe(growing(30))), Decided::BackedOff);
        assert_eq!(flow.incumbent_rows(), 500);
    }

    #[test]
    fn a_chunk_projected_past_the_byte_ceiling_halves_the_target() {
        let settings = FlowSettings {
            max_chunk_bytes: 1_024 * 1_024,
            ..unbounded()
        };
        let mut flow = FlowController::new(settings);
        // 2 KiB per row over a 1 000-row target projects 2 MiB.
        let wide = FlowSample {
            rows: 1_000,
            elapsed: Duration::from_millis(10),
            bytes: 1_000 * 2_048,
            sink_pending: None,
            outcome: FlowOutcome::Ok,
            at: Instant::now(),
        };
        assert_eq!(decided(flow.observe(wide)), Decided::BackedOff);
        assert_eq!(flow.incumbent_rows(), 500);
    }

    #[test]
    fn narrow_rows_at_the_same_row_count_stay_under_the_byte_ceiling() {
        let settings = FlowSettings {
            max_chunk_bytes: 1_024 * 1_024,
            ..unbounded()
        };
        let mut flow = FlowController::new(settings);
        // 16 bytes per row over a 1 000-row target projects 16 KiB. The
        // proportional estimate is what makes this distinguishable from the
        // wide-row case: a slice's own reported size would carry its parent's
        // whole buffers.
        let narrow = FlowSample {
            rows: 1_000,
            elapsed: Duration::from_millis(10),
            bytes: 1_000 * 16,
            sink_pending: None,
            outcome: FlowOutcome::Ok,
            at: Instant::now(),
        };
        assert_ne!(decided(flow.observe(narrow)), Decided::BackedOff);
        assert_eq!(flow.incumbent_rows(), 1_000);
    }

    #[test]
    fn the_target_never_falls_below_min_rows() {
        let settings = FlowSettings {
            min_rows: 256,
            start_rows: 1_024,
            ..unbounded()
        };
        let mut flow = FlowController::new(settings);
        let base = Instant::now();
        let mut clock = Duration::ZERO;
        // One failure, then enough clean passes to let the cooldown expire, so
        // every cycle is free to halve again.
        for _ in 0..10 {
            for pass in 0..=settings.backoff_cooldown {
                clock += Duration::from_millis(10);
                let outcome = if pass == 0 {
                    FlowOutcome::Error
                } else {
                    FlowOutcome::Ok
                };
                flow.observe(FlowSample {
                    rows: flow.target_rows() as u64,
                    elapsed: Duration::from_millis(1),
                    bytes: 8_000,
                    sink_pending: None,
                    outcome,
                    at: base + clock,
                });
            }
        }
        assert_eq!(flow.incumbent_rows(), 256);
        assert_eq!(flow.target_rows(), 256);
    }

    #[test]
    fn a_chronic_guard_at_the_floor_keeps_the_search_state() {
        let settings = FlowSettings {
            min_rows: 1_000,
            max_rows: 8_000,
            start_rows: 1_000,
            ..unbounded()
        };
        let mut flow = FlowController::new(settings);
        let base = Instant::now();

        // A failing pass at the floor has nowhere to go: it must not arm a
        // cooldown or destroy the search state, or the controller would sit at
        // `min_rows` forever. It is still adverse evidence, so it reports
        // itself as such rather than as an ordinary quiet pass.
        for i in 0..20 {
            let failing = FlowSample {
                rows: 1_000,
                elapsed: Duration::from_millis(10),
                bytes: 8_000,
                sink_pending: None,
                outcome: FlowOutcome::Error,
                at: base + Duration::from_millis(i * 10),
            };
            assert_eq!(decided(flow.observe(failing)), Decided::HeldAtFloor);
        }
        assert_eq!(flow.incumbent_rows(), 1_000);

        // The moment the failures stop, the search resumes: no cooldown was
        // armed, so the very next epoch can commit.
        let mut driver = Driver {
            flow,
            base: Instant::now(),
            offset: Duration::ZERO,
            rate_for: overhead_bound,
            last: FlowAdjustment::Held,
            adjustments: Vec::new(),
            targets: Vec::new(),
            notices: Vec::new(),
        };
        assert_eq!(decided(driver.close_epoch(16)), Decided::Grew);
        assert_eq!(driver.incumbent(), 2_000);
    }

    #[test]
    fn a_back_off_holds_the_target_through_its_cooldown() {
        let mut flow = FlowController::new(unbounded());
        let base = Instant::now();
        let clean = |at: Instant| FlowSample {
            rows: 1_000,
            elapsed: Duration::from_millis(10),
            bytes: 8_000,
            sink_pending: None,
            outcome: FlowOutcome::Ok,
            at,
        };
        let failing = FlowSample {
            outcome: FlowOutcome::Error,
            ..clean(base)
        };
        assert_eq!(decided(flow.observe(failing)), Decided::BackedOff);
        assert_eq!(flow.incumbent_rows(), 500);

        for i in 0..unbounded().backoff_cooldown {
            assert_eq!(
                flow.observe(clean(base + Duration::from_millis(u64::from(i) * 10))),
                FlowAdjustment::Held
            );
            assert_eq!(
                flow.target_rows(),
                500,
                "the cooldown holds the reduced size"
            );
        }
    }

    #[test]
    fn growth_after_a_back_off_is_additive_under_the_congestion_ceiling() {
        let mut driver = Driver::new(unbounded(), overhead_bound);
        driver.close_epoch(16);
        assert_eq!(driver.incumbent(), 2_000);

        let failing = FlowSample {
            rows: 2_000,
            elapsed: Duration::from_millis(10),
            bytes: 16_000,
            sink_pending: None,
            outcome: FlowOutcome::Error,
            at: driver.base + driver.offset,
        };
        assert_eq!(decided(driver.flow.observe(failing)), Decided::BackedOff);
        assert_eq!(driver.incumbent(), 1_000);

        // The first epoch after a ceiling asks whether smaller is faster; for
        // an overhead-bound pipeline it is not, so the direction flips and the
        // climb resumes additively, an eighth of the failed size at a time.
        for _ in 0..3 {
            driver.close_epoch(16);
        }
        assert!(
            driver.incumbent() > 1_000 && driver.incumbent() <= 2_000,
            "additive growth must stay under the failed size, got {}",
            driver.incumbent()
        );
    }

    #[test]
    fn a_candidate_that_breaches_the_latency_objective_loses_in_stream_mode() {
        let settings = FlowSettings {
            target_latency_ms: 20,
            ..unbounded()
        };
        // Overhead-bound: the larger arm moves more rows per second but takes
        // 10 ms + 1 µs/row, so at 2 000 rows a pass costs 12 ms and at 4 000
        // it costs 14 ms. Only sizes whose mean pass stays inside 20 ms may
        // win.
        let mut driver = Driver::new(settings, overhead_bound);
        for _ in 0..6 {
            driver.close_epoch(16);
        }
        let pass_ms = 10.0 + driver.incumbent() as f64 * 0.001;
        assert!(
            pass_ms <= 20.0,
            "the committed size must respect the objective, {pass_ms} ms at {} rows",
            driver.incumbent()
        );
        assert!(
            driver.incumbent() > 1_000,
            "throughput must still be pursued inside the objective"
        );
    }

    #[test]
    fn a_latency_objective_unachievable_at_the_floor_is_abandoned() {
        let settings = FlowSettings {
            min_rows: 1_000,
            max_rows: 8_000,
            start_rows: 1_000,
            target_latency_ms: 1,
            ..unbounded()
        };
        let mut flow = FlowController::new(settings);
        let sample = FlowSample {
            rows: 1_000,
            elapsed: Duration::from_millis(400),
            bytes: 8_000,
            sink_pending: None,
            outcome: FlowOutcome::Ok,
            at: Instant::now(),
        };
        flow.observe(sample);
        assert!(flow.latency_objective_abandoned());
        assert_eq!(flow.take_latency_notice(), Some(true));
        assert_eq!(flow.take_latency_notice(), None, "a notice is taken once");
    }

    #[test]
    fn an_achievable_pass_at_the_floor_restores_the_latency_objective() {
        // One admissible size, so every pass runs at the floor and the latch
        // is exercised without the experiment moving the arm.
        let settings = FlowSettings {
            min_rows: 1_000,
            max_rows: 1_000,
            start_rows: 1_000,
            target_latency_ms: 100,
            ..unbounded()
        };
        let mut flow = FlowController::new(settings);
        let at = Instant::now();
        let slow = FlowSample {
            rows: 1_000,
            elapsed: Duration::from_millis(400),
            bytes: 8_000,
            sink_pending: None,
            outcome: FlowOutcome::Ok,
            at,
        };
        flow.observe(slow);
        assert!(flow.latency_objective_abandoned());

        let quick = FlowSample {
            elapsed: Duration::from_millis(10),
            ..slow
        };
        flow.observe(quick);
        assert!(!flow.latency_objective_abandoned());
        assert_eq!(flow.take_latency_notice(), Some(false));
    }

    /// Overhead-bound and slow: 1 024 and 2 048 meet a 250 ms objective while
    /// 4 096 and 8 192 breach it, and rows per second still rises with size.
    /// The two objectives disagree, which is the only case where "throughput
    /// subject to a latency objective" says anything.
    fn slow_overhead_bound(rows: usize) -> f64 {
        let seconds = 0.05 + rows as f64 * 0.000_07;
        rows as f64 / seconds
    }

    /// Every size takes 400 ms, so no size the controller can pick meets a
    /// 1 ms objective.
    fn uniformly_slow(rows: usize) -> f64 {
        rows as f64 / 0.4
    }

    #[test]
    fn a_breaching_incumbent_loses_to_a_smaller_candidate_that_meets_the_objective() {
        let settings = FlowSettings {
            min_rows: 1_024,
            max_rows: 65_536,
            start_rows: 4_096,
            target_latency_ms: 250,
            ..unbounded()
        };
        let mut driver = Driver::new(settings, slow_overhead_bound);
        // 4 096 rows cost 337 ms, so the size in effect misses the objective;
        // the up-candidate misses it by more, so the first epoch only turns
        // the search around.
        assert_eq!(driver.close_epoch(16), FlowAdjustment::Held);
        assert_eq!(driver.incumbent(), 4_096);

        // 2 048 rows cost 193 ms and move fewer rows per second. The objective
        // is a constraint, not a veto on growth, so it wins anyway.
        assert_eq!(decided(driver.close_epoch(16)), Decided::Shrank);
        assert_eq!(driver.incumbent(), 2_048);
    }

    #[test]
    fn an_objective_no_size_can_meet_is_abandoned_at_the_floor() {
        let settings = FlowSettings {
            min_rows: 1_024,
            max_rows: 65_536,
            start_rows: 4_096,
            target_latency_ms: 1,
            ..unbounded()
        };
        let mut driver = Driver::new(settings, uniformly_slow);
        for _ in 0..6 {
            driver.close_epoch(16);
        }
        assert!(
            driver.targets.contains(&1_024),
            "an unmeetable objective must drive the search to the floor: {:?}",
            driver.targets
        );
        assert!(
            driver.flow.latency_objective_abandoned(),
            "a breach at the floor proves no admissible size meets it"
        );
        assert_eq!(
            driver.notices,
            vec![true],
            "the runner is told once, when the objective is given up"
        );
    }

    #[test]
    fn a_disabled_controller_holds_one_constant_target() {
        let settings = FlowSettings {
            enabled: false,
            ..unbounded()
        };
        let mut driver = Driver::new(settings, overhead_bound);
        assert!(!driver.flow.enabled());
        driver.passes(40, Duration::from_millis(120_000));
        assert!(
            driver.targets.iter().all(|rows| *rows == 1_000),
            "a disabled controller never moves: {:?}",
            driver.targets
        );
    }

    #[test]
    fn a_pinned_size_holds_through_epochs_and_guards() {
        let settings = FlowSettings {
            fixed_rows: Some(4_096),
            ..unbounded()
        };
        let mut driver = Driver::new(settings, overhead_bound);
        assert_eq!(driver.target(), 4_096);
        driver.passes(40, Duration::from_millis(120_000));
        assert!(driver.targets.iter().all(|rows| *rows == 4_096));

        let failing = FlowSample {
            rows: 4_096,
            elapsed: Duration::from_millis(10),
            bytes: 32_768,
            sink_pending: None,
            outcome: FlowOutcome::Error,
            at: driver.base + driver.offset,
        };
        assert_eq!(driver.flow.observe(failing), FlowAdjustment::Held);
        assert_eq!(driver.target(), 4_096, "a pinned size never moves");
    }

    #[test]
    fn a_pinned_controller_still_reports_throughput() {
        let settings = FlowSettings {
            fixed_rows: Some(1_000),
            ..unbounded()
        };
        let mut flow = FlowController::new(settings);
        flow.observe(FlowSample {
            rows: 1_000,
            elapsed: Duration::from_millis(100),
            bytes: 8_000,
            sink_pending: None,
            outcome: FlowOutcome::Ok,
            at: Instant::now(),
        });
        assert!(
            (flow.throughput() - 10_000.0).abs() < 1.0,
            "1 000 rows in 100 ms is 10 000 rows/s, got {}",
            flow.throughput()
        );
    }

    #[test]
    fn an_idle_pass_is_not_evidence() {
        let mut driver = Driver::new(unbounded(), overhead_bound);
        driver.passes(8, Duration::from_millis(1_000));
        let throughput = driver.flow.throughput();
        let target = driver.target();

        let idle = FlowSample {
            rows: 0,
            elapsed: Duration::from_millis(5),
            bytes: 0,
            sink_pending: None,
            outcome: FlowOutcome::Ok,
            at: driver.base + driver.offset + Duration::from_millis(120_000),
        };
        assert_eq!(driver.flow.observe(idle), FlowAdjustment::Held);
        assert_eq!(
            driver.target(),
            target,
            "an idle pass must not close an epoch or move the target"
        );
        assert_eq!(driver.flow.throughput(), throughput);
    }

    /// Bigger chunks move more rows per second but take longer per pass: the
    /// shape a latency objective exists to refuse.
    fn faster_but_slower_per_pass(rows: usize) -> f64 {
        if rows > 4_096 { 12_000.0 } else { 10_000.0 }
    }

    /// The plan a direct `run_stream` call hands the runner decides whether
    /// stream mode's own contract holds. [`FlowPlan::default`] is the batch
    /// policy, so a stream service assembled in code would otherwise commit a
    /// candidate that breaches the per-pass objective the mode exists to
    /// promise.
    #[test]
    fn the_stream_plan_refuses_a_latency_breaching_candidate_and_the_batch_plan_takes_it() {
        let stream = FlowPlan::stream_default().for_source("in");
        let batch = FlowPlan::default().for_source("in");
        assert_eq!(stream.target_latency_ms, STREAM_LATENCY_OBJECTIVE_MS);
        assert_eq!(batch.target_latency_ms, 0);
        // `faster_but_slower_per_pass` is a plain `fn`, so it cannot read the
        // settings: it hard-codes the step this pair of defaults produces. If
        // a tuning commit moves them, fail here rather than further down with
        // a confusing "the candidate should have lost".
        assert_eq!(
            (stream.start_rows, stream.growth_factor),
            (4_096, 2.0),
            "the rate curve below is keyed to a 4096 incumbent and an 8192 candidate"
        );

        // 8 192 rows at 12 000 rows/s is 683 ms a pass: faster than the
        // incumbent by every throughput measure, and far past 250 ms.
        let mut under_stream = Driver::new(stream, faster_but_slower_per_pass);
        assert_eq!(under_stream.close_epoch(8), FlowAdjustment::Held);
        assert_eq!(
            under_stream.incumbent(),
            4_096,
            "a candidate breaching target_latency_ms must lose however many rows it moved"
        );

        let mut under_batch = Driver::new(batch, faster_but_slower_per_pass);
        assert_eq!(decided(under_batch.close_epoch(8)), Decided::Grew);
        assert_eq!(
            under_batch.incumbent(),
            8_192,
            "a batch pass carries no per-item latency contract, so rows per second alone decides"
        );
    }

    /// A candidate trip that reduces to the incumbent already in effect, at
    /// the floor: `back_off` must report the same [`HeldAtFloor`] a trip on
    /// the Incumbent arm would, not [`BackedOff`], or `FlowEpisode`'s
    /// `(kind, to_rows, cause)` dedup never collapses a chronic-but-
    /// intermittent congestion into one marker: it alternates on every
    /// trip that happens to land on a different arm, forever.
    #[test]
    fn a_candidate_trip_that_clamps_back_onto_the_floor_reports_held_at_floor() {
        let settings = FlowSettings {
            min_rows: 1_024,
            max_rows: 65_536,
            start_rows: 4_096,
            ..unbounded()
        };
        let mut flow = FlowController::new(settings);
        let base = Instant::now();
        let mut clock = Duration::ZERO;
        let expire_cooldown = |flow: &mut FlowController, clock: &mut Duration| {
            for _ in 0..=u64::from(settings.backoff_cooldown) {
                *clock += Duration::from_millis(10);
                flow.observe(FlowSample {
                    rows: flow.target_rows() as u64,
                    elapsed: Duration::from_millis(1),
                    bytes: 8_000,
                    sink_pending: None,
                    outcome: FlowOutcome::Ok,
                    at: base + *clock,
                });
            }
        };
        let fail_at_target = |flow: &mut FlowController, clock: &mut Duration| {
            *clock += Duration::from_millis(10);
            let rows = flow.target_rows() as u64;
            flow.observe(FlowSample {
                rows,
                elapsed: Duration::from_millis(10),
                bytes: rows * 8,
                sink_pending: None,
                outcome: FlowOutcome::Error,
                at: base + *clock,
            })
        };

        // Walk the incumbent onto the floor: 4096 fails (-> 2048), then the
        // 2048 incumbent fails against its own smaller candidate (-> 1024,
        // the floor). Two genuine moves, both still `BackedOff`.
        assert_eq!(
            decided(fail_at_target(&mut flow, &mut clock)),
            Decided::BackedOff
        );
        assert_eq!(flow.incumbent_rows(), 2_048);
        expire_cooldown(&mut flow, &mut clock);
        assert_eq!(
            decided(fail_at_target(&mut flow, &mut clock)),
            Decided::BackedOff
        );
        assert_eq!(flow.incumbent_rows(), 1_024, "halved onto the floor");

        // The next candidate (1280, an eighth of the 1280 ceiling below a
        // floor incumbent that cannot go lower) also fails. `reduced` clamps
        // straight back to the unchanged 1024 incumbent: nothing moved, so
        // this must read as `HeldAtFloor`, not `BackedOff`.
        expire_cooldown(&mut flow, &mut clock);
        let candidate_trip = fail_at_target(&mut flow, &mut clock);
        assert_eq!(decided(candidate_trip), Decided::HeldAtFloor);
        assert_eq!(flow.incumbent_rows(), 1_024, "still at the floor, unmoved");

        // A trip on the plain Incumbent arm at the floor takes the other
        // early return entirely (`failed <= floor()`); its `to_rows` must
        // match the candidate-trip case above, or `FlowEpisode` still sees
        // two different dedup keys for the same steady state.
        expire_cooldown(&mut flow, &mut clock);
        clock += Duration::from_millis(10);
        let incumbent_trip = flow.observe(FlowSample {
            rows: flow.target_rows() as u64,
            elapsed: Duration::from_millis(10),
            bytes: 8_000,
            sink_pending: None,
            outcome: FlowOutcome::Error,
            at: base + clock,
        });
        assert_eq!(decided(incumbent_trip), Decided::HeldAtFloor);
        let (
            FlowAdjustment::HeldAtFloor(candidate_move),
            FlowAdjustment::HeldAtFloor(incumbent_move),
        ) = (candidate_trip, incumbent_trip)
        else {
            unreachable!("both asserted HeldAtFloor above");
        };
        assert_eq!(
            candidate_move.to_rows, incumbent_move.to_rows,
            "a candidate-arm trip and an incumbent-arm trip at the floor must \
             dedup to the same episode key"
        );
    }

    /// `back_off`'s incumbent-unchanged collapse must not fire away from the
    /// floor: with no ceiling yet, `growth_factor`/`backoff_factor` default
    /// to exact inverses, so a candidate trip at *any* incumbent (`candidate
    /// = 2 * incumbent`, `reduced = failed / 2 = incumbent`) also clamps
    /// back onto the unchanged incumbent. That is a real, still-searching
    /// rejection nowhere near `min_rows` and must stay `BackedOff`, or every
    /// such trip in the workspace's default configuration would misreport
    /// as [`HeldAtFloor`](FlowAdjustment::HeldAtFloor).
    #[test]
    fn a_candidate_trip_far_from_the_floor_stays_backed_off() {
        let settings = FlowSettings {
            min_rows: 1_024,
            max_rows: 65_536,
            start_rows: 4_096,
            ..unbounded()
        };
        let mut flow = FlowController::new(settings);
        let base = Instant::now();
        flow.observe(FlowSample {
            rows: flow.target_rows() as u64,
            elapsed: Duration::from_millis(10),
            bytes: 8_000,
            sink_pending: None,
            outcome: FlowOutcome::Ok,
            at: base,
        });
        assert_eq!(flow.target_rows(), 8_192, "arm toggled to the candidate");
        let adjustment = flow.observe(FlowSample {
            rows: flow.target_rows() as u64,
            elapsed: Duration::from_millis(10),
            bytes: 8_000,
            sink_pending: None,
            outcome: FlowOutcome::Error,
            at: base + Duration::from_millis(10),
        });
        assert_eq!(decided(adjustment), Decided::BackedOff);
        assert_eq!(
            flow.incumbent_rows(),
            4_096,
            "unchanged, but nowhere near the floor"
        );
    }
}
