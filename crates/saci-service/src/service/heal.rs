//! Connector self-healing: replacing a broken connector with a fresh one
//! built from the same factory and the same config.
//!
//! [`RetryingSource`] and [`RetryingSink`] re-drive the *same*
//! instance, which recovers a call that failed for a reason the instance
//! survived. It cannot recover a poisoned handle: a `TcpSink` whose peer went
//! away fails every later write on that same dead socket, however many times
//! it is re-driven. Healing is the layer above: after
//! [`HealSettings::after_failures`] consecutive failures the wrapper drops the
//! instance and asks its factory for another one.
//!
//! The wrappers live here rather than beside the retry wrappers in
//! `saci-core`, because a rebuild needs the [`Registry`] and the declared node
//! spec, both of which are host concepts, and because the metric writers are.
//!
//! ## What a rebuild is allowed to cost
//!
//! Only a connector that answers `Ok(())` from
//! [`SourceFactory::rebuildable`](saci_connector::SourceFactory::rebuildable)
//! is ever healed, so the loss a rebuild can cause is bounded by what that
//! connector already documents. The builder is what consults it; by the time
//! a [`HealingSource`] or [`HealingSink`] exists, the answer was `Ok`.
//!
//! ## Pacing
//!
//! [`heal_if_due`](HealingSource::next_batch) never sleeps. A rebuild is
//! scheduled for a deadline and performed at the head of the first call past
//! it, so the runner's own pacing is what lets the deadline elapse and
//! nothing is added to the item path.
//!
//! ## State machine
//!
//! ```text
//! Healthy --fail--> Failing{n} --n reaches after_failures--> Scheduled{1}
//! Scheduled{k} --deadline, build ok--> Probing{k} --ok--> Healthy
//!                                                 --fail--> Scheduled{k+1}
//! Scheduled{k} --deadline, build fails--> Scheduled{k+1} | Exhausted
//! ```
//!
//! `Probing` is what stops a flapping peer from being rebuilt at the base
//! delay forever: a rebuild that lands but does not hold resumes the backoff
//! where it left off instead of re-counting `after_failures`.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use tokio::time::Instant;

use saci_connector::{ChannelBridge, ConfigValue, ConnectorContext, NodeIdentity};
use saci_core::error::SaciError;
use saci_core::io::retry::{RetryingSink, RetryingSource};
use saci_core::io::{sink::Sink, source::Source};
use saci_core::retry::{RetryMode, SystemConfig};
use saci_transformer::Transformer;

use super::factories::missing_factory_error;
use super::registry::Registry;
use super::sampling::HEAL_TARGET;

/// Ceiling on the exponent a backoff computes, so an unlimited-attempt
/// schedule never depends on `f64` infinity saturating into the delay cap.
const MAX_BACKOFF_EXPONENT: usize = 30;

/// Resolved self-healing policy for one node.
///
/// Built from [`HealConfig`](super::config::HealConfig) by layering a node's
/// block over the top-level one and both over [`Default`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HealSettings {
    /// Whether a failing connector is replaced at all.
    pub enabled: bool,
    /// Consecutive failed operations before the first rebuild.
    pub after_failures: u32,
    /// Delay before the first rebuild.
    pub base_delay_ms: u64,
    /// Growth factor applied per further attempt.
    pub multiplier: f64,
    /// Ceiling on the computed delay.
    pub max_delay_ms: u64,
    /// Fraction of the delay randomised, in `0.0..=1.0`.
    pub jitter: f64,
    /// Rebuild attempts before the node gives up; `0` never gives up.
    pub max_attempts: u32,
}

impl Default for HealSettings {
    /// On, at three failures, one second, doubling to a minute, forever.
    ///
    /// A node that stops healing needs an operator, and a broker down for an
    /// hour must come back on its own, so the schedule tops out at one
    /// rebuild a minute rather than giving up.
    fn default() -> Self {
        Self {
            enabled: true,
            after_failures: 3,
            base_delay_ms: 1_000,
            multiplier: 2.0,
            max_delay_ms: 60_000,
            jitter: 0.1,
            max_attempts: 0,
        }
    }
}

impl HealSettings {
    /// The delay before rebuild attempt `attempt` (1-based).
    ///
    /// Reuses `saci-core`'s own backoff arithmetic rather than repeating its
    /// jitter formula here.
    pub(crate) fn delay_for(&self, attempt: u32) -> Duration {
        let max_delay = Duration::from_millis(self.max_delay_ms);
        RetryMode::exponential_custom(
            usize::MAX,
            Duration::from_millis(self.base_delay_ms),
            self.multiplier,
            max_delay,
            self.jitter,
        )
        .delay_for_attempt((attempt.saturating_sub(1) as usize).min(MAX_BACKOFF_EXPONENT))
        .unwrap_or(max_delay)
    }
}

/// Whether a wrapper drives a source or a sink, for the metric attribute key
/// and the log field name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Source,
    Sink,
}

impl Role {
    fn healed(self, id: &str) {
        match self {
            Self::Source => crate::metrics::instruments().source_heal(id),
            Self::Sink => crate::metrics::instruments().sink_heal(id),
        }
    }

    fn heal_failed(self, id: &str) {
        match self {
            Self::Source => crate::metrics::instruments().source_heal_failure(id),
            Self::Sink => crate::metrics::instruments().sink_heal_failure(id),
        }
    }

    /// `"source"` or `"sink"`, the log field naming the node.
    const fn label(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Sink => "sink",
        }
    }
}

/// Everything a rebuild needs, shared by a node's initial build and every
/// heal so the two can never drift.
///
/// `Send + Sync` because every part is: [`Registry`] holds
/// `Box<dyn SourceFactory>` and both factory traits are `Send + Sync +
/// 'static`, [`Transformer`] and [`ChannelBridge`] likewise, and
/// [`ConfigValue`] is a `serde_json::Value`.
pub(crate) struct Rebuilder {
    registry: Arc<Registry>,
    type_name: String,
    node_id: String,
    config: ConfigValue,
    transformer: Option<Arc<dyn Transformer>>,
    channels: Option<Arc<dyn ChannelBridge>>,
    /// Where this node sits, for a connector that names itself to a peer.
    /// `None` for a node built outside a workflow, such as a dead-letter
    /// store's own source and sink.
    identity: Option<NodeIdentity>,
    /// The retry policy the built instance is wrapped in. `None` for a source
    /// in `run_mode kind="stream"`, where the runner's own re-poll is the
    /// retry loop.
    retry: Option<SystemConfig>,
}

impl Rebuilder {
    /// Capture what a rebuild needs. `retry` is `None` to hand the instance
    /// over unwrapped, and the identity is attached separately with
    /// [`with_identity`](Self::with_identity).
    pub(crate) fn new(
        registry: Arc<Registry>,
        type_name: &str,
        node_id: &str,
        config: ConfigValue,
        transformer: Option<Arc<dyn Transformer>>,
        channels: Option<Arc<dyn ChannelBridge>>,
        retry: Option<SystemConfig>,
    ) -> Self {
        Self {
            registry,
            type_name: type_name.to_string(),
            node_id: node_id.to_string(),
            config,
            transformer,
            channels,
            identity: None,
            retry,
        }
    }

    /// Name the workflow node this rebuilder builds for, so every build and
    /// every heal hands the connector the same [`NodeIdentity`].
    ///
    /// Left unset for a connector built outside a workflow, such as a
    /// dead-letter store's own source and sink.
    pub(crate) fn with_identity(mut self, identity: NodeIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    fn context(&self) -> ConnectorContext {
        let mut ctx = ConnectorContext::new(self.transformer.clone());
        if let Some(channels) = &self.channels {
            ctx = ctx.with_channels(channels.clone());
        }
        if let Some(identity) = &self.identity {
            ctx = ctx.with_identity(identity.clone());
        }
        ctx
    }

    /// Build one source instance, wrapped in its retry policy when it has one.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when no factory is registered for
    /// the declared type, or whatever the factory itself refused with.
    pub(crate) fn build_source(&self) -> Result<Box<dyn Source>, SaciError> {
        let factory = self
            .registry
            .source(&self.type_name)
            .ok_or_else(|| missing_factory_error("source", &self.type_name, &self.node_id))?;
        let built = factory.build(&self.config, &self.context())?;
        Ok(match self.retry {
            Some(retry) => Box::new(RetryingSource::new(built, retry, &self.node_id)),
            None => built,
        })
    }

    /// Build one sink instance, wrapped in its retry policy when it has one.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when no factory is registered for
    /// the declared type, or whatever the factory itself refused with.
    pub(crate) fn build_sink(&self) -> Result<Box<dyn Sink>, SaciError> {
        let factory = self
            .registry
            .sink(&self.type_name)
            .ok_or_else(|| missing_factory_error("sink", &self.type_name, &self.node_id))?;
        let built = factory.build(&self.config, &self.context())?;
        Ok(match self.retry {
            Some(retry) => Box::new(RetryingSink::new(built, retry, &self.node_id)),
            None => built,
        })
    }
}

/// Where one node stands between healthy and given up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HealState {
    Healthy,
    Failing {
        consecutive: u32,
    },
    /// A rebuild is due at `at`. `tokio::time::Instant`, not `std`'s, so a
    /// paused test clock controls it.
    Scheduled {
        attempt: u32,
        at: Instant,
    },
    /// A rebuild landed; the next operation decides whether it took.
    Probing {
        attempt: u32,
    },
    Exhausted,
}

/// The bookkeeping both wrappers share: the state machine, the schedule and
/// the reporting. Generic over nothing, so it is one copy of the logic.
struct Healer {
    rebuilder: Rebuilder,
    settings: HealSettings,
    state: HealState,
    role: Role,
    workflow_id: String,
    node_id: String,
    /// Why the connector has no instance, set when a rebuild's build failed.
    down: Option<String>,
    /// Latched `true` the moment a rebuilt instance's first operation
    /// succeeds, and cleared by whoever reads it. The dead letter queue
    /// watches it: a sink that just came back is the one moment a replay is
    /// most likely to land, and waiting out the backoff schedule instead
    /// would hold the letters for up to a minute.
    recovered: Arc<std::sync::atomic::AtomicBool>,
}

impl Healer {
    fn new(
        rebuilder: Rebuilder,
        settings: HealSettings,
        role: Role,
        workflow_id: &str,
        node_id: &str,
    ) -> Self {
        Self {
            rebuilder,
            settings,
            state: HealState::Healthy,
            role,
            workflow_id: workflow_id.to_string(),
            node_id: node_id.to_string(),
            down: None,
            recovered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// The error a call returns while the node holds no instance.
    fn down_error(&self) -> SaciError {
        SaciError::generic(format!(
            "{}: connector is down: {}",
            self.node_id,
            self.down.as_deref().unwrap_or("rebuild failed")
        ))
    }

    /// Whether a rebuild is due now, consuming the schedule if so.
    ///
    /// Returns the attempt number to perform, or `None`.
    fn due(&mut self) -> Option<u32> {
        match self.state {
            HealState::Scheduled { attempt, at } if Instant::now() >= at => Some(attempt),
            _ => None,
        }
    }

    /// Record that attempt `attempt` produced a working instance.
    fn built(&mut self, attempt: u32) {
        self.down = None;
        self.state = HealState::Probing { attempt };
        self.role.healed(&self.node_id);
        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: HEAL_TARGET,
            workflow = %self.workflow_id,
            node = %self.node_id,
            role = self.role.label(),
            attempt,
            "connector healed"
        );
    }

    /// Record that attempt `attempt` produced nothing usable, and schedule
    /// the next one.
    fn build_failed(&mut self, attempt: u32, error: String) {
        self.role.heal_failed(&self.node_id);
        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: HEAL_TARGET,
            workflow = %self.workflow_id,
            node = %self.node_id,
            role = self.role.label(),
            attempt,
            error = %error,
            "connector heal failed"
        );
        self.down = Some(error);
        self.schedule(attempt + 1, None);
    }

    /// Move to `Scheduled { attempt }`, or to `Exhausted` when the node has
    /// spent its budget.
    fn schedule(&mut self, attempt: u32, consecutive: Option<u32>) {
        if self.settings.max_attempts != 0 && attempt > self.settings.max_attempts {
            self.state = HealState::Exhausted;
            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: HEAL_TARGET,
                workflow = %self.workflow_id,
                node = %self.node_id,
                role = self.role.label(),
                attempts = self.settings.max_attempts,
                "connector heal exhausted"
            );
            return;
        }
        let delay = self.settings.delay_for(attempt);
        self.state = HealState::Scheduled {
            attempt,
            at: Instant::now() + delay,
        };
        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: HEAL_TARGET,
            workflow = %self.workflow_id,
            node = %self.node_id,
            role = self.role.label(),
            attempt,
            delay_ms = delay.as_millis() as u64,
            consecutive = consecutive.unwrap_or(0),
            "connector heal scheduled"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = consecutive;
    }

    /// A delegated operation succeeded.
    fn success(&mut self) {
        match self.state {
            HealState::Healthy => {}
            HealState::Probing { .. } => {
                self.state = HealState::Healthy;
                self.recovered
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: HEAL_TARGET,
                    workflow = %self.workflow_id,
                    node = %self.node_id,
                    role = self.role.label(),
                    "connector recovered"
                );
            }
            _ => self.state = HealState::Healthy,
        }
    }

    /// A delegated operation failed.
    fn failure(&mut self) {
        match self.state {
            HealState::Healthy => {
                if self.settings.after_failures <= 1 {
                    self.schedule(1, Some(1));
                } else {
                    self.state = HealState::Failing { consecutive: 1 };
                }
            }
            HealState::Failing { consecutive } => {
                let consecutive = consecutive + 1;
                if consecutive >= self.settings.after_failures {
                    self.schedule(1, Some(consecutive));
                } else {
                    self.state = HealState::Failing { consecutive };
                }
            }
            // The rebuild already proved the connector was broken, so the
            // backoff resumes where it left off instead of re-counting.
            HealState::Probing { attempt } => self.schedule(attempt + 1, None),
            HealState::Scheduled { .. } | HealState::Exhausted => {}
        }
    }
}

/// A [`Source`] that is replaced with a fresh instance from its factory when
/// its polls keep failing.
///
/// See the [module docs](self) for the state machine and what a rebuild is
/// allowed to cost.
pub struct HealingSource {
    inner: Option<Box<dyn Source>>,
    healer: Healer,
    /// Pinned at construction. [`Source::schema`] returns a value, not a
    /// `Result`, and the workflow graph's schema agreement was validated
    /// against this one at load time, so a rebuild may not change it.
    schema: Arc<Schema>,
    /// The last size hint, re-applied to a rebuilt instance so a healed
    /// source keeps its flow-control credit.
    last_hint: Option<usize>,
}

impl HealingSource {
    /// Wrap `inner`, rebuilding it through `rebuilder` under `settings`.
    pub(crate) fn new(
        inner: Box<dyn Source>,
        rebuilder: Rebuilder,
        settings: HealSettings,
        workflow_id: &str,
        node_id: &str,
    ) -> Self {
        let schema = inner.schema();
        Self {
            inner: Some(inner),
            healer: Healer::new(rebuilder, settings, Role::Source, workflow_id, node_id),
            schema,
            last_hint: None,
        }
    }

    /// Rebuild if the schedule is due. Never sleeps and never fails: a
    /// rebuild that does not land leaves the node down, and the call that
    /// follows reports it.
    fn heal_if_due(&mut self) {
        let Some(attempt) = self.healer.due() else {
            return;
        };
        // Drop the broken instance before asking for another, so an exclusive
        // resource it holds (a bound listen port, an open file handle) is
        // released first.
        self.inner = None;
        match self.healer.rebuilder.build_source() {
            Ok(fresh) => {
                if let Some(mismatch) = schema_mismatch(&self.schema, &fresh.schema()) {
                    drop(fresh);
                    self.healer.build_failed(attempt, mismatch);
                    return;
                }
                let mut fresh = fresh;
                if let Some(rows) = self.last_hint {
                    fresh.request_batch_rows(rows);
                }
                self.inner = Some(fresh);
                self.healer.built(attempt);
            }
            Err(e) => self.healer.build_failed(attempt, e.to_string()),
        }
    }
}

#[async_trait]
impl Source for HealingSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.heal_if_due();
        let Some(inner) = self.inner.as_mut() else {
            return Err(self.healer.down_error());
        };
        // `Ok(None)` is EOF, not a failure: it takes the success path, the
        // same call the retry wrapper never retries.
        match inner.next_batch().await {
            Ok(batch) => {
                self.healer.success();
                Ok(batch)
            }
            Err(e) => {
                self.healer.failure();
                Err(e)
            }
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.inner.as_ref().and_then(|inner| inner.estimated_rows())
    }

    fn request_batch_rows(&mut self, rows: usize) {
        self.last_hint = Some(rows);
        if let Some(inner) = self.inner.as_mut() {
            inner.request_batch_rows(rows);
        }
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        // No heal here, for the reason `HealingSink::finish` gives: `finish`
        // runs once at exit, and a rebuilt source has consumed nothing to
        // make durable.
        let Some(inner) = self.inner.as_mut() else {
            return Err(self.healer.down_error());
        };
        match inner.finish().await {
            Ok(()) => {
                self.healer.success();
                Ok(())
            }
            Err(e) => {
                self.healer.failure();
                Err(e)
            }
        }
    }
}

/// A [`Sink`] that is replaced with a fresh instance from its factory when its
/// writes keep failing.
///
/// The old instance is dropped without `finish`: it was unwritable, so
/// `finish` would fail too. Whatever it had accepted and not yet written is
/// gone, which is why a sink that buffers across an outage refuses
/// [`SinkFactory::rebuildable`](saci_connector::SinkFactory::rebuildable) and
/// is never wrapped in this.
pub struct HealingSink {
    inner: Option<Box<dyn Sink>>,
    healer: Healer,
    /// Pinned at construction, for the same reason as
    /// [`HealingSource::schema`].
    schema: Arc<Schema>,
}

impl HealingSink {
    /// Wrap `inner`, rebuilding it through `rebuilder` under `settings`.
    pub(crate) fn new(
        inner: Box<dyn Sink>,
        rebuilder: Rebuilder,
        settings: HealSettings,
        workflow_id: &str,
        node_id: &str,
    ) -> Self {
        let schema = inner.schema();
        Self {
            inner: Some(inner),
            healer: Healer::new(rebuilder, settings, Role::Sink, workflow_id, node_id),
            schema,
        }
    }

    /// A handle on the flag this node sets when a rebuilt instance's first
    /// write succeeds.
    ///
    /// The reader clears it, so a swap answers "has this sink come back since
    /// I last looked". The dead letter queue is that reader.
    pub(crate) fn recovered_flag(&self) -> Arc<std::sync::atomic::AtomicBool> {
        Arc::clone(&self.healer.recovered)
    }

    /// Rebuild if the schedule is due. See [`HealingSource::heal_if_due`].
    fn heal_if_due(&mut self) {
        let Some(attempt) = self.healer.due() else {
            return;
        };
        self.inner = None;
        match self.healer.rebuilder.build_sink() {
            Ok(fresh) => {
                if let Some(mismatch) = schema_mismatch(&self.schema, &fresh.schema()) {
                    drop(fresh);
                    self.healer.build_failed(attempt, mismatch);
                    return;
                }
                self.inner = Some(fresh);
                self.healer.built(attempt);
            }
            Err(e) => self.healer.build_failed(attempt, e.to_string()),
        }
    }
}

#[async_trait]
impl Sink for HealingSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.heal_if_due();
        let Some(inner) = self.inner.as_mut() else {
            return Err(self.healer.down_error());
        };
        match inner.write_batch(batch).await {
            Ok(()) => {
                self.healer.success();
                Ok(())
            }
            Err(e) => {
                self.healer.failure();
                Err(e)
            }
        }
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        // No heal here: `finish` runs once at exit, and a rebuilt sink would
        // have nothing of this run's to flush.
        let Some(inner) = self.inner.as_mut() else {
            return Err(self.healer.down_error());
        };
        match inner.finish().await {
            Ok(()) => {
                self.healer.success();
                Ok(())
            }
            Err(e) => {
                self.healer.failure();
                Err(e)
            }
        }
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn pending_rows(&self) -> Option<usize> {
        // `None` while the node is down: the backlog is unknown, and a false
        // zero would read to flow control as a drained sink.
        self.inner.as_ref().and_then(|inner| inner.pending_rows())
    }
}

/// The reason a rebuilt connector's schema is unusable, or `None` when the
/// two agree.
///
/// A rebuild that changed the schema would break the field-for-field
/// agreement `validate_workflow_graph` checked at load time, so it is
/// discarded rather than installed.
fn schema_mismatch(pinned: &Schema, fresh: &Schema) -> Option<String> {
    if pinned.fields() == fresh.fields() {
        return None;
    }
    Some(format!(
        "rebuilt connector reports schema {:?} but the workflow was validated against {:?}",
        fresh.fields(),
        pinned.fields()
    ))
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    #[cfg(feature = "connector-saci")]
    use saci_connector::from_kdl_str;
    use saci_connector::{ConfigMap, ConfigValue, SinkFactory, SourceFactory};

    /// What the test factory does next, shared with the test that owns it.
    ///
    /// Per-test rather than a set of statics: the lib test binary runs its
    /// tests on one thread pool, so process-global counters would be shared
    /// between concurrently running tests.
    #[derive(Default)]
    struct Script {
        /// Instances the factory has handed out.
        builds: AtomicUsize,
        /// Operations each fresh instance answers before it starts failing.
        good_ops: AtomicUsize,
        /// Fail the factory itself rather than the instances it makes.
        build_fails: AtomicBool,
        /// Make the next instance report a different schema.
        wrong_schema: AtomicBool,
        /// The last size hint any instance received.
        last_hint: AtomicUsize,
        /// Make every instance's `finish` fail, regardless of `good_ops`.
        finish_fails: AtomicBool,
    }

    impl Script {
        fn new(good_ops: usize) -> Arc<Self> {
            Arc::new(Self {
                good_ops: AtomicUsize::new(good_ops),
                ..Self::default()
            })
        }

        fn builds(&self) -> usize {
            self.builds.load(Ordering::SeqCst)
        }

        fn set_good_ops(&self, n: usize) {
            self.good_ops.store(n, Ordering::SeqCst);
        }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn other_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]))
    }

    fn one_row() -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![1i64]))])
            .expect("one-row batch")
    }

    /// A source whose instances answer `good_ops` batches and then fail every
    /// later poll, the shape of a connector holding a poisoned handle.
    struct ScriptedSource {
        script: Arc<Script>,
        schema: Arc<Schema>,
        remaining: usize,
    }

    #[async_trait]
    impl Source for ScriptedSource {
        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }

        async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
            if self.remaining == 0 {
                return Err(SaciError::generic("the connector is down"));
            }
            self.remaining -= 1;
            Ok(Some(one_row()))
        }

        async fn finish(&mut self) -> Result<(), SaciError> {
            if self.script.finish_fails.load(Ordering::SeqCst) {
                Err(SaciError::generic("finish failed"))
            } else {
                Ok(())
            }
        }

        fn request_batch_rows(&mut self, rows: usize) {
            self.script.last_hint.store(rows, Ordering::SeqCst);
        }
    }

    struct ScriptedSink {
        schema: Arc<Schema>,
        remaining: usize,
    }

    #[async_trait]
    impl Sink for ScriptedSink {
        async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<(), SaciError> {
            if self.remaining == 0 {
                return Err(SaciError::generic("the peer went away"));
            }
            self.remaining -= 1;
            Ok(())
        }

        async fn finish(&mut self) -> Result<(), SaciError> {
            Ok(())
        }

        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }
    }

    struct ScriptedFactory(Arc<Script>);

    impl ScriptedFactory {
        /// Record the build and report whether the factory itself refuses.
        fn admit(&self) -> Result<(), SaciError> {
            self.0.builds.fetch_add(1, Ordering::SeqCst);
            if self.0.build_fails.load(Ordering::SeqCst) {
                return Err(SaciError::generic("the broker refused the connection"));
            }
            Ok(())
        }
    }

    impl SourceFactory for ScriptedFactory {
        fn type_name(&self) -> &'static str {
            "scripted"
        }

        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Source>, SaciError> {
            self.admit()?;
            let schema = if self.0.wrong_schema.load(Ordering::SeqCst) {
                other_schema()
            } else {
                schema()
            };
            Ok(Box::new(ScriptedSource {
                script: Arc::clone(&self.0),
                schema,
                remaining: self.0.good_ops.load(Ordering::SeqCst),
            }))
        }

        fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
            Ok(())
        }
    }

    impl SinkFactory for ScriptedFactory {
        fn type_name(&self) -> &'static str {
            "scripted"
        }

        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Sink>, SaciError> {
            self.admit()?;
            Ok(Box::new(ScriptedSink {
                schema: schema(),
                remaining: self.0.good_ops.load(Ordering::SeqCst),
            }))
        }

        fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
            Ok(())
        }
    }

    fn rebuilder(script: &Arc<Script>) -> Rebuilder {
        let mut registry = Registry::new();
        registry.register_source(ScriptedFactory(Arc::clone(script)));
        registry.register_sink(ScriptedFactory(Arc::clone(script)));
        Rebuilder::new(
            Arc::new(registry),
            "scripted",
            "node-1",
            ConfigValue::Object(ConfigMap::new()),
            None,
            None,
            None,
        )
    }

    /// 1 s base, doubling to a 4 s cap, no jitter: the tests assert on the
    /// deadline itself, so the schedule must be exact.
    fn settings(after_failures: u32, max_attempts: u32) -> HealSettings {
        HealSettings {
            after_failures,
            base_delay_ms: 1_000,
            multiplier: 2.0,
            max_delay_ms: 4_000,
            jitter: 0.0,
            max_attempts,
            ..HealSettings::default()
        }
    }

    fn healing_source(
        script: &Arc<Script>,
        after_failures: u32,
        max_attempts: u32,
        node_id: &str,
    ) -> HealingSource {
        let rebuilder = rebuilder(script);
        let inner = rebuilder.build_source().expect("first instance builds");
        HealingSource::new(
            inner,
            rebuilder,
            settings(after_failures, max_attempts),
            "wf",
            node_id,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_is_rebuilt_only_after_its_threshold_and_only_past_its_deadline() {
        let script = Script::new(1);
        let mut source = healing_source(&script, 3, 0, "node-1");

        assert!(source.next_batch().await.is_ok(), "the first poll succeeds");
        for _ in 0..3 {
            assert!(source.next_batch().await.is_err(), "the handle is dead");
        }
        assert_eq!(
            script.builds(),
            1,
            "the threshold schedules a rebuild, it does not perform one"
        );

        // Still short of the 1000 ms deadline.
        tokio::time::advance(Duration::from_millis(900)).await;
        assert!(source.next_batch().await.is_err());
        assert_eq!(script.builds(), 1, "the deadline holds");

        // Past it: the next call rebuilds first, and the fresh instance has
        // good polls again.
        script.set_good_ops(5);
        tokio::time::advance(Duration::from_millis(200)).await;
        assert!(
            source.next_batch().await.is_ok(),
            "the rebuilt instance answers"
        );
        assert_eq!(script.builds(), 2, "exactly one rebuild");
    }

    #[tokio::test(start_paused = true)]
    async fn a_rebuilt_source_reporting_another_schema_is_discarded() {
        let script = Script::new(0);
        let mut source = healing_source(&script, 1, 0, "node-1");
        let pinned = source.schema();

        assert!(source.next_batch().await.is_err(), "the handle is dead");
        script.wrong_schema.store(true, Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(1_100)).await;

        let err = source
            .next_batch()
            .await
            .expect_err("a mismatched rebuild leaves the node down");
        assert!(
            err.to_string().contains("connector is down"),
            "the call reports the node is down: {err}"
        );
        assert_eq!(
            source.schema(),
            pinned,
            "the pinned schema outlives a rejected rebuild"
        );

        // The rejected instance was counted as a failed attempt, so the next
        // deadline is one step further out and a good build then lands.
        script.wrong_schema.store(false, Ordering::SeqCst);
        script.set_good_ops(1);
        tokio::time::advance(Duration::from_millis(2_100)).await;
        assert!(
            source.next_batch().await.is_ok(),
            "a matching rebuild is installed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_healed_source_keeps_its_flow_control_credit() {
        let script = Script::new(0);
        let mut source = healing_source(&script, 1, 0, "node-1");

        source.request_batch_rows(512);
        assert_eq!(script.last_hint.load(Ordering::SeqCst), 512);
        assert!(source.next_batch().await.is_err());

        script.last_hint.store(0, Ordering::SeqCst);
        script.set_good_ops(1);
        tokio::time::advance(Duration::from_millis(1_100)).await;
        assert!(source.next_batch().await.is_ok());
        assert_eq!(
            script.last_hint.load(Ordering::SeqCst),
            512,
            "the hint is re-applied to the rebuilt instance"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_stops_rebuilding_once_it_spends_max_attempts() {
        let script = Script::new(0);
        let mut source = healing_source(&script, 1, 2, "node-1");
        script.build_fails.store(true, Ordering::SeqCst);

        assert!(source.next_batch().await.is_err(), "the handle is dead");
        let after_first = script.builds();

        // Two scheduled attempts, both refused by the factory.
        tokio::time::advance(Duration::from_millis(1_100)).await;
        assert!(source.next_batch().await.is_err());
        tokio::time::advance(Duration::from_millis(2_100)).await;
        assert!(source.next_batch().await.is_err());
        assert_eq!(script.builds() - after_first, 2, "both attempts were spent");

        // Exhausted: no further call reaches the factory, however long it waits.
        script.build_fails.store(false, Ordering::SeqCst);
        script.set_good_ops(1);
        let spent = script.builds();
        tokio::time::advance(Duration::from_secs(600)).await;
        let err = source.next_batch().await.expect_err("the node gave up");
        assert!(err.to_string().contains("connector is down"), "{err}");
        assert_eq!(
            script.builds(),
            spent,
            "an exhausted node never touches its factory again"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_flapping_source_resumes_its_backoff_instead_of_restarting_it() {
        let script = Script::new(0);
        let mut source = healing_source(&script, 1, 0, "node-1");

        assert!(source.next_batch().await.is_err());
        tokio::time::advance(Duration::from_millis(1_100)).await;
        // The rebuild lands but the fresh instance fails its probe at once.
        assert!(source.next_batch().await.is_err(), "the probe fails");
        assert_eq!(script.builds(), 2);

        // A restarted schedule would rebuild again after 1000 ms; a resumed
        // one waits 2000 ms.
        tokio::time::advance(Duration::from_millis(1_100)).await;
        assert!(source.next_batch().await.is_err());
        assert_eq!(
            script.builds(),
            2,
            "the backoff resumed at the second step rather than restarting"
        );

        script.set_good_ops(1);
        tokio::time::advance(Duration::from_millis(1_000)).await;
        assert!(source.next_batch().await.is_ok());
        assert_eq!(script.builds(), 3);
    }

    /// `finish` on a node whose instance is down returns the same down
    /// error `next_batch` would, and never asks the factory for a rebuild:
    /// unlike `next_batch`, `finish` runs once at exit and never heals.
    #[tokio::test(start_paused = true)]
    async fn a_downed_source_finish_returns_the_down_error_without_rebuilding() {
        let script = Script::new(0);
        let mut source = healing_source(&script, 1, 0, "node-1");
        script.build_fails.store(true, Ordering::SeqCst);

        assert!(source.next_batch().await.is_err(), "the handle is dead");
        tokio::time::advance(Duration::from_millis(1_100)).await;
        assert!(
            source.next_batch().await.is_err(),
            "the rebuild attempt is refused, leaving the node down"
        );
        let builds_before = script.builds();

        let err = source
            .finish()
            .await
            .expect_err("no instance to make durable");
        assert!(
            err.to_string().contains("connector is down"),
            "finish reports the same down error next_batch would: {err}"
        );
        assert_eq!(
            script.builds(),
            builds_before,
            "finish never asks the factory for a rebuild"
        );
    }

    /// A failed `finish` feeds `Healer::failure`, exactly like a failed
    /// `next_batch`: the schedule it opens is what the next poll rebuilds
    /// from.
    #[tokio::test(start_paused = true)]
    async fn a_failed_source_finish_feeds_the_healer_so_the_next_poll_rebuilds() {
        let script = Script::new(0);
        let mut source = healing_source(&script, 1, 0, "node-1");
        script.finish_fails.store(true, Ordering::SeqCst);

        assert!(
            source.finish().await.is_err(),
            "finish surfaces the inner error"
        );
        assert_eq!(
            script.builds(),
            1,
            "the threshold schedules a rebuild, it does not perform one"
        );

        script.finish_fails.store(false, Ordering::SeqCst);
        script.set_good_ops(1);
        tokio::time::advance(Duration::from_millis(1_100)).await;
        assert!(
            source.next_batch().await.is_ok(),
            "the rebuild scheduled by the failed finish lands on the next poll"
        );
        assert_eq!(
            script.builds(),
            2,
            "exactly one rebuild, fed by finish's failure"
        );
    }

    /// A successful `finish` feeds `Healer::success`, which resets a pending
    /// schedule: a poll past the original deadline finds nothing due and
    /// polls the same dead instance again instead of rebuilding.
    #[tokio::test(start_paused = true)]
    async fn a_successful_source_finish_clears_a_pending_schedule() {
        let script = Script::new(0);
        let mut source = healing_source(&script, 1, 0, "node-1");

        assert!(
            source.next_batch().await.is_err(),
            "the handle is dead, a rebuild is scheduled"
        );
        assert!(
            source.finish().await.is_ok(),
            "the old instance can still be told to finish"
        );

        // Had the schedule survived, this poll past the original 1000 ms
        // deadline would rebuild; a successful finish reset the node to
        // healthy, so the dead instance is polled again instead.
        tokio::time::advance(Duration::from_millis(1_100)).await;
        assert!(
            source.next_batch().await.is_err(),
            "still the same dead instance"
        );
        assert_eq!(
            script.builds(),
            1,
            "finish's success cleared the pending rebuild"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_sink_whose_peer_returns_accepts_the_batch_after_the_heal() {
        let script = Script::new(1);
        let rebuilder = rebuilder(&script);
        let inner = rebuilder.build_sink().expect("first instance builds");
        let mut sink = HealingSink::new(inner, rebuilder, settings(1, 0), "wf", "node-1");
        let batch = one_row();

        assert!(sink.write_batch(&batch).await.is_ok());
        assert!(
            sink.write_batch(&batch).await.is_err(),
            "the peer went away"
        );

        script.set_good_ops(3);
        tokio::time::advance(Duration::from_millis(1_100)).await;
        assert!(
            sink.write_batch(&batch).await.is_ok(),
            "the rebuilt sink accepts the batch that follows the heal"
        );
        assert_eq!(script.builds(), 2);
    }

    /// The dead letter queue replays the moment a sink comes back, so the
    /// flag must latch on the first successful write after a rebuild and
    /// clear for whoever reads it next.
    #[tokio::test(start_paused = true)]
    async fn a_recovered_sink_latches_its_flag_once_for_its_reader() {
        let script = Script::new(1);
        let rebuilder = rebuilder(&script);
        let inner = rebuilder.build_sink().expect("first instance builds");
        let sink = HealingSink::new(inner, rebuilder, settings(1, 0), "wf", "node-1");
        let flag = sink.recovered_flag();
        let mut sink = sink;
        let batch = one_row();

        assert!(sink.write_batch(&batch).await.is_ok());
        assert!(
            !flag.swap(false, Ordering::Relaxed),
            "a sink that never failed has not recovered"
        );
        assert!(
            sink.write_batch(&batch).await.is_err(),
            "the peer went away"
        );

        script.set_good_ops(3);
        tokio::time::advance(Duration::from_millis(1_100)).await;
        assert!(sink.write_batch(&batch).await.is_ok(), "the rebuild took");

        assert!(
            flag.swap(false, Ordering::Relaxed),
            "the write that proved the rebuild sets the flag"
        );
        assert!(
            !flag.swap(false, Ordering::Relaxed),
            "the reader cleared it, so the next look reports nothing new"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_sink_that_is_down_reports_no_backlog_rather_than_zero() {
        let script = Script::new(0);
        let rebuilder = rebuilder(&script);
        let inner = rebuilder.build_sink().expect("first instance builds");
        let mut sink = HealingSink::new(inner, rebuilder, settings(1, 0), "wf", "node-1");

        assert!(sink.write_batch(&one_row()).await.is_err());
        script.build_fails.store(true, Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(1_100)).await;
        assert!(sink.write_batch(&one_row()).await.is_err());
        assert_eq!(
            sink.pending_rows(),
            None,
            "a node holding no instance knows no backlog, and a zero would \\
             read to flow control as a drained sink"
        );
    }

    #[cfg(feature = "metrics")]
    #[tokio::test(start_paused = true)]
    async fn a_heal_writes_both_the_unattributed_and_the_attributed_counters() {
        use prometheus::TextEncoder;

        let script = Script::new(0);
        // An id unlikely to collide with another test writing the same
        // process-global instruments.
        let mut source = healing_source(&script, 1, 0, "heal-probe-src");

        assert!(source.next_batch().await.is_err());
        script.wrong_schema.store(true, Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(1_100)).await;
        assert!(
            source.next_batch().await.is_err(),
            "the rebuild is rejected"
        );

        script.wrong_schema.store(false, Ordering::SeqCst);
        script.set_good_ops(1);
        tokio::time::advance(Duration::from_millis(2_100)).await;
        assert!(source.next_batch().await.is_ok(), "the rebuild lands");

        let text = TextEncoder::new()
            .encode_to_string(&crate::metrics::test_registry().gather())
            .expect("encode prometheus text");
        for series in [
            "saci_connector_heals_total",
            "saci_connector_heal_failures_total",
        ] {
            let lines: Vec<&str> = text
                .lines()
                .filter(|line| line.starts_with(series))
                .collect();
            assert!(
                lines
                    .iter()
                    .any(|line| line.contains(r#"source="heal-probe-src""#)),
                "{series} must carry the source attribute:\n{text}"
            );
            assert!(
                lines.iter().any(|line| !line.contains("source=")),
                "{series} must keep its unattributed form:\n{text}"
            );
        }
    }

    #[test]
    fn the_backoff_grows_to_its_cap_and_stays_there() {
        let s = settings(1, 0);
        assert_eq!(s.delay_for(1), Duration::from_millis(1_000));
        assert_eq!(s.delay_for(2), Duration::from_millis(2_000));
        assert_eq!(s.delay_for(3), Duration::from_millis(4_000));
        assert_eq!(
            s.delay_for(4),
            Duration::from_millis(4_000),
            "the cap holds"
        );
        assert_eq!(
            s.delay_for(u32::MAX),
            Duration::from_millis(4_000),
            "an unlimited schedule never overflows past the cap"
        );
    }

    /// The `Rebuilder` carries the node identity into every heal, so a
    /// `saci` source rebuilds on a fresh ephemeral port without losing the
    /// identity its factory requires.
    #[cfg(feature = "connector-saci")]
    #[test]
    fn a_rebuilt_saci_source_binds_a_fresh_port_and_keeps_its_identity() {
        let mut registry = Registry::new();
        registry.register_source(saci_connector_saci::SaciSourceFactory);
        registry.register_sink(saci_connector_saci::SaciSinkFactory);

        let config = from_kdl_str(
            r#"
bind "127.0.0.1:0"
schema_fields "v" type="Int64" nullable=#false
"#,
        )
        .expect("parse saci source config");

        let rebuilder = Rebuilder::new(
            Arc::new(registry),
            "saci",
            "node-1",
            config,
            None,
            None,
            None,
        )
        .with_identity(NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "wf".to_string(),
            node: "node-1".to_string(),
        });

        rebuilder
            .build_source()
            .expect("the first build binds an ephemeral port");
        rebuilder
            .build_source()
            .expect("a heal rebuild binds a fresh ephemeral port and keeps the identity");
    }
}
