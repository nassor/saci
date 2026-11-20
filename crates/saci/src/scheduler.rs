//! [`Scheduler`]: multi-pipeline orchestrator with DAG scheduling.
//!
//! A `Scheduler` owns one or more [`Pipeline`]s and drives them forward on each
//! tick. Pipelines declare ordering and data-flow dependencies on other
//! pipelines via [`PipelineConfig`], so the scheduler can build a topological
//! stage plan and apply backpressure.
//!
//! ## Dependencies
//!
//! - [`PipelineConfig::after`]: run *after* the named pipeline, regardless of
//!   how many rows it produced.
//! - [`PipelineConfig::after_data`]: run *after* the named pipeline **and**
//!   skip this pipeline when the predecessor produced zero rows.
//!
//! ## Backpressure
//!
//! [`PipelineConfig::pause_when`] pauses a pipeline when a caller-supplied
//! closure returns `true`. The closure can probe sink depth via
//! [`Pipeline::sink_pending_rows`](crate::pipeline::Pipeline::sink_pending_rows).
//!
//! This is the embedder's mechanism, for a caller that builds a `Scheduler`
//! and drives `Pipeline`s itself. A service workflow never constructs
//! one: its runners walk the declared node graph and get automatic per-source
//! admission control instead (`crate::service::flow`), which reads the
//! same `Sink::pending_rows` probe directly. The two answer the same
//! question and never share a call path, so a predicate written here has no
//! effect on a service workflow.
//!
//! ## Example
//!
//! ```rust
//! use saci::prelude::*;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> SaciResult<()> {
//! let mut sched = Scheduler::new()
//!     .pipeline(Pipeline::new("ingest"))
//!     .pipeline_with(Pipeline::new("enrich"), PipelineConfig::new().after("ingest"));
//! sched.tick().await?;
//! # Ok(())
//! # }
//! ```

use std::collections::{HashMap, VecDeque};

use crate::error::{SaciError, SaciResult};
use crate::pipeline::Pipeline;

/// How one pipeline depends on another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DependencyKind {
    /// Run after the named pipeline completes (ordering only).
    Order,
    /// Run after the named pipeline **and** skip if it produced zero rows.
    Data,
}

/// A predicate that pauses a pipeline for the current tick.
type PausePredicate = Box<dyn Fn(&Pipeline) -> bool + Send + Sync>;

/// Per-pipeline scheduling configuration.
#[derive(Default)]
pub struct PipelineConfig {
    dependencies: Vec<(String, DependencyKind)>,
    priority: i32,
    pause_when: Option<PausePredicate>,
}

impl std::fmt::Debug for PipelineConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineConfig")
            .field("dependencies", &self.dependencies)
            .field("priority", &self.priority)
            .field("pause_when", &self.pause_when.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl PipelineConfig {
    /// Create a default config with no dependencies or backpressure.
    pub fn new() -> Self {
        Self::default()
    }

    /// Run after `pipeline`, whatever it produced.
    pub fn after(mut self, pipeline: impl Into<String>) -> Self {
        self.dependencies
            .push((pipeline.into(), DependencyKind::Order));
        self
    }

    /// Run after `pipeline`, and skip the tick when it produced zero rows.
    pub fn after_data(mut self, pipeline: impl Into<String>) -> Self {
        self.dependencies
            .push((pipeline.into(), DependencyKind::Data));
        self
    }

    /// Set pipeline priority (lower = runs first within a stage; default 0).
    pub fn priority(mut self, p: i32) -> Self {
        self.priority = p;
        self
    }

    /// Skip the pipeline for a tick whenever `f`, given the pipeline, returns
    /// `true`.
    pub fn pause_when(mut self, f: impl Fn(&Pipeline) -> bool + Send + Sync + 'static) -> Self {
        self.pause_when = Some(Box::new(f));
        self
    }
}

/// Multi-pipeline orchestrator with DAG scheduling.
///
/// Owns a collection of [`Pipeline`]s and runs them on each [`tick`](Self::tick).
pub struct Scheduler {
    pipelines: Vec<Pipeline>,
    configs: Vec<PipelineConfig>,
    /// Cached topological stage plan: `Vec<stage>` where each stage is
    /// `Vec<pipeline_index>`. `None` until the next tick after a change.
    stage_plan: Option<Vec<Vec<usize>>>,
}

impl Scheduler {
    /// Create an empty scheduler.
    pub fn new() -> Self {
        Self {
            pipelines: Vec::new(),
            configs: Vec::new(),
            stage_plan: None,
        }
    }

    /// Add a pipeline with a default [`PipelineConfig`].
    ///
    /// Pipelines are ticked in stage order; within a stage they run in
    /// registration order (modified by priority).
    pub fn pipeline(self, p: Pipeline) -> Self {
        self.pipeline_with(p, PipelineConfig::default())
    }

    /// Add a pipeline with an explicit [`PipelineConfig`].
    pub fn pipeline_with(mut self, p: Pipeline, config: PipelineConfig) -> Self {
        self.pipelines.push(p);
        self.configs.push(config);
        self.stage_plan = None;
        self
    }

    /// Return a shared reference to the pipeline with the given name, or `None`.
    pub fn get(&self, name: &str) -> Option<&Pipeline> {
        self.pipelines.iter().find(|p| p.name() == name)
    }

    /// Return an exclusive reference to the pipeline with the given name, or `None`.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Pipeline> {
        self.pipelines.iter_mut().find(|p| p.name() == name)
    }

    /// Return a slice over all registered pipelines.
    pub fn pipelines(&self) -> &[Pipeline] {
        &self.pipelines
    }

    fn is_paused(&self, idx: usize) -> bool {
        self.configs[idx]
            .pause_when
            .as_ref()
            .is_some_and(|f| f(&self.pipelines[idx]))
    }

    /// Run all pipelines sequentially according to the DAG stage plan.
    ///
    /// Within each stage, pipelines are sorted by priority (lower first), then
    /// registration order. A pipeline is skipped when an
    /// [`after_data`](PipelineConfig::after_data) dependency produced zero
    /// rows, or when its [`pause_when`](PipelineConfig::pause_when) holds.
    ///
    /// Returns the first error encountered; remaining pipelines in that stage
    /// and all later stages do not run.
    pub async fn tick(&mut self) -> SaciResult<()> {
        let stages = match self.stage_plan.take() {
            Some(stages) => stages,
            None => build_stages(&self.pipelines, &self.configs)?,
        };
        let result = self.run_stages(&stages).await;
        self.stage_plan = Some(stages);
        result
    }

    async fn run_stages(&mut self, stages: &[Vec<usize>]) -> SaciResult<()> {
        // Track which pipelines were skipped so Data-dependents can be skipped too.
        let mut produced_zero: Vec<bool> = vec![false; self.pipelines.len()];
        let mut ordered: Vec<usize> = Vec::new();

        for stage in stages {
            // Sort by priority within stage (stable, preserves registration order for ties).
            ordered.clear();
            ordered.extend_from_slice(stage);
            ordered.sort_by_key(|&idx| self.configs[idx].priority);

            for &idx in &ordered {
                if self.should_skip_data_dep(idx, &produced_zero) || self.is_paused(idx) {
                    produced_zero[idx] = true;
                    continue;
                }

                let stats = self.pipelines[idx].run().await?;
                produced_zero[idx] = stats.rows_produced == 0;
            }
        }

        Ok(())
    }

    fn should_skip_data_dep(&self, idx: usize, produced_zero: &[bool]) -> bool {
        self.configs[idx]
            .dependencies
            .iter()
            .any(|(dep_name, kind)| {
                *kind == DependencyKind::Data
                    && self
                        .pipelines
                        .iter()
                        .position(|p| p.name() == dep_name)
                        .is_some_and(|dep_idx| produced_zero[dep_idx])
            })
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

fn build_stages(
    pipelines: &[Pipeline],
    configs: &[PipelineConfig],
) -> Result<Vec<Vec<usize>>, SaciError> {
    let n = pipelines.len();
    if n == 0 {
        return Ok(Vec::new());
    }

    let name_to_idx: HashMap<&str, usize> = pipelines
        .iter()
        .enumerate()
        .map(|(i, p)| (p.name(), i))
        .collect();

    // adjacency[i] = list of pipeline indices that depend on i (i.e. i → dependent).
    let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); n];
    // in_degree[i] = number of pipelines that i depends on.
    let mut in_degree: Vec<usize> = vec![0; n];

    for (idx, config) in configs.iter().enumerate() {
        for (dep_name, _kind) in &config.dependencies {
            let dep_idx = name_to_idx.get(dep_name.as_str()).copied().ok_or_else(|| {
                SaciError::configuration(format!(
                    "pipeline '{}' declares dependency on unknown pipeline '{dep_name}'",
                    pipelines[idx].name()
                ))
            })?;
            adjacency[dep_idx].push(idx);
            in_degree[idx] += 1;
        }
    }

    // Kahn's algorithm.
    let mut queue: VecDeque<usize> = (0..n).filter(|&i| in_degree[i] == 0).collect();
    let mut stages: Vec<Vec<usize>> = Vec::new();
    let mut visited = 0usize;

    while !queue.is_empty() {
        let stage: Vec<usize> = queue.drain(..).collect();
        visited += stage.len();

        let mut next_queue: VecDeque<usize> = VecDeque::new();
        for &node in &stage {
            for &dep in &adjacency[node] {
                in_degree[dep] -= 1;
                if in_degree[dep] == 0 {
                    next_queue.push_back(dep);
                }
            }
        }

        stages.push(stage);
        queue = next_queue;
    }

    if visited != n {
        return Err(SaciError::scheduler(
            "pipeline dependency graph contains a cycle".to_string(),
        ));
    }

    Ok(stages)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "engine")]
    use crate::dataset::Dataset;
    #[cfg(feature = "engine")]
    use crate::system::{System, SystemMeta};
    #[cfg(feature = "engine")]
    use async_trait::async_trait;
    #[cfg(feature = "engine")]
    use std::sync::{Arc, Mutex};

    #[cfg(feature = "engine")]
    fn make_pipeline(name: &'static str, run_count: Arc<Mutex<usize>>) -> Pipeline {
        #[derive(Clone)]
        struct BumpSystem {
            count: Arc<Mutex<usize>>,
        }

        #[async_trait]
        impl System for BumpSystem {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("bump")
            }
            async fn run(&self, _data: &mut Dataset) -> Result<(), SaciError> {
                *self.count.lock().unwrap() += 1;
                Ok(())
            }
        }

        Pipeline::new(name).system(BumpSystem { count: run_count })
    }

    #[cfg(feature = "engine")]
    fn make_failing_pipeline(name: &'static str) -> Pipeline {
        struct FailSystem;

        #[async_trait]
        impl System for FailSystem {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("fail")
            }
            async fn run(&self, _data: &mut Dataset) -> Result<(), SaciError> {
                Err(SaciError::generic("intentional failure"))
            }
        }

        Pipeline::new(name).system(FailSystem)
    }

    #[cfg(feature = "engine")]
    fn make_tracker(name: &'static str, order: Arc<Mutex<Vec<&'static str>>>) -> Pipeline {
        struct TrackSystem {
            name: &'static str,
            order: Arc<Mutex<Vec<&'static str>>>,
        }
        #[async_trait]
        impl System for TrackSystem {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("track")
            }
            async fn run(&self, _data: &mut Dataset) -> Result<(), SaciError> {
                self.order.lock().unwrap().push(self.name);
                Ok(())
            }
        }
        Pipeline::new(name).system(TrackSystem { name, order })
    }

    #[test]
    fn test_scheduler_new_empty() {
        let sched = Scheduler::new();
        assert_eq!(sched.pipelines().len(), 0);
    }

    #[test]
    fn test_scheduler_default_empty() {
        let sched = Scheduler::default();
        assert_eq!(sched.pipelines().len(), 0);
    }

    #[test]
    fn test_pipeline_increments_count() {
        let sched = Scheduler::new()
            .pipeline(Pipeline::new("a"))
            .pipeline(Pipeline::new("b"));
        assert_eq!(sched.pipelines().len(), 2);
    }

    #[test]
    fn test_get_by_name() {
        let mut sched = Scheduler::new()
            .pipeline(Pipeline::new("alpha"))
            .pipeline(Pipeline::new("beta"));
        assert!(sched.get("alpha").is_some());
        assert!(sched.get("beta").is_some());
        assert!(sched.get("missing").is_none());
        assert!(sched.get_mut("alpha").is_some());
        assert!(sched.get_mut("absent").is_none());
    }

    #[cfg(feature = "engine")]
    #[tokio::test]
    async fn test_tick_runs_both_pipelines() {
        let count_a = Arc::new(Mutex::new(0usize));
        let count_b = Arc::new(Mutex::new(0usize));

        let mut sched = Scheduler::new()
            .pipeline(make_pipeline("a", Arc::clone(&count_a)))
            .pipeline(make_pipeline("b", Arc::clone(&count_b)));

        sched.tick().await.unwrap();

        assert_eq!(*count_a.lock().unwrap(), 1);
        assert_eq!(*count_b.lock().unwrap(), 1);
    }

    #[cfg(feature = "engine")]
    #[tokio::test]
    async fn test_tick_returns_error_and_stops_early() {
        let count = Arc::new(Mutex::new(0usize));

        let mut sched = Scheduler::new()
            .pipeline(make_failing_pipeline("fail_first"))
            .pipeline(make_pipeline("second", Arc::clone(&count)));

        let result = sched.tick().await;
        assert!(result.is_err());
        // Second pipeline did not run (both in stage 0, fail aborts stage).
        assert_eq!(*count.lock().unwrap(), 0);
    }

    /// The plan is rebuilt after a change, so a pipeline added between ticks
    /// runs on the next one.
    #[cfg(feature = "engine")]
    #[tokio::test]
    async fn a_pipeline_added_after_a_tick_runs_on_the_next() {
        let count_a = Arc::new(Mutex::new(0usize));
        let count_b = Arc::new(Mutex::new(0usize));

        let mut sched = Scheduler::new().pipeline(make_pipeline("a", Arc::clone(&count_a)));
        sched.tick().await.unwrap();

        let mut sched = sched.pipeline(make_pipeline("b", Arc::clone(&count_b)));
        sched.tick().await.unwrap();

        assert_eq!(*count_a.lock().unwrap(), 2);
        assert_eq!(*count_b.lock().unwrap(), 1);
    }

    #[cfg(feature = "engine")]
    #[tokio::test]
    async fn test_dag_order_dependency_respected() {
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        // Registered dependent-first, so only the dependency puts `a` first.
        let mut sched = Scheduler::new()
            .pipeline_with(
                make_tracker("b", Arc::clone(&order)),
                PipelineConfig::new().after("a"),
            )
            .pipeline(make_tracker("a", Arc::clone(&order)));

        sched.tick().await.unwrap();

        let result = order.lock().unwrap().clone();
        assert_eq!(result, vec!["a", "b"]);
    }

    #[cfg(feature = "engine")]
    #[tokio::test]
    async fn test_dag_data_dependency_skips_when_zero_rows() {
        // a produces no rows (no systems, dataset empty → rows_produced = 0).
        let count_b = Arc::new(Mutex::new(0usize));

        let mut sched = Scheduler::new().pipeline(Pipeline::new("a")).pipeline_with(
            make_pipeline("b", Arc::clone(&count_b)),
            PipelineConfig::new().after_data("a"),
        );

        sched.tick().await.unwrap();

        assert_eq!(*count_b.lock().unwrap(), 0);
    }

    #[test]
    fn test_dag_cycle_returns_error() {
        let sched = Scheduler::new()
            .pipeline_with(Pipeline::new("a"), PipelineConfig::new().after("b"))
            .pipeline_with(Pipeline::new("b"), PipelineConfig::new().after("a"));

        let err = build_stages(&sched.pipelines, &sched.configs)
            .unwrap_err()
            .to_string();
        assert!(err.contains("cycle"));
    }

    #[test]
    fn test_dag_unknown_dep_returns_error() {
        let sched = Scheduler::new().pipeline_with(
            Pipeline::new("a"),
            PipelineConfig::new().after("does_not_exist"),
        );

        let err = build_stages(&sched.pipelines, &sched.configs)
            .unwrap_err()
            .to_string();
        assert!(err.contains("does_not_exist"));
    }

    #[cfg(feature = "engine")]
    #[tokio::test]
    async fn test_priority_ordering_within_stage() {
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        // Register in reverse priority order: high (10), then low (-1).
        let mut sched = Scheduler::new()
            .pipeline_with(
                make_tracker("high_num", Arc::clone(&order)),
                PipelineConfig::new().priority(10),
            )
            .pipeline_with(
                make_tracker("low_num", Arc::clone(&order)),
                PipelineConfig::new().priority(-1),
            );

        sched.tick().await.unwrap();

        let result = order.lock().unwrap().clone();
        // lower priority number → runs first
        assert_eq!(result, vec!["low_num", "high_num"]);
    }

    #[cfg(feature = "engine")]
    #[tokio::test]
    async fn test_pause_when_skips_pipeline() {
        let count = Arc::new(Mutex::new(0usize));

        let mut sched = Scheduler::new().pipeline_with(
            make_pipeline("throttled", Arc::clone(&count)),
            PipelineConfig::new().pause_when(|_p| true),
        );

        sched.tick().await.unwrap();

        assert_eq!(*count.lock().unwrap(), 0);
    }
}
