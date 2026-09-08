//! [`Scheduler`]: multi-pipeline orchestrator with DAG scheduling.
//!
//! A `Scheduler` owns one or more [`Pipeline`]s and drives them forward on each
//! tick. Pipelines declare ordering and data-flow dependencies on other
//! pipelines via [`PipelineConfig`], so the scheduler can build a topological
//! stage plan and apply backpressure.
//!
//! ## Dependency kinds
//!
//! - [`DependencyKind::Order`]: run *after* the named pipeline, regardless of
//!   how many rows it produced.
//! - [`DependencyKind::Data`]: run *after* the named pipeline **and** skip this
//!   pipeline when the predecessor produced zero rows.
//!
//! ## Backpressure
//!
//! [`BackpressureSpec::Predicate`] pauses a pipeline when a caller-supplied
//! closure returns `true`. The closure can probe sink depth via
//! [`Pipeline::sink_pending_rows`](crate::pipeline::Pipeline::sink_pending_rows)
//! under the `io` feature.
//!
//! This is the embedder's mechanism, for a caller that builds a `Scheduler`
//! and drives `Pipeline`s itself. A `saci-service` workflow never constructs
//! one: its runners walk the declared node graph and get automatic per-source
//! admission control instead (`saci_service::service::flow`), which reads the
//! same `Sink::pending_rows` probe directly. The two answer the same
//! question and never share a call path, so a predicate written here has no
//! effect on a service workflow.
//!
//! ## Example
//!
//! ```rust,ignore
//! use saci_core::prelude::*;
//!
//! # #[tokio::main]
//! # async fn main() -> SaciResult<()> {
//! let p1 = Pipeline::new("ingest");
//! let p2 = Pipeline::new("enrich");
//!
//! let mut sched = Scheduler::new();
//! sched.add_pipeline(p1);
//! sched.add_pipeline(p2);
//! sched.tick().await?;
//! # Ok(())
//! # }
//! ```

use std::collections::{HashMap, VecDeque};
use std::sync::OnceLock;

use crate::error::{SaciError, SaciResult};
use crate::pipeline::Pipeline;

/// How one pipeline depends on another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DependencyKind {
    /// Run after the named pipeline completes (ordering only).
    Order,
    /// Run after the named pipeline **and** skip if it produced zero rows.
    Data,
}

/// Backpressure policy for a pipeline.
///
/// When the policy triggers the pipeline is skipped for the current tick.
pub enum BackpressureSpec {
    /// Skip the pipeline when this closure returns `true`.
    ///
    /// Receives the *current* [`Pipeline`] as argument.
    Predicate(Box<dyn Fn(&Pipeline) -> bool + Send + Sync>),
}

impl std::fmt::Debug for BackpressureSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Predicate(_) => f.write_str("Predicate(<fn>)"),
        }
    }
}

/// Per-pipeline scheduling configuration.
#[derive(Debug, Default)]
pub struct PipelineConfig {
    /// Dependencies on other pipelines by name.
    pub dependencies: Vec<(String, DependencyKind)>,
    /// Lower number = higher priority within a stage (default 0).
    pub priority: i32,
    /// Optional backpressure policy.
    pub backpressure: Option<BackpressureSpec>,
}

impl PipelineConfig {
    /// Create a default config with no dependencies or backpressure.
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare that this pipeline must run after `dep_name`.
    pub fn after(mut self, dep_name: impl Into<String>, kind: DependencyKind) -> Self {
        self.dependencies.push((dep_name.into(), kind));
        self
    }

    /// Set pipeline priority (lower = runs first within a stage).
    pub fn priority(mut self, p: i32) -> Self {
        self.priority = p;
        self
    }

    /// Set a predicate-based backpressure policy.
    pub fn backpressure(mut self, spec: BackpressureSpec) -> Self {
        self.backpressure = Some(spec);
        self
    }
}

/// Multi-pipeline orchestrator with DAG scheduling.
///
/// Owns a collection of [`Pipeline`]s and runs them on each [`tick`](Self::tick).
pub struct Scheduler {
    pipelines: Vec<Pipeline>,
    configs: Vec<PipelineConfig>,
    /// Cached topological stage plan: `Vec<stage>` where each stage is Vec<pipeline_index>.
    stage_plan: OnceLock<Result<Vec<Vec<usize>>, SaciError>>,
}

impl Scheduler {
    /// Create an empty scheduler.
    pub fn new() -> Self {
        Self {
            pipelines: Vec::new(),
            configs: Vec::new(),
            stage_plan: OnceLock::new(),
        }
    }

    /// Add a pipeline with a default [`PipelineConfig`].
    ///
    /// Pipelines are ticked in stage order; within a stage they run in
    /// registration order (modified by priority).
    pub fn add_pipeline(&mut self, p: Pipeline) -> &mut Self {
        self.pipelines.push(p);
        self.configs.push(PipelineConfig::default());
        self
    }

    /// Add a pipeline with an explicit [`PipelineConfig`].
    pub fn add_pipeline_with_config(&mut self, p: Pipeline, config: PipelineConfig) -> &mut Self {
        self.pipelines.push(p);
        self.configs.push(config);
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

    /// Ensure the stage plan is built, returning an error reference on failure.
    fn ensure_stage_plan(&self) -> SaciResult<()> {
        self.stage_plan
            .get_or_init(|| build_stages(&self.pipelines, &self.configs))
            .as_ref()
            .map(|_| ())
            .map_err(|e| SaciError::scheduler(e.to_string()))
    }

    fn is_backpressured(&self, idx: usize) -> bool {
        let Some(spec) = self.configs[idx].backpressure.as_ref() else {
            return false;
        };
        match spec {
            BackpressureSpec::Predicate(f) => f(&self.pipelines[idx]),
        }
    }

    /// Run all pipelines sequentially according to the DAG stage plan.
    ///
    /// Within each stage, pipelines are sorted by priority (lower first), then
    /// registration order. A pipeline is skipped when a `Data` dependency
    /// produced zero rows, or when it is under backpressure.
    ///
    /// Returns the first error encountered; remaining pipelines in that stage
    /// and all later stages do not run.
    pub async fn tick(&mut self) -> SaciResult<()> {
        self.ensure_stage_plan()?;

        let stages = self
            .stage_plan
            .get()
            .unwrap()
            .as_ref()
            .map_err(|e| SaciError::scheduler(e.to_string()))?
            .clone();

        // Track which pipelines were skipped so Data-dependents can be skipped too.
        let mut produced_zero: Vec<bool> = vec![false; self.pipelines.len()];

        for stage in &stages {
            // Sort by priority within stage (stable, preserves registration order for ties).
            let mut ordered: Vec<usize> = stage.clone();
            ordered.sort_by_key(|&idx| self.configs[idx].priority);

            for &idx in &ordered {
                if self.should_skip_data_dep(idx, &produced_zero) {
                    produced_zero[idx] = true;
                    continue;
                }
                if self.is_backpressured(idx) {
                    produced_zero[idx] = true;
                    continue;
                }

                #[cfg(feature = "io")]
                self.pipelines[idx].run_with_io().await?;
                #[cfg(not(feature = "io"))]
                self.pipelines[idx].run().await?;

                let rows = self.pipelines[idx].last_stats().rows_produced;
                produced_zero[idx] = rows == 0;
            }
        }

        Ok(())
    }

    fn should_skip_data_dep(&self, idx: usize, produced_zero: &[bool]) -> bool {
        for (dep_name, kind) in &self.configs[idx].dependencies {
            if *kind == DependencyKind::Data
                && let Some(dep_idx) = self.pipelines.iter().position(|p| p.name() == dep_name)
                && produced_zero[dep_idx]
            {
                return true;
            }
        }
        false
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
    #[cfg(feature = "runtime")]
    use crate::dataset::Dataset;
    #[cfg(feature = "runtime")]
    use crate::error::SaciError;
    #[cfg(feature = "runtime")]
    use crate::system::{System, SystemMeta};
    #[cfg(feature = "runtime")]
    use async_trait::async_trait;
    #[cfg(feature = "runtime")]
    use std::sync::{Arc, Mutex};

    #[cfg(feature = "runtime")]
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

        let mut p = Pipeline::new(name);
        p.add_system(BumpSystem { count: run_count });
        p
    }

    #[cfg(feature = "runtime")]
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

        let mut p = Pipeline::new(name);
        p.add_system(FailSystem);
        p
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
    fn test_add_pipeline_increments_count() {
        let mut sched = Scheduler::new();
        sched.add_pipeline(Pipeline::new("a"));
        sched.add_pipeline(Pipeline::new("b"));
        assert_eq!(sched.pipelines().len(), 2);
    }

    #[test]
    fn test_get_by_name_found() {
        let mut sched = Scheduler::new();
        sched.add_pipeline(Pipeline::new("alpha"));
        sched.add_pipeline(Pipeline::new("beta"));
        assert!(sched.get("alpha").is_some());
        assert!(sched.get("beta").is_some());
    }

    #[test]
    fn test_get_by_name_not_found() {
        let mut sched = Scheduler::new();
        sched.add_pipeline(Pipeline::new("alpha"));
        assert!(sched.get("missing").is_none());
    }

    #[test]
    fn test_get_mut_by_name() {
        let mut sched = Scheduler::new();
        sched.add_pipeline(Pipeline::new("target"));
        assert!(sched.get_mut("target").is_some());
        assert!(sched.get_mut("absent").is_none());
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_tick_runs_both_pipelines() {
        let count_a = Arc::new(Mutex::new(0usize));
        let count_b = Arc::new(Mutex::new(0usize));

        let mut sched = Scheduler::new();
        sched.add_pipeline(make_pipeline("a", Arc::clone(&count_a)));
        sched.add_pipeline(make_pipeline("b", Arc::clone(&count_b)));

        sched.tick().await.unwrap();

        assert_eq!(*count_a.lock().unwrap(), 1);
        assert_eq!(*count_b.lock().unwrap(), 1);
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_tick_returns_error_and_stops_early() {
        let count = Arc::new(Mutex::new(0usize));

        let mut sched = Scheduler::new();
        sched.add_pipeline(make_failing_pipeline("fail_first"));
        sched.add_pipeline(make_pipeline("second", Arc::clone(&count)));

        let result = sched.tick().await;
        assert!(result.is_err());
        // Second pipeline did not run (both in stage 0, fail aborts stage).
        assert_eq!(*count.lock().unwrap(), 0);
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_dag_order_dependency_respected() {
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

        fn make_ordered(name: &'static str, order: Arc<Mutex<Vec<&'static str>>>) -> Pipeline {
            struct OrderedSystem {
                name: &'static str,
                order: Arc<Mutex<Vec<&'static str>>>,
            }
            #[async_trait]
            impl System for OrderedSystem {
                fn meta(&self) -> SystemMeta {
                    SystemMeta::new("ordered")
                }
                async fn run(&self, _data: &mut Dataset) -> Result<(), SaciError> {
                    self.order.lock().unwrap().push(self.name);
                    Ok(())
                }
            }
            let mut p = Pipeline::new(name);
            p.add_system(OrderedSystem {
                name,
                order: Arc::clone(&order),
            });
            p
        }

        let mut sched = Scheduler::new();
        sched.add_pipeline(make_ordered("a", Arc::clone(&order)));
        sched.add_pipeline_with_config(
            make_ordered("b", Arc::clone(&order)),
            PipelineConfig::new().after("a", DependencyKind::Order),
        );

        sched.tick().await.unwrap();

        let result = order.lock().unwrap().clone();
        assert_eq!(result, vec!["a", "b"]);
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_dag_data_dependency_skips_when_zero_rows() {
        // a produces no rows (no systems, dataset empty → rows_produced = 0).
        let count_b = Arc::new(Mutex::new(0usize));

        let mut sched = Scheduler::new();
        sched.add_pipeline(Pipeline::new("a")); // no systems, no rows
        sched.add_pipeline_with_config(
            make_pipeline("b", Arc::clone(&count_b)),
            PipelineConfig::new().after("a", DependencyKind::Data),
        );

        sched.tick().await.unwrap();

        assert_eq!(*count_b.lock().unwrap(), 0);
    }

    #[test]
    fn test_dag_cycle_returns_error() {
        let mut sched = Scheduler::new();
        sched.add_pipeline_with_config(
            Pipeline::new("a"),
            PipelineConfig::new().after("b", DependencyKind::Order),
        );
        sched.add_pipeline_with_config(
            Pipeline::new("b"),
            PipelineConfig::new().after("a", DependencyKind::Order),
        );

        let result = build_stages(&sched.pipelines, &sched.configs);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cycle"));
    }

    #[test]
    fn test_dag_unknown_dep_returns_error() {
        let mut sched = Scheduler::new();
        sched.add_pipeline_with_config(
            Pipeline::new("a"),
            PipelineConfig::new().after("does_not_exist", DependencyKind::Order),
        );

        let result = build_stages(&sched.pipelines, &sched.configs);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("does_not_exist"));
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_priority_ordering_within_stage() {
        let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));

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
            let mut p = Pipeline::new(name);
            p.add_system(TrackSystem {
                name,
                order: Arc::clone(&order),
            });
            p
        }

        let mut sched = Scheduler::new();
        // Register in reverse priority order: high (10), then low (-1).
        sched.add_pipeline_with_config(
            make_tracker("high_num", Arc::clone(&order)),
            PipelineConfig::new().priority(10),
        );
        sched.add_pipeline_with_config(
            make_tracker("low_num", Arc::clone(&order)),
            PipelineConfig::new().priority(-1),
        );

        sched.tick().await.unwrap();

        let result = order.lock().unwrap().clone();
        // lower priority number → runs first
        assert_eq!(result, vec!["low_num", "high_num"]);
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_backpressure_predicate_skips_pipeline() {
        let count = Arc::new(Mutex::new(0usize));

        let mut sched = Scheduler::new();
        sched.add_pipeline_with_config(
            make_pipeline("throttled", Arc::clone(&count)),
            PipelineConfig::new().backpressure(BackpressureSpec::Predicate(Box::new(|_p| true))),
        );

        sched.tick().await.unwrap();

        assert_eq!(*count.lock().unwrap(), 0);
    }

    #[cfg(feature = "runtime")]
    #[tokio::test]
    async fn test_last_stats_after_tick() {
        let count = Arc::new(Mutex::new(0usize));
        let mut sched = Scheduler::new();
        sched.add_pipeline(make_pipeline("p", Arc::clone(&count)));

        sched.tick().await.unwrap();

        let stats = sched.get("p").unwrap().last_stats();
        assert_eq!(stats.systems_run, 1);
    }
}
