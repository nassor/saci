use std::time::Instant;

use crate::dataset::Dataset;
use crate::error::{SaciError, SaciResult};
use crate::pipeline::RunStats;
use crate::retry::RetryMode;

#[cfg(feature = "engine")]
use std::sync::Arc;

#[cfg(feature = "engine")]
use crate::system::{
    ParallelSystem, SLICE_PARALLEL_THRESHOLD, STAGE_INLINE_THRESHOLD, SliceWriteSet, WriteSet,
    merge_entries,
};

#[cfg(feature = "engine")]
use crate::io::{read_all, write_all};

#[cfg(feature = "tracing")]
use tracing::{Instrument as _, info_span};

use super::dag::{Plan, SystemEntry};
use super::{Pipeline, ensure_plan};

/// Returns number of retries before success (0 = first attempt succeeded).
async fn run_arrow_system_with_retries(
    sys: &dyn crate::system::System,
    mode: &RetryMode,
    data: &mut Dataset,
) -> SaciResult<u32> {
    let (_, retries) = crate::retry::run_with_retries(mode, async || match sys.run_sync(data) {
        Some(sync_result) => sync_result,
        None => sys.run(data).await,
    })
    .await?;
    Ok(retries)
}

/// Returns `(write_set, retries)` where retries = 0 means first attempt succeeded.
#[cfg(feature = "engine")]
async fn run_parallel_system_with_retries(
    sys: Arc<dyn ParallelSystem>,
    mode: &RetryMode,
    data: &Dataset,
) -> SaciResult<(WriteSet, u32)> {
    let use_slices = data.rows() as u32 >= SLICE_PARALLEL_THRESHOLD && sys.sliced();
    crate::retry::run_with_retries(mode, async || {
        if use_slices {
            run_parallel_system_sliced(Arc::clone(&sys), data).await
        } else if let Some(result) = sys.run_sync(data) {
            result
        } else {
            sys.run(data).await
        }
    })
    .await
}

/// Logical CPU count, resolved once.
///
/// `num_cpus::get()` inspects the OS on every call; this is on the per-batch
/// path for every sliced system, so it is cached.
#[cfg(feature = "engine")]
fn cpu_count() -> u32 {
    static N: std::sync::LazyLock<u32> = std::sync::LazyLock::new(|| num_cpus::get().max(1) as u32);
    *N
}

/// Chunks to create per logical CPU when slicing a system.
///
/// Oversubscribing rayon gives it work to steal, which matters because SMT
/// siblings and dual-CCD parts do not retire equal chunks in equal time.
/// Throughput still improves up to 16x per CPU on a 32-CPU part, but the gain
/// is workload-dependent: systems whose slices are concatenated by
/// `merge_slices` pay a per-chunk copy, and narrow scans regress a few percent
/// past 4x. Hence a default of 4x, overridable with
/// `SACI_SLICE_CHUNKS_PER_CPU`.
#[cfg(feature = "engine")]
fn chunks_per_cpu() -> u32 {
    static N: std::sync::LazyLock<u32> = std::sync::LazyLock::new(|| {
        std::env::var("SACI_SLICE_CHUNKS_PER_CPU")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(4)
    });
    *N
}

/// Rows below which splitting further stops paying for itself.
#[cfg(feature = "engine")]
const MIN_ROWS_PER_CHUNK: u32 = 4_096;

/// Split `total_rows` into equal-ish ranges for parallel dispatch.
///
/// Chunk count is clamped to `[cpu_count, cpu_count * chunks_per_cpu]`: never
/// fewer than one chunk per CPU, so a large batch always fills the machine, and
/// never so many that chunks fall below [`MIN_ROWS_PER_CHUNK`].
#[cfg(feature = "engine")]
fn compute_row_ranges(total_rows: u32) -> Vec<std::ops::Range<u32>> {
    let cpus = cpu_count();
    let by_rows = total_rows / MIN_ROWS_PER_CHUNK;
    let num_chunks = by_rows.clamp(cpus, cpus.saturating_mul(chunks_per_cpu()));
    let chunk_size = total_rows.div_ceil(num_chunks);
    (0..num_chunks)
        .map(|i| {
            let start = i * chunk_size;
            let end = ((i + 1) * chunk_size).min(total_rows);
            start..end
        })
        .filter(|r| !r.is_empty())
        .collect()
}

/// Run `sys` over every row range of `data` on rayon, then merge.
#[cfg(feature = "engine")]
fn run_slices(sys: &dyn ParallelSystem, data: &Dataset) -> Result<WriteSet, SaciError> {
    use rayon::prelude::*;

    let slices = compute_row_ranges(data.rows() as u32)
        .into_par_iter()
        .map(|range| sys.run_slice(data, range))
        .collect::<Result<Vec<SliceWriteSet>, SaciError>>()?;
    sys.merge_slices(slices)
}

#[cfg(feature = "engine")]
async fn run_parallel_system_sliced(
    sys: Arc<dyn ParallelSystem>,
    data: &Dataset,
) -> Result<WriteSet, SaciError> {
    // SAFETY: spawn_blocking is awaited before return; data outlives the task.
    let world_ptr = data as *const Dataset as usize;

    tokio::task::spawn_blocking(move || {
        let data = unsafe { &*(world_ptr as *const Dataset) };
        run_slices(sys.as_ref(), data)
    })
    .await
    .map_err(|e| SaciError::generic(format!("spawn_blocking join error: {e}")))
    .and_then(|r| r)
}

/// Returns total retry count across all systems in the stage.
#[cfg(feature = "engine")]
async fn run_parallel_stage(
    systems: &[SystemEntry],
    plan: &Plan,
    stage: &[usize],
    data: &mut Dataset,
) -> SaciResult<u32> {
    struct TaskDesc {
        sys: Arc<dyn ParallelSystem>,
        mode: RetryMode,
        use_slices: bool,
        #[cfg_attr(not(feature = "tracing"), allow(dead_code))]
        name: &'static str,
    }

    let world_rows = data.rows() as u32;

    let mut task_descs: Vec<TaskDesc> = Vec::with_capacity(stage.len());
    for &sys_idx in stage {
        if let SystemEntry::Parallel(sys) = &systems[sys_idx] {
            task_descs.push(TaskDesc {
                sys: Arc::clone(sys),
                mode: plan.retries[sys_idx],
                use_slices: world_rows >= SLICE_PARALLEL_THRESHOLD && sys.sliced(),
                name: plan.metas[sys_idx].name,
            });
        }
    }

    // SAFETY: all JoinHandles are awaited before `data` is next accessed.
    let world_ptr = data as *const Dataset as usize;

    let handle = tokio::runtime::Handle::current();
    let num_tasks = task_descs.len();

    // `spawn_blocking` runs outside the task-local context, so the stage span
    // is captured here and re-entered inside the closure.
    #[cfg(feature = "tracing")]
    let stage_span = tracing::Span::current();

    let handles: Vec<tokio::task::JoinHandle<Result<(WriteSet, u32), SaciError>>> = task_descs
        .into_iter()
        .map(|desc| {
            let handle = handle.clone();
            #[cfg(feature = "tracing")]
            let stage_span = stage_span.clone();
            tokio::task::spawn_blocking(move || {
                // The closure body has no `.await` (only `handle.block_on`), so
                // plain guards are correct, as in `run_with_retries_blocking`.
                #[cfg(feature = "tracing")]
                let _stage_guard = stage_span.enter();
                #[cfg(feature = "tracing")]
                let system_span = info_span!("system.execute", system = desc.name);
                #[cfg(feature = "tracing")]
                let _system_guard = system_span.enter();

                // SAFETY: world_ptr is valid; all handles are awaited before return.
                let data = unsafe { &*(world_ptr as *const Dataset) };

                crate::retry::run_with_retries_blocking(&desc.mode, || {
                    if desc.use_slices {
                        run_slices(desc.sys.as_ref(), data)
                    } else if let Some(result) = desc.sys.run_sync(data) {
                        result
                    } else {
                        handle.block_on(desc.sys.run(data))
                    }
                })
            })
        })
        .collect();

    type StageTaskResult = Result<Result<(WriteSet, u32), SaciError>, tokio::task::JoinError>;

    // Await ALL handles before inspecting results (SAFETY: raw ptr must not alias).
    let mut join_results: Vec<StageTaskResult> = Vec::with_capacity(num_tasks);
    for handle in handles {
        join_results.push(handle.await);
    }

    let mut write_sets: Vec<WriteSet> = Vec::with_capacity(num_tasks);
    let mut total_retries: u32 = 0;
    for result in join_results {
        let (ws, retries) =
            result.map_err(|e| SaciError::generic(format!("parallel stage task join: {e}")))??;
        write_sets.push(ws);
        total_retries += retries;
    }

    #[cfg(debug_assertions)]
    {
        let mut seen_keys = std::collections::HashSet::new();
        for ws in &write_sets {
            for (component, field, _) in ws.iter() {
                debug_assert!(
                    seen_keys.insert((component, field)),
                    "Two ParallelSystems in the same stage both write field '{component}.{field}'."
                );
            }
        }
    }

    let mut fields = Vec::new();
    let mut resource_updates = Vec::new();
    for ws in write_sets {
        let (ws_fields, ws_updates) = ws.into_parts();
        merge_entries(&mut fields, ws_fields);
        resource_updates.extend(ws_updates);
    }

    data.apply_write_set(WriteSet::from_parts(fields, resource_updates))?;
    Ok(total_retries)
}

/// Execute one stage against `data`, returning `(systems_run, retries)` for
/// that stage alone.
///
/// Separate from [`run_stages`] so the `pipeline.stage` span can wrap the whole
/// future: the body awaits, and a span guard held across an await attributes
/// another task's work to this stage.
async fn run_stage(
    systems: &[SystemEntry],
    plan: &Plan,
    stage: &[usize],
    data: &mut Dataset,
) -> SaciResult<(usize, u32)> {
    // Concurrent dispatch of a whole stage costs a `spawn_blocking` per
    // system plus write-set merging; below `STAGE_INLINE_THRESHOLD` rows
    // that dominates the work itself, so fall through to the sequential
    // loop. Systems sharing a stage are non-conflicting by construction, so
    // the result is identical either way.
    #[cfg(feature = "engine")]
    if stage.len() > 1
        && data.rows() as u32 >= STAGE_INLINE_THRESHOLD
        && stage.iter().all(|&idx| systems[idx].is_parallel())
    {
        let retries = run_parallel_stage(systems, plan, stage, data).await?;
        return Ok((stage.len(), retries));
    }

    let mut systems_run = 0usize;
    let mut retries: u32 = 0;

    for &sys_idx in stage {
        let mode = &plan.retries[sys_idx];
        #[cfg(feature = "tracing")]
        let span = info_span!("system.execute", system = plan.metas[sys_idx].name);

        match &systems[sys_idx] {
            SystemEntry::Sequential(sys) => {
                let fut = run_arrow_system_with_retries(sys.as_ref(), mode, data);
                #[cfg(feature = "tracing")]
                let system_retries = fut.instrument(span).await?;
                #[cfg(not(feature = "tracing"))]
                let system_retries = fut.await?;
                retries += system_retries;
            }
            #[cfg(feature = "engine")]
            SystemEntry::Parallel(sys) => {
                let fut = run_parallel_system_with_retries(Arc::clone(sys), mode, data);
                #[cfg(feature = "tracing")]
                let (write_set, system_retries) = fut.instrument(span).await?;
                #[cfg(not(feature = "tracing"))]
                let (write_set, system_retries) = fut.await?;
                data.apply_write_set(write_set)?;
                retries += system_retries;
            }
            #[cfg(not(feature = "engine"))]
            SystemEntry::Parallel(_) => {
                return Err(SaciError::configuration(
                    "ParallelSystem is not supported on the processor path; register a System",
                ));
            }
        }
        systems_run += 1;
    }

    Ok((systems_run, retries))
}

/// Execute all stages against `data`, returning `(systems_run, retries)`.
async fn run_stages(
    systems: &[SystemEntry],
    plan: &Plan,
    data: &mut Dataset,
) -> SaciResult<(usize, u32)> {
    let mut systems_run = 0usize;
    let mut retries: u32 = 0;

    for (stage_idx, stage) in plan.stages.iter().enumerate() {
        let fut = run_stage(systems, plan, stage, data);
        #[cfg(feature = "tracing")]
        let (ran, stage_retries) = fut
            .instrument(info_span!(
                "pipeline.stage",
                stage = stage_idx,
                systems = stage.len()
            ))
            .await?;
        #[cfg(not(feature = "tracing"))]
        let (ran, stage_retries) = {
            let _ = stage_idx;
            fut.await?
        };
        systems_run += ran;
        retries += stage_retries;
    }

    Ok((systems_run, retries))
}

/// Run every stage of `plan` against `data` once, under one `pipeline.run`
/// span.
async fn exec(
    name: &str,
    systems: &[SystemEntry],
    plan: &Plan,
    data: &mut Dataset,
) -> SaciResult<RunStats> {
    let start = Instant::now();
    let rows_before = data.live_rows() as isize;
    let (systems_run, retries) = exec_stages(name, systems, plan, data).await?;
    Ok(RunStats {
        rows_produced: data.live_rows() as isize - rows_before,
        systems_run,
        elapsed: start.elapsed(),
        retries,
    })
}

/// [`exec`] without the timing and row accounting, for a caller that runs it
/// once per stream item and measures the whole stream instead.
async fn exec_stages(
    name: &str,
    systems: &[SystemEntry],
    plan: &Plan,
    data: &mut Dataset,
) -> SaciResult<(usize, u32)> {
    if plan.stages.is_empty() {
        return Ok((0, 0));
    }

    #[cfg(feature = "tracing")]
    let span = info_span!(
        "pipeline.run",
        pipeline = %name,
        stages = plan.stages.len(),
        rows = data.rows()
    );
    #[cfg(not(feature = "tracing"))]
    let _ = name;
    let fut = run_stages(systems, plan, data);
    #[cfg(feature = "tracing")]
    let fut = fut.instrument(span);
    fut.await
}

impl Pipeline {
    /// Run the pipeline once against its own dataset.
    ///
    /// Each attached source is drained into its component first; then every
    /// stage runs; then each attached sink receives its component's rows and
    /// is finished. With nothing attached, only the stages run.
    ///
    /// # Errors
    ///
    /// The first construction or validation error, then the first source,
    /// system or sink error.
    pub async fn run(&mut self) -> SaciResult<RunStats> {
        let Self {
            name,
            data,
            systems,
            plan,
            pending_error,
            #[cfg(feature = "engine")]
            sources,
            #[cfg(feature = "engine")]
            sinks,
            ..
        } = self;
        let plan = ensure_plan(plan, pending_error.as_ref(), systems, data.schemas())?;

        #[cfg(feature = "engine")]
        if !sources.is_empty() || !sinks.is_empty() {
            let start = Instant::now();
            let rows_before = data.live_rows() as isize;
            for (comp, src) in sources.iter_mut() {
                read_all(src.as_mut(), data, comp).await?;
            }
            let stats = exec(name, systems, plan, data).await?;
            for (comp, sink) in sinks.iter_mut() {
                write_all(sink.as_mut(), data, comp).await?;
                sink.finish().await?;
            }
            return Ok(RunStats {
                rows_produced: data.live_rows() as isize - rows_before,
                elapsed: start.elapsed(),
                ..stats
            });
        }

        exec(name, systems, plan, data).await
    }

    /// Run every stage against a separately provided `Dataset`.
    ///
    /// For hosts that own their datasets: a distributed runner, a processor
    /// SDK. Attached sources and sinks are ignored.
    ///
    /// # Errors
    ///
    /// The first construction or validation error, then the first system
    /// error.
    pub async fn run_on(&self, data: &mut Dataset) -> SaciResult<RunStats> {
        let plan = ensure_plan(
            &self.plan,
            self.pending_error.as_ref(),
            &self.systems,
            data.schemas(),
        )?;
        exec(&self.name, &self.systems, plan, data).await
    }

    /// Process each source batch individually, end-to-end, as it arrives.
    ///
    /// Where [`run`](Self::run) drains the source to EOF, runs once over
    /// everything, then flushes, this pulls one
    /// [`RecordBatch`](arrow_array::RecordBatch) at a time and runs the full
    /// system DAG plus every sink for that item alone. Sinks are finalised
    /// once, after the source reaches EOF.
    ///
    /// Requires exactly one source; zero or more sinks are fine.
    ///
    /// Each item is processed against a cleared dataset, and [`Dataset::clear`]
    /// drops per-run resources, so state that must survive across items has to
    /// live inside the system structs themselves.
    ///
    /// # Errors
    ///
    /// Any source, system or sink error aborts the stream and is returned;
    /// retry-and-continue policy belongs to the caller.
    #[cfg(feature = "engine")]
    pub async fn run_stream(&mut self) -> SaciResult<RunStats> {
        let Self {
            name,
            data,
            systems,
            plan,
            pending_error,
            sources,
            sinks,
            ..
        } = self;
        if sources.len() != 1 {
            return Err(SaciError::configuration(format!(
                "run_stream requires exactly one source; {} configured",
                sources.len()
            )));
        }
        let plan = ensure_plan(plan, pending_error.as_ref(), systems, data.schemas())?;

        let start = Instant::now();
        let mut total = RunStats::default();
        let (comp, source) = (sources[0].0, &mut sources[0].1);

        while let Some(batch) = source.next_batch().await? {
            data.clear();
            data.append_record_batch(comp, batch)?;
            let (systems_run, retries) = exec_stages(name, systems, plan, data).await?;
            total.systems_run += systems_run;
            total.retries += retries;

            for (sink_comp, sink) in sinks.iter_mut() {
                write_all(sink.as_mut(), data, sink_comp).await?;
            }
            total.rows_produced += data.live_rows() as isize;
        }

        for (_, sink) in sinks.iter_mut() {
            sink.finish().await?;
        }

        total.elapsed = start.elapsed();
        Ok(total)
    }
}

// Every test here drives `Pipeline::run` / `run_on` through the tokio+rayon
// stage executor, so the module is gated on `engine`. The processor path is
// covered end-to-end by `saci-processor-smoketest` against a real component.
#[cfg(all(test, feature = "engine"))]
mod tests {
    use std::sync::Arc;

    use arrow_array::Float64Array;
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;
    use crate::error::SaciError;
    use crate::pipeline::Pipeline;
    use crate::retry::RetryMode;
    use crate::row::Row;
    use crate::system::{FieldRef, System, SystemMeta, WriteSet};

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct ExecOrder {
        id: u64,
        total: f64,
    }

    impl ExecOrder {
        const TOTAL: FieldRef<ExecOrder> = FieldRef::new("total");
    }

    impl Component for ExecOrder {
        fn name() -> &'static str {
            "ExecOrder"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new("total", DataType::Float64, false),
            ]))
        }
    }

    fn make_orders(n: usize) -> Vec<ExecOrder> {
        (0..n)
            .map(|i| ExecOrder {
                id: i as u64,
                total: i as f64 * 10.0,
            })
            .collect()
    }

    fn orders_pipeline(name: &str, n: usize) -> Pipeline {
        let mut p = Pipeline::new(name).component::<ExecOrder>();
        p.data_mut().append::<ExecOrder>(&make_orders(n)).unwrap();
        p
    }

    struct BumpSystem {
        field: &'static str,
    }

    #[async_trait::async_trait]
    impl System for BumpSystem {
        fn meta(&self) -> SystemMeta {
            SystemMeta::new("bump")
                .reads_named("ExecOrder", "id")
                .writes_named("ExecOrder", self.field)
        }

        async fn run(&self, data: &mut Dataset) -> crate::SaciResult<()> {
            let total_col = data
                .column_named("ExecOrder", self.field)?
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .clone();
            let bumped: Float64Array = total_col.iter().map(|v| v.map(|x| x + 1.0)).collect();
            let ws = WriteSet::new().put_named("ExecOrder", self.field, Arc::new(bumped));
            data.apply_write_set(ws)
        }
    }

    /// Fails `fail_times` times with a retryable error, then succeeds.
    struct FlakySystem {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        fail_times: usize,
    }

    #[async_trait::async_trait]
    impl System for FlakySystem {
        fn meta(&self) -> SystemMeta {
            SystemMeta::new("flaky").writes_named("ExecOrder", "total")
        }

        fn retry(&self) -> RetryMode {
            RetryMode::fixed(5, std::time::Duration::ZERO)
        }

        async fn run(&self, _data: &mut Dataset) -> crate::SaciResult<()> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < self.fail_times {
                Err(SaciError::system_execution("transient"))
            } else {
                Ok(())
            }
        }
    }

    fn flaky(fail_times: usize) -> FlakySystem {
        FlakySystem {
            calls: Arc::default(),
            fail_times,
        }
    }

    #[tokio::test]
    async fn test_pipeline_run_single_system() {
        let mut p = orders_pipeline("test", 5).system(BumpSystem { field: "total" });

        p.run().await.unwrap();

        let col = p.data().column(ExecOrder::TOTAL).unwrap();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 1.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_run_on_uses_external_dataset() {
        let p = Pipeline::new("template");

        let mut ext = Dataset::new();
        ext.register::<ExecOrder>().unwrap();
        ext.append::<ExecOrder>(&make_orders(3)).unwrap();

        p.run_on(&mut ext).await.unwrap();
        assert_eq!(ext.rows(), 3);
    }

    #[tokio::test]
    async fn test_retries_counts_failures_before_success() {
        let mut p = orders_pipeline("retry-test", 2).system(flaky(3));

        let stats = p.run().await.unwrap();
        assert_eq!(stats.retries, 3);
    }

    #[tokio::test]
    async fn test_retries_zero_on_clean_run() {
        let mut p = orders_pipeline("no-retry-test", 2).system(BumpSystem { field: "total" });

        let stats = p.run().await.unwrap();
        assert_eq!(stats.retries, 0);
    }

    #[tokio::test]
    async fn test_pipeline_marks_rows_dead_via_system() {
        struct KillFirstSystem;
        #[async_trait::async_trait]
        impl System for KillFirstSystem {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("kill").writes_named("ExecOrder", "id")
            }
            async fn run(&self, data: &mut Dataset) -> crate::SaciResult<()> {
                data.mark_dead(Row::new(0));
                Ok(())
            }
        }

        let mut p = orders_pipeline("kill-test", 5).system(KillFirstSystem);
        p.run().await.unwrap();
        assert_eq!(p.data().live_rows(), 4);
    }

    #[tokio::test]
    async fn test_run_on_returns_this_calls_stats() {
        let p = Pipeline::new("run-on-stats-test")
            .component::<ExecOrder>()
            .system(BumpSystem { field: "total" })
            .system(flaky(2));

        let mut ext = Dataset::new();
        ext.register::<ExecOrder>().unwrap();
        ext.append::<ExecOrder>(&make_orders(3)).unwrap();

        let stats = p.run_on(&mut ext).await.unwrap();
        assert_eq!(stats.systems_run, 2, "two systems should have run");
        assert_eq!(
            stats.retries, 2,
            "FlakySystem failed twice before succeeding"
        );
        assert!(stats.elapsed > std::time::Duration::ZERO);
    }

    /// With no systems the stats are zeroed.
    #[tokio::test]
    async fn test_run_on_empty_pipeline_stats_are_zero() {
        let p = Pipeline::new("empty-run-on");
        let mut ext = Dataset::new();
        ext.register::<ExecOrder>().unwrap();

        let stats = p.run_on(&mut ext).await.unwrap();
        assert_eq!(stats.systems_run, 0);
        assert_eq!(stats.retries, 0);
    }

    // -----------------------------------------------------------------
    // Sources, sinks and stream mode
    // -----------------------------------------------------------------

    fn one_order(id: u64, total: f64) -> arrow_array::RecordBatch {
        use arrow_array::{Float64Array, RecordBatch, UInt64Array};
        RecordBatch::try_new(
            ExecOrder::schema(),
            vec![
                Arc::new(UInt64Array::from(vec![id])),
                Arc::new(Float64Array::from(vec![total])),
            ],
        )
        .unwrap()
    }

    /// Yields pre-built batches in order, then EOF.
    struct VecSource {
        batches: std::collections::VecDeque<arrow_array::RecordBatch>,
        schema: Arc<Schema>,
    }

    #[async_trait::async_trait]
    impl crate::io::Source for VecSource {
        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }

        async fn next_batch(&mut self) -> Result<Option<arrow_array::RecordBatch>, SaciError> {
            Ok(self.batches.pop_front())
        }
    }

    /// Forwards every written batch to the test, which reads them back after the
    /// run. Unbounded, so a write never blocks the runner.
    struct CollectSink {
        tx: tokio::sync::mpsc::UnboundedSender<arrow_array::RecordBatch>,
        schema: Arc<Schema>,
    }

    #[async_trait::async_trait]
    impl crate::io::Sink for CollectSink {
        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }

        async fn write_batch(&mut self, batch: &arrow_array::RecordBatch) -> Result<(), SaciError> {
            self.tx
                .send(batch.clone())
                .map_err(|e| SaciError::generic(format!("CollectSink: {e}")))
        }

        async fn finish(&mut self) -> Result<(), SaciError> {
            Ok(())
        }
    }

    fn vec_source(n: u64) -> VecSource {
        VecSource {
            batches: (0..n)
                .map(|i| one_order(i, (i as f64 + 1.0) * 10.0))
                .collect(),
            schema: ExecOrder::schema(),
        }
    }

    fn doubling_system(counter: Arc<std::sync::atomic::AtomicUsize>) -> impl System + 'static {
        crate::system::system_fn(
            SystemMeta::new("double")
                .reads_named("ExecOrder", "total")
                .writes_named("ExecOrder", "total"),
            move |data| {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let col = data
                    .column(ExecOrder::TOTAL)?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .clone();
                let doubled: Float64Array = col.iter().map(|v| v.map(|x| x * 2.0)).collect();
                data.set_column(ExecOrder::TOTAL, Arc::new(doubled))
            },
        )
    }

    /// `run` drains every source before the stages and hands every row to
    /// the sink after them.
    #[tokio::test]
    async fn test_run_drains_sources_and_fills_sinks() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut p = Pipeline::new("io")
            .component::<ExecOrder>()
            .system(doubling_system(Arc::clone(&calls)))
            .source("ExecOrder", vec_source(3))
            .sink(
                "ExecOrder",
                CollectSink {
                    tx,
                    schema: ExecOrder::schema(),
                },
            );

        let stats = p.run().await.unwrap();
        assert_eq!(stats.rows_produced, 3);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let batch = rx.try_recv().unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    /// Each source batch is a separate end-to-end invocation: three one-row
    /// batches in, three transformed sink writes out, three system calls.
    #[tokio::test]
    async fn test_run_stream_processes_each_batch_individually() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut p = Pipeline::new("stream")
            .component::<ExecOrder>()
            .system(doubling_system(Arc::clone(&calls)))
            .source("ExecOrder", vec_source(3))
            .sink(
                "ExecOrder",
                CollectSink {
                    tx,
                    schema: ExecOrder::schema(),
                },
            );

        let stats = p.run_stream().await.unwrap();

        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "system must run once per source batch"
        );
        assert_eq!(stats.systems_run, 3);
        assert_eq!(stats.rows_produced, 3);

        let mut got = Vec::new();
        while let Ok(batch) = rx.try_recv() {
            got.push(batch);
        }
        assert_eq!(got.len(), 3, "one sink write per item");
        for (i, batch) in got.iter().enumerate() {
            assert_eq!(batch.num_rows(), 1);
            let col = batch
                .column(batch.schema().index_of("total").unwrap())
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let expected = (i as f64 + 1.0) * 10.0 * 2.0;
            assert!(
                (col.value(0) - expected).abs() < 1e-9,
                "item {i}: got {}, want {expected}",
                col.value(0)
            );
        }
    }

    #[tokio::test]
    async fn test_run_stream_requires_exactly_one_source() {
        let empty = || VecSource {
            batches: std::collections::VecDeque::new(),
            schema: ExecOrder::schema(),
        };

        let mut none = Pipeline::new("no-source").component::<ExecOrder>();
        let err = none.run_stream().await.unwrap_err().to_string();
        assert!(
            err.contains("run_stream requires exactly one source; 0 configured"),
            "got: {err}"
        );

        let mut two = Pipeline::new("two-sources")
            .component::<ExecOrder>()
            .source("ExecOrder", empty())
            .source("ExecOrder", empty());
        let err = two.run_stream().await.unwrap_err().to_string();
        assert!(
            err.contains("run_stream requires exactly one source; 2 configured"),
            "got: {err}"
        );
    }

    /// A multi-system all-parallel stage must produce identical results whether
    /// it runs inline (below `STAGE_INLINE_THRESHOLD`) or dispatched.
    #[tokio::test]
    async fn test_small_parallel_stage_matches_dispatched_results() {
        use crate::system::{ParallelSystem, STAGE_INLINE_THRESHOLD, SystemMeta};

        #[derive(Serialize, Deserialize, Clone, Debug)]
        struct StageRow {
            seed: f64,
            a: f64,
            b: f64,
        }

        impl Component for StageRow {
            fn name() -> &'static str {
                "StageRow"
            }
            fn schema() -> Arc<Schema> {
                Arc::new(Schema::new(vec![
                    Field::new("seed", DataType::Float64, false),
                    Field::new("a", DataType::Float64, false),
                    Field::new("b", DataType::Float64, false),
                ]))
            }
        }

        struct Scale {
            field: &'static str,
            factor: f64,
        }

        #[async_trait::async_trait]
        impl ParallelSystem for Scale {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("scale")
                    .reads_named("StageRow", "seed")
                    .writes_named("StageRow", self.field)
            }
            async fn run(&self, data: &Dataset) -> crate::SaciResult<WriteSet> {
                let seed = data
                    .column_named("StageRow", "seed")?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let out: Float64Array = seed.iter().map(|v| v.map(|x| x * self.factor)).collect();
                Ok(WriteSet::new().put_named("StageRow", self.field, Arc::new(out)))
            }
        }

        async fn run_n(n: usize) -> Vec<(f64, f64)> {
            let mut p = Pipeline::new("stage")
                .component::<StageRow>()
                .parallel_system(Scale {
                    field: "a",
                    factor: 2.0,
                })
                .parallel_system(Scale {
                    field: "b",
                    factor: 3.0,
                });
            let rows: Vec<StageRow> = (0..n)
                .map(|i| StageRow {
                    seed: i as f64,
                    a: 0.0,
                    b: 0.0,
                })
                .collect();
            p.data_mut().append::<StageRow>(&rows).unwrap();
            p.run().await.unwrap();

            let a = p.data().column_named("StageRow", "a").unwrap();
            let b = p.data().column_named("StageRow", "b").unwrap();
            let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
            let b = b.as_any().downcast_ref::<Float64Array>().unwrap();
            (0..n).map(|i| (a.value(i), b.value(i))).collect()
        }

        let small = run_n(10).await; // inline path
        let large = run_n(STAGE_INLINE_THRESHOLD as usize + 16).await; // dispatched path

        for (i, (a, b)) in small.iter().enumerate() {
            assert!((a - i as f64 * 2.0).abs() < 1e-9, "row {i} field a");
            assert!((b - i as f64 * 3.0).abs() < 1e-9, "row {i} field b");
            assert_eq!((*a, *b), large[i], "row {i} differs between paths");
        }
    }

    /// The slice fan-out must produce exactly what a single whole-range pass
    /// produces, for all three shapes [`ParallelSystem::merge_slices`] has to
    /// stitch back together: a fixed-width column, a column carrying nulls, and
    /// a variable-width column.
    ///
    /// The row count is over `SLICE_PARALLEL_THRESHOLD`, so the fan-out engages
    /// and the merge runs over many segments. The same system below the
    /// threshold goes through the default `run`, one slice over every row.
    #[tokio::test]
    async fn test_sliced_system_matches_unsliced_for_every_merge_shape() {
        use crate::system::{ParallelSystem, SLICE_PARALLEL_THRESHOLD, SliceWriteSet};
        use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};

        const ROWS: usize = 140_000; // > 100 000 rows and > 1 MB of f64

        struct Wide;
        impl Component for Wide {
            fn name() -> &'static str {
                "Wide"
            }
            fn schema() -> Arc<Schema> {
                Arc::new(Schema::new(vec![
                    Field::new("seed", DataType::UInt64, false),
                    Field::new("doubled", DataType::Float64, false),
                    Field::new("maybe", DataType::Float64, true),
                    Field::new("label", DataType::Utf8, false),
                ]))
            }
        }

        /// Compute the three outputs for `rows` of `seed`.
        fn outputs(seed: &UInt64Array) -> (Float64Array, Float64Array, StringArray) {
            let doubled: Float64Array = seed.values().iter().map(|&v| v as f64 * 2.0).collect();
            let maybe: Float64Array = seed
                .values()
                .iter()
                .map(|&v| (v % 3 != 0).then_some(v as f64))
                .collect();
            let label: StringArray = seed
                .values()
                .iter()
                .map(|&v| Some(format!("r{v}")))
                .collect();
            (doubled, maybe, label)
        }

        fn seed_column(data: &Dataset) -> UInt64Array {
            data.column_named("Wide", "seed")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .clone()
        }

        /// Writes its kernel once, in `run_slice`.
        struct Fan;

        #[async_trait::async_trait]
        impl ParallelSystem for Fan {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("fan")
                    .reads_named("Wide", "seed")
                    .writes_named("Wide", "doubled")
                    .writes_named("Wide", "maybe")
                    .writes_named("Wide", "label")
            }

            fn sliced(&self) -> bool {
                true
            }

            fn run_slice(
                &self,
                data: &Dataset,
                rows: std::ops::Range<u32>,
            ) -> crate::SaciResult<SliceWriteSet> {
                let seed = seed_column(data);
                let slice = seed.slice(rows.start as usize, (rows.end - rows.start) as usize);
                let (doubled, maybe, label) = outputs(&slice);
                Ok(SliceWriteSet::new(rows)
                    .put_named("Wide", "doubled", Arc::new(doubled))
                    .put_named("Wide", "maybe", Arc::new(maybe))
                    .put_named("Wide", "label", Arc::new(label)))
            }
        }

        async fn run_rows(rows: usize) -> (Float64Array, Float64Array, StringArray) {
            let mut p = Pipeline::new("fan")
                .component::<Wide>()
                .parallel_system(Fan);
            let seed: UInt64Array = (0..rows as u64).collect::<Vec<_>>().into();
            let batch = RecordBatch::try_new(
                Wide::schema(),
                vec![
                    Arc::new(seed),
                    // start from zeroed/absent outputs so the write-back is
                    // what puts the computed values in place
                    Arc::new(Float64Array::from(vec![0.0; rows])),
                    Arc::new(Float64Array::from(vec![None; rows])),
                    Arc::new(StringArray::from(vec![""; rows])),
                ],
            )
            .unwrap();
            p.data_mut().append_record_batch("Wide", batch).unwrap();
            p.run().await.unwrap();

            let get = |name: &str| p.data().column_named("Wide", name).unwrap().clone();
            (
                get("doubled")
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .clone(),
                get("maybe")
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .clone(),
                get("label")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone(),
            )
        }

        assert!(ROWS as u32 > SLICE_PARALLEL_THRESHOLD);
        let (d_slice, m_slice, l_slice) = run_rows(ROWS).await;
        let seed: UInt64Array = (0..ROWS as u64).collect::<Vec<_>>().into();
        let (d_whole, m_whole, l_whole) = outputs(&seed);

        assert_eq!(d_slice.len(), ROWS);
        assert_eq!(d_slice, d_whole, "fixed-width column differs");
        assert_eq!(m_slice, m_whole, "nullable column differs");
        assert_eq!(l_slice, l_whole, "variable-width column differs");
        assert_eq!(m_slice.null_count(), ROWS.div_ceil(3), "nulls lost");
        assert_eq!(d_slice.value(ROWS - 1), (ROWS - 1) as f64 * 2.0);
        assert_eq!(l_slice.value(ROWS - 1), format!("r{}", ROWS - 1));

        // Below the threshold the default `run` covers every row in one slice.
        let small = 1_000;
        let (d_small, m_small, l_small) = run_rows(small).await;
        let seed: UInt64Array = (0..small as u64).collect::<Vec<_>>().into();
        assert_eq!((d_small, m_small, l_small), outputs(&seed));
    }
}
