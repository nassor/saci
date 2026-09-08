use std::time::Instant;

use crate::error::{SaciError, SaciResult};
use crate::pipeline::RunStats;
use crate::retry::SystemConfig;

#[cfg(feature = "runtime")]
use std::sync::Arc;

#[cfg(feature = "runtime")]
use crate::system::{
    ParallelSystem, SLICE_PARALLEL_THRESHOLD, STAGE_INLINE_THRESHOLD, SliceWriteSet, WriteSet,
};

#[cfg(feature = "io")]
use crate::io::{drain_dataset, drain_into_dataset};

#[cfg(feature = "tracing")]
use tracing::{Instrument as _, info_span};

use super::Pipeline;
use super::dag::{ExpandedMeta, SystemEntry};
use crate::dataset::Dataset;

/// Returns number of retries before success (0 = first attempt succeeded).
async fn run_arrow_system_with_retries(
    sys: &dyn crate::system::System,
    config: &SystemConfig,
    data: &mut Dataset,
) -> SaciResult<u32> {
    let (_, retries) = crate::retry::run_with_retries(config, async || match sys.run_sync(data) {
        Some(sync_result) => sync_result,
        None => sys.run(data).await,
    })
    .await?;
    Ok(retries)
}

/// Returns `(write_set, retries)` where retries = 0 means first attempt succeeded.
#[cfg(feature = "runtime")]
async fn run_parallel_system_with_retries(
    sys: Arc<dyn ParallelSystem>,
    config: &SystemConfig,
    data: &Dataset,
) -> SaciResult<(WriteSet, u32)> {
    let use_slices =
        data.rows() as u32 >= SLICE_PARALLEL_THRESHOLD && sys.run_slice(data, 0..0).is_some();
    crate::retry::run_with_retries(config, async || {
        if use_slices {
            run_parallel_system_sliced(Arc::clone(&sys), data).await
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
#[cfg(feature = "runtime")]
fn cpu_count() -> u32 {
    static N: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *N.get_or_init(|| num_cpus::get().max(1) as u32)
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
#[cfg(feature = "runtime")]
fn chunks_per_cpu() -> u32 {
    static N: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("SACI_SLICE_CHUNKS_PER_CPU")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(4)
    })
}

/// Rows below which splitting further stops paying for itself.
#[cfg(feature = "runtime")]
const MIN_ROWS_PER_CHUNK: u32 = 4_096;

/// Split `total_rows` into equal-ish ranges for parallel dispatch.
///
/// Chunk count is clamped to `[cpu_count, cpu_count * chunks_per_cpu]`: never
/// fewer than one chunk per CPU, so a large batch always fills the machine, and
/// never so many that chunks fall below [`MIN_ROWS_PER_CHUNK`].
#[cfg(feature = "runtime")]
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

#[cfg(feature = "runtime")]
async fn run_parallel_system_sliced(
    sys: Arc<dyn ParallelSystem>,
    data: &Dataset,
) -> Result<WriteSet, SaciError> {
    use rayon::prelude::*;

    let ranges = compute_row_ranges(data.rows() as u32);

    // SAFETY: spawn_blocking is awaited before return; data outlives the task.
    let world_ptr = data as *const Dataset as usize;

    tokio::task::spawn_blocking(move || {
        let data = unsafe { &*(world_ptr as *const Dataset) };

        let slice_results: Vec<Result<SliceWriteSet, SaciError>> = ranges
            .into_par_iter()
            .map(|range| {
                sys.run_slice(data, range.clone()).unwrap_or_else(|| {
                    Err(SaciError::generic(
                        "ParallelSystem::run_slice returned None during slice execution",
                    ))
                })
            })
            .collect();

        let mut slices = Vec::with_capacity(slice_results.len());
        for r in slice_results {
            slices.push(r?);
        }

        sys.merge_slices(slices)
    })
    .await
    .map_err(|e| SaciError::generic(format!("spawn_blocking join error: {e}")))
    .and_then(|r| r)
}

/// Returns total retry count across all systems in the stage.
#[cfg(feature = "runtime")]
async fn run_parallel_stage(
    systems: &[SystemEntry],
    configs: &[SystemConfig],
    metas: &[ExpandedMeta],
    stage: &[usize],
    data: &mut Dataset,
) -> SaciResult<u32> {
    struct TaskDesc {
        sys: Arc<dyn ParallelSystem>,
        config: SystemConfig,
        use_slices: bool,
        #[cfg_attr(not(feature = "tracing"), allow(dead_code))]
        name: &'static str,
    }

    let world_rows = data.rows() as u32;

    let mut task_descs: Vec<TaskDesc> = Vec::with_capacity(stage.len());
    for &sys_idx in stage {
        if let SystemEntry::Parallel(ref sys) = systems[sys_idx] {
            let config = &configs[sys_idx];
            let use_slices =
                world_rows >= SLICE_PARALLEL_THRESHOLD && sys.run_slice(data, 0..0).is_some();
            task_descs.push(TaskDesc {
                sys: Arc::clone(sys),
                config: *config,
                use_slices,
                name: metas[sys_idx].name,
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

                crate::retry::run_with_retries_blocking(&desc.config, || {
                    if desc.use_slices {
                        use rayon::prelude::*;
                        let ranges = compute_row_ranges(world_rows);

                        let slice_results: Vec<Result<SliceWriteSet, SaciError>> = ranges
                            .into_par_iter()
                            .map(|range| {
                                desc.sys.run_slice(data, range).unwrap_or_else(|| {
                                    Err(SaciError::generic(
                                        "run_slice returned None during parallel stage slice",
                                    ))
                                })
                            })
                            .collect();

                        let mut slices = Vec::with_capacity(slice_results.len());
                        for r in slice_results {
                            slices.push(r?);
                        }
                        desc.sys.merge_slices(slices)
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
            for key in ws.fields.keys() {
                debug_assert!(
                    seen_keys.insert(*key),
                    "Two ParallelSystems in the same stage both write field '{}.{}'.",
                    key.0,
                    key.1
                );
            }
        }
    }

    let mut merged = WriteSet::new();
    for ws in write_sets {
        for (key, array) in ws.fields {
            merged.fields.insert(key, array);
        }
        merged.resource_updates.extend(ws.resource_updates);
    }

    data.apply_write_set(merged)?;
    Ok(total_retries)
}

/// Execute one stage against `data`, returning `(systems_run, retries)` for
/// that stage alone.
///
/// Separate from [`run_stages`] so the `pipeline.stage` span can wrap the whole
/// future: the body awaits, and a span guard held across an await attributes
/// another task's work to this stage.
#[cfg(feature = "runtime")]
async fn run_stage(
    systems: &[SystemEntry],
    configs: &[SystemConfig],
    metas: &[ExpandedMeta],
    stage: &[usize],
    data: &mut Dataset,
) -> SaciResult<(usize, u32)> {
    let mut systems_run = 0usize;
    let mut retries: u32 = 0;

    let all_parallel = stage.iter().all(|&idx| systems[idx].is_parallel());

    // Concurrent dispatch of a whole stage costs a `spawn_blocking` per
    // system plus write-set merging; below `STAGE_INLINE_THRESHOLD` rows
    // that dominates the work itself, so fall through to the sequential
    // loop. Systems sharing a stage are non-conflicting by construction, so
    // the result is identical either way.
    if all_parallel && stage.len() > 1 && data.rows() as u32 >= STAGE_INLINE_THRESHOLD {
        systems_run += stage.len();
        retries += run_parallel_stage(systems, configs, metas, stage, data).await?;
    } else if all_parallel && stage.len() == 1 {
        let sys_idx = stage[0];
        let config = &configs[sys_idx];
        if let SystemEntry::Parallel(sys) = &systems[sys_idx] {
            let fut = run_parallel_system_with_retries(Arc::clone(sys), config, data);
            #[cfg(feature = "tracing")]
            let (write_set, system_retries) = fut
                .instrument(info_span!("system.execute", system = metas[sys_idx].name))
                .await?;
            #[cfg(not(feature = "tracing"))]
            let (write_set, system_retries) = fut.await?;
            data.apply_write_set(write_set)?;
            systems_run += 1;
            retries += system_retries;
        }
    } else {
        for &sys_idx in stage {
            let config = &configs[sys_idx];
            #[cfg(feature = "tracing")]
            let span = info_span!("system.execute", system = metas[sys_idx].name);

            match &systems[sys_idx] {
                SystemEntry::Sequential(sys) => {
                    // `max_attempts == 1` needs no special case: the retry
                    // driver wraps the first failure in `RetryExhausted`
                    // with attempt 1 and never sleeps.
                    let fut = run_arrow_system_with_retries(sys.as_ref(), config, data);
                    #[cfg(feature = "tracing")]
                    let system_retries = fut.instrument(span).await?;
                    #[cfg(not(feature = "tracing"))]
                    let system_retries = fut.await?;
                    retries += system_retries;
                    systems_run += 1;
                }
                SystemEntry::Parallel(sys) => {
                    let fut = run_parallel_system_with_retries(Arc::clone(sys), config, data);
                    #[cfg(feature = "tracing")]
                    let (write_set, system_retries) = fut.instrument(span).await?;
                    #[cfg(not(feature = "tracing"))]
                    let (write_set, system_retries) = fut.await?;
                    data.apply_write_set(write_set)?;
                    systems_run += 1;
                    retries += system_retries;
                }
            }
        }
    }

    Ok((systems_run, retries))
}

/// Execute all stages against `data`, returning `(systems_run, retries_this_batch)`.
#[cfg(feature = "runtime")]
async fn run_stages(
    systems: &[SystemEntry],
    configs: &[SystemConfig],
    metas: &[ExpandedMeta],
    stages: &[Vec<usize>],
    data: &mut Dataset,
) -> SaciResult<(usize, u32)> {
    let mut systems_run = 0usize;
    let mut retries_this_batch: u32 = 0;

    for (stage_idx, stage) in stages.iter().enumerate() {
        let fut = run_stage(systems, configs, metas, stage, data);
        #[cfg(feature = "tracing")]
        let (ran, retries) = fut
            .instrument(info_span!(
                "pipeline.stage",
                stage = stage_idx,
                systems = stage.len()
            ))
            .await?;
        #[cfg(not(feature = "tracing"))]
        let (ran, retries) = {
            let _ = stage_idx;
            fut.await?
        };
        systems_run += ran;
        retries_this_batch += retries;
    }

    Ok((systems_run, retries_this_batch))
}

/// Execute all stages sequentially (no-runtime/processor path).
/// `ParallelSystem` entries are rejected; register only `System` impls.
#[cfg(not(feature = "runtime"))]
async fn run_stages_sequential(
    systems: &[SystemEntry],
    configs: &[SystemConfig],
    metas: &[ExpandedMeta],
    stages: &[Vec<usize>],
    data: &mut Dataset,
) -> SaciResult<(usize, u32)> {
    let mut systems_run = 0usize;
    let mut retries_this_batch: u32 = 0;

    #[cfg(not(feature = "tracing"))]
    let _ = metas;

    for (stage_idx, stage) in stages.iter().enumerate() {
        #[cfg(feature = "tracing")]
        let stage_span = info_span!("pipeline.stage", stage = stage_idx, systems = stage.len());
        #[cfg(not(feature = "tracing"))]
        let _ = stage_idx;

        for &sys_idx in stage {
            let config = &configs[sys_idx];

            match &systems[sys_idx] {
                SystemEntry::Sequential(sys) => {
                    let fut = run_arrow_system_with_retries(sys.as_ref(), config, data);
                    #[cfg(feature = "tracing")]
                    let system_retries = fut
                        .instrument(
                            info_span!(parent: &stage_span, "system.execute", system = metas[sys_idx].name),
                        )
                        .await?;
                    #[cfg(not(feature = "tracing"))]
                    let system_retries = fut.await?;
                    retries_this_batch += system_retries;
                    systems_run += 1;
                }
                SystemEntry::Parallel(_) => {
                    return Err(SaciError::generic(
                        "ParallelSystem is not supported on the processor (no-runtime) path",
                    ));
                }
            }
        }
    }

    Ok((systems_run, retries_this_batch))
}

impl Pipeline {
    /// Run all systems against `self.data`.
    #[cfg(feature = "runtime")]
    pub async fn run(&mut self) -> SaciResult<()> {
        self.ensure_plan(self.data.schemas())?;

        let start = Instant::now();
        let rows_before = self.data.live_rows() as isize;

        let Self {
            name,
            data,
            systems,
            stages,
            expanded_metas,
            configs,
            ..
        } = self;

        let stages_val = stages.get().unwrap().as_ref().unwrap();
        let configs_val = configs.get().unwrap();
        let metas_val = expanded_metas.get().unwrap().as_ref().unwrap();

        if stages_val.is_empty() {
            self.last_stats.set(RunStats {
                rows_produced: 0,
                systems_run: 0,
                duration_millis: start.elapsed().as_millis() as u64,
                retries_this_batch: 0,
            });
            return Ok(());
        }

        #[cfg(feature = "tracing")]
        let span = info_span!(
            "pipeline.run",
            pipeline = %name,
            stages = stages_val.len(),
            rows = data.rows()
        );
        #[cfg(not(feature = "tracing"))]
        let _ = name;
        let fut = run_stages(systems, configs_val, metas_val, stages_val, data);
        #[cfg(feature = "tracing")]
        let (systems_run, retries_this_batch) = fut.instrument(span).await?;
        #[cfg(not(feature = "tracing"))]
        let (systems_run, retries_this_batch) = fut.await?;

        self.last_stats.set(RunStats {
            rows_produced: data.live_rows() as isize - rows_before,
            systems_run,
            duration_millis: start.elapsed().as_millis() as u64,
            retries_this_batch,
        });
        Ok(())
    }

    /// Run all systems against `self.data` (processor/sequential path).
    ///
    /// `ParallelSystem` entries are not supported here: register only `System`
    /// impls when targeting wasm32-wasip2.
    #[cfg(not(feature = "runtime"))]
    pub async fn run(&mut self) -> SaciResult<()> {
        self.ensure_plan(self.data.schemas())?;

        let start = Instant::now();
        let rows_before = self.data.live_rows() as isize;

        // Clone stages/configs/metas to break the borrow conflict with
        // `&mut self.data`.
        let stages = self.stages.get().unwrap().as_ref().unwrap().clone();
        let configs: Vec<SystemConfig> = self.configs.get().unwrap().to_vec();
        let metas = self.expanded_metas.get().unwrap().as_ref().unwrap().clone();

        if stages.is_empty() {
            self.last_stats.set(RunStats {
                rows_produced: 0,
                systems_run: 0,
                duration_millis: start.elapsed().as_millis() as u64,
                retries_this_batch: 0,
            });
            return Ok(());
        }

        #[cfg(feature = "tracing")]
        let span = info_span!(
            "pipeline.run",
            pipeline = %self.name,
            stages = stages.len(),
            rows = self.data.rows()
        );
        let fut = run_stages_sequential(&self.systems, &configs, &metas, &stages, &mut self.data);
        #[cfg(feature = "tracing")]
        let (systems_run, retries_this_batch) = fut.instrument(span).await?;
        #[cfg(not(feature = "tracing"))]
        let (systems_run, retries_this_batch) = fut.await?;

        self.last_stats.set(RunStats {
            rows_produced: self.data.live_rows() as isize - rows_before,
            systems_run,
            duration_millis: start.elapsed().as_millis() as u64,
            retries_this_batch,
        });
        Ok(())
    }

    /// Run all systems against a separately-provided `Dataset`, returning the
    /// stats for **this call**.
    ///
    /// An escape hatch for distributed runners and WASM processors that manage
    /// their own per-partition datasets. Sources and sinks on `self` are
    /// ignored. The returned [`RunStats`] is also recorded in
    /// [`last_stats`](Pipeline::last_stats); callers that need a per-call value
    /// should use the return value rather than reading the shared cell back.
    #[cfg(feature = "runtime")]
    pub async fn run_on_with_stats(&self, data: &mut Dataset) -> SaciResult<RunStats> {
        self.ensure_plan(data.schemas())?;

        let start = Instant::now();
        let rows_before = data.live_rows() as isize;

        let stages_val = self.stages.get().unwrap().as_ref().unwrap();
        let configs_val = self.configs.get().unwrap();
        let metas_val = self.expanded_metas.get().unwrap().as_ref().unwrap();

        if stages_val.is_empty() {
            let stats = RunStats {
                rows_produced: 0,
                systems_run: 0,
                duration_millis: start.elapsed().as_millis() as u64,
                retries_this_batch: 0,
            };
            self.last_stats.set(stats);
            return Ok(stats);
        }

        #[cfg(feature = "tracing")]
        let span = info_span!(
            "pipeline.run",
            pipeline = %self.name,
            stages = stages_val.len(),
            rows = data.rows()
        );
        let fut = run_stages(&self.systems, configs_val, metas_val, stages_val, data);
        #[cfg(feature = "tracing")]
        let (systems_run, retries_this_batch) = fut.instrument(span).await?;
        #[cfg(not(feature = "tracing"))]
        let (systems_run, retries_this_batch) = fut.await?;

        let stats = RunStats {
            rows_produced: data.live_rows() as isize - rows_before,
            systems_run,
            duration_millis: start.elapsed().as_millis() as u64,
            retries_this_batch,
        };
        self.last_stats.set(stats);
        Ok(stats)
    }

    /// Run all systems against a separately-provided `Dataset` (processor/sequential
    /// path), returning the stats for **this call**.
    #[cfg(not(feature = "runtime"))]
    pub async fn run_on_with_stats(&self, data: &mut Dataset) -> SaciResult<RunStats> {
        self.ensure_plan(data.schemas())?;

        let start = Instant::now();
        let rows_before = data.live_rows() as isize;

        let stages_val = self.stages.get().unwrap().as_ref().unwrap();
        let configs_val = self.configs.get().unwrap();
        let metas_val = self.expanded_metas.get().unwrap().as_ref().unwrap();

        if stages_val.is_empty() {
            let stats = RunStats {
                rows_produced: 0,
                systems_run: 0,
                duration_millis: start.elapsed().as_millis() as u64,
                retries_this_batch: 0,
            };
            self.last_stats.set(stats);
            return Ok(stats);
        }

        #[cfg(feature = "tracing")]
        let span = info_span!(
            "pipeline.run",
            pipeline = %self.name,
            stages = stages_val.len(),
            rows = data.rows()
        );
        let fut = run_stages_sequential(&self.systems, configs_val, metas_val, stages_val, data);
        #[cfg(feature = "tracing")]
        let (systems_run, retries_this_batch) = fut.instrument(span).await?;
        #[cfg(not(feature = "tracing"))]
        let (systems_run, retries_this_batch) = fut.await?;

        let stats = RunStats {
            rows_produced: data.live_rows() as isize - rows_before,
            systems_run,
            duration_millis: start.elapsed().as_millis() as u64,
            retries_this_batch,
        };
        self.last_stats.set(stats);
        Ok(stats)
    }

    /// Run all systems against a separately-provided `Dataset`.
    ///
    /// Thin wrapper over [`run_on_with_stats`](Self::run_on_with_stats) that
    /// discards the per-call stats; they remain readable via
    /// [`last_stats`](Pipeline::last_stats).
    pub async fn run_on(&self, data: &mut Dataset) -> SaciResult<()> {
        self.run_on_with_stats(data).await.map(|_| ())
    }

    /// Drain sources → run → drain sinks.
    #[cfg(feature = "io")]
    pub async fn run_with_io(&mut self) -> SaciResult<()> {
        let start = Instant::now();
        let rows_before = self.data.live_rows() as isize;

        {
            let Self { sources, data, .. } = &mut *self;
            for (comp, src) in sources.iter_mut() {
                drain_into_dataset(src.as_mut(), data, comp).await?;
            }
        }
        self.run().await?;
        let systems_run = self.last_stats.get().systems_run;
        let retries_this_batch = self.last_stats.get().retries_this_batch;
        {
            let Self { sinks, data, .. } = &mut *self;
            for (comp, sink) in sinks.iter_mut() {
                drain_dataset(data, comp, sink.as_mut()).await?;
                sink.finish().await?;
            }
        }

        self.last_stats.set(RunStats {
            rows_produced: self.data.live_rows() as isize - rows_before,
            systems_run,
            duration_millis: start.elapsed().as_millis() as u64,
            retries_this_batch,
        });
        Ok(())
    }

    /// Process each source batch individually, end-to-end, as it arrives.
    ///
    /// Where [`run_with_io`](Self::run_with_io) drains the source to EOF, runs
    /// once over everything, then flushes, this pulls one
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
    #[cfg(feature = "io")]
    pub async fn run_stream(&mut self) -> SaciResult<RunStats> {
        if self.sources.len() != 1 {
            return Err(SaciError::configuration(format!(
                "run_stream requires exactly one source; {} configured",
                self.sources.len()
            )));
        }
        self.ensure_plan(self.data.schemas())?;

        let start = Instant::now();
        let mut systems_run = 0usize;
        let mut retries_this_batch = 0u32;
        let mut rows_produced = 0isize;

        {
            let Self {
                name,
                data,
                systems,
                stages,
                expanded_metas,
                configs,
                sources,
                sinks,
                ..
            } = &mut *self;

            let stages_val = stages.get().unwrap().as_ref().unwrap();
            let configs_val = configs.get().unwrap();
            let metas_val = expanded_metas.get().unwrap().as_ref().unwrap();
            #[cfg(not(feature = "tracing"))]
            let _ = name;
            let (comp, source) = {
                let (c, s) = &mut sources[0];
                (*c, s)
            };

            while let Some(batch) = source.next_batch().await? {
                data.clear();
                data.append_record_batch(comp, batch)?;

                if !stages_val.is_empty() {
                    #[cfg(feature = "tracing")]
                    let span = info_span!(
                        "pipeline.run",
                        pipeline = %name,
                        stages = stages_val.len(),
                        rows = data.rows()
                    );
                    let fut = run_stages(systems, configs_val, metas_val, stages_val, data);
                    #[cfg(feature = "tracing")]
                    let (run, retries) = fut.instrument(span).await?;
                    #[cfg(not(feature = "tracing"))]
                    let (run, retries) = fut.await?;
                    systems_run += run;
                    retries_this_batch += retries;
                }

                for (sink_comp, sink) in sinks.iter_mut() {
                    drain_dataset(data, sink_comp, sink.as_mut()).await?;
                }
                rows_produced += data.live_rows() as isize;
            }

            for (_, sink) in sinks.iter_mut() {
                sink.finish().await?;
            }
        }

        let stats = RunStats {
            rows_produced,
            systems_run,
            duration_millis: start.elapsed().as_millis() as u64,
            retries_this_batch,
        };
        self.last_stats.set(stats);
        Ok(stats)
    }
}

// Every test here drives `Pipeline::run` / `run_on` through the tokio+rayon
// stage executor, so the module is gated on `runtime`. The processor path is
// covered end-to-end by `saci-processor-smoketest` against a real component.
#[cfg(all(test, feature = "runtime"))]
mod tests {
    use std::sync::Arc;

    use arrow_array::Float64Array;
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;
    // Named at module scope only by the `Source`/`Sink` impls for the stream
    // tests below, which are themselves `io`-gated. The two retry tests that
    // need it import it locally.
    #[cfg(feature = "io")]
    use crate::error::SaciError;
    use crate::pipeline::Pipeline;
    use crate::row::Row;
    use crate::system::{System, SystemMeta, WriteSet};

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct ExecOrder {
        id: u64,
        total: f64,
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

    struct BumpSystem {
        field: &'static str,
    }

    #[async_trait::async_trait]
    impl System for BumpSystem {
        fn meta(&self) -> SystemMeta {
            SystemMeta::new("bump")
                .read("ExecOrder", "id")
                .write("ExecOrder", self.field)
        }

        async fn run(&self, data: &mut Dataset) -> crate::SaciResult<()> {
            let batch = data.columns::<ExecOrder>().unwrap().clone();
            let total_col = batch
                .column(batch.schema().index_of(self.field).unwrap())
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let bumped: Float64Array = total_col.iter().map(|v| v.map(|x| x + 1.0)).collect();
            let ws = WriteSet::new().put("ExecOrder", self.field, Arc::new(bumped));
            data.apply_write_set(ws)
        }
    }

    #[tokio::test]
    async fn test_pipeline_run_single_system() {
        let mut p = Pipeline::new("test");
        p.data.register_component::<ExecOrder>().unwrap();
        p.data.append::<ExecOrder>(&make_orders(5)).unwrap();
        p.add_system(BumpSystem { field: "total" });

        p.run().await.unwrap();

        let col = p.data.column::<ExecOrder>("total").unwrap();
        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((arr.value(0) - 1.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn test_run_on_uses_external_dataset() {
        let p = Pipeline::new("template");

        let mut ext = Dataset::new();
        ext.register_component::<ExecOrder>().unwrap();
        ext.append::<ExecOrder>(&make_orders(3)).unwrap();

        p.run_on(&mut ext).await.unwrap();
        assert_eq!(ext.rows(), 3);
    }

    #[tokio::test]
    async fn test_retries_this_batch_counts_failures_before_success() {
        use crate::error::SaciError;
        use crate::retry::{RetryMode, SystemConfig};
        use crate::system::SystemMeta;
        use std::sync::atomic::{AtomicUsize, Ordering};

        static CALL_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct FlakySystem {
            fail_times: usize,
        }

        #[async_trait::async_trait]
        impl System for FlakySystem {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("flaky").write("ExecOrder", "total")
            }

            fn config(&self) -> SystemConfig {
                SystemConfig {
                    retry_mode: RetryMode::Fixed {
                        retries: 5,
                        delay: std::time::Duration::ZERO,
                    },
                }
            }

            async fn run(&self, _data: &mut Dataset) -> crate::SaciResult<()> {
                let n = CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                if n < self.fail_times {
                    Err(SaciError::generic("transient"))
                } else {
                    Ok(())
                }
            }
        }

        CALL_COUNT.store(0, Ordering::SeqCst);
        let mut p = Pipeline::new("retry-test");
        p.data.register_component::<ExecOrder>().unwrap();
        p.data.append::<ExecOrder>(&make_orders(2)).unwrap();
        p.add_system(FlakySystem { fail_times: 3 });

        p.run().await.unwrap();
        assert_eq!(p.last_stats().retries_this_batch, 3);
    }

    #[tokio::test]
    async fn test_retries_this_batch_zero_on_clean_run() {
        let mut p = Pipeline::new("no-retry-test");
        p.data.register_component::<ExecOrder>().unwrap();
        p.data.append::<ExecOrder>(&make_orders(2)).unwrap();
        p.add_system(BumpSystem { field: "total" });

        p.run().await.unwrap();
        assert_eq!(p.last_stats().retries_this_batch, 0);
    }

    #[tokio::test]
    async fn test_pipeline_marks_rows_dead_via_system() {
        struct KillFirstSystem;
        #[async_trait::async_trait]
        impl System for KillFirstSystem {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("kill").write("ExecOrder", "id")
            }
            async fn run(&self, data: &mut Dataset) -> crate::SaciResult<()> {
                data.mark_dead(Row::new(0));
                Ok(())
            }
        }

        let mut p = Pipeline::new("kill-test");
        p.data.register_component::<ExecOrder>().unwrap();
        p.data.append::<ExecOrder>(&make_orders(5)).unwrap();
        p.add_system(KillFirstSystem);
        p.run().await.unwrap();
        assert_eq!(p.data.live_rows(), 4);
    }

    #[tokio::test]
    async fn test_run_on_populates_last_stats() {
        use crate::error::SaciError;
        use crate::retry::{RetryMode, SystemConfig};
        use crate::system::SystemMeta;
        use std::sync::atomic::{AtomicUsize, Ordering};

        static CALL_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct FlakySystem2 {
            fail_times: usize,
        }

        #[async_trait::async_trait]
        impl System for FlakySystem2 {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("flaky2").write("ExecOrder", "total")
            }

            fn config(&self) -> SystemConfig {
                SystemConfig {
                    retry_mode: RetryMode::Fixed {
                        retries: 5,
                        delay: std::time::Duration::ZERO,
                    },
                }
            }

            async fn run(&self, _data: &mut Dataset) -> crate::SaciResult<()> {
                let n = CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                if n < self.fail_times {
                    Err(SaciError::generic("transient"))
                } else {
                    Ok(())
                }
            }
        }

        CALL_COUNT.store(0, Ordering::SeqCst);
        let mut p = Pipeline::new("run-on-stats-test");
        p.data.register_component::<ExecOrder>().unwrap();
        p.add_system(BumpSystem { field: "total" });
        p.add_system(FlakySystem2 { fail_times: 2 });

        let mut ext = Dataset::new();
        ext.register_component::<ExecOrder>().unwrap();
        ext.append::<ExecOrder>(&make_orders(3)).unwrap();

        p.run_on(&mut ext).await.unwrap();

        let stats = p.last_stats();
        assert_eq!(stats.systems_run, 2, "two systems should have run");
        assert_eq!(
            stats.retries_this_batch, 2,
            "FlakySystem2 failed twice before succeeding"
        );
        assert!(stats.duration_millis > 0, "duration should be populated");
    }

    /// With no systems the stats are zeroed, not left over from a prior run.
    #[tokio::test]
    async fn test_run_on_empty_pipeline_stats_are_zero() {
        let p = Pipeline::new("empty-run-on");
        let mut ext = Dataset::new();

        // Use a separately-registered component so ensure_plan sees a schema.
        struct Dummy;
        impl Component for Dummy {
            fn name() -> &'static str {
                "Dummy"
            }
            fn schema() -> std::sync::Arc<arrow_schema::Schema> {
                std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "x",
                    arrow_schema::DataType::UInt8,
                    false,
                )]))
            }
        }
        ext.register_component::<Dummy>().unwrap();

        p.run_on(&mut ext).await.unwrap();

        let stats = p.last_stats();
        assert_eq!(stats.systems_run, 0);
        assert_eq!(stats.retries_this_batch, 0);
    }

    // -----------------------------------------------------------------
    // Stream mode
    // -----------------------------------------------------------------

    #[cfg(feature = "io")]
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
    #[cfg(feature = "io")]
    struct VecSource {
        batches: std::collections::VecDeque<arrow_array::RecordBatch>,
        schema: Arc<Schema>,
    }

    #[cfg(feature = "io")]
    #[async_trait::async_trait]
    impl crate::io::source::Source for VecSource {
        fn schema(&self) -> Arc<Schema> {
            Arc::clone(&self.schema)
        }

        async fn next_batch(&mut self) -> Result<Option<arrow_array::RecordBatch>, SaciError> {
            Ok(self.batches.pop_front())
        }
    }

    /// Forwards every written batch to the test, which reads them back after the
    /// run. Unbounded, so a write never blocks the runner.
    #[cfg(feature = "io")]
    struct CollectSink {
        tx: tokio::sync::mpsc::UnboundedSender<arrow_array::RecordBatch>,
        schema: Arc<Schema>,
    }

    #[cfg(feature = "io")]
    #[async_trait::async_trait]
    impl crate::io::sink::Sink for CollectSink {
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

    /// Each source batch is a separate end-to-end invocation: three one-row
    /// batches in, three transformed sink writes out, three system calls.
    #[cfg(feature = "io")]
    #[tokio::test]
    async fn test_run_stream_processes_each_batch_individually() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use crate::system::{SystemMeta, system_fn};

        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);

        let mut p = Pipeline::new("stream");
        p.data.register_component::<ExecOrder>().unwrap();
        p.add_system(system_fn(
            SystemMeta::new("double")
                .read("ExecOrder", "total")
                .write("ExecOrder", "total"),
            move |data| {
                counter.fetch_add(1, Ordering::SeqCst);
                let batch = data.columns::<ExecOrder>().unwrap().clone();
                let col = batch
                    .column(batch.schema().index_of("total").unwrap())
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let doubled: Float64Array = col.iter().map(|v| v.map(|x| x * 2.0)).collect();
                data.apply_write_set(WriteSet::new().put("ExecOrder", "total", Arc::new(doubled)))
            },
        ));

        let source = VecSource {
            batches: (0..3u64)
                .map(|i| one_order(i, (i as f64 + 1.0) * 10.0))
                .collect(),
            schema: ExecOrder::schema(),
        };
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sink = CollectSink {
            tx,
            schema: ExecOrder::schema(),
        };
        p.add_source("ExecOrder", source);
        p.add_sink("ExecOrder", sink);

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

    #[cfg(feature = "io")]
    #[tokio::test]
    async fn test_run_stream_requires_exactly_one_source() {
        let empty = || VecSource {
            batches: std::collections::VecDeque::new(),
            schema: ExecOrder::schema(),
        };

        let mut none = Pipeline::new("no-source");
        none.data.register_component::<ExecOrder>().unwrap();
        let err = none.run_stream().await.unwrap_err().to_string();
        assert!(
            err.contains("run_stream requires exactly one source; 0 configured"),
            "got: {err}"
        );

        let mut two = Pipeline::new("two-sources");
        two.data.register_component::<ExecOrder>().unwrap();
        two.add_source("ExecOrder", empty());
        two.add_source("ExecOrder", empty());
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
                    .read("StageRow", "seed")
                    .write("StageRow", self.field)
            }
            async fn run(&self, data: &Dataset) -> crate::SaciResult<WriteSet> {
                let batch = data.columns::<StageRow>().unwrap();
                let seed = batch
                    .column(batch.schema().index_of("seed").unwrap())
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let out: Float64Array = seed.iter().map(|v| v.map(|x| x * self.factor)).collect();
                Ok(WriteSet::new().put("StageRow", self.field, Arc::new(out)))
            }
        }

        async fn run_n(n: usize) -> Vec<(f64, f64)> {
            let mut p = Pipeline::new("stage");
            p.data.register_component::<StageRow>().unwrap();
            let rows: Vec<StageRow> = (0..n)
                .map(|i| StageRow {
                    seed: i as f64,
                    a: 0.0,
                    b: 0.0,
                })
                .collect();
            p.data.append::<StageRow>(&rows).unwrap();
            p.add_parallel_system(Scale {
                field: "a",
                factor: 2.0,
            });
            p.add_parallel_system(Scale {
                field: "b",
                factor: 3.0,
            });
            p.run().await.unwrap();

            let a = p.data.column::<StageRow>("a").unwrap();
            let b = p.data.column::<StageRow>("b").unwrap();
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

    /// The slice fan-out must produce exactly what the single-threaded `run()`
    /// produces, for all three shapes [`ParallelSystem::merge_slices`] has to
    /// stitch back together: a fixed-width column, a column carrying nulls, and
    /// a variable-width column.
    ///
    /// The row count is over `SLICE_PARALLEL_THRESHOLD`, so the fan-out engages
    /// and the merge runs over many segments. Any change to how segments are
    /// merged has to keep all three shapes intact, and the nullable and
    /// variable-width ones are what a fixed-width fast path would have to
    /// refuse.
    #[cfg(feature = "runtime")]
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
            let batch = data.columns::<Wide>().unwrap();
            batch
                .column(batch.schema().index_of("seed").unwrap())
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .clone()
        }

        /// `sliced` selects whether `run_slice` is offered to the scheduler.
        struct Fan {
            sliced: bool,
        }

        #[async_trait::async_trait]
        impl ParallelSystem for Fan {
            fn meta(&self) -> SystemMeta {
                SystemMeta::new("fan")
                    .read("Wide", "seed")
                    .write("Wide", "doubled")
                    .write("Wide", "maybe")
                    .write("Wide", "label")
            }

            async fn run(&self, data: &Dataset) -> crate::SaciResult<WriteSet> {
                let (doubled, maybe, label) = outputs(&seed_column(data));
                Ok(WriteSet::new()
                    .put("Wide", "doubled", Arc::new(doubled))
                    .put("Wide", "maybe", Arc::new(maybe))
                    .put("Wide", "label", Arc::new(label)))
            }

            fn run_slice(
                &self,
                data: &Dataset,
                rows: std::ops::Range<u32>,
            ) -> Option<crate::SaciResult<SliceWriteSet>> {
                if !self.sliced {
                    return None;
                }
                let seed = seed_column(data);
                let slice = seed.slice(rows.start as usize, (rows.end - rows.start) as usize);
                let (doubled, maybe, label) = outputs(&slice);
                Some(Ok(SliceWriteSet::new(rows)
                    .put("Wide", "doubled", Arc::new(doubled))
                    .put("Wide", "maybe", Arc::new(maybe))
                    .put("Wide", "label", Arc::new(label))))
            }
        }

        async fn run_with(sliced: bool) -> (Float64Array, Float64Array, StringArray) {
            let mut p = Pipeline::new("fan");
            p.data.register_component::<Wide>().unwrap();
            let seed: UInt64Array = (0..ROWS as u64).collect::<Vec<_>>().into();
            let batch = RecordBatch::try_new(
                Wide::schema(),
                vec![
                    Arc::new(seed),
                    // start from zeroed/absent outputs so the write-back is
                    // what puts the computed values in place
                    Arc::new(Float64Array::from(vec![0.0; ROWS])),
                    Arc::new(Float64Array::from(vec![None; ROWS])),
                    Arc::new(StringArray::from(vec![""; ROWS])),
                ],
            )
            .unwrap();
            p.data.append_record_batch("Wide", batch).unwrap();
            p.add_parallel_system(Fan { sliced });
            p.run().await.unwrap();

            let get = |name: &str| p.data.column::<Wide>(name).unwrap();
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
        let (d_slice, m_slice, l_slice) = run_with(true).await;
        let (d_whole, m_whole, l_whole) = run_with(false).await;

        assert_eq!(d_slice.len(), ROWS);
        assert_eq!(d_slice, d_whole, "fixed-width column differs");
        assert_eq!(m_slice, m_whole, "nullable column differs");
        assert_eq!(l_slice, l_whole, "variable-width column differs");
        assert_eq!(m_slice.null_count(), ROWS.div_ceil(3), "nulls lost");
        assert_eq!(d_slice.value(ROWS - 1), (ROWS - 1) as f64 * 2.0);
        assert_eq!(l_slice.value(ROWS - 1), format!("r{}", ROWS - 1));
    }
}
