use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::runtime::RuntimeOutput;
use crate::{Dataset, SaciError, SaciResult};
use async_trait::async_trait;
use wasmtime::Store;

use crate::descriptor::template_dataset_from;

use super::bindings::{PipelineDescriptor, RunError, SaciPipeline, SaciPipelinePre};
use super::engine::{EPOCH_TICK, WasmEngine};
use super::host_impl::HostState;

/// One instantiated processor a runtime keeps between calls.
type Warm = (Store<HostState>, SaciPipeline);

/// Locks through poison: a panic while an instance was being put back must not fail every later call.
fn idle(warm: &Mutex<Vec<Warm>>) -> std::sync::MutexGuard<'_, Vec<Warm>> {
    warm.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Host-side WASM pipeline runtime implementing [`crate::runtime::PipelineRuntime`].
///
/// Each `run_batch` call serialises the dataset to Arrow IPC bytes, calls the
/// processor's `run-batch` export via wasmtime, then reads the output IPC bytes
/// back over the dataset contents. By default the call runs on a fresh `Store`;
/// [`with_warm_instances`](Self::with_warm_instances) keeps instances between calls.
///
/// Linking (WASI 0.3 plus host-io) and instantiation planning happen once, at
/// load time, producing a reusable `SaciPipelinePre`; a call then costs store
/// creation plus instantiation from that plan. Unless warm instances are on,
/// processor linear memory never survives a batch, so the checkpoint blob is
/// the only channel for processor state.
///
/// Processor traps are mapped to `SaciError::SystemExecution`. The call timeout
/// limits runaway processor execution, in 100 ms epoch ticks.
pub struct WasmPipelineRuntime {
    name: String,
    engine: WasmEngine,
    /// Pre-linked and pre-planned component, reused across calls. Cloning is an
    /// `Arc` bump.
    pre: SaciPipelinePre<HostState>,
    /// Shared with every per-call `HostState`; never mutated after load.
    config: Arc<HashMap<String, String>>,
    /// Per-call epoch deadline in ticks, 100 ms per tick.
    epoch_deadline: u64,
    /// The workflow this runtime belongs to, and this node's declared id.
    ///
    /// Set by [`with_identity`](Self::with_identity), which the service
    /// builder calls immediately after loading. That is the last place that
    /// knows a runtime's workflow and node id before `Box<dyn PipelineRuntime>`
    /// erases it. Attributes this processor's `saci_processor_*` samples and
    /// its `processor.batch` span; empty for a runtime built directly and
    /// never given an identity.
    workflow_id: String,
    processor_id: String,
    /// Cached descriptor, populated on first `describe()` call.
    descriptor: Mutex<Option<PipelineDescriptor>>,
    /// Component names extracted from the descriptor, for `declared_components()`.
    component_names: OnceLock<Vec<String>>,
    /// Byte length of the last input IPC stream this runtime encoded, used to
    /// pre-size the next one.
    ///
    /// A pipeline's batch sizes are near-constant while it runs: the
    /// flow-control row count changes rarely. So the previous length is a
    /// good capacity guess, and it saves the growth reallocations a
    /// `Vec::new()` would pay. Relaxed because a stale or racing value only
    /// costs one resize.
    last_ipc_len: std::sync::atomic::AtomicUsize,
    /// Instantiated processors kept between calls, at most `warm_limit`; always empty while
    /// `warm_limit` is 0.
    warm: Arc<Mutex<Vec<Warm>>>,
    warm_limit: usize,
}

/// Everything one processor call needs from a [`WasmPipelineRuntime`], owned so
/// the call can move onto a blocking thread.
///
/// Every field is an `Arc` bump or a short string, so building one is cheap
/// enough to do per batch.
struct CallParts {
    engine: WasmEngine,
    pre: SaciPipelinePre<HostState>,
    name: String,
    config: Arc<HashMap<String, String>>,
    epoch_deadline: u64,
    processor_id: String,
}

impl WasmPipelineRuntime {
    /// Prepare a WASM component from raw bytes for running.
    ///
    /// Compiling, linking and pre-instantiation are synchronous and expensive,
    /// and all three go through `WasmEngine::program`, which does them at
    /// most once per engine per distinct set of bytes. The runtime is `Send`
    /// and can be wrapped in `Arc` for sharing.
    pub fn from_bytes(
        engine: WasmEngine,
        name: impl Into<String>,
        wasm_bytes: &[u8],
        config: HashMap<String, String>,
        timeout: Duration,
    ) -> SaciResult<Self> {
        let pre = engine
            .program(wasm_bytes)
            .map_err(|e| SaciError::Configuration(format!("wasm compile error: {e}")))?;

        Ok(Self {
            name: name.into(),
            engine,
            pre,
            config: Arc::new(config),
            epoch_deadline: u64::try_from(
                timeout.as_millis().div_ceil(EPOCH_TICK.as_millis()).max(1),
            )
            .unwrap_or(u64::MAX),
            workflow_id: String::new(),
            processor_id: String::new(),
            descriptor: Mutex::new(None),
            component_names: OnceLock::new(),
            last_ipc_len: std::sync::atomic::AtomicUsize::new(0),
            warm: Arc::new(Mutex::new(Vec::new())),
            warm_limit: 0,
        })
    }

    /// Declare this runtime's workflow and node id.
    ///
    /// Consuming rather than a setter because a runtime's identity never
    /// changes once built, and the service builder assigns it immediately
    /// after loading, which is the last place that knows it before `Box<dyn
    /// PipelineRuntime>` erases it. Left unset, every sample this runtime
    /// writes carries an empty processor id.
    #[must_use]
    pub fn with_identity(mut self, workflow_id: String, processor_id: String) -> Self {
        self.workflow_id = workflow_id;
        self.processor_id = processor_id;
        self
    }

    /// Keep up to `max_idle` instantiated processors between calls.
    ///
    /// - With `max_idle > 0` a call reuses an instance left by an earlier call, so the
    ///   processor's linear memory survives between calls of that instance.
    /// - That memory is a cache only: a runtime holds several instances when calls overlap,
    ///   an instance is dropped after a trap or a `run-error`, and any call may land on a
    ///   fresh instance. Durable state still travels only through `checkpoint` and `prior`.
    /// - The config is fixed for the life of the runtime, so anything a processor derives
    ///   from its config alone is safe to cache.
    /// - An idle instance keeps its pool slot (128 per engine) and its linear memory at its
    ///   high-water mark.
    /// - `0`, the default, builds a fresh `Store` for every call.
    #[must_use]
    pub fn with_warm_instances(mut self, max_idle: usize) -> Self {
        self.warm_limit = max_idle;
        self
    }

    /// Build a fresh `Store` and instantiate the pre-linked component.
    ///
    /// Takes [`CallParts`] by value rather than `&self` so it can run inside
    /// `spawn_blocking` without borrowing the runtime across the thread
    /// boundary, and so the pipeline name is allocated once per call rather
    /// than cloned again here.
    fn make_store_and_instance(parts: CallParts) -> SaciResult<(Store<HostState>, SaciPipeline)> {
        let CallParts {
            engine,
            pre,
            name,
            config,
            epoch_deadline,
            processor_id,
        } = parts;

        let host = HostState::new(name, config, processor_id);
        let mut store = Store::new(&engine.engine, host);
        store.set_epoch_deadline(epoch_deadline);

        let instance = pollster::block_on(pre.instantiate_async(&mut store)).map_err(|e| {
            SaciError::SystemExecution(format!("processor trap (instantiate): {e}"))
        })?;

        Ok((store, instance))
    }

    /// Everything `self` contributes to one processor call.
    fn call_parts(&self) -> CallParts {
        CallParts {
            engine: self.engine.clone(),
            pre: self.pre.clone(),
            name: self.name.clone(),
            config: Arc::clone(&self.config),
            epoch_deadline: self.epoch_deadline,
            processor_id: self.processor_id.clone(),
        }
    }

    /// Call `describe()` and cache the result.
    ///
    /// The first call instantiates a fresh store; subsequent calls return the
    /// cached descriptor without any processor round-trip.
    pub fn describe(&self) -> SaciResult<PipelineDescriptor> {
        {
            let guard = self.descriptor.lock().unwrap();
            if let Some(d) = guard.as_ref() {
                return Ok(d.clone());
            }
        }

        let (mut store, instance) = Self::make_store_and_instance(self.call_parts())?;
        let iface = instance.saci_pipeline_pipeline();
        let desc = pollster::block_on(iface.call_describe(&mut store))
            .map_err(|e| SaciError::SystemExecution(format!("processor trap (describe): {e}")))?;

        let names: Vec<String> = desc.components.iter().map(|c| c.name.clone()).collect();
        self.component_names.get_or_init(|| names);

        let mut guard = self.descriptor.lock().unwrap();
        *guard = Some(desc.clone());
        Ok(desc)
    }
}

#[async_trait(?Send)]
impl crate::runtime::PipelineRuntime for WasmPipelineRuntime {
    fn name(&self) -> &str {
        &self.name
    }

    /// Serialise, call the processor, and read the result back, carrying the
    /// batch's routing decision alongside the state blob.
    async fn run_batch(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> SaciResult<RuntimeOutput> {
        // `debug`, not `info`: one of these opens per batch under the runner's
        // own per-item tree, and `log_level` is what keeps a subscriber from
        // materialising every one. The default `error` materialises no span at
        // all. A processor's own `log` records keep their declared level and
        // stay visible without it.
        #[cfg(feature = "tracing")]
        let batch_span = tracing::debug_span!(
            "processor.batch",
            workflow = %self.workflow_id,
            processor = %self.processor_id,
            rows_in = data.rows() as u64,
            rows_out = tracing::field::Empty,
            systems_run = tracing::field::Empty,
            retries = tracing::field::Empty,
            guest_wall_us = tracing::field::Empty
        );

        // Pre-size from the previous batch's encoded length. A pipeline's
        // batches are near-constant in size, so this lands on the nose and
        // spares the growth reallocations a `Vec::new()` pays. For a
        // megabyte-scale batch that is around twenty doublings, each
        // memcpying everything written so far.
        let mut ipc_bytes: Vec<u8> =
            Vec::with_capacity(self.last_ipc_len.load(std::sync::atomic::Ordering::Relaxed));
        data.write_ipc(&mut ipc_bytes)?;
        self.last_ipc_len
            .store(ipc_bytes.len(), std::sync::atomic::Ordering::Relaxed);

        // Component Model Async bindings take owned `Vec<u8>` and
        // `Option<Vec<u8>>` parameters by value.
        let prior_owned = prior.map(<[u8]>::to_vec);
        let parts = self.call_parts();
        let warm = Arc::clone(&self.warm);
        let (warm_limit, epoch_deadline) = (self.warm_limit, self.epoch_deadline);

        // `spawn_blocking` runs outside the task-local context, so the span is
        // moved into the closure and entered there. That is what puts the
        // processor's own `host-io::log` lines inside this trace.
        #[cfg(feature = "tracing")]
        let call_span = batch_span.clone();

        // Guest Arrow compute and runaway loops run synchronous Wasm bytecode
        // between host yields. Driving `run-batch` on `spawn_blocking` keeps
        // CPU-bound batches off Tokio's worker pool so the `WasmEngine` epoch
        // ticker is never starved when interrupting a spinning guest. Nothing
        // borrowed from `data` crosses the boundary: IPC bytes in, IPC bytes
        // out.
        let joined = tokio::task::spawn_blocking(move || -> SaciResult<_> {
            // The closure body has no `.await`, so a plain guard is correct.
            #[cfg(feature = "tracing")]
            let _call_guard = call_span.enter();
            // The guard is a temporary: nothing is locked while instantiating or calling.
            let kept = if warm_limit > 0 {
                idle(&warm).pop()
            } else {
                None
            };
            let (mut store, instance) = match kept {
                Some(kept) => kept,
                None => Self::make_store_and_instance(parts)?,
            };
            // Relative to the engine's current epoch: a kept store's previous deadline has passed.
            store.set_epoch_deadline(epoch_deadline);
            let result = pollster::block_on(store.run_concurrent(async |accessor| {
                instance
                    .saci_pipeline_pipeline()
                    .call_run_batch(accessor, ipc_bytes, prior_owned)
                    .await
            }))
            .and_then(|inner| inner)
            .map_err(|e| SaciError::SystemExecution(format!("processor trap (run-batch): {e}")))?;
            // Only an instance whose batch succeeded is kept: wasmtime refuses to re-enter one that
            // trapped, and after a `run-error` its memory may hold a half-finished batch.
            if warm_limit > 0 && result.is_ok() {
                let mut idle = idle(&warm);
                if idle.len() < warm_limit {
                    idle.push((store, instance));
                }
            }
            Ok(result)
        })
        .await
        .map_err(|e| SaciError::SystemExecution(format!("processor task join failed: {e}")))?;

        match joined? {
            Ok(result) => {
                let m = &result.metrics;
                crate::metrics::instruments().processor_batch(
                    &self.processor_id,
                    m.wall_ns,
                    m.rows_in,
                    m.rows_out,
                    m.systems_run,
                    m.retries,
                );
                #[cfg(feature = "tracing")]
                {
                    batch_span.record("rows_out", m.rows_out);
                    batch_span.record("systems_run", m.systems_run);
                    batch_span.record("retries", m.retries);
                    batch_span.record("guest_wall_us", m.wall_ns / 1_000);
                }
                let mut out_slice: &[u8] = &result.output;
                *data = Dataset::read_ipc(&mut out_slice)?;
                let routes = result.routes;
                Ok(RuntimeOutput {
                    state: result.checkpoint,
                    routes,
                })
            }
            Err(RunError::Retryable(msg)) => Err(SaciError::SystemExecution(format!(
                "processor retryable: {msg}"
            ))),
            Err(RunError::Permanent(msg)) => Err(SaciError::SystemExecution(format!(
                "processor permanent: {msg}"
            ))),
            // run-batch MUST NOT emit schema-mismatch; treat as permanent bug.
            Err(RunError::SchemaMismatch(msg)) => Err(SaciError::SystemExecution(format!(
                "processor schema-mismatch in run-batch (processor bug): {msg}"
            ))),
        }
    }

    fn declared_components(&self) -> Vec<&str> {
        match self.component_names.get() {
            Some(names) => names.iter().map(String::as_str).collect(),
            None => Vec::new(),
        }
    }

    /// Report the processor's own `describe()` record.
    ///
    /// Reads the cache `PipelineRuntimeLoader::load` warms at startup, so this
    /// never instantiates a store. A runtime built directly, before any
    /// `describe()` call, reports the empty default.
    fn descriptor_info(&self) -> crate::runtime::RuntimeDescriptorInfo {
        let guard = self
            .descriptor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match guard.as_ref() {
            Some(d) => crate::runtime::RuntimeDescriptorInfo {
                name: d.name.clone(),
                version: d.version.clone(),
                stateful: d.stateful,
                schema_fingerprint: d.schema_fingerprint.clone(),
            },
            None => crate::runtime::RuntimeDescriptorInfo::default(),
        }
    }

    fn template_dataset(&self) -> Dataset {
        let descriptor = match self.describe() {
            Ok(d) => d,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(error = %_e, "template_dataset: describe() failed, returning empty dataset");
                return Dataset::new();
            }
        };

        template_dataset_from(
            descriptor
                .components
                .iter()
                .map(|comp| (comp.name.as_str(), comp.arrow_schema_ipc.as_slice())),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn from_bytes_rejects_invalid_wasm() {
        let engine = WasmEngine::new().unwrap();
        let result = WasmPipelineRuntime::from_bytes(
            engine,
            "bad",
            b"not wasm at all",
            HashMap::new(),
            Duration::from_secs(1),
        );
        let err = result.err().expect("expected error");
        let msg = err.to_string();
        assert!(msg.contains("wasm compile error"), "got: {msg}");
    }
}
