use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;
use wasmtime::Engine;
use wasmtime::component::{Component, HasSelf, Linker};

use super::bindings::{SaciPipeline, SaciPipelinePre};
use super::host_impl::HostState;

/// Epoch tick interval for processor deadline enforcement.
const EPOCH_TICK: Duration = Duration::from_millis(100);

/// Concurrent processor calls the instance pool is sized for.
///
/// The WIT contract forbids reusing a `Store` across calls, so every batch
/// instantiates. The pooling allocator makes that cheap by handing out
/// pre-reserved slots, and the slot counts are a hard ceiling on how many
/// calls may be in flight at once.
///
/// **Exhaustion is an error, not a queue.** Wasmtime returns
/// `PoolConcurrencyLimitError` ("maximum concurrent limit of N for … reached")
/// with no fallback to on-demand allocation;
/// [`WasmPipelineRuntime`](super::WasmPipelineRuntime) surfaces it as
/// `SaciError::SystemExecution("processor trap (instantiate): …")` and the
/// runner treats the batch as failed. The on-demand allocator had no such
/// limit, so this is a behaviour change under extreme load.
///
/// Reaching it takes 128 processor nodes each holding a batch at the same
/// instant: `run_stream`, `standalone` and `DistributedRunner` all drive one
/// batch at a time per node, so in-flight calls are bounded by node count, not
/// by throughput. Tokio's 512 default blocking threads (every call body runs
/// in `spawn_blocking`) is the absolute ceiling above this value; the comment
/// above [`POOL_MEMORIES_PER_CALL`] is where the address-space cost of
/// raising it is worked out.
const POOL_CALLS: u32 = 128;

// Core instances, memories and tables reserved per concurrent processor call.
//
// One component instance expands into several core instances, and the
// component model's adapter modules carry their own tables. The smoketest
// fixture needs 3 core instances, 1 memory and 2 tables per call; components
// built with other toolchains, such as the polyglot Go and C# processors,
// split into more, so these leave room rather than tracking one fixture.
//
// Memories are the expensive one, and they are what caps `POOL_CALLS`. Every
// memory slot reserves a full `Config::memory_reservation` (4 GiB by default
// on 64-bit, which is what lets wasm32 skip bounds checks), so an engine
// reserves `POOL_CALLS * POOL_MEMORIES_PER_CALL * 4 GiB` of address space: 1
// TiB at these values. That reservation is virtual, never committed, but it
// still bounds how many engines one process can hold, since address space is
// finite. Real code builds one engine per service: `ServiceBuilder` caches it
// and `with_wasm_engine` shares it, so 1 TiB is ample. If `WasmEngine::new`
// starts failing, the process's live address-space reservation is what to
// check.

/// Core instances reserved per concurrent processor call.
const POOL_CORE_INSTANCES_PER_CALL: u32 = 8;
/// Linear memories reserved per concurrent processor call.
const POOL_MEMORIES_PER_CALL: u32 = 2;
/// Tables reserved per concurrent processor call.
const POOL_TABLES_PER_CALL: u32 = 4;

/// Largest linear memory a pooled processor instance may grow to, in bytes.
///
/// 4 GiB is the whole wasm32 addressable range, so pooling imposes no growth
/// limit the on-demand allocator did not already impose. It is also free: a
/// slot's reservation is `Config::memory_reservation` (4 GiB), which this
/// cannot exceed, so lowering it would shrink the growth ceiling without
/// returning any address space.
const POOL_MAX_MEMORY: usize = 4 * 1024 * 1024 * 1024;

/// Bytes of each recycled table reset with `memset` instead of being
/// decommitted and faulted back in on the next call.
///
/// A pooled table is returned to its slot by
/// `TablePool::reset_table_pages_to_zero`, which decommits everything past
/// this many bytes. The next instantiation then takes a page fault per page
/// as the guest touches decommitted table pages; this bound is what keeps
/// enough of the table resident to stop that.
///
/// 1 MiB comfortably exceeds any table wasmtime's default 20 000-element limit
/// can produce, and the reset only touches `min(this, table size)`, so a
/// generous value costs nothing.
///
/// The linear-memory counterpart, `linear_memory_keep_resident`, is
/// deliberately not set. Wasmtime routes memory resets through
/// `MemoryImageSlot::clear_and_remain_ready`, which branches on
/// `decommit_behavior()`. The Windows implementation returns
/// `DecommitBehavior::Zero`, the arm that re-maps anonymous memory and never
/// reads the setting. The table path has no such branch, which is why only
/// this one is set.
const POOL_KEEP_RESIDENT: usize = 1024 * 1024;

/// One prepared program, beside the component bytes it came from.
type CachedProgram = (Vec<u8>, SaciPipelinePre<HostState>);

/// Host-side wasmtime [`Engine`] with epoch interruption enabled, plus the
/// processor programs prepared against it.
///
/// Cheap to clone: wasmtime wraps `Arc<Engine>` internally and the two other
/// fields are `Arc`s. A background tokio task increments the epoch every 100 ms
/// so processors past their deadline are interrupted cleanly; the task stops
/// when the last clone drops. Create one with [`WasmEngine::new`] at service
/// startup and share it, through
/// [`ServiceBuilder::with_wasm_engine`](crate::service::builder::ServiceBuilder::with_wasm_engine)
/// when several builders are assembled in one process.
#[derive(Clone)]
pub struct WasmEngine {
    pub(crate) engine: Engine,
    /// Components compiled, linked and pre-instantiated against `engine`, each
    /// beside the bytes it came from.
    ///
    /// A compiled component is only usable with the `Engine` that compiled it,
    /// so the cache belongs to the engine rather than to a loader or a builder.
    ///
    /// A scan comparing bytes rather than a map keyed by a digest: a service
    /// declares a handful of distinct modules, slice equality is a `memcmp`
    /// that most candidates exit on their length, and hashing a 4 MB component
    /// costs more in a test build than the whole comparison.
    programs: Arc<Mutex<Vec<CachedProgram>>>,
    /// Stops the epoch ticker once no clone of this engine is left.
    _ticker: Arc<Ticker>,
}

/// Aborts the epoch ticker task on drop.
///
/// The ticker holds an `Engine` clone and loops forever, so without this the
/// task and everything the engine retains, including every compiled program,
/// outlive the last [`WasmEngine`] handle for the life of the process.
struct Ticker(JoinHandle<()>);

impl Drop for Ticker {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl WasmEngine {
    /// Create the engine and spawn the epoch ticker task.
    ///
    /// # Errors
    ///
    /// Returns whatever [`Engine::new`] rejects about the configuration.
    ///
    /// # Panics
    ///
    /// Panics if called outside a tokio runtime: the ticker is a
    /// `tokio::spawn`.
    pub fn new() -> wasmtime::Result<Self> {
        let mut config = wasmtime::Config::new();
        config.wasm_component_model(true);
        config.epoch_interruption(true);

        // The WIT contract makes a fresh `Store` per call mandatory
        // (`crates/saci-processor/wit/pipeline.wit`), so instantiation is on
        // the per-batch path: the default on-demand allocator maps a fresh
        // linear memory for every batch and hands it back to the OS when the
        // store drops. The pooling allocator keeps that same fresh store and
        // fresh instance per call; the contract does not change. It only
        // recycles a pre-reserved slot instead of asking the OS each time.
        let mut pool = wasmtime::PoolingAllocationConfig::new();
        pool.total_component_instances(POOL_CALLS);
        pool.total_core_instances(POOL_CALLS * POOL_CORE_INSTANCES_PER_CALL);
        pool.total_memories(POOL_CALLS * POOL_MEMORIES_PER_CALL);
        pool.total_tables(POOL_CALLS * POOL_TABLES_PER_CALL);
        pool.max_memory_size(POOL_MAX_MEMORY);
        pool.table_keep_resident(POOL_KEEP_RESIDENT);
        config.allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(pool));

        let engine = Engine::new(&config)?;
        let ticker = Self::spawn_ticker(engine.clone());
        Ok(Self {
            engine,
            programs: Arc::new(Mutex::new(Vec::new())),
            _ticker: Arc::new(Ticker(ticker)),
        })
    }

    /// The linked, pre-instantiated program `wasm_bytes` yields, prepared at
    /// most once per engine.
    ///
    /// Three synchronous steps: Cranelift compiles the component, the WASI-p2
    /// and host-io imports are linked, and the instantiation is planned. The
    /// 4 MB smoketest component costs about 1.7 s of one fast core, nearly all
    /// of it Cranelift, so a config naming one module from several processor
    /// nodes, or a process that assembles many services against one engine,
    /// pays that once and answers every later ask in about 0.1 ms. Cloning the
    /// answer is an `Arc` bump.
    ///
    /// Two threads asking for the same unprepared program at once both prepare
    /// it and the second insert wins; the cost is one duplicate compile, never
    /// a wrong program, and neither step holds the lock.
    ///
    /// # Errors
    ///
    /// Returns whatever wasmtime rejects about the bytes, the imports, or the
    /// component's exports, labelled by the step that rejected it.
    pub(crate) fn program(
        &self,
        wasm_bytes: &[u8],
    ) -> wasmtime::Result<SaciPipelinePre<HostState>> {
        if let Some((_, program)) = self
            .lock_programs()
            .iter()
            .find(|(bytes, _)| bytes == wasm_bytes)
        {
            return Ok(program.clone());
        }

        let component = Component::from_binary(&self.engine, wasm_bytes)?;
        let mut linker: Linker<HostState> = Linker::new(&self.engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)
            .map_err(|e| wasmtime::Error::msg(format!("wasi linker error: {e}")))?;
        SaciPipeline::add_to_linker::<_, HasSelf<_>>(&mut linker, |s| s)
            .map_err(|e| wasmtime::Error::msg(format!("host linker error: {e}")))?;
        let instance_pre = linker
            .instantiate_pre(&component)
            .map_err(|e| wasmtime::Error::msg(format!("pre-instantiate error: {e}")))?;
        let program = SaciPipelinePre::new(instance_pre)
            .map_err(|e| wasmtime::Error::msg(format!("binding error: {e}")))?;

        self.lock_programs()
            .push((wasm_bytes.to_vec(), program.clone()));
        Ok(program)
    }

    /// Lock through a poisoned mutex: a panic while one program was being
    /// cached must not make every later load fail.
    fn lock_programs(&self) -> std::sync::MutexGuard<'_, Vec<CachedProgram>> {
        self.programs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn spawn_ticker(engine: Engine) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(EPOCH_TICK);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                engine.increment_epoch();
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn engine_creates_successfully() {
        let engine = WasmEngine::new().unwrap();
        let _store: wasmtime::Store<()> = wasmtime::Store::new(&engine.engine, ());
    }

    /// The ticker keeps running while any clone is alive and stops with the
    /// last one, so a process that builds many engines does not accumulate
    /// 100 ms wakeups.
    #[tokio::test]
    async fn the_epoch_ticker_stops_with_the_last_clone() {
        let engine = WasmEngine::new().unwrap();
        let clone = engine.clone();
        let handle = engine._ticker.0.abort_handle();

        drop(engine);
        tokio::time::sleep(EPOCH_TICK * 3).await;
        assert!(!handle.is_finished(), "a live clone must keep it ticking");

        drop(clone);
        tokio::time::sleep(EPOCH_TICK).await;
        assert!(handle.is_finished(), "the last drop must abort it");
    }
}
