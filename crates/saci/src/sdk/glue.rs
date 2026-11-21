//! What `#[processor]` and `#[plugin]` expand to, written once as ordinary
//! functions: the host backend behind [`sdk::host`](super::host), the
//! pipeline slot, `describe` and `run-batch`.
//!
//! Reachable only as `saci::__private::*`. Its contents are stable only within
//! a single `saci` version, and the macros are the only legitimate consumers.

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::OnceLock;
use std::time::Instant;

use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;

use super::host::Level;
use super::state::RouteDecision;
use crate::{Dataset, Pipeline, SaciError, SaciResult};

/// The host calls an author's code can make, whichever boundary it is behind.
///
/// A WebAssembly processor answers them through the `saci:pipeline/host-io`
/// imports, a native plugin through its host vtable, a unit test through
/// [`with_config`](super::host::with_config).
pub trait HostIo {
    /// The config value the host injected under `key`, if any.
    fn config(&self, key: &str) -> Option<String>;
    /// Emit a log line at `level`.
    fn log(&self, level: Level, target: &str, message: &str);
    /// Observe a named metric.
    fn metric(&self, name: &str, value: f64);
}

thread_local! {
    /// The backend [`sdk::host`](super::host) calls go to on this thread.
    ///
    /// Thread-local rather than a static: a native host may drive two plugin
    /// instances from two threads at once, each with its own config.
    static ACTIVE: Cell<Option<*const dyn HostIo>> = const { Cell::new(None) };
}

/// Install `host` for good on this thread. Idempotent.
///
/// For a single-threaded wasm instance, whose `run-batch` futures may
/// interleave: a scope guard there could clear the backend under a call that
/// is still running.
pub fn install_host(host: &'static dyn HostIo) {
    ACTIVE.set(Some(host as *const dyn HostIo));
}

/// Install a backend until the guard drops, then restore the previous one.
///
/// For the native plugin thunks, which own the whole call on one thread, and
/// for [`with_config`](super::host::with_config).
pub struct HostScope<'a> {
    previous: Option<*const dyn HostIo>,
    _host: PhantomData<&'a dyn HostIo>,
}

impl<'a> HostScope<'a> {
    /// Make `host` the active backend while the guard lives.
    pub fn enter(host: &'a dyn HostIo) -> Self {
        let ptr: *const (dyn HostIo + 'a) = host;
        // SAFETY: the guard borrows `host` for `'a` and restores the previous
        // backend on drop, so the erased lifetime is never observed past `'a`.
        let ptr: *const (dyn HostIo + 'static) = unsafe { std::mem::transmute(ptr) };
        let previous = ACTIVE.replace(Some(ptr));
        Self {
            previous,
            _host: PhantomData,
        }
    }
}

impl Drop for HostScope<'_> {
    fn drop(&mut self) {
        ACTIVE.set(self.previous);
    }
}

/// Run `f` against the active backend, or return `None` when none is
/// installed.
pub(crate) fn with_active<R>(f: impl FnOnce(&dyn HostIo) -> R) -> Option<R> {
    let ptr = ACTIVE.get()?;
    // SAFETY: a pointer in `ACTIVE` is either `'static` (`install_host`) or
    // borrowed by a live `HostScope` that removes it before the borrow ends.
    Some(f(unsafe { &*ptr }))
}

/// Holds the lazily built [`Pipeline`] owned by an SDK's exported entry points.
///
/// An SDK creates exactly one instance per exported component or plugin
/// instance. The pipeline is built on the first call to any entry point, then
/// reused.
///
/// Entry points get a shared reference, not a lock: the run method they call
/// takes `&self`, and a guard held across the `.await` of an async export would
/// block, or trap on single-threaded wasm, any call that entered the instance
/// while the first was suspended.
///
/// `OnceLock` rather than `LazyLock`: the initializer is the caller's `build`
/// function, handed to [`PipelineSlot::pipeline`] per call, so there is nothing
/// for a `LazyLock` to store at declaration time.
pub struct PipelineSlot {
    pipeline: OnceLock<Pipeline>,
}

impl Default for PipelineSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineSlot {
    /// Construct an empty slot.
    pub const fn new() -> Self {
        Self {
            pipeline: OnceLock::new(),
        }
    }

    /// Return the pipeline, initialising via `build` on first use.
    pub fn pipeline<F>(&self, build: F) -> &Pipeline
    where
        F: FnOnce() -> Pipeline,
    {
        self.pipeline.get_or_init(build)
    }
}

// A processor keeps its slot in a `static`, which needs `Pipeline: Send + Sync`.
// A processor builds without `engine`; with it a pipeline owns boxed sources and
// sinks, which are `Send` only (a native plugin keeps its slot in a heap
// instance, not a static, so it does not need this).
#[cfg(not(feature = "engine"))]
const _: () = {
    const fn assert_sync<T: Sync>() {}
    assert_sync::<PipelineSlot>();
};

/// What a pipeline reports about itself at load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Descriptor {
    /// The pipeline's name.
    pub name: String,
    /// The guest crate's version.
    pub version: String,
    /// Every registered component, sorted by name.
    pub components: Vec<ComponentSchema>,
    /// Whether the pipeline carries state across batches.
    pub stateful: bool,
    /// [`SchemaRegistry::fingerprint`](crate::SchemaRegistry::fingerprint) as
    /// eight lowercase hex digits.
    pub schema_fingerprint: String,
}

/// One component of a [`Descriptor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentSchema {
    /// The component name.
    pub name: String,
    /// The Arrow schema as IPC schema-message bytes; empty if it failed to
    /// encode, which the host refuses at load with a clean error.
    pub arrow_schema_ipc: Vec<u8>,
}

/// Describe `pipeline` for the host.
pub fn describe(pipeline: &Pipeline, version: &str) -> Descriptor {
    let registry = pipeline.data().schemas();
    let mut entries: Vec<(&'static str, std::sync::Arc<Schema>)> = registry
        .iter()
        .map(|(name, entry)| (*name, entry.schema.clone()))
        .collect();
    entries.sort_by_key(|(name, _)| *name);

    Descriptor {
        name: pipeline.name().to_string(),
        version: version.to_string(),
        components: entries
            .into_iter()
            .map(|(name, schema)| ComponentSchema {
                name: name.to_string(),
                arrow_schema_ipc: schema_to_ipc_bytes(&schema).unwrap_or_default(),
            })
            .collect(),
        stateful: pipeline.is_stateful(),
        schema_fingerprint: fingerprint_hex(registry.fingerprint()),
    }
}

/// What one `run-batch` measured.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchMetrics {
    /// Wall time of the whole call, decode to encode.
    pub wall_ns: u64,
    /// Rows decoded from the input.
    pub rows_in: u64,
    /// Rows encoded into the output.
    pub rows_out: u64,
    /// Systems that ran.
    pub systems_run: u32,
    /// System retries during the run.
    pub retries: u32,
}

/// What one successful `run-batch` produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchOutput {
    /// The output dataset as Arrow IPC.
    pub output: Vec<u8>,
    /// The state blob to persist, when the pipeline is stateful.
    pub checkpoint: Option<Vec<u8>>,
    /// The branches the output is delivered to, when a system decided.
    pub routes: Option<Vec<String>>,
    /// The call's measurements.
    pub metrics: BatchMetrics,
}

/// Why a `run-batch` failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchError {
    /// Whether running the same batch again may succeed.
    pub retryable: bool,
    /// What went wrong.
    pub message: String,
}

impl BatchError {
    fn permanent(message: String) -> Self {
        Self {
            retryable: false,
            message,
        }
    }
}

/// Decode `input`, run `pipeline` over it with the `prior` state, and encode
/// the result.
///
/// # Errors
///
/// A decode, state or encode failure is permanent; a run failure is
/// retryable when [`SaciError::is_retryable`] says so.
pub async fn run_batch(
    pipeline: &Pipeline,
    input: &[u8],
    prior: Option<&[u8]>,
) -> Result<BatchOutput, BatchError> {
    let start = Instant::now();

    let mut dataset = Dataset::read_ipc(&mut &input[..])
        .map_err(|e| BatchError::permanent(format!("ipc decode: {e}")))?;
    // Measured before the state blob is merged, so `rows_in` reports the
    // data plane only.
    let rows_in = dataset.rows() as u64;

    pipeline
        .restore_state(&mut dataset, prior)
        .map_err(|e| BatchError::permanent(format!("state restore: {e}")))?;

    let stats = pipeline
        .run_on(&mut dataset)
        .await
        .map_err(|err: SaciError| BatchError {
            retryable: err.is_retryable(),
            message: err.to_string(),
        })?;

    let rows_out = dataset.rows() as u64;

    let checkpoint = pipeline
        .capture_state(&dataset)
        .map_err(|e| BatchError::permanent(format!("state capture: {e}")))?;
    let routes = dataset
        .take_resource::<RouteDecision>()
        .map(RouteDecision::into_branches);

    let mut output = Vec::with_capacity(input.len());
    dataset
        .write_ipc(&mut output)
        .map_err(|e| BatchError::permanent(format!("ipc encode: {e}")))?;

    Ok(BatchOutput {
        output,
        checkpoint,
        routes,
        metrics: BatchMetrics {
            wall_ns: start.elapsed().as_nanos() as u64,
            rows_in,
            rows_out,
            systems_run: stats.systems_run as u32,
            retries: stats.retries,
        },
    })
}

/// Serialize a single Arrow [`Schema`] as IPC schema-message bytes.
///
/// Writes an empty `StreamWriter`, whose stream start carries the schema
/// descriptor and nothing else. This is the shape a host parses out of a
/// component descriptor.
pub fn schema_to_ipc_bytes(schema: &Schema) -> SaciResult<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| SaciError::generic(format!("schema_to_ipc_bytes new: {e}")))?;
        writer
            .finish()
            .map_err(|e| SaciError::generic(format!("schema_to_ipc_bytes finish: {e}")))?;
    }
    Ok(buf)
}

/// Format a [`crate::SchemaRegistry::fingerprint`] value as the stable 8-char
/// lowercase hex string a descriptor's `schema_fingerprint` field carries.
pub fn fingerprint_hex(fp: u32) -> String {
    format!("{fp:08x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A processor keeps its slot in a `static`, so a slot must be `Sync` when
    /// built without `engine`.
    #[cfg(not(feature = "engine"))]
    #[test]
    fn a_pipeline_slot_is_a_sync_static_without_engine() {
        static SLOT: PipelineSlot = PipelineSlot::new();
        assert_eq!(SLOT.pipeline(|| Pipeline::new("static")).name(), "static");
    }

    #[test]
    fn fingerprint_hex_is_lowercase_and_zero_padded_to_eight() {
        assert_eq!(fingerprint_hex(0), "00000000");
        assert_eq!(fingerprint_hex(0xd52f95a6), "d52f95a6");
        assert_eq!(fingerprint_hex(0xff), "000000ff");
        assert_eq!(fingerprint_hex(u32::MAX), "ffffffff");
    }

    /// The slot builds the pipeline once and hands every caller the same
    /// shared reference, with nothing locked.
    #[test]
    fn a_pipeline_slot_builds_once() {
        let slot = PipelineSlot::new();
        let first = slot.pipeline(|| Pipeline::new("first"));
        assert_eq!(first.name(), "first");
        // The second call must not rebuild, so the name from the first build
        // survives even though this closure would produce a different one.
        let second = slot.pipeline(|| Pipeline::new("second"));
        assert_eq!(second.name(), "first");
        assert!(std::ptr::eq(first, second));
    }

    #[test]
    fn describe_sorts_components_and_reports_the_name() {
        let schema = |field: &str| {
            std::sync::Arc::new(Schema::new(vec![arrow_schema::Field::new(
                field,
                arrow_schema::DataType::Int64,
                false,
            )]))
        };
        let pipeline = Pipeline::new("demo")
            .schema("Zeta", schema("z"))
            .schema("Alpha", schema("a"));
        let descriptor = describe(&pipeline, "1.2.3");
        assert_eq!(descriptor.name, "demo");
        assert_eq!(descriptor.version, "1.2.3");
        assert!(!descriptor.stateful);
        let names: Vec<&str> = descriptor
            .components
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(names, ["Alpha", "Zeta"]);
        assert!(
            descriptor
                .components
                .iter()
                .all(|c| !c.arrow_schema_ipc.is_empty())
        );
        assert_eq!(
            descriptor.schema_fingerprint,
            fingerprint_hex(pipeline.data().schemas().fingerprint())
        );
    }
}
