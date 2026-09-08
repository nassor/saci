//! Runtime glue referenced by [`crate::export_plugin!`] expansions.
//!
//! Not public API. Its contents are stable only within a single `saci-plugin`
//! version, and the macro is the only legitimate consumer.
//!
//! Almost everything the boundary needs lives here as an ordinary function
//! rather than inside the macro body, so it is unit-testable and exists once in
//! the binary instead of once per expansion.

use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};

use base64::Engine as _;
use saci_core::sdk::{PipelineSlot, classify_run_error, fingerprint_hex, schema_to_ipc_bytes};
use saci_core::{Dataset, Pipeline};

pub use pollster;
pub use saci_core::sdk::RouteDecision;
pub use saci_core::sdk::{NoState, ProcessorState, ProcessorStateSpec, Stateful};
pub use saci_plugin_abi::{
    SACI_ABI_VERSION, SaciBuffer, SaciHostV1, SaciPluginV1, SaciRunMetrics, SaciRunResult,
    SaciSlice, SaciStatus, log_level,
};

/// One loaded plugin instance.
///
/// The host may load the same library twice with different config, so the
/// pipeline and the host vtable are per-instance rather than per-process
/// statics. `saci_plugin_v1` boxes one of these and hands the pointer back as
/// [`saci_plugin_abi::SaciPluginV1::instance`].
pub struct PluginInstance {
    slot: PipelineSlot,
    host: SaciHostV1,
}

impl PluginInstance {
    /// Wrap a host vtable for a fresh instance.
    pub const fn new(host: SaciHostV1) -> Self {
        Self {
            slot: PipelineSlot::new(),
            host,
        }
    }

    /// Leak into a raw pointer for the ABI's `instance` slot.
    pub fn into_raw(self) -> *mut core::ffi::c_void {
        Box::into_raw(Box::new(self)).cast()
    }

    /// Borrow an instance the ABI handed back.
    ///
    /// # Safety
    ///
    /// `instance` must be a pointer produced by [`PluginInstance::into_raw`]
    /// that has not yet been passed to [`PluginInstance::from_raw`].
    pub unsafe fn borrow<'a>(instance: *mut core::ffi::c_void) -> Option<&'a Self> {
        if instance.is_null() {
            return None;
        }
        Some(unsafe { &*instance.cast::<Self>() })
    }

    /// Retake ownership so the instance drops.
    ///
    /// # Safety
    ///
    /// `instance` must come from [`PluginInstance::into_raw`] and must not be
    /// used afterwards.
    pub unsafe fn from_raw(instance: *mut core::ffi::c_void) {
        if instance.is_null() {
            return;
        }
        drop(unsafe { Box::from_raw(instance.cast::<Self>()) });
    }
}

thread_local! {
    /// The host vtable for the call in progress on this thread.
    ///
    /// Thread-local rather than a static: the host never calls one instance
    /// concurrently, but it may drive two different instances from two threads
    /// at once, and each has its own config map.
    static ACTIVE_HOST: Cell<*const SaciHostV1> = const { Cell::new(core::ptr::null()) };
}

/// Publishes a host vtable for the duration of one boundary call.
pub struct HostScope {
    previous: *const SaciHostV1,
}

impl HostScope {
    /// Publish `host` on this thread until the returned guard drops.
    pub fn enter(host: &SaciHostV1) -> Self {
        let previous = ACTIVE_HOST.replace(host as *const SaciHostV1);
        Self { previous }
    }
}

impl Drop for HostScope {
    fn drop(&mut self) {
        ACTIVE_HOST.set(self.previous);
    }
}

fn with_host<R>(f: impl FnOnce(&SaciHostV1) -> R) -> Option<R> {
    let ptr = ACTIVE_HOST.get();
    if ptr.is_null() {
        return None;
    }
    Some(f(unsafe { &*ptr }))
}

/// Read a config value the host injected.
///
/// Returns `None` outside a boundary call, when the host supplied no
/// `get_config` callback, or when the key is absent.
pub fn config_get(key: &str) -> Option<String> {
    with_host(|host| {
        let get_config = host.get_config?;
        let mut out = SaciSlice::empty();
        let found =
            unsafe { get_config(host.ctx, SaciSlice::from_bytes(key.as_bytes()), &mut out) };
        if found == 0 {
            return None;
        }
        // The host promises this slice outlives the instance, but copying keeps
        // the plugin's own API owned and immune to a host that gets it wrong.
        let bytes = unsafe { out.as_bytes() };
        Some(String::from_utf8_lossy(bytes).into_owned())
    })
    .flatten()
}

/// Emit a log line through the host, if it offered the callback.
pub fn host_log(level: u32, target: &str, message: &str) {
    with_host(|host| {
        if let Some(log) = host.log {
            unsafe {
                log(
                    host.ctx,
                    level,
                    SaciSlice::from_bytes(target.as_bytes()),
                    SaciSlice::from_bytes(message.as_bytes()),
                );
            }
        }
    });
}

/// Record a metric through the host, if it offered the callback.
pub fn host_metric(name: &str, value: f64) {
    with_host(|host| {
        if let Some(metric) = host.metric {
            unsafe { metric(host.ctx, SaciSlice::from_bytes(name.as_bytes()), value) }
        }
    });
}

/// Hand a `Vec` to the host as a plugin-owned buffer.
///
/// An empty vector becomes a null buffer: `Vec::new()` has a dangling pointer,
/// and there is nothing to free, so sending null keeps
/// `free_buffer` a clean no-op instead of reconstructing a dangling `Vec`.
pub fn vec_into_buffer(bytes: Vec<u8>) -> SaciBuffer {
    if bytes.capacity() == 0 {
        return SaciBuffer::null();
    }
    let mut bytes = bytes;
    let buffer = SaciBuffer {
        ptr: bytes.as_mut_ptr(),
        len: bytes.len(),
        cap: bytes.capacity(),
    };
    core::mem::forget(bytes);
    buffer
}

/// Reclaim a buffer this plugin allocated.
///
/// # Safety
///
/// `buffer` must have come from [`vec_into_buffer`] in this same library and
/// must not have been freed already.
pub unsafe fn free_buffer(buffer: SaciBuffer) {
    if buffer.ptr.is_null() || buffer.cap == 0 {
        return;
    }
    drop(unsafe { Vec::from_raw_parts(buffer.ptr, buffer.len, buffer.cap) });
}

/// Write a message into a host-provided error slot, if the host supplied one.
fn write_err(err: *mut SaciBuffer, message: &str) {
    if err.is_null() {
        return;
    }
    unsafe { *err = vec_into_buffer(message.as_bytes().to_vec()) };
}

/// Run `body`, converting a panic into [`SaciStatus::PERMANENT`].
///
/// Unwinding out of an `extern "C"` frame aborts, so every boundary call funnels
/// through here. The payload's message is forwarded when it is a string, which
/// covers `panic!`, `unwrap` and `expect`.
fn guard(err: *mut SaciBuffer, body: impl FnOnce() -> SaciStatus) -> SaciStatus {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(status) => status,
        Err(payload) => {
            let message = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "plugin panicked with a non-string payload".to_string()
            };
            write_err(err, &format!("panic: {message}"));
            SaciStatus::PERMANENT
        }
    }
}

#[derive(serde::Serialize)]
struct ManifestComponent {
    name: String,
    arrow_schema_ipc_base64: String,
}

#[derive(serde::Serialize)]
struct Manifest<'a> {
    name: &'a str,
    version: &'a str,
    stateful: bool,
    schema_fingerprint: String,
    components: Vec<ManifestComponent>,
}

/// Build the JSON manifest the host parses at load.
///
/// Components are sorted by name so the order is stable across runs and
/// matches what a host comparing two loads expects. A component whose schema
/// fails to serialise gets empty bytes rather than aborting the whole
/// descriptor: the host rejects a zero-length schema with a message naming the
/// component, which beats a load failure that names nothing.
pub fn manifest_json<S: ProcessorStateSpec>(
    pipeline: &Pipeline,
    version: &str,
) -> Result<String, String> {
    let registry = pipeline.data.schemas();

    let mut entries: Vec<(&'static str, std::sync::Arc<arrow_schema::Schema>)> = registry
        .iter()
        .map(|(name, entry)| (*name, entry.schema.clone()))
        .collect();
    entries.sort_by_key(|(name, _)| *name);

    let engine = base64::engine::general_purpose::STANDARD;
    let components = entries
        .into_iter()
        .map(|(name, schema)| ManifestComponent {
            name: name.to_string(),
            arrow_schema_ipc_base64: engine
                .encode(schema_to_ipc_bytes(&schema).unwrap_or_default()),
        })
        .collect();

    let manifest = Manifest {
        name: pipeline.name(),
        version,
        stateful: S::STATEFUL,
        schema_fingerprint: fingerprint_hex(registry.fingerprint()),
        components,
    };

    serde_json::to_string(&manifest).map_err(|e| format!("manifest encode: {e}"))
}

/// Body of the ABI's `describe` slot.
///
/// # Safety
///
/// `instance` must come from [`PluginInstance::into_raw`]. `manifest_json` and
/// `err` must be writable or null.
pub unsafe fn describe_impl<S: ProcessorStateSpec>(
    instance: *mut core::ffi::c_void,
    build: fn() -> Pipeline,
    version: &str,
    manifest_json_out: *mut SaciBuffer,
    err: *mut SaciBuffer,
) -> SaciStatus {
    guard(err, || {
        let Some(instance) = (unsafe { PluginInstance::borrow(instance) }) else {
            write_err(err, "describe called with a null instance");
            return SaciStatus::PERMANENT;
        };
        if manifest_json_out.is_null() {
            write_err(err, "describe called with a null manifest out-parameter");
            return SaciStatus::PERMANENT;
        }

        let _scope = HostScope::enter(&instance.host);
        let pipeline = instance.slot.pipeline(build);

        match manifest_json::<S>(&pipeline, version) {
            Ok(json) => {
                unsafe { *manifest_json_out = vec_into_buffer(json.into_bytes()) };
                SaciStatus::OK
            }
            Err(message) => {
                write_err(err, &message);
                SaciStatus::PERMANENT
            }
        }
    })
}

/// Body of the ABI's `run_batch` slot.
///
/// # Safety
///
/// `instance` must come from [`PluginInstance::into_raw`]. `input` and `prior`
/// must describe readable bytes for the duration of the call. `out` and `err`
/// must be writable or null.
#[allow(clippy::too_many_arguments)]
pub unsafe fn run_batch_impl<S: ProcessorStateSpec>(
    instance: *mut core::ffi::c_void,
    build: fn() -> Pipeline,
    input: SaciSlice,
    prior: SaciSlice,
    has_prior: i32,
    out: *mut SaciRunResult,
    err: *mut SaciBuffer,
) -> SaciStatus {
    guard(err, || {
        let started = std::time::Instant::now();

        let Some(instance) = (unsafe { PluginInstance::borrow(instance) }) else {
            write_err(err, "run_batch called with a null instance");
            return SaciStatus::PERMANENT;
        };
        if out.is_null() {
            write_err(err, "run_batch called with a null result out-parameter");
            return SaciStatus::PERMANENT;
        }

        let _scope = HostScope::enter(&instance.host);

        let input_bytes = unsafe { input.as_bytes() };
        let mut reader: &[u8] = input_bytes;
        let mut dataset = match Dataset::read_ipc(&mut reader) {
            Ok(dataset) => dataset,
            Err(e) => {
                write_err(err, &format!("ipc decode: {e}"));
                return SaciStatus::PERMANENT;
            }
        };

        // Measured before the state blob merges in, so `rows_in` reports the
        // data plane only.
        let rows_in = dataset.rows() as u64;

        // `has_prior` is authoritative: a zero flag means a cold start even if
        // the slice happens to be non-empty.
        let prior_bytes = if has_prior == 0 {
            None
        } else {
            Some(unsafe { prior.as_bytes() })
        };

        if let Err(e) = S::restore(&mut dataset, prior_bytes) {
            write_err(err, &format!("state restore: {e}"));
            return SaciStatus::PERMANENT;
        }

        let pipeline = instance.slot.pipeline(build);
        let stats = match pollster::block_on(pipeline.run_on_with_stats(&mut dataset)) {
            Ok(stats) => stats,
            Err(e) => {
                let (is_retryable, message) = classify_run_error(&e);
                write_err(err, &message);
                return if is_retryable {
                    SaciStatus::RETRYABLE
                } else {
                    SaciStatus::PERMANENT
                };
            }
        };

        let rows_out = dataset.rows() as u64;

        let checkpoint = match S::capture(&dataset) {
            Ok(checkpoint) => checkpoint,
            Err(e) => {
                write_err(err, &format!("state capture: {e}"));
                return SaciStatus::PERMANENT;
            }
        };

        let mut output: Vec<u8> = Vec::new();
        if let Err(e) = dataset.write_ipc(&mut output) {
            write_err(err, &format!("ipc encode: {e}"));
            return SaciStatus::PERMANENT;
        }

        let (checkpoint_buffer, has_checkpoint) = match checkpoint {
            Some(blob) => (vec_into_buffer(blob), 1),
            None => (SaciBuffer::null(), 0),
        };
        let routes: Option<Vec<String>> = dataset
            .get_resource::<RouteDecision>()
            .map(|decision| decision.0.clone());
        let (routes_buffer, has_routes) = match routes {
            Some(list) => {
                let json = match serde_json::to_string(&list) {
                    Ok(json) => json,
                    Err(e) => {
                        write_err(err, &format!("routes encode: {e}"));
                        return SaciStatus::PERMANENT;
                    }
                };
                (vec_into_buffer(json.into_bytes()), 1)
            }
            None => (SaciBuffer::null(), 0),
        };

        unsafe {
            *out = SaciRunResult {
                output: vec_into_buffer(output),
                checkpoint: checkpoint_buffer,
                has_checkpoint,
                metrics: SaciRunMetrics {
                    wall_ns: started.elapsed().as_nanos() as u64,
                    rows_in,
                    rows_out,
                    systems_run: stats.systems_run as u32,
                    retries: stats.retries_this_batch,
                },
                routes: routes_buffer,
                has_routes,
            };
        }
        SaciStatus::OK
    })
}

// Log levels reach plugin authors through the public `crate::host` module; the
// `log_level` re-export above is what an expansion and that module both use.

#[cfg(test)]
mod tests {
    use super::*;
    use saci_core::sdk::NoState;

    #[test]
    fn an_empty_vec_becomes_a_null_buffer_that_frees_cleanly() {
        let buffer = vec_into_buffer(Vec::new());
        assert!(buffer.is_null());
        assert_eq!(buffer.cap, 0);
        // A no-op, and specifically not a `Vec::from_raw_parts` on a dangling
        // pointer.
        unsafe { free_buffer(buffer) };
    }

    #[test]
    fn buffers_round_trip_their_bytes_and_free() {
        let buffer = vec_into_buffer(vec![1, 2, 3, 4]);
        assert!(!buffer.is_null());
        assert_eq!(buffer.len, 4);
        assert_eq!(unsafe { buffer.as_bytes() }, &[1, 2, 3, 4]);
        unsafe { free_buffer(buffer) };
    }

    #[test]
    fn config_get_outside_a_call_is_none() {
        assert_eq!(config_get("anything"), None);
    }

    #[test]
    fn host_calls_outside_a_scope_do_not_dereference_null() {
        host_log(log_level::INFO, "t", "m");
        host_metric("m", 1.0);
    }

    #[test]
    fn a_host_with_no_callbacks_is_tolerated() {
        let host = SaciHostV1::empty();
        let _scope = HostScope::enter(&host);
        assert_eq!(config_get("k"), None);
        host_log(log_level::WARN, "t", "m");
        host_metric("m", 2.0);
    }

    // A host that answers get_config from a fixed table, so the scope plumbing
    // can be exercised without a real host.
    static TABLE: &[(&str, &str)] = &[("multiplier", "10"), ("empty", "")];

    unsafe extern "C" fn table_get_config(
        _ctx: *mut core::ffi::c_void,
        key: SaciSlice,
        out: *mut SaciSlice,
    ) -> i32 {
        let key = core::str::from_utf8(unsafe { key.as_bytes() }).unwrap_or("");
        for (name, value) in TABLE {
            if *name == key {
                unsafe { *out = SaciSlice::from_bytes(value.as_bytes()) };
                return 1;
            }
        }
        0
    }

    fn table_host() -> SaciHostV1 {
        SaciHostV1 {
            ctx: core::ptr::null_mut(),
            log: None,
            metric: None,
            get_config: Some(table_get_config),
        }
    }

    #[test]
    fn config_get_reads_through_the_host_vtable() {
        let host = table_host();
        let _scope = HostScope::enter(&host);
        assert_eq!(config_get("multiplier").as_deref(), Some("10"));
        assert_eq!(config_get("empty").as_deref(), Some(""));
        assert_eq!(config_get("absent"), None);
    }

    #[test]
    fn a_scope_restores_the_previous_host_on_drop() {
        let host = table_host();
        {
            let _scope = HostScope::enter(&host);
            assert!(config_get("multiplier").is_some());
        }
        assert_eq!(config_get("multiplier"), None);
    }

    #[test]
    fn manifest_json_sorts_components_and_carries_the_fingerprint() {
        let mut pipeline = Pipeline::new("demo");
        // Two components registered out of alphabetical order, so the sort is
        // observable in the output.
        pipeline.data.register_raw_component("Zeta", zeta_schema());
        pipeline
            .data
            .register_raw_component("Alpha", alpha_schema());

        let json = manifest_json::<NoState>(&pipeline, "9.9.9").expect("manifest");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");

        assert_eq!(parsed["name"], "demo");
        assert_eq!(parsed["version"], "9.9.9");
        assert_eq!(parsed["stateful"], false);

        let components = parsed["components"].as_array().expect("components array");
        assert_eq!(components.len(), 2);
        assert_eq!(components[0]["name"], "Alpha");
        assert_eq!(components[1]["name"], "Zeta");
        assert!(
            !components[0]["arrow_schema_ipc_base64"]
                .as_str()
                .expect("base64 string")
                .is_empty(),
            "a registered component must carry schema bytes"
        );

        let fingerprint = parsed["schema_fingerprint"].as_str().expect("fingerprint");
        assert_eq!(fingerprint.len(), 8, "got {fingerprint}");
        assert_eq!(
            fingerprint,
            fingerprint_hex(pipeline.data.schemas().fingerprint())
        );
    }

    #[test]
    fn manifest_json_reports_stateful_from_the_spec() {
        let pipeline = Pipeline::new("stateful-demo");
        let json = manifest_json::<saci_core::sdk::Stateful<StateRow>>(&pipeline, "0.1.0")
            .expect("manifest");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        assert_eq!(parsed["stateful"], true);
    }

    fn alpha_schema() -> std::sync::Arc<arrow_schema::Schema> {
        std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "a",
            arrow_schema::DataType::Int64,
            false,
        )]))
    }

    fn zeta_schema() -> std::sync::Arc<arrow_schema::Schema> {
        std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "z",
            arrow_schema::DataType::Float64,
            false,
        )]))
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct StateRow {
        total: u64,
    }

    impl saci_core::Component for StateRow {
        fn name() -> &'static str {
            "StateRow"
        }
        fn schema() -> std::sync::Arc<arrow_schema::Schema> {
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "total",
                arrow_schema::DataType::UInt64,
                false,
            )]))
        }
    }
}
