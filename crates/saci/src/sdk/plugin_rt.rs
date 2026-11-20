//! The native plugin side of the C ABI, called from the `#[plugin]` thunks.
//!
//! Reachable only as `saci::__private::plugin`. Almost everything the boundary
//! needs lives here as an ordinary function rather than inside the macro body,
//! so it is unit-testable and exists once in the binary instead of once per
//! expansion.

use std::panic::{AssertUnwindSafe, catch_unwind};

use base64::Engine as _;

use super::glue::{self, HostIo, HostScope, PipelineSlot};
use super::host::Level;
use crate::Pipeline;

pub use crate::plugin_abi::{
    SACI_ABI_VERSION, SaciBuffer, SaciHostV1, SaciPluginV1, SaciRunMetrics, SaciRunResult,
    SaciSlice, SaciStatus,
};

/// The host vtable as an [`sdk::host`](super::host) backend.
struct PluginHost(SaciHostV1);

impl HostIo for PluginHost {
    fn config(&self, key: &str) -> Option<String> {
        let host = &self.0;
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
    }

    fn log(&self, level: Level, target: &str, message: &str) {
        let host = &self.0;
        if let Some(log) = host.log {
            unsafe {
                log(
                    host.ctx,
                    level as u32,
                    SaciSlice::from_bytes(target.as_bytes()),
                    SaciSlice::from_bytes(message.as_bytes()),
                );
            }
        }
    }

    fn metric(&self, name: &str, value: f64) {
        let host = &self.0;
        if let Some(metric) = host.metric {
            unsafe { metric(host.ctx, SaciSlice::from_bytes(name.as_bytes()), value) }
        }
    }
}

/// One loaded plugin instance.
///
/// The host may load the same library twice with different config, so the
/// pipeline and the host vtable are per-instance rather than per-process
/// statics. `saci_plugin_v1` boxes one of these and hands the pointer back as
/// [`SaciPluginV1::instance`].
pub struct PluginInstance {
    slot: PipelineSlot,
    host: PluginHost,
}

impl PluginInstance {
    /// Wrap a host vtable for a fresh instance.
    pub const fn new(host: SaciHostV1) -> Self {
        Self {
            slot: PipelineSlot::new(),
            host: PluginHost(host),
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
    unsafe fn borrow<'a>(instance: *mut core::ffi::c_void) -> Option<&'a Self> {
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

/// Hand a `Vec` to the host as a plugin-owned buffer.
///
/// An empty vector becomes a null buffer: `Vec::new()` has a dangling pointer,
/// and there is nothing to free, so sending null keeps
/// `free_buffer` a clean no-op instead of reconstructing a dangling `Vec`.
fn vec_into_buffer(bytes: Vec<u8>) -> SaciBuffer {
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
/// `buffer` must have come from this library and must not have been freed
/// already.
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
struct Manifest {
    name: String,
    version: String,
    stateful: bool,
    schema_fingerprint: String,
    components: Vec<ManifestComponent>,
}

/// Build the JSON manifest the host parses at load.
///
/// The manifest carries [`glue::describe`]'s record with each schema in
/// base64. A component whose schema fails to serialise gets empty bytes
/// rather than aborting the whole descriptor: the host rejects a zero-length
/// schema with a message naming the component, which beats a load failure
/// that names nothing.
fn manifest_json(pipeline: &Pipeline, version: &str) -> Result<String, String> {
    let descriptor = glue::describe(pipeline, version);
    let engine = base64::engine::general_purpose::STANDARD;
    let manifest = Manifest {
        name: descriptor.name,
        version: descriptor.version,
        stateful: descriptor.stateful,
        schema_fingerprint: descriptor.schema_fingerprint,
        components: descriptor
            .components
            .into_iter()
            .map(|c| ManifestComponent {
                name: c.name,
                arrow_schema_ipc_base64: engine.encode(c.arrow_schema_ipc),
            })
            .collect(),
    };
    serde_json::to_string(&manifest).map_err(|e| format!("manifest encode: {e}"))
}

/// Body of the ABI's `describe` slot.
///
/// # Safety
///
/// `instance` must come from [`PluginInstance::into_raw`]. `manifest_json` and
/// `err` must be writable or null.
pub unsafe fn describe_impl(
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

        match manifest_json(pipeline, version) {
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
pub unsafe fn run_batch_impl(
    instance: *mut core::ffi::c_void,
    build: fn() -> Pipeline,
    input: SaciSlice,
    prior: SaciSlice,
    has_prior: i32,
    out: *mut SaciRunResult,
    err: *mut SaciBuffer,
) -> SaciStatus {
    guard(err, || {
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
        // `has_prior` is authoritative: a zero flag means a cold start even if
        // the slice happens to be non-empty.
        let prior_bytes = if has_prior == 0 {
            None
        } else {
            Some(unsafe { prior.as_bytes() })
        };

        let pipeline = instance.slot.pipeline(build);
        let batch = match pollster::block_on(glue::run_batch(pipeline, input_bytes, prior_bytes)) {
            Ok(batch) => batch,
            Err(e) => {
                write_err(err, &e.message);
                return if e.retryable {
                    SaciStatus::RETRYABLE
                } else {
                    SaciStatus::PERMANENT
                };
            }
        };

        let (routes_buffer, has_routes) = match batch.routes {
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
        let (checkpoint_buffer, has_checkpoint) = match batch.checkpoint {
            Some(blob) => (vec_into_buffer(blob), 1),
            None => (SaciBuffer::null(), 0),
        };

        let m = batch.metrics;
        unsafe {
            *out = SaciRunResult {
                output: vec_into_buffer(batch.output),
                checkpoint: checkpoint_buffer,
                has_checkpoint,
                metrics: SaciRunMetrics {
                    wall_ns: m.wall_ns,
                    rows_in: m.rows_in,
                    rows_out: m.rows_out,
                    systems_run: m.systems_run,
                    retries: m.retries,
                },
                routes: routes_buffer,
                has_routes,
            };
        }
        SaciStatus::OK
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sdk::host;

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
    fn a_host_with_no_callbacks_is_tolerated() {
        let host = PluginHost(SaciHostV1::empty());
        let _scope = HostScope::enter(&host);
        assert_eq!(host::config("k"), None);
        host::warn("t", "m");
        host::metric("m", 2.0);
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

    fn table_host() -> PluginHost {
        PluginHost(SaciHostV1 {
            ctx: core::ptr::null_mut(),
            log: None,
            metric: None,
            get_config: Some(table_get_config),
        })
    }

    #[test]
    fn config_reads_through_the_host_vtable() {
        let host = table_host();
        let _scope = HostScope::enter(&host);
        assert_eq!(host::config("multiplier").as_deref(), Some("10"));
        assert_eq!(host::config("empty").as_deref(), Some(""));
        assert_eq!(host::config("absent"), None);
    }

    #[test]
    fn a_scope_restores_the_previous_host_on_drop() {
        let host = table_host();
        {
            let _scope = HostScope::enter(&host);
            assert!(host::config("multiplier").is_some());
        }
        assert_eq!(host::config("multiplier"), None);
    }

    #[test]
    fn manifest_json_sorts_components_and_carries_the_fingerprint() {
        let pipeline = Pipeline::new("demo")
            .schema("Zeta", zeta_schema())
            .schema("Alpha", alpha_schema());

        let json = manifest_json(&pipeline, "9.9.9").expect("manifest");
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
            glue::fingerprint_hex(pipeline.data().schemas().fingerprint())
        );
    }

    #[test]
    fn manifest_json_reports_stateful_from_the_pipeline() {
        let pipeline = Pipeline::new("stateful-demo").state::<StateRow>();
        let json = manifest_json(&pipeline, "0.1.0").expect("manifest");
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

    impl crate::Component for StateRow {
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
