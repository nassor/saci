//! The three callbacks a native plugin may call back through.
//!
//! These are the C ABI counterpart of the `host-io` WIT interface the WASM
//! processor imports, and they route to the same places: `tracing` for log lines
//! and metrics, the load-time config map for `get_config`.

use std::collections::HashMap;
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};

use saci_plugin_abi::{SaciHostV1, SaciSlice, log_level};

/// What the host callbacks read.
///
/// Immutable once built. `get_config` hands the plugin a [`SaciSlice`] borrowed
/// straight out of `config`, and the ABI promises that slice stays valid for
/// the life of the plugin instance. That holds only because nothing ever
/// mutates this struct.
///
/// `target` is the library file stem, not the pipeline name. The two are
/// separate because a plugin can log during `describe`, before any manifest
/// name exists: `NativePluginRuntime::name` reports the manifest name, while
/// every log line the plugin emits carries the file stem.
pub(crate) struct HostCtx {
    /// Key and value config from the `[pipeline.plugin.config]` table.
    pub config: HashMap<String, String>,
    /// Library file stem, prefixing every log line and metric.
    pub target: String,
}

/// Build the host vtable pointing at `ctx`.
///
/// The caller keeps `ctx` alive at a stable address for the whole life of the
/// plugin instance, because the plugin stores this pointer and calls back
/// through it, including from `destroy`.
pub(crate) fn host_vtable(ctx: &HostCtx) -> SaciHostV1 {
    SaciHostV1 {
        // The callbacks only ever reconstruct a shared reference, so the
        // `*mut` the ABI asks for is never written through.
        ctx: std::ptr::from_ref(ctx).cast_mut().cast::<c_void>(),
        log: Some(host_log),
        metric: Some(host_metric),
        get_config: Some(host_get_config),
    }
}

/// Recover the context a callback was handed.
///
/// # Safety
///
/// `ctx` is either null or the pointer [`host_vtable`] built, and the
/// [`HostCtx`] it names outlives the call.
unsafe fn ctx_ref<'a>(ctx: *mut c_void) -> Option<&'a HostCtx> {
    if ctx.is_null() {
        None
    } else {
        Some(unsafe { &*ctx.cast::<HostCtx>() })
    }
}

/// Emit a structured log line on the plugin's behalf.
///
/// # Safety
///
/// `ctx` is the pointer [`host_vtable`] stored, and both slices are valid for
/// the duration of this call.
pub(crate) unsafe extern "C" fn host_log(
    ctx: *mut c_void,
    level: u32,
    target: SaciSlice,
    message: SaciSlice,
) {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let Some(host) = (unsafe { ctx_ref(ctx) }) else {
            return;
        };
        let module = String::from_utf8_lossy(unsafe { target.as_bytes() });
        let text = String::from_utf8_lossy(unsafe { message.as_bytes() });
        emit_log(&host.target, level, &module, &text);
    }));

    if outcome.is_err() {
        report_callback_panic("log");
    }
}

/// Record a named metric on the plugin's behalf.
///
/// # Safety
///
/// `ctx` is the pointer [`host_vtable`] stored, and `name` is valid for the
/// duration of this call.
pub(crate) unsafe extern "C" fn host_metric(ctx: *mut c_void, name: SaciSlice, value: f64) {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let Some(host) = (unsafe { ctx_ref(ctx) }) else {
            return;
        };
        let metric = String::from_utf8_lossy(unsafe { name.as_bytes() });
        record_metric(&host.target, &metric, value);
    }));

    if outcome.is_err() {
        report_callback_panic("metric");
    }
}

/// Look up a config value for the plugin.
///
/// Returns 1 and writes `out` when the key is present, 0 otherwise. A null
/// context, a null `out`, and a key that is not UTF-8 all read as absent, so a
/// misbehaving plugin gets a miss rather than a dereference.
///
/// # Safety
///
/// `ctx` is the pointer [`host_vtable`] stored, `key` is valid for the duration
/// of this call, and `out` is either null or writable.
pub(crate) unsafe extern "C" fn host_get_config(
    ctx: *mut c_void,
    key: SaciSlice,
    out: *mut SaciSlice,
) -> i32 {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if out.is_null() {
            return 0;
        }
        let Some(host) = (unsafe { ctx_ref(ctx) }) else {
            return 0;
        };
        let Ok(key) = std::str::from_utf8(unsafe { key.as_bytes() }) else {
            return 0;
        };
        let Some(value) = host.config.get(key) else {
            return 0;
        };

        // Borrowed from `HostCtx`, which the runtime keeps alive and never
        // mutates, so the slice outlives the plugin instance as the ABI says.
        unsafe { out.write(SaciSlice::from_bytes(value.as_bytes())) };
        1
    }));

    outcome.unwrap_or(0)
}

/// Route one log line to the host subscriber.
fn emit_log(plugin: &str, level: u32, module: &str, message: &str) {
    // The gate keeps the dep out of plain `plugin` builds, and the fallback
    // keeps the values visible instead of dropping them.
    #[cfg(feature = "tracing")]
    {
        use tracing::{debug, error, info, trace, warn};
        match level {
            log_level::TRACE => trace!(plugin = %plugin, target = %module, "{}", message),
            log_level::DEBUG => debug!(plugin = %plugin, target = %module, "{}", message),
            log_level::WARN => warn!(plugin = %plugin, target = %module, "{}", message),
            log_level::ERROR => error!(plugin = %plugin, target = %module, "{}", message),
            // The ABI says an unrecognised level reads as INFO.
            _ => info!(plugin = %plugin, target = %module, "{}", message),
        }
    }
    #[cfg(not(feature = "tracing"))]
    {
        let level_str = match level {
            log_level::TRACE => "TRACE",
            log_level::DEBUG => "DEBUG",
            log_level::WARN => "WARN",
            log_level::ERROR => "ERROR",
            _ => "INFO",
        };
        eprintln!("[{level_str}] [{plugin}] {module}: {message}");
    }
}

/// Record one metric value as a `trace` event.
///
/// The C ABI `metric` callback (plugin-chosen names) writes no Prometheus
/// series: the six `saci_processor_*` names are host-reserved for the per-batch
/// numbers the runtime reports. Those numbers land here as trace events too,
/// and `NativePluginRuntime::report_metrics` records the series on top,
/// exactly like the wasm host.
pub(crate) fn record_metric(plugin: &str, name: &str, value: f64) {
    #[cfg(feature = "tracing")]
    {
        tracing::trace!(plugin = %plugin, metric = %name, value = value, "plugin metric");
    }
    #[cfg(not(feature = "tracing"))]
    {
        let _ = (plugin, name, value);
    }
}

/// Report a panic caught at the boundary.
///
/// Unwinding out of an `extern "C"` function aborts the process, so a panic in
/// a callback is logged and swallowed.
fn report_callback_panic(callback: &str) {
    #[cfg(feature = "tracing")]
    {
        tracing::error!(callback = %callback, "panic in a plugin host callback, swallowed");
    }
    #[cfg(not(feature = "tracing"))]
    {
        eprintln!("[ERROR] panic in plugin host callback `{callback}`, swallowed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(pairs: &[(&str, &str)]) -> HostCtx {
        HostCtx {
            config: pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            target: "fixture".to_string(),
        }
    }

    /// Call `get_config` the way a plugin would.
    fn get_config(host: &HostCtx, key: &[u8]) -> Option<Vec<u8>> {
        let vtable = host_vtable(host);
        let callback = vtable.get_config.expect("get_config slot filled");
        let mut out = SaciSlice::empty();
        let found = unsafe { callback(vtable.ctx, SaciSlice::from_bytes(key), &mut out) };
        if found == 1 {
            Some(unsafe { out.as_bytes() }.to_vec())
        } else {
            None
        }
    }

    #[test]
    fn get_config_returns_a_present_value() {
        let host = ctx(&[("smoketest.multiplier", "10")]);
        assert_eq!(
            get_config(&host, b"smoketest.multiplier"),
            Some(b"10".to_vec())
        );
    }

    #[test]
    fn get_config_misses_on_an_absent_key() {
        let host = ctx(&[("a", "1")]);
        assert_eq!(get_config(&host, b"b"), None);
    }

    #[test]
    fn get_config_misses_on_a_non_utf8_key() {
        let host = ctx(&[("a", "1")]);
        assert_eq!(get_config(&host, &[0xff, 0xfe]), None);
    }

    #[test]
    fn get_config_misses_on_a_null_context_or_output() {
        let host = ctx(&[("a", "1")]);
        let vtable = host_vtable(&host);
        let callback = vtable.get_config.expect("get_config slot filled");

        let mut out = SaciSlice::empty();
        let key = SaciSlice::from_bytes(b"a");
        assert_eq!(
            unsafe { callback(std::ptr::null_mut(), key, &mut out) },
            0,
            "a null context reads as a miss"
        );
        assert_eq!(
            unsafe { callback(vtable.ctx, key, std::ptr::null_mut()) },
            0,
            "a null output pointer reads as a miss"
        );
    }

    #[test]
    fn log_and_metric_accept_a_null_context() {
        let vtable = host_vtable(&ctx(&[]));
        let log = vtable.log.expect("log slot filled");
        let metric = vtable.metric.expect("metric slot filled");

        unsafe {
            log(
                std::ptr::null_mut(),
                log_level::ERROR,
                SaciSlice::from_bytes(b"mod"),
                SaciSlice::from_bytes(b"msg"),
            );
            metric(std::ptr::null_mut(), SaciSlice::from_bytes(b"m"), 1.0);
        }
    }

    #[test]
    fn log_accepts_every_level_and_an_unrecognised_one() {
        let host = ctx(&[]);
        let vtable = host_vtable(&host);
        let log = vtable.log.expect("log slot filled");

        for level in [
            log_level::TRACE,
            log_level::DEBUG,
            log_level::INFO,
            log_level::WARN,
            log_level::ERROR,
            99,
        ] {
            unsafe {
                log(
                    vtable.ctx,
                    level,
                    SaciSlice::from_bytes(b"mod"),
                    SaciSlice::from_bytes(b"msg"),
                );
            }
        }
    }
}
