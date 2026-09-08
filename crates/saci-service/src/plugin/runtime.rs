//! [`NativePluginRuntime`], the host side of the native plugin ABI.

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use saci_core::runtime::RuntimeOutput;
use saci_core::{Dataset, SaciError, SaciResult};
use saci_plugin_abi::{SaciBuffer, SaciRunMetrics, SaciRunResult, SaciSlice, SaciStatus};

use crate::descriptor::template_dataset_from;

use super::host_impl::{self, HostCtx};
use super::loader::LoadedPlugin;
use super::manifest::PluginManifest;

/// What a plugin reported about one batch.
///
/// Mirrors the ABI's `SaciRunMetrics` so a caller reading metrics off the
/// runtime needs no dependency on `saci-plugin-abi`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PluginBatchMetrics {
    /// Wall-clock nanoseconds the plugin spent on the batch.
    pub wall_ns: u64,
    /// Live rows the plugin decoded from the input.
    pub rows_in: u64,
    /// Live rows the plugin encoded into the output.
    pub rows_out: u64,
    /// Systems the plugin ran.
    pub systems_run: u32,
    /// Retry attempts the plugin made internally.
    pub retries: u32,
}

impl From<SaciRunMetrics> for PluginBatchMetrics {
    fn from(metrics: SaciRunMetrics) -> Self {
        Self {
            wall_ns: metrics.wall_ns,
            rows_in: metrics.rows_in,
            rows_out: metrics.rows_out,
            systems_run: metrics.systems_run,
            retries: metrics.retries,
        }
    }
}

/// Host-side native plugin runtime implementing
/// [`saci_core::runtime::PipelineRuntime`].
///
/// Each call serialises the dataset to Arrow IPC bytes, calls the plugin's
/// `run_batch` through the loaded vtable, and reads the output bytes back over
/// the dataset. The plugin keeps whatever state it needs between batches, and
/// the host carries it as an opaque checkpoint blob.
///
/// Load is where a plugin's self-description is checked: the manifest is
/// parsed, every component schema is decoded, and the declared fingerprint must
/// match what those schemas hash to. That is what makes
/// [`template_dataset`](saci_core::runtime::PipelineRuntime::template_dataset)
/// infallible.
///
/// A plugin cannot be preempted. There is no epoch deadline equivalent, so a
/// plugin that never returns holds the calling task forever.
pub struct NativePluginRuntime {
    /// Field order is load-bearing: `plugin` drops first, so `destroy` runs
    /// while `host_ctx` is still alive. The plugin holds a `SaciHostV1` pointing
    /// into `host_ctx` and may call back through it from `destroy`.
    plugin: LoadedPlugin,
    host_ctx: Box<HostCtx>,
    manifest: PluginManifest,
    /// Component name and Arrow IPC schema bytes, decoded once at load.
    components: Vec<(String, Vec<u8>)>,
    /// Metrics from the most recent batch, for callers that report them.
    last_metrics: Mutex<Option<PluginBatchMetrics>>,
    /// The workflow this plugin belongs to, and this node's declared id. Set
    /// by [`with_identity`](Self::with_identity); empty for a runtime built
    /// directly and never given an identity. This attributes the
    /// `saci_processor_*` series recorded by `report_metrics` and the
    /// `processor.batch` span.
    workflow_id: String,
    processor_id: String,
}

/// Reports what the plugin declared about itself, not the pointers behind it.
///
/// `unwrap_err` and `expect_err` on a `SaciResult<NativePluginRuntime>` require
/// this, and a caller logging a runtime wants its identity rather than a
/// library handle.
impl std::fmt::Debug for NativePluginRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativePluginRuntime")
            .field("name", &self.manifest.name)
            .field("version", &self.manifest.version)
            .field("stateful", &self.manifest.stateful)
            .field("target", &self.host_ctx.target)
            .field(
                "components",
                &self
                    .components
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl NativePluginRuntime {
    /// Load a plugin from a shared library and validate what it declares.
    ///
    /// The manifest is authoritative for identity, so there is no
    /// caller-supplied name. `describe` runs once here, which puts every
    /// failure a plugin can report about itself at load time rather than on the
    /// first batch.
    ///
    /// `config` reaches the plugin through the `get_config` callback and is
    /// never mutated afterwards.
    pub fn open(path: &Path, config: HashMap<String, String>) -> SaciResult<Self> {
        let target = path.file_stem().or_else(|| path.file_name()).map_or_else(
            || path.display().to_string(),
            |stem| stem.to_string_lossy().into_owned(),
        );

        // Boxed so the address stays put: the plugin stores a pointer to it.
        let host_ctx = Box::new(HostCtx { config, target });
        let host = Box::new(host_impl::host_vtable(host_ctx.as_ref()));
        let plugin = LoadedPlugin::open(path, host)?;

        let manifest_json = describe(&plugin, path)?;
        let manifest = PluginManifest::parse(&manifest_json)?;
        let components = manifest.decode_components()?;

        let template = template_dataset_from(
            components
                .iter()
                .map(|(name, schema_ipc)| (name.as_str(), schema_ipc.as_slice())),
        );

        // This is what `SaciStatus::SCHEMA_MISMATCH` is reserved for. A plugin
        // embeds its schema constants and its fingerprint separately, so the
        // two drift independently; recomputing the fingerprint from the decoded
        // schemas catches a plugin whose constants no longer describe what it
        // says they describe.
        let host_fingerprint = format!("{:08x}", template.schemas().fingerprint());
        if manifest.schema_fingerprint != host_fingerprint {
            return Err(SaciError::configuration(format!(
                "plugin `{}` declares schema fingerprint {} but its component schemas hash to {}",
                path.display(),
                manifest.schema_fingerprint,
                host_fingerprint
            )));
        }

        #[cfg(feature = "tracing")]
        tracing::debug!(
            plugin = %manifest.name,
            version = %manifest.version,
            stateful = manifest.stateful,
            components = components.len(),
            "loaded native plugin"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = (&manifest.version, manifest.stateful);

        Ok(Self {
            plugin,
            host_ctx,
            manifest,
            components,
            last_metrics: Mutex::new(None),
            workflow_id: String::new(),
            processor_id: String::new(),
        })
    }

    /// Declare this runtime's workflow and node id.
    ///
    /// Consuming rather than a setter because a runtime's identity never
    /// changes once built, and the service builder assigns it immediately
    /// after loading. Left unset, the `processor.batch` span carries empty
    /// `workflow`/`processor` fields.
    #[must_use]
    pub fn with_identity(mut self, workflow_id: String, processor_id: String) -> Self {
        self.workflow_id = workflow_id;
        self.processor_id = processor_id;
        self
    }

    /// What the plugin reported about its most recent batch.
    ///
    /// `None` until the first `run_on` or `run_on_with_state` returns.
    #[must_use]
    pub fn last_batch_metrics(&self) -> Option<PluginBatchMetrics> {
        match self.last_metrics.lock() {
            Ok(guard) => *guard,
            // A panic while holding the lock leaves the value intact: it is a
            // plain `Copy` struct with no invariant to break.
            Err(poisoned) => *poisoned.into_inner(),
        }
    }

    /// Store the batch metrics, emit them through the host metric sink, and
    /// record the `saci_processor_*` series exactly like the wasm host does.
    fn report_metrics(&self, metrics: PluginBatchMetrics) {
        let target = self.host_ctx.target.as_str();
        host_impl::record_metric(target, "plugin.wall_ns", metrics.wall_ns as f64);
        host_impl::record_metric(target, "plugin.rows_in", metrics.rows_in as f64);
        host_impl::record_metric(target, "plugin.rows_out", metrics.rows_out as f64);
        host_impl::record_metric(target, "plugin.systems_run", f64::from(metrics.systems_run));
        host_impl::record_metric(target, "plugin.retries", f64::from(metrics.retries));

        crate::metrics::instruments().processor_batch(
            &self.processor_id,
            metrics.wall_ns,
            metrics.rows_in,
            metrics.rows_out,
            metrics.systems_run,
            metrics.retries,
        );

        match self.last_metrics.lock() {
            Ok(mut guard) => *guard = Some(metrics),
            Err(poisoned) => *poisoned.into_inner() = Some(metrics),
        }
    }

    /// Serialise, call the plugin, and read the result back, carrying the
    /// batch's routing decision alongside the state blob.
    async fn run_batch(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> SaciResult<RuntimeOutput> {
        // The processor's span in the host-side trace. The body has no
        // `.await`, so a plain guard is correct and the plugin's own
        // `log`/`metric` callbacks land inside it.
        //
        // `debug`, not `info`: one of these opens per batch under the runner's
        // own per-item tree, and `log_level` is what keeps a subscriber from
        // materialising every one. The default `error` materialises no span at
        // all. A plugin's own `log` records keep their declared level and stay
        // visible without it.
        #[cfg(feature = "tracing")]
        let batch_span = tracing::debug_span!(
            "processor.batch",
            workflow = %self.workflow_id,
            processor = %self.processor_id,
            rows_in = data.rows() as u64,
            rows_out = tracing::field::Empty,
            systems_run = tracing::field::Empty,
            retries = tracing::field::Empty,
            plugin_wall_us = tracing::field::Empty
        );
        #[cfg(feature = "tracing")]
        let _batch_guard = batch_span.enter();

        let mut input: Vec<u8> = Vec::new();
        data.write_ipc(&mut input)?;

        let prior_slice = prior.map_or_else(SaciSlice::empty, SaciSlice::from_bytes);
        let mut result = SaciRunResult::empty();
        let mut err = SaciBuffer::null();

        // The call runs inline rather than on `spawn_blocking`. The WASM path
        // needs that hop only because `add_to_linker_sync` routes WASI imports
        // through `Handle::block_on`, which panics on a thread already driving
        // a tokio runtime. A plugin imports nothing and carries no such
        // constraint, so staying inline avoids an `Arc` and a `Sync` claim on
        // plugin state. The cost is that a wedged plugin blocks this task: the
        // ABI has no epoch deadline equivalent.
        //
        // SAFETY: `input` and `prior` outlive the call, and `result` and `err`
        // are live out-parameters the plugin may write once each.
        //
        // `result`/`err` are not read in the `Err` arm below: a panic could
        // have left either half-written, and the only sound thing to do with
        // a half-written value is not look at it. Nothing here owns an
        // allocation until `take_buffer` runs, so not reading them leaks
        // nothing on the common (pre-panic) path.
        let status = match catch_ffi_panic(|| unsafe {
            (self.plugin.run_batch_fn())(
                self.plugin.instance(),
                SaciSlice::from_bytes(&input),
                prior_slice,
                i32::from(prior.is_some()),
                &mut result,
                &mut err,
            )
        }) {
            Ok(status) => status,
            Err(msg) => {
                return Err(SaciError::SystemExecution(format!(
                    "plugin panicked in run_batch (host-caught, plugin bug): {msg}"
                )));
            }
        };

        if !status.is_ok() {
            // A failing plugin should write nothing but `err`. Returning the
            // other two costs a null check and closes the leak if it did.
            drop(self.plugin.take_buffer(result.output));
            drop(self.plugin.take_buffer(result.checkpoint));
            drop(self.plugin.take_buffer(result.routes));
            let message = self.plugin.take_buffer(err);
            return Err(run_batch_error(status, &message));
        }

        if result.output.is_null() {
            drop(self.plugin.take_buffer(result.checkpoint));
            drop(self.plugin.take_buffer(result.routes));
            drop(self.plugin.take_buffer(err));
            return Err(SaciError::SystemExecution(format!(
                "plugin permanent: `{}` returned OK from run_batch without an output buffer",
                self.manifest.name
            )));
        }

        let output = self.plugin.take_buffer(result.output);
        let checkpoint = if result.has_checkpoint == 0 {
            // Not flagged, so there is nothing to persist. A plugin that
            // allocated one anyway must still not leak it.
            drop(self.plugin.take_buffer(result.checkpoint));
            None
        } else {
            Some(self.plugin.take_buffer(result.checkpoint))
        };
        let routes = if result.has_routes == 0 {
            // Not flagged, so there is no routing decision. A plugin that
            // allocated a buffer anyway must still not leak it.
            drop(self.plugin.take_buffer(result.routes));
            None
        } else {
            let buf = self.plugin.take_buffer(result.routes);
            let list: Vec<String> = serde_json::from_slice(&buf).map_err(|e| {
                SaciError::system_execution(format!(
                    "plugin `{}` returned malformed routes JSON: {e}",
                    self.manifest.name
                ))
            })?;
            Some(list)
        };
        drop(self.plugin.take_buffer(err));

        *data = Dataset::read_ipc(&mut output.as_slice())?;
        let metrics: PluginBatchMetrics = result.metrics.into();
        #[cfg(feature = "tracing")]
        {
            batch_span.record("rows_out", metrics.rows_out);
            batch_span.record("systems_run", metrics.systems_run);
            batch_span.record("retries", metrics.retries);
            batch_span.record("plugin_wall_us", metrics.wall_ns / 1_000);
        }
        self.report_metrics(metrics);

        Ok(RuntimeOutput {
            state: checkpoint,
            routes,
        })
    }
}

#[async_trait(?Send)]
impl saci_core::runtime::PipelineRuntime for NativePluginRuntime {
    fn name(&self) -> &str {
        &self.manifest.name
    }

    async fn run_on(&self, data: &mut Dataset) -> SaciResult<()> {
        self.run_batch(data, None).await.map(|_| ())
    }

    async fn run_on_with_state(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> SaciResult<Option<Vec<u8>>> {
        self.run_batch(data, prior).await.map(|out| out.state)
    }

    async fn run_on_with_state_and_routes(
        &self,
        data: &mut Dataset,
        prior: Option<&[u8]>,
    ) -> SaciResult<RuntimeOutput> {
        self.run_batch(data, prior).await
    }

    fn declared_components(&self) -> Vec<&str> {
        self.components
            .iter()
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Report the validated manifest's self-description.
    ///
    /// `open` already parsed and validated the manifest, so every field here is
    /// what the plugin declared, never a default.
    fn descriptor_info(&self) -> saci_core::runtime::RuntimeDescriptorInfo {
        saci_core::runtime::RuntimeDescriptorInfo {
            name: self.manifest.name.clone(),
            version: self.manifest.version.clone(),
            stateful: self.manifest.stateful,
            schema_fingerprint: self.manifest.schema_fingerprint.clone(),
        }
    }

    fn template_dataset(&self) -> Dataset {
        // Infallible: `open` already decoded every schema, registered them once
        // to compute the fingerprint, and refused the plugin on a mismatch.
        template_dataset_from(
            self.components
                .iter()
                .map(|(name, schema_ipc)| (name.as_str(), schema_ipc.as_slice())),
        )
    }
}

/// Call `describe` once and copy the manifest bytes out.
fn describe(plugin: &LoadedPlugin, path: &Path) -> SaciResult<Vec<u8>> {
    let mut manifest = SaciBuffer::null();
    let mut err = SaciBuffer::null();

    // SAFETY: both out-parameters are live and the plugin writes each at most
    // once. `manifest`/`err` are not read in the panic arm below; see the
    // `run_on_with_state` call site for the same reasoning.
    let status = match catch_ffi_panic(|| unsafe {
        (plugin.describe_fn())(plugin.instance(), &mut manifest, &mut err)
    }) {
        Ok(status) => status,
        Err(msg) => {
            return Err(SaciError::configuration(format!(
                "plugin `{}` panicked in describe (host-caught, plugin bug): {msg}",
                path.display()
            )));
        }
    };

    if !status.is_ok() {
        drop(plugin.take_buffer(manifest));
        let message = plugin.take_buffer(err);
        return Err(SaciError::configuration(format!(
            "plugin `{}` failed describe: {}",
            path.display(),
            status_message(status, &message)
        )));
    }

    let json = plugin.take_buffer(manifest);
    // The ABI leaves `err` untouched on success. A plugin that wrote one anyway
    // must not leak it.
    drop(plugin.take_buffer(err));

    if json.is_empty() {
        return Err(SaciError::configuration(format!(
            "plugin `{}` returned an empty manifest from describe",
            path.display()
        )));
    }

    Ok(json)
}

/// The message a plugin attached to a status, or a stand-in naming the numeric
/// status when it attached none.
fn status_message(status: SaciStatus, message: &[u8]) -> String {
    if message.is_empty() {
        format!("plugin returned status {} with no message", status.as_i32())
    } else {
        String::from_utf8_lossy(message).into_owned()
    }
}

/// Map a non-OK `run_batch` status onto a [`SaciError`].
///
/// The variants line up with the WIT `run-error` arms the WASM runner maps, so
/// the same plugin failure reads the same way whichever runtime ran it.
fn run_batch_error(status: SaciStatus, message: &[u8]) -> SaciError {
    let detail = status_message(status, message);

    if status == SaciStatus::RETRYABLE {
        SaciError::SystemExecution(format!("plugin retryable: {detail}"))
    } else if status == SaciStatus::PERMANENT {
        SaciError::SystemExecution(format!("plugin permanent: {detail}"))
    } else if status == SaciStatus::SCHEMA_MISMATCH {
        // Schema mismatch is a load-time status. Seeing it here means the
        // plugin returned a status it promised never to return.
        SaciError::SystemExecution(format!(
            "plugin schema-mismatch in run_batch (plugin bug): {detail}"
        ))
    } else {
        SaciError::SystemExecution(format!(
            "plugin unknown status {}: {detail}",
            status.as_i32()
        ))
    }
}

/// Call `f`, converting an escaped panic into `Err(message)` instead of
/// letting it reach the caller.
///
/// `describe` and `run_batch` are `extern "C-unwind"` specifically so this is
/// possible: an unguarded plugin's panic reaches here as a catchable unwind
/// instead of aborting the process (see the `# Unwinding` section on
/// `saci_plugin_abi`). A plugin built with `export_plugin!` never exercises
/// the `Err` arm here, because its own internal guard already turned the
/// panic into a normal `PERMANENT` status before this call could see it. This
/// is the fallback for a plugin that does not guard itself.
fn catch_ffi_panic(f: impl FnOnce() -> SaciStatus) -> Result<SaciStatus, String> {
    catch_unwind(AssertUnwindSafe(f)).map_err(|payload| panic_payload_message(&*payload))
}

/// Stringify a caught panic payload for inclusion in an error message.
///
/// `panic!`, `.unwrap()` and `.expect()` all payload a `&str` or `String`.
fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_keeps_the_plugins_message() {
        let err = run_batch_error(SaciStatus::RETRYABLE, b"partition busy");
        assert!(matches!(err, SaciError::SystemExecution(_)));
        assert!(err.to_string().contains("plugin retryable"), "{err}");
        assert!(err.to_string().contains("partition busy"), "{err}");
    }

    #[test]
    fn permanent_keeps_the_plugins_message() {
        let err = run_batch_error(SaciStatus::PERMANENT, b"bad row shape");
        assert!(err.to_string().contains("plugin permanent"), "{err}");
        assert!(err.to_string().contains("bad row shape"), "{err}");
    }

    #[test]
    fn schema_mismatch_from_run_batch_names_it_a_plugin_bug() {
        let err = run_batch_error(SaciStatus::SCHEMA_MISMATCH, b"Counter drifted");
        let text = err.to_string();
        assert!(text.contains("schema-mismatch in run_batch"), "{text}");
        assert!(text.contains("plugin bug"), "{text}");
        assert!(text.contains("Counter drifted"), "{text}");
    }

    #[test]
    fn an_unrecognised_status_names_its_number() {
        let err = run_batch_error(SaciStatus(42), b"who knows");
        assert!(err.to_string().contains("unknown status 42"), "{err}");
    }

    #[test]
    fn an_empty_message_names_the_status_instead() {
        let err = run_batch_error(SaciStatus::PERMANENT, b"");
        let text = err.to_string();
        assert!(text.contains("status 2"), "{text}");
        assert!(text.contains("no message"), "{text}");
    }

    #[test]
    fn metrics_convert_field_for_field() {
        let metrics = PluginBatchMetrics::from(SaciRunMetrics {
            wall_ns: 7,
            rows_in: 3,
            rows_out: 3,
            systems_run: 1,
            retries: 0,
        });
        assert_eq!(
            metrics,
            PluginBatchMetrics {
                wall_ns: 7,
                rows_in: 3,
                rows_out: 3,
                systems_run: 1,
                retries: 0,
            }
        );
    }

    #[test]
    fn catch_ffi_panic_converts_a_string_payload() {
        let result = catch_ffi_panic(|| panic!("simulated plugin bug"));
        let msg = result.expect_err("a panicking closure must be caught, not propagated");
        assert!(msg.contains("simulated plugin bug"), "{msg}");
    }

    #[test]
    fn catch_ffi_panic_passes_a_normal_status_through_unchanged() {
        assert_eq!(catch_ffi_panic(|| SaciStatus::OK), Ok(SaciStatus::OK));
        assert_eq!(
            catch_ffi_panic(|| SaciStatus::RETRYABLE),
            Ok(SaciStatus::RETRYABLE)
        );
    }

    #[test]
    fn catch_ffi_panic_names_a_non_string_payload() {
        let result = catch_ffi_panic(|| std::panic::panic_any(42i32));
        let msg = result.expect_err("must be caught");
        assert_eq!(msg, "non-string panic payload");
    }
}
