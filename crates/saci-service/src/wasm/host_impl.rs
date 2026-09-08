use std::collections::HashMap;
use std::sync::Arc;

use super::bindings::{HostIo, LogLevel, TypesHost};
use wasmtime_wasi::{ResourceTable, WasiCtx, WasiCtxBuilder, WasiCtxView, WasiView};

/// Per-store processor state passed as the wasmtime `Store<T>` data.
///
/// Holds the config map injected at load time so the processor can call
/// `get-config` during `init` and `run-batch`. Config values are strings;
/// processors parse numerics themselves. The WASI context is here because
/// transitive deps (arrow-ipc, serde_arrow, std) require WASI imports.
pub struct HostState {
    /// Prefixes log output.
    pub name: String,
    /// Key/value config from the `config` node inside `wasm`. Shared
    /// with the owning runtime, so a processor call costs one `Arc` bump and no map
    /// copy.
    pub config: Arc<HashMap<String, String>>,
    /// This node's declared id.
    ///
    /// Carried here so a `host-io::metric` call can be attributed to the node
    /// that made it: the import gives the host a name and a value and nothing
    /// that identifies the caller.
    pub processor_id: String,
    pub wasi_ctx: WasiCtx,
    pub resource_table: ResourceTable,
}

impl HostState {
    pub fn new(
        name: impl Into<String>,
        config: Arc<HashMap<String, String>>,
        processor_id: String,
    ) -> Self {
        Self {
            name: name.into(),
            config,
            processor_id,
            wasi_ctx: WasiCtxBuilder::new().build(),
            resource_table: ResourceTable::new(),
        }
    }
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi_ctx,
            table: &mut self.resource_table,
        }
    }
}

// The `types` WIT interface generates an empty Host marker trait.
impl TypesHost for HostState {}

impl HostIo for HostState {
    fn log(&mut self, level: LogLevel, target: String, message: String) {
        // Routes to the host tracing subscriber when the `tracing` feature is
        // on; the gate keeps the dep out of plain `wasm` builds.
        #[cfg(feature = "tracing")]
        {
            use tracing::{debug, error, info, trace, warn};
            match level {
                LogLevel::Trace => trace!(pipeline = %self.name, target = %target, "{}", message),
                LogLevel::Debug => debug!(pipeline = %self.name, target = %target, "{}", message),
                LogLevel::Info => info!(pipeline = %self.name, target = %target, "{}", message),
                LogLevel::Warn => warn!(pipeline = %self.name, target = %target, "{}", message),
                LogLevel::Error => error!(pipeline = %self.name, target = %target, "{}", message),
            }
        }
        #[cfg(not(feature = "tracing"))]
        {
            let level_str = match level {
                LogLevel::Trace => "TRACE",
                LogLevel::Debug => "DEBUG",
                LogLevel::Info => "INFO",
                LogLevel::Warn => "WARN",
                LogLevel::Error => "ERROR",
            };
            eprintln!("[{}] [{}] {}: {}", level_str, self.name, target, message);
        }
    }

    fn metric(&mut self, name: String, value: f64) {
        crate::metrics::instruments().processor_metric(&self.processor_id, &name, value);
        #[cfg(feature = "tracing")]
        tracing::trace!(pipeline = %self.name, metric = %name, value = value, "processor metric");
        #[cfg(not(feature = "tracing"))]
        let _ = (name, value);
    }

    fn get_config(&mut self, key: String) -> Option<String> {
        self.config.get(&key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_host(config: &[(&str, &str)]) -> HostState {
        HostState::new(
            "test-pipeline",
            Arc::new(
                config
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            "test-pipeline".to_string(),
        )
    }

    #[test]
    fn get_config_returns_value() {
        let mut host = make_host(&[("batch_size", "512")]);
        assert_eq!(host.get_config("batch_size".into()), Some("512".into()));
    }

    #[test]
    fn get_config_returns_none_for_missing() {
        let mut host = make_host(&[]);
        assert_eq!(host.get_config("missing".into()), None);
    }

    #[test]
    fn log_does_not_panic() {
        let mut host = make_host(&[]);
        host.log(LogLevel::Info, "test".into(), "hello".into());
        host.log(LogLevel::Error, "test".into(), "boom".into());
    }

    /// `host-io::metric` records the value, and names past the cap are dropped.
    ///
    /// Both assertions live in one test because the name cap is per-process
    /// state: a second test filling it would see whatever this one left behind.
    #[cfg(feature = "metrics")]
    #[test]
    fn metric_records_processor_series() {
        use prometheus::TextEncoder;

        let registry = crate::metrics::test_registry();
        let mut host = make_host(&[]);
        host.metric("rows_flagged".into(), 7.0);

        let text = TextEncoder::new()
            .encode_to_string(&registry.gather())
            .expect("encode prometheus text");
        assert!(
            text.contains("saci_processor_metric"),
            "the histogram should exist:\n{text}"
        );
        assert!(
            text.contains(r#"metric="rows_flagged""#),
            "the metric name should become an attribute:\n{text}"
        );

        // Enough distinct names to take the total past the cap, so the number of
        // live label values must settle at MAX_PROCESSOR_METRIC_NAMES.
        let over_cap = crate::metrics::MAX_PROCESSOR_METRIC_NAMES + 64;
        for i in 0..over_cap {
            host.metric(format!("generated_{i}"), i as f64);
        }

        let text = TextEncoder::new()
            .encode_to_string(&registry.gather())
            .expect("encode prometheus text");
        // Attribution is additive: each admitted name writes both the
        // unattributed series and one under `processor="<id>"`, so only the
        // unattributed form maps one to one with a distinct name.
        let distinct = text
            .lines()
            .filter(|l| l.starts_with("saci_processor_metric_count{") && !l.contains("processor="))
            .count();
        assert_eq!(
            distinct,
            crate::metrics::MAX_PROCESSOR_METRIC_NAMES,
            "attribute cardinality must stop at the cap:\n{text}"
        );
    }

    /// Without the `metrics` feature the call is a no-op, and must still not
    /// panic.
    #[cfg(not(feature = "metrics"))]
    #[test]
    fn metric_does_not_panic() {
        let mut host = make_host(&[]);
        host.metric("rows_processed".into(), 1024.0);
    }
}
