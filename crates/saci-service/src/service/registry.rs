//! Factory registry for the SACI service layer.
//!
//! The registry maps string type names (from the config file) to factory
//! objects that construct concrete [`Source`](saci_core::io::source::Source)
//! and [`Sink`](saci_core::io::sink::Sink) instances, and format names to the
//! [`TransformerFactory`] that decodes the bytes those connectors move.
//!
//! The runtime itself is supplied directly as a `Box<dyn PipelineRuntime>` or
//! loaded from a WASM module via
//! [`PipelineRuntimeLoader`](super::loader::PipelineRuntimeLoader).
//!
//! ## Usage
//!
//! ```rust
//! # #[cfg(feature = "service")]
//! # {
//! use saci_service::service::registry::Registry;
//!
//! let mut registry = Registry::new();
//! // register_source / register_sink / register_transformer go here
//! assert_eq!(registry.source_count(), 0);
//! assert_eq!(registry.transformer_count(), 0);
//! # }
//! ```

use std::collections::HashMap;

pub use saci_connector::{SinkFactory, SourceFactory};
pub use saci_transformer::{Transformer, TransformerFactory, TransformerRegistry};

/// Central registry mapping type names to their IO factories.
///
/// Factories are registered at startup, before any config is loaded, and
/// looked up by the `type_name` string from the config. The runtime is
/// supplied separately through [`ServiceBuilder::with_runtime`](super::ServiceBuilder::with_runtime) or loaded from
/// a WASM module.
///
/// ## Example
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use saci_service::service::registry::Registry;
///
/// let registry = Registry::new();
/// assert_eq!(registry.source_count(), 0);
/// assert_eq!(registry.sink_count(), 0);
/// assert_eq!(registry.transformer_count(), 0);
/// # }
/// ```
#[derive(Default)]
pub struct Registry {
    sources: HashMap<String, Box<dyn SourceFactory>>,
    sinks: HashMap<String, Box<dyn SinkFactory>>,
    transformers: TransformerRegistry,
}

impl Registry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a source factory.
    ///
    /// If a factory with the same `type_name` is already registered, it is
    /// silently replaced.
    pub fn register_source<F: SourceFactory>(&mut self, factory: F) -> &mut Self {
        self.sources
            .insert(factory.type_name().to_string(), Box::new(factory));
        self
    }

    /// Register a sink factory.
    ///
    /// If a factory with the same `type_name` is already registered, it is
    /// silently replaced.
    pub fn register_sink<F: SinkFactory>(&mut self, factory: F) -> &mut Self {
        self.sinks
            .insert(factory.type_name().to_string(), Box::new(factory));
        self
    }

    /// Register a transformer factory.
    ///
    /// If a factory with the same `format_name` is already registered, it is
    /// silently replaced.
    pub fn register_transformer<F: TransformerFactory>(&mut self, factory: F) -> &mut Self {
        self.transformers.register(factory);
        self
    }

    /// Look up a source factory by type name.
    pub fn source(&self, type_name: &str) -> Option<&dyn SourceFactory> {
        self.sources.get(type_name).map(|f| f.as_ref())
    }

    /// Look up a sink factory by type name.
    pub fn sink(&self, type_name: &str) -> Option<&dyn SinkFactory> {
        self.sinks.get(type_name).map(|f| f.as_ref())
    }

    /// Registered source type names, sorted.
    ///
    /// The counterpart of [`TransformerRegistry::formats`], and what lets a
    /// test compare the built-in table against what actually registered.
    pub fn source_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.sources.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }

    /// Registered sink type names, sorted.
    pub fn sink_names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.sinks.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }

    /// The transformer registry a [`ConnectorContext`] is built from.
    ///
    /// [`ConnectorContext`]: saci_connector::ConnectorContext
    pub fn transformers(&self) -> &TransformerRegistry {
        &self.transformers
    }

    /// Returns the number of registered source factories.
    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    /// Returns the number of registered sink factories.
    pub fn sink_count(&self) -> usize {
        self.sinks.len()
    }

    /// Returns the number of registered transformer factories.
    pub fn transformer_count(&self) -> usize {
        self.transformers.count()
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use saci_connector::{ConfigMap, ConfigValue, ConnectorContext};
    use saci_core::error::SaciError;
    use saci_core::io::sink::Sink;
    use saci_core::io::source::Source;
    use std::sync::Arc;

    struct TestSourceFactory;
    impl SourceFactory for TestSourceFactory {
        fn type_name(&self) -> &'static str {
            "TestSource"
        }
        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Source>, SaciError> {
            use saci_connector_channel::ChannelSource;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (_tx, src) = ChannelSource::new(schema, 1);
            Ok(Box::new(src))
        }
    }

    struct TestTransformer;
    impl Transformer for TestTransformer {
        fn format(&self) -> &'static str {
            "test-format"
        }
    }

    struct TestTransformerFactory;
    impl TransformerFactory for TestTransformerFactory {
        fn format_name(&self) -> &'static str {
            "test-format"
        }
        fn build(&self, _options: &ConfigValue) -> Result<Arc<dyn Transformer>, SaciError> {
            Ok(Arc::new(TestTransformer))
        }
    }

    struct TestSinkFactory;
    impl SinkFactory for TestSinkFactory {
        fn type_name(&self) -> &'static str {
            "TestSink"
        }
        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Sink>, SaciError> {
            use saci_connector_channel::ChannelSink;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (sink, _rx) = ChannelSink::new(schema, 1);
            Ok(Box::new(sink))
        }
    }

    #[test]
    fn test_register_and_lookup_source_factory() {
        let mut reg = Registry::new();
        reg.register_source(TestSourceFactory);
        assert!(reg.source("TestSource").is_some());
        assert!(reg.source("Missing").is_none());
    }

    #[test]
    fn test_register_and_lookup_sink_factory() {
        let mut reg = Registry::new();
        reg.register_sink(TestSinkFactory);
        assert!(reg.sink("TestSink").is_some());
        assert!(reg.sink("Missing").is_none());
    }

    #[test]
    fn test_register_and_lookup_transformer_factory() {
        let mut reg = Registry::new();
        reg.register_transformer(TestTransformerFactory);
        assert!(reg.transformers().get("test-format").is_some());
        assert!(reg.transformers().get("missing").is_none());
    }

    #[test]
    fn test_registry_counts() {
        let mut reg = Registry::new();
        assert_eq!(reg.source_count(), 0);
        assert_eq!(reg.sink_count(), 0);
        assert_eq!(reg.transformer_count(), 0);
        reg.register_source(TestSourceFactory);
        assert_eq!(reg.source_count(), 1);
        reg.register_sink(TestSinkFactory);
        assert_eq!(reg.sink_count(), 1);
        reg.register_transformer(TestTransformerFactory);
        assert_eq!(reg.transformer_count(), 1);
    }

    #[test]
    fn test_duplicate_registration_replaces() {
        let mut reg = Registry::new();
        reg.register_source(TestSourceFactory);
        reg.register_source(TestSourceFactory);
        assert_eq!(reg.source_count(), 1);
    }

    #[test]
    fn test_source_factory_builds_source() {
        let src = TestSourceFactory
            .build(
                &ConfigValue::Object(ConfigMap::new()),
                &ConnectorContext::new(None),
            )
            .unwrap();
        assert_eq!(src.schema().fields().len(), 1);
    }

    #[test]
    fn test_sink_factory_builds_sink() {
        let sink = TestSinkFactory
            .build(
                &ConfigValue::Object(ConfigMap::new()),
                &ConnectorContext::new(None),
            )
            .unwrap();
        assert_eq!(sink.schema().fields().len(), 1);
    }

    #[test]
    fn test_default_registry_is_empty() {
        let reg = Registry::default();
        assert_eq!(reg.source_count(), 0);
        assert_eq!(reg.sink_count(), 0);
        assert_eq!(reg.transformer_count(), 0);
    }
}
