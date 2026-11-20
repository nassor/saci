//! Factory registry for the SACI service layer.
//!
//! The registry maps string type names (from the config file) to factory
//! objects that construct concrete [`Source`](crate::io::source::Source)
//! and [`Sink`](crate::io::sink::Sink) instances, and format names to the
//! [`CodecFactory`] that decodes the bytes those connectors move.
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
//! use saci::service::registry::Registry;
//!
//! let mut registry = Registry::new();
//! // register_source / register_sink / register_codec go here
//! assert_eq!(registry.source_count(), 0);
//! assert_eq!(registry.codec_count(), 0);
//! # }
//! ```

use std::collections::HashMap;

pub use crate::codec::{Codec, CodecFactory, CodecRegistry};
pub use crate::connector::{SinkFactory, SourceFactory};

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
/// use saci::service::registry::Registry;
///
/// let registry = Registry::new();
/// assert_eq!(registry.source_count(), 0);
/// assert_eq!(registry.sink_count(), 0);
/// assert_eq!(registry.codec_count(), 0);
/// # }
/// ```
#[derive(Default)]
pub struct Registry {
    sources: HashMap<String, Box<dyn SourceFactory>>,
    sinks: HashMap<String, Box<dyn SinkFactory>>,
    codecs: CodecRegistry,
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

    /// Register a codec factory.
    ///
    /// If a factory with the same `format_name` is already registered, it is
    /// silently replaced.
    pub fn register_codec<F: CodecFactory>(&mut self, factory: F) -> &mut Self {
        self.codecs.register(factory);
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
    /// The counterpart of [`CodecRegistry::names`], and what lets a
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

    /// The codec registry a [`ConnectorContext`] is built from.
    ///
    /// [`ConnectorContext`]: crate::connector::ConnectorContext
    pub fn codecs(&self) -> &CodecRegistry {
        &self.codecs
    }

    /// Returns the number of registered source factories.
    pub fn source_count(&self) -> usize {
        self.sources.len()
    }

    /// Returns the number of registered sink factories.
    pub fn sink_count(&self) -> usize {
        self.sinks.len()
    }

    /// Returns the number of registered codec factories.
    pub fn codec_count(&self) -> usize {
        self.codecs.len()
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::connector::ConfigValue;
    #[cfg(feature = "connector-channel")]
    use crate::connector::{ConfigMap, ConnectorContext};
    use crate::error::SaciError;
    #[cfg(feature = "connector-channel")]
    use crate::io::{sink::Sink, source::Source};
    #[cfg(feature = "connector-channel")]
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[cfg(feature = "connector-channel")]
    struct TestSourceFactory;
    #[cfg(feature = "connector-channel")]
    impl SourceFactory for TestSourceFactory {
        fn type_name(&self) -> &'static str {
            "TestSource"
        }
        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Source>, SaciError> {
            use crate::connector::channel::ChannelSource;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (_tx, src) = ChannelSource::new(schema, 1);
            Ok(Box::new(src))
        }
    }

    struct TestCodec;
    impl Codec for TestCodec {
        fn format(&self) -> &'static str {
            "test-format"
        }
    }

    struct TestCodecFactory;
    impl CodecFactory for TestCodecFactory {
        fn format_name(&self) -> &'static str {
            "test-format"
        }
        fn build(&self, _options: &ConfigValue) -> Result<Arc<dyn Codec>, SaciError> {
            Ok(Arc::new(TestCodec))
        }
    }

    #[cfg(feature = "connector-channel")]
    struct TestSinkFactory;
    #[cfg(feature = "connector-channel")]
    impl SinkFactory for TestSinkFactory {
        fn type_name(&self) -> &'static str {
            "TestSink"
        }
        fn build(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> Result<Box<dyn Sink>, SaciError> {
            use crate::connector::channel::ChannelSink;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (sink, _rx) = ChannelSink::new(schema, 1);
            Ok(Box::new(sink))
        }
    }

    #[cfg(feature = "connector-channel")]
    #[test]
    fn test_register_and_lookup_source_factory() {
        let mut reg = Registry::new();
        reg.register_source(TestSourceFactory);
        assert!(reg.source("TestSource").is_some());
        assert!(reg.source("Missing").is_none());
    }

    #[cfg(feature = "connector-channel")]
    #[test]
    fn test_register_and_lookup_sink_factory() {
        let mut reg = Registry::new();
        reg.register_sink(TestSinkFactory);
        assert!(reg.sink("TestSink").is_some());
        assert!(reg.sink("Missing").is_none());
    }

    #[test]
    fn test_register_and_lookup_codec_factory() {
        let mut reg = Registry::new();
        reg.register_codec(TestCodecFactory);
        assert!(reg.codecs().get("test-format").is_some());
        assert!(reg.codecs().get("missing").is_none());
    }

    #[cfg(feature = "connector-channel")]
    #[test]
    fn test_registry_counts() {
        let mut reg = Registry::new();
        assert_eq!(reg.source_count(), 0);
        assert_eq!(reg.sink_count(), 0);
        assert_eq!(reg.codec_count(), 0);
        reg.register_source(TestSourceFactory);
        assert_eq!(reg.source_count(), 1);
        reg.register_sink(TestSinkFactory);
        assert_eq!(reg.sink_count(), 1);
        reg.register_codec(TestCodecFactory);
        assert_eq!(reg.codec_count(), 1);
    }

    #[cfg(feature = "connector-channel")]
    #[test]
    fn test_duplicate_registration_replaces() {
        let mut reg = Registry::new();
        reg.register_source(TestSourceFactory);
        reg.register_source(TestSourceFactory);
        assert_eq!(reg.source_count(), 1);
    }

    #[cfg(feature = "connector-channel")]
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

    #[cfg(feature = "connector-channel")]
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
        assert_eq!(reg.codec_count(), 0);
    }
}
