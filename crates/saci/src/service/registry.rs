//! Connector and codec registry for the SACI service layer.
//!
//! The registry maps connector names (the `connector` property of a config's
//! `source` and `sink` nodes) to the [`Connector`] objects that construct
//! concrete [`Source`](crate::io::Source) and [`Sink`](crate::io::Sink)
//! instances, and format names to the [`CodecFactory`] that decodes the bytes
//! those connectors move.
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
//! use saci::service::Registry;
//!
//! let registry = Registry::new();
//! // `Registry::register` and `Registry::register_codec` go here
//! assert_eq!(registry.len(), 0);
//! assert_eq!(registry.codec_count(), 0);
//! # }
//! ```

use std::collections::HashMap;

pub use crate::codec::{Codec, CodecFactory, CodecRegistry};
pub use crate::connector::{Connector, Rebuild, Role};

/// Central registry mapping connector names to their [`Connector`]s and
/// format names to their codec factories.
///
/// Connectors are registered at startup, before any config is loaded, and
/// looked up by the `connector` name from the config. The runtime is
/// supplied separately through [`ServiceBuilder::runtime`](super::ServiceBuilder::runtime) or loaded from
/// a WASM module.
///
/// ## Example
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use saci::service::Registry;
///
/// let registry = Registry::new();
/// assert_eq!(registry.len(), 0);
/// assert_eq!(registry.codec_count(), 0);
/// # }
/// ```
#[derive(Default)]
pub struct Registry {
    connectors: HashMap<&'static str, Box<dyn Connector>>,
    codecs: CodecRegistry,
}

impl Registry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a connector.
    ///
    /// If a connector with the same [`name`](Connector::name) is already
    /// registered, it is silently replaced.
    pub fn register<C: Connector>(&mut self, connector: C) -> &mut Self {
        self.connectors
            .insert(connector.name(), Box::new(connector));
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

    /// Look up a connector by name.
    pub fn connector(&self, name: &str) -> Option<&dyn Connector> {
        self.connectors.get(name).map(|c| c.as_ref())
    }

    /// Registered connector names, sorted.
    ///
    /// The counterpart of [`CodecRegistry::names`], and what lets a
    /// test compare the built-in table against what actually registered.
    pub fn names(&self) -> Vec<&'static str> {
        let mut names: Vec<&'static str> = self.connectors.keys().copied().collect();
        names.sort_unstable();
        names
    }

    /// The codec registry a [`ConnectorContext`] is built from.
    ///
    /// [`ConnectorContext`]: crate::connector::ConnectorContext
    pub fn codecs(&self) -> &CodecRegistry {
        &self.codecs
    }

    /// Returns the number of registered connectors.
    pub fn len(&self) -> usize {
        self.connectors.len()
    }

    /// True when no connector is registered.
    pub fn is_empty(&self) -> bool {
        self.connectors.is_empty()
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
    use crate::error::SaciResult;
    #[cfg(feature = "connector-channel")]
    use crate::io::{Sink, Source};
    #[cfg(feature = "connector-channel")]
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[cfg(feature = "connector-channel")]
    struct TestConnector;
    #[cfg(feature = "connector-channel")]
    impl Connector for TestConnector {
        fn name(&self) -> &'static str {
            "test"
        }
        fn source(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> SaciResult<Box<dyn Source>> {
            use crate::connector::channel::ChannelSource;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (src, _tx) = ChannelSource::new(schema, 1);
            Ok(Box::new(src))
        }
        fn sink(
            &self,
            _config: &ConfigValue,
            _ctx: &ConnectorContext,
        ) -> SaciResult<Box<dyn Sink>> {
            use crate::connector::channel::ChannelSink;
            let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let (sink, _rx) = ChannelSink::new(schema, 1);
            Ok(Box::new(sink))
        }
    }

    /// Serves only sinks: the source half is the trait's default.
    #[cfg(feature = "connector-channel")]
    struct SinkOnlyConnector;
    #[cfg(feature = "connector-channel")]
    impl Connector for SinkOnlyConnector {
        fn name(&self) -> &'static str {
            "sink-only"
        }
        fn roles(&self) -> &'static [Role] {
            &[Role::Sink]
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
    #[test]
    fn test_register_and_lookup_connector() {
        let mut reg = Registry::new();
        reg.register(TestConnector);
        assert!(reg.connector("test").is_some());
        assert!(reg.connector("Missing").is_none());
        assert_eq!(reg.names(), vec!["test"]);
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
        assert_eq!(reg.len(), 0);
        assert!(reg.is_empty());
        assert_eq!(reg.codec_count(), 0);
        reg.register(TestConnector);
        assert_eq!(reg.len(), 1);
        reg.register_codec(TestCodecFactory);
        assert_eq!(reg.codec_count(), 1);
    }

    #[cfg(feature = "connector-channel")]
    #[test]
    fn test_duplicate_registration_replaces() {
        let mut reg = Registry::new();
        reg.register(TestConnector);
        reg.register(TestConnector);
        assert_eq!(reg.len(), 1);
    }

    #[cfg(feature = "connector-channel")]
    #[test]
    fn test_connector_builds_source() {
        let src = TestConnector
            .source(
                &ConfigValue::Object(ConfigMap::new()),
                &ConnectorContext::new(None),
            )
            .unwrap();
        assert_eq!(src.schema().fields().len(), 1);
    }

    #[cfg(feature = "connector-channel")]
    #[test]
    fn test_connector_builds_sink() {
        let sink = TestConnector
            .sink(
                &ConfigValue::Object(ConfigMap::new()),
                &ConnectorContext::new(None),
            )
            .unwrap();
        assert_eq!(sink.schema().fields().len(), 1);
    }

    #[cfg(feature = "connector-channel")]
    #[test]
    fn test_a_missing_role_is_a_configuration_error() {
        let err = SinkOnlyConnector
            .source(
                &ConfigValue::Object(ConfigMap::new()),
                &ConnectorContext::new(None),
            )
            .err()
            .expect("a sink-only connector has no source");
        assert!(
            err.to_string()
                .contains("connector 'sink-only' has no source"),
            "{err}"
        );
    }

    #[test]
    fn test_default_registry_is_empty() {
        let reg = Registry::default();
        assert_eq!(reg.len(), 0);
        assert_eq!(reg.codec_count(), 0);
    }
}
