//! [`TransformerFactory`] and [`TransformerRegistry`]: name to format lookup.

use std::collections::HashMap;
use std::sync::Arc;

use saci_config::ConfigValue;
use saci_core::error::SaciError;

use crate::transformer::Transformer;

/// Factory for one named byte format.
pub trait TransformerFactory: Send + Sync + 'static {
    /// The name that appears in config as `format="<name>"`.
    fn format_name(&self) -> &'static str;

    /// Build a transformer from its `options` table.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when an option is of the wrong type
    /// or holds a value the format cannot honour.
    fn build(&self, options: &ConfigValue) -> Result<Arc<dyn Transformer>, SaciError>;
}

/// Name to factory map, mirroring the host's source and sink registry. It
/// lives below `saci-connector` because connectors resolve formats at build
/// time.
///
/// Lookup returns `Option`, never an error: the wording for a `format` key
/// naming nothing lives in the connector context that reads the key, exactly as
/// `Registry::source` leaves its wording to `ServiceBuilder::build_source_node`.
#[derive(Default)]
pub struct TransformerRegistry {
    factories: HashMap<String, Box<dyn TransformerFactory>>,
}

impl TransformerRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a transformer factory.
    ///
    /// Registering a `format_name` that is already taken replaces it.
    pub fn register<F: TransformerFactory>(&mut self, factory: F) -> &mut Self {
        self.factories
            .insert(factory.format_name().to_string(), Box::new(factory));
        self
    }

    /// Look up a factory by format name.
    pub fn get(&self, format: &str) -> Option<&dyn TransformerFactory> {
        self.factories.get(format).map(|f| f.as_ref())
    }

    /// The number of registered formats.
    pub fn count(&self) -> usize {
        self.factories.len()
    }

    /// Registered names, sorted. Used to spell out a bad `format` key.
    pub fn formats(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.factories.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transformer::Transformer;

    struct Named(&'static str);

    struct NamedFactory(&'static str);

    impl Transformer for Named {
        fn format(&self) -> &'static str {
            self.0
        }
    }

    impl TransformerFactory for NamedFactory {
        fn format_name(&self) -> &'static str {
            self.0
        }
        fn build(&self, _options: &ConfigValue) -> Result<Arc<dyn Transformer>, SaciError> {
            Ok(Arc::new(Named(self.0)))
        }
    }

    #[test]
    fn a_registered_factory_is_found_by_its_format_name() {
        let mut registry = TransformerRegistry::new();
        registry.register(NamedFactory("csv"));
        assert_eq!(registry.count(), 1);
        assert!(registry.get("ndjson").is_none());

        let built = registry
            .get("csv")
            .expect("csv is registered")
            .build(&ConfigValue::Object(saci_config::ConfigMap::new()))
            .expect("build");
        assert_eq!(built.format(), "csv");
    }

    #[test]
    fn registering_one_name_twice_keeps_a_single_entry() {
        let mut registry = TransformerRegistry::new();
        registry.register(NamedFactory("csv"));
        registry.register(NamedFactory("csv"));
        assert_eq!(registry.count(), 1);
    }

    #[test]
    fn formats_are_reported_sorted() {
        let mut registry = TransformerRegistry::new();
        registry.register(NamedFactory("parquet"));
        registry.register(NamedFactory("arrow-ipc"));
        registry.register(NamedFactory("ndjson"));
        assert_eq!(registry.formats(), vec!["arrow-ipc", "ndjson", "parquet"]);
    }

    #[test]
    fn an_empty_registry_reports_no_formats() {
        let registry = TransformerRegistry::default();
        assert_eq!(registry.count(), 0);
        assert!(registry.formats().is_empty());
    }
}
