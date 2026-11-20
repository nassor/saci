//! [`CodecFactory`] and [`CodecRegistry`]: name to format lookup.

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::ConfigValue;
use crate::error::SaciError;

use crate::codec::contract::Codec;

/// Factory for one named byte format.
pub trait CodecFactory: Send + Sync + 'static {
    /// The name that appears in config as `format="<name>"`.
    fn format_name(&self) -> &'static str;

    /// Build a codec from its `options` table.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when an option is of the wrong type,
    /// holds a value the format cannot honour, or is a key the format does not
    /// read.
    fn build(&self, options: &ConfigValue) -> Result<Arc<dyn Codec>, SaciError>;
}

/// Refuse an `options` key a format does not read.
///
/// A misspelled key would otherwise be dropped and the codec built with its
/// default. `known` lists the keys `format` reads; every other key is a
/// [`SaciError::Configuration`] naming it, and an options value that is not a
/// table has no keys to refuse.
#[cfg(any(
    feature = "codec-arrow-ipc",
    feature = "codec-avro",
    feature = "codec-csv",
    feature = "codec-ndjson",
    feature = "codec-parquet",
    feature = "service"
))]
pub(crate) fn reject_unknown_options(
    format: &str,
    options: &ConfigValue,
    known: &[&str],
) -> Result<(), SaciError> {
    let Some(table) = options.as_object() else {
        return Ok(());
    };
    match table.keys().find(|key| !known.contains(&key.as_str())) {
        Some(key) => Err(SaciError::configuration(format!(
            "{format}: unknown option '{key}'"
        ))),
        None => Ok(()),
    }
}

/// Name to factory map, mirroring the host's source and sink registry. It
/// lives below `crate::connector` because connectors resolve formats at build
/// time.
///
/// Lookup returns `Option`, never an error: the wording for a `format` key
/// naming nothing lives in the connector context that reads the key, exactly as
/// `Registry::source` leaves its wording to `ServiceBuilder::build_source_node`.
#[derive(Default)]
pub struct CodecRegistry {
    factories: HashMap<String, Box<dyn CodecFactory>>,
}

impl CodecRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a codec factory.
    ///
    /// Registering a `format_name` that is already taken replaces it.
    pub fn register<F: CodecFactory>(&mut self, factory: F) -> &mut Self {
        self.factories
            .insert(factory.format_name().to_string(), Box::new(factory));
        self
    }

    /// Look up a factory by format name.
    pub fn get(&self, format: &str) -> Option<&dyn CodecFactory> {
        self.factories.get(format).map(|f| f.as_ref())
    }

    /// The number of registered formats.
    pub fn len(&self) -> usize {
        self.factories.len()
    }

    /// True when no format is registered.
    pub fn is_empty(&self) -> bool {
        self.factories.is_empty()
    }

    /// Registered names, sorted. Used to spell out a bad `format` key.
    pub fn names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.factories.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::contract::Codec;

    struct Named(&'static str);

    struct NamedFactory(&'static str);

    impl Codec for Named {
        fn format(&self) -> &'static str {
            self.0
        }
    }

    impl CodecFactory for NamedFactory {
        fn format_name(&self) -> &'static str {
            self.0
        }
        fn build(&self, _options: &ConfigValue) -> Result<Arc<dyn Codec>, SaciError> {
            Ok(Arc::new(Named(self.0)))
        }
    }

    #[test]
    fn a_registered_factory_is_found_by_its_format_name() {
        let mut registry = CodecRegistry::new();
        registry.register(NamedFactory("csv"));
        assert_eq!(registry.len(), 1);
        assert!(registry.get("ndjson").is_none());

        let built = registry
            .get("csv")
            .expect("csv is registered")
            .build(&ConfigValue::Object(crate::config::ConfigMap::new()))
            .expect("build");
        assert_eq!(built.format(), "csv");
    }

    #[test]
    fn registering_one_name_twice_keeps_a_single_entry() {
        let mut registry = CodecRegistry::new();
        registry.register(NamedFactory("csv"));
        registry.register(NamedFactory("csv"));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn names_are_reported_sorted() {
        let mut registry = CodecRegistry::new();
        registry.register(NamedFactory("parquet"));
        registry.register(NamedFactory("arrow-ipc"));
        registry.register(NamedFactory("ndjson"));
        assert_eq!(registry.names(), vec!["arrow-ipc", "ndjson", "parquet"]);
    }

    #[test]
    fn an_empty_registry_reports_no_formats() {
        let registry = CodecRegistry::default();
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());
        assert!(registry.names().is_empty());
    }
}
