//! Typed reads of the host-injected config.

use std::cell::RefCell;
use std::collections::HashMap;

use super::error::{Error, Result};
use super::host;

/// Typed access to the `config` values the host injected through the
/// processor's or plugin's node of the service config.
///
/// A two-parameter `#[transform]` receives one, built once per batch. Each
/// key is fetched from the host once for the life of the value; later reads
/// of the same key allocate nothing.
///
/// One method, [`get`](Self::get): absent key yields the default, present but
/// unparseable is an error. Silently falling back to the default on a malformed
/// value would hide an operator's typo behind working-looking behaviour.
///
/// ```ignore
/// #[transform]
/// pub fn settle(row: &mut Order, config: &sdk::Config) -> sdk::Result<()> {
///     let floor: f64 = config.get("min_amount", 0.0)?;
///     row.valid = row.amount > floor;
///     Ok(())
/// }
/// ```
#[derive(Default)]
pub struct Config {
    cache: RefCell<HashMap<String, Option<String>>>,
}

impl Config {
    /// A config reader over the active host. See
    /// [`sdk::host`](super::host).
    pub fn new() -> Self {
        Self::default()
    }

    /// Read `key` and parse it as `T`, falling back to `default` when the host
    /// injected no such key.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is present but its value does not parse
    /// as `T`.
    pub fn get<T: std::str::FromStr>(&self, key: &str, default: T) -> Result<T> {
        if let Some(raw) = self.cache.borrow().get(key) {
            return parse(key, raw.as_deref(), default);
        }
        let raw = host::config(key);
        let parsed = parse(key, raw.as_deref(), default);
        self.cache.borrow_mut().insert(key.to_string(), raw);
        parsed
    }
}

fn parse<T: std::str::FromStr>(key: &str, raw: Option<&str>, default: T) -> Result<T> {
    match raw {
        None => Ok(default),
        // `T::Err` carries no `Display` bound, so the message names the
        // target type and the offending value instead of the parse error.
        Some(raw) => raw.parse::<T>().map_err(|_| {
            Error::new(format!(
                "config key '{key}': value '{raw}' does not parse as {}",
                std::any::type_name::<T>()
            ))
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::super::glue::{HostIo, HostScope};
    use super::super::host::{Level, with_config};
    use super::*;

    #[test]
    fn a_present_key_is_parsed() {
        assert_eq!(
            with_config(&[("k", "7")], || Config::new().get("k", 0)),
            Ok(7)
        );
    }

    #[test]
    fn an_absent_key_yields_the_default() {
        assert_eq!(with_config(&[], || Config::new().get("k", 3.0)), Ok(3.0));
    }

    #[test]
    fn a_present_but_unparseable_key_is_an_error_not_the_default() {
        let err = with_config(&[("mangled", "x")], || {
            Config::new().get::<i64>("mangled", 0)
        })
        .unwrap_err();
        assert!(err.message().contains("mangled"), "{}", err.message());
        assert!(err.message().contains("'x'"), "{}", err.message());
    }

    /// Counts the host lookups a `Config` makes.
    struct CountingHost {
        lookups: Cell<usize>,
    }

    impl HostIo for CountingHost {
        fn config(&self, key: &str) -> Option<String> {
            self.lookups.set(self.lookups.get() + 1);
            (key == "k").then(|| "5".to_string())
        }
        fn log(&self, _: Level, _: &str, _: &str) {}
        fn metric(&self, _: &str, _: f64) {}
    }

    #[test]
    fn a_key_is_fetched_from_the_host_once() {
        let host = CountingHost {
            lookups: Cell::new(0),
        };
        let _scope = HostScope::enter(&host);
        let config = Config::new();
        assert_eq!(config.get("k", 0), Ok(5));
        assert_eq!(config.get("k", 0), Ok(5));
        assert_eq!(config.get("absent", 1), Ok(1));
        assert_eq!(config.get("absent", 1), Ok(1));
        assert_eq!(host.lookups.get(), 2, "one lookup per distinct key");
    }
}
