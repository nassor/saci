//! Typed reads of the host-injected processor config.

use crate::{Error, Result};

/// Typed access to the `config` values the host injected via the `wasm` node
/// of the service config.
///
/// A two-parameter `#[transform]` receives one of these. It wraps the
/// `saci_config_get` function `#[processor]` emits into the processor crate,
/// which is where it has to live: the `saci:pipeline/host-io` `get-config`
/// import is only reachable from the crate that expanded
/// `wit_bindgen::generate!`.
///
/// One method, [`get`](Self::get): absent key yields the default, present but
/// unparseable is an error. Silently falling back to the default on a malformed
/// value would hide an operator's typo behind working-looking behaviour.
///
/// ```ignore
/// #[transform(component = Order)]
/// pub fn settle(row: &mut Order, config: &saci_processor::Config) -> saci_processor::Result<()> {
///     let floor: f64 = config.get("min_amount", 0.0)?;
///     row.valid = row.amount > floor;
///     Ok(())
/// }
/// ```
#[derive(Clone, Copy)]
pub struct Config {
    get_raw: fn(&str) -> Option<String>,
}

impl Config {
    /// Wrap a raw config getter.
    ///
    /// Called by the `#[transform]` expansion with the `saci_config_get` that
    /// `#[processor]` emitted. There is no reason to call it by hand outside a
    /// test double.
    pub const fn new(get_raw: fn(&str) -> Option<String>) -> Self {
        Self { get_raw }
    }

    /// Read `key` and parse it as `T`, falling back to `default` when the host
    /// injected no such key.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is present but its value does not parse
    /// as `T`.
    pub fn get<T: std::str::FromStr>(&self, key: &str, default: T) -> Result<T> {
        match (self.get_raw)(key) {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn present(key: &str) -> Option<String> {
        match key {
            "min_amount" => Some("12.5".to_string()),
            "mangled" => Some("not-a-number".to_string()),
            _ => None,
        }
    }

    #[test]
    fn an_absent_key_yields_the_default() {
        let config = Config::new(present);
        assert_eq!(config.get("threshold", 3.0).unwrap(), 3.0);
    }

    #[test]
    fn a_present_key_is_parsed() {
        let config = Config::new(present);
        assert_eq!(config.get("min_amount", 0.0).unwrap(), 12.5);
    }

    #[test]
    fn a_present_but_unparseable_key_is_an_error_not_the_default() {
        let config = Config::new(present);
        let err = config.get::<f64>("mangled", 0.0).unwrap_err();
        assert!(err.message().contains("mangled"), "{}", err.message());
        assert!(err.message().contains("not-a-number"), "{}", err.message());
    }
}
