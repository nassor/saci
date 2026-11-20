//! Calls into the host running the pipeline: logs, metrics and config.
//!
//! The same functions work in a WebAssembly processor (through the
//! `saci:pipeline/host-io` imports) and in a native plugin (through the host
//! vtable); `#[processor]` and `#[plugin]` wire the backend. With no backend
//! installed, as in a plain unit test, every call is a no-op and
//! [`config`] returns `None`. [`with_config`] installs a map-backed backend
//! for a test.

use std::collections::HashMap;
use std::str::FromStr;

use super::glue::{HostIo, HostScope, with_active};

/// Log severity, in the order the WIT `log-level` enum and the plugin C ABI
/// number it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum Level {
    /// Finest detail.
    Trace = 0,
    /// Diagnostic detail.
    Debug = 1,
    /// Normal operation.
    Info = 2,
    /// Something unexpected that the pipeline survived.
    Warn = 3,
    /// A failure.
    Error = 4,
}

fn log(level: Level, target: &str, message: &str) {
    with_active(|host| host.log(level, target, message));
}

/// Emit a trace-level log line.
pub fn trace(target: &str, message: &str) {
    log(Level::Trace, target, message);
}

/// Emit a debug-level log line.
pub fn debug(target: &str, message: &str) {
    log(Level::Debug, target, message);
}

/// Emit an info-level log line.
pub fn info(target: &str, message: &str) {
    log(Level::Info, target, message);
}

/// Emit a warn-level log line.
pub fn warn(target: &str, message: &str) {
    log(Level::Warn, target, message);
}

/// Emit an error-level log line.
pub fn error(target: &str, message: &str) {
    log(Level::Error, target, message);
}

/// Observe a named metric. The host records it as the
/// `saci_processor_metric` histogram, labelled `metric="<name>"`.
pub fn metric(name: &str, value: f64) {
    with_active(|host| host.metric(name, value));
}

/// The config value the host injected under `key`, or `None` when the key is
/// absent or no host is attached.
pub fn config(key: &str) -> Option<String> {
    with_active(|host| host.config(key)).flatten()
}

/// The config value under `key`, parsed as `T`.
///
/// `None` means the key is absent; `Some(Err(_))` means it is present but does
/// not parse.
pub fn config_parse<T: FromStr>(key: &str) -> Option<Result<T, T::Err>> {
    config(key).map(|value| value.parse::<T>())
}

/// Run `f` with a host whose config holds exactly `pairs` and whose logs and
/// metrics go nowhere. For unit tests of code that reads config.
///
/// ```rust
/// use saci::sdk;
///
/// let floor = sdk::host::with_config(&[("min_amount", "12.5")], || {
///     sdk::host::config_parse::<f64>("min_amount")
/// });
/// assert_eq!(floor, Some(Ok(12.5)));
/// ```
pub fn with_config<R>(pairs: &[(&str, &str)], f: impl FnOnce() -> R) -> R {
    let host = MapHost(
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
    );
    let _scope = HostScope::enter(&host);
    f()
}

/// The backend [`with_config`] installs.
struct MapHost(HashMap<String, String>);

impl HostIo for MapHost {
    fn config(&self, key: &str) -> Option<String> {
        self.0.get(key).cloned()
    }

    fn log(&self, _level: Level, _target: &str, _message: &str) {}

    fn metric(&self, _name: &str, _value: f64) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_a_host_every_call_is_a_no_op() {
        info("t", "m");
        metric("m", 1.0);
        assert_eq!(config("k"), None);
    }

    #[test]
    fn with_config_scopes_the_backend() {
        assert_eq!(with_config(&[("k", "7")], || config("k")), Some("7".into()));
        assert_eq!(config("k"), None, "the backend is gone after the call");
    }

    #[test]
    fn config_parse_distinguishes_absent_from_malformed() {
        with_config(&[("n", "x")], || {
            assert!(config_parse::<i32>("missing").is_none());
            assert!(matches!(config_parse::<i32>("n"), Some(Err(_))));
        });
    }

    #[test]
    fn levels_match_the_wit_and_c_abi_order() {
        assert_eq!(Level::Trace as u32, 0);
        assert_eq!(Level::Info as u32, 2);
        assert_eq!(Level::Error as u32, 4);
    }
}
