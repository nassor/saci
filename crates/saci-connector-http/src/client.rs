//! The one reqwest client both halves are built on, and the header map they
//! send with every request.
//!
//! reqwest 0.13's `rustls-no-provider` links rustls without a crypto provider
//! and panics when a `Client` is built before one is installed, so
//! [`build_client`] installs `ring` first. That is the backend the rest of the
//! workspace links (saci-connector-postgresql's TLS path, saci-service's own
//! client), and the install is process global and idempotent, so a host that
//! already installed one keeps it.

use std::sync::LazyLock;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

use saci_core::error::SaciError;

/// The workspace's `ring` crypto provider, installed on first force.
///
/// Process global and idempotent: a provider another crate installed first, or
/// a racing install from another thread, is left in place.
static RING_PROVIDER: LazyLock<()> = LazyLock::new(|| {
    let _ = rustls::crypto::ring::default_provider().install_default();
});

/// Build the client one half sends every request through.
///
/// `timeout` is the whole-request budget, connect through body. `what` names
/// the connector in the error, so a failure says which node rejected it.
///
/// HTTPS needs no configuration: rustls verifies the peer against the platform
/// trust store through `rustls-platform-verifier`, which reqwest's rustls
/// feature brings in. There is no key to turn that off.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when reqwest cannot build a client, for
/// example when the platform trust store cannot be read.
pub(crate) fn build_client(what: &str, timeout: Duration) -> Result<reqwest::Client, SaciError> {
    LazyLock::force(&RING_PROVIDER);
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| SaciError::configuration(format!("{what}: cannot build an HTTP client: {e}")))
}

/// Turn configured `(name, value)` pairs into the map every request carries.
///
/// Done once in the constructor rather than per request, so a header a server
/// would reject as malformed is a config error before the first batch.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] naming the offending header when the
/// name is not a valid HTTP field name or the value holds bytes a field value
/// cannot carry.
pub(crate) fn header_map(
    what: &str,
    headers: Vec<(String, String)>,
) -> Result<HeaderMap, SaciError> {
    let mut map = HeaderMap::with_capacity(headers.len());
    for (name, value) in headers {
        let parsed_name = HeaderName::try_from(name.as_str()).map_err(|e| {
            SaciError::configuration(format!("{what}: '{name}' is not a valid header name: {e}"))
        })?;
        let parsed_value = HeaderValue::try_from(value.as_str()).map_err(|e| {
            SaciError::configuration(format!("{what}: header '{name}' has an invalid value: {e}"))
        })?;
        map.append(parsed_name, parsed_value);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_client_builds_with_the_ring_provider_installed() {
        build_client("HttpSource", Duration::from_secs(1)).expect("client builds");
    }

    #[test]
    fn header_pairs_become_a_map_keeping_repeated_names() {
        let map = header_map(
            "HttpSink",
            vec![
                ("x-token".to_string(), "abc".to_string()),
                ("accept".to_string(), "text/csv".to_string()),
                ("accept".to_string(), "text/plain".to_string()),
            ],
        )
        .expect("valid headers");
        assert_eq!(map.get("x-token").expect("x-token"), "abc");
        assert_eq!(map.get_all("accept").iter().count(), 2);
    }

    #[test]
    fn a_bad_header_name_is_a_configuration_error_naming_it() {
        let Err(err) = header_map(
            "HttpSource",
            vec![("bad header".to_string(), "v".to_string())],
        ) else {
            panic!("a space is not legal in a header name");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'bad header'"), "got: {err}");
    }

    #[test]
    fn a_bad_header_value_is_a_configuration_error_naming_the_header() {
        let Err(err) = header_map(
            "HttpSink",
            vec![("x-token".to_string(), "a\nb".to_string())],
        ) else {
            panic!("a newline is not legal in a header value");
        };
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("header 'x-token'"), "got: {err}");
    }
}
