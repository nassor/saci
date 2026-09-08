//! Connecting, and building a JetStream context, from [`ConnectionConfig`].
//!
//! Both halves of the connector connect lazily, on the first `next_batch` or
//! `write_batch`, so nothing here runs during `saci-service validate`.

use std::path::PathBuf;
use std::time::Duration;

use async_nats::jetstream::{self, context::ContextBuilder};
use async_nats::{Client, ConnectOptions};

use saci_core::error::SaciError;

use crate::config::{AuthConfig, ConnectionConfig};

/// Open a connection described by `cfg`.
///
/// `what` prefixes every error, so a failure names the connector that hit it.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when a credential file cannot be read,
/// and [`SaciError::Generic`] when the servers cannot be reached.
pub(crate) async fn connect(cfg: &ConnectionConfig, what: &str) -> Result<Client, SaciError> {
    let mut options = ConnectOptions::new()
        .connection_timeout(Duration::from_millis(cfg.connect_timeout_ms))
        .ping_interval(Duration::from_millis(cfg.ping_interval_ms))
        .subscription_capacity(cfg.subscription_capacity)
        .client_capacity(cfg.client_capacity)
        .read_buffer_capacity(cfg.read_buffer_capacity)
        // 0 means "wait forever", which the builder spells as `None`.
        .request_timeout(if cfg.request_timeout_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(cfg.request_timeout_ms))
        })
        // Spelled out rather than leaning on the builder's own `Some(0) => None`
        // normalisation, so the configured meaning of 0 is readable here.
        .max_reconnects(if cfg.max_reconnects == 0 {
            None
        } else {
            Some(cfg.max_reconnects)
        });

    if let Some(name) = &cfg.name {
        options = options.name(name);
    }
    if cfg.reconnect_delay_ms > 0 {
        let delay = Duration::from_millis(cfg.reconnect_delay_ms);
        options = options.reconnect_delay_callback(move |_attempt| delay);
    }
    if cfg.retry_on_initial_connect {
        options = options.retry_on_initial_connect();
    }
    if cfg.no_echo {
        options = options.no_echo();
    }
    if cfg.ignore_discovered_servers {
        options = options.ignore_discovered_servers();
    }
    if cfg.retain_servers_order {
        options = options.retain_servers_order();
    }
    if let Some(prefix) = &cfg.inbox_prefix {
        options = options.custom_inbox_prefix(prefix);
    }

    options = options.require_tls(cfg.tls.require);
    if cfg.tls.tls_first {
        // Also forces `tls_required`, so `require = false` with
        // `tls_first = true` still encrypts.
        options = options.tls_first();
    }
    if let Some(path) = &cfg.tls.root_certificates {
        options = options.add_root_certificates(PathBuf::from(path));
    }
    if let (Some(cert), Some(key)) = (&cfg.tls.client_certificate, &cfg.tls.client_key) {
        options = options.add_client_certificate(PathBuf::from(cert), PathBuf::from(key));
    }

    // Auth last: `credentials_file` is the one async builder, so it cannot sit
    // inside the chain above.
    options = match &cfg.auth {
        AuthConfig::None => options,
        AuthConfig::Token { token, token_file } => {
            options.token(secret(what, "auth.token", token, token_file).await?)
        }
        AuthConfig::UserPassword {
            user,
            password,
            password_file,
        } => options.user_and_password(
            user.clone(),
            secret(what, "auth.password", password, password_file).await?,
        ),
        AuthConfig::Nkey { seed, seed_file } => {
            options.nkey(secret(what, "auth.seed", seed, seed_file).await?)
        }
        AuthConfig::Credentials { path } => options.credentials_file(path).await.map_err(|e| {
            SaciError::configuration(format!(
                "{what}: cannot read connection.auth.path '{path}': {e}"
            ))
        })?,
    };

    options.connect(cfg.servers.clone()).await.map_err(|e| {
        SaciError::generic(format!("{what}: cannot connect to {:?}: {e}", cfg.servers))
    })
}

/// The inline value, or the trimmed contents of the file beside it.
///
/// `ConnectionConfig::validate` has already proved exactly one of the two is
/// set, so the `None` arm is unreachable for a validated config and still
/// returns an error rather than panicking.
async fn secret(
    what: &str,
    key: &str,
    inline: &Option<String>,
    file: &Option<String>,
) -> Result<String, SaciError> {
    match (inline, file) {
        (Some(value), _) => Ok(value.clone()),
        (None, Some(path)) => Ok(tokio::fs::read_to_string(path)
            .await
            .map_err(|e| {
                SaciError::configuration(format!(
                    "{what}: cannot read connection.{key}_file '{path}': {e}"
                ))
            })?
            .trim()
            .to_string()),
        (None, None) => Err(SaciError::configuration(format!(
            "{what}: connection.{key} has neither an inline value nor a file"
        ))),
    }
}

/// Build a JetStream context over `client`.
///
/// `domain` and `api_prefix` are mutually exclusive; config validation rejects
/// both being set.
pub(crate) fn jetstream_context(
    client: Client,
    domain: Option<&str>,
    api_prefix: Option<&str>,
    timeout: Duration,
    ack_timeout: Duration,
    max_ack_inflight: usize,
    backpressure_on_inflight: bool,
) -> jetstream::Context {
    // `api_prefix` and `domain` are the type-state's entry point and can only be
    // called first, and `domain` is only shorthand for a prefix, so resolving
    // the prefix here leaves one chain to follow.
    ContextBuilder::new()
        .api_prefix(api_prefix_for(domain, api_prefix))
        .timeout(timeout)
        .ack_timeout(ack_timeout)
        .max_ack_inflight(max_ack_inflight)
        .backpressure_on_inflight(backpressure_on_inflight)
        .build(client)
}

/// The JetStream API prefix a config that names neither `domain` nor
/// `api_prefix` gets. Identical to the client's own default.
const DEFAULT_API_PREFIX: &str = "$JS.API";

/// The API prefix these two keys name.
///
/// A `domain` is shorthand for `$JS.{domain}.API`, which is what
/// `ContextBuilder::domain` writes.
fn api_prefix_for(domain: Option<&str>, api_prefix: Option<&str>) -> String {
    match (domain, api_prefix) {
        (Some(domain), None) => format!("$JS.{domain}.API"),
        (None, Some(prefix)) => prefix.to_string(),
        _ => DEFAULT_API_PREFIX.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_absent_domain_and_prefix_use_the_client_default() {
        assert_eq!(api_prefix_for(None, None), "$JS.API");
    }

    #[test]
    fn a_domain_becomes_the_bracketed_prefix() {
        assert_eq!(api_prefix_for(Some("hub"), None), "$JS.hub.API");
    }

    #[test]
    fn an_explicit_prefix_is_used_verbatim() {
        assert_eq!(api_prefix_for(None, Some("MY.JS.API")), "MY.JS.API");
    }

    #[tokio::test]
    async fn an_inline_secret_is_returned_as_is() {
        let value = secret("NatsSource", "auth.token", &Some("t0ken".into()), &None)
            .await
            .expect("an inline value needs no file");
        assert_eq!(value, "t0ken");
    }

    #[tokio::test]
    async fn a_missing_secret_file_names_itself() {
        let err = secret(
            "NatsSource",
            "auth.token",
            &None,
            &Some("no/such/token".into()),
        )
        .await
        .expect_err("the file does not exist");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("no/such/token"), "got: {err}");
    }

    #[tokio::test]
    async fn neither_an_inline_value_nor_a_file_is_an_error_not_a_panic() {
        let err = secret("NatsSink", "auth.seed", &None, &None)
            .await
            .expect_err("validation rejects this, so the builder must not panic on it");
        assert_eq!(err.category(), "configuration");
    }
}
