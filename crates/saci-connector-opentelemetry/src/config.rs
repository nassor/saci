//! The configuration both halves read from their node's `config` table.
//!
//! Every struct carries `#[serde(deny_unknown_fields)]`, so an unexpected key
//! is a configuration error rather than a silently ignored one.

use std::collections::HashMap;

use saci_connector::{ConfigValue, one_or_many};
use saci_core::error::SaciError;
use serde::Deserialize;

/// The standard OTLP/HTTP listen address.
pub const DEFAULT_BIND: &str = "127.0.0.1:4318";

/// The OTLP/HTTP path one body of metrics is posted to.
pub const DEFAULT_PATH: &str = "/v1/metrics";

/// How many requests may wait for the runner before a sender waits with them.
pub const DEFAULT_BUFFER: usize = 64;

/// The largest body either mode reads, in bytes.
pub const DEFAULT_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// How often a pull mode reads its url.
pub const DEFAULT_INTERVAL_MS: u64 = 10_000;

/// How long a sink waits for a response.
pub const DEFAULT_TIMEOUT_MS: u64 = 10_000;

/// How many rows one export carries.
pub const DEFAULT_BATCH_ROWS: usize = 8_192;

/// The body encoding an OTLP/HTTP node speaks.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    /// `application/x-protobuf`, the encoding every OTLP/HTTP client speaks
    /// and the one an empty `content-type` means.
    #[default]
    Protobuf,
    /// `application/json`.
    Json,
}

/// A bearer token a push mode requires of every request.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AuthConfig {
    /// The token, compared in constant time.
    pub bearer: String,
}

/// The push mode: a listener an OTLP/HTTP client posts to.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PushMode {
    /// The address to listen on.
    #[serde(default = "default_bind")]
    pub bind: String,
    /// The path a body is posted to.
    #[serde(default = "default_path")]
    pub path: String,
    /// How many requests may wait for the runner.
    #[serde(default = "default_buffer")]
    pub buffer: usize,
    /// The bearer token every request must present.
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    /// The largest body accepted.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
}

/// The pull mode: a url read once or on an interval.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PullMode {
    /// The url a body is read from.
    pub url: String,
    /// How long to wait between reads. Ignored when `stop_at_end` is set.
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
    /// Headers to send with every read.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Whether to read once and finish, rather than reading on an interval.
    #[serde(default)]
    pub stop_at_end: bool,
    /// The largest body accepted.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
}

/// The read strategy of one source.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceModeConfig {
    /// Listen for OTLP/HTTP bodies.
    Push(PushMode),
    /// Read a url that answers one body.
    Pull(PullMode),
}

impl SourceModeConfig {
    /// The mode's own name, for messages.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Push(_) => "push",
            Self::Pull(_) => "pull",
        }
    }

    /// Whether the source reaches EOF on its own, which decides whether only
    /// the stream runner may drive it.
    pub fn is_live(&self) -> bool {
        match self {
            // A listener has no last request.
            Self::Push(_) => true,
            Self::Pull(pull) => !pull.stop_at_end,
        }
    }
}

/// Configuration for the metrics source.
///
/// Every field has a default, so a config that needs only the standard
/// listener declares nothing at all.
#[derive(Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OpenTelemetryMetricsSourceConfig {
    /// The listen address of the push mode, when no `mode` block is given.
    #[serde(default)]
    pub bind: Option<String>,
    /// The path of the push mode, when no `mode` block is given.
    #[serde(default)]
    pub path: Option<String>,
    /// The read strategy. Absent means `push`.
    #[serde(default)]
    pub mode: Option<SourceModeConfig>,
    /// The bearer token of the push mode, when no `mode` block is given.
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    /// How many requests may wait for the runner, when no `mode` block is
    /// given.
    #[serde(default)]
    pub buffer: Option<usize>,
    /// The largest body accepted, when no `mode` block is given.
    #[serde(default)]
    pub max_body_bytes: Option<usize>,
    /// The Arrow schema to emit, as `schema_fields` entries. Parsed by
    /// `saci_connector::parse_optional_schema_fields` from the same table so
    /// the type vocabulary matches every other connector; declared here only
    /// so `deny_unknown_fields` accepts the key.
    #[serde(default, deserialize_with = "one_or_many")]
    pub schema_fields: Vec<ConfigValue>,
}

impl OpenTelemetryMetricsSourceConfig {
    /// The read strategy, with the top-level shorthand filled in.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when a shorthand key and a `mode`
    /// block are both present, or when a value is not usable.
    pub fn resolve_mode(&self, what: &str) -> Result<SourceModeConfig, SaciError> {
        let shorthand = self.bind.is_some()
            || self.path.is_some()
            || self.auth.is_some()
            || self.buffer.is_some()
            || self.max_body_bytes.is_some();
        if self.mode.is_some() && shorthand {
            return Err(SaciError::configuration(format!(
                "{what}: bind, path, auth, buffer and max_body_bytes belong to the push mode, and a \
                 'mode' block sets them for whichever kind it names"
            )));
        }
        // `buffer` is the connector's one lenient key: a value below one would
        // deadlock the listener, so it is clamped rather than refused, the way
        // `tcp`, `udp` and `http-push` clamp it.
        let mode = match &self.mode {
            Some(mode) => mode.clone(),
            None => SourceModeConfig::Push(PushMode {
                bind: self.bind.clone().unwrap_or_else(default_bind),
                path: self.path.clone().unwrap_or_else(default_path),
                buffer: self.buffer.unwrap_or(DEFAULT_BUFFER).max(1),
                auth: self.auth.clone(),
                max_body_bytes: self.max_body_bytes.unwrap_or(DEFAULT_MAX_BODY_BYTES),
            }),
        };
        match &mode {
            SourceModeConfig::Push(push) => {
                if push.buffer == 0 {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.buffer must be at least 1"
                    )));
                }
                if push.max_body_bytes == 0 {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.max_body_bytes must be at least 1"
                    )));
                }
                if let Some(auth) = &push.auth
                    && auth.bearer.is_empty()
                {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.auth.bearer must not be empty"
                    )));
                }
                if !push.path.starts_with('/') {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.path must start with '/', and it is '{}'",
                        push.path
                    )));
                }
            }
            SourceModeConfig::Pull(pull) => {
                if !pull.url.starts_with("http://") && !pull.url.starts_with("https://") {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.url must start with 'http://' or 'https://', and it is '{}'",
                        pull.url
                    )));
                }
                if pull.interval_ms == 0 && !pull.stop_at_end {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.interval_ms must be at least 1 when the pull mode repeats"
                    )));
                }
                if pull.max_body_bytes == 0 {
                    return Err(SaciError::configuration(format!(
                        "{what}: mode.max_body_bytes must be at least 1"
                    )));
                }
            }
        }
        Ok(mode)
    }
}

/// Configuration for the metrics sink.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OpenTelemetryMetricsSinkConfig {
    /// The collector's endpoint, either an OTLP/HTTP url or a base url the
    /// `/v1/metrics` path is appended to.
    pub url: String,
    /// The body encoding to export with.
    #[serde(default)]
    pub encoding: Encoding,
    /// Headers to send with every export.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// How long one export waits for a response.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
    /// The metric name a batch with no name column exports under.
    #[serde(default)]
    pub metric_name: Option<String>,
    /// The `service.name` resource attribute every export carries.
    #[serde(default)]
    pub service_name: Option<String>,
    /// How many rows one export carries at most.
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
    /// The Arrow schema the sink declares to the workflow graph, as
    /// `schema_fields` entries. Parsed by
    /// `saci_connector::parse_optional_schema_fields` from the same table so
    /// the type vocabulary matches every other connector; declared here only
    /// so `deny_unknown_fields` accepts the key.
    ///
    /// The nodes on either side of a `link` must declare the same schema, so a
    /// source that projects its rows declares this list too. The conversion
    /// itself reads the batch's own columns by name and needs no declaration.
    #[serde(default, deserialize_with = "one_or_many")]
    pub schema_fields: Vec<ConfigValue>,
}

impl OpenTelemetryMetricsSinkConfig {
    /// The endpoint every export is posted to, with the standard path added
    /// when the config names only a base url.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `url` is not an HTTP url or a
    /// value is not usable.
    pub fn endpoint(&self, what: &str) -> Result<String, SaciError> {
        if !self.url.starts_with("http://") && !self.url.starts_with("https://") {
            return Err(SaciError::configuration(format!(
                "{what}: url must start with 'http://' or 'https://', and it is '{}'",
                self.url
            )));
        }
        if self.batch_rows == 0 {
            return Err(SaciError::configuration(format!(
                "{what}: batch_rows must be at least 1"
            )));
        }
        if self.timeout_ms == 0 {
            return Err(SaciError::configuration(format!(
                "{what}: timeout_ms must be at least 1"
            )));
        }
        let trimmed = self.url.trim_end_matches('/');
        let endpoint = if trimmed.ends_with("/v1/metrics") {
            trimmed.to_string()
        } else {
            format!("{trimmed}{DEFAULT_PATH}")
        };
        Ok(endpoint)
    }
}

fn default_bind() -> String {
    DEFAULT_BIND.to_string()
}

fn default_path() -> String {
    DEFAULT_PATH.to_string()
}

fn default_buffer() -> usize {
    DEFAULT_BUFFER
}

fn default_max_body_bytes() -> usize {
    DEFAULT_MAX_BODY_BYTES
}

fn default_interval_ms() -> u64 {
    DEFAULT_INTERVAL_MS
}

fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

fn default_batch_rows() -> usize {
    DEFAULT_BATCH_ROWS
}

#[cfg(test)]
mod tests {
    use saci_connector::from_kdl_str;

    use super::*;

    fn source(text: &str) -> OpenTelemetryMetricsSourceConfig {
        OpenTelemetryMetricsSourceConfig::deserialize(from_kdl_str(text).expect("valid KDL"))
            .expect("the config parses")
    }

    fn sink(text: &str) -> OpenTelemetryMetricsSinkConfig {
        OpenTelemetryMetricsSinkConfig::deserialize(from_kdl_str(text).expect("valid KDL"))
            .expect("the config parses")
    }

    #[test]
    fn a_source_without_a_mode_block_listens_on_the_standard_address() {
        let mode = source("")
            .resolve_mode("src")
            .expect("the defaults resolve");
        let SourceModeConfig::Push(push) = mode else {
            panic!("no mode block means push");
        };
        assert_eq!(push.bind, DEFAULT_BIND);
        assert_eq!(push.path, DEFAULT_PATH);
        assert_eq!(push.buffer, DEFAULT_BUFFER);
        assert_eq!(push.max_body_bytes, DEFAULT_MAX_BODY_BYTES);
        assert!(push.auth.is_none());
    }

    #[test]
    fn the_shorthand_keys_fill_the_push_mode_in() {
        let config = source(
            "bind \"0.0.0.0:9999\"\npath \"/custom\"\nbuffer 8\nmax_body_bytes 1024\nauth bearer=\"t\"",
        );
        let mode = config.resolve_mode("src").expect("the shorthand resolves");
        let SourceModeConfig::Push(push) = mode else {
            panic!("the shorthand builds the push mode");
        };
        assert_eq!(push.bind, "0.0.0.0:9999");
        assert_eq!(push.path, "/custom");
        assert_eq!(push.buffer, 8);
        assert_eq!(push.max_body_bytes, 1024);
        assert_eq!(push.auth.expect("a token").bearer, "t");
    }

    #[test]
    fn a_pull_mode_decides_whether_the_source_is_live() {
        let repeating = source("mode kind=\"pull\" url=\"http://127.0.0.1:1/\"");
        let mode = repeating
            .resolve_mode("src")
            .expect("the pull mode resolves");
        assert!(mode.is_live());
        assert_eq!(mode.kind(), "pull");
        let SourceModeConfig::Pull(pull) = mode else {
            panic!("a pull mode block builds the pull mode");
        };
        assert_eq!(pull.interval_ms, DEFAULT_INTERVAL_MS);

        let once = source(
            "mode kind=\"pull\" url=\"http://127.0.0.1:1/\" stop_at_end=#true interval_ms=0",
        );
        let mode = once.resolve_mode("src").expect("the pull mode resolves");
        assert!(!mode.is_live());
    }

    #[test]
    fn a_shorthand_key_beside_a_mode_block_is_refused() {
        let config = source("bind \"0.0.0.0:1\"\nmode kind=\"pull\" url=\"http://127.0.0.1:1/\"");
        let error = config.resolve_mode("src").expect_err("both is a mistake");
        assert!(
            error.to_string().contains("belong to the push mode"),
            "{error}"
        );
    }

    #[test]
    fn an_empty_token_is_refused() {
        let config = source("auth bearer=\"\"");
        let error = config
            .resolve_mode("src")
            .expect_err("an empty token is no token");
        assert!(error.to_string().contains("bearer"), "{error}");
    }

    #[test]
    fn a_relative_path_is_refused() {
        let config = source("path \"metrics\"");
        let error = config.resolve_mode("src").expect_err("a path is absolute");
        assert!(error.to_string().contains("must start with"), "{error}");
    }

    #[test]
    fn a_zero_interval_without_stop_at_end_is_refused() {
        let config = source("mode kind=\"pull\" url=\"http://127.0.0.1:1/\" interval_ms=0");
        let error = config
            .resolve_mode("src")
            .expect_err("a repeated read needs an interval");
        assert!(error.to_string().contains("interval_ms"), "{error}");
    }

    #[test]
    fn an_unknown_key_is_refused() {
        assert!(
            OpenTelemetryMetricsSourceConfig::deserialize(
                from_kdl_str("bin \"0.0.0.0:1\"").expect("valid KDL")
            )
            .is_err()
        );
        assert!(
            OpenTelemetryMetricsSinkConfig::deserialize(
                from_kdl_str("url \"http://127.0.0.1:1\"\nencodng \"json\"").expect("valid KDL")
            )
            .is_err()
        );
    }

    #[test]
    fn an_unknown_key_inside_a_mode_block_is_refused() {
        assert!(
            OpenTelemetryMetricsSourceConfig::deserialize(
                from_kdl_str("mode kind=\"pull\" url=\"http://127.0.0.1:1/\" intervl_ms=1")
                    .expect("valid KDL")
            )
            .is_err()
        );
        assert!(
            OpenTelemetryMetricsSourceConfig::deserialize(
                from_kdl_str("mode kind=\"push\" bnd=\"0.0.0.0:1\"").expect("valid KDL")
            )
            .is_err()
        );
    }

    #[test]
    fn a_mode_kind_nothing_names_is_refused() {
        assert!(
            OpenTelemetryMetricsSourceConfig::deserialize(
                from_kdl_str("mode kind=\"stream\" url=\"http://127.0.0.1:1/\"")
                    .expect("valid KDL")
            )
            .is_err()
        );
    }

    #[test]
    fn a_sink_url_gains_the_standard_path_when_it_names_only_a_base() {
        let config = sink("url \"http://localhost:4318\"");
        assert_eq!(
            config.endpoint("sink").expect("the endpoint resolves"),
            "http://localhost:4318/v1/metrics"
        );
        assert_eq!(config.encoding, Encoding::Protobuf);
        assert_eq!(config.batch_rows, DEFAULT_BATCH_ROWS);
        assert_eq!(config.timeout_ms, DEFAULT_TIMEOUT_MS);
    }

    #[test]
    fn a_sink_url_that_already_names_the_path_keeps_it() {
        let config = sink("url \"http://localhost:4318/v1/metrics\"\nencoding \"json\"");
        assert_eq!(
            config.endpoint("sink").expect("the endpoint resolves"),
            "http://localhost:4318/v1/metrics"
        );
        assert_eq!(config.encoding, Encoding::Json);
    }

    #[test]
    fn a_sink_url_of_the_wrong_shape_is_refused() {
        let config = sink("url \"localhost:4318\"");
        let error = config.endpoint("sink").expect_err("a url needs its scheme");
        assert!(error.to_string().contains("http://"), "{error}");
    }
}
