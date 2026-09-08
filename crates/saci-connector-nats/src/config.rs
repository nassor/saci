//! Serde-derived configuration for the NATS source and sink.
//!
//! Every struct and enum here carries `#[serde(deny_unknown_fields)]`: a key the
//! connector cannot honour is a configuration error, not something to drop
//! silently. `async-nats` has no string-keyed property bag, so there is no
//! passthrough table: every knob is a named key.
//!
//! Both top-level configs expose `validate`, which the constructors in
//! [`crate::source`] and [`crate::sink`] call before they build anything.
//! Validation returns [`SaciError::Configuration`] naming the offending key on
//! the first violation, and reads no file and opens no socket, so
//! `saci-service validate` stays broker-free.
//!
//! The `mode` child node is internally tagged on `kind` and picks core NATS
//! (`kind="core"`) or JetStream (`kind="jetstream"`), matching the `run_mode`
//! node the service config already uses.

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::time::Duration;

use async_nats::header::{HeaderName, HeaderValue};
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, ReplayPolicy};
use async_nats::jetstream::stream::{Compression, DiscardPolicy, RetentionPolicy, StorageType};
use serde::Deserialize;

use saci_connector::ConfigValue;
use saci_core::error::SaciError;

// ---------------------------------------------------------------- connection

/// Connection, auth and TLS settings shared by the source and the sink.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    /// Server URLs. `nats://`, `tls://`, `ws://` and `wss://` are accepted, and
    /// a bare `host:port` means `nats://host:port`. At least one is required.
    #[serde(deserialize_with = "saci_connector::one_or_many")]
    pub servers: Vec<String>,
    /// Client name the server reports in its connection list.
    #[serde(default)]
    pub name: Option<String>,
    /// TCP plus handshake budget for one connect attempt.
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// How long a request waits for its reply. 0 waits forever.
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    /// Interval between client pings.
    #[serde(default = "default_ping_interval_ms")]
    pub ping_interval_ms: u64,
    /// Reconnect attempts before the client gives up. 0 is unlimited.
    #[serde(default)]
    pub max_reconnects: usize,
    /// Fixed delay between reconnect attempts. 0 keeps the library's own
    /// backoff.
    #[serde(default)]
    pub reconnect_delay_ms: u64,
    /// Return from `connect` before the first connection succeeds and establish
    /// it in the background.
    #[serde(default = "default_true")]
    pub retry_on_initial_connect: bool,
    /// Messages buffered per subscription before the client drops.
    #[serde(default = "default_subscription_capacity")]
    pub subscription_capacity: usize,
    /// Commands buffered between the client handle and its connection task.
    #[serde(default = "default_client_capacity")]
    pub client_capacity: usize,
    /// Socket read buffer, in bytes.
    #[serde(default = "default_read_buffer_capacity")]
    pub read_buffer_capacity: u16,
    /// Ask the server not to echo this connection's own publishes back to its
    /// own subscriptions.
    #[serde(default)]
    pub no_echo: bool,
    /// Prefix for the inbox subjects request/reply uses. Defaults to `_INBOX`.
    #[serde(default)]
    pub inbox_prefix: Option<String>,
    /// Connect only to the configured servers, ignoring the ones the cluster
    /// advertises.
    #[serde(default)]
    pub ignore_discovered_servers: bool,
    /// Try servers in the configured order instead of shuffling them.
    #[serde(default)]
    pub retain_servers_order: bool,
    /// How the connection authenticates.
    #[serde(default)]
    pub auth: AuthConfig,
    /// Whether and how the connection is encrypted.
    #[serde(default)]
    pub tls: TlsConfig,
}

// `Default` must agree with the serde defaults above, so a config that omits a
// key and a `ConnectionConfig::default()` built in Rust describe the same
// connection. `servers` has no default: it is required.
impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            servers: Vec::new(),
            name: None,
            connect_timeout_ms: default_connect_timeout_ms(),
            request_timeout_ms: default_request_timeout_ms(),
            ping_interval_ms: default_ping_interval_ms(),
            max_reconnects: 0,
            reconnect_delay_ms: 0,
            retry_on_initial_connect: true,
            subscription_capacity: default_subscription_capacity(),
            client_capacity: default_client_capacity(),
            read_buffer_capacity: default_read_buffer_capacity(),
            no_echo: false,
            inbox_prefix: None,
            ignore_discovered_servers: false,
            retain_servers_order: false,
            auth: AuthConfig::None,
            tls: TlsConfig::default(),
        }
    }
}

/// How the connection authenticates, chosen by `kind`.
///
/// Each secret has an inline form and a `_file` form for a secret mount, and
/// exactly one of the pair must be set.
#[derive(Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthConfig {
    /// No credentials. What an absent `auth` node means.
    #[default]
    None,
    /// A bearer token.
    Token {
        /// The token itself.
        #[serde(default)]
        token: Option<String>,
        /// Path to a file whose trimmed contents are the token.
        #[serde(default)]
        token_file: Option<String>,
    },
    /// A user name and password.
    UserPassword {
        /// The user name.
        user: String,
        /// The password itself.
        #[serde(default)]
        password: Option<String>,
        /// Path to a file whose trimmed contents are the password.
        #[serde(default)]
        password_file: Option<String>,
    },
    /// An NKey seed.
    Nkey {
        /// The seed itself.
        #[serde(default)]
        seed: Option<String>,
        /// Path to a file whose trimmed contents are the seed.
        #[serde(default)]
        seed_file: Option<String>,
    },
    /// A `.creds` file holding a JWT and its NKey seed.
    Credentials {
        /// Path to the credentials file.
        path: String,
    },
}

/// Whether and how the connection is encrypted.
#[derive(Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// Refuse to connect without TLS. A `tls://` server URL requires it too.
    #[serde(default)]
    pub require: bool,
    /// Perform the TLS handshake before the NATS `INFO` exchange. Needs
    /// `handshake_first` on the server, and implies `require`.
    #[serde(default)]
    pub tls_first: bool,
    /// PEM bundle of trusted roots. Absent means the OS trust store.
    #[serde(default)]
    pub root_certificates: Option<String>,
    /// Client certificate, for mutual TLS. Needs `client_key`.
    #[serde(default)]
    pub client_certificate: Option<String>,
    /// Private key for `client_certificate`.
    #[serde(default)]
    pub client_key: Option<String>,
}

impl ConnectionConfig {
    /// Check every cross-field invariant, and that each server URL parses.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the offending key on the
    /// first violation.
    pub fn validate(&self, what: &str) -> Result<(), SaciError> {
        if self.servers.is_empty() {
            return Err(SaciError::configuration(format!(
                "{what}: connection.servers must name at least one server"
            )));
        }
        for server in &self.servers {
            // Pure parsing, no DNS and no socket, so `validate` stays
            // broker-free.
            server
                .parse::<async_nats::ServerAddr>()
                .map_err(|e| {
                    SaciError::configuration(format!(
                        "{what}: connection.servers entry '{server}' is not a NATS URL: {e}"
                    ))
                })
                .map(drop)?;
        }
        for (key, value) in [
            ("ping_interval_ms", self.ping_interval_ms),
            ("connect_timeout_ms", self.connect_timeout_ms),
            ("read_buffer_capacity", u64::from(self.read_buffer_capacity)),
            ("subscription_capacity", self.subscription_capacity as u64),
            ("client_capacity", self.client_capacity as u64),
        ] {
            if value == 0 {
                return Err(SaciError::configuration(format!(
                    "{what}: connection.{key} must be at least 1"
                )));
            }
        }
        self.auth.validate(what)?;
        self.tls.validate(what)
    }
}

impl AuthConfig {
    fn validate(&self, what: &str) -> Result<(), SaciError> {
        let pair = match self {
            AuthConfig::None | AuthConfig::Credentials { .. } => return Ok(()),
            AuthConfig::Token { token, token_file } => {
                ("token", "token", "token_file", token, token_file)
            }
            AuthConfig::UserPassword {
                password,
                password_file,
                ..
            } => (
                "user_password",
                "password",
                "password_file",
                password,
                password_file,
            ),
            AuthConfig::Nkey { seed, seed_file } => ("nkey", "seed", "seed_file", seed, seed_file),
        };
        let (kind, inline_key, file_key, inline, file) = pair;
        if inline.is_some() == file.is_some() {
            return Err(SaciError::configuration(format!(
                "{what}: connection.auth kind = \"{kind}\" needs exactly one of '{inline_key}' or \
                 '{file_key}'"
            )));
        }
        Ok(())
    }
}

impl TlsConfig {
    fn validate(&self, what: &str) -> Result<(), SaciError> {
        if self.client_certificate.is_some() != self.client_key.is_some() {
            return Err(SaciError::configuration(format!(
                "{what}: connection.tls needs both 'client_certificate' and 'client_key', or \
                 neither"
            )));
        }
        Ok(())
    }
}

// -------------------------------------------------------------------- source

/// Configuration for [`NatsSource`](crate::NatsSource).
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct NatsSourceConfig {
    /// Connection, auth and TLS.
    pub connection: ConnectionConfig,
    /// Core subject subscription or JetStream pull consumer.
    pub mode: SourceMode,
    /// Maximum messages folded into one `RecordBatch`.
    ///
    /// Only in force when the host asks for no size:
    /// `Source::request_batch_rows` overwrites it before every poll, and
    /// `saci-service` sends that hint from the first poll onward unless the
    /// source's `flow_control` block sets `enabled #false`, or the source is
    /// on a path to a windowed node, where the runner withholds the hint so
    /// this value alone decides how large an arrival is.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// How long one `next_batch` keeps collecting after its first message.
    #[serde(default = "default_poll_timeout_ms")]
    pub poll_timeout_ms: u64,
    /// Report EOF once nothing more is available, making the source usable from
    /// the batch run modes. Off by default: a NATS subscription is a live
    /// source.
    #[serde(default)]
    pub stop_at_end: bool,
    /// Declared Arrow schema. Parsed by `saci_connector::parse_schema_fields`
    /// from the same table so the type vocabulary matches every other
    /// connector; declared here only so `deny_unknown_fields` accepts the key.
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub schema_fields: Vec<ConfigValue>,
}

/// The read strategy, chosen by `kind`.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceMode {
    /// A plain subject subscription. No persistence, no acks.
    Core(CoreSourceMode),
    /// A JetStream pull consumer over a stream.
    ///
    /// Boxed because the JetStream surface is an order of magnitude larger than
    /// the core one, and an enum is as large as its largest variant.
    Jetstream(Box<JetstreamSourceMode>),
}

/// A core NATS subject subscription.
#[derive(Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct CoreSourceMode {
    /// Subject to subscribe to. Comma-separated for several. Wildcards
    /// (`*`, `>`) are the server's own.
    pub subject: String,
    /// Queue group name. Every subscriber in one group sees a disjoint share of
    /// the subject, which is how a core subject spreads across instances.
    #[serde(default)]
    pub queue_group: Option<String>,
}

/// A JetStream pull consumer over a stream.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct JetstreamSourceMode {
    /// Stream to consume.
    pub stream: String,
    /// Durable consumer name. Absent creates an ephemeral consumer, whose
    /// progress the server forgets.
    #[serde(default)]
    pub durable_name: Option<String>,
    /// Consumer name, for a named ephemeral consumer. Defaults to
    /// `durable_name`.
    #[serde(default)]
    pub consumer_name: Option<String>,
    /// A short description of the consumer's purpose.
    #[serde(default)]
    pub description: Option<String>,
    /// Subjects within the stream this consumer sees. Empty means all of them.
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub filter_subjects: Vec<String>,
    /// Which message the consumer starts at.
    #[serde(default)]
    pub deliver_policy: DeliverPolicyConfig,
    /// How messages are acknowledged.
    #[serde(default)]
    pub ack_policy: AckPolicyConfig,
    /// Wait for the server to confirm each ack. Slower, and removes the window
    /// in which a lost ack redelivers.
    #[serde(default)]
    pub double_ack: bool,
    /// How long the server waits for an ack before redelivering. 0 keeps the
    /// server default.
    #[serde(default)]
    pub ack_wait_ms: u64,
    /// Delivery attempts per message before the *server* stops redelivering
    /// and drops it. 0 keeps the server default, and the server reads 0 as
    /// unlimited: nothing on the broker side ever retires a message this
    /// consumer keeps failing on. `max_decode_attempts` is the bound the
    /// source itself enforces, so a message the format cannot decode is
    /// retired loudly rather than redelivered forever.
    #[serde(default)]
    pub max_deliver: i64,
    /// Delivery attempts the source spends on a message before it terminates
    /// it and reports an error naming the stream and the sequence.
    ///
    /// A payload the configured format cannot decode fails identically on
    /// every redelivery, and an undecodable window is never acknowledged, so
    /// without a bound the server redelivers it after each `ack_wait`
    /// forever and the consumer never advances past it. On the last permitted
    /// attempt the source sends `+TERM` for that one message — the rest of
    /// the window is left unacknowledged and redelivered as usual — and
    /// returns an error naming it, so the rows it carried are lost with a
    /// record of which message lost them.
    ///
    /// Counted from the server's own delivery count for that message, so
    /// restarts count too. Clamped by `max_deliver` when that is set, so the
    /// source reports the message rather than letting the server retire it
    /// silently. 0 disables the bound and restores unbounded redelivery.
    #[serde(default = "default_max_decode_attempts")]
    pub max_decode_attempts: u32,
    /// Unacknowledged messages allowed in flight. 0 keeps the server default.
    #[serde(default)]
    pub max_ack_pending: i64,
    /// Concurrent pull requests the server holds. 0 keeps the server default.
    #[serde(default)]
    pub max_waiting: i64,
    /// Ceiling on a pull request's message count. 0 keeps the server default.
    #[serde(default)]
    pub max_batch: i64,
    /// Ceiling on a pull request's byte count. 0 keeps the server default.
    #[serde(default)]
    pub max_bytes: i64,
    /// Ceiling on a pull request's expiry. 0 keeps the server default.
    #[serde(default)]
    pub max_expires_ms: u64,
    /// How long an unused consumer survives. 0 keeps the server default.
    #[serde(default)]
    pub inactive_threshold_ms: u64,
    /// Consumer replicas in a clustered JetStream. 0 follows the stream.
    #[serde(default)]
    pub num_replicas: usize,
    /// Keep consumer state in memory even on a file-backed stream.
    #[serde(default)]
    pub memory_storage: bool,
    /// Deliver headers without payloads. Decoding then sees empty payloads, so
    /// this is for header-only streams.
    #[serde(default)]
    pub headers_only: bool,
    /// Delivery rate ceiling, in bits per second. 0 is unlimited.
    #[serde(default)]
    pub rate_limit_bps: u64,
    /// Percentage of acks the server samples for observability, 0 to 100.
    #[serde(default)]
    pub sample_frequency: u8,
    /// Whether messages arrive as fast as possible or at their original rate.
    #[serde(default)]
    pub replay_policy: ReplayPolicyConfig,
    /// Per-redelivery backoff sequence. Empty uses `ack_wait_ms` each time.
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub backoff_ms: Vec<u64>,
    /// Additional consumer metadata.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    /// How long a `stop_at_end` pull request stays open server-side.
    #[serde(default = "default_fetch_expires_ms")]
    pub fetch_expires_ms: u64,
    /// Byte ceiling on one collected window. 0 leaves it to `batch_size`.
    #[serde(default)]
    pub fetch_max_bytes: usize,
    /// Idle heartbeat interval on a pull request. 0 keeps the library default.
    #[serde(default)]
    pub heartbeat_ms: u64,
    /// JetStream domain. Sets the API prefix to `$JS.{domain}.API`.
    #[serde(default)]
    pub domain: Option<String>,
    /// JetStream API prefix. Defaults to `$JS.API`.
    #[serde(default)]
    pub api_prefix: Option<String>,
    /// Timeout on each JetStream API request.
    #[serde(default = "default_js_timeout_ms")]
    pub api_timeout_ms: u64,
    /// Whether and how SACI creates the stream.
    #[serde(default)]
    pub stream_provision: StreamProvision,
}

// `Default` must agree with the serde defaults above, so a mode built in Rust
// and one parsed from a `mode` node naming only `stream` describe the same
// consumer. `stream` has no default: it is required.
impl Default for JetstreamSourceMode {
    fn default() -> Self {
        Self {
            stream: String::new(),
            durable_name: None,
            consumer_name: None,
            description: None,
            filter_subjects: Vec::new(),
            deliver_policy: DeliverPolicyConfig::All,
            ack_policy: AckPolicyConfig::Explicit,
            double_ack: false,
            ack_wait_ms: 0,
            max_deliver: 0,
            max_decode_attempts: default_max_decode_attempts(),
            max_ack_pending: 0,
            max_waiting: 0,
            max_batch: 0,
            max_bytes: 0,
            max_expires_ms: 0,
            inactive_threshold_ms: 0,
            num_replicas: 0,
            memory_storage: false,
            headers_only: false,
            rate_limit_bps: 0,
            sample_frequency: 0,
            replay_policy: ReplayPolicyConfig::Instant,
            backoff_ms: Vec::new(),
            metadata: BTreeMap::new(),
            fetch_expires_ms: default_fetch_expires_ms(),
            fetch_max_bytes: 0,
            heartbeat_ms: 0,
            domain: None,
            api_prefix: None,
            api_timeout_ms: default_js_timeout_ms(),
            stream_provision: StreamProvision::default(),
        }
    }
}

/// Which message a JetStream consumer starts at, chosen by `kind`.
#[derive(Deserialize, Debug, Clone, Default, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeliverPolicyConfig {
    /// The oldest message still in the stream.
    #[default]
    All,
    /// The last message in the stream.
    Last,
    /// Only messages published after the consumer was created.
    New,
    /// The last message of every subject.
    LastPerSubject,
    /// A given stream sequence.
    ByStartSequence {
        /// The sequence to start at.
        start_sequence: u64,
    },
    /// The first message at or after a given instant.
    ByStartTime {
        /// RFC 3339 timestamp.
        start_time: String,
    },
}

impl DeliverPolicyConfig {
    /// Convert to the client's own policy type.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `start_time` is not RFC 3339.
    /// [`NatsSourceConfig::validate`] rejects that first, so a validated config
    /// cannot reach the error.
    pub fn to_async_nats(&self) -> Result<DeliverPolicy, SaciError> {
        Ok(match self {
            DeliverPolicyConfig::All => DeliverPolicy::All,
            DeliverPolicyConfig::Last => DeliverPolicy::Last,
            DeliverPolicyConfig::New => DeliverPolicy::New,
            DeliverPolicyConfig::LastPerSubject => DeliverPolicy::LastPerSubject,
            DeliverPolicyConfig::ByStartSequence { start_sequence } => {
                DeliverPolicy::ByStartSequence {
                    start_sequence: *start_sequence,
                }
            }
            DeliverPolicyConfig::ByStartTime { start_time } => DeliverPolicy::ByStartTime {
                start_time: parse_start_time(start_time)?,
            },
        })
    }
}

/// Parse an RFC 3339 timestamp into the client's datetime type.
///
/// Goes through `async_nats::datetime::parse_rfc3339` rather than a datetime
/// crate of this connector's own: the alias behind it is `time::OffsetDateTime`
/// or `chrono::DateTime<Utc>` depending on a feature any crate in the build may
/// flip, and this way the connector never names either.
fn parse_start_time(raw: &str) -> Result<async_nats::datetime::DateTime, SaciError> {
    async_nats::datetime::parse_rfc3339(raw).map_err(|e| {
        SaciError::configuration(format!(
            "'mode.deliver_policy.start_time' is not an RFC 3339 timestamp: {e}"
        ))
    })
}

/// How a JetStream consumer acknowledges messages.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AckPolicyConfig {
    /// Every message is acknowledged individually.
    #[default]
    Explicit,
    /// Acknowledging one message acknowledges every earlier one.
    All,
    /// Nothing is acknowledged, so nothing is redelivered.
    None,
}

impl AckPolicyConfig {
    fn to_async_nats(self) -> AckPolicy {
        match self {
            AckPolicyConfig::Explicit => AckPolicy::Explicit,
            AckPolicyConfig::All => AckPolicy::All,
            AckPolicyConfig::None => AckPolicy::None,
        }
    }
}

/// Whether messages arrive as fast as possible or at their original rate.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplayPolicyConfig {
    /// As fast as the consumer takes them.
    #[default]
    Instant,
    /// At the rate the stream received them.
    Original,
}

impl ReplayPolicyConfig {
    fn to_async_nats(self) -> ReplayPolicy {
        match self {
            ReplayPolicyConfig::Instant => ReplayPolicy::Instant,
            ReplayPolicyConfig::Original => ReplayPolicy::Original,
        }
    }
}

impl NatsSourceConfig {
    /// Check every cross-field invariant this config must satisfy.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the offending key on the
    /// first violation.
    pub fn validate(&self) -> Result<(), SaciError> {
        let what = "NatsSource";
        self.connection.validate(what)?;

        if self.batch_size == 0 {
            return Err(SaciError::configuration(format!(
                "{what} config: 'batch_size' must be at least 1"
            )));
        }
        if self.poll_timeout_ms == 0 {
            return Err(SaciError::configuration(format!(
                "{what} config: 'poll_timeout_ms' must be at least 1"
            )));
        }

        match &self.mode {
            SourceMode::Core(core) => {
                if core.subject.split(',').any(|s| s.trim().is_empty()) {
                    return Err(SaciError::configuration(format!(
                        "{what} config: 'mode.subject' must name at least one non-empty subject"
                    )));
                }
            }
            SourceMode::Jetstream(js) => js.validate(what)?,
        }
        Ok(())
    }
}

impl JetstreamSourceMode {
    fn validate(&self, what: &str) -> Result<(), SaciError> {
        if self.stream.trim().is_empty() {
            return Err(SaciError::configuration(format!(
                "{what} config: 'mode.stream' must not be empty"
            )));
        }
        validate_js_api(what, self.domain.as_deref(), self.api_prefix.as_deref())?;
        if self.fetch_expires_ms == 0 {
            return Err(SaciError::configuration(format!(
                "{what} config: 'mode.fetch_expires_ms' must be at least 1"
            )));
        }
        if let DeliverPolicyConfig::ByStartTime { start_time } = &self.deliver_policy {
            parse_start_time(start_time)
                .map_err(|e| SaciError::configuration(format!("{what} config: {}", e.message())))?;
        }
        if self.double_ack && self.ack_policy == AckPolicyConfig::None {
            return Err(SaciError::configuration(format!(
                "{what} config: 'mode.double_ack' needs an ack policy; 'none' acknowledges nothing"
            )));
        }
        self.stream_provision.validate(what)
    }

    /// The pull consumer this mode describes.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `deliver_policy` carries an
    /// unparseable timestamp.
    pub(crate) fn to_consumer_config(
        &self,
    ) -> Result<async_nats::jetstream::consumer::pull::Config, SaciError> {
        // Assigned onto `Default` rather than written as one struct literal:
        // `pull::Config` grows a field per `server_2_*` feature, and any crate
        // in the build may turn one on.
        let mut config = async_nats::jetstream::consumer::pull::Config {
            durable_name: self.durable_name.clone(),
            name: self
                .consumer_name
                .clone()
                .or_else(|| self.durable_name.clone()),
            description: self.description.clone(),
            deliver_policy: self.deliver_policy.to_async_nats()?,
            ack_policy: self.ack_policy.to_async_nats(),
            ack_wait: Duration::from_millis(self.ack_wait_ms),
            max_deliver: self.max_deliver,
            replay_policy: self.replay_policy.to_async_nats(),
            rate_limit: self.rate_limit_bps,
            sample_frequency: self.sample_frequency,
            max_waiting: self.max_waiting,
            max_ack_pending: self.max_ack_pending,
            headers_only: self.headers_only,
            max_batch: self.max_batch,
            max_bytes: self.max_bytes,
            max_expires: Duration::from_millis(self.max_expires_ms),
            inactive_threshold: Duration::from_millis(self.inactive_threshold_ms),
            num_replicas: self.num_replicas,
            memory_storage: self.memory_storage,
            backoff: self
                .backoff_ms
                .iter()
                .copied()
                .map(Duration::from_millis)
                .collect(),
            ..Default::default()
        };
        // `filter_subjects` supersedes the singular `filter_subject`, so the
        // plural one is what this connector sets; both exist because
        // `server_2_10` is on.
        config.filter_subjects = self.filter_subjects.clone();
        config.metadata = self
            .metadata
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<HashMap<_, _>>();
        Ok(config)
    }
}

// ------------------------------------------------------------ stream provision

/// Whether and how SACI creates the JetStream stream it reads or writes.
///
/// `create` defaults to `true`, matching the Kafka connector's `TopicProvision`:
/// a stream that does not exist yet is created on first use. Set it to `false`
/// to require one that already exists.
///
/// An empty `subjects` is filled from what the half already declares: the sink's
/// own `subject`, the source's `filter_subjects`. Those are exactly the subjects
/// that half writes or reads, so the created stream captures them rather than a
/// guess. With both empty the server's own default applies, which is the stream
/// name as its sole subject.
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StreamProvision {
    /// Create the stream when it does not exist. On by default.
    #[serde(default = "default_true")]
    pub create: bool,
    /// Subjects the stream captures. Derived from the half's own subjects when
    /// empty.
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub subjects: Vec<String>,
    /// When a message becomes removable.
    #[serde(default)]
    pub retention: RetentionConfig,
    /// Where the stream keeps its messages.
    #[serde(default)]
    pub storage: StorageConfig,
    /// What happens once a limit is hit.
    #[serde(default)]
    pub discard: DiscardConfig,
    /// Message compression.
    #[serde(default)]
    pub compression: CompressionConfig,
    /// Replicas per message in a clustered JetStream, 1 to 5.
    #[serde(default = "default_replicas")]
    pub num_replicas: usize,
    /// Messages the stream holds before `discard` applies. -1 is unlimited.
    #[serde(default = "default_unlimited_i64")]
    pub max_messages: i64,
    /// Messages per subject. -1 is unlimited.
    #[serde(default = "default_unlimited_i64")]
    pub max_messages_per_subject: i64,
    /// Bytes the stream holds before `discard` applies. -1 is unlimited.
    #[serde(default = "default_unlimited_i64")]
    pub max_bytes: i64,
    /// Largest message the stream accepts. -1 is unlimited.
    #[serde(default = "default_unlimited_i32")]
    pub max_message_size: i32,
    /// Consumers the stream allows. -1 is unlimited.
    #[serde(default = "default_unlimited_i32")]
    pub max_consumers: i32,
    /// How long a message survives. 0 is no age limit.
    #[serde(default)]
    pub max_age_ms: u64,
    /// Window in which a repeated `Nats-Msg-Id` is dropped. 0 keeps the server
    /// default.
    #[serde(default)]
    pub duplicate_window_ms: u64,
    /// Allow rollup headers, which replace a subject's history.
    #[serde(default)]
    pub allow_rollup: bool,
    /// Refuse message deletes.
    #[serde(default)]
    pub deny_delete: bool,
    /// Refuse purges.
    #[serde(default)]
    pub deny_purge: bool,
    /// Allow direct gets, which a non-leader replica can serve.
    #[serde(default)]
    pub allow_direct: bool,
    /// Allow atomic batch publishes, which `mode.atomic_batch` needs. Server
    /// 2.12+.
    #[serde(default)]
    pub allow_atomic: bool,
    /// A short description of the stream's purpose.
    #[serde(default)]
    pub description: Option<String>,
    /// Additional stream metadata.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

// `Default` must agree with the serde defaults above: omitting the whole
// `stream_provision` node uses `Default`, and that is what makes an
// unlimited field -1 rather than the 0 the client's own `Config::default`
// writes.
impl Default for StreamProvision {
    fn default() -> Self {
        Self {
            create: true,
            subjects: Vec::new(),
            retention: RetentionConfig::default(),
            storage: StorageConfig::default(),
            discard: DiscardConfig::default(),
            compression: CompressionConfig::default(),
            num_replicas: default_replicas(),
            max_messages: default_unlimited_i64(),
            max_messages_per_subject: default_unlimited_i64(),
            max_bytes: default_unlimited_i64(),
            max_message_size: default_unlimited_i32(),
            max_consumers: default_unlimited_i32(),
            max_age_ms: 0,
            duplicate_window_ms: 0,
            allow_rollup: false,
            deny_delete: false,
            deny_purge: false,
            allow_direct: false,
            allow_atomic: false,
            description: None,
            metadata: BTreeMap::new(),
        }
    }
}

/// When a JetStream message becomes removable.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RetentionConfig {
    /// Once a message, byte or age limit is reached.
    #[default]
    Limits,
    /// Once every known consumer has acknowledged it.
    Interest,
    /// Once the first consumer has acknowledged it.
    WorkQueue,
}

/// Where a JetStream stream keeps its messages.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StorageConfig {
    /// On disk.
    #[default]
    File,
    /// In memory only.
    Memory,
}

/// What a JetStream stream does once a limit is hit.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DiscardConfig {
    /// Drop the oldest messages to make room.
    #[default]
    Old,
    /// Refuse the new message.
    New,
}

/// JetStream message compression.
#[derive(Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompressionConfig {
    /// Store messages as published.
    #[default]
    None,
    /// S2 block compression.
    S2,
}

impl StreamProvision {
    fn validate(&self, what: &str) -> Result<(), SaciError> {
        if !(1..=5).contains(&self.num_replicas) {
            return Err(SaciError::configuration(format!(
                "{what} config: 'mode.stream_provision.num_replicas' must be within 1..=5"
            )));
        }
        Ok(())
    }

    /// The stream configuration this table describes, under `name`.
    ///
    /// `fallback_subjects` is what the calling half declares it writes or reads,
    /// used when `subjects` is empty. Passing an empty slice leaves the subject
    /// list empty, and the server then captures the stream name alone.
    ///
    /// Every limit is written explicitly: the client's `Config` derives
    /// `Default`, which zeroes them, while the wire meaning of unlimited is -1.
    pub fn to_stream_config(
        &self,
        name: &str,
        fallback_subjects: &[String],
    ) -> async_nats::jetstream::stream::Config {
        async_nats::jetstream::stream::Config {
            name: name.to_string(),
            subjects: if self.subjects.is_empty() {
                fallback_subjects.to_vec()
            } else {
                self.subjects.clone()
            },
            retention: match self.retention {
                RetentionConfig::Limits => RetentionPolicy::Limits,
                RetentionConfig::Interest => RetentionPolicy::Interest,
                RetentionConfig::WorkQueue => RetentionPolicy::WorkQueue,
            },
            storage: match self.storage {
                StorageConfig::File => StorageType::File,
                StorageConfig::Memory => StorageType::Memory,
            },
            discard: match self.discard {
                DiscardConfig::Old => DiscardPolicy::Old,
                DiscardConfig::New => DiscardPolicy::New,
            },
            compression: Some(match self.compression {
                CompressionConfig::None => Compression::None,
                CompressionConfig::S2 => Compression::S2,
            }),
            num_replicas: self.num_replicas,
            max_messages: self.max_messages,
            max_messages_per_subject: self.max_messages_per_subject,
            max_bytes: self.max_bytes,
            max_message_size: self.max_message_size,
            max_consumers: self.max_consumers,
            max_age: Duration::from_millis(self.max_age_ms),
            duplicate_window: Duration::from_millis(self.duplicate_window_ms),
            allow_rollup: self.allow_rollup,
            deny_delete: self.deny_delete,
            deny_purge: self.deny_purge,
            allow_direct: self.allow_direct,
            allow_atomic_publish: self.allow_atomic,
            description: self.description.clone(),
            metadata: self
                .metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<HashMap<_, _>>(),
            // Mirrors, sources, republish, subject transforms and placement are
            // stream topologies rather than connector settings, so they keep
            // the client's own defaults.
            ..Default::default()
        }
    }
}

// ---------------------------------------------------------------------- sink

/// Configuration for [`NatsSink`](crate::NatsSink).
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct NatsSinkConfig {
    /// Connection, auth and TLS.
    pub connection: ConnectionConfig,
    /// Core subject publish or JetStream publish with acks.
    pub mode: SinkMode,
    /// See [`NatsSourceConfig::schema_fields`].
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub schema_fields: Vec<ConfigValue>,
}

/// The write strategy, chosen by `kind`.
#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SinkMode {
    /// Plain subject publish. No persistence, no per-message ack.
    Core(CoreSinkMode),
    /// JetStream publish, acknowledged by the stream.
    ///
    /// Boxed for the same reason as [`SourceMode::Jetstream`].
    Jetstream(Box<JetstreamSinkMode>),
}

/// A core NATS subject publisher.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct CoreSinkMode {
    /// Subject to publish to, and the fallback when `subject_field` renders
    /// null.
    pub subject: String,
    /// Column whose rendered value is the subject for that row. Row-per-message
    /// formats only.
    #[serde(default)]
    pub subject_field: Option<String>,
    /// Headers set on every message.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Header name to column name. Row-per-message formats only.
    #[serde(default)]
    pub header_fields: BTreeMap<String, String>,
    /// Reply subject set on every message.
    #[serde(default)]
    pub reply_subject: Option<String>,
    /// How long a flush waits for the server to acknowledge the write.
    #[serde(default = "default_flush_timeout_ms")]
    pub flush_timeout_ms: u64,
    /// Flush at the end of every `write_batch`, making it a durability
    /// boundary for a protocol with no per-message ack.
    #[serde(default = "default_true")]
    pub flush_every_batch: bool,
}

// `Default` must agree with the serde defaults above; see
// [`JetstreamSourceMode`]'s own note.
impl Default for CoreSinkMode {
    fn default() -> Self {
        Self {
            subject: String::new(),
            subject_field: None,
            headers: BTreeMap::new(),
            header_fields: BTreeMap::new(),
            reply_subject: None,
            flush_timeout_ms: default_flush_timeout_ms(),
            flush_every_batch: true,
        }
    }
}

/// A JetStream publisher.
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct JetstreamSinkMode {
    /// Stream this sink writes into. Named rather than inferred from the
    /// subject because `stream_provision` needs a name to create, and fetching
    /// it once at startup turns a stream typo into an error instead of a
    /// `no responders` publish failure per batch.
    pub stream: String,
    /// Subject to publish to, and the fallback when `subject_field` renders
    /// null. It must be one `stream` captures.
    pub subject: String,
    /// Column whose rendered value is the subject for that row. Row-per-message
    /// formats only.
    #[serde(default)]
    pub subject_field: Option<String>,
    /// Headers set on every message.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Header name to column name. Row-per-message formats only.
    #[serde(default)]
    pub header_fields: BTreeMap<String, String>,
    /// Column whose rendered value becomes `Nats-Msg-Id`, which the stream's
    /// duplicate window deduplicates on. Row-per-message formats only.
    #[serde(default)]
    pub message_id_field: Option<String>,
    /// Send `Nats-Expected-Stream: {stream}`, so a publish that would land in
    /// another stream is refused.
    #[serde(default)]
    pub expected_stream: bool,
    /// Publish a multi-message batch as one JetStream atomic batch
    /// (all-or-nothing) through the `Nats-Batch-*` headers. Needs the stream to
    /// allow atomic publishes, which `stream_provision.allow_atomic` grants
    /// when this node provisions it, and server 2.12+.
    #[serde(default)]
    pub atomic_batch: bool,
    /// Send `Nats-Expected-Last-Sequence` on the first message of every batch,
    /// so a publish that would reorder the stream against another writer is
    /// refused. Requires `atomic_batch`, and makes this sink the stream's only
    /// writer.
    #[serde(default)]
    pub expected_last_sequence: bool,
    /// Timeout on each JetStream API request.
    #[serde(default = "default_js_timeout_ms")]
    pub api_timeout_ms: u64,
    /// How long the client waits for an ack whose future was orphaned by a
    /// mid-batch error.
    #[serde(default = "default_ack_timeout_ms")]
    pub ack_timeout_ms: u64,
    /// Publishes allowed in flight without an ack.
    #[serde(default = "default_max_ack_inflight")]
    pub max_ack_inflight: usize,
    /// Wait for an in-flight slot instead of erroring when `max_ack_inflight`
    /// is reached.
    #[serde(default = "default_true")]
    pub backpressure_on_inflight: bool,
    /// JetStream domain. Sets the API prefix to `$JS.{domain}.API`.
    #[serde(default)]
    pub domain: Option<String>,
    /// JetStream API prefix. Defaults to `$JS.API`.
    #[serde(default)]
    pub api_prefix: Option<String>,
    /// Whether and how SACI creates the stream.
    #[serde(default)]
    pub stream_provision: StreamProvision,
}

// `Default` must agree with the serde defaults above; see
// [`JetstreamSourceMode`]'s own note.
impl Default for JetstreamSinkMode {
    fn default() -> Self {
        Self {
            stream: String::new(),
            subject: String::new(),
            subject_field: None,
            headers: BTreeMap::new(),
            header_fields: BTreeMap::new(),
            message_id_field: None,
            expected_stream: false,
            atomic_batch: false,
            expected_last_sequence: false,
            api_timeout_ms: default_js_timeout_ms(),
            ack_timeout_ms: default_ack_timeout_ms(),
            max_ack_inflight: default_max_ack_inflight(),
            backpressure_on_inflight: true,
            domain: None,
            api_prefix: None,
            stream_provision: StreamProvision::default(),
        }
    }
}

impl NatsSinkConfig {
    /// Check every cross-field invariant this config must satisfy.
    ///
    /// Whether `subject_field`, `header_fields` and `message_id_field` are
    /// honourable depends on the resolved transformer's message shape, which
    /// only [`NatsSink::new`](crate::NatsSink::new) knows, so those checks live
    /// there.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] naming the offending key on the
    /// first violation.
    pub fn validate(&self) -> Result<(), SaciError> {
        let what = "NatsSink";
        self.connection.validate(what)?;

        let (subject, headers, header_fields) = match &self.mode {
            SinkMode::Core(core) => (&core.subject, &core.headers, &core.header_fields),
            SinkMode::Jetstream(js) => (&js.subject, &js.headers, &js.header_fields),
        };
        if subject.trim().is_empty() {
            return Err(SaciError::configuration(format!(
                "{what} config: 'mode.subject' must not be empty"
            )));
        }
        // `HeaderMap::insert` panics on an illegal name or value, so both are
        // checked here rather than at publish time.
        for (name, value) in headers {
            validate_header_name(what, "mode.headers", name)?;
            HeaderValue::from_str(value).map_err(|e| {
                SaciError::configuration(format!(
                    "{what} config: 'mode.headers' value for '{name}' is not a legal NATS header \
                     value: {e}"
                ))
            })?;
        }
        for name in header_fields.keys() {
            validate_header_name(what, "mode.header_fields", name)?;
        }

        if let SinkMode::Jetstream(js) = &self.mode {
            validate_js_api(what, js.domain.as_deref(), js.api_prefix.as_deref())?;
            if js.stream.trim().is_empty() {
                return Err(SaciError::configuration(format!(
                    "{what} config: 'mode.stream' must not be empty"
                )));
            }
            if js.max_ack_inflight == 0 {
                return Err(SaciError::configuration(format!(
                    "{what} config: 'mode.max_ack_inflight' must be at least 1"
                )));
            }
            if js.expected_last_sequence && !js.atomic_batch {
                return Err(SaciError::configuration(format!(
                    "{what} config: 'mode.expected_last_sequence' needs 'mode.atomic_batch #true'"
                )));
            }
            if js.atomic_batch && js.stream_provision.create && !js.stream_provision.allow_atomic {
                return Err(SaciError::configuration(format!(
                    "{what} config: 'mode.atomic_batch' needs 'stream_provision.allow_atomic \
                     #true' when the stream is provisioned"
                )));
            }
            js.stream_provision.validate(what)?;
        }
        Ok(())
    }
}

fn validate_header_name(what: &str, key: &str, name: &str) -> Result<(), SaciError> {
    HeaderName::from_str(name)
        .map_err(|e| {
            SaciError::configuration(format!(
                "{what} config: '{key}' name '{name}' is not a legal NATS header name: {e}"
            ))
        })
        .map(drop)
}

/// `domain` and `api_prefix` are two ways to write the same prefix, so at most
/// one may be set.
fn validate_js_api(
    what: &str,
    domain: Option<&str>,
    api_prefix: Option<&str>,
) -> Result<(), SaciError> {
    if domain.is_some() && api_prefix.is_some() {
        return Err(SaciError::configuration(format!(
            "{what} config: 'mode.domain' and 'mode.api_prefix' are two ways to say the same \
             thing; set one"
        )));
    }
    Ok(())
}

fn default_true() -> bool {
    true
}
fn default_connect_timeout_ms() -> u64 {
    5_000
}
fn default_request_timeout_ms() -> u64 {
    10_000
}
fn default_ping_interval_ms() -> u64 {
    60_000
}
fn default_subscription_capacity() -> usize {
    65_536
}
fn default_client_capacity() -> usize {
    2_048
}
fn default_read_buffer_capacity() -> u16 {
    65_535
}
fn default_batch_size() -> usize {
    1_000
}
fn default_poll_timeout_ms() -> u64 {
    1_000
}
/// Five delivery attempts before the source retires a message it cannot
/// decode: enough for a redelivery caused by anything other than the payload
/// (a restart, a lost ack, a window nobody read) to pass, and bounded, so a
/// payload that will never decode costs at most five `ack_wait` windows
/// rather than the stream's whole life.
fn default_max_decode_attempts() -> u32 {
    5
}
fn default_fetch_expires_ms() -> u64 {
    5_000
}
fn default_js_timeout_ms() -> u64 {
    5_000
}
fn default_ack_timeout_ms() -> u64 {
    30_000
}
fn default_max_ack_inflight() -> usize {
    5_000
}
fn default_flush_timeout_ms() -> u64 {
    30_000
}
fn default_replicas() -> usize {
    1
}
fn default_unlimited_i64() -> i64 {
    -1
}
fn default_unlimited_i32() -> i32 {
    -1
}

#[cfg(test)]
mod tests {
    use super::*;

    use saci_connector::from_kdl_str;

    const CONNECTION: &str = "connection {\n    servers \"nats://localhost:4222\"\n}\n";

    /// Deserialize a KDL config fragment, keeping the deserializer's own error
    /// so a test can assert on the key it names.
    fn parse<T: serde::de::DeserializeOwned>(raw: &str) -> Result<T, String> {
        T::deserialize(from_kdl_str(raw).expect("parse kdl")).map_err(|e| e.to_string())
    }

    /// `top` holds top-level nodes; `mode` holds the whole `mode` node.
    fn source(top: &str, mode: &str) -> NatsSourceConfig {
        parse(&format!("{top}\n{CONNECTION}{mode}")).unwrap_or_else(|e| panic!("parse: {e}"))
    }

    fn sink(mode: &str) -> NatsSinkConfig {
        parse(&format!("{CONNECTION}{mode}")).unwrap_or_else(|e| panic!("parse: {e}"))
    }

    fn core_source(top: &str) -> NatsSourceConfig {
        source(top, "mode kind=\"core\" subject=\"orders\"\n")
    }

    /// `extra` holds child nodes of the `mode` node.
    fn js_source(extra: &str) -> NatsSourceConfig {
        source(
            "",
            &format!("mode kind=\"jetstream\" stream=\"ORDERS\" {{\n{extra}\n}}\n"),
        )
    }

    fn core_sink(extra: &str) -> NatsSinkConfig {
        sink(&format!(
            "mode kind=\"core\" subject=\"orders\" {{\n{extra}\n}}\n"
        ))
    }

    #[test]
    fn a_full_source_config_parses_every_key() {
        let raw = r#"
batch_size 64
poll_timeout_ms 250
stop_at_end #true

connection {
    servers "nats://a:4222" "b:4223"
    name "saci"
    connect_timeout_ms 1000
    request_timeout_ms 0
    ping_interval_ms 30000
    max_reconnects 7
    reconnect_delay_ms 500
    retry_on_initial_connect #false
    subscription_capacity 128
    client_capacity 64
    read_buffer_capacity 4096
    no_echo #true
    inbox_prefix "_SACI"
    ignore_discovered_servers #true
    retain_servers_order #true
    auth kind="credentials" path="/run/secrets/saci.creds"
    tls {
        require #true
        tls_first #true
        root_certificates "/etc/ssl/roots.pem"
        client_certificate "/etc/ssl/saci.pem"
        client_key "/etc/ssl/saci.key"
    }
}

mode kind="jetstream" stream="ORDERS" {
    durable_name "saci"
    consumer_name "saci"
    description "orders into saci"
    filter_subjects "orders.new" "orders.amended"
    ack_policy "all"
    double_ack #true
    ack_wait_ms 15000
    max_deliver 5
    max_decode_attempts 3
    max_ack_pending 200
    max_waiting 32
    max_batch 500
    max_bytes 1048576
    max_expires_ms 60000
    inactive_threshold_ms 300000
    num_replicas 1
    memory_storage #true
    headers_only #false
    rate_limit_bps 1000000
    sample_frequency 10
    replay_policy "original"
    backoff_ms 1000 5000
    fetch_expires_ms 2000
    fetch_max_bytes 65536
    heartbeat_ms 500
    domain "hub"
    api_timeout_ms 3000
    deliver_policy kind="by_start_sequence" start_sequence=42
    metadata owner="saci"
    stream_provision {
        create #true
        subjects "orders.>"
        retention "work_queue"
        storage "memory"
        discard "new"
        compression "s2"
        num_replicas 3
        max_messages 1000
        max_messages_per_subject 10
        max_bytes 2048
        max_message_size 4096
        max_consumers 8
        max_age_ms 86400000
        duplicate_window_ms 120000
        allow_rollup #true
        deny_delete #true
        deny_purge #true
        allow_direct #true
        description "orders"
        metadata team="data"
    }
}

schema_fields "id" type="int64"
"#;
        let cfg: NatsSourceConfig = parse(raw).unwrap_or_else(|e| panic!("parse: {e}"));
        cfg.validate().expect("a fully specified config is valid");
        assert_eq!(cfg.connection.servers.len(), 2);
        assert_eq!(cfg.connection.request_timeout_ms, 0);
        let SourceMode::Jetstream(js) = &cfg.mode else {
            panic!("kind=\"jetstream\" must select the JetStream mode");
        };
        assert_eq!(js.ack_policy, AckPolicyConfig::All);
        assert_eq!(js.replay_policy, ReplayPolicyConfig::Original);
        assert_eq!(js.max_decode_attempts, 3);
        assert_eq!(js.stream_provision.retention, RetentionConfig::WorkQueue);
        assert_eq!(js.stream_provision.compression, CompressionConfig::S2);
        assert_eq!(
            js.deliver_policy,
            DeliverPolicyConfig::ByStartSequence { start_sequence: 42 }
        );
        assert_eq!(js.metadata.get("owner").map(String::as_str), Some("saci"));
        // Written once, so a single value in the tree.
        assert_eq!(js.stream_provision.subjects, vec!["orders.>"]);
        assert_eq!(cfg.schema_fields.len(), 1);
    }

    #[test]
    fn a_full_sink_config_parses_every_key() {
        let raw = r#"
connection {
    servers "nats://localhost:4222"
    auth kind="user_password" user="saci" password_file="/run/secrets/pw"
}

mode kind="jetstream" stream="ORDERS" subject="orders.enriched" {
    subject_field "route"
    message_id_field "id"
    expected_stream #true
    atomic_batch #true
    expected_last_sequence #true
    api_timeout_ms 2000
    ack_timeout_ms 4000
    max_ack_inflight 100
    backpressure_on_inflight #false
    api_prefix "MY.JS.API"
    headers "X-Producer"="saci"
    header_fields "X-Route"="route"
    stream_provision create=#true allow_atomic=#true subjects="orders.enriched"
}
"#;
        let cfg: NatsSinkConfig = parse(raw).unwrap_or_else(|e| panic!("parse: {e}"));
        cfg.validate().expect("a fully specified config is valid");
        let SinkMode::Jetstream(js) = &cfg.mode else {
            panic!("kind=\"jetstream\" must select the JetStream mode");
        };
        assert!(js.expected_stream);
        assert!(js.atomic_batch);
        assert!(js.expected_last_sequence);
        assert!(js.stream_provision.allow_atomic);
        assert_eq!(js.max_ack_inflight, 100);
        assert_eq!(cfg.connection.servers, vec!["nats://localhost:4222"]);
        assert_eq!(js.stream_provision.subjects, vec!["orders.enriched"]);
        assert_eq!(
            js.header_fields.get("X-Route").map(String::as_str),
            Some("route")
        );
    }

    #[test]
    fn an_unknown_key_is_rejected() {
        let err = parse::<NatsSourceConfig>(
            "typo 1\nconnection {\n    servers \"x\"\n}\nmode kind=\"core\" subject=\"s\"\n",
        )
        .expect_err("deny_unknown_fields must reject an unknown top-level key");
        assert!(err.contains("typo"), "got: {err}");

        let err = parse::<NatsSourceConfig>(
            "connection {\n    servers \"x\"\n    nope 1\n}\nmode kind=\"core\" subject=\"s\"\n",
        )
        .expect_err("deny_unknown_fields must reject an unknown connection key");
        assert!(err.contains("nope"), "got: {err}");

        let err = parse::<NatsSourceConfig>(
            "connection {\n    servers \"x\"\n}\nmode kind=\"core\" subject=\"s\" nope=1\n",
        )
        .expect_err("deny_unknown_fields must reject an unknown mode key");
        assert!(err.contains("nope"), "got: {err}");
    }

    #[test]
    fn an_empty_server_list_is_rejected() {
        let cfg = NatsSourceConfig {
            connection: ConnectionConfig::default(),
            ..core_source("")
        };
        let err = cfg.validate().expect_err("servers is required");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("connection.servers"), "got: {err}");
    }

    #[test]
    fn a_server_url_with_a_foreign_scheme_names_itself() {
        let cfg = NatsSourceConfig {
            connection: ConnectionConfig {
                servers: vec!["http://localhost:4222".to_string()],
                ..ConnectionConfig::default()
            },
            ..core_source("")
        };
        let err = cfg.validate().expect_err("http is not a NATS scheme");
        assert!(
            err.message().contains("http://localhost:4222"),
            "got: {err}"
        );
    }

    #[test]
    fn a_bare_host_and_port_is_a_valid_server() {
        core_source("")
            .validate()
            .expect("the default server list parses");
        let cfg = NatsSourceConfig {
            connection: ConnectionConfig {
                servers: vec!["localhost:4222".to_string()],
                ..ConnectionConfig::default()
            },
            ..core_source("")
        };
        cfg.validate().expect("a bare host:port means nats://");
    }

    #[test]
    fn every_zero_capacity_and_interval_is_rejected_by_name() {
        for key in [
            "ping_interval_ms",
            "connect_timeout_ms",
            "read_buffer_capacity",
            "subscription_capacity",
            "client_capacity",
        ] {
            let raw = format!(
                "connection {{\n    servers \"localhost:4222\"\n    {key} 0\n}}\n\
                 mode kind=\"core\" subject=\"s\"\n"
            );
            let cfg: NatsSourceConfig = parse(&raw).unwrap_or_else(|e| panic!("parse {key}: {e}"));
            let err = cfg.validate().expect_err("zero must be rejected");
            assert!(
                err.message().contains(&format!("connection.{key}")),
                "expected {key}, got: {err}"
            );
        }
    }

    #[test]
    fn every_auth_kind_round_trips_and_needs_exactly_one_secret() {
        let cases = [
            ("kind=\"none\"", true),
            ("kind=\"token\" token=\"t\"", true),
            ("kind=\"token\" token_file=\"/t\"", true),
            ("kind=\"token\"", false),
            ("kind=\"token\" token=\"t\" token_file=\"/t\"", false),
            ("kind=\"user_password\" user=\"u\" password=\"p\"", true),
            ("kind=\"user_password\" user=\"u\"", false),
            ("kind=\"nkey\" seed=\"s\"", true),
            ("kind=\"nkey\" seed_file=\"/s\"", true),
            ("kind=\"nkey\"", false),
            ("kind=\"credentials\" path=\"/c\"", true),
        ];
        for (auth, valid) in cases {
            let raw = format!(
                "connection {{\n    servers \"localhost:4222\"\n    auth {auth}\n}}\n\
                 mode kind=\"core\" subject=\"s\"\n"
            );
            let cfg: NatsSourceConfig =
                parse(&raw).unwrap_or_else(|e| panic!("parse {auth:?}: {e}"));
            assert_eq!(
                cfg.validate().is_ok(),
                valid,
                "{auth:?} should be {}",
                if valid { "accepted" } else { "rejected" }
            );
        }
    }

    #[test]
    fn an_absent_auth_table_means_no_auth() {
        assert_eq!(core_source("").connection.auth, AuthConfig::None);
    }

    #[test]
    fn half_a_client_certificate_is_rejected() {
        let cfg = NatsSourceConfig {
            connection: ConnectionConfig {
                servers: vec!["localhost:4222".to_string()],
                tls: TlsConfig {
                    client_certificate: Some("/c.pem".to_string()),
                    ..TlsConfig::default()
                },
                ..ConnectionConfig::default()
            },
            ..core_source("")
        };
        let err = cfg.validate().expect_err("a certificate needs its key");
        assert!(err.message().contains("client_key"), "got: {err}");
    }

    #[test]
    fn a_zero_batch_size_or_poll_timeout_is_rejected() {
        let err = core_source("batch_size 0")
            .validate()
            .expect_err("a zero batch collects nothing");
        assert!(err.message().contains("'batch_size'"), "got: {err}");

        let err = core_source("poll_timeout_ms 0")
            .validate()
            .expect_err("a zero window never opens");
        assert!(err.message().contains("'poll_timeout_ms'"), "got: {err}");
    }

    #[test]
    fn an_empty_element_in_a_subject_list_is_rejected() {
        let cfg = source("", "mode kind=\"core\" subject=\"a,,b\"\n");
        let err = cfg
            .validate()
            .expect_err("an empty subject is not a subject");
        assert!(err.message().contains("'mode.subject'"), "got: {err}");
    }

    #[test]
    fn an_empty_stream_name_is_rejected() {
        let cfg = source("", "mode kind=\"jetstream\" stream=\"  \"\n");
        let err = cfg.validate().expect_err("a stream needs a name");
        assert!(err.message().contains("'mode.stream'"), "got: {err}");
    }

    #[test]
    fn a_domain_and_an_api_prefix_together_are_rejected() {
        let cfg = js_source("domain \"hub\"\napi_prefix \"MY.JS.API\"");
        let err = cfg.validate().expect_err("they name the same thing");
        assert!(err.message().contains("'mode.domain'"), "got: {err}");
    }

    #[test]
    fn a_zero_fetch_expiry_is_rejected() {
        let cfg = js_source("fetch_expires_ms 0");
        let err = cfg.validate().expect_err("a zero expiry never opens");
        assert!(
            err.message().contains("'mode.fetch_expires_ms'"),
            "got: {err}"
        );
    }

    #[test]
    fn double_ack_with_ack_policy_none_is_rejected() {
        let cfg = js_source("ack_policy \"none\"\ndouble_ack #true");
        let err = cfg
            .validate()
            .expect_err("there is nothing to confirm twice");
        assert!(err.message().contains("'mode.double_ack'"), "got: {err}");
    }

    #[test]
    fn a_replica_count_outside_one_to_five_is_rejected() {
        for replicas in ["0", "6"] {
            let cfg = js_source(&format!("stream_provision num_replicas={replicas}"));
            let err = cfg.validate().expect_err("JetStream allows 1 to 5");
            assert!(
                err.message()
                    .contains("'mode.stream_provision.num_replicas'"),
                "got: {err}"
            );
        }
    }

    #[test]
    fn a_malformed_start_time_is_rejected_and_mapped() {
        let cfg = js_source("deliver_policy kind=\"by_start_time\" start_time=\"yesterday\"");
        let err = cfg.validate().expect_err("that is not RFC 3339");
        assert!(
            err.message().contains("'mode.deliver_policy.start_time'"),
            "got: {err}"
        );

        let policy = DeliverPolicyConfig::ByStartTime {
            start_time: "yesterday".to_string(),
        };
        assert!(policy.to_async_nats().is_err());
    }

    #[test]
    fn every_deliver_policy_variant_maps() {
        let cases = [
            (DeliverPolicyConfig::All, DeliverPolicy::All),
            (DeliverPolicyConfig::Last, DeliverPolicy::Last),
            (DeliverPolicyConfig::New, DeliverPolicy::New),
            (
                DeliverPolicyConfig::LastPerSubject,
                DeliverPolicy::LastPerSubject,
            ),
            (
                DeliverPolicyConfig::ByStartSequence { start_sequence: 9 },
                DeliverPolicy::ByStartSequence { start_sequence: 9 },
            ),
        ];
        for (config, expected) in cases {
            assert_eq!(config.to_async_nats().expect("maps"), expected);
        }
        let mapped = DeliverPolicyConfig::ByStartTime {
            start_time: "2024-01-02T03:04:05Z".to_string(),
        }
        .to_async_nats()
        .expect("RFC 3339 parses");
        assert!(matches!(mapped, DeliverPolicy::ByStartTime { .. }));
    }

    #[test]
    fn a_consumer_config_carries_the_plural_subject_filter() {
        let js = js_source("filter_subjects \"a.b\" \"a.c\"\ndurable_name \"saci\"");
        let SourceMode::Jetstream(js) = &js.mode else {
            panic!("jetstream mode");
        };
        let consumer = js.to_consumer_config().expect("valid");
        assert_eq!(consumer.filter_subject, "");
        assert_eq!(consumer.filter_subjects, vec!["a.b", "a.c"]);
        // An absent `consumer_name` follows `durable_name`, so a durable
        // consumer is addressable by one name.
        assert_eq!(consumer.name.as_deref(), Some("saci"));
        assert_eq!(consumer.durable_name.as_deref(), Some("saci"));

        // A list written once is a single value in the tree, which
        // `saci_connector::one_or_many` accepts as a one-element list.
        let once = js_source("filter_subjects \"a.b\"\nbackoff_ms 250");
        let SourceMode::Jetstream(once) = &once.mode else {
            panic!("jetstream mode");
        };
        assert_eq!(once.filter_subjects, vec!["a.b"]);
        assert_eq!(once.backoff_ms, vec![250]);
    }

    #[test]
    fn a_stream_is_created_by_default() {
        let cfg = js_source("");
        let SourceMode::Jetstream(js) = &cfg.mode else {
            panic!("jetstream mode");
        };
        assert!(
            js.stream_provision.create,
            "omitting the node must still create the stream"
        );
    }

    #[test]
    fn stream_provision_writes_minus_one_for_every_unlimited_field() {
        let config = StreamProvision::default().to_stream_config("ORDERS", &[]);
        assert_eq!(config.name, "ORDERS");
        assert_eq!(config.max_messages, -1);
        assert_eq!(config.max_messages_per_subject, -1);
        assert_eq!(config.max_bytes, -1);
        assert_eq!(config.max_message_size, -1);
        assert_eq!(config.max_consumers, -1);
        assert_eq!(config.num_replicas, 1);
        assert_eq!(config.compression, Some(Compression::None));
        assert_eq!(config.max_age, Duration::ZERO);
    }

    #[test]
    fn an_empty_subject_list_falls_back_to_what_the_half_declares() {
        let fallback = vec!["orders.enriched".to_string()];
        let derived = StreamProvision::default().to_stream_config("ORDERS", &fallback);
        assert_eq!(derived.subjects, fallback);

        // An explicit list wins, so a stream can capture more than the one
        // subject this half touches.
        let explicit = StreamProvision {
            subjects: vec!["orders.>".to_string()],
            ..StreamProvision::default()
        }
        .to_stream_config("ORDERS", &fallback);
        assert_eq!(explicit.subjects, vec!["orders.>"]);

        // Neither: the server captures the stream name alone.
        assert!(
            StreamProvision::default()
                .to_stream_config("ORDERS", &[])
                .subjects
                .is_empty()
        );
    }

    /// Every hand-written `Default` must describe the same thing the serde
    /// defaults do, or a config built in Rust and a parsed one disagree.
    #[test]
    fn the_hand_written_defaults_match_the_serde_defaults() {
        let parsed = js_source("stream_provision num_replicas=1");
        let SourceMode::Jetstream(js) = &parsed.mode else {
            panic!("jetstream mode");
        };
        assert_eq!(js.stream_provision, StreamProvision::default());
        let expected = JetstreamSourceMode {
            stream: "ORDERS".to_string(),
            ..JetstreamSourceMode::default()
        };
        assert_eq!(
            js.to_consumer_config().expect("maps"),
            expected.to_consumer_config().expect("maps")
        );
        assert_eq!(js.fetch_expires_ms, expected.fetch_expires_ms);
        assert_eq!(js.api_timeout_ms, expected.api_timeout_ms);

        let connection: ConnectionConfig =
            parse("servers \"localhost:4222\"").unwrap_or_else(|e| panic!("parse: {e}"));
        assert_eq!(
            connection.connect_timeout_ms,
            ConnectionConfig::default().connect_timeout_ms
        );
        assert_eq!(
            connection.retry_on_initial_connect,
            ConnectionConfig::default().retry_on_initial_connect
        );

        let core = core_sink("");
        let SinkMode::Core(core) = &core.mode else {
            panic!("core mode");
        };
        let core_default = CoreSinkMode::default();
        assert_eq!(core.flush_timeout_ms, core_default.flush_timeout_ms);
        assert_eq!(core.flush_every_batch, core_default.flush_every_batch);

        let js = sink("mode kind=\"jetstream\" stream=\"S\" subject=\"s\"\n");
        let SinkMode::Jetstream(js) = &js.mode else {
            panic!("jetstream mode");
        };
        let js_default = JetstreamSinkMode::default();
        assert_eq!(js.atomic_batch, js_default.atomic_batch);
        assert_eq!(js.expected_last_sequence, js_default.expected_last_sequence);
        assert_eq!(js.ack_timeout_ms, js_default.ack_timeout_ms);
        assert_eq!(js.max_ack_inflight, js_default.max_ack_inflight);
        assert_eq!(
            js.backpressure_on_inflight,
            js_default.backpressure_on_inflight
        );
        assert_eq!(js.api_timeout_ms, js_default.api_timeout_ms);
    }

    #[test]
    fn an_illegal_static_header_name_is_rejected() {
        let cfg = core_sink("headers \"Bad Name\"=\"v\"");
        let err = cfg.validate().expect_err("a space is not legal in a name");
        assert!(err.message().contains("'mode.headers'"), "got: {err}");
    }

    #[test]
    fn an_illegal_static_header_value_is_rejected() {
        let cfg = core_sink("headers \"X-Key\"=\"a\\r\\nPUB x 0\\r\\n\\r\\n\"");
        let err = cfg.validate().expect_err("CRLF would break framing");
        assert!(err.message().contains("'mode.headers'"), "got: {err}");
    }

    #[test]
    fn an_illegal_header_field_name_is_rejected() {
        let cfg = core_sink("header_fields \"Bad:Name\"=\"col\"");
        let err = cfg.validate().expect_err("a colon is not legal in a name");
        assert!(err.message().contains("'mode.header_fields'"), "got: {err}");
    }

    #[test]
    fn an_empty_sink_subject_is_rejected() {
        let cfg = sink("mode kind=\"core\" subject=\" \"\n");
        let err = cfg.validate().expect_err("a publish needs a subject");
        assert!(err.message().contains("'mode.subject'"), "got: {err}");
    }

    #[test]
    fn a_zero_ack_inflight_is_rejected() {
        let cfg = sink("mode kind=\"jetstream\" stream=\"S\" subject=\"s\" max_ack_inflight=0\n");
        let err = cfg.validate().expect_err("a zero window publishes nothing");
        assert!(
            err.message().contains("'mode.max_ack_inflight'"),
            "got: {err}"
        );
    }

    #[test]
    fn a_jetstream_sink_needs_a_stream_name() {
        let cfg = sink("mode kind=\"jetstream\" stream=\"\" subject=\"s\"\n");
        let err = cfg.validate().expect_err("provisioning needs a name");
        assert!(err.message().contains("'mode.stream'"), "got: {err}");
    }

    #[test]
    fn expected_last_sequence_requires_atomic_batch() {
        let cfg = sink(
            "mode kind=\"jetstream\" stream=\"S\" subject=\"s\" expected_last_sequence=#true\n",
        );
        let err = cfg.validate().expect_err("guard requires atomic_batch");
        assert!(
            err.message()
                .contains("'mode.expected_last_sequence' needs 'mode.atomic_batch #true'"),
            "got: {err}"
        );
    }

    #[test]
    fn atomic_batch_requires_allow_atomic_when_provisioning() {
        let cfg = sink("mode kind=\"jetstream\" stream=\"S\" subject=\"s\" atomic_batch=#true\n");
        let err = cfg.validate().expect_err("provisioned stream must opt in");
        assert!(
            err.message().contains(
                "'mode.atomic_batch' needs 'stream_provision.allow_atomic #true' when the stream \
                 is provisioned"
            ),
            "got: {err}"
        );
    }

    #[test]
    fn atomic_batch_is_allowed_on_a_pre_existing_stream() {
        let cfg = sink(
            "mode kind=\"jetstream\" stream=\"S\" subject=\"s\" atomic_batch=#true {\n\
             \x20   stream_provision create=#false\n}\n",
        );
        cfg.validate()
            .expect("a pre-existing stream is the operator's responsibility");
    }
}
