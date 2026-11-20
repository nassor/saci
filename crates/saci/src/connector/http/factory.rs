//! The `http` and `http-listen` connectors.
//!
//! Both resolve their byte format through the codec the host bound
//! to this connector instance via a declared `codec` key. The schema is
//! where they differ: a document mode source hands the format whatever
//! `field` entries say, including nothing, and lets the format decide, while a
//! sink and a listen source always need one, because it is the schema the rows
//! are written with and the one the graph is validated against before a request
//! can arrive.
//!
//! Neither connector makes a request at build time, so `saci-service validate`
//! touches no network. A
//! listen source does bind its sockets, the way the `tcp` and `udp` sources do,
//! so a port already in use is a load-time error rather than a listener that
//! never accepts.

use std::path::PathBuf;
use std::time::Duration;

use crate::connector::{
    ConfigValue, Connector, ConnectorContext, Rebuild, Role, parse_fields, parse_optional_fields,
    schema_of,
};
use crate::error::SaciError;
use crate::io::{Sink, Source};

use crate::connector::SchemaFrom;
use crate::connector::http::sink::{ContentEncoding, HttpSinkConfig};
use crate::connector::http::source::{
    BatchWindow, HttpClientConfig, HttpSourceConfig, Pagination, ReadMode,
};
use crate::connector::http::tls::TlsFiles;
use crate::connector::http::{
    HttpListenConfig, HttpListenSource, HttpSink, HttpSource, HttpVersion, ServerTls,
};

/// Whole-request budget when `timeout_ms` is absent.
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
/// Method the sink sends when `method` is absent.
const DEFAULT_METHOD: &str = "POST";
/// Largest request body a listening source accepts when `max_body_bytes` is absent.
const DEFAULT_LISTEN_BODY_BYTES: i64 = 67_108_864;
/// Documents a listening source queues when `buffer` is absent.
const DEFAULT_LISTEN_BUFFER: i64 = 64;

/// The keys an `HttpSource` config may carry.
const SOURCE_KEYS: &[&str] = &[
    "url",
    "timeout_ms",
    "headers",
    "http_version",
    "tls",
    "max_body_bytes",
    "schema_from",
    "field",
    "pagination",
    "conditional",
    "mode",
];
/// The keys an `HttpSink` config may carry.
const SINK_KEYS: &[&str] = &[
    "url",
    "timeout_ms",
    "headers",
    "http_version",
    "tls",
    "method",
    "content_encoding",
    "field",
];
/// The keys an `HttpListenSource` config may carry.
const LISTEN_KEYS: &[&str] = &[
    "bind",
    "path",
    "methods",
    "max_body_bytes",
    "buffer",
    "tls",
    "quic_bind",
    "auth",
    "schema_from",
    "field",
];

/// Refuse the first key of `config` that `allowed` does not list.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] naming the key: a misspelled optional
/// key read as absent would run the connector with a default the config author
/// never chose.
fn reject_unknown_keys(config: &ConfigValue, allowed: &[&str]) -> Result<(), SaciError> {
    let Some(table) = config.as_object() else {
        return Ok(());
    };
    match table.keys().find(|key| !allowed.contains(&key.as_str())) {
        Some(key) => Err(SaciError::configuration(format!(
            "http: unknown key '{key}'"
        ))),
        None => Ok(()),
    }
}

/// The `url` a connector needs, or a configuration error naming the key.
fn required_url<'a>(config: &'a ConfigValue, what: &str) -> Result<&'a str, SaciError> {
    config.get("url").and_then(|v| v.as_str()).ok_or_else(|| {
        SaciError::configuration(format!("{what} config requires a 'url' string field"))
    })
}

/// The whole-request budget, `timeout_ms` milliseconds.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `timeout_ms` is present and is
/// not an integer of at least one: a zero would mean no budget at all, and a
/// string read as the default would ignore the budget a config author asked
/// for.
fn timeout(config: &ConfigValue, what: &str) -> Result<Duration, SaciError> {
    let ms = positive(config, what, "timeout_ms")?.map_or(DEFAULT_TIMEOUT_MS, |ms| ms as u64);
    Ok(Duration::from_millis(ms))
}

/// Every `(name, value)` pair the `headers` table holds.
///
/// KDL writes it as a nested node, so `headers { authorization "Bearer x" }`
/// and `headers authorization="Bearer x"` both arrive as a table of scalars.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `headers` is not a table, or when a
/// value in it is not a string: a header silently dropped for being the wrong
/// shape is worse than a rejected config.
fn headers(config: &ConfigValue, what: &str) -> Result<Vec<(String, String)>, SaciError> {
    let Some(value) = config.get("headers") else {
        return Ok(Vec::new());
    };
    let table = value.as_object().ok_or_else(|| {
        SaciError::configuration(format!(
            "{what} config.headers must be a table of string values"
        ))
    })?;
    table
        .iter()
        .map(|(name, value)| {
            value
                .as_str()
                .map(|value| (name.clone(), value.to_string()))
                .ok_or_else(|| {
                    SaciError::configuration(format!(
                        "{what} config.headers['{name}'] must be a string"
                    ))
                })
        })
        .collect()
}

/// Where the source takes the schema it hands the format from.
///
/// Absent means [`SchemaFrom::Config`], which is what every schemaless format
/// wants.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `schema_from` is present and is
/// neither `"config"` nor `"payload"`: a misspelling silently read as the default
/// would take the body's own schema as a projection target instead of checking
/// it, in production rather than at load.
fn schema_from(config: &ConfigValue, what: &str) -> Result<SchemaFrom, SaciError> {
    let Some(value) = config.get("schema_from") else {
        return Ok(SchemaFrom::Config);
    };
    match value.as_str() {
        Some("config") => Ok(SchemaFrom::Config),
        Some("payload") => Ok(SchemaFrom::Payload),
        _ => Err(SaciError::configuration(format!(
            "{what} config.schema_from must be \"config\" or \"payload\""
        ))),
    }
}

/// Which HTTP version this half speaks.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `http_version` is present and is
/// not one of the four spellings. A misspelling read as the default would
/// silently downgrade a node a config author asked to speak HTTP/2 or HTTP/3.
fn http_version(config: &ConfigValue, what: &str) -> Result<HttpVersion, SaciError> {
    let Some(value) = config.get("http_version") else {
        return Ok(HttpVersion::Auto);
    };
    match value.as_str() {
        Some("auto") => Ok(HttpVersion::Auto),
        Some("1.1") => Ok(HttpVersion::Http11),
        Some("2") => Ok(HttpVersion::Http2),
        Some("3") => Ok(HttpVersion::Http3),
        _ => Err(SaciError::configuration(format!(
            "{what} config.http_version must be \"auto\", \"1.1\", \"2\" or \"3\""
        ))),
    }
}

/// The `tls` block: `ca_file`, `cert_file`, `key_file`, all optional.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `tls` is not a table or one of its
/// entries is not a string. A path silently dropped for being the wrong shape
/// would leave a connection unauthenticated, which is the one failure mode a
/// `tls` block exists to prevent.
fn tls_files(config: &ConfigValue, what: &str) -> Result<TlsFiles, SaciError> {
    let Some(value) = config.get("tls") else {
        return Ok(TlsFiles::default());
    };
    let table = value
        .as_object()
        .ok_or_else(|| SaciError::configuration(format!("{what} config.tls must be a table")))?;
    let mut files = TlsFiles::default();
    for (key, value) in table {
        let path = value.as_str().ok_or_else(|| {
            SaciError::configuration(format!("{what} config.tls['{key}'] must be a string"))
        })?;
        match key.as_str() {
            "ca_file" => files.ca_file = Some(PathBuf::from(path)),
            "cert_file" => files.cert_file = Some(PathBuf::from(path)),
            "key_file" => files.key_file = Some(PathBuf::from(path)),
            _ => {
                return Err(SaciError::configuration(format!(
                    "{what} config.tls['{key}'] is not a tls key: expected 'ca_file', \
                     'cert_file' or 'key_file'"
                )));
            }
        }
    }
    Ok(files)
}

/// One optional positive integer key.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when the key is present and is not an
/// integer of at least one.
fn positive(config: &ConfigValue, what: &str, key: &str) -> Result<Option<usize>, SaciError> {
    let Some(value) = config.get(key) else {
        return Ok(None);
    };
    match value.as_i64() {
        Some(number) if number >= 1 => Ok(Some(number as usize)),
        _ => Err(SaciError::configuration(format!(
            "{what} config.{key} must be an integer of at least 1"
        ))),
    }
}

/// One optional positive integer inside the `mode` table.
fn mode_positive(
    table: &crate::connector::ConfigMap,
    key: &str,
    default: u64,
) -> Result<u64, SaciError> {
    let Some(value) = table.get(key) else {
        return Ok(default);
    };
    match value.as_i64() {
        Some(number) if number >= 1 => Ok(number as u64),
        _ => Err(SaciError::configuration(format!(
            "HttpSource config.mode['{key}'] must be an integer of at least 1"
        ))),
    }
}

/// The `mode` block, and the `pagination`/`conditional` keys that only apply to
/// its `document` kind.
///
/// `mode kind="lines"` arrives as a table because a KDL node with properties
/// does; the absence of the block is `document` with no pagination.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] for a `mode` that is not a table, a
/// `kind` outside the three names, a non-string `event`, a non-positive
/// `batch_rows`/`poll_timeout_ms`/`reconnect_ms`, or a `pagination` or
/// `conditional` key next to a streaming kind.
fn read_mode(config: &ConfigValue) -> Result<ReadMode, SaciError> {
    let pagination = match config.get("pagination").map(ConfigValue::as_str) {
        None => Pagination::None,
        Some(Some("none")) => Pagination::None,
        Some(Some("link")) => Pagination::Link,
        Some(_) => {
            return Err(SaciError::configuration(
                "HttpSource config.pagination must be \"none\" or \"link\"",
            ));
        }
    };
    let conditional = match config.get("conditional") {
        None => false,
        Some(value) => value.as_bool().ok_or_else(|| {
            SaciError::configuration("HttpSource config.conditional must be a boolean")
        })?,
    };

    let Some(value) = config.get("mode") else {
        return Ok(ReadMode::Document {
            pagination,
            conditional,
        });
    };
    let table = value.as_object().ok_or_else(|| {
        SaciError::configuration("HttpSource config.mode must be a table with a 'kind' key")
    })?;
    for key in table.keys() {
        if !matches!(
            key.as_str(),
            "kind" | "event" | "batch_rows" | "poll_timeout_ms" | "reconnect_ms"
        ) {
            return Err(SaciError::configuration(format!(
                "HttpSource config.mode: unknown key '{key}'"
            )));
        }
    }

    let kind = match table.get("kind") {
        None => "document",
        Some(value) => match value.as_str() {
            Some(kind @ ("document" | "lines" | "events")) => kind,
            _ => {
                return Err(SaciError::configuration(
                    "HttpSource config.mode.kind must be \"document\", \"lines\" or \"events\"",
                ));
            }
        },
    };

    if kind == "document" {
        return Ok(ReadMode::Document {
            pagination,
            conditional,
        });
    }
    // Both apply to a finite resource: a live body has no next page and no
    // validator to compare, so silently ignoring them would hide a mistake.
    if pagination == Pagination::Link {
        return Err(SaciError::configuration(
            "HttpSource: pagination applies to mode kind=\"document\" only",
        ));
    }
    if conditional {
        return Err(SaciError::configuration(
            "HttpSource: conditional applies to mode kind=\"document\" only",
        ));
    }

    let window = BatchWindow {
        batch_rows: mode_positive(table, "batch_rows", 1_000)? as usize,
        poll_timeout: Duration::from_millis(mode_positive(table, "poll_timeout_ms", 1_000)?),
        reconnect: Duration::from_millis(mode_positive(table, "reconnect_ms", 1_000)?),
    };
    let event = match table.get("event") {
        None => None,
        Some(value) => Some(
            value
                .as_str()
                .ok_or_else(|| {
                    SaciError::configuration("HttpSource config.mode.event must be a string")
                })?
                .to_string(),
        ),
    };
    if kind == "lines" {
        if event.is_some() {
            return Err(SaciError::configuration(
                "HttpSource: mode.event applies to mode kind=\"events\" only",
            ));
        }
        return Ok(ReadMode::Lines(window));
    }
    Ok(ReadMode::Events { event, window })
}

/// How a sink compresses each body.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `content_encoding` is present and
/// is not one of the three tokens reqwest can decode on the way back.
fn content_encoding(config: &ConfigValue) -> Result<Option<ContentEncoding>, SaciError> {
    let Some(value) = config.get("content_encoding") else {
        return Ok(None);
    };
    match value.as_str() {
        Some("gzip") => Ok(Some(ContentEncoding::Gzip)),
        Some("br") => Ok(Some(ContentEncoding::Br)),
        Some("zstd") => Ok(Some(ContentEncoding::Zstd)),
        _ => Err(SaciError::configuration(
            "HttpSink config.content_encoding must be \"gzip\", \"br\" or \"zstd\"",
        )),
    }
}

/// The transport half of a client's options, shared by the source and the sink.
fn client_options(
    config: &ConfigValue,
    ctx: &ConnectorContext,
    what: &'static str,
) -> Result<HttpClientConfig, SaciError> {
    Ok(HttpClientConfig {
        http_version: http_version(config, what)?,
        tls: tls_files(config, what)?,
        headers: headers(config, what)?,
        timeout: timeout(config, what)?,
        identity: ctx.identity(what).ok().cloned(),
    })
}

/// The `http` connector: [`HttpSource`] and [`HttpSink`].
///
/// # Source config
///
/// Config fields:
/// - `url` (string, required): the resource to read.
/// - `headers` (table, optional): request headers, `name "value"` per entry.
/// - `timeout_ms` (integer, optional, default `30000`): whole-request budget.
///   In a streaming mode it is the connect budget and the idle gap between
///   body chunks instead, because a live body has no end.
/// - `http_version` (`"auto"`, `"1.1"`, `"2"` or `"3"`, optional, default
///   `"auto"`): which version to speak. `"auto"` negotiates HTTP/2 or
///   HTTP/1.1 over ALPN on https and sends HTTP/1.1 on http; it never reaches
///   HTTP/3. `"2"` is prior knowledge, so it is h2c on an http url. `"3"`
///   needs an https url.
/// - `tls` (table, optional): `ca_file` adds a private CA to the platform
///   trust store, `cert_file` plus `key_file` present a client certificate.
/// - `max_body_bytes` (integer, optional): the largest body to accept in a
///   document mode read, or one frame in a streaming kind. A
///   `content-length` above it is refused before the body is read; absent
///   means no cap.
/// - `mode` (table, optional, default `kind="document"`): `kind="document"` is
///   one finite document per read; `kind="lines"` reads a body that never ends
///   as newline-delimited records; `kind="events"` reads one as
///   `text/event-stream`, optionally filtered to one `event` name. The
///   streaming kinds take `batch_rows`, `poll_timeout_ms` and `reconnect_ms`.
///   A window's frames are lost together when the decoder refuses one of
///   them, so a smaller `batch_rows` bounds that loss.
/// - `pagination` (`"none"` or `"link"`, optional, default `"none"`): follow
///   `Link: <...>; rel="next"` across pages before reporting EOF.
///   `kind="document"` only.
/// - `conditional` (boolean, optional, default `#false`): send
///   `if-none-match`/`if-modified-since` from the last response's validators,
///   so an unchanged resource answers `304` and decodes nothing.
///   `kind="document"` only.
/// - `field` (one or more, optional): the declared Arrow schema. Required by
///   `csv`, by any `schema_from "payload"` source, and by both streaming kinds;
///   under the default `schema_from "config"` it is a projection target for
///   `parquet`, `avro` and `arrow-ipc`; `ndjson` infers it when absent.
/// - `schema_from` (`"config"` or `"payload"`, optional, default `"config"`):
///   where the schema handed to the format comes from. `"config"` hands
///   the `field` entries to the format, which a self-describing one projects onto;
///   `"payload"` withholds it and checks the body's own schema against it field
///   for field. `kind="document"` only.
///
/// Any key outside this list is refused as `http: unknown key '{key}'`.
///
/// The byte format is whatever codec the `source` node's `codec`
/// key names; see [`ConnectorContext::codec`].
///
/// # Sink config
///
/// - `url` (string, required): the endpoint each batch is sent to. A `307` or
///   `308` is followed, up to ten hops; every other 3xx is reported as the
///   status it is, because following it would drop the body. Over HTTP/3 no
///   redirect is followed.
/// - `method` (string, optional, default `"POST"`): the HTTP method.
/// - `headers` (table, optional): request headers, `name "value"` per entry. A
///   `content-type` here wins over the one the format implies; an
///   `accept-encoding` here turns off the automatic one, which is how a source
///   asks for an uncompressed response.
/// - `timeout_ms` (integer, optional, default `30000`): whole-request budget.
/// - `http_version` (`"auto"`, `"1.1"`, `"2"` or `"3"`, optional, default
///   `"auto"`): which version to speak, as on the source.
/// - `tls` (table, optional): `ca_file`, `cert_file`, `key_file`, as on the
///   source.
/// - `content_encoding` (`"gzip"`, `"br"` or `"zstd"`, optional): compress
///   each body and declare it. Refused next to a `content-encoding` header.
/// - `field` (one or more, required): the Arrow schema each body is written
///   with.
///
/// Any key outside this list is refused as `http: unknown key '{key}'`.
///
/// The byte format is whatever codec the `sink` node's `codec` key
/// names; see [`ConnectorContext::codec`].
pub struct HttpConnector;

impl Connector for HttpConnector {
    fn name(&self) -> &'static str {
        "http"
    }

    fn source(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        reject_unknown_keys(config, SOURCE_KEYS)?;
        let url = required_url(config, "HttpSource")?;
        let codec = ctx.codec("HttpSource")?;
        let client = client_options(config, ctx, "HttpSource")?;
        let schema = parse_optional_fields(config, "HttpSource")?
            .map(|fields| schema_of(&fields))
            .transpose()?;
        Ok(Box::new(HttpSource::new(
            HttpSourceConfig {
                url: url.to_string(),
                client,
                schema_from: schema_from(config, "HttpSource")?,
                mode: read_mode(config)?,
                max_body_bytes: positive(config, "HttpSource", "max_body_bytes")?,
            },
            schema,
            codec,
        )?))
    }

    fn sink(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        reject_unknown_keys(config, SINK_KEYS)?;
        let url = required_url(config, "HttpSink")?;
        let codec = ctx.codec("HttpSink")?;
        let schema = schema_of(&parse_fields(config, "HttpSink")?)?;
        let method = match config.get("method") {
            None => DEFAULT_METHOD,
            Some(value) => value.as_str().ok_or_else(|| {
                SaciError::configuration("HttpSink config.method must be a string")
            })?,
        };
        Ok(Box::new(HttpSink::new(
            HttpSinkConfig {
                url: url.to_string(),
                client: client_options(config, ctx, "HttpSink")?,
                method: method.to_string(),
                content_encoding: content_encoding(config)?,
            },
            schema,
            codec,
        )?))
    }

    /// A source holds no state a rebuild would lose: a document mode instance
    /// re-issues the same GET, and a streaming one redials. A sink sends one
    /// self-contained request per batch and buffers nothing between them, so a
    /// fresh instance is indistinguishable from the one it replaced.
    fn rebuild(&self, _role: Role, _config: &ConfigValue) -> Rebuild {
        Rebuild::Yes
    }

    /// A streaming `mode` (`lines`, `events`) reads a body that never ends; a
    /// document read ends.
    fn live(&self, config: &ConfigValue) -> bool {
        matches!(
            config
                .get("mode")
                .and_then(|mode| mode.get("kind"))
                .and_then(ConfigValue::as_str),
            Some("lines" | "events")
        )
    }
}

/// The `http-listen` connector: a source that serves [`HttpListenSource`].
///
/// It has no sink role.
///
/// # Config
///
/// Config fields:
/// - `bind` (string, required): the `host:port` to listen on.
/// - `path` (string, optional, default `"/"`): the one path that accepts
///   documents.
/// - `methods` (string or list, optional, default `"POST"`): the methods that
///   accept documents.
/// - `max_body_bytes` (integer, optional, default `67108864`): the largest
///   request body to accept. A `content-length` above it is refused before
///   the body is read.
/// - `buffer` (integer, optional, default `64`): accepted documents that may
///   queue before a request in flight waits for its response. A document is
///   one entry however many batches it decoded to, which is what makes it
///   queued whole or not at all.
/// - `tls` (table, optional): `cert_file` plus `key_file` terminate TLS,
///   offering `h2` and `http/1.1` over ALPN. Both keys or neither.
/// - `quic_bind` (string, optional): a UDP `host:port` to serve HTTP/3 on,
///   advertised to TCP clients as `alt-svc`. Needs `tls`.
/// - `auth` (table, optional): `bearer "token"` demands that token from every
///   request.
/// - `field` (one or more, required): the Arrow schema every document is read
///   against.
/// - `schema_from` (`"config"` or `"payload"`, optional, default `"config"`): as
///   on [`HttpConnector`].
///
/// Any key outside this list is refused as `http: unknown key '{key}'`.
///
/// The byte format is whatever codec the `source` node's `codec`
/// key names; see [`ConnectorContext::codec`].
pub struct HttpListenConnector;

impl Connector for HttpListenConnector {
    fn name(&self) -> &'static str {
        "http-listen"
    }

    fn roles(&self) -> &'static [Role] {
        &[Role::Source]
    }

    fn source(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        reject_unknown_keys(config, LISTEN_KEYS)?;
        let what = "HttpListenSource";
        let bind = config.get("bind").and_then(|v| v.as_str()).ok_or_else(|| {
            SaciError::configuration(format!("{what} config requires a 'bind' string"))
        })?;
        let codec = ctx.codec(what)?;
        let declared = schema_of(&parse_fields(config, what)?)?;

        let path = match config.get("path") {
            None => "/".to_string(),
            Some(value) => match value.as_str() {
                Some(path) if path.starts_with('/') => path.to_string(),
                _ => {
                    return Err(SaciError::configuration(format!(
                        "{what} config.path must be a string starting with '/'"
                    )));
                }
            },
        };
        let methods = listen_methods(config, what)?;
        let tls = listen_tls(config, what)?;
        let quic_bind = match config.get("quic_bind") {
            None => None,
            Some(value) => Some(
                value
                    .as_str()
                    .ok_or_else(|| {
                        SaciError::configuration(format!(
                            "{what} config.quic_bind must be a string"
                        ))
                    })?
                    .to_string(),
            ),
        };
        let bearer = listen_bearer(config, what)?;

        Ok(Box::new(HttpListenSource::new(
            HttpListenConfig {
                bind: bind.to_string(),
                schema_from: schema_from(config, what)?,
                path,
                methods,
                max_body_bytes: positive(config, what, "max_body_bytes")?
                    .unwrap_or(DEFAULT_LISTEN_BODY_BYTES as usize),
                // Lenient, like `tcp`'s and `udp`'s: a zero is a request for
                // no queueing at all, which the channel cannot express.
                buffer: config
                    .get("buffer")
                    .and_then(ConfigValue::as_i64)
                    .unwrap_or(DEFAULT_LISTEN_BUFFER)
                    .max(1) as usize,
                tls,
                quic_bind,
                bearer,
                identity: ctx.identity(what).ok().cloned(),
            },
            declared,
            codec,
        )?))
    }

    /// The listener is dropped before a rebuild, so the replacement binds the
    /// same address, exactly as the `tcp` source's does.
    fn rebuild(&self, _role: Role, _config: &ConfigValue) -> Rebuild {
        Rebuild::Yes
    }

    /// A listener never reaches the end of its input.
    fn live(&self, _config: &ConfigValue) -> bool {
        true
    }

    fn detail_keys(&self, role: Role) -> &'static [&'static str] {
        match role {
            Role::Source => &["bind", "path", "quic_bind"],
            Role::Sink => &[],
        }
    }
}

/// The `methods` key: one method name, or a list of them.
///
/// `crate::config`'s rule 3 means two or more arguments arrive as an array and
/// exactly one arrives as a scalar, so both shapes are read here.
fn listen_methods(config: &ConfigValue, what: &str) -> Result<Vec<String>, SaciError> {
    let Some(value) = config.get("methods") else {
        return Ok(vec![DEFAULT_METHOD.to_string()]);
    };
    let refused = || {
        SaciError::configuration(format!(
            "{what} config.methods must be one HTTP method name or a list of them"
        ))
    };
    if let Some(one) = value.as_str() {
        return Ok(vec![one.to_string()]);
    }
    let list = value.as_array().ok_or_else(refused)?;
    if list.is_empty() {
        return Err(refused());
    }
    list.iter()
        .map(|entry| entry.as_str().map(str::to_string).ok_or_else(refused))
        .collect()
}

/// The listening source's `tls` block: the pair, or neither.
fn listen_tls(config: &ConfigValue, what: &str) -> Result<Option<ServerTls>, SaciError> {
    let files = tls_files(config, what)?;
    if files.ca_file.is_some() {
        return Err(SaciError::configuration(format!(
            "{what} config.tls['ca_file'] is not a listener key: a listening source authenticates \
             callers with auth {{ bearer }}, not with a client certificate"
        )));
    }
    match (files.cert_file, files.key_file) {
        (None, None) => Ok(None),
        (Some(cert_file), Some(key_file)) => Ok(Some(ServerTls {
            cert_file,
            key_file,
        })),
        _ => Err(SaciError::configuration(format!(
            "{what} config.tls needs both 'cert_file' and 'key_file'"
        ))),
    }
}

/// The `auth` block's bearer token.
fn listen_bearer(config: &ConfigValue, what: &str) -> Result<Option<String>, SaciError> {
    let Some(value) = config.get("auth") else {
        return Ok(None);
    };
    let table = value
        .as_object()
        .ok_or_else(|| SaciError::configuration(format!("{what} config.auth must be a table")))?;
    match table.get("bearer") {
        Some(value) => match value.as_str() {
            Some(token) if !token.is_empty() => Ok(Some(token.to_string())),
            _ => Err(SaciError::configuration(format!(
                "{what} config.auth.bearer must be a non-empty string"
            ))),
        },
        None => Err(SaciError::configuration(format!(
            "{what} config.auth needs a 'bearer' string"
        ))),
    }
}

#[cfg(all(test, feature = "codec-csv", feature = "codec-ndjson"))]
mod tests {
    use std::sync::Arc;

    use crate::codec::csv::CsvCodecFactory;
    use crate::codec::ndjson::NdjsonCodecFactory;
    use crate::codec::{Codec, CodecFactory};
    use crate::connector::{ConfigMap, from_kdl_str};

    use super::*;

    fn csv_codec() -> Arc<dyn Codec> {
        CsvCodecFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("csv codec builds")
    }

    fn ndjson_codec() -> Arc<dyn Codec> {
        NdjsonCodecFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("ndjson codec builds")
    }

    fn empty_config() -> ConfigValue {
        ConfigValue::Object(ConfigMap::new())
    }

    fn config(raw: &str) -> ConfigValue {
        from_kdl_str(raw).expect("parse test config")
    }

    const SCHEMA: &str = "field \"id\" type=\"int64\" nullable=#false\n";

    /// Build a source from `raw` and expect a configuration error whose
    /// message contains `fragment`.
    fn source_refuses(raw: &str, fragment: &str) {
        let ctx = ConnectorContext::new(Some(csv_codec()));
        let Err(err) = HttpConnector.source(&config(raw), &ctx) else {
            panic!("expected a refusal naming {fragment}");
        };
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains(fragment), "got: {err}");
    }

    /// The same for a sink.
    fn sink_refuses(raw: &str, fragment: &str) {
        let ctx = ConnectorContext::new(Some(csv_codec()));
        let Err(err) = HttpConnector.sink(&config(raw), &ctx) else {
            panic!("expected a refusal naming {fragment}");
        };
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains(fragment), "got: {err}");
    }

    /// The same for a listen source, which needs a message-capable format.
    fn listen_refuses(raw: &str, fragment: &str) {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        let Err(err) = HttpListenConnector.source(&config(raw), &ctx) else {
            panic!("expected a refusal naming {fragment}");
        };
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains(fragment), "got: {err}");
    }

    #[test]
    fn the_names_match_the_config_connector_key() {
        assert_eq!(HttpConnector.name(), "http");
        assert_eq!(HttpListenConnector.name(), "http-listen");
    }

    #[test]
    fn http_serves_both_roles_and_http_listen_only_the_source() {
        assert_eq!(HttpConnector.roles(), &[Role::Source, Role::Sink]);
        assert_eq!(HttpListenConnector.roles(), &[Role::Source]);
    }

    #[test]
    fn only_a_streaming_http_mode_is_live() {
        assert!(!HttpConnector.live(&empty_config()));
        assert!(!HttpConnector.live(&config("mode kind=\"document\"")));
        assert!(HttpConnector.live(&config("mode kind=\"lines\"")));
        assert!(HttpConnector.live(&config("mode kind=\"events\"")));
        assert!(HttpListenConnector.live(&empty_config()));
    }

    #[test]
    fn the_dashboard_detail_keys_are_the_listener_ones() {
        assert_eq!(
            HttpListenConnector.detail_keys(Role::Source),
            &["bind", "path", "quic_bind"]
        );
        assert!(HttpListenConnector.detail_keys(Role::Sink).is_empty());
        assert!(HttpConnector.detail_keys(Role::Source).is_empty());
        assert!(HttpConnector.detail_keys(Role::Sink).is_empty());
    }

    #[test]
    fn a_missing_url_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(csv_codec()));

        let Err(err) = HttpConnector.source(&empty_config(), &ctx) else {
            panic!("url is required");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "HttpSource config requires a 'url' string field"
        );

        let Err(err) = HttpConnector.sink(&empty_config(), &ctx) else {
            panic!("url is required");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "HttpSink config requires a 'url' string field"
        );
    }

    #[test]
    fn a_missing_bind_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        let Err(err) = HttpListenConnector.source(&empty_config(), &ctx) else {
            panic!("a listener needs an address");
        };
        assert_eq!(
            err.message(),
            "HttpListenSource config requires a 'bind' string"
        );
    }

    /// A misspelled optional key read as absent would run the node with a
    /// default the config author never chose, and a key that belongs to
    /// another role is as foreign here as a misspelling.
    #[test]
    fn an_unknown_source_key_is_refused_by_name() {
        let ctx = ConnectorContext::new(Some(csv_codec()));
        for key in ["timeut_ms 500", "method \"POST\""] {
            let Err(err) = HttpConnector.source(
                &config(&format!(
                    "url \"http://127.0.0.1:1/data.csv\"\n{key}\n{SCHEMA}"
                )),
                &ctx,
            ) else {
                panic!("'{key}' is not a source key");
            };
            assert_eq!(err.category(), "configuration");
            let name = key.split(' ').next().expect("a key name");
            assert_eq!(err.message(), format!("http: unknown key '{name}'"));
        }
    }

    #[test]
    fn an_unknown_sink_key_is_refused_by_name() {
        let ctx = ConnectorContext::new(Some(csv_codec()));
        for key in ["methd \"PUT\"", "pagination \"link\""] {
            let Err(err) = HttpConnector.sink(
                &config(&format!(
                    "url \"http://127.0.0.1:1/ingest\"\n{key}\n{SCHEMA}"
                )),
                &ctx,
            ) else {
                panic!("'{key}' is not a sink key");
            };
            assert_eq!(err.category(), "configuration");
            let name = key.split(' ').next().expect("a key name");
            assert_eq!(err.message(), format!("http: unknown key '{name}'"));
        }
    }

    #[test]
    fn an_unknown_listen_key_is_refused_by_name() {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        for key in ["pth \"/orders\"", "url \"http://127.0.0.1:1/\""] {
            let Err(err) = HttpListenConnector.source(
                &config(&format!("bind \"127.0.0.1:0\"\n{key}\n{SCHEMA}")),
                &ctx,
            ) else {
                panic!("'{key}' is not a listen key");
            };
            assert_eq!(err.category(), "configuration");
            let name = key.split(' ').next().expect("a key name");
            assert_eq!(err.message(), format!("http: unknown key '{name}'"));
        }
    }

    /// A misspelling read as the default would project the body onto the
    /// declared schema instead of checking it, on the first batch, in
    /// production.
    #[test]
    fn an_unknown_schema_from_is_a_configuration_error() {
        source_refuses(
            &format!("url \"https://example.invalid/orders.csv\"\nschema_from \"body\"\n{SCHEMA}"),
            "HttpSource config.schema_from must be \"config\" or \"payload\"",
        );
    }

    /// Only a parsed `"payload"` reaches the constructor's demand for a declared
    /// schema, so this is what distinguishes it from the default.
    #[test]
    fn schema_from_payload_reaches_the_source() {
        let ctx = ConnectorContext::new(Some(csv_codec()));
        source_refuses(
            "url \"https://example.invalid/orders.csv\"\nschema_from \"payload\"",
            "'field'",
        );

        HttpConnector
            .source(&config("url \"https://example.invalid/orders.csv\""), &ctx)
            .expect("the default takes no schema at all");
    }

    #[test]
    fn a_source_with_no_bound_codec_is_a_configuration_error() {
        let ctx = ConnectorContext::new(None);
        let raw = format!("url \"http://127.0.0.1:1/data.csv\"\n{SCHEMA}");

        let Err(err) = HttpConnector.source(&config(&raw), &ctx) else {
            panic!("a source that moves bytes needs a bound codec");
        };
        assert_eq!(
            err.message(),
            "HttpSource moves bytes and needs a 'codec' key naming a declared codec"
        );
    }

    #[test]
    fn a_sink_with_no_bound_codec_is_a_configuration_error() {
        let ctx = ConnectorContext::new(None);
        let raw = format!("url \"http://127.0.0.1:1/ingest\"\n{SCHEMA}");

        let Err(err) = HttpConnector.sink(&config(&raw), &ctx) else {
            panic!("a sink that moves bytes needs a bound codec");
        };
        assert_eq!(
            err.message(),
            "HttpSink moves bytes and needs a 'codec' key naming a declared codec"
        );
    }

    #[test]
    fn a_sink_without_fields_is_a_configuration_error() {
        sink_refuses("url \"http://127.0.0.1:1/ingest\"\n", "'field'");
    }

    /// The whole point of building without a request: an endpoint that does not
    /// exist still builds, so `validate` never depends on the network.
    #[test]
    fn both_halves_build_against_an_endpoint_that_is_down() {
        let ctx = ConnectorContext::new(Some(csv_codec()));
        let raw = format!(
            r#"
url "http://127.0.0.1:1/data.csv"
timeout_ms 500
headers {{
    accept "text/csv"
}}
{SCHEMA}"#
        );

        let source = HttpConnector
            .source(&config(&raw), &ctx)
            .expect("source builds");
        assert_eq!(source.schema().fields().len(), 1);
        assert_eq!(source.schema().field(0).name(), "id");

        let sink = HttpConnector
            .sink(&config(&raw), &ctx)
            .expect("sink builds");
        assert_eq!(sink.schema().fields().len(), 1);
    }

    #[test]
    fn a_source_without_fields_reports_an_empty_schema() {
        let ctx = ConnectorContext::new(Some(csv_codec()));
        let source = HttpConnector
            .source(&config("url \"http://127.0.0.1:1/data.csv\"\n"), &ctx)
            .expect("the format decides whether it needs a declared schema");
        assert_eq!(source.schema().fields().len(), 0);
        assert_eq!(source.estimated_rows(), None);
    }

    #[test]
    fn a_non_string_header_value_is_a_configuration_error_naming_the_key() {
        sink_refuses(
            &format!(
                r#"
url "http://127.0.0.1:1/ingest"
headers {{
    retries 3
}}
{SCHEMA}"#
            ),
            "'retries'",
        );
    }

    #[test]
    fn a_scalar_headers_key_is_a_configuration_error() {
        sink_refuses(
            &format!("url \"http://127.0.0.1:1/ingest\"\nheaders \"accept\"\n{SCHEMA}"),
            "HttpSink config.headers must be a table of string values",
        );
    }

    #[test]
    fn an_invalid_method_is_rejected_by_the_sink() {
        sink_refuses(
            &format!("url \"http://127.0.0.1:1/ingest\"\nmethod \"PO ST\"\n{SCHEMA}"),
            "'PO ST'",
        );
    }

    /// Every other optional key refuses a wrong type, and a method silently
    /// defaulted to `POST` would send a config author's `PUT` nowhere.
    #[test]
    fn a_non_string_method_is_a_configuration_error() {
        sink_refuses(
            &format!("url \"http://127.0.0.1:1/ingest\"\nmethod 7\n{SCHEMA}"),
            "HttpSink config.method must be a string",
        );
    }

    #[test]
    fn a_non_integer_timeout_is_a_configuration_error() {
        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\ntimeout_ms \"30s\"\n{SCHEMA}"),
            "HttpSource config.timeout_ms must be an integer of at least 1",
        );
        sink_refuses(
            &format!("url \"http://127.0.0.1:1/ingest\"\ntimeout_ms \"30s\"\n{SCHEMA}"),
            "HttpSink config.timeout_ms must be an integer of at least 1",
        );
    }

    /// A budget of zero milliseconds is a request no client can make, so it
    /// is refused rather than floored into something else.
    #[test]
    fn a_zero_timeout_is_a_configuration_error() {
        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\ntimeout_ms 0\n{SCHEMA}"),
            "config.timeout_ms must be an integer of at least 1",
        );
    }

    // ── the new keys ────────────────────────────────────────────────────────

    #[test]
    fn every_http_version_spelling_is_accepted_and_nothing_else_is() {
        let ctx = ConnectorContext::new(Some(csv_codec()));
        for version in ["auto", "1.1", "2"] {
            HttpConnector
                .source(
                    &config(&format!(
                        "url \"http://127.0.0.1:1/data.csv\"\nhttp_version \"{version}\"\n{SCHEMA}"
                    )),
                    &ctx,
                )
                .unwrap_or_else(|e| panic!("'{version}' is a version, got: {e}"));
        }
        HttpConnector
            .source(
                &config(&format!(
                    "url \"https://127.0.0.1:1/data.csv\"\nhttp_version \"3\"\n{SCHEMA}"
                )),
                &ctx,
            )
            .expect("HTTP/3 over https builds");

        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\nhttp_version \"1\"\n{SCHEMA}"),
            "config.http_version must be \"auto\", \"1.1\", \"2\" or \"3\"",
        );
    }

    #[test]
    fn http3_over_cleartext_is_refused_on_both_halves() {
        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\nhttp_version \"3\"\n{SCHEMA}"),
            "http_version \"3\" needs an https url",
        );
        sink_refuses(
            &format!("url \"http://127.0.0.1:1/ingest\"\nhttp_version \"3\"\n{SCHEMA}"),
            "http_version \"3\" needs an https url",
        );
    }

    #[test]
    fn a_scalar_tls_key_is_a_configuration_error() {
        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\ntls \"on\"\n{SCHEMA}"),
            "HttpSource config.tls must be a table",
        );
    }

    #[test]
    fn a_non_string_tls_path_is_a_configuration_error() {
        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\ntls {{\n ca_file 3\n}}\n{SCHEMA}"),
            "config.tls['ca_file'] must be a string",
        );
    }

    #[test]
    fn an_unknown_tls_key_is_a_configuration_error() {
        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\ntls {{\n verify \"no\"\n}}\n{SCHEMA}"),
            "is not a tls key",
        );
    }

    #[test]
    fn a_non_positive_max_body_bytes_is_a_configuration_error() {
        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\nmax_body_bytes 0\n{SCHEMA}"),
            "config.max_body_bytes must be an integer of at least 1",
        );
        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\nmax_body_bytes \"1M\"\n{SCHEMA}"),
            "config.max_body_bytes must be an integer of at least 1",
        );
    }

    #[test]
    fn an_unknown_mode_kind_is_a_configuration_error() {
        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\nmode kind=\"tail\"\n{SCHEMA}"),
            "config.mode.kind must be \"document\", \"lines\" or \"events\"",
        );
    }

    #[test]
    fn a_scalar_mode_is_a_configuration_error() {
        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\nmode \"lines\"\n{SCHEMA}"),
            "config.mode must be a table with a 'kind' key",
        );
    }

    #[test]
    fn a_non_positive_window_key_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        let Err(err) = HttpConnector.source(
            &config(&format!(
                "url \"http://127.0.0.1:1/live\"\nmode kind=\"lines\" batch_rows=0\n{SCHEMA}"
            )),
            &ctx,
        ) else {
            panic!("a window of zero frames never closes");
        };
        assert!(
            err.message()
                .contains("config.mode['batch_rows'] must be an integer of at least 1"),
            "got: {err}"
        );
    }

    #[test]
    fn the_old_batch_size_spelling_is_refused_by_name() {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        let Err(err) = HttpConnector.source(
            &config(&format!(
                "url \"http://127.0.0.1:1/live\"\nmode kind=\"lines\" batch_size=5\n{SCHEMA}"
            )),
            &ctx,
        ) else {
            panic!("`batch_size` was replaced by `batch_rows`");
        };
        assert!(err.message().contains("'batch_size'"), "got: {err}");
    }

    #[test]
    fn a_non_string_mode_event_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        let Err(err) = HttpConnector.source(
            &config(&format!(
                "url \"http://127.0.0.1:1/live\"\nmode kind=\"events\" event=7\n{SCHEMA}"
            )),
            &ctx,
        ) else {
            panic!("an event name is a string");
        };
        assert_eq!(
            err.message(),
            "HttpSource config.mode.event must be a string"
        );
    }

    #[test]
    fn an_event_filter_on_the_lines_kind_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        let Err(err) = HttpConnector.source(
            &config(&format!(
                "url \"http://127.0.0.1:1/live\"\nmode kind=\"lines\" event=\"x\"\n{SCHEMA}"
            )),
            &ctx,
        ) else {
            panic!("newline framing has no event names");
        };
        assert!(err.message().contains("mode.event applies"), "got: {err}");
    }

    #[test]
    fn an_unknown_pagination_is_a_configuration_error() {
        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\npagination \"cursor\"\n{SCHEMA}"),
            "config.pagination must be \"none\" or \"link\"",
        );
    }

    #[test]
    fn a_non_boolean_conditional_is_a_configuration_error() {
        source_refuses(
            &format!("url \"http://127.0.0.1:1/data.csv\"\nconditional \"yes\"\n{SCHEMA}"),
            "config.conditional must be a boolean",
        );
    }

    #[test]
    fn pagination_and_conditional_are_refused_next_to_a_streaming_kind() {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        for (key, fragment) in [
            (
                "pagination \"link\"",
                "HttpSource: pagination applies to mode kind=\"document\" only",
            ),
            (
                "conditional #true",
                "HttpSource: conditional applies to mode kind=\"document\" only",
            ),
        ] {
            let Err(err) = HttpConnector.source(
                &config(&format!(
                    "url \"http://127.0.0.1:1/live\"\nmode kind=\"lines\"\n{key}\n{SCHEMA}"
                )),
                &ctx,
            ) else {
                panic!("a live body has no pages and no validators");
            };
            assert_eq!(err.message(), fragment, "got: {err}");
        }
    }

    #[test]
    fn a_streaming_kind_without_fields_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        let Err(err) = HttpConnector.source(
            &config("url \"http://127.0.0.1:1/live\"\nmode kind=\"events\""),
            &ctx,
        ) else {
            panic!("a message decoder is built before the first byte arrives");
        };
        assert!(
            err.message().contains("needs a 'field' entry"),
            "got: {err}"
        );
    }

    #[test]
    fn schema_from_payload_is_refused_next_to_a_streaming_kind() {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        let Err(err) = HttpConnector.source(
            &config(&format!(
                "url \"http://127.0.0.1:1/live\"\nmode kind=\"lines\"\nschema_from \"payload\"\n{SCHEMA}"
            )),
            &ctx,
        ) else {
            panic!("a live body's schema is the declared one");
        };
        assert_eq!(
            err.message(),
            "HttpSource: schema_from \"payload\" applies to mode kind=\"document\" only"
        );
    }

    /// Every registered format has a message decoder, so the config-time
    /// refusal is reached through a stand-in declared here; it is what stops
    /// a streaming source from connecting, closing, and delivering nothing.
    #[test]
    fn a_format_with_no_message_decoder_is_refused_for_a_streaming_kind() {
        struct StreamOnly;

        impl Codec for StreamOnly {
            fn format(&self) -> &'static str {
                "stream-only"
            }
        }

        let ctx = ConnectorContext::new(Some(Arc::new(StreamOnly)));
        let Err(err) = HttpConnector.source(
            &config(&format!(
                "url \"http://127.0.0.1:1/live\"\nmode kind=\"lines\"\n{SCHEMA}"
            )),
            &ctx,
        ) else {
            panic!("a format with no message decoder cannot read a live body");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message()
                .contains("does not support decoding discrete messages"),
            "got: {err}"
        );
        assert!(err.message().contains("'stream-only'"), "got: {err}");
    }

    #[test]
    fn an_unknown_content_encoding_is_a_configuration_error() {
        sink_refuses(
            &format!("url \"http://127.0.0.1:1/ingest\"\ncontent_encoding \"lz4\"\n{SCHEMA}"),
            "config.content_encoding must be \"gzip\", \"br\" or \"zstd\"",
        );
    }

    #[test]
    fn content_encoding_next_to_its_own_header_is_refused() {
        sink_refuses(
            &format!(
                r#"
url "http://127.0.0.1:1/ingest"
content_encoding "gzip"
headers {{
    content-encoding "gzip"
}}
{SCHEMA}"#
            ),
            "'content-encoding' is set by content_encoding",
        );
    }

    // ── the listen source's keys ────────────────────────────────────────────

    #[test]
    fn a_listen_source_binds_and_reports_the_declared_schema() {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        let source = HttpListenConnector
            .source(
                &config(&format!(
                    "bind \"127.0.0.1:0\"\npath \"/orders\"\nmethods \"POST\" \"PUT\"\n{SCHEMA}"
                )),
                &ctx,
            )
            .expect("the listener binds at load");
        assert_eq!(source.schema().fields().len(), 1);
    }

    #[test]
    fn a_path_without_a_leading_slash_is_a_configuration_error() {
        listen_refuses(
            &format!("bind \"127.0.0.1:0\"\npath \"orders\"\n{SCHEMA}"),
            "config.path must be a string starting with '/'",
        );
    }

    #[test]
    fn a_non_string_method_in_the_list_is_a_configuration_error() {
        listen_refuses(
            &format!("bind \"127.0.0.1:0\"\nmethods \"POST\" 7\n{SCHEMA}"),
            "config.methods must be one HTTP method name or a list of them",
        );
    }

    #[test]
    fn a_non_string_quic_bind_is_a_configuration_error() {
        listen_refuses(
            &format!("bind \"127.0.0.1:0\"\nquic_bind 9700\n{SCHEMA}"),
            "config.quic_bind must be a string",
        );
    }

    #[test]
    fn quic_bind_without_tls_is_a_configuration_error() {
        listen_refuses(
            &format!("bind \"127.0.0.1:0\"\nquic_bind \"127.0.0.1:0\"\n{SCHEMA}"),
            "quic_bind needs a 'tls' block",
        );
    }

    #[test]
    fn half_a_listener_tls_pair_is_a_configuration_error() {
        listen_refuses(
            &format!("bind \"127.0.0.1:0\"\ntls {{\n cert_file \"c.pem\"\n}}\n{SCHEMA}"),
            "config.tls needs both 'cert_file' and 'key_file'",
        );
    }

    /// A listener has no peer to trust, and reading `ca_file` as if it did
    /// would look like client authentication that is not happening.
    #[test]
    fn a_ca_file_on_the_listener_is_a_configuration_error() {
        listen_refuses(
            &format!("bind \"127.0.0.1:0\"\ntls {{\n ca_file \"ca.pem\"\n}}\n{SCHEMA}"),
            "is not a listener key",
        );
    }

    #[test]
    fn a_scalar_auth_block_is_a_configuration_error() {
        listen_refuses(
            &format!("bind \"127.0.0.1:0\"\nauth \"token\"\n{SCHEMA}"),
            "config.auth must be a table",
        );
    }

    /// An `auth` block that is a table, just without the one key it exists
    /// for, names that key rather than the shape it already has.
    #[test]
    fn an_auth_block_without_a_bearer_key_is_a_configuration_error() {
        listen_refuses(
            &format!("bind \"127.0.0.1:0\"\nauth {{\n token \"x\"\n}}\n{SCHEMA}"),
            "HttpListenSource config.auth needs a 'bearer' string",
        );
    }

    #[test]
    fn an_empty_bearer_token_is_a_configuration_error() {
        listen_refuses(
            &format!("bind \"127.0.0.1:0\"\nauth {{\n bearer \"\"\n}}\n{SCHEMA}"),
            "config.auth.bearer must be a non-empty string",
        );
    }

    /// A zero cannot be expressed by a channel, so it is clamped rather than
    /// refused, matching `tcp` and `udp`.
    #[test]
    fn a_zero_buffer_is_clamped_rather_than_refused() {
        let ctx = ConnectorContext::new(Some(ndjson_codec()));
        HttpListenConnector
            .source(
                &config(&format!("bind \"127.0.0.1:0\"\nbuffer 0\n{SCHEMA}")),
                &ctx,
            )
            .expect("buffer is the one lenient key");
    }

    #[test]
    fn both_connectors_are_rebuildable() {
        assert_eq!(
            HttpConnector.rebuild(Role::Source, &empty_config()),
            Rebuild::Yes
        );
        assert_eq!(
            HttpConnector.rebuild(Role::Sink, &empty_config()),
            Rebuild::Yes
        );
        assert_eq!(
            HttpListenConnector.rebuild(Role::Source, &empty_config()),
            Rebuild::Yes
        );
    }
}
