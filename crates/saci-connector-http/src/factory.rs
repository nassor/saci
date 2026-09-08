//! The HTTP source, sink and push source factories.
//!
//! All three resolve their byte format through the transformer the host bound
//! to this connector instance via a declared `transformer` key. The schema is
//! where they differ: a document mode source hands the format whatever
//! `schema_fields` says, including nothing, and lets the format decide, while a
//! sink and a push source always need one, because it is the schema the rows
//! are written with and the one the graph is validated against before a request
//! can arrive.
//!
//! No factory makes a request, so `saci-service validate` touches no network. A
//! push source does bind its sockets, the way the `tcp` and `udp` sources do,
//! so a port already in use is a load-time error rather than a listener that
//! never accepts.

use std::path::PathBuf;
use std::time::Duration;

use saci_connector::{
    ConfigValue, ConnectorContext, SinkFactory, SourceFactory, parse_optional_schema_fields,
    parse_schema_fields,
};
use saci_core::error::SaciError;
use saci_core::io::{sink::Sink, source::Source};

use crate::sink::{ContentEncoding, HttpSinkOptions};
use crate::source::{HttpClientOptions, HttpSourceOptions, Pagination, ReadMode, Window};
use crate::tls::TlsFiles;
use crate::{
    HttpPushOptions, HttpPushSource, HttpSink, HttpSource, HttpVersion, SchemaFrom, ServerTls,
};

/// Whole-request budget when `timeout_ms` is absent.
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
/// Method the sink sends when `method` is absent.
const DEFAULT_METHOD: &str = "POST";
/// Largest request body a push source accepts when `max_body_bytes` is absent.
const DEFAULT_PUSH_BODY_BYTES: i64 = 67_108_864;
/// Documents a push source queues when `buffer` is absent.
const DEFAULT_PUSH_BUFFER: i64 = 64;

/// The `url` a factory needs, or a configuration error naming the key.
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
/// neither `"config"` nor `"body"`: a misspelling silently read as the default
/// would take the body's own schema as a projection target instead of checking
/// it, in production rather than at load.
fn schema_from(config: &ConfigValue, what: &str) -> Result<SchemaFrom, SaciError> {
    let Some(value) = config.get("schema_from") else {
        return Ok(SchemaFrom::Config);
    };
    match value.as_str() {
        Some("config") => Ok(SchemaFrom::Config),
        Some("body") => Ok(SchemaFrom::Body),
        _ => Err(SaciError::configuration(format!(
            "{what} config.schema_from must be \"config\" or \"body\""
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
    table: &saci_connector::ConfigMap,
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
/// `batch_size`/`poll_timeout_ms`/`reconnect_ms`, or a `pagination` or
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

    let window = Window {
        batch_size: mode_positive(table, "batch_size", 1_000)? as usize,
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
) -> Result<HttpClientOptions, SaciError> {
    Ok(HttpClientOptions {
        http_version: http_version(config, what)?,
        tls: tls_files(config, what)?,
        headers: headers(config, what)?,
        timeout: timeout(config, what)?,
        identity: ctx.identity(what).ok().cloned(),
    })
}

/// Factory for [`HttpSource`].
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
///   streaming kinds take `batch_size`, `poll_timeout_ms` and `reconnect_ms`.
///   A window's frames are lost together when the decoder refuses one of
///   them, so a smaller `batch_size` bounds that loss.
/// - `pagination` (`"none"` or `"link"`, optional, default `"none"`): follow
///   `Link: <...>; rel="next"` across pages before reporting EOF.
///   `kind="document"` only.
/// - `conditional` (boolean, optional, default `#false`): send
///   `if-none-match`/`if-modified-since` from the last response's validators,
///   so an unchanged resource answers `304` and decodes nothing.
///   `kind="document"` only.
/// - `schema_fields` (list, optional): the declared Arrow schema. Required by
///   `csv`, by any `schema_from "body"` source, and by both streaming kinds;
///   under the default `schema_from "config"` it is a projection target for
///   `parquet`, `avro` and `arrow-ipc`; `ndjson` infers it when absent.
/// - `schema_from` (`"config"` or `"body"`, optional, default `"config"`):
///   where the schema handed to the format comes from. `"config"` hands
///   `schema_fields` to the format, which a self-describing one projects onto;
///   `"body"` withholds it and checks the body's own schema against it field
///   for field. `kind="document"` only.
///
/// The byte format is whatever transformer the `source` node's `transformer`
/// key names; see [`ConnectorContext::transformer`].
pub struct HttpSourceFactory;

impl SourceFactory for HttpSourceFactory {
    fn type_name(&self) -> &'static str {
        "HttpSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let url = required_url(config, "HttpSource")?;
        let transformer = ctx.transformer("HttpSource")?;
        Ok(Box::new(HttpSource::new(
            url,
            transformer,
            HttpSourceOptions {
                client: client_options(config, ctx, "HttpSource")?,
                declared: parse_optional_schema_fields(config, "HttpSource")?,
                schema_from: schema_from(config, "HttpSource")?,
                mode: read_mode(config)?,
                max_body_bytes: positive(config, "HttpSource", "max_body_bytes")?,
            },
        )?))
    }

    /// The source holds no state a rebuild would lose: a document mode
    /// instance re-issues the same GET, and a streaming one redials.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`HttpSink`].
///
/// Config fields:
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
/// - `schema_fields` (list, required): the Arrow schema each body is written
///   with.
///
/// The byte format is whatever transformer the `sink` node's `transformer` key
/// names; see [`ConnectorContext::transformer`].
pub struct HttpSinkFactory;

impl SinkFactory for HttpSinkFactory {
    fn type_name(&self) -> &'static str {
        "HttpSink"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let url = required_url(config, "HttpSink")?;
        let transformer = ctx.transformer("HttpSink")?;
        let schema = parse_schema_fields(config, "HttpSink")?;
        let method = match config.get("method") {
            None => DEFAULT_METHOD,
            Some(value) => value.as_str().ok_or_else(|| {
                SaciError::configuration("HttpSink config.method must be a string")
            })?,
        };
        Ok(Box::new(HttpSink::new(
            url,
            schema,
            transformer,
            HttpSinkOptions {
                client: client_options(config, ctx, "HttpSink")?,
                method: method.to_string(),
                content_encoding: content_encoding(config)?,
            },
        )?))
    }

    /// One self-contained request per batch, nothing buffered between them,
    /// so a fresh instance is indistinguishable from the one it replaced.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// Factory for [`HttpPushSource`].
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
/// - `schema_fields` (list, required): the Arrow schema every document is read
///   against.
/// - `schema_from` (`"config"` or `"body"`, optional, default `"config"`): as
///   on [`HttpSourceFactory`].
///
/// The byte format is whatever transformer the `source` node's `transformer`
/// key names; see [`ConnectorContext::transformer`].
pub struct HttpPushSourceFactory;

impl SourceFactory for HttpPushSourceFactory {
    fn type_name(&self) -> &'static str {
        "HttpPushSource"
    }

    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let what = "HttpPushSource";
        let bind = config.get("bind").and_then(|v| v.as_str()).ok_or_else(|| {
            SaciError::configuration(format!("{what} config requires a 'bind' string"))
        })?;
        let transformer = ctx.transformer(what)?;
        let declared = parse_schema_fields(config, what)?;

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
        let methods = push_methods(config, what)?;
        let tls = push_tls(config, what)?;
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
        let bearer = push_bearer(config, what)?;

        Ok(Box::new(HttpPushSource::new(
            bind,
            transformer,
            HttpPushOptions {
                declared,
                schema_from: schema_from(config, what)?,
                path,
                methods,
                max_body_bytes: positive(config, what, "max_body_bytes")?
                    .unwrap_or(DEFAULT_PUSH_BODY_BYTES as usize),
                // Lenient, like `tcp`'s and `udp`'s: a zero is a request for
                // no queueing at all, which the channel cannot express.
                buffer: config
                    .get("buffer")
                    .and_then(ConfigValue::as_i64)
                    .unwrap_or(DEFAULT_PUSH_BUFFER)
                    .max(1) as usize,
                tls,
                quic_bind,
                bearer,
                identity: ctx.identity(what).ok().cloned(),
            },
        )?))
    }

    /// The listener is dropped before a rebuild, so the replacement binds the
    /// same address, exactly as `TcpIngestSourceFactory`'s does.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Ok(())
    }
}

/// The `methods` key: one method name, or a list of them.
///
/// `saci_config`'s rule 3 means two or more arguments arrive as an array and
/// exactly one arrives as a scalar, so both shapes are read here.
fn push_methods(config: &ConfigValue, what: &str) -> Result<Vec<String>, SaciError> {
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

/// The push source's `tls` block: the pair, or neither.
fn push_tls(config: &ConfigValue, what: &str) -> Result<Option<ServerTls>, SaciError> {
    let files = tls_files(config, what)?;
    if files.ca_file.is_some() {
        return Err(SaciError::configuration(format!(
            "{what} config.tls['ca_file'] is not a listener key: a push source authenticates \
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
fn push_bearer(config: &ConfigValue, what: &str) -> Result<Option<String>, SaciError> {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use saci_connector::{ConfigMap, from_kdl_str};
    use saci_transformer::{Transformer, TransformerFactory};
    use saci_transformer_csv::CsvTransformerFactory;
    use saci_transformer_ndjson::NdjsonTransformerFactory;

    use super::*;

    fn csv_transformer() -> Arc<dyn Transformer> {
        CsvTransformerFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("csv transformer builds")
    }

    fn ndjson_transformer() -> Arc<dyn Transformer> {
        NdjsonTransformerFactory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("ndjson transformer builds")
    }

    fn empty_config() -> ConfigValue {
        ConfigValue::Object(ConfigMap::new())
    }

    fn config(raw: &str) -> ConfigValue {
        from_kdl_str(raw).expect("parse test config")
    }

    const SCHEMA: &str = "schema_fields \"id\" type=\"Int64\" nullable=#false\n";

    /// Build a source from `raw` and expect a configuration error whose
    /// message contains `fragment`.
    fn source_refuses(raw: &str, fragment: &str) {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        let Err(err) = HttpSourceFactory.build(&config(raw), &ctx) else {
            panic!("expected a refusal naming {fragment}");
        };
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains(fragment), "got: {err}");
    }

    /// The same for a sink.
    fn sink_refuses(raw: &str, fragment: &str) {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        let Err(err) = HttpSinkFactory.build(&config(raw), &ctx) else {
            panic!("expected a refusal naming {fragment}");
        };
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains(fragment), "got: {err}");
    }

    /// The same for a push source, which needs a message-capable format.
    fn push_refuses(raw: &str, fragment: &str) {
        let ctx = ConnectorContext::new(Some(ndjson_transformer()));
        let Err(err) = HttpPushSourceFactory.build(&config(raw), &ctx) else {
            panic!("expected a refusal naming {fragment}");
        };
        assert_eq!(err.category(), "configuration", "got: {err}");
        assert!(err.message().contains(fragment), "got: {err}");
    }

    #[test]
    fn the_type_names_match_the_config_type_key() {
        assert_eq!(HttpSourceFactory.type_name(), "HttpSource");
        assert_eq!(HttpSinkFactory.type_name(), "HttpSink");
        assert_eq!(HttpPushSourceFactory.type_name(), "HttpPushSource");
    }

    #[test]
    fn a_missing_url_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));

        let Err(err) = HttpSourceFactory.build(&empty_config(), &ctx) else {
            panic!("url is required");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "HttpSource config requires a 'url' string field"
        );

        let Err(err) = HttpSinkFactory.build(&empty_config(), &ctx) else {
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
        let ctx = ConnectorContext::new(Some(ndjson_transformer()));
        let Err(err) = HttpPushSourceFactory.build(&empty_config(), &ctx) else {
            panic!("a listener needs an address");
        };
        assert_eq!(
            err.message(),
            "HttpPushSource config requires a 'bind' string"
        );
    }

    /// A misspelling read as the default would project the body onto the
    /// declared schema instead of checking it, on the first batch, in
    /// production.
    #[test]
    fn an_unknown_schema_from_is_a_configuration_error() {
        source_refuses(
            &format!(
                "url \"https://example.invalid/orders.csv\"\nschema_from \"object\"\n{SCHEMA}"
            ),
            "HttpSource config.schema_from must be \"config\" or \"body\"",
        );
    }

    /// Only a parsed `"body"` reaches the constructor's demand for a declared
    /// schema, so this is what distinguishes it from the default.
    #[test]
    fn schema_from_body_reaches_the_source() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        source_refuses(
            "url \"https://example.invalid/orders.csv\"\nschema_from \"body\"",
            "schema_fields",
        );

        HttpSourceFactory
            .build(&config("url \"https://example.invalid/orders.csv\""), &ctx)
            .expect("the default takes no schema at all");
    }

    #[test]
    fn a_source_with_no_bound_transformer_is_a_configuration_error() {
        let ctx = ConnectorContext::new(None);
        let raw = format!("url \"http://127.0.0.1:1/data.csv\"\n{SCHEMA}");

        let Err(err) = HttpSourceFactory.build(&config(&raw), &ctx) else {
            panic!("a source that moves bytes needs a bound transformer");
        };
        assert_eq!(
            err.message(),
            "HttpSource moves bytes and needs a 'transformer' key naming a declared transformer"
        );
    }

    #[test]
    fn a_sink_with_no_bound_transformer_is_a_configuration_error() {
        let ctx = ConnectorContext::new(None);
        let raw = format!("url \"http://127.0.0.1:1/ingest\"\n{SCHEMA}");

        let Err(err) = HttpSinkFactory.build(&config(&raw), &ctx) else {
            panic!("a sink that moves bytes needs a bound transformer");
        };
        assert_eq!(
            err.message(),
            "HttpSink moves bytes and needs a 'transformer' key naming a declared transformer"
        );
    }

    #[test]
    fn a_sink_without_schema_fields_is_a_configuration_error() {
        sink_refuses("url \"http://127.0.0.1:1/ingest\"\n", "schema_fields");
    }

    /// The whole point of building without a request: an endpoint that does not
    /// exist still builds, so `validate` never depends on the network.
    #[test]
    fn both_halves_build_against_an_endpoint_that_is_down() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        let raw = format!(
            r#"
url "http://127.0.0.1:1/data.csv"
timeout_ms 500
headers {{
    accept "text/csv"
}}
{SCHEMA}"#
        );

        let source = HttpSourceFactory
            .build(&config(&raw), &ctx)
            .expect("source builds");
        assert_eq!(source.schema().fields().len(), 1);
        assert_eq!(source.schema().field(0).name(), "id");

        let sink = HttpSinkFactory
            .build(&config(&raw), &ctx)
            .expect("sink builds");
        assert_eq!(sink.schema().fields().len(), 1);
    }

    #[test]
    fn a_source_without_schema_fields_reports_an_empty_schema() {
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        let source = HttpSourceFactory
            .build(&config("url \"http://127.0.0.1:1/data.csv\"\n"), &ctx)
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
    fn an_invalid_method_is_rejected_by_the_sink_factory() {
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
        let ctx = ConnectorContext::new(Some(csv_transformer()));
        for version in ["auto", "1.1", "2"] {
            HttpSourceFactory
                .build(
                    &config(&format!(
                        "url \"http://127.0.0.1:1/data.csv\"\nhttp_version \"{version}\"\n{SCHEMA}"
                    )),
                    &ctx,
                )
                .unwrap_or_else(|e| panic!("'{version}' is a version, got: {e}"));
        }
        HttpSourceFactory
            .build(
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
        let ctx = ConnectorContext::new(Some(ndjson_transformer()));
        let Err(err) = HttpSourceFactory.build(
            &config(&format!(
                "url \"http://127.0.0.1:1/live\"\nmode kind=\"lines\" batch_size=0\n{SCHEMA}"
            )),
            &ctx,
        ) else {
            panic!("a window of zero frames never closes");
        };
        assert!(
            err.message()
                .contains("config.mode['batch_size'] must be an integer of at least 1"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_string_mode_event_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(ndjson_transformer()));
        let Err(err) = HttpSourceFactory.build(
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
        let ctx = ConnectorContext::new(Some(ndjson_transformer()));
        let Err(err) = HttpSourceFactory.build(
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
        let ctx = ConnectorContext::new(Some(ndjson_transformer()));
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
            let Err(err) = HttpSourceFactory.build(
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
    fn a_streaming_kind_without_schema_fields_is_a_configuration_error() {
        let ctx = ConnectorContext::new(Some(ndjson_transformer()));
        let Err(err) = HttpSourceFactory.build(
            &config("url \"http://127.0.0.1:1/live\"\nmode kind=\"events\""),
            &ctx,
        ) else {
            panic!("a message decoder is built before the first byte arrives");
        };
        assert!(
            err.message().contains("needs a 'schema_fields' list"),
            "got: {err}"
        );
    }

    #[test]
    fn schema_from_body_is_refused_next_to_a_streaming_kind() {
        let ctx = ConnectorContext::new(Some(ndjson_transformer()));
        let Err(err) = HttpSourceFactory.build(
            &config(&format!(
                "url \"http://127.0.0.1:1/live\"\nmode kind=\"lines\"\nschema_from \"body\"\n{SCHEMA}"
            )),
            &ctx,
        ) else {
            panic!("a live body's schema is the declared one");
        };
        assert_eq!(
            err.message(),
            "HttpSource: schema_from \"body\" applies to mode kind=\"document\" only"
        );
    }

    /// Every registered format has a message decoder, so the config-time
    /// refusal is reached through a stand-in declared here; it is what stops
    /// a streaming source from connecting, closing, and delivering nothing.
    #[test]
    fn a_format_with_no_message_decoder_is_refused_for_a_streaming_kind() {
        struct StreamOnly;

        impl Transformer for StreamOnly {
            fn format(&self) -> &'static str {
                "stream-only"
            }
        }

        let ctx = ConnectorContext::new(Some(Arc::new(StreamOnly)));
        let Err(err) = HttpSourceFactory.build(
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

    // ── the push source's keys ──────────────────────────────────────────────

    #[test]
    fn a_push_source_binds_and_reports_the_declared_schema() {
        let ctx = ConnectorContext::new(Some(ndjson_transformer()));
        let source = HttpPushSourceFactory
            .build(
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
        push_refuses(
            &format!("bind \"127.0.0.1:0\"\npath \"orders\"\n{SCHEMA}"),
            "config.path must be a string starting with '/'",
        );
    }

    #[test]
    fn a_non_string_method_in_the_list_is_a_configuration_error() {
        push_refuses(
            &format!("bind \"127.0.0.1:0\"\nmethods \"POST\" 7\n{SCHEMA}"),
            "config.methods must be one HTTP method name or a list of them",
        );
    }

    #[test]
    fn a_non_string_quic_bind_is_a_configuration_error() {
        push_refuses(
            &format!("bind \"127.0.0.1:0\"\nquic_bind 9700\n{SCHEMA}"),
            "config.quic_bind must be a string",
        );
    }

    #[test]
    fn quic_bind_without_tls_is_a_configuration_error() {
        push_refuses(
            &format!("bind \"127.0.0.1:0\"\nquic_bind \"127.0.0.1:0\"\n{SCHEMA}"),
            "quic_bind needs a 'tls' block",
        );
    }

    #[test]
    fn half_a_listener_tls_pair_is_a_configuration_error() {
        push_refuses(
            &format!("bind \"127.0.0.1:0\"\ntls {{\n cert_file \"c.pem\"\n}}\n{SCHEMA}"),
            "config.tls needs both 'cert_file' and 'key_file'",
        );
    }

    /// A listener has no peer to trust, and reading `ca_file` as if it did
    /// would look like client authentication that is not happening.
    #[test]
    fn a_ca_file_on_the_listener_is_a_configuration_error() {
        push_refuses(
            &format!("bind \"127.0.0.1:0\"\ntls {{\n ca_file \"ca.pem\"\n}}\n{SCHEMA}"),
            "is not a listener key",
        );
    }

    #[test]
    fn a_scalar_auth_block_is_a_configuration_error() {
        push_refuses(
            &format!("bind \"127.0.0.1:0\"\nauth \"token\"\n{SCHEMA}"),
            "config.auth must be a table",
        );
    }

    /// An `auth` block that is a table, just without the one key it exists
    /// for, names that key rather than the shape it already has.
    #[test]
    fn an_auth_block_without_a_bearer_key_is_a_configuration_error() {
        push_refuses(
            &format!("bind \"127.0.0.1:0\"\nauth {{\n token \"x\"\n}}\n{SCHEMA}"),
            "HttpPushSource config.auth needs a 'bearer' string",
        );
    }

    #[test]
    fn an_empty_bearer_token_is_a_configuration_error() {
        push_refuses(
            &format!("bind \"127.0.0.1:0\"\nauth {{\n bearer \"\"\n}}\n{SCHEMA}"),
            "config.auth.bearer must be a non-empty string",
        );
    }

    /// A zero cannot be expressed by a channel, so it is clamped rather than
    /// refused, matching `tcp` and `udp`.
    #[test]
    fn a_zero_buffer_is_clamped_rather_than_refused() {
        let ctx = ConnectorContext::new(Some(ndjson_transformer()));
        HttpPushSourceFactory
            .build(
                &config(&format!("bind \"127.0.0.1:0\"\nbuffer 0\n{SCHEMA}")),
                &ctx,
            )
            .expect("buffer is the one lenient key");
    }

    #[test]
    fn a_push_source_is_rebuildable() {
        HttpPushSourceFactory
            .rebuildable(&empty_config())
            .expect("the listener is dropped before a rebuild");
    }
}
