//! `saci-connector-http`: HTTP and HTTPS [`Source`]s and a [`Sink`] for SACI,
//! over HTTP/1.1, HTTP/2 or HTTP/3.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! Three node types, two of which dial out and one of which listens.
//!
//! [`HttpSource`] reads. Its default `mode kind="document"` is one GET of a
//! finite resource: the body is spooled to a temp file as it arrives, decoded
//! through the transformer's stream read surface, and the source reports EOF
//! when that stream ends and re-arms, so every run mode can drive it and
//! `interval` re-reads the resource per tick. `pagination "link"` follows
//! `Link` headers across pages first, and `conditional #true` sends the last
//! response's validators so an unchanged resource answers `304`.
//! `mode kind="lines"` and `mode kind="events"` instead read a body that never
//! ends, newline-delimited or `text/event-stream`, one batch per window of
//! frames; both are live sources for `run_mode kind="stream"`.
//!
//! [`HttpSink`] writes: one request per batch, where the body is a
//! self-contained document the transformer writes, a csv with its header row,
//! a block of ndjson, one whole parquet or avro container. The `content-type`
//! follows from the format unless `headers` names one, and `content_encoding`
//! compresses the whole body.
//!
//! [`HttpPushSource`] listens. It accepts the same documents [`HttpSink`]
//! sends, on one path and one set of methods, optionally behind a bearer token,
//! and answers `202` once every batch of a document is queued. One TCP listener
//! serves HTTP/1.1 and HTTP/2 (h2c by prior knowledge, or `h2` over ALPN under
//! `tls`), and `quic_bind` adds HTTP/3.
//!
//! No half touches the network while it is built, so `saci-service validate`
//! needs no reachable endpoint; a push source does bind its sockets, the way
//! the `tcp` and `udp` sources do. HTTPS works with no configuration: rustls
//! verifies the peer against the platform trust store, and the optional `tls`
//! block only adds a private CA to it or presents a client certificate.
//! Response bodies are decompressed transparently, and a `headers` entry for
//! `accept-encoding` is how a source asks for that not to happen.
//!
//! The byte format comes from a [`Transformer`](saci_transformer::Transformer),
//! resolved by the host from the `transformer` key naming a declared
//! `transformer` node, so csv, ndjson, parquet and avro are one connector
//! rather than four.
//!
//! ```kdl
//! transformer "orders_csv" name="Orders CSV" format="csv" {
//!     options has_headers=#true
//! }
//!
//! source "orders_in" type="HttpSource" transformer="orders_csv" component="Order" {
//!     config {
//!         url "https://data.internal/orders.csv"
//!         http_version "2"
//!         conditional #true
//!         headers {
//!             authorization "Bearer ${ORDERS_TOKEN}"
//!         }
//!         schema_fields "id" type="Int64" nullable=#false
//!     }
//! }
//!
//! sink "orders_out" type="HttpSink" transformer="orders_csv" component="Order" {
//!     config {
//!         url "https://sink.internal/orders"
//!         method "POST"
//!         content_encoding "zstd"
//!         schema_fields "id" type="Int64" nullable=#false
//!     }
//! }
//! ```

#![deny(missing_docs)]

mod client;
mod document;
pub mod factory;
mod framing;
mod h3client;
mod metrics;
pub mod push;
pub mod sink;
pub mod source;
mod tls;

pub use client::HttpVersion;
pub use factory::{HttpPushSourceFactory, HttpSinkFactory, HttpSourceFactory};
pub use push::{HttpPushOptions, HttpPushSource, ServerTls};
pub use sink::{ContentEncoding, HttpSink, HttpSinkOptions};
pub use source::{
    HttpClientOptions, HttpSource, HttpSourceOptions, Pagination, ReadMode, SchemaFrom, Window,
};
pub use tls::TlsFiles;
