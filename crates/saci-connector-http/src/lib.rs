//! `saci-connector-http`: an HTTP and HTTPS [`Source`] and [`Sink`] for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! [`HttpSource`] is one GET. The response body is spooled to a temp file,
//! decoded through the transformer's stream read surface, and the source
//! reports EOF when that stream ends, so every run mode can drive it.
//! [`HttpSink`] is one request per batch, where the body is a self-contained
//! document the transformer writes: a csv with its header row, a block of
//! ndjson, one whole parquet or avro container.
//!
//! Neither half touches the network while it is built, so `saci-service
//! validate` needs no reachable endpoint. HTTPS works with no configuration:
//! rustls verifies the peer against the platform trust store, and there is no
//! key to turn that off.
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
//!         schema_fields "id" type="Int64" nullable=#false
//!     }
//! }
//! ```

#![deny(missing_docs)]

mod client;
pub mod factory;
pub mod sink;
pub mod source;

pub use factory::{HttpSinkFactory, HttpSourceFactory};
pub use sink::HttpSink;
pub use source::{HttpSource, SchemaFrom};
