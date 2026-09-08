//! `saci-connector-s3`: an S3 [`Source`] and [`Sink`] for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! This crate moves object bytes over any S3-compatible endpoint (AWS, MinIO,
//! R2, Ceph RGW, RustFS) and nothing else: the object client, the row/byte/age
//! flush thresholds, and the timestamped key layout. The byte format comes from
//! a [`Transformer`](saci_transformer::Transformer), resolved by the host from
//! the `transformer` key naming a declared `transformer` node, so csv, ndjson
//! and parquet are one connector rather than three.
//!
//! A source lists its prefix once and streams each object through a
//! transformer, reaching EOF when the listing is exhausted. A sink accumulates
//! encoded rows in memory and uploads them as one object when a flush threshold
//! fires — enough rows, enough encoded bytes, or enough wall-clock time since
//! the object took its first batch. Every object key is prefixed with a
//! lexicographically sortable UTC timestamp, so a listing replays objects in
//! the order they were written.
//!
//! ```kdl
//! transformer "orders-csv" name="Orders CSV" format="csv" {
//!     options has_headers=#true
//! }
//!
//! source "orders_in" type="S3Source" transformer="orders-csv" component="Order" {
//!     config prefix="incoming/orders" {
//!         connection bucket="saci-orders" endpoint="http://127.0.0.1:9000" allow_http=#true
//!         schema_fields "id" type="Int64" nullable=#false
//!     }
//! }
//! ```
#![deny(missing_docs)]

pub mod config;
pub mod factory;
pub mod sink;
pub mod source;

mod key;

pub use config::{Flush, S3ConnectionConfig, S3SinkConfig, S3SourceConfig, SchemaFrom};
pub use factory::{S3SinkFactory, S3SourceFactory};
pub use sink::S3Sink;
pub use source::S3Source;
