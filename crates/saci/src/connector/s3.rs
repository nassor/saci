//! The S3 connector: an S3 [`Source`] and [`Sink`] for SACI.
//!
//! [`Source`]: crate::io::Source
//! [`Sink`]: crate::io::Sink
//!
//! This module moves object bytes over any S3-compatible endpoint (AWS, MinIO,
//! R2, Ceph RGW, RustFS) and nothing else: the object client, the row/byte/age
//! flush thresholds, and the timestamped key layout. The byte format comes from
//! a [`Codec`](crate::codec::Codec), resolved by the host from
//! the `codec` key naming a declared `codec` node, so csv, ndjson
//! and parquet are one connector rather than three.
//!
//! A source lists its prefix once and streams each object through a
//! codec, reaching EOF when the listing is exhausted. A sink accumulates
//! encoded rows in memory and uploads them as one object when a flush threshold
//! fires — enough rows, enough encoded bytes, or enough wall-clock time since
//! the object took its first batch. Every object key is prefixed with a
//! lexicographically sortable UTC timestamp, so a listing replays objects in
//! the order they were written.
//!
//! ```kdl
//! codec "orders-csv" name="Orders CSV" format="csv" {
//!     config has_headers=#true
//! }
//!
//! source "orders_in" connector="s3" codec="orders-csv" component="Order" {
//!     config prefix="incoming/orders" {
//!         connection bucket="saci-orders" endpoint="http://127.0.0.1:9000" allow_http=#true
//!         field "id" type="int64" nullable=#false
//!     }
//! }
//! ```
#![deny(missing_docs)]

pub mod config;
pub mod factory;
pub mod sink;
pub mod source;

mod key;

pub use config::{ConnectionConfig, Flush, S3SinkConfig, S3SourceConfig};
pub use factory::S3Connector;
pub use sink::S3Sink;
pub use source::S3Source;
