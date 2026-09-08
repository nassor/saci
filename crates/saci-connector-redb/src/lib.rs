//! `saci-connector-redb`: an embedded [redb] [`Source`] and [`Sink`] for SACI.
//!
//! [redb]: https://github.com/cberner/redb
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! One redb file under a directory the config names, one table inside it, one
//! entry per batch. Each value is a self-contained document in whatever byte
//! format the node's [`Transformer`](saci_transformer::Transformer) supplies,
//! so csv, ndjson, parquet, avro and Arrow IPC are one connector rather than
//! five. Keys are `{key_prefix}{seq:020}{key_suffix}`, so the table's own
//! lexicographic order is insertion order, and a reopened sink resumes the
//! sequence from the highest key already in the file.
//!
//! redb locks the file for a database handle's lifetime: the sink's write
//! handle takes an exclusive OS lock, the source's read-only handle a shared
//! one. Several sources may therefore read one file at once, while a sink
//! excludes every other handle.
//! [`RedbSink::finish`](saci_core::io::sink::Sink::finish) drops its database
//! and releases the lock; [`RedbSource`] opens the file on its first batch and
//! drops it at EOF.
//!
//! ```kdl
//! transformer "orders-csv" name="Orders CSV" format="csv" {
//!     options has_headers=#true
//! }
//!
//! sink "orders_out" type="RedbSink" transformer="orders-csv" component="Order" {
//!     config {
//!         directory "/data/orders"
//!         file "orders.redb"
//!         key_prefix "orders/"
//!         key_suffix ".csv"
//!         schema_fields "id" type="Int64" nullable=#false
//!     }
//! }
//! ```

#![deny(missing_docs)]

pub mod config;
pub mod factory;
mod key;
pub mod sink;
pub mod source;

pub use config::{DurabilityMode, RedbSinkConfig, RedbSourceConfig};
pub use factory::{RedbSinkFactory, RedbSourceFactory};
pub use sink::RedbSink;
pub use source::RedbSource;
