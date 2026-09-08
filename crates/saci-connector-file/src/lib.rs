//! `saci-connector-file`: a local-file [`Source`] and [`Sink`] for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! This crate owns local-file IO and nothing else: the handle, the reader
//! thread, and the channel [`FileSource::next_batch`](saci_core::io::source::Source::next_batch) awaits so the async
//! executor never blocks on the disk. The byte format comes from a
//! [`Transformer`](saci_transformer::Transformer), resolved by the host from
//! the `transformer` key naming a declared `transformer` node, so csv, ndjson
//! and parquet are one connector rather than three.
//!
//! ```kdl
//! transformer "orders-csv" name="Orders CSV" format="csv" {
//!     options has_headers=#true
//! }
//!
//! source "orders_in" type="FileSource" transformer="orders-csv" component="Order" {
//!     config path="/data/orders.csv" {
//!         schema_fields "id" type="Int64" nullable=#false
//!     }
//! }
//! ```

#![deny(missing_docs)]

pub mod factory;
pub mod sink;
pub mod source;

pub use factory::{FileSinkFactory, FileSourceFactory};
pub use sink::FileSink;
pub use source::FileSource;
