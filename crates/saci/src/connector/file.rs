//! The file connector: a local-file [`Source`] and [`Sink`] for SACI.
//!
//! [`Source`]: crate::io::Source
//! [`Sink`]: crate::io::Sink
//!
//! This module owns local-file IO and nothing else: the handle, the reader
//! thread, and the channel [`FileSource::next_batch`](crate::io::Source::next_batch) awaits so the async
//! executor never blocks on the disk. The byte format comes from a
//! [`Codec`](crate::codec::Codec), resolved by the host from
//! the `codec` key naming a declared `codec` node, so csv, ndjson
//! and parquet are one connector rather than three.
//!
//! ```kdl
//! codec "orders-csv" name="Orders CSV" format="csv" {
//!     config has_headers=#true
//! }
//!
//! source "orders_in" connector="file" codec="orders-csv" component="Order" {
//!     config path="/data/orders.csv" {
//!         field "id" type="int64" nullable=#false
//!     }
//! }
//! ```

#![deny(missing_docs)]

/// The name this connector registers under and a config's `connector` property selects.
pub const NAME: &str = "file";

mod config;

pub mod factory;
pub mod sink;
pub mod source;

pub use config::{FileSinkConfig, FileSourceConfig};
pub use factory::FileConnector;
pub use sink::FileSink;
pub use source::FileSource;
