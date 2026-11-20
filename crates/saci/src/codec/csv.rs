//! The `csv` byte format for SACI.
//!
//! CSV carries no schema, so [`CsvCodec`] requires a declared one on the
//! read side and writes exactly the schema it is handed on the write side. The
//! only option is `has_headers`, which skips a first row of column names while
//! reading and emits one while writing.
//!
//! Stream surface only: a CSV file has a beginning and an end, and one CSV row
//! is not a self-contained message. A `format="csv"` on a message transport
//! fails at build with the codec contract's `unsupported` error.
//!
//! ```kdl
//! transformer "csv_fmt" format="csv" {
//!     options has_headers=#true
//! }
//!
//! source "orders" type="FileSource" component="Order" transformer="csv_fmt" {
//!     config {
//!         path "/data/orders.csv"
//!     }
//! }
//! ```

#![deny(missing_docs)]

mod codec;

pub use codec::{CsvCodec, CsvCodecFactory};
