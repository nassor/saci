//! `saci-transformer-parquet`: the `parquet` byte format for SACI.
//!
//! Parquet is self-describing, so [`ParquetTransformer`] reads its schema from
//! the file footer unless the config declared one, which becomes a column
//! projection pushed into the reader. It is also the only format that reports
//! `estimated_rows`, summed from row-group metadata without reading any data.
//! Writes use Snappy compression.
//!
//! The message surface is one whole file per payload, so the shape is
//! `PerBatch`: a Parquet file's footer is what makes it readable, and a
//! payload has to carry it.
//!
//! ```kdl
//! source "trades" type="FileSource" {
//!     config path="/data/trades.parquet" format="parquet"
//! }
//! ```

#![deny(missing_docs)]

pub mod transformer;

pub use transformer::{ParquetTransformer, ParquetTransformerFactory};
