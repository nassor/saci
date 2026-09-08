//! `saci-transformer-ndjson`: the `ndjson` byte format for SACI.
//!
//! One JSON object per line, read and written through `arrow-json`.
//!
//! [`NdjsonTransformer`] implements both surfaces. As a stream it reads a file
//! against a declared schema, or infers one from the first `infer_max` records
//! when none is declared, and writes one object per row. As a message codec it
//! decodes a window of payloads into one batch and encodes one message per row,
//! which is what a Kafka topic of JSON records needs.
//!
//! ```kdl
//! transformer "ndjson_fmt" format="ndjson" {
//!     options infer_max=1024
//! }
//!
//! source "orders" type="FileSource" component="Order" transformer="ndjson_fmt" {
//!     config {
//!         path "/data/orders.ndjson"
//!     }
//! }
//! ```

#![deny(missing_docs)]

pub mod transformer;

pub use transformer::{NdjsonTransformer, NdjsonTransformerFactory};
