//! The `ndjson` byte format for SACI.
//!
//! One JSON object per line, read and written through `arrow-json`.
//!
//! [`NdjsonCodec`] implements both surfaces. As a stream it reads a file
//! against a declared schema, or infers one from the first `infer_max` records
//! when none is declared, and writes one object per row. As a message codec it
//! decodes a window of payloads into one batch and encodes one message per row,
//! which is what a Kafka topic of JSON records needs.
//!
//! ```kdl
//! codec "ndjson_fmt" format="ndjson" {
//!     config infer_max=1024
//! }
//!
//! source "orders" connector="file" component="Order" codec="ndjson_fmt" {
//!     config {
//!         path "/data/orders.ndjson"
//!     }
//! }
//! ```

#![deny(missing_docs)]

mod codec;

pub use codec::{NdjsonCodec, NdjsonCodecFactory};
