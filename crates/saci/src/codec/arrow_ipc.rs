//! The `arrow-ipc` byte format for SACI.
//!
//! One payload, and one whole file, is one Arrow IPC stream: a schema message
//! followed by one message per `RecordBatch` and an end-of-stream marker. This
//! is the format a TCP frame and an Arrow-native Kafka topic carry, and it is
//! the only one that needs no schema declaration on the wire while still
//! projecting what it decodes onto the schema it was given.
//!
//! Both surfaces, on the one encapsulation. The message surface splits
//! `PerBatch`; the stream surface reads and writes the same bytes end to end,
//! so a `file`, `http` or `s3` node carries them too and the stream a sink
//! wrote decodes through the message decoder unchanged.
//!
//! ```kdl
//! codec "ipc" format="arrow-ipc"
//!
//! source "wire" connector="tcp" codec="ipc" component="Reading" {
//!     config bind="0.0.0.0:9500" {
//!         field "v" type="int64" nullable=#false
//!     }
//! }
//! ```

#![deny(missing_docs)]

mod codec;

pub use codec::{ArrowIpcCodec, ArrowIpcCodecFactory};
