//! `saci-connector-tcp`: a live TCP [`Source`] and a client-mode TCP [`Sink`]
//! for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! [`TcpIngestSource`] listens on `bind` and yields one `RecordBatch` per
//! received frame. [`TcpSink`] dials `connect` and writes one frame per encoded
//! message. A frame is a `u32` big-endian length prefix followed by that many
//! payload bytes, so the two halves are wire compatible; the
//! [`Transformer`](saci_transformer::Transformer) the host resolved from the
//! node's `transformer` key decodes or encodes them.
//!
//! Framing is transport and belongs here; decoding is format and does not. That
//! split is why the same listener carries Arrow IPC or newline-delimited JSON
//! without a line of code in this crate knowing either.
//!
//! The source never reaches EOF, so only the stream runner can drive it. Its
//! `config` table takes `bind`, an optional `buffer` and `max_frame_bytes`, and
//! a `schema_fields` array declaring the Arrow schema. The sink's takes
//! `connect` and `schema_fields`, and it runs under any run mode.
//!
//! ```kdl
//! transformer "frames-ipc" name="Frames Arrow IPC" format="arrow-ipc"
//!
//! source "ingest" type="tcp" transformer="frames-ipc" component="Reading" {
//!     config bind="0.0.0.0:9500" max_frame_bytes=8388608 {
//!         schema_fields "v" type="Int64" nullable=#false
//!     }
//! }
//!
//! sink "forward" type="tcp" transformer="frames-ipc" component="Reading" {
//!     config connect="collector.internal:9600" {
//!         schema_fields "v" type="Int64" nullable=#false
//!     }
//! }
//! ```

pub mod factory;
pub mod sink;
pub mod source;

pub use factory::{TcpSinkFactory, TcpSourceFactory};
pub use sink::TcpSink;
pub use source::TcpIngestSource;
