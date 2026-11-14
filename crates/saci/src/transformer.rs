//! The transformer contract: the byte formats SACI connectors read and write.
//!
//! A connector moves bytes; a [`Transformer`] gives them meaning. A file
//! connector opens a handle and owns the reader thread, a TCP connector frames
//! payloads off a socket, a Kafka connector consumes messages: all three then
//! hand the bytes to a transformer selected by one `format` key in the same
//! `config` table.
//!
//! A transformer has two surfaces and need not implement both:
//!
//! - the stream surface, [`BatchReader`] and [`BatchWriter`], for a byte stream
//!   with a start and an end, which is what a file connector uses;
//! - the message surface, [`MessageDecoder`] and
//!   [`Transformer::encode_messages`], for discrete payloads, which is what TCP
//!   and Kafka use.
//!
//! Every capability a format does not have returns [`unsupported`], so a
//! mismatch is a configuration error naming the format and the capability
//! rather than a compile-time absence.
//!
//! [`TransformerRegistry`] maps a `format` name to a [`TransformerFactory`],
//! mirroring the host's source and sink registry. It lives outside
//! [`crate::connector`] because connectors resolve formats at build time. A
//! factory reads its options out of a [`ConfigValue`], the configuration tree
//! [`crate::config`] parses KDL into; it is re-exported here so an implementor
//! names one path.
//!
//! Each format is a child module behind its own `transformer-<format>`
//! feature.

#![deny(missing_docs)]

pub mod batch;
pub mod contract;
pub mod message;
pub mod registry;

pub use crate::config::{ConfigMap, ConfigValue, from_kdl_str, one_or_many};
pub use batch::{BatchReader, BatchWriter};
pub use contract::{Transformer, unsupported};
pub use message::{MessageDecoder, MessageShape};
pub use registry::{TransformerFactory, TransformerRegistry};

// The dead letter queue names the Arrow IPC transformer directly, so `service`
// compiles it; registering it as a `format` stays behind its own feature.
#[cfg(any(feature = "transformer-arrow-ipc", feature = "service"))]
pub mod arrow_ipc;
#[cfg(feature = "transformer-avro")]
pub mod avro;
#[cfg(feature = "transformer-csv")]
pub mod csv;
#[cfg(feature = "transformer-ndjson")]
pub mod ndjson;
#[cfg(feature = "transformer-parquet")]
pub mod parquet;
