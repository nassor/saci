//! The codec contract: the byte formats SACI connectors read and write.
//!
//! A connector moves bytes; a [`Codec`] gives them meaning. A file
//! connector opens a handle and owns the reader thread, a TCP connector frames
//! payloads off a socket, a Kafka connector consumes messages: all three then
//! hand the bytes to a codec selected by one `format` key in the same
//! `config` table.
//!
//! A codec has two surfaces and need not implement both:
//!
//! - the stream surface, [`BatchReader`] and [`BatchWriter`], for a byte stream
//!   with a start and an end, which is what a file connector uses;
//! - the message surface, [`MessageDecoder`] and
//!   [`Codec::encode_messages`], for discrete payloads, which is what TCP
//!   and Kafka use.
//!
//! Every capability a format does not have returns [`unsupported`], so a
//! mismatch is a configuration error naming the format and the capability
//! rather than a compile-time absence.
//!
//! [`CodecRegistry`] maps a `format` name to a [`CodecFactory`],
//! mirroring the host's source and sink registry. It lives outside
//! [`crate::connector`] because connectors resolve formats at build time. A
//! factory reads its options out of a [`ConfigValue`], the configuration tree
//! [`crate::config`] parses KDL into; it is re-exported here so an implementor
//! names one path.
//!
//! Each format is a child module behind its own `codec-<format>`
//! feature.

#![deny(missing_docs)]

pub mod batch;
pub mod contract;
pub mod message;
pub mod registry;

pub use crate::config::{ConfigMap, ConfigValue, from_kdl_str, one_or_many};
pub use crate::io::CorruptRecord;
pub use batch::{BatchReader, BatchWriter};
pub use contract::{Codec, unsupported};
pub use message::{MessageDecoder, MessageShape};
pub use registry::{CodecFactory, CodecRegistry};

/// The bytes [`Codec::open_reader`] reads: an open file, or a document already
/// in memory.
///
/// A codec reads either variant through [`Read`](std::io::Read) and
/// [`Seek`](std::io::Seek), so a connector that holds a whole document in memory
/// (a redb value, an HTTP request body)
/// hands it over as it is instead of spooling it to a temporary file. A
/// connector that streams a large document to disk passes the spool file.
/// Either way the input is positioned at the start of the document; build one
/// with [`From`].
#[derive(Debug)]
pub enum ReadInput {
    /// An open file, positioned at the start of the document.
    File(std::fs::File),
    /// A document held in memory, positioned at its first byte.
    Memory(std::io::Cursor<Vec<u8>>),
}

impl std::io::Read for ReadInput {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(file) => file.read(buf),
            Self::Memory(cursor) => cursor.read(buf),
        }
    }
}

impl std::io::Seek for ReadInput {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        match self {
            Self::File(file) => file.seek(pos),
            Self::Memory(cursor) => cursor.seek(pos),
        }
    }
}

impl From<std::fs::File> for ReadInput {
    fn from(file: std::fs::File) -> Self {
        Self::File(file)
    }
}

impl From<Vec<u8>> for ReadInput {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Memory(std::io::Cursor::new(bytes))
    }
}

// The dead letter queue names the Arrow IPC codec directly, so `service`
// compiles it; registering it as a `format` stays behind its own feature.
#[cfg(any(feature = "codec-arrow-ipc", feature = "service"))]
pub mod arrow_ipc;
#[cfg(any(feature = "codec-arrow-ipc", feature = "service"))]
pub use arrow_ipc::ArrowIpcCodec;
#[cfg(feature = "codec-avro")]
pub mod avro;
#[cfg(feature = "codec-avro")]
pub use avro::AvroCodec;
#[cfg(feature = "codec-csv")]
pub mod csv;
#[cfg(feature = "codec-csv")]
pub use csv::CsvCodec;
#[cfg(feature = "codec-ndjson")]
pub mod ndjson;
#[cfg(feature = "codec-ndjson")]
pub use ndjson::NdjsonCodec;
#[cfg(feature = "codec-parquet")]
pub mod parquet;
#[cfg(feature = "codec-parquet")]
pub use parquet::ParquetCodec;
