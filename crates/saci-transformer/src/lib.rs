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
//! mirroring the host's source and sink registry. It lives below
//! `saci-connector` because connectors resolve formats at build time. A
//! factory reads its options out of a [`ConfigValue`], the configuration tree
//! `saci-config` parses KDL into; it is re-exported here so an implementor
//! names one path.

#![deny(missing_docs)]

pub mod batch;
pub mod message;
pub mod registry;
pub mod transformer;

pub use batch::{BatchReader, BatchWriter};
pub use message::{MessageDecoder, MessageShape};
pub use registry::{TransformerFactory, TransformerRegistry};
pub use saci_config::{ConfigMap, ConfigValue, from_kdl_str, one_or_many};
pub use transformer::{Transformer, unsupported};
