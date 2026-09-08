//! `saci-transformer-avro`: the `avro` byte format for SACI.
//!
//! Both surfaces are implemented. The stream surface is an Avro **object
//! container file**: a header carrying the schema, then compressed blocks of
//! records. The message surface is one Avro record per payload, which is what a
//! Kafka topic or a TCP frame carries.
//!
//! A container file describes itself, so [`AvroTransformer`] takes the file's
//! schema when reading one unless the config declared one, which projects the
//! file down to those columns; writing takes the schema it is handed.
//! Everything on the message surface uses the declared schema: a payload
//! carries a fingerprint, not a schema.
//!
//! Avro is a wider type system than Arrow in one direction and a narrower one in
//! the other: `int8`, `uint8` and `uint16` all travel as Avro `int`. A decoded
//! batch is therefore cast to the declared columns, so a declared `int8`
//! narrows back and a value that does not fit is an error rather than a silent
//! null.
//!
//! `compression` applies to the container-file writer only. `null`, `deflate`,
//! `snappy` and `zstd` are written and read; a file written elsewhere with
//! `bzip2` or `xz` fails on read naming the codec.
//!
//! Message framing is detected per payload rather than fixed by config. A
//! payload starting `0xC3` is Avro single-object encoding, one starting `0x00`
//! is Confluent-framed. Single-object framing is always accepted; Confluent
//! framing is accepted only when `schema_id` names the registry id to resolve,
//! and rejected naming that option otherwise. One stream may therefore carry
//! both, though every record inside one payload shares that payload's framing.
//!
//! `schema_id` also picks what writing emits: Confluent framing under that id
//! when set, single-object framing with a Rabin fingerprint of the schema
//! otherwise. A single-object producer and its consumer must declare the same
//! columns, because the fingerprint is over the schema itself.
//!
//! ```kdl
//! transformer "avro_fmt" format="avro" {
//!     options schema_id=42
//! }
//!
//! source "orders_file" type="FileSource" component="Order" transformer="avro_fmt" {
//!     config path="/data/orders.avro"
//! }
//!
//! sink "orders_topic" type="KafkaSink" component="Order" transformer="avro_fmt" {
//!     config topic="orders"
//! }
//! ```

#![deny(missing_docs)]

pub mod transformer;

pub use arrow_avro::compression::CompressionCodec;
pub use transformer::{AvroTransformer, AvroTransformerFactory};
