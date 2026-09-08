//! `saci-connector-kafka`: a Kafka [`Source`] and [`Sink`] for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! [`KafkaSource`] consumes a topic (or several) with `rdkafka`'s
//! `StreamConsumer`; [`KafkaSink`] produces to one with a `FutureProducer`.
//! What a message payload means is the
//! [`Transformer`](saci_transformer::Transformer) the node's `transformer` key
//! names, and its [`MessageShape`](saci_transformer::MessageShape) decides
//! whether a batch becomes one message per row or one message in total.
//!
//! Both sides are declarative: the Arrow schema is written in the service
//! configuration, not introspected, because
//! [`SourceFactory::build`]/[`SinkFactory::build`] are synchronous and open no
//! connection.
//!
//! [`SourceFactory::build`]: saci_connector::factory::SourceFactory::build
//! [`SinkFactory::build`]: saci_connector::factory::SinkFactory::build
//!
//! # Delivery semantics
//!
//! At-least-once. `Source` has no ack hook, so `KafkaSource` commits the offsets
//! of the previous batch at the start of the next `next_batch` call, not when
//! the batch is handed over. A crash between the two replays that batch.
//!
//! # Transport
//!
//! Plaintext only. TLS and SASL need librdkafka features this crate does not
//! enable; a build that needs them turns on `rdkafka/ssl-vendored` or
//! `rdkafka/sasl` itself.

#![deny(missing_docs)]

pub mod admin;
pub mod config;
pub mod factory;
pub mod sink;
pub mod source;

pub use config::{KafkaSinkConfig, KafkaSourceConfig, TopicProvision};
pub use factory::{KafkaSinkFactory, KafkaSourceFactory};
pub use sink::KafkaSink;
pub use source::KafkaSource;
