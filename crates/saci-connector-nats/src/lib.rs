//! `saci-connector-nats`: a NATS [`Source`] and [`Sink`] for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! One `mode` child node picks which NATS the connector speaks.
//! `kind="core"` is plain subject pub/sub with no persistence;
//! `kind="jetstream"` is a durable stream, read through a pull consumer and
//! written with publish acks. What a message payload means is the
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
//! JetStream is at-least-once. `Source` has no ack hook, so [`NatsSource`]
//! acknowledges the previous batch at the start of the next `next_batch` call,
//! not when the batch is handed over. A crash between the two redelivers that
//! batch. [`NatsSink`] waits for every publish ack by default, so a returned
//! `write_batch` means the stream holds the rows.
//!
//! Core NATS is at-most-once and has no ack at all: a message consumed while the
//! pipeline later fails is gone, and a publish is acknowledged only by the
//! connection-wide flush `write_batch` ends with. A `queue_group` spreads one
//! core subject across several SACI instances.
//!
//! # Transport
//!
//! `nats://`, `tls://`, `ws://` and `wss://` server URLs, and a bare
//! `host:port` meaning `nats://host:port`. TLS is configured under the
//! `connection`'s own `tls` node, including a root bundle and a client
//! certificate for mutual TLS. Its `auth` node covers a token, a user and
//! password, an NKey seed and a `.creds` file, each with a `_file` form for a
//! secret mount.

#![deny(missing_docs)]

pub mod config;
pub mod connect;
pub mod factory;
pub mod provision;
pub mod render;
pub mod sink;
pub mod source;

pub use config::{
    AckPolicyConfig, AuthConfig, CompressionConfig, ConnectionConfig, CoreSinkMode, CoreSourceMode,
    DeliverPolicyConfig, DiscardConfig, JetstreamSinkMode, JetstreamSourceMode, NatsSinkConfig,
    NatsSourceConfig, ReplayPolicyConfig, RetentionConfig, SinkMode, SourceMode, StorageConfig,
    StreamProvision, TlsConfig,
};
pub use factory::{NatsSinkFactory, NatsSourceFactory};
pub use sink::NatsSink;
pub use source::NatsSource;
