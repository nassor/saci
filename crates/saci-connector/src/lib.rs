//! Factory contract every SACI connector implements.
//!
//! [`SourceFactory`] and [`SinkFactory`] build a
//! [`Source`](saci_core::io::source::Source) or a
//! [`Sink`](saci_core::io::sink::Sink) from one opaque `config` table. They
//! live below `saci-service` so a connector crate can implement them without
//! depending on the host that loads it.
//!
//! That table is a [`ConfigValue`], the tree `saci-config` parses the KDL
//! configuration file into. It is re-exported here, together with
//! [`from_kdl_str`] and [`one_or_many`], so a connector crate depends on
//! `saci-connector` alone.
//!
//! [`ConnectorContext`] is what a factory can reach while it builds: the
//! transformer the config bound to this connector instance, if any, resolved
//! by the host against the [`TransformerRegistry`](saci_transformer::TransformerRegistry)
//! once per workflow build, and the [`NodeIdentity`] naming the service,
//! workflow and node it was declared on. A connector that moves bytes never
//! owns a list of byte formats or a `format` config key of its own.
//!
//! [`parse_schema_fields`] reads the `schema_fields` entries a connector
//! declares its Arrow schema with when the format carries no schema of its
//! own; [`parse_optional_schema_fields`] is the same read for a connector
//! whose format may carry one.

pub mod channel;
pub mod context;
pub mod factory;
pub mod schema;

pub use channel::ChannelBridge;
pub use context::{ConnectorContext, NodeIdentity};
pub use factory::{REBUILD_UNDECLARED, SinkFactory, SourceFactory};
pub use saci_config::{ConfigMap, ConfigValue, from_kdl_str, one_or_many};
pub use schema::{parse_optional_schema_fields, parse_schema_fields};
