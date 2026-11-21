//! The [`Connector`] contract every SACI connector implements, and the connectors.
//!
//! A [`Connector`] builds a [`Source`](crate::io::Source), a
//! [`Sink`](crate::io::Sink) or both from one opaque `config` table, and
//! reports in one place whether the host may rebuild it to heal it
//! ([`Rebuild`]), whether a source built from a config ever ends
//! ([`Connector::live`]) and which config keys the dashboard may show
//! ([`Connector::detail_keys`]). It lives outside `service` so a connector
//! implements it without the host that loads it.
//!
//! That table is a [`ConfigValue`], the tree [`crate::config`] parses the
//! KDL configuration file into. It is re-exported here, together with
//! [`from_kdl_str`] and [`one_or_many`], so a connector names one path.
//!
//! [`ConnectorContext`] is what a connector can reach while it builds: the
//! codec the config bound to this connector instance, if any, resolved
//! by the host against the [`CodecRegistry`](crate::codec::CodecRegistry)
//! once per workflow build, and the [`NodeIdentity`] naming the service,
//! workflow and node it was declared on. A connector that moves bytes never
//! owns a list of byte formats or a `format` config key of its own.
//!
//! [`parse_fields`] reads the `field` entries a connector declares its Arrow
//! schema with when the format carries no schema of its own;
//! [`parse_optional_fields`] is the same read for a connector whose format may
//! carry one, and [`schema_of`] turns the entries into the Arrow schema.
//!
//! Each connector is a child module behind its own `connector-<name>`
//! feature.

pub mod bridge;
pub mod context;
pub mod factory;
pub mod schema;

pub use crate::config::{ConfigMap, ConfigValue, from_kdl_str, one_or_many};
pub use bridge::ChannelBridge;
pub use context::{ConnectorContext, NodeIdentity};
pub use factory::{Connector, REBUILD_UNDECLARED, Rebuild, Role};
pub use schema::{
    FieldSpec, FieldType, SchemaFrom, parse_fields, parse_optional_fields, schema_of,
};

#[cfg(feature = "connector-channel")]
pub mod channel;
#[cfg(feature = "connector-clickhouse")]
pub mod clickhouse;
#[cfg(feature = "connector-file")]
pub mod file;
#[cfg(feature = "connector-http")]
pub mod http;
#[cfg(feature = "connector-kafka")]
pub mod kafka;
#[cfg(feature = "connector-mariadb")]
pub mod mariadb;
#[cfg(feature = "connector-nats")]
pub mod nats;
#[cfg(feature = "connector-otlp")]
pub mod otlp;
#[cfg(feature = "connector-peer")]
pub mod peer;
#[cfg(feature = "connector-postgresql")]
pub mod postgresql;
#[cfg(feature = "connector-redb")]
pub mod redb;
#[cfg(feature = "connector-s3")]
pub mod s3;
#[cfg(feature = "connector-tcp")]
pub mod tcp;
#[cfg(feature = "connector-turso")]
pub mod turso;
#[cfg(feature = "connector-udp")]
pub mod udp;
