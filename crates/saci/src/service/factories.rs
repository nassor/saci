//! Built-in connector and codec re-exports and registration.
//!
//! Two kinds of component reach the registry here. A [`Connector`](crate::connector::Connector) builds a
//! [`Source`](crate::io::Source) or a [`Sink`](crate::io::Sink) that moves
//! data; a codec factory builds the byte format a connector's `codec` key
//! names. One feature per crate decides whether it is compiled in and
//! registered:
//!
//! - `connector-channel`: [`ChannelConnector`]
//! - `connector-clickhouse`: [`ClickhouseConnector`]
//! - `connector-file`: [`FileConnector`]
//! - `connector-http`: [`HttpConnector`], [`HttpListenConnector`]
//! - `connector-kafka`: [`KafkaConnector`]
//! - `connector-mariadb`: [`MariadbConnector`]
//! - `connector-nats`: [`NatsConnector`]
//! - `connector-otlp`: [`OtlpMetricsConnector`]
//! - `connector-postgresql`: [`PostgresConnector`]
//! - `connector-redb`: [`RedbConnector`]
//! - `connector-s3`: [`S3Connector`]
//! - `connector-peer`: [`PeerConnector`]
//! - `connector-tcp`: [`TcpConnector`]
//! - `connector-udp`: [`UdpConnector`]
//! - `connector-turso`: [`TursoConnector`]
//! - `codec-arrow-ipc`: [`ArrowIpcCodecFactory`]
//! - `codec-avro`: [`AvroCodecFactory`]
//! - `codec-csv`: [`CsvCodecFactory`]
//! - `codec-ndjson`: [`NdjsonCodecFactory`]
//! - `codec-parquet`: [`ParquetCodecFactory`]
//!
//! [`ServiceBuilder::new`] adds every enabled connector and codec to the
//! builder in one call. [`BUILTIN_CONNECTOR_FEATURES`] names every connector
//! and [`BUILTIN_CODEC_FEATURES`] every format name the crate carries,
//! whether or not the feature is on, so a lookup that misses can still say
//! which feature to build with.
//!
//! The same question has a third shape, and this module owns its wording too:
//! a `wasm` node, a `plugin` node and `mode "cluster"` name a **host** rather
//! than a registry key, so there is no lookup to miss and no user-supplied
//! alternative. [`missing_cluster_host_error`] and its two processor
//! counterparts phrase those refusals through the one formatter the connector
//! and format hints already end with, so an operator meets one sentence
//! whatever they declared.

#[cfg(feature = "codec-arrow-ipc")]
pub use crate::codec::arrow_ipc::ArrowIpcCodecFactory;
#[cfg(feature = "codec-avro")]
pub use crate::codec::avro::AvroCodecFactory;
#[cfg(feature = "codec-csv")]
pub use crate::codec::csv::CsvCodecFactory;
#[cfg(feature = "codec-ndjson")]
pub use crate::codec::ndjson::NdjsonCodecFactory;
#[cfg(feature = "codec-parquet")]
pub use crate::codec::parquet::ParquetCodecFactory;
#[cfg(feature = "connector-channel")]
pub use crate::connector::channel::ChannelConnector;
#[cfg(feature = "connector-clickhouse")]
pub use crate::connector::clickhouse::ClickhouseConnector;
#[cfg(feature = "connector-file")]
pub use crate::connector::file::FileConnector;
#[cfg(feature = "connector-http")]
pub use crate::connector::http::{HttpConnector, HttpListenConnector};
#[cfg(feature = "connector-kafka")]
pub use crate::connector::kafka::KafkaConnector;
#[cfg(feature = "connector-mariadb")]
pub use crate::connector::mariadb::MariadbConnector;
#[cfg(feature = "connector-nats")]
pub use crate::connector::nats::NatsConnector;
#[cfg(feature = "connector-otlp")]
pub use crate::connector::otlp::OtlpMetricsConnector;
#[cfg(feature = "connector-peer")]
pub use crate::connector::peer::PeerConnector;
#[cfg(feature = "connector-postgresql")]
pub use crate::connector::postgresql::PostgresConnector;
#[cfg(feature = "connector-redb")]
pub use crate::connector::redb::RedbConnector;
#[cfg(feature = "connector-s3")]
pub use crate::connector::s3::S3Connector;
#[cfg(feature = "connector-tcp")]
pub use crate::connector::tcp::TcpConnector;
#[cfg(feature = "connector-turso")]
pub use crate::connector::turso::TursoConnector;
#[cfg(feature = "connector-udp")]
pub use crate::connector::udp::UdpConnector;

use std::sync::LazyLock;

use crate::connector::{Connector, Role};
use crate::error::{SaciError, SaciResult};

use super::builder::ServiceBuilder;
use super::registry::Registry;

/// Every built-in connector name and the feature that compiles it in.
///
/// The keys are the strings [`Connector::name`](crate::connector::Connector::name)
/// returns, which is what a config's `connector =` names. One row covers both
/// roles of a connector; `http-listen` serves sources only.
///
/// Listed unconditionally, unlike the re-exports above. A connector compiled
/// out takes its `name` with it, so this table is the only place a binary
/// built without `connector-postgresql` can learn that `postgresql` is a
/// connector this crate ships rather than a name nothing here provides.
/// `builtin_connector_table_matches_the_registry` pins it against the registry
/// in both directions.
pub const BUILTIN_CONNECTOR_FEATURES: &[(&str, &str)] = &[
    ("channel", "connector-channel"),
    ("clickhouse", "connector-clickhouse"),
    ("file", "connector-file"),
    ("http", "connector-http"),
    ("http-listen", "connector-http"),
    ("kafka", "connector-kafka"),
    ("mariadb", "connector-mariadb"),
    ("nats", "connector-nats"),
    ("otlp-metrics", "connector-otlp"),
    ("peer", "connector-peer"),
    ("postgresql", "connector-postgresql"),
    ("redb", "connector-redb"),
    ("s3", "connector-s3"),
    ("tcp", "connector-tcp"),
    ("turso", "connector-turso"),
    ("udp", "connector-udp"),
];

/// Every built-in format name and the feature that compiles it in.
///
/// The keys are the strings
/// [`CodecFactory::format_name`](crate::codec::CodecFactory::format_name)
/// returns, which is what a `codec` node's `format =` names. Kept apart
/// from [`BUILTIN_CONNECTOR_FEATURES`] because a format name and a connector
/// type name are different namespaces: one table would let a `format` key
/// answer with a connector feature.
///
/// Listed unconditionally, for the same reason: a factory compiled out takes
/// its `format_name` with it, so nothing else in a binary built without
/// `codec-csv` knows that `csv` is a format this crate ships.
/// `builtin_codec_table_matches_the_registry` pins it against the
/// registry in both directions.
pub const BUILTIN_CODEC_FEATURES: &[(&str, &str)] = &[
    ("arrow-ipc", "codec-arrow-ipc"),
    ("avro", "codec-avro"),
    ("csv", "codec-csv"),
    ("ndjson", "codec-ndjson"),
    ("parquet", "codec-parquet"),
];

/// The feature that compiles in the connector registering `name`.
///
/// `None` for a name no connector crate here registers, which is what keeps a
/// user-defined connector out of the feature advice: `mongodb` is not in
/// [`BUILTIN_CONNECTOR_FEATURES`], so nothing claims a flag would supply it.
///
/// # Example
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use saci::service::builtin_feature;
///
/// assert_eq!(builtin_feature("postgresql"), Some("connector-postgresql"));
/// assert_eq!(builtin_feature("mongodb"), None);
/// # }
/// ```
pub fn builtin_feature(name: &str) -> Option<&'static str> {
    tabled_feature(BUILTIN_CONNECTOR_FEATURES, name)
}

/// The feature that compiles in the codec registering `format`.
///
/// `None` for a format no codec crate here registers, which is what
/// keeps a user-registered format out of the feature advice: `protobuf` is not
/// in [`BUILTIN_CODEC_FEATURES`], so nothing claims a flag would supply
/// it.
///
/// # Example
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use saci::service::builtin_codec_feature;
///
/// assert_eq!(builtin_codec_feature("csv"), Some("codec-csv"));
/// assert_eq!(builtin_codec_feature("protobuf"), None);
/// # }
/// ```
pub fn builtin_codec_feature(format: &str) -> Option<&'static str> {
    tabled_feature(BUILTIN_CODEC_FEATURES, format)
}

/// The feature paired with `key` in one of the two tables above.
fn tabled_feature(table: &[(&'static str, &'static str)], key: &str) -> Option<&'static str> {
    table
        .iter()
        .find(|(name, _)| *name == key)
        .map(|(_, feature)| *feature)
}

/// The error a [`Registry`](super::registry::Registry) lookup that missed
/// reports, naming the feature when the connector is a built-in.
///
/// `role` is the node that asked. The message keeps its
/// `no connector named '{name}' is registered` opening in both arms, which is
/// what the binary's `validate` classifies on.
///
/// Reached only after a lookup already failed, so a binary that registers its
/// own `postgresql` connector never consults the table and never sees the
/// hint. The hint addresses both callers that reach here, an operator running
/// the binary and a library embedder driving `ServiceBuilder` directly,
/// because either mistake produces this same miss.
pub(crate) fn missing_factory_error(role: Role, name: &str, node_id: &str) -> SaciError {
    match builtin_feature(name) {
        Some(feature) => SaciError::configuration(format!(
            "no connector named '{name}' is registered \
             (required by {role} '{node_id}'): that is a built-in connector this \
             binary was built without, so rebuild or reinstall with \
             `--features {feature}`, or register your own connector under that name"
        )),
        None => SaciError::configuration(format!(
            "no connector named '{name}' is registered (required by {role} '{node_id}')"
        )),
    }
}

/// The error a registered connector reports when a node asks it for a role it
/// does not serve.
pub(crate) fn missing_role_error(role: Role, name: &str, node_id: &str) -> SaciError {
    SaciError::configuration(format!(
        "connector '{name}' has no {role} (required by {role} '{node_id}')"
    ))
}

/// Find the connector `name` for a node of `role`.
///
/// # Errors
///
/// [`missing_factory_error`] when nothing is registered under `name`;
/// [`missing_role_error`] when the connector exists but does not serve `role`.
pub(crate) fn lookup_connector<'a>(
    registry: &'a Registry,
    role: Role,
    name: &str,
    node_id: &str,
) -> SaciResult<&'a dyn Connector> {
    let connector = registry
        .connector(name)
        .ok_or_else(|| missing_factory_error(role, name, node_id))?;
    if connector.roles().contains(&role) {
        Ok(connector)
    } else {
        Err(missing_role_error(role, name, node_id))
    }
}

/// The error a [`CodecRegistry`](crate::codec::CodecRegistry)
/// lookup that missed reports, naming the feature when the format is a
/// built-in.
///
/// `registered` is what the registry does hold, printed as the historic
/// `(registered: ...)` tail so an operator can see whether the binary carries
/// any format at all. Both arms open identically, so anything reading this
/// message keeps working when the hint appears.
///
/// Reached only after the lookup already failed, so a binary registering its
/// own `csv` codec never consults the table.
pub(crate) fn missing_codec_error(node_id: &str, format: &str, registered: &[&str]) -> SaciError {
    let list = if registered.is_empty() {
        "none".to_string()
    } else {
        registered.join(", ")
    };
    match builtin_codec_feature(format) {
        Some(feature) => SaciError::configuration(format!(
            "codec '{node_id}' names format '{format}', which no codec is \
             registered for (registered: {list}): that is a built-in format this binary \
             was built without, so rebuild or reinstall with `--features {feature}`, or \
             register your own codec under that name"
        )),
        None => SaciError::configuration(format!(
            "codec '{node_id}' names format '{format}', which no codec is \
             registered for (registered: {list})"
        )),
    }
}

/// The one sentence every "this binary was built without it" refusal ends
/// with.
///
/// `subject` names what the config declared, `needs` the capability it needs,
/// and `feature` the `--features` flag that compiles that capability in. The
/// tail matches the one [`missing_factory_error`] and
/// [`missing_codec_error`] append, so the three read alike.
fn capability_error(subject: &str, needs: &str, feature: &str) -> SaciError {
    SaciError::configuration(format!(
        "{subject} needs the {needs}, which this binary was built without, so \
         rebuild or reinstall with `--features {feature}`"
    ))
}

/// The error a declared `wasm` node reports in a binary with no wasm host.
///
/// Unlike a connector type name, this one has no second kind: nothing an
/// embedder registers at serve time can supply the wasmtime host, so the
/// advice is unconditional rather than a hint.
pub(crate) fn missing_wasm_host_error(node_id: &str, workflow_id: &str) -> SaciError {
    capability_error(
        &format!("workflow '{workflow_id}': wasm node '{node_id}'"),
        "wasmtime processor host",
        "wasm-host",
    )
}

/// The error a declared `plugin` node reports in a binary with no plugin host.
pub(crate) fn missing_plugin_host_error(node_id: &str, workflow_id: &str) -> SaciError {
    capability_error(
        &format!("workflow '{workflow_id}': plugin node '{node_id}'"),
        "native plugin host",
        "plugin-host",
    )
}

/// The error a declared `window` block reports in a binary with no
/// windowing engine.
///
/// `kind` is `"wasm"` or `"plugin"`, the two node kinds a block can sit on.
/// The geometry parses in every build, so the refusal names the flag rather
/// than letting serde report `window` as an unknown key.
pub(crate) fn missing_windows_engine_error(
    kind: &str,
    node_id: &str,
    workflow_id: &str,
) -> SaciError {
    capability_error(
        &format!("workflow '{workflow_id}': the `window` block on {kind} node '{node_id}'"),
        "windowing engine",
        "windows",
    )
}

/// The error `mode "cluster"` reports in a binary with no Raft stack.
///
/// Public because the binary's `serve` needs it for the arm that keeps its
/// `ServiceMode` match exhaustive in a `service`-only build; every other
/// caller reaches it through
/// [`validate_build_capabilities`](super::validation::validate_build_capabilities).
pub fn missing_cluster_host_error() -> SaciError {
    capability_error(
        "the config's `mode \"cluster\"`",
        "cluster runner and Raft stack",
        "service-cluster",
    )
}

/// The error a declared `encryption` / `encrypt` / `decrypt` block reports in a
/// binary built without `crypto`.
pub(crate) fn missing_crypto_error(subject: &str) -> SaciError {
    capability_error(subject, "data encryption module", "crypto")
}

/// Register every enabled connector into `registry`.
///
/// A connector reaches the registry only when its feature is on.
#[allow(unused_variables)]
fn register_builtin_connectors(registry: &mut Registry) {
    #[cfg(feature = "connector-channel")]
    registry.register(ChannelConnector);

    #[cfg(feature = "connector-file")]
    registry.register(FileConnector);

    #[cfg(feature = "connector-http")]
    registry
        .register(HttpConnector)
        .register(HttpListenConnector);

    #[cfg(feature = "connector-kafka")]
    registry.register(KafkaConnector);

    #[cfg(feature = "connector-nats")]
    registry.register(NatsConnector);

    #[cfg(feature = "connector-otlp")]
    registry.register(OtlpMetricsConnector);

    #[cfg(feature = "connector-postgresql")]
    registry.register(PostgresConnector);

    #[cfg(feature = "connector-mariadb")]
    registry.register(MariadbConnector);

    #[cfg(feature = "connector-clickhouse")]
    registry.register(ClickhouseConnector);

    #[cfg(feature = "connector-turso")]
    registry.register(TursoConnector);

    #[cfg(feature = "connector-s3")]
    registry.register(S3Connector);

    #[cfg(feature = "connector-tcp")]
    registry.register(TcpConnector);

    #[cfg(feature = "connector-udp")]
    registry.register(UdpConnector);

    #[cfg(feature = "connector-peer")]
    registry.register(PeerConnector);

    #[cfg(feature = "connector-redb")]
    registry.register(RedbConnector);
}

/// The connectors this crate compiles in, registered once for the process.
///
/// What a check that has no [`ServiceBuilder`] in hand asks: config
/// validation reads [`Connector::live`] through it, because a config is
/// validated before any builder registers a connector of its own.
pub(crate) fn builtin_registry() -> &'static Registry {
    static BUILTIN: LazyLock<Registry> = LazyLock::new(|| {
        let mut registry = Registry::new();
        register_builtin_connectors(&mut registry);
        registry
    });
    &BUILTIN
}

/// Register every enabled connector and codec into `builder`.
/// [`ServiceBuilder::new`] is this applied to [`ServiceBuilder::empty`].
///
/// A crate reaches the registry only when its feature is on. Supply the runtime
/// separately through [`ServiceBuilder::runtime`] or `pipeline.wasm` in the
/// config.
pub(crate) fn register_builtin_factories(builder: ServiceBuilder) -> ServiceBuilder {
    // Codecs first: a connector resolves its `format` key against the
    // registry, so the format has to be in it by the time config is built.
    #[cfg(feature = "codec-arrow-ipc")]
    let builder = builder.codec(ArrowIpcCodecFactory);

    #[cfg(feature = "codec-avro")]
    let builder = builder.codec(AvroCodecFactory);

    #[cfg(feature = "codec-csv")]
    let builder = builder.codec(CsvCodecFactory);

    #[cfg(feature = "codec-ndjson")]
    let builder = builder.codec(NdjsonCodecFactory);

    #[cfg(feature = "codec-parquet")]
    let builder = builder.codec(ParquetCodecFactory);

    let mut builder = builder;
    register_builtin_connectors(builder.registry_mut());

    #[cfg(feature = "connector-channel")]
    let builder = builder.channels(std::sync::Arc::new(
        crate::connector::channel::ChannelRegistry::default(),
    ));

    builder
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hint arm must not disturb the opening the binary's `validate`
    /// classifies a missing-connector error by.
    ///
    /// `build` cannot produce this message in a build that has every
    /// connector, which is the build the suite runs, so the producer is called
    /// directly. Without this, appending the hint could silently break
    /// `is_unknown_factory_error` and turn a warning into a hard failure with
    /// no test to say so.
    #[test]
    fn the_feature_hint_keeps_the_classified_prefix() {
        let message = missing_factory_error(Role::Source, "postgresql", "pg_orders").message();
        assert!(
            message.starts_with("no connector named 'postgresql' is registered"),
            "validate classifies on this opening: {message}"
        );
        assert!(
            message.contains("required by source 'pg_orders'"),
            "the message names the node that asked: {message}"
        );
        assert!(
            message.contains("--features connector-postgresql"),
            "the hint must name the feature: {message}"
        );
    }

    /// A name no connector here registers gets the plain message, so nothing
    /// tells the author of a user-defined connector that a build flag
    /// supplies it.
    #[test]
    fn an_unknown_connector_gets_no_feature_hint() {
        let message = missing_factory_error(Role::Sink, "clickhouse-x", "orders_out").message();
        assert_eq!(
            message,
            "no connector named 'clickhouse-x' is registered (required by sink 'orders_out')"
        );
    }

    /// A registered connector asked for a role it lacks says which.
    #[test]
    fn a_missing_role_names_the_connector_and_the_role() {
        let message = missing_role_error(Role::Sink, "http-listen", "out").message();
        assert_eq!(
            message,
            "connector 'http-listen' has no sink (required by sink 'out')"
        );
    }

    /// The table is the registry's key set, in both directions.
    ///
    /// A new connector registering without a table entry leaves its
    /// missing-connector error unable to name a feature, and an entry left
    /// behind by a removed connector makes one name a feature that no longer
    /// exists. Nothing else catches either.
    ///
    /// Gated on `all`, the build that carries every connector, because the
    /// check is bidirectional: with a connector compiled out the registry is
    /// legitimately missing names the table must still carry.
    /// `--all-features` selects it.
    #[cfg(feature = "all")]
    #[test]
    fn builtin_connector_table_matches_the_registry() {
        let builder = ServiceBuilder::new();
        let registered = builder.registry().names();

        let mut tabled: Vec<&str> = BUILTIN_CONNECTOR_FEATURES
            .iter()
            .map(|(name, _)| *name)
            .collect();
        tabled.sort_unstable();

        assert_eq!(
            registered, tabled,
            "register_builtin_factories registers {registered:?} but \
             BUILTIN_CONNECTOR_FEATURES lists {tabled:?}; add the connector's \
             name and its feature to the table, or drop the stale entry, so a \
             missing-connector error can name the feature to build with"
        );
    }

    /// A format this crate carries but was compiled out names its feature.
    ///
    /// A build with every codec cannot reach this arm through
    /// `build`, so the producer is called directly. The opening is pinned
    /// too: it is what an operator greps for and what any future classifier
    /// would match on.
    #[test]
    fn a_missing_builtin_format_names_its_feature() {
        let message = missing_codec_error("csv_fmt", "csv", &[]).message();
        assert!(
            message.starts_with(
                "codec 'csv_fmt' names format 'csv', which no codec is registered for"
            ),
            "the opening must survive the hint: {message}"
        );
        assert!(
            message.contains("(registered: none)"),
            "an operator needs to see the binary carries no format at all: {message}"
        );
        assert!(
            message.contains("--features codec-csv"),
            "the hint must name the feature: {message}"
        );
    }

    /// A format no codec here registers gets the plain message, so an
    /// embedder registering their own is never told a build flag supplies it.
    #[test]
    fn an_unknown_format_gets_no_feature_hint() {
        let message = missing_codec_error("pb_fmt", "protobuf", &["csv", "ndjson"]).message();
        assert_eq!(
            message,
            "codec 'pb_fmt' names format 'protobuf', which no codec is \
             registered for (registered: csv, ndjson)"
        );
    }

    /// The format table is the codec registry's key set, in both
    /// directions.
    ///
    /// A sixth codec registering without a table entry leaves its
    /// missing-format error unable to name a feature, and an entry a removed
    /// codec left behind makes one name a feature that no longer
    /// exists.
    ///
    /// Gated on `all`, the build that carries every codec, because the
    /// check is bidirectional: with one compiled out the registry is
    /// legitimately missing a name the table must still carry.
    /// `--all-features` selects it.
    #[cfg(feature = "all")]
    #[test]
    fn builtin_codec_table_matches_the_registry() {
        let builder = ServiceBuilder::new();
        let registered = builder.registry().codecs().names();

        let mut tabled: Vec<&str> = BUILTIN_CODEC_FEATURES
            .iter()
            .map(|(name, _)| *name)
            .collect();
        tabled.sort_unstable();

        assert_eq!(
            registered, tabled,
            "register_builtin_factories registers formats {registered:?} but \
             BUILTIN_CODEC_FEATURES lists {tabled:?}; add the format and its \
             feature to the table, or drop the stale entry, so a missing-format error \
             can name the feature to build with"
        );
    }

    /// No connector lists a config key that could carry a credential in its
    /// topology detail: the dashboard shows every listed key's value verbatim.
    #[test]
    fn no_builtin_detail_key_names_a_credential() {
        let registry = builtin_registry();
        for name in registry.names() {
            let connector = registry.connector(name).expect("a listed name resolves");
            for role in [Role::Source, Role::Sink] {
                for key in connector.detail_keys(role) {
                    for secret in [
                        "password",
                        "dsn",
                        "secret",
                        "token",
                        "key_file",
                        "auth",
                        "tls",
                        "cert_file",
                        "encryption",
                        "hexkey",
                        "sslkey",
                    ] {
                        assert!(
                            !key.contains(secret),
                            "connector '{name}' lists detail key '{key}' for its {role}, \
                             which names a credential"
                        );
                    }
                }
            }
        }
    }

    /// The four capability refusals name their feature and read alike.
    ///
    /// None of them is reachable through `build` or
    /// `validate_build_capabilities` in a build that carries every host,
    /// which is the build the suite runs, so the producers are called
    /// directly, the same reason
    /// `the_feature_hint_keeps_the_classified_prefix` calls
    /// `missing_factory_error` directly. Without this, the wording would be
    /// checked only where the hosts are compiled out, which is one reduced
    /// build rather than every build.
    #[test]
    fn every_host_refusal_names_the_feature_that_supplies_it() {
        assert_eq!(
            missing_wasm_host_error("transform", "orders").message(),
            "workflow 'orders': wasm node 'transform' needs the wasmtime processor host, \
             which this binary was built without, so rebuild or reinstall with \
             `--features wasm-host`"
        );
        assert_eq!(
            missing_plugin_host_error("audit", "orders").message(),
            "workflow 'orders': plugin node 'audit' needs the native plugin host, which \
             this binary was built without, so rebuild or reinstall with \
             `--features plugin-host`"
        );
        assert_eq!(
            missing_cluster_host_error().message(),
            "the config's `mode \"cluster\"` needs the cluster runner and Raft stack, \
             which this binary was built without, so rebuild or reinstall with \
             `--features service-cluster`"
        );
        assert_eq!(
            missing_windows_engine_error("wasm", "aggregate", "orders").message(),
            "workflow 'orders': the `window` block on wasm node 'aggregate' needs the \
             windowing engine, which this binary was built without, so rebuild or \
             reinstall with `--features windows`"
        );
        assert_eq!(
            missing_crypto_error("the config's `encryption` block").message(),
            "the config's `encryption` block needs the data encryption module, which this \
             binary was built without, so rebuild or reinstall with `--features crypto`"
        );
    }
}
