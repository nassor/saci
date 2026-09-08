//! Built-in factory re-exports and registration.
//!
//! Two kinds of factory reach the registry here. A connector factory builds a
//! [`Source`](saci_core::io::source::Source) or a
//! [`Sink`](saci_core::io::sink::Sink) that moves bytes; a transformer factory
//! builds the byte format a connector's `format` key names. One feature per
//! crate decides whether it is compiled in and registered:
//!
//! - `connector-channel`: [`ChannelSourceFactory`], [`ChannelSinkFactory`]
//! - `connector-file`: [`FileSourceFactory`], [`FileSinkFactory`]
//! - `connector-http`: [`HttpSourceFactory`], [`HttpSinkFactory`]
//! - `connector-kafka`: [`KafkaSourceFactory`], [`KafkaSinkFactory`]
//! - `connector-nats`: [`NatsSourceFactory`], [`NatsSinkFactory`]
//! - `connector-postgresql`: [`PostgresSourceFactory`], [`PostgresSinkFactory`]
//! - `connector-redb`: [`RedbSourceFactory`], [`RedbSinkFactory`]
//! - `connector-s3`: [`S3SourceFactory`], [`S3SinkFactory`]
//! - `connector-saci`: [`SaciSourceFactory`], [`SaciSinkFactory`]
//! - `connector-tcp`: [`TcpSourceFactory`], [`TcpSinkFactory`]
//! - `connector-turso`: [`TursoSourceFactory`], [`TursoSinkFactory`]
//! - `transformer-arrow-ipc`: [`ArrowIpcTransformerFactory`]
//! - `transformer-avro`: [`AvroTransformerFactory`]
//! - `transformer-csv`: [`CsvTransformerFactory`]
//! - `transformer-ndjson`: [`NdjsonTransformerFactory`]
//! - `transformer-parquet`: [`ParquetTransformerFactory`]
//!
//! [`register_builtin_factories`] adds every enabled factory to a
//! [`ServiceBuilder`] in one call. [`BUILTIN_CONNECTOR_FEATURES`] names every
//! connector type name and [`BUILTIN_TRANSFORMER_FEATURES`] every format name
//! the crate carries, whether or not the feature is on, so a lookup that
//! misses can still say which feature to build with.
//!
//! The same question has a third shape, and this module owns its wording too:
//! a `wasm` node, a `plugin` node and `mode "cluster"` name a **host** rather
//! than a registry key, so there is no lookup to miss and no user-supplied
//! alternative. [`missing_cluster_host_error`] and its two processor
//! counterparts phrase those refusals through the one formatter the connector
//! and format hints already end with, so an operator meets one sentence
//! whatever they declared.

#[cfg(feature = "connector-channel")]
pub use saci_connector_channel::{ChannelSinkFactory, ChannelSourceFactory};
#[cfg(feature = "connector-file")]
pub use saci_connector_file::{FileSinkFactory, FileSourceFactory};
#[cfg(feature = "connector-http")]
pub use saci_connector_http::{HttpSinkFactory, HttpSourceFactory};
#[cfg(feature = "connector-kafka")]
pub use saci_connector_kafka::{KafkaSinkFactory, KafkaSourceFactory};
#[cfg(feature = "connector-nats")]
pub use saci_connector_nats::{NatsSinkFactory, NatsSourceFactory};
#[cfg(feature = "connector-postgresql")]
pub use saci_connector_postgresql::{PostgresSinkFactory, PostgresSourceFactory};
#[cfg(feature = "connector-redb")]
pub use saci_connector_redb::{RedbSinkFactory, RedbSourceFactory};
#[cfg(feature = "connector-s3")]
pub use saci_connector_s3::{S3SinkFactory, S3SourceFactory};
#[cfg(feature = "connector-saci")]
pub use saci_connector_saci::{SaciSinkFactory, SaciSourceFactory};
#[cfg(feature = "connector-tcp")]
pub use saci_connector_tcp::{TcpSinkFactory, TcpSourceFactory};
#[cfg(feature = "connector-turso")]
pub use saci_connector_turso::{TursoSinkFactory, TursoSourceFactory};
#[cfg(feature = "transformer-arrow-ipc")]
pub use saci_transformer_arrow_ipc::ArrowIpcTransformerFactory;
#[cfg(feature = "transformer-avro")]
pub use saci_transformer_avro::AvroTransformerFactory;
#[cfg(feature = "transformer-csv")]
pub use saci_transformer_csv::CsvTransformerFactory;
#[cfg(feature = "transformer-ndjson")]
pub use saci_transformer_ndjson::NdjsonTransformerFactory;
#[cfg(feature = "transformer-parquet")]
pub use saci_transformer_parquet::ParquetTransformerFactory;

use crate::error::SaciError;

use super::builder::ServiceBuilder;

/// Every built-in connector type name and the feature that compiles it in.
///
/// The keys are the strings
/// [`SourceFactory::type_name`](saci_connector::SourceFactory::type_name) and
/// [`SinkFactory::type_name`](saci_connector::SinkFactory::type_name) return,
/// which is what a config's `type =` names. Both TCP halves register as
/// `"tcp"`, so that one entry covers the source and the sink.
///
/// Listed unconditionally, unlike the re-exports above. A factory compiled out
/// takes its `type_name` with it, so this table is the only place a binary
/// built without `connector-postgresql` can learn that `PostgresSource` is a
/// connector this crate ships rather than a name nothing here provides.
/// `builtin_connector_table_matches_the_registry` pins it against the registry
/// in both directions.
pub const BUILTIN_CONNECTOR_FEATURES: &[(&str, &str)] = &[
    ("ChannelSource", "connector-channel"),
    ("ChannelSink", "connector-channel"),
    ("FileSource", "connector-file"),
    ("FileSink", "connector-file"),
    ("HttpSource", "connector-http"),
    ("HttpSink", "connector-http"),
    ("KafkaSource", "connector-kafka"),
    ("KafkaSink", "connector-kafka"),
    ("NatsSource", "connector-nats"),
    ("NatsSink", "connector-nats"),
    ("PostgresSource", "connector-postgresql"),
    ("PostgresSink", "connector-postgresql"),
    ("RedbSource", "connector-redb"),
    ("RedbSink", "connector-redb"),
    ("S3Source", "connector-s3"),
    ("S3Sink", "connector-s3"),
    ("tcp", "connector-tcp"),
    ("saci", "connector-saci"),
    ("TursoSource", "connector-turso"),
    ("TursoSink", "connector-turso"),
];

/// Every built-in format name and the feature that compiles it in.
///
/// The keys are the strings
/// [`TransformerFactory::format_name`](saci_transformer::TransformerFactory::format_name)
/// returns, which is what a `transformer` node's `format =` names. Kept apart
/// from [`BUILTIN_CONNECTOR_FEATURES`] because a format name and a connector
/// type name are different namespaces: one table would let a `format` key
/// answer with a connector feature.
///
/// Listed unconditionally, for the same reason: a factory compiled out takes
/// its `format_name` with it, so nothing else in a binary built without
/// `transformer-csv` knows that `csv` is a format this crate ships.
/// `builtin_transformer_table_matches_the_registry` pins it against the
/// registry in both directions.
pub const BUILTIN_TRANSFORMER_FEATURES: &[(&str, &str)] = &[
    ("arrow-ipc", "transformer-arrow-ipc"),
    ("avro", "transformer-avro"),
    ("csv", "transformer-csv"),
    ("ndjson", "transformer-ndjson"),
    ("parquet", "transformer-parquet"),
];

/// The feature that compiles in the connector registering `type_name`.
///
/// `None` for a name no connector crate here registers, which is what keeps a
/// user-defined type out of the feature advice: `MongoSource` is not in
/// [`BUILTIN_CONNECTOR_FEATURES`], so nothing claims a flag would supply it.
///
/// # Example
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use saci_service::service::factories::builtin_feature;
///
/// assert_eq!(builtin_feature("PostgresSource"), Some("connector-postgresql"));
/// assert_eq!(builtin_feature("MongoSource"), None);
/// # }
/// ```
pub fn builtin_feature(type_name: &str) -> Option<&'static str> {
    tabled_feature(BUILTIN_CONNECTOR_FEATURES, type_name)
}

/// The feature that compiles in the transformer registering `format`.
///
/// `None` for a format no transformer crate here registers, which is what
/// keeps a user-registered format out of the feature advice: `protobuf` is not
/// in [`BUILTIN_TRANSFORMER_FEATURES`], so nothing claims a flag would supply
/// it.
///
/// # Example
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use saci_service::service::factories::builtin_transformer_feature;
///
/// assert_eq!(builtin_transformer_feature("csv"), Some("transformer-csv"));
/// assert_eq!(builtin_transformer_feature("protobuf"), None);
/// # }
/// ```
pub fn builtin_transformer_feature(format: &str) -> Option<&'static str> {
    tabled_feature(BUILTIN_TRANSFORMER_FEATURES, format)
}

/// The feature paired with `key` in one of the two tables above.
fn tabled_feature(table: &[(&'static str, &'static str)], key: &str) -> Option<&'static str> {
    table
        .iter()
        .find(|(name, _)| *name == key)
        .map(|(_, feature)| *feature)
}

/// The error a [`Registry`](super::registry::Registry) lookup that missed
/// reports, naming the feature when the type is a built-in.
///
/// `kind` is `"source"` or `"sink"`. The message keeps its
/// `no {kind} factory registered` opening in both arms, which is what the
/// binary's `validate` classifies on.
///
/// Reached only after a lookup already failed, so a binary that registers its
/// own `PostgresSource` never consults the table and never sees the hint. The
/// hint addresses both callers that reach here, an operator running the
/// binary and a library embedder driving `ServiceBuilder` directly, because
/// either mistake produces this same miss.
pub(crate) fn missing_factory_error(kind: &str, type_name: &str, node_id: &str) -> SaciError {
    match builtin_feature(type_name) {
        Some(feature) => SaciError::configuration(format!(
            "no {kind} factory registered for type '{type_name}' \
             (required by {kind} '{node_id}'): that is a built-in connector this \
             binary was built without, so rebuild or reinstall with \
             `--features {feature}`, or register your own factory under that name"
        )),
        None => SaciError::configuration(format!(
            "no {kind} factory registered for type '{type_name}' \
             (required by {kind} '{node_id}')"
        )),
    }
}

/// The error a [`TransformerRegistry`](saci_transformer::TransformerRegistry)
/// lookup that missed reports, naming the feature when the format is a
/// built-in.
///
/// `registered` is what the registry does hold, printed as the historic
/// `(registered: ...)` tail so an operator can see whether the binary carries
/// any format at all. Both arms open identically, so anything reading this
/// message keeps working when the hint appears.
///
/// Reached only after the lookup already failed, so a binary registering its
/// own `csv` transformer never consults the table.
pub(crate) fn missing_transformer_error(
    node_id: &str,
    format: &str,
    registered: &[&str],
) -> SaciError {
    let list = if registered.is_empty() {
        "none".to_string()
    } else {
        registered.join(", ")
    };
    match builtin_transformer_feature(format) {
        Some(feature) => SaciError::configuration(format!(
            "transformer '{node_id}' names format '{format}', which no transformer is \
             registered for (registered: {list}): that is a built-in format this binary \
             was built without, so rebuild or reinstall with `--features {feature}`, or \
             register your own transformer under that name"
        )),
        None => SaciError::configuration(format!(
            "transformer '{node_id}' names format '{format}', which no transformer is \
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
/// [`missing_transformer_error`] append, so the three read alike.
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
        "wasm",
    )
}

/// The error a declared `plugin` node reports in a binary with no plugin host.
pub(crate) fn missing_plugin_host_error(node_id: &str, workflow_id: &str) -> SaciError {
    capability_error(
        &format!("workflow '{workflow_id}': plugin node '{node_id}'"),
        "native plugin host",
        "plugin",
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

/// Register every enabled connector's and transformer's factories into
/// `builder`.
///
/// A crate reaches the registry only when its feature is on. Supply the runtime
/// separately through [`ServiceBuilder::with_runtime`] or `pipeline.wasm` in the
/// config.
///
/// # Example
///
/// ```rust
/// # #[cfg(feature = "service")]
/// # {
/// use saci_service::service::builder::ServiceBuilder;
/// use saci_service::service::factories::register_builtin_factories;
///
/// let builder = register_builtin_factories(ServiceBuilder::new());
/// # }
/// ```
pub fn register_builtin_factories(builder: ServiceBuilder) -> ServiceBuilder {
    // Transformers first: a connector factory resolves its `format` key against
    // the registry, so the format has to be in it by the time config is built.
    #[cfg(feature = "transformer-arrow-ipc")]
    let builder = builder.register_transformer(ArrowIpcTransformerFactory);

    #[cfg(feature = "transformer-avro")]
    let builder = builder.register_transformer(AvroTransformerFactory);

    #[cfg(feature = "transformer-csv")]
    let builder = builder.register_transformer(CsvTransformerFactory);

    #[cfg(feature = "transformer-ndjson")]
    let builder = builder.register_transformer(NdjsonTransformerFactory);

    #[cfg(feature = "transformer-parquet")]
    let builder = builder.register_transformer(ParquetTransformerFactory);

    #[cfg(feature = "connector-channel")]
    let builder = builder
        .register_source(ChannelSourceFactory)
        .register_sink(ChannelSinkFactory)
        .with_channel_bridge(std::sync::Arc::new(
            saci_connector_channel::ChannelRegistry::default(),
        ));

    #[cfg(feature = "connector-file")]
    let builder = builder
        .register_source(FileSourceFactory)
        .register_sink(FileSinkFactory);

    #[cfg(feature = "connector-http")]
    let builder = builder
        .register_source(HttpSourceFactory)
        .register_sink(HttpSinkFactory);

    #[cfg(feature = "connector-kafka")]
    let builder = builder
        .register_source(KafkaSourceFactory)
        .register_sink(KafkaSinkFactory);

    #[cfg(feature = "connector-nats")]
    let builder = builder
        .register_source(NatsSourceFactory)
        .register_sink(NatsSinkFactory);

    #[cfg(feature = "connector-postgresql")]
    let builder = builder
        .register_source(PostgresSourceFactory)
        .register_sink(PostgresSinkFactory);

    #[cfg(feature = "connector-turso")]
    let builder = builder
        .register_source(TursoSourceFactory)
        .register_sink(TursoSinkFactory);

    #[cfg(feature = "connector-s3")]
    let builder = builder
        .register_source(S3SourceFactory)
        .register_sink(S3SinkFactory);

    #[cfg(feature = "connector-tcp")]
    let builder = builder
        .register_source(TcpSourceFactory)
        .register_sink(TcpSinkFactory);

    #[cfg(feature = "connector-saci")]
    let builder = builder
        .register_source(SaciSourceFactory)
        .register_sink(SaciSinkFactory);

    #[cfg(feature = "connector-redb")]
    let builder = builder
        .register_source(RedbSourceFactory)
        .register_sink(RedbSinkFactory);

    builder
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hint arm must not disturb the opening the binary's `validate`
    /// classifies a missing-factory error by.
    ///
    /// `build_all` cannot produce this message in a build that has every
    /// connector, which is the build the suite runs, so the producer is called
    /// directly. Without this, appending the hint could silently break
    /// `is_unknown_factory_error` and turn a warning into a hard failure with
    /// no test to say so.
    #[test]
    fn the_feature_hint_keeps_the_classified_prefix() {
        let message = missing_factory_error("source", "PostgresSource", "pg_orders").message();
        assert!(
            message.starts_with("no source factory registered"),
            "validate classifies on this opening: {message}"
        );
        assert!(
            message.contains("for type 'PostgresSource'"),
            "validate reads the type name back out of this: {message}"
        );
        assert!(
            message.contains("--features connector-postgresql"),
            "the hint must name the feature: {message}"
        );
    }

    /// A type no connector here registers gets the plain message, so nothing
    /// tells the author of a user-defined factory that a build flag supplies
    /// it.
    #[test]
    fn an_unknown_type_gets_no_feature_hint() {
        let message = missing_factory_error("sink", "ClickHouseSink", "orders_out").message();
        assert_eq!(
            message,
            "no sink factory registered for type 'ClickHouseSink' (required by sink 'orders_out')"
        );
    }

    /// The table is the registry's key set, in both directions.
    ///
    /// A tenth connector registering its factories without a table entry
    /// leaves its missing-factory error unable to name a feature, and an entry
    /// left behind by a removed connector makes one name a feature that no
    /// longer exists. Nothing else catches either.
    ///
    /// Gated on `all`, the build that carries every connector, because the
    /// check is bidirectional: with a connector compiled out the registry is
    /// legitimately missing names the table must still carry.
    /// `--all-features` selects it.
    #[cfg(feature = "all")]
    #[test]
    fn builtin_connector_table_matches_the_registry() {
        let builder = register_builtin_factories(ServiceBuilder::new());
        let registry = builder.registry();

        let mut registered = registry.source_names();
        registered.extend(registry.sink_names());
        registered.sort_unstable();
        registered.dedup();

        let mut tabled: Vec<&str> = BUILTIN_CONNECTOR_FEATURES
            .iter()
            .map(|(name, _)| *name)
            .collect();
        tabled.sort_unstable();
        tabled.dedup();

        assert_eq!(
            registered, tabled,
            "register_builtin_factories registers {registered:?} but \
             BUILTIN_CONNECTOR_FEATURES lists {tabled:?}; add the connector's type \
             names and its feature to the table, or drop the stale entry, so a \
             missing-factory error can name the feature to build with"
        );
    }

    /// A format this crate carries but was compiled out names its feature.
    ///
    /// A build with every transformer cannot reach this arm through
    /// `build_all`, so the producer is called directly. The opening is pinned
    /// too: it is what an operator greps for and what any future classifier
    /// would match on.
    #[test]
    fn a_missing_builtin_format_names_its_feature() {
        let message = missing_transformer_error("csv_fmt", "csv", &[]).message();
        assert!(
            message.starts_with(
                "transformer 'csv_fmt' names format 'csv', which no transformer is registered for"
            ),
            "the opening must survive the hint: {message}"
        );
        assert!(
            message.contains("(registered: none)"),
            "an operator needs to see the binary carries no format at all: {message}"
        );
        assert!(
            message.contains("--features transformer-csv"),
            "the hint must name the feature: {message}"
        );
    }

    /// A format no transformer here registers gets the plain message, so an
    /// embedder registering their own is never told a build flag supplies it.
    #[test]
    fn an_unknown_format_gets_no_feature_hint() {
        let message = missing_transformer_error("pb_fmt", "protobuf", &["csv", "ndjson"]).message();
        assert_eq!(
            message,
            "transformer 'pb_fmt' names format 'protobuf', which no transformer is \
             registered for (registered: csv, ndjson)"
        );
    }

    /// The format table is the transformer registry's key set, in both
    /// directions.
    ///
    /// A sixth transformer registering without a table entry leaves its
    /// missing-format error unable to name a feature, and an entry a removed
    /// transformer left behind makes one name a feature that no longer
    /// exists.
    ///
    /// Gated on `all`, the build that carries every transformer, because the
    /// check is bidirectional: with one compiled out the registry is
    /// legitimately missing a name the table must still carry.
    /// `--all-features` selects it.
    #[cfg(feature = "all")]
    #[test]
    fn builtin_transformer_table_matches_the_registry() {
        let builder = register_builtin_factories(ServiceBuilder::new());
        let registered = builder.registry().transformers().formats();

        let mut tabled: Vec<&str> = BUILTIN_TRANSFORMER_FEATURES
            .iter()
            .map(|(name, _)| *name)
            .collect();
        tabled.sort_unstable();

        assert_eq!(
            registered, tabled,
            "register_builtin_factories registers formats {registered:?} but \
             BUILTIN_TRANSFORMER_FEATURES lists {tabled:?}; add the format and its \
             feature to the table, or drop the stale entry, so a missing-format error \
             can name the feature to build with"
        );
    }

    /// The four capability refusals name their feature and read alike.
    ///
    /// None of them is reachable through `build_all` or
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
             `--features wasm`"
        );
        assert_eq!(
            missing_plugin_host_error("audit", "orders").message(),
            "workflow 'orders': plugin node 'audit' needs the native plugin host, which \
             this binary was built without, so rebuild or reinstall with \
             `--features plugin`"
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
    }
}
