//! The connector/transformer/processor matrix, one test per slice.
//!
//! Every `{source connector, sink connector, byte format, processor runtime}`
//! combination is built from KDL, assembled through the real factory registry,
//! run through the real standalone or stream runner, and asserted against the
//! capability table in [`matrix`].
//!
//! The `live` module holds one test per `{source, processor, format}` slice,
//! named `live::<source>::<processor>::<format>`, each running its source
//! against every sink, plus `live::maximal::workflow`. nextest runs every test
//! in its own process, so the slices run in parallel; none of them starts a
//! container. They read the Kafka, NATS, PostgreSQL, S3 and ClickHouse
//! endpoints from the `SACI_MATRIX_*` variables `cargo xtask matrix` sets
//! after starting one container per resource for the whole run, and skip the
//! cases that need a resource whose variable is unset.
//!
//! ```bash
//! cargo xtask matrix                                 # every slice
//! cargo xtask matrix -E 'test(/^live::kafka::/)'     # one source's slices
//! ```
//!
//! Every live test is `#[ignore]`d and reached only through the `matrix`
//! nextest profile the task selects; the other tests here are Docker-free and
//! run under every profile.

#![cfg(all(
    feature = "service",
    feature = "wasm",
    feature = "plugin",
    feature = "connector-channel",
    feature = "connector-file",
    feature = "connector-http",
    feature = "connector-clickhouse",
    feature = "connector-kafka",
    feature = "connector-nats",
    feature = "connector-opentelemetry",
    feature = "connector-postgresql",
    feature = "connector-redb",
    feature = "connector-s3",
    feature = "connector-saci",
    feature = "connector-tcp",
    feature = "connector-turso",
    feature = "connector-udp",
    feature = "transformer-arrow-ipc",
    feature = "transformer-avro",
    feature = "transformer-csv",
    feature = "transformer-ndjson",
    feature = "transformer-parquet",
))]

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use saci_core::error::SaciError;
use saci_service::service::builder::ServiceBuilder;
use saci_service::service::factories::register_builtin_factories;
use saci_transformer::{ConfigMap, ConfigValue, unsupported};

#[path = "common/matrix.rs"]
mod matrix;

use matrix::{
    CONNECTORS, Case, Connector, FORMATS, Fixtures, Format, PROCESSORS, ProcessorKind, Report,
    Resources, run_case, run_maximal,
};

/// Run one slice and fail on any case that missed its expectation.
///
/// Every case of the slice runs at once: `PipelineRuntime` is `?Send`, so
/// `join_all` polls them all on this one task, and a slice has at most one
/// case per sink.
async fn run_slice(source: Connector, processor: ProcessorKind, format: Format) {
    let started = Instant::now();
    let fixtures = Fixtures::resolve().expect("processor fixtures");
    if processor == ProcessorKind::Wasm {
        fixtures
            .compile_wasm()
            .expect("smoketest component compiles");
    }
    let resources = Resources::connect().await;
    println!("resources: {}", resources.describe());

    let reports = futures::future::join_all(
        Case::slice(source, processor, format)
            .into_iter()
            .map(|case| run_case(case, &resources, &fixtures)),
    )
    .await;
    let report = Report::new(reports, started.elapsed());
    report.print();

    let failures = report.failures();
    let named = failures
        .iter()
        .map(|c| format!("  {} [{}]: {}", c.name(), c.outcome.label(), c.detail))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        failures.is_empty(),
        "{} of {} matrix cases did not meet their expectation:\n{named}",
        failures.len(),
        report.cases.len()
    );
}

/// One module per source, one per processor inside it, one test per format
/// inside that. Each list also becomes a constant, which
/// [`every_slice_has_a_test`] compares against the matrix's own dimensions.
macro_rules! live_slices {
    (
        sources: [$($source:ident => $connector:ident),* $(,)?],
        processors: $processors:tt,
        formats: $formats:tt $(,)?
    ) => {
        pub const SOURCES: &[Connector] = &[$(Connector::$connector),*];
        pub const PROCESSORS: &[ProcessorKind] = live_slices!(@list ProcessorKind $processors);
        pub const FORMATS: &[Format] = live_slices!(@list Format $formats);
        $(
            pub mod $source {
                use super::*;
                live_slices!(@processors $connector $processors $formats);
            }
        )*
    };
    (@list $kind:ident [$($name:ident => $variant:ident),* $(,)?]) => {
        &[$($kind::$variant),*]
    };
    (@processors $connector:ident [$($processor:ident => $kind:ident),* $(,)?] $formats:tt) => {
        $(
            pub mod $processor {
                use super::*;
                live_slices!(@formats $connector $kind $formats);
            }
        )*
    };
    (@formats $connector:ident $kind:ident [$($format:ident => $variant:ident),* $(,)?]) => {
        $(
            #[tokio::test(flavor = "multi_thread")]
            #[ignore = "needs the containers `cargo xtask matrix` starts"]
            async fn $format() {
                run_slice(Connector::$connector, ProcessorKind::$kind, Format::$variant).await;
            }
        )*
    };
}

/// The Docker-backed tests, reached through `cargo xtask matrix`.
mod live {
    use super::*;

    live_slices! {
        sources: [
            channel => Channel,
            file => File,
            http => Http,
            http_push => HttpPush,
            tcp => Tcp,
            udp => Udp,
            saci => Saci,
            kafka => Kafka,
            nats => Nats,
            postgresql => Postgresql,
            clickhouse => Clickhouse,
            s3 => S3,
            turso => Turso,
            redb => Redb,
            opentelemetry => OpenTelemetry,
        ],
        processors: [native => Native, wasm => Wasm, plugin => Plugin],
        formats: [
            none => None,
            arrow_ipc => ArrowIpc,
            avro => Avro,
            csv => Csv,
            ndjson => Ndjson,
            parquet => Parquet,
        ],
    }

    /// The maximal workflow: every available source, every processor runtime
    /// and every available sink in one stream-mode workflow.
    pub mod maximal {
        use super::*;

        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "needs the containers `cargo xtask matrix` starts"]
        async fn workflow() {
            let fixtures = Fixtures::resolve().expect("processor fixtures");
            fixtures
                .compile_wasm()
                .expect("smoketest component compiles");
            let resources = Resources::connect().await;
            println!("resources: {}", resources.describe());
            let report = run_maximal(&resources, &fixtures)
                .await
                .expect("the maximal workflow must build and deliver rows");
            report.print();
        }
    }
}

/// Every `{source, processor, format}` slice of the matrix has a live test.
///
/// The live tests are generated from hand-written lists, because a test name
/// has to be an identifier. A connector, runtime or format added to the
/// matrix's dimensions and not to those lists would compile, and no test
/// would ever run its slices.
#[test]
fn every_slice_has_a_test() {
    let mut sources = live::SOURCES.to_vec();
    sources.sort_unstable();
    let mut expected = CONNECTORS.to_vec();
    expected.sort_unstable();
    assert_eq!(
        sources, expected,
        "connector_matrix.rs's live_slices! sources must list every entry of CONNECTORS"
    );

    let labels = |kinds: &[ProcessorKind]| {
        let mut labels: Vec<&str> = kinds.iter().map(|kind| kind.label()).collect();
        labels.sort_unstable();
        labels
    };
    assert_eq!(
        labels(live::PROCESSORS),
        labels(&PROCESSORS),
        "connector_matrix.rs's live_slices! processors must list every entry of PROCESSORS"
    );

    let labels = |formats: &[Format]| {
        let mut labels: Vec<&str> = formats.iter().map(|format| format.label()).collect();
        labels.sort_unstable();
        labels
    };
    assert_eq!(
        labels(live::FORMATS),
        labels(&FORMATS),
        "connector_matrix.rs's live_slices! formats must list every entry of FORMATS"
    );
}

/// The registered factories `register_builtin_factories` wires up must match
/// this file's own [`CONNECTORS`]/[`FORMATS`] dimension lists exactly.
///
/// The two connector dimensions are not the same list: `HttpPushSource`
/// registers a source and no sink, so the source names are [`CONNECTORS`]
/// through `Connector::source_type` and the sink names are the same array
/// through `Connector::sink_type`, which answers `None` for that one entry.
///
/// Without this test, registering one more connector or transformer
/// format compiles and runs clean: the matrix simply never builds a case for
/// it. This test builds the real registry the way [`run_case`] does and
/// compares its shape against the matrix's own lists, so a factory
/// registered outside them fails here instead of silently losing coverage.
///
/// [`Registry`] and [`TransformerRegistry`] both enumerate what they hold:
/// `Registry::source_names`, `Registry::sink_names` and
/// [`TransformerRegistry::formats`] each answer with a sorted list of
/// registered keys, so all three dimensions are compared as name sets. A count
/// would pass a connector renamed or swapped for another, which is exactly the
/// coverage this test exists to pin.
///
/// Docker-free and not `#[ignore]`d: it inspects the registry alone, with no
/// config, container, or running service involved.
///
/// [`Registry`]: saci_service::service::registry::Registry
/// [`TransformerRegistry`]: saci_transformer::TransformerRegistry
/// [`TransformerRegistry::formats`]: saci_transformer::TransformerRegistry::formats
#[test]
fn dimensions_cover_the_registry() {
    let builder = register_builtin_factories(ServiceBuilder::new());
    let registry = builder.registry();

    let source_names = registry.source_names();
    let sink_names = registry.sink_names();
    let mut expected_sources: Vec<&str> = CONNECTORS
        .iter()
        .copied()
        .map(Connector::source_type)
        .collect();
    expected_sources.sort_unstable();
    let mut expected_sinks: Vec<&str> = CONNECTORS
        .iter()
        .copied()
        .filter_map(Connector::sink_type)
        .collect();
    expected_sinks.sort_unstable();
    assert_eq!(
        source_names, expected_sources,
        "register_builtin_factories registered source factories {source_names:?} but \
         common/matrix.rs's CONNECTORS names {expected_sources:?}; every entry counts here, \
         the sink-less http-push included, which is why this expectation and the sink one \
         below are not the same list; a source connector was registered outside that array \
         (or one listed there no longer registers under that name); update CONNECTORS and \
         Connector::source_type to match",
    );
    assert_eq!(
        sink_names, expected_sinks,
        "register_builtin_factories registered sink factories {sink_names:?} but \
         common/matrix.rs's CONNECTORS names {expected_sinks:?} with a sink half; the sink \
         dimension is CONNECTORS through Connector::sink_type, because HttpPushSource \
         registers a source and no sink, so a sink connector was registered outside that \
         array (or sink_type now answers for the wrong entries); update CONNECTORS or \
         sink_type to match",
    );

    let mut expected_formats: Vec<&str> = FORMATS
        .iter()
        .copied()
        .map(Format::label)
        .filter(|label| *label != "none")
        .collect();
    expected_formats.sort_unstable();
    let actual_formats = registry.transformers().formats();
    assert_eq!(
        actual_formats, expected_formats,
        "register_builtin_factories registered transformer formats {actual_formats:?} but \
         common/matrix.rs's FORMATS names {expected_formats:?}; add the missing format's key to \
         FORMATS (or drop a stale entry) so the matrix covers every registered format",
    );
}

/// A one-column, one-row batch. `Transformer::encode_messages` needs a batch to
/// answer at all, and a single non-null `Int64` column is what every format
/// with an encoder can write.
fn probe_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64]))])
        .expect("probe batch matches its own schema")
}

/// Is `err` the `Transformer` contract's refusal of `capability`, rather than a
/// real failure from a format that does implement it?
///
/// A transformer that has the method answers: `Ok`, or its own error. Only the
/// trait's default body produces `unsupported`, so that exact message is what
/// separates "no such surface" from "surface present, this input rejected".
fn refuses(err: &SaciError, format: &str, capability: &str) -> bool {
    err.message() == unsupported(format, capability).message()
}

/// Every registered transformer's [`Transformer::message_shape`] must agree
/// with whether it really answers both message calls.
///
/// `message_shape()` is a declaration, and six connector call sites
/// (`KafkaSource`, `KafkaSink`, `NatsSource`, `NatsSink`, `TcpIngestSource`,
/// `TcpSink`) refuse a config at *build* time on its `is_none()`. Nothing in
/// the type system ties that declaration to the methods: a transformer
/// declaring `Some` without an encoder passes a message sink's gate and fails
/// on the first batch, and one declaring `None` with both methods is refused
/// for no reason. This test is what ties them together, for every transformer
/// the registry can build.
///
/// The surfaces are probed rather than read off each `impl` block: calling
/// both methods and asking whether the answer is the contract's [`unsupported`]
/// refusal is the only check that cannot be fooled by an override that forwards
/// to a default.
///
/// One `message_shape()` answers for both directions, so a transformer that
/// implements exactly one of the two message methods cannot be declared
/// honestly at all; that is the first assertion, and it fails loudly instead of
/// letting a half surface hide behind `None`.
///
/// Docker-free and not `#[ignore]`d, like [`dimensions_cover_the_registry`]: it
/// builds transformers straight from the registry, with no config, container or
/// running service involved.
///
/// [`Transformer`]: saci_transformer::Transformer
/// [`Transformer::message_shape`]: saci_transformer::Transformer::message_shape
/// [`unsupported`]: saci_transformer::unsupported
#[test]
fn message_shape_agrees_with_the_message_surface() {
    let builder = register_builtin_factories(ServiceBuilder::new());
    let transformers = builder.registry().transformers();
    let batch = probe_batch();

    for format in transformers.formats() {
        let transformer = transformers
            .get(format)
            .expect("a format the registry lists resolves to its factory")
            .build(&ConfigValue::Object(ConfigMap::new()))
            .unwrap_or_else(|e| panic!("transformer '{format}' builds from empty options: {e}"));

        let decodes = match transformer.open_message_decoder(batch.schema()) {
            Ok(_) => true,
            Err(e) => !refuses(&e, format, "decoding discrete messages"),
        };
        let encodes = match transformer.encode_messages(&batch) {
            Ok(_) => true,
            Err(e) => !refuses(&e, format, "encoding discrete messages"),
        };
        let declared = transformer.message_shape();

        assert_eq!(
            decodes, encodes,
            "transformer '{format}' implements open_message_decoder={decodes} and \
             encode_messages={encodes}: half a message surface. One message_shape() answers \
             for both directions, so no declaration describes this transformer; implement the \
             missing method, or make message_shape() direction-aware and split the connector \
             gates that read it",
        );
        assert_eq!(
            declared.is_some(),
            decodes,
            "transformer '{format}' declares message_shape()={declared:?} but implements \
             open_message_decoder={decodes} and encode_messages={encodes}; a message connector \
             refuses a config at build on message_shape().is_none(), so a Some declaration \
             without the methods fails on the first batch and a None declaration with them \
             refuses a config that would work. Fix whichever of the two is wrong in \
             saci-transformer-{format}",
        );
    }
}
