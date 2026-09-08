//! The connector/transformer/processor matrix, in one test.
//!
//! Every `{source connector, sink connector, byte format, processor runtime}`
//! combination is built from KDL, assembled through the real factory registry,
//! run through the real standalone or stream runner, and asserted against the
//! capability table in [`matrix`]. The maximal workflow follows in the same
//! test.
//!
//! There is exactly one test function because nextest gives every test its own
//! process: a second one would start a second Kafka broker, NATS server,
//! PostgreSQL server and S3 endpoint. All four are started once here, behind a
//! `OnceCell`, and shared by every case.
//!
//! ```bash
//! rustup target add wasm32-wasip2
//! cargo build --release -p saci-processor-smoketest --target wasm32-wasip2
//! cargo build -p saci-plugin-smoketest
//! cargo nextest run -p saci-service --all-features --test connector_matrix \
//!     --run-ignored ignored-only
//! ```
//!
//! `#[ignore]`d: it is a whole-matrix sweep against four containers, not a
//! per-edit check.

#![cfg(all(
    feature = "service",
    feature = "wasm",
    feature = "plugin",
    feature = "connector-channel",
    feature = "connector-file",
    feature = "connector-http",
    feature = "connector-kafka",
    feature = "connector-nats",
    feature = "connector-postgresql",
    feature = "connector-redb",
    feature = "connector-s3",
    feature = "connector-saci",
    feature = "connector-tcp",
    feature = "connector-turso",
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
use tokio::sync::Semaphore;

#[path = "common/matrix.rs"]
mod matrix;
#[path = "common/smoketest.rs"]
mod smoketest;

use matrix::{
    CONNECTORS, Case, FORMATS, Fixtures, Format, Report, Resources, run_case, run_maximal,
};

/// How many cases build and run at once.
///
/// Each case owns its own temp directory, channel, topic, stream, table and
/// object prefix, and every listener binds `127.0.0.1:0`, so the only shared
/// state is the four containers.
const CONCURRENCY: usize = 16;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "whole connector matrix against four containers; run explicitly"]
async fn full_matrix() {
    let started = Instant::now();
    let fixtures = Fixtures::resolve(smoketest::smoketest_wasm_path()).expect("processor fixtures");
    let resources = Resources::default();
    let permits = Semaphore::new(CONCURRENCY);

    // Every container is started before any case runs: a readiness poll that
    // shares the poll loop with a thousand case futures can miss its budget
    // for want of being polled.
    let up = resources.warm().await;
    println!("resources: {}", Resources::describe(&up));

    // `PipelineRuntime` is `?Send`, so a case future cannot be spawned onto the
    // runtime: `join_all` polls every case on this one task and the semaphore
    // bounds how many are past their resource gate. The `tcp`-sourced cases run
    // as a second phase because their stream runner is stopped by cancellation
    // on a wall-clock settle window, which only holds when the task is not also
    // driving the other thousand cases.
    let (batch_cases, stream_cases) = Case::phases();
    let batch_reports = futures::future::join_all(
        batch_cases
            .iter()
            .copied()
            .map(|case| run_case(case, &resources, &fixtures, &permits)),
    )
    .await;
    let stream_reports = futures::future::join_all(
        stream_cases
            .iter()
            .copied()
            .map(|case| run_case(case, &resources, &fixtures, &permits)),
    )
    .await;

    let maximal = run_maximal(&resources, &fixtures).await;

    let mut reports = batch_reports;
    reports.extend(stream_reports);
    let report = Report::new(reports, started.elapsed());
    report.print();
    match &maximal {
        Ok(report) => report.print(),
        Err(e) => println!("\n=== maximal workflow ===\nFAILED: {e}"),
    }

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
    maximal.expect("the maximal workflow must build and deliver rows");
}

/// The registered factories `register_builtin_factories` wires up must match
/// this file's own [`CONNECTORS`]/[`FORMATS`] dimension lists exactly.
///
/// Without this test, registering a ninth connector or a sixth transformer
/// format compiles and runs clean: the matrix simply never builds a case for
/// it. This test builds the real registry the way [`run_case`] does and
/// compares its shape against the matrix's own lists, so a factory
/// registered outside them fails here instead of silently losing coverage.
///
/// [`Registry`] and [`TransformerRegistry`] expose counts, and, for
/// transformers only, the sorted list of registered format names
/// ([`TransformerRegistry::formats`]); there is no equivalent enumeration for
/// source or sink type names, so those two dimensions are checked by count
/// instead.
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

    let source_count = registry.source_count();
    let sink_count = registry.sink_count();
    let expected_connectors = CONNECTORS.len();
    assert_eq!(
        source_count, expected_connectors,
        "register_builtin_factories registered {source_count} source factories but \
         common/matrix.rs's CONNECTORS lists {expected_connectors}; a source connector was \
         registered outside that array (or one listed there no longer registers); update \
         CONNECTORS to match",
    );
    assert_eq!(
        sink_count, expected_connectors,
        "register_builtin_factories registered {sink_count} sink factories but \
         common/matrix.rs's CONNECTORS lists {expected_connectors}; a sink connector was \
         registered outside that array (or one listed there no longer registers); update \
         CONNECTORS to match",
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
