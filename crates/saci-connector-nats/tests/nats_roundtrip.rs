//! [`NatsSource`] and [`NatsSink`] against a real NATS server.
//!
//! Soft-skips without Docker; see `common::try_start`. Each test uses its own
//! subject and stream name so the whole suite can share the one server.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures_util::StreamExt;

use saci_connector_nats::{
    ConnectionConfig, CoreSinkMode, CoreSourceMode, DeliverPolicyConfig, JetstreamSinkMode,
    JetstreamSourceMode, NatsSink, NatsSinkConfig, NatsSource, NatsSourceConfig, SinkMode,
    SourceMode, StreamProvision,
};
use saci_core::dataset::Dataset;
use saci_core::io::sink::Sink;
use saci_core::io::source::{Source, drain_into_dataset};
use saci_transformer::Transformer;
use saci_transformer_arrow_ipc::ArrowIpcTransformer;
use saci_transformer_ndjson::NdjsonTransformer;

const COMPONENT: &str = "TestRow";

/// The default payload format. The factories resolve it from the registry; a
/// direct construction like these tests' hands it over explicitly.
fn ndjson() -> Arc<dyn Transformer> {
    Arc::new(NdjsonTransformer::default())
}

fn arrow_ipc() -> Arc<dyn Transformer> {
    Arc::new(ArrowIpcTransformer::new())
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("total", DataType::Float64, true),
    ]))
}

fn batch_of(schema: Arc<Schema>, ids: &[i64], names: &[&str], totals: &[f64]) -> RecordBatch {
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
            Arc::new(Float64Array::from(totals.to_vec())),
        ],
    )
    .expect("valid batch")
}

fn sample_batch(schema: Arc<Schema>) -> RecordBatch {
    batch_of(schema, &[1, 2, 3], &["a", "b", "c"], &[1.5, 2.5, 3.5])
}

fn connection(url: &str) -> ConnectionConfig {
    ConnectionConfig {
        servers: vec![url.to_string()],
        ..ConnectionConfig::default()
    }
}

/// A bounded core source, so `drain_into_dataset` terminates.
fn core_source_cfg(url: &str, subject: &str) -> NatsSourceConfig {
    NatsSourceConfig {
        connection: connection(url),
        mode: SourceMode::Core(CoreSourceMode {
            subject: subject.to_string(),
            queue_group: None,
        }),
        batch_size: 1000,
        poll_timeout_ms: 2_000,
        stop_at_end: true,
        schema_fields: vec![],
    }
}

fn core_sink_cfg(url: &str, subject: &str) -> NatsSinkConfig {
    NatsSinkConfig {
        connection: connection(url),
        mode: SinkMode::Core(CoreSinkMode {
            subject: subject.to_string(),
            ..CoreSinkMode::default()
        }),
        schema_fields: vec![],
    }
}

/// A bounded JetStream source over a durable pull consumer.
fn js_source_cfg(url: &str, stream: &str, durable: &str) -> NatsSourceConfig {
    NatsSourceConfig {
        connection: connection(url),
        mode: SourceMode::Jetstream(Box::new(JetstreamSourceMode {
            stream: stream.to_string(),
            durable_name: Some(durable.to_string()),
            fetch_expires_ms: 2_000,
            ..JetstreamSourceMode::default()
        })),
        batch_size: 1000,
        poll_timeout_ms: 2_000,
        stop_at_end: true,
        schema_fields: vec![],
    }
}

/// A JetStream sink over `subject`. It names no `stream_provision`, so it
/// exercises the defaults: the stream is created, and its subject list is
/// derived from `subject`.
fn js_sink_cfg(url: &str, stream: &str, subject: &str) -> NatsSinkConfig {
    NatsSinkConfig {
        connection: connection(url),
        mode: SinkMode::Jetstream(Box::new(JetstreamSinkMode {
            stream: stream.to_string(),
            subject: subject.to_string(),
            ..JetstreamSinkMode::default()
        })),
        schema_fields: vec![],
    }
}

fn assert_sample_values(out: &RecordBatch) {
    assert_eq!(out.num_rows(), 3);
    let ids = out
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column is Int64");
    let names = out
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column is Utf8");
    let totals = out
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("total column is Float64");
    assert_eq!(ids.values(), &[1, 2, 3]);
    assert_eq!(
        (0..names.len()).map(|i| names.value(i)).collect::<Vec<_>>(),
        vec!["a", "b", "c"]
    );
    assert_eq!(totals.values(), &[1.5, 2.5, 3.5]);
}

/// Every id the source read, in order.
fn ids_of(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column is Int64")
        .values()
        .to_vec()
}

#[tokio::test]
async fn core_ndjson_round_trips_through_a_subject() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let subject = nats.subject("core-json");
    let schema = schema();

    // Core NATS drops a message with no subscriber, so the source subscribes
    // before anything is published.
    let mut source = NatsSource::new(
        core_source_cfg(&nats.url(), &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    assert!(
        source.next_batch().await.expect("empty poll").is_none(),
        "an empty subject with stop_at_end reports EOF, and subscribes on the way"
    );

    let mut sink = NatsSink::new(
        core_sink_cfg(&nats.url(), &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema.clone());
    let rows = drain_into_dataset(&mut source, &mut dataset, COMPONENT)
        .await
        .expect("drain");
    assert_eq!(rows, 3);
    assert_sample_values(dataset.batch_for(COMPONENT).expect("component present"));
}

#[tokio::test]
async fn core_arrow_ipc_sends_one_message_per_batch() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let subject = nats.subject("core-arrow-ipc");
    let schema = schema();

    // A raw subscription counts the messages, which is what proves arrow-ipc
    // sent one for the whole batch rather than one per row.
    let client = nats.client().await;
    let mut raw = client
        .subscribe(subject.clone())
        .await
        .expect("raw subscribe");

    let mut source = NatsSource::new(
        core_source_cfg(&nats.url(), &subject),
        schema.clone(),
        arrow_ipc(),
    )
    .expect("source builds");
    assert!(source.next_batch().await.expect("empty poll").is_none());

    let mut sink = NatsSink::new(
        core_sink_cfg(&nats.url(), &subject),
        schema.clone(),
        arrow_ipc(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let message = tokio::time::timeout(Duration::from_secs(5), raw.next())
        .await
        .expect("one message arrives")
        .expect("the subscription is live");
    assert_eq!(message.subject.as_str(), subject);
    assert!(
        tokio::time::timeout(Duration::from_millis(500), raw.next())
            .await
            .is_err(),
        "arrow-ipc emits one message per batch, so there is no second one"
    );

    let out = source
        .next_batch()
        .await
        .expect("next_batch")
        .expect("the batch arrived");
    assert_eq!(out.schema().fields(), schema.fields());
    assert_sample_values(&out);
}

#[tokio::test]
async fn core_subject_field_routes_each_row_to_its_own_subject() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let base = nats.subject("core-route");
    let schema = schema();
    let client = nats.client().await;
    let mut first = client
        .subscribe(format!("{base}.a"))
        .await
        .expect("subscribe a");
    let mut second = client
        .subscribe(format!("{base}.b"))
        .await
        .expect("subscribe b");

    let mut sink = NatsSink::new(
        NatsSinkConfig {
            mode: SinkMode::Core(CoreSinkMode {
                subject: format!("{base}.fallback"),
                subject_field: Some("name".to_string()),
                ..CoreSinkMode::default()
            }),
            ..core_sink_cfg(&nats.url(), &base)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    // A rendered cell replaces the configured subject outright, so the column
    // holds the whole subject.
    sink.write_batch(&batch_of(
        schema,
        &[1, 2],
        &[&format!("{base}.a"), &format!("{base}.b")],
        &[1.5, 2.5],
    ))
    .await
    .expect("write_batch");
    sink.finish().await.expect("finish");

    for (label, subscriber) in [("a", &mut first), ("b", &mut second)] {
        let message = tokio::time::timeout(Duration::from_secs(5), subscriber.next())
            .await
            .unwrap_or_else(|_| panic!("a message must reach {base}.{label}"))
            .expect("the subscription is live");
        assert_eq!(message.subject.as_str(), format!("{base}.{label}"));
    }
}

#[tokio::test]
async fn core_header_fields_and_static_headers_reach_the_message() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let subject = nats.subject("core-headers");
    let schema = schema();
    let client = nats.client().await;
    let mut raw = client
        .subscribe(subject.clone())
        .await
        .expect("raw subscribe");

    let mut headers = BTreeMap::new();
    headers.insert("X-Producer".to_string(), "saci".to_string());
    let mut header_fields = BTreeMap::new();
    header_fields.insert("X-Id".to_string(), "id".to_string());

    let mut sink = NatsSink::new(
        NatsSinkConfig {
            mode: SinkMode::Core(CoreSinkMode {
                subject: subject.clone(),
                headers,
                header_fields,
                ..CoreSinkMode::default()
            }),
            ..core_sink_cfg(&nats.url(), &subject)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&batch_of(schema, &[7], &["a"], &[1.5]))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let message = tokio::time::timeout(Duration::from_secs(5), raw.next())
        .await
        .expect("one message arrives")
        .expect("the subscription is live");
    let got = message.headers.expect("the message carries headers");
    assert_eq!(
        got.get("X-Producer").map(|v| v.as_str()),
        Some("saci"),
        "the static header must be on every message"
    );
    assert_eq!(
        got.get("X-Id").map(|v| v.as_str()),
        Some("7"),
        "the rendered cell must become the header value"
    );
}

#[tokio::test]
async fn a_queue_group_splits_a_subject_between_two_sources() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let subject = nats.subject("core-queue");
    let schema = schema();
    let group = "saci-queue".to_string();

    let mut first = NatsSource::new(
        NatsSourceConfig {
            mode: SourceMode::Core(CoreSourceMode {
                subject: subject.clone(),
                queue_group: Some(group.clone()),
            }),
            ..core_source_cfg(&nats.url(), &subject)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("first source builds");
    let mut second = NatsSource::new(
        NatsSourceConfig {
            mode: SourceMode::Core(CoreSourceMode {
                subject: subject.clone(),
                queue_group: Some(group),
            }),
            ..core_source_cfg(&nats.url(), &subject)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("second source builds");
    // A first poll subscribes; the stream runner primes every source's first
    // poll before the round-robin blocks on any of them, so both
    // subscriptions exist before publishing starts.
    assert!(first.next_batch().await.expect("empty poll").is_none());
    assert!(second.next_batch().await.expect("empty poll").is_none());

    let mut sink = NatsSink::new(
        core_sink_cfg(&nats.url(), &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    let ids: Vec<i64> = (1..=8).collect();
    let names: Vec<&str> = ids.iter().map(|_| "x").collect();
    let totals: Vec<f64> = ids.iter().map(|i| *i as f64).collect();
    sink.write_batch(&batch_of(schema, &ids, &names, &totals))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut seen = Vec::new();
    for source in [&mut first, &mut second] {
        while let Some(batch) = source.next_batch().await.expect("next_batch") {
            seen.extend(ids_of(&batch));
        }
    }
    seen.sort_unstable();
    assert_eq!(
        seen, ids,
        "a queue group must deliver every message exactly once across the group"
    );
}

#[tokio::test]
async fn jetstream_round_trips_through_a_provisioned_stream() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_ROUNDTRIP");
    let subject = nats.subject("js-roundtrip");
    let schema = schema();

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    // A stream that did not exist before the sink ran now holds the three
    // messages, which is what `stream_provision.create = true` bought.
    let mut info = nats
        .jetstream()
        .await
        .get_stream(&stream)
        .await
        .expect("the sink provisioned the stream");
    assert_eq!(
        info.info().await.expect("stream info").state.messages,
        3,
        "ndjson emits one message per row"
    );

    let mut source = NatsSource::new(
        js_source_cfg(&nats.url(), &stream, "saci-roundtrip"),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema.clone());
    let rows = drain_into_dataset(&mut source, &mut dataset, COMPONENT)
        .await
        .expect("drain");
    assert_eq!(rows, 3);
    assert_sample_values(dataset.batch_for(COMPONENT).expect("component present"));
}

#[tokio::test]
async fn jetstream_acks_the_previous_batch_at_the_start_of_the_next_call() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_ACK");
    let subject = nats.subject("js-ack");
    let schema = schema();
    let durable = "saci-ack";

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut source = NatsSource::new(
        js_source_cfg(&nats.url(), &stream, durable),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    let batch = source
        .next_batch()
        .await
        .expect("next_batch")
        .expect("three messages are waiting");
    assert_eq!(batch.num_rows(), 3);

    let js = nats.jetstream().await;
    let stream_handle = js.get_stream(&stream).await.expect("stream exists");
    let outstanding = stream_handle
        .consumer_info(durable)
        .await
        .expect("consumer info")
        .num_ack_pending;
    assert_eq!(
        outstanding, 3,
        "the acks must trail the handover by one call, which is what makes \
         delivery at-least-once"
    );

    assert!(
        source
            .next_batch()
            .await
            .expect("second next_batch")
            .is_none(),
        "the stream is drained"
    );
    let outstanding = stream_handle
        .consumer_info(durable)
        .await
        .expect("consumer info")
        .num_ack_pending;
    assert_eq!(
        outstanding, 0,
        "the second call must have acknowledged the first batch"
    );
}

#[tokio::test]
async fn a_duplicate_message_id_is_deduplicated_inside_the_window() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_DEDUPE");
    let subject = nats.subject("js-dedupe");
    let schema = schema();

    let mut sink = NatsSink::new(
        NatsSinkConfig {
            mode: SinkMode::Jetstream(Box::new(JetstreamSinkMode {
                stream: stream.clone(),
                subject: subject.clone(),
                message_id_field: Some("id".to_string()),
                // Only the duplicate window is named; `create` and the subject
                // list are the defaults.
                stream_provision: StreamProvision {
                    duplicate_window_ms: 120_000,
                    ..StreamProvision::default()
                },
                ..JetstreamSinkMode::default()
            })),
            ..js_sink_cfg(&nats.url(), &stream, &subject)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("first write");
    sink.write_batch(&sample_batch(schema))
        .await
        .expect("second write of the same rows");
    sink.finish().await.expect("finish");

    let mut info = nats
        .jetstream()
        .await
        .get_stream(&stream)
        .await
        .expect("stream exists");
    assert_eq!(
        info.info().await.expect("stream info").state.messages,
        3,
        "the duplicate window must drop the second copy of every Nats-Msg-Id"
    );
}

#[tokio::test]
async fn deliver_policy_new_skips_what_was_already_in_the_stream() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_NEW");
    let subject = nats.subject("js-new");
    let schema = schema();

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&batch_of(schema.clone(), &[1, 2], &["a", "b"], &[1.0, 2.0]))
        .await
        .expect("seed write");

    // Creating the consumer is what fixes "new", so the source must start
    // before the second write and after the first.
    let mut source = NatsSource::new(
        NatsSourceConfig {
            mode: SourceMode::Jetstream(Box::new(JetstreamSourceMode {
                stream: stream.clone(),
                durable_name: Some("saci-new".to_string()),
                deliver_policy: DeliverPolicyConfig::New,
                fetch_expires_ms: 2_000,
                ..JetstreamSourceMode::default()
            })),
            ..js_source_cfg(&nats.url(), &stream, "saci-new")
        },
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    assert!(
        source.next_batch().await.expect("first poll").is_none(),
        "kind = \"new\" must skip the seeded rows"
    );

    sink.write_batch(&batch_of(schema, &[3], &["c"], &[3.0]))
        .await
        .expect("second write");
    sink.finish().await.expect("finish");

    let batch = source
        .next_batch()
        .await
        .expect("next_batch")
        .expect("the row published after the consumer was created");
    assert_eq!(ids_of(&batch), vec![3]);
}

#[tokio::test]
async fn stop_at_end_reports_eof_once_the_stream_is_drained() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_EOF");
    let subject = nats.subject("js-eof");
    let schema = schema();

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut source = NatsSource::new(
        js_source_cfg(&nats.url(), &stream, "saci-eof"),
        schema,
        ndjson(),
    )
    .expect("source builds");
    assert_eq!(
        source
            .next_batch()
            .await
            .expect("first next_batch")
            .expect("three messages are waiting")
            .num_rows(),
        3
    );
    assert!(
        source
            .next_batch()
            .await
            .expect("second next_batch")
            .is_none(),
        "a drained stream with stop_at_end reports EOF"
    );
}

#[tokio::test]
async fn an_empty_window_is_not_eof_while_the_consumer_still_owes_messages() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_OWED");
    let subject = nats.subject("js-owed");
    let schema = schema();

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    // `ack_wait` deliberately outlasts one `fetch_expires_ms` window: a
    // redelivery therefore cannot land inside the single window a source that
    // trusts an empty window would have asked for.
    let cfg = |url: &str| NatsSourceConfig {
        mode: SourceMode::Jetstream(Box::new(JetstreamSourceMode {
            stream: stream.clone(),
            durable_name: Some("saci-owed".to_string()),
            fetch_expires_ms: 1_000,
            ack_wait_ms: 4_000,
            ..JetstreamSourceMode::default()
        })),
        poll_timeout_ms: 3_000,
        ..js_source_cfg(url, &stream, "saci-owed")
    };

    // One source takes the whole stream and is dropped without ever
    // acknowledging it: the server counts all three messages as delivered and
    // holds them until `ack_wait` expires. That is the state a window nobody
    // read leaves behind, and from the next consumer's side it is
    // indistinguishable from a drained stream until the server is asked.
    let mut abandoned =
        NatsSource::new(cfg(&nats.url()), schema.clone(), ndjson()).expect("source builds");
    let taken = abandoned
        .next_batch()
        .await
        .expect("first next_batch")
        .expect("three messages are waiting");
    assert_eq!(ids_of(&taken), vec![1, 2, 3]);
    drop(abandoned);

    let mut resumed =
        NatsSource::new(cfg(&nats.url()), schema.clone(), ndjson()).expect("source builds");
    let redelivered = tokio::time::timeout(Duration::from_secs(20), resumed.next_batch())
        .await
        .expect("the confirm-drained budget covers one ack_wait")
        .expect("next_batch")
        .expect("a stream that still owes three messages must not report EOF");
    assert_eq!(
        ids_of(&redelivered),
        vec![1, 2, 3],
        "every message the consumer still owed must come back"
    );
}

#[tokio::test]
async fn a_cancelled_window_keeps_the_messages_already_pulled() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_CANCEL");
    let subject = nats.subject("js-cancel");
    let schema = schema();

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    // Live rather than `stop_at_end`, and `batch_size` 2: a window this source
    // cannot close on one message is a window that can be cancelled while it
    // holds that message.
    let mut source = NatsSource::new(
        NatsSourceConfig {
            batch_size: 2,
            poll_timeout_ms: 5_000,
            stop_at_end: false,
            ..js_source_cfg(&nats.url(), &stream, "saci-cancel")
        },
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");

    sink.write_batch(&batch_of(schema.clone(), &[1], &["a"], &[1.0]))
        .await
        .expect("write_batch");
    assert!(
        tokio::time::timeout(Duration::from_millis(1_500), source.next_batch())
            .await
            .is_err(),
        "a window one message short of batch_size must still be open, \
         or this test cancels nothing"
    );

    // Two more, so the resumed window has to ask for the one place it has
    // left rather than for another whole `batch_size`.
    sink.write_batch(&batch_of(schema.clone(), &[2, 3], &["b", "c"], &[2.0, 3.0]))
        .await
        .expect("write_batch");
    let resumed = tokio::time::timeout(Duration::from_secs(15), source.next_batch())
        .await
        .expect("the resumed poll must finish inside its own window")
        .expect("the resumed poll must not error")
        .expect("a live source blocks until it has rows");
    assert_eq!(
        ids_of(&resumed),
        vec![1, 2],
        "message 1 was pulled by the cancelled call, so it must be handed over \
         by this one rather than acknowledged unseen, and the resumed window \
         must still hold at most batch_size rows"
    );
}

#[tokio::test]
async fn a_missing_stream_names_itself_when_provisioning_is_off() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_MISSING");
    let schema = schema();

    let mut source = NatsSource::new(
        NatsSourceConfig {
            mode: SourceMode::Jetstream(Box::new(JetstreamSourceMode {
                stream: stream.clone(),
                durable_name: Some("saci-missing".to_string()),
                stream_provision: StreamProvision {
                    create: false,
                    ..StreamProvision::default()
                },
                ..JetstreamSourceMode::default()
            })),
            ..js_source_cfg(&nats.url(), &stream, "saci-missing")
        },
        schema,
        ndjson(),
    )
    .expect("source builds: it opens nothing");
    let err = source
        .next_batch()
        .await
        .expect_err("create = false against a stream that does not exist");
    assert_eq!(err.category(), "generic");
    assert!(err.message().contains(&stream), "got: {err}");
    assert!(
        err.message().contains("stream_provision.create = true"),
        "the error must name the opt-in that would have created it, got: {err}"
    );
}

/// The flipped default is load bearing: a source pointed at a stream nobody has
/// created yet creates it, rather than failing.
#[tokio::test]
async fn a_source_creates_its_stream_by_default() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_SOURCE_CREATES");
    let subject = nats.subject("js-source-creates");
    let schema = schema();

    let mut source = NatsSource::new(
        NatsSourceConfig {
            mode: SourceMode::Jetstream(Box::new(JetstreamSourceMode {
                stream: stream.clone(),
                durable_name: Some("saci-creates".to_string()),
                // The subject list of the created stream comes from here.
                filter_subjects: vec![subject.clone()],
                fetch_expires_ms: 2_000,
                ..JetstreamSourceMode::default()
            })),
            ..js_source_cfg(&nats.url(), &stream, "saci-creates")
        },
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    assert!(
        source.next_batch().await.expect("first poll").is_none(),
        "a freshly created stream is empty, so this is EOF and not an error"
    );

    let mut info = nats
        .jetstream()
        .await
        .get_stream(&stream)
        .await
        .expect("the source provisioned the stream");
    assert_eq!(
        info.info().await.expect("stream info").config.subjects,
        vec![subject.clone()],
        "filter_subjects supplied the created stream's subject list"
    );

    // And the stream it made actually captures what the source reads.
    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema))
        .await
        .expect("write");
    sink.finish().await.expect("finish");

    let batch = source
        .next_batch()
        .await
        .expect("next_batch")
        .expect("the rows the sink published");
    assert_eq!(ids_of(&batch), vec![1, 2, 3]);
}

#[tokio::test]
async fn estimated_rows_reports_what_jetstream_still_owes() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_PENDING");
    let subject = nats.subject("js-pending");
    let schema = schema();

    let mut sink = NatsSink::new(
        js_sink_cfg(&nats.url(), &stream, &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut source = NatsSource::new(
        NatsSourceConfig {
            batch_size: 1,
            ..js_source_cfg(&nats.url(), &stream, "saci-pending")
        },
        schema,
        ndjson(),
    )
    .expect("source builds");
    assert_eq!(
        source.estimated_rows(),
        None,
        "nothing has been pulled yet, so there is no number to report"
    );
    source
        .next_batch()
        .await
        .expect("next_batch")
        .expect("one message");
    assert_eq!(
        source.estimated_rows(),
        Some(2),
        "two of the three messages are still waiting for this consumer"
    );
}

/// An undecodable payload is never acknowledged, so JetStream keeps
/// redelivering it and every attempt fails on the same bytes.
/// `max_decode_attempts` is what turns that into a bounded, named outcome:
/// the source terminates that one message, reports an error carrying the
/// stream and the sequence it retired, and then advances — so a stream with
/// one poison message neither loops forever nor swallows it in silence, and
/// the message that shared its window still arrives.
#[tokio::test]
async fn an_undecodable_message_is_retired_by_name_instead_of_redelivered_forever() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_POISON");
    let subject = nats.subject("js-poison");
    let schema = schema();

    // Published raw: the poison payload is exactly what no ndjson decoder can
    // accept, which a sink writing this schema could never produce.
    let js = nats.jetstream().await;
    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream.clone(),
        subjects: vec![subject.clone()],
        ..Default::default()
    })
    .await
    .expect("stream is created");
    for payload in [
        "}{ this is not ndjson",
        "{\"id\":2,\"name\":\"b\",\"total\":2.5}",
    ] {
        js.publish(subject.clone(), payload.into())
            .await
            .expect("publish")
            .await
            .expect("publish ack");
    }

    const ATTEMPTS: u32 = 2;
    let mut source = NatsSource::new(
        NatsSourceConfig {
            mode: SourceMode::Jetstream(Box::new(JetstreamSourceMode {
                stream: stream.clone(),
                durable_name: Some("saci-poison".to_string()),
                // Short enough that two redeliveries fit the test's budget.
                ack_wait_ms: 1_000,
                fetch_expires_ms: 500,
                max_decode_attempts: ATTEMPTS,
                ..JetstreamSourceMode::default()
            })),
            poll_timeout_ms: 1_000,
            ..js_source_cfg(&nats.url(), &stream, "saci-poison")
        },
        schema,
        ndjson(),
    )
    .expect("source builds");

    let mut errors: Vec<String> = Vec::new();
    let mut ids: Vec<i64> = Vec::new();
    let mut reached_eof = false;
    // One call per iteration, each bounded: a source stuck on the poison
    // message fails this loop instead of hanging the suite.
    for _ in 0..8 {
        match tokio::time::timeout(Duration::from_secs(20), source.next_batch())
            .await
            .expect("a next_batch that neither decodes nor gives up is a livelock")
        {
            Ok(Some(batch)) => ids.extend(ids_of(&batch)),
            Ok(None) => {
                reached_eof = true;
                break;
            }
            Err(e) => errors.push(e.message().to_string()),
        }
    }

    assert!(
        reached_eof,
        "the source must advance past the poison message; errors so far: {errors:?}"
    );
    assert_eq!(
        errors.len() as u32,
        ATTEMPTS,
        "the undecodable message costs one error per permitted delivery and no more: {errors:?}"
    );
    let retired = errors.last().expect("at least one attempt failed");
    assert!(
        retired.contains(&stream) && retired.contains("@1"),
        "the last error must name the stream and the sequence it retired: {retired}"
    );
    assert!(
        retired.contains("terminated"),
        "the last error must say the message was retired, not merely that it failed: {retired}"
    );
    assert_eq!(
        ids,
        vec![2],
        "only the poison message is lost; the one that shared its window arrives"
    );
}

/// `Source::request_batch_rows` caps the very next collected window rather
/// than being a starting size a poll timeout eventually outgrows: with more
/// messages waiting than the hint, `next_batch` returns once the hint is met
/// instead of collecting everything the subject already holds. A later,
/// larger hint then collects more in one window.
#[tokio::test]
async fn request_batch_rows_caps_the_next_collected_window() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let subject = nats.subject("request-batch-rows");
    let schema = schema();

    // Core NATS drops a message with no subscriber, so the source subscribes
    // before anything is published.
    let mut source = NatsSource::new(
        core_source_cfg(&nats.url(), &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    assert!(
        source.next_batch().await.expect("empty poll").is_none(),
        "an empty subject with stop_at_end reports EOF, and subscribes on the way"
    );

    let mut sink = NatsSink::new(
        core_sink_cfg(&nats.url(), &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&batch_of(
        schema.clone(),
        &[1, 2, 3, 4, 5, 6],
        &["a", "b", "c", "d", "e", "f"],
        &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
    ))
    .await
    .expect("write_batch");
    sink.finish().await.expect("finish");

    // A hint smaller than what is already waiting caps the window before the
    // poll timeout ever has to decide anything.
    source.request_batch_rows(2);
    let first = source
        .next_batch()
        .await
        .expect("first poll")
        .expect("messages are waiting");
    assert!(
        first.num_rows() <= 2,
        "a batch_rows hint of 2 must cap the window at 2, got {}",
        first.num_rows()
    );

    // A larger hint on the next call collects more messages in one window.
    source.request_batch_rows(100);
    let second = source
        .next_batch()
        .await
        .expect("second poll")
        .expect("remaining messages are waiting");
    assert!(
        second.num_rows() > first.num_rows(),
        "a larger hint must collect more than the earlier smaller one, got {} then {}",
        first.num_rows(),
        second.num_rows()
    );

    // `stop_at_end` drains whatever remains and then reports EOF.
    let mut total = first.num_rows() + second.num_rows();
    while let Some(batch) = source.next_batch().await.expect("drain remainder") {
        total += batch.num_rows();
    }
    assert_eq!(total, 6, "every published row must eventually arrive");
}

/// A hint of `0` must not produce a zero-sized fetch that can never advance:
/// `request_batch_rows` clamps it to at least 1, so the source still makes
/// progress and eventually drains every row.
#[tokio::test]
async fn a_zero_batch_rows_hint_still_advances_the_source() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let subject = nats.subject("request-batch-rows-zero");
    let schema = schema();

    let mut source = NatsSource::new(
        core_source_cfg(&nats.url(), &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    assert!(
        source.next_batch().await.expect("empty poll").is_none(),
        "an empty subject with stop_at_end reports EOF, and subscribes on the way"
    );

    let mut sink = NatsSink::new(
        core_sink_cfg(&nats.url(), &subject),
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    source.request_batch_rows(0);

    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema.clone());
    let rows = drain_into_dataset(&mut source, &mut dataset, COMPONENT)
        .await
        .expect("drain");
    assert_eq!(
        rows, 3,
        "a hint clamped to 1 must still drain every row across enough polls"
    );
}

/// Every row the stream holds, in order.
async fn rows_in_stream(nats: &common::NatsContainer, stream: &str, durable: &str) -> Vec<i64> {
    let mut source = NatsSource::new(
        js_source_cfg(&nats.url(), stream, durable),
        schema(),
        ndjson(),
    )
    .expect("source builds");
    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema());
    drain_into_dataset(&mut source, &mut dataset, COMPONENT)
        .await
        .expect("drain");
    match dataset.batch_for(COMPONENT) {
        Some(batch) => ids_of(batch),
        None => Vec::new(),
    }
}

/// A JetStream context's ack permits are only returned once their own ack
/// arrives, so a batch larger than `max_ack_inflight` deadlocks a publish loop
/// that defers every await to after the loop. Draining in windows is what
/// makes the batch go through.
#[tokio::test]
async fn a_jetstream_sink_drains_acks_in_windows_above_max_ack_inflight() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_ACK_WINDOW");
    let subject = nats.subject("js-ack-window");
    let schema = schema();
    let six = batch_of(
        schema.clone(),
        &[1, 2, 3, 4, 5, 6],
        &["a", "b", "c", "d", "e", "f"],
        &[1.5, 2.5, 3.5, 4.5, 5.5, 6.5],
    );

    let mut sink = NatsSink::new(
        NatsSinkConfig {
            mode: SinkMode::Jetstream(Box::new(JetstreamSinkMode {
                stream: stream.clone(),
                subject: subject.clone(),
                max_ack_inflight: 2,
                backpressure_on_inflight: true,
                ..JetstreamSinkMode::default()
            })),
            ..js_sink_cfg(&nats.url(), &stream, &subject)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");

    tokio::time::timeout(Duration::from_secs(10), sink.write_batch(&six))
        .await
        .expect("six messages through a two-permit pool must not deadlock")
        .expect("write_batch");

    assert_eq!(
        rows_in_stream(&nats, &stream, "saci-ack-window").await,
        vec![1, 2, 3, 4, 5, 6],
        "every row must reach the stream, in publish order"
    );
}

/// `atomic_batch` publishes the whole batch as one unit, so the commit ack is
/// what proves the stream has all of it.
#[tokio::test]
async fn a_jetstream_sink_atomic_batch_commits_a_multi_row_batch() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_ATOMIC");
    let subject = nats.subject("js-atomic");
    let schema = schema();

    let mut sink = NatsSink::new(
        NatsSinkConfig {
            mode: SinkMode::Jetstream(Box::new(JetstreamSinkMode {
                stream: stream.clone(),
                subject: subject.clone(),
                atomic_batch: true,
                stream_provision: StreamProvision {
                    allow_atomic: true,
                    ..StreamProvision::default()
                },
                ..JetstreamSinkMode::default()
            })),
            ..js_sink_cfg(&nats.url(), &stream, &subject)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("an atomic batch of three messages commits");
    sink.finish().await.expect("finish");

    let mut info = nats
        .jetstream()
        .await
        .get_stream(&stream)
        .await
        .expect("the sink provisioned the stream");
    assert_eq!(
        info.info().await.expect("stream info").state.messages,
        3,
        "the committed batch stores every message, the commit marker included"
    );

    assert_eq!(
        rows_in_stream(&nats, &stream, "saci-atomic").await,
        vec![1, 2, 3],
        "a committed atomic batch must read back in stream order"
    );
}

/// A one-message batch at `max_ack_inflight = 1` makes the final ack drain find
/// nothing pending, which must not be mistaken for sequence 0: the next guarded
/// batch would then claim a last sequence the stream never had, and be refused.
#[tokio::test]
async fn a_guarded_sink_keeps_its_sequence_across_an_exact_window() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_EXACT_WINDOW");
    let subject = nats.subject("js-exact-window");
    let schema = schema();

    let mut sink = NatsSink::new(
        NatsSinkConfig {
            mode: SinkMode::Jetstream(Box::new(JetstreamSinkMode {
                stream: stream.clone(),
                subject: subject.clone(),
                atomic_batch: true,
                expected_last_sequence: true,
                max_ack_inflight: 1,
                stream_provision: StreamProvision {
                    allow_atomic: true,
                    ..StreamProvision::default()
                },
                ..JetstreamSinkMode::default()
            })),
            ..js_sink_cfg(&nats.url(), &stream, &subject)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");

    for id in 1..=3_i64 {
        let row = batch_of(schema.clone(), &[id], &["x"], &[id as f64]);
        sink.write_batch(&row)
            .await
            .unwrap_or_else(|e| panic!("guarded write {id}: {e}"));
    }

    assert_eq!(
        rows_in_stream(&nats, &stream, "saci-exact-window").await,
        vec![1, 2, 3],
        "every guarded batch must expect the sequence its predecessor committed"
    );
}

/// `expected_last_sequence` turns the stream into a single-writer log: a publish
/// that would land after someone else's is refused rather than reordering the
/// stream.
#[tokio::test]
async fn expected_last_sequence_rejects_an_interleaved_publish() {
    let Some(nats) = common::try_start().await else {
        return;
    };
    let stream = nats.stream("JS_EXPECTED_SEQ");
    let subject = nats.subject("js-expected-seq");
    let schema = schema();

    let mut sink = NatsSink::new(
        NatsSinkConfig {
            mode: SinkMode::Jetstream(Box::new(JetstreamSinkMode {
                stream: stream.clone(),
                subject: subject.clone(),
                atomic_batch: true,
                expected_last_sequence: true,
                stream_provision: StreamProvision {
                    allow_atomic: true,
                    ..StreamProvision::default()
                },
                ..JetstreamSinkMode::default()
            })),
            ..js_sink_cfg(&nats.url(), &stream, &subject)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("the first guarded batch has nothing to expect");

    // A second writer lands one message between the two guarded batches.
    let foreign = br#"{"id":999,"name":"foreign","total":0.0}"#;
    nats.jetstream()
        .await
        .publish(subject.clone(), foreign.as_slice().into())
        .await
        .expect("publish")
        .await
        .expect("foreign message acked");

    let err = sink
        .write_batch(&sample_batch(schema))
        .await
        .expect_err("the stream moved on, so the guard must refuse");
    assert!(
        err.message().contains("expected last sequence")
            || err.message().contains("wrong last sequence"),
        "the server's own rejection must surface, got: {err}"
    );
    assert_eq!(
        rows_in_stream(&nats, &stream, "saci-expected-seq").await,
        vec![1, 2, 3, 999],
        "the refused batch must leave the stream exactly as it was, so the \
         second copy of rows 1..3 is what would show up here"
    );
}
