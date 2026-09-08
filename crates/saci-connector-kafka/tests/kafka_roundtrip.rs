//! [`KafkaSource`] and [`KafkaSink`] against a real Kafka broker.
//!
//! Soft-skips without Docker; see `common::try_start`. Each test uses its own
//! topic name so the whole suite can share the one broker.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use rdkafka::Message as _;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;

use saci_connector_kafka::{
    KafkaSink, KafkaSinkConfig, KafkaSource, KafkaSourceConfig, TopicProvision,
};
use saci_core::dataset::Dataset;
use saci_core::io::sink::Sink;
use saci_core::io::source::{Source, drain_into_dataset};
use saci_transformer::Transformer;
use saci_transformer_arrow_ipc::ArrowIpcTransformer;
use saci_transformer_ndjson::NdjsonTransformer;

const COMPONENT: &str = "TestRow";

/// The window `run_stream`'s source prime gives a source's first
/// `next_batch` before it drops the future.
const PRIME_WINDOW: Duration = Duration::from_secs(1);

/// A poll window wide enough that idling one out is unmistakable next to
/// [`DRAINED_BOUND`].
const CANCEL_POLL_MS: u64 = 45_000;

/// What a `stop_at_end` source that has read everything may take to hand it
/// over. Three times a cold subscribe-and-fetch, a third of the poll window
/// it must not sit through.
const DRAINED_BOUND: Duration = Duration::from_secs(15);

/// The poll window the live-mode cancellation test gives its source. A poll
/// bounded above it observes what a source that lost a message returns,
/// instead of ending on a timeout that says only "slow".
const LIVE_POLL_MS: u64 = 20_000;

/// The payload format most of these tests use. The factories are handed the
/// transformer the host resolved; a direct construction like these tests'
/// builds it here.
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

/// Like [`batch_of`], with explicit per-row nulls: what the compacted-topic
/// tests need for a tombstone-shaped row (every non-key column null) or a
/// partial update.
fn nullable_batch(
    schema: Arc<Schema>,
    ids: &[i64],
    names: &[Option<&str>],
    totals: &[Option<f64>],
) -> RecordBatch {
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

/// A source config with sane, fast-failing defaults; override fields with
/// `..source_cfg(...)`.
fn source_cfg(brokers: &str, topic: &str, group_id: &str) -> KafkaSourceConfig {
    KafkaSourceConfig {
        brokers: brokers.to_string(),
        topic: topic.to_string(),
        group_id: group_id.to_string(),
        batch_size: 1000,
        poll_timeout_ms: 5_000,
        auto_offset_reset: "earliest".to_string(),
        commit_on_drain: true,
        stop_at_end: true,
        key_field: None,
        compacted: false,
        provision: TopicProvision::default(),
        properties: BTreeMap::new(),
        schema_fields: vec![],
    }
}

/// A sink config with sane defaults; override fields with `..sink_cfg(...)`.
fn sink_cfg(brokers: &str, topic: &str) -> KafkaSinkConfig {
    KafkaSinkConfig {
        brokers: brokers.to_string(),
        topic: topic.to_string(),
        key_field: None,
        tombstones: false,
        flush_timeout_ms: 30_000,
        provision: TopicProvision::default(),
        properties: BTreeMap::new(),
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

/// Drain a compacted [`KafkaSource`] built against [`schema`] to its
/// permanent EOF: every surviving row, plus the row count `next_batch`
/// handed back on each call, in call order.
async fn drain_compacted(
    source: &mut KafkaSource,
) -> (Vec<(i64, Option<String>, Option<f64>)>, Vec<usize>) {
    let mut rows = Vec::new();
    let mut sizes = Vec::new();
    while let Some(out) = source.next_batch().await.expect("drain") {
        sizes.push(out.num_rows());
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
        assert_eq!(ids.null_count(), 0, "every surviving row carries its key");
        for i in 0..out.num_rows() {
            rows.push((
                ids.value(i),
                (!names.is_null(i)).then(|| names.value(i).to_string()),
                (!totals.is_null(i)).then(|| totals.value(i)),
            ));
        }
    }
    (rows, sizes)
}

/// Produce one wave of rows to `topic` and flush it, the way a producer feeds
/// a live source between two of its polls.
async fn produce_wave(brokers: &str, topic: &str, batch: &RecordBatch) {
    let mut sink =
        KafkaSink::new(sink_cfg(brokers, topic), batch.schema(), ndjson()).expect("sink builds");
    sink.write_batch(batch).await.expect("write_batch");
    sink.finish().await.expect("finish");
}

/// The `id` column of `batch`, in row order.
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
async fn json_roundtrip() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("json-roundtrip");
    let schema = schema();

    let mut sink = KafkaSink::new(sink_cfg(kafka.brokers(), &topic), schema.clone(), ndjson())
        .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut source = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, "json-roundtrip-group"),
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
async fn arrow_ipc_roundtrip() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("arrow-ipc-roundtrip");
    let schema = schema();

    let mut sink = KafkaSink::new(
        sink_cfg(kafka.brokers(), &topic),
        schema.clone(),
        arrow_ipc(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut source = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, "arrow-ipc-roundtrip-group"),
        schema.clone(),
        arrow_ipc(),
    )
    .expect("source builds");
    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema.clone());
    let rows = drain_into_dataset(&mut source, &mut dataset, COMPONENT)
        .await
        .expect("drain");
    assert_eq!(rows, 3);
    let out = dataset.batch_for(COMPONENT).expect("component present");
    assert_eq!(out.schema().fields(), schema.fields());
    assert_sample_values(out);
}

#[tokio::test]
async fn sink_creates_topic_by_default() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("sink-creates-by-default");
    let schema = schema();

    let mut sink = KafkaSink::new(
        KafkaSinkConfig {
            provision: TopicProvision {
                partitions: 3,
                ..TopicProvision::default()
            },
            ..sink_cfg(kafka.brokers(), &topic)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema))
        .await
        .expect("write_batch provisions the topic before writing");

    let partitions = common::topic_partitions(kafka.brokers(), &topic)
        .await
        .expect("topic must exist after write_batch, though AUTO_CREATE_TOPICS_ENABLE=false");
    assert_eq!(partitions, 3);
}

#[tokio::test]
async fn create_false_does_not_create() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("create-false");
    let schema = schema();

    let mut properties = BTreeMap::new();
    // Bounds the produce error's arrival regardless of librdkafka's default
    // 5-minute `message.timeout.ms`, so a wrongly-succeeding send can only
    // make this test slow, never hang.
    properties.insert("message.timeout.ms".to_string(), "5000".to_string());

    let mut sink = KafkaSink::new(
        KafkaSinkConfig {
            provision: TopicProvision {
                create: false,
                ..TopicProvision::default()
            },
            properties,
            ..sink_cfg(kafka.brokers(), &topic)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");

    let err = sink
        .write_batch(&sample_batch(schema))
        .await
        .expect_err("a topic that does not exist and cannot auto-create must error");
    assert_eq!(err.category(), "generic");

    assert!(
        common::topic_partitions(kafka.brokers(), &topic)
            .await
            .is_none(),
        "create = false must not have created the topic"
    );
}

#[tokio::test]
async fn source_creates_topic_by_default() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("source-creates-by-default");
    let schema = schema();

    let mut source = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, "source-creates-group"),
        schema,
        ndjson(),
    )
    .expect("source builds");
    let batch = source
        .next_batch()
        .await
        .expect("first next_batch on an empty, freshly-created topic must not error");
    assert!(
        batch.is_none(),
        "empty topic with stop_at_end must report EOF"
    );

    assert!(
        common::topic_partitions(kafka.brokers(), &topic)
            .await
            .is_some(),
        "the source must have created the topic before subscribing"
    );
}

#[tokio::test]
async fn topic_config_is_applied() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("topic-config-applied");
    let schema = schema();

    let mut provision_config = BTreeMap::new();
    provision_config.insert("retention.ms".to_string(), "60000".to_string());

    let mut sink = KafkaSink::new(
        KafkaSinkConfig {
            provision: TopicProvision {
                config: provision_config,
                ..TopicProvision::default()
            },
            ..sink_cfg(kafka.brokers(), &topic)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema))
        .await
        .expect("write_batch provisions the topic before writing");

    let value = common::describe_topic_config(kafka.brokers(), &topic, "retention.ms")
        .await
        .expect("retention.ms must be readable back from the broker");
    assert_eq!(value, "60000");
}

#[tokio::test]
async fn properties_reach_librdkafka() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let schema = schema();

    let mut bad_properties = BTreeMap::new();
    bad_properties.insert("this.is.not.a.property".to_string(), "x".to_string());
    let Err(err) = KafkaSource::new(
        KafkaSourceConfig {
            properties: bad_properties,
            ..source_cfg(
                kafka.brokers(),
                &kafka.topic("properties-bad"),
                "properties-group",
            )
        },
        schema.clone(),
        ndjson(),
    ) else {
        panic!("an unknown librdkafka property must be rejected at construction");
    };
    assert_eq!(err.category(), "configuration");
    assert!(err.message().contains("this.is.not.a.property"));

    let mut good_properties = BTreeMap::new();
    good_properties.insert("client.id".to_string(), "saci-test".to_string());
    KafkaSource::new(
        KafkaSourceConfig {
            properties: good_properties,
            ..source_cfg(
                kafka.brokers(),
                &kafka.topic("properties-good"),
                "properties-group",
            )
        },
        schema,
        ndjson(),
    )
    .expect("a known librdkafka property must construct successfully");
}

#[tokio::test]
async fn offsets_commit_between_drains() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("offsets-commit");
    let schema = schema();
    let group_id = "offsets-commit-g1";

    let mut sink = KafkaSink::new(sink_cfg(kafka.brokers(), &topic), schema.clone(), ndjson())
        .expect("sink builds");
    sink.write_batch(&batch_of(
        schema.clone(),
        &[1, 2, 3, 4],
        &["a", "b", "c", "d"],
        &[1.0, 2.0, 3.0, 4.0],
    ))
    .await
    .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut first_source = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, group_id),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    let mut first_dataset = Dataset::new();
    first_dataset.register_raw_component(COMPONENT, schema.clone());
    let first = drain_into_dataset(&mut first_source, &mut first_dataset, COMPONENT)
        .await
        .expect("first drain");
    assert_eq!(first, 4);
    drop(first_source);

    // `commit_on_drain` commits asynchronously (`CommitMode::Async`): give the
    // background poll thread time to actually reach the broker before a
    // second consumer in the same group checks what was committed.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut second_source = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, group_id),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    let mut second_dataset = Dataset::new();
    second_dataset.register_raw_component(COMPONENT, schema);
    let second = drain_into_dataset(&mut second_source, &mut second_dataset, COMPONENT)
        .await
        .expect("second drain");
    assert_eq!(
        second, 0,
        "a second consumer in the same group must see no rows after offsets were committed"
    );
}

#[tokio::test]
async fn a_cancelled_first_poll_hands_its_messages_to_the_next_call() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("cancel-safe-prime");
    let schema = schema();
    produce_wave(kafka.brokers(), &topic, &sample_batch(schema.clone())).await;

    let mut source = KafkaSource::new(
        KafkaSourceConfig {
            poll_timeout_ms: CANCEL_POLL_MS,
            ..source_cfg(kafka.brokers(), &topic, "cancel-safe-prime-group")
        },
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");

    // Exactly what the stream runner's source prime does: one bounded first
    // poll, with the future dropped when the bound elapses. Whatever the cold
    // consumer has taken off the broker by then is inside that dropped call.
    let mut rows = match tokio::time::timeout(PRIME_WINDOW, source.next_batch()).await {
        Ok(primed) => primed
            .expect("the primed poll must not error")
            .map_or(0, |batch| batch.num_rows()),
        Err(_elapsed) => 0,
    };

    while rows < 3 {
        let next = tokio::time::timeout(DRAINED_BOUND, source.next_batch())
            .await
            .expect(
                "a drained stop_at_end source must hand over what it holds, \
                 not idle out its poll window",
            )
            .expect("a poll after a cancelled one must not error");
        let Some(batch) = next else { break };
        rows += batch.num_rows();
    }
    assert_eq!(
        rows, 3,
        "a cancelled next_batch must not lose the messages the consumer had already fetched"
    );
}

#[tokio::test]
async fn a_cancelled_window_keeps_the_messages_already_fetched() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("cancel-safe-window");
    let schema = schema();

    // Live rather than `stop_at_end`, and `batch_size` 2: a window this
    // source cannot close until a second message arrives is a window that can
    // be cancelled while it holds the first.
    let mut source = KafkaSource::new(
        KafkaSourceConfig {
            batch_size: 2,
            poll_timeout_ms: LIVE_POLL_MS,
            stop_at_end: false,
            ..source_cfg(kafka.brokers(), &topic, "cancel-safe-window-group")
        },
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");

    // A full window first, so the consumer is subscribed and its partitions
    // assigned before the window that gets cancelled. Delivery is immediate
    // from then on, which is what makes the cancellation below certain to
    // land on a source that is holding a message.
    produce_wave(
        kafka.brokers(),
        &topic,
        &batch_of(schema.clone(), &[1, 2], &["a", "b"], &[1.0, 2.0]),
    )
    .await;
    let first = tokio::time::timeout(DRAINED_BOUND, source.next_batch())
        .await
        .expect("a window filled to batch_size must not wait on poll_timeout_ms")
        .expect("the first poll must not error")
        .expect("a live source blocks until it has rows");
    assert_eq!(ids_of(&first), vec![1, 2]);

    // Half of what `batch_size` asks for, so the window stays open holding
    // row 3 while it waits for a second row.
    produce_wave(
        kafka.brokers(),
        &topic,
        &batch_of(schema.clone(), &[3], &["c"], &[3.0]),
    )
    .await;
    assert!(
        tokio::time::timeout(Duration::from_secs(3), source.next_batch())
            .await
            .is_err(),
        "a half-filled window must still be open after 3 s, or this test cancels nothing"
    );

    // Bounded above `poll_timeout_ms`, so a source that dropped row 3 along
    // with the cancelled future is caught returning the wrong rows rather
    // than merely being slow.
    produce_wave(
        kafka.brokers(),
        &topic,
        &batch_of(schema.clone(), &[4], &["d"], &[4.0]),
    )
    .await;
    let resumed = tokio::time::timeout(Duration::from_secs(30), source.next_batch())
        .await
        .expect("the resumed poll must finish inside its own poll window")
        .expect("the resumed poll must not error")
        .expect("a live source blocks until it has rows");
    assert_eq!(
        ids_of(&resumed),
        vec![3, 4],
        "row 3 was taken off the broker by the cancelled call and must survive it"
    );
}

#[tokio::test]
async fn a_cancelled_poll_commits_nothing_it_did_not_hand_over() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("cancel-safe-commit");
    let schema = schema();
    let group_id = "cancel-safe-commit-group";
    produce_wave(kafka.brokers(), &topic, &sample_batch(schema.clone())).await;

    // Two windows in a row that cannot close on three messages, both
    // cancelled: the source takes all three off the broker, hands no batch
    // over, and still runs the commit every call makes on entry.
    let mut source = KafkaSource::new(
        KafkaSourceConfig {
            batch_size: 4,
            poll_timeout_ms: LIVE_POLL_MS,
            stop_at_end: false,
            ..source_cfg(kafka.brokers(), &topic, group_id)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    for _ in 0..2 {
        assert!(
            tokio::time::timeout(Duration::from_secs(5), source.next_batch())
                .await
                .is_err(),
            "a window short of batch_size must still be open after 5 s, \
             or this test cancels nothing"
        );
    }
    drop(source);

    // `commit_on_drain` commits asynchronously, so a commit that must not
    // exist is given the same time to reach the broker that a real one needs.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut second = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, group_id),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema);
    let rows = drain_into_dataset(&mut second, &mut dataset, COMPONENT)
        .await
        .expect("second drain");
    assert_eq!(
        rows, 3,
        "a poll that handed no batch to its caller must have acked nothing"
    );
}

#[tokio::test]
async fn a_drained_source_returns_its_rows_without_idling_out_its_poll_window() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("drained-no-idle");
    let schema = schema();
    produce_wave(kafka.brokers(), &topic, &sample_batch(schema.clone())).await;

    let mut source = KafkaSource::new(
        KafkaSourceConfig {
            poll_timeout_ms: CANCEL_POLL_MS,
            ..source_cfg(kafka.brokers(), &topic, "drained-no-idle-group")
        },
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");

    let batch = tokio::time::timeout(DRAINED_BOUND, source.next_batch())
        .await
        .expect(
            "every seeded message is on the topic and every partition reports EOF, \
             so the window must end on that and not on poll_timeout_ms",
        )
        .expect("the first poll must not error")
        .expect("the seeded rows must come back");
    assert_sample_values(&batch);
}

#[tokio::test]
async fn key_field_sets_the_message_key() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("key-field");
    let schema = schema();

    let mut sink = KafkaSink::new(
        KafkaSinkConfig {
            key_field: Some("id".to_string()),
            ..sink_cfg(kafka.brokers(), &topic)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&sample_batch(schema))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut raw_cfg = rdkafka::ClientConfig::new();
    raw_cfg.set("bootstrap.servers", kafka.brokers());
    raw_cfg.set("group.id", "key-field-check");
    raw_cfg.set("auto.offset.reset", "earliest");
    let consumer: BaseConsumer = raw_cfg.create().expect("raw consumer");
    consumer.subscribe(&[topic.as_str()]).expect("subscribe");

    let mut seen = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && seen.len() < 3 {
        match consumer.poll(Duration::from_millis(500)) {
            Some(Ok(message)) => {
                let key = message
                    .key()
                    .map(|k| String::from_utf8_lossy(k).into_owned())
                    .unwrap_or_default();
                let payload = message
                    .payload()
                    .map(|p| String::from_utf8_lossy(p).into_owned())
                    .unwrap_or_default();
                seen.push((key, payload));
            }
            Some(Err(KafkaError::PartitionEOF(_))) | None => {}
            Some(Err(e)) => panic!("poll failed: {e}"),
        }
    }
    assert_eq!(seen.len(), 3, "expected all 3 messages within the deadline");

    for (key, payload) in &seen {
        let value: serde_json::Value = serde_json::from_str(payload).expect("valid json payload");
        let id = value["id"].as_i64().expect("payload has an id field");
        assert_eq!(
            key,
            &id.to_string(),
            "message key must equal the rendered id"
        );
    }
}

#[tokio::test]
async fn compacted_topic_retains_duplicates_and_tombstone() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("compacted-raw");
    common::create_compacted_topic(kafka.brokers(), &topic, 1)
        .await
        .expect("compacted topic created");

    let cleanup_policy = common::describe_topic_config(kafka.brokers(), &topic, "cleanup.policy")
        .await
        .expect("cleanup.policy must be readable back from the broker");
    assert_eq!(cleanup_policy, "compact");

    let mut producer_cfg = rdkafka::ClientConfig::new();
    producer_cfg.set("bootstrap.servers", kafka.brokers());
    let producer: FutureProducer = producer_cfg.create().expect("raw producer");
    let send_timeout = Timeout::After(Duration::from_secs(10));

    // Compaction never runs in this test: a raw consumer over this
    // single-partition topic must see every record below in produce order,
    // duplicate key and all.
    producer
        .send(
            FutureRecord::to(&topic).key("dup-key").payload("first"),
            send_timeout,
        )
        .await
        .expect("produce dup-key/first");
    producer
        .send(
            FutureRecord::to(&topic).key("dup-key").payload("second"),
            send_timeout,
        )
        .await
        .expect("produce dup-key/second");
    producer
        .send(
            FutureRecord::to(&topic).key("solo-key").payload("solo"),
            send_timeout,
        )
        .await
        .expect("produce solo-key/solo");

    // A compacted-topic delete marker is a message with a key and a *null*
    // payload, not merely an empty one. Never calling `.payload(..)` leaves
    // `FutureRecord::payload` at its default `None`; `()` is the payload
    // type only because nothing else supplies one to infer it from.
    let tombstone: FutureRecord<'_, str, ()> = FutureRecord::to(&topic).key("tomb-key");
    producer
        .send(tombstone, send_timeout)
        .await
        .expect("produce tomb-key tombstone");

    let mut raw_cfg = rdkafka::ClientConfig::new();
    raw_cfg.set("bootstrap.servers", kafka.brokers());
    raw_cfg.set("group.id", "compacted-raw-check");
    raw_cfg.set("auto.offset.reset", "earliest");
    let consumer: BaseConsumer = raw_cfg.create().expect("raw consumer");
    consumer.subscribe(&[topic.as_str()]).expect("subscribe");

    let mut seen: Vec<(String, Option<String>)> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && seen.len() < 4 {
        match consumer.poll(Duration::from_millis(500)) {
            Some(Ok(message)) => {
                let key = message
                    .key()
                    .map(|k| String::from_utf8_lossy(k).into_owned())
                    .unwrap_or_default();
                let payload = message
                    .payload()
                    .map(|p| String::from_utf8_lossy(p).into_owned());
                seen.push((key, payload));
            }
            Some(Err(KafkaError::PartitionEOF(_))) | None => {}
            Some(Err(e)) => panic!("poll failed: {e}"),
        }
    }
    assert_eq!(
        seen.len(),
        4,
        "expected all 4 raw messages within the deadline"
    );

    // One partition, and each send above was awaited before the next was
    // issued, so produce order is the only possible log order.
    assert_eq!(
        seen,
        vec![
            ("dup-key".to_string(), Some("first".to_string())),
            ("dup-key".to_string(), Some("second".to_string())),
            ("solo-key".to_string(), Some("solo".to_string())),
            ("tomb-key".to_string(), None),
        ],
        "raw consumer must see every record, duplicates and tombstone included, \
         with the exact key/payload each was produced with"
    );
}

#[tokio::test]
async fn compacted_snapshot_deduplicates_and_applies_tombstones() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("compacted-snapshot");
    common::create_compacted_topic(kafka.brokers(), &topic, 3)
        .await
        .expect("compacted topic created");
    let schema = schema();

    // A tombstone-enabled sink writes the keyed log: three keys, then an
    // update for key 1 and a delete for key 2.
    let mut sink = KafkaSink::new(
        KafkaSinkConfig {
            key_field: Some("id".to_string()),
            tombstones: true,
            ..sink_cfg(kafka.brokers(), &topic)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");
    sink.write_batch(&nullable_batch(
        schema.clone(),
        &[1, 2, 3],
        &[Some("a"), Some("b"), Some("c")],
        &[Some(1.0), Some(2.0), Some(3.0)],
    ))
    .await
    .expect("first write");
    sink.write_batch(&nullable_batch(
        schema.clone(),
        &[1, 2],
        &[Some("a2"), None],
        &[Some(11.0), None],
    ))
    .await
    .expect("second write");
    sink.finish().await.expect("flush");

    // A raw producer adds key 4 with a payload that carries no `id` column
    // at all: a keyless record cannot be produced to a compacted topic (the
    // broker rejects it with InvalidRecord), but a payload silent about the
    // key column decodes fine, since compacted mode never looks for the key
    // there.
    let mut producer_cfg = rdkafka::ClientConfig::new();
    producer_cfg.set("bootstrap.servers", kafka.brokers());
    let producer: FutureProducer = producer_cfg.create().expect("raw producer");
    producer
        .send(
            FutureRecord::to(&topic)
                .key("4")
                .payload(br#"{"name":"d","total":4.0}"#.as_slice()),
            Timeout::After(Duration::from_secs(10)),
        )
        .await
        .expect("keyed send with no id column in the payload");

    let mut source = KafkaSource::new(
        KafkaSourceConfig {
            key_field: Some("id".to_string()),
            compacted: true,
            batch_size: 2,
            ..source_cfg(kafka.brokers(), &topic, "compacted-snapshot-group")
        },
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");

    let (mut rows, sizes) = drain_compacted(&mut source).await;
    rows.sort_by_key(|(id, _, _)| *id);
    assert_eq!(
        rows,
        vec![
            (1, Some("a2".to_string()), Some(11.0)),
            (3, Some("c".to_string()), Some(3.0)),
            (4, Some("d".to_string()), Some(4.0)),
        ],
        "key 1 keeps its newest value, key 2 was tombstoned, key 4 got its id from the wire"
    );
    assert_eq!(
        sizes,
        vec![2, 1],
        "batch_size 2 chunks three survivors into 2 then 1"
    );
    assert!(
        source.next_batch().await.expect("after eof").is_none(),
        "a compacted snapshot's EOF is permanent"
    );
}

#[tokio::test]
async fn an_empty_compacted_topic_reports_eof() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("compacted-empty");
    common::create_compacted_topic(kafka.brokers(), &topic, 2)
        .await
        .expect("compacted topic created");

    let mut source = KafkaSource::new(
        KafkaSourceConfig {
            key_field: Some("id".to_string()),
            compacted: true,
            ..source_cfg(kafka.brokers(), &topic, "compacted-empty-group")
        },
        schema(),
        ndjson(),
    )
    .expect("source builds");

    assert!(
        source.next_batch().await.expect("empty snapshot").is_none(),
        "every partition empty is a snapshot of nothing, not an error"
    );
}

#[tokio::test]
async fn compacted_snapshot_orders_survivors_by_partition_then_offset() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("compacted-multi-partition");
    common::create_compacted_topic(kafka.brokers(), &topic, 3)
        .await
        .expect("compacted topic created");

    let mut producer_cfg = rdkafka::ClientConfig::new();
    producer_cfg.set("bootstrap.servers", kafka.brokers());
    let producer: FutureProducer = producer_cfg.create().expect("raw producer");
    let send_timeout = Timeout::After(Duration::from_secs(10));

    // Kafka's key hash always routes one key to one partition, so a
    // "duplicated key within one partition" cannot be arranged by choosing
    // keys; it is pinned explicitly with `.partition(..)` here instead,
    // which also makes the expected (partition, offset) order below exact
    // rather than dependent on where the default partitioner lands a key.
    // Sequentially awaited, so produce order is log order.
    for (partition, key, name, total) in [
        (0, "10", "p0", 10.0),
        (1, "20", "p1", 20.0),
        (2, "30", "p2-old", 30.0), // superseded below
        (2, "30", "p2-new", 31.0), // same key, same partition: an update
        (2, "40", "p2b", 40.0),
    ] {
        let payload = format!(r#"{{"name":"{name}","total":{total}}}"#);
        producer
            .send(
                FutureRecord::to(&topic)
                    .partition(partition)
                    .key(key)
                    .payload(payload.as_bytes()),
                send_timeout,
            )
            .await
            .expect("produce pinned-partition record");
    }

    let mut source = KafkaSource::new(
        KafkaSourceConfig {
            key_field: Some("id".to_string()),
            compacted: true,
            batch_size: 2,
            ..source_cfg(kafka.brokers(), &topic, "compacted-multi-partition-group")
        },
        schema(),
        ndjson(),
    )
    .expect("source builds");

    let (rows, sizes) = drain_compacted(&mut source).await;
    assert_eq!(
        sizes,
        vec![2, 2],
        "batch_size 2 chunks four survivors into 2 then 2"
    );
    // Not sorted before comparing: this is the order `next_batch` actually
    // returned. Partition 0's key, then partition 1's, then partition 2's
    // two survivors in offset order: key 30's LWW winner sits at the offset
    // of its second, winning write, ahead of key 40's later offset.
    assert_eq!(
        rows,
        vec![
            (10, Some("p0".to_string()), Some(10.0)),
            (20, Some("p1".to_string()), Some(20.0)),
            (30, Some("p2-new".to_string()), Some(31.0)),
            (40, Some("p2b".to_string()), Some(40.0)),
        ],
        "one row per key, the duplicated key 30 keeping only its newest value, in \
         (partition, offset) order"
    );
}

#[tokio::test]
async fn each_compacted_snapshot_is_a_fresh_full_read() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("compacted-freshness");
    common::create_compacted_topic(kafka.brokers(), &topic, 1)
        .await
        .expect("compacted topic created");
    let schema = schema();

    let mut sink = KafkaSink::new(
        KafkaSinkConfig {
            key_field: Some("id".to_string()),
            ..sink_cfg(kafka.brokers(), &topic)
        },
        schema.clone(),
        ndjson(),
    )
    .expect("sink builds");

    // State A: two keys, snapshotted before state B exists.
    sink.write_batch(&nullable_batch(
        schema.clone(),
        &[1, 2],
        &[Some("a"), Some("b")],
        &[Some(1.0), Some(2.0)],
    ))
    .await
    .expect("write state A");
    sink.finish().await.expect("flush state A");

    let mut first_source = KafkaSource::new(
        KafkaSourceConfig {
            key_field: Some("id".to_string()),
            compacted: true,
            ..source_cfg(kafka.brokers(), &topic, "compacted-freshness-first")
        },
        schema.clone(),
        ndjson(),
    )
    .expect("first source builds");
    let (first_rows, _) = drain_compacted(&mut first_source).await;
    let mut first_ids: Vec<i64> = first_rows.into_iter().map(|(id, _, _)| id).collect();
    first_ids.sort_unstable();
    assert_eq!(
        first_ids,
        vec![1, 2],
        "the first snapshot's cut is captured before state B is produced, so it must not see \
         key 3"
    );

    // State B: a new key, produced only after the first snapshot finished.
    sink.write_batch(&nullable_batch(
        schema.clone(),
        &[3],
        &[Some("c")],
        &[Some(3.0)],
    ))
    .await
    .expect("write state B");
    sink.finish().await.expect("flush state B");

    let mut second_source = KafkaSource::new(
        KafkaSourceConfig {
            key_field: Some("id".to_string()),
            compacted: true,
            ..source_cfg(kafka.brokers(), &topic, "compacted-freshness-second")
        },
        schema.clone(),
        ndjson(),
    )
    .expect("second source builds");
    let (second_rows, _) = drain_compacted(&mut second_source).await;
    let mut second_ids: Vec<i64> = second_rows.into_iter().map(|(id, _, _)| id).collect();
    second_ids.sort_unstable();
    assert_eq!(
        second_ids,
        vec![1, 2, 3],
        "a brand new compacted source re-reads the whole topic: state A's keys plus state B's, \
         not just what changed"
    );
}

/// `Source::request_batch_rows` caps the very next collection window rather
/// than being a starting size a poll timeout eventually outgrows: with more
/// messages waiting than the hint, `next_batch` returns once the hint is met
/// instead of collecting everything the topic already holds. A later, larger
/// hint then collects more in one window.
#[tokio::test]
async fn request_batch_rows_caps_the_next_collection_window() {
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("request-batch-rows");
    let schema = schema();

    let mut sink = KafkaSink::new(sink_cfg(kafka.brokers(), &topic), schema.clone(), ndjson())
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

    let mut source = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, "request-batch-rows-g1"),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");

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
    let Some(kafka) = common::try_start().await else {
        return;
    };
    let topic = kafka.topic("request-batch-rows-zero");
    let schema = schema();

    let mut sink = KafkaSink::new(sink_cfg(kafka.brokers(), &topic), schema.clone(), ndjson())
        .expect("sink builds");
    sink.write_batch(&sample_batch(schema.clone()))
        .await
        .expect("write_batch");
    sink.finish().await.expect("finish");

    let mut source = KafkaSource::new(
        source_cfg(kafka.brokers(), &topic, "request-batch-rows-zero-g1"),
        schema.clone(),
        ndjson(),
    )
    .expect("source builds");
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
