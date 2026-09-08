//! Testcontainers harness for a real single-node Kafka broker.
//!
//! Every test opens with
//!
//! ```rust,ignore
//! let Some(kafka) = common::try_start().await else { return; };
//! ```
//!
//! so the suite soft-skips when no Docker daemon is reachable.
//!
//! Auto topic creation is disabled on the broker, so a test that asserts a
//! topic exists is asserting that this connector created it.

#![allow(dead_code)]

use std::net::TcpListener;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::RDKafkaErrorCode;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// A running single-node KRaft Kafka broker.
pub struct KafkaContainer {
    /// Held so the container lives as long as the test.
    _container: ContainerAsync<GenericImage>,
    brokers: String,
}

impl KafkaContainer {
    /// The `bootstrap.servers` value for this broker.
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    /// A topic name unique to this test run.
    pub fn topic(&self, stem: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_nanos();
        format!("{stem}-{nanos}")
    }
}

/// Start a Kafka broker, or return `None` with a printed reason.
pub async fn try_start() -> Option<KafkaContainer> {
    match start().await {
        Ok(container) => Some(container),
        Err(e) => {
            eprintln!("SKIP: kafka container unavailable: {e}");
            None
        }
    }
}

async fn start() -> anyhow::Result<KafkaContainer> {
    // A Kafka broker bakes its advertised listener into config at boot, but
    // the host port is only known after the container starts. Break the
    // cycle by picking the host port ourselves first.
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);

    let image = GenericImage::new("apache/kafka", "3.9.0")
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://127.0.0.1:{port}"),
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_mapped_port(port, 9092.tcp());

    let container = image.start().await?;
    let brokers = format!("127.0.0.1:{port}");

    // The startup log line can precede the broker actually accepting
    // connections, so poll metadata until it succeeds or a deadline passes,
    // mirroring the Postgres harness's connect loop.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last_err = None;
    while Instant::now() < deadline {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &brokers);
        match cfg.create::<BaseConsumer>() {
            Ok(consumer) => match consumer.fetch_metadata(None, Duration::from_secs(5)) {
                Ok(_) => {
                    return Ok(KafkaContainer {
                        _container: container,
                        brokers,
                    });
                }
                Err(e) => last_err = Some(e.to_string()),
            },
            Err(e) => last_err = Some(e.to_string()),
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(anyhow::anyhow!(
        "broker never accepted a connection: {}",
        last_err.unwrap_or_default()
    ))
}

/// Partition count for `topic` on `brokers`, or `None` if it does not exist.
pub async fn topic_partitions(brokers: &str, topic: &str) -> Option<usize> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers);
    let consumer: BaseConsumer = cfg.create().ok()?;
    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .ok()?;
    let entry = metadata.topics().iter().find(|t| t.name() == topic)?;
    if entry.error().is_some() {
        return None;
    }
    Some(entry.partitions().len())
}

/// One broker-side topic config value, or `None` if the topic or key does
/// not exist.
pub async fn describe_topic_config(brokers: &str, topic: &str, key: &str) -> Option<String> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers);
    let admin: AdminClient<DefaultClientContext> = cfg.create().ok()?;
    let results = admin
        .describe_configs(&[ResourceSpecifier::Topic(topic)], &AdminOptions::new())
        .await
        .ok()?;
    let resource = results.into_iter().next()?.ok()?;
    resource.entry_map().get(key)?.value.clone()
}

/// Create `topic` on `brokers` with `partitions` partitions and log
/// compaction enabled (`cleanup.policy=compact`), treating an existing
/// topic as success.
///
/// Mirrors `saci_connector_kafka::admin::ensure_topics`'s idempotent create,
/// which integration tests cannot call directly because it is `pub(crate)`.
pub async fn create_compacted_topic(
    brokers: &str,
    topic: &str,
    partitions: i32,
) -> anyhow::Result<()> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", brokers);
    let admin: AdminClient<DefaultClientContext> = cfg.create()?;

    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1))
        .set("cleanup.policy", "compact");
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_millis(10_000)));

    let results = admin.create_topics(&[new_topic], &opts).await?;
    for result in results {
        match result {
            Ok(_) => {}
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => {
                anyhow::bail!("cannot create compacted topic '{name}': {code}");
            }
        }
    }
    Ok(())
}
