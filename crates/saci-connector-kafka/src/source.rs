//! [`KafkaSource`]: a Kafka topic consumer [`Source`].
//!
//! Live by default: with `stop_at_end = false`, [`next_batch`](Source::next_batch)
//! blocks on the first message and never returns `Ok(None)`, exactly like a
//! live TCP ingestion source idling for a connection. Setting
//! `stop_at_end = true` makes it usable from the batch run modes: once every
//! assigned partition has reported end-of-partition, it reports EOF.
//!
//! [`next_batch`](Source::next_batch) is cancel-safe: the messages the
//! consumer has taken from the broker live on the source, not in the future,
//! so dropping that future — as `run_stream`'s one-second source prime does —
//! hands them to the next call instead of losing them.
//!
//! `compacted = true` reads the topic as the keyed log a compacted topic is:
//! one bounded pass over every partition, from its log start to the high
//! watermark captured when the pass began, reduced to the newest value per
//! key, then EOF. Committed group offsets play no part, because a snapshot is
//! always the whole state.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, BinaryArray, LargeBinaryArray, RecordBatch, StringArray};
use arrow_cast::display::FormatOptions;
use arrow_cast::{CastOptions, can_cast_types, cast_with_options};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use tokio::time::{Instant, timeout_at};

use saci_core::error::SaciError;
use saci_core::io::source::Source;
use saci_transformer::{MessageShape, Transformer};

use crate::admin::ensure_topics;
use crate::config::{KafkaSourceConfig, client_config};

/// One message pulled off the consumer, with its payload copied out of the
/// borrowed librdkafka buffer so it can outlive the `recv()` call.
struct ReceivedMessage {
    topic: String,
    partition: i32,
    offset: i64,
    /// `None` for a tombstone (null payload), which is skipped rather than
    /// decoded.
    payload: Option<Vec<u8>>,
}

/// One surviving record of a compacted snapshot: the newest value seen for
/// its key, and the log coordinates it was read at.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CompactedEntry {
    /// Position of the record's topic in [`KafkaSource::topics`].
    topic: usize,
    partition: i32,
    offset: i64,
    key: Vec<u8>,
    value: Vec<u8>,
}

/// What [`CompactedState`] keeps for a key that is still alive.
#[derive(Debug)]
struct Latest {
    partition: i32,
    offset: i64,
    value: Vec<u8>,
}

/// The compaction rule itself: reduce a keyed log to the newest value per
/// key, with tombstones deleting.
///
/// Pure — it names no Kafka type — so every rule it encodes is testable
/// without a broker. Keys and values are raw bytes here, before any decoding,
/// because a tombstone carries no payload to decode.
#[derive(Debug)]
struct CompactedState {
    /// One map per topic, indexed by the topic's position in the source's
    /// topic list: a key identifies a record within one topic, so two topics
    /// that happen to share a key hold two independent records. Within a
    /// topic no per-partition split is needed, because Kafka's own key hash
    /// already puts every record for a key in one partition.
    per_topic: Vec<HashMap<Vec<u8>, Latest>>,
}

impl CompactedState {
    /// An empty state covering `topics` topics.
    fn new(topics: usize) -> Self {
        Self {
            per_topic: (0..topics).map(|_| HashMap::new()).collect(),
        }
    }

    /// Fold one record in.
    ///
    /// `value` is `None` for a tombstone, which deletes the key; a tombstone
    /// for a key the state does not hold is ignored. A record applies only
    /// when its offset is at least the offset already recorded for its key,
    /// so the outcome does not depend on the order records arrive in.
    fn apply(
        &mut self,
        topic: usize,
        partition: i32,
        offset: i64,
        key: &[u8],
        value: Option<Vec<u8>>,
    ) {
        let Some(alive) = self.per_topic.get_mut(topic) else {
            return;
        };
        match value {
            Some(value) => match alive.get_mut(key) {
                Some(current) if offset >= current.offset => {
                    current.partition = partition;
                    current.offset = offset;
                    current.value = value;
                }
                // Superseded by a record already applied.
                Some(_) => {}
                // Only a record that survives is worth a copy of its key.
                None => {
                    alive.insert(
                        key.to_vec(),
                        Latest {
                            partition,
                            offset,
                            value,
                        },
                    );
                }
            },
            None => {
                if alive
                    .get(key)
                    .is_some_and(|current| offset >= current.offset)
                {
                    alive.remove(key);
                }
            }
        }
    }

    /// The surviving records in log order: by topic, then partition, then
    /// offset.
    fn into_survivors(self) -> Vec<CompactedEntry> {
        let mut survivors: Vec<CompactedEntry> = self
            .per_topic
            .into_iter()
            .enumerate()
            .flat_map(|(topic, alive)| {
                alive.into_iter().map(move |(key, current)| CompactedEntry {
                    topic,
                    partition: current.partition,
                    offset: current.offset,
                    key,
                    value: current.value,
                })
            })
            .collect();
        // (topic, partition, offset) is unique, so an unstable sort is total.
        survivors.sort_unstable_by_key(|entry| (entry.topic, entry.partition, entry.offset));
        survivors
    }
}

/// One partition's point-in-time cut: read from `low`, the log start that
/// compaction has already moved past deleted history, up to but not including
/// `high`, the high watermark captured before the read began. Records
/// appended after that capture are outside the snapshot.
#[derive(Debug)]
struct PartitionCut {
    /// Position of the partition's topic in [`KafkaSource::topics`].
    topic: usize,
    partition: i32,
    low: i64,
    high: i64,
}

/// Live Kafka [`Source`]: one `RecordBatch` per collected window of messages.
///
/// The listener/consumer connects lazily: [`new`](Self::new) only builds and
/// validates the `StreamConsumer`, so `saci-service validate` stays
/// broker-free. The first [`next_batch`](Source::next_batch) call provisions
/// the topic (unless opted out) and subscribes.
pub struct KafkaSource {
    consumer: StreamConsumer,
    /// Kept so the admin client used for topic provisioning shares the same
    /// bootstrap servers and properties as the consumer.
    client_config: ClientConfig,
    topics: Vec<String>,
    schema: Arc<Schema>,
    /// The payload codec. A decoder is opened per window rather than held for
    /// the source's life: `Source` is `Send + Sync`, and a decoder need only be
    /// `Send`, which `arrow_json`'s is and no more. Opening one costs an
    /// allocation per schema, not per row.
    transformer: Arc<dyn Transformer>,
    /// Effective fetch size for the next collection window or
    /// compacted-snapshot chunk. `batch_size` from config seeds this, but a
    /// host that sends [`Source::request_batch_rows`] overwrites it before the
    /// first poll, not after some initial one: under `saci-service`'s flow
    /// control the configured value never reaches the broker. It stays
    /// authoritative when flow control is disabled for this source, and when
    /// the source feeds a windowed node, which `saci-service` never hints.
    cfg: KafkaSourceConfig,
    /// Set once the topic has been provisioned and `subscribe` has been
    /// called, so both happen exactly once.
    started: bool,
    /// Messages taken from the broker and not yet handed to the caller. They
    /// live here rather than in [`next_batch`](Source::next_batch)'s future
    /// so dropping that future loses nothing: the broker's fetch position has
    /// already moved past them, and librdkafka re-delivers neither the
    /// messages nor a second `PartitionEOF` for a position it has already
    /// reported. Always empty while `pending_commit` is set, which is what
    /// keeps a commit from acking a message the caller has not seen.
    buffer: Vec<ReceivedMessage>,
    /// Set once a batch has been handed to the caller and cleared once its
    /// offsets are committed at the start of the next call. This is what
    /// makes delivery at-least-once: a crash between the two replays the
    /// batch.
    pending_commit: bool,
    /// Partition indices observed as end-of-partition when `stop_at_end` is
    /// set. `enable.partition.eof` events from librdkafka carry only the
    /// partition index, not the topic name, so this cannot disambiguate two
    /// simultaneously subscribed topics that reuse the same partition index.
    /// That is a fast-path optimisation only: [`next_batch`](Source::next_batch)
    /// still falls back to its poll-window deadline to decide EOF correctly,
    /// so an index collision costs a slower drain, never a wrong result.
    eof: HashSet<i32>,
    /// The schema payloads are decoded into. Compacted mode makes the key
    /// column nullable there, because the key travels beside the payload
    /// rather than inside it and is written over that column after the
    /// decode; every other mode decodes straight into `schema`.
    decode_schema: Arc<Schema>,
    /// Position of `key_field` in `schema`, resolved once at construction.
    /// `None` outside compacted mode, the one mode that reads message keys.
    key_index: Option<usize>,
    /// Compacted mode's one-shot snapshot: the surviving records in log
    /// order, how many of them have been handed out, and whether the bounded
    /// read has run. Nothing is emitted until the read finishes, so a record
    /// read late can still supersede one read early. All three stay at their
    /// empty defaults in every other mode.
    snapshot: Vec<CompactedEntry>,
    snapshot_cursor: usize,
    snapshot_taken: bool,
}

impl KafkaSource {
    /// Validate the config and create the consumer. Opens no connection:
    /// librdkafka connects lazily in the background on first use.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `cfg` fails validation,
    /// `transformer` has no message codec, `compacted` is set against a
    /// format that emits one message per batch or a `key_field` the declared
    /// schema cannot hold a raw key in, or librdkafka rejects a property in
    /// `cfg.properties`.
    pub fn new(
        cfg: KafkaSourceConfig,
        schema: Arc<Schema>,
        transformer: Arc<dyn Transformer>,
    ) -> Result<Self, SaciError> {
        cfg.validate()?;
        if transformer.message_shape().is_none() {
            return Err(SaciError::configuration(format!(
                "KafkaSource: format '{}' has no message codec",
                transformer.format()
            )));
        }
        let topics = cfg.topics();

        let (key_index, decode_schema) = if cfg.compacted {
            let Some(key_field) = cfg.key_field.as_deref() else {
                return Err(SaciError::configuration(
                    "KafkaSource config: 'compacted' needs 'key_field'",
                ));
            };
            let (index, decode_schema) =
                compacted_key_column(key_field, &schema, transformer.as_ref())?;
            (Some(index), decode_schema)
        } else {
            (None, Arc::clone(&schema))
        };

        let eof_default = if cfg.stop_at_end { "true" } else { "false" };
        let defaults: [(&str, &str); 4] = [
            ("group.id", cfg.group_id.as_str()),
            ("auto.offset.reset", cfg.auto_offset_reset.as_str()),
            ("enable.auto.commit", "false"),
            ("enable.partition.eof", eof_default),
        ];
        let client_config = client_config(&cfg.brokers, &defaults, &cfg.properties);

        let consumer: StreamConsumer = client_config.create().map_err(|e| {
            SaciError::configuration(format!("KafkaSource: cannot create consumer: {e}"))
        })?;

        Ok(Self {
            consumer,
            client_config,
            topics,
            schema,
            transformer,
            cfg,
            started: false,
            buffer: Vec::new(),
            pending_commit: false,
            eof: HashSet::new(),
            decode_schema,
            key_index,
            snapshot: Vec::new(),
            snapshot_cursor: 0,
            snapshot_taken: false,
        })
    }

    async fn ensure_started(&mut self) -> Result<(), SaciError> {
        if self.started {
            return Ok(());
        }
        // Provision before subscribe: librdkafka's default
        // `topic.metadata.refresh.interval.ms` is 300000, so a consumer that
        // subscribes to a not-yet-existing topic can sit blind for 5 minutes.
        ensure_topics(&self.client_config, &self.topics, &self.cfg.provision).await?;
        if self.cfg.provision.create {
            // `ensure_topics` just created these through a separate
            // `AdminClient` connection; this consumer's own metadata can lag
            // behind that by a moment. `subscribe` resolves partitions once
            // and does not retry a topic it did not find, so it must not run
            // until this consumer's own metadata view has caught up.
            self.await_topic_metadata().await?;
        }
        let refs: Vec<&str> = self.topics.iter().map(String::as_str).collect();
        self.consumer
            .subscribe(&refs)
            .map_err(|e| SaciError::generic(format!("KafkaSource: subscribe failed: {e}")))?;
        self.started = true;
        Ok(())
    }

    /// Poll metadata until every topic in `self.topics` is visible to this
    /// consumer's own connection, or `poll_timeout_ms` elapses.
    async fn await_topic_metadata(&self) -> Result<(), SaciError> {
        let deadline = Instant::now() + Duration::from_millis(self.cfg.poll_timeout_ms);
        loop {
            let all_visible = self.topics.iter().all(|topic| {
                self.consumer
                    .fetch_metadata(Some(topic), BROKER_QUERY_TIMEOUT)
                    .is_ok_and(|metadata| {
                        metadata.topics().iter().any(|t| {
                            t.name() == topic && t.error().is_none() && !t.partitions().is_empty()
                        })
                    })
            });
            if all_visible {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(self.unknown_topic_error());
            }
            tokio::time::sleep(UNKNOWN_TOPIC_RETRY_BACKOFF).await;
        }
    }

    /// Commit the previous batch's offsets, if one is pending. Committing
    /// here (at the start of the *next* poll) rather than when the batch was
    /// produced is what makes delivery at-least-once.
    fn commit_pending(&mut self) -> Result<(), SaciError> {
        if self.pending_commit && self.cfg.commit_on_drain {
            self.consumer
                .commit_consumer_state(CommitMode::Async)
                .map_err(|e| SaciError::generic(format!("KafkaSource: commit failed: {e}")))?;
        }
        self.pending_commit = false;
        Ok(())
    }

    /// `true` once every currently assigned partition index has reported EOF.
    /// `false` while the assignment has not settled yet (right after
    /// `subscribe`, before the group rebalance completes).
    fn fully_drained(&self) -> Result<bool, SaciError> {
        let assignment = self
            .consumer
            .assignment()
            .map_err(|e| SaciError::generic(format!("KafkaSource: cannot read assignment: {e}")))?;
        if assignment.count() == 0 {
            return Ok(false);
        }
        Ok(assignment
            .elements()
            .iter()
            .all(|elem| self.eof.contains(&elem.partition())))
    }

    /// Copy a borrowed message's fields out so it can be buffered across
    /// `recv()` calls.
    fn take_message(message: &rdkafka::message::BorrowedMessage<'_>) -> ReceivedMessage {
        ReceivedMessage {
            topic: message.topic().to_string(),
            partition: message.partition(),
            offset: message.offset(),
            payload: message.payload().map(<[u8]>::to_vec),
        }
    }

    /// Collect into [`self.buffer`](Self::buffer) for a live
    /// (non-`stop_at_end`) source: the first message blocks indefinitely,
    /// exactly like `TcpIngestSource` idling for a connection; the rest are
    /// bounded by one `poll_timeout_ms` window. Messages a cancelled call
    /// left behind already satisfy the blocking phase.
    async fn collect_live(&mut self) -> Result<(), SaciError> {
        let unknown_topic_deadline =
            Instant::now() + Duration::from_millis(self.cfg.poll_timeout_ms);
        while self.buffer.is_empty() {
            match self.consumer.recv().await {
                Ok(message) => self.buffer.push(Self::take_message(&message)),
                // Meaningless for a live source: more data may still arrive.
                Err(KafkaError::PartitionEOF(_)) => continue,
                Err(e) if is_unknown_topic(&e) => {
                    if Instant::now() >= unknown_topic_deadline {
                        return Err(self.unknown_topic_error());
                    }
                    tokio::time::sleep(UNKNOWN_TOPIC_RETRY_BACKOFF).await;
                }
                Err(e) => return Err(SaciError::generic(format!("KafkaSource: poll failed: {e}"))),
            }
        }

        let deadline = Instant::now() + Duration::from_millis(self.cfg.poll_timeout_ms);
        while self.buffer.len() < self.cfg.batch_size {
            match timeout_at(deadline, self.consumer.recv()).await {
                Ok(Ok(message)) => self.buffer.push(Self::take_message(&message)),
                Ok(Err(KafkaError::PartitionEOF(_))) => {}
                Ok(Err(e)) if is_unknown_topic(&e) => {
                    tokio::time::sleep(UNKNOWN_TOPIC_RETRY_BACKOFF).await;
                }
                Ok(Err(e)) => {
                    return Err(SaciError::generic(format!("KafkaSource: poll failed: {e}")));
                }
                Err(_elapsed) => break,
            }
        }
        Ok(())
    }

    /// Collect into [`self.buffer`](Self::buffer) for a `stop_at_end` source:
    /// every receive is bounded by one `poll_timeout_ms` window, and
    /// `PartitionEOF` is bookkeeping, not an error. Leaves the buffer empty
    /// when the topic is fully drained and no cancelled call left anything
    /// behind; returns an error, not a silent empty buffer, when the window
    /// elapses while the topic's metadata never resolved (e.g.
    /// `provision.create = false` against a topic that does not exist).
    ///
    /// The fully-drained break does not wait for an empty buffer: a source
    /// that has read everything hands over what it holds at once rather than
    /// idling out the rest of its window for messages that cannot come.
    async fn collect_bounded(&mut self) -> Result<(), SaciError> {
        // What a cancelled call left behind, after every partition had
        // already reported EOF to it. librdkafka reports one EOF per fetch
        // position, so polling for a second one would idle out the whole
        // window before handing over messages already in hand.
        if !self.buffer.is_empty() && self.fully_drained()? {
            return Ok(());
        }
        let mut saw_unknown_topic = false;
        let deadline = Instant::now() + Duration::from_millis(self.cfg.poll_timeout_ms);
        while self.buffer.len() < self.cfg.batch_size {
            match timeout_at(deadline, self.consumer.recv()).await {
                Ok(Ok(message)) => self.buffer.push(Self::take_message(&message)),
                Ok(Err(KafkaError::PartitionEOF(partition))) => {
                    self.eof.insert(partition);
                    if self.fully_drained()? {
                        break;
                    }
                }
                Ok(Err(e)) if is_unknown_topic(&e) => {
                    saw_unknown_topic = true;
                    tokio::time::sleep(UNKNOWN_TOPIC_RETRY_BACKOFF).await;
                }
                Ok(Err(e)) => {
                    return Err(SaciError::generic(format!("KafkaSource: poll failed: {e}")));
                }
                Err(_elapsed) => break,
            }
        }
        if self.buffer.is_empty() && saw_unknown_topic {
            return Err(self.unknown_topic_error());
        }
        Ok(())
    }

    /// Feed a whole window through one decoder and take the batch out.
    ///
    /// Each push names the message it failed on, keeping per-message
    /// attribution; the transformer names the format that rejected it.
    fn decode_window(&self, messages: &[ReceivedMessage]) -> Result<RecordBatch, SaciError> {
        let mut decoder = self
            .transformer
            .open_message_decoder(Arc::clone(&self.schema))?;
        for message in messages {
            let Some(payload) = &message.payload else {
                continue; // tombstone, already excluded from the buffer
            };
            decoder.push(payload).map_err(|e| {
                SaciError::generic(format!(
                    "KafkaSource: decode failed for {}[{}]@{}: {e}",
                    message.topic, message.partition, message.offset
                ))
            })?;
        }
        decoder
            .flush()?
            .ok_or_else(|| SaciError::generic("KafkaSource: window decoded no rows"))
    }

    /// Hand out the next `batch_size` chunk of the compacted snapshot,
    /// reading the whole snapshot on the first call.
    ///
    /// `Ok(None)` once every survivor has been emitted, and on every call
    /// after: a snapshot is one point-in-time cut of the log, so its EOF is
    /// final rather than "nothing right now".
    async fn next_snapshot_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        if !self.snapshot_taken {
            self.snapshot = self.read_snapshot().await?;
            self.snapshot_taken = true;
        }

        let start = self.snapshot_cursor;
        let end = self.snapshot.len().min(start + self.cfg.batch_size);
        if start >= end {
            // Release the state as soon as the last chunk is out; every later
            // call takes this same branch and reports EOF again.
            self.snapshot = Vec::new();
            self.snapshot_cursor = 0;
            return Ok(None);
        }

        let batch = self.decode_snapshot_chunk(&self.snapshot[start..end])?;
        self.snapshot_cursor = end;
        Ok(Some(batch))
    }

    /// Read every partition from its log start to the high watermark captured
    /// before the read began, and reduce it to the newest value per key.
    ///
    /// Partitions are assigned by hand at their low watermark rather than
    /// subscribed: a snapshot is the whole state of the topic, so a committed
    /// group offset must not shorten it. Nothing is emitted while this runs,
    /// because a record read late supersedes one read early and a consumer
    /// that had already emitted the early one would have published a value
    /// the log no longer holds.
    async fn read_snapshot(&self) -> Result<Vec<CompactedEntry>, SaciError> {
        // Provision before assign, for the same reason `ensure_started` does
        // it before subscribe.
        ensure_topics(&self.client_config, &self.topics, &self.cfg.provision).await?;
        if self.cfg.provision.create {
            self.await_topic_metadata().await?;
        }

        let cuts = self.partition_cuts()?;
        if cuts.is_empty() {
            // Every partition is empty (low == high). A snapshot of nothing.
            return Ok(Vec::new());
        }

        let mut assignment = TopicPartitionList::with_capacity(cuts.len());
        for cut in &cuts {
            let topic = self.topics[cut.topic].as_str();
            assignment
                .add_partition_offset(topic, cut.partition, Offset::Offset(cut.low))
                .map_err(|e| {
                    SaciError::generic(format!(
                        "KafkaSource: cannot assign {topic}[{}] at offset {}: {e}",
                        cut.partition, cut.low
                    ))
                })?;
        }
        self.consumer.assign(&assignment).map_err(|e| {
            SaciError::generic(format!("KafkaSource: snapshot assignment failed: {e}"))
        })?;

        let window = Duration::from_millis(self.cfg.poll_timeout_ms);
        let mut state = CompactedState::new(self.topics.len());
        let mut reached = vec![false; cuts.len()];
        let mut pending = cuts.len();
        // Idle rather than absolute: one window with nothing on it ends the
        // read, but a topic that keeps delivering is read to its cut however
        // long that takes. An absolute budget would silently truncate a large
        // snapshot into a wrong answer.
        let mut deadline = Instant::now() + window;
        while pending > 0 {
            match timeout_at(deadline, self.consumer.recv()).await {
                Ok(Ok(message)) => {
                    deadline = Instant::now() + window;
                    let topic = message.topic();
                    let partition = message.partition();
                    let Some(index) = cuts.iter().position(|cut| {
                        cut.partition == partition && self.topics[cut.topic] == topic
                    }) else {
                        continue;
                    };
                    if reached[index] {
                        continue;
                    }
                    let cut = &cuts[index];
                    let offset = message.offset();
                    // At or past the captured watermark is outside the cut:
                    // it was appended after the snapshot began. A record with
                    // no key carries no keyed state either — a compacted
                    // topic refuses one, and compaction could never remove
                    // it — so both are read past rather than folded in.
                    if offset < cut.high
                        && let Some(key) = message.key()
                    {
                        state.apply(
                            cut.topic,
                            partition,
                            offset,
                            key,
                            message.payload().map(<[u8]>::to_vec),
                        );
                    }
                    if offset + 1 >= cut.high {
                        reached[index] = true;
                        pending -= 1;
                    }
                }
                // Bookkeeping for a live source; here the captured watermark
                // is what ends a partition, so this says nothing new.
                Ok(Err(KafkaError::PartitionEOF(_))) => {}
                Ok(Err(e)) if is_unknown_topic(&e) => {
                    tokio::time::sleep(UNKNOWN_TOPIC_RETRY_BACKOFF).await;
                }
                Ok(Err(e)) => {
                    return Err(SaciError::generic(format!("KafkaSource: poll failed: {e}")));
                }
                // A whole window with nothing on it while partitions are
                // still short of their cut: what is left are offsets no
                // consumer is delivered — gaps compaction left behind, or
                // transaction control records — so the state is as complete
                // as the log can make it.
                Err(_elapsed) => break,
            }
        }

        Ok(state.into_survivors())
    }

    /// The cut this snapshot reads: every partition of every topic, from its
    /// low watermark to its high watermark. An empty partition (low == high)
    /// contributes nothing and is not an error; a topic with no partitions at
    /// all is the unknown-topic case.
    fn partition_cuts(&self) -> Result<Vec<PartitionCut>, SaciError> {
        let mut cuts = Vec::new();
        for (index, topic) in self.topics.iter().enumerate() {
            let metadata = self
                .consumer
                .fetch_metadata(Some(topic), BROKER_QUERY_TIMEOUT)
                .map_err(|e| {
                    SaciError::generic(format!(
                        "KafkaSource: cannot read metadata for '{topic}': {e}"
                    ))
                })?;
            let partitions = metadata
                .topics()
                .iter()
                .find(|entry| entry.name() == topic && entry.error().is_none())
                .map(|entry| entry.partitions())
                .filter(|partitions| !partitions.is_empty())
                .ok_or_else(|| self.unknown_topic_error())?;
            for partition in partitions {
                let (low, high) = self
                    .consumer
                    .fetch_watermarks(topic, partition.id(), BROKER_QUERY_TIMEOUT)
                    .map_err(|e| {
                        SaciError::generic(format!(
                            "KafkaSource: cannot read watermarks for {topic}[{}]: {e}",
                            partition.id()
                        ))
                    })?;
                if low < high {
                    cuts.push(PartitionCut {
                        topic: index,
                        partition: partition.id(),
                        low,
                        high,
                    });
                }
            }
        }
        Ok(cuts)
    }

    /// Decode one chunk of survivors and attach their keys.
    ///
    /// The transformer sees the values alone, exactly as it does for a live
    /// window, and the raw keys are written over the key column afterwards:
    /// the key is not in the payload, which is why `decode_schema` makes that
    /// column nullable and why the format has to emit one message per row.
    fn decode_snapshot_chunk(&self, entries: &[CompactedEntry]) -> Result<RecordBatch, SaciError> {
        let Some(key_index) = self.key_index else {
            return Err(SaciError::generic(
                "KafkaSource: a compacted snapshot has no key column",
            ));
        };
        let mut decoder = self
            .transformer
            .open_message_decoder(Arc::clone(&self.decode_schema))?;
        for entry in entries {
            decoder.push(&entry.value).map_err(|e| {
                SaciError::generic(format!(
                    "KafkaSource: decode failed for {}[{}]@{}: {e}",
                    self.topics[entry.topic], entry.partition, entry.offset
                ))
            })?;
        }
        let decoded = decoder
            .flush()?
            .ok_or_else(|| SaciError::generic("KafkaSource: snapshot chunk decoded no rows"))?;
        if decoded.num_rows() != entries.len() {
            return Err(SaciError::generic(format!(
                "KafkaSource: {} compacted messages decoded into {} rows; 'compacted' needs one \
                 row per message to carry its key",
                entries.len(),
                decoded.num_rows()
            )));
        }

        let mut columns = decoded.columns().to_vec();
        columns[key_index] = key_column(entries, self.schema.field(key_index), &self.topics)?;
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(|e| {
            SaciError::generic(format!(
                "KafkaSource: cannot attach the message key column: {e}"
            ))
        })
    }

    /// A freshly created topic's metadata can take a moment to reach this
    /// consumer (`ensure_topics` created it moments ago through the admin
    /// client, a separate connection), and a topic that will never exist
    /// under `provision.create = false` looks identical until this window
    /// elapses. Named so the two cases are distinguishable in logs, not so
    /// SACI itself distinguishes them.
    fn unknown_topic_error(&self) -> SaciError {
        SaciError::generic(format!(
            "KafkaSource: poll failed: topic(s) {:?} were never visible to the consumer; if \
             provision.create = false, they may not exist",
            self.topics
        ))
    }
}

/// A freshly created topic briefly returns `UnknownTopicOrPartition` to a
/// consumer whose own metadata has not caught up yet, even though the admin
/// client that created it already got a success response.
const UNKNOWN_TOPIC_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// How long one metadata or watermark question may wait on the broker.
const BROKER_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

/// A key that does not parse into the declared column type is an error, not a
/// silent null: the key is the identity of a compacted record, so a null one
/// would merge unrelated rows downstream.
const KEY_CAST: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: FormatOptions::new(),
};

fn is_unknown_topic(err: &KafkaError) -> bool {
    matches!(
        err,
        KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition)
    )
}

/// Resolve compacted mode's key column: where `key_field` sits in the
/// declared schema, and the schema payloads are decoded into.
///
/// That decode schema is the declared one with the key column made nullable.
/// The key travels beside the payload rather than inside it, so the decoder
/// never sees a value for that column and would refuse to build a
/// non-nullable one; the raw keys are written over it after the decode.
fn compacted_key_column(
    key_field: &str,
    schema: &Arc<Schema>,
    transformer: &dyn Transformer,
) -> Result<(usize, Arc<Schema>), SaciError> {
    if transformer.message_shape() != Some(MessageShape::PerRow) {
        return Err(SaciError::configuration(format!(
            "KafkaSource config: 'compacted' needs a row-per-message format; '{}' emits one \
             message per batch, which carries no per-row key",
            transformer.format()
        )));
    }
    let Ok(index) = schema.index_of(key_field) else {
        return Err(SaciError::configuration(format!(
            "KafkaSource config: 'key_field' names '{key_field}', which is not one of the \
             declared schema_fields {:?}",
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>()
        )));
    };
    let data_type = schema.field(index).data_type();
    if !matches!(data_type, DataType::Binary | DataType::LargeBinary)
        && !can_cast_types(&DataType::Utf8, data_type)
    {
        return Err(SaciError::configuration(format!(
            "KafkaSource config: 'key_field' column '{key_field}' is {data_type}, which a raw \
             message key cannot populate; declare it as binary, or as a type a string parses into"
        )));
    }

    let fields: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(position, field)| {
            if position == index {
                Arc::new(field.as_ref().clone().with_nullable(true))
            } else {
                Arc::clone(field)
            }
        })
        .collect();
    let decode_schema = Arc::new(Schema::new(fields).with_metadata(schema.metadata().clone()));
    Ok((index, decode_schema))
}

/// The raw message keys as a column of `field`'s declared type.
///
/// A binary column takes the bytes exactly as they came off the wire.
/// Everything else goes through UTF-8 and then Arrow's string cast, which is
/// the inverse of how [`KafkaSink`](crate::KafkaSink) renders a key: it
/// formats one cell into text, so text is what a SACI-written key is.
fn key_column(
    entries: &[CompactedEntry],
    field: &Field,
    topics: &[String],
) -> Result<ArrayRef, SaciError> {
    match field.data_type() {
        DataType::Binary => Ok(Arc::new(BinaryArray::from_iter_values(
            entries.iter().map(|entry| entry.key.as_slice()),
        ))),
        DataType::LargeBinary => Ok(Arc::new(LargeBinaryArray::from_iter_values(
            entries.iter().map(|entry| entry.key.as_slice()),
        ))),
        data_type => {
            let mut text = Vec::with_capacity(entries.len());
            for entry in entries {
                text.push(str::from_utf8(&entry.key).map_err(|e| {
                    SaciError::generic(format!(
                        "KafkaSource: the key of {}[{}]@{} is not UTF-8, which column '{}' \
                         needs: {e}",
                        topics[entry.topic],
                        entry.partition,
                        entry.offset,
                        field.name()
                    ))
                })?);
            }
            let keys = StringArray::from_iter_values(text);
            if data_type == &DataType::Utf8 {
                return Ok(Arc::new(keys));
            }
            cast_with_options(&keys, data_type, &KEY_CAST).map_err(|e| {
                SaciError::generic(format!(
                    "KafkaSource: a message key does not parse into column '{}' ({data_type}): {e}",
                    field.name()
                ))
            })
        }
    }
}

#[async_trait]
impl Source for KafkaSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Cancel-safe: every message the consumer has taken from the broker is
    /// held on the source, so a dropped future hands them to the next call.
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        // A snapshot of a compacted topic keeps its whole state across calls
        // and reports EOF for good once it is out, so it shares nothing with
        // the subscribe-commit-poll path below.
        if self.cfg.compacted {
            return self.next_snapshot_batch().await;
        }

        self.ensure_started().await?;
        // Safe to commit here: `pending_commit` is set only by a call that
        // handed its whole buffer to the caller, so the consumer's position
        // covers nothing the caller has not seen.
        self.commit_pending()?;

        if self.cfg.stop_at_end {
            self.collect_bounded().await?;
        } else {
            self.collect_live().await?;
        }

        if self.buffer.is_empty() {
            return Ok(None);
        }

        let window = std::mem::take(&mut self.buffer);
        let batch = self.decode_window(&window)?;
        self.pending_commit = true;
        Ok(Some(batch))
    }

    /// A plain field write: sets `cfg.batch_size`, which every collection
    /// window and compacted-snapshot chunk already reads by reference, so it
    /// takes effect on the very next call. No reconnect, no re-subscription.
    /// `saci-service` sends the hint ahead of the first poll, so `batch_size`
    /// in config is not a starting size; it is authoritative only where the
    /// host sends no hint, meaning flow control disabled for this source or a
    /// source feeding a windowed node.
    fn request_batch_rows(&mut self, rows: usize) {
        self.cfg.batch_size = rows.max(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Array, Int64Array};
    use arrow_schema::Fields;
    use saci_connector::{ConfigMap, ConfigValue};
    use saci_transformer::TransformerFactory;

    /// Fold one record of the first topic in, keys and values as text for
    /// readability. `None` is a tombstone.
    fn apply(
        state: &mut CompactedState,
        partition: i32,
        offset: i64,
        key: &str,
        value: Option<&str>,
    ) {
        state.apply(
            0,
            partition,
            offset,
            key.as_bytes(),
            value.map(|value| value.as_bytes().to_vec()),
        );
    }

    /// Survivors as `(key, value)` text, in the order they came back.
    fn survivors(state: CompactedState) -> Vec<(String, String)> {
        state
            .into_survivors()
            .into_iter()
            .map(|entry| {
                (
                    String::from_utf8(entry.key).expect("test keys are text"),
                    String::from_utf8(entry.value).expect("test values are text"),
                )
            })
            .collect()
    }

    #[test]
    fn an_empty_log_has_no_survivors() {
        assert!(survivors(CompactedState::new(1)).is_empty());
    }

    #[test]
    fn the_newest_value_for_a_key_wins() {
        let mut state = CompactedState::new(1);
        apply(&mut state, 0, 0, "a", Some("first"));
        apply(&mut state, 0, 7, "a", Some("second"));

        let entries = state.into_survivors();
        assert_eq!(entries.len(), 1, "one key, one row");
        assert_eq!(entries[0].value, b"second");
        assert_eq!(entries[0].offset, 7, "the survivor keeps its own offset");
    }

    #[test]
    fn an_out_of_order_replay_still_keeps_the_newest_offset() {
        let mut state = CompactedState::new(1);
        apply(&mut state, 0, 5, "a", Some("late"));
        apply(&mut state, 0, 2, "a", Some("early"));

        assert_eq!(
            survivors(state),
            vec![("a".to_string(), "late".to_string())]
        );
    }

    #[test]
    fn a_tombstone_removes_its_key() {
        let mut state = CompactedState::new(1);
        apply(&mut state, 0, 0, "a", Some("value"));
        apply(&mut state, 0, 1, "b", Some("kept"));
        apply(&mut state, 0, 2, "a", None);

        assert_eq!(
            survivors(state),
            vec![("b".to_string(), "kept".to_string())]
        );
    }

    #[test]
    fn a_tombstone_for_an_unseen_key_is_ignored() {
        let mut state = CompactedState::new(1);
        apply(&mut state, 0, 0, "gone", None);

        assert!(
            survivors(state).is_empty(),
            "a delete of nothing leaves nothing behind"
        );
    }

    #[test]
    fn a_value_after_a_tombstone_brings_the_key_back() {
        let mut state = CompactedState::new(1);
        apply(&mut state, 0, 0, "a", Some("first"));
        apply(&mut state, 0, 1, "a", None);
        apply(&mut state, 0, 2, "a", Some("again"));

        assert_eq!(
            survivors(state),
            vec![("a".to_string(), "again".to_string())]
        );
    }

    #[test]
    fn a_tombstone_older_than_the_value_it_meets_does_not_delete() {
        let mut state = CompactedState::new(1);
        apply(&mut state, 0, 9, "a", Some("newest"));
        apply(&mut state, 0, 3, "a", None);

        assert_eq!(
            survivors(state),
            vec![("a".to_string(), "newest".to_string())]
        );
    }

    #[test]
    fn survivors_come_back_in_partition_then_offset_order() {
        let mut state = CompactedState::new(1);
        apply(&mut state, 1, 4, "d", Some("p1o4"));
        apply(&mut state, 0, 8, "b", Some("p0o8"));
        apply(&mut state, 1, 0, "c", Some("p1o0"));
        apply(&mut state, 0, 2, "a", Some("p0o2"));

        let order: Vec<(i32, i64)> = state
            .into_survivors()
            .iter()
            .map(|entry| (entry.partition, entry.offset))
            .collect();
        assert_eq!(order, vec![(0, 2), (0, 8), (1, 0), (1, 4)]);
    }

    #[test]
    fn each_topic_keeps_its_own_keys() {
        let mut state = CompactedState::new(2);
        state.apply(0, 0, 0, b"shared", Some(b"from-first".to_vec()));
        state.apply(1, 0, 0, b"shared", Some(b"from-second".to_vec()));

        let entries = state.into_survivors();
        assert_eq!(entries.len(), 2, "one key per topic, not one overall");
        assert_eq!(entries[0].topic, 0);
        assert_eq!(entries[0].value, b"from-first");
        assert_eq!(entries[1].topic, 1);
        assert_eq!(entries[1].value, b"from-second");
    }

    fn topics() -> Vec<String> {
        vec!["orders".to_string()]
    }

    fn keyed(keys: &[&[u8]]) -> Vec<CompactedEntry> {
        keys.iter()
            .enumerate()
            .map(|(offset, key)| CompactedEntry {
                topic: 0,
                partition: 0,
                offset: offset as i64,
                key: key.to_vec(),
                value: b"{}".to_vec(),
            })
            .collect()
    }

    #[test]
    fn a_utf8_key_column_takes_the_key_verbatim() {
        let field = Field::new("id", DataType::Utf8, false);
        let column = key_column(&keyed(&[b"a", b"b"]), &field, &topics()).expect("text keys");

        let keys = column
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("a utf8 column");
        assert_eq!(keys.value(0), "a");
        assert_eq!(keys.value(1), "b");
        assert_eq!(keys.null_count(), 0, "every survivor has a key");
    }

    #[test]
    fn a_binary_key_column_takes_bytes_that_are_not_text() {
        let field = Field::new("id", DataType::Binary, false);
        let column = key_column(&keyed(&[&[0xff, 0x00]]), &field, &topics()).expect("raw keys");

        let keys = column
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("a binary column");
        assert_eq!(keys.value(0), &[0xff, 0x00]);
    }

    #[test]
    fn an_int64_key_column_parses_the_rendered_key() {
        // `KafkaSink` renders a key by formatting the cell, so an integer key
        // is the text of that integer on the wire.
        let field = Field::new("id", DataType::Int64, false);
        let column = key_column(&keyed(&[b"7", b"42"]), &field, &topics()).expect("numeric keys");

        let keys = column
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("an int64 column");
        assert_eq!(keys.values(), &[7, 42]);
    }

    #[test]
    fn a_key_that_is_not_text_fails_a_text_column() {
        let field = Field::new("id", DataType::Utf8, false);
        let err = key_column(&keyed(&[&[0xff]]), &field, &topics()).expect_err("not UTF-8");
        assert!(err.message().contains("orders[0]@0"), "got: {err}");
    }

    #[test]
    fn a_key_that_does_not_parse_fails_its_column() {
        // Never a silent null: the key is the identity of the row.
        let field = Field::new("id", DataType::Int64, false);
        let err = key_column(&keyed(&[b"not-a-number"]), &field, &topics())
            .expect_err("an unparseable key");
        assert!(err.message().contains("'id'"), "got: {err}");
    }

    fn transformer(factory: impl TransformerFactory) -> Arc<dyn Transformer> {
        factory
            .build(&ConfigValue::Object(ConfigMap::new()))
            .expect("a transformer factory must build from empty options")
    }

    fn declared(key_type: DataType) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", key_type, false),
            Field::new("label", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn the_decode_schema_makes_only_the_key_column_nullable() {
        let schema = declared(DataType::Int64);
        let (index, decode_schema) = compacted_key_column(
            "id",
            &schema,
            transformer(saci_transformer_ndjson::NdjsonTransformerFactory).as_ref(),
        )
        .expect("a declared key column");

        assert_eq!(index, 0);
        assert!(
            decode_schema.field(0).is_nullable(),
            "the payload carries no key, so the decoder must be allowed to leave it empty"
        );
        assert!(!decode_schema.field(1).is_nullable());
        assert_eq!(decode_schema.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn a_key_field_outside_the_declared_schema_is_a_configuration_error() {
        let err = compacted_key_column(
            "missing",
            &declared(DataType::Int64),
            transformer(saci_transformer_ndjson::NdjsonTransformerFactory).as_ref(),
        )
        .expect_err("an undeclared key column");

        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("\"id\""), "got: {err}");
    }

    #[test]
    fn a_batch_per_message_format_cannot_be_compacted() {
        let err = compacted_key_column(
            "id",
            &declared(DataType::Int64),
            transformer(saci_transformer_arrow_ipc::ArrowIpcTransformerFactory).as_ref(),
        )
        .expect_err("arrow-ipc carries one message per batch");

        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("row-per-message"), "got: {err}");
    }

    #[test]
    fn a_key_column_no_key_can_populate_is_a_configuration_error() {
        let err = compacted_key_column(
            "id",
            &declared(DataType::Struct(Fields::empty())),
            transformer(saci_transformer_ndjson::NdjsonTransformerFactory).as_ref(),
        )
        .expect_err("a struct column cannot hold a message key");

        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'key_field'"), "got: {err}");
    }
}
