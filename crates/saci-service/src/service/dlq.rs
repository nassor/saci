//! The dead letter queue: what happens to a batch a sink refused.
//!
//! Without a `dlq` block a refused batch is logged, counted in
//! [`StandaloneStats::iteration_errors`](super::standalone::StandaloneStats)
//! and dropped. With one, the runner hands it to [`DeadLetterQueue`]'s
//! `record`, which stores it as one row of a fixed envelope schema in a store
//! the workflow named, and its `replay` reads those rows back and
//! offers each batch to its sink again.
//!
//! ## What is captured
//!
//! Sink write failures, and nothing else. A processor error is not a dead
//! letter: the batch it failed on came from upstream and is still upstream's,
//! so replaying it would run the processor twice on data it never accepted.
//!
//! ## The envelope
//!
//! One letter is one row of [`envelope_schema`]. The failed batch travels in
//! the `payload` column as one Arrow IPC stream, encoded by
//! [`ArrowIpcTransformer`]: IPC is already the host to processor wire format,
//! reproduces the batch and its schema metadata exactly, and needs no encode
//! pass over the values. The other seven columns are what
//! the dashboard groups and filters on, so a reader never decodes a payload
//! to answer "what is waiting, and why".
//!
//! ## Which stores, and what each guarantees
//!
//! A store is a connector pair: the sink half records, the source half
//! replays. `DLQ_STORES` is the table. `exclusive` marks a store whose sink
//! half holds the file against its own source half, which is why a replay
//! there finishes the sink first and rebuilds it afterwards. `confirms_end`
//! marks a source half whose end of stream is an answer about the store
//! rather than one elapsed poll window; a drain of a store without it leaves
//! [`DlqSummary::known`] false however cleanly it ended.
//!
//! Every source half consumes a window at the head of the next fetch: redb
//! pops the key whose stream has just ended, a broker commits or acks the
//! offsets it handed over last. A drain that stops early therefore leaves the
//! window it just read in the store, and the letters of that window are
//! dropped rather than written back, so the store holds each of them once and
//! offers them again on the next replay. The ones that window had already
//! delivered are delivered a second time, which is the at-least-once rule all
//! three sources document.
//!
//! No store is loss-free across a crash inside a replay. redb's source half
//! deletes what it yielded in [`Source::finish`], which has to run before the
//! sink half can reopen the file, so a crash between that delete and the
//! write-back loses the letters that failed again. A broker loses the same
//! window the same way. The window is one replay wide in both cases.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::{Array, BinaryArray, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_schema::Schema;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use saci_connector::{ConfigMap, ConfigValue, parse_schema_fields};
use saci_core::error::{SaciError, SaciResult};
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use saci_inspector_wire::{DlqGroup, DlqReplayReport, DlqSummary, DlqTrigger};
use saci_transformer::Transformer;
use saci_transformer_arrow_ipc::ArrowIpcTransformer;

use super::config::{DlqBlock, DlqReplayPoint, RetryConfig, ServiceConfig, WorkflowSpec};
use super::heal::{HealSettings, Rebuilder};
use super::registry::Registry;
use super::sampling::DLQ_TARGET;
use super::standalone::{NodeRunStats, StandaloneStats};

/// One store the `dlq` block may name.
pub(crate) struct DlqStoreKind {
    /// The name the block names it by.
    pub(crate) id: &'static str,
    /// Connector `type` of the recording half.
    pub(crate) sink_type: &'static str,
    /// Connector `type` of the replaying half.
    pub(crate) source_type: &'static str,
    /// Whether the sink half holds the store against its own source half, so
    /// the two can never be open at once.
    pub(crate) exclusive: bool,
    /// Whether the source half's end of stream means the store is empty.
    ///
    /// `RedbSource` walks a key list it scanned once, and `NatsSource`
    /// refuses to report the end until JetStream's own counters say this
    /// consumer has nothing waiting and nothing unacknowledged, so both
    /// answer about the store. `KafkaSource`'s `stop_at_end` drain also ends
    /// on an elapsed `poll_timeout_ms` window, and a group join that has not
    /// settled yet reports no assignment at all, so its end of stream means
    /// "nothing arrived in one window" and proves nothing about the topic.
    pub(crate) confirms_end: bool,
}

/// Every store a `dlq` block may name.
pub(crate) const DLQ_STORES: &[DlqStoreKind] = &[
    DlqStoreKind {
        id: "redb",
        sink_type: "RedbSink",
        source_type: "RedbSource",
        exclusive: true,
        confirms_end: true,
    },
    DlqStoreKind {
        id: "kafka",
        sink_type: "KafkaSink",
        source_type: "KafkaSource",
        exclusive: false,
        confirms_end: false,
    },
    DlqStoreKind {
        id: "nats",
        sink_type: "NatsSink",
        source_type: "NatsSource",
        exclusive: false,
        confirms_end: true,
    },
];

/// The store `id` names, or `None` when nothing does.
pub(crate) fn store_kind(id: &str) -> Option<&'static DlqStoreKind> {
    DLQ_STORES.iter().find(|kind| kind.id == id)
}

/// Every store name, for the refusal a bad one gets.
pub(crate) fn store_names() -> String {
    DLQ_STORES
        .iter()
        .map(|kind| kind.id)
        .collect::<Vec<_>>()
        .join(", ")
}

/// The envelope's columns, in order, as `(name, Arrow type string)`.
///
/// One table for both forms the schema takes: the Arrow [`Schema`] the
/// payload decoder projects onto, and the `schema_fields` array both store
/// halves are configured with. Deriving them from one list is what keeps a
/// connector's declared schema and the batch the layer writes identical.
const ENVELOPE_FIELDS: &[(&str, &str)] = &[
    ("workflow", "utf8"),
    ("sink", "utf8"),
    ("component", "utf8"),
    ("reason", "utf8"),
    ("failed_at_unix_ms", "uint64"),
    ("replays", "uint32"),
    ("rows", "uint32"),
    ("payload", "binary"),
];

/// The envelope's `schema_fields` as a store half's config carries it.
pub(crate) fn envelope_schema_fields() -> ConfigValue {
    ConfigValue::Array(
        ENVELOPE_FIELDS
            .iter()
            .map(|(name, type_name)| {
                let mut entry = ConfigMap::new();
                entry.insert("id".to_string(), ConfigValue::from(*name));
                entry.insert("type".to_string(), ConfigValue::from(*type_name));
                entry.insert("nullable".to_string(), ConfigValue::Bool(false));
                ConfigValue::Object(entry)
            })
            .collect(),
    )
}

/// The Arrow schema one dead letter is a row of.
pub fn envelope_schema() -> Arc<Schema> {
    static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
        let mut config = ConfigMap::new();
        config.insert("schema_fields".to_string(), envelope_schema_fields());
        parse_schema_fields(&ConfigValue::Object(config), "dead letter envelope")
            .expect("ENVELOPE_FIELDS names only supported Arrow types")
    });
    Arc::clone(&SCHEMA)
}

/// Wall clock in Unix milliseconds.
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The failure as a letter records it, which is also what the dashboard
/// groups by.
///
/// A [`SaciError::RetryExhausted`] is unwrapped to the error of its last
/// attempt: its own message carries `after N attempt(s):`, and N varies
/// between two failures of the same cause, so grouping on it would file every
/// letter under its own reason. Unwrapping is recursive because a healing
/// wrapper's error may itself carry a retried one.
///
/// `message()` rather than `Display`: the latter prefixes the variant
/// (`Error: `, `Configuration error: `), which says nothing a reader of a
/// grouping key needs.
pub(crate) fn reason_of(error: &SaciError) -> String {
    let mut current = error;
    while let SaciError::RetryExhausted { source, .. } = current {
        current = source;
    }
    current.message()
}

/// One dead letter, decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Letter {
    /// Declared id of the sink that refused the batch.
    pub(crate) sink: String,
    /// The sink's component name, carried so a replay can report it without
    /// resolving the node again.
    pub(crate) component: String,
    /// The failure, from [`reason_of`].
    pub(crate) reason: String,
    /// When this was recorded, or last re-recorded.
    pub(crate) failed_at_unix_ms: u64,
    /// How many replays this letter has failed in.
    pub(crate) replays: u32,
    /// Rows in `payload`.
    pub(crate) rows: u32,
    /// The refused batch as one Arrow IPC stream.
    pub(crate) payload: Vec<u8>,
}

impl Letter {
    /// Encode a refused batch into a letter.
    ///
    /// # Errors
    ///
    /// Returns whatever [`Transformer::encode_messages`] refused the batch
    /// with, and [`SaciError::Generic`] when it yielded no payload, which
    /// `arrow-ipc` never does: it splits `PerBatch`.
    pub(crate) fn from_failure(
        batch: &RecordBatch,
        sink: &str,
        component: &str,
        error: &SaciError,
        ipc: &ArrowIpcTransformer,
    ) -> Result<Self, SaciError> {
        let mut payloads = ipc.encode_messages(batch)?;
        let payload = payloads.pop().ok_or_else(|| {
            SaciError::generic("dead letter: arrow-ipc encoded no payload for the refused batch")
        })?;
        Ok(Self {
            sink: sink.to_string(),
            component: component.to_string(),
            reason: reason_of(error),
            failed_at_unix_ms: now_unix_ms(),
            replays: 0,
            // Saturating rather than wrapping: the column is what the
            // dashboard reports, so a batch wider than a u32 is better read
            // as "at the ceiling" than as its remainder.
            rows: u32::try_from(batch.num_rows()).unwrap_or(u32::MAX),
            payload,
        })
    }

    /// Decode row `row` of an envelope batch.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the batch does not carry the
    /// envelope's columns and types, which is what a store holding rows some
    /// other writer put there looks like.
    pub(crate) fn from_row(batch: &RecordBatch, row: usize) -> Result<Self, SaciError> {
        let utf8 = |name: &str| -> Result<String, SaciError> {
            let column = column_of(batch, name)?;
            let values = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| envelope_mismatch(name, "utf8"))?;
            Ok(values.value(row).to_string())
        };
        let u32_of = |name: &str| -> Result<u32, SaciError> {
            let column = column_of(batch, name)?;
            let values = column
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| envelope_mismatch(name, "uint32"))?;
            Ok(values.value(row))
        };
        let payload = {
            let column = column_of(batch, "payload")?;
            let values = column
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| envelope_mismatch("payload", "binary"))?;
            values.value(row).to_vec()
        };
        let failed_at_unix_ms = {
            let column = column_of(batch, "failed_at_unix_ms")?;
            let values = column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| envelope_mismatch("failed_at_unix_ms", "uint64"))?;
            values.value(row)
        };
        Ok(Self {
            sink: utf8("sink")?,
            component: utf8("component")?,
            reason: utf8("reason")?,
            failed_at_unix_ms,
            replays: u32_of("replays")?,
            rows: u32_of("rows")?,
            payload,
        })
    }

    /// This letter as a one-row envelope batch of `workflow`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the arrays do not build, which
    /// requires the envelope schema and [`ENVELOPE_FIELDS`] to disagree.
    pub(crate) fn to_envelope(&self, workflow: &str) -> Result<RecordBatch, SaciError> {
        RecordBatch::try_new(
            envelope_schema(),
            vec![
                Arc::new(StringArray::from(vec![workflow])),
                Arc::new(StringArray::from(vec![self.sink.as_str()])),
                Arc::new(StringArray::from(vec![self.component.as_str()])),
                Arc::new(StringArray::from(vec![self.reason.as_str()])),
                Arc::new(UInt64Array::from(vec![self.failed_at_unix_ms])),
                Arc::new(UInt32Array::from(vec![self.replays])),
                Arc::new(UInt32Array::from(vec![self.rows])),
                Arc::new(BinaryArray::from(vec![self.payload.as_slice()])),
            ],
        )
        .map_err(|e| SaciError::generic(format!("dead letter: building an envelope row: {e}")))
    }

    /// The refused batch, decoded against `schema`.
    ///
    /// # Errors
    ///
    /// Returns the decoder's own error, and [`SaciError::Generic`] when the
    /// payload decoded to no batch at all.
    pub(crate) fn payload_batch(
        &self,
        schema: Arc<Schema>,
        ipc: &ArrowIpcTransformer,
    ) -> Result<RecordBatch, SaciError> {
        let mut decoder = ipc.open_message_decoder(schema)?;
        decoder.push(&self.payload)?;
        decoder.flush()?.ok_or_else(|| {
            SaciError::generic(format!(
                "dead letter: the payload recorded for sink '{}' decoded to no batch",
                self.sink
            ))
        })
    }
}

/// One column of an envelope batch, by name.
fn column_of<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a arrow_array::ArrayRef, SaciError> {
    batch.column_by_name(name).ok_or_else(|| {
        SaciError::generic(format!(
            "dead letter: a stored row carries no '{name}' column; the store holds rows \
             this queue did not write"
        ))
    })
}

fn envelope_mismatch(name: &str, expected: &str) -> SaciError {
    SaciError::generic(format!(
        "dead letter: a stored row's '{name}' column is not {expected}"
    ))
}

/// Which letters a replay offers to their sink.
///
/// A letter the filter excludes is written back unchanged, replay count and
/// timestamp included: it was never attempted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReplayFilter {
    /// Only letters this sink refused.
    pub sink: Option<String>,
    /// Only letters carrying this reason.
    pub reason: Option<String>,
}

impl ReplayFilter {
    /// Whether `letter` is offered to its sink.
    fn admits(&self, letter: &Letter) -> bool {
        self.sink.as_ref().is_none_or(|sink| *sink == letter.sink)
            && self
                .reason
                .as_ref()
                .is_none_or(|reason| *reason == letter.reason)
    }
}

/// Why a replay request was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqError {
    /// A request is already waiting for the runner to reach its replay point.
    Busy,
}

/// One requested replay, waiting for the runner.
struct PendingRequest {
    trigger: DlqTrigger,
    filter: ReplayFilter,
    reply: oneshot::Sender<DlqReplayReport>,
}

/// The half of one workflow's queue the HTTP control plane holds.
///
/// The queue itself lives in the runner and is touched by nothing else, so a
/// request crosses over as one slot the runner drains at its replay point,
/// and the summary crosses back as one snapshot the runner replaces. Both are
/// `std` locks: neither is ever held across an await.
pub struct DlqShared {
    request: Mutex<Option<PendingRequest>>,
    summary: RwLock<DlqSummary>,
}

impl DlqShared {
    /// An empty queue's shared half, before the runner has read the store.
    pub fn new(workflow: &str, block: &DlqBlock) -> Self {
        Self {
            request: Mutex::new(None),
            summary: RwLock::new(DlqSummary {
                workflow: workflow.to_string(),
                store: block.store.clone(),
                replay: block.replay.as_str().to_string(),
                known: false,
                letters: 0,
                rows: 0,
                groups: Vec::new(),
                last_replay: None,
                next_auto_replay_unix_ms: None,
                replay_pending: false,
            }),
        }
    }

    /// What the runner last published.
    pub fn summary(&self) -> DlqSummary {
        self.read_summary().clone()
    }

    fn read_summary(&self) -> std::sync::RwLockReadGuard<'_, DlqSummary> {
        self.summary
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn publish(&self, summary: DlqSummary) {
        *self
            .summary
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = summary;
    }

    /// Ask the runner to replay at its next replay point.
    ///
    /// # Errors
    ///
    /// Returns [`DlqError::Busy`] when a request is already waiting.
    pub fn request(
        &self,
        trigger: DlqTrigger,
        filter: ReplayFilter,
    ) -> Result<oneshot::Receiver<DlqReplayReport>, DlqError> {
        let mut slot = self
            .request
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_some() {
            return Err(DlqError::Busy);
        }
        let (reply, receiver) = oneshot::channel();
        *slot = Some(PendingRequest {
            trigger,
            filter,
            reply,
        });
        // Still holding the slot: a runner that takes the request between the
        // insert and this flag publishes a summary reading `has_request() ==
        // false`, and the flag set afterwards would then stay latched true
        // until some later publish cleared it. Lock order is slot then
        // summary, and `publish` takes the summary alone, so nothing inverts
        // it.
        self.set_pending(true);
        drop(slot);
        Ok(receiver)
    }

    fn take_request(&self) -> Option<PendingRequest> {
        self.request
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }

    fn has_request(&self) -> bool {
        self.request
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    fn set_pending(&self, pending: bool) {
        self.summary
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .replay_pending = pending;
    }

    /// Take a queued request and answer it with `report`, standing in for the
    /// runner. `None` when nothing was queued.
    #[cfg(test)]
    pub(crate) fn answer_for_test(&self, report: DlqReplayReport) -> Option<DlqTrigger> {
        let request = self.take_request()?;
        let trigger = request.trigger;
        let _ = request.reply.send(report);
        self.set_pending(false);
        Some(trigger)
    }
}

/// Every declared queue, by workflow id.
///
/// Built from the config rather than from a running service, so
/// `GET /api/dlq` answers before the first pass and a workflow the lifecycle
/// plane has stopped keeps the summary its last run published.
pub struct DlqRegistry {
    entries: Vec<(String, Arc<DlqShared>)>,
}

impl DlqRegistry {
    /// One entry per workflow that declares a `dlq` block.
    pub fn from_config(config: &ServiceConfig) -> Self {
        let entries = config
            .workflows
            .iter()
            .filter_map(|workflow| {
                let dlq = workflow.dlq.as_ref()?;
                Some((
                    workflow.id.clone(),
                    Arc::new(DlqShared::new(&workflow.id, &dlq.0)),
                ))
            })
            .collect();
        Self { entries }
    }

    /// The queue workflow `id` declared, or `None`.
    pub fn get(&self, id: &str) -> Option<&Arc<DlqShared>> {
        self.entries
            .iter()
            .find(|(workflow, _)| workflow == id)
            .map(|(_, shared)| shared)
    }

    /// Every queue's summary, in declaration order.
    pub fn list(&self) -> Vec<DlqSummary> {
        self.entries
            .iter()
            .map(|(_, shared)| shared.summary())
            .collect()
    }

    /// Whether no workflow declares a queue.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// What one replay needs from the runner that owns the workflow's nodes.
pub(crate) struct ReplayCtx<'a> {
    /// Every node's sink, `None` for a source or processor node.
    pub(crate) sinks: &'a mut [Option<Box<dyn Sink>>],
    /// Every node's declared id.
    pub(crate) ids: &'a [String],
    /// Every sink node's recovered flag, when it is healed.
    pub(crate) recovered: &'a [Option<Arc<AtomicBool>>],
    /// Per-node counters a delivered letter lands in.
    pub(crate) node_stats: &'a mut [NodeRunStats],
    /// The run's counters.
    pub(crate) stats: &'a mut StandaloneStats,
    /// Shutdown, which ends a drain like an error does.
    pub(crate) cancel: &'a CancellationToken,
}

/// One workflow's dead letter queue, owned by its runner.
///
/// Every method runs inline between passes, so `record` and `replay` never
/// overlap and the store is touched from one place.
pub struct DeadLetterQueue {
    workflow_id: String,
    store: &'static DlqStoreKind,
    replay_point: DlqReplayPoint,
    sink_rebuilder: Rebuilder,
    source_rebuilder: Rebuilder,
    /// The recording half. `None` while it is down, and while a replay holds
    /// the store on an `exclusive` store.
    sink: Option<Box<dyn Sink>>,
    /// Failed rebuilds of the recording half, for its backoff.
    sink_attempt: u32,
    /// When the next rebuild of the recording half may be tried.
    sink_down_until: Option<Instant>,
    ipc: Arc<ArrowIpcTransformer>,
    shared: Arc<DlqShared>,
    /// Whether the startup replay has run.
    started_once: bool,
    /// Letters waiting, as far as this process knows.
    pending_letters: u64,
    /// Automatic replays that left letters behind, for the backoff.
    auto_attempt: u32,
    /// When the automatic schedule next comes due.
    next_auto_replay: Option<Instant>,
    /// Whether a replay of this process has read the store through without
    /// error. Mirrors [`DlqSummary::known`], cached here so the pass path
    /// reads a bool instead of cloning the whole summary out of its lock.
    known: bool,
}

/// Counts and identity only: the two rebuilders hold trait objects that are
/// not `Debug`, and the store's config carries whatever credentials the
/// block declared.
impl std::fmt::Debug for DeadLetterQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeadLetterQueue")
            .field("workflow_id", &self.workflow_id)
            .field("store", &self.store.id)
            .field("replay", &self.replay_point.as_str())
            .field("pending_letters", &self.pending_letters)
            .finish_non_exhaustive()
    }
}

impl DeadLetterQueue {
    /// Build both halves' rebuilders and open the recording half.
    ///
    /// The recording half is opened now, the way a sink node is, so a store
    /// that cannot be opened refuses the workflow at build rather than at the
    /// first failure.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the block names a store this
    /// build carries no connector for, when a `nats` store's mode is not
    /// JetStream, and whatever the store's sink factory refused its config
    /// with.
    pub(crate) fn build(
        workflow: &WorkflowSpec,
        data_dir: &Path,
        registry: &Arc<Registry>,
        shared: Arc<DlqShared>,
    ) -> SaciResult<Self> {
        let block = &workflow
            .dlq
            .as_ref()
            .expect("build is called only for a workflow declaring a dlq block")
            .0;
        let store = store_kind(&block.store).ok_or_else(|| {
            SaciError::configuration(format!(
                "workflow '{}': dlq store '{}' is not one of {}",
                workflow.id,
                block.store,
                store_names()
            ))
        })?;
        validate_store_block(&workflow.id, store, block)?;

        let injected = injected_keys(store, data_dir, &workflow.id);
        let sink_config = half_config(block, &block.sink, &injected.sink);
        let source_config = half_config(block, &block.source, &injected.source);
        let ipc: Arc<dyn Transformer> = Arc::new(ArrowIpcTransformer::new());
        let node_id = format!("{}/dlq", workflow.id);

        let sink_rebuilder = Rebuilder::new(
            registry.clone(),
            store.sink_type,
            &node_id,
            sink_config,
            Some(Arc::clone(&ipc)),
            None,
            Some(RetryConfig::default().to_system_config()),
        );
        // No retry wrapper on the source half: a replay is one bounded drain,
        // and an error ends it rather than re-driving a store that is not
        // answering.
        let source_rebuilder = Rebuilder::new(
            registry.clone(),
            store.source_type,
            &node_id,
            source_config,
            Some(ipc),
            None,
            None,
        );
        // Both halves are proven at build, not at the first failure: every
        // built-in connector validates its config without touching a socket,
        // so a key one half refuses is a load-time error the way a sink
        // node's is. The source half is dropped again, because a replay
        // builds its own; on an exclusive store the sink half holds the file
        // and this instance is what the runner keeps.
        drop(source_rebuilder.build_source()?);
        let sink = sink_rebuilder.build_sink()?;

        // The shared half outlives this instance: a lifecycle restart builds
        // a second queue over the same `DlqShared`. Seeding from it is what
        // stops a fresh instance whose first replay errors from downgrading
        // a `known` an earlier one earned. The startup replay still runs
        // either way, because `!started_once` is checked first.
        let known = shared.summary().known;

        Ok(Self {
            workflow_id: workflow.id.clone(),
            store,
            replay_point: block.replay,
            sink_rebuilder,
            source_rebuilder,
            sink: Some(sink),
            sink_attempt: 0,
            sink_down_until: None,
            ipc: Arc::new(ArrowIpcTransformer::new()),
            shared,
            started_once: false,
            pending_letters: 0,
            auto_attempt: 0,
            next_auto_replay: None,
            known,
        })
    }

    /// Store one batch a sink refused. Never fails outward: a queue that
    /// cannot take a letter counts it lost and says so.
    pub(crate) async fn record(
        &mut self,
        batch: &RecordBatch,
        sink: &str,
        component: &str,
        error: &SaciError,
        stats: &mut StandaloneStats,
    ) {
        let rows = batch.num_rows() as u64;
        if self.sink.is_none() && !self.rebuild_record_half() {
            #[cfg(feature = "tracing")]
            tracing::error!(
                target: DLQ_TARGET,
                workflow = %self.workflow_id,
                sink,
                rows,
                "dead letter store unavailable (letter lost)"
            );
            self.count_lost(sink);
            return;
        }
        let letter = match Letter::from_failure(batch, sink, component, error, &self.ipc) {
            Ok(letter) => letter,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    target: DLQ_TARGET,
                    workflow = %self.workflow_id,
                    sink,
                    error = %_e,
                    "dead letter could not be encoded (letter lost)"
                );
                self.count_lost(sink);
                return;
            }
        };
        let envelope = match letter.to_envelope(&self.workflow_id) {
            Ok(envelope) => envelope,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    target: DLQ_TARGET,
                    workflow = %self.workflow_id,
                    sink,
                    error = %_e,
                    "dead letter could not be encoded (letter lost)"
                );
                self.count_lost(sink);
                return;
            }
        };
        let store = self.sink.as_mut().expect("the record half is open");
        match store.write_batch(&envelope).await {
            Ok(()) => {
                self.sink_attempt = 0;
                self.pending_letters += 1;
                stats.dead_letters_recorded += 1;
                crate::metrics::instruments().dlq_recorded(sink, rows);
                self.add_to_summary(&letter);
                self.schedule_auto_replay();
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    target: DLQ_TARGET,
                    workflow = %self.workflow_id,
                    sink,
                    rows,
                    reason = %letter.reason,
                    "dead letter recorded"
                );
            }
            Err(_e) => {
                // The instance proved unwritable, so it goes before the next
                // rebuild, exactly as `HealingSink` drops one.
                self.sink = None;
                self.schedule_record_retry();
                #[cfg(feature = "tracing")]
                tracing::error!(
                    target: DLQ_TARGET,
                    workflow = %self.workflow_id,
                    sink,
                    rows,
                    error = %_e,
                    "dead letter store write failed (letter lost)"
                );
                self.count_lost(sink);
            }
        }
    }

    /// Replay at the head of a pass, when the workflow asked for that point.
    pub(crate) async fn at_head(&mut self, ctx: ReplayCtx<'_>) {
        if self.replay_point == DlqReplayPoint::BeforeSources {
            self.maybe_replay(ctx).await;
        }
    }

    /// Replay at the tail of a pass, when the workflow asked for that point.
    pub(crate) async fn at_tail(&mut self, ctx: ReplayCtx<'_>) {
        if self.replay_point == DlqReplayPoint::AfterSources {
            self.maybe_replay(ctx).await;
        }
    }

    /// Finalise the recording half and publish the last summary.
    pub(crate) async fn finish(&mut self) {
        if let Some(sink) = self.sink.as_mut()
            && let Err(_e) = sink.finish().await
        {
            #[cfg(feature = "tracing")]
            tracing::error!(
                target: DLQ_TARGET,
                workflow = %self.workflow_id,
                error = %_e,
                "dead letter store finish error"
            );
        }
        self.sink = None;
        self.publish_summary(None);
    }

    /// Run a replay when the startup pass, a request, a healed sink or the
    /// schedule calls for one.
    async fn maybe_replay(&mut self, ctx: ReplayCtx<'_>) {
        // Every flag is swapped, not just the first `true` one: a second
        // healed sink left latched would trigger a redundant replay later.
        let mut healed = false;
        for flag in ctx.recovered.iter().flatten() {
            healed |= flag.swap(false, Ordering::Relaxed);
        }
        let request = self.shared.take_request();
        // A shutdown is not the moment to open a store: `drain` would end on
        // the same check, after an exclusive store had paid a whole
        // finish/open/finish/rebuild cycle for nothing.
        if request.is_none() && ctx.cancel.is_cancelled() {
            self.started_once = true;
            return;
        }
        // A store this process has read through and left empty needs no
        // drain: only a request, or the first pass of a process that has
        // never looked, can find something in it.
        let empty_and_known = self.pending_letters == 0 && self.known;
        let trigger = if let Some(request) = &request {
            request.trigger
        } else if !self.started_once {
            DlqTrigger::Startup
        } else if empty_and_known {
            self.started_once = true;
            return;
        } else if healed {
            DlqTrigger::Heal
        } else if self
            .next_auto_replay
            .is_some_and(|due| Instant::now() >= due)
        {
            DlqTrigger::Schedule
        } else {
            self.started_once = true;
            return;
        };
        self.started_once = true;
        let filter = request
            .as_ref()
            .map(|request| request.filter.clone())
            .unwrap_or_default();
        let report = self.replay(trigger, filter, ctx).await;
        if let Some(request) = request {
            // A closed receiver means the HTTP handler gave up waiting and
            // answered 202; the report is still the one the summary carries.
            let _ = request.reply.send(report);
        }
    }

    /// Drain the store once, offering every admitted letter to its sink.
    async fn replay(
        &mut self,
        trigger: DlqTrigger,
        filter: ReplayFilter,
        ctx: ReplayCtx<'_>,
    ) -> DlqReplayReport {
        let started_at_unix_ms = now_unix_ms();
        let started = Instant::now();
        let mut report = DlqReplayReport {
            trigger,
            started_at_unix_ms,
            duration_ms: 0,
            delivered: 0,
            retained: 0,
            purged: 0,
            lost: 0,
            error: None,
        };
        // An exclusive store's sink half holds the file, so it goes before
        // the source half opens and is rebuilt once the drain is over.
        if self.store.exclusive {
            self.close_record_half().await;
        }
        let mut retained: Vec<Letter> = Vec::new();
        match self.source_rebuilder.build_source() {
            Ok(mut source) => {
                self.drain(
                    &mut *source,
                    &filter,
                    trigger,
                    &mut retained,
                    &mut report,
                    ctx,
                )
                .await;
                if let Err(_e) = source.finish().await {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        target: DLQ_TARGET,
                        workflow = %self.workflow_id,
                        error = %_e,
                        "dead letter store finish error after a replay"
                    );
                }
            }
            Err(e) => report.error = Some(e.to_string()),
        }

        // Written after the drain for every store, exclusive or not, so a
        // re-recorded letter is never read back inside the same drain.
        self.rewrite(&mut retained, &mut report).await;

        report.retained = retained.len() as u64;
        report.duration_ms = started.elapsed().as_millis() as u64;
        self.pending_letters = retained.len() as u64;
        // An errored replay keeps the schedule whether or not it retained
        // anything. A source half that could not be built, or a `next_batch`
        // that failed, leaves a store this process has learnt nothing about:
        // with no letter in hand and no schedule, `maybe_replay` would find
        // no trigger again and whatever an earlier run left behind would wait
        // for a request.
        if retained.is_empty() && report.error.is_none() {
            self.auto_attempt = 0;
            self.next_auto_replay = None;
        } else {
            self.auto_attempt += 1;
            self.next_auto_replay =
                Some(Instant::now() + HealSettings::default().delay_for(self.auto_attempt));
        }
        // A clean drain proves the store is empty only where its source half
        // reports an end it can stand behind. On a store whose end of stream
        // is one elapsed poll window, a replay that came back empty leaves
        // `known` false, so `empty_and_known` cannot swallow the heal and
        // schedule triggers that would look again.
        let read_through = report.error.is_none() && self.store.confirms_end;
        self.regroup(&retained, read_through, Some(report.clone()));

        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: DLQ_TARGET,
            workflow = %self.workflow_id,
            trigger = trigger.as_str(),
            delivered = report.delivered,
            retained = report.retained,
            purged = report.purged,
            lost = report.lost,
            error = report.error.as_deref().unwrap_or(""),
            "dead letter replay finished"
        );
        report
    }

    /// One bounded drain of the source half.
    async fn drain(
        &mut self,
        source: &mut dyn Source,
        filter: &ReplayFilter,
        trigger: DlqTrigger,
        retained: &mut Vec<Letter>,
        report: &mut DlqReplayReport,
        ctx: ReplayCtx<'_>,
    ) {
        let ReplayCtx {
            sinks,
            ids,
            node_stats,
            stats,
            cancel,
            ..
        } = ctx;
        let mut warned_unknown = false;
        // A sink that refused one letter refuses the rest, and every attempt
        // pays that sink's whole retry policy inline in the pass. One
        // thousand letters against a dead sink would stall the workflow for
        // minutes, so the first refusal retires that sink for this drain and
        // its remaining letters are written straight back.
        let mut refused: Vec<usize> = Vec::new();
        // Where the letters of the most recently fetched window start.
        //
        // Every store consumes a window at the head of the *next* fetch:
        // `RedbSource` pops a key once the following `next_batch` sees that
        // entry's stream end, and both brokers commit or ack the previous
        // window before collecting a new one. A drain that stops before that
        // next fetch therefore leaves the window it just read in the store,
        // and `rewrite` writing those letters back again would duplicate
        // them. Cutting `retained` back to here instead leaves the window in
        // the store exactly once, and the letters of it that were already
        // delivered are offered again on the next replay, which is the
        // at-least-once rule every one of these sources documents.
        let mut window = retained.len();
        loop {
            if cancel.is_cancelled() {
                retained.truncate(window);
                report.error = Some("cancelled".to_string());
                return;
            }
            let envelopes = match source.next_batch().await {
                Ok(None) => return,
                // Not an end of stream anywhere: all three source halves
                // report EOF as `Ok(None)`, so this is one window that
                // decoded to no row and the drain goes on.
                Ok(Some(envelopes)) if envelopes.num_rows() == 0 => continue,
                Ok(Some(envelopes)) => envelopes,
                Err(e) => {
                    // No cut here: a failed fetch has already consumed the
                    // window before it. The brokers commit or ack at the head
                    // of the fetch that then failed, and `RedbSource` pops
                    // the finished entry's key in the same call, before the
                    // open that can fail. So `rewrite` is the only thing
                    // keeping those letters, and keeping them duplicates
                    // nothing.
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        target: DLQ_TARGET,
                        workflow = %self.workflow_id,
                        error = %e,
                        "dead letter replay aborted"
                    );
                    report.error = Some(e.to_string());
                    return;
                }
            };
            window = retained.len();
            // A purge discards the window without decoding it: a row this
            // envelope cannot read is exactly what a purge is for, and
            // decoding first would leave a store holding one foreign row
            // with no way to empty it.
            if trigger == DlqTrigger::Purge {
                report.purged += envelopes.num_rows() as u64;
                continue;
            }
            for row in 0..envelopes.num_rows() {
                let letter = match Letter::from_row(&envelopes, row) {
                    Ok(letter) => letter,
                    Err(e) => {
                        retained.truncate(window);
                        report.error = Some(e.to_string());
                        return;
                    }
                };
                if !filter.admits(&letter) {
                    retained.push(letter);
                    continue;
                }
                let Some(index) = ids.iter().position(|id| *id == letter.sink) else {
                    if !warned_unknown {
                        warned_unknown = true;
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            target: DLQ_TARGET,
                            workflow = %self.workflow_id,
                            sink = %letter.sink,
                            "a dead letter names a sink this workflow no longer declares; \
                             purge the queue to discard it"
                        );
                    }
                    retained.push(letter);
                    continue;
                };
                if refused.contains(&index) {
                    retained.push(letter);
                    continue;
                }
                let Some(sink) = sinks[index].as_mut() else {
                    retained.push(letter);
                    continue;
                };
                let batch = match letter.payload_batch(sink.schema(), &self.ipc) {
                    Ok(batch) => batch,
                    Err(e) => {
                        // Written back with the decode failure as its reason,
                        // the way a refusal is, rather than ending the drain.
                        // The payload is in hand and this window is consumed
                        // by the next fetch, so the letter is stored once;
                        // returning here would leave its entry at the head of
                        // the store and block every letter behind it on this
                        // replay and on every later one, with a purge the only
                        // way out.
                        retained.push(Letter {
                            reason: reason_of(&e),
                            failed_at_unix_ms: now_unix_ms(),
                            replays: letter.replays.saturating_add(1),
                            ..letter
                        });
                        continue;
                    }
                };
                let rows = batch.num_rows() as u64;
                match sink.write_batch(&batch).await {
                    Ok(()) => {
                        report.delivered += 1;
                        node_stats[index].rows += rows;
                        node_stats[index].batches += 1;
                        stats.sink_batches_written += 1;
                        stats.dead_letters_replayed += 1;
                        crate::metrics::instruments().sink_write(&ids[index], rows);
                        crate::metrics::instruments().dlq_replayed(&ids[index]);
                    }
                    Err(e) => {
                        refused.push(index);
                        retained.push(Letter {
                            reason: reason_of(&e),
                            failed_at_unix_ms: now_unix_ms(),
                            replays: letter.replays.saturating_add(1),
                            ..letter
                        });
                    }
                }
            }
        }
    }

    /// Write every retained letter back, counting whatever the store refuses
    /// as lost.
    ///
    /// The first write the store refuses ends the loop: the record half is
    /// wrapped in its retry policy, so offering it the rest would pay that
    /// policy per letter inline in the pass, which is the stall `drain`'s
    /// refused-sink retirement exists to prevent. Every letter from there on
    /// is lost either way, because nothing is buffered in memory.
    async fn rewrite(&mut self, retained: &mut Vec<Letter>, report: &mut DlqReplayReport) {
        if retained.is_empty() {
            if self.store.exclusive {
                self.reopen_record_half();
            }
            return;
        }
        if !self.reopen_record_half() {
            report.lost += retained.len() as u64;
            for letter in retained.iter() {
                crate::metrics::instruments().dlq_lost(&letter.sink);
            }
            #[cfg(feature = "tracing")]
            tracing::error!(
                target: DLQ_TARGET,
                workflow = %self.workflow_id,
                letters = retained.len(),
                "dead letter store unavailable after a replay (letters lost)"
            );
            retained.clear();
            return;
        }
        let store = self.sink.as_mut().expect("the record half is open");
        let mut lost = Vec::new();
        let mut store_down_from = None;
        for (index, letter) in retained.iter().enumerate() {
            let envelope = match letter.to_envelope(&self.workflow_id) {
                Ok(envelope) => envelope,
                // One row that cannot be built says nothing about the store,
                // so the rest are still offered.
                Err(_e) => {
                    lost.push(index);
                    continue;
                }
            };
            if let Err(_e) = store.write_batch(&envelope).await {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    target: DLQ_TARGET,
                    workflow = %self.workflow_id,
                    sink = %letter.sink,
                    letters = retained.len() - index,
                    error = %_e,
                    "dead letter store refused a write-back (the rest are lost too)"
                );
                store_down_from = Some(index);
                break;
            }
        }
        if let Some(from) = store_down_from {
            // The instance proved unwritable, so it goes before the next
            // rebuild, exactly as `record` drops one.
            self.sink = None;
            self.schedule_record_retry();
            // Ascending and disjoint from the `to_envelope` indices, which
            // are all below the letter that broke the loop.
            lost.extend(from..retained.len());
        }
        for &index in lost.iter().rev() {
            let letter = retained.remove(index);
            report.lost += 1;
            crate::metrics::instruments().dlq_lost(&letter.sink);
        }
    }

    /// Finalise and drop the recording half, for a store that holds itself
    /// exclusively.
    async fn close_record_half(&mut self) {
        if let Some(sink) = self.sink.as_mut()
            && let Err(_e) = sink.finish().await
        {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: DLQ_TARGET,
                workflow = %self.workflow_id,
                error = %_e,
                "dead letter store finish error before a replay"
            );
        }
        self.sink = None;
    }

    /// Reopen the recording half at the end of a replay, whatever its
    /// backoff says.
    ///
    /// The backoff belongs to the record path, where a store that just
    /// refused a write is unlikely to take the next one either. A replay
    /// arrives here having closed that half itself and, on an exclusive
    /// store, having just read the same file through its source half, so any
    /// pending backoff describes a store that is no longer down. Honouring it
    /// would count every retained letter lost and leave the queue unable to
    /// record for the rest of the step.
    fn reopen_record_half(&mut self) -> bool {
        if self.sink.is_some() {
            return true;
        }
        self.sink_attempt = 0;
        self.sink_down_until = None;
        self.rebuild_record_half()
    }

    /// Rebuild the recording half when its backoff allows, reporting whether
    /// it is open afterwards.
    fn rebuild_record_half(&mut self) -> bool {
        if self.sink.is_some() {
            return true;
        }
        if self
            .sink_down_until
            .is_some_and(|until| Instant::now() < until)
        {
            return false;
        }
        match self.sink_rebuilder.build_sink() {
            Ok(sink) => {
                self.sink = Some(sink);
                self.sink_attempt = 0;
                self.sink_down_until = None;
                true
            }
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    target: DLQ_TARGET,
                    workflow = %self.workflow_id,
                    error = %_e,
                    "dead letter store could not be opened"
                );
                self.schedule_record_retry();
                false
            }
        }
    }

    /// Hold the next rebuild of the recording half for one backoff step.
    fn schedule_record_retry(&mut self) {
        self.sink_attempt = self.sink_attempt.saturating_add(1);
        self.sink_down_until =
            Some(Instant::now() + HealSettings::default().delay_for(self.sink_attempt));
    }

    /// Count one letter the store would not take. The batch is gone: nothing
    /// is buffered in memory waiting for the store to come back.
    fn count_lost(&self, sink: &str) {
        crate::metrics::instruments().dlq_lost(sink);
    }

    /// Put the schedule one backoff step out when nothing is scheduled.
    fn schedule_auto_replay(&mut self) {
        if self.next_auto_replay.is_none() {
            self.auto_attempt = 0;
            self.next_auto_replay = Some(Instant::now() + HealSettings::default().delay_for(1));
        }
    }

    /// Fold one newly recorded letter into the published summary.
    fn add_to_summary(&mut self, letter: &Letter) {
        let mut summary = self.shared.summary();
        summary.letters += 1;
        summary.rows += u64::from(letter.rows);
        match summary
            .groups
            .iter_mut()
            .find(|group| group.sink == letter.sink && group.reason == letter.reason)
        {
            Some(group) => {
                group.letters += 1;
                group.rows += u64::from(letter.rows);
                group.last_failed_at_unix_ms = letter.failed_at_unix_ms;
                group.max_replays = group.max_replays.max(letter.replays);
            }
            None => summary.groups.push(DlqGroup {
                sink: letter.sink.clone(),
                reason: letter.reason.clone(),
                letters: 1,
                rows: u64::from(letter.rows),
                first_failed_at_unix_ms: letter.failed_at_unix_ms,
                last_failed_at_unix_ms: letter.failed_at_unix_ms,
                max_replays: letter.replays,
            }),
        }
        self.publish(summary);
    }

    /// Recompute the summary from what a replay left behind.
    fn regroup(&mut self, retained: &[Letter], known: bool, last_replay: Option<DlqReplayReport>) {
        self.known |= known;
        let mut summary = self.shared.summary();
        summary.known = self.known;
        summary.letters = retained.len() as u64;
        summary.rows = retained.iter().map(|l| u64::from(l.rows)).sum();
        summary.groups = group(retained);
        if last_replay.is_some() {
            summary.last_replay = last_replay;
        }
        self.publish(summary);
    }

    /// Publish the last summary, optionally replacing the replay report.
    fn publish_summary(&mut self, last_replay: Option<DlqReplayReport>) {
        let mut summary = self.shared.summary();
        if last_replay.is_some() {
            summary.last_replay = last_replay;
        }
        self.publish(summary);
    }

    /// Stamp the schedule and the pending flag onto `summary` and publish it.
    fn publish(&self, mut summary: DlqSummary) {
        summary.next_auto_replay_unix_ms = self.next_auto_replay.map(|due| {
            let remaining = due.saturating_duration_since(Instant::now());
            now_unix_ms() + remaining.as_millis() as u64
        });
        summary.replay_pending = self.shared.has_request();
        crate::metrics::instruments().dlq_pending(&self.workflow_id, summary.letters);
        self.shared.publish(summary);
    }
}

/// Group letters by `(sink, reason)`, in first-seen order.
fn group(letters: &[Letter]) -> Vec<DlqGroup> {
    let mut groups: Vec<DlqGroup> = Vec::new();
    for letter in letters {
        match groups
            .iter_mut()
            .find(|group| group.sink == letter.sink && group.reason == letter.reason)
        {
            Some(group) => {
                group.letters += 1;
                group.rows += u64::from(letter.rows);
                group.first_failed_at_unix_ms =
                    group.first_failed_at_unix_ms.min(letter.failed_at_unix_ms);
                group.last_failed_at_unix_ms =
                    group.last_failed_at_unix_ms.max(letter.failed_at_unix_ms);
                group.max_replays = group.max_replays.max(letter.replays);
            }
            None => groups.push(DlqGroup {
                sink: letter.sink.clone(),
                reason: letter.reason.clone(),
                letters: 1,
                rows: u64::from(letter.rows),
                first_failed_at_unix_ms: letter.failed_at_unix_ms,
                last_failed_at_unix_ms: letter.failed_at_unix_ms,
                max_replays: letter.replays,
            }),
        }
    }
    groups
}

/// The keys the layer supplies per half, when the block did not.
struct InjectedKeys {
    source: Vec<(&'static str, ConfigValue)>,
    sink: Vec<(&'static str, ConfigValue)>,
}

/// What each store needs that the user has no reason to write out.
fn injected_keys(store: &DlqStoreKind, data_dir: &Path, workflow_id: &str) -> InjectedKeys {
    match store.id {
        "redb" => {
            let directory = ConfigValue::from(data_dir.join("dlq").to_string_lossy().into_owned());
            let file = ConfigValue::from(format!("{workflow_id}.redb"));
            let table = ConfigValue::from("dead_letters");
            InjectedKeys {
                source: vec![
                    ("directory", directory.clone()),
                    ("file", file.clone()),
                    ("table", table.clone()),
                    // The store is a queue: a replayed letter is gone once
                    // the drain that read it finished.
                    ("consume", ConfigValue::Bool(true)),
                ],
                sink: vec![("directory", directory), ("file", file), ("table", table)],
            }
        }
        // A broker's source half must reach EOF once it is caught up, or a
        // replay would never end.
        _ => InjectedKeys {
            source: vec![("stop_at_end", ConfigValue::Bool(true))],
            sink: Vec::new(),
        },
    }
}

/// One half's config: the block's shared keys, that half's own keys over
/// them, the injected defaults where the key is still absent, and the
/// envelope schema, which is never the user's to set.
pub(crate) fn half_config(
    block: &DlqBlock,
    half: &ConfigValue,
    injected: &[(&str, ConfigValue)],
) -> ConfigValue {
    let mut config = block.config.clone();
    if let ConfigValue::Object(own) = half {
        for (key, value) in own {
            config.insert(key.clone(), value.clone());
        }
    }
    for (key, value) in injected {
        if !config.contains_key(*key) {
            config.insert((*key).to_string(), value.clone());
        }
    }
    config.insert("schema_fields".to_string(), envelope_schema_fields());
    ConfigValue::Object(config)
}

/// Refuse a store block this layer cannot honour.
///
/// Only NATS has such a rule: core NATS is at-most-once with no subscriber
/// queue, so a letter published with nothing listening is gone, which is the
/// one thing a dead letter store may not do.
fn validate_store_block(
    workflow_id: &str,
    store: &DlqStoreKind,
    block: &DlqBlock,
) -> SaciResult<()> {
    if store.id != "nats" {
        return Ok(());
    }
    for (half, own) in [("source", &block.source), ("sink", &block.sink)] {
        // The half's own `mode` wins over a shared one, exactly as
        // `half_config` merges them.
        let Some(mode) = own.get("mode").or_else(|| block.config.get("mode")) else {
            return Err(SaciError::configuration(format!(
                "workflow '{workflow_id}' dlq \"nats\": {half} needs a mode block"
            )));
        };
        if mode.get("kind").and_then(ConfigValue::as_str) != Some("jetstream") {
            return Err(SaciError::configuration(format!(
                "workflow '{workflow_id}' dlq \"nats\": {half} mode kind must be \
                 \"jetstream\"; core NATS is at-most-once and drops a letter published \
                 with no subscriber"
            )));
        }
    }
    Ok(())
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};

    fn payload_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("blob", DataType::Binary, true),
        ]))
    }

    fn payload_batch() -> RecordBatch {
        RecordBatch::try_new(
            payload_schema(),
            vec![
                Arc::new(Int64Array::from(vec![7i64, 9])),
                Arc::new(BinaryArray::from(vec![
                    Some(b"one".as_slice()),
                    Some(b"two".as_slice()),
                ])),
            ],
        )
        .expect("payload batch")
    }

    fn letter(sink: &str, reason: &str, rows: u32, replays: u32, at: u64) -> Letter {
        Letter {
            sink: sink.to_string(),
            component: "Order".to_string(),
            reason: reason.to_string(),
            failed_at_unix_ms: at,
            replays,
            rows,
            payload: Vec::new(),
        }
    }

    #[test]
    fn an_envelope_round_trips_a_batch_carrying_binary_columns() {
        let ipc = ArrowIpcTransformer::new();
        let original = payload_batch();
        let letter = Letter::from_failure(
            &original,
            "orders_out",
            "Order",
            &SaciError::generic("the sink is down"),
            &ipc,
        )
        .expect("encode");
        let envelope = letter.to_envelope("orders").expect("envelope");

        assert_eq!(envelope.num_rows(), 1);
        assert_eq!(envelope.schema(), envelope_schema());

        let decoded = Letter::from_row(&envelope, 0).expect("decode row");
        assert_eq!(decoded, letter);
        assert_eq!(decoded.rows, 2);
        assert_eq!(decoded.reason, "the sink is down");

        let back = decoded
            .payload_batch(payload_schema(), &ipc)
            .expect("decode payload");
        assert_eq!(back, original);
    }

    #[test]
    fn a_retry_exhausted_error_reports_the_failure_underneath_it() {
        let inner = SaciError::generic("connection refused");
        let wrapped = SaciError::retry_exhausted(SaciError::retry_exhausted(inner.clone(), 3), 2);
        assert_eq!(reason_of(&wrapped), "connection refused");
        assert!(
            !reason_of(&wrapped).contains("attempt"),
            "the varying attempt count must not reach the grouping key"
        );
        assert!(
            !reason_of(&inner).starts_with("Error:"),
            "the variant prefix says nothing a grouping key needs"
        );
    }

    #[test]
    fn a_half_block_wins_over_the_shared_keys_and_the_injected_defaults() {
        let block = DlqBlock {
            store: "redb".to_string(),
            replay: DlqReplayPoint::default(),
            source: serde_json::json!({ "table": "from_source", "check_integrity": true }),
            sink: ConfigValue::Object(ConfigMap::new()),
            config: match serde_json::json!({ "table": "shared", "directory": "/shared" }) {
                ConfigValue::Object(map) => map,
                _ => unreachable!("a literal object"),
            },
        };
        let injected = [
            ("table", ConfigValue::from("injected")),
            ("consume", ConfigValue::Bool(true)),
        ];
        let config = half_config(&block, &block.source, &injected);

        assert_eq!(config["table"], ConfigValue::from("from_source"));
        assert_eq!(config["directory"], ConfigValue::from("/shared"));
        assert_eq!(config["check_integrity"], ConfigValue::Bool(true));
        assert_eq!(config["consume"], ConfigValue::Bool(true));
        assert_eq!(config["schema_fields"], envelope_schema_fields());
    }

    #[test]
    fn a_declared_schema_fields_is_replaced_by_the_envelope() {
        let block = DlqBlock {
            config: match serde_json::json!({ "schema_fields": "nonsense" }) {
                ConfigValue::Object(map) => map,
                _ => unreachable!("a literal object"),
            },
            ..DlqBlock::default()
        };
        let config = half_config(&block, &block.sink, &[]);
        assert_eq!(config["schema_fields"], envelope_schema_fields());
    }

    #[test]
    fn letters_group_by_sink_and_reason() {
        let groups = group(&[
            letter("out", "down", 2, 0, 100),
            letter("out", "down", 3, 4, 300),
            letter("other", "down", 1, 0, 200),
        ]);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].sink, "out");
        assert_eq!(groups[0].letters, 2);
        assert_eq!(groups[0].rows, 5);
        assert_eq!(groups[0].first_failed_at_unix_ms, 100);
        assert_eq!(groups[0].last_failed_at_unix_ms, 300);
        assert_eq!(groups[0].max_replays, 4);
        assert_eq!(groups[1].sink, "other");
        assert_eq!(groups[1].letters, 1);
    }

    #[test]
    fn every_store_in_the_table_is_named_by_store_names() {
        let names = store_names();
        for kind in DLQ_STORES {
            assert!(names.contains(kind.id), "{names} omits {}", kind.id);
            assert_eq!(store_kind(kind.id).map(|k| k.id), Some(kind.id));
        }
        assert!(store_kind("mongodb").is_none());
    }

    /// Both halves are built at load time, so a key only one of them refuses
    /// is a startup failure rather than something the first replay finds.
    ///
    /// The source half is the one that would otherwise slip through: nothing
    /// builds it until a replay runs, and a replay reports its error into
    /// `last_replay` instead of failing the run.
    #[cfg(feature = "connector-redb")]
    #[test]
    fn a_key_only_one_half_refuses_fails_the_build() {
        use crate::service::registry::Registry as R;

        let mut registry = R::new();
        registry.register_sink(saci_connector_redb::RedbSinkFactory);
        registry.register_source(saci_connector_redb::RedbSourceFactory);
        let registry = Arc::new(registry);
        let dir = tempfile::tempdir().expect("tempdir");

        let refuse = |block: DlqBlock| {
            let spec = spec_with(block.clone());
            DeadLetterQueue::build(
                &spec,
                dir.path(),
                &registry,
                Arc::new(DlqShared::new(&spec.id, &block)),
            )
            .expect_err("the half refuses the key")
            .to_string()
        };

        // `consume` is a source key, so the sink half refuses it.
        let err = refuse(DlqBlock {
            sink: serde_json::json!({ "consume": true }),
            ..DlqBlock::default()
        });
        assert!(err.contains("RedbSink config"), "got: {err}");
        assert!(err.contains("consume"), "got: {err}");

        // `compact` is a sink key, so the source half refuses it.
        let err = refuse(DlqBlock {
            source: serde_json::json!({ "compact": false }),
            ..DlqBlock::default()
        });
        assert!(err.contains("RedbSource config"), "got: {err}");
        assert!(err.contains("compact"), "got: {err}");
    }

    /// A workflow declaring a store whose connector this binary does not
    /// carry must hear which `--features` flag supplies it, the same answer a
    /// `sink` node naming `RedbSink` gets. This is what
    /// `--no-default-features --features service` reaches, where no connector
    /// is registered at all.
    #[test]
    fn a_store_this_build_carries_no_connector_for_names_its_feature() {
        let spec = spec_with(DlqBlock::default());
        let err = DeadLetterQueue::build(
            &spec,
            std::path::Path::new("/tmp/saci-dlq-missing"),
            &Arc::new(Registry::new()),
            Arc::new(DlqShared::new(&spec.id, &DlqBlock::default())),
        )
        .expect_err("an empty registry supplies neither half");
        let message = err.to_string();
        assert!(message.contains("RedbSource"), "got: {message}");
        assert!(
            message.contains("--features connector-redb"),
            "the refusal must name the flag that supplies it: {message}"
        );
        assert!(message.contains("orders/dlq"), "got: {message}");
    }

    #[test]
    fn a_nats_store_is_refused_unless_both_halves_are_jetstream() {
        let jetstream = serde_json::json!({ "mode": { "kind": "jetstream" } });

        let no_mode = DlqBlock {
            store: "nats".to_string(),
            ..DlqBlock::default()
        };
        let err = build_err(&no_mode);
        assert!(err.contains("source needs a mode block"), "got: {err}");

        let core_source = DlqBlock {
            store: "nats".to_string(),
            source: serde_json::json!({ "mode": { "kind": "core" } }),
            sink: jetstream.clone(),
            ..DlqBlock::default()
        };
        let err = build_err(&core_source);
        assert!(
            err.contains("source mode kind must be \"jetstream\"") && err.contains("at-most-once"),
            "got: {err}"
        );

        let core_sink = DlqBlock {
            store: "nats".to_string(),
            source: jetstream,
            sink: serde_json::json!({ "mode": { "kind": "core" } }),
            ..DlqBlock::default()
        };
        let err = build_err(&core_sink);
        assert!(
            err.contains("sink mode kind must be \"jetstream\""),
            "got: {err}"
        );
    }

    /// A sink node's stand-in, so a replay has somewhere to deliver to.
    #[cfg(feature = "connector-redb")]
    struct TestSink {
        /// Rows this sink accepted.
        delivered: Arc<std::sync::atomic::AtomicU64>,
        /// Calls to `write_batch`, accepted or not.
        attempts: Arc<std::sync::atomic::AtomicU64>,
        /// Refuse every write, the way a sink that is still down does.
        refuse: bool,
        /// Cancelled from inside the first write, which is how a test stops a
        /// drain part way through a window.
        cancel_on_write: Option<CancellationToken>,
    }

    #[cfg(feature = "connector-redb")]
    impl TestSink {
        fn accepting(delivered: &Arc<std::sync::atomic::AtomicU64>) -> Self {
            Self {
                delivered: Arc::clone(delivered),
                attempts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                refuse: false,
                cancel_on_write: None,
            }
        }

        fn refusing() -> Self {
            Self {
                delivered: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                attempts: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                refuse: true,
                cancel_on_write: None,
            }
        }

        /// Refuses every write and counts the calls into `attempts`.
        fn refusing_counted(attempts: &Arc<std::sync::atomic::AtomicU64>) -> Self {
            Self {
                attempts: Arc::clone(attempts),
                ..Self::refusing()
            }
        }
    }

    #[cfg(feature = "connector-redb")]
    #[async_trait::async_trait]
    impl Sink for TestSink {
        async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
            self.attempts.fetch_add(1, Ordering::Relaxed);
            if let Some(cancel) = self.cancel_on_write.take() {
                cancel.cancel();
            }
            if self.refuse {
                return Err(SaciError::generic("the sink is down"));
            }
            self.delivered
                .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            Ok(())
        }

        async fn finish(&mut self) -> Result<(), SaciError> {
            Ok(())
        }

        fn schema(&self) -> Arc<Schema> {
            payload_schema()
        }
    }

    /// The one sink node a replay sees, plus the counters it writes into.
    #[cfg(feature = "connector-redb")]
    struct Nodes {
        sinks: Vec<Option<Box<dyn Sink>>>,
        ids: Vec<String>,
        recovered: Vec<Option<Arc<AtomicBool>>>,
        node_stats: Vec<NodeRunStats>,
        stats: StandaloneStats,
        cancel: CancellationToken,
    }

    #[cfg(feature = "connector-redb")]
    impl Nodes {
        fn new(cancel: CancellationToken, sink: TestSink) -> Self {
            Self {
                sinks: vec![Some(Box::new(sink))],
                ids: vec!["out".to_string()],
                recovered: vec![None],
                node_stats: vec![NodeRunStats::default()],
                stats: StandaloneStats::default(),
                cancel,
            }
        }

        fn ctx(&mut self) -> ReplayCtx<'_> {
            ReplayCtx {
                sinks: &mut self.sinks,
                ids: &self.ids,
                recovered: &self.recovered,
                node_stats: &mut self.node_stats,
                stats: &mut self.stats,
                cancel: &self.cancel,
            }
        }
    }

    /// A queue over a real redb store under `dir`.
    #[cfg(feature = "connector-redb")]
    fn redb_queue(
        dir: &Path,
        shared: &Arc<DlqShared>,
        block: DlqBlock,
    ) -> (DeadLetterQueue, StandaloneStats) {
        let mut registry = Registry::new();
        registry.register_sink(saci_connector_redb::RedbSinkFactory);
        registry.register_source(saci_connector_redb::RedbSourceFactory);
        let spec = spec_with(block);
        let queue = DeadLetterQueue::build(&spec, dir, &Arc::new(registry), Arc::clone(shared))
            .expect("the redb store opens");
        (queue, StandaloneStats::default())
    }

    /// Record one letter for sink `out`, carrying `batch`.
    #[cfg(feature = "connector-redb")]
    async fn record_batch(
        queue: &mut DeadLetterQueue,
        stats: &mut StandaloneStats,
        batch: &RecordBatch,
    ) {
        queue
            .record(
                batch,
                "out",
                "Order",
                &SaciError::generic("the sink is down"),
                stats,
            )
            .await;
    }

    /// Record one two-row letter for sink `out`.
    #[cfg(feature = "connector-redb")]
    async fn record_one(queue: &mut DeadLetterQueue, stats: &mut StandaloneStats) {
        record_batch(queue, stats, &payload_batch()).await;
    }

    /// A letter whose payload no longer decodes against its sink's schema is
    /// written back with the decode failure as its reason.
    ///
    /// Ending the drain there would leave its entry at the head of the store,
    /// so every letter behind it would stay undelivered on this replay and on
    /// every later one, with a purge of the whole queue the only way out.
    #[cfg(feature = "connector-redb")]
    #[tokio::test]
    async fn an_undecodable_payload_does_not_block_the_letters_behind_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shared = Arc::new(DlqShared::new("orders", &DlqBlock::default()));
        let (mut queue, mut stats) = redb_queue(dir.path(), &shared, DlqBlock::default());

        // Recorded first, so the store hands it over first: a payload missing
        // a column the sink's schema requires, which is what a sink whose
        // schema widened after the letter was stored looks like.
        let narrow = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let missing_column =
            RecordBatch::try_new(narrow, vec![Arc::new(Int64Array::from(vec![1i64]))])
                .expect("a one-column batch");
        record_batch(&mut queue, &mut stats, &missing_column).await;
        record_one(&mut queue, &mut stats).await;

        let delivered = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut nodes = Nodes::new(CancellationToken::new(), TestSink::accepting(&delivered));
        queue.at_head(nodes.ctx()).await;
        queue.finish().await;

        let summary = shared.summary();
        let report = summary.last_replay.as_ref().expect("the replay ran");
        assert_eq!(report.error, None, "one bad payload is not a failed drain");
        assert_eq!(report.delivered, 1, "the letter behind it went out");
        assert_eq!(delivered.load(Ordering::Relaxed), 2);
        assert_eq!(report.retained, 1);
        assert_eq!(summary.groups.len(), 1);
        assert!(
            summary.groups[0].reason.contains("arrow-ipc"),
            "the group key is the decode failure: {:?}",
            summary.groups[0].reason
        );
    }

    /// The first write-back the store refuses ends the loop and retires the
    /// record half.
    ///
    /// The half is wrapped in `RetryConfig::default()`, four attempts over
    /// 100, 200 and 400 ms, so offering it every remaining letter would sleep
    /// most of a second per letter inline in the pass: a thousand retained
    /// letters against a down store is the same minutes-long stall `drain`
    /// retires a refusing sink to avoid. Leaving the proven-unwritable
    /// instance in place would also make the next `record` pay it again,
    /// where `record` itself drops one on the first failure.
    #[cfg(feature = "connector-redb")]
    #[tokio::test]
    async fn a_refused_write_back_retires_the_record_half_instead_of_retrying_per_letter() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shared = Arc::new(DlqShared::new("orders", &DlqBlock::default()));
        let (mut queue, _) = redb_queue(dir.path(), &shared, DlqBlock::default());

        // Stand in for a record half that opened and then went down, which is
        // what `rewrite` finds after a drain whose sink refused.
        let attempts = Arc::new(std::sync::atomic::AtomicU64::new(0));
        queue.sink = Some(Box::new(TestSink::refusing_counted(&attempts)));

        let mut retained = vec![
            letter("out", "the sink is down", 2, 0, 100),
            letter("other", "the sink is down", 1, 0, 200),
        ];
        let mut report = DlqReplayReport {
            trigger: DlqTrigger::Manual,
            started_at_unix_ms: 0,
            duration_ms: 0,
            delivered: 0,
            retained: 0,
            purged: 0,
            lost: 0,
            error: None,
        };
        queue.rewrite(&mut retained, &mut report).await;

        assert_eq!(
            attempts.load(Ordering::Relaxed),
            1,
            "the second letter must not pay the retry policy against a store \
             that just proved unwritable"
        );
        assert_eq!(report.lost, 2, "neither letter is buffered anywhere");
        assert!(retained.is_empty());
        assert!(queue.sink.is_none(), "the unwritable instance is dropped");
        assert!(
            queue.sink_down_until.is_some(),
            "the next rebuild waits out a backoff step"
        );
    }

    /// A backoff the record path left behind must not survive into the replay
    /// that closed the record half itself.
    ///
    /// The source half has just read the same file through, so the store is
    /// demonstrably openable; honouring the timer would count every retained
    /// letter lost and leave the queue unable to record for the rest of it.
    #[cfg(feature = "connector-redb")]
    #[tokio::test]
    async fn a_stale_record_backoff_does_not_lose_the_letters_a_replay_retained() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shared = Arc::new(DlqShared::new("orders", &DlqBlock::default()));
        let (mut queue, mut stats) = redb_queue(dir.path(), &shared, DlqBlock::default());
        record_one(&mut queue, &mut stats).await;
        assert_eq!(stats.dead_letters_recorded, 1);

        // What a refused record write leaves behind: the half dropped and its
        // rebuild held off for one backoff step.
        queue.sink_attempt = 3;
        queue.sink_down_until = Some(Instant::now() + std::time::Duration::from_secs(600));

        let mut nodes = Nodes::new(CancellationToken::new(), TestSink::refusing());
        queue.at_head(nodes.ctx()).await;

        let report = shared
            .summary()
            .last_replay
            .expect("the startup replay ran");
        assert_eq!(report.lost, 0, "the store is open to the source half");
        assert_eq!(report.retained, 1);

        // The letter really is back in the store: a replay against a sink
        // that takes it delivers it.
        let delivered = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut nodes = Nodes::new(CancellationToken::new(), TestSink::accepting(&delivered));
        let _receiver = shared
            .request(DlqTrigger::Manual, ReplayFilter::default())
            .expect("no other request is queued");
        queue.at_head(nodes.ctx()).await;

        assert_eq!(
            delivered.load(Ordering::Relaxed),
            2,
            "the letter's two rows"
        );
        assert_eq!(shared.summary().letters, 0);
    }

    /// A drain that stops early leaves the window it just read in the store,
    /// because every source half consumes a window at the head of the next
    /// fetch. Writing those letters back as well would store them twice.
    #[cfg(feature = "connector-redb")]
    #[tokio::test]
    async fn an_interrupted_replay_leaves_each_letter_in_the_store_exactly_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let shared = Arc::new(DlqShared::new("orders", &DlqBlock::default()));
        let (mut queue, mut stats) = redb_queue(dir.path(), &shared, DlqBlock::default());
        record_one(&mut queue, &mut stats).await;
        record_one(&mut queue, &mut stats).await;
        assert_eq!(stats.dead_letters_recorded, 2);

        let delivered = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let cancel = CancellationToken::new();
        let mut nodes = Nodes::new(
            cancel.clone(),
            TestSink {
                delivered: Arc::clone(&delivered),
                cancel_on_write: Some(cancel),
                ..TestSink::refusing()
            },
        );
        queue.at_head(nodes.ctx()).await;
        queue.finish().await;

        let report = shared
            .summary()
            .last_replay
            .expect("the startup replay ran");
        assert_eq!(report.error.as_deref(), Some("cancelled"));
        assert_eq!(
            report.retained, 0,
            "the window that letter came out of is still in the store"
        );

        // A second replay, over the same file, finds two letters and no more.
        let (mut queue, _) = redb_queue(dir.path(), &shared, DlqBlock::default());
        let mut nodes = Nodes::new(CancellationToken::new(), TestSink::accepting(&delivered));
        queue.at_head(nodes.ctx()).await;
        queue.finish().await;

        assert_eq!(
            delivered.load(Ordering::Relaxed),
            4,
            "two letters of two rows each, delivered once apiece"
        );
        assert_eq!(shared.summary().letters, 0);

        // And nothing is left behind for a third.
        let (mut queue, _) = redb_queue(dir.path(), &shared, DlqBlock::default());
        let mut nodes = Nodes::new(CancellationToken::new(), TestSink::accepting(&delivered));
        queue.at_head(nodes.ctx()).await;
        queue.finish().await;
        assert_eq!(delivered.load(Ordering::Relaxed), 4, "the store is empty");
    }

    /// A replay that errored with nothing in hand must put itself back on the
    /// schedule. It learnt nothing about the store, so `known` stays false and
    /// no other trigger would bring the queue back: whatever an earlier run
    /// left behind would sit there until someone asked for it by hand.
    #[cfg(feature = "connector-redb")]
    #[tokio::test]
    async fn a_replay_whose_source_half_failed_schedules_its_own_retry() {
        let dir = tempfile::tempdir().expect("tempdir");
        // The sink half keeps the injected file; the source half opens one
        // that is not there, which is a `next_batch` failure from here.
        let block = DlqBlock {
            source: serde_json::json!({ "file": "absent.redb" }),
            ..DlqBlock::default()
        };
        let shared = Arc::new(DlqShared::new("orders", &block));
        let (mut queue, _) = redb_queue(dir.path(), &shared, block);

        let delivered = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut nodes = Nodes::new(CancellationToken::new(), TestSink::accepting(&delivered));
        queue.at_head(nodes.ctx()).await;

        let summary = shared.summary();
        let report = summary.last_replay.as_ref().expect("the replay ran");
        assert!(
            report.error.is_some(),
            "the source half cannot open a file that is not there"
        );
        assert_eq!(report.retained, 0);
        assert!(
            summary.next_auto_replay_unix_ms.is_some(),
            "an errored replay is the one that most needs another"
        );
        assert!(!summary.known, "an errored drain read nothing through");
    }

    /// One stored row this queue cannot read must not make the store
    /// unpurgeable: a purge is exactly the answer to a row it cannot read.
    #[cfg(feature = "connector-redb")]
    #[tokio::test]
    async fn a_purge_discards_a_row_the_envelope_cannot_read() {
        struct OneBatch(Option<RecordBatch>);

        #[async_trait::async_trait]
        impl Source for OneBatch {
            fn schema(&self) -> Arc<Schema> {
                envelope_schema()
            }

            async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
                Ok(self.0.take())
            }
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let shared = Arc::new(DlqShared::new("orders", &DlqBlock::default()));
        let (mut queue, _) = redb_queue(dir.path(), &shared, DlqBlock::default());

        // What a store holding rows some other writer put there looks like.
        let foreign = RecordBatch::try_from_iter(vec![(
            "junk",
            Arc::new(StringArray::from(vec!["?"])) as arrow_array::ArrayRef,
        )])
        .expect("a one-column batch");
        let mut source = OneBatch(Some(foreign));

        let delivered = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut nodes = Nodes::new(CancellationToken::new(), TestSink::accepting(&delivered));
        let mut retained = Vec::new();
        let mut report = DlqReplayReport {
            trigger: DlqTrigger::Purge,
            started_at_unix_ms: 0,
            duration_ms: 0,
            delivered: 0,
            retained: 0,
            purged: 0,
            lost: 0,
            error: None,
        };
        queue
            .drain(
                &mut source,
                &ReplayFilter::default(),
                DlqTrigger::Purge,
                &mut retained,
                &mut report,
                nodes.ctx(),
            )
            .await;
        queue.finish().await;

        assert_eq!(report.purged, 1);
        assert_eq!(report.error, None);
        assert!(retained.is_empty());
    }

    /// A workflow whose only declaration is `block`.
    fn spec_with(block: DlqBlock) -> WorkflowSpec {
        WorkflowSpec {
            id: "orders".to_string(),
            name: None,
            transformers: Vec::new(),
            sources: Vec::new(),
            wasm: Vec::new(),
            plugin: Vec::new(),
            sinks: Vec::new(),
            links: Vec::new(),
            dlq: Some(crate::service::config::DlqConfig(block)),
        }
    }

    /// The refusal `block` earns against an empty registry.
    fn build_err(block: &DlqBlock) -> String {
        let spec = spec_with(block.clone());
        DeadLetterQueue::build(
            &spec,
            std::path::Path::new("/tmp/saci-dlq-missing"),
            &Arc::new(Registry::new()),
            Arc::new(DlqShared::new(&spec.id, block)),
        )
        .expect_err("the block is refused")
        .to_string()
    }
}
