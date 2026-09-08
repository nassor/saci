//! Harness for the connector/transformer/processor test matrix.
//!
//! One [`Case`] is one `{source connector, sink connector, byte format,
//! processor runtime}` tuple. [`run_case`] turns it into a KDL config, loads it
//! through [`ServiceConfig::load`], assembles it with
//! [`ServiceBuilder::build_all`], runs it through the real standalone or stream
//! runner, and asserts the exact rows arrive at the sink. [`Report`] collects
//! every case's outcome and renders the matrix.
//!
//! # Dimensions
//!
//! - **Connectors** (both ends): `channel`, `file`, `http`, `tcp`, `kafka`,
//!   `nats`, `postgresql`, `redb`, `s3`, `turso`. `datafusion` is excluded on
//!   both ends:
//!   `saci-connector-datafusion` registers no `SourceFactory`, so no `type` name
//!   reaches it from a config, and `DataFusionSource` needs a live
//!   `SessionContext` handed to it in Rust. It is unreachable from the config
//!   path this matrix exercises.
//! - **Byte formats**: `arrow-ipc`, `avro`, `csv`, `ndjson`, `parquet`, plus
//!   [`Format::None`] for the three connectors that carry `RecordBatch`es
//!   natively (`channel`, `postgresql`, `turso`).
//! - **Processor runtimes**: a native [`Pipeline`] registered through
//!   [`ServiceBuilder::with_runtime`], the `saci-processor-smoketest`
//!   WebAssembly component loaded from `module`, and the
//!   `saci-plugin-smoketest` cdylib loaded from `library`.
//!
//! One format per case, applied to every byte-carrying end. The independent
//! source-format x sink-format cross product is not taken: it is 32400 cases
//! against 1800, and a mixed pair adds no connector or transformer coverage
//! over the two paired cases that already cover both halves.
//!
//! The transformer dimension is not reduced either: every one of the 1800
//! cases runs, so nothing is traded away to fit a time budget.
//!
//! # Row schema per processor
//!
//! [`validate_workflow_graph`] compares `fields()` field for field at both ends
//! of every link, so the row type is a function of the processor:
//!
//! | Processor | Component | Fields | Transform asserted |
//! |---|---|---|---|
//! | native `Pipeline` | `Order` | `id: Int64`, `label: Utf8`, `total: Float64` | `double_total` doubles `total` |
//! | WASM smoketest | `Ping` | `seq: UInt64` | identity |
//! | native plugin | `Counter` | `id: Int64`, `seen: Int64` | `seen = (total + row + 1) * 1` |
//!
//! `Ping`/`Counter` mirror what `crates/saci-processor-smoketest/src/lib.rs` and
//! `crates/saci-plugin-smoketest/src/lib.rs` declare.
//!
//! The plugin's own state accumulates across batches, and the expected
//! `1, 2, 3` holds under both batch shapes a case can produce. A `one_shot`
//! case delivers one batch of three rows, so `seen = (total + row + 1)` is
//! `1, 2, 3` off `total = 0`. A `tcp`-sourced case with a `PerRow` format
//! delivers three single-row batches instead — the tcp source flushes its
//! decoder once per frame (`saci-connector-tcp/src/source.rs`, `push` then
//! `flush`) — and `total` advancing `0, 1, 2` across them yields the same
//! `1, 2, 3`. Changing the seeded row count or the frame count breaks that
//! coincidence.
//!
//! # Capability table
//!
//! Which surface a connector drives, read off its own call sites:
//!
//! | Connector | Surface | Evidence |
//! |---|---|---|
//! | `channel`, `postgresql` | rows | neither crate mentions a transformer method |
//! | `file` | stream | `saci-connector-file/src/{source,sink}.rs` call `open_reader`/`open_writer` |
//! | `redb` | stream | `saci-connector-redb/src/{source,sink}.rs` spool one entry |
//! | `http` | stream | `saci-connector-http/src/{source,sink}.rs` spool a whole document |
//! | `s3` | stream | `saci-connector-s3/src/{source,sink}.rs` spool one object |
//! | `tcp` | message | `saci-connector-tcp/src/{source,sink}.rs` call `open_message_decoder`/`encode_messages`, the sink behind a `message_shape` gate |
//! | `kafka` | message | `saci-connector-kafka/src/{source,sink}.rs`, plus a `message_shape` gate |
//! | `nats` | message | `saci-connector-nats/src/{source,sink}.rs`, plus a `message_shape` gate |
//!
//! Which surface a format offers, taken from the methods each transformer
//! actually overrides (the [`Transformer`] contract defaults every unsupported
//! method to `saci_transformer::unsupported`):
//!
//! | Format | Stream | Message | `message_shape` |
//! |---|---|---|---|
//! | `arrow-ipc` | yes | yes | `PerBatch` |
//! | `avro` | yes | yes | `PerRow` |
//! | `csv` | yes | yes | `PerRow` |
//! | `ndjson` | yes | yes | `PerRow` |
//! | `parquet` | yes | yes | `PerBatch` |
//!
//! Every format offers both surfaces, so no case in this matrix is refused for
//! wanting a surface its format lacks. What remains is the type systems: the
//! text and container formats do not all carry every Arrow type the three row
//! schemas declare.
//!
//! Four further rules are properties of a *source* or of a format's own type
//! system rather than of the surfaces above, and each one was read out of the
//! code it constrains:
//!
//! - A stream source of a self-describing format lets the stream carry the
//!   schema, so most of those cases declare none: `arrow-ipc`, `avro` and
//!   `parquet` project onto a declared schema in `open_reader`, but a
//!   document's own schema is what these cases are here to exercise.
//!   `FileSource` passes `schema_fields` straight through, so it simply
//!   declares none. `S3Source` has `schema_from "object"` and `HttpSource`
//!   has `schema_from "body"`: both keep the declared schema for the link
//!   check, hand the format nothing, and compare the schema the stream turned
//!   out to carry against the declared one field for field. `RedbSource` is
//!   the exception: it requires `schema_fields` and always hands it to the
//!   format as a projection target, so every `redb` case declares one and
//!   every self-describing format is cast back to it.
//! - The Avro object container file has no unsigned integer type:
//!   `arrow-avro` writes `UInt64` as `long` and reads it back as `Int64`. So
//!   the WASM processor's `Ping.seq: UInt64` cannot survive an `avro` *stream*
//!   source that declares no schema, while its `avro` message cases
//!   round-trip unchanged, because the message decoder casts back to the
//!   declared schema. A schemaless stream read has no declaration to cast
//!   back to, so the file's own `Int64` is what travels: `FileSource`
//!   surfaces it as a build-time link mismatch, and the `http` and `s3`
//!   sources' `schema_from` cross-check surfaces it at drain time. A `redb`
//!   source declares one, so its own `avro` reads cast back and pass.
//! - `PgFieldType` (`saci-connector-postgresql/src/config.rs`) has no unsigned
//!   variant at all, so a `uint64` `schema_fields` entry is not a
//!   PostgreSQL config. Every `postgresql` x WASM case is rejected while
//!   parsing that node's `config`.
//! - CSV is text and carries no types, so the declared schema governs both
//!   ends and every column of all three row schemas round-trips exactly,
//!   `Ping.seq: UInt64` included. The one value it cannot carry is an empty
//!   string, which it writes as nothing and reads back as a null. No row
//!   fixture here holds one, so no `csv` case is refused for a type reason.
//!
//! # Where a refusal lands
//!
//! [`Expect::Rejected`] carries the site, because the three transports do not
//! agree on when they check:
//!
//! | Refusal | Site | Evidence |
//! |---|---|---|
//! | byte connector with no `transformer` key | build | `ConnectorContext::transformer` |
//! | `avro` + WASM on a `file` source | build | the container file's `Int64` disagrees with `Ping.seq` |
//! | `avro` + WASM on an `http`/`s3` source | run | the `schema_from` cross-check compares the stream's `Int64` against `Ping.seq` at drain time |
//! | `uint64` on a `postgresql` node | build | no `PgFieldType` variant |
//!
//! Every transformer implements all four transformer methods, so every byte
//! connector, on either end, has exactly one format it refuses,
//! [`Format::None`], and it refuses it while building. The `message_shape`
//! gates in `KafkaSink`, `NatsSink` and `TcpSink::connect`, and the decoder
//! `TcpIngestSource::new` opens, are all still live; no registered format
//! trips them.
//!
//! The order is the order the service checks in, and it decides which refusal
//! a case observes: every node is built in topological order (source before
//! sink), then `validate_workflow_graph` runs over the finished nodes, and only
//! then does the runner drain and write. So `file -> postgresql` with a WASM
//! processor reports the source's missing transformer, not the PostgreSQL
//! node's unsigned column.
//!
//! An `s3` source keeps its refusal under the default four attempts:
//! `S3Source::next_batch` peeks the front of its listing and pops a location
//! only once that object has been fully and successfully drained, so a retry
//! re-attempts the same object and raises the same error rather than finding
//! an empty listing and reporting EOF.
//!
//! [`StandaloneStats`] counts non-fatal errors without keeping their text, so
//! a [`Site::Run`] refusal that names a message fragment is asserted against a
//! second, direct build of the same source. See [`SourceProbe`].
//!
//! # Run modes
//!
//! `is_live_source` (`saci-service/src/service/config.rs`) makes only three
//! source types EOF-free: `tcp` unconditionally, `KafkaSource` without
//! `stop_at_end`/`compacted`, and `NatsSource` without `stop_at_end`. Every
//! case here sets `stop_at_end #true`, so `tcp` is the only source that forces
//! `run_mode kind="stream"`; everything else runs `one_shot`. An `http` source
//! reaches EOF after its single GET.
//!
//! A stream-mode case ends by cancellation, because a `tcp` source has no EOF
//! at all: its `rx` is closed only by dropping the source. The runner publishes
//! its live stats as part of an iteration rather than after it, so the last
//! item's sink write is never visible through them — the counters stop one item
//! short whatever the wait. The driver therefore waits for one item less than
//! the frame count and gives the last item a settle window. That window is wall
//! clock, and every case future is polled by one task (`PipelineRuntime` is
//! `?Send`, so no case can be spawned), so the `tcp`-sourced cases run as a
//! second phase, after the other 1008 have finished competing for the poll
//! loop. See [`Case::phases`].
//!
//! # Isolation
//!
//! One container per external resource for the whole run, behind a
//! [`OnceCell`]. Cases run concurrently under a [`Semaphore`] and isolate
//! themselves inside the shared resource: a nanosecond timestamp plus an atomic
//! counter names a unique Kafka topic, NATS stream and subject, PostgreSQL
//! table pair, S3 object prefix (one bucket for the run), channel name and temp
//! directory. Every TCP and HTTP listener binds `127.0.0.1:0`.
//!
//! A resource is gated per case, never globally: a case is [`Outcome::Skipped`]
//! only when it needs a live resource that is unavailable. A build-time refusal
//! needs no resource at all — nothing in this workspace connects while a
//! factory builds — so those cases run against a placeholder endpoint and keep
//! their coverage on a machine with no Docker daemon.
//!
//! # Two places the assertion is deliberately loose
//!
//! - **`Ping.seq` through an Avro stream sink.** The Avro object container file
//!   has no unsigned integer type, so the column comes back `Int64`. The values
//!   are still asserted; only that one type mapping, forced by the format, is
//!   allowed. Every other format returns `UInt64` and a widened column is a
//!   failure. See [`RowKind::render`].
//! - **A repeated HTTP document.** The engine's delivery contract is
//!   at-least-once and the HTTP client resends a request whose connection went
//!   away underneath it, so a document identical to the one before it counts
//!   once. The comparison is on decoded rows, not bytes, because an Avro
//!   container file carries a random sync marker.
//!
//! # What a green matrix does not claim
//!
//! Two properties are outside what any case here can observe, so a pass is
//! silent about both.
//!
//! - **The bytes a format writes.** [`read_back`] decodes a `file`, `http` or
//!   `tcp` sink through the same [`Transformer`] the sink wrote with, and
//!   reads a `kafka`, `nats` or `s3` sink back through the sibling source
//!   connector. Only the `postgresql` readback goes around the connector, in
//!   SQL. A change that corrupts a format symmetrically therefore cancels out:
//!   every case round-trips its rows while the file on disk, the object, the
//!   message and the frame are all in the wrong format. What a transformer
//!   emits is pinned by that transformer's own unit tests. This matrix pins
//!   that a connector pair can carry rows through it.
//! - **Where a build refusal went.** [`execute`] derives `build_only` from
//!   [`Case::expect`], so a [`Site::Build`] case resolves placeholder
//!   endpoints, seeds nothing and returns as soon as `build_all` succeeds. It
//!   reports that the refusal is gone and cannot distinguish a combination
//!   that is now supported from one that is now refused at run. The other
//!   direction is exact: a [`Site::Run`] case whose build fails reports the
//!   build message it did not expect.
//!
//! # What the config has to carry for a `channel` end
//!
//! `ServiceConfig::validate` pairs every channel name's `ChannelSink` with its
//! `ChannelSource` across the whole config, so a half held by the harness is
//! rejected outright. A `channel` source therefore comes with a second
//! workflow that feeds it from an ndjson file, and a `channel` sink with a
//! second workflow that drains it into one; those are the bytes the case is
//! seeded from and read back through. Both are declared in the same config and
//! run as their own `BuiltService` alongside the case's, exactly as
//! `channel_bridge.rs` does.
//!
//! # What the maximal workflow cannot do
//!
//! [`run_maximal`] declares every available source, all three processor
//! runtimes and every available sink in **one** stream-mode `WorkflowSpec`. It
//! cannot fan one source into all three processors: the three runtimes declare
//! three different components with three different schemas, and
//! `validate_workflow_graph` compares `fields()` on every link, so a source
//! carrying `Order` cannot feed the processor that declares `Ping`. Each
//! processor therefore owns its own sources and its own sinks inside the one
//! workflow. Every node is linked; nothing is declared for show.
//!
//! What it asserts: every source's rows were ingested
//! (`rows_processed == 3 x sources`), every sink holds exactly the rows of
//! every source feeding its processor and no others, and the run reports no
//! error. Each source's ids are offset by [`SOURCE_ID_STRIDE`], so a source
//! that silently delivered nothing fails the sink comparison even when every
//! other source's rows are present. The per-sink counts are printed.
//!
//! Every available connector is a source here as well as a sink, and the two
//! sources whose first poll the prime can cancel tolerate that. `KafkaSource`
//! holds the messages it has taken from the broker on the source itself, so
//! the dropped prime future hands them to the rotation; with `stop_at_end
//! #true` it returns them as soon as every partition reports EOF instead of
//! waiting out `poll_timeout_ms`. `NatsSource` with `stop_at_end #true` uses
//! `fetch`, which sets `no_wait` and returns what is already on the stream.
//!
//! The rotation itself is serial: after the prime, `run_stream` awaits one
//! source's `next_batch` to completion with only cancellation racing it, so a
//! source that idles out its poll window holds up every source behind it. For a
//! cancel-safe source that costs wall clock and nothing else, because a source
//! polled during the prime hands its prefetched batch to the rotation. A
//! source mid-way through a batch is never held up either: a carry-over chunk
//! is served ahead of any poll, so no source is polled while a tail is
//! outstanding.

use std::collections::BTreeMap;
use std::io::{Read as _, Seek as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use redb::ReadableDatabase as _;
use tokio::sync::{OnceCell, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;

use saci_core::dataset::Dataset;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use saci_core::pipeline::Pipeline;
use saci_core::system::{SystemMeta, WriteSet, system_fn};
use saci_service::service::builder::ServiceBuilder;
use saci_service::service::config::ServiceConfig;
use saci_service::service::factories::register_builtin_factories;
use saci_service::service::standalone::{StandaloneStats, run_standalone};
use saci_service::wasm::WasmEngine;
use saci_transformer::Transformer;

use testcontainers::core::{ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

// ── Dimensions ───────────────────────────────────────────────────────────────

/// One connector, on either end of a workflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Connector {
    Channel,
    File,
    Http,
    Tcp,
    Saci,
    Kafka,
    Nats,
    Postgresql,
    S3,
    Turso,
    Redb,
}

/// Every connector this matrix covers, in report order.
pub const CONNECTORS: [Connector; 11] = [
    Connector::Channel,
    Connector::File,
    Connector::Http,
    Connector::Tcp,
    Connector::Saci,
    Connector::Kafka,
    Connector::Nats,
    Connector::Postgresql,
    Connector::S3,
    Connector::Turso,
    Connector::Redb,
];

/// Which transformer surface a connector drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    /// Carries `RecordBatch`es; no transformer is resolved at all.
    Rows,
    /// One seekable document per drain or write.
    Stream,
    /// Discrete payloads.
    Message,
}

impl Connector {
    /// The name used in the report.
    pub fn label(self) -> &'static str {
        match self {
            Self::Channel => "channel",
            Self::File => "file",
            Self::Http => "http",
            Self::Tcp => "tcp",
            Self::Saci => "saci",
            Self::Kafka => "kafka",
            Self::Nats => "nats",
            Self::Postgresql => "postgresql",
            Self::S3 => "s3",
            Self::Turso => "turso",
            Self::Redb => "redb",
        }
    }

    fn surface(self) -> Surface {
        match self {
            Self::Channel | Self::Postgresql | Self::Turso | Self::Saci => Surface::Rows,
            Self::File | Self::Http | Self::S3 | Self::Redb => Surface::Stream,
            Self::Tcp | Self::Kafka | Self::Nats => Surface::Message,
        }
    }

    /// The external resource this connector needs to move a row, if any.
    fn resource(self) -> Option<Resource> {
        match self {
            Self::Channel
            | Self::File
            | Self::Http
            | Self::Tcp
            | Self::Saci
            | Self::Turso
            | Self::Redb => None,
            Self::Kafka => Some(Resource::Kafka),
            Self::Nats => Some(Resource::Nats),
            Self::Postgresql => Some(Resource::Postgres),
            Self::S3 => Some(Resource::S3),
        }
    }
}

/// One byte format, or the absence of one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    None,
    ArrowIpc,
    Avro,
    Csv,
    Ndjson,
    Parquet,
}

/// Every format this matrix covers, in report order.
pub const FORMATS: [Format; 6] = [
    Format::None,
    Format::ArrowIpc,
    Format::Avro,
    Format::Csv,
    Format::Ndjson,
    Format::Parquet,
];

impl Format {
    /// The `format` key, `None` for [`Format::None`].
    fn key(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::ArrowIpc => Some("arrow-ipc"),
            Self::Avro => Some("avro"),
            Self::Csv => Some("csv"),
            Self::Ndjson => Some("ndjson"),
            Self::Parquet => Some("parquet"),
        }
    }

    /// The name used in the report.
    pub fn label(self) -> &'static str {
        self.key().unwrap_or("none")
    }

    /// A transformer instance matching what the registry builds for this
    /// format, for seeding and reading back outside the service.
    fn transformer(self) -> Option<Arc<dyn Transformer>> {
        match self {
            Self::None => None,
            Self::ArrowIpc => Some(Arc::new(
                saci_transformer_arrow_ipc::ArrowIpcTransformer::new(),
            )),
            Self::Avro => Some(Arc::new(saci_transformer_avro::AvroTransformer::new(
                None, None,
            ))),
            Self::Csv => Some(Arc::new(saci_transformer_csv::CsvTransformer::new(true))),
            Self::Ndjson => Some(Arc::new(
                saci_transformer_ndjson::NdjsonTransformer::default(),
            )),
            Self::Parquet => Some(Arc::new(saci_transformer_parquet::ParquetTransformer::new())),
        }
    }

    /// A self-describing format reads the schema the document carries. Every
    /// case here declares none for one of these, so a document's own schema is
    /// both what the link check sees and what a decoded document is compared
    /// against.
    fn carries_its_own_schema(self) -> bool {
        matches!(self, Self::Avro | Self::Parquet | Self::ArrowIpc)
    }

    /// Does this format offer the surface `connector` drives?
    fn fits(self, connector: Connector) -> bool {
        match connector.surface() {
            // A rows connector resolves no transformer, so any declaration is
            // simply unreferenced.
            Surface::Rows => true,
            // Every transformer implements both byte surfaces, so the only
            // thing a byte connector cannot carry is the absence of a format.
            Surface::Stream | Surface::Message => self != Self::None,
        }
    }
}

/// One processor runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorKind {
    /// A native [`Pipeline`] injected with [`ServiceBuilder::with_runtime`].
    Native,
    /// The `saci-processor-smoketest` component, loaded from `module`.
    Wasm,
    /// The `saci-plugin-smoketest` cdylib, loaded from `library`.
    Plugin,
}

/// Every processor runtime this matrix covers, in report order.
pub const PROCESSORS: [ProcessorKind; 3] = [
    ProcessorKind::Native,
    ProcessorKind::Wasm,
    ProcessorKind::Plugin,
];

impl ProcessorKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Wasm => "wasm",
            Self::Plugin => "plugin",
        }
    }

    /// The row type this runtime's schema forces on both ends.
    fn row(self) -> RowKind {
        match self {
            Self::Native => RowKind::Order,
            Self::Wasm => RowKind::Ping,
            Self::Plugin => RowKind::Counter,
        }
    }
}

// ── Row types ────────────────────────────────────────────────────────────────

/// The component a case carries, fixed by its processor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowKind {
    /// `Order { id: Int64, label: Utf8, total: Float64 }`, the native
    /// pipeline's own.
    Order,
    /// `Ping { seq: UInt64 }`, mirroring `saci-processor-smoketest`.
    Ping,
    /// `Counter { id: Int64, seen: Int64 }`, mirroring `saci-plugin-smoketest`.
    Counter,
}

/// How far apart two sources' ids sit.
///
/// A workflow declaring several sources seeds each one from the same three
/// rows offset by this stride, so every delivered row names the source it came
/// from: an upserting PostgreSQL sink cannot collapse two sources onto one
/// primary key, and the HTTP readback's repeated-document rule cannot collapse
/// two sources' documents into one.
const SOURCE_ID_STRIDE: usize = 100;

impl RowKind {
    fn component(self) -> &'static str {
        match self {
            Self::Order => "Order",
            Self::Ping => "Ping",
            Self::Counter => "Counter",
        }
    }

    fn schema(self) -> Arc<Schema> {
        match self {
            Self::Order => Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, false),
                Field::new("total", DataType::Float64, false),
            ])),
            Self::Ping => Arc::new(Schema::new(vec![Field::new(
                "seq",
                DataType::UInt64,
                false,
            )])),
            Self::Counter => Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("seen", DataType::Int64, false),
            ])),
        }
    }

    /// The schema a readback declares.
    ///
    /// `widened` is the Avro object container file's doing: it has no unsigned
    /// integer type, so an object written from `Ping` comes back carrying
    /// `Int64` and a reader declaring `UInt64` refuses it.
    fn readback_schema(self, widened: bool) -> Arc<Schema> {
        if self == Self::Ping && widened {
            return Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
        }
        self.schema()
    }

    /// The batch source `index` feeds in: the same three rows every source
    /// carries, with their ids offset by [`SOURCE_ID_STRIDE`].
    fn input(self, index: usize) -> RecordBatch {
        let base = (index * SOURCE_ID_STRIDE) as i64;
        let seq = (index * SOURCE_ID_STRIDE) as u64;
        let schema = self.schema();
        let columns: Vec<arrow_array::ArrayRef> = match self {
            Self::Order => vec![
                Arc::new(Int64Array::from(vec![base + 1, base + 2, base + 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float64Array::from(vec![1.5_f64, 2.5, 3.5])),
            ],
            Self::Ping => vec![Arc::new(UInt64Array::from(vec![seq + 1, seq + 2, seq + 3]))],
            Self::Counter => vec![
                Arc::new(Int64Array::from(vec![base + 10, base + 20, base + 30])),
                Arc::new(Int64Array::from(vec![0_i64, 0, 0])),
            ],
        };
        RecordBatch::try_new(schema, columns).expect("row fixture matches its own schema")
    }

    /// What a sink must hold for the rows source `index` fed in, once the
    /// processor has run.
    fn expected(self, index: usize) -> Vec<String> {
        let base = (index * SOURCE_ID_STRIDE) as i64;
        match self {
            // `double_total` doubles every total.
            Self::Order => vec![
                format!("{}|a|3.0000", base + 1),
                format!("{}|b|5.0000", base + 2),
                format!("{}|c|7.0000", base + 3),
            ],
            // The WASM smoketest touches no data column.
            Self::Ping => vec![
                (base + 1).to_string(),
                (base + 2).to_string(),
                (base + 3).to_string(),
            ],
            // `AdvanceSystem` numbers a cold-start batch from one, whichever
            // ids the rows carry.
            Self::Counter => vec![
                format!("{}|1", base + 10),
                format!("{}|2", base + 20),
                format!("{}|3", base + 30),
            ],
        }
    }

    /// The rows of `batch`, rendered so a comparison reads in a failure
    /// message.
    ///
    /// Column types are the case's own, so a format that silently widened one
    /// is a mismatch rather than a panic. `widened_unsigned` is the one
    /// exception: an Avro object container file has no unsigned integer type,
    /// so a stream Avro sink writes `Ping.seq` as `long` and it reads back
    /// `Int64`. The values are still asserted; only the type is allowed to
    /// differ, and only where Avro's own type system forces it.
    fn render(self, batch: &RecordBatch, widened_unsigned: bool) -> Result<Vec<String>, String> {
        let ints = |name: &str| -> Result<Vec<i64>, String> {
            let column = batch
                .column_by_name(name)
                .ok_or_else(|| format!("column '{name}' missing from the sink's rows"))?;
            let values = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| format!("column '{name}' came back as {:?}", column.data_type()))?;
            Ok(values.iter().map(Option::unwrap_or_default).collect())
        };
        match self {
            Self::Order => {
                let id = ints("id")?;
                let label = batch
                    .column_by_name("label")
                    .ok_or("column 'label' missing from the sink's rows")?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or("column 'label' did not come back as Utf8")?
                    .iter()
                    .map(|v| v.unwrap_or_default().to_string())
                    .collect::<Vec<_>>();
                let total = batch
                    .column_by_name("total")
                    .ok_or("column 'total' missing from the sink's rows")?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or("column 'total' did not come back as Float64")?
                    .iter()
                    .map(Option::unwrap_or_default)
                    .collect::<Vec<_>>();
                Ok((0..batch.num_rows())
                    .map(|r| format!("{}|{}|{:.4}", id[r], label[r], total[r]))
                    .collect())
            }
            Self::Ping => {
                let column = batch
                    .column_by_name("seq")
                    .ok_or("column 'seq' missing from the sink's rows")?;
                if let Some(seq) = column.as_any().downcast_ref::<UInt64Array>() {
                    return Ok(seq
                        .iter()
                        .map(|v| v.unwrap_or_default().to_string())
                        .collect());
                }
                match column.as_any().downcast_ref::<Int64Array>() {
                    Some(seq) if widened_unsigned => Ok(seq
                        .iter()
                        .map(|v| v.unwrap_or_default().to_string())
                        .collect()),
                    _ => Err(format!(
                        "column 'seq' came back as {:?}",
                        column.data_type()
                    )),
                }
            }
            Self::Counter => {
                let id = ints("id")?;
                let seen = ints("seen")?;
                Ok((0..batch.num_rows())
                    .map(|r| format!("{}|{}", id[r], seen[r]))
                    .collect())
            }
        }
    }

    /// `schema_fields` entries, one per line, at `indent`.
    fn schema_fields_kdl(self, indent: &str) -> String {
        self.schema()
            .fields()
            .iter()
            .map(|field| {
                let ty = match field.data_type() {
                    DataType::Int64 => "int64",
                    DataType::UInt64 => "uint64",
                    DataType::Float64 => "float64",
                    DataType::Utf8 => "utf8",
                    other => unreachable!("no matrix row carries {other:?}"),
                };
                format!(
                    "{indent}schema_fields \"{}\" type=\"{ty}\" nullable=#false",
                    field.name()
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// `CREATE TABLE` column list for a PostgreSQL end.
    fn postgres_columns(self) -> &'static str {
        match self {
            Self::Order => {
                "id bigint PRIMARY KEY, label text NOT NULL, total double precision NOT NULL"
            }
            // Rejected before any table is needed: PgFieldType has no unsigned
            // variant, so the source/sink config does not parse.
            Self::Ping => "seq bigint PRIMARY KEY",
            Self::Counter => "id bigint PRIMARY KEY, seen bigint NOT NULL",
        }
    }

    /// The column a `polling` PostgreSQL source advances its cursor on.
    fn postgres_cursor(self) -> &'static str {
        match self {
            Self::Order | Self::Counter => "id",
            Self::Ping => "seq",
        }
    }

    /// `INSERT` statement seeding source `index`'s PostgreSQL table.
    fn postgres_seed(self, table: &str, index: usize) -> String {
        let base = (index * SOURCE_ID_STRIDE) as i64;
        match self {
            Self::Order => format!(
                "INSERT INTO {table} (id, label, total) VALUES \
                 ({},'a',1.5),({},'b',2.5),({},'c',3.5)",
                base + 1,
                base + 2,
                base + 3
            ),
            Self::Ping => format!(
                "INSERT INTO {table} (seq) VALUES ({}),({}),({})",
                base + 1,
                base + 2,
                base + 3
            ),
            Self::Counter => format!(
                "INSERT INTO {table} (id, seen) VALUES ({},0),({},0),({},0)",
                base + 10,
                base + 20,
                base + 30
            ),
        }
    }

    /// A native pipeline declaring this component, with the transform the
    /// expected rows assert.
    fn native_pipeline(self) -> Pipeline {
        let component = self.component();
        let schema = self.schema();
        let mut pipeline = Pipeline::new("matrix");
        pipeline
            .data
            .register_raw_component(component, Arc::clone(&schema));
        if self == Self::Order {
            pipeline.add_system(system_fn(
                SystemMeta::new("double_total")
                    .read(component, "total")
                    .write(component, "total"),
                move |data: &mut Dataset| {
                    let Some(batch) = data.batch_for(component) else {
                        return Ok(());
                    };
                    if batch.num_rows() == 0 {
                        return Ok(());
                    }
                    let totals = batch
                        .column_by_name("total")
                        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
                        .ok_or_else(|| {
                            saci_core::error::SaciError::generic(
                                "matrix: Order.total is not Float64",
                            )
                        })?;
                    let doubled: Float64Array = totals.iter().map(|v| v.map(|v| v * 2.0)).collect();
                    data.apply_write_set(WriteSet::new().put(component, "total", Arc::new(doubled)))
                },
            ));
        }
        pipeline
    }
}

// ── Expectations ─────────────────────────────────────────────────────────────

/// Where a refusal is observed.
///
/// There is no "the run was clean and delivered nothing" site: every refusal
/// in this matrix either fails a build or is reported by the runner. A
/// combination that ran clean and delivered nothing would be a silent bug, not
/// a capability, so it gets fixed rather than a site of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Site {
    /// `ServiceConfig::load` or `ServiceBuilder::build_all` returns an error.
    Build,
    /// The service builds and the runner reports a non-fatal error; no row
    /// reaches the sink.
    Run,
}

impl Site {
    fn label(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Run => "run",
        }
    }
}

/// What a case must do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expect {
    /// Build, run, and deliver exactly the expected rows.
    Supported,
    /// Be refused at `site`. A non-empty `fragment` must appear in the
    /// message: at [`Site::Build`] in the error `build_all` returns, at
    /// [`Site::Run`] in what the source's own connector reports through
    /// [`SourceProbe`], because the runner keeps only a count. Empty means the
    /// site alone is the assertion.
    Rejected {
        site: Site,
        reason: &'static str,
        fragment: &'static str,
    },
}

/// One point of the matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Case {
    pub source: Connector,
    pub sink: Connector,
    pub format: Format,
    pub processor: ProcessorKind,
}

impl Case {
    /// Every case, split into the two phases they run in.
    ///
    /// A `tcp`-sourced case needs `run_mode kind="stream"`, whose runner is
    /// cancelled rather than reaching EOF, and the live stats it is cancelled
    /// on are published one item behind. Those cases therefore need a settle
    /// window measured in wall clock, which only means anything when the
    /// process is not also driving a thousand other cases: every case future
    /// is polled by one task, because `PipelineRuntime` is `?Send` and cannot
    /// be spawned. Running them as a second phase is what makes that window
    /// hold.
    pub fn phases() -> (Vec<Self>, Vec<Self>) {
        Self::all().into_iter().partition(|c| !c.stream_mode())
    }

    /// The canonical report position: processor, then format, then source,
    /// then sink.
    fn rank(self) -> (usize, usize, usize, usize) {
        (
            PROCESSORS
                .iter()
                .position(|p| *p == self.processor)
                .unwrap_or(PROCESSORS.len()),
            FORMATS
                .iter()
                .position(|f| *f == self.format)
                .unwrap_or(FORMATS.len()),
            CONNECTORS
                .iter()
                .position(|c| *c == self.source)
                .unwrap_or(CONNECTORS.len()),
            CONNECTORS
                .iter()
                .position(|c| *c == self.sink)
                .unwrap_or(CONNECTORS.len()),
        )
    }

    /// Every case, in report order.
    fn all() -> Vec<Self> {
        let mut cases = Vec::with_capacity(CONNECTORS.len() * CONNECTORS.len() * 18);
        for processor in PROCESSORS {
            for format in FORMATS {
                for source in CONNECTORS {
                    for sink in CONNECTORS {
                        cases.push(Self {
                            source,
                            sink,
                            format,
                            processor,
                        });
                    }
                }
            }
        }
        cases
    }

    fn row(self) -> RowKind {
        self.processor.row()
    }

    /// What this combination must do, derived from the capability table.
    ///
    /// The order matters, because it is the order the service checks in:
    /// `build_all` parses and builds every node in topological order (the
    /// source before the sink), then runs `validate_workflow_graph` over the
    /// finished nodes, and only then does the runner drain the source and
    /// write the sink.
    pub fn expect(self) -> Expect {
        let ends = [(self.source, true), (self.sink, false)];

        // 1. Node build, source first.
        for (connector, _is_source) in ends {
            if let Some(refusal) = self.node_build_refusal(connector) {
                return refusal;
            }
        }

        // 2. `validate_workflow_graph`, once every node exists.
        if self.source == Connector::File
            && self.format == Format::Avro
            && self.row() == RowKind::Ping
        {
            return Expect::Rejected {
                site: Site::Build,
                reason: "the avro container file has no unsigned integer type",
                fragment: "schema differs between source",
            };
        }

        // 3. The run: the source drains before the sink writes.
        for (connector, is_source) in ends {
            if let Some(refusal) = self.run_refusal(connector, is_source) {
                return refusal;
            }
        }

        Expect::Supported
    }

    /// What refuses `connector`'s own node while the factory builds it.
    fn node_build_refusal(self, connector: Connector) -> Option<Expect> {
        let rejected = |reason: &'static str, fragment: &'static str| {
            Some(Expect::Rejected {
                site: Site::Build,
                reason,
                fragment,
            })
        };

        // The node's own `config` is deserialized before anything else, and
        // `PgFieldType` has no unsigned variant to hold `Ping.seq`. The
        // fragment is serde's own unknown-variant wording rather than the type
        // name alone: `is_cursor_capable` refuses the same column with a
        // message that also carries `uint64`, so the bare name would be
        // satisfied by a refusal from a different check.
        if connector == Connector::Postgresql && self.row() == RowKind::Ping {
            return rejected(
                "PgFieldType has no unsigned variant",
                "unknown variant `uint64`",
            );
        }
        if connector == Connector::Turso && self.row() == RowKind::Ping {
            return rejected(
                "TursoFieldType has no unsigned variant",
                "unknown variant `uint64`",
            );
        }
        if connector.surface() == Surface::Rows {
            return None;
        }
        if self.format == Format::None {
            return rejected(
                "a byte connector needs a transformer",
                "needs a 'transformer' key",
            );
        }
        // Every transformer implements both byte surfaces, so [`Format::None`]
        // above is the only format any byte connector refuses. Nothing else
        // reaches a node without the surface it drives, on either end: a
        // `file`, `http` or `s3` node opens a reader or a writer, a `kafka`,
        // `nats` or `tcp` sink gates on `message_shape` and a `tcp` source
        // opens a decoder, and all five formats answer all four.
        assert!(
            self.format.fits(connector),
            "every format offers the surface a {} node drives",
            connector.label()
        );
        None
    }

    /// What refuses `connector` once rows are moving.
    fn run_refusal(self, connector: Connector, is_source: bool) -> Option<Expect> {
        let refused_at_run = |reason: &'static str, fragment: &'static str| {
            Some(Expect::Rejected {
                site: Site::Run,
                reason,
                fragment,
            })
        };

        if connector.surface() == Surface::Rows {
            return None;
        }
        // A format its connector's surface cannot carry is settled while the
        // node builds, for every byte connector: the three message connectors
        // gate on the codec they need, and every format offers the stream
        // surface the other three drive.
        assert!(
            self.format.fits(connector),
            "a format {} cannot carry is refused while building",
            connector.label()
        );
        if !is_source || !self.format.carries_its_own_schema() {
            return None;
        }
        match connector {
            // Both stream sources hand the format nothing and cross-check the
            // schema the stream turned out to carry: `schema_from "body"` on
            // an `http` source, `schema_from "object"` on an `s3` one. Every
            // self-describing format therefore reads, except that `arrow-avro`
            // reads the `long` it wrote for `Ping.seq` back as `Int64`, which
            // the cross-check refuses.
            Connector::Http | Connector::S3
                if self.format == Format::Avro && self.row() == RowKind::Ping =>
            {
                refused_at_run(
                    "the avro container file has no unsigned integer type",
                    "carries schema [seq: Int64] but the config declared [seq: UInt64]",
                )
            }
            _ => None,
        }
    }

    /// The resources this case must have live. A build-time refusal needs
    /// none: no factory in this workspace connects while it builds.
    pub fn resources(self) -> Vec<Resource> {
        if matches!(
            self.expect(),
            Expect::Rejected {
                site: Site::Build,
                ..
            }
        ) {
            return Vec::new();
        }
        let mut out = Vec::new();
        for connector in [self.source, self.sink] {
            if let Some(resource) = connector.resource()
                && !out.contains(&resource)
            {
                out.push(resource);
            }
        }
        out
    }

    /// How many rows the runner must report processed: three on a supported
    /// case, none on a refused one.
    fn expected_rows(self) -> u64 {
        match self.expect() {
            Expect::Supported => 3,
            Expect::Rejected { .. } => 0,
        }
    }

    /// A `tcp` or `saci` source never reaches EOF, so either forces the
    /// stream runner.
    fn stream_mode(self) -> bool {
        matches!(self.source, Connector::Tcp | Connector::Saci)
    }
}

// ── Report ───────────────────────────────────────────────────────────────────

/// What a case did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Supported,
    Rejected,
    Skipped,
    Failed,
}

impl Outcome {
    /// The name used in the report and in the failure list.
    pub fn label(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Rejected => "rejected",
            Self::Skipped => "skipped",
            Self::Failed => "FAILED",
        }
    }

    fn glyph(self) -> char {
        match self {
            Self::Supported => '+',
            Self::Rejected => '-',
            Self::Skipped => '.',
            Self::Failed => 'X',
        }
    }
}

/// One row of the printed matrix.
#[derive(Debug, Clone)]
pub struct CaseReport {
    pub case: Case,
    pub outcome: Outcome,
    pub site: Option<Site>,
    pub detail: String,
    pub elapsed: Duration,
}

impl CaseReport {
    /// The one-line name a failure list uses.
    pub fn name(&self) -> String {
        format!(
            "{} -> {} [{}] via {}",
            self.case.source.label(),
            self.case.sink.label(),
            self.case.format.label(),
            self.case.processor.label()
        )
    }
}

/// Every case's outcome, rendered together.
pub struct Report {
    pub cases: Vec<CaseReport>,
    pub elapsed: Duration,
}

impl Report {
    /// Collect every phase's reports into one, in canonical matrix order.
    pub fn new(mut cases: Vec<CaseReport>, elapsed: Duration) -> Self {
        cases.sort_by_key(|report| report.case.rank());
        Self { cases, elapsed }
    }

    fn count(&self, outcome: Outcome) -> usize {
        self.cases.iter().filter(|c| c.outcome == outcome).count()
    }

    /// The cases that must be empty for the test to pass.
    pub fn failures(&self) -> Vec<&CaseReport> {
        self.cases
            .iter()
            .filter(|c| c.outcome == Outcome::Failed)
            .collect()
    }

    /// Print the per-case table, the per-processor grids and the totals.
    pub fn print(&self) {
        println!(
            "\n=== connector matrix: {} cases in {:.1?} ===\n",
            self.cases.len(),
            self.elapsed
        );
        println!(
            "{:<12} {:<12} {:<10} {:<8} {:<10} {:<8} {:>9}  detail",
            "source", "sink", "transformer", "runtime", "outcome", "site", "elapsed"
        );
        for report in &self.cases {
            // One line per case: a failed case's detail also carries the whole
            // config it ran, which the failure list in `full_matrix` prints in
            // full.
            println!(
                "{:<12} {:<12} {:<10} {:<8} {:<10} {:<8} {:>8.1?}  {}",
                report.case.source.label(),
                report.case.sink.label(),
                report.case.format.label(),
                report.case.processor.label(),
                report.outcome.label(),
                report.site.map_or("-", Site::label),
                report.elapsed,
                report.detail.lines().next().unwrap_or_default()
            );
        }

        println!("\n--- grids: rows are sources, columns are sinks ---");
        println!("    + supported   - rejected   . skipped   X FAILED");
        for processor in PROCESSORS {
            for format in FORMATS {
                let mut grid = BTreeMap::new();
                for report in &self.cases {
                    if report.case.processor == processor && report.case.format == format {
                        grid.insert(
                            (report.case.source, report.case.sink),
                            report.outcome.glyph(),
                        );
                    }
                }
                if grid.is_empty() {
                    continue;
                }
                println!(
                    "\n{} / {}\n{:<12}{}",
                    processor.label(),
                    format.label(),
                    "",
                    CONNECTORS
                        .iter()
                        .map(|c| format!("{:<4}", &c.label()[..3.min(c.label().len())]))
                        .collect::<String>()
                );
                for source in CONNECTORS {
                    let row: String = CONNECTORS
                        .iter()
                        .map(|sink| {
                            format!("{:<4}", grid.get(&(source, *sink)).copied().unwrap_or('?'))
                        })
                        .collect();
                    println!("{:<12}{row}", source.label());
                }
            }
        }

        println!("\n--- totals ---");
        println!("supported : {}", self.count(Outcome::Supported));
        println!("rejected  : {}", self.count(Outcome::Rejected));
        println!("skipped   : {}", self.count(Outcome::Skipped));
        println!("FAILED    : {}", self.count(Outcome::Failed));
        println!("wall clock: {:.1?}", self.elapsed);

        let ran = self.cases.len() - self.count(Outcome::Skipped);
        if ran == 0 {
            println!(
                "\n!!! NOTHING RAN: every one of {} cases was skipped, so this run proves nothing",
                self.cases.len()
            );
        } else {
            println!("\ncases that actually ran: {ran} of {}", self.cases.len());
        }

        let mut skipped_by_resource: BTreeMap<&str, usize> = BTreeMap::new();
        for report in &self.cases {
            if report.outcome == Outcome::Skipped {
                *skipped_by_resource
                    .entry(report.detail.as_str())
                    .or_default() += 1;
            }
        }
        for (reason, count) in skipped_by_resource {
            println!("skipped: {count:>4}  {reason}");
        }
    }
}

// ── Shared resources ─────────────────────────────────────────────────────────

/// One external resource, started once for the whole run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resource {
    Kafka,
    Nats,
    Postgres,
    S3,
}

impl Resource {
    fn label(self) -> &'static str {
        match self {
            Self::Kafka => "kafka",
            Self::Nats => "nats",
            Self::Postgres => "postgres",
            Self::S3 => "s3",
        }
    }
}

/// A live Kafka broker.
pub struct KafkaResource {
    _container: ContainerAsync<GenericImage>,
    pub brokers: String,
}

/// A live NATS server with JetStream enabled.
pub struct NatsResource {
    _container: ContainerAsync<GenericImage>,
    pub url: String,
}

/// A live PostgreSQL server.
pub struct PostgresResource {
    _container: ContainerAsync<GenericImage>,
    pub dsn: String,
}

/// A live S3-compatible endpoint with one bucket created.
pub struct S3Resource {
    _container: ContainerAsync<GenericImage>,
    pub connection: saci_connector_s3::S3ConnectionConfig,
}

/// The four containers, each started at most once.
#[derive(Default)]
pub struct Resources {
    kafka: OnceCell<Option<KafkaResource>>,
    nats: OnceCell<Option<NatsResource>>,
    postgres: OnceCell<Option<PostgresResource>>,
    s3: OnceCell<Option<S3Resource>>,
}

impl Resources {
    pub async fn kafka(&self) -> Option<&KafkaResource> {
        self.kafka.get_or_init(try_start_kafka).await.as_ref()
    }

    pub async fn nats(&self) -> Option<&NatsResource> {
        self.nats.get_or_init(try_start_nats).await.as_ref()
    }

    pub async fn postgres(&self) -> Option<&PostgresResource> {
        self.postgres.get_or_init(try_start_postgres).await.as_ref()
    }

    pub async fn s3(&self) -> Option<&S3Resource> {
        self.s3.get_or_init(try_start_s3).await.as_ref()
    }

    /// Start all four containers before any case runs, and report which came
    /// up.
    ///
    /// Lazily starting them from inside a case would put every readiness poll
    /// on the same task that is driving a thousand other case futures, so a
    /// slow server could miss the readiness deadline its own `try_start_*`
    /// sets for want of being polled.
    pub async fn warm(&self) -> Vec<(Resource, bool)> {
        let mut up = Vec::new();
        for resource in [
            Resource::Kafka,
            Resource::Nats,
            Resource::Postgres,
            Resource::S3,
        ] {
            up.push((resource, self.available(resource).await));
        }
        up
    }

    /// The report line naming which resources a run had.
    pub fn describe(up: &[(Resource, bool)]) -> String {
        up.iter()
            .map(|(resource, live)| {
                format!("{}={}", resource.label(), if *live { "up" } else { "down" })
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    async fn available(&self, resource: Resource) -> bool {
        match resource {
            Resource::Kafka => self.kafka().await.is_some(),
            Resource::Nats => self.nats().await.is_some(),
            Resource::Postgres => self.postgres().await.is_some(),
            Resource::S3 => self.s3().await.is_some(),
        }
    }
}

/// Start a single-node KRaft broker, or report why not.
///
/// The advertised listener has to name a host port before the container runs,
/// so the port is reserved here and mapped explicitly.
async fn try_start_kafka() -> Option<KafkaResource> {
    use rdkafka::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};

    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).ok()?;
    let port = listener.local_addr().ok()?.port();
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

    let container = match image.start().await {
        Ok(container) => container,
        Err(e) => {
            eprintln!("SKIP: kafka container unavailable: {e}");
            return None;
        }
    };
    let brokers = format!("127.0.0.1:{port}");

    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", &brokers);
        if let Ok(consumer) = cfg.create::<BaseConsumer>()
            && consumer
                .fetch_metadata(None, Duration::from_secs(5))
                .is_ok()
        {
            return Some(KafkaResource {
                _container: container,
                brokers,
            });
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    eprintln!("SKIP: kafka broker never accepted a connection");
    None
}

/// Start a JetStream-enabled NATS server, or report why not.
async fn try_start_nats() -> Option<NatsResource> {
    let image = GenericImage::new("nats", "2.11-alpine")
        .with_exposed_port(4222_u16.tcp())
        .with_cmd(["-js", "-sd", "/tmp/nats"]);

    let container = match image.start().await {
        Ok(container) => container,
        Err(e) => {
            eprintln!("SKIP: nats container unavailable: {e}");
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(4222_u16.tcp()).await {
        Ok(port) => port,
        Err(e) => {
            eprintln!("SKIP: nats container port unavailable: {e}");
            return None;
        }
    };
    let url = format!("nats://127.0.0.1:{port}");

    // `-js` starts the subsystem after the client port opens, so a JetStream
    // API round trip is the only sound readiness gate.
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(client) = async_nats::connect(&url).await {
            let js = async_nats::jetstream::new(client);
            match js.get_stream("SACI_MATRIX_PROBE").await {
                Err(e) if e.to_string().contains("timed out") => {}
                _ => {
                    return Some(NatsResource {
                        _container: container,
                        url,
                    });
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    eprintln!("SKIP: nats server never answered the JetStream API");
    None
}

/// Start PostgreSQL, or report why not.
async fn try_start_postgres() -> Option<PostgresResource> {
    const PASSWORD: &str = "saci";

    let image = GenericImage::new("postgres", "18-alpine")
        .with_exposed_port(5432_u16.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", PASSWORD)
        .with_cmd(["postgres", "-c", "fsync=off", "-c", "max_connections=400"]);

    let container = match image.start().await {
        Ok(container) => container,
        Err(e) => {
            eprintln!("SKIP: postgres container unavailable: {e}");
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(5432_u16.tcp()).await {
        Ok(port) => port,
        Err(e) => {
            eprintln!("SKIP: cannot map the postgres port: {e}");
            return None;
        }
    };
    let dsn = format!("postgres://postgres:{PASSWORD}@127.0.0.1:{port}/postgres");

    // The readiness line is logged once during initdb, so poll the real server.
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok((client, connection)) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls).await
        {
            let driver = tokio::spawn(async move {
                let _ = connection.await;
            });
            let ready = client.simple_query("SELECT 1").await.is_ok();
            driver.abort();
            if ready {
                return Some(PostgresResource {
                    _container: container,
                    dsn,
                });
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    eprintln!("SKIP: postgres never accepted a connection");
    None
}

/// Start an S3-compatible endpoint and create the run's one bucket, or report
/// why not.
async fn try_start_s3() -> Option<S3Resource> {
    const ACCESS_KEY: &str = "saciaccesskey";
    const SECRET_KEY: &str = "sacisecretkey";

    let image = GenericImage::new("rustfs/rustfs", "1.0.0-rc.3")
        .with_exposed_port(9000_u16.tcp())
        .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
        .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
        .with_env_var("RUSTFS_ADDRESS", "0.0.0.0:9000")
        .with_env_var("RUSTFS_CONSOLE_ENABLE", "false");
    let container = match image.start().await {
        Ok(container) => container,
        Err(e) => {
            eprintln!("SKIP: rustfs container unavailable: {e}");
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(9000_u16.tcp()).await {
        Ok(port) => port,
        Err(e) => {
            eprintln!("SKIP: rustfs container port unavailable: {e}");
            return None;
        }
    };
    let connection = saci_connector_s3::S3ConnectionConfig {
        bucket: format!("saci-matrix-{}", nanos()),
        endpoint: Some(format!("http://127.0.0.1:{port}")),
        access_key_id: Some(ACCESS_KEY.to_string()),
        secret_access_key: Some(SECRET_KEY.to_string()),
        allow_http: true,
        ..Default::default()
    };

    // A signed CreateBucket through the container's own curl both waits out
    // startup and creates the run's single bucket.
    let deadline = Instant::now() + Duration::from_secs(180);
    let user = format!("{ACCESS_KEY}:{SECRET_KEY}");
    loop {
        // Inside the container the server listens on 9000; the mapped port
        // exists only on the host, so a probe run through `docker exec` names
        // the container's own port.
        let url = format!("http://127.0.0.1:9000/{}", connection.bucket);
        let cmd = ExecCommand::new([
            "curl",
            "-fsS",
            "-o",
            "/dev/null",
            "-X",
            "PUT",
            "--aws-sigv4",
            "aws:amz:us-east-1:s3",
            "--user",
            user.as_str(),
            url.as_str(),
        ]);
        match container.exec(cmd).await {
            Ok(mut result) => {
                // The exit code is only final once the exec's output streams
                // have been consumed; reading it straight away reports
                // `None` for a command that has in fact succeeded.
                let _ = result.stdout_to_vec().await;
                let _ = result.stderr_to_vec().await;
                match result.exit_code().await {
                    Ok(Some(0)) => {
                        return Some(S3Resource {
                            _container: container,
                            connection,
                        });
                    }
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("SKIP: rustfs exec exit code unavailable: {e}");
                        return None;
                    }
                }
            }
            Err(e) => {
                eprintln!("SKIP: rustfs exec failed: {e}");
                return None;
            }
        }
        if Instant::now() >= deadline {
            eprintln!("SKIP: rustfs never accepted a signed CreateBucket within 180s");
            return None;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// ── Fixtures and uniqueness ──────────────────────────────────────────────────

/// The two processor artifacts, resolved once, and the one engine every wasm
/// case compiles the component against.
pub struct Fixtures {
    /// `target/wasm32-wasip2/release/saci_processor_smoketest.wasm`.
    pub wasm: PathBuf,
    /// The `saci-plugin-smoketest` cdylib under the running profile.
    pub plugin: PathBuf,
    /// Shared by every case through
    /// [`ServiceBuilder::with_wasm_engine`], so the 4 MB component is
    /// Cranelift-compiled once for the whole matrix instead of once per case.
    ///
    /// That compile is synchronous and takes about 1.7 s on one fast core.
    /// `PipelineRuntime` is `?Send`, so every case future is polled by one
    /// task: a per-case compile blocks every other case for its whole
    /// duration, and the 239 wasm cases that reach the processor node would
    /// hold the poll loop for over 400 s on a fast machine and several times
    /// that on a two core runner, past every in-flight case's
    /// [`CASE_BUDGET`]. One engine also means one epoch ticker task rather
    /// than one per case.
    pub engine: WasmEngine,
}

impl Fixtures {
    /// Resolve both artifacts, reporting the build command for a missing one.
    pub fn resolve(wasm: PathBuf) -> Result<Self, String> {
        if !wasm.exists() {
            return Err(format!(
                "smoketest component not found at {}; run `cargo build --release \
                 -p saci-processor-smoketest --target wasm32-wasip2` first",
                wasm.display()
            ));
        }
        let plugin = plugin_artifact();
        if !plugin.exists() {
            return Err(format!(
                "smoketest plugin not found at {}; run `cargo build -p saci-plugin-smoketest` first",
                plugin.display()
            ));
        }
        let engine = WasmEngine::new().map_err(|e| format!("wasmtime engine: {e}"))?;
        Ok(Self {
            wasm,
            plugin,
            engine,
        })
    }
}

/// Locate the built cdylib under whichever profile the test binary itself ran
/// under: `current_exe` is `<target>/<profile>/deps/<test-bin>`.
fn plugin_artifact() -> PathBuf {
    let exe = std::env::current_exe().expect("current_exe");
    let profile = exe
        .parent()
        .and_then(Path::parent)
        .expect("profile directory above deps/");
    profile.join(format!(
        "{}saci_plugin_smoketest{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ))
}

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos()
}

/// A name no other case in this process can collide with: the process start
/// timestamp plus a monotonic counter.
fn unique(stem: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static BASE: LazyLock<u128> = LazyLock::new(nanos);
    format!(
        "{stem}_{}_{}",
        *BASE,
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// A quoted KDL string reads backslashes as escapes, so a Windows path goes in
/// with forward slashes.
fn kdl_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

// ── Byte encoding helpers ────────────────────────────────────────────────────

/// Encode `batch` as one self-contained document in `format`.
fn encode_document(format: Format, batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let transformer = format
        .transformer()
        .ok_or("Format::None encodes no document")?;
    let mut spool = tempfile::NamedTempFile::new().map_err(|e| e.to_string())?;
    let handle = spool.reopen().map_err(|e| e.to_string())?;
    let mut writer = transformer
        .open_writer(Box::new(handle), batch.schema())
        .map_err(|e| format!("open_writer: {e}"))?;
    writer
        .write_batch(batch)
        .map_err(|e| format!("write_batch: {e}"))?;
    writer.finish().map_err(|e| format!("finish: {e}"))?;
    let mut bytes = Vec::new();
    spool.rewind().map_err(|e| e.to_string())?;
    spool.read_to_end(&mut bytes).map_err(|e| e.to_string())?;
    Ok(bytes)
}

/// Decode one self-contained `format` document into its batches.
fn decode_document(format: Format, row: RowKind, bytes: &[u8]) -> Result<Vec<RecordBatch>, String> {
    let transformer = format
        .transformer()
        .ok_or("Format::None decodes no document")?;
    let mut spool = tempfile::tempfile().map_err(|e| e.to_string())?;
    spool.write_all(bytes).map_err(|e| e.to_string())?;
    spool.rewind().map_err(|e| e.to_string())?;
    let declared = (!format.carries_its_own_schema()).then(|| row.schema());
    let mut reader = transformer
        .open_reader(spool, declared)
        .map_err(|e| format!("open_reader: {e}"))?;
    let mut batches = Vec::new();
    while let Some(batch) = reader
        .next_batch()
        .map_err(|e| format!("next_batch: {e}"))?
    {
        batches.push(batch);
    }
    Ok(batches)
}

/// One length-prefixed TCP frame per encoded payload, framed individually.
///
/// The frame count is the stream runner's item count for this batch: the tcp
/// source flushes its decoder per frame, so a `PerRow` format turns three rows
/// into three items.
fn encode_frames(format: Format, batch: &RecordBatch) -> Result<Vec<Vec<u8>>, String> {
    let transformer = format
        .transformer()
        .ok_or("Format::None encodes no frame")?;
    let payloads = transformer
        .encode_messages(batch)
        .map_err(|e| format!("encode_messages: {e}"))?;
    payloads
        .into_iter()
        .map(|payload| {
            let len = u32::try_from(payload.len())
                .map_err(|_| "payload exceeds a u32 length".to_string())?;
            let mut frame = Vec::with_capacity(payload.len() + 4);
            frame.extend_from_slice(&len.to_be_bytes());
            frame.extend_from_slice(&payload);
            Ok(frame)
        })
        .collect()
}

/// The bytes one `saci` session needs to deliver `batch`: a hello naming a
/// stand-in peer, then one data frame.
///
/// Both go out as one transport entry, because the `saci` source reads the
/// hello before it accepts and the accept is what the first data frame waits
/// on: splitting them would make the harness wait for a reply it does not read.
fn saci_hello_and_batch(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    use saci_connector_saci::wire::{Frame, PROTOCOL_VERSION, data_header, encode_frame};

    let schema = batch.schema();
    let mut out = Vec::new();
    encode_frame(
        &mut out,
        &Frame::Hello {
            version: PROTOCOL_VERSION,
            identity: saci_connector::NodeIdentity {
                service: "matrix".to_string(),
                workflow: "feed".to_string(),
                node: "out".to_string(),
            },
            schema: Arc::clone(&schema),
        },
    )
    .map_err(|e| format!("encode hello: {e}"))?;

    let buffers = arrow_ipc::writer::StreamEncoder::try_new(&schema)
        .map_err(|e| format!("stream encoder: {e}"))?
        .encode(batch)
        .map_err(|e| format!("encode batch: {e}"))?;
    let ipc_len: usize = buffers.iter().map(|b| b.len()).sum();
    out.extend_from_slice(&data_header(None, ipc_len).map_err(|e| format!("data header: {e}"))?);
    for buffer in &buffers {
        out.extend_from_slice(buffer);
    }
    Ok(out)
}

/// Walk one captured `saci` session: a hello naming a peer, then data frames.
///
/// The hello's identity is asserted here rather than only decoded, because
/// naming the sending service, workflow and node is the connector's whole
/// reason to exist.
fn decode_saci_session(bytes: &[u8]) -> Result<Vec<RecordBatch>, String> {
    use arrow_buffer::Buffer;
    use saci_connector_saci::wire::{Frame, decode_frame};

    let mut decoder = arrow_ipc::reader::StreamDecoder::new();
    let mut batches = Vec::new();
    let mut at = 0usize;
    let mut first = true;
    while at + 4 <= bytes.len() {
        let len =
            u32::from_be_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]) as usize;
        at += 4;
        if at + len > bytes.len() {
            return Err(format!(
                "truncated saci frame: wanted {len} bytes, {} left",
                bytes.len() - at
            ));
        }
        let frame = decode_frame(Buffer::from(bytes[at..at + len].to_vec()))
            .map_err(|e| format!("decode saci frame: {e}"))?;
        at += len;
        match (first, frame) {
            (true, Frame::Hello { identity, .. }) => {
                if identity.service.is_empty()
                    || identity.workflow.is_empty()
                    || identity.node.is_empty()
                {
                    return Err(format!("the hello names an incomplete peer: {identity:?}"));
                }
            }
            (true, other) => {
                return Err(format!(
                    "a session opens with a hello, got frame kind {}",
                    other.kind()
                ));
            }
            (false, Frame::Data { ipc, .. }) => {
                let mut buf = ipc;
                while !buf.is_empty() {
                    match decoder.decode(&mut buf) {
                        Ok(Some(batch)) => batches.push(batch),
                        Ok(None) => break,
                        Err(e) => return Err(format!("decode saci ipc: {e}")),
                    }
                }
            }
            (false, other) => {
                return Err(format!(
                    "a session carries data frames, got frame kind {}",
                    other.kind()
                ));
            }
        }
        first = false;
    }
    Ok(batches)
}

/// Decode a stream of length-prefixed frames into one batch per flush window.
fn decode_frames(format: Format, row: RowKind, bytes: &[u8]) -> Result<Vec<RecordBatch>, String> {
    let transformer = format
        .transformer()
        .ok_or("Format::None decodes no frame")?;
    let mut decoder = transformer
        .open_message_decoder(row.schema())
        .map_err(|e| format!("open_message_decoder: {e}"))?;
    let mut at = 0usize;
    let mut pushed = 0usize;
    while at + 4 <= bytes.len() {
        let len =
            u32::from_be_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]) as usize;
        at += 4;
        if at + len > bytes.len() {
            return Err(format!(
                "truncated frame: wanted {len} bytes, {} left",
                bytes.len() - at
            ));
        }
        decoder
            .push(&bytes[at..at + len])
            .map_err(|e| format!("push: {e}"))?;
        at += len;
        pushed += 1;
    }
    if pushed == 0 {
        return Ok(Vec::new());
    }
    Ok(decoder
        .flush()
        .map_err(|e| format!("flush: {e}"))?
        .into_iter()
        .collect())
}

// ── Local probe servers ──────────────────────────────────────────────────────

/// A hand-rolled HTTP endpoint on an OS-assigned port.
///
/// It answers every request with `body` and records every request body. The
/// listener is non-blocking and polled, so a case whose sink never posted still
/// shuts the probe down, and each accepted connection is served on its own
/// thread: an abandoned connection then times out on its own instead of
/// blocking the requests queued behind it. Each handler reserves its slot at
/// accept time, so `finish` reports bodies in arrival order however the
/// handlers interleave.
struct HttpProbe {
    url: String,
    captured: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
    handlers: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,
    stop: Arc<AtomicBool>,
    acceptor: Option<std::thread::JoinHandle<()>>,
}

impl HttpProbe {
    fn spawn(body: Vec<u8>) -> Result<Self, String> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        let url = format!(
            "http://{}/matrix",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;

        let captured: Arc<Mutex<Vec<Option<Vec<u8>>>>> = Arc::new(Mutex::new(Vec::new()));
        let handlers: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let acceptor = {
            let captured = Arc::clone(&captured);
            let handlers = Arc::clone(&handlers);
            let stop = Arc::clone(&stop);
            let body = Arc::new(body);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            if stream.set_nonblocking(false).is_err() {
                                continue;
                            }
                            let slot = {
                                let mut guard = lock(&captured);
                                guard.push(None);
                                guard.len() - 1
                            };
                            let captured = Arc::clone(&captured);
                            let body = Arc::clone(&body);
                            let handler = std::thread::spawn(move || {
                                if let Some(request) = exchange(stream, &body) {
                                    lock(&captured)[slot] = Some(request);
                                }
                            });
                            lock(&handlers).push(handler);
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            })
        };
        Ok(Self {
            url,
            captured,
            handlers,
            stop,
            acceptor: Some(acceptor),
        })
    }

    /// Stop serving and take every captured request body, in arrival order.
    fn finish(mut self) -> Vec<Vec<u8>> {
        self.shutdown();
        std::mem::take(&mut *lock(&self.captured))
            .into_iter()
            .flatten()
            .collect()
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        for handler in std::mem::take(&mut *lock(&self.handlers)) {
            let _ = handler.join();
        }
    }
}

/// Lock through a poisoned mutex: a panicking request handler must not make the
/// rest of the probe unreadable.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Drop for HttpProbe {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A stand-in `saci` source: it accepts every session and keeps the bytes.
///
/// Same thread shape as [`HttpProbe`]: a non-blocking acceptor thread that
/// reserves each connection's slot at accept time, and one handler thread per
/// connection, so `finish` reports sessions in arrival order however the
/// handlers interleave.
///
/// The handler writes `accept` before it has read the hello. That is sound
/// because the two directions are independent: the sink reads the reply only
/// after its own hello is out, and nothing in the reply depends on what the
/// hello said.
struct SaciProbe {
    addr: std::net::SocketAddr,
    captured: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
    handlers: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,
    stop: Arc<AtomicBool>,
    acceptor: Option<std::thread::JoinHandle<()>>,
}

impl SaciProbe {
    fn spawn() -> Result<Self, String> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
        let addr = listener.local_addr().map_err(|e| e.to_string())?;
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;

        let captured: Arc<Mutex<Vec<Option<Vec<u8>>>>> = Arc::new(Mutex::new(Vec::new()));
        let handlers: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let acceptor = {
            let captured = Arc::clone(&captured);
            let handlers = Arc::clone(&handlers);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            if stream.set_nonblocking(false).is_err() {
                                continue;
                            }
                            let slot = {
                                let mut guard = lock(&captured);
                                guard.push(None);
                                guard.len() - 1
                            };
                            let captured = Arc::clone(&captured);
                            let stop = Arc::clone(&stop);
                            let handler = std::thread::spawn(move || {
                                if let Some(session) = accept_session(stream, &stop) {
                                    lock(&captured)[slot] = Some(session);
                                }
                            });
                            lock(&handlers).push(handler);
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            })
        };
        Ok(Self {
            addr,
            captured,
            handlers,
            stop,
            acceptor: Some(acceptor),
        })
    }

    /// Stop serving and take every session's bytes, in arrival order.
    fn finish(mut self) -> Vec<Vec<u8>> {
        self.shutdown();
        std::mem::take(&mut *lock(&self.captured))
            .into_iter()
            .flatten()
            .collect()
    }

    fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(acceptor) = self.acceptor.take() {
            let _ = acceptor.join();
        }
        for handler in std::mem::take(&mut *lock(&self.handlers)) {
            let _ = handler.join();
        }
    }
}

impl Drop for SaciProbe {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Accept one session at once, then read it until the sink closes or the probe
/// is asked to stop.
///
/// The read timeout is short and the loop rechecks `stop`, rather than one
/// `read_to_end` under a long timeout: a stream-mode case holds its sink open
/// for as long as the runner lives, which under a loaded full-matrix run is
/// well past any timeout worth waiting out, and a timed-out `read_to_end`
/// discards everything it had already read. `finish` sets `stop` only after
/// the built service is gone, so the normal path still ends on a clean EOF.
fn accept_session(mut stream: std::net::TcpStream, stop: &AtomicBool) -> Option<Vec<u8>> {
    stream
        .set_read_timeout(Some(Duration::from_millis(250)))
        .ok()?;
    let mut accept = Vec::new();
    saci_connector_saci::wire::encode_frame(&mut accept, &saci_connector_saci::wire::Frame::Accept)
        .ok()?;
    stream.write_all(&accept).ok()?;
    stream.flush().ok()?;

    let mut session = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match stream.read(&mut chunk) {
            // A clean close: the sink is done with this session.
            Ok(0) => return Some(session),
            Ok(read) => session.extend_from_slice(&chunk[..read]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if stop.load(Ordering::Relaxed) {
                    return Some(session);
                }
            }
            // A reset costs the bytes already read nothing: they were
            // delivered, which is what this probe is here to observe.
            Err(_) => return Some(session),
        }
    }
}

/// Read one request, answer with `body`, and return the request body.
///
/// `connection: close` keeps one accept per request: the client cannot pool a
/// socket the server said it would close.
fn exchange(mut stream: std::net::TcpStream, body: &[u8]) -> Option<Vec<u8>> {
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .ok()?;
    let mut buffered = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(at) = buffered.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            break buffered.len();
        }
        buffered.extend_from_slice(&chunk[..read]);
    };

    let head = String::from_utf8_lossy(&buffered[..head_end]);
    let length: usize = head
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.trim().parse().ok())
        .unwrap_or(0);

    let mut request = buffered[head_end..].to_vec();
    while request.len() < length {
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&chunk[..read]);
    }

    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).ok()?;
    stream.write_all(body).ok()?;
    stream.flush().ok()?;
    Some(request)
}

/// A free port for a `tcp` source's `bind`, which the config needs before the
/// factory binds it for real.
fn reserved_port() -> Result<u16, String> {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    let port = probe.local_addr().map_err(|e| e.to_string())?.port();
    drop(probe);
    Ok(port)
}

// ── Source and sink assembly ─────────────────────────────────────────────────

/// A prepared source: the KDL it declares plus everything that has to outlive
/// the run.
struct SourceSide {
    kdl: String,
    /// A whole extra `workflow` block the config must carry, used to declare
    /// the paired half of a channel. `ServiceConfig::validate` requires every
    /// channel name to pair one `ChannelSink` with one `ChannelSource` across
    /// the whole config, so a harness-held half is not an option.
    paired: Option<String>,
    /// `(bind address, frame bytes)` a `tcp` source is fed once the runner is
    /// listening.
    tcp: Option<(String, Vec<Vec<u8>>)>,
    /// How to build this source again outside the service, so a run-time
    /// refusal can be asserted on its own message. `StandaloneStats` counts
    /// non-fatal errors without keeping their text, so the runner alone can
    /// only say that something failed.
    probe: Option<SourceProbe>,
    _probe: Option<HttpProbe>,
    _dir: Option<tempfile::TempDir>,
}

/// A second, direct build of a source, used to name the refusal the runner
/// only counts.
enum SourceProbe {
    Http {
        url: String,
        format: Format,
        schema: Arc<Schema>,
    },
    S3 {
        connection: saci_connector_s3::S3ConnectionConfig,
        prefix: String,
        format: Format,
        schema: Arc<Schema>,
        schema_from: saci_connector_s3::SchemaFrom,
    },
}

impl SourceProbe {
    /// The error the connector itself reports for this source, or `Err` when
    /// it reports none.
    async fn refusal(self) -> Result<String, String> {
        let source: Box<dyn Source> = match self {
            Self::Http {
                url,
                format,
                schema,
            } => Box::new(
                saci_connector_http::HttpSource::new(
                    &url,
                    Some(schema),
                    saci_connector_http::SchemaFrom::Body,
                    format
                        .transformer()
                        .ok_or("a probed source always names a format")?,
                    Vec::new(),
                    Duration::from_secs(15),
                )
                .map_err(|e| format!("probe source: {e}"))?,
            ),
            Self::S3 {
                connection,
                prefix,
                format,
                schema,
                schema_from,
            } => Box::new(
                saci_connector_s3::S3Source::new(
                    saci_connector_s3::S3SourceConfig {
                        connection,
                        prefix,
                        schema_from,
                        schema_fields: Vec::new(),
                    },
                    schema,
                    format
                        .transformer()
                        .ok_or("a probed source always names a format")?,
                )
                .map_err(|e| format!("probe source: {e}"))?,
            ),
        };
        match drain(source).await {
            Err(message) => Ok(message),
            Ok(batches) => Err(format!(
                "the connector refused nothing: it drained {} batch(es)",
                batches.len()
            )),
        }
    }
}

/// A prepared sink: the KDL it declares plus how to read its rows back.
struct SinkSide {
    kdl: String,
    /// The paired `ChannelSource` workflow, for the same reason as
    /// [`SourceSide::paired`].
    paired: Option<String>,
    readback: Readback,
}

/// Where a sink's rows are read back from.
enum Readback {
    File {
        path: PathBuf,
        format: Format,
        _dir: tempfile::TempDir,
    },
    Redb {
        dir: PathBuf,
        format: Format,
        _dir: tempfile::TempDir,
    },
    Http {
        probe: HttpProbe,
        format: Format,
    },
    Tcp {
        listener: tokio::net::TcpListener,
        format: Format,
    },
    /// A `saci` sink dials a stand-in peer that accepts every session and
    /// keeps the bytes; the frames are walked back on `finish`.
    Saci {
        probe: SaciProbe,
    },
    Kafka {
        brokers: String,
        topic: String,
        format: Format,
    },
    Nats {
        url: String,
        stream: String,
        format: Format,
    },
    Postgres {
        dsn: String,
        table: String,
    },
    Turso {
        path: PathBuf,
        table: String,
        _dir: tempfile::TempDir,
    },
    S3 {
        connection: saci_connector_s3::S3ConnectionConfig,
        prefix: String,
        format: Format,
    },
}

/// The endpoint a config names: the live container's, or an unreachable
/// placeholder for a case that is refused before anything connects.
struct Endpoints {
    brokers: String,
    nats_url: String,
    dsn: String,
    s3: saci_connector_s3::S3ConnectionConfig,
}

impl Endpoints {
    async fn resolve(resources: &Resources, live: bool) -> Self {
        let kafka = if live { resources.kafka().await } else { None };
        let nats = if live { resources.nats().await } else { None };
        let postgres = if live {
            resources.postgres().await
        } else {
            None
        };
        let s3 = if live { resources.s3().await } else { None };
        Self {
            brokers: kafka.map_or_else(|| "127.0.0.1:1".to_string(), |k| k.brokers.clone()),
            nats_url: nats.map_or_else(|| "nats://127.0.0.1:1".to_string(), |n| n.url.clone()),
            dsn: postgres.map_or_else(
                || "postgres://postgres:saci@127.0.0.1:1/postgres".to_string(),
                |p| p.dsn.clone(),
            ),
            s3: s3.map_or_else(
                || saci_connector_s3::S3ConnectionConfig {
                    bucket: "saci-matrix-placeholder".to_string(),
                    endpoint: Some("http://127.0.0.1:1".to_string()),
                    access_key_id: Some("key".to_string()),
                    secret_access_key: Some("secret".to_string()),
                    allow_http: true,
                    ..Default::default()
                },
                |s| s.connection.clone(),
            ),
        }
    }
}

/// Open a PostgreSQL client whose connection task lives as long as the client.
async fn pg_client(dsn: &str) -> Result<tokio_postgres::Client, String> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .map_err(|e| format!("postgres connect: {e}"))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

fn s3_connection_kdl(connection: &saci_connector_s3::S3ConnectionConfig, indent: &str) -> String {
    format!(
        "{indent}connection {{\n\
         {indent}    bucket \"{}\"\n\
         {indent}    endpoint \"{}\"\n\
         {indent}    access_key_id \"{}\"\n\
         {indent}    secret_access_key \"{}\"\n\
         {indent}    allow_http #true\n\
         {indent}}}",
        connection.bucket,
        connection.endpoint.as_deref().unwrap_or(""),
        connection.access_key_id.as_deref().unwrap_or(""),
        connection.secret_access_key.as_deref().unwrap_or(""),
    )
}

/// Open a raw Turso connection to `path`.
async fn turso_client(path: &std::path::Path) -> Result<turso::Connection, String> {
    let db = turso::Builder::new_local(path.to_str().ok_or("non-utf8 turso path")?)
        .build()
        .await
        .map_err(|e| format!("turso open: {e}"))?;
    db.connect().map_err(|e| format!("turso connect: {e}"))
}

/// Read one integer column of a Turso row.
fn turso_i64(row: &turso::Row, idx: usize) -> Result<i64, String> {
    match row.get_value(idx).map_err(|e| e.to_string())? {
        turso::Value::Integer(i) => Ok(i),
        other => Err(format!(
            "turso readback column {idx}: expected an integer, got {other:?}"
        )),
    }
}

/// Read one real column of a Turso row.
fn turso_f64(row: &turso::Row, idx: usize) -> Result<f64, String> {
    match row.get_value(idx).map_err(|e| e.to_string())? {
        turso::Value::Real(f) => Ok(f),
        turso::Value::Integer(i) => Ok(i as f64),
        other => Err(format!(
            "turso readback column {idx}: expected a real, got {other:?}"
        )),
    }
}

/// Read one text column of a Turso row.
fn turso_text(row: &turso::Row, idx: usize) -> Result<String, String> {
    match row.get_value(idx).map_err(|e| e.to_string())? {
        turso::Value::Text(text) => Ok(text),
        other => Err(format!(
            "turso readback column {idx}: expected text, got {other:?}"
        )),
    }
}

/// Build the source node and seed whatever it will read.
///
/// Local seeding is unconditional: `FileSource::open` reads the format's
/// header while the factory builds, so even a case refused at build time needs
/// a well-formed file. `live` is false for a case refused at build, and gates
/// only the seeding that would touch a container.
///
/// `index` is this source's position among the sources of one workflow; it
/// offsets the seeded ids by [`SOURCE_ID_STRIDE`] so a sink's rows name which
/// source delivered them.
async fn prepare_source(
    case: Case,
    endpoints: &Endpoints,
    live: bool,
    index: usize,
) -> Result<SourceSide, String> {
    let row = case.row();
    let component = row.component();
    let batch = row.input(index);
    let format = case.format;
    let transformer_key = format
        .key()
        .map(|_| " transformer=\"fmt\"".to_string())
        .unwrap_or_default();
    let fields = row.schema_fields_kdl("            ");
    // A source cannot be seeded in a format it could not read: those cases are
    // refused, so the transport is left empty and the refusal is what the case
    // observes.
    let seedable = format.fits(case.source);
    let seed_bytes = if seedable && case.source.surface() == Surface::Stream {
        encode_document(format, &batch)?
    } else {
        Vec::new()
    };
    let mut side = SourceSide {
        kdl: String::new(),
        paired: None,
        tcp: None,
        probe: None,
        _probe: None,
        _dir: None,
    };

    match case.source {
        Connector::Channel => {
            // The paired `ChannelSink` has to be declared in the same config,
            // so a second workflow feeds the channel from an ndjson file. Its
            // sink dropping when that workflow finishes is what makes this
            // source see EOF.
            let name = unique("chan");
            let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
            let path = dir.path().join("feed.ndjson");
            std::fs::write(&path, encode_document(Format::Ndjson, &batch)?)
                .map_err(|e| format!("channel feed seed: {e}"))?;
            let feed = unique("feed");
            side.paired = Some(format!(
                "workflow \"{feed}\" {{\n\
                 \x20   transformer \"{feed}_fmt\" format=\"ndjson\"\n\n\
                 \x20   source \"{feed}_in\" type=\"FileSource\" component=\"{component}\" transformer=\"{feed}_fmt\" {{\n\
                 \x20       config {{\n            path \"{}\"\n{fields}\n        }}\n    }}\n\n\
                 \x20   sink \"{feed}_out\" type=\"ChannelSink\" component=\"{component}\" {{\n\
                 \x20       config name=\"{name}\" buffer=8 {{\n{fields}\n        }}\n    }}\n\n\
                 \x20   link from=\"{feed}_in\" to=\"{feed}_out\"\n}}",
                kdl_path(&path)
            ));
            side.kdl = format!(
                "    source \"in\" type=\"ChannelSource\" component=\"{component}\" {{\n\
                 \x20       config name=\"{name}\" buffer=8 {{\n{fields}\n        }}\n    }}"
            );
            side._dir = Some(dir);
        }
        Connector::File => {
            let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
            let path = dir.path().join("source.dat");
            std::fs::write(&path, seed_bytes).map_err(|e| format!("file seed: {e}"))?;
            // `arrow-ipc`, `avro` and `parquet` read the schema the file
            // carries, which is then the one the link check sees.
            let declared = if format.carries_its_own_schema() {
                String::new()
            } else {
                format!("\n{fields}")
            };
            side.kdl = format!(
                "    source \"in\" type=\"FileSource\" component=\"{component}\"{transformer_key} {{\n\
                 \x20       config {{\n            path \"{}\"{declared}\n        }}\n    }}",
                kdl_path(&path)
            );
            side._dir = Some(dir);
        }
        Connector::Redb => {
            let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
            seed_redb(dir.path(), &seed_bytes)?;
            // `RedbSource` requires `schema_fields` and hands it to the format
            // as a projection target, whatever the format, so every `redb`
            // case declares one.
            side.kdl = format!(
                "    source \"in\" type=\"RedbSource\" component=\"{component}\"{transformer_key} {{\n\
                 \x20       config {{\n            directory \"{}\"\n{fields}\n        }}\n    }}",
                kdl_path(dir.path())
            );
            side._dir = Some(dir);
        }
        Connector::Http => {
            let body = seed_bytes;
            let probe = HttpProbe::spawn(body)?;
            // `schema_from "body"` reads a self-describing format under the
            // schema the body carries rather than projecting it onto the
            // declared one: the declared schema stays behind for the link check
            // and is cross-checked against the body's own.
            let schema_from_kdl = if format.carries_its_own_schema() {
                "\n            schema_from \"body\""
            } else {
                ""
            };
            side.probe = format.key().map(|_| SourceProbe::Http {
                url: probe.url.clone(),
                format,
                schema: row.schema(),
            });
            side.kdl = format!(
                "    source \"in\" type=\"HttpSource\" component=\"{component}\"{transformer_key} {{\n\
                 \x20       config {{\n            url \"{}\"{schema_from_kdl}\n            timeout_ms 15000\n{fields}\n        }}\n    }}",
                probe.url
            );
            side._probe = Some(probe);
        }
        Connector::Tcp => {
            let bind = format!("127.0.0.1:{}", reserved_port()?);
            // A format with no message decoder is refused while the source
            // builds, so no frame is ever sent: same rule as `seed_bytes`
            // above.
            let frames = if seedable {
                encode_frames(format, &batch)?
            } else {
                Vec::new()
            };
            side.kdl = format!(
                "    source \"in\" type=\"tcp\" component=\"{component}\"{transformer_key} {{\n\
                 \x20       config {{\n            bind \"{bind}\"\n            buffer 8\n            max_frame_bytes 1048576\n{fields}\n        }}\n    }}"
            );
            side.tcp = Some((bind, frames));
        }
        Connector::Saci => {
            let bind = format!("127.0.0.1:{}", reserved_port()?);
            // One transport entry, so `drive_tcp_stream`'s `frames.len() - 1`
            // item arithmetic still holds: the hello rides in front of the
            // first data frame rather than being an entry of its own, and one
            // data frame is one batch whatever the declared format is. A
            // `saci` node resolves no transformer, so `format` never gates
            // this half.
            let bytes = saci_hello_and_batch(&batch)?;
            side.kdl = format!(
                "    source \"in\" type=\"saci\" component=\"{component}\" {{\n\
                 \x20       config {{\n            bind \"{bind}\"\n            buffer 8\n            max_frame_bytes 1048576\n{fields}\n        }}\n    }}"
            );
            side.tcp = Some((bind, vec![bytes]));
        }
        Connector::Kafka => {
            let topic = unique("matrix-in");
            if live && seedable {
                seed_kafka(&endpoints.brokers, &topic, format, &batch).await?;
            }
            side.kdl = format!(
                "    source \"in\" type=\"KafkaSource\" component=\"{component}\"{transformer_key} {{\n\
                 \x20       config {{\n            brokers \"{}\"\n            topic \"{topic}\"\n\
                 \x20           group_id \"{}\"\n            stop_at_end #true\n            poll_timeout_ms 25000\n{fields}\n        }}\n    }}",
                endpoints.brokers,
                unique("matrix-group")
            );
        }
        Connector::Nats => {
            let stream = unique("MATRIX_IN").to_uppercase();
            let subject = format!("matrix.in.{}", unique("s"));
            if live && seedable {
                seed_nats(&endpoints.nats_url, &stream, &subject, format, &batch).await?;
            }
            side.kdl = format!(
                "    source \"in\" type=\"NatsSource\" component=\"{component}\"{transformer_key} {{\n\
                 \x20       config {{\n            stop_at_end #true\n            poll_timeout_ms 20000\n\
                 \x20           connection {{\n                servers \"{}\"\n            }}\n\
                 \x20           mode kind=\"jetstream\" {{\n                stream \"{stream}\"\n\
                 \x20               durable_name \"{}\"\n                fetch_expires_ms 15000\n            }}\n{fields}\n        }}\n    }}",
                endpoints.nats_url,
                unique("d")
            );
        }
        Connector::Postgresql => {
            let table = unique("matrix_in").to_lowercase();
            if live {
                let client = pg_client(&endpoints.dsn).await?;
                client
                    .batch_execute(&format!(
                        "CREATE TABLE {table} ({}); {}",
                        row.postgres_columns(),
                        row.postgres_seed(&table, index)
                    ))
                    .await
                    .map_err(|e| format!("postgres seed: {e}"))?;
            }
            side.kdl = format!(
                "    source \"in\" type=\"PostgresSource\" component=\"{component}\" {{\n\
                 \x20       config {{\n            name \"{table}\"\n            batch_rows 100\n\
                 \x20           connection {{\n                dsn \"{}\"\n                sslmode \"disable\"\n            }}\n\
                 \x20           mode kind=\"polling\" table=\"{table}\" cursor_column=\"{}\"\n{fields}\n        }}\n    }}",
                endpoints.dsn,
                row.postgres_cursor()
            );
        }
        Connector::Turso => {
            // Embedded, so there is no container: an OS-assigned temp file
            // isolates the case exactly as a unique topic or table would.
            let dir = tempfile::tempdir().map_err(|e| format!("turso temp dir: {e}"))?;
            let path = dir.path().join("in.db");
            let table = unique("matrix_in").to_lowercase();
            if live {
                let conn = turso_client(&path).await?;
                conn.execute(
                    &format!("CREATE TABLE {table} ({})", row.postgres_columns()),
                    (),
                )
                .await
                .map_err(|e| format!("turso seed table: {e}"))?;
                conn.execute(&row.postgres_seed(&table, index), ())
                    .await
                    .map_err(|e| format!("turso seed rows: {e}"))?;
            }
            // The PostgreSQL column list and seed are portable SQL, which the
            // engine accepts type-name for type-name.
            side.kdl = format!(
                "    source \"in\" type=\"TursoSource\" component=\"{component}\" {{\n\
                 \x20       config {{\n            name \"{table}\"\n            batch_rows 100\n\
                 \x20           connection path=\"{}\"\n\
                 \x20           mode kind=\"polling\" table=\"{table}\" cursor_column=\"{}\"\n{fields}\n        }}\n    }}",
                kdl_path(&path),
                row.postgres_cursor()
            );
            side._dir = Some(dir);
        }
        Connector::S3 => {
            let prefix = unique("in");
            // The object is always real, and every format a case can name has
            // a stream writer to encode one with. A node carrying no
            // transformer at all is refused while building, so `live` is
            // false there; an empty prefix would make a refusal
            // indistinguishable from an empty bucket.
            if live {
                seed_s3(&endpoints.s3, &prefix, format, &batch).await?;
            }
            // `schema_from "object"` reads a self-describing format under the
            // schema the object carries rather than projecting it onto the
            // declared one: the declared schema is cross-checked against the
            // object's rather than handed to the reader.
            let (schema_from, schema_from_kdl) = if format.carries_its_own_schema() {
                (
                    saci_connector_s3::SchemaFrom::Object,
                    "\n            schema_from \"object\"",
                )
            } else {
                (saci_connector_s3::SchemaFrom::Config, "")
            };
            side.probe = format.key().map(|_| SourceProbe::S3 {
                connection: endpoints.s3.clone(),
                prefix: prefix.clone(),
                format,
                schema: row.schema(),
                schema_from,
            });
            side.kdl = format!(
                "    source \"in\" type=\"S3Source\" component=\"{component}\"{transformer_key} {{\n\
                 \x20       config {{\n            prefix \"{prefix}\"{schema_from_kdl}\n{}\n{fields}\n        }}\n    }}",
                s3_connection_kdl(&endpoints.s3, "            ")
            );
        }
    }
    Ok(side)
}

/// Build the sink node and whatever reads its rows back.
///
/// `read_back` is false for a case refused at build time, which needs no
/// readback and must not create a table on a container that may be absent.
async fn prepare_sink(
    case: Case,
    endpoints: &Endpoints,
    read_back: bool,
) -> Result<SinkSide, String> {
    let row = case.row();
    let component = row.component();
    let format = case.format;
    let transformer_key = format
        .key()
        .map(|_| " transformer=\"fmt\"".to_string())
        .unwrap_or_default();
    let fields = row.schema_fields_kdl("            ");
    let mut paired = None;

    let (kdl, readback) = match case.sink {
        Connector::Channel => {
            // The paired `ChannelSource` has to be declared in the same
            // config, so a second workflow drains the channel into an ndjson
            // file, which is what the rows are read back out of.
            let name = unique("chan");
            let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
            let path = dir.path().join("drain.ndjson");
            let drain = unique("drain");
            paired = Some(format!(
                "workflow \"{drain}\" {{\n\
                 \x20   transformer \"{drain}_fmt\" format=\"ndjson\"\n\n\
                 \x20   source \"{drain}_in\" type=\"ChannelSource\" component=\"{component}\" {{\n\
                 \x20       config name=\"{name}\" buffer=8 {{\n{fields}\n        }}\n    }}\n\n\
                 \x20   sink \"{drain}_out\" type=\"FileSink\" component=\"{component}\" transformer=\"{drain}_fmt\" {{\n\
                 \x20       config {{\n            path \"{}\"\n            truncate #true\n{fields}\n        }}\n    }}\n\n\
                 \x20   link from=\"{drain}_in\" to=\"{drain}_out\"\n}}",
                kdl_path(&path)
            ));
            (
                format!(
                    "    sink \"out\" type=\"ChannelSink\" component=\"{component}\" {{\n\
                     \x20       config name=\"{name}\" buffer=8 {{\n{fields}\n        }}\n    }}"
                ),
                Readback::File {
                    path,
                    format: Format::Ndjson,
                    _dir: dir,
                },
            )
        }
        Connector::File => {
            let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
            let path = dir.path().join("sink.dat");
            (
                format!(
                    "    sink \"out\" type=\"FileSink\" component=\"{component}\"{transformer_key} {{\n\
                     \x20       config {{\n            path \"{}\"\n            truncate #true\n{fields}\n        }}\n    }}",
                    kdl_path(&path)
                ),
                Readback::File {
                    path,
                    format,
                    _dir: dir,
                },
            )
        }
        Connector::Redb => {
            let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
            let directory = dir.path().to_path_buf();
            (
                format!(
                    "    sink \"out\" type=\"RedbSink\" component=\"{component}\"{transformer_key} {{\n\
                     \x20       config {{\n            directory \"{}\"\n{fields}\n        }}\n    }}",
                    kdl_path(&directory)
                ),
                Readback::Redb {
                    dir: directory,
                    format,
                    _dir: dir,
                },
            )
        }
        Connector::Http => {
            let probe = HttpProbe::spawn(Vec::new())?;
            let kdl = format!(
                "    sink \"out\" type=\"HttpSink\" component=\"{component}\"{transformer_key} {{\n\
                 \x20       config {{\n            url \"{}\"\n            method \"POST\"\n            timeout_ms 15000\n{fields}\n        }}\n    }}",
                probe.url
            );
            (kdl, Readback::Http { probe, format })
        }
        Connector::Tcp => {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(|e| format!("tcp collector bind: {e}"))?;
            let addr = listener
                .local_addr()
                .map_err(|e| format!("tcp collector addr: {e}"))?;
            (
                format!(
                    "    sink \"out\" type=\"tcp\" component=\"{component}\"{transformer_key} {{\n\
                     \x20       config {{\n            connect \"{addr}\"\n{fields}\n        }}\n    }}"
                ),
                Readback::Tcp { listener, format },
            )
        }
        Connector::Saci => {
            let probe = SaciProbe::spawn()?;
            let addr = probe.addr;
            (
                format!(
                    "    sink \"out\" type=\"saci\" component=\"{component}\" {{\n\
                     \x20       config {{\n            connect \"{addr}\"\n{fields}\n        }}\n    }}"
                ),
                Readback::Saci { probe },
            )
        }
        Connector::Kafka => {
            let topic = unique("matrix-out");
            (
                format!(
                    "    sink \"out\" type=\"KafkaSink\" component=\"{component}\"{transformer_key} {{\n\
                     \x20       config {{\n            brokers \"{}\"\n            topic \"{topic}\"\n{fields}\n        }}\n    }}",
                    endpoints.brokers
                ),
                Readback::Kafka {
                    brokers: endpoints.brokers.clone(),
                    topic,
                    format,
                },
            )
        }
        Connector::Nats => {
            let stream = unique("MATRIX_OUT").to_uppercase();
            let subject = format!("matrix.out.{}", unique("s"));
            (
                format!(
                    "    sink \"out\" type=\"NatsSink\" component=\"{component}\"{transformer_key} {{\n\
                     \x20       config {{\n            connection {{\n                servers \"{}\"\n            }}\n\
                     \x20           mode kind=\"jetstream\" {{\n                stream \"{stream}\"\n                subject \"{subject}\"\n            }}\n{fields}\n        }}\n    }}",
                    endpoints.nats_url
                ),
                Readback::Nats {
                    url: endpoints.nats_url.clone(),
                    stream,
                    format,
                },
            )
        }
        Connector::Postgresql => {
            let table = unique("matrix_out").to_lowercase();
            if read_back {
                let client = pg_client(&endpoints.dsn).await?;
                client
                    .batch_execute(&format!(
                        "CREATE TABLE {table} ({})",
                        row.postgres_columns()
                    ))
                    .await
                    .map_err(|e| format!("postgres sink table: {e}"))?;
            }
            (
                format!(
                    "    sink \"out\" type=\"PostgresSink\" component=\"{component}\" {{\n\
                     \x20       config {{\n            name \"{table}\"\n            table \"{table}\"\n\
                     \x20           write_mode \"upsert\"\n            conflict_columns \"{}\"\n\
                     \x20           connection {{\n                dsn \"{}\"\n                sslmode \"disable\"\n            }}\n{fields}\n        }}\n    }}",
                    row.postgres_cursor(),
                    endpoints.dsn
                ),
                Readback::Postgres {
                    dsn: endpoints.dsn.clone(),
                    table,
                },
            )
        }
        Connector::Turso => {
            let dir = tempfile::tempdir().map_err(|e| format!("turso temp dir: {e}"))?;
            let path = dir.path().join("out.db");
            let table = unique("matrix_out").to_lowercase();
            if read_back {
                let conn = turso_client(&path).await?;
                conn.execute(
                    &format!("CREATE TABLE {table} ({})", row.postgres_columns()),
                    (),
                )
                .await
                .map_err(|e| format!("turso sink table: {e}"))?;
            }
            (
                format!(
                    "    sink \"out\" type=\"TursoSink\" component=\"{component}\" {{\n\
                     \x20       config {{\n            name \"{table}\"\n            table \"{table}\"\n\
                     \x20           write_mode \"upsert\"\n            conflict_columns \"{}\"\n\
                     \x20           connection path=\"{}\"\n{fields}\n        }}\n    }}",
                    row.postgres_cursor(),
                    kdl_path(&path)
                ),
                Readback::Turso {
                    path,
                    table,
                    _dir: dir,
                },
            )
        }
        Connector::S3 => {
            let prefix = unique("out");
            (
                format!(
                    "    sink \"out\" type=\"S3Sink\" component=\"{component}\"{transformer_key} {{\n\
                     \x20       config {{\n            prefix \"{prefix}\"\n            suffix \".dat\"\n\
                     \x20           flush max_rows=0 max_bytes=0 max_age_ms=0\n{}\n{fields}\n        }}\n    }}",
                    s3_connection_kdl(&endpoints.s3, "            ")
                ),
                Readback::S3 {
                    connection: endpoints.s3.clone(),
                    prefix,
                    format,
                },
            )
        }
    };
    Ok(SinkSide {
        kdl,
        paired,
        readback,
    })
}

// ── Seeding through the connectors themselves ────────────────────────────────

async fn seed_kafka(
    brokers: &str,
    topic: &str,
    format: Format,
    batch: &RecordBatch,
) -> Result<(), String> {
    let transformer = format.transformer().ok_or("kafka needs a format")?;
    let mut sink = saci_connector_kafka::KafkaSink::new(
        saci_connector_kafka::KafkaSinkConfig {
            brokers: brokers.to_string(),
            topic: topic.to_string(),
            key_field: None,
            tombstones: false,
            flush_timeout_ms: 30_000,
            provision: saci_connector_kafka::TopicProvision::default(),
            properties: Default::default(),
            schema_fields: Vec::new(),
        },
        batch.schema(),
        transformer,
    )
    .map_err(|e| format!("kafka seed sink: {e}"))?;
    sink.write_batch(batch)
        .await
        .map_err(|e| format!("kafka seed write: {e}"))?;
    sink.finish()
        .await
        .map_err(|e| format!("kafka seed finish: {e}"))
}

async fn seed_nats(
    url: &str,
    stream: &str,
    subject: &str,
    format: Format,
    batch: &RecordBatch,
) -> Result<(), String> {
    let transformer = format.transformer().ok_or("nats needs a format")?;
    let mut sink = saci_connector_nats::NatsSink::new(
        saci_connector_nats::NatsSinkConfig {
            connection: saci_connector_nats::ConnectionConfig {
                servers: vec![url.to_string()],
                ..Default::default()
            },
            mode: saci_connector_nats::SinkMode::Jetstream(Box::new(
                saci_connector_nats::JetstreamSinkMode {
                    stream: stream.to_string(),
                    subject: subject.to_string(),
                    ..Default::default()
                },
            )),
            schema_fields: Vec::new(),
        },
        batch.schema(),
        transformer,
    )
    .map_err(|e| format!("nats seed sink: {e}"))?;
    sink.write_batch(batch)
        .await
        .map_err(|e| format!("nats seed write: {e}"))?;
    sink.finish()
        .await
        .map_err(|e| format!("nats seed finish: {e}"))
}

async fn seed_s3(
    connection: &saci_connector_s3::S3ConnectionConfig,
    prefix: &str,
    format: Format,
    batch: &RecordBatch,
) -> Result<(), String> {
    let transformer = format.transformer().ok_or("s3 needs a format")?;
    let mut sink = saci_connector_s3::S3Sink::new(
        saci_connector_s3::S3SinkConfig {
            connection: connection.clone(),
            prefix: prefix.to_string(),
            suffix: ".dat".to_string(),
            flush: saci_connector_s3::Flush {
                max_rows: 0,
                max_bytes: 0,
                max_age_ms: 0,
            },
            schema_fields: Vec::new(),
        },
        batch.schema(),
        transformer,
    )
    .map_err(|e| format!("s3 seed sink: {e}"))?;
    sink.write_batch(batch)
        .await
        .map_err(|e| format!("s3 seed write: {e}"))?;
    sink.finish()
        .await
        .map_err(|e| format!("s3 seed finish: {e}"))
}

/// The table `saci-connector-redb` defaults to.
const REDB_TABLE: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new("records");

/// Seed one redb entry under the key a fresh `RedbSink` would have used.
///
/// The `Database` is dropped before this returns: redb holds an exclusive OS
/// lock on the file for a handle's lifetime, so the source could not open a
/// file this function still held.
fn seed_redb(dir: &Path, bytes: &[u8]) -> Result<(), String> {
    let db = redb::Database::create(dir.join("saci.redb"))
        .map_err(|e| format!("redb seed create: {e}"))?;
    let txn = db
        .begin_write()
        .map_err(|e| format!("redb seed begin_write: {e}"))?;
    {
        let mut table = txn
            .open_table(REDB_TABLE)
            .map_err(|e| format!("redb seed open table: {e}"))?;
        table
            .insert("00000000000000000000", bytes)
            .map_err(|e| format!("redb seed insert: {e}"))?;
    }
    txn.commit().map_err(|e| format!("redb seed commit: {e}"))
}

/// Every entry value in a redb file, in key order.
///
/// A sink that never wrote leaves a file with no table at all, which is the
/// observable "no rows" answer rather than a harness failure. An open failure
/// is not: it would mean the sink never released its exclusive lock.
fn read_redb_entries(dir: &Path) -> Result<Vec<Vec<u8>>, String> {
    let path = dir.join("saci.redb");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let db = redb::Database::open(&path).map_err(|e| format!("redb readback open: {e}"))?;
    let txn = db
        .begin_read()
        .map_err(|e| format!("redb readback begin_read: {e}"))?;
    let table = match txn.open_table(REDB_TABLE) {
        Ok(t) => t,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(e) => return Err(format!("redb readback open table: {e}")),
    };
    let mut values = Vec::new();
    for entry in table
        .range::<&str>(..)
        .map_err(|e| format!("redb readback range: {e}"))?
    {
        let (_, value) = entry.map_err(|e| format!("redb readback entry: {e}"))?;
        values.push(value.value().to_vec());
    }
    Ok(values)
}

// ── Reading a sink back ──────────────────────────────────────────────────────

/// Drain a source into batches, bounded so a live transport cannot hang a case.
async fn drain(mut source: Box<dyn Source>) -> Result<Vec<RecordBatch>, String> {
    let mut batches = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(60), source.next_batch()).await {
            Ok(Ok(Some(batch))) => batches.push(batch),
            Ok(Ok(None)) => return Ok(batches),
            Ok(Err(e)) => return Err(format!("readback drain: {e}")),
            Err(_) => return Err("readback drain timed out".to_string()),
        }
    }
}

/// Every row the sink holds, rendered for comparison.
///
/// `widened_unsigned` is passed straight to [`RowKind::render`]: only an Avro
/// stream sink is allowed to hand `Ping.seq` back as `Int64`.
async fn read_back(
    readback: Readback,
    row: RowKind,
    widened_unsigned: bool,
) -> Result<Vec<String>, String> {
    let batches = match readback {
        Readback::File { path, format, _dir } => {
            // A sink that never wrote leaves no file at all, which is the
            // observable "no rows" answer rather than a harness failure.
            let bytes = std::fs::read(&path).unwrap_or_default();
            if bytes.is_empty() {
                Vec::new()
            } else {
                decode_document(format, row, &bytes)?
            }
        }
        Readback::Redb { dir, format, _dir } => {
            // One entry per accepted batch, each a self-contained document.
            let mut batches = Vec::new();
            for bytes in read_redb_entries(&dir)? {
                batches.extend(decode_document(format, row, &bytes)?);
            }
            batches
        }
        Readback::Http { probe, format } => {
            // A document identical to the one before it is one delivery, not
            // two: the engine's contract is at-least-once and the HTTP client
            // resends a request whose connection went away underneath it. The
            // comparison is on the decoded rows rather than the bytes, because
            // an Avro object container file carries a random sync marker and
            // two encodings of one batch are never byte-identical.
            let mut rendered: Vec<String> = Vec::new();
            let mut previous: Option<Vec<String>> = None;
            for body in probe.finish() {
                if body.is_empty() {
                    continue;
                }
                let mut group = Vec::new();
                for batch in decode_document(format, row, &body)? {
                    group.extend(row.render(&batch, widened_unsigned)?);
                }
                if previous.as_ref() == Some(&group) {
                    continue;
                }
                rendered.extend(group.iter().cloned());
                previous = Some(group);
            }
            return Ok(rendered);
        }
        Readback::Tcp { listener, format } => {
            // The sink dialled while the run was live and its connection was
            // closed when the built service dropped, so the frames are already
            // in the socket buffer and the accept returns at once.
            match tokio::time::timeout(Duration::from_secs(5), listener.accept()).await {
                Ok(Ok((mut stream, _))) => {
                    let mut bytes = Vec::new();
                    tokio::time::timeout(
                        Duration::from_secs(10),
                        tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut bytes),
                    )
                    .await
                    .map_err(|_| "tcp readback timed out".to_string())?
                    .map_err(|e| format!("tcp readback: {e}"))?;
                    decode_frames(format, row, &bytes)?
                }
                // No dial at all is the observable "no rows" answer.
                Ok(Err(e)) => return Err(format!("tcp collector accept: {e}")),
                Err(_) => Vec::new(),
            }
        }
        Readback::Saci { probe } => {
            // Each accepted session was read to close by its own handler
            // thread, so `finish` returns whole sessions with nothing left in
            // flight. No session at all is the observable "no rows" answer,
            // as for tcp.
            let mut batches = Vec::new();
            for session in probe.finish() {
                batches.extend(decode_saci_session(&session)?);
            }
            batches
        }
        Readback::Kafka {
            brokers,
            topic,
            format,
        } => {
            let transformer = format.transformer().ok_or("kafka needs a format")?;
            let source = saci_connector_kafka::KafkaSource::new(
                saci_connector_kafka::KafkaSourceConfig {
                    brokers,
                    topic,
                    group_id: unique("matrix-readback"),
                    batch_size: 1000,
                    poll_timeout_ms: 20_000,
                    auto_offset_reset: "earliest".to_string(),
                    commit_on_drain: true,
                    stop_at_end: true,
                    key_field: None,
                    compacted: false,
                    provision: saci_connector_kafka::TopicProvision::default(),
                    properties: Default::default(),
                    schema_fields: Vec::new(),
                },
                row.schema(),
                transformer,
            )
            .map_err(|e| format!("kafka readback source: {e}"))?;
            drain(Box::new(source)).await?
        }
        Readback::Nats {
            url,
            stream,
            format,
        } => {
            let transformer = format.transformer().ok_or("nats needs a format")?;
            let source = saci_connector_nats::NatsSource::new(
                saci_connector_nats::NatsSourceConfig {
                    connection: saci_connector_nats::ConnectionConfig {
                        servers: vec![url],
                        ..Default::default()
                    },
                    mode: saci_connector_nats::SourceMode::Jetstream(Box::new(
                        saci_connector_nats::JetstreamSourceMode {
                            stream,
                            durable_name: Some(unique("r")),
                            fetch_expires_ms: 3_000,
                            ..Default::default()
                        },
                    )),
                    batch_size: 1000,
                    poll_timeout_ms: 5_000,
                    stop_at_end: true,
                    schema_fields: Vec::new(),
                },
                row.schema(),
                transformer,
            )
            .map_err(|e| format!("nats readback source: {e}"))?;
            drain(Box::new(source)).await?
        }
        Readback::Postgres { dsn, table } => {
            let client = pg_client(&dsn).await?;
            let columns = row
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>();
            let query = format!(
                "SELECT {} FROM {table} ORDER BY {}",
                columns.join(", "),
                row.postgres_cursor()
            );
            let rows = client
                .query(&query, &[])
                .await
                .map_err(|e| format!("postgres readback: {e}"))?;
            // Read straight out of SQL rather than through the connector: the
            // sink is what is under test, so the readback must not reuse it.
            return Ok(rows
                .iter()
                .map(|pg| match row {
                    RowKind::Order => format!(
                        "{}|{}|{:.4}",
                        pg.get::<_, i64>(0),
                        pg.get::<_, &str>(1),
                        pg.get::<_, f64>(2)
                    ),
                    RowKind::Ping => pg.get::<_, i64>(0).to_string(),
                    RowKind::Counter => {
                        format!("{}|{}", pg.get::<_, i64>(0), pg.get::<_, i64>(1))
                    }
                })
                .collect());
        }
        Readback::Turso { path, table, .. } => {
            let conn = turso_client(&path).await?;
            let columns = row
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>();
            let query = format!(
                "SELECT {} FROM {table} ORDER BY {}",
                columns.join(", "),
                row.postgres_cursor()
            );
            let mut rows = conn
                .query(&query, ())
                .await
                .map_err(|e| format!("turso readback: {e}"))?;
            // Read straight out of SQL rather than through the connector: the
            // sink is what is under test, so the readback must not reuse it.
            let mut out = Vec::new();
            while let Some(sql_row) = rows
                .next()
                .await
                .map_err(|e| format!("turso readback: {e}"))?
            {
                out.push(match row {
                    RowKind::Order => format!(
                        "{}|{}|{:.4}",
                        turso_i64(&sql_row, 0)?,
                        turso_text(&sql_row, 1)?,
                        turso_f64(&sql_row, 2)?
                    ),
                    RowKind::Ping => turso_i64(&sql_row, 0)?.to_string(),
                    RowKind::Counter => {
                        format!("{}|{}", turso_i64(&sql_row, 0)?, turso_i64(&sql_row, 1)?)
                    }
                });
            }
            return Ok(out);
        }
        Readback::S3 {
            connection,
            prefix,
            format,
        } => {
            let transformer = format.transformer().ok_or("s3 needs a format")?;
            let source = saci_connector_s3::S3Source::new(
                saci_connector_s3::S3SourceConfig {
                    connection,
                    prefix,
                    schema_from: if format.carries_its_own_schema() {
                        saci_connector_s3::SchemaFrom::Object
                    } else {
                        saci_connector_s3::SchemaFrom::Config
                    },
                    schema_fields: Vec::new(),
                },
                row.readback_schema(widened_unsigned),
                transformer,
            )
            .map_err(|e| format!("s3 readback source: {e}"))?;
            drain(Box::new(source)).await?
        }
    };

    let mut rendered = Vec::new();
    for batch in &batches {
        rendered.extend(row.render(batch, widened_unsigned)?);
    }
    Ok(rendered)
}

// ── Config assembly ──────────────────────────────────────────────────────────

/// The processor node, and whether it needs a runtime injected.
fn processor_kdl(case: Case, fixtures: &Fixtures) -> String {
    match case.processor {
        ProcessorKind::Native => "    wasm \"proc\" name=\"native-pipeline\"".to_string(),
        ProcessorKind::Wasm => format!("    wasm \"proc\" module=\"{}\"", kdl_path(&fixtures.wasm)),
        ProcessorKind::Plugin => format!(
            "    plugin \"proc\" library=\"{}\" {{\n        config \"smoketest.multiplier\"=\"1\"\n    }}",
            kdl_path(&fixtures.plugin)
        ),
    }
}

/// The whole config for one case.
///
/// `feeder` and `drainer` are the paired-channel workflows, declared around
/// the case's own so `build_all` builds them in that order: a build failure
/// inside the case's workflow is then the first one reported.
fn case_kdl(
    case: Case,
    source: &SourceSide,
    sink: &SinkSide,
    processor: &str,
    data_dir: &Path,
) -> String {
    let transformer = match case.format.key() {
        Some("csv") => {
            "    transformer \"fmt\" format=\"csv\" {\n        options has_headers=#true\n    }\n"
                .to_string()
        }
        Some(key) => format!("    transformer \"fmt\" format=\"{key}\"\n"),
        None => String::new(),
    };
    let run_mode = if case.stream_mode() {
        "stream"
    } else {
        "one_shot"
    };
    let feeder = source
        .paired
        .as_deref()
        .map(|w| format!("{w}\n\n"))
        .unwrap_or_default();
    let drainer = sink
        .paired
        .as_deref()
        .map(|w| format!("\n{w}\n"))
        .unwrap_or_default();
    format!(
        r#"mode "standalone"

node id=1 name="saci-matrix" data_dir="{data_dir}"

run_mode kind="{run_mode}"

{feeder}workflow "matrix" {{
{transformer}{source_kdl}

{processor}

{sink_kdl}

    link from="in" to="proc"
    link from="proc" to="out"
}}
{drainer}
http disabled=#true

observability log_level="error"
"#,
        data_dir = kdl_path(data_dir),
        source_kdl = source.kdl,
        sink_kdl = sink.kdl,
    )
}

/// How long one case may take before it is recorded as failed.
///
/// Generous: a Kafka case waits out a cold consumer-group join, and every case
/// future shares one poll loop.
const CASE_BUDGET: Duration = Duration::from_secs(180);

// ── Running one case ─────────────────────────────────────────────────────────

/// Build and run one case, then assert its expectation.
///
/// The returned report is the only channel: a failure is recorded rather than
/// panicked, so one broken combination does not hide the other 1151.
pub async fn run_case(
    case: Case,
    resources: &Resources,
    fixtures: &Fixtures,
    permits: &Semaphore,
) -> CaseReport {
    let expect = case.expect();

    for resource in case.resources() {
        if !resources.available(resource).await {
            return CaseReport {
                case,
                outcome: Outcome::Skipped,
                site: None,
                detail: format!("{} unavailable", resource.label()),
                elapsed: Duration::ZERO,
            };
        }
    }

    let _permit = permits
        .acquire()
        .await
        .expect("the matrix semaphore is never closed");

    // Timed from the permit, so the column is the case's own cost rather than
    // how long it queued behind the concurrency bound.
    let started = Instant::now();
    // Bounded, because one case that never returns holds its permit and keeps
    // `join_all` from resolving: every other case's result would be lost with
    // no report printed at all.
    let result = tokio::time::timeout(CASE_BUDGET, execute(case, &expect, resources, fixtures))
        .await
        .unwrap_or_else(|_| Err(format!("timed out after {CASE_BUDGET:?}")));
    let (outcome, detail) = match result {
        Ok(refusal) => match &expect {
            Expect::Supported => (Outcome::Supported, String::new()),
            // The message is carried, not just the predicted reason: a
            // fragment is a substring test, so a refusal that drifted to a
            // different check can still satisfy it. The report is where that
            // shows, because the case itself still passes.
            Expect::Rejected { reason, .. } if refusal.is_empty() => {
                (Outcome::Rejected, (*reason).to_string())
            }
            Expect::Rejected { reason, .. } => (Outcome::Rejected, format!("{reason}: {refusal}")),
        },
        Err(detail) => (Outcome::Failed, detail),
    };

    CaseReport {
        case,
        outcome,
        site: match &expect {
            Expect::Supported => None,
            Expect::Rejected { site, .. } => Some(*site),
        },
        detail,
        elapsed: started.elapsed(),
    }
}

/// The refusal message a rejected case matched, empty for a supported one;
/// `Err` describes what the case did instead of meeting its expectation.
async fn execute(
    case: Case,
    expect: &Expect,
    resources: &Resources,
    fixtures: &Fixtures,
) -> Result<String, String> {
    let row = case.row();
    let build_only = matches!(
        expect,
        Expect::Rejected {
            site: Site::Build,
            ..
        }
    );
    let endpoints = Endpoints::resolve(resources, !build_only).await;

    let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    // One source per case, so it is source 0 and its ids are unoffset.
    let mut source = prepare_source(case, &endpoints, !build_only, 0).await?;
    let sink = prepare_sink(case, &endpoints, !build_only).await?;
    let processor = processor_kdl(case, fixtures);
    let raw = case_kdl(case, &source, &sink, &processor, dir.path());

    let config_path = dir.path().join("matrix.kdl");
    std::fs::write(&config_path, &raw).map_err(|e| format!("write config: {e}"))?;

    // 1. Config parse, `WorkflowSpec::validate` and `ServiceConfig::validate`.
    let config = match ServiceConfig::load(&config_path) {
        Ok(config) => config,
        Err(e) => return check_build_refusal(expect, &e.to_string(), &raw),
    };

    // 2. Factory assembly plus `validate_workflow_graph`. The channel factories
    //    reach the default `ChannelRegistry` that `register_builtin_factories`
    //    attaches, so the paired halves meet inside this builder alone. The
    //    engine is the run's one engine, so a wasm case instantiates an
    //    already compiled component instead of compiling its own.
    let mut builder =
        register_builtin_factories(ServiceBuilder::new()).with_wasm_engine(fixtures.engine.clone());
    if case.processor == ProcessorKind::Native {
        builder = builder.with_runtime("proc", Box::new(row.native_pipeline()));
    }
    let mut services = match builder.build_all(&config) {
        Ok(services) => services,
        Err(e) => return check_build_refusal(expect, &e.to_string(), &raw),
    };
    if build_only {
        return Err(format!(
            "expected a build-time refusal, but the service built\n--- config ---\n{raw}"
        ));
    }
    // `build_all` preserves declaration order: the feeder, then the case, then
    // the drainer.
    let case_index = usize::from(source.paired.is_some());
    if case_index >= services.len() {
        return Err(format!("build_all returned {} service(s)", services.len()));
    }
    let built = services.remove(case_index);

    // 3. Run. A paired workflow gets its own token: the drainer's
    //    `ChannelSource` only reaches EOF once the case's service has dropped
    //    its `ChannelSink`, so cancelling both at once would race the drain.
    let cancel = CancellationToken::new();
    let helper_cancel = CancellationToken::new();
    let live = Arc::new(RwLock::new(StandaloneStats::default()));
    let helpers = futures::future::join_all(
        services
            .into_iter()
            .map(|service| run_standalone(service, &config, helper_cancel.clone(), None, None)),
    );
    let runner = run_standalone(
        built,
        &config,
        cancel.clone(),
        Some(Arc::clone(&live)),
        None,
    );
    let (result, helper_results) = if case.stream_mode() {
        let (bind, frames) = source
            .tcp
            .clone()
            .ok_or("a stream-mode case must be tcp- or saci-sourced")?;
        // What the runner must report before the stream may be cancelled. A
        // refused stream-mode case is always refused by its sink now that the
        // tcp source's own refusal lands at build, and a sink refusal still
        // drains every row and reports an error instead of writing. The sink
        // target is one item short of the frame count, because the live
        // snapshot never shows the last item's write.
        let targets = match expect {
            Expect::Supported => StreamTargets {
                rows: case.expected_rows(),
                batches: (frames.len() as u64).saturating_sub(1),
                errors: 0,
            },
            Expect::Rejected { .. } => StreamTargets {
                rows: 3,
                batches: 0,
                errors: 1,
            },
        };
        let driver = drive_tcp_stream(bind, frames, Arc::clone(&live), targets, cancel.clone());
        let (result, helper_results, driven) = tokio::join!(runner, helpers, driver);
        driven?;
        (result, helper_results)
    } else {
        tokio::join!(runner, helpers)
    };
    for helper in helper_results {
        helper.map_err(|e| format!("a paired channel workflow failed: {e}"))?;
    }
    let stats = result.map_err(|e| format!("run failed: {e}"))?;
    let seen = format!(
        "iterations={} rows={} source_batches={} sink_batches={} errors={}",
        stats.iterations,
        stats.rows_processed,
        stats.source_batches_drained,
        stats.sink_batches_written,
        stats.iteration_errors
    );

    // 4. Compare what arrived.
    // Only Avro's own type system forces `Ping.seq` to come back widened.
    let widened = case.sink.surface() == Surface::Stream
        && case.format == Format::Avro
        && row == RowKind::Ping;
    let rows = read_back(sink.readback, row, widened).await?;
    let expected = row.expected(0);
    match expect {
        Expect::Supported => {
            if stats.iteration_errors != 0 {
                return Err(format!(
                    "{} non-fatal error(s) during the run; sink holds {rows:?} [{seen}]\
                     \n--- config ---\n{raw}",
                    stats.iteration_errors
                ));
            }
            if rows != expected {
                return Err(format!(
                    "sink holds {rows:?}, expected {expected:?} [{seen}]\
                     \n--- config ---\n{raw}"
                ));
            }
            Ok(String::new())
        }
        Expect::Rejected {
            site,
            reason,
            fragment,
        } => {
            if !rows.is_empty() {
                return Err(format!(
                    "expected no rows ({reason}), sink holds {rows:?} [{seen}]\
                     \n--- config ---\n{raw}"
                ));
            }
            match site {
                Site::Build => unreachable!("a build refusal returns before the run"),
                Site::Run if stats.iteration_errors == 0 => Err(format!(
                    "expected the runner to report an error ({reason}), it reported none [{seen}]\
                     \n--- config ---\n{raw}"
                )),
                // The runner counted the error but kept no message, so the
                // refusal named in the capability table is asserted against
                // the connector that raises it.
                Site::Run if !fragment.is_empty() => {
                    let probe = source
                        .probe
                        .take()
                        .ok_or("a run refusal naming a fragment needs a source probe")?;
                    let message = probe.refusal().await?;
                    if message.contains(fragment) {
                        Ok(message)
                    } else {
                        Err(format!(
                            "refused at run for the wrong reason: expected {reason:?} \
                             (message containing {fragment:?}), got: {message} [{seen}]\
                             \n--- config ---\n{raw}"
                        ))
                    }
                }
                // The site alone is the assertion, and the runner kept no
                // message to report.
                Site::Run => Ok(String::new()),
            }
        }
    }
}

/// A build-time error is the answer only when the case expected one, with the
/// message the capability table predicted. The matched message is returned so
/// the report can print what actually refused the case.
fn check_build_refusal(expect: &Expect, message: &str, raw: &str) -> Result<String, String> {
    match expect {
        Expect::Rejected {
            site: Site::Build,
            fragment,
            reason,
        } => {
            if message.contains(fragment) {
                Ok(message.to_string())
            } else {
                Err(format!(
                    "refused at build for the wrong reason: expected {reason:?} \
                     (message containing {fragment:?}), got: {message}\n--- config ---\n{raw}"
                ))
            }
        }
        _ => Err(format!(
            "unexpected build failure: {message}\n--- config ---\n{raw}"
        )),
    }
}

/// What the runner must have reported before a stream-mode case is cancelled.
#[derive(Debug, Clone, Copy)]
struct StreamTargets {
    rows: u64,
    batches: u64,
    errors: u64,
}

impl StreamTargets {
    fn met(&self, stats: &StandaloneStats) -> bool {
        stats.rows_processed >= self.rows
            && stats.sink_batches_written >= self.batches
            && stats.iteration_errors >= self.errors
    }
}

/// Wait until `predicate` holds against the runner's live stats, or `budget`
/// elapses. Timing out is not an error here: what the runner did or did not
/// report is the case's own answer.
async fn await_stats(
    live: &Arc<RwLock<StandaloneStats>>,
    budget: Duration,
    predicate: impl Fn(&StandaloneStats) -> bool,
) {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if predicate(&*live.read().await) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Feed a `tcp` source, wait until the runner has reported `targets`, then
/// cancel.
///
/// The listener is bound by the factory, so the connection waits in the backlog
/// until the accept loop starts. The source flushes its decoder once per frame,
/// so the item count equals the frame count however the frames are batched on
/// the wire.
///
/// `run_stream` publishes its live snapshot after an item, but only once
/// `PUBLISH_INTERVAL` (100 ms) has elapsed since the last publish, plus once at
/// exit. A stream fed three frames back to back finishes each item well inside
/// that window, so the throttle skips the publishes and the counters sit one or
/// two items behind whatever the runner has actually done — no amount of
/// waiting advances them, because no further item is coming. `targets` is set
/// one item short for that reason, and the last item is given a settle window
/// rather than being waited for.
async fn drive_tcp_stream(
    bind: String,
    frames: Vec<Vec<u8>>,
    live: Arc<RwLock<StandaloneStats>>,
    targets: StreamTargets,
    cancel: CancellationToken,
) -> Result<(), String> {
    use tokio::io::AsyncWriteExt as _;

    let connected = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::net::TcpStream::connect(&bind),
    )
    .await;
    let mut producer = match connected {
        Ok(Ok(stream)) => stream,
        Ok(Err(e)) => {
            cancel.cancel();
            return Err(format!("connect to the tcp source at {bind}: {e}"));
        }
        Err(_) => {
            cancel.cancel();
            return Err(format!("connecting to the tcp source at {bind} timed out"));
        }
    };
    for frame in &frames {
        if let Err(e) = producer.write_all(frame).await {
            cancel.cancel();
            return Err(format!("write frame: {e}"));
        }
    }
    let _ = producer.flush().await;

    await_stats(&live, Duration::from_secs(30), |stats| targets.met(stats)).await;
    // The last item is invisible in the snapshot, so it gets a window rather
    // than a wait. Wide enough to survive a loaded host: every case future
    // shares one poll loop with a WASM or plugin call in it.
    tokio::time::sleep(Duration::from_secs(4)).await;
    drop(producer);
    cancel.cancel();
    Ok(())
}

// ── Maximal workflow ─────────────────────────────────────────────────────────

/// What the one maximal workflow declared and delivered.
pub struct MaximalReport {
    pub nodes: usize,
    pub links: usize,
    pub sources: Vec<String>,
    pub sinks: Vec<String>,
    pub processors: Vec<String>,
    pub excluded: Vec<String>,
    pub delivered: Vec<(String, usize)>,
    pub elapsed: Duration,
}

impl MaximalReport {
    pub fn print(&self) {
        println!("\n=== maximal workflow ===");
        println!("nodes      : {}", self.nodes);
        println!("links      : {}", self.links);
        println!("sources    : {}", self.sources.join(", "));
        println!("processors : {}", self.processors.join(", "));
        println!("sinks      : {}", self.sinks.join(", "));
        for (node, rows) in &self.delivered {
            println!("delivered  : {node} -> {rows} row(s)");
        }
        for note in &self.excluded {
            println!("excluded   : {note}");
        }
        println!("elapsed    : {:.1?}", self.elapsed);
    }
}

/// One node of the maximal workflow.
struct MaxNode {
    id: String,
    kdl: String,
}

/// Build, validate and run one stream-mode workflow declaring every available
/// source, every processor runtime whose schema can participate, and every
/// available sink.
///
/// The three runtimes carry three different components, and
/// [`validate_workflow_graph`] compares schemas field for field on every link,
/// so one graph cannot fan a single source into all three processors. Each
/// processor therefore gets its own sources and its own sinks inside the one
/// workflow, which is participation rather than a node left unlinked.
///
/// Every available connector appears on both ends; a connector whose external
/// resource had no reachable container is named in the returned
/// [`MaximalReport::excluded`].
pub async fn run_maximal(
    resources: &Resources,
    fixtures: &Fixtures,
) -> Result<MaximalReport, String> {
    let started = Instant::now();
    let endpoints = Endpoints::resolve(resources, true).await;
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;

    let mut available = Vec::new();
    let mut excluded = Vec::new();
    for connector in CONNECTORS {
        match connector.resource() {
            None => available.push(connector),
            Some(resource) => {
                if resources.available(resource).await {
                    available.push(connector);
                } else {
                    excluded.push(format!(
                        "{}: {} unavailable",
                        connector.label(),
                        resource.label()
                    ));
                }
            }
        }
    }

    // Each processor's own transport assignment: a format every listed
    // connector admits, and a row type its runtime declares. Every connector
    // not excluded above appears on both ends.
    let plan: [(ProcessorKind, Format, &[Connector], &[Connector]); 3] = [
        (
            ProcessorKind::Native,
            Format::Ndjson,
            &[
                Connector::Channel,
                Connector::File,
                Connector::Redb,
                Connector::Postgresql,
                Connector::Turso,
                Connector::Saci,
            ],
            &[
                Connector::Channel,
                Connector::File,
                Connector::Redb,
                Connector::Postgresql,
                Connector::Turso,
                Connector::Saci,
            ],
        ),
        (
            ProcessorKind::Wasm,
            Format::Ndjson,
            &[
                Connector::Http,
                Connector::Kafka,
                Connector::Nats,
                Connector::S3,
            ],
            &[
                Connector::Http,
                Connector::Kafka,
                Connector::Nats,
                Connector::S3,
            ],
        ),
        (
            ProcessorKind::Plugin,
            Format::ArrowIpc,
            &[Connector::Tcp],
            &[Connector::Tcp],
        ),
    ];

    let mut transformers: Vec<String> = Vec::new();
    let mut nodes: Vec<MaxNode> = Vec::new();
    let mut links: Vec<String> = Vec::new();
    let mut source_labels = Vec::new();
    let mut sink_labels = Vec::new();
    let mut processor_labels = Vec::new();
    let mut readbacks: Vec<(String, Readback, RowKind, Vec<String>)> = Vec::new();
    let mut tcp_feeds: Vec<(String, Vec<Vec<u8>>)> = Vec::new();
    let mut paired: Vec<String> = Vec::new();
    let mut keepalive: Vec<SourceSide> = Vec::new();
    let mut expected_rows = 0usize;
    // Every source in the workflow gets its own id offset, so a sink's rows
    // name which source delivered them.
    let mut source_index = 0usize;

    for (index, (processor, format, sources, sinks)) in plan.into_iter().enumerate() {
        let row = processor.row();
        let live_sources: Vec<Connector> = sources
            .iter()
            .copied()
            .filter(|c| available.contains(c))
            .collect();
        let live_sinks: Vec<Connector> = sinks
            .iter()
            .copied()
            .filter(|c| available.contains(c))
            .collect();
        if live_sources.is_empty() || live_sinks.is_empty() {
            excluded.push(format!(
                "{}: no available source/sink pair for its {} rows",
                processor.label(),
                row.component()
            ));
            continue;
        }

        // Which sources feed this processor, and therefore which rows every
        // one of its sinks must hold.
        let mut fed_by: Vec<usize> = Vec::new();
        let fmt_id = format!("fmt{index}");
        transformers.push(match format {
            Format::Csv => format!(
                "    transformer \"{fmt_id}\" format=\"csv\" {{\n        options has_headers=#true\n    }}"
            ),
            other => format!(
                "    transformer \"{fmt_id}\" format=\"{}\"",
                other.label()
            ),
        });

        let proc_id = format!("proc{index}");
        processor_labels.push(format!("{} ({})", proc_id, processor.label()));
        nodes.push(MaxNode {
            id: proc_id.clone(),
            kdl: processor_kdl(
                Case {
                    source: Connector::Channel,
                    sink: Connector::Channel,
                    format,
                    processor,
                },
                fixtures,
            )
            .replace("\"proc\"", &format!("\"{proc_id}\"")),
        });

        for connector in live_sources {
            let case = Case {
                source: connector,
                sink: connector,
                format,
                processor,
            };
            let side = prepare_source(case, &endpoints, true, source_index).await?;
            let id = format!("in_{}_{index}", connector.label());
            if let Some((bind, frames)) = side.tcp.clone() {
                tcp_feeds.push((bind, frames));
            }
            if let Some(workflow) = side.paired.clone() {
                paired.push(workflow);
            }
            nodes.push(MaxNode {
                id: id.clone(),
                kdl: side
                    .kdl
                    .replace("\"in\"", &format!("\"{id}\""))
                    .replace("transformer=\"fmt\"", &format!("transformer=\"{fmt_id}\"")),
            });
            links.push(format!("    link from=\"{id}\" to=\"{proc_id}\""));
            source_labels.push(format!("{id} ({})", connector.label()));
            expected_rows += 3;
            fed_by.push(source_index);
            source_index += 1;
            keepalive.push(side);
        }

        for connector in live_sinks {
            let case = Case {
                source: connector,
                sink: connector,
                format,
                processor,
            };
            let side = prepare_sink(case, &endpoints, true).await?;
            let id = format!("out_{}_{index}", connector.label());
            if let Some(workflow) = side.paired.clone() {
                paired.push(workflow);
            }
            nodes.push(MaxNode {
                id: id.clone(),
                kdl: side
                    .kdl
                    .replace("\"out\"", &format!("\"{id}\""))
                    .replace("transformer=\"fmt\"", &format!("transformer=\"{fmt_id}\"")),
            });
            links.push(format!("    link from=\"{proc_id}\" to=\"{id}\""));
            sink_labels.push(format!("{id} ({})", connector.label()));
            let mut expected: Vec<String> = fed_by
                .iter()
                .flat_map(|source| row.expected(*source))
                .collect();
            expected.sort();
            readbacks.push((id, side.readback, row, expected));
        }
    }

    if nodes.is_empty() {
        return Err("no processor could be given both a source and a sink".to_string());
    }

    let body: Vec<String> = transformers
        .into_iter()
        .chain(nodes.iter().map(|n| n.kdl.clone()))
        .chain(links.iter().cloned())
        .collect();
    let raw = format!(
        r#"mode "standalone"

node id=1 name="saci-matrix-maximal" data_dir="{}"

run_mode kind="stream"

workflow "maximal" {{
{}
}}

{}
http disabled=#true

observability log_level="error"
"#,
        kdl_path(dir.path()),
        body.join("\n\n"),
        paired
            .iter()
            .map(|w| format!("{w}\n\n"))
            .collect::<String>(),
    );

    let config_path = dir.path().join("maximal.kdl");
    std::fs::write(&config_path, &raw).map_err(|e| format!("write config: {e}"))?;
    let config = ServiceConfig::load(&config_path)
        .map_err(|e| format!("the maximal config must validate: {e}\n--- config ---\n{raw}"))?;

    let mut builder =
        register_builtin_factories(ServiceBuilder::new()).with_wasm_engine(fixtures.engine.clone());
    for node in &nodes {
        if node.kdl.contains("name=\"native-pipeline\"") {
            builder = builder.with_runtime(
                node.id.clone(),
                Box::new(ProcessorKind::Native.row().native_pipeline()),
            );
        }
    }
    let mut services = builder
        .build_all(&config)
        .map_err(|e| format!("the maximal workflow must build: {e}\n--- config ---\n{raw}"))?;
    // The maximal workflow is declared first, so it is the first service.
    let built = services.remove(0);
    let node_count = built.nodes.len();

    let cancel = CancellationToken::new();
    let helper_cancel = CancellationToken::new();
    let live = Arc::new(RwLock::new(StandaloneStats::default()));
    let helpers = futures::future::join_all(
        services
            .into_iter()
            .map(|service| run_standalone(service, &config, helper_cancel.clone(), None, None)),
    );
    let runner = run_standalone(
        built,
        &config,
        cancel.clone(),
        Some(Arc::clone(&live)),
        None,
    );
    let driver = async {
        // Every producer stays open until the run is cancelled, the way
        // `drive_tcp_stream` holds its one socket. A `saci` source answers a
        // hello with an accept frame, so a producer that closed as soon as it
        // had written would take a reset back and lose the session before the
        // data frame was read.
        let mut producers = Vec::with_capacity(tcp_feeds.len());
        for (bind, frames) in &tcp_feeds {
            let mut producer = tokio::time::timeout(
                Duration::from_secs(10),
                tokio::net::TcpStream::connect(bind),
            )
            .await
            .map_err(|_| format!("connecting to {bind} timed out"))?
            .map_err(|e| format!("connect to {bind}: {e}"))?;
            for frame in frames {
                tokio::io::AsyncWriteExt::write_all(&mut producer, frame)
                    .await
                    .map_err(|e| format!("write frame: {e}"))?;
            }
            let _ = tokio::io::AsyncWriteExt::flush(&mut producer).await;
            producers.push(producer);
        }
        // Wait for every source's rows before cancelling. The ceiling is a
        // bound on that wait, not an expectation of it: reaching it is not an
        // error, and it normally is reached.
        //
        // This reads the runner's live snapshot, which `run_stream` publishes
        // after an item only once 100 ms have passed since the last publish,
        // plus once at exit. Every source here is seeded before the run, so
        // the rotation delivers all of them back to back and the last items
        // land inside one throttle window: the snapshot can stop well short
        // of `expected_rows` (measured: 12 of 24) and no amount of waiting
        // advances it, because nothing further is coming. The settle loop
        // below is what ends the run, and the assertions are made against the
        // final stats the runner returns, which are exact either way.
        //
        // Measured: the snapshot stops advancing about a second into the run,
        // after the case sweep has already had the containers warm for
        // minutes. The ceiling is what this workflow costs, so it is set for
        // a loaded host rather than for a poll window: the serial rotation
        // never pays one here, because it stalls on the first source with
        // nothing pending (`channel`, on its second visit) until cancellation.
        await_stats(&live, Duration::from_secs(60), |stats| {
            stats.rows_processed >= expected_rows as u64
        })
        .await;
        // The publish throttle hides the last items, so the run is given a
        // window in which nothing more arrives rather than being cut at the
        // first sight of the last row.
        let mut settled = 0u32;
        let mut last = 0u64;
        while settled < 20 {
            let seen = live.read().await.sink_batches_written;
            if seen == last {
                settled += 1;
            } else {
                settled = 0;
                last = seen;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        drop(producers);
        cancel.cancel();
        Ok::<(), String>(())
    };
    let (result, helper_results, driven) = tokio::join!(runner, helpers, driver);
    driven?;
    for helper in helper_results {
        helper.map_err(|e| format!("a paired channel workflow failed: {e}"))?;
    }
    let stats = result.map_err(|e| format!("the maximal run failed: {e}"))?;

    // Every sink must hold exactly the rows of every source feeding its
    // processor, and no others. Each source seeds its three rows offset by
    // `SOURCE_ID_STRIDE`, so a source that silently delivered nothing fails
    // here even when every other source's rows are present, and neither an
    // upserting PostgreSQL sink nor the HTTP readback's repeated-document rule
    // can collapse two sources onto one row. The comparison is on sorted
    // multisets because the rotation's arrival order is not part of the
    // contract.
    let mut delivered = Vec::new();
    let mut shortfalls = Vec::new();
    for (id, readback, row, expected) in readbacks {
        let mut rows = read_back(readback, row, false).await?;
        rows.sort();
        if rows != expected {
            shortfalls.push(format!("{id}: holds {rows:?}, expected {expected:?}"));
        }
        delivered.push((id, rows.len()));
    }
    // Exact, and immune to a sink-side collapse: every declared source must
    // have had its three rows ingested. A source that silently delivered
    // nothing fails here even when every sink holds rows from the others.
    if stats.rows_processed != expected_rows as u64 {
        shortfalls.push(format!(
            "ingestion: {} row(s) processed, expected {expected_rows}",
            stats.rows_processed
        ));
    }
    if !shortfalls.is_empty() || stats.iteration_errors != 0 {
        return Err(format!(
            "the maximal workflow did not deliver every source's rows to every sink: [{}]; \
             {} row(s) processed, {} error(s)\n--- config ---\n{raw}",
            shortfalls.join("; "),
            stats.rows_processed,
            stats.iteration_errors
        ));
    }
    drop(keepalive);

    Ok(MaximalReport {
        nodes: node_count,
        links: links.len(),
        sources: source_labels,
        sinks: sink_labels,
        processors: processor_labels,
        excluded,
        delivered,
        elapsed: started.elapsed(),
    })
}
