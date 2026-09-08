//! Publish the integrity example's four source streams, receive every sink's
//! output over HTTP, and prove row for row that nothing was lost, duplicated
//! into a different value, or misrouted.
//!
//! This is the example that is also a test. It publishes `Order`s to Kafka,
//! `Payment`s to NATS JetStream, `AuditRow`s to PostgreSQL and one `Catalog`
//! CSV to a file, then serves the four `HttpSink` endpoints the two services
//! post their results to, recomputes every derived field independently, and
//! exits non-zero when any assertion fails.
//!
//! ```text
//! docker compose -f examples/integrity/docker-compose.yml up -d
//! cargo run -p saci-service --example integrity_check --features service
//! # then, in two more terminals, the two services the example drives:
//! cargo run -p saci-service \
//!     --features connector-kafka,connector-nats,connector-http,connector-channel,connector-file,transformer-ndjson,transformer-csv,transformer-arrow-ipc,wasm,windows \
//!     -- serve --config examples/integrity/integrity.kdl
//! cargo run -p saci-service \
//!     --features connector-postgresql,connector-http,transformer-parquet,wasm \
//!     -- serve --config examples/integrity/integrity_audit.kdl
//! ```
//!
//! # Two services, not one
//!
//! `run_mode` is global to a config. `examples/integrity/integrity.kdl` runs
//! `kind="stream"` and holds the `ingest` and `settle` workflows; its control
//! plane is `--stream-url`. A stream-mode source that reports EOF leaves the
//! rotation permanently, which is right for the catalog `FileSource` and fatal
//! for a `PostgresSource`, so the CDC half lives in
//! `examples/integrity/integrity_audit.kdl` under `kind="interval"`, with the
//! `audit` workflow and the `--audit-url` control plane.
//!
//! # Flags
//!
//! `--duration-secs` publish window in seconds (default 120; `0` runs until
//! Ctrl-C), `--rate` iterations per second (default 40), `--ts-step-ms`
//! simulated milliseconds per published item (default 10), `--seed` (default
//! a fixed constant), `--bind` receiver address (default `127.0.0.1:9099`),
//! `--stream-url` (default `http://127.0.0.1:8088`), `--audit-url` (default
//! `http://127.0.0.1:8089`), `--kafka-brokers` (default `localhost:9092`),
//! `--nats-url` (default `nats://localhost:4222`), `--pg-dsn` (default
//! `postgres://postgres:saci@127.0.0.1:5432/saci`), `--catalog-file` (default
//! `examples/integrity/catalog.csv`, the file both services read),
//! `--drain-secs` grace after the last publish before the final verification
//! (default 20), `--window-warmup-secs` seconds at the start of simulated
//! event time whose window groups are reported but not asserted (default 45),
//! `--lifecycle-delay-secs` (default 15), `--pause-secs` (default 6),
//! `--stop-secs` (default 10), `--no-lifecycle`, `--quiet`.
//!
//! # Event time is simulated, and its speed is load bearing
//!
//! The clock starts at [`BASE_TS_MS`] on every run and advances by
//! `--ts-step-ms` per published item, orders, payments and audit rows alike,
//! so tumbling windows close continuously and the same seed reproduces the
//! same window ids. `Catalog` rows carry no timestamp at all; `Order`,
//! `Payment` and the audit rows do.
//!
//! One iteration publishes about 2.03 items (one order, a payment every
//! second iteration, an audit insert every third, an audit update every
//! fifth), so event time runs at `rate * 2.03 * ts-step-ms` milliseconds per
//! wall second. Keep that near or below 1000. The windowed processor takes
//! its watermark from the merged `ClassifiedOrder` and `Payment` streams and
//! drops anything below `watermark - 2000`; orders travel the longer path
//! (Kafka, `ingest`, the classify router, the channel bridge, `settle`) while
//! payments reach `settle` straight from NATS, so real pipeline lag turns
//! into event-time lateness multiplied by that speed. At the defaults the
//! clock runs at about 0.81 times wall time, which leaves roughly 2.5 real
//! seconds of slack. Raising `--rate` means lowering `--ts-step-ms` to match,
//! or every order arrives late, `order_count` goes to zero in every window,
//! and the only trace is the processor's late-row metric.
//!
//! Event time restarting at [`BASE_TS_MS`] on every run is what makes window
//! ids reproducible, and it is why `integrity.kdl` declares no `store "redb"`
//! block. `aggregate` keeps its watermark and its open groups in a checkpoint
//! blob the runner threads from pass to pass in memory; a blob persisted from
//! an earlier run would put the watermark a whole run ahead of the first
//! arrival, and every arrival would be beyond `allowed_lateness_ms` with no
//! window ever opening. Nothing about this run therefore survives on the
//! stream half's disk.
//!
//! # What the delivery assertions mean
//!
//! Kafka's offset commit, JetStream's acks and the `cdc_logical` slot advance
//! all happen at the start of the *next* `next_batch` call. `pause`/`resume`
//! keeps the same source object alive, so it is lossless; `stop`/`start`
//! rebuilds it, so the last in-flight batch is redelivered. Every assertion
//! here is therefore "nothing missing, values stable across duplicates", never
//! "exactly once". Duplicates are counted and reported, not failed.
//!
//! # Schema assertions
//!
//! `arrow-ipc` and `parquet` are self-describing, so `/sink/window` and
//! `/sink/audit` are decoded with no declared schema and their schemas are
//! asserted field for field. `ndjson` and `csv` carry no schema, so those two
//! are decoded against the declared contract. What proves their columns are
//! the CSV header row, the key set `arrow-json` infers from the NDJSON body,
//! and every recomputed value matching.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::routing::post;
// `random_range` lives on `RngExt` in rand 0.10, not on `Rng`.
use rand::{RngExt as _, SeedableRng as _};
use saci_inspector_wire::{ServiceLifecycleReport, WorkflowStatus};
use saci_transformer::Transformer;
use saci_transformer_arrow_ipc::ArrowIpcTransformer;
use saci_transformer_csv::CsvTransformer;
use saci_transformer_ndjson::NdjsonTransformer;
use saci_transformer_parquet::ParquetTransformer;
use tokio_util::sync::CancellationToken;

/// Simulated epoch the example's clock starts at, milliseconds.
const BASE_TS_MS: i64 = 1_700_000_000_000;
/// Tumbling window size the `settle` workflow declares.
const WINDOW_SIZE_MS: i64 = 10_000;
/// Lateness budget the `settle` workflow declares.
const ALLOWED_LATENESS_MS: i64 = 2_000;
/// Orders packed into one Kafka message. The source is configured
/// `batch_size 1` with flow control off, so one message is exactly one batch
/// and every row in it routes the same way.
const ORDERS_PER_MESSAGE: usize = 5;
/// The consumer group `integrity.kdl` gives `orders_kafka`. Deleted with the
/// topic at startup so a committed offset cannot outrank
/// `auto_offset_reset "earliest"` and skip the run's first orders.
const KAFKA_GROUP: &str = "saci-integrity";
/// Items one publish iteration emits on average: one order, a payment every
/// second iteration, an audit insert every third, an audit update every
/// fifth. `rate * this * ts_step_ms` is how fast simulated time runs.
const ITEMS_PER_ITERATION: f64 = 1.0 + 1.0 / 2.0 + 1.0 / 3.0 + 1.0 / 5.0;
/// Kafka topic the `ingest` workflow consumes.
const KAFKA_TOPIC: &str = "integrity.orders";
/// JetStream subject the `settle` workflow consumes.
const NATS_SUBJECT: &str = "integrity.payments";
/// JetStream stream the audit-free half publishes into. Provisioned by the
/// service, never by this publisher: one owner of the settings.
const NATS_STREAM: &str = "INTEGRITY";
/// The audit table the `cdc_logical` source decodes.
const AUDIT_TABLE: &str = "public.order_audit";
/// The replication slot `integrity_audit.kdl` names. Dropped at startup so a
/// re-run starts from an empty change stream.
const AUDIT_SLOT: &str = "saci_integrity_slot";

/// Windowing key and the region column of every component that carries one.
const REGIONS: [&str; 4] = ["emea", "amer", "apac", "latam"];
/// The two priorities, which are also the two branches.
const PRIORITIES: [&str; 2] = ["express", "standard"];
/// Statuses an audit row can carry. Every one of them is "ok".
const STATUSES: [&str; 4] = ["placed", "picked", "shipped", "settled"];
/// Currencies a payment can carry.
const CURRENCIES: [&str; 3] = ["EUR", "USD", "GBP"];

/// The catalog as the committed CSV carries it: the sku order rows are drawn
/// from, and the `taxable` flag every `line_total` depends on.
///
/// Read, never written. `examples/integrity/catalog.csv` is tracked, both
/// services read it through a `FileSource`, and `cargo xtask validate` opens
/// the same path, so the one file is the source of truth for all of them and
/// a run that rewrote it would dirty the working tree. Deliberately carries
/// no timestamp column, unlike every other component here.
struct Catalog {
    skus: Vec<String>,
    taxable: HashMap<String, bool>,
}

// ── derived-field rules, recomputed here independently of the processors ────

/// 64-bit FNV-1a, returned as `i64`: the checksum every component carries.
fn fnv1a64(bytes: &[u8]) -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash as i64
}

/// Round to two decimals, the rule every money field follows.
fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

/// `qty * unit_price`, plus 20% when the catalog row is taxable.
fn line_total(qty: i32, unit_price: f64, taxable: bool) -> f64 {
    let raw = f64::from(qty) * unit_price;
    round2(if taxable { raw * 1.20 } else { raw })
}

/// `"express"` for an express order, `"standard"` for everything else.
fn branch_of(priority: &str) -> &'static str {
    if priority == "express" {
        "express"
    } else {
        "standard"
    }
}

/// The `ClassifiedOrder` checksum.
#[allow(clippy::too_many_arguments)]
fn classified_checksum(
    order_id: i64,
    sku: &str,
    region: &str,
    priority: &str,
    qty: i32,
    unit_price: f64,
    total: f64,
    taxable: bool,
    branch: &str,
    event_ms: i64,
) -> i64 {
    fnv1a64(
        format!(
            "{order_id}|{sku}|{region}|{priority}|{qty}|{unit_price:.4}|{total:.2}|{taxable}|\
             {branch}|{event_ms}"
        )
        .as_bytes(),
    )
}

/// The `RegionTotal` checksum.
fn region_checksum(
    window_id: i64,
    region: &str,
    order_count: i64,
    payment_count: i64,
    order_amount: f64,
    payment_amount: f64,
) -> i64 {
    fnv1a64(
        format!(
            "{window_id}|{region}|{order_count}|{payment_count}|{order_amount:.2}|\
             {payment_amount:.2}"
        )
        .as_bytes(),
    )
}

/// `AuditVerdict.ok`: a known status, a real revision, and a change that added
/// or altered a row rather than removing one.
fn audit_ok(status: &str, revision: i32, op: &str) -> bool {
    STATUSES.contains(&status) && revision >= 1 && (op == "I" || op == "U")
}

/// The `AuditVerdict` checksum.
fn audit_checksum(
    audit_id: i64,
    order_id: i64,
    op: &str,
    status: &str,
    revision: i32,
    changed_ms: i64,
    ok: bool,
) -> i64 {
    fnv1a64(format!("{audit_id}|{order_id}|{op}|{status}|{revision}|{changed_ms}|{ok}").as_bytes())
}

/// Tumbling window id: integer division of event time by the window size.
fn window_id_of(event_ms: i64) -> i64 {
    event_ms.div_euclid(WINDOW_SIZE_MS)
}

// ── the four contract schemas ───────────────────────────────────────────────

fn required(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, false)
}

/// `ClassifiedOrder`, received at `/sink/express` and `/sink/standard`.
fn classified_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required("order_id", DataType::Int64),
        required("sku", DataType::Utf8),
        required("region", DataType::Utf8),
        required("priority", DataType::Utf8),
        required("qty", DataType::Int32),
        required("unit_price", DataType::Float64),
        required("line_total", DataType::Float64),
        required("taxable", DataType::Boolean),
        required("branch", DataType::Utf8),
        required("checksum", DataType::Int64),
        required("event_ms", DataType::Int64),
    ]))
}

/// `RegionTotal`, received at `/sink/window`.
fn region_total_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required("window_id", DataType::Int64),
        required("region", DataType::Utf8),
        required("order_count", DataType::Int64),
        required("payment_count", DataType::Int64),
        required("order_amount", DataType::Float64),
        required("payment_amount", DataType::Float64),
        required("checksum", DataType::Int64),
    ]))
}

/// `AuditVerdict`, received at `/sink/audit`.
fn audit_verdict_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        required("audit_id", DataType::Int64),
        required("order_id", DataType::Int64),
        required("op", DataType::Utf8),
        required("status", DataType::Utf8),
        required("revision", DataType::Int32),
        required("changed_ms", DataType::Int64),
        required("ok", DataType::Boolean),
        required("checksum", DataType::Int64),
    ]))
}

// ── command line ────────────────────────────────────────────────────────────

struct Args {
    duration_secs: u64,
    rate: u64,
    ts_step_ms: i64,
    seed: u64,
    bind: String,
    stream_url: String,
    audit_url: String,
    kafka_brokers: String,
    nats_url: String,
    pg_dsn: String,
    catalog_file: String,
    drain_secs: u64,
    /// Simulated event-time seconds excluded from the window assertion.
    window_warmup_secs: u32,
    lifecycle_delay_secs: u64,
    pause_secs: u64,
    stop_secs: u64,
    no_lifecycle: bool,
    quiet: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            duration_secs: 120,
            rate: 40,
            // Paired with `rate` above: see the module doc's event-time note.
            ts_step_ms: 10,
            seed: 0x5eed_1234_abcd_0007,
            bind: "127.0.0.1:9099".to_string(),
            stream_url: "http://127.0.0.1:8088".to_string(),
            audit_url: "http://127.0.0.1:8089".to_string(),
            kafka_brokers: "localhost:9092".to_string(),
            nats_url: "nats://localhost:4222".to_string(),
            pg_dsn: "postgres://postgres:saci@127.0.0.1:5432/saci".to_string(),
            catalog_file: "examples/integrity/catalog.csv".to_string(),
            drain_secs: 20,
            window_warmup_secs: 45,
            lifecycle_delay_secs: 15,
            pause_secs: 6,
            stop_secs: 10,
            no_lifecycle: false,
            quiet: false,
        }
    }
}

/// Parse `--key value` pairs.
///
/// Hand-rolled rather than clap: the binary's own CLI needs clap, but an
/// example does not, and `cargo check --examples` should not pull a derive
/// macro for it.
fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || argv.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--duration-secs" => {
                args.duration_secs = value()?
                    .parse()
                    .map_err(|e| format!("--duration-secs: {e}"))?;
            }
            "--rate" => args.rate = value()?.parse().map_err(|e| format!("--rate: {e}"))?,
            "--ts-step-ms" => {
                args.ts_step_ms = value()?.parse().map_err(|e| format!("--ts-step-ms: {e}"))?;
            }
            "--seed" => args.seed = value()?.parse().map_err(|e| format!("--seed: {e}"))?,
            "--bind" => args.bind = value()?,
            "--stream-url" => args.stream_url = value()?,
            "--audit-url" => args.audit_url = value()?,
            "--kafka-brokers" => args.kafka_brokers = value()?,
            "--nats-url" => args.nats_url = value()?,
            "--pg-dsn" => args.pg_dsn = value()?,
            "--catalog-file" => args.catalog_file = value()?,
            "--drain-secs" => {
                args.drain_secs = value()?.parse().map_err(|e| format!("--drain-secs: {e}"))?;
            }
            "--window-warmup-secs" => {
                args.window_warmup_secs = value()?
                    .parse()
                    .map_err(|e| format!("--window-warmup-secs: {e}"))?;
            }
            "--lifecycle-delay-secs" => {
                args.lifecycle_delay_secs = value()?
                    .parse()
                    .map_err(|e| format!("--lifecycle-delay-secs: {e}"))?;
            }
            "--pause-secs" => {
                args.pause_secs = value()?.parse().map_err(|e| format!("--pause-secs: {e}"))?;
            }
            "--stop-secs" => {
                args.stop_secs = value()?.parse().map_err(|e| format!("--stop-secs: {e}"))?;
            }
            "--no-lifecycle" => args.no_lifecycle = true,
            "--quiet" => args.quiet = true,
            "--help" | "-h" => {
                println!(
                    "usage: integrity_check [--duration-secs N (0 = until Ctrl-C)] [--rate N] \
                     [--ts-step-ms N] [--seed N] [--bind ADDR] [--stream-url URL] \
                     [--audit-url URL] [--kafka-brokers LIST] [--nats-url URL] [--pg-dsn DSN] \
                     [--catalog-file PATH] [--drain-secs N] \
                     [--window-warmup-secs N] \
                     [--lifecycle-delay-secs N] [--pause-secs N] [--stop-secs N] \
                     [--no-lifecycle] [--quiet]"
                );
                std::process::exit(0);
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    if args.rate == 0 {
        return Err("--rate must be at least 1".to_string());
    }
    if args.ts_step_ms <= 0 {
        return Err("--ts-step-ms must be at least 1".to_string());
    }
    Ok(args)
}

// ── what was published ──────────────────────────────────────────────────────

struct Order {
    order_id: i64,
    sku: String,
    qty: i32,
    unit_price: f64,
    region: String,
    priority: &'static str,
    event_ms: i64,
}

impl Order {
    fn ndjson(&self) -> String {
        let Self {
            order_id,
            sku,
            qty,
            unit_price,
            region,
            priority,
            event_ms,
        } = self;
        format!(
            r#"{{"order_id":{order_id},"sku":"{sku}","qty":{qty},"unit_price":{unit_price:.4},"region":"{region}","priority":"{priority}","event_ms":{event_ms}}}"#
        )
    }
}

/// A published payment. The id is the map key, so it is not repeated here.
struct Payment {
    amount: f64,
    region: String,
    event_ms: i64,
}

/// One change made to `public.order_audit`, and therefore one row the CDC
/// source must deliver.
struct AuditChange {
    audit_id: i64,
    order_id: i64,
    op: &'static str,
    status: &'static str,
    revision: i32,
    changed_ms: i64,
}

// ── what was received ───────────────────────────────────────────────────────

#[derive(Default, Clone, Copy)]
struct SinkStats {
    batches: u64,
    rows: u64,
    unique: u64,
    duplicates: u64,
    mismatches: u64,
}

/// The values a `ClassifiedOrder` first arrived with, kept so a redelivery can
/// be checked for stability.
struct SeenOrder {
    sink: &'static str,
    line_total: f64,
    taxable: bool,
    branch: String,
    checksum: i64,
}

/// Every distinct emission the window sink delivered for one
/// `(window_id, region)`, summed. A re-fire carries the delta rather than a
/// restated total, so the sum is what the published rows must equal.
#[derive(Default)]
struct SeenWindow {
    emissions: u32,
    /// Rows that arrived inside a body already delivered once, so they were
    /// a `RetryingSink` re-send and were not summed.
    retried: u32,
    order_count: i64,
    payment_count: i64,
    order_amount: f64,
    payment_amount: f64,
}

struct SeenAudit {
    order_id: i64,
    status: String,
    revision: i32,
    changed_ms: i64,
    ok: bool,
    checksum: i64,
}

/// Everything the run knows: what was published, what came back, and every
/// disagreement between the two.
#[derive(Default)]
struct Registry {
    orders: HashMap<i64, Order>,
    /// Digests of window bodies already received, so a `RetryingSink`
    /// re-send is not summed into a group's totals twice.
    window_bodies: HashSet<u64>,
    payments: HashMap<i64, Payment>,
    /// Keyed by `(audit_id, op)`: an insert and its later update are two
    /// distinct changes, and a redelivery of either maps onto the same key.
    audits: HashMap<(i64, &'static str), AuditChange>,
    max_event_ms: i64,
    /// `(window_id, region)` groups whose window is definitely closed, filled
    /// by the final verification and reported as the window sink's expectation.
    closed_window_groups: u64,
    /// Closed groups excluded from the window assertion because their event
    /// time falls inside the fan-in warm-up.
    warmup_window_groups: u64,

    sinks: BTreeMap<&'static str, SinkStats>,
    seen_orders: HashMap<i64, SeenOrder>,
    seen_windows: HashMap<(i64, String), SeenWindow>,
    seen_audits: HashMap<(i64, String), SeenAudit>,
    schemas_checked: HashSet<&'static str>,
    types_seen: BTreeSet<String>,
    failures: Vec<String>,
    failure_count: u64,
    /// Mirrors `--quiet`: suppresses the immediate first-failure line.
    quiet: bool,
}

/// Failures kept verbatim; the rest are counted only.
const MAX_KEPT_FAILURES: usize = 40;

impl Registry {
    /// Record a disagreement. The first one is also printed at once: the
    /// final report is minutes away, and "verify continuously so a failure
    /// surfaces early" is worth nothing if the operator only learns what
    /// broke after the run.
    fn fail(&mut self, message: String) {
        self.failure_count += 1;
        if self.failure_count == 1 && !self.quiet {
            eprintln!("first failure: {message}");
        }
        if self.failures.len() < MAX_KEPT_FAILURES {
            self.failures.push(message);
        }
    }

    fn stats(&mut self, sink: &'static str) -> &mut SinkStats {
        self.sinks.entry(sink).or_default()
    }
}

/// The receiver's shared state: the catalog is immutable, everything else is
/// behind one lock, which is uncontended at these rates.
struct Verifier {
    /// The committed catalog, read at startup. `taxable` is the one fact a
    /// derived field depends on; a sku the catalog never listed is untaxed.
    catalog: Catalog,
    registry: Mutex<Registry>,
    /// Bodies answered, so the operator sees progress before the final report.
    requests: AtomicU64,
}

impl Verifier {
    fn taxable(&self, sku: &str) -> bool {
        self.catalog.taxable.get(sku).copied().unwrap_or(false)
    }
}

// ── decoding a sink body through the real transformers ──────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
enum SinkKind {
    Express,
    Standard,
    Window,
    Audit,
}

impl SinkKind {
    fn label(self) -> &'static str {
        match self {
            Self::Express => "express",
            Self::Standard => "standard",
            Self::Window => "window",
            Self::Audit => "audit",
        }
    }

    fn schema(self) -> Arc<Schema> {
        match self {
            Self::Express | Self::Standard => classified_schema(),
            Self::Window => region_total_schema(),
            Self::Audit => audit_verdict_schema(),
        }
    }

    /// The transformer, and the schema to declare. `arrow-ipc` and `parquet`
    /// are given none, so each body's own schema governs the read.
    fn reader_setup(self) -> (Box<dyn Transformer>, Option<Arc<Schema>>) {
        match self {
            Self::Express => (
                Box::new(NdjsonTransformer::default()),
                Some(classified_schema()),
            ),
            // `HttpSink` writes through the stream surface, which emits the
            // header row `has_headers` asks for; the message surface would
            // not.
            Self::Standard => (
                Box::new(CsvTransformer::new(true)),
                Some(classified_schema()),
            ),
            Self::Window => (Box::new(ArrowIpcTransformer::new()), None),
            Self::Audit => (Box::new(ParquetTransformer::new()), None),
        }
    }
}

/// Spool the body to a temp file and pull every batch out of it.
///
/// `open_reader` takes a `std::fs::File` because Parquet reads its footer
/// before any row group, so an in-memory body has to land on disk first. This
/// is what `HttpSource` does with a response body.
fn decode(sink: SinkKind, body: &[u8]) -> Result<(Arc<Schema>, Vec<RecordBatch>), String> {
    let (transformer, declared) = sink.reader_setup();
    let mut tmp =
        tempfile::NamedTempFile::new().map_err(|e| format!("temp file for the body: {e}"))?;
    tmp.write_all(body)
        .and_then(|()| tmp.flush())
        .map_err(|e| format!("writing the body to a temp file: {e}"))?;
    let file = tmp
        .reopen()
        .map_err(|e| format!("reopening the spooled body: {e}"))?;

    let mut reader = transformer
        .open_reader(file, declared)
        .map_err(|e| format!("{}: open_reader: {e}", sink.label()))?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    while let Some(batch) = reader
        .next_batch()
        .map_err(|e| format!("{}: next_batch: {e}", sink.label()))?
    {
        batches.push(batch);
    }
    Ok((schema, batches))
}

/// Compare a decoded schema with the contract, field for field.
fn schema_drift(actual: &Schema, expected: &Schema) -> Option<String> {
    if actual.fields().len() != expected.fields().len() {
        return Some(format!(
            "field count {} != {}",
            actual.fields().len(),
            expected.fields().len()
        ));
    }
    for (got, want) in actual.fields().iter().zip(expected.fields()) {
        if got.name() != want.name()
            || got.data_type() != want.data_type()
            || got.is_nullable() != want.is_nullable()
        {
            return Some(format!(
                "field {:?} ({:?}, nullable={}) != {:?} ({:?}, nullable={})",
                got.name(),
                got.data_type(),
                got.is_nullable(),
                want.name(),
                want.data_type(),
                want.is_nullable()
            ));
        }
    }
    None
}

fn i64_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int64Array, String> {
    typed_col::<Int64Array>(batch, name, "Int64")
}

fn i32_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Int32Array, String> {
    typed_col::<Int32Array>(batch, name, "Int32")
}

fn f64_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a Float64Array, String> {
    typed_col::<Float64Array>(batch, name, "Float64")
}

fn str_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, String> {
    typed_col::<StringArray>(batch, name, "Utf8")
}

fn bool_col<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a BooleanArray, String> {
    typed_col::<BooleanArray>(batch, name, "Boolean")
}

fn typed_col<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    name: &str,
    want: &str,
) -> Result<&'a T, String> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|e| format!("column {name}: {e}"))?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("column {name} is not {want}"))
}

// ── verification of one received body ───────────────────────────────────────

fn verify_body(verifier: &Verifier, sink: SinkKind, body: &[u8]) {
    let label = sink.label();
    let decoded = decode(sink, body);
    let (schema, batches) = match decoded {
        Ok(pair) => pair,
        Err(message) => {
            let mut registry = verifier.registry.lock().expect("registry lock");
            registry.stats(label).batches += 1;
            registry.fail(format!("{label}: {message}"));
            return;
        }
    };

    let mut registry = verifier.registry.lock().expect("registry lock");
    registry.stats(label).batches += 1;

    if registry.schemas_checked.insert(label) {
        let expected = sink.schema();
        if let Some(drift) = schema_drift(&schema, &expected) {
            registry.fail(format!("{label}: schema drift: {drift}"));
        }
        for field in expected.fields() {
            registry.types_seen.insert(field.data_type().to_string());
        }
        if sink == SinkKind::Standard {
            check_csv_header(&mut registry, body, &expected);
        }
        if sink == SinkKind::Express {
            check_ndjson_field_names(&mut registry, body, &expected);
        }
    }

    // A `RetryingSink` re-sends the whole body byte for byte, so identity of
    // the body is what separates a retry from a genuine second firing.
    let repeat_body = sink == SinkKind::Window && !registry.window_bodies.insert(body_digest(body));

    for batch in &batches {
        let outcome = match sink {
            SinkKind::Express => verify_classified(verifier, &mut registry, "express", batch),
            SinkKind::Standard => verify_classified(verifier, &mut registry, "standard", batch),
            SinkKind::Window => verify_window(&mut registry, batch, repeat_body),
            SinkKind::Audit => verify_audit(&mut registry, batch),
        };
        if let Err(message) = outcome {
            registry.fail(format!("{label}: {message}"));
        }
    }
}

/// The CSV body's own header row names the columns and their order, which is
/// the one thing decoding against a declared schema cannot prove.
fn check_csv_header(registry: &mut Registry, body: &[u8], expected: &Schema) {
    let header = String::from_utf8_lossy(body);
    let first = header.lines().next().unwrap_or_default().trim_end();
    let want = expected
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect::<Vec<_>>()
        .join(",");
    if first != want {
        registry.fail(format!("standard: csv header {first:?} != {want:?}"));
    }
}

/// Decode the NDJSON body a second time with no declared schema: the keys
/// `arrow-json` finds are the keys the body really carries, which is what
/// stops a body from being silently reinterpreted through the declared
/// schema. Inference sorts the field names, so this proves the key *set*;
/// the order is proved instead by every recomputed value matching.
fn check_ndjson_field_names(registry: &mut Registry, body: &[u8], expected: &Schema) {
    let inferred = match decode_inferred_ndjson(body) {
        Ok(schema) => schema,
        Err(message) => {
            registry.fail(format!("express: ndjson inference: {message}"));
            return;
        }
    };
    let got = inferred
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<BTreeSet<_>>();
    let want = expected
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect::<BTreeSet<_>>();
    if got != want {
        registry.fail(format!("express: ndjson keys {got:?} != {want:?}"));
    }
}

fn decode_inferred_ndjson(body: &[u8]) -> Result<Arc<Schema>, String> {
    let mut tmp = tempfile::NamedTempFile::new().map_err(|e| e.to_string())?;
    tmp.write_all(body)
        .and_then(|()| tmp.flush())
        .map_err(|e| e.to_string())?;
    let file = tmp.reopen().map_err(|e| e.to_string())?;
    let reader = NdjsonTransformer::default()
        .open_reader(file, None)
        .map_err(|e| e.to_string())?;
    Ok(reader.schema())
}

fn verify_classified(
    verifier: &Verifier,
    registry: &mut Registry,
    sink: &'static str,
    batch: &RecordBatch,
) -> Result<(), String> {
    let order_id = i64_col(batch, "order_id")?;
    let sku = str_col(batch, "sku")?;
    let region = str_col(batch, "region")?;
    let priority = str_col(batch, "priority")?;
    let qty = i32_col(batch, "qty")?;
    let unit_price = f64_col(batch, "unit_price")?;
    let line = f64_col(batch, "line_total")?;
    let taxable = bool_col(batch, "taxable")?;
    let branch = str_col(batch, "branch")?;
    let checksum = i64_col(batch, "checksum")?;
    let event_ms = i64_col(batch, "event_ms")?;

    for row in 0..batch.num_rows() {
        registry.stats(sink).rows += 1;
        let id = order_id.value(row);

        // Routing: the sink a row lands at is the branch it must carry, and
        // the branch is a function of the priority it was published with.
        let got_branch = branch.value(row);
        let got_priority = priority.value(row);
        if got_branch != sink || got_priority != sink {
            registry.fail(format!(
                "{sink}: order {id} arrived at the wrong sink: branch={got_branch:?} \
                 priority={got_priority:?}"
            ));
            registry.stats(sink).mismatches += 1;
        }

        let Some(published) = registry.orders.get(&id) else {
            registry.fail(format!("{sink}: order {id} was never published"));
            registry.stats(sink).mismatches += 1;
            continue;
        };
        let want_taxable = verifier.taxable(&published.sku);
        let want_branch = branch_of(published.priority);
        let want_total = line_total(published.qty, published.unit_price, want_taxable);
        let want_checksum = classified_checksum(
            published.order_id,
            &published.sku,
            &published.region,
            published.priority,
            published.qty,
            published.unit_price,
            want_total,
            want_taxable,
            want_branch,
            published.event_ms,
        );

        let mut wrong = Vec::new();
        if sku.value(row) != published.sku {
            wrong.push(format!("sku {:?} != {:?}", sku.value(row), published.sku));
        }
        if region.value(row) != published.region {
            wrong.push(format!(
                "region {:?} != {:?}",
                region.value(row),
                published.region
            ));
        }
        if qty.value(row) != published.qty {
            wrong.push(format!("qty {} != {}", qty.value(row), published.qty));
        }
        if (unit_price.value(row) - published.unit_price).abs() > 1e-9 {
            wrong.push(format!(
                "unit_price {} != {}",
                unit_price.value(row),
                published.unit_price
            ));
        }
        if event_ms.value(row) != published.event_ms {
            wrong.push(format!(
                "event_ms {} != {}",
                event_ms.value(row),
                published.event_ms
            ));
        }
        if (line.value(row) - want_total).abs() > 1e-6 {
            wrong.push(format!("line_total {} != {want_total}", line.value(row)));
        }
        if taxable.value(row) != want_taxable {
            wrong.push(format!("taxable {} != {want_taxable}", taxable.value(row)));
        }
        if got_branch != want_branch {
            wrong.push(format!("branch {got_branch:?} != {want_branch:?}"));
        }
        if checksum.value(row) != want_checksum {
            wrong.push(format!(
                "checksum {} != {want_checksum}",
                checksum.value(row)
            ));
        }
        if !wrong.is_empty() {
            registry.fail(format!("{sink}: order {id}: {}", wrong.join("; ")));
            registry.stats(sink).mismatches += 1;
        }

        // At-least-once is legal; changing values across a redelivery is not.
        let previous = registry.seen_orders.get(&id).map(|seen| {
            let unstable = (seen.line_total - line.value(row)).abs() > 1e-9
                || seen.taxable != taxable.value(row)
                || seen.branch != got_branch
                || seen.checksum != checksum.value(row)
                || seen.sink != sink;
            (unstable, seen.sink)
        });
        match previous {
            Some((unstable, first_sink)) => {
                registry.stats(sink).duplicates += 1;
                if unstable {
                    registry.fail(format!(
                        "{sink}: order {id} redelivered with different values (first seen at \
                         {first_sink})"
                    ));
                    registry.stats(sink).mismatches += 1;
                }
            }
            None => {
                registry.stats(sink).unique += 1;
                registry.seen_orders.insert(
                    id,
                    SeenOrder {
                        sink,
                        line_total: line.value(row),
                        taxable: taxable.value(row),
                        branch: got_branch.to_string(),
                        checksum: checksum.value(row),
                    },
                );
            }
        }
    }
    Ok(())
}

/// Identity of one received body, used only to spot a `RetryingSink`
/// re-send. Not a security hash: it compares bodies this process just
/// received against each other.
fn body_digest(body: &[u8]) -> u64 {
    use std::hash::{Hash as _, Hasher as _};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    body.hash(&mut hasher);
    hasher.finish()
}

fn verify_window(
    registry: &mut Registry,
    batch: &RecordBatch,
    repeat_body: bool,
) -> Result<(), String> {
    let window_id = i64_col(batch, "window_id")?;
    let region = str_col(batch, "region")?;
    let order_count = i64_col(batch, "order_count")?;
    let payment_count = i64_col(batch, "payment_count")?;
    let order_amount = f64_col(batch, "order_amount")?;
    let payment_amount = f64_col(batch, "payment_amount")?;
    let checksum = i64_col(batch, "checksum")?;

    for row in 0..batch.num_rows() {
        registry.stats("window").rows += 1;
        let id = window_id.value(row);
        let name = region.value(row);

        // The one thing that is checkable per row: this row's own checksum
        // over this row's own six fields.
        let want = region_checksum(
            id,
            name,
            order_count.value(row),
            payment_count.value(row),
            round2(order_amount.value(row)),
            round2(payment_amount.value(row)),
        );
        if checksum.value(row) != want {
            registry.fail(format!(
                "window: {id}/{name} checksum {} != {want} for ({}, {}, {:.2}, {:.2})",
                checksum.value(row),
                order_count.value(row),
                payment_count.value(row),
                order_amount.value(row),
                payment_amount.value(row),
            ));
            registry.stats("window").mismatches += 1;
        }

        // Two different things can put the same `(window_id, region)` on the
        // wire twice, and they need opposite treatment.
        //
        // A re-fire is new information: `retain` drops the group as it
        // emits, so a row arriving after the window closed recreates the
        // group empty and the next emission carries only the delta, never a
        // restated total. Those must be summed.
        //
        // A `RetryingSink` retry is the same information twice: every sink
        // is wrapped, so a refused POST is re-sent. Summing it would
        // over-count. A retry re-sends the whole body byte for byte, which
        // is why `repeat_body` is decided once per request rather than per
        // row: deduplicating on the row checksum instead would silently drop
        // two genuine re-fires that happened to carry an identical delta,
        // since `RegionTotal` carries nothing that distinguishes one firing
        // from another.
        let key = (id, name.to_string());
        if repeat_body {
            registry.seen_windows.entry(key).or_default().retried += 1;
            registry.stats("window").duplicates += 1;
        } else {
            let entry = registry.seen_windows.entry(key).or_default();
            let first = entry.emissions == 0;
            entry.emissions += 1;
            entry.order_count += order_count.value(row);
            entry.payment_count += payment_count.value(row);
            entry.order_amount += order_amount.value(row);
            entry.payment_amount += payment_amount.value(row);
            if first {
                registry.stats("window").unique += 1;
            } else {
                registry.stats("window").duplicates += 1;
            }
        }
    }
    Ok(())
}

fn verify_audit(registry: &mut Registry, batch: &RecordBatch) -> Result<(), String> {
    let audit_id = i64_col(batch, "audit_id")?;
    let order_id = i64_col(batch, "order_id")?;
    let op = str_col(batch, "op")?;
    let status = str_col(batch, "status")?;
    let revision = i32_col(batch, "revision")?;
    let changed_ms = i64_col(batch, "changed_ms")?;
    let ok = bool_col(batch, "ok")?;
    let checksum = i64_col(batch, "checksum")?;

    for row in 0..batch.num_rows() {
        registry.stats("audit").rows += 1;
        let id = audit_id.value(row);
        let got_op = op.value(row);
        let key = (id, got_op.to_string());

        let published = match got_op {
            "I" => registry.audits.get(&(id, "I")),
            "U" => registry.audits.get(&(id, "U")),
            other => {
                registry.fail(format!("audit: {id} carries an unknown op {other:?}"));
                registry.stats("audit").mismatches += 1;
                continue;
            }
        };
        let Some(published) = published else {
            registry.fail(format!(
                "audit: {id} op {got_op:?} was never applied to {AUDIT_TABLE}"
            ));
            registry.stats("audit").mismatches += 1;
            continue;
        };

        let want_ok = audit_ok(published.status, published.revision, published.op);
        let want_checksum = audit_checksum(
            published.audit_id,
            published.order_id,
            published.op,
            published.status,
            published.revision,
            published.changed_ms,
            want_ok,
        );
        let mut wrong = Vec::new();
        if order_id.value(row) != published.order_id {
            wrong.push(format!(
                "order_id {} != {}",
                order_id.value(row),
                published.order_id
            ));
        }
        if status.value(row) != published.status {
            wrong.push(format!(
                "status {:?} != {:?}",
                status.value(row),
                published.status
            ));
        }
        if revision.value(row) != published.revision {
            wrong.push(format!(
                "revision {} != {}",
                revision.value(row),
                published.revision
            ));
        }
        if changed_ms.value(row) != published.changed_ms {
            wrong.push(format!(
                "changed_ms {} != {}",
                changed_ms.value(row),
                published.changed_ms
            ));
        }
        if ok.value(row) != want_ok {
            wrong.push(format!("ok {} != {want_ok}", ok.value(row)));
        }
        if checksum.value(row) != want_checksum {
            wrong.push(format!(
                "checksum {} != {want_checksum}",
                checksum.value(row)
            ));
        }
        if !wrong.is_empty() {
            registry.fail(format!("audit: {id} op {got_op}: {}", wrong.join("; ")));
            registry.stats("audit").mismatches += 1;
        }

        let incoming = SeenAudit {
            order_id: order_id.value(row),
            status: status.value(row).to_string(),
            revision: revision.value(row),
            changed_ms: changed_ms.value(row),
            ok: ok.value(row),
            checksum: checksum.value(row),
        };
        let previous = registry.seen_audits.get(&key).map(|seen| {
            seen.order_id != incoming.order_id
                || seen.status != incoming.status
                || seen.revision != incoming.revision
                || seen.changed_ms != incoming.changed_ms
                || seen.ok != incoming.ok
                || seen.checksum != incoming.checksum
        });
        match previous {
            Some(unstable) => {
                registry.stats("audit").duplicates += 1;
                if unstable {
                    registry.fail(format!(
                        "audit: {id} op {got_op} redelivered with different values"
                    ));
                    registry.stats("audit").mismatches += 1;
                }
            }
            None => {
                registry.stats("audit").unique += 1;
                registry.seen_audits.insert(key, incoming);
            }
        }
    }
    Ok(())
}

// ── the receiver ────────────────────────────────────────────────────────────

/// Every handler answers `200` with an empty body, even for a body that failed
/// verification: a non-2xx would fail the sink's `write_batch`, and the point
/// of the run is to observe what the service delivers, not to break it.
async fn receive(verifier: Arc<Verifier>, sink: SinkKind, body: Bytes) -> StatusCode {
    let handled = tokio::task::spawn_blocking(move || {
        verify_body(&verifier, sink, &body);
        verifier.requests.fetch_add(1, Ordering::Relaxed);
    })
    .await;
    if let Err(e) = handled {
        eprintln!("receiver: verification task failed: {e}");
    }
    StatusCode::OK
}

fn router(verifier: Arc<Verifier>) -> Router {
    Router::new()
        .route(
            "/sink/express",
            post(
                |State(state): State<Arc<Verifier>>, body: Bytes| async move {
                    receive(state, SinkKind::Express, body).await
                },
            ),
        )
        .route(
            "/sink/standard",
            post(
                |State(state): State<Arc<Verifier>>, body: Bytes| async move {
                    receive(state, SinkKind::Standard, body).await
                },
            ),
        )
        .route(
            "/sink/window",
            post(
                |State(state): State<Arc<Verifier>>, body: Bytes| async move {
                    receive(state, SinkKind::Window, body).await
                },
            ),
        )
        .route(
            "/sink/audit",
            post(
                |State(state): State<Arc<Verifier>>, body: Bytes| async move {
                    receive(state, SinkKind::Audit, body).await
                },
            ),
        )
        // axum caps a `Bytes` extractor at 2 MiB and answers 413 *before* the
        // handler runs, which would break the "never answer non-2xx" rule the
        // moment a workflow resumes and drains a whole backlog into one body.
        .layer(DefaultBodyLimit::disable())
        .with_state(verifier)
}

// ── lifecycle driving ───────────────────────────────────────────────────────

struct LifecycleStep {
    name: String,
    outcome: String,
    ok: bool,
}

struct Lifecycle {
    client: reqwest::Client,
    stream_url: String,
    audit_url: String,
    steps: Arc<Mutex<Vec<LifecycleStep>>>,
    quiet: bool,
}

impl Lifecycle {
    fn record(&self, name: impl Into<String>, ok: bool, outcome: impl Into<String>) {
        let step = LifecycleStep {
            name: name.into(),
            outcome: outcome.into(),
            ok,
        };
        if !self.quiet {
            println!(
                "lifecycle {} {}: {}",
                if ok { "ok  " } else { "FAIL" },
                step.name,
                step.outcome
            );
        }
        self.steps.lock().expect("steps lock").push(step);
    }

    async fn workflows(&self, base: &str) -> Result<Vec<WorkflowStatus>, String> {
        let response = self
            .client
            .get(format!("{base}/api/workflows"))
            .send()
            .await
            .map_err(|e| format!("GET {base}/api/workflows: {e}"))?;
        if !response.status().is_success() {
            return Err(format!("GET {base}/api/workflows: {}", response.status()));
        }
        response
            .json::<Vec<WorkflowStatus>>()
            .await
            .map_err(|e| format!("GET {base}/api/workflows body: {e}"))
    }

    async fn verb(&self, base: &str, id: &str, verb: &str) -> Result<(StatusCode, String), String> {
        let response = self
            .client
            .post(format!("{base}/api/workflows/{id}/{verb}"))
            .send()
            .await
            .map_err(|e| format!("POST {base}/api/workflows/{id}/{verb}: {e}"))?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Ok((status, body))
    }

    async fn service_verb(&self, base: &str, verb: &str) -> Result<ServiceLifecycleReport, String> {
        let response = self
            .client
            .post(format!("{base}/api/service/{verb}"))
            .send()
            .await
            .map_err(|e| format!("POST {base}/api/service/{verb}: {e}"))?;
        let status = response.status();
        if status == StatusCode::CONFLICT {
            return Err(format!(
                "POST {base}/api/service/{verb}: 409, not one workflow accepted the verb"
            ));
        }
        response
            .json::<ServiceLifecycleReport>()
            .await
            .map_err(|e| format!("POST {base}/api/service/{verb} body: {e}"))
    }

    /// Wait until `id` reports one of `wanted`, or give up.
    async fn await_state(&self, base: &str, id: &str, wanted: &[&str], budget: Duration) -> String {
        let deadline = Instant::now() + budget;
        let mut last = "unknown".to_string();
        loop {
            if let Ok(list) = self.workflows(base).await
                && let Some(status) = list.iter().find(|w| w.id == id)
            {
                last = status.state.as_str().to_string();
                if wanted.contains(&last.as_str()) {
                    return last;
                }
            }
            if Instant::now() >= deadline {
                return last;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    async fn run(&self, args: &LifecycleTimings, published_before_pause: Arc<AtomicU64>) {
        // 1. every workflow present and running, on both control planes.
        match self.workflows(&self.stream_url).await {
            Ok(list) => {
                let ids: Vec<&str> = list.iter().map(|w| w.id.as_str()).collect();
                let present = ["ingest", "settle"].iter().all(|id| ids.contains(id));
                let running = list
                    .iter()
                    .filter(|w| w.id == "ingest" || w.id == "settle")
                    .all(|w| w.state.as_str() == "running");
                self.record(
                    "GET /api/workflows (stream)",
                    present && running,
                    format!("{ids:?}"),
                );
            }
            Err(e) => self.record("GET /api/workflows (stream)", false, e),
        }
        match self.workflows(&self.audit_url).await {
            Ok(list) => {
                let ids: Vec<&str> = list.iter().map(|w| w.id.as_str()).collect();
                let ok = ids.contains(&"audit")
                    && list
                        .iter()
                        .any(|w| w.id == "audit" && w.state.as_str() == "running");
                self.record("GET /api/workflows (audit)", ok, format!("{ids:?}"));
            }
            Err(e) => self.record("GET /api/workflows (audit)", false, e),
        }

        // 2. pause settle, keep publishing, resume. Nothing may be lost: the
        //    JetStream source object survives the park and the bridge is an
        //    in-process channel.
        let before = published_before_pause.load(Ordering::Relaxed);
        match self.verb(&self.stream_url, "settle", "pause").await {
            Ok((status, body)) if status.is_success() => {
                let state = self
                    .await_state(
                        &self.stream_url,
                        "settle",
                        &["paused"],
                        Duration::from_secs(10),
                    )
                    .await;
                self.record(
                    "POST /api/workflows/settle/pause",
                    state == "paused",
                    format!("{status} -> {state}"),
                );
                let _ = body;
            }
            Ok((status, body)) => self.record(
                "POST /api/workflows/settle/pause",
                false,
                format!("{status}: {body}"),
            ),
            Err(e) => self.record("POST /api/workflows/settle/pause", false, e),
        }
        tokio::time::sleep(Duration::from_secs(args.pause_secs)).await;
        let during = published_before_pause.load(Ordering::Relaxed) - before;
        match self.verb(&self.stream_url, "settle", "resume").await {
            Ok((status, body)) if status.is_success() => {
                let state = self
                    .await_state(
                        &self.stream_url,
                        "settle",
                        &["running"],
                        Duration::from_secs(10),
                    )
                    .await;
                self.record(
                    "POST /api/workflows/settle/resume",
                    state == "running",
                    format!("{status} -> {state}, {during} items published while parked"),
                );
                let _ = body;
            }
            Ok((status, body)) => self.record(
                "POST /api/workflows/settle/resume",
                false,
                format!("{status}: {body}"),
            ),
            Err(e) => self.record("POST /api/workflows/settle/resume", false, e),
        }

        // 3. stop audit, keep inserting, start it again.
        match self.verb(&self.audit_url, "audit", "stop").await {
            Ok((status, body)) if status.is_success() => {
                let state = self
                    .await_state(
                        &self.audit_url,
                        "audit",
                        &["stopped"],
                        Duration::from_secs(15),
                    )
                    .await;
                self.record(
                    "POST /api/workflows/audit/stop",
                    state == "stopped",
                    format!("{status} -> {state}"),
                );
                let _ = body;
            }
            Ok((status, body)) => self.record(
                "POST /api/workflows/audit/stop",
                false,
                format!("{status}: {body}"),
            ),
            Err(e) => self.record("POST /api/workflows/audit/stop", false, e),
        }
        tokio::time::sleep(Duration::from_secs(args.stop_secs)).await;
        match self.verb(&self.audit_url, "audit", "start").await {
            Ok((status, body)) if status.is_success() => {
                let state = self
                    .await_state(
                        &self.audit_url,
                        "audit",
                        &["running"],
                        Duration::from_secs(20),
                    )
                    .await;
                self.record(
                    "POST /api/workflows/audit/start",
                    state == "running",
                    format!("{status} -> {state}"),
                );
                let _ = body;
            }
            Ok((status, body)) => self.record(
                "POST /api/workflows/audit/start",
                false,
                format!("{status}: {body}"),
            ),
            Err(e) => self.record("POST /api/workflows/audit/start", false, e),
        }

        // 4. both stream workflows hold a ChannelSink or ChannelSource, which
        //    is bound to one mpsc pair created once per process, so a stop has
        //    to be refused with 409 rather than silently rebuilding it.
        for id in ["ingest", "settle"] {
            match self.verb(&self.stream_url, id, "stop").await {
                Ok((status, body)) => self.record(
                    format!("POST /api/workflows/{id}/stop is refused"),
                    status == StatusCode::CONFLICT,
                    format!("{status}: {}", body.trim()),
                ),
                Err(e) => self.record(
                    format!("POST /api/workflows/{id}/stop is refused"),
                    false,
                    e,
                ),
            }
        }

        // 5. service-wide pause and resume, on both control planes.
        for (label, base) in [("stream", &self.stream_url), ("audit", &self.audit_url)] {
            match self.service_verb(base, "pause").await {
                Ok(report) => self.record(
                    format!("POST /api/service/pause ({label})"),
                    !report.applied.is_empty(),
                    format!(
                        "applied={} refused={} settled={}",
                        report.applied.len(),
                        report.refused.len(),
                        report.settled
                    ),
                ),
                Err(e) => self.record(format!("POST /api/service/pause ({label})"), false, e),
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            match self.service_verb(base, "resume").await {
                Ok(report) => {
                    let all_running = report
                        .applied
                        .iter()
                        .all(|w| matches!(w.state.as_str(), "running" | "starting"));
                    self.record(
                        format!("POST /api/service/resume ({label})"),
                        !report.applied.is_empty() && all_running,
                        format!(
                            "applied={} refused={} settled={}",
                            report.applied.len(),
                            report.refused.len(),
                            report.settled
                        ),
                    );
                }
                Err(e) => self.record(format!("POST /api/service/resume ({label})"), false, e),
            }
        }
    }
}

struct LifecycleTimings {
    pause_secs: u64,
    stop_secs: u64,
}

// ── setup ───────────────────────────────────────────────────────────────────

/// Read the committed catalog CSV: a header row plus four columns.
///
/// A missing or malformed file is a hard error before anything is published,
/// because the `taxable` this returns is what every expected `line_total` and
/// every expected checksum is built from. Reading the same bytes the
/// `FileSource` reads is what makes the two agree by construction rather than
/// by two generators happening to produce the same rows.
fn read_catalog(path: &str) -> Result<Catalog, String> {
    let hint = "examples/integrity/catalog.csv is committed, and both services read it through \
                the same path";
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("reading the catalog {path}: {e}; {hint}"))?;
    let mut lines = text.lines();
    let header = lines
        .next()
        .ok_or_else(|| format!("the catalog {path} is empty; {hint}"))?
        .trim_end();
    if header != "sku,name,taxable,weight_kg" {
        return Err(format!(
            "the catalog {path} has header {header:?}, expected \
             \"sku,name,taxable,weight_kg\"; {hint}"
        ));
    }
    let mut catalog = Catalog {
        skus: Vec::new(),
        taxable: HashMap::new(),
    };
    for (index, line) in lines.enumerate() {
        let line = line.trim_end();
        if line.is_empty() {
            continue;
        }
        let row = index + 2;
        let fields: Vec<&str> = line.split(',').collect();
        let [sku, _name, taxable, _weight] = fields.as_slice() else {
            return Err(format!(
                "the catalog {path} line {row} has {} columns, expected 4; {hint}",
                fields.len()
            ));
        };
        let taxable: bool = taxable
            .parse()
            .map_err(|e| format!("the catalog {path} line {row}: taxable {taxable:?}: {e}"))?;
        catalog.skus.push((*sku).to_string());
        catalog.taxable.insert((*sku).to_string(), taxable);
    }
    if catalog.skus.is_empty() {
        return Err(format!("the catalog {path} has no rows; {hint}"));
    }
    Ok(catalog)
}

/// Delete and recreate the orders topic, so a re-run starts from an empty log.
///
/// Recreating rather than merely creating is what makes the run repeatable:
/// the source consumes from the beginning of the topic, so a previous run's
/// messages would be replayed into this run's verification as orders it never
/// published. The service provisions the same topic with one partition and
/// one replica, so whichever of the two wins the race the topic is identical;
/// one partition is also what keeps a batch's delivery order stable.
async fn reset_kafka_topic(brokers: &str) -> Result<(), String> {
    use rdkafka::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::types::RDKafkaErrorCode;

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .map_err(|e| format!("kafka admin client: {e}"))?;

    match admin
        .delete_topics(&[KAFKA_TOPIC], &AdminOptions::new())
        .await
    {
        Ok(results) => {
            for result in results {
                match result {
                    Ok(_) | Err((_, RDKafkaErrorCode::UnknownTopicOrPartition)) => {}
                    Err((name, code)) => return Err(format!("deleting topic {name}: {code}")),
                }
            }
        }
        Err(e) => return Err(format!("deleting {KAFKA_TOPIC}: {e}")),
    }

    // Deleting a topic does not synchronously drop the group's committed
    // offsets. A surviving offset outranks the source's `auto_offset_reset`
    // for as long as it still points inside the recreated log, so the service
    // would start mid-log and the run's first orders would never be consumed,
    // which reads as data loss rather than a stale offset.
    //
    // `NonEmptyGroup` means the group still has members. Neither control
    // plane answered when this run started, so the only thing that can hold
    // membership now is a process that died without leaving the group, and
    // the coordinator evicts it once `session.timeout.ms` (45s by
    // librdkafka's default) expires. So this waits it out. A warning would be
    // useless: by the time it is printed the topic has already been
    // recreated, and there is nothing the operator can still do about it.
    let group_deadline = Instant::now() + Duration::from_secs(75);
    let mut announced = false;
    loop {
        let mut members_remain = false;
        match admin
            .delete_groups(&[KAFKA_GROUP], &AdminOptions::new())
            .await
        {
            Ok(results) => {
                for result in results {
                    match result {
                        Ok(_)
                        | Err((_, RDKafkaErrorCode::GroupIdNotFound))
                        | Err((_, RDKafkaErrorCode::UnknownTopicOrPartition)) => {}
                        Err((_, RDKafkaErrorCode::NonEmptyGroup)) => members_remain = true,
                        Err((name, code)) => {
                            return Err(format!("deleting consumer group {name}: {code}"));
                        }
                    }
                }
            }
            Err(e) => return Err(format!("deleting consumer group {KAFKA_GROUP}: {e}")),
        }
        if !members_remain {
            break;
        }
        // Said once, not every 2s: without it the run looks hung between the
        // topic delete and the recreate line.
        if !announced {
            announced = true;
            println!(
                "consumer group {KAFKA_GROUP} still has members; waiting up to 75s for \
                 session.timeout.ms to evict them"
            );
        }
        if Instant::now() >= group_deadline {
            return Err(format!(
                "consumer group {KAFKA_GROUP} still has members 75s after the delete. A member \
                 outlives its process until session.timeout.ms expires; wait for it, or delete \
                 the group by hand: kafka-consumer-groups.sh --bootstrap-server {brokers} \
                 --delete --group {KAFKA_GROUP}"
            ));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // Deletion is asynchronous on the broker: creating again before the old
    // log segments are gone answers TopicAlreadyExists and leaves the old
    // messages in place, which is the failure this whole function exists to
    // prevent. Poll cluster metadata until the name is really gone.
    let probe: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "saci-integrity-topic-probe")
        .set("allow.auto.create.topics", "false")
        .create()
        .map_err(|e| format!("kafka metadata client: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let metadata = probe
            .fetch_metadata(None, Duration::from_secs(5))
            .map_err(|e| format!("kafka metadata: {e}"))?;
        if !metadata.topics().iter().any(|t| t.name() == KAFKA_TOPIC) {
            break;
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "{KAFKA_TOPIC} was still present 30s after the delete"
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let topic = NewTopic::new(KAFKA_TOPIC, 1, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([&topic], &AdminOptions::new())
        .await
        .map_err(|e| format!("creating {KAFKA_TOPIC}: {e}"))?;
    for result in results {
        match result {
            Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => return Err(format!("creating topic {name}: {code}")),
        }
    }
    Ok(())
}

/// Empty a JetStream stream left over from a previous run, without touching
/// its settings.
///
/// The service owns the stream's configuration through `stream_provision`;
/// this only drops the messages, and only before the services start, so a
/// durable consumer never re-reads a previous run's payments as if they were
/// this run's. A stream that does not exist yet is the first-run case and
/// needs nothing.
async fn purge_stream(url: &str) -> Result<(), String> {
    let client = async_nats::connect(url)
        .await
        .map_err(|e| format!("connecting to NATS at {url}: {e}"))?;
    let context = async_nats::jetstream::new(client);
    let Ok(stream) = context.get_stream(NATS_STREAM).await else {
        return Ok(());
    };
    stream
        .purge()
        .await
        .map_err(|e| format!("purging JetStream stream {NATS_STREAM}: {e}"))?;
    Ok(())
}

/// Sleep between polls, unless Ctrl-C arrived first; `false` means it did.
///
/// Every startup wait polls on a budget measured in minutes, so without this
/// a Ctrl-C while waiting for a service would be ignored until the whole
/// budget elapsed.
async fn nap(cancel: &CancellationToken, period: Duration) -> bool {
    tokio::select! {
        () = tokio::time::sleep(period) => true,
        () = cancel.cancelled() => false,
    }
}

/// Wait for the service to provision the JetStream stream. The publisher never
/// creates it: one owner of the stream settings.
async fn await_stream(
    context: &async_nats::jetstream::Context,
    budget: Duration,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let deadline = Instant::now() + budget;
    loop {
        let error = match context.get_stream(NATS_STREAM).await {
            Ok(_) => return Ok(()),
            Err(e) => e.to_string(),
        };
        if Instant::now() >= deadline {
            let seconds = budget.as_secs();
            return Err(format!(
                "JetStream stream {NATS_STREAM} did not appear within {seconds}s; the stream is \
                 provisioned by examples/integrity/integrity.kdl, so start that service: {error}"
            ));
        }
        if !nap(cancel, Duration::from_millis(500)).await {
            return Err("interrupted while waiting for the JetStream stream".to_string());
        }
    }
}

/// Empty the audit table and take ownership of the CDC replication slot.
///
/// The order of the four statements is the whole point.
///
/// 1. Drop the slot, so the run starts from an empty change stream instead of
///    replaying the previous run's rows.
/// 2. Truncate the table. `saci_integrity_pub` publishes `TRUNCATE`
///    (`pg_publication.pubtruncate` is on by default), so doing this while no
///    slot exists is what keeps the statement out of the change stream the
///    audit service decodes.
/// 3. Create the slot, before a single audit row is published. A logical slot
///    only captures WAL written after it exists, so leaving creation to the
///    connector's `slot_autocreate` would leave every change committed before
///    the audit service's first drain cycle permanently uncapturable. Owning
///    the slot here is what closes that window.
/// 4. Assert it is there and unheld.
///
/// Step 3 issues the statement
/// `crates/saci-connector-postgresql/src/source/logical.rs`'s `create_slot`
/// issues, argument for argument, so the connector's own call answers
/// `DUPLICATE_OBJECT` and it accepts this slot as its own: the plugin is
/// `pgoutput`, and the two-argument form leaves every optional argument
/// (`temporary`, and `two_phase` on a server that has it) at `false`.
async fn reset_audit_stream(client: &tokio_postgres::Client) -> Result<(), String> {
    client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
             WHERE slot_name = $1",
            &[&AUDIT_SLOT],
        )
        .await
        .map_err(|e| {
            format!(
                "dropping replication slot {AUDIT_SLOT}: {e}. A slot that refuses to drop is \
                 held by another session, so this run cannot own it; stop whatever holds it"
            )
        })?;
    client
        .simple_query(&format!("TRUNCATE {AUDIT_TABLE}"))
        .await
        .map_err(|e| format!("truncating {AUDIT_TABLE}: {e}"))?;
    client
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&AUDIT_SLOT],
        )
        .await
        .map_err(|e| {
            format!(
                "creating replication slot {AUDIT_SLOT}: {e}. It needs wal_level = logical, a \
                 free entry in max_replication_slots, and a role with REPLICATION"
            )
        })?;

    let row = client
        .query_opt(
            "SELECT plugin::text, slot_type::text, temporary, active \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&AUDIT_SLOT],
        )
        .await
        .map_err(|e| format!("reading pg_replication_slots for {AUDIT_SLOT}: {e}"))?
        .ok_or_else(|| {
            format!(
                "replication slot {AUDIT_SLOT} does not exist after it was created; nothing \
                 would capture the audit changes this run is about to publish"
            )
        })?;
    let plugin: String = row.get(0);
    let slot_type: String = row.get(1);
    let temporary: bool = row.get(2);
    let active: bool = row.get(3);
    if plugin != "pgoutput" || slot_type != "logical" || temporary || active {
        return Err(format!(
            "replication slot {AUDIT_SLOT} is not the slot the audit service will accept: \
             plugin={plugin} slot_type={slot_type} temporary={temporary} active={active}, \
             want plugin=pgoutput slot_type=logical temporary=false active=false"
        ));
    }
    Ok(())
}

/// One `/health` GET: is a service answering *right now*?
///
/// The short per-request timeout is deliberate. This is asked once as a
/// precondition and then in a poll loop, and neither wants the client's
/// ten-second default.
async fn answers_health(client: &reqwest::Client, base: &str) -> bool {
    matches!(
        client
            .get(format!("{base}/health"))
            .timeout(Duration::from_secs(2))
            .send()
            .await,
        Ok(response) if response.status().is_success()
    )
}

/// Refuse the run while either service is up, before anything is destroyed.
///
/// The startup reset is not idempotent housekeeping, it is demolition: it
/// deletes the Kafka topic, deletes the consumer group, drops every message
/// in the JetStream stream, truncates the audit table and drops the
/// replication slot. Four of those merely disrupt a live consumer. The fifth
/// destroys data that cannot be recovered: a logical replication slot
/// captures only WAL written after it exists, so every audit change
/// committed between the drop and whenever a live `PostgresSource` rebuilds
/// its session is gone, and the run reports it as pipeline loss. There is no
/// correct way to do this against a running service, so there is no flag to
/// override it.
async fn refuse_live_services(
    client: &reqwest::Client,
    stream_url: &str,
    audit_url: &str,
) -> Result<(), String> {
    let mut live: Vec<String> = Vec::new();
    for (label, base) in [("stream", stream_url), ("audit", audit_url)] {
        if answers_health(client, base).await {
            live.push(format!("the {label} control plane at {base}"));
        }
    }
    if live.is_empty() {
        return Ok(());
    }
    Err(format!(
        "{} answered /health, so a service is already running. This run's startup reset deletes \
         the Kafka topic {KAFKA_TOPIC}, deletes the consumer group {KAFKA_GROUP}, drops every \
         message in the JetStream stream {NATS_STREAM}, truncates {AUDIT_TABLE} and drops the \
         replication slot {AUDIT_SLOT}, all of it shared with a running service. Doing that \
         under a live consumer takes the topic away from a subscribed source and the slot away \
         from the CDC reader, and a logical slot cannot capture WAL written before it existed, \
         so the audit changes published during the gap are unrecoverable and the run reports a \
         loss it caused itself. Stop both services, then re-run this.",
        live.join(" and ")
    ))
}

async fn await_health(
    client: &reqwest::Client,
    base: &str,
    budget: Duration,
    cancel: &CancellationToken,
) -> bool {
    let deadline = Instant::now() + budget;
    loop {
        if answers_health(client, base).await {
            return true;
        }
        if Instant::now() >= deadline || !nap(cancel, Duration::from_millis(500)).await {
            return false;
        }
    }
}

async fn await_workflows(
    client: &reqwest::Client,
    base: &str,
    wanted: &[&str],
    budget: Duration,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let deadline = Instant::now() + budget;
    loop {
        if let Ok(response) = client.get(format!("{base}/api/workflows")).send().await
            && let Ok(list) = response.json::<Vec<WorkflowStatus>>().await
        {
            let ids: Vec<&str> = list.iter().map(|w| w.id.as_str()).collect();
            if wanted.iter().all(|id| ids.contains(id)) {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(format!("{base} never reported the workflows {wanted:?}"));
        }
        if !nap(cancel, Duration::from_millis(500)).await {
            return Err(format!("interrupted while waiting for {base}"));
        }
    }
}

// ── final verification ──────────────────────────────────────────────────────

/// Expected totals for one `(window_id, region)` group.
#[derive(Default)]
struct ExpectedWindow {
    order_count: i64,
    payment_count: i64,
    order_amount: f64,
    payment_amount: f64,
}

/// Is the missing set the oldest rows, with a surviving tail behind them?
///
/// `flags` marks each published row as missing or not, oldest first, and
/// `missing` is how many are missing in total. When the first `missing` flags
/// are all set and at least one row after them arrived, every later row did,
/// which is the only shape a reader that attached late can produce. A run
/// where *everything* is missing is a different fault: the reader delivered
/// nothing at all, which the "received nothing" check names on its own.
fn is_prefix_gap(flags: &[bool], missing: usize) -> bool {
    missing > 0 && missing < flags.len() && flags[..missing].iter().all(|&flag| flag)
}

/// The sentence a prefix-shaped gap earns, in place of a list of ids.
///
/// A reader that started after the publisher did loses the oldest rows and
/// nothing else, so the shape of a gap names its cause. A list of 579 ids
/// does not.
fn prefix_gap_note(missing: usize, total: usize, last_missing_ms: i64, cause: &str) -> String {
    format!(
        "The missing set is a contiguous PREFIX of the published range: the OLDEST {missing} of \
         {total}, covering simulated event time {BASE_TS_MS}..{last_missing_ms} (the first \
         {:.1}s of it), and nothing after that is missing. {cause}",
        (last_missing_ms - BASE_TS_MS) as f64 / 1000.0
    )
}

fn finalise(verifier: &Verifier, warmup_secs: u32) -> Vec<String> {
    let mut registry = verifier.registry.lock().expect("registry lock");
    let mut problems = Vec::new();

    // 1. completeness of the two order sinks, taken together.
    let missing: Vec<i64> = {
        let seen = &registry.seen_orders;
        let mut ids: Vec<i64> = registry
            .orders
            .keys()
            .copied()
            .filter(|id| !seen.contains_key(id))
            .collect();
        ids.sort_unstable();
        ids
    };
    if !missing.is_empty() {
        let shown: Vec<i64> = missing.iter().copied().take(20).collect();
        let mut problem = format!(
            "{} of {} published orders never arrived at /sink/express or /sink/standard: {:?}{}",
            missing.len(),
            registry.orders.len(),
            shown,
            if missing.len() > shown.len() {
                " ..."
            } else {
                ""
            }
        );
        // Publication order is ascending `order_id`; `orders` is a HashMap,
        // so that order has to be built rather than read off the map.
        let flags: Vec<bool> = {
            let seen = &registry.seen_orders;
            let mut ids: Vec<i64> = registry.orders.keys().copied().collect();
            ids.sort_unstable();
            ids.iter().map(|id| !seen.contains_key(id)).collect()
        };
        if is_prefix_gap(&flags, missing.len())
            && let Some(last) = missing.last().and_then(|id| registry.orders.get(id))
        {
            problem.push(' ');
            problem.push_str(&prefix_gap_note(
                missing.len(),
                registry.orders.len(),
                last.event_ms,
                "That is what a Kafka consumer that joined at a later offset than this run's \
                 first message looks like: a committed offset for the group that survived the \
                 topic delete, still pointed inside the recreated log and so outranked \
                 `auto_offset_reset \"earliest\"`, or a subscribed source whose topic was \
                 deleted under it. Look there before looking at the pipeline.",
            ));
        }
        problems.push(problem);
    }

    // 5. windows that are definitely closed. The trailing open window is
    //    ignored: its rows are still accumulating in the processor.
    let mut expected_windows: HashMap<(i64, &str), ExpectedWindow> = HashMap::new();
    for order in registry.orders.values() {
        let taxable = verifier.taxable(&order.sku);
        let total = line_total(order.qty, order.unit_price, taxable);
        let entry = expected_windows
            .entry((window_id_of(order.event_ms), order.region.as_str()))
            .or_default();
        entry.order_count += 1;
        entry.order_amount += total;
    }
    for payment in registry.payments.values() {
        let entry = expected_windows
            .entry((window_id_of(payment.event_ms), payment.region.as_str()))
            .or_default();
        entry.payment_count += 1;
        entry.payment_amount += payment.amount;
    }
    let watermark = registry.max_event_ms;
    // The windowed node is a fan-in whose two legs have very different
    // shapes: `payments_nats` drains up to 500 rows per batch straight into
    // `settle`, while an order crosses Kafka at one message per batch, the
    // classify processor and the channel bridge first. Until both legs are
    // streaming, the payment leg alone carries the watermark forward and the
    // host drops whole order arrivals that fall below
    // `watermark - allowed_lateness_ms`. Nothing is lost from the run (the
    // rows are drained and counted, see the late-arrival figures the report
    // prints); those windows simply cannot be reasoned about, so the earliest
    // `--window-warmup-secs` of event time are excluded from this assertion.
    let warmup_end = BASE_TS_MS + i64::from(warmup_secs) * 1000;
    let mut closed_groups = 0u64;
    let mut skipped_groups = 0u64;
    let mut window_problems = Vec::new();
    for ((window_id, region), expected) in &expected_windows {
        let window_end = (window_id + 1) * WINDOW_SIZE_MS;
        if window_end + ALLOWED_LATENESS_MS > watermark {
            continue;
        }
        if window_end <= warmup_end {
            skipped_groups += 1;
            continue;
        }
        closed_groups += 1;
        let order_amount = round2(expected.order_amount);
        let payment_amount = round2(expected.payment_amount);
        let key = (*window_id, (*region).to_string());
        let Some(seen) = registry.seen_windows.get(&key) else {
            window_problems.push(format!("window {window_id}/{region} never arrived"));
            continue;
        };
        // Summed over every emission: each row's own checksum was already
        // checked against its own fields when it arrived, so what is left to
        // prove here is that the emissions add up to what was published.
        if seen.order_count != expected.order_count
            || seen.payment_count != expected.payment_count
            || (round2(seen.order_amount) - order_amount).abs() > 0.011
            || (round2(seen.payment_amount) - payment_amount).abs() > 0.011
        {
            window_problems.push(format!(
                "window {window_id}/{region}: {} emissions summing to ({}, {}, {:.2}, {:.2}) \
                 want ({}, {}, {order_amount:.2}, {payment_amount:.2})",
                seen.emissions,
                seen.order_count,
                seen.payment_count,
                seen.order_amount,
                seen.payment_amount,
                expected.order_count,
                expected.payment_count,
            ));
        }
    }
    registry.closed_window_groups = closed_groups;
    registry.warmup_window_groups = skipped_groups;
    let shown_windows = window_problems.len().min(20);
    if !window_problems.is_empty() {
        let mut problem = format!(
            "{} of {closed_groups} closed window groups disagree: {:?}{}",
            window_problems.len(),
            &window_problems[..shown_windows],
            if window_problems.len() > shown_windows {
                " ..."
            } else {
                ""
            }
        );
        // Every asserted group missing with nothing at all on the sink is not
        // a windowing disagreement: no window ever closed, and one cause
        // produces that while both order sinks stay complete.
        if window_problems.len() as u64 == closed_groups
            && registry
                .sinks
                .get("window")
                .is_none_or(|stats| stats.rows == 0)
        {
            problem.push(' ');
            problem.push_str(
                "Not one group arrived and /sink/window received no rows, so no window ever \
                 closed while both order sinks came out complete. `aggregate` opens a window \
                 only for a row at or above `watermark - allowed_lateness_ms`, so its whole \
                 merged stream was late: read `aggregate.late_rows` and \
                 `saci_window_late_arrivals_total` on the stream half's /metrics. From an \
                 otherwise healthy run the one cause is a watermark restored from an earlier \
                 run, which is why integrity.kdl declares no `store \"redb\"` block; if one was \
                 added, its checkpoint blob is where to look.",
            );
        }
        problems.push(problem);
    }

    // 6. every audit change arrived. Both lists below are in publication
    //    order, oldest change first: `audits` is a HashMap keyed by
    //    `(audit_id, op)`, so a sort of that key interleaves an insert with
    //    an unrelated later update and hides the shape of a gap, while
    //    `changed_ms` is the publisher's own monotonic clock and is unique
    //    per change.
    let audits_in_order: Vec<(i64, String, bool)> = {
        let seen = &registry.seen_audits;
        let mut rows: Vec<(i64, String, bool)> = registry
            .audits
            .values()
            .map(|change| {
                (
                    change.changed_ms,
                    format!("{}/{}", change.audit_id, change.op),
                    !seen.contains_key(&(change.audit_id, change.op.to_string())),
                )
            })
            .collect();
        rows.sort_unstable_by_key(|(changed_ms, _, _)| *changed_ms);
        rows
    };
    let missing_audits: Vec<(i64, &str)> = audits_in_order
        .iter()
        .filter(|(_, _, missing)| *missing)
        .map(|(changed_ms, label, _)| (*changed_ms, label.as_str()))
        .collect();
    if !missing_audits.is_empty() {
        let shown = missing_audits.len().min(20);
        let labels: Vec<&str> = missing_audits[..shown]
            .iter()
            .map(|(_, label)| *label)
            .collect();
        let mut problem = format!(
            "{} of {} audit changes never arrived at /sink/audit, oldest first: {:?}{}",
            missing_audits.len(),
            registry.audits.len(),
            labels,
            if missing_audits.len() > shown {
                " ..."
            } else {
                ""
            }
        );
        let flags: Vec<bool> = audits_in_order
            .iter()
            .map(|(_, _, missing)| *missing)
            .collect();
        if is_prefix_gap(&flags, missing_audits.len())
            && let Some((last_ms, _)) = missing_audits.last()
        {
            problem.push(' ');
            problem.push_str(&prefix_gap_note(
                missing_audits.len(),
                registry.audits.len(),
                *last_ms,
                "That is what a replication slot created after publishing began looks like: a \
                 logical slot carries only WAL written after it exists, so a change committed \
                 before it was created can never be read. This publisher creates the slot \
                 itself, before the first insert, so a gap of this shape means the slot was \
                 dropped and remade during the run: an audit service that was already running \
                 when the reset dropped its slot, or a second publisher against the same \
                 database. Look there before looking at the pipeline.",
            ));
        }
        problems.push(problem);
    }

    // 7. every scalar type the contract declares was actually decoded.
    for want in ["Int64", "Int32", "Float64", "Utf8", "Boolean"] {
        if !registry.types_seen.iter().any(|seen| seen == want) {
            problems.push(format!(
                "no decoded schema carried a {want} column; type coverage dropped out"
            ));
        }
    }
    for sink in ["express", "standard", "window", "audit"] {
        if !registry.schemas_checked.contains(sink) {
            problems.push(format!(
                "/sink/{sink} received nothing, so nothing was checked"
            ));
        }
    }

    for problem in &problems {
        registry.fail(problem.clone());
    }
    problems
}

// ── the run ─────────────────────────────────────────────────────────────────

/// Everything the publish loop needs, built before a single row is on the
/// wire.
///
/// Assembling this is the whole of exit code 2: every failure in [`prepare`]
/// happens with nothing published, so the run has no verdict to report.
struct Prepared {
    verifier: Arc<Verifier>,
    server: tokio::task::JoinHandle<()>,
    http: reqwest::Client,
    kafka: rdkafka::producer::FutureProducer,
    jetstream: async_nats::jetstream::Context,
    pg: tokio_postgres::Client,
    insert_audit: tokio_postgres::Statement,
    update_audit: tokio_postgres::Statement,
}

/// Refuse, reset, serve, wait: everything that has to be true before the
/// first order is published.
async fn prepare(args: &Args, cancel: &CancellationToken) -> Result<Prepared, String> {
    // reqwest 0.13's `rustls-no-provider` refuses to build a client until a
    // crypto provider is installed.
    saci_service::service::install_ring_provider();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| format!("building the HTTP client: {e}"))?;

    // 1. Nothing below runs until both services are known to be down. Every
    //    one of those steps takes state a running service holds.
    refuse_live_services(&http, &args.stream_url, &args.audit_url).await?;

    let catalog = read_catalog(&args.catalog_file)?;
    let catalog_size = catalog.skus.len();
    let verifier = Arc::new(Verifier {
        catalog,
        registry: Mutex::new(Registry {
            quiet: args.quiet,
            ..Registry::default()
        }),
        requests: AtomicU64::new(0),
    });
    println!(
        "catalog read from {} ({catalog_size} skus, no timestamp column)",
        args.catalog_file
    );

    // 2. The receiver comes first: a sink whose endpoint refuses the first
    //    batch costs the workflow a retry cycle, and a long enough outage
    //    fails the run.
    let listener = tokio::net::TcpListener::bind(&args.bind)
        .await
        .map_err(|e| format!("binding the verification endpoint to {}: {e}", args.bind))?;
    let bound = listener
        .local_addr()
        .map_err(|e| format!("reading back the address bound to {}: {e}", args.bind))?;
    let serve_cancel = cancel.clone();
    let server = tokio::spawn({
        let verifier = Arc::clone(&verifier);
        async move {
            let _ = axum::serve(listener, router(verifier))
                .with_graceful_shutdown(async move { serve_cancel.cancelled().await })
                .await;
        }
    });
    println!("receiver listening on http://{bound}/sink/{{express,standard,window,audit}}");

    // 3. Every transport emptied of the previous run's data, so a re-run
    //    verifies its own stream rather than replaying an older one.
    reset_kafka_topic(&args.kafka_brokers).await?;
    println!(
        "kafka topic {KAFKA_TOPIC} recreated empty on {} and consumer group {KAFKA_GROUP} \
         deleted with it",
        args.kafka_brokers
    );
    purge_stream(&args.nats_url).await?;

    let (pg, pg_connection) = tokio_postgres::connect(&args.pg_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|e| format!("connecting to PostgreSQL at {}: {e}", args.pg_dsn))?;
    tokio::spawn(async move {
        if let Err(e) = pg_connection.await {
            eprintln!("postgres connection closed: {e}");
        }
    });
    reset_audit_stream(&pg).await?;
    let insert_audit = pg
        .prepare(&format!(
            "INSERT INTO {AUDIT_TABLE} (audit_id, order_id, status, revision, changed_ms) \
             VALUES ($1, $2, $3, $4, $5)"
        ))
        .await
        .map_err(|e| format!("preparing the audit insert: {e}"))?;
    let update_audit = pg
        .prepare(&format!(
            "UPDATE {AUDIT_TABLE} SET status = $2, revision = $3, changed_ms = $4 \
             WHERE audit_id = $1"
        ))
        .await
        .map_err(|e| format!("preparing the audit update: {e}"))?;
    println!(
        "{AUDIT_TABLE} truncated; replication slot {AUDIT_SLOT} dropped and recreated by this \
         run, so the change stream starts here and nothing can be committed into a gap"
    );

    // 4. Only now may the services start. Everything they consume exists and
    //    is empty, and nothing else will be reset under them.
    println!();
    println!("start both services now, then this run continues on its own:");
    println!(
        "  saci-service serve --config examples/integrity/integrity.kdl        \
         # {} , workflows ingest + settle",
        args.stream_url
    );
    println!(
        "  saci-service serve --config examples/integrity/integrity_audit.kdl  \
         # {} , workflow audit",
        args.audit_url
    );
    println!();
    for (label, base) in [("stream", &args.stream_url), ("audit", &args.audit_url)] {
        if !await_health(&http, base, Duration::from_secs(300), cancel).await {
            let why = if cancel.is_cancelled() {
                "interrupted"
            } else {
                "never answered"
            };
            return Err(format!("{base}/health ({label} service) {why}"));
        }
    }
    await_workflows(
        &http,
        &args.stream_url,
        &["ingest", "settle"],
        Duration::from_secs(120),
        cancel,
    )
    .await?;
    await_workflows(
        &http,
        &args.audit_url,
        &["audit"],
        Duration::from_secs(120),
        cancel,
    )
    .await?;
    println!("both control planes report their workflows");

    // 5. Transports.
    let kafka: rdkafka::producer::FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", &args.kafka_brokers)
        .set("message.timeout.ms", "10000")
        .create()
        .map_err(|e| {
            format!(
                "creating the Kafka producer for {}: {e}",
                args.kafka_brokers
            )
        })?;
    let nats = async_nats::connect(&args.nats_url)
        .await
        .map_err(|e| format!("connecting to NATS at {}: {e}", args.nats_url))?;
    let jetstream = async_nats::jetstream::new(nats);
    await_stream(&jetstream, Duration::from_secs(120), cancel).await?;
    println!("JetStream stream {NATS_STREAM} is provisioned; publishing starts");

    Ok(Prepared {
        verifier,
        server,
        http,
        kafka,
        jetstream,
        pg,
        insert_audit,
        update_audit,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("error: {message}");
            std::process::exit(2);
        }
    };

    let cancel = CancellationToken::new();
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            cancel.cancel();
        });
    }

    // Exit 2 is the README's code for a setup failure: the run published
    // nothing, so it has no verdict. A verdict, pass or fail, can only come
    // out of the code below this point.
    let Prepared {
        verifier,
        server,
        http,
        kafka,
        jetstream,
        pg,
        insert_audit,
        update_audit,
    } = match prepare(&args, &cancel).await {
        Ok(prepared) => prepared,
        Err(message) => {
            eprintln!();
            eprintln!("error: {message}");
            std::process::exit(2);
        }
    };
    let skus = verifier.catalog.skus.clone();

    // The one number that decides whether the windowed node sees any orders
    // at all, printed rather than left implicit: an operator who raises
    // --rate without lowering --ts-step-ms starves it silently.
    let speedup = args.rate as f64 * ITEMS_PER_ITERATION * args.ts_step_ms as f64 / 1000.0;
    let skew_budget_ms = f64::from(u32::try_from(ALLOWED_LATENESS_MS).unwrap_or(u32::MAX))
        / speedup.max(f64::EPSILON);
    println!(
        "event time runs at {speedup:.2}x wall clock ({} items/s x {} ms), so the pipeline can \
         absorb {skew_budget_ms:.0} ms of real fan-in skew inside allowed_lateness_ms = \
         {ALLOWED_LATENESS_MS}",
        (args.rate as f64 * ITEMS_PER_ITERATION).round(),
        args.ts_step_ms
    );
    if skew_budget_ms < 500.0 {
        eprintln!(
            "warning: {skew_budget_ms:.0} ms of skew budget is below the 500 ms a Kafka batch \
             alone can cost. Lower --ts-step-ms or --rate, or the windowed node will drop every \
             order as late and /sink/window will under-count."
        );
    }

    // 7. Lifecycle, driven while the data keeps flowing.
    let steps = Arc::new(Mutex::new(Vec::new()));
    let published_count = Arc::new(AtomicU64::new(0));
    let lifecycle_task = if args.no_lifecycle {
        None
    } else {
        let lifecycle = Lifecycle {
            client: http.clone(),
            stream_url: args.stream_url.clone(),
            audit_url: args.audit_url.clone(),
            steps: Arc::clone(&steps),
            quiet: args.quiet,
        };
        let timings = LifecycleTimings {
            pause_secs: args.pause_secs,
            stop_secs: args.stop_secs,
        };
        let delay = Duration::from_secs(args.lifecycle_delay_secs);
        let counter = Arc::clone(&published_count);
        Some(tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            lifecycle.run(&timings, counter).await;
        }))
    };

    // 8. Publish.
    let mut rng = rand::rngs::StdRng::seed_from_u64(args.seed);
    let spacing = Duration::from_secs_f64(1.0 / args.rate as f64);
    let started = Instant::now();
    let publish_deadline =
        (args.duration_secs > 0).then(|| started + Duration::from_secs(args.duration_secs));

    let mut clock = BASE_TS_MS;
    let mut order_id: i64 = 0;
    let mut payment_id: i64 = 0;
    let mut audit_id: i64 = 0;
    let mut iteration: u64 = 0;
    let mut pending: Vec<String> = Vec::with_capacity(ORDERS_PER_MESSAGE);
    let mut pending_priority = PRIORITIES[0];
    let mut message_index: usize = 0;
    let mut awaiting_update: Vec<(i64, i64)> = Vec::new();
    let mut kafka_messages: u64 = 0;

    loop {
        if cancel.is_cancelled() {
            break;
        }
        if publish_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        iteration += 1;

        // One order per iteration.
        clock += args.ts_step_ms;
        order_id += 1;
        // Drawn from the committed catalog, so every order names a sku the
        // classify processor really saw.
        let sku = skus[rng.random_range(0..skus.len())].clone();
        let order = Order {
            order_id,
            sku,
            qty: rng.random_range(1i32..9),
            // Whole cents, so `{:.4}` on the wire round-trips to the same f64
            // on both sides of the checksum.
            unit_price: f64::from(rng.random_range(500i32..20_000)) / 100.0,
            region: REGIONS[rng.random_range(0..REGIONS.len())].to_string(),
            priority: pending_priority,
            event_ms: clock,
        };
        pending.push(order.ndjson());
        {
            let mut registry = verifier.registry.lock().expect("registry lock");
            registry.max_event_ms = registry.max_event_ms.max(clock);
            registry.orders.insert(order_id, order);
        }
        published_count.fetch_add(1, Ordering::Relaxed);

        if pending.len() == ORDERS_PER_MESSAGE {
            let payload = pending.join("\n");
            let record = rdkafka::producer::FutureRecord::to(KAFKA_TOPIC)
                .key(pending_priority)
                .payload(&payload);
            kafka
                .send(
                    record,
                    rdkafka::util::Timeout::After(Duration::from_secs(10)),
                )
                .await
                .map_err(|(e, _)| e)?;
            kafka_messages += 1;
            pending.clear();
            // Alternate, so both branches fire and each Kafka message (which
            // is exactly one batch) routes as a whole.
            message_index += 1;
            pending_priority = PRIORITIES[message_index % PRIORITIES.len()];
        }

        // A payment every other iteration.
        if iteration.is_multiple_of(2) {
            clock += args.ts_step_ms;
            payment_id += 1;
            let amount = f64::from(rng.random_range(1_000i32..50_000)) / 100.0;
            let region = REGIONS[rng.random_range(0..REGIONS.len())];
            let currency = CURRENCIES[rng.random_range(0..CURRENCIES.len())];
            let settled = rng.random_range(0..2) == 0;
            let line = format!(
                r#"{{"payment_id":{payment_id},"order_id":{order_id},"amount":{amount:.2},"currency":"{currency}","settled":{settled},"region":"{region}","event_ms":{clock}}}"#
            );
            jetstream.publish(NATS_SUBJECT, line.into()).await?.await?;
            {
                let mut registry = verifier.registry.lock().expect("registry lock");
                registry.max_event_ms = registry.max_event_ms.max(clock);
                registry.payments.insert(
                    payment_id,
                    Payment {
                        amount,
                        region: region.to_string(),
                        event_ms: clock,
                    },
                );
            }
            published_count.fetch_add(1, Ordering::Relaxed);
        }

        // An audit insert every third iteration.
        if iteration.is_multiple_of(3) {
            clock += args.ts_step_ms;
            audit_id += 1;
            let status = STATUSES[0];
            pg.execute(
                &insert_audit,
                &[&audit_id, &order_id, &status, &1i32, &clock],
            )
            .await?;
            {
                let mut registry = verifier.registry.lock().expect("registry lock");
                registry.max_event_ms = registry.max_event_ms.max(clock);
                registry.audits.insert(
                    (audit_id, "I"),
                    AuditChange {
                        audit_id,
                        order_id,
                        op: "I",
                        status,
                        revision: 1,
                        changed_ms: clock,
                    },
                );
            }
            awaiting_update.push((audit_id, order_id));
            published_count.fetch_add(1, Ordering::Relaxed);
        }

        // And an update of an earlier one every fifth, so the change stream
        // carries `op = "U"` as well as `"I"`.
        if iteration.is_multiple_of(5)
            && let Some((id, order)) = awaiting_update.pop()
        {
            clock += args.ts_step_ms;
            let status = STATUSES[1 + rng.random_range(0..STATUSES.len() - 1)];
            pg.execute(&update_audit, &[&id, &status, &2i32, &clock])
                .await?;
            {
                let mut registry = verifier.registry.lock().expect("registry lock");
                registry.max_event_ms = registry.max_event_ms.max(clock);
                registry.audits.insert(
                    (id, "U"),
                    AuditChange {
                        audit_id: id,
                        order_id: order,
                        op: "U",
                        status,
                        revision: 2,
                        changed_ms: clock,
                    },
                );
            }
            published_count.fetch_add(1, Ordering::Relaxed);
        }

        if !args.quiet && iteration.is_multiple_of(200) {
            let registry = verifier.registry.lock().expect("registry lock");
            println!(
                "published {order_id} orders / {payment_id} payments / {audit_id} audit rows; \
                 received {} bodies, {} failures so far",
                verifier.requests.load(Ordering::Relaxed),
                registry.failure_count
            );
        }

        // Deadline-based rather than cumulative sleeping, so a slow publish
        // does not push every later iteration out by the same amount.
        let deadline = started + spacing * u32::try_from(iteration).unwrap_or(u32::MAX);
        if let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
            tokio::select! {
                () = tokio::time::sleep(remaining) => {}
                () = cancel.cancelled() => break,
            }
        }
    }

    // Whatever is left in the buffer is still a whole batch of one priority.
    if !pending.is_empty() {
        let payload = pending.join("\n");
        let record = rdkafka::producer::FutureRecord::to(KAFKA_TOPIC)
            .key(pending_priority)
            .payload(&payload);
        kafka
            .send(
                record,
                rdkafka::util::Timeout::After(Duration::from_secs(10)),
            )
            .await
            .map_err(|(e, _)| e)?;
        kafka_messages += 1;
    }

    if let Some(task) = lifecycle_task {
        let _ = task.await;
    }

    println!(
        "publishing finished after {:.1}s; draining for {}s",
        started.elapsed().as_secs_f64(),
        args.drain_secs
    );
    tokio::time::sleep(Duration::from_secs(args.drain_secs)).await;

    let problems = finalise(&verifier, args.window_warmup_secs);
    report(&verifier, &steps, kafka_messages, &args);

    cancel.cancel();
    let _ = server.await;

    let failures = verifier
        .registry
        .lock()
        .expect("registry lock")
        .failure_count;
    let lifecycle_failed = steps.lock().expect("steps lock").iter().any(|s| !s.ok);
    if failures > 0 || lifecycle_failed || !problems.is_empty() {
        std::process::exit(1);
    }
    Ok(())
}

fn report(
    verifier: &Verifier,
    steps: &Arc<Mutex<Vec<LifecycleStep>>>,
    kafka_messages: u64,
    args: &Args,
) {
    let registry = verifier.registry.lock().expect("registry lock");
    let expected_express = registry
        .orders
        .values()
        .filter(|o| o.priority == "express")
        .count();
    let expected_standard = registry.orders.len() - expected_express;
    let expected_audit = registry.audits.len();

    println!();
    println!("── integrity report ─────────────────────────────────────────────");
    println!(
        "published: {} orders in {kafka_messages} kafka messages, {} payments, {} audit changes; \
         {} catalog rows read from {}",
        registry.orders.len(),
        registry.payments.len(),
        expected_audit,
        verifier.catalog.skus.len(),
        args.catalog_file
    );
    println!(
        "simulated event time: {BASE_TS_MS} .. {} ({} ms per item)",
        registry.max_event_ms, args.ts_step_ms
    );
    println!();
    println!(
        "{:<10} {:>8} {:>9} {:>8} {:>7} {:>10} {:>9}",
        "sink", "bodies", "rows", "unique", "expect", "duplicate", "mismatch"
    );
    for (sink, expected) in [
        ("express", expected_express as u64),
        ("standard", expected_standard as u64),
        // Only definitely-closed groups are asserted on; the trailing open
        // window is still accumulating inside the processor.
        ("window", registry.closed_window_groups),
        ("audit", expected_audit as u64),
    ] {
        let stats = registry.sinks.get(sink).copied().unwrap_or_default();
        println!(
            "{sink:<10} {:>8} {:>9} {:>8} {:>7} {:>10} {:>9}",
            stats.batches, stats.rows, stats.unique, expected, stats.duplicates, stats.mismatches
        );
    }
    println!();
    println!(
        "window coverage: {} closed (window_id, region) groups VERIFIED, {} NOT VERIFIED \
         (excluded: their event time falls in the first {}s, the fan-in warm-up). The window \
         aggregate is the only component with an exclusion; completeness for orders, payments \
         and audit changes above is absolute, over every published row.",
        registry.closed_window_groups, registry.warmup_window_groups, args.window_warmup_secs
    );
    println!(
        "  why: the windowed node is a fan-in. Its payment leg reaches `settle` straight from \
         NATS while its order leg crosses Kafka, `classify` and the channel bridge, so until \
         both legs stream the payment leg alone carries the watermark and the host drops whole \
         order arrivals below `watermark - {ALLOWED_LATENESS_MS}`. Those rows are drained and \
         counted, never lost: read `saci_window_late_arrivals_total` and `aggregate.late_rows` \
         on {}/metrics. Raise --window-warmup-secs if the excluded span is too short, lower it \
         to widen coverage.",
        args.stream_url
    );
    println!(
        "  a group's totals are checked as the SUM of its distinct emissions: a re-fire carries \
         only the delta, because the processor drops the group as it emits and a later in-budget \
         row recreates it empty. A RetryingSink re-send repeats a whole body byte for byte and \
         is counted, not summed again. No party emits a checksum over the summed totals, so \
         what is checked per row is that row's own checksum over its own six fields."
    );
    println!();
    println!("decoded column types: {:?}", registry.types_seen);

    let steps = steps.lock().expect("steps lock");
    if steps.is_empty() {
        println!("lifecycle: skipped (--no-lifecycle)");
    } else {
        println!();
        println!("lifecycle:");
        for step in steps.iter() {
            println!(
                "  [{}] {}: {}",
                if step.ok { "ok" } else { "FAIL" },
                step.name,
                step.outcome
            );
        }
        println!();
        println!(
            "what this proves: pause and resume keep the same source object alive, so the \
             JetStream consumer and the channel bridge lose nothing while `settle` is parked; \
             duplicates may still appear, because an offset, an ack and a slot advance are all \
             written at the start of the next next_batch call rather than when a batch is handed \
             downstream. stop and start rebuild the source, so `audit` redelivers the last \
             in-flight batch on restart: the assertion there is that nothing is missing and that \
             every redelivery carries identical values, never that delivery is exactly once."
        );
    }

    if registry.failure_count == 0 {
        println!();
        println!("VERDICT: PASS, every published row was accounted for");
    } else {
        println!();
        println!(
            "failures ({} total, first {} shown):",
            registry.failure_count,
            registry.failures.len()
        );
        for failure in &registry.failures {
            println!("  - {failure}");
        }
        println!();
        println!("VERDICT: FAIL");
    }
}
