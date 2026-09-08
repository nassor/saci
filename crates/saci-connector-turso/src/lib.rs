//! `saci-connector-turso`: a Turso [`Source`] and [`Sink`] for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! Turso is an in-process, SQLite-compatible SQL database. The connector reaches
//! it two ways: an embedded local database file
//! ([`turso::Builder::new_local`]) or an embedded replica kept in sync with a
//! remote endpoint ([`turso::sync::Builder::new_remote`], against Turso Cloud or
//! a self-hosted sqld). Both hand out the same connection type, so the source
//! and sink behave identically either way; only `connection.path` plus an
//! optional `connection.remote` table chooses between them.
//!
//! Both halves are declarative: the Arrow schema is written in the service
//! configuration, not introspected, because [`SourceFactory::build`] is
//! synchronous and cannot open a connection. A declared column the table cannot
//! fill is a loud error rather than a silent coercion.
//!
//! [`SourceFactory::build`]: saci_connector::factory::SourceFactory::build
//!
//! # Source modes
//!
//! One `mode` node inside the source's `config` picks the read strategy.
//!
//! `kind="polling"` runs an incremental query ordered by a cursor column,
//! resuming from a durable offset row. It sees inserts, and updates only when
//! the cursor column is an `updated_at`-style value the writer bumps. It never
//! sees deletes.
//!
//! `kind="dump"` paginates the whole table and starts over once it is
//! exhausted, so every scan re-reads every row. `max_batches_per_cycle` yields
//! control mid-scan without restarting it.
//!
//! `kind="cdc"` reads the change table the engine writes when a connection has
//! `PRAGMA capture_data_changes_conn` enabled. It sees inserts,
//! updates and deletes alike. Capture is **per connection**: a connection
//! records only the changes made through that same connection, so the source
//! can only observe a table some writer opted into. It reads the change table
//! regardless; a missing table is a loud configuration error naming that pragma
//! rather than an empty stream.
//!
//! Each mode returns `Ok(None)` once it is caught up, so the batch runners as
//! well as the stream runner can drive it.
//!
//! ## Reserved fields for `cdc`
//!
//! `cdc` fills six `__`-prefixed field names from the change stream instead of
//! from the decoded row. Declare the ones you want, in any position; any other
//! `__`-prefixed field name is a configuration error. Every non-reserved field
//! is decoded from the row image: `after` for an insert or update, `before` for
//! a delete. A field the chosen change mode does not capture is null.
//!
//! | field | required `type` | value |
//! |---|---|---|
//! | `__op` | `utf8` | `"I"`, `"U"` or `"D"` |
//! | `__change_id` | `int64` | the change's `change_id` |
//! | `__change_time` | `int64` | Unix epoch seconds |
//! | `__txn_id` | `int64` | the change's `change_txn_id` |
//! | `__table` | `utf8` | the record's `table_name` |
//! | `__rowid` | `int64` | the changed row's rowid |
//!
//! A COMMIT record carries no image and is skipped, though its `change_id` still
//! advances the cursor. The engine's change-image decoder refuses BLOB values,
//! so a `binary` field cannot be read in `cdc` mode.
//!
//! ## Change-table retention
//!
//! `retention="keep"` (the default) leaves acknowledged records for an operator
//! to prune. `retention="delete_acked"` deletes them as the cursor advances; it
//! is refused on a synced connection, whose sync engine consumes the same change
//! table.
//!
//! # Sink write modes
//!
//! Rows are buffered and flushed in one transaction once `chunk_rows` rows are
//! pending, so a pipeline iteration lands atomically downstream.
//!
//! `write_mode="append"` inserts directly. `"upsert"` adds
//! `ON CONFLICT (…) DO UPDATE`, and `"ignore_conflicts"` the same with
//! `DO NOTHING`. Both need `conflict_columns`, and SQLite resolves the conflict
//! target when the statement is prepared: a column set with no matching
//! `PRIMARY KEY` or `UNIQUE` constraint is refused as the sink connects, naming
//! the columns.
//!
//! A `capture` block enables change capture on the sink's own connection, so the
//! sink's writes are recorded in the change table for a `cdc` source to read.
//!
//! `transaction="deferred"` (the default) and `"immediate"` are the two ordinary
//! `BEGIN` forms. `"concurrent"` is the MVCC path: the sink sets
//! `journal_mode = 'mvcc'` on its connection and flushes between
//! `BEGIN CONCURRENT` and `COMMIT`, retrying a write-write conflict up to
//! `conflict_retries` times. MVCC and change capture are mutually exclusive in
//! the engine, so `transaction="concurrent"` cannot be combined with a
//! `capture` block.
//!
//! # Encryption
//!
//! `connection.encryption` enables page-level encryption at rest for an
//! embedded database: `cipher` names the algorithm (`aegis256` and the other
//! AEGIS variants, or `aes128gcm`/`aes256gcm`) and `hexkey` its hex key. The
//! engine encrypts every page, the database file and the WAL, and the key is
//! never stored on disk. A synced replica takes no local encryption, so the two
//! cannot be combined.
//!
//! # Declared types
//!
//! SQLite is dynamically typed, so every value is coerced to the declared type
//! and a value that does not fit is an error. `utf8` is also the declared type
//! for a date or timestamp column, which SQLite stores as text.
//!
//! | `type` | accepted value | Arrow |
//! |---|---|---|
//! | `utf8` | `TEXT` | `Utf8` |
//! | `int64` | `INTEGER` | `Int64` |
//! | `float64` | `REAL`, or an `INTEGER` widened | `Float64` |
//! | `bool` | `INTEGER` 0/1 | `Boolean` |
//! | `binary` | `BLOB` | `Binary` |
//! | `decimal128` | `TEXT` or `INTEGER`, with `precision` and `scale` | `Decimal128` |
//!
//! There is no unsigned integer: the engine's only integer is a signed 64-bit
//! one, so a `uint64` field is not a Turso config.
//!
//! # Delivery semantics
//!
//! The `polling` and `cdc` cursors are committed at the start of the *next*
//! fetch, because [`Source`](saci_core::io::source::Source) has no
//! acknowledgement hook. A crash mid-cycle replays that cycle: delivery is
//! **at-least-once**, matching what the distributed layer already promises.
//! Configure the sink `write_mode="upsert"` or `"ignore_conflicts"` to make
//! replays idempotent.
//!
//! # Configuration
//!
//! ```kdl
//! source "turso_orders" type="TursoSource" component="OrderChange" {
//!     config name="turso_orders" batch_rows=8192 {
//!         connection path="orders.db"
//!         mode kind="polling" table="orders" cursor_column="id"
//!         schema_fields "id" type="int64" nullable=#false
//!         schema_fields "total" type="float64"
//!     }
//! }
//!
//! sink "turso_enriched" type="TursoSink" component="EnrichedOrder" {
//!     config name="turso_enriched" table="enriched_orders" \
//!         write_mode="upsert" conflict_columns="id" {
//!         connection path="orders.db"
//!         schema_fields "id" type="int64" nullable=#false
//!         schema_fields "total" type="float64"
//!     }
//! }
//! ```
//!
//! A synced connection adds a `remote` table:
//!
//! ```kdl
//! connection path="replica.db" {
//!     remote url="${SACI_TURSO_URL}" token="${SACI_TURSO_TOKEN}"
//! }
//! ```
//!
//! A synced source pulls once per drain cycle and a synced sink pushes after its
//! final flush. `connection.remote` carries the endpoint, the token, the
//! bootstrap switch, an optional long-poll budget and an optional
//! `logical_mvcc_pull` override for a server that insists on the MVCC
//! logical-log stream.
//!
//! `${VAR}` and `${VAR:-default}` are substituted by the service before the
//! configuration is parsed, so credentials stay out of the file. Nothing in this
//! crate ever logs a remote token: every message names the embedded path or the
//! remote URL's host only.
//!
//! # Cargo features
//!
//! - `tracing`: emit `tracing` events.
//! - `metrics`: record into the process-global OpenTelemetry meter `saci`.

#![deny(missing_docs)]

pub mod config;
pub mod factory;
pub mod sink;
pub mod source;

mod cdc;
mod connection;
mod metrics;
mod offsets;
mod types;

pub use config::{
    CaptureConfig, CaptureMode, CdcMode, ConnectionConfig, CursorMode, DumpMode, EncryptionConfig,
    FieldSpec, RemoteConfig, Retention, SourceMode, SyncConfig, TransactionMode, TursoFieldType,
    TursoSinkConfig, TursoSourceConfig, WriteMode,
};
pub use factory::{TursoSinkFactory, TursoSourceFactory};
pub use sink::TursoSink;
pub use source::TursoSource;
