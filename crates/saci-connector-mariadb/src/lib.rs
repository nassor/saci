//! `saci-connector-mariadb`: a MariaDB and MySQL [`Source`] and [`Sink`] for SACI.
//!
//! [`Source`]: saci_core::io::source::Source
//! [`Sink`]: saci_core::io::sink::Sink
//!
//! One crate reads from and writes to both MariaDB and MySQL servers. A
//! `mysql://` and a `mariadb://` DSN are the same thing: the connector asks the
//! server which one answered and adapts the few statements that differ.
//!
//! Both halves are declarative: the Arrow schema is written in the service
//! configuration, not introspected, because [`SourceFactory::build`] is
//! synchronous and cannot open a connection. The declared schema is checked
//! against the live catalog on the first query and a column the server cannot
//! fill is a loud error rather than a silent coercion.
//!
//! [`SourceFactory::build`]: saci_connector::factory::SourceFactory::build
//!
//! # Source modes
//!
//! One `mode` node inside the source's `config` picks the read strategy.
//!
//! `kind="polling"` runs an incremental query over a live table, ordered by a
//! cursor column, resuming from a durable offset row. It sees inserts, and it
//! sees updates only when the cursor column is an `updated_at`-style value that
//! the writer bumps. It never sees deletes.
//!
//! `kind="cdc_trigger"` reads a changelog table. With `install_triggers #false`
//! that is the operator's own outbox, read the way `polling` reads a table, and
//! it can prune rows it has acknowledged with `retention="delete_acked"`. With
//! `install_triggers #true` the connector creates a changelog table and three
//! triggers on the watched table, so inserts, updates and deletes all arrive;
//! the cursor is the changelog's own `__seq`. The connector never drops a
//! trigger or a table.
//!
//! A cursor over an `AUTO_INCREMENT` column (a polled table's id, an outbox's
//! id, the changelog's `__seq`) is numbered when a row is written, not when its
//! transaction commits. The source holds back the rows behind a gap in the
//! numbers until a late commit fills it or the gap is passed: at once when
//! `INNODB_TRX` shows no running transaction that could fill it, otherwise after
//! `gap_timeout_ms` (or `gap_max_wait_ms` while such a transaction runs). A
//! `where_clause` does not hide the gaps.
//!
//! `kind="cdc_binlog"` reads the row binlog, so inserts, updates and deletes
//! arrive with their transaction metadata and no trigger or outbox is needed.
//! `position "file"` resumes by binlog file and position on both servers;
//! `position "gtid"` resumes by GTID set on MySQL. `snapshot "initial"` reads
//! the table's existing rows first, as one consistent snapshot, then streams
//! every change made after it.
//!
//! Every mode returns `Ok(None)` once it is caught up, so the batch runners as
//! well as the stream runner can drive it.
//!
//! ## Server prerequisites for `cdc_binlog`
//!
//! - MySQL 8.2 or newer, or MariaDB 10.5.2 or newer
//! - `log_bin` on, `binlog_format = ROW` and `binlog_row_image = FULL`
//! - on MySQL, `binlog_row_value_options` empty, so a partial JSON update never
//!   reaches the stream
//! - for `position "gtid"`, MySQL with `gtid_mode = ON`
//! - the connecting user holds `REPLICATION SLAVE` and `REPLICATION CLIENT`
//!   (MySQL) or `REPLICATION SLAVE` and `BINLOG MONITOR` (MariaDB), and for
//!   `snapshot "initial"` also `RELOAD`
//!
//! Each source registers as a replica with its own `server_id`; two sources on
//! one server need two ids.
//!
//! ## Reserved metadata columns
//!
//! A change source fills field names with the `__` prefix from the change
//! stream instead of from the row. Declare the ones you want, in any position;
//! any other `__`-prefixed field name is a configuration error, except in an
//! operator outbox, where every column is an ordinary column.
//!
//! | field | required `type` | `cdc_binlog` | installed `cdc_trigger` |
//! |---|---|---|---|
//! | `__op` | `utf8` | `"I"`, `"U"`, `"D"`, `"R"` | `"I"`, `"U"`, `"D"` |
//! | `__table` | `utf8` | `"schema.table"` | `"schema.table"` |
//! | `__binlog_file` | `utf8` | the binlog file | none |
//! | `__binlog_pos` | `uint64` | the event's position | none |
//! | `__gtid` | `utf8` | the transaction's GTID, NULL on MariaDB | none |
//! | `__seq` | `uint64` | none | the changelog's sequence number |
//! | `__commit_ts` | `timestamp_micros_utc` | the transaction's commit time | the change's time |
//!
//! `R` is a row of the initial snapshot. An update that changes a primary key
//! arrives as a `D` of the old row then an `I` of the new one.
//!
//! # Sink write modes
//!
//! One flush is one transaction.
//!
//! `write_mode="append"` is a plain multi-row `INSERT`. `"upsert"` adds
//! `ON DUPLICATE KEY UPDATE`, and `"ignore_conflicts"` leaves an existing row as
//! it is. Both need `conflict_columns` equal to the table's primary key or one of
//! its unique indexes. A batch that repeats a conflict key applies in batch
//! order, so the last row wins under `"upsert"` and the first under
//! `"ignore_conflicts"`, unless `dedupe_order_column` names the column whose
//! greatest value wins. `"apply_changes"` reads a declared
//! `__op` column and applies each row in order: `I`, `U` and `R` upsert, `D`
//! deletes. It is how a `cdc_binlog` source mirrors a table, deletes included.
//!
//! The sink session runs a strict `sql_mode`, so an out-of-range value or a
//! truncation fails the flush and writes nothing.
//!
//! # Types
//!
//! Every column type is read and written through one table in
//! `types::TYPES`. A type with no Arrow counterpart reads as `utf8` in the
//! server's own text form. The connector never narrows silently: a `bigint`
//! declared `int32` is a configuration error naming the types the column can
//! fill.
//!
//! # Delivery semantics
//!
//! Every source commits its offset at the start of the *next* drain cycle,
//! because [`Source`](saci_core::io::source::Source) has no acknowledgement
//! hook. A crash mid-cycle replays that cycle: delivery is **at-least-once**,
//! matching what the distributed layer already promises. Configure the sink
//! `write_mode="upsert"`, `"ignore_conflicts"` or `"apply_changes"` to make
//! replays idempotent.
//!
//! # Configuration
//!
//! ```kdl
//! source "orders_cdc" type="MariadbSource" component="OrderChange" {
//!     config name="orders_cdc" batch_rows=8192 {
//!         connection dsn="${SACI_MARIADB_DSN}" ssl_mode="verify_identity" \
//!             connect_timeout_ms=5000 statement_timeout_ms=30000
//!         mode kind="cdc_binlog" table="shop.orders" position="file" \
//!             snapshot="initial" server_id=4100
//!         schema_fields "__op" type="utf8" nullable=#false
//!         schema_fields "id" type="int64" nullable=#false
//!         schema_fields "total" type="decimal128" precision=12 scale=2
//!         schema_fields "note" type="utf8" mariadb_type="varchar"
//!     }
//! }
//!
//! sink "orders_mirror" type="MariadbSink" component="OrderChange" {
//!     config name="orders_mirror" table="shop.orders_copy" \
//!         write_mode="apply_changes" conflict_columns="id" {
//!         connection dsn="${SACI_MARIADB_SINK_DSN}" ssl_mode="verify_identity"
//!         schema_fields "__op" type="utf8" nullable=#false
//!         schema_fields "id" type="int64" nullable=#false
//!         schema_fields "total" type="decimal128" precision=12 scale=2
//!         schema_fields "note" type="utf8"
//!     }
//! }
//! ```
//!
//! The `source` node's declared id is what the service logs; the `name` inside
//! its `config` is what the connector sees, because the factory is handed only
//! the `config` sub-table. Keep them equal for readable output.
//!
//! `${VAR}` and `${VAR:-default}` are substituted by the service before the
//! configuration is parsed, so credentials stay out of the file. Nothing in this
//! crate ever logs a DSN, user or password: every message names the target as
//! `host:port/db` only.
//!
//! # Cargo features
//!
//! - `tls` (default): rustls-backed TLS on the `ring` provider. Every
//!   `ssl_mode` but `disabled` needs it.
//! - `tracing`: emit `tracing` events.
//! - `metrics`: record into the process-global OpenTelemetry meter `saci`.

#![deny(missing_docs)]

pub mod config;

pub mod factory;

pub mod sink;
pub mod source;

mod connection;
mod encode;
mod metrics;
mod offsets;
mod types;
mod values;

pub use factory::{MariadbSinkFactory, MariadbSourceFactory};
pub use sink::MariadbSink;
pub use source::MariadbSource;

pub use config::{
    BinlogMode, ConnectionConfig, CursorMode, FieldSpec, MariadbFieldType, MariadbSinkConfig,
    MariadbSourceConfig, PositionMode, ReconnectConfig, Retention, SnapshotMode, SourceMode,
    SslModeConfig, TriggerMode, WriteMode,
};
