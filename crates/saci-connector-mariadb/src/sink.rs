//! [`MariadbSink`]: batched `INSERT` into a MariaDB or MySQL table, with upsert,
//! conflict-ignoring and change-applying write modes.
//!
//! One flush is one transaction, so a pipeline iteration lands atomically
//! downstream. `append` is a plain `INSERT`; `upsert` adds
//! `ON DUPLICATE KEY UPDATE`; `ignore_conflicts` rewrites a key column to itself
//! (never `INSERT IGNORE`, which also hides truncation and type errors);
//! `apply_changes` walks the declared `__op` column in order, upserting `I`, `U`
//! and `R` rows and deleting `D` rows.
//!
//! [`new`](MariadbSink::new) opens no connection, mirroring
//! [`MariadbSource::new`](crate::source::MariadbSource::new): the first flush of
//! a session connects, reads the target's catalog, checks it against the
//! declared schema and, for every mode but `append`, checks that
//! `conflict_columns` is exactly one unique key.
//!
//! # Statements
//!
//! Rows are written in chunks of `min(chunk_rows, 65535 / written columns)`,
//! because a prepared statement carries at most 65535 placeholders, and of at
//! most the session's `max_allowed_packet` in estimated bytes (`Chunker`),
//! because the server refuses a statement over that whatever it holds. A
//! `DELETE` carries at most 1000 keys: past a few thousand keys
//! both servers abandon range access and scan the table, which took minutes on
//! MySQL for 16 383 composite keys.
//!
//! The sink connection keeps no statement cache (see
//! `Connector::new`), so the sink owns its
//! statement handles: a full chunk statement is prepared once per flush, a
//! partial chunk is prepared, run and closed on the spot, and every handle is
//! closed before the transaction ends, on success and on failure. Do not turn
//! the cache on to save the prepares: MySQL reprepares a cached statement
//! against a redefined table with stale parameter state and stores wrong
//! values. Do not leave the cache off without closing handles either: mysql_async
//! then leaks a server-side statement per `exec`.
//!
//! # Failure
//!
//! A failed flush rolls back whole. The batch whose `write_batch` call failed is
//! dropped from the buffer, because the caller was told it failed and re-drives
//! it (`RetryingSink` does); keeping it too would write its rows twice. Rows an
//! earlier call accepted under `flush_rows`, and everything a failed `finish`
//! held, stay buffered for the next flush.
//!
//! # Cancellation
//!
//! A call can be dropped before it returns (a runner `select!`s on a cancel
//! token, a caller wraps it in `timeout`). The driver is then between a command
//! and its reply, or inside the transaction, and a connection in that state
//! cannot be reused: the next statement would read the dropped one's reply, or
//! implicitly commit its transaction. A flush therefore marks itself `in_flight`
//! and `recover`, run first thing in every call, discards the session when the
//! mark is still set; the server rolls an open transaction back when the socket
//! closes. The buffer is only cleared once a commit returns, so a dropped flush
//! loses no row. A dropped `write_batch` is treated like a failed one (its own
//! batch is taken out, the caller re-drives it), a dropped `finish` keeps what it
//! held, and `truncate_before_first_write` runs again because no flush has
//! returned. If the dropped flush did commit, the re-drive writes its rows a
//! second time: harmless for `upsert`, `ignore_conflicts` and `apply_changes`,
//! and the same duplicate an `append` retry after a lost `COMMIT` makes.
//! `pending_rows` still counts a dropped call's rows until the next call.
//!
//! # What strict mode does not catch
//!
//! The session `sql_mode` turns out-of-range values and truncation into errors.
//! It does not cover fraction digits a `time`, `datetime` or `timestamp` column
//! cannot hold: MariaDB truncates them and MySQL rounds them, silently. The sink
//! refuses such a value itself.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::Schema;
use async_trait::async_trait;
use mysql_async::prelude::*;
use mysql_async::{Statement, Transaction, TxOpts, Value};
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;

use crate::config::{FieldSpec, MariadbSinkConfig, Role, WriteMode};
use crate::connection::{
    Connector, Flavor, Session, TableRef, columns_of, ignore_tail, mysql_detail, quote_columns,
    quote_ident, unique_keys_of, upsert_tail,
};
use crate::encode::{ColumnReader, resolve_columns, row_values};
use crate::metrics::Instruments;
use crate::types::{Access, Family, ResolvedColumn, validate_columns};

const WHAT: &str = "MariadbSink";

/// Most placeholders a prepared statement may carry, on every supported server.
const PLACEHOLDER_LIMIT: usize = 65_535;

/// Most keys one `DELETE` names. Measured on both servers: 1 000 composite keys
/// run in 14 to 28 ms, 16 383 in 1.7 s (MariaDB) to 238 s (MySQL).
pub(crate) const DELETE_KEYS_PER_STATEMENT: usize = 1_000;

/// A MariaDB or MySQL [`Sink`].
pub struct MariadbSink {
    connector: Connector,
    session: Option<Session>,
    /// Settled on the first flush of a session.
    prepared: Option<Prepared>,
    table: TableRef,
    schema: Arc<Schema>,
    /// The declared fields the sink writes, in declared order: every field, or
    /// under `apply_changes` every field without the `__` prefix.
    written: Vec<FieldSpec>,
    /// Every declared field, to find the conflict columns and `__op` in a batch.
    fields: Vec<FieldSpec>,
    write_mode: WriteMode,
    conflict_columns: Vec<String>,
    /// `update_columns` when given, otherwise every written column that is not a
    /// conflict column.
    update_columns: Vec<String>,
    dedupe_order_column: Option<String>,
    chunk_rows: usize,
    flush_rows: usize,
    truncate_before_first_write: bool,
    buffer: Vec<RecordBatch>,
    buffered_rows: usize,
    /// Rows of the batch a `write_batch` is flushing, the last in `buffer`, while
    /// that call is running. Still set at the next call means it was dropped.
    own_rows: usize,
    /// Set while a flush uses the connection. Still set at the next call means
    /// the flush was dropped and the connection cannot be trusted.
    in_flight: bool,
    /// Set once a flush committed. It never resets: a reconnect that truncated
    /// again would delete rows an earlier flush of the same run wrote.
    first_write_done: bool,
    instruments: Instruments,
}

/// What one session needs to write, resolved against the catalog.
#[derive(Debug)]
struct Prepared {
    /// The quoted target, `` `db`.`t` ``.
    table: String,
    /// `INSERT INTO <t> (<cols>) VALUES `.
    insert_prefix: String,
    /// The update clause, with a leading space, or empty.
    tail: String,
    rows_per_insert: usize,
    keys_per_delete: usize,
    /// Most bytes one statement may carry: the session's `max_allowed_packet`
    /// less [`PACKET_SLACK`].
    packet_budget: usize,
    /// `(index into the written fields, modulus)` for each temporal column
    /// whose server type keeps fewer than six fraction digits.
    fraction_guards: Vec<(usize, u32)>,
}

/// One error out of a flush: what to report, how to count it, and whether the
/// session is gone.
#[derive(Debug)]
struct Failure {
    error: SaciError,
    kind: &'static str,
    fatal: bool,
}

impl Failure {
    fn encode(error: SaciError) -> Self {
        Self {
            error,
            kind: "encode",
            fatal: false,
        }
    }

    fn server(kind: &'static str, what: String, e: &mysql_async::Error) -> Self {
        Self {
            error: SaciError::generic(format!("{WHAT}: {what}: {}", mysql_detail(e))),
            kind,
            fatal: e.is_fatal(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunKind {
    Upsert,
    Delete,
}

/// Consecutive rows that take the same action, as `(batch, row)` positions.
#[derive(Debug)]
struct Run {
    kind: RunKind,
    rows: Vec<(u32, u32)>,
}

impl MariadbSink {
    /// Validate `cfg` and prepare the sink. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for any violation
    /// [`MariadbSinkConfig::validate`] reports, for a DSN that does not parse,
    /// for an unreadable `password_file` or `ssl_ca`, for a bare table name when
    /// the DSN names no database, and for a TLS mode this build lacks.
    pub fn new(cfg: MariadbSinkConfig) -> Result<Self, SaciError> {
        cfg.validate()?;

        let fields = cfg
            .schema_fields
            .iter()
            .map(|spec| spec.to_arrow_field())
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Arc::new(Schema::new(fields));
        let connector = Connector::new(Role::Sink, &cfg.connection)?;
        let table = connector.resolve_table("table", &cfg.table)?;
        let written = cfg.written_fields().into_iter().cloned().collect();
        let update_columns = cfg
            .effective_update_columns()
            .into_iter()
            .map(str::to_string)
            .collect();

        #[cfg(feature = "tracing")]
        tracing::info!(
            sink = %cfg.name,
            table = %table,
            write_mode = cfg.write_mode.as_str(),
            columns = cfg.schema_fields.len(),
            "mariadb sink configured"
        );

        Ok(Self {
            connector,
            session: None,
            prepared: None,
            table,
            schema,
            written,
            fields: cfg.schema_fields.clone(),
            write_mode: cfg.write_mode,
            conflict_columns: cfg.conflict_columns.clone(),
            update_columns,
            dedupe_order_column: cfg.dedupe_order_column.clone(),
            chunk_rows: cfg.chunk_rows,
            flush_rows: cfg.flush_rows,
            truncate_before_first_write: cfg.truncate_before_first_write,
            buffer: Vec::new(),
            buffered_rows: 0,
            own_rows: 0,
            in_flight: false,
            first_write_done: false,
            instruments: Instruments::sink(&cfg.name),
        })
    }

    /// Reject a batch whose schema is not the declared one.
    fn check_schema(&self, batch: &RecordBatch) -> Result<(), SaciError> {
        let expected = self.schema.fields();
        let actual = batch.schema_ref().fields();
        if expected.len() != actual.len() {
            return Err(SaciError::generic(format!(
                "{WHAT}: batch has {} column(s) but '{}' declares {}",
                actual.len(),
                self.table,
                expected.len()
            )));
        }
        for (want, got) in expected.iter().zip(actual.iter()) {
            if want.name() != got.name() {
                return Err(SaciError::generic(format!(
                    "{WHAT}: batch column '{}' does not match the declared column '{}'; \
                     schema_fields order is the batch order",
                    got.name(),
                    want.name()
                )));
            }
            if want.data_type() != got.data_type() {
                return Err(SaciError::generic(format!(
                    "{WHAT}: batch column '{}' is {:?} but '{}' declares {:?}",
                    got.name(),
                    got.data_type(),
                    self.table,
                    want.data_type()
                )));
            }
        }
        Ok(())
    }

    /// Connect if needed and settle the catalog checks once per session.
    async fn ensure_session(&mut self) -> Result<(), SaciError> {
        if let Some(session) = self.session.as_mut()
            && session.conn.ping().await.is_err()
        {
            // An idle connection the server or a proxy dropped: replace it now
            // rather than fail a flush on it.
            self.session = None;
        }
        if self.session.is_none() {
            self.prepared = None;
            match self.connector.connect_with_retry().await {
                Ok(session) => self.session = Some(session),
                Err(e) => {
                    self.instruments.error("connect");
                    return Err(e);
                }
            }
        }
        if self.prepared.is_none() {
            match self.resolve().await {
                Ok(prepared) => self.prepared = Some(prepared),
                Err(e) => {
                    self.instruments.error("write");
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Read the target's catalog, check the declared schema and the conflict key
    /// against it, and build the statement shapes.
    async fn resolve(&mut self) -> Result<Prepared, SaciError> {
        let Some(session) = self.session.as_mut() else {
            return Err(SaciError::generic(format!("{WHAT}: no session")));
        };
        let catalog = columns_of(
            &mut session.conn,
            WHAT,
            &self.table,
            self.connector.target(),
        )
        .await?;
        let resolved = validate_columns(WHAT, Access::Write, self.written.iter(), &catalog)?;

        if self.write_mode != WriteMode::Append {
            let keys = unique_keys_of(&mut session.conn, WHAT, &self.table).await?;
            if !key_matches(&self.conflict_columns, &keys) {
                return Err(SaciError::configuration(format!(
                    "{WHAT}: conflict_columns ({}) match no PRIMARY KEY or UNIQUE index of '{}'; \
                     ON DUPLICATE KEY UPDATE would resolve on a different key",
                    self.conflict_columns.join(", "),
                    self.table
                )));
            }
        }

        // A statement is one packet, and the server refuses a packet over
        // `max_allowed_packet` (MariaDB also drops the connection), however few
        // rows it holds.
        let packet: Option<u64> = session
            .conn
            .query_first("SELECT @@max_allowed_packet")
            .await
            .map_err(|e| {
                SaciError::generic(format!(
                    "{WHAT}: cannot read max_allowed_packet on {}: {}",
                    self.connector.target(),
                    mysql_detail(&e)
                ))
            })?;
        let packet =
            usize::try_from(packet.unwrap_or(DEFAULT_MAX_ALLOWED_PACKET)).unwrap_or(usize::MAX);
        let columns = self.written.len();
        let rows_per_insert = rows_per_insert(self.chunk_rows, columns);
        let keys_per_delete = keys_per_delete(rows_per_insert, self.conflict_columns.len());
        let table = self.table.quoted();
        let insert_prefix = format!(
            "INSERT INTO {table} ({}) VALUES ",
            quote_columns(self.written.iter().map(|f| f.name.as_str()))
        );
        let update: Vec<&str> = self.update_columns.iter().map(String::as_str).collect();
        let tail = tail_for(
            self.write_mode,
            session.flavor,
            &update,
            self.conflict_columns.first().map_or("", String::as_str),
        );

        Ok(Prepared {
            table,
            insert_prefix,
            tail,
            rows_per_insert,
            keys_per_delete,
            packet_budget: packet.saturating_sub(PACKET_SLACK),
            fraction_guards: fraction_guards(&resolved),
        })
    }

    /// Settle what a call that was dropped before it returned left behind.
    ///
    /// A runner that `select!`s on a cancel token drops the call it awaits, and
    /// the driver then holds a connection between a command and its reply, or
    /// inside the transaction. Reusing it would read the dropped call's reply
    /// as the next one's, or implicitly commit the dropped transaction. So the
    /// session goes, and the server rolls the open transaction back when the
    /// socket closes. The next flush reconnects and reads the catalog again.
    ///
    /// A dropped `write_batch` never told its caller whether the batch landed;
    /// like a failed one it is taken out of the buffer, because the caller is
    /// expected to re-drive it. A dropped `finish`, and the rows earlier calls
    /// accepted, stay buffered. Nothing marks `first_write_done` for a flush
    /// that did not return, so `truncate_before_first_write` clears the target
    /// again, which is right whether or not the dropped flush committed.
    fn recover(&mut self) {
        if self.in_flight {
            self.session = None;
            self.prepared = None;
            self.in_flight = false;
        }
        if self.own_rows > 0 {
            self.buffer.pop();
            self.buffered_rows -= self.own_rows;
            self.own_rows = 0;
            self.instruments.pending(self.buffered_rows as u64);
        }
    }

    /// Write everything buffered in one transaction.
    async fn flush(&mut self) -> Result<(), SaciError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        // Cleared only when the connection is known to be in step again, which
        // a returned result is: the driver closes the stream on a fatal error.
        self.in_flight = true;
        let result = self.flush_buffered().await;
        self.in_flight = false;
        result
    }

    async fn flush_buffered(&mut self) -> Result<(), SaciError> {
        self.ensure_session().await?;

        let started = Instant::now();
        let truncate = self.truncate_before_first_write && !self.first_write_done;
        // A copy, not a `take`: the buffer is the only record of these rows, and
        // a call dropped inside `write` must leave them in it. A batch is a
        // handful of `Arc`s, so the copy is cheap.
        let batches = self.buffer.clone();
        let rows = self.buffered_rows;
        let outcome = self.write(&batches, truncate).await;

        match outcome {
            Ok(()) => {
                self.buffer.clear();
                self.buffered_rows = 0;
                self.first_write_done = true;
                self.instruments.batch(rows as u64);
                self.instruments.observe(started.elapsed().as_secs_f64());
                self.instruments.pending(0);
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    table = %self.table,
                    rows,
                    batches = batches.len(),
                    elapsed_us = started.elapsed().as_micros(),
                    "mariadb sink flushed"
                );
                Ok(())
            }
            Err(failure) => {
                if failure.fatal {
                    self.session = None;
                    self.prepared = None;
                }
                // The rows stay in the buffer for `finish` or the next write.
                self.instruments.error(failure.kind);
                Err(failure.error)
            }
        }
    }

    /// Plan the rows, then run them in one transaction.
    async fn write(&mut self, batches: &[RecordBatch], truncate: bool) -> Result<(), Failure> {
        let runs = self.plan_runs(batches)?;
        let (Some(session), Some(prepared)) = (self.session.as_mut(), self.prepared.as_ref())
        else {
            return Err(Failure::encode(SaciError::generic(format!(
                "{WHAT}: no session"
            ))));
        };
        let written: Vec<&FieldSpec> = self.written.iter().collect();
        let key_fields = key_fields(&self.fields, &self.conflict_columns);
        let ctx = Apply {
            prepared,
            written: &written,
            key_fields: &key_fields,
            key_columns: &self.conflict_columns,
            table: &self.table,
        };

        let table = &self.table;
        let mut tx = session
            .conn
            .start_transaction(TxOpts::default())
            .await
            .map_err(|e| {
                Failure::server(
                    "write",
                    format!("cannot open a transaction for '{table}'"),
                    &e,
                )
            })?;

        let mut handles = Handles::default();
        let body = ctx
            .run(&mut tx, &mut handles, batches, &runs, truncate)
            .await;
        // Handles die before the transaction does, on every path.
        handles.close(&mut tx).await;
        match body {
            Ok(()) => tx.commit().await.map_err(|e| {
                Failure::server(
                    "commit",
                    format!("cannot commit the write to '{table}'"),
                    &e,
                )
            }),
            Err(failure) => {
                let _ = tx.rollback().await;
                Err(failure)
            }
        }
    }

    /// Split the buffered rows into runs of upserts and deletes, in order.
    fn plan_runs(&self, batches: &[RecordBatch]) -> Result<Vec<Run>, Failure> {
        let everything = || {
            batches
                .iter()
                .enumerate()
                .flat_map(|(b, batch)| (0..batch.num_rows()).map(move |r| (b as u32, r as u32)))
        };
        match self.write_mode {
            WriteMode::Append | WriteMode::Upsert | WriteMode::IgnoreConflicts => {
                let mut rows: Vec<(u32, u32)> = everything().collect();
                if let Some(order) = &self.dedupe_order_column {
                    rows = self.dedupe(batches, rows, order)?;
                }
                Ok(vec![Run {
                    kind: RunKind::Upsert,
                    rows,
                }])
            }
            WriteMode::ApplyChanges => self.split_changes(batches),
        }
    }

    /// `apply_changes`: classify every row by `__op` before anything is written.
    fn split_changes(&self, batches: &[RecordBatch]) -> Result<Vec<Run>, Failure> {
        let key_fields = key_fields(&self.fields, &self.conflict_columns);
        let mut runs: Vec<Run> = Vec::new();
        let mut scratch: Vec<Value> = Vec::new();
        for (b, batch) in batches.iter().enumerate() {
            let ops = batch
                .column_by_name("__op")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    Failure::encode(SaciError::generic(format!(
                        "{WHAT}: apply_changes needs a utf8 '__op' column in the batch"
                    )))
                })?;
            let key_readers = resolve_columns(WHAT, batch, &key_fields).map_err(Failure::encode)?;
            for row in 0..batch.num_rows() {
                let kind = if ops.is_null(row) {
                    return Err(unknown_op(row, None));
                } else {
                    match ops.value(row) {
                        "I" | "U" | "R" => RunKind::Upsert,
                        "D" => RunKind::Delete,
                        other => return Err(unknown_op(row, Some(other))),
                    }
                };
                if kind == RunKind::Delete {
                    scratch.clear();
                    row_values(&key_readers, row, &mut scratch).map_err(Failure::encode)?;
                    if scratch.iter().any(|v| matches!(v, Value::NULL)) {
                        return Err(Failure::encode(SaciError::generic(format!(
                            "{WHAT}: row {row} has __op 'D' with a NULL conflict key, which \
                             matches no row"
                        ))));
                    }
                }
                let position = (b as u32, row as u32);
                match runs.last_mut() {
                    Some(run) if run.kind == kind => run.rows.push(position),
                    _ => runs.push(Run {
                        kind,
                        rows: vec![position],
                    }),
                }
            }
        }
        Ok(runs)
    }

    /// Keep, per conflict key, the row with the greatest `order` value; a tie
    /// goes to the later row. A key with a NULL part never conflicts, so its row
    /// is always kept. Values compare exactly, not by the column's collation.
    fn dedupe(
        &self,
        batches: &[RecordBatch],
        rows: Vec<(u32, u32)>,
        order: &str,
    ) -> Result<Vec<(u32, u32)>, Failure> {
        let key_fields = key_fields(&self.fields, &self.conflict_columns);
        let mut key_readers = Vec::with_capacity(batches.len());
        let mut order_arrays = Vec::with_capacity(batches.len());
        for batch in batches {
            key_readers.push(resolve_columns(WHAT, batch, &key_fields).map_err(Failure::encode)?);
            order_arrays.push(batch.column_by_name(order).ok_or_else(|| {
                Failure::encode(SaciError::generic(format!(
                    "{WHAT}: batch has no column '{order}' named by dedupe_order_column"
                )))
            })?);
        }

        let mut keep = vec![true; rows.len()];
        let mut winners: HashMap<Vec<u8>, (usize, Order<'_>)> = HashMap::new();
        let mut values: Vec<Value> = Vec::new();
        let mut key: Vec<u8> = Vec::new();
        for (i, &(b, r)) in rows.iter().enumerate() {
            values.clear();
            row_values(&key_readers[b as usize], r as usize, &mut values)
                .map_err(Failure::encode)?;
            key.clear();
            if !encode_key(&values, &mut key) {
                continue;
            }
            let value = order_value(order_arrays[b as usize].as_ref(), r as usize);
            match winners.get_mut(&key) {
                Some((held, held_value)) => {
                    if value.compare(held_value) == Ordering::Less {
                        keep[i] = false;
                    } else {
                        keep[*held] = false;
                        *held = i;
                        *held_value = value;
                    }
                }
                None => {
                    winners.insert(key.clone(), (i, value));
                }
            }
        }
        Ok(rows
            .into_iter()
            .zip(keep)
            .filter_map(|(row, kept)| kept.then_some(row))
            .collect())
    }
}

fn unknown_op(row: usize, op: Option<&str>) -> Failure {
    let shown = op.map_or_else(|| "NULL".to_string(), |op| format!("'{op}'"));
    Failure::encode(SaciError::generic(format!(
        "{WHAT}: row {row} has __op {shown}; apply_changes understands I, U, R and D"
    )))
}

/// The declared fields named by `conflict_columns`, in that order.
fn key_fields<'a>(fields: &'a [FieldSpec], conflict_columns: &[String]) -> Vec<&'a FieldSpec> {
    conflict_columns
        .iter()
        .filter_map(|name| fields.iter().find(|f| &f.name == name))
        .collect()
}

/// Rows per `INSERT`: the configured chunk, held under the placeholder limit.
fn rows_per_insert(chunk_rows: usize, written_columns: usize) -> usize {
    chunk_rows
        .min(PLACEHOLDER_LIMIT / written_columns.max(1))
        .max(1)
}

/// Keys per `DELETE`: the insert chunk, held under the delete cap and the
/// placeholder limit.
fn keys_per_delete(rows_per_insert: usize, key_columns: usize) -> usize {
    rows_per_insert
        .min(DELETE_KEYS_PER_STATEMENT)
        .min(PLACEHOLDER_LIMIT / key_columns.max(1))
        .max(1)
}

/// The clause that follows `VALUES (..)`, with a leading space, or empty.
fn tail_for(mode: WriteMode, flavor: Flavor, update: &[&str], key: &str) -> String {
    match mode {
        WriteMode::Append => String::new(),
        WriteMode::IgnoreConflicts => ignore_tail(key),
        WriteMode::Upsert | WriteMode::ApplyChanges => upsert_tail(flavor, update, key),
    }
}

/// `INSERT INTO .. VALUES (?,..),(?,..)` plus `tail`.
fn insert_sql(prefix: &str, columns: usize, rows: usize, tail: &str) -> String {
    let one = format!("({})", vec!["?"; columns].join(","));
    format!("{prefix}{}{tail}", vec![one; rows].join(","))
}

/// A `DELETE` naming `keys` keys: `IN (..)` for one key column,
/// `(a = ? AND b = ?) OR ..` for several.
fn delete_sql(table: &str, key_columns: &[String], keys: usize) -> String {
    if let [column] = key_columns {
        return format!(
            "DELETE FROM {table} WHERE {} IN ({})",
            quote_ident(column),
            vec!["?"; keys].join(",")
        );
    }
    let one = format!(
        "({})",
        key_columns
            .iter()
            .map(|c| format!("{} = ?", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(" AND ")
    );
    format!("DELETE FROM {table} WHERE {}", vec![one; keys].join(" OR "))
}

/// Whether `conflict` is, as an unordered set compared without regard to ASCII
/// case (the servers' column-name rule), the column set of one of `keys`.
fn key_matches(conflict: &[String], keys: &[Vec<String>]) -> bool {
    keys.iter().any(|key| {
        key.len() == conflict.len()
            && conflict
                .iter()
                .all(|c| key.iter().any(|k| k.eq_ignore_ascii_case(c)))
    })
}

/// `(index, modulus)` for each written temporal column that keeps fewer than
/// six fraction digits.
fn fraction_guards(resolved: &[ResolvedColumn]) -> Vec<(usize, u32)> {
    resolved
        .iter()
        .enumerate()
        .filter(|(_, c)| {
            matches!(
                c.family,
                Family::Time | Family::DateTime | Family::Timestamp
            ) && c.fractional < 6
        })
        .map(|(i, c)| (i, 10u32.pow(u32::from(6 - c.fractional))))
        .collect()
}

/// Append the canonical bytes of a key to `out`. Returns `false`, leaving `out`
/// meaningless, when any part is NULL.
fn encode_key(values: &[Value], out: &mut Vec<u8>) -> bool {
    for value in values {
        match value {
            Value::NULL => return false,
            Value::Int(v) => {
                out.push(1);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::UInt(v) => {
                out.push(2);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::Float(v) => {
                out.push(3);
                // `+ 0.0` turns -0.0 into 0.0, which the servers compare equal.
                out.extend_from_slice(&(*v + 0.0).to_bits().to_le_bytes());
            }
            Value::Double(v) => {
                out.push(4);
                out.extend_from_slice(&(*v + 0.0).to_bits().to_le_bytes());
            }
            Value::Bytes(v) => {
                out.push(5);
                out.extend_from_slice(&(v.len() as u64).to_le_bytes());
                out.extend_from_slice(v);
            }
            Value::Date(y, mo, d, h, mi, s, us) => {
                out.push(6);
                out.extend_from_slice(&y.to_le_bytes());
                out.extend_from_slice(&[*mo, *d, *h, *mi, *s]);
                out.extend_from_slice(&us.to_le_bytes());
            }
            Value::Time(neg, days, h, mi, s, us) => {
                out.push(7);
                out.push(u8::from(*neg));
                out.extend_from_slice(&days.to_le_bytes());
                out.extend_from_slice(&[*h, *mi, *s]);
                out.extend_from_slice(&us.to_le_bytes());
            }
        }
    }
    true
}

/// A comparable `dedupe_order_column` value. NULL is the lowest, so it never
/// beats a value.
#[derive(Debug, Clone, Copy)]
enum Order<'a> {
    Null,
    Int(i128),
    Float(f64),
    Bytes(&'a [u8]),
}

impl Order<'_> {
    fn compare(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Order::Null, Order::Null) => Ordering::Equal,
            (Order::Null, _) => Ordering::Less,
            (_, Order::Null) => Ordering::Greater,
            (Order::Int(a), Order::Int(b)) => a.cmp(b),
            (Order::Float(a), Order::Float(b)) => a.total_cmp(b),
            (Order::Bytes(a), Order::Bytes(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

/// The order value of `array` at `row`.
fn order_value(array: &dyn Array, row: usize) -> Order<'_> {
    if array.is_null(row) {
        return Order::Null;
    }
    macro_rules! int {
        ($($t:ty),*) => {$(
            if let Some(a) = array.as_any().downcast_ref::<$t>() {
                return Order::Int(i128::from(a.value(row)));
            }
        )*};
    }
    int!(
        Int8Array,
        Int16Array,
        Int32Array,
        Int64Array,
        UInt8Array,
        UInt16Array,
        UInt32Array,
        UInt64Array,
        Date32Array,
        Time64MicrosecondArray,
        TimestampMicrosecondArray
    );
    let any = array.as_any();
    if let Some(a) = any.downcast_ref::<BooleanArray>() {
        return Order::Int(i128::from(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<Decimal128Array>() {
        return Order::Int(a.value(row));
    }
    if let Some(a) = any.downcast_ref::<Float32Array>() {
        return Order::Float(f64::from(a.value(row)));
    }
    if let Some(a) = any.downcast_ref::<Float64Array>() {
        return Order::Float(a.value(row));
    }
    if let Some(a) = any.downcast_ref::<StringArray>() {
        return Order::Bytes(a.value(row).as_bytes());
    }
    if let Some(a) = any.downcast_ref::<BinaryArray>() {
        return Order::Bytes(a.value(row));
    }
    Order::Null
}

/// Bytes a statement may spare beyond its parameters: the packet header, the
/// statement id and flags, and the null bitmap, over-counted.
const PACKET_SLACK: usize = 1024;

/// `max_allowed_packet` assumed when the server reports none: MariaDB's
/// default, the smaller of the two products'.
const DEFAULT_MAX_ALLOWED_PACKET: u64 = 16 * 1024 * 1024;

/// The bytes `value` adds to an execute packet, over-counted: a length prefix
/// of up to nine bytes before a string, the fixed width of everything else.
fn value_bytes(value: &Value) -> usize {
    match value {
        Value::NULL => 0,
        Value::Bytes(bytes) => bytes.len() + 9,
        Value::Int(_) | Value::UInt(_) | Value::Double(_) => 8,
        Value::Float(_) => 4,
        Value::Date(..) => 12,
        Value::Time(..) => 13,
    }
}

/// Splits one run of rows into statements of at most `max_rows` rows and, so
/// that a statement fits in one packet, at most `budget` estimated bytes. A row
/// that is over the budget on its own still goes out, alone, for the server to
/// refuse by name.
struct Chunker<'r> {
    rows: std::slice::Iter<'r, (u32, u32)>,
    /// The row that did not fit the last chunk, already read.
    carry: Option<Vec<Value>>,
    max_rows: usize,
    budget: usize,
    /// The statement text outside its per-row part.
    base: usize,
    /// What each row adds to the statement text and the packet besides its
    /// values.
    per_row: usize,
}

impl<'r> Chunker<'r> {
    fn new(
        rows: &'r [(u32, u32)],
        max_rows: usize,
        budget: usize,
        base: usize,
        per_row: usize,
    ) -> Self {
        Self {
            rows: rows.iter(),
            carry: None,
            max_rows,
            budget,
            base,
            per_row,
        }
    }

    /// The next statement's parameters, `columns` per row, and its row count, or
    /// `None` once every row is out. `fill` appends one row's values.
    fn next_chunk(
        &mut self,
        columns: usize,
        mut fill: impl FnMut(u32, u32, &mut Vec<Value>) -> Result<(), Failure>,
    ) -> Result<Option<(Vec<Value>, usize)>, Failure> {
        let mut bytes = self.base;
        let (mut params, mut rows) = match self.carry.take() {
            Some(carried) => {
                bytes += self.per_row + carried.iter().map(value_bytes).sum::<usize>();
                (carried, 1)
            }
            None => (
                Vec::with_capacity(self.max_rows.min(self.rows.len()) * columns),
                0,
            ),
        };
        while rows < self.max_rows {
            let Some(&(b, r)) = self.rows.next() else {
                break;
            };
            let start = params.len();
            fill(b, r, &mut params)?;
            let row_bytes = self.per_row + params[start..].iter().map(value_bytes).sum::<usize>();
            if rows > 0 && bytes + row_bytes > self.budget {
                self.carry = Some(params.split_off(start));
                break;
            }
            rows += 1;
            bytes += row_bytes;
        }
        Ok((rows > 0).then_some((params, rows)))
    }
}

/// The two statement handles a flush may keep open: one full-size `INSERT` and
/// one full-size `DELETE`.
#[derive(Default)]
struct Handles {
    insert: Option<Statement>,
    delete: Option<Statement>,
}

impl Handles {
    /// Close every open handle. A failure is ignored: a dead connection frees
    /// them itself.
    async fn close(&mut self, tx: &mut Transaction<'_>) {
        for stmt in [self.insert.take(), self.delete.take()]
            .into_iter()
            .flatten()
        {
            let _ = tx.close(stmt).await;
        }
    }
}

/// Run `sql` with `params`. A full chunk reuses `slot`'s handle, preparing it on
/// first use; a partial chunk prepares, runs and closes a handle of its own.
async fn run_statement(
    tx: &mut Transaction<'_>,
    slot: &mut Option<Statement>,
    full: bool,
    sql: String,
    params: Vec<Value>,
) -> Result<(), mysql_async::Error> {
    if full {
        let stmt = match slot {
            Some(stmt) => stmt,
            None => slot.insert(tx.prep(sql).await?),
        };
        return tx.exec_drop(&*stmt, params).await;
    }
    let stmt = tx.prep(sql).await?;
    let result = tx.exec_drop(&stmt, params).await;
    let _ = tx.close(stmt).await;
    result
}

/// Everything a flush's statements need, borrowed from the sink.
struct Apply<'a> {
    prepared: &'a Prepared,
    written: &'a [&'a FieldSpec],
    key_fields: &'a [&'a FieldSpec],
    key_columns: &'a [String],
    table: &'a TableRef,
}

impl Apply<'_> {
    async fn run(
        &self,
        tx: &mut Transaction<'_>,
        handles: &mut Handles,
        batches: &[RecordBatch],
        runs: &[Run],
        truncate: bool,
    ) -> Result<(), Failure> {
        let table = self.table;
        if truncate {
            tx.query_drop(format!("DELETE FROM {}", self.prepared.table))
                .await
                .map_err(|e| {
                    Failure::server(
                        "write",
                        format!("cannot clear '{table}' for truncate_before_first_write"),
                        &e,
                    )
                })?;
        }

        let mut readers: Vec<Vec<ColumnReader<'_>>> = Vec::with_capacity(batches.len());
        let mut key_readers: Vec<Vec<ColumnReader<'_>>> = Vec::with_capacity(batches.len());
        for batch in batches {
            readers.push(resolve_columns(WHAT, batch, self.written).map_err(Failure::encode)?);
            key_readers
                .push(resolve_columns(WHAT, batch, self.key_fields).map_err(Failure::encode)?);
        }

        let p = self.prepared;
        for run in runs {
            match run.kind {
                RunKind::Upsert => {
                    let columns = self.written.len();
                    let mut chunks = Chunker::new(
                        &run.rows,
                        p.rows_per_insert,
                        p.packet_budget,
                        p.insert_prefix.len() + p.tail.len(),
                        4 * columns + columns / 8 + 4,
                    );
                    while let Some((params, rows)) = chunks.next_chunk(columns, |b, r, out| {
                        let start = out.len();
                        row_values(&readers[b as usize], r as usize, out)
                            .map_err(Failure::encode)?;
                        self.guard_fractions(&out[start..], r as usize)
                    })? {
                        let sql = insert_sql(&p.insert_prefix, columns, rows, &p.tail);
                        run_statement(
                            tx,
                            &mut handles.insert,
                            rows == p.rows_per_insert,
                            sql,
                            params,
                        )
                        .await
                        .map_err(|e| {
                            Failure::server("write", format!("INSERT into '{table}' failed"), &e)
                        })?;
                    }
                }
                RunKind::Delete => {
                    let columns = self.key_columns.len();
                    let mut chunks = Chunker::new(
                        &run.rows,
                        p.keys_per_delete,
                        p.packet_budget,
                        p.table.len(),
                        self.key_columns.iter().map(|c| c.len() + 16).sum::<usize>() + 4,
                    );
                    while let Some((params, keys)) = chunks.next_chunk(columns, |b, r, out| {
                        row_values(&key_readers[b as usize], r as usize, out)
                            .map_err(Failure::encode)
                    })? {
                        let sql = delete_sql(&p.table, self.key_columns, keys);
                        run_statement(
                            tx,
                            &mut handles.delete,
                            keys == p.keys_per_delete,
                            sql,
                            params,
                        )
                        .await
                        .map_err(|e| {
                            Failure::server("write", format!("DELETE from '{table}' failed"), &e)
                        })?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Refuse fraction digits a narrower server column would round or truncate.
    fn guard_fractions(&self, row_values: &[Value], row: usize) -> Result<(), Failure> {
        for &(index, modulus) in &self.prepared.fraction_guards {
            let micros = match row_values.get(index) {
                Some(Value::Date(.., us)) | Some(Value::Time(.., us)) => *us,
                _ => continue,
            };
            if micros % modulus != 0 {
                let digits = 6 - modulus.ilog10();
                return Err(Failure::encode(SaciError::generic(format!(
                    "{WHAT}: column '{}' row {row}: the value carries fraction digits beyond the \
                     {digits} the server column keeps; the server would round (MySQL) or \
                     truncate (MariaDB) it silently",
                    self.written[index].name
                ))));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for MariadbSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.recover();
        self.check_schema(batch)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let rows = batch.num_rows();
        self.buffered_rows += rows;
        self.buffer.push(batch.clone());
        self.instruments.pending(self.buffered_rows as u64);

        if self.flush_rows == 0 || self.buffered_rows >= self.flush_rows {
            // Dropped inside the flush, this call leaves `own_rows` set, and the
            // next call takes the batch back out (`recover`).
            self.own_rows = rows;
            let flushed = self.flush().await;
            self.own_rows = 0;
            if let Err(e) = flushed {
                // The caller is told this batch failed and re-drives it (the
                // service's retry wrapper does), so it is not kept: holding it
                // too would write its rows twice. Rows an earlier call accepted
                // stay buffered for the next write or `finish`.
                self.buffer.pop();
                self.buffered_rows -= rows;
                self.instruments.pending(self.buffered_rows as u64);
                return Err(e);
            }
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        self.recover();
        self.flush().await
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn pending_rows(&self) -> Option<usize> {
        Some(self.buffered_rows)
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::ArrayRef;
    use saci_connector::from_kdl_str;
    use serde::Deserialize as _;

    use super::*;

    const SINK: &str = r#"
name "orders_out"
table "shop.enriched"
write_mode "upsert"
conflict_columns "id"

connection dsn="mysql://user:pw@db:3306/shop" ssl_mode="disabled"

schema_fields "id" type="int64" nullable=#false
schema_fields "total" type="float64"
"#;

    const CHANGES: &str = r#"
name "orders_out"
table "shop.enriched"
write_mode "apply_changes"
conflict_columns "id"

connection dsn="mysql://user:pw@db:3306/shop" ssl_mode="disabled"

schema_fields "__op" type="utf8" nullable=#false
schema_fields "id" type="int64" nullable=#false
schema_fields "total" type="float64"
"#;

    const DEDUPE: &str = r#"
name "orders_out"
table "shop.enriched"
write_mode "upsert"
conflict_columns "id"
dedupe_order_column "seq"

connection dsn="mysql://user:pw@db:3306/shop" ssl_mode="disabled"

schema_fields "id" type="int64" nullable=#false
schema_fields "seq" type="int64" nullable=#true
schema_fields "total" type="float64"
"#;

    fn config(kdl: &str) -> MariadbSinkConfig {
        MariadbSinkConfig::deserialize(from_kdl_str(kdl).expect("parse kdl")).expect("parse")
    }

    fn sink(kdl: &str) -> MariadbSink {
        MariadbSink::new(config(kdl)).expect("sink builds")
    }

    fn batch(sink: &MariadbSink, columns: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(sink.schema(), columns).expect("batch")
    }

    fn changes(ops: &[Option<&str>], ids: &[i64]) -> RecordBatch {
        let sink = sink(&CHANGES.replace("nullable=#false", "nullable=#true"));
        batch(
            &sink,
            vec![
                Arc::new(StringArray::from(ops.to_vec())),
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(Float64Array::from(vec![1.0; ids.len()])),
            ],
        )
    }

    fn positions(runs: &[Run]) -> Vec<(RunKind, Vec<(u32, u32)>)> {
        runs.iter().map(|r| (r.kind, r.rows.clone())).collect()
    }

    #[test]
    fn the_schema_follows_the_declared_fields_in_order() {
        let sink = sink(SINK);
        let schema = sink.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["id", "total"]);
        assert!(!schema.field(0).is_nullable());
    }

    #[test]
    fn building_a_sink_opens_no_connection() {
        // Nothing listens on port 1; a sink that dialled at build time would
        // fail here or hang.
        let text = SINK.replace("db:3306", "127.0.0.1:1");
        MariadbSink::new(config(&text)).expect("build touches no socket");
    }

    #[test]
    fn a_new_sink_holds_nothing_back() {
        assert_eq!(sink(SINK).pending_rows(), Some(0));
    }

    #[test]
    fn the_resolved_settings_reach_the_sink() {
        let sink = sink(SINK);
        assert_eq!(sink.table.to_string(), "shop.enriched");
        assert_eq!(sink.write_mode, WriteMode::Upsert);
        assert_eq!(sink.conflict_columns, vec!["id".to_string()]);
        assert_eq!(sink.update_columns, vec!["total".to_string()]);
        assert_eq!(sink.written.len(), 2);
        assert_eq!(sink.chunk_rows, 1000);
    }

    #[test]
    fn apply_changes_writes_no_reserved_column() {
        let sink = sink(CHANGES);
        assert_eq!(sink.schema().fields().len(), 3);
        let written: Vec<&str> = sink.written.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(written, vec!["id", "total"]);
    }

    #[test]
    fn a_config_that_fails_validation_never_builds_a_sink() {
        let err = MariadbSink::new(config(&SINK.replace("conflict_columns \"id\"\n", "")))
            .err()
            .expect("upsert needs conflict columns");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("conflict_columns"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn a_bare_table_with_no_database_in_the_dsn_is_refused_at_build() {
        let text = SINK
            .replace("db:3306/shop", "db:3306")
            .replace("shop.enriched", "enriched");
        let err = MariadbSink::new(config(&text))
            .err()
            .expect("a bare table needs the DSN's database");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message()
                .contains("connection.dsn names no database, so table 'enriched'"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn a_batch_with_the_wrong_columns_is_refused_before_it_is_buffered() {
        let sink = sink(SINK);
        let other = RecordBatch::try_new(
            Arc::new(Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .expect("batch");
        let err = sink.check_schema(&other).expect_err("one column short");
        assert!(
            err.message().contains("has 1 column(s) but"),
            "{}",
            err.message()
        );
        assert_eq!(sink.pending_rows(), Some(0));
    }

    #[tokio::test]
    async fn a_write_dropped_inside_its_flush_is_taken_back_by_the_next_call() {
        // Nothing listens on port 1, so the flush parks in connect and backoff
        // and the timeout drops it there.
        let mut sink = sink(&SINK.replace("db:3306", "127.0.0.1:1"));
        let accepted = batch(
            &sink,
            vec![
                Arc::new(Int64Array::from(vec![1_i64])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        );
        // Buffered under a threshold, so no flush and no connection.
        sink.flush_rows = 2;
        sink.write_batch(&accepted).await.expect("buffered");

        let own = batch(
            &sink,
            vec![
                Arc::new(Int64Array::from(vec![2_i64])),
                Arc::new(Float64Array::from(vec![2.0])),
            ],
        );
        let dropped =
            tokio::time::timeout(std::time::Duration::from_millis(1), sink.write_batch(&own)).await;
        assert!(dropped.is_err(), "the flush was still connecting");
        assert!(sink.in_flight, "the dropped flush leaves its marker");
        assert_eq!(sink.own_rows, 1);
        assert_eq!(sink.buffer.len(), 2, "both batches are still buffered");

        sink.recover();
        assert!(!sink.in_flight);
        assert_eq!(sink.own_rows, 0);
        assert_eq!(sink.buffer.len(), 1, "the dropped call's batch is gone");
        assert_eq!(sink.pending_rows(), Some(1), "the accepted row stays");
        assert!(sink.session.is_none() && sink.prepared.is_none());
    }

    #[tokio::test]
    async fn a_finish_dropped_inside_its_flush_keeps_every_row() {
        let mut sink = sink(&SINK.replace("db:3306", "127.0.0.1:1"));
        sink.flush_rows = 10;
        let rows = batch(
            &sink,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
        );
        sink.write_batch(&rows).await.expect("buffered");
        let dropped =
            tokio::time::timeout(std::time::Duration::from_millis(1), sink.finish()).await;
        assert!(dropped.is_err(), "the flush was still connecting");
        sink.recover();
        assert!(!sink.in_flight);
        assert_eq!(
            sink.pending_rows(),
            Some(2),
            "a dropped finish keeps its rows"
        );
        assert_eq!(sink.buffer.len(), 1);
    }

    #[test]
    fn recovering_a_sink_that_nothing_interrupted_changes_nothing() {
        let mut sink = sink(SINK);
        sink.recover();
        assert!(!sink.in_flight);
        assert_eq!(sink.pending_rows(), Some(0));
    }

    #[test]
    fn inserts_are_chunked_under_the_placeholder_limit() {
        assert_eq!(rows_per_insert(1000, 5), 1000);
        assert_eq!(rows_per_insert(100_000, 5), 13_107);
        assert_eq!(13_107 * 5, 65_535);
        assert_eq!(rows_per_insert(1000, 70_000), 1);
        assert_eq!(rows_per_insert(1000, 0), 1000);
    }

    #[test]
    fn deletes_carry_at_most_a_thousand_keys() {
        assert_eq!(keys_per_delete(13_107, 1), 1000);
        assert_eq!(keys_per_delete(500, 1), 500);
        assert_eq!(keys_per_delete(13_107, 2), 1000);
        assert_eq!(keys_per_delete(100_000, 100_000), 1);
    }

    #[test]
    fn an_insert_statement_lists_one_placeholder_group_per_row() {
        assert_eq!(
            insert_sql("INSERT INTO `d`.`t` (`a`, `b`) VALUES ", 2, 3, ""),
            "INSERT INTO `d`.`t` (`a`, `b`) VALUES (?,?),(?,?),(?,?)"
        );
    }

    #[test]
    fn the_tail_follows_the_write_mode_and_the_flavour() {
        let update = ["total"];
        assert_eq!(
            tail_for(WriteMode::Append, Flavor::Mysql, &update, "id"),
            ""
        );
        assert_eq!(
            tail_for(WriteMode::Upsert, Flavor::Mariadb, &update, "id"),
            " ON DUPLICATE KEY UPDATE `total` = VALUES(`total`)"
        );
        assert_eq!(
            tail_for(WriteMode::Upsert, Flavor::Mysql, &update, "id"),
            " AS __saci_new ON DUPLICATE KEY UPDATE `total` = __saci_new.`total`"
        );
        assert_eq!(
            tail_for(WriteMode::ApplyChanges, Flavor::Mariadb, &update, "id"),
            " ON DUPLICATE KEY UPDATE `total` = VALUES(`total`)"
        );
        for flavor in [Flavor::Mariadb, Flavor::Mysql] {
            assert_eq!(
                tail_for(WriteMode::IgnoreConflicts, flavor, &update, "id"),
                " ON DUPLICATE KEY UPDATE `id` = `id`"
            );
            // A key-only table has nothing to update: it resolves like ignore.
            assert_eq!(
                tail_for(WriteMode::Upsert, flavor, &[], "id"),
                " ON DUPLICATE KEY UPDATE `id` = `id`"
            );
        }
    }

    #[test]
    fn a_single_key_delete_uses_in() {
        assert_eq!(
            delete_sql("`d`.`t`", &["id".to_string()], 3),
            "DELETE FROM `d`.`t` WHERE `id` IN (?,?,?)"
        );
    }

    #[test]
    fn a_composite_key_delete_ors_the_key_pairs() {
        assert_eq!(
            delete_sql("`d`.`t`", &["a".to_string(), "b`c".to_string()], 2),
            "DELETE FROM `d`.`t` WHERE (`a` = ? AND `b``c` = ?) OR (`a` = ? AND `b``c` = ?)"
        );
    }

    #[test]
    fn the_conflict_key_matches_a_unique_index_as_a_set_ignoring_case() {
        let keys = vec![
            vec!["id".to_string()],
            vec!["Tenant".to_string(), "code".to_string()],
        ];
        let want = |cols: &[&str]| cols.iter().map(|c| c.to_string()).collect::<Vec<_>>();
        assert!(key_matches(&want(&["ID"]), &keys));
        assert!(key_matches(&want(&["code", "tenant"]), &keys));
        assert!(!key_matches(&want(&["tenant"]), &keys));
        assert!(!key_matches(&want(&["id", "code"]), &keys));
        assert!(!key_matches(&want(&["id"]), &[]));
    }

    #[test]
    fn append_upsert_and_ignore_plan_one_run_in_batch_order() {
        let sink = sink(SINK);
        let b = batch(
            &sink,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(Float64Array::from(vec![1.0, 2.0])),
            ],
        );
        let runs = sink.plan_runs(&[b.clone(), b]).expect("plans");
        assert_eq!(
            positions(&runs),
            vec![(RunKind::Upsert, vec![(0, 0), (0, 1), (1, 0), (1, 1)])]
        );
    }

    #[test]
    fn apply_changes_splits_runs_at_each_change_of_action() {
        let sink = sink(CHANGES);
        let b = changes(
            &[
                Some("I"),
                Some("U"),
                Some("D"),
                Some("D"),
                Some("R"),
                Some("D"),
            ],
            &[1, 2, 3, 4, 5, 6],
        );
        let runs = sink.plan_runs(&[b]).expect("plans");
        assert_eq!(
            positions(&runs),
            vec![
                (RunKind::Upsert, vec![(0, 0), (0, 1)]),
                (RunKind::Delete, vec![(0, 2), (0, 3)]),
                (RunKind::Upsert, vec![(0, 4)]),
                (RunKind::Delete, vec![(0, 5)]),
            ]
        );
    }

    #[test]
    fn an_unknown_op_names_its_row_and_the_ops_it_knows() {
        let sink = sink(CHANGES);
        let failure = sink
            .plan_runs(&[changes(&[Some("I"), Some("X")], &[1, 2])])
            .expect_err("X is no op");
        assert_eq!(failure.kind, "encode");
        assert_eq!(
            failure.error.message(),
            "MariadbSink: row 1 has __op 'X'; apply_changes understands I, U, R and D"
        );
        let failure = sink
            .plan_runs(&[changes(&[None], &[1])])
            .expect_err("NULL is no op");
        assert!(
            failure.error.message().contains("row 0 has __op NULL;"),
            "{}",
            failure.error.message()
        );
    }

    #[test]
    fn a_delete_with_a_null_key_is_refused() {
        let sink = sink(&CHANGES.replace("nullable=#false", "nullable=#true"));
        let b = batch(
            &sink,
            vec![
                Arc::new(StringArray::from(vec!["D"])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        );
        let failure = sink.plan_runs(&[b]).expect_err("NULL key");
        assert!(
            failure.error.message().contains("NULL conflict key"),
            "{}",
            failure.error.message()
        );
    }

    #[test]
    fn dedupe_keeps_the_greatest_order_and_the_later_row_on_a_tie() {
        let sink = sink(DEDUPE);
        let one = batch(
            &sink,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 1, 2, 2])),
                Arc::new(Int64Array::from(vec![5_i64, 9, 4, 4])),
                Arc::new(Float64Array::from(vec![0.0; 4])),
            ],
        );
        let two = batch(
            &sink,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 3])),
                Arc::new(Int64Array::from(vec![7_i64, 1])),
                Arc::new(Float64Array::from(vec![0.0; 2])),
            ],
        );
        let runs = sink.plan_runs(&[one, two]).expect("plans");
        // id 1: orders 5, 9, 7 keep (0,1); id 2: tie keeps the later (0,3);
        // id 3 is alone.
        assert_eq!(
            positions(&runs),
            vec![(RunKind::Upsert, vec![(0, 1), (0, 3), (1, 1)])]
        );
    }

    #[test]
    fn dedupe_never_collapses_rows_with_a_null_key_and_ranks_null_order_lowest() {
        let sink = sink(DEDUPE.replace("nullable=#false", "nullable=#true").as_str());
        let b = batch(
            &sink,
            vec![
                Arc::new(Int64Array::from(vec![None, None, Some(1), Some(1)])),
                Arc::new(Int64Array::from(vec![Some(1_i64), Some(2), None, Some(0)])),
                Arc::new(Float64Array::from(vec![0.0; 4])),
            ],
        );
        let runs = sink.plan_runs(&[b]).expect("plans");
        assert_eq!(
            positions(&runs),
            vec![(RunKind::Upsert, vec![(0, 0), (0, 1), (0, 3)])]
        );
    }

    #[test]
    fn order_values_compare_by_type() {
        let decimals = Decimal128Array::from(vec![Some(95_i128), Some(1000)])
            .with_precision_and_scale(10, 1)
            .expect("decimal");
        assert_eq!(
            order_value(&decimals, 0).compare(&order_value(&decimals, 1)),
            Ordering::Less
        );
        let floats = Float64Array::from(vec![Some(f64::NAN), Some(1.0), None]);
        assert_eq!(
            order_value(&floats, 0).compare(&order_value(&floats, 1)),
            Ordering::Greater
        );
        assert_eq!(
            order_value(&floats, 2).compare(&order_value(&floats, 1)),
            Ordering::Less
        );
        let text = StringArray::from(vec!["b", "a"]);
        assert_eq!(
            order_value(&text, 0).compare(&order_value(&text, 1)),
            Ordering::Greater
        );
    }

    #[test]
    fn key_encoding_separates_values_and_skips_null_parts() {
        let mut a = Vec::new();
        let mut b = Vec::new();
        assert!(encode_key(
            &[Value::Int(1), Value::Bytes(b"ab".to_vec())],
            &mut a
        ));
        assert!(encode_key(
            &[Value::Int(1), Value::Bytes(b"a".to_vec())],
            &mut b
        ));
        assert_ne!(a, b);
        a.clear();
        b.clear();
        assert!(encode_key(&[Value::Double(0.0)], &mut a));
        assert!(encode_key(&[Value::Double(-0.0)], &mut b));
        assert_eq!(a, b);
        assert!(!encode_key(&[Value::Int(1), Value::NULL], &mut Vec::new()));
    }

    #[test]
    fn fraction_guards_cover_temporal_columns_narrower_than_six_digits() {
        let spec = |ty: &str| {
            let text = format!(
                "name \"s\"\ntable \"d.t\"\nconnection dsn=\"mysql://u@h/d\" ssl_mode=\"disabled\"\n\
                 schema_fields \"c\" type=\"{ty}\"\n"
            );
            config(&text).schema_fields.remove(0)
        };
        let mut narrow = ResolvedColumn::synthetic(&spec("timestamp_micros"));
        narrow.family = Family::DateTime;
        narrow.fractional = 3;
        let mut full = narrow.clone();
        full.fractional = 6;
        let mut plain = ResolvedColumn::synthetic(&spec("int64"));
        plain.family = Family::Integer(64);
        plain.fractional = 0;
        assert_eq!(fraction_guards(&[plain, narrow, full]), vec![(1, 1000)]);
    }

    #[test]
    fn a_value_with_a_finer_fraction_than_the_column_is_refused() {
        let spec = config(SINK).schema_fields.remove(0);
        let written = [&spec];
        let prepared = Prepared {
            table: String::new(),
            insert_prefix: String::new(),
            tail: String::new(),
            rows_per_insert: 1,
            keys_per_delete: 1,
            packet_budget: usize::MAX,
            fraction_guards: vec![(0, 1000)],
        };
        let table = TableRef {
            schema: "s".into(),
            name: "t".into(),
        };
        let ctx = Apply {
            prepared: &prepared,
            written: &written,
            key_fields: &[],
            key_columns: &[],
            table: &table,
        };
        ctx.guard_fractions(&[Value::Date(2024, 1, 1, 0, 0, 0, 123_000)], 0)
            .expect("milliseconds fit a 3 digit column");
        ctx.guard_fractions(&[Value::Date(2024, 1, 1, 0, 0, 0, 0)], 0)
            .expect("whole seconds fit");
        let failure = ctx
            .guard_fractions(&[Value::Time(false, 0, 1, 2, 3, 123_456)], 4)
            .expect_err("microseconds do not");
        assert_eq!(failure.kind, "encode");
        assert!(
            failure.error.message().starts_with(
                "MariadbSink: column 'id' row 4: the value carries fraction digits beyond the 3 "
            ),
            "{}",
            failure.error.message()
        );
    }

    fn chunks(
        rows: &[(u32, u32)],
        max_rows: usize,
        budget: usize,
        value: impl Fn(u32) -> Value,
    ) -> Vec<usize> {
        let mut chunker = Chunker::new(rows, max_rows, budget, 0, 0);
        let mut sizes = Vec::new();
        while let Some((params, n)) = chunker
            .next_chunk(1, |_, r, out| {
                out.push(value(r));
                Ok(())
            })
            .expect("fill never fails")
        {
            assert_eq!(params.len(), n, "one parameter per row here");
            sizes.push(n);
        }
        sizes
    }

    #[test]
    fn a_chunk_stops_at_the_row_limit() {
        let rows: Vec<(u32, u32)> = (0..7).map(|r| (0, r)).collect();
        assert_eq!(chunks(&rows, 3, usize::MAX, |_| Value::Int(1)), [3, 3, 1]);
    }

    #[test]
    fn a_chunk_stops_before_the_row_that_would_pass_the_byte_budget() {
        // Each row counts 100 + 9 bytes: three fit in 400, the fourth starts the
        // next statement with the row already read, not read twice.
        let rows: Vec<(u32, u32)> = (0..7).map(|r| (0, r)).collect();
        let reads = std::cell::Cell::new(0);
        let mut chunker = Chunker::new(&rows, 1000, 400, 0, 0);
        let mut sizes = Vec::new();
        while let Some((_, n)) = chunker
            .next_chunk(1, |_, _, out| {
                reads.set(reads.get() + 1);
                out.push(Value::Bytes(vec![0; 100]));
                Ok(())
            })
            .expect("fill never fails")
        {
            sizes.push(n);
        }
        assert_eq!(sizes, [3, 3, 1]);
        assert_eq!(reads.get(), 7, "a carried row is not read again");
    }

    #[test]
    fn a_row_over_the_budget_on_its_own_goes_out_alone() {
        let rows: Vec<(u32, u32)> = (0..3).map(|r| (0, r)).collect();
        let sizes = chunks(&rows, 1000, 50, |r| {
            Value::Bytes(vec![0; if r == 1 { 500 } else { 5 }])
        });
        assert_eq!(sizes, [1, 1, 1]);
    }

    #[test]
    fn no_rows_make_no_chunk() {
        assert!(chunks(&[], 10, 100, |_| Value::NULL).is_empty());
    }

    #[test]
    fn a_fill_failure_reaches_the_caller() {
        let rows = [(0_u32, 0_u32)];
        let mut chunker = Chunker::new(&rows, 10, 100, 0, 0);
        let failure = chunker
            .next_chunk(1, |_, _, _| {
                Err(Failure::encode(SaciError::generic("refused")))
            })
            .expect_err("the row is refused");
        assert_eq!(failure.kind, "encode");
    }
}
