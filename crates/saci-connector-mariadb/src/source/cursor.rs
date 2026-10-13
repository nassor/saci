//! The `polling` and `cdc_trigger` reader.
//!
//! One [`CursorReader`] serves `polling`, a `cdc_trigger` over an operator
//! outbox (`install_triggers #false`), and a `cdc_trigger` over the changelog
//! the connector installs (`install_triggers #true`, see [`super::triggers`]).
//! They differ in the table read, the cursor column, and what happens to
//! acknowledged rows.
//!
//! # Query shapes
//!
//! Three statements, run over the binary protocol with the offset bound:
//!
//! ```text
//! -- no committed offset yet
//! SELECT <cols>, CAST(<cur> AS CHAR) FROM <t> [WHERE (<where>)]
//!     ORDER BY <cur> LIMIT <n>
//! -- resumed, no tiebreak column
//! ... WHERE <cur> > ? [AND (<where>)] ...
//! -- resumed, with a tiebreak column
//! ... WHERE (<cur> > ? OR (<cur> = ? AND <tie> > ?)) [AND (<where>)]
//!     ORDER BY <cur>, <tie> ...
//! ```
//!
//! The tiebreak comparison is written out rather than as the row value
//! `(cur, tie) > (?, ?)`: neither server range-scans a row value comparison, so
//! every batch would read the index from its start. The expanded form is a
//! range on `(cur, tie)`. `delete_acked` takes the same expansion.
//!
//! # Bound values
//!
//! The offset is stored as the text `CAST(<cur> AS CHAR)` returned. An integer
//! cursor binds as a typed integer (`Value::Int` or `Value::UInt`) with no
//! `CAST`: MySQL types `CAST(? AS UNSIGNED)` over a text parameter as signed,
//! so an unsigned cursor above 2^63 would re-read the table from its start and
//! `delete_acked` would fail. A date and a timestamp bind as text through
//! `CAST(? AS DATE)` and `CAST(? AS DATETIME(6))`, which both servers compare
//! exactly, and text binds as it is.
//!
//! # Cycle boundary
//!
//! `next_batch` returning `Ok(None)` ends a drain cycle, not the source. The
//! offset is committed at the *start* of the next cycle, which is what makes
//! delivery at-least-once: a crash mid-cycle replays the cycle.
//!
//! The in-memory offset outlives a reconnect: only session state (the
//! statement cache, the resolved columns, the offset table check and the
//! installed triggers) is rebuilt, so a lost connection neither re-reads a
//! delivered batch nor re-resolves `initial "now"`.
//!
//! # Commit order
//!
//! A cursor the server numbers with `AUTO_INCREMENT` (an operator's outbox, the
//! installed changelog's `__seq`, a polled table's id) is allocated when a row
//! is written, not when its transaction commits. [`super::sequence`] holds back
//! the rows behind a gap in those numbers until a late commit fills it or the
//! gap is passed (`gap_timeout_ms`, or `INNODB_TRX` when the user can read it),
//! so the cursor never moves past a row that may still commit.
//!
//! With a `where_clause` the rows read hide the numbers the filter drops, so the
//! holes are found on the unfiltered numbers instead: a first query reads the
//! cursor values of the next window, and the filtered query is bounded by the
//! last number before the first open hole. A window the filter empties moves
//! the offset to its end.
//!
//! Acknowledged rows are pruned by a `DELETE` limited to the count of rows at
//! or before the offset, never by the bare range. A range delete reads one
//! record past its range to find the end, and that record can be an open
//! transaction's uncommitted insert, which InnoDB makes the delete wait for.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use mysql_async::prelude::Queryable;
use mysql_async::{Params, Row, Value};
use saci_core::error::SaciError;

use crate::config::{
    FieldSpec, MariadbFieldType, MariadbSourceConfig, Retention, Role, SourceMode, find_field,
};
use crate::connection::{
    Connector, Session, TableRef, columns_of, exec_closing, mysql_detail, quote_ident, server_code,
};
use crate::metrics::Instruments;
use crate::offsets::{Offset, OffsetStore};
use crate::source::sequence::{self, Holes, Passed, Release};
use crate::source::triggers::{self, Installed, TriggerSpec};
use crate::types::{Access, ResolvedColumn, validate_columns};
use crate::values::{Cell, ColumnBuilder};

const WHAT: &str = "MariadbSource";

/// Alias for the cursor text column, which cannot collide with a declared field
/// because `__`-prefixed names are refused outside the change-stream modes.
const CURSOR_ALIAS: &str = "__saci_cursor";
/// Alias for the tiebreak text column.
const TIEBREAK_ALIAS: &str = "__saci_tiebreak";

/// The most reads one call makes to settle holes it has just found: a read, the
/// check of the running transactions, and a read again when a hole is already
/// settled.
const MAX_PASSES: usize = 3;

/// How long a prune waits on a lock another transaction holds, in seconds. An
/// open writer's uncommitted record below the offset (a hole passed while its
/// transaction was still running) is such a lock.
const PRUNE_LOCK_WAIT_SECS: u32 = 2;

/// How long a prune that hit a lock wait is left alone before the next try.
const PRUNE_RETRY: Duration = Duration::from_secs(5);

/// `ER_LOCK_WAIT_TIMEOUT` and `ER_LOCK_DEADLOCK`: the statement lost a lock to
/// another transaction, and can be run again later.
const LOCK_LOST: [u16; 2] = [1205, 1213];

/// How long to wait before reading `INNODB_TRX` again when the list does not yet
/// show the reader's own transaction. The server rebuilds the table only after
/// 0.1 seconds without a read, so this is long enough for the next read to
/// rebuild it, unless something else keeps reading it.
const TRX_RETRY: Duration = Duration::from_millis(150);

/// How many times a current transaction list is asked for before the holes are
/// left to the timeout.
const TRX_TRIES: usize = 3;

/// How an offset's text binds against a column of the declared type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Bind {
    Signed,
    Unsigned,
    Date,
    DateTime,
    Text,
}

impl Bind {
    fn of(ty: MariadbFieldType) -> Self {
        use MariadbFieldType as T;
        match ty {
            T::Int8 | T::Int16 | T::Int32 | T::Int64 => Bind::Signed,
            T::UInt8 | T::UInt16 | T::UInt32 | T::UInt64 => Bind::Unsigned,
            T::Date32 => Bind::Date,
            T::TimestampMicros | T::TimestampMicrosUtc => Bind::DateTime,
            _ => Bind::Text,
        }
    }

    /// Whether the cursor is an integer, the only kind a server numbers.
    fn is_integer(self) -> bool {
        matches!(self, Bind::Signed | Bind::Unsigned)
    }

    /// The placeholder as it appears in a predicate.
    fn placeholder(self) -> &'static str {
        match self {
            Bind::Date => "CAST(? AS DATE)",
            Bind::DateTime => "CAST(? AS DATETIME(6))",
            Bind::Signed | Bind::Unsigned | Bind::Text => "?",
        }
    }

    /// What the text must be, for an error message.
    fn describe(self) -> &'static str {
        match self {
            Bind::Signed => "a signed 64-bit integer",
            Bind::Unsigned => "an unsigned 64-bit integer",
            Bind::Date => "a date, YYYY-MM-DD",
            Bind::DateTime => "a timestamp, YYYY-MM-DD HH:MM:SS[.ffffff]",
            Bind::Text => "text",
        }
    }

    /// The parameter for `text`, or why it is not a value of this kind.
    fn value(self, text: &str) -> Result<Value, &'static str> {
        let bad = || self.describe();
        match self {
            Bind::Signed => text.parse::<i64>().map(Value::Int).map_err(|_| bad()),
            Bind::Unsigned => text.parse::<u64>().map(Value::UInt).map_err(|_| bad()),
            Bind::Date if is_date(text) => Ok(Value::Bytes(text.as_bytes().to_vec())),
            Bind::DateTime if is_datetime(text) => Ok(Value::Bytes(text.as_bytes().to_vec())),
            Bind::Text => Ok(Value::Bytes(text.as_bytes().to_vec())),
            Bind::Date | Bind::DateTime => Err(bad()),
        }
    }
}

/// `YYYY-MM-DD`, digits only.
fn is_date(text: &str) -> bool {
    let b = text.as_bytes();
    b.len() == 10
        && b.iter().enumerate().all(|(i, c)| {
            if i == 4 || i == 7 {
                *c == b'-'
            } else {
                c.is_ascii_digit()
            }
        })
}

/// `YYYY-MM-DD HH:MM:SS`, optionally `.` and one to six digits.
fn is_datetime(text: &str) -> bool {
    let Some((date, time)) = text.split_once([' ', 'T']) else {
        return false;
    };
    let (clock, fraction) = match time.split_once('.') {
        Some((clock, fraction)) => (clock, Some(fraction)),
        None => (time, None),
    };
    let cb = clock.as_bytes();
    is_date(date)
        && cb.len() == 8
        && cb.iter().enumerate().all(|(i, c)| {
            if i == 2 || i == 5 {
                *c == b':'
            } else {
                c.is_ascii_digit()
            }
        })
        && fraction
            .is_none_or(|f| (1..=6).contains(&f.len()) && f.bytes().all(|c| c.is_ascii_digit()))
}

/// A cursor column and everything derived from it.
#[derive(Debug, Clone)]
struct CursorColumn {
    quoted: String,
    name: String,
    bind: Bind,
}

impl CursorColumn {
    fn resolve(fields: &[FieldSpec], name: &str) -> Result<Self, SaciError> {
        let field = find_field(fields, name).ok_or_else(|| {
            SaciError::configuration(format!(
                "{WHAT}: '{name}' does not name a declared schema_fields entry"
            ))
        })?;
        Ok(Self {
            quoted: quote_ident(name),
            name: name.to_string(),
            bind: Bind::of(field.ty),
        })
    }
}

/// Which cursor predicate a statement carries. `pub(crate)` so `source.rs`'s
/// dispatch test can name [`Shape::All`] when calling
/// [`CursorReader::select_sql`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shape {
    /// No predicate: the source has no committed offset.
    All,
    /// `cursor > ?`.
    Cursor,
    /// `cursor > ? OR (cursor = ? AND tiebreak > ?)`.
    Pair,
}

/// A declared column as the reader fills it.
struct ReadColumn {
    resolved: ResolvedColumn,
    /// Whether the column is in the `SELECT` list. `__table` of an installed
    /// changelog is the one that is not: the reader writes the watched table's
    /// name itself.
    from_sql: bool,
}

/// Incremental reader over a table ordered by a cursor column.
pub(crate) struct CursorReader {
    connector: Connector,
    session: Option<Session>,
    offsets: OffsetStore,
    schema: Arc<Schema>,

    /// The table the `SELECT` reads: the table itself, or the changelog.
    table: TableRef,
    offset_table: String,
    fields: Vec<FieldSpec>,
    cursor: CursorColumn,
    tiebreak: Option<CursorColumn>,
    where_clause: Option<String>,
    initial: String,
    retention: Retention,
    batch_rows: usize,
    max_batches_per_cycle: usize,
    /// How long a hole in an `AUTO_INCREMENT` cursor holds the rows after it
    /// back when no transaction list is readable. Zero reads past a hole at once.
    gap_timeout: Duration,
    /// The open holes of the cursor, when each was first seen and which
    /// transactions could fill it. Outlives a session, like the offset.
    holes: Holes,
    /// The step of the cursor's `AUTO_INCREMENT` sequence, read once per
    /// session; `None` when the cursor is not one or the hold is off.
    step: Option<i128>,
    /// Whether this session may read `INNODB_TRX`.
    trx_readable: bool,
    /// The fallback to the timeout has been logged.
    fallback_logged: bool,
    /// `Some` with `install_triggers #true`.
    trigger: Option<TriggerSpec>,
    installed: Option<Installed>,

    /// Advances within a cycle as batches are emitted.
    offset: Option<Offset>,
    /// The position that may be persisted: the last row actually handed out.
    committed: Option<Offset>,
    /// The offset has not been written to the offset table yet.
    store_pending: bool,
    /// Rows were handed out since the last prune.
    prune_pending: bool,
    /// A prune that waited on a lock is not tried again before this.
    prune_after: Option<Instant>,
    /// Whether the offset table has been read.
    loaded: bool,
    /// Set when the previous call ended a cycle.
    cycle_start: bool,
    batches_this_cycle: usize,
    /// A `next_batch` call is running, or was dropped before it returned.
    in_flight: bool,
    columns: Vec<ReadColumn>,

    instruments: Instruments,
}

impl CursorReader {
    /// Build the reader from a validated config. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the DSN, the table, the offset
    /// table or the cursor column cannot be resolved, or a literal `initial` is
    /// not a value of the cursor's type.
    pub(crate) fn new(cfg: &MariadbSourceConfig, schema: Arc<Schema>) -> Result<Self, SaciError> {
        let connector = Connector::new(Role::Source, &cfg.connection)?;

        let (table, cursor_name, tiebreak_name, where_clause, initial, retention, trigger) =
            match &cfg.mode {
                SourceMode::Polling(mode) => (
                    connector.resolve_table("mode.table", &mode.table)?,
                    mode.cursor_column.clone(),
                    mode.tiebreak_column.clone(),
                    mode.where_clause.clone(),
                    mode.initial.clone(),
                    Retention::Keep,
                    None,
                ),
                SourceMode::CdcTrigger(mode) if !mode.install_triggers => (
                    connector.resolve_table("mode.table", &mode.table)?,
                    mode.cursor_column.clone().ok_or_else(|| {
                        SaciError::configuration(format!(
                            "{WHAT}: mode.cursor_column is required with install_triggers #false"
                        ))
                    })?,
                    mode.tiebreak_column.clone(),
                    mode.where_clause.clone(),
                    mode.initial.clone(),
                    mode.effective_retention(),
                    None,
                ),
                SourceMode::CdcTrigger(mode) => {
                    let watched = connector.resolve_table("mode.table", &mode.table)?;
                    let named = mode.effective_changelog_table(&cfg.name);
                    // A bare changelog name lives beside the watched table, not
                    // in the DSN's database.
                    let changelog = if named.contains('.') {
                        connector.resolve_table("mode.changelog_table", &named)?
                    } else {
                        TableRef {
                            schema: watched.schema.clone(),
                            name: named,
                        }
                    };
                    (
                        changelog.clone(),
                        "__seq".to_string(),
                        None,
                        None,
                        mode.initial.clone(),
                        mode.effective_retention(),
                        Some(TriggerSpec::new(&cfg.name, watched, changelog)),
                    )
                }
                SourceMode::CdcBinlog(_) => {
                    return Err(SaciError::configuration(format!(
                        "{WHAT}: CursorReader cannot serve mode kind = \"cdc_binlog\""
                    )));
                }
            };
        let gap_timeout = cfg.mode.gap_timeout();
        let gap_max_wait = cfg.mode.gap_max_wait();

        let offset_table = connector.resolve_table("mode.offset_table", cfg.mode.offset_table())?;
        let cursor = CursorColumn::resolve(&cfg.schema_fields, &cursor_name)?;
        let tiebreak = tiebreak_name
            .as_deref()
            .map(|name| CursorColumn::resolve(&cfg.schema_fields, name))
            .transpose()?;

        if !matches!(initial.as_str(), "beginning" | "now")
            && let Err(wanted) = cursor.bind.value(&initial)
        {
            return Err(SaciError::configuration(format!(
                "{WHAT}: mode.initial '{initial}' is not {wanted}, which cursor column '{}' needs",
                cursor.name
            )));
        }

        Ok(Self {
            offsets: OffsetStore::new(
                WHAT,
                offset_table.clone(),
                &cfg.name,
                cfg.mode.offset_table_autocreate(),
            ),
            offset_table: offset_table.to_string(),
            connector,
            session: None,
            schema,
            table,
            fields: cfg.schema_fields.clone(),
            cursor,
            tiebreak,
            where_clause,
            initial,
            retention,
            batch_rows: cfg.batch_rows,
            max_batches_per_cycle: cfg.max_batches_per_cycle,
            gap_timeout,
            holes: Holes::new(gap_timeout, gap_max_wait),
            step: None,
            trx_readable: false,
            fallback_logged: false,
            trigger,
            installed: None,
            offset: None,
            committed: None,
            store_pending: false,
            prune_pending: false,
            prune_after: None,
            loaded: false,
            cycle_start: false,
            batches_this_cycle: 0,
            in_flight: false,
            columns: Vec::new(),
            instruments: Instruments::source(&cfg.name, cfg.mode.label()),
        })
    }

    /// Set the rows the next query fetches. [`MariadbSource`](super::MariadbSource)
    /// clamps the hint to at least 1 before it gets here; the `LIMIT` is
    /// written into every statement as it is sent, so the next batch uses it.
    pub(crate) fn set_batch_rows(&mut self, rows: usize) {
        self.batch_rows = rows.max(1);
    }

    /// The rows the next query fetches, for the tests that observe
    /// `Source::request_batch_rows` through `MariadbSource`.
    #[cfg(test)]
    pub(crate) fn batch_rows(&self) -> usize {
        self.batch_rows
    }

    /// Pull the next batch, or `Ok(None)` at the end of a drain cycle.
    ///
    /// A call the host drops before it returns (the stream runner does on
    /// cancellation) may leave the session mid-statement or half prepared, so
    /// the next call opens a fresh one. The offsets and the cycle state are
    /// only ever moved by code that runs to its end without an `await`, or by
    /// steps that are safe to run again, so they carry over.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        if std::mem::replace(&mut self.in_flight, true) {
            self.session = None;
        }
        let result = self.step().await;
        self.in_flight = false;
        result
    }

    async fn step(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.ensure_session().await?;

        if self.cycle_start {
            self.commit().await?;
            self.cycle_start = false;
            self.batches_this_cycle = 0;
        }

        if self.max_batches_per_cycle > 0 && self.batches_this_cycle >= self.max_batches_per_cycle {
            self.cycle_start = true;
            return Ok(None);
        }

        let started = Instant::now();
        let (rows, floor) = if self.step.is_none() {
            let rows = self.query().await?;
            self.instruments.observe(started.elapsed().as_secs_f64());
            (rows, None)
        } else if self.where_clause.is_some() {
            self.read_filtered().await?
        } else {
            (self.read_sequence().await?, None)
        };

        if rows.is_empty() {
            self.cycle_start = true;
            return Ok(None);
        }

        // Rows captured after a column changed type may be truncated, and the
        // triggers keep running under the old definition: check before handing
        // any of them over, not only when a session starts.
        if let Some(spec) = &self.trigger {
            let session = self.session.as_mut().expect("session");
            if let Err(e) =
                triggers::verify_types(WHAT, &mut session.conn, spec, self.connector.target()).await
            {
                self.instruments.error("trigger");
                return Err(e);
            }
        }

        let batch = self.build_batch(&rows)?;
        if let Some(bound) = floor {
            // The filtered read matched everything up to `bound`, so the next
            // read starts after it, not after the last row it returned.
            self.advance_to(bound);
        }
        self.batches_this_cycle += 1;
        self.instruments.batch(batch.num_rows() as u64);
        #[cfg(feature = "tracing")]
        tracing::debug!(
            table = %self.table,
            rows = batch.num_rows(),
            elapsed_us = started.elapsed().as_micros(),
            "mariadb cursor batch"
        );
        Ok(Some(batch))
    }

    /// Connect if there is no session, and prepare it: the offset table, the
    /// triggers, the column types, and, once, the offset itself.
    async fn ensure_session(&mut self) -> Result<(), SaciError> {
        if self.session.is_some() {
            return Ok(());
        }

        let session = match self.connector.connect_with_retry().await {
            Ok(session) => session,
            Err(e) => {
                self.instruments.error("connect");
                return Err(e);
            }
        };

        // A new session is a clean slate for everything it carried. The offset
        // is the process's own and stays.
        self.columns.clear();
        self.installed = None;
        self.step = None;
        self.trx_readable = false;
        self.offsets.reset();
        self.session = Some(session);

        if let Err(e) = self.prepare_session().await {
            // Whatever failed, the next call starts from a fresh connection.
            self.session = None;
            return Err(e);
        }
        Ok(())
    }

    async fn prepare_session(&mut self) -> Result<(), SaciError> {
        let session = self.session.as_mut().expect("session just assigned");
        if let Err(e) = self.offsets.ensure(&mut session.conn).await {
            self.instruments.error("offset");
            return Err(e);
        }
        if self.retention == Retention::DeleteAcked {
            // The prune is the one statement of this session that can wait on
            // another transaction's lock, so it waits briefly and is retried
            // later instead of holding the cycle.
            let statement =
                format!("SET SESSION innodb_lock_wait_timeout = {PRUNE_LOCK_WAIT_SECS}");
            if let Err(e) = session.conn.query_drop(&statement).await {
                return Err(SaciError::generic(format!(
                    "{WHAT}: cannot run '{statement}' on {}: {}",
                    self.connector.target(),
                    mysql_detail(&e)
                )));
            }
        }

        if let Some(spec) = &self.trigger {
            match triggers::install(WHAT, &mut session.conn, spec, self.connector.target()).await {
                Ok(installed) => self.installed = Some(installed),
                Err(e) => {
                    self.instruments.error("trigger");
                    return Err(e);
                }
            }
        }

        self.resolve_columns().await?;
        self.detect_sequence().await?;

        if !self.loaded {
            let session = self.session.as_mut().expect("session");
            let stored = match self.offsets.load(&mut session.conn).await {
                Ok(stored) => stored,
                Err(e) => {
                    self.instruments.error("offset");
                    return Err(e);
                }
            };
            self.offset = match stored {
                Some(stored) => {
                    self.validate_stored(&stored)?;
                    Some(stored)
                }
                None => {
                    let resolved = self.resolve_initial().await?;
                    self.store_pending = resolved.is_some();
                    resolved
                }
            };
            self.committed = self.offset.clone();
            self.loaded = true;
            #[cfg(feature = "tracing")]
            tracing::info!(
                table = %self.table,
                cursor = ?self.offset,
                "mariadb cursor source resumed"
            );
        }
        Ok(())
    }

    /// Refuse a stored offset the cursor's type cannot bind: bound as NULL it
    /// would match no row and stall the source without a word.
    fn validate_stored(&self, stored: &Offset) -> Result<(), SaciError> {
        let check = |column: &CursorColumn, text: &str| {
            column.bind.value(text).map(drop).map_err(|wanted| {
                SaciError::configuration(format!(
                    "{WHAT}: the offset stored in '{}' for this source ('{text}') is not \
                     {wanted}, which cursor column '{}' needs; delete its row or set the \
                     cursor column back",
                    self.offset_table, column.name
                ))
            })
        };
        check(&self.cursor, &stored.cursor)?;
        if let (Some(tiebreak), Some(text)) = (&self.tiebreak, &stored.tiebreak) {
            check(tiebreak, text)?;
        }
        Ok(())
    }

    /// Read the table's column types once per session and pair each with its
    /// declared field.
    async fn resolve_columns(&mut self) -> Result<(), SaciError> {
        if !self.columns.is_empty() {
            return Ok(());
        }
        let session = self.session.as_mut().expect("session");
        let target = self.connector.target();

        let resolved = match &self.installed {
            None => {
                let catalog = columns_of(&mut session.conn, WHAT, &self.table, target)
                    .await
                    .inspect_err(|_| self.instruments.error("query"))?;
                validate_columns(WHAT, Access::Query, &self.fields, &catalog)
                    .inspect_err(|_| self.instruments.error("query"))?
                    .into_iter()
                    .map(|resolved| ReadColumn {
                        resolved,
                        from_sql: true,
                    })
                    .collect::<Vec<_>>()
            }
            Some(installed) => {
                // The data fields are the watched table's own columns, so they
                // resolve against it and a mismatch names it. The metadata
                // fields resolve against the changelog; `__table` has no
                // column at all.
                let (meta, data): (Vec<&FieldSpec>, Vec<&FieldSpec>) =
                    self.fields.iter().partition(|f| f.is_reserved());
                let (table, meta): (Vec<&FieldSpec>, Vec<&FieldSpec>) =
                    meta.into_iter().partition(|f| f.name == "__table");
                let data = validate_columns(WHAT, Access::Query, data, &installed.watched)
                    .inspect_err(|_| self.instruments.error("query"))?;
                let meta = validate_columns(WHAT, Access::Query, meta, &installed.changelog)
                    .inspect_err(|_| self.instruments.error("query"))?;
                let mut all: Vec<ReadColumn> = Vec::with_capacity(self.fields.len());
                for field in &self.fields {
                    let (resolved, from_sql) = if table.iter().any(|t| t.name == field.name) {
                        (ResolvedColumn::synthetic(field), false)
                    } else {
                        let found = data
                            .iter()
                            .chain(&meta)
                            .find(|r| r.spec.name == field.name)
                            .expect("validate_columns resolves every declared field");
                        (found.clone(), true)
                    };
                    all.push(ReadColumn { resolved, from_sql });
                }
                all
            }
        };
        self.columns = resolved;
        Ok(())
    }

    /// Apply `initial` when the offset table holds nothing for this source.
    async fn resolve_initial(&mut self) -> Result<Option<Offset>, SaciError> {
        match self.initial.as_str() {
            "beginning" => Ok(None),
            "now" => {
                let mut sql = format!("SELECT CAST({} AS CHAR)", self.cursor.quoted);
                if let Some(tiebreak) = &self.tiebreak {
                    sql.push_str(&format!(", CAST({} AS CHAR)", tiebreak.quoted));
                }
                sql.push_str(&format!(" FROM {}", self.table.quoted()));
                if let Some(predicate) = &self.where_clause {
                    sql.push_str(&format!(" WHERE ({predicate})"));
                }
                sql.push_str(&format!(" ORDER BY {} DESC", self.cursor.quoted));
                if let Some(tiebreak) = &self.tiebreak {
                    sql.push_str(&format!(", {} DESC", tiebreak.quoted));
                }
                sql.push_str(" LIMIT 1");

                let session = self.session.as_mut().expect("session");
                let row = exec_closing(&mut session.conn, &sql, Params::Empty)
                    .await
                    .map_err(|e| {
                        self.read_error(
                            format!("cannot read the current cursor maximum of '{}'", self.table),
                            &e,
                        )
                    })?
                    .into_iter()
                    .next();
                match row {
                    None => Ok(None),
                    Some(row) => self.offset_of(&row, 0).map(Some),
                }
            }
            literal => Ok(Some(Offset {
                cursor: literal.to_string(),
                tiebreak: None,
            })),
        }
    }

    /// The offset `row` carries at `at` (cursor) and `at + 1` (tiebreak).
    fn offset_of(&self, row: &Row, at: usize) -> Result<Offset, SaciError> {
        let text = |index: usize| match row.as_ref(index) {
            Some(Value::Bytes(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
            _ => None,
        };
        let cursor = text(at).ok_or_else(|| self.null_cursor())?;
        let tiebreak = match &self.tiebreak {
            None => None,
            // `tiebreak > ?` matches nothing for NULL, and an offset without a
            // tiebreak continues at the next cursor value, which would skip
            // the rest of the run of equal cursors.
            Some(column) => Some(text(at + 1).ok_or_else(|| {
                SaciError::generic(format!(
                    "{WHAT}: tiebreak column '{}' of '{}' is NULL; a tiebreak column must be NOT \
                     NULL for the source to resume inside a run of equal cursor values",
                    column.name, self.table
                ))
            })?),
        };
        Ok(Offset { cursor, tiebreak })
    }

    /// Find out whether the cursor is a sequence the server numbers, its step,
    /// and whether the user may read `INNODB_TRX`, once per session.
    async fn detect_sequence(&mut self) -> Result<(), SaciError> {
        if self.gap_timeout.is_zero() || !self.cursor.bind.is_integer() {
            return Ok(());
        }
        let session = self.session.as_mut().expect("session");
        let step = sequence::step_of(&mut session.conn, WHAT, &self.table, &self.cursor.name)
            .await
            .inspect_err(|_| self.instruments.error("query"))?;
        let Some(step) = step else {
            return Ok(());
        };
        self.step = Some(step);
        #[cfg(feature = "tracing")]
        tracing::debug!(table = %self.table, step = %step, "mariadb cursor is an AUTO_INCREMENT sequence");
        self.probe_transactions().await
    }

    /// Probe `INNODB_TRX` once for the session. A user without `PROCESS` is
    /// refused, and the source falls back to the timeout.
    async fn probe_transactions(&mut self) -> Result<(), SaciError> {
        let session = self.session.as_mut().expect("session");
        match sequence::trx_readable(&mut session.conn).await {
            Ok(()) => {
                self.trx_readable = true;
                Ok(())
            }
            Err(e) if e.is_fatal() => Err(self.read_error(
                format!("cannot read the running transactions for '{}'", self.table),
                &e,
            )),
            Err(e) => {
                self.lose_evidence(&mysql_detail(&e));
                Ok(())
            }
        }
    }

    /// Go back to the timeout alone for every hole, and say so once.
    fn lose_evidence(&mut self, reason: &str) {
        self.trx_readable = false;
        self.holes.forget_evidence();
        if !std::mem::replace(&mut self.fallback_logged, true) {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                table = %self.table,
                reason,
                gap_timeout_ms = u64::try_from(self.gap_timeout.as_millis()).unwrap_or(u64::MAX),
                "mariadb source cannot read information_schema.INNODB_TRX, so a hole in an \
                 AUTO_INCREMENT cursor is passed after gap_timeout_ms alone; grant PROCESS to \
                 wait for the transactions that could fill it"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = reason;
        }
    }

    /// The ids of the running transactions that have modified rows, or `None`
    /// when the list is not readable.
    async fn writers(&mut self) -> Result<Option<HashSet<u64>>, SaciError> {
        if !self.trx_readable {
            return Ok(None);
        }
        let session = self.session.as_mut().expect("session");
        match sequence::active_writers(&mut session.conn).await {
            Ok(active) => Ok(Some(active)),
            Err(e) if e.is_fatal() => Err(self.read_error(
                format!("cannot read the running transactions for '{}'", self.table),
                &e,
            )),
            Err(e) => {
                self.lose_evidence(&mysql_detail(&e));
                Ok(None)
            }
        }
    }

    /// Before a read: forget, for every open hole, the transactions that have
    /// ended since it was recorded. A hole left empty is then empty in the read
    /// that follows.
    async fn trim_blockers(&mut self) -> Result<(), SaciError> {
        if self.holes.tracking()
            && let Some(active) = self.writers().await?
        {
            self.holes.trim(&active);
        }
        Ok(())
    }

    /// After a read that showed holes: record the running transactions as the
    /// ones that could fill the holes that have none recorded. Returns whether
    /// one of them is already settled, so the read is worth repeating at once.
    ///
    /// Only this probe needs a list newer than the read, and it proves the list
    /// current ([`sequence::fresh_writers`]). A later probe that returns an older
    /// snapshot can only keep a finished transaction in a hole's set a little
    /// longer.
    async fn settle(&mut self, release: &Release) -> Result<bool, SaciError> {
        if release.unattached.is_empty() || !self.trx_readable {
            return Ok(false);
        }
        let Some(active) = self.fresh_writers().await? else {
            return Ok(false);
        };
        self.holes.attach(&release.unattached, &active);
        Ok(self.holes.settled(&release.unattached))
    }

    /// The running writers from a list that includes this session's own
    /// transaction, asked for up to [`TRX_TRIES`] times. `None` when the list
    /// stays older than the read, or stops being readable.
    async fn fresh_writers(&mut self) -> Result<Option<HashSet<u64>>, SaciError> {
        for attempt in 0..TRX_TRIES {
            if attempt > 0 {
                tokio::time::sleep(TRX_RETRY).await;
            }
            let session = self.session.as_mut().expect("session");
            match sequence::fresh_writers(&mut session.conn, &self.table).await {
                Ok(Some(active)) => return Ok(Some(active)),
                Ok(None) => {}
                Err(e) if e.is_fatal() => {
                    return Err(self.read_error(
                        format!("cannot read the running transactions for '{}'", self.table),
                        &e,
                    ));
                }
                Err(e) => {
                    self.lose_evidence(&mysql_detail(&e));
                    return Ok(None);
                }
            }
        }
        #[cfg(feature = "tracing")]
        tracing::debug!(
            table = %self.table,
            "mariadb INNODB_TRX list stayed older than the read; the hole waits for gap_timeout_ms"
        );
        Ok(None)
    }

    /// Judge `values`, the cursors of a read in order: how many may be
    /// delivered, and what to report about the holes that are passed.
    fn decide(&mut self, values: &[i128]) -> Result<Release, SaciError> {
        let step = self.step.expect("a sequence cursor has a step");
        let last = self
            .offset
            .as_ref()
            .map(|offset| {
                offset.cursor.parse::<i128>().map_err(|_| {
                    SaciError::generic(format!(
                        "{WHAT}: the stored cursor '{}' is not an integer, which cursor column \
                         '{}' needs",
                        offset.cursor, self.cursor.name
                    ))
                })
            })
            .transpose()?;

        let release = self.holes.release(Instant::now(), step, last, values);
        self.instruments
            .held_rows((values.len() - release.deliver) as u64);
        for passed in &release.passed {
            self.report(passed);
        }
        Ok(release)
    }

    /// Count a hole that was passed, and warn when a row may still commit
    /// into it.
    fn report(&self, passed: &Passed) {
        self.instruments.gaps(1, passed.outcome.label());
        #[cfg(feature = "tracing")]
        if passed.outcome != sequence::Outcome::Settled {
            tracing::warn!(
                table = %self.table,
                after = %passed.hole.last,
                before = %passed.hole.next,
                outcome = passed.outcome.label(),
                "mariadb sequence hole passed while a row could still commit into it; such a row is not delivered"
            );
        } else {
            tracing::debug!(
                table = %self.table,
                after = %passed.hole.last,
                before = %passed.hole.next,
                "mariadb sequence hole passed: no running transaction can fill it"
            );
        }
    }

    /// The cursor of each of `rows`, with the cursor at `at`.
    fn row_numbers(&self, rows: &[Row], at: usize) -> Result<Vec<i128>, SaciError> {
        rows.iter().map(|row| self.cursor_number(row, at)).collect()
    }

    /// Read the next rows of a cursor with no filter, and cut them to the ones
    /// before the first hole that may still be filled.
    async fn read_sequence(&mut self) -> Result<Vec<Row>, SaciError> {
        let at = self.columns.iter().filter(|c| c.from_sql).count();
        let mut probed = false;
        let mut pass = 0;
        loop {
            pass += 1;
            if !probed {
                self.trim_blockers().await?;
            }
            let started = Instant::now();
            let mut rows = self.query().await?;
            self.instruments.observe(started.elapsed().as_secs_f64());
            let values = self.row_numbers(&rows, at)?;
            let release = self.decide(&values)?;
            if pass < MAX_PASSES && self.settle(&release).await? {
                probed = true;
                continue;
            }
            rows.truncate(release.deliver);
            return Ok(rows);
        }
    }

    /// Read the next window of cursor values with no filter, and judge it.
    async fn read_ids(&mut self) -> Result<(Vec<i128>, Release), SaciError> {
        let mut probed = false;
        let mut pass = 0;
        loop {
            pass += 1;
            if !probed {
                self.trim_blockers().await?;
            }
            let values = self.query_ids().await?;
            let release = self.decide(&values)?;
            if pass < MAX_PASSES && self.settle(&release).await? {
                probed = true;
                continue;
            }
            return Ok((values, release));
        }
    }

    /// Read the next rows of a filtered cursor. The holes come from the
    /// unfiltered numbers, and the filtered rows are read only up to the last
    /// number before the first open hole. The second value is that bound when
    /// the rows are everything the filter matches up to it, and the offset
    /// should move there.
    ///
    /// The numbers are read first: a transaction that commits between the two
    /// queries shows up in the second, never the other way round, so a row the
    /// first query lacked can only be early, not lost.
    async fn read_filtered(&mut self) -> Result<(Vec<Row>, Option<i128>), SaciError> {
        loop {
            let (values, release) = self.read_ids().await?;
            if release.deliver == 0 {
                return Ok((Vec::new(), None));
            }
            let bound = values[release.deliver - 1];
            let started = Instant::now();
            let rows = self.query_to(Some(bound)).await?;
            self.instruments.observe(started.elapsed().as_secs_f64());
            if rows.len() >= self.batch_rows {
                return Ok((rows, None));
            }
            if !rows.is_empty() {
                return Ok((rows, Some(bound)));
            }
            // The filter dropped the whole window: move past it and read on.
            self.advance_to(bound);
        }
    }

    /// Move the offset to `bound`, a number every matching row at or before
    /// which has been handed over.
    fn advance_to(&mut self, bound: i128) {
        let offset = Offset {
            cursor: bound.to_string(),
            tiebreak: None,
        };
        self.offset = Some(offset.clone());
        self.committed = Some(offset);
        self.store_pending = true;
        self.prune_pending = true;
    }

    /// The cursor `row` carries at `at`, as the integer it is.
    fn cursor_number(&self, row: &Row, at: usize) -> Result<i128, SaciError> {
        let Some(Value::Bytes(bytes)) = row.as_ref(at) else {
            return Err(self.null_cursor());
        };
        std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| {
                self.instruments.error("decode");
                SaciError::generic(format!(
                    "{WHAT}: cursor column '{}' of '{}' returned a value that is not an integer",
                    self.cursor.name, self.table
                ))
            })
    }

    /// The error for a NULL cursor value.
    fn null_cursor(&self) -> SaciError {
        SaciError::generic(format!(
            "{WHAT}: cursor column '{}' of '{}' is NULL; a cursor column must be NOT NULL \
             for the source to make progress",
            self.cursor.name, self.table
        ))
    }

    /// Persist the committed offset and, for `delete_acked`, prune the table.
    async fn commit(&mut self) -> Result<(), SaciError> {
        let Some(offset) = self.committed.clone() else {
            return Ok(());
        };

        if self.store_pending {
            let session = self.session.as_mut().expect("session");
            let flavor = session.flavor;
            if let Err(e) = self.offsets.store(&mut session.conn, flavor, &offset).await {
                self.instruments.error("offset");
                // The error is not the driver's, so a dead connection cannot be
                // told from a refused write: reconnect either way.
                self.session = None;
                return Err(e);
            }
            self.store_pending = false;
        }

        if self.prune_pending && self.retention == Retention::DeleteAcked {
            if self.prune_after.is_some_and(|after| Instant::now() < after) {
                return Ok(());
            }
            match self.prune(&offset).await? {
                Some(deleted) => {
                    self.prune_after = None;
                    #[cfg(feature = "tracing")]
                    tracing::debug!(table = %self.table, deleted, "mariadb outbox rows pruned");
                    #[cfg(not(feature = "tracing"))]
                    let _ = deleted;
                }
                None => {
                    // A writer still open below the offset holds a lock the
                    // delete needs. The rows stay until a later cycle.
                    self.prune_after = Some(Instant::now() + PRUNE_RETRY);
                    #[cfg(feature = "tracing")]
                    tracing::debug!(table = %self.table, "mariadb prune waited on a lock; retrying later");
                    return Ok(());
                }
            }
        }
        self.prune_pending = false;
        Ok(())
    }

    /// Delete the rows at or before `offset`, returning how many, or `None`
    /// when the delete lost a lock wait and should be tried again later.
    ///
    /// The rows are counted first and the `DELETE` is limited to that count. A
    /// bare range delete reads one record past its range to find the end, and
    /// InnoDB makes it wait for that record's lock. An open writer's uncommitted
    /// insert is that record when a hole follows the offset, or when a row is in
    /// flight at the table's tail. A limited delete stops after its last row and
    /// never reads past it. An open writer's record can still lie inside the
    /// range, below a hole that was passed while it ran: the delete then waits
    /// [`PRUNE_LOCK_WAIT_SECS`] and gives up.
    async fn prune(&mut self, offset: &Offset) -> Result<Option<u64>, SaciError> {
        let (sql, params) = self.count_statement(offset)?;
        let session = self.session.as_mut().expect("session");
        let counted = match exec_closing(&mut session.conn, &sql, Params::Positional(params)).await
        {
            Ok(rows) => rows,
            Err(e) => {
                return Err(self.read_error(
                    format!("cannot count acknowledged rows of '{}'", self.table),
                    &e,
                ));
            }
        };
        let rows = counted
            .first()
            .and_then(|row| row.get_opt::<u64, _>(0))
            .and_then(Result::ok)
            .unwrap_or(0);
        if rows == 0 {
            return Ok(Some(0));
        }

        let (sql, params) = self.delete_statement(offset, rows)?;
        let session = self.session.as_mut().expect("session");
        match exec_closing(&mut session.conn, &sql, Params::Positional(params)).await {
            Ok(_) => Ok(Some(session.conn.affected_rows())),
            Err(e) if server_code(&e).is_some_and(|code| LOCK_LOST.contains(&code)) => Ok(None),
            Err(e) => Err(self.read_error(
                format!("cannot prune acknowledged rows from '{}'", self.table),
                &e,
            )),
        }
    }

    /// The rows at or before `offset`, matching the read ordering: a `WHERE`
    /// predicate and its parameters.
    fn acknowledged(&self, offset: &Offset) -> Result<(String, Vec<Value>), SaciError> {
        let cursor = self.bind_value(&self.cursor, &offset.cursor)?;
        let cur = &self.cursor;
        match (&self.tiebreak, &offset.tiebreak) {
            (Some(tie), Some(tie_text)) => Ok((
                format!(
                    "{c} < {p} OR ({c} = {p} AND {t} <= {tp})",
                    c = cur.quoted,
                    p = cur.bind.placeholder(),
                    t = tie.quoted,
                    tp = tie.bind.placeholder(),
                ),
                vec![cursor.clone(), cursor, self.bind_value(tie, tie_text)?],
            )),
            _ => Ok((
                format!("{c} <= {p}", c = cur.quoted, p = cur.bind.placeholder()),
                vec![cursor],
            )),
        }
    }

    /// `SELECT COUNT(*)` of the rows at or before `offset`.
    pub(crate) fn count_statement(
        &self,
        offset: &Offset,
    ) -> Result<(String, Vec<Value>), SaciError> {
        let (predicate, params) = self.acknowledged(offset)?;
        Ok((
            format!(
                "SELECT COUNT(*) FROM {} WHERE {predicate}",
                self.table.quoted()
            ),
            params,
        ))
    }

    /// `DELETE` the first `rows` rows at or before `offset`, in the read
    /// ordering.
    pub(crate) fn delete_statement(
        &self,
        offset: &Offset,
        rows: u64,
    ) -> Result<(String, Vec<Value>), SaciError> {
        let (predicate, params) = self.acknowledged(offset)?;
        let mut order = self.cursor.quoted.clone();
        if let Some(tie) = &self.tiebreak {
            order.push_str(", ");
            order.push_str(&tie.quoted);
        }
        Ok((
            format!(
                "DELETE FROM {} WHERE {predicate} ORDER BY {order} LIMIT {rows}",
                self.table.quoted()
            ),
            params,
        ))
    }

    fn bind_value(&self, column: &CursorColumn, text: &str) -> Result<Value, SaciError> {
        column.bind.value(text).map_err(|wanted| {
            SaciError::generic(format!(
                "{WHAT}: cursor value '{text}' is not {wanted}, which '{}' needs",
                column.name
            ))
        })
    }

    /// Run the statement the current offset selects.
    async fn query(&mut self) -> Result<Vec<Row>, SaciError> {
        self.query_to(None).await
    }

    /// Run the statement the current offset selects, with the cursor at or below
    /// `bound` when there is one.
    async fn query_to(&mut self, bound: Option<i128>) -> Result<Vec<Row>, SaciError> {
        let shape = self.shape();
        let sql = self.select_sql_to(shape, bound.is_some());
        let mut params = self.select_params(shape)?;
        if let Some(bound) = bound {
            params.push(self.bind_value(&self.cursor, &bound.to_string())?);
        }
        let session = self.session.as_mut().expect("session");
        let params = if params.is_empty() {
            Params::Empty
        } else {
            Params::Positional(params)
        };
        match exec_closing(&mut session.conn, &sql, params).await {
            Ok(rows) => Ok(rows),
            Err(e) => Err(self.read_error(format!("cannot read '{}'", self.table), &e)),
        }
    }

    /// Read the cursor values after the offset, with no filter: the next window
    /// of numbers a filtered read is judged against.
    async fn query_ids(&mut self) -> Result<Vec<i128>, SaciError> {
        let sql = self.ids_sql();
        let params = match &self.offset {
            Some(offset) => vec![self.bind_value(&self.cursor, &offset.cursor)?],
            None => Vec::new(),
        };
        let session = self.session.as_mut().expect("session");
        let params = if params.is_empty() {
            Params::Empty
        } else {
            Params::Positional(params)
        };
        let rows = match exec_closing(&mut session.conn, &sql, params).await {
            Ok(rows) => rows,
            Err(e) => {
                return Err(self.read_error(format!("cannot read '{}'", self.table), &e));
            }
        };
        self.row_numbers(&rows, 0)
    }

    /// The statement behind [`query_ids`](Self::query_ids): the cursor alone, in
    /// order, over a window wider than a batch so that a selective filter reads
    /// few windows. With a tiebreak the window starts at the offset's own
    /// number, not after it: the rest of a run of equal numbers the last batch
    /// cut is still to read, and with no higher number yet the window would
    /// otherwise be empty. Equal to the offset is not a hole.
    fn ids_sql(&self) -> String {
        let cur = &self.cursor;
        let comparison = if self.shape() == Shape::Pair {
            ">="
        } else {
            ">"
        };
        let after = if self.offset.is_some() {
            format!(
                " WHERE {} {comparison} {}",
                cur.quoted,
                cur.bind.placeholder()
            )
        } else {
            String::new()
        };
        format!(
            "SELECT CAST({c} AS CHAR) FROM {table}{after} ORDER BY {c} LIMIT {window}",
            c = cur.quoted,
            table = self.table.quoted(),
            window = self.batch_rows.saturating_mul(4).max(256),
        )
    }

    /// The parameters of `shape`, in placeholder order.
    fn select_params(&self, shape: Shape) -> Result<Vec<Value>, SaciError> {
        let (Shape::Cursor | Shape::Pair, Some(offset)) = (shape, &self.offset) else {
            return Ok(Vec::new());
        };
        let cursor = self.bind_value(&self.cursor, &offset.cursor)?;
        if shape == Shape::Cursor {
            return Ok(vec![cursor]);
        }
        let tiebreak = self.tiebreak.as_ref().expect("pair shape needs a tiebreak");
        let text = offset
            .tiebreak
            .as_deref()
            .expect("pair shape needs a value");
        Ok(vec![
            cursor.clone(),
            cursor,
            self.bind_value(tiebreak, text)?,
        ])
    }

    /// Which statement the current offset needs.
    fn shape(&self) -> Shape {
        match (&self.offset, &self.tiebreak) {
            (None, _) => Shape::All,
            (Some(offset), Some(_)) if offset.tiebreak.is_some() => Shape::Pair,
            (Some(_), _) => Shape::Cursor,
        }
    }

    /// Whether `field` is read from the server, as opposed to filled in here.
    fn is_projected(&self, field: &FieldSpec) -> bool {
        !(self.trigger.is_some() && field.name == "__table")
    }

    /// The select list: each declared column the server holds.
    fn projection(&self) -> String {
        self.fields
            .iter()
            .filter(|field| self.is_projected(field))
            .map(|field| quote_ident(&field.name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Build the unbounded `SELECT` for `shape`. `pub(crate)` so a dispatch-level
    /// test in `source.rs` can assert the hint `MariadbSource::request_batch_rows`
    /// applies reaches the query, not just the `batch_rows` field.
    #[cfg(test)]
    pub(crate) fn select_sql(&self, shape: Shape) -> String {
        self.select_sql_to(shape, false)
    }

    /// The `SELECT` for `shape`, also bounded above by a `cursor <= ?` when
    /// `bounded`, whose parameter follows the shape's own.
    fn select_sql_to(&self, shape: Shape, bounded: bool) -> String {
        let mut sql = format!(
            "SELECT {}, CAST({} AS CHAR) AS {}",
            self.projection(),
            self.cursor.quoted,
            quote_ident(CURSOR_ALIAS)
        );
        if let Some(tiebreak) = &self.tiebreak {
            sql.push_str(&format!(
                ", CAST({} AS CHAR) AS {}",
                tiebreak.quoted,
                quote_ident(TIEBREAK_ALIAS)
            ));
        }
        sql.push_str(&format!(" FROM {}", self.table.quoted()));

        let cur = &self.cursor;
        let predicate = match shape {
            Shape::All => None,
            Shape::Cursor => Some(format!("{} > {}", cur.quoted, cur.bind.placeholder())),
            Shape::Pair => {
                let tie = self
                    .tiebreak
                    .as_ref()
                    .expect("pair shape needs a tiebreak column");
                Some(format!(
                    "({c} > {p} OR ({c} = {p} AND {t} > {tp}))",
                    c = cur.quoted,
                    p = cur.bind.placeholder(),
                    t = tie.quoted,
                    tp = tie.bind.placeholder(),
                ))
            }
        };
        let bound = bounded.then(|| format!("{} <= {}", cur.quoted, cur.bind.placeholder()));
        let extra = self.where_clause.as_ref().map(|extra| format!("({extra})"));
        let conditions: Vec<String> = predicate.into_iter().chain(bound).chain(extra).collect();
        if !conditions.is_empty() {
            sql.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
        }

        sql.push_str(&format!(" ORDER BY {}", cur.quoted));
        if let Some(tiebreak) = &self.tiebreak {
            sql.push_str(&format!(", {}", tiebreak.quoted));
        }
        sql.push_str(&format!(" LIMIT {}", self.batch_rows));
        sql
    }

    /// Decode `rows` into a batch and advance the in-memory cursor.
    fn build_batch(&mut self, rows: &[Row]) -> Result<RecordBatch, SaciError> {
        let mut builders = self
            .columns
            .iter()
            .map(|column| ColumnBuilder::new(&column.resolved, rows.len()))
            .collect::<Result<Vec<_>, _>>()?;
        let table_name = self
            .trigger
            .as_ref()
            .map(|spec| spec.watched.to_string())
            .unwrap_or_default();

        for row in rows {
            let mut index = 0;
            for (column, builder) in self.columns.iter().zip(builders.iter_mut()) {
                let pushed = if column.from_sql {
                    let value = row.as_ref(index).ok_or_else(|| {
                        SaciError::generic(format!(
                            "{WHAT}: '{}' returned a row with no column {index}",
                            self.table
                        ))
                    })?;
                    index += 1;
                    Cell::from_protocol(value, &column.resolved).and_then(|cell| builder.push(cell))
                } else {
                    builder.push(Cell::Text(table_name.as_bytes()))
                };
                pushed.map_err(|e| {
                    self.instruments.error("decode");
                    SaciError::generic(format!(
                        "{WHAT}: cannot read '{}': {}",
                        self.table,
                        e.message()
                    ))
                })?;
            }
        }

        let last = rows.last().expect("caller checked the slice is non-empty");
        let advanced = self
            .offset_of(last, self.columns.iter().filter(|c| c.from_sql).count())
            .inspect_err(|_| self.instruments.error("decode"))?;

        let arrays = builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(|e| {
            self.instruments.error("decode");
            SaciError::generic(format!(
                "{WHAT}: cannot assemble a batch from '{}': {e}",
                self.table
            ))
        })?;

        self.offset = Some(advanced.clone());
        self.committed = Some(advanced);
        self.store_pending = true;
        self.prune_pending = true;
        Ok(batch)
    }

    /// A failed statement as an error, counting it and dropping the session
    /// when the connection itself is gone. A server error leaves the session
    /// usable, including the statement-timeout error.
    fn read_error(&mut self, context: String, e: &mysql_async::Error) -> SaciError {
        self.instruments.error("query");
        if e.is_fatal() {
            self.session = None;
        }
        SaciError::generic(format!("{WHAT}: {context}: {}", mysql_detail(e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use saci_connector::from_kdl_str;
    use serde::Deserialize as _;

    const HEAD: &str = "name \"src\"\nbatch_rows 100\n\n\
         connection dsn=\"mysql://h:3306/sales\" ssl_mode=\"disabled\"\n\n";

    const FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"updated_at\" type=\"timestamp_micros_utc\" nullable=#false
schema_fields \"label\" type=\"utf8\"
";

    fn config_of(mode: &str, fields: &str) -> MariadbSourceConfig {
        let text = format!("{HEAD}{mode}\n{fields}");
        let cfg = MariadbSourceConfig::deserialize(from_kdl_str(&text).expect("parse kdl"))
            .expect("parse");
        cfg.validate().expect("valid config");
        cfg
    }

    fn reader_of(mode: &str, fields: &str) -> CursorReader {
        let cfg = config_of(mode, fields);
        let schema = Arc::new(Schema::new(
            cfg.schema_fields
                .iter()
                .map(|f| f.to_arrow_field().expect("field"))
                .collect::<Vec<_>>(),
        ));
        CursorReader::new(&cfg, schema).expect("reader")
    }

    fn reader(extra: &str) -> CursorReader {
        reader_of(
            &format!(
                "mode kind=\"polling\" table=\"sales.orders\" cursor_column=\"updated_at\" {extra}"
            ),
            FIELDS,
        )
    }

    #[test]
    fn the_first_query_carries_no_cursor_predicate() {
        assert_eq!(
            reader("").select_sql(Shape::All),
            "SELECT `id`, `updated_at`, `label`, CAST(`updated_at` AS CHAR) AS `__saci_cursor` \
             FROM `sales`.`orders` ORDER BY `updated_at` LIMIT 100"
        );
    }

    #[test]
    fn set_batch_rows_changes_the_query_limit() {
        let mut reader = reader("");
        assert_eq!(reader.batch_rows(), 100, "the configured default");
        reader.set_batch_rows(50);
        assert_eq!(reader.batch_rows(), 50);
        assert!(reader.select_sql(Shape::All).ends_with("LIMIT 50"));
        reader.set_batch_rows(0);
        assert_eq!(reader.batch_rows(), 1, "clamped to at least 1");
    }

    #[test]
    fn an_integer_cursor_binds_typed_with_a_bare_placeholder() {
        let reader = reader_of(
            "mode kind=\"polling\" table=\"sales.orders\" cursor_column=\"id\"",
            FIELDS,
        );
        let sql = reader.select_sql(Shape::Cursor);
        assert!(sql.contains("WHERE `id` > ? ORDER BY `id`"), "{sql}");
        assert_eq!(
            Bind::Signed.value("9007199254740993"),
            Ok(Value::Int(9_007_199_254_740_993))
        );
        assert_eq!(
            Bind::Unsigned.value("18446744073709551613"),
            Ok(Value::UInt(18_446_744_073_709_551_613))
        );
        assert!(Bind::Signed.value("9223372036854775808").is_err());
        assert!(Bind::Unsigned.value("-1").is_err());
        assert!(Bind::Signed.value("12abc").is_err());
    }

    #[test]
    fn a_date_and_a_timestamp_cursor_are_cast_to_their_column_type() {
        let sql = reader("").select_sql(Shape::Cursor);
        assert!(
            sql.contains("WHERE `updated_at` > CAST(? AS DATETIME(6))"),
            "{sql}"
        );
        let dated = reader_of(
            "mode kind=\"polling\" table=\"sales.orders\" cursor_column=\"day\"",
            "schema_fields \"id\" type=\"int64\"\nschema_fields \"day\" type=\"date32\" nullable=#false\n",
        );
        assert!(
            dated
                .select_sql(Shape::Cursor)
                .contains("WHERE `day` > CAST(? AS DATE)")
        );
    }

    #[test]
    fn a_utf8_cursor_binds_bare() {
        let reader = reader_of(
            "mode kind=\"polling\" table=\"sales.orders\" cursor_column=\"label\"",
            FIELDS,
        );
        assert!(
            reader
                .select_sql(Shape::Cursor)
                .contains("WHERE `label` > ? ORDER BY")
        );
        assert_eq!(
            Bind::Text.value("anything"),
            Ok(Value::Bytes(b"anything".to_vec()))
        );
    }

    #[test]
    fn dates_and_timestamps_are_checked_before_they_bind() {
        assert!(Bind::Date.value("2024-01-30").is_ok());
        assert!(Bind::Date.value("2024-1-30").is_err());
        assert!(Bind::Date.value("x").is_err());
        assert!(Bind::DateTime.value("2024-01-30 10:20:30").is_ok());
        assert!(Bind::DateTime.value("2024-01-30 10:20:30.123456").is_ok());
        assert!(Bind::DateTime.value("2024-01-30T10:20:30.5").is_ok());
        assert!(Bind::DateTime.value("2024-01-30 10:20:30.1234567").is_err());
        assert!(Bind::DateTime.value("2024-01-30 10:20:30.").is_err());
        assert!(Bind::DateTime.value("2024-01-30").is_err());
    }

    #[test]
    fn a_tiebreak_expands_the_comparison_so_the_index_range_scans() {
        let mut reader = reader("tiebreak_column=\"id\"");
        let sql = reader.select_sql(Shape::Pair);
        assert!(
            sql.contains(
                "WHERE (`updated_at` > CAST(? AS DATETIME(6)) OR \
                 (`updated_at` = CAST(? AS DATETIME(6)) AND `id` > ?))"
            ),
            "{sql}"
        );
        assert!(sql.contains("ORDER BY `updated_at`, `id`"), "{sql}");
        assert!(
            sql.contains("CAST(`id` AS CHAR) AS `__saci_tiebreak`"),
            "{sql}"
        );

        reader.offset = Some(Offset {
            cursor: "2024-01-30 10:20:30.123456".to_string(),
            tiebreak: Some("7".to_string()),
        });
        let params = reader.select_params(Shape::Pair).expect("params");
        assert_eq!(
            params,
            vec![
                Value::Bytes(b"2024-01-30 10:20:30.123456".to_vec()),
                Value::Bytes(b"2024-01-30 10:20:30.123456".to_vec()),
                Value::Int(7),
            ]
        );
    }

    #[test]
    fn a_where_clause_is_anded_into_every_shape() {
        let reader = reader("tiebreak_column=\"id\" where_clause=\"status = 'open'\"");
        assert!(
            reader
                .select_sql(Shape::All)
                .contains("FROM `sales`.`orders` WHERE (status = 'open') ORDER BY")
        );
        assert!(
            reader
                .select_sql(Shape::Cursor)
                .contains("WHERE `updated_at` > CAST(? AS DATETIME(6)) AND (status = 'open')")
        );
        assert!(
            reader
                .select_sql(Shape::Pair)
                .contains("`id` > ?)) AND (status = 'open') ORDER BY")
        );
    }

    #[test]
    fn shape_follows_the_offset_and_the_tiebreak_configuration() {
        let mut reader = reader("tiebreak_column=\"id\"");
        assert_eq!(reader.shape(), Shape::All);

        // A literal `initial` has no tiebreak value yet, so the cursor-only
        // predicate is used until the first batch supplies one.
        reader.offset = Some(Offset {
            cursor: "2024-01-01 00:00:00".to_string(),
            tiebreak: None,
        });
        assert_eq!(reader.shape(), Shape::Cursor);
        reader.offset = Some(Offset {
            cursor: "2024-01-01 00:00:00".to_string(),
            tiebreak: Some("7".to_string()),
        });
        assert_eq!(reader.shape(), Shape::Pair);

        let mut plain = self::reader("");
        plain.offset = Some(Offset {
            cursor: "2024-01-01 00:00:00".to_string(),
            tiebreak: Some("ignored".to_string()),
        });
        assert_eq!(plain.shape(), Shape::Cursor);
    }

    #[test]
    fn delete_acked_counts_then_deletes_that_many_in_the_read_ordering() {
        let reader = reader("tiebreak_column=\"id\"");
        let offset = Offset {
            cursor: "2024-01-01 00:00:00".to_string(),
            tiebreak: Some("7".to_string()),
        };
        let (sql, params) = reader.count_statement(&offset).expect("count");
        assert_eq!(
            sql,
            "SELECT COUNT(*) FROM `sales`.`orders` WHERE `updated_at` < CAST(? AS DATETIME(6)) \
             OR (`updated_at` = CAST(? AS DATETIME(6)) AND `id` <= ?)"
        );
        assert_eq!(params.len(), 3);
        let (sql, params) = reader.delete_statement(&offset, 5).expect("delete");
        assert_eq!(
            sql,
            "DELETE FROM `sales`.`orders` WHERE `updated_at` < CAST(? AS DATETIME(6)) OR \
             (`updated_at` = CAST(? AS DATETIME(6)) AND `id` <= ?) \
             ORDER BY `updated_at`, `id` LIMIT 5"
        );
        assert_eq!(params.len(), 3);
        assert_eq!(params[2], Value::Int(7));

        let signed = reader_of(
            "mode kind=\"polling\" table=\"sales.orders\" cursor_column=\"id\"",
            FIELDS,
        );
        let offset = Offset {
            cursor: "9007199254740993".to_string(),
            tiebreak: None,
        };
        let (sql, params) = signed.delete_statement(&offset, 1).expect("delete");
        assert_eq!(
            sql,
            "DELETE FROM `sales`.`orders` WHERE `id` <= ? ORDER BY `id` LIMIT 1"
        );
        assert_eq!(params, vec![Value::Int(9_007_199_254_740_993)]);

        let unsigned = reader_of(
            "mode kind=\"polling\" table=\"sales.orders\" cursor_column=\"n\"",
            "schema_fields \"n\" type=\"uint64\" nullable=#false\n",
        );
        let (sql, params) = unsigned
            .count_statement(&Offset {
                cursor: "18446744073709551615".to_string(),
                tiebreak: None,
            })
            .expect("count");
        assert_eq!(sql, "SELECT COUNT(*) FROM `sales`.`orders` WHERE `n` <= ?");
        assert_eq!(params, vec![Value::UInt(u64::MAX)]);
    }

    /// Rows as `query` returns them for a reader with no declared columns: the
    /// cursor text alone, in the alias column.
    fn cursor_rows(cursors: &[&str]) -> Vec<Row> {
        let columns: Arc<[mysql_async::Column]> = Arc::from(vec![
            mysql_common::packets::Column::new(
                mysql_common::constants::ColumnType::MYSQL_TYPE_VAR_STRING,
            )
            .with_name(CURSOR_ALIAS.as_bytes()),
        ]);
        cursors
            .iter()
            .map(|cursor| {
                mysql_common::row::new_row(
                    vec![Value::Bytes(cursor.as_bytes().to_vec())],
                    Arc::clone(&columns),
                )
            })
            .collect()
    }

    fn sequence_reader(extra: &str, last: &str) -> CursorReader {
        let mut reader = reader_of(
            &format!("mode kind=\"polling\" table=\"sales.orders\" cursor_column=\"id\" {extra}"),
            FIELDS,
        );
        reader.step = Some(1);
        reader.offset = Some(Offset {
            cursor: last.to_string(),
            tiebreak: None,
        });
        reader
    }

    #[test]
    fn gap_timeout_ms_defaults_to_ten_seconds_and_zero_is_the_opt_out() {
        assert_eq!(
            sequence_reader("", "4").gap_timeout,
            Duration::from_secs(10)
        );
        assert_eq!(
            sequence_reader("gap_timeout_ms=250", "4").gap_timeout,
            Duration::from_millis(250)
        );
        assert!(
            sequence_reader("gap_timeout_ms=0", "4")
                .gap_timeout
                .is_zero()
        );
        let outbox = reader_of(
            "mode kind=\"cdc_trigger\" table=\"sales.outbox\" cursor_column=\"id\" gap_timeout_ms=40",
            FIELDS,
        );
        assert_eq!(outbox.gap_timeout, Duration::from_millis(40));
        let installed = reader_of(
            "mode kind=\"cdc_trigger\" table=\"sales.orders\" install_triggers=#true \
             gap_timeout_ms=70",
            "schema_fields \"__seq\" type=\"uint64\" nullable=#false\n\
             schema_fields \"__op\" type=\"utf8\" nullable=#false\n",
        );
        assert_eq!(installed.gap_timeout, Duration::from_millis(70));
    }

    /// What a read does with its rows, without a server: judge the numbers and
    /// cut the rows to the ones that may go out.
    fn hold_back(reader: &mut CursorReader, rows: &mut Vec<Row>) -> Result<(), SaciError> {
        let values = reader.row_numbers(rows, 0)?;
        let release = reader.decide(&values)?;
        rows.truncate(release.deliver);
        Ok(())
    }

    #[test]
    fn a_hole_holds_the_rows_after_it_back_until_a_row_fills_it() {
        let mut reader = sequence_reader("", "4");
        let mut rows = cursor_rows(&["6", "7"]);
        hold_back(&mut reader, &mut rows).expect("hold back");
        assert!(rows.is_empty(), "5 may still commit");

        let mut rows = cursor_rows(&["5", "6", "7"]);
        hold_back(&mut reader, &mut rows).expect("hold back");
        assert_eq!(rows.len(), 3, "5 committed, so nothing is held");
    }

    #[test]
    fn rows_before_a_hole_go_out_and_the_ones_behind_it_wait() {
        let mut reader = sequence_reader("", "4");
        let mut rows = cursor_rows(&["5", "6", "9", "10"]);
        hold_back(&mut reader, &mut rows).expect("hold back");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn with_no_transaction_list_a_hole_older_than_the_timeout_is_passed_over() {
        let mut reader = sequence_reader("gap_timeout_ms=20", "4");
        let mut rows = cursor_rows(&["6", "7"]);
        hold_back(&mut reader, &mut rows).expect("hold back");
        assert!(rows.is_empty());
        std::thread::sleep(Duration::from_millis(40));
        let mut rows = cursor_rows(&["6", "7"]);
        hold_back(&mut reader, &mut rows).expect("hold back");
        assert_eq!(rows.len(), 2, "the hole is given up and both rows go out");
    }

    #[test]
    fn a_null_cursor_is_an_error_not_a_hole() {
        let mut reader = sequence_reader("", "4");
        let columns: Arc<[mysql_async::Column]> =
            Arc::from(vec![mysql_common::packets::Column::new(
                mysql_common::constants::ColumnType::MYSQL_TYPE_VAR_STRING,
            )]);
        let mut rows = vec![mysql_common::row::new_row(vec![Value::NULL], columns)];
        let err = hold_back(&mut reader, &mut rows).expect_err("refused");
        assert!(err.message().contains("is NULL"), "{}", err.message());
    }

    #[test]
    fn the_number_window_reads_the_cursor_alone_and_is_wider_than_a_batch() {
        let mut reader = sequence_reader("where_clause=\"label = 'keep'\"", "4");
        assert_eq!(
            reader.ids_sql(),
            "SELECT CAST(`id` AS CHAR) FROM `sales`.`orders` WHERE `id` > ? ORDER BY `id` \
             LIMIT 400"
        );
        reader.offset = None;
        assert_eq!(
            reader.ids_sql(),
            "SELECT CAST(`id` AS CHAR) FROM `sales`.`orders` ORDER BY `id` LIMIT 400"
        );
        reader.set_batch_rows(10);
        assert!(
            reader.ids_sql().ends_with("LIMIT 256"),
            "a floor on the window"
        );
    }

    #[test]
    fn with_a_tiebreak_the_number_window_starts_at_the_offsets_own_number() {
        let mut reader = sequence_reader(
            "tiebreak_column=\"updated_at\" where_clause=\"label = 'keep'\"",
            "4",
        );
        reader.offset = Some(Offset {
            cursor: "4".to_string(),
            tiebreak: Some("2024-01-01 00:00:00".to_string()),
        });
        assert!(
            reader.ids_sql().contains("WHERE `id` >= ? ORDER BY"),
            "the rest of a tie run the last batch cut is still to read: {}",
            reader.ids_sql()
        );
        // After a bound the offset has no tiebreak, and the run is finished.
        reader.offset = Some(Offset {
            cursor: "4".to_string(),
            tiebreak: None,
        });
        assert!(reader.ids_sql().contains("WHERE `id` > ? ORDER BY"));
    }

    #[test]
    fn a_bounded_select_adds_the_upper_bound_ahead_of_the_filter() {
        let reader = sequence_reader("where_clause=\"label = 'keep'\"", "4");
        assert_eq!(
            reader.select_sql_to(Shape::Cursor, true),
            "SELECT `id`, `updated_at`, `label`, CAST(`id` AS CHAR) AS `__saci_cursor` \
             FROM `sales`.`orders` WHERE `id` > ? AND `id` <= ? AND (label = 'keep') \
             ORDER BY `id` LIMIT 100"
        );
        assert_eq!(
            reader.select_sql_to(Shape::Cursor, false),
            reader.select_sql(Shape::Cursor)
        );
    }

    #[test]
    fn moving_the_offset_to_a_bound_marks_it_for_storing_and_pruning() {
        let mut reader = sequence_reader("", "4");
        reader.advance_to(40);
        assert_eq!(
            reader.offset,
            Some(Offset {
                cursor: "40".to_string(),
                tiebreak: None
            })
        );
        assert_eq!(reader.committed, reader.offset);
        assert!(reader.store_pending && reader.prune_pending);
    }

    #[test]
    fn a_malformed_literal_initial_is_refused_at_build() {
        let cfg = config_of(
            "mode kind=\"polling\" table=\"sales.orders\" cursor_column=\"id\" initial=\"abc\"",
            FIELDS,
        );
        let schema = Arc::new(Schema::empty());
        let Err(err) = CursorReader::new(&cfg, schema) else {
            panic!("a text literal is not an int64 cursor");
        };
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message().contains("mode.initial 'abc' is not a signed"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn a_malformed_stored_offset_names_where_it_came_from() {
        let reader = reader("");
        let err = reader
            .validate_stored(&Offset {
                cursor: "not a time".to_string(),
                tiebreak: None,
            })
            .expect_err("refused");
        let message = err.message();
        assert!(
            message.contains("offset stored in 'sales.saci_source_offsets'"),
            "{message}"
        );
        assert!(message.contains("'not a time'"), "{message}");
    }

    #[test]
    fn an_injected_table_name_is_quoted_not_interpolated() {
        let cfg = config_of(
            "mode kind=\"polling\" table=\"orders`; DROP TABLE x; --\" cursor_column=\"id\"",
            FIELDS,
        );
        let reader = CursorReader::new(&cfg, Arc::new(Schema::empty())).expect("reader");
        let sql = reader.select_sql(Shape::All);
        assert!(
            sql.contains("FROM `sales`.`orders``; DROP TABLE x; --` ORDER BY"),
            "{sql}"
        );
    }

    #[test]
    fn an_installed_changelog_is_read_by_seq_with_table_filled_client_side() {
        let reader = reader_of(
            "mode kind=\"cdc_trigger\" table=\"sales.orders\" install_triggers=#true",
            "schema_fields \"__seq\" type=\"uint64\" nullable=#false\n\
             schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
             schema_fields \"__table\" type=\"utf8\" nullable=#false\n\
             schema_fields \"__commit_ts\" type=\"timestamp_micros_utc\" nullable=#false\n\
             schema_fields \"id\" type=\"int64\"\n",
        );
        assert_eq!(
            reader.select_sql(Shape::Cursor),
            "SELECT `__seq`, `__op`, `__commit_ts`, `id`, CAST(`__seq` AS CHAR) AS `__saci_cursor` \
             FROM `sales`.`saci_cdc_src` WHERE `__seq` > ? ORDER BY `__seq` LIMIT 100"
        );
        assert_eq!(reader.retention, Retention::DeleteAcked);
        let (sql, params) = reader
            .delete_statement(
                &Offset {
                    cursor: "18446744073709551615".to_string(),
                    tiebreak: None,
                },
                3,
            )
            .expect("delete");
        assert_eq!(
            sql,
            "DELETE FROM `sales`.`saci_cdc_src` WHERE `__seq` <= ? ORDER BY `__seq` LIMIT 3"
        );
        assert_eq!(params, vec![Value::UInt(u64::MAX)]);
    }

    #[test]
    fn an_outbox_defaults_to_keeping_its_rows_and_a_named_changelog_keeps_its_schema() {
        let outbox = reader_of(
            "mode kind=\"cdc_trigger\" table=\"sales.outbox\" cursor_column=\"id\"",
            FIELDS,
        );
        assert_eq!(outbox.retention, Retention::Keep);
        let named = reader_of(
            "mode kind=\"cdc_trigger\" table=\"sales.orders\" install_triggers=#true \
             changelog_table=\"audit.log\"",
            "schema_fields \"__seq\" type=\"uint64\" nullable=#false\n\
             schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
             schema_fields \"__table\" type=\"utf8\" nullable=#false\n\
             schema_fields \"__commit_ts\" type=\"timestamp_micros_utc\" nullable=#false\n",
        );
        assert_eq!(named.table.to_string(), "audit.log");
    }

    #[test]
    fn a_connection_error_drops_the_session_and_a_server_error_does_not() {
        assert!(mysql_async::Error::from(mysql_async::DriverError::ConnectionClosed).is_fatal());
        let server = mysql_async::Error::Server(mysql_async::ServerError {
            code: 1969,
            message: "timeout".to_string(),
            state: "70100".to_string(),
        });
        assert!(!server.is_fatal());
    }

    #[test]
    fn cdc_binlog_is_not_a_cursor_mode() {
        let cfg = config_of(
            "mode kind=\"cdc_binlog\" table=\"sales.orders\"",
            "schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
             schema_fields \"__table\" type=\"utf8\" nullable=#false\n\
             schema_fields \"__binlog_file\" type=\"utf8\" nullable=#false\n\
             schema_fields \"__binlog_pos\" type=\"uint64\" nullable=#false\n\
             schema_fields \"__gtid\" type=\"utf8\"\n\
             schema_fields \"__commit_ts\" type=\"timestamp_micros_utc\" nullable=#false\n\
             schema_fields \"id\" type=\"int64\"\n",
        );
        let Err(err) = CursorReader::new(&cfg, Arc::new(Schema::empty())) else {
            panic!("cdc_binlog is not a cursor mode");
        };
        assert_eq!(err.category(), "configuration");
    }
}
