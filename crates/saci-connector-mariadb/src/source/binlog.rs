//! The `cdc_binlog` reader.
//!
//! Reads the row binlog of one table by file position or by GTID set, emitting
//! `I`, `U`, `D` (and `R` from the optional [`super::snapshot`]) rows with their
//! transaction metadata.
//!
//! # What the reader receives
//!
//! [`MariadbSource::new`](super::MariadbSource::new) validates the config and
//! builds the Arrow schema, then calls [`BinlogReader::new`] with the validated
//! config and the batch schema. `new` opens no connection.
//!
//! # Connections
//!
//! A *control* session runs the preflight, the catalog reads, the offset table
//! and the position statements. The binlog stream consumes a connection of its
//! own, opened per drain cycle with `BINLOG_DUMP_NON_BLOCK`, so the server ends
//! the stream when it has sent everything and that end is the cycle's
//! `Ok(None)`. The snapshot, when configured, has a third.
//!
//! # Transactions and positions
//!
//! Rows are buffered until their transaction commits (an `XID` event, or a
//! `COMMIT`/`ROLLBACK` query for a non-transactional table, or a DDL query
//! outside a transaction) and are then queued together, stamped with the
//! commit's position, time and GTID. The last row of a transaction carries an
//! acknowledgement: handing it to the pipeline moves the in-memory committed
//! position past the transaction. The position is stored in the offset table at
//! the start of the next cycle, so delivery is at-least-once.
//!
//! Row events carry no usable position of their own (MariaDB writes 0, and the
//! events inside a MySQL compressed transaction do too), so every row of a
//! transaction gets the position of the event that ends it; for a compressed
//! transaction that is the outer `TRANSACTION_PAYLOAD_EVENT`.
//!
//! Transactions that touch no row of this table still move the read position:
//! the stream keeps a *tail* (the end of the last transaction read, the newest
//! file, the grown GTID set) that is acknowledged together with the last queued
//! row, or alone when nothing is queued. Without it a quiet table on a busy
//! server would re-scan the same range every cycle and eventually point at a
//! purged file.
//!
//! A tail acknowledged alone moves the position in memory always, but is stored
//! only when the stream read news: a transaction that touched a table other
//! than the offset table, DDL, or (file mode) a rotation into a newer file. The
//! connector's own offset writes are in the binlog it reads; storing for them
//! would make each cycle's store the next cycle's news, and an idle source
//! would write forever.
//!
//! # Schema history
//!
//! A row is decoded with the definition of the table that was in force when it
//! was written, which is not the catalog's after an `ALTER TABLE`. The
//! [`history`](super::history) module records every definition the source
//! reads with the position it holds from. A stream starts with the one in
//! force at its start position (a `Timeline`) and switches to each recorded
//! one its commits reach, never reading the catalog while one is ahead, because
//! the catalog may be newer than the DDL being replayed. Past the last
//! recorded one a DDL marks the definition stale; the catalog is read again at
//! the next row event of the table or at the end of the stream, and a
//! definition that differs is checked against that row event's table map,
//! recorded and put in use. A queued row keeps the `Plan` it was read under,
//! and a batch never spans two plans.
//!
//! # Limits
//!
//! - A transaction is buffered whole, so one transaction of a million rows is a
//!   million rows in memory; `max_changes_per_cycle` stops a cycle at the first
//!   commit that reaches it but cannot split a transaction.
//! - A transaction is acknowledged by its last row, so `max_batches_per_cycle`
//!   never ends a cycle inside one: when the cap is reached with a transaction
//!   open, the rest of that transaction goes out first, in batches of the usual
//!   size and with nothing from the next one, and then the cycle ends.
//! - The dump connection stays open while the pipeline works on a batch. A
//!   server drops a dump whose consumer stalls longer than its network write
//!   timeout; the error abandons the cycle and the retry reads again from the
//!   committed position.
//! - Savepoint rollbacks inside a transaction are not undone in the output, XA
//!   transactions are not understood, and `TRUNCATE TABLE` writes no row event.
//!
//! # MariaDB and MySQL on the wire
//!
//! - MariaDB wraps its own GTID, annotate and checkpoint events in `QUERY_EVENT`s
//!   whose text starts `# Dum` (a truncated "Dummy event replacing ..."), unless
//!   the replica declares a capability this reader does not. They carry no
//!   information and are ignored, so `__gtid` is NULL on MariaDB.
//! - MariaDB's first `ROTATE_EVENT` of a dump has four trailing checksum bytes
//!   on its name, because it precedes the format description. File mode keeps
//!   the file it asked for instead.
//! - MariaDB sends version 1 row events and no column metadata: unsigned
//!   integers arrive sign-extended. [`Cell::from_binlog`] masks them to the
//!   column's width.
//! - A MySQL transaction compressed with `binlog_transaction_compression` is a
//!   `TRANSACTION_PAYLOAD_EVENT` whose inner events the driver yields as
//!   ordinary ones.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use futures_util::StreamExt;
use mysql_async::binlog::events::{Event, EventData, RowsEventData, TableMapEvent};
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStream, BinlogStreamRequest, Conn, Row, Value};
use mysql_common::constants::ColumnType;
use mysql_common::packets::{Sid, Tag};
use saci_core::error::SaciError;

use super::gtid::{GtidSet, format_gtid};
use super::history::{Definition, Entry, History, HistoryStore, reached};
use super::snapshot::{Snapshot, SnapshotSpec, plan_keys};
use crate::config::{FieldSpec, MariadbSourceConfig, PositionMode, Role, SnapshotMode, SourceMode};
use crate::connection::{
    Connector, ER_ACCESS_DENIED_ERROR, ER_MASTER_FATAL_ERROR_READING_BINLOG,
    ER_SPECIFIC_ACCESS_DENIED_ERROR, Flavor, Session, TableRef, mysql_detail, server_code,
};
use crate::metrics::Instruments;
use crate::offsets::{BinlogOffset, OffsetStore};
use crate::types::{Access, CatalogColumn, Family, ResolvedColumn, validate_columns};
use crate::values::{Cell, ColumnBuilder};

/// How long closing a stream may take before it is abandoned.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

const WHAT: &str = "MariadbSource";

// ------------------------------------------------------------------ positions

/// Where the reader stands in the binlog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Position {
    /// A binlog file and the byte position of the next event.
    File {
        /// The binlog file name.
        name: String,
        /// The byte position.
        pos: u64,
    },
    /// The GTIDs executed so far.
    Gtid(GtidSet),
}

impl Position {
    /// The position a stored or configured offset names, which must match the
    /// configured `position` mode.
    pub(super) fn from_offset(offset: BinlogOffset, mode: PositionMode) -> Result<Self, String> {
        match (offset, mode) {
            (BinlogOffset::File { file, pos }, PositionMode::File) => {
                Ok(Position::File { name: file, pos })
            }
            (BinlogOffset::Gtid(text), PositionMode::Gtid) => text.parse().map(Position::Gtid),
            (BinlogOffset::File { .. }, PositionMode::Gtid) => {
                Err("it is a file position but mode.position is \"gtid\"".to_string())
            }
            (BinlogOffset::Gtid(_), PositionMode::File) => {
                Err("it is a GTID set but mode.position is \"file\"".to_string())
            }
        }
    }

    /// The offset-table form.
    pub(super) fn to_offset(&self) -> BinlogOffset {
        match self {
            Position::File { name, pos } => BinlogOffset::File {
                file: name.clone(),
                pos: *pos,
            },
            Position::Gtid(set) => BinlogOffset::Gtid(set.to_string()),
        }
    }

    /// Move forward past an acknowledged transaction.
    fn apply(&mut self, ack: &Ack) -> Result<(), String> {
        match (&mut *self, ack) {
            (Position::File { .. }, Ack::File { name, pos }) => {
                *self = Position::File {
                    name: name.to_string(),
                    pos: *pos,
                };
                Ok(())
            }
            (Position::Gtid(set), Ack::Gtid { uuid, tag, gno }) => {
                set.insert(*uuid, tag.as_deref(), *gno)
            }
            (Position::Gtid(_), Ack::Set(set)) => {
                *self = Position::Gtid(set.clone());
                Ok(())
            }
            _ => Err("an acknowledgement of the other position kind".to_string()),
        }
    }
}

/// What handing a row to the pipeline acknowledges.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ack {
    /// The end of a transaction or the start of a new file, in file mode.
    File {
        /// The binlog file.
        name: Arc<str>,
        /// The byte position of the next event.
        pos: u64,
    },
    /// One transaction, in GTID mode.
    Gtid {
        /// The server UUID.
        uuid: [u8; 16],
        /// The GTID tag (MySQL 8.4).
        tag: Option<String>,
        /// The transaction number.
        gno: u64,
    },
    /// Everything read so far, in GTID mode.
    Set(GtidSet),
}

/// The parts of a dump request a position selects: the file name and byte
/// position (file mode), or the GTID blocks.
struct RequestParts {
    filename: Vec<u8>,
    pos: u64,
    sids: Vec<Sid<'static>>,
    gtid: bool,
}

fn request_parts(position: &Position) -> RequestParts {
    match position {
        Position::File { name, pos } => RequestParts {
            filename: name.as_bytes().to_vec(),
            pos: *pos,
            sids: Vec::new(),
            gtid: false,
        },
        Position::Gtid(set) => RequestParts {
            filename: Vec::new(),
            pos: 4,
            sids: set.to_sids(),
            gtid: true,
        },
    }
}

// ----------------------------------------------------------- the event cycle

/// One transaction's row as read, before its commit stamps it.
#[derive(Debug)]
struct Change {
    /// `I`, `U` or `D`.
    op: u8,
    /// After image for `I` and `U`, before image for `D`.
    image: BinlogRow,
    /// The plan of the definition the row was written under.
    plan: Arc<Plan>,
    file: Option<Arc<str>>,
    /// The position of the event that ended the transaction.
    pos: u64,
    gtid: Option<Arc<str>>,
    commit_micros: i64,
    ack: Option<Ack>,
    /// The last row of its transaction.
    tx_end: bool,
}

/// What one event did to the cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Handled {
    Continue,
    /// The cycle has read as many changes as it may; stop at this commit.
    Stop,
    /// A row event for the table arrived whose shape differs from the catalog.
    Reload,
}

/// How a `QUERY_EVENT` bears on the transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Query {
    Ignore,
    Begin,
    Commit { ddl: bool },
}

/// Classify a query's text.
///
/// - MariaDB's `# Dum...` placeholders are ignored.
/// - `BEGIN` opens a transaction; `COMMIT` and `ROLLBACK` end one (a rolled
///   back group still applied what it wrote to a non-transactional table).
/// - Any other statement outside a transaction is DDL (or a flush, or a
///   statement-format change), which ends nothing but moves the position and
///   may have changed the table; inside a transaction (`SAVEPOINT`, ...) it is
///   ignored.
fn classify_query(text: &[u8], in_tx: bool) -> Query {
    let text = text.trim_ascii();
    if text.starts_with(b"# Dum") {
        Query::Ignore
    } else if text.eq_ignore_ascii_case(b"BEGIN") {
        Query::Begin
    } else if text.eq_ignore_ascii_case(b"COMMIT") || text.eq_ignore_ascii_case(b"ROLLBACK") {
        Query::Commit { ddl: false }
    } else if in_tx {
        Query::Ignore
    } else {
        Query::Commit { ddl: true }
    }
}

/// The GTID of the transaction being read.
#[derive(Debug, Clone)]
struct GtidKey {
    uuid: [u8; 16],
    tag: Option<String>,
    gno: u64,
    text: Arc<str>,
}

/// Everything one stream (one drain cycle) tracks between events. Pure state:
/// the tests drive it without a server.
#[derive(Debug, Default)]
struct Cycle {
    file_mode: bool,
    /// The binlog file the events belong to.
    file: Option<Arc<str>>,
    /// The file the dump asked for, in file mode.
    requested: Option<Arc<str>>,
    /// Whether the stream's first format description has arrived.
    fde_seen: bool,
    in_tx: bool,
    gtid: Option<GtidKey>,
    /// The outer position of a compressed transaction.
    payload_end: Option<u32>,
    tx: Vec<(u8, BinlogRow, Arc<Plan>)>,
    rows_read: usize,
    /// Changes after which the cycle stops at a commit; 0 for no limit.
    limit: usize,
    /// GTID mode: the start set, grown by every transaction read.
    read_set: GtidSet,
    gtid_dirty: bool,
    /// File mode: the end of the last transaction, or the newest file's start.
    file_tail: Option<(Arc<str>, u64)>,
    /// A DDL statement was read since the catalog was last read.
    ddl_seen: bool,
    /// A transaction other than the connector's own offset writes was read:
    /// rows of this table, rows of any other table, or DDL.
    foreign: bool,
    /// The definitions this stream decodes with.
    timeline: Timeline,
    /// Where the stream started.
    start: Option<Position>,
    /// The position of the last DDL read since the definition in use was
    /// settled: where a definition read because of it holds from.
    ddl_at: Option<Position>,
    /// What the table map of the row event that asked for a reload says.
    shape: Option<Shape>,
}

impl Cycle {
    /// A cycle with no definition, for the tests, which drive it without a
    /// table.
    #[cfg(test)]
    fn new(start: &Position, limit: usize) -> Self {
        Self::with_timeline(start, limit, Timeline::default())
    }

    /// A cycle starting at `start` that decodes with `timeline`.
    fn with_timeline(start: &Position, limit: usize, timeline: Timeline) -> Self {
        let cycle = Self {
            start: Some(start.clone()),
            limit,
            timeline,
            ..Self::default()
        };
        match start {
            Position::File { name, .. } => {
                let name: Arc<str> = Arc::from(name.as_str());
                Self {
                    file_mode: true,
                    file: Some(Arc::clone(&name)),
                    requested: Some(name),
                    ..cycle
                }
            }
            Position::Gtid(set) => Self {
                file_mode: false,
                read_set: set.clone(),
                ..cycle
            },
        }
    }

    fn on_format_description(&mut self) {
        self.fde_seen = true;
    }

    /// A rotate. The first one of a dump precedes the format description: in
    /// file mode its name is not trusted (MariaDB appends the checksum to it)
    /// and the requested file stands.
    fn on_rotate(&mut self, name_raw: &[u8], position: u64) {
        let name: Arc<str> = match (&self.requested, self.file_mode && !self.fde_seen) {
            (Some(requested), true) => Arc::clone(requested),
            _ => Arc::from(String::from_utf8_lossy(name_raw).as_ref()),
        };
        if self.file_mode {
            self.file_tail = Some((Arc::clone(&name), position));
        }
        self.file = Some(name);
    }

    fn on_gtid(&mut self, uuid: [u8; 16], tag: Option<&str>, gno: u64) {
        let tag = tag.filter(|tag| !tag.is_empty());
        self.gtid = Some(GtidKey {
            uuid,
            tag: tag.map(str::to_string),
            gno,
            text: Arc::from(format_gtid(&uuid, tag, gno)),
        });
    }

    fn on_payload(&mut self, log_pos: u32) {
        self.payload_end = Some(log_pos);
    }

    fn on_begin(&mut self) {
        self.in_tx = true;
    }

    fn on_row(&mut self, op: u8, image: BinlogRow) {
        self.tx.push((op, image, Arc::clone(&self.timeline.active)));
    }

    /// How far the stream has read: the end of the last commit, else where it
    /// started.
    fn tail_position(&self) -> Option<Position> {
        if !self.file_mode {
            return Some(Position::Gtid(self.read_set.clone()));
        }
        match &self.file_tail {
            Some((name, pos)) => Some(Position::File {
                name: name.to_string(),
                pos: *pos,
            }),
            None => self.start.clone(),
        }
    }

    /// Whether a commit has brought the stream to the position `at`.
    fn reaches(&self, at: &Position) -> bool {
        match at {
            Position::File { .. } => {
                self.file_mode && self.tail_position().is_some_and(|tail| reached(at, &tail))
            }
            Position::Gtid(set) => !self.file_mode && set.is_subset_of(&self.read_set),
        }
    }

    /// Switch to every recorded definition the stream has now reached. From
    /// there on the history is authoritative, so a DDL read before it no longer
    /// marks the definition stale.
    fn advance_timeline(&mut self) -> Result<(), SaciError> {
        while self
            .timeline
            .ahead
            .front()
            .is_some_and(|step| self.reaches(&step.at))
        {
            let Some(step) = self.timeline.ahead.pop_front() else {
                break;
            };
            self.timeline.active = step.plan.map_err(SaciError::configuration)?;
            self.ddl_seen = false;
            self.ddl_at = None;
        }
        Ok(())
    }

    /// A commit point: stamp the buffered rows and queue them.
    fn on_commit(
        &mut self,
        queue: &mut VecDeque<Change>,
        timestamp: u32,
        log_pos: u32,
        ddl: bool,
    ) -> Result<Handled, SaciError> {
        let end = self.payload_end.take().unwrap_or(log_pos);
        self.in_tx = false;
        let gtid = self.gtid.take();
        if end == 0 && self.file_mode {
            return Err(SaciError::generic(format!(
                "{WHAT}: a binlog commit event carries no position, so the stream cannot be \
                 resumed after it"
            )));
        }
        if !self.file_mode
            && let Some(gtid) = &gtid
        {
            self.read_set
                .insert(gtid.uuid, gtid.tag.as_deref(), gtid.gno)
                .map_err(|e| {
                    SaciError::generic(format!("{WHAT}: cannot track a binlog GTID: {e}"))
                })?;
            self.gtid_dirty = true;
        }

        let commit_micros = i64::from(timestamp) * 1_000_000;
        let ack = if self.file_mode {
            self.file.as_ref().map(|name| Ack::File {
                name: Arc::clone(name),
                pos: u64::from(end),
            })
        } else {
            gtid.as_ref().map(|gtid| Ack::Gtid {
                uuid: gtid.uuid,
                tag: gtid.tag.clone(),
                gno: gtid.gno,
            })
        };
        let rows = std::mem::take(&mut self.tx);
        let count = rows.len();
        for (index, (op, image, plan)) in rows.into_iter().enumerate() {
            queue.push_back(Change {
                op,
                image,
                plan,
                file: self.file.clone(),
                pos: u64::from(end),
                gtid: gtid.as_ref().map(|gtid| Arc::clone(&gtid.text)),
                commit_micros,
                ack: if index + 1 == count {
                    ack.clone()
                } else {
                    None
                },
                tx_end: index + 1 == count,
            });
        }
        if self.file_mode
            && let Some(name) = &self.file
        {
            self.file_tail = Some((Arc::clone(name), u64::from(end)));
        }
        if ddl {
            self.ddl_seen = true;
            self.ddl_at = self.tail_position();
        }
        if ddl || count > 0 {
            self.foreign = true;
        }
        self.rows_read += count;
        self.advance_timeline()?;
        Ok(if self.limit > 0 && self.rows_read >= self.limit {
            Handled::Stop
        } else {
            Handled::Continue
        })
    }

    /// The end of the stream: drop an uncommitted transaction (it is re-read
    /// next cycle) and hand back the position to acknowledge, with whether it
    /// is worth storing when no row of this table carries it: only a stream
    /// that read news, or that crossed into a newer file in file mode, moves
    /// the stored position. The connector's own offset writes are in the
    /// binlog too, and counting them would make every cycle store a position
    /// that the next cycle reads as news, so an idle source would write forever.
    fn finish(&mut self) -> Option<(Ack, bool)> {
        self.tx.clear();
        self.in_tx = false;
        if self.file_mode {
            let rotated = self.file_tail.as_ref().is_some_and(|(name, _)| {
                self.requested
                    .as_ref()
                    .is_none_or(|requested| **requested != **name)
            });
            let worth_storing = self.foreign || rotated;
            self.file_tail
                .take()
                .map(|(name, pos)| (Ack::File { name, pos }, worth_storing))
        } else if self.gtid_dirty {
            self.gtid_dirty = false;
            Some((Ack::Set(self.read_set.clone()), self.foreign))
        } else {
            None
        }
    }
}

/// Restore the zero padding the binlog strips from `BINARY(n)` values, so a
/// row reads the same as over the protocol.
fn pad_binary(row: &mut BinlogRow, pads: &[(usize, usize)]) {
    for &(index, width) in pads {
        if let Some(BinlogValue::Value(Value::Bytes(bytes))) = row.as_ref(index)
            && bytes.len() < width
        {
            let mut padded = bytes.clone();
            padded.resize(width, 0);
            row.place(index, BinlogValue::Value(Value::Bytes(padded)));
        }
    }
}

/// Whether an update changed any primary key column, which the targets see as a
/// delete of the old key and an insert of the new one.
fn key_changed(key: &[usize], before: &BinlogRow, after: &BinlogRow) -> bool {
    key.iter().any(|&i| before.as_ref(i) != after.as_ref(i))
}

/// How many queued rows the next batch takes: up to `batch_rows`, or with
/// `to_tx_end` no further than the end of the transaction at the head of the
/// queue, and in any case no further than the head's plan reaches: one plan
/// decodes a batch. The queue holds whole transactions, so that end is always
/// in it.
fn take_count(queue: &VecDeque<Change>, batch_rows: usize, to_tx_end: bool) -> usize {
    let mut count = batch_rows.min(queue.len());
    if to_tx_end {
        count = queue
            .iter()
            .position(|change| change.tx_end)
            .map_or(count, |end| count.min(end + 1));
    }
    let Some(head) = queue.front() else {
        return 0;
    };
    queue
        .iter()
        .take(count)
        .take_while(|change| Arc::ptr_eq(&change.plan, &head.plan))
        .count()
}

// ----------------------------------------------------------------- the plan

/// A declared field's source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Meta {
    Op,
    Table,
    File,
    Pos,
    Gtid,
    CommitTs,
}

fn meta_of(name: &str) -> Option<Meta> {
    Some(match name {
        "__op" => Meta::Op,
        "__table" => Meta::Table,
        "__binlog_file" => Meta::File,
        "__binlog_pos" => Meta::Pos,
        "__gtid" => Meta::Gtid,
        "__commit_ts" => Meta::CommitTs,
        _ => return None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot {
    /// The `index`th selected data column (also the snapshot `SELECT` index).
    Data(usize),
    Meta(Meta),
    /// A declared column the definition has no column for: NULL.
    Absent,
}

#[derive(Debug)]
struct FieldPlan {
    column: ResolvedColumn,
    slot: Slot,
}

/// How a declared column that a definition lacks is treated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Missing {
    /// A configuration error: the column is a typo, or was never there.
    Refuse,
    /// NULL, for the rows that definition describes, when the field is
    /// nullable: the column was added later or dropped since.
    Null,
}

/// What one definition of the table says about how its rows decode, resolved
/// once per definition.
#[derive(Debug, Default)]
struct Plan {
    /// The definition this plan was resolved from.
    definition: Arc<Definition>,
    /// The number of columns of the table, which a row image must match.
    ncols: usize,
    /// Row image indexes of the primary key.
    pk_ordinals: Vec<usize>,
    pk_names: Vec<String>,
    /// `(row image index, width)` of every `BINARY(n)` column: the binlog
    /// strips its trailing zero padding, the protocol keeps it.
    binary_pads: Vec<(usize, usize)>,
    /// One per declared field, in declared order.
    fields: Vec<FieldPlan>,
    /// `schema.table`, the `__table` value.
    table_text: String,
    /// `(row image index, name, wire shape)` of every declared data column:
    /// what the table map of a row event must say for the plan to decode it.
    wire: Vec<(usize, String, Wire)>,
}

impl Plan {
    /// The resolved data columns in `SELECT` order.
    fn data_columns(&self) -> Vec<ResolvedColumn> {
        self.fields
            .iter()
            .filter(|field| matches!(field.slot, Slot::Data(_)))
            .map(|field| field.column.clone())
            .collect()
    }

    /// How a row event's table map, given as its column count and the wire
    /// shape of each column it names, disagrees with the columns this plan
    /// decodes, or `None` when it agrees. Only what the table map carries is
    /// compared: a column it has no type for, or metadata a server leaves out,
    /// is skipped. A disagreement means the rows were written under another
    /// definition than this plan's, so decoding them would read bytes as the
    /// wrong type.
    fn disagrees(
        &self,
        columns: usize,
        wire_of: impl Fn(usize) -> Option<Wire>,
    ) -> Option<Disagreement> {
        if columns != self.ncols {
            return Some(Disagreement::Columns { binlog: columns });
        }
        self.wire.iter().find_map(|(ordinal, name, expected)| {
            let actual = wire_of(*ordinal)?;
            (!expected.accepts(actual)).then(|| {
                Disagreement::Column(format!(
                    "column '{name}' is {} in the binlog and {} in the catalog",
                    actual.describe(),
                    expected.describe()
                ))
            })
        })
    }
}

/// How a row event's table map disagrees with a plan.
#[derive(Debug, PartialEq, Eq)]
enum Disagreement {
    /// The row image has this many columns.
    Columns { binlog: usize },
    /// A column's type, as the clause an error quotes.
    Column(String),
}

impl Disagreement {
    /// The error for rows that no definition on record decodes.
    fn error(&self, table: &TableRef, plan: &Plan, remedy: &str) -> SaciError {
        match self {
            Disagreement::Columns { binlog } => SaciError::generic(format!(
                "{WHAT}: table '{table}' changed shape under the binlog (binlog has {binlog} \
                 columns, catalog has {}); {remedy}",
                plan.ncols
            )),
            Disagreement::Column(mismatch) => SaciError::generic(format!(
                "{WHAT}: table '{table}' changed under the binlog ({mismatch}); the rows were \
                 written under another definition of the table, so they are not decoded; {remedy}"
            )),
        }
    }
}

/// What an error about rows no definition decodes tells the operator to do.
fn remedy_text(store: Option<&HistoryStore>) -> String {
    match store {
        Some(store) => format!(
            "the schema history in '{}' holds no definition that decodes them (the table was \
             changed more than once while the source was stopped, or the position is older \
             than the history); restart from a position after the change",
            store.table()
        ),
        None => "mode.schema_history is #false, so only the table's current definition decodes \
                 rows; restart from a position after the change"
            .to_string(),
    }
}

/// The most columns a table has, which bounds what a table map is believed to
/// name.
const MAX_TABLE_COLUMNS: usize = 4096;

/// What a row event's table map says about the table, kept for the reload its
/// disagreement with the definition in use asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Shape {
    columns: usize,
    wires: Vec<Option<Wire>>,
}

impl Shape {
    fn of(tme: &TableMapEvent<'_>) -> Self {
        let columns = usize::try_from(tme.columns_count()).unwrap_or(usize::MAX);
        let wires = (0..columns.min(MAX_TABLE_COLUMNS))
            .map(|ordinal| {
                let ty = tme.get_column_type(ordinal).ok().flatten()?;
                Some(Wire::of(ty, tme.get_column_metadata(ordinal)))
            })
            .collect();
        Self { columns, wires }
    }

    fn wire(&self, ordinal: usize) -> Option<Wire> {
        self.wires.get(ordinal).copied().flatten()
    }
}

/// A definition recorded for a position the stream has not reached yet.
#[derive(Debug)]
struct Step {
    at: Position,
    /// The plan, or why the definition cannot decode the declared fields.
    plan: Result<Arc<Plan>, String>,
}

/// The definitions one stream decodes with, in the order it reaches them.
#[derive(Debug, Default)]
struct Timeline {
    /// What the rows read now are decoded with.
    active: Arc<Plan>,
    /// The recorded definitions of positions the stream has not reached,
    /// oldest first. While there are any, the history is authoritative.
    ahead: VecDeque<Step>,
}

/// The kind of value a row image carries for a column: what the table map says
/// and what the catalog expects of the declared field's column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wire {
    Int(u8),
    Float,
    /// `(precision, scale)` when known.
    Decimal(Option<(u8, u8)>),
    Date,
    /// The temporal kinds carry their fractional-second digits when known.
    DateTime(Option<u8>),
    Timestamp(Option<u8>),
    Time(Option<u8>),
    Year,
    Bit,
    /// Text, bytes, enum, set, JSON, geometry: carried as bytes or an index
    /// that the catalog's labels resolve.
    Other,
}

impl Wire {
    /// What the table map's `ty` and `metadata` say.
    fn of(ty: ColumnType, metadata: Option<&[u8]>) -> Self {
        use ColumnType::*;
        match ty {
            MYSQL_TYPE_TINY => Wire::Int(8),
            MYSQL_TYPE_SHORT => Wire::Int(16),
            MYSQL_TYPE_INT24 => Wire::Int(24),
            MYSQL_TYPE_LONG => Wire::Int(32),
            MYSQL_TYPE_LONGLONG => Wire::Int(64),
            MYSQL_TYPE_FLOAT | MYSQL_TYPE_DOUBLE => Wire::Float,
            MYSQL_TYPE_DECIMAL | MYSQL_TYPE_NEWDECIMAL => Wire::Decimal(match metadata {
                Some(&[precision, scale]) => Some((precision, scale)),
                _ => None,
            }),
            MYSQL_TYPE_DATE | MYSQL_TYPE_NEWDATE => Wire::Date,
            MYSQL_TYPE_DATETIME2 => Wire::DateTime(fsp(metadata)),
            MYSQL_TYPE_TIMESTAMP2 => Wire::Timestamp(fsp(metadata)),
            MYSQL_TYPE_TIME2 => Wire::Time(fsp(metadata)),
            MYSQL_TYPE_DATETIME => Wire::DateTime(None),
            MYSQL_TYPE_TIMESTAMP => Wire::Timestamp(None),
            MYSQL_TYPE_TIME => Wire::Time(None),
            MYSQL_TYPE_YEAR => Wire::Year,
            MYSQL_TYPE_BIT => Wire::Bit,
            _ => Wire::Other,
        }
    }

    /// What the catalog expects of a declared column of `column`, with its
    /// decimal precision and scale from the catalog row.
    fn expected(column: &ResolvedColumn, catalog: Option<&CatalogColumn>) -> Self {
        match column.family {
            Family::Integer(bits) => Wire::Int(bits),
            Family::Float => Wire::Float,
            Family::Decimal => Wire::Decimal(catalog.and_then(|c| {
                Some((
                    u8::try_from(c.numeric_precision?).ok()?,
                    u8::try_from(c.numeric_scale?).ok()?,
                ))
            })),
            Family::Date => Wire::Date,
            Family::DateTime => Wire::DateTime(digits(catalog)),
            Family::Timestamp => Wire::Timestamp(digits(catalog)),
            Family::Time => Wire::Time(digits(catalog)),
            Family::Year => Wire::Year,
            Family::Bit => Wire::Bit,
            _ => Wire::Other,
        }
    }

    /// Whether a column the catalog expects as `self` may arrive as `actual`.
    fn accepts(self, actual: Wire) -> bool {
        match (self, actual) {
            (Wire::Decimal(Some(a)), Wire::Decimal(Some(b))) => a == b,
            (Wire::Decimal(_), Wire::Decimal(_)) => true,
            (Wire::DateTime(a), Wire::DateTime(b))
            | (Wire::Timestamp(a), Wire::Timestamp(b))
            | (Wire::Time(a), Wire::Time(b)) => a.is_none() || b.is_none() || a == b,
            _ => self == actual,
        }
    }

    fn describe(self) -> String {
        match self {
            Wire::Int(bits) => format!("a {bits}-bit integer"),
            Wire::Float => "a float".to_string(),
            Wire::Decimal(Some((precision, scale))) => {
                format!("a decimal({precision},{scale})")
            }
            Wire::Decimal(None) => "a decimal".to_string(),
            Wire::Date => "a date".to_string(),
            Wire::DateTime(digits) => temporal("a datetime", digits),
            Wire::Timestamp(digits) => temporal("a timestamp", digits),
            Wire::Time(digits) => temporal("a time", digits),
            Wire::Year => "a year".to_string(),
            Wire::Bit => "a bit".to_string(),
            Wire::Other => "text, bytes or an enumeration".to_string(),
        }
    }
}
/// The fractional-second digits a `*2` temporal column's table map metadata
/// names: one byte.
fn fsp(metadata: Option<&[u8]>) -> Option<u8> {
    match metadata {
        Some(&[digits]) => Some(digits),
        _ => None,
    }
}

/// The fractional-second digits the catalog row names.
fn digits(catalog: Option<&CatalogColumn>) -> Option<u8> {
    catalog.and_then(|c| u8::try_from(c.datetime_precision?).ok())
}

fn temporal(kind: &str, digits: Option<u8>) -> String {
    match digits {
        Some(digits) => format!("{kind}({digits})"),
        None => kind.to_string(),
    }
}

/// Resolve the declared fields against one definition of the table.
///
/// With [`Missing::Null`] a declared column the definition lacks reads NULL
/// for the rows it describes, unless its field is declared `nullable=#false`.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when a declared field is absent from
/// the definition under [`Missing::Refuse`], is declared not nullable and
/// absent under [`Missing::Null`], or has a type the column cannot fill.
fn build_plan(
    table: &TableRef,
    declared: &[FieldSpec],
    definition: &Arc<Definition>,
    missing: Missing,
) -> Result<Plan, SaciError> {
    let lenient = missing == Missing::Null;
    if lenient
        && let Some(spec) = declared
            .iter()
            .find(|spec| !spec.is_reserved() && !spec.nullable && !definition.has(&spec.name))
    {
        return Err(SaciError::configuration(format!(
            "{WHAT}: column '{}' is declared nullable=#false, but a definition of '{table}' in \
             the schema history has no such column, so the rows it describes would hold NULL \
             there; declare the field nullable",
            spec.name
        )));
    }
    let catalog = &definition.columns;
    let resolved = validate_columns(
        WHAT,
        Access::Binlog,
        declared
            .iter()
            .filter(|spec| !spec.is_reserved() && (!lenient || definition.has(&spec.name))),
        catalog,
    )?;
    let mut data = resolved.into_iter();
    let mut fields = Vec::with_capacity(declared.len());
    let mut select = 0;
    for spec in declared {
        match meta_of(&spec.name) {
            Some(meta) if spec.is_reserved() => fields.push(FieldPlan {
                column: ResolvedColumn::synthetic(spec),
                slot: Slot::Meta(meta),
            }),
            _ if lenient && !definition.has(&spec.name) => fields.push(FieldPlan {
                column: ResolvedColumn::synthetic(spec),
                slot: Slot::Absent,
            }),
            _ => {
                let column = data.next().ok_or_else(|| {
                    SaciError::generic(format!(
                        "{WHAT}: the catalog of '{table}' resolved fewer columns than were declared"
                    ))
                })?;
                fields.push(FieldPlan {
                    column,
                    slot: Slot::Data(select),
                });
                select += 1;
            }
        }
    }
    let pk_ordinals = definition
        .primary_key
        .iter()
        .filter_map(|name| {
            catalog
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(name))
                .map(|column| column.ordinal)
        })
        .collect();
    let binary_pads = catalog
        .iter()
        .filter(|column| column.data_type == "binary")
        .filter_map(|column| {
            let width = column
                .column_type
                .split_once('(')?
                .1
                .split_once(')')?
                .0
                .parse()
                .ok()?;
            Some((column.ordinal, width))
        })
        .collect();
    let wire = fields
        .iter()
        .filter(|field| matches!(field.slot, Slot::Data(_)))
        .map(|field| {
            let ordinal = field.column.ordinal;
            let row = catalog.iter().find(|c| c.ordinal == ordinal);
            (
                ordinal,
                field.column.spec.name.clone(),
                Wire::expected(&field.column, row),
            )
        })
        .collect();
    Ok(Plan {
        definition: Arc::clone(definition),
        wire,
        ncols: catalog.len(),
        binary_pads,
        pk_ordinals,
        pk_names: definition.primary_key.clone(),
        fields,
        table_text: table.to_string(),
    })
}

/// The table with the schema and name the server stores, which is what the
/// binlog's table maps carry.
async fn exact_table(conn: &mut Conn, table: &TableRef) -> Result<TableRef, SaciError> {
    let row: Option<(String, String)> = conn
        .exec_first(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
            (table.schema.as_str(), table.name.as_str()),
        )
        .await
        .map_err(|e| {
            SaciError::generic(format!(
                "{WHAT}: cannot look up table '{table}': {}",
                mysql_detail(&e)
            ))
        })?;
    Ok(match row {
        Some((schema, name)) => TableRef { schema, name },
        // `columns_of` reports the missing table.
        None => table.clone(),
    })
}

// ----------------------------------------------------------------- the server

/// The server's current binlog position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Status {
    pub(super) file: String,
    pub(super) pos: u64,
    /// MySQL's executed GTID set, from the same row.
    pub(super) gtid: Option<GtidSet>,
}

impl Status {
    /// The position a source in `mode` resumes from.
    pub(super) fn position(&self, mode: PositionMode) -> Result<Position, SaciError> {
        match mode {
            PositionMode::File => Ok(Position::File {
                name: self.file.clone(),
                pos: self.pos,
            }),
            PositionMode::Gtid => self.gtid.clone().map(Position::Gtid).ok_or_else(|| {
                SaciError::configuration(format!(
                    "{WHAT}: position \"gtid\" streams MySQL GTIDs only; use position \"file\" \
                     on MariaDB"
                ))
            }),
        }
    }
}

/// The statement that reports the position.
fn status_sql(flavor: Flavor) -> &'static str {
    match flavor {
        Flavor::Mysql => "SHOW BINARY LOG STATUS",
        Flavor::Mariadb => "SHOW BINLOG STATUS",
    }
}

/// The grant hint for a privilege error, empty for any other.
pub(super) fn grant_hint(flavor: Flavor, code: Option<u16>) -> &'static str {
    if !matches!(
        code,
        Some(ER_SPECIFIC_ACCESS_DENIED_ERROR | ER_ACCESS_DENIED_ERROR)
    ) {
        return "";
    }
    match flavor {
        Flavor::Mysql => "; grant REPLICATION SLAVE and REPLICATION CLIENT",
        Flavor::Mariadb => "; grant REPLICATION SLAVE and BINLOG MONITOR",
    }
}

fn column<T: mysql_async::prelude::FromValue>(row: &Row, index: usize) -> Result<T, String> {
    row.get_opt(index)
        .ok_or_else(|| format!("column {index} is missing"))?
        .map_err(|_| format!("column {index} has an unexpected type"))
}

/// Read the position from the status statement.
pub(super) async fn read_status(
    conn: &mut Conn,
    flavor: Flavor,
    what: &str,
    target: &str,
) -> Result<Status, SaciError> {
    let sql = status_sql(flavor);
    let row: Option<Row> = conn.query_first(sql).await.map_err(|e| {
        SaciError::generic(format!(
            "{what}: '{sql}' failed on {target}: {}{}",
            mysql_detail(&e),
            grant_hint(flavor, server_code(&e))
        ))
    })?;
    let row = row.ok_or_else(|| {
        SaciError::configuration(format!(
            "{what}: '{sql}' returned no row on {target}; binary logging is off"
        ))
    })?;
    let bad = |reason: String| {
        SaciError::generic(format!("{what}: cannot read '{sql}' on {target}: {reason}"))
    };
    let file: String = column(&row, 0).map_err(bad)?;
    let pos: u64 = column(&row, 1).map_err(bad)?;
    let gtid = if flavor == Flavor::Mysql && row.len() > 4 {
        let text: String = column(&row, 4).map_err(bad)?;
        Some(text.parse::<GtidSet>().map_err(|e| {
            SaciError::generic(format!("{what}: cannot read the executed GTID set: {e}"))
        })?)
    } else {
        None
    };
    Ok(Status { file, pos, gtid })
}

/// Refuse a server that cannot serve this source.
async fn preflight(
    session: &mut Session,
    mode: PositionMode,
    target: &str,
) -> Result<(), SaciError> {
    let supported = match session.flavor {
        Flavor::Mysql => session.version >= (8, 2, 0),
        Flavor::Mariadb => session.version >= (10, 5, 2),
    };
    if !supported {
        return Err(SaciError::configuration(format!(
            "{WHAT}: cdc_binlog needs MySQL 8.2 or later, or MariaDB 10.5.2 or later; the server \
             reports {}",
            session.describe()
        )));
    }
    let fail = |what: &str, e: &mysql_async::Error| {
        SaciError::generic(format!(
            "{WHAT}: cannot read {what} on {target}: {}",
            mysql_detail(e)
        ))
    };
    let (log_bin, format, image): (String, String, String) = session
        .conn
        .query_first("SELECT @@global.log_bin, @@global.binlog_format, @@global.binlog_row_image")
        .await
        .map_err(|e| fail("the binlog settings", &e))?
        .ok_or_else(|| {
            SaciError::generic(format!("{WHAT}: the binlog settings query returned no row"))
        })?;
    if log_bin != "1" {
        return Err(SaciError::configuration(format!(
            "{WHAT}: cdc_binlog needs binary logging on {target}; start the server with log_bin \
             enabled (it reports log_bin = {log_bin})"
        )));
    }
    if !format.eq_ignore_ascii_case("ROW") {
        return Err(SaciError::configuration(format!(
            "{WHAT}: cdc_binlog needs binlog_format = ROW on {target}; the server reports \
             '{format}'"
        )));
    }
    if !image.eq_ignore_ascii_case("FULL") {
        return Err(SaciError::configuration(format!(
            "{WHAT}: cdc_binlog needs binlog_row_image = FULL on {target}; the server reports \
             '{image}'"
        )));
    }
    if session.flavor == Flavor::Mysql {
        let options: Option<String> = session
            .conn
            .query_first("SELECT @@global.binlog_row_value_options")
            .await
            .map_err(|e| fail("binlog_row_value_options", &e))?;
        let options = options.unwrap_or_default();
        if !options.is_empty() {
            return Err(SaciError::configuration(format!(
                "{WHAT}: cdc_binlog needs binlog_row_value_options = '' on {target}, because a \
                 partial JSON update is not a row image; the server reports '{options}'"
            )));
        }
    }
    if session.flavor == Flavor::Mariadb {
        let compress: Option<String> = session
            .conn
            .query_first("SELECT @@global.log_bin_compress")
            .await
            .map_err(|e| fail("log_bin_compress", &e))?;
        let compress = compress.unwrap_or_default();
        if matches!(compress.to_ascii_uppercase().as_str(), "1" | "ON") {
            return Err(SaciError::configuration(format!(
                "{WHAT}: cdc_binlog needs log_bin_compress = OFF on {target}, because the binlog \
                 then carries compressed row events this reader cannot decode"
            )));
        }
    }
    if mode == PositionMode::Gtid {
        match session.flavor {
            Flavor::Mariadb => {
                return Err(SaciError::configuration(format!(
                    "{WHAT}: position \"gtid\" streams MySQL GTIDs only; use position \"file\" \
                     on MariaDB"
                )));
            }
            Flavor::Mysql => {
                let gtid_mode: Option<String> = session
                    .conn
                    .query_first("SELECT @@global.gtid_mode")
                    .await
                    .map_err(|e| fail("gtid_mode", &e))?;
                let gtid_mode = gtid_mode.unwrap_or_default();
                if !gtid_mode.eq_ignore_ascii_case("ON") {
                    return Err(SaciError::configuration(format!(
                        "{WHAT}: position \"gtid\" needs gtid_mode = ON on {target}; the server \
                         reports '{gtid_mode}'"
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Resolve `mode.initial` when the offset table holds nothing.
async fn resolve_initial(
    conn: &mut Conn,
    flavor: Flavor,
    mode: PositionMode,
    initial: &str,
    target: &str,
) -> Result<Position, SaciError> {
    match initial {
        "now" => read_status(conn, flavor, WHAT, target)
            .await?
            .position(mode),
        "beginning" => match mode {
            PositionMode::File => {
                let row: Option<Row> = conn.query_first("SHOW BINARY LOGS").await.map_err(|e| {
                    SaciError::generic(format!(
                        "{WHAT}: 'SHOW BINARY LOGS' failed on {target}: {}{}",
                        mysql_detail(&e),
                        grant_hint(flavor, server_code(&e))
                    ))
                })?;
                let row = row.ok_or_else(|| {
                    SaciError::configuration(format!(
                        "{WHAT}: {target} lists no binary logs; binary logging is off"
                    ))
                })?;
                let name: String = column(&row, 0).map_err(|e| {
                    SaciError::generic(format!("{WHAT}: cannot read 'SHOW BINARY LOGS': {e}"))
                })?;
                Ok(Position::File { name, pos: 4 })
            }
            PositionMode::Gtid => {
                let purged: Option<String> = conn
                    .query_first("SELECT @@global.gtid_purged")
                    .await
                    .map_err(|e| {
                        SaciError::generic(format!(
                            "{WHAT}: cannot read gtid_purged on {target}: {}",
                            mysql_detail(&e)
                        ))
                    })?;
                purged
                    .unwrap_or_default()
                    .parse()
                    .map(Position::Gtid)
                    .map_err(|e| {
                        SaciError::generic(format!("{WHAT}: cannot read gtid_purged: {e}"))
                    })
            }
        },
        literal => {
            let text = literal.strip_prefix("gtid:").unwrap_or(literal);
            let offset = match mode {
                PositionMode::File => BinlogOffset::parse(literal),
                PositionMode::Gtid => Ok(BinlogOffset::Gtid(text.to_string())),
            }
            .map_err(|e| {
                SaciError::configuration(format!("{WHAT}: mode.initial '{literal}': {e}"))
            })?;
            Position::from_offset(offset, mode).map_err(|e| {
                SaciError::configuration(format!("{WHAT}: mode.initial '{literal}': {e}"))
            })
        }
    }
}

// ------------------------------------------------------------- batch building

/// The metadata of one emitted row.
struct MetaRow<'a> {
    op: &'a [u8],
    file: Option<&'a str>,
    pos: u64,
    gtid: Option<&'a str>,
    micros: i64,
}

fn meta_cell<'a>(meta: Meta, table: &'a str, row: &MetaRow<'a>) -> Cell<'a> {
    match meta {
        Meta::Op => Cell::Text(row.op),
        Meta::Table => Cell::Text(table.as_bytes()),
        Meta::File => row
            .file
            .map_or(Cell::Null, |file| Cell::Text(file.as_bytes())),
        Meta::Pos => Cell::UInt(row.pos),
        Meta::Gtid => row
            .gtid
            .map_or(Cell::Null, |gtid| Cell::Text(gtid.as_bytes())),
        Meta::CommitTs => Cell::EpochMicros(row.micros),
    }
}

/// Build one batch from `items`, asking `cell` for each declared field.
///
/// `context` is whatever `cell` needs besides the item and the field; passing
/// it as an argument keeps the borrow the returned cell holds on it simple.
fn assemble<C, T>(
    plan: &Plan,
    schema: &Arc<Schema>,
    items: &[T],
    context: &C,
    cell: impl for<'a> Fn(&'a C, &'a T, &'a FieldPlan) -> Result<Cell<'a>, SaciError>,
) -> Result<RecordBatch, SaciError> {
    let mut builders = plan
        .fields
        .iter()
        .map(|field| ColumnBuilder::new(&field.column, items.len()))
        .collect::<Result<Vec<_>, _>>()?;
    for item in items {
        for (builder, field) in builders.iter_mut().zip(&plan.fields) {
            builder.push(cell(context, item, field)?)?;
        }
    }
    let columns = builders.iter_mut().map(ColumnBuilder::finish).collect();
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| {
        SaciError::generic(format!(
            "{WHAT}: cannot assemble a batch of '{}': {e}",
            plan.table_text
        ))
    })
}

/// One cell of a binlog row.
fn binlog_cell<'a>(
    plan: &'a Plan,
    change: &'a Change,
    field: &'a FieldPlan,
) -> Result<Cell<'a>, SaciError> {
    match field.slot {
        Slot::Data(_) => {
            if change.image.len() != plan.ncols {
                return Err(SaciError::generic(format!(
                    "{WHAT}: a binlog row of '{}' has {} columns but the catalog has {}; \
                     binlog_row_image must be FULL",
                    plan.table_text,
                    change.image.len(),
                    plan.ncols
                )));
            }
            let value = change.image.as_ref(field.column.ordinal).ok_or_else(|| {
                SaciError::generic(format!(
                    "{WHAT}: a binlog row of '{}' has no value for column '{}'",
                    plan.table_text, field.column.spec.name
                ))
            })?;
            Cell::from_binlog(value, &field.column)
        }
        Slot::Absent => Ok(Cell::Null),
        Slot::Meta(meta) => Ok(meta_cell(
            meta,
            &plan.table_text,
            &MetaRow {
                op: std::slice::from_ref(&change.op),
                file: change.file.as_deref(),
                pos: change.pos,
                gtid: change.gtid.as_deref(),
                micros: change.commit_micros,
            },
        )),
    }
}

/// What a snapshot row's cells need besides the row.
struct SnapshotContext<'p> {
    plan: &'p Plan,
    file: String,
    pos: u64,
    micros: i64,
}

/// One cell of a snapshot row.
fn snapshot_cell<'a>(
    context: &'a SnapshotContext<'_>,
    row: &'a Row,
    field: &'a FieldPlan,
) -> Result<Cell<'a>, SaciError> {
    match field.slot {
        Slot::Data(index) => {
            let value = row.as_ref(index).ok_or_else(|| {
                SaciError::generic(format!(
                    "{WHAT}: a snapshot row of '{}' has no value for column '{}'",
                    context.plan.table_text, field.column.spec.name
                ))
            })?;
            Cell::from_protocol(value, &field.column)
        }
        Slot::Absent => Ok(Cell::Null),
        Slot::Meta(meta) => Ok(meta_cell(
            meta,
            &context.plan.table_text,
            &MetaRow {
                op: b"R",
                file: Some(&context.file),
                pos: context.pos,
                gtid: None,
                micros: context.micros,
            },
        )),
    }
}

// -------------------------------------------------------------------- reader

/// Where the source is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// The offset table has not been read yet.
    Unresolved,
    /// Reading the table's existing rows.
    Snapshot,
    /// Reading the binlog.
    Streaming,
}

/// Reader over the row binlog of one table.
pub(crate) struct BinlogReader {
    connector: Connector,
    offsets: OffsetStore,
    table: TableRef,
    offset_table: TableRef,
    server_id: u32,
    schema: Arc<Schema>,
    fields: Vec<FieldSpec>,
    position_mode: PositionMode,
    initial: String,
    snapshot_initial: bool,
    batch_rows: usize,
    max_batches_per_cycle: usize,
    max_changes_per_cycle: usize,
    instruments: Instruments,
    /// The recorded definitions of the table, oldest tag first.
    history: History,
    /// Where they are kept, when `mode.schema_history` is on.
    history_store: Option<HistoryStore>,
    /// What an error about a definition tells the operator to do next.
    remedy: String,

    /// The session for the catalog, offsets and position statements.
    control: Option<Session>,
    /// Whether the preflight, catalog and offset table are done on `control`.
    session_ready: bool,
    flavor: Flavor,
    /// The plan of the definition last put in use, so a stream that starts
    /// again under the same definition does not resolve it again.
    plans: Option<(Arc<Definition>, Arc<Plan>)>,
    /// The plan the snapshot decodes with: the current catalog, strictly.
    snapshot_plan: Option<Arc<Plan>>,
    /// Whether the offset table has been read this process.
    loaded: bool,
    phase: Phase,
    snapshot: Option<Snapshot>,
    /// The newest position whose rows have all been handed out. `None` until
    /// a snapshot completes.
    committed: Option<Position>,
    /// Whether `committed` has moved since it was stored.
    stored_dirty: bool,

    queue: VecDeque<Change>,
    stream: Option<BinlogStream>,
    /// An event pulled from the stream but not handled yet: a row event that
    /// needs the table's definition read first.
    held: Option<Event>,
    cy: Cycle,
    stream_done: bool,
    cycle_start: bool,
    batches_this_cycle: usize,
    /// The last batch handed out ended inside a transaction, whose remaining
    /// rows are the head of `queue`.
    mid_tx: bool,
    /// A `next_batch` call is running, or was dropped before it returned.
    in_flight: bool,
}

impl BinlogReader {
    /// Build the reader from a validated config. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the DSN, the table or the
    /// offset table cannot be resolved, or when the config is not a
    /// `cdc_binlog` one.
    pub(crate) fn new(cfg: &MariadbSourceConfig, schema: Arc<Schema>) -> Result<Self, SaciError> {
        let SourceMode::CdcBinlog(mode) = &cfg.mode else {
            return Err(SaciError::configuration(
                "MariadbSource: BinlogReader serves mode kind \"cdc_binlog\" only",
            ));
        };
        let connector = Connector::new(Role::Source, &cfg.connection)?;
        let table = connector.resolve_table("mode.table", &mode.table)?;
        let offset_table = connector.resolve_table("mode.offset_table", &mode.offset_table)?;
        let history_store = if mode.schema_history {
            let history_table = connector.resolve_table(
                "mode.schema_history_table",
                &mode.effective_schema_history_table(),
            )?;
            Some(HistoryStore::new(
                Role::Source.what(),
                history_table,
                &cfg.name,
                &table,
                mode.offset_table_autocreate,
            ))
        } else {
            None
        };
        let remedy = remedy_text(history_store.as_ref());
        Ok(Self {
            offset_table: offset_table.clone(),
            offsets: OffsetStore::new(
                Role::Source.what(),
                offset_table,
                &cfg.name,
                mode.offset_table_autocreate,
            ),
            connector,
            table,
            server_id: mode.effective_server_id(&cfg.name),
            schema,
            fields: cfg.schema_fields.clone(),
            position_mode: mode.position,
            initial: mode.initial.clone(),
            snapshot_initial: mode.snapshot == SnapshotMode::Initial,
            batch_rows: cfg.batch_rows,
            max_batches_per_cycle: cfg.max_batches_per_cycle,
            max_changes_per_cycle: mode.max_changes_per_cycle,
            instruments: Instruments::source(&cfg.name, cfg.mode.label()),
            history: History::default(),
            history_store,
            remedy,
            control: None,
            session_ready: false,
            flavor: Flavor::Mysql,
            plans: None,
            snapshot_plan: None,
            loaded: false,
            phase: Phase::Unresolved,
            snapshot: None,
            committed: None,
            stored_dirty: false,
            queue: VecDeque::new(),
            stream: None,
            held: None,
            cy: Cycle::default(),
            stream_done: false,
            cycle_start: false,
            batches_this_cycle: 0,
            mid_tx: false,
            in_flight: false,
        })
    }

    /// Set the rows per emitted batch. [`MariadbSource`](super::MariadbSource)
    /// clamps the hint to at least 1 before it gets here.
    pub(crate) fn set_batch_rows(&mut self, rows: usize) {
        self.batch_rows = rows;
    }

    /// The rows per emitted batch, for the tests that observe
    /// `Source::request_batch_rows` through `MariadbSource`.
    #[cfg(test)]
    pub(crate) fn batch_rows(&self) -> usize {
        self.batch_rows
    }

    /// Pull the next batch, or `Ok(None)` at the end of a drain cycle.
    ///
    /// A failure abandons the cycle's stream and queue, so the next call reads
    /// again from the committed position. So does a call the host dropped
    /// before it returned (the stream runner does on cancellation): a
    /// connection it left mid-statement answers the next statement with the
    /// reply to the one before, so the control session is replaced and a
    /// snapshot, whose read view is tied to its connection, starts over.
    pub(crate) async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        if std::mem::replace(&mut self.in_flight, true) {
            self.control = None;
            self.session_ready = false;
            self.snapshot = None;
            self.abandon_cycle().await;
        }
        let result = match self.step().await {
            Ok(batch) => Ok(batch),
            Err(e) => {
                self.abandon_cycle().await;
                Err(e)
            }
        };
        self.in_flight = false;
        result
    }

    async fn step(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.ensure_session().await?;

        if self.cycle_start {
            if let Err(e) = self.store_committed().await {
                self.control = None;
                self.session_ready = false;
                return Err(e);
            }
            self.cycle_start = false;
            self.batches_this_cycle = 0;
            self.stream_done = false;
        }

        // A cycle ends on a transaction boundary: the rest of an open
        // transaction goes out first, or a cap smaller than one transaction
        // would re-read its first batches every cycle and never get past them.
        let capped =
            self.max_batches_per_cycle > 0 && self.batches_this_cycle >= self.max_batches_per_cycle;
        if capped && self.mid_tx {
            if let Some(batch) = self.take_queued(true)? {
                return Ok(Some(batch));
            }
            self.mid_tx = false;
        }
        if capped {
            // Whatever is queued or still unread is behind `committed` and is
            // read again next cycle from a fresh stream.
            self.abandon_cycle().await;
            self.cycle_start = true;
            return Ok(None);
        }

        if self.phase == Phase::Snapshot
            && let Some(batch) = self.snapshot_page().await?
        {
            self.batches_this_cycle += 1;
            self.instruments.batch(batch.num_rows() as u64);
            return Ok(Some(batch));
        }

        loop {
            let queued = self.queue.len();
            if (queued >= self.batch_rows.max(1) || (queued > 0 && self.stream_done))
                && let Some(batch) = self.take_queued(false)?
            {
                return Ok(Some(batch));
            }
            if self.stream_done {
                self.cycle_start = true;
                return Ok(None);
            }
            let started = Instant::now();
            self.read_more().await?;
            self.instruments.observe(started.elapsed().as_secs_f64());
        }
    }

    // ------------------------------------------------------------- session

    /// Connect, check the server and the table, and load the position, as far
    /// as this session has not already.
    async fn ensure_session(&mut self) -> Result<(), SaciError> {
        let result = self.setup().await;
        if result.is_err() {
            self.control = None;
            self.session_ready = false;
        }
        result
    }

    async fn setup(&mut self) -> Result<(), SaciError> {
        if self.control.is_none() {
            self.session_ready = false;
            let session = match self.connector.connect_with_retry().await {
                Ok(session) => session,
                Err(e) => {
                    self.instruments.error("connect");
                    return Err(e);
                }
            };
            self.flavor = session.flavor;
            self.control = Some(session);
            self.offsets.reset();
            if let Some(store) = &mut self.history_store {
                store.reset();
            }
        }
        if !self.session_ready {
            self.prepare().await?;
            self.session_ready = true;
        }
        if !self.loaded {
            self.load_position().await?;
            self.loaded = true;
        }
        Ok(())
    }

    /// Preflight, resolve the table, create the offset and history tables.
    async fn prepare(&mut self) -> Result<(), SaciError> {
        let target = self.connector.target().to_string();
        let session = self.control.as_mut().expect("a control session");
        preflight(session, self.position_mode, &target).await?;
        self.table = exact_table(&mut session.conn, &self.table).await?;
        self.offsets
            .ensure(&mut session.conn)
            .await
            .inspect_err(|_| self.instruments.error("offset"))?;
        if let Some(store) = &mut self.history_store {
            store.watch(&self.table);
            store
                .ensure(&mut session.conn)
                .await
                .inspect_err(|_| self.instruments.error("schema"))?;
        }
        Ok(())
    }

    /// Read the catalog again, after a DDL or a row event the definition in use
    /// does not fit.
    ///
    /// A definition that differs is checked against the row event that asked
    /// for it, recorded in the history tagged with the position of the last DDL
    /// read (else the stream's tail), and put in use. One that does not differ
    /// records nothing, and the event is handled again under the definition in
    /// use, which is where a disagreement becomes an error.
    async fn refresh_plan(&mut self) -> Result<(), SaciError> {
        let target = self.connector.target().to_string();
        let session = self.control.as_mut().expect("a control session");
        self.table = exact_table(&mut session.conn, &self.table).await?;
        let definition = Definition::read(&mut session.conn, WHAT, &self.table, &target)
            .await
            .inspect_err(|_| self.instruments.error("query"))?;
        let tag = self.cy.ddl_at.take().or_else(|| self.cy.tail_position());
        let shape = self.cy.shape.take();
        self.cy.ddl_seen = false;
        if definition == *self.cy.timeline.active.definition {
            return Ok(());
        }
        let definition = Arc::new(definition);
        let plan = Arc::new(
            build_plan(&self.table, &self.fields, &definition, Missing::Null)
                .inspect_err(|_| self.instruments.error("query"))?,
        );
        if let Some(shape) = shape
            && let Some(disagreement) = plan.disagrees(shape.columns, |ordinal| shape.wire(ordinal))
        {
            return Err(disagreement.error(&self.table, &plan, &self.remedy));
        }
        let Some(at) = tag else {
            return Err(SaciError::generic(format!(
                "{WHAT}: the binlog stream has no position to record the definition of '{}' at",
                self.table
            )));
        };
        self.record(Entry {
            id: None,
            at,
            definition: Arc::clone(&definition),
        })
        .await?;
        self.plans = Some((definition, Arc::clone(&plan)));
        self.cy.timeline.active = plan;
        Ok(())
    }

    /// Decide where to start: the stored offset, else a snapshot, else
    /// `mode.initial`, and make sure a definition of the table is in force
    /// there.
    async fn load_position(&mut self) -> Result<(), SaciError> {
        self.load_history().await?;
        let target = self.connector.target().to_string();
        let session = self.control.as_mut().expect("a control session");
        let stored = self
            .offsets
            .load_binlog(&mut session.conn)
            .await
            .inspect_err(|_| self.instruments.error("offset"))?;
        match stored {
            Some(offset) => {
                let position = Position::from_offset(offset, self.position_mode).map_err(|e| {
                    SaciError::configuration(format!(
                        "{WHAT}: the stored offset in '{}' is not usable: {e}; delete the \
                         source's row or change mode.position",
                        self.offset_table
                    ))
                })?;
                #[cfg(feature = "tracing")]
                tracing::info!(table = %self.table, position = %position.to_offset(), "binlog source resumed");
                self.committed = Some(position.clone());
                self.phase = Phase::Streaming;
                self.establish(&position, false).await?;
            }
            None if self.snapshot_initial => {
                self.phase = Phase::Snapshot;
            }
            None => {
                let position = resolve_initial(
                    &mut session.conn,
                    session.flavor,
                    self.position_mode,
                    &self.initial,
                    &target,
                )
                .await
                .inspect_err(|_| self.instruments.error("query"))?;
                self.committed = Some(position.clone());
                self.phase = Phase::Streaming;
                // The definition goes in before the position that depends on
                // it, and both at once, not one cycle late: `now` resolved
                // again after a restart would skip what happened in between.
                self.establish(&position, true).await?;
                self.stored_dirty = true;
                self.store_committed().await?;
            }
        }
        Ok(())
    }

    /// Read this source's recorded definitions of the table.
    async fn load_history(&mut self) -> Result<(), SaciError> {
        let Some(store) = &self.history_store else {
            return Ok(());
        };
        let session = self.control.as_mut().expect("a control session");
        let entries = store
            .load(&mut session.conn, self.position_mode)
            .await
            .inspect_err(|_| self.instruments.error("schema"))?;
        self.history = History::new(entries);
        Ok(())
    }

    /// Make sure a definition is in force at `position`, where the stream
    /// starts.
    ///
    /// One in the history is used as recorded, after checking that the declared
    /// fields fit it and that none names a column neither it nor the catalog
    /// has. Without one the catalog is taken to describe the table there, is
    /// checked as strictly as on any first connection, and is recorded.
    ///
    /// A `fresh` start has no stored position behind it, so what is recorded
    /// for it may predate a change made since: only a definition equal to the
    /// catalog's is used as it is, and any other is outvoted by a new entry at
    /// `position`.
    async fn establish(&mut self, position: &Position, fresh: bool) -> Result<(), SaciError> {
        let target = self.connector.target().to_string();
        let session = self.control.as_mut().expect("a control session");
        let current = Arc::new(
            Definition::read(&mut session.conn, WHAT, &self.table, &target)
                .await
                .inspect_err(|_| self.instruments.error("query"))?,
        );
        let in_force = self
            .history
            .in_force(position)
            .map(|entry| Arc::clone(&entry.definition))
            .filter(|definition| !fresh || **definition == *current);
        let Some(definition) = in_force else {
            build_plan(&self.table, &self.fields, &current, Missing::Refuse)
                .inspect_err(|_| self.instruments.error("query"))?;
            return self
                .record(Entry {
                    id: None,
                    at: position.clone(),
                    definition: current,
                })
                .await;
        };
        self.plan_for(&definition)?;
        let unknown: Vec<&FieldSpec> = self
            .fields
            .iter()
            .filter(|spec| {
                !spec.is_reserved() && !definition.has(&spec.name) && !current.has(&spec.name)
            })
            .collect();
        if unknown.is_empty() {
            return Ok(());
        }
        validate_columns(WHAT, Access::Binlog, unknown, &current.columns).map(drop)
    }

    /// Add a definition to the history, and to the history table when the
    /// source keeps one.
    async fn record(&mut self, mut entry: Entry) -> Result<(), SaciError> {
        if let Some(store) = &self.history_store {
            let session = self.control.as_mut().expect("a control session");
            let id = store
                .insert(&mut session.conn, &entry.at, &entry.definition)
                .await
                .inspect_err(|_| self.instruments.error("schema"))?;
            entry.id = Some(id);
        }
        #[cfg(feature = "tracing")]
        tracing::info!(table = %self.table, at = %entry.at.to_offset(), columns = entry.definition.columns.len(), "table definition recorded");
        self.history.push(entry);
        Ok(())
    }

    /// The plan that decodes rows under `definition`, resolved once.
    fn plan_for(&mut self, definition: &Arc<Definition>) -> Result<Arc<Plan>, SaciError> {
        if let Some((held, plan)) = &self.plans
            && Arc::ptr_eq(held, definition)
        {
            return Ok(Arc::clone(plan));
        }
        let plan = Arc::new(build_plan(
            &self.table,
            &self.fields,
            definition,
            Missing::Null,
        )?);
        self.plans = Some((Arc::clone(definition), Arc::clone(&plan)));
        Ok(plan)
    }

    /// The definitions a stream starting at `position` decodes with: the one in
    /// force there, then every recorded one it will reach.
    fn timeline_at(&mut self, position: &Position) -> Result<Timeline, SaciError> {
        let Some(definition) = self
            .history
            .in_force(position)
            .map(|entry| Arc::clone(&entry.definition))
        else {
            return Err(SaciError::generic(format!(
                "{WHAT}: no definition of '{}' is in force at {}",
                self.table,
                position.to_offset()
            )));
        };
        let active = self.plan_for(&definition)?;
        let ahead = self
            .history
            .after(position)
            .iter()
            .map(|entry| Step {
                at: entry.at.clone(),
                plan: build_plan(&self.table, &self.fields, &entry.definition, Missing::Null)
                    .map(Arc::new)
                    .map_err(|e| e.message().to_string()),
            })
            .collect();
        Ok(Timeline { active, ahead })
    }

    /// Store the committed position when it has moved.
    async fn store_committed(&mut self) -> Result<(), SaciError> {
        if !self.stored_dirty {
            return Ok(());
        }
        let Some(position) = self.committed.clone() else {
            return Ok(());
        };
        let offset = position.to_offset();
        let session = self.control.as_mut().expect("a control session");
        self.offsets
            .store_binlog(&mut session.conn, session.flavor, &offset)
            .await
            .inspect_err(|_| self.instruments.error("offset"))?;
        self.stored_dirty = false;
        // A restart begins at the position just stored, so no definition before
        // the one in force there can be needed again.
        self.prune_history(&position).await;
        Ok(())
    }

    /// Drop the definitions before the one in force at the stored `position`.
    /// A failed delete is counted and logged, not raised: the history only
    /// grows, and the next store tries again.
    async fn prune_history(&mut self, position: &Position) {
        let ids: Vec<u64> = self
            .history
            .prune(position)
            .iter()
            .filter_map(|entry| entry.id)
            .collect();
        let (Some(store), Some(session)) = (&self.history_store, self.control.as_mut()) else {
            return;
        };
        if let Err(_e) = store.delete(&mut session.conn, &ids).await {
            self.instruments.error("schema");
            #[cfg(feature = "tracing")]
            tracing::warn!(error = %_e.message(), "schema history not pruned");
        }
    }

    // ------------------------------------------------------------- snapshot

    /// The next snapshot batch, or `None` once the snapshot is complete and the
    /// reader has moved on to the stream.
    async fn snapshot_page(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        let result = self.snapshot_page_inner().await;
        if result.is_err() {
            // A failed snapshot starts over with a new read view.
            if let Some(snapshot) = self.snapshot.take() {
                snapshot.close().await;
            }
            self.instruments.error("snapshot");
        }
        result
    }

    async fn snapshot_page_inner(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        if self.snapshot.is_none() {
            let started = self.start_snapshot().await?;
            self.snapshot = Some(started);
        }
        let limit = self.batch_rows.max(1);
        let started = Instant::now();
        let snapshot = self.snapshot.as_mut().expect("a snapshot");
        let rows = snapshot.next_page(WHAT, limit).await?;
        self.instruments.observe(started.elapsed().as_secs_f64());
        let last = rows.len() < limit;

        let context = SnapshotContext {
            plan: self.snapshot_plan.as_deref().expect("a snapshot plan"),
            file: snapshot.status.file.clone(),
            pos: snapshot.status.pos,
            micros: snapshot.started_micros,
        };
        let batch = if rows.is_empty() {
            None
        } else {
            Some(assemble(
                context.plan,
                &self.schema,
                &rows,
                &context,
                snapshot_cell,
            )?)
        };

        if last {
            let finished = self.snapshot.take().expect("a snapshot");
            let position = finished.status.position(self.position_mode)?;
            finished.close().await;
            #[cfg(feature = "tracing")]
            tracing::info!(table = %self.table, position = %position.to_offset(), "snapshot finished");
            self.committed = Some(position);
            self.stored_dirty = true;
            self.phase = Phase::Streaming;
            self.snapshot_plan = None;
        }
        Ok(batch)
    }

    async fn start_snapshot(&mut self) -> Result<Snapshot, SaciError> {
        let target = self.connector.target().to_string();
        let session = self.control.as_mut().expect("a control session");
        let definition = Arc::new(
            Definition::read(&mut session.conn, WHAT, &self.table, &target)
                .await
                .inspect_err(|_| self.instruments.error("query"))?,
        );
        let plan = Arc::new(
            build_plan(&self.table, &self.fields, &definition, Missing::Refuse)
                .inspect_err(|_| self.instruments.error("query"))?,
        );
        let data = plan.data_columns();
        let key = plan_keys(WHAT, &self.table, &plan.pk_names, &data)?;
        let spec = SnapshotSpec {
            table: self.table.clone(),
            columns: data.iter().map(|c| c.spec.name.clone()).collect(),
            key,
        };
        let session = match self.connector.connect_with_retry().await {
            Ok(session) => session,
            Err(e) => {
                self.instruments.error("connect");
                return Err(e);
            }
        };
        #[cfg(feature = "tracing")]
        tracing::info!(table = %self.table, "snapshot started");
        let snapshot = Snapshot::open(session, WHAT, self.connector.target(), spec).await?;
        // The catalog was read before the position was pinned. Read it again:
        // equal means no DDL fell in between, so the definition is the one in
        // force at the position, which is where the stream starts. It is
        // recorded now, so a missing privilege surfaces before the table is
        // read.
        let session = self.control.as_mut().expect("a control session");
        let pinned = match Definition::read(&mut session.conn, WHAT, &self.table, &target).await {
            Ok(again) if again == *definition => snapshot.status.position(self.position_mode),
            Ok(_) => Err(SaciError::generic(format!(
                "{WHAT}: '{}' changed while the snapshot was starting, so the definition read \
                 for it may not be the one at its position; the snapshot starts again",
                self.table
            ))),
            Err(e) => Err(e),
        };
        let recorded = match pinned {
            Ok(at) => {
                self.record(Entry {
                    id: None,
                    at,
                    definition,
                })
                .await
            }
            Err(e) => Err(e),
        };
        if let Err(e) = recorded {
            snapshot.close().await;
            return Err(e);
        }
        self.snapshot_plan = Some(plan);
        Ok(snapshot)
    }

    // --------------------------------------------------------------- stream

    /// Open the dump at the committed position.
    async fn open_stream(&mut self) -> Result<(), SaciError> {
        let position = self.committed.clone().ok_or_else(|| {
            SaciError::generic(format!(
                "{WHAT}: the binlog stream has no position to start from"
            ))
        })?;
        let timeline = self.timeline_at(&position)?;
        let parts = request_parts(&position);
        let session = match self.connector.connect_with_retry().await {
            Ok(session) => session,
            Err(e) => {
                self.instruments.error("connect");
                return Err(e);
            }
        };
        let mut request = BinlogStreamRequest::new(self.server_id).with_non_blocking();
        request = if parts.gtid {
            request.with_gtid().with_gtid_set(parts.sids)
        } else {
            request.with_filename(&parts.filename).with_pos(parts.pos)
        };
        let stream = session
            .conn
            .get_binlog_stream(request)
            .await
            .map_err(|e| self.binlog_error(&e, true))?;
        #[cfg(feature = "tracing")]
        tracing::info!(table = %self.table, position = %position.to_offset(), "binlog stream opened");
        self.cy = Cycle::with_timeline(&position, self.max_changes_per_cycle, timeline);
        self.stream = Some(stream);
        self.held = None;
        Ok(())
    }

    /// Close the stream, bounded: a dump that is still sending must not wedge
    /// the cycle.
    async fn close_stream(&mut self) {
        if let Some(stream) = self.stream.take() {
            let _ = tokio::time::timeout(CLOSE_TIMEOUT, stream.close()).await;
        }
    }

    /// Drop everything of the cycle in flight. `committed` is untouched, so the
    /// next cycle reads again from it.
    async fn abandon_cycle(&mut self) {
        self.close_stream().await;
        self.queue.clear();
        self.held = None;
        self.cy = Cycle::default();
        self.stream_done = false;
        self.mid_tx = false;
    }

    /// The end of the stream (EOF or the change limit): close it and
    /// acknowledge the tail.
    ///
    /// A tail that only moves past the connector's own offset writes moves the
    /// position in memory but is not stored, see [`Cycle::finish`].
    async fn finish_stream(&mut self) -> Result<(), SaciError> {
        self.close_stream().await;
        self.stream_done = true;
        // A DDL read last, with no row of the table after it, leaves a
        // definition nobody has asked for yet: record it now, while its
        // position is known, instead of at the next row.
        if self.cy.ddl_seen && self.cy.timeline.ahead.is_empty() {
            self.refresh_plan().await?;
        }
        if let Some((tail, worth_storing)) = self.cy.finish() {
            match self.queue.back_mut() {
                Some(last) => last.ack = Some(tail),
                None => {
                    if let Some(position) = &mut self.committed {
                        let before = position.clone();
                        position.apply(&tail).map_err(|e| {
                            SaciError::generic(format!(
                                "{WHAT}: cannot move the binlog position: {e}"
                            ))
                        })?;
                        if *position != before && worth_storing {
                            self.stored_dirty = true;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Read events until a batch worth of rows is queued at a commit boundary,
    /// the stream ends, or the cycle's change limit is reached.
    async fn read_more(&mut self) -> Result<(), SaciError> {
        if self.stream.is_none() {
            self.open_stream().await?;
        }
        let batch_rows = self.batch_rows.max(1);
        let mut reloaded = false;
        loop {
            if self.queue.len() >= batch_rows {
                return Ok(());
            }
            let event = match self.held.take() {
                Some(event) => event,
                None => {
                    let next = self.stream.as_mut().expect("an open stream").next().await;
                    match next {
                        None => return self.finish_stream().await,
                        Some(Err(e)) => return Err(self.binlog_error(&e, false)),
                        Some(Ok(event)) => event,
                    }
                }
            };
            let handled = {
                let context = EventContext {
                    stream: self.stream.as_ref().expect("an open stream"),
                    table: &self.table,
                    offset_table: &self.offset_table,
                    history_table: self.history_store.as_ref().map(HistoryStore::table),
                    instruments: &self.instruments,
                    reloaded,
                    remedy: &self.remedy,
                };
                handle_event(&mut self.cy, &mut self.queue, &context, &event)
            }
            .inspect_err(|_| self.instruments.error("decode"))?;
            match handled {
                Handled::Continue => reloaded = false,
                Handled::Stop => return self.finish_stream().await,
                Handled::Reload => {
                    self.held = Some(event);
                    // Rows already queued keep the plan they were read under.
                    self.refresh_plan().await?;
                    reloaded = true;
                }
            }
        }
    }

    /// Hand out up to a batch of the queued rows and acknowledge their
    /// transactions.
    fn take_queued(&mut self, to_tx_end: bool) -> Result<Option<RecordBatch>, SaciError> {
        let count = take_count(&self.queue, self.batch_rows.max(1), to_tx_end);
        if count == 0 {
            return Ok(None);
        }
        let changes: Vec<Change> = self.queue.drain(..count).collect();
        // `take_count` stops where the definition changes, so one plan decodes
        // the whole batch.
        let Some(plan) = changes.first().map(|change| Arc::clone(&change.plan)) else {
            return Ok(None);
        };
        let plan: &Plan = &plan;
        let batch = assemble(plan, &self.schema, &changes, plan, binlog_cell)
            .inspect_err(|_| self.instruments.error("decode"))?;

        if let Some(position) = &mut self.committed {
            for ack in changes.iter().filter_map(|change| change.ack.as_ref()) {
                position.apply(ack).map_err(|e| {
                    SaciError::generic(format!("{WHAT}: cannot move the binlog position: {e}"))
                })?;
                self.stored_dirty = true;
            }
        }
        self.mid_tx = changes.last().is_some_and(|change| !change.tx_end);
        self.batches_this_cycle += 1;
        self.instruments.batch(batch.num_rows() as u64);
        #[cfg(feature = "tracing")]
        tracing::debug!(table = %self.table, rows = batch.num_rows(), queued = self.queue.len(), "binlog batch");
        Ok(Some(batch))
    }

    /// A dump or stream failure as the error a caller sees.
    fn binlog_error(&self, e: &mysql_async::Error, opening: bool) -> SaciError {
        self.instruments.error("binlog");
        let code = server_code(e);
        let hint = match code {
            Some(ER_MASTER_FATAL_ERROR_READING_BINLOG) => format!(
                "; the stored position is no longer on the server (purged?): delete the \
                 source's row from '{}' and set mode.initial",
                self.offset_table
            ),
            Some(ER_ACCESS_DENIED_ERROR) if !opening => String::new(),
            _ => grant_hint(self.flavor, code).to_string(),
        };
        SaciError::generic(format!(
            "{WHAT}: binlog read of '{}' on {} failed: {}{hint}",
            self.table,
            self.connector.target(),
            mysql_detail(e)
        ))
    }
}

// ------------------------------------------------------------- event handling

struct EventContext<'a> {
    stream: &'a BinlogStream,
    table: &'a TableRef,
    offset_table: &'a TableRef,
    /// The schema history table, when the source keeps one: writes to it are
    /// the connector's own, like the offset table's.
    history_table: Option<&'a TableRef>,
    instruments: &'a Instruments,
    /// The definition was just read for this event: a disagreement is final.
    reloaded: bool,
    /// What an error about a definition tells the operator to do next.
    remedy: &'a str,
}

fn decode_error(table: &TableRef, e: impl std::fmt::Display) -> SaciError {
    SaciError::generic(format!(
        "{WHAT}: cannot decode the binlog of '{table}': {e}"
    ))
}

fn handle_event(
    cy: &mut Cycle,
    queue: &mut VecDeque<Change>,
    context: &EventContext<'_>,
    event: &Event,
) -> Result<Handled, SaciError> {
    let header = event.header();
    // `None` is a type the decoder does not know.
    let Some(data) = event
        .read_data()
        .map_err(|e| decode_error(context.table, e))?
    else {
        return handle_unknown(cy, queue, context, event);
    };
    match data {
        EventData::FormatDescriptionEvent(_) => cy.on_format_description(),
        EventData::RotateEvent(rotate) => cy.on_rotate(rotate.name_raw(), rotate.position()),
        EventData::GtidEvent(gtid) => {
            cy.on_gtid(gtid.sid(), gtid.tag().map(Tag::as_str), gtid.gno());
        }
        EventData::TransactionPayloadEvent(_) => cy.on_payload(header.log_pos()),
        EventData::QueryEvent(query) => match classify_query(query.query_raw(), cy.in_tx) {
            Query::Ignore => {}
            Query::Begin => cy.on_begin(),
            Query::Commit { ddl } => {
                return cy.on_commit(queue, header.timestamp(), header.log_pos(), ddl);
            }
        },
        EventData::XidEvent(_) => {
            return cy.on_commit(queue, header.timestamp(), header.log_pos(), false);
        }
        EventData::IncidentEvent(incident) => {
            return Err(SaciError::generic(format!(
                "{WHAT}: the binlog of '{}' holds an incident event, so changes were lost on \
                 the server: {incident:?}",
                context.table
            )));
        }
        EventData::RowsEvent(rows) => return handle_rows(cy, context, &rows),
        _ => {}
    }
    Ok(Handled::Continue)
}

/// MariaDB's `QUERY_COMPRESSED_EVENT`, written under `log_bin_compress`.
const MARIADB_QUERY_COMPRESSED: u8 = 165;

/// MariaDB's compressed row events, written under `log_bin_compress`: write,
/// update and delete, in the version 1 and the version 2 row layout.
const MARIADB_ROWS_COMPRESSED: std::ops::RangeInclusive<u8> = 166..=171;

/// An event of a type `mysql_common` does not decode.
///
/// Nearly all are server bookkeeping and change nothing here. MariaDB's
/// compressed events are not: skipping a compressed row event of this table
/// would drop its changes and move the position past them without a word, so
/// that one is an error. A compressed row event of another table is skipped
/// as any other row of another table is. A compressed statement is DDL or a
/// statement-format change, because `BEGIN`, `COMMIT` and `ROLLBACK` are
/// shorter than the smallest `log_bin_compress_min_len` and are never
/// compressed, so outside a transaction it is read as the DDL it is, which
/// moves the position and marks the catalog stale.
fn handle_unknown(
    cy: &mut Cycle,
    queue: &mut VecDeque<Change>,
    context: &EventContext<'_>,
    event: &Event,
) -> Result<Handled, SaciError> {
    let header = event.header();
    let raw = header.event_type_raw();
    if raw == MARIADB_QUERY_COMPRESSED {
        return if cy.in_tx {
            Ok(Handled::Continue)
        } else {
            cy.on_commit(queue, header.timestamp(), header.log_pos(), true)
        };
    }
    if !MARIADB_ROWS_COMPRESSED.contains(&raw) {
        return Ok(Handled::Continue);
    }
    // The table id leads the post-header, which is not compressed.
    let table_id = event.data().get(..6).map(|bytes| {
        let mut id = [0u8; 8];
        id[..6].copy_from_slice(bytes);
        u64::from_le_bytes(id)
    });
    let tme = table_id.and_then(|id| context.stream.get_tme(id));
    if let Some(tme) = tme
        && (tme.database_name_raw() != context.table.schema.as_bytes()
            || tme.table_name_raw() != context.table.name.as_bytes())
    {
        skip_other_table(cy, context, tme);
        return Ok(Handled::Continue);
    }
    Err(decode_error(
        context.table,
        "a row event compressed by MariaDB's log_bin_compress, which this reader cannot decode; \
         set log_bin_compress = OFF on the server and start the source after that event with \
         mode.initial",
    ))
}

/// A row event of a table this source does not read. It is news to the
/// position, which follows it, unless it is a write to the offset table or the
/// schema history table: the connector's own stores (and those of any source
/// sharing the tables) are in the binlog too, and would otherwise be news
/// every cycle.
fn skip_other_table(cy: &mut Cycle, context: &EventContext<'_>, tme: &TableMapEvent<'_>) {
    let own = [Some(context.offset_table), context.history_table]
        .into_iter()
        .flatten()
        .any(|store| {
            tme.database_name_raw()
                .eq_ignore_ascii_case(store.schema.as_bytes())
                && tme
                    .table_name_raw()
                    .eq_ignore_ascii_case(store.name.as_bytes())
        });
    if !own {
        cy.foreign = true;
    }
    context.instruments.skipped(1);
}

fn handle_rows(
    cy: &mut Cycle,
    context: &EventContext<'_>,
    rows: &RowsEventData<'_>,
) -> Result<Handled, SaciError> {
    let table = context.table;
    let tme = context.stream.get_tme(rows.table_id()).ok_or_else(|| {
        decode_error(
            table,
            format!(
                "a row event names table id {} with no table map",
                rows.table_id()
            ),
        )
    })?;
    if tme.database_name_raw() != table.schema.as_bytes()
        || tme.table_name_raw() != table.name.as_bytes()
    {
        skip_other_table(cy, context, tme);
        return Ok(Handled::Continue);
    }
    let plan = Arc::clone(&cy.timeline.active);
    let binlog_columns = usize::try_from(tme.columns_count()).unwrap_or(usize::MAX);
    let disagreement = plan.disagrees(binlog_columns, |ordinal| {
        let ty = tme.get_column_type(ordinal).ok().flatten()?;
        Some(Wire::of(ty, tme.get_column_metadata(ordinal)))
    });
    // Past the last recorded definition a DDL says the table may have changed;
    // before it the history is authoritative and the catalog may be newer than
    // the DDL being replayed.
    let edge = cy.timeline.ahead.is_empty();
    if (cy.ddl_seen && edge) || disagreement.is_some() {
        if edge && !context.reloaded {
            cy.shape = Some(Shape::of(tme));
            return Ok(Handled::Reload);
        }
        if let Some(disagreement) = disagreement {
            return Err(disagreement.error(table, &plan, context.remedy));
        }
    }
    for item in rows.rows(tme) {
        let (mut before, mut after) = item.map_err(|e| decode_error(table, e))?;
        for row in [before.as_mut(), after.as_mut()].into_iter().flatten() {
            pad_binary(row, &plan.binary_pads);
        }
        match (before, after) {
            (None, Some(after)) => cy.on_row(b'I', after),
            (Some(before), None) => cy.on_row(b'D', before),
            (Some(before), Some(after)) => {
                if key_changed(&plan.pk_ordinals, &before, &after) {
                    cy.on_row(b'D', before);
                    cy.on_row(b'I', after);
                } else {
                    cy.on_row(b'U', after);
                }
            }
            (None, None) => {}
        }
    }
    Ok(Handled::Continue)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::config::MariadbFieldType;

    const UUID: [u8; 16] = [
        0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42, 0x95,
        0x62,
    ];
    const UUID_TEXT: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn image(values: &[i64]) -> BinlogRow {
        BinlogRow::new(
            values
                .iter()
                .map(|v| Some(BinlogValue::Value(Value::Int(*v))))
                .collect(),
            Arc::from(Vec::new()),
        )
    }

    fn file_start(name: &str, pos: u64) -> Position {
        Position::File {
            name: name.to_string(),
            pos,
        }
    }

    fn set(text: &str) -> GtidSet {
        text.parse().expect("gtid set")
    }

    fn ops(queue: &VecDeque<Change>) -> String {
        queue.iter().map(|c| char::from(c.op)).collect()
    }

    // ----------------------------------------------------------- positions

    #[test]
    fn a_file_offset_becomes_a_file_position_and_back() {
        let offset = BinlogOffset::parse("file:mysql-bin.000003:1274").expect("offset");
        let position = Position::from_offset(offset.clone(), PositionMode::File).expect("file");
        assert_eq!(position, file_start("mysql-bin.000003", 1274));
        assert_eq!(position.to_offset(), offset);
    }

    #[test]
    fn a_gtid_offset_becomes_a_gtid_position_and_back() {
        let text = format!("{UUID_TEXT}:1-5:7");
        let offset = BinlogOffset::parse(&format!("gtid:{text}")).expect("offset");
        let position = Position::from_offset(offset, PositionMode::Gtid).expect("gtid");
        assert_eq!(position, Position::Gtid(set(&text)));
        assert_eq!(position.to_offset().to_string(), format!("gtid:{text}"));
    }

    #[test]
    fn an_offset_of_the_other_kind_is_refused_naming_both() {
        let file = BinlogOffset::parse("file:a.000001:4").expect("offset");
        let err = Position::from_offset(file, PositionMode::Gtid).expect_err("refused");
        assert_eq!(err, "it is a file position but mode.position is \"gtid\"");
        let gtid = BinlogOffset::parse(&format!("gtid:{UUID_TEXT}:1")).expect("offset");
        let err = Position::from_offset(gtid, PositionMode::File).expect_err("refused");
        assert_eq!(err, "it is a GTID set but mode.position is \"file\"");
    }

    #[test]
    fn an_empty_gtid_set_is_a_position_the_offset_table_holds() {
        let empty = Position::Gtid(GtidSet::default());
        assert_eq!(empty.to_offset().to_string(), "gtid:");
        let back = BinlogOffset::parse("gtid:").expect("offset");
        assert_eq!(
            Position::from_offset(back, PositionMode::Gtid).expect("gtid"),
            empty
        );
    }

    #[test]
    fn acknowledgements_move_a_position_forward() {
        let mut position = file_start("a.000001", 4);
        position
            .apply(&Ack::File {
                name: Arc::from("a.000002"),
                pos: 900,
            })
            .expect("file ack");
        assert_eq!(position, file_start("a.000002", 900));

        let mut position = Position::Gtid(set(&format!("{UUID_TEXT}:1-3")));
        position
            .apply(&Ack::Gtid {
                uuid: UUID,
                tag: None,
                gno: 4,
            })
            .expect("gtid ack");
        assert_eq!(position, Position::Gtid(set(&format!("{UUID_TEXT}:1-4"))));
        position
            .apply(&Ack::Set(set(&format!("{UUID_TEXT}:1-9"))))
            .expect("set ack");
        assert_eq!(position, Position::Gtid(set(&format!("{UUID_TEXT}:1-9"))));
    }

    #[test]
    fn an_acknowledgement_of_the_other_kind_is_an_error() {
        let mut position = file_start("a", 4);
        assert!(position.apply(&Ack::Set(GtidSet::default())).is_err());
    }

    #[test]
    fn a_file_position_requests_its_file_and_a_gtid_position_its_blocks() {
        let parts = request_parts(&file_start("mysql-bin.000002", 1226));
        assert_eq!(parts.filename, b"mysql-bin.000002");
        assert_eq!(parts.pos, 1226);
        assert!(!parts.gtid && parts.sids.is_empty());

        let parts = request_parts(&Position::Gtid(set(&format!("{UUID_TEXT}:1-5:7"))));
        assert!(parts.gtid && parts.filename.is_empty());
        assert_eq!(parts.pos, 4);
        assert_eq!(parts.sids.len(), 1);
        let intervals: Vec<(u64, u64)> = parts.sids[0]
            .intervals()
            .iter()
            .map(|i| (i.start(), i.end()))
            .collect();
        // The wire end is exclusive.
        assert_eq!(intervals, vec![(1, 6), (7, 8)]);
    }

    // -------------------------------------------------------------- queries

    #[test]
    fn a_mariadb_placeholder_query_is_ignored() {
        for text in [
            &b"# Dummy event replacing event type 160 that slave cannot handle."[..],
            b"# Dum",
            b"  # Dummy event replacin",
        ] {
            assert_eq!(classify_query(text, false), Query::Ignore);
            assert_eq!(classify_query(text, true), Query::Ignore);
        }
    }

    #[test]
    fn begin_opens_and_commit_or_rollback_close_a_transaction() {
        assert_eq!(classify_query(b"BEGIN", false), Query::Begin);
        assert_eq!(classify_query(b"begin", false), Query::Begin);
        assert_eq!(
            classify_query(b"COMMIT", true),
            Query::Commit { ddl: false }
        );
        assert_eq!(
            classify_query(b"ROLLBACK", true),
            Query::Commit { ddl: false }
        );
    }

    #[test]
    fn a_statement_outside_a_transaction_is_ddl_and_inside_one_is_ignored() {
        let ddl = b"CREATE TABLE t (a INT)";
        assert_eq!(classify_query(ddl, false), Query::Commit { ddl: true });
        assert_eq!(classify_query(b"SAVEPOINT s1", true), Query::Ignore);
        assert_eq!(classify_query(ddl, true), Query::Ignore);
    }

    // ---------------------------------------------------------------- cycle

    fn commit(cy: &mut Cycle, queue: &mut VecDeque<Change>, pos: u32) -> Handled {
        cy.on_commit(queue, 1_700_000_000, pos, false)
            .expect("commit")
    }

    #[test]
    fn rows_are_queued_only_at_the_commit_with_its_position_and_time() {
        let mut cy = Cycle::new(&file_start("a.000001", 4), 0);
        let mut queue = VecDeque::new();
        cy.on_begin();
        cy.on_row(b'I', image(&[1]));
        cy.on_row(b'I', image(&[2]));
        assert!(queue.is_empty(), "nothing is queued before the commit");
        assert_eq!(commit(&mut cy, &mut queue, 500), Handled::Continue);

        assert_eq!(ops(&queue), "II");
        for change in &queue {
            assert_eq!(change.pos, 500);
            assert_eq!(change.commit_micros, 1_700_000_000_000_000);
            assert_eq!(change.file.as_deref(), Some("a.000001"));
        }
        // Only the last row carries the acknowledgement.
        assert_eq!(queue[0].ack, None);
        assert!(
            !queue[0].tx_end && queue[1].tx_end,
            "only the last row ends it"
        );
        assert_eq!(
            queue[1].ack,
            Some(Ack::File {
                name: Arc::from("a.000001"),
                pos: 500
            })
        );
    }

    #[test]
    fn every_row_of_a_compressed_transaction_takes_the_outer_position() {
        let mut cy = Cycle::new(&file_start("a.000001", 4), 0);
        let mut queue = VecDeque::new();
        cy.on_payload(2093);
        cy.on_begin();
        cy.on_row(b'I', image(&[1]));
        // The inner XID has log_pos 0.
        assert_eq!(commit(&mut cy, &mut queue, 0), Handled::Continue);
        assert_eq!(queue[0].pos, 2093);

        // The outer position applies to one transaction only.
        cy.on_row(b'I', image(&[2]));
        assert!(cy.on_commit(&mut queue, 1, 0, false).is_err());
    }

    #[test]
    fn a_commit_with_no_position_in_file_mode_is_an_error() {
        let mut cy = Cycle::new(&file_start("a", 4), 0);
        let mut queue = VecDeque::new();
        let err = cy.on_commit(&mut queue, 1, 0, false).expect_err("refused");
        assert!(
            err.message().contains("carries no position"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn the_gtid_of_a_transaction_is_stamped_on_its_rows() {
        let mut cy = Cycle::new(&Position::Gtid(set(&format!("{UUID_TEXT}:1-2"))), 0);
        let mut queue = VecDeque::new();
        cy.on_gtid(UUID, None, 3);
        cy.on_begin();
        cy.on_row(b'U', image(&[1]));
        commit(&mut cy, &mut queue, 700);
        assert_eq!(
            queue[0].gtid.as_deref(),
            Some(format!("{UUID_TEXT}:3").as_str())
        );
        assert_eq!(
            queue[0].ack,
            Some(Ack::Gtid {
                uuid: UUID,
                tag: None,
                gno: 3
            })
        );
    }

    #[test]
    fn a_tagged_gtid_keeps_its_tag_in_text_and_acknowledgement() {
        let mut cy = Cycle::new(&Position::Gtid(GtidSet::default()), 0);
        let mut queue = VecDeque::new();
        cy.on_gtid(UUID, Some("blue"), 2);
        cy.on_row(b'I', image(&[1]));
        commit(&mut cy, &mut queue, 10);
        assert_eq!(
            queue[0].gtid.as_deref(),
            Some(format!("{UUID_TEXT}:blue:2").as_str())
        );
        assert_eq!(
            queue[0].ack,
            Some(Ack::Gtid {
                uuid: UUID,
                tag: Some("blue".to_string()),
                gno: 2
            })
        );
    }

    #[test]
    fn a_transaction_without_rows_for_the_table_moves_only_the_tail() {
        let mut cy = Cycle::new(&file_start("a.000001", 4), 0);
        let mut queue = VecDeque::new();
        cy.on_begin();
        commit(&mut cy, &mut queue, 800);
        assert!(queue.is_empty());
        // Nothing but writes of the offset table (or of nothing): the position
        // moves in memory and is not worth a store.
        assert_eq!(
            cy.finish(),
            Some((
                Ack::File {
                    name: Arc::from("a.000001"),
                    pos: 800
                },
                false
            ))
        );

        // The same range with a transaction of another table in it is news.
        let mut busy = Cycle::new(&file_start("a.000001", 4), 0);
        busy.on_begin();
        busy.foreign = true;
        commit(&mut busy, &mut queue, 800);
        assert_eq!(busy.finish().map(|(_, worth)| worth), Some(true));
    }

    #[test]
    fn a_rotation_is_worth_storing_without_any_other_news() {
        let mut cy = Cycle::new(&file_start("a.000001", 900), 0);
        cy.on_format_description();
        cy.on_rotate(b"a.000002", 4);
        assert_eq!(
            cy.finish(),
            Some((
                Ack::File {
                    name: Arc::from("a.000002"),
                    pos: 4
                },
                true
            ))
        );
    }

    #[test]
    fn ddl_and_rows_of_the_table_are_news() {
        let mut queue = VecDeque::new();
        let mut ddl = Cycle::new(&file_start("a", 4), 0);
        ddl.on_commit(&mut queue, 1, 50, true).expect("ddl");
        assert_eq!(ddl.finish().map(|(_, worth)| worth), Some(true));

        let mut rows = Cycle::new(&file_start("a", 4), 0);
        rows.on_row(b'I', image(&[1]));
        commit(&mut rows, &mut queue, 60);
        assert_eq!(rows.finish().map(|(_, worth)| worth), Some(true));
    }

    #[test]
    fn a_quiet_gtid_stream_grows_the_read_set_for_every_transaction() {
        let mut cy = Cycle::new(&Position::Gtid(set(&format!("{UUID_TEXT}:1-2"))), 0);
        let mut queue = VecDeque::new();
        for gno in 3..=5 {
            cy.on_gtid(UUID, None, gno);
            commit(&mut cy, &mut queue, 100 * gno as u32);
        }
        assert_eq!(
            cy.finish(),
            Some((Ack::Set(set(&format!("{UUID_TEXT}:1-5"))), false))
        );
        assert_eq!(cy.finish(), None, "the tail is acknowledged once");

        let mut busy = Cycle::new(&Position::Gtid(set(&format!("{UUID_TEXT}:1-2"))), 0);
        busy.foreign = true;
        busy.on_gtid(UUID, None, 3);
        commit(&mut busy, &mut queue, 300);
        assert_eq!(busy.finish().map(|(_, worth)| worth), Some(true));
    }

    #[test]
    fn a_stream_that_read_nothing_has_no_tail() {
        let mut file = Cycle::new(&file_start("a", 4), 0);
        assert_eq!(file.finish(), None);
        let mut gtid = Cycle::new(&Position::Gtid(set(&format!("{UUID_TEXT}:1"))), 0);
        assert_eq!(gtid.finish(), None);
    }

    #[test]
    fn the_cycle_stops_at_the_first_commit_that_reaches_the_limit() {
        let mut cy = Cycle::new(&file_start("a", 4), 3);
        let mut queue = VecDeque::new();
        cy.on_row(b'I', image(&[1]));
        cy.on_row(b'I', image(&[2]));
        assert_eq!(commit(&mut cy, &mut queue, 100), Handled::Continue);
        cy.on_row(b'I', image(&[3]));
        cy.on_row(b'I', image(&[4]));
        // 4 rows read: the whole second transaction is queued, then stop.
        assert_eq!(commit(&mut cy, &mut queue, 200), Handled::Stop);
        assert_eq!(queue.len(), 4);
    }

    #[test]
    fn an_unfinished_transaction_is_dropped_at_the_end_of_the_stream() {
        let mut cy = Cycle::new(&file_start("a", 4), 0);
        let mut queue = VecDeque::new();
        cy.on_begin();
        cy.on_row(b'I', image(&[1]));
        let tail = cy.finish();
        assert_eq!(tail, None);
        assert!(queue.is_empty());
        assert_eq!(commit(&mut cy, &mut queue, 10), Handled::Continue);
        assert!(queue.is_empty(), "the dropped row does not reappear");
    }

    #[test]
    fn a_rotate_names_the_new_file_and_moves_the_tail_there() {
        let mut cy = Cycle::new(&file_start("a.000001", 900), 0);
        cy.on_format_description();
        cy.on_rotate(b"a.000002", 4);
        let mut queue = VecDeque::new();
        cy.on_row(b'I', image(&[1]));
        commit(&mut cy, &mut queue, 300);
        assert_eq!(queue[0].file.as_deref(), Some("a.000002"));
        assert_eq!(
            queue[0].ack,
            Some(Ack::File {
                name: Arc::from("a.000002"),
                pos: 300
            })
        );
    }

    #[test]
    fn the_first_rotate_of_a_file_dump_keeps_the_requested_name() {
        // MariaDB appends four checksum bytes to this one.
        let mut cy = Cycle::new(&file_start("mariadb-bin.000002", 1226), 0);
        cy.on_rotate(b"mariadb-bin.000002\xe8mz\xcd", 1226);
        assert_eq!(cy.file.as_deref(), Some("mariadb-bin.000002"));
        assert_eq!(
            cy.finish(),
            Some((
                Ack::File {
                    name: Arc::from("mariadb-bin.000002"),
                    pos: 1226
                },
                false
            )),
            "the file the dump asked for is not a rotation"
        );
    }

    #[test]
    fn the_first_rotate_of_a_gtid_dump_names_the_file() {
        let mut cy = Cycle::new(&Position::Gtid(GtidSet::default()), 0);
        cy.on_rotate(b"mysql-bin.000003", 4);
        assert_eq!(cy.file.as_deref(), Some("mysql-bin.000003"));
        assert_eq!(
            cy.finish(),
            None,
            "a GTID stream acknowledges sets, not files"
        );
    }

    #[test]
    fn a_ddl_commit_flags_the_catalog_stale() {
        let mut cy = Cycle::new(&file_start("a", 4), 0);
        let mut queue = VecDeque::new();
        assert!(!cy.ddl_seen);
        cy.on_commit(&mut queue, 1, 50, true).expect("ddl");
        assert!(cy.ddl_seen);
    }

    // ---------------------------------------------------------- definitions

    /// A plan told apart from another by its column count alone.
    fn plan_of(ncols: usize) -> Arc<Plan> {
        Arc::new(Plan {
            ncols,
            ..Plan::default()
        })
    }

    fn timeline(active: &Arc<Plan>, ahead: Vec<(Position, Result<Arc<Plan>, String>)>) -> Timeline {
        Timeline {
            active: Arc::clone(active),
            ahead: ahead
                .into_iter()
                .map(|(at, plan)| Step { at, plan })
                .collect(),
        }
    }

    #[test]
    fn rows_keep_the_plan_in_use_when_they_were_read_and_a_batch_ends_where_it_changes() {
        let (old, new) = (plan_of(2), plan_of(3));
        let mut cy = Cycle::with_timeline(
            &file_start("a", 4),
            0,
            timeline(&old, vec![(file_start("a", 100), Ok(Arc::clone(&new)))]),
        );
        let mut queue = VecDeque::new();
        cy.on_row(b'I', image(&[1]));
        // The commit queues the row, then reaches the recorded definition.
        commit(&mut cy, &mut queue, 100);
        cy.on_row(b'I', image(&[2]));
        commit(&mut cy, &mut queue, 200);
        assert!(Arc::ptr_eq(&queue[0].plan, &old));
        assert!(Arc::ptr_eq(&queue[1].plan, &new));
        assert_eq!(take_count(&queue, 10, false), 1, "one plan to a batch");
    }

    #[test]
    fn a_recorded_definition_is_switched_to_by_the_commit_that_reaches_its_tag() {
        let (old, new) = (plan_of(2), plan_of(3));
        let mut cy = Cycle::with_timeline(
            &file_start("a", 4),
            0,
            timeline(&old, vec![(file_start("a", 500), Ok(Arc::clone(&new)))]),
        );
        let mut queue = VecDeque::new();
        commit(&mut cy, &mut queue, 499);
        assert!(Arc::ptr_eq(&cy.timeline.active, &old));
        assert_eq!(cy.timeline.ahead.len(), 1);
        commit(&mut cy, &mut queue, 500);
        assert!(Arc::ptr_eq(&cy.timeline.active, &new));
        assert!(cy.timeline.ahead.is_empty());
    }

    #[test]
    fn a_ddl_at_a_recorded_tag_settles_the_definition_and_one_past_the_last_marks_it_stale() {
        let (old, new) = (plan_of(2), plan_of(3));
        let mut cy = Cycle::with_timeline(
            &file_start("a", 4),
            0,
            timeline(&old, vec![(file_start("a", 50), Ok(Arc::clone(&new)))]),
        );
        let mut queue = VecDeque::new();
        cy.on_commit(&mut queue, 1, 50, true).expect("ddl");
        assert!(Arc::ptr_eq(&cy.timeline.active, &new));
        assert!(!cy.ddl_seen && cy.ddl_at.is_none(), "the history decided");

        cy.on_commit(&mut queue, 1, 80, true).expect("ddl");
        assert!(cy.ddl_seen);
        assert_eq!(cy.ddl_at, Some(file_start("a", 80)));
    }

    #[test]
    fn a_ddl_ahead_of_a_recorded_tag_does_not_settle_anything() {
        let (old, new) = (plan_of(2), plan_of(3));
        let mut cy = Cycle::with_timeline(
            &file_start("a", 4),
            0,
            timeline(&old, vec![(file_start("a", 500), Ok(new))]),
        );
        let mut queue = VecDeque::new();
        cy.on_commit(&mut queue, 1, 50, true).expect("ddl");
        assert!(Arc::ptr_eq(&cy.timeline.active, &old));
        assert_eq!(cy.timeline.ahead.len(), 1);
    }

    #[test]
    fn a_gtid_stream_reaches_a_tag_once_its_read_set_contains_it() {
        let (old, new) = (plan_of(2), plan_of(3));
        let start = Position::Gtid(set(&format!("{UUID_TEXT}:1-2")));
        let tag = Position::Gtid(set(&format!("{UUID_TEXT}:1-4")));
        let mut cy = Cycle::with_timeline(&start, 0, timeline(&old, vec![(tag, Ok(new))]));
        let mut queue = VecDeque::new();
        cy.on_gtid(UUID, None, 3);
        commit(&mut cy, &mut queue, 100);
        assert!(Arc::ptr_eq(&cy.timeline.active, &old));
        cy.on_gtid(UUID, None, 4);
        commit(&mut cy, &mut queue, 200);
        assert_eq!(cy.timeline.active.ncols, 3);
    }

    #[test]
    fn a_definition_that_cannot_decode_the_declared_fields_fails_when_it_is_reached() {
        let old = plan_of(2);
        let mut cy = Cycle::with_timeline(
            &file_start("a", 4),
            0,
            timeline(
                &old,
                vec![(file_start("a", 100), Err("column 'x' is gone".to_string()))],
            ),
        );
        let mut queue = VecDeque::new();
        commit(&mut cy, &mut queue, 99);
        let err = cy
            .on_commit(&mut queue, 1, 100, false)
            .expect_err("the plan is an error");
        assert!(
            err.message().contains("column 'x' is gone"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn the_tail_position_is_where_the_stream_has_read_to() {
        let mut queue = VecDeque::new();
        let mut file = Cycle::new(&file_start("a.000001", 4), 0);
        assert_eq!(file.tail_position(), Some(file_start("a.000001", 4)));
        commit(&mut file, &mut queue, 300);
        assert_eq!(file.tail_position(), Some(file_start("a.000001", 300)));
        file.on_format_description();
        file.on_rotate(b"a.000002", 4);
        assert_eq!(file.tail_position(), Some(file_start("a.000002", 4)));

        let mut gtid = Cycle::new(&Position::Gtid(set(&format!("{UUID_TEXT}:1-2"))), 0);
        gtid.on_gtid(UUID, None, 3);
        commit(&mut gtid, &mut queue, 100);
        assert_eq!(
            gtid.tail_position(),
            Some(Position::Gtid(set(&format!("{UUID_TEXT}:1-3"))))
        );
    }

    fn catalog_column(name: &str, ordinal: usize, data_type: &str) -> CatalogColumn {
        CatalogColumn {
            name: name.to_string(),
            ordinal,
            data_type: data_type.to_string(),
            column_type: data_type.to_string(),
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
            charset: None,
        }
    }

    fn definition(columns: &[(&str, &str)]) -> Arc<Definition> {
        Arc::new(Definition {
            columns: columns
                .iter()
                .enumerate()
                .map(|(ordinal, (name, ty))| catalog_column(name, ordinal, ty))
                .collect(),
            primary_key: columns
                .iter()
                .take(1)
                .map(|(name, _)| (*name).to_string())
                .collect(),
        })
    }

    fn declared(name: &str, ty: MariadbFieldType, nullable: bool) -> FieldSpec {
        FieldSpec {
            name: name.to_string(),
            ty,
            nullable,
            precision: None,
            scale: None,
            mariadb_type: None,
        }
    }

    fn orders() -> TableRef {
        TableRef {
            schema: "saci".to_string(),
            name: "orders".to_string(),
        }
    }

    #[test]
    fn a_declared_column_a_definition_lacks_reads_null_when_nullable_and_is_refused_otherwise() {
        let def = definition(&[("id", "bigint"), ("note", "varchar")]);
        let fields = [
            declared("id", MariadbFieldType::Int64, false),
            declared("extra", MariadbFieldType::Int64, true),
            declared("note", MariadbFieldType::Utf8, true),
        ];
        let plan = build_plan(&orders(), &fields, &def, Missing::Null).expect("a plan");
        let slots: Vec<Slot> = plan.fields.iter().map(|field| field.slot).collect();
        assert_eq!(slots, [Slot::Data(0), Slot::Absent, Slot::Data(1)]);
        assert_eq!(plan.ncols, 2);
        assert_eq!(
            plan.data_columns().len(),
            2,
            "the absent field is not selected"
        );
        assert!(Arc::ptr_eq(&plan.definition, &def));

        // Strictly, the same declaration is a typo.
        let err = build_plan(&orders(), &fields, &def, Missing::Refuse).expect_err("refused");
        assert!(
            err.message()
                .contains("column 'extra' is declared but the server has no such column"),
            "{}",
            err.message()
        );

        // A field that may not be NULL cannot stand in for a missing column.
        let required = [
            declared("id", MariadbFieldType::Int64, false),
            declared("extra", MariadbFieldType::Int64, false),
        ];
        let err = build_plan(&orders(), &required, &def, Missing::Null).expect_err("refused");
        assert!(
            err.message().contains("column 'extra'") && err.message().contains("nullable=#false"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn a_plan_keeps_the_key_ordinals_of_its_own_definition() {
        let wide = definition(&[("a", "bigint"), ("id", "bigint")]);
        let wide = Arc::new(Definition {
            primary_key: vec!["id".to_string()],
            ..(*wide).clone()
        });
        let fields = [declared("id", MariadbFieldType::Int64, false)];
        let plan = build_plan(&orders(), &fields, &wide, Missing::Null).expect("a plan");
        assert_eq!(plan.pk_ordinals, [1]);
        assert_eq!(plan.pk_names, ["id"]);

        let narrow = definition(&[("id", "bigint")]);
        let plan = build_plan(&orders(), &fields, &narrow, Missing::Null).expect("a plan");
        assert_eq!(plan.pk_ordinals, [0], "dropping a column moves the key");
    }

    #[test]
    fn a_plan_disagrees_with_a_table_map_of_another_count_or_type() {
        let def = definition(&[("id", "bigint"), ("n", "int")]);
        let fields = [
            declared("id", MariadbFieldType::Int64, false),
            declared("n", MariadbFieldType::Int64, true),
        ];
        let plan = build_plan(&orders(), &fields, &def, Missing::Null).expect("a plan");
        assert_eq!(
            plan.disagrees(2, |_| None),
            None,
            "a table map with no types"
        );
        assert_eq!(
            plan.disagrees(2, |ordinal| Some(if ordinal == 0 {
                Wire::Int(64)
            } else {
                Wire::Int(32)
            })),
            None
        );
        assert_eq!(
            plan.disagrees(3, |_| None),
            Some(Disagreement::Columns { binlog: 3 })
        );
        assert_eq!(
            plan.disagrees(2, |_| Some(Wire::Int(64))),
            Some(Disagreement::Column(
                "column 'n' is a 64-bit integer in the binlog and a 32-bit integer in the catalog"
                    .to_string()
            ))
        );
    }

    #[test]
    fn a_disagreement_names_the_table_both_counts_and_the_remedy() {
        let err = Disagreement::Columns { binlog: 2 }.error(&orders(), &plan_of(3), "do this");
        assert_eq!(
            err.message(),
            "MariadbSource: table 'saci.orders' changed shape under the binlog (binlog has 2 \
             columns, catalog has 3); do this"
        );
        let err = Disagreement::Column("column 'n' is a bit".to_string()).error(
            &orders(),
            &plan_of(3),
            "do this",
        );
        assert!(
            err.message()
                .contains("changed under the binlog (column 'n' is a bit)")
                && err.message().ends_with("do this"),
            "{}",
            err.message()
        );
        assert!(remedy_text(None).contains("mode.schema_history is #false"));
    }

    #[test]
    fn a_shape_answers_for_the_columns_its_table_map_named() {
        let shape = Shape {
            columns: 2,
            wires: vec![Some(Wire::Date), None],
        };
        assert_eq!(shape.wire(0), Some(Wire::Date));
        assert_eq!(shape.wire(1), None);
        assert_eq!(shape.wire(9), None);
    }

    // ------------------------------------------------------------ key change

    #[test]
    fn binary_values_get_their_zero_padding_back() {
        let mut row = BinlogRow::new(
            vec![
                Some(BinlogValue::Value(Value::Bytes(vec![10]))),
                Some(BinlogValue::Value(Value::Bytes(vec![1, 2, 3, 4]))),
                Some(BinlogValue::Value(Value::NULL)),
            ],
            Arc::from(Vec::new()),
        );
        pad_binary(&mut row, &[(0, 4), (1, 4), (2, 4)]);
        assert_eq!(
            row.as_ref(0),
            Some(&BinlogValue::Value(Value::Bytes(vec![10, 0, 0, 0])))
        );
        assert_eq!(
            row.as_ref(1),
            Some(&BinlogValue::Value(Value::Bytes(vec![1, 2, 3, 4])))
        );
        assert_eq!(row.as_ref(2), Some(&BinlogValue::Value(Value::NULL)));
    }

    #[test]
    fn an_update_that_changes_a_key_column_is_detected() {
        assert!(key_changed(&[0], &image(&[1, 5]), &image(&[2, 5])));
        assert!(!key_changed(&[0], &image(&[1, 5]), &image(&[1, 6])));
        assert!(
            !key_changed(&[], &image(&[1]), &image(&[2])),
            "a table with no key never splits an update"
        );
        assert!(key_changed(&[0, 1], &image(&[1, 5]), &image(&[1, 6])));
    }

    // ------------------------------------------------------------- hand-out

    fn change_under(op: u8, ack: Option<Ack>, plan: &Arc<Plan>) -> Change {
        Change {
            op,
            image: image(&[1]),
            plan: Arc::clone(plan),
            file: None,
            pos: 0,
            gtid: None,
            commit_micros: 0,
            ack,
            tx_end: false,
        }
    }

    fn change(op: u8, ack: Option<Ack>) -> Change {
        change_under(op, ack, &Arc::new(Plan::default()))
    }

    /// `lengths` transactions of that many rows each, queued as a commit would,
    /// all under one plan.
    fn queue_of(lengths: &[usize]) -> VecDeque<Change> {
        let plan = Arc::new(Plan::default());
        let mut queue = VecDeque::new();
        for &length in lengths {
            for index in 0..length {
                let mut row = change_under(b'I', None, &plan);
                row.tx_end = index + 1 == length;
                queue.push_back(row);
            }
        }
        queue
    }

    #[test]
    fn a_batch_never_spans_two_plans() {
        let (first, second) = (plan_of(1), plan_of(2));
        let mut queue = VecDeque::new();
        for plan in [&first, &first, &second, &second, &second] {
            queue.push_back(change_under(b'I', None, plan));
        }
        assert_eq!(take_count(&queue, 100, false), 2);
        queue.drain(..2);
        assert_eq!(take_count(&queue, 100, false), 3);
        assert_eq!(take_count(&queue, 2, false), 2);
    }

    #[test]
    fn a_batch_takes_up_to_its_size_or_to_the_end_of_the_open_transaction() {
        let queue = queue_of(&[5, 2]);
        assert_eq!(take_count(&queue, 2, false), 2);
        assert_eq!(take_count(&queue, 3, false), 3);
        assert_eq!(take_count(&queue, 100, false), 7, "capped by the queue");
        // Closing the transaction: never past its last row, never over the size.
        assert_eq!(take_count(&queue, 100, true), 5);
        assert_eq!(take_count(&queue, 3, true), 3);

        let rest = queue_of(&[2, 4]);
        assert_eq!(
            take_count(&rest, 3, true),
            2,
            "the first transaction ends at 2"
        );
        assert_eq!(take_count(&VecDeque::new(), 3, true), 0);
    }

    #[test]
    fn a_row_in_the_middle_of_a_transaction_acknowledges_nothing() {
        let mut position = file_start("a", 4);
        let rows = [
            change(b'I', None),
            change(
                b'I',
                Some(Ack::File {
                    name: Arc::from("a"),
                    pos: 90,
                }),
            ),
        ];
        // A batch holding only the first row leaves the position behind.
        for ack in rows[..1].iter().filter_map(|c| c.ack.as_ref()) {
            position.apply(ack).expect("apply");
        }
        assert_eq!(position, file_start("a", 4));
        for ack in rows.iter().filter_map(|c| c.ack.as_ref()) {
            position.apply(ack).expect("apply");
        }
        assert_eq!(position, file_start("a", 90));
    }

    // ---------------------------------------------------------------- hints

    #[test]
    fn a_privilege_error_names_the_grants_of_the_server_flavour() {
        assert_eq!(
            grant_hint(Flavor::Mysql, Some(ER_SPECIFIC_ACCESS_DENIED_ERROR)),
            "; grant REPLICATION SLAVE and REPLICATION CLIENT"
        );
        assert_eq!(
            grant_hint(Flavor::Mariadb, Some(ER_ACCESS_DENIED_ERROR)),
            "; grant REPLICATION SLAVE and BINLOG MONITOR"
        );
        assert_eq!(grant_hint(Flavor::Mysql, Some(1062)), "");
        assert_eq!(grant_hint(Flavor::Mysql, None), "");
    }

    #[test]
    fn the_status_statement_depends_on_the_server() {
        assert_eq!(status_sql(Flavor::Mysql), "SHOW BINARY LOG STATUS");
        assert_eq!(status_sql(Flavor::Mariadb), "SHOW BINLOG STATUS");
    }

    #[test]
    fn a_status_gives_a_file_position_or_a_gtid_position() {
        let status = Status {
            file: "mysql-bin.000003".to_string(),
            pos: 99,
            gtid: Some(set(&format!("{UUID_TEXT}:1-4"))),
        };
        assert_eq!(
            status.position(PositionMode::File).expect("file"),
            file_start("mysql-bin.000003", 99)
        );
        assert_eq!(
            status.position(PositionMode::Gtid).expect("gtid"),
            Position::Gtid(set(&format!("{UUID_TEXT}:1-4")))
        );
        let mariadb = Status {
            gtid: None,
            ..status
        };
        let err = mariadb.position(PositionMode::Gtid).expect_err("refused");
        assert!(
            err.message().contains("MySQL GTIDs only"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn reserved_names_map_to_their_metadata() {
        for (name, meta) in [
            ("__op", Meta::Op),
            ("__table", Meta::Table),
            ("__binlog_file", Meta::File),
            ("__binlog_pos", Meta::Pos),
            ("__gtid", Meta::Gtid),
            ("__commit_ts", Meta::CommitTs),
        ] {
            assert_eq!(meta_of(name), Some(meta));
        }
        assert_eq!(meta_of("id"), None);
    }

    #[test]
    fn metadata_cells_carry_the_row_values() {
        let row = MetaRow {
            op: b"U",
            file: Some("a.000001"),
            pos: 77,
            gtid: None,
            micros: 5,
        };
        assert_eq!(meta_cell(Meta::Op, "s.t", &row), Cell::Text(b"U"));
        assert_eq!(meta_cell(Meta::Table, "s.t", &row), Cell::Text(b"s.t"));
        assert_eq!(meta_cell(Meta::File, "s.t", &row), Cell::Text(b"a.000001"));
        assert_eq!(meta_cell(Meta::Pos, "s.t", &row), Cell::UInt(77));
        assert_eq!(meta_cell(Meta::Gtid, "s.t", &row), Cell::Null);
        assert_eq!(meta_cell(Meta::CommitTs, "s.t", &row), Cell::EpochMicros(5));
    }
}
