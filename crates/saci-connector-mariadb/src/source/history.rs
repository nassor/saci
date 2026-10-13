//! The schema history of a `cdc_binlog` source: which definition of the table
//! was in force at which binlog position.
//!
//! A row event carries the values of a row and, in its table map, the column
//! types, but no names, no signedness and no character set. Decoding a row
//! needs the table's definition, and the definition of today is wrong for a row
//! written before an `ALTER TABLE`. A source that resumes from a stored
//! position before such a change must decode each row with the definition that
//! was in force when the row was written, so the source records every
//! definition it reads, tagged with the position from which it holds.
//!
//! # What is recorded
//!
//! A [`Definition`] is the columns and primary key of the watched table as
//! `information_schema` reports them: name, ordinal, type, precision, scale,
//! fractional digits, signedness, character set. It is stored as JSON in one
//! row of the history table, next to the offset table:
//!
//! ```text
//! CREATE TABLE IF NOT EXISTS <offset_table>_schema (
//!   id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
//!   source_name VARCHAR(191) NOT NULL,
//!   table_name VARCHAR(129) NOT NULL,
//!   position_value TEXT NOT NULL,
//!   definition_json MEDIUMTEXT NOT NULL,
//!   recorded_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
//!   KEY saci_schema_history_source (source_name)
//! )
//! ```
//!
//! Rows are keyed by the source's `name` and the watched table, so sources
//! sharing the table or the history table never read each other's rows. A
//! definition holds from its [`Entry::at`] on: a file position
//! (`file:<name>:<position>`) or a GTID set (`gtid:<set>`), the same text the
//! offset table stores.
//!
//! # When an entry is recorded
//!
//! - At the start: the current catalog, taken to describe the table at the
//!   position the source starts from. A source with no stored position always
//!   records it, unless the history already holds that definition there, because
//!   an older entry may predate a change made since. A stored position records
//!   it only when no entry is at or before it. That is exact for `initial "now"`
//!   and for the position a snapshot pins, and the best available for a
//!   literal, `"beginning"` or a stored position nothing was recorded for; the
//!   table map check of every row event says when it is not.
//! - When the stream reads a DDL statement and then a row event of the table,
//!   or the end of the stream, and the catalog now differs from the definition
//!   in use: the new definition, tagged with the position of the last DDL read.
//!   A row event whose table map disagrees with the definition in use reads
//!   the catalog the same way and tags it with the position just before the
//!   event's transaction.
//!
//! An entry is written before the position that depends on it is stored, so a
//! restart never finds a position whose definition is missing because of a
//! crash.
//!
//! # How a restart uses it
//!
//! The definition in force at the start position is the newest entry whose
//! tag is at or before it. The entries after it are *ahead*: the stream
//! switches to each when a commit reaches its tag. While entries are ahead the
//! history is authoritative and the catalog is not read: the catalog may be
//! newer than the DDL being replayed. Past the last entry the stream is at the
//! *edge*, where a DDL marks the definition stale and the catalog is read
//! again, as it was before the history existed.
//!
//! # Ordering
//!
//! Binlog files order by the number after the last `.` of the name, because
//! `mysql-bin.1000000` sorts before `mysql-bin.999999` as text; a name without
//! one orders as text. GTID sets order by containment: a tag is at or before a
//! position when the position's set contains it, and sets that contain one
//! another sort by size.
//!
//! # Retention
//!
//! The entries before the one in force at the stored position are deleted once
//! that position is stored, because a restart never starts before it. The
//! cost is that an operator who rewinds the offset row past the retained
//! entries gets the table map refusal of a position with no history. A failed
//! delete is counted and logged, not raised: the history only grows.

use std::cmp::Ordering;
use std::sync::Arc;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params, Value};
use saci_core::error::SaciError;
use serde::{Deserialize, Serialize};

use super::binlog::Position;
use crate::config::PositionMode;
use crate::connection::{
    ER_NO_SUCH_TABLE, TableRef, columns_of, exec_closing, mysql_detail, primary_key_of, server_code,
};
use crate::offsets::BinlogOffset;
use crate::types::CatalogColumn;

/// The version of the JSON form a definition is stored in.
const FORMAT: u32 = 1;

// ---------------------------------------------------------------- definition

/// The columns and primary key of the watched table at one point in its life.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Definition {
    /// Every column, in table order, with the ordinal a row image indexes by.
    pub(crate) columns: Vec<CatalogColumn>,
    /// The primary key's column names in key order, empty when it has none.
    pub(crate) primary_key: Vec<String>,
}

#[derive(Serialize)]
struct StoredOut<'a> {
    format: u32,
    columns: &'a [CatalogColumn],
    primary_key: &'a [String],
}

#[derive(Deserialize)]
struct StoredIn {
    format: u32,
    columns: Vec<CatalogColumn>,
    primary_key: Vec<String>,
}

impl Definition {
    /// Read the table's current definition.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the table does not exist or is
    /// not visible, and [`SaciError::Generic`] when the catalog cannot be read.
    pub(crate) async fn read(
        conn: &mut Conn,
        what: &str,
        table: &TableRef,
        target: &str,
    ) -> Result<Self, SaciError> {
        let columns = columns_of(conn, what, table, target).await?;
        let primary_key = primary_key_of(conn, what, table).await?;
        Ok(Self {
            columns,
            primary_key,
        })
    }

    /// Whether the table has a column called `name`, without regard to case.
    pub(crate) fn has(&self, name: &str) -> bool {
        self.columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(name))
    }

    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&StoredOut {
            format: FORMAT,
            columns: &self.columns,
            primary_key: &self.primary_key,
        })
    }

    fn from_json(text: &str) -> Result<Self, String> {
        let stored: StoredIn =
            serde_json::from_str(text).map_err(|e| format!("the definition is not valid: {e}"))?;
        if stored.format != FORMAT {
            return Err(format!(
                "the definition has format {}, which this version reads as {FORMAT} only",
                stored.format
            ));
        }
        Ok(Self {
            columns: stored.columns,
            primary_key: stored.primary_key,
        })
    }
}

// ------------------------------------------------------------------ ordering

/// The number a binlog file name ends in: `3` for `mysql-bin.000003`.
fn log_index(name: &str) -> Option<u64> {
    name.rsplit_once('.')?.1.parse().ok()
}

/// Where `a` stands against `b` in the binlog, both being positions in a file.
pub(crate) fn file_order(a_name: &str, a_pos: u64, b_name: &str, b_pos: u64) -> Ordering {
    match (log_index(a_name), log_index(b_name)) {
        (Some(a), Some(b)) => a
            .cmp(&b)
            .then_with(|| a_name.cmp(b_name))
            .then(a_pos.cmp(&b_pos)),
        _ => a_name.cmp(b_name).then(a_pos.cmp(&b_pos)),
    }
}

/// Whether a definition tagged `at` holds at `position`: `at` is no later.
/// Positions of different kinds never compare.
pub(crate) fn reached(at: &Position, position: &Position) -> bool {
    match (at, position) {
        (Position::File { name: a, pos: ap }, Position::File { name: b, pos: bp }) => {
            file_order(a, *ap, b, *bp) != Ordering::Greater
        }
        (Position::Gtid(a), Position::Gtid(b)) => a.is_subset_of(b),
        _ => false,
    }
}

/// Where tags sort: by position in a file, by size for GTID sets.
fn rank(a: &Position, b: &Position) -> Ordering {
    match (a, b) {
        (Position::File { name: a, pos: ap }, Position::File { name: b, pos: bp }) => {
            file_order(a, *ap, b, *bp)
        }
        (Position::Gtid(a), Position::Gtid(b)) => a.transactions().cmp(&b.transactions()),
        _ => Ordering::Equal,
    }
}

// ------------------------------------------------------------------- history

/// One definition and the position from which it holds.
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    /// The history table's `id`, `None` for an entry only held in memory.
    pub(crate) id: Option<u64>,
    /// The definition holds from this position on.
    pub(crate) at: Position,
    pub(crate) definition: Arc<Definition>,
}

/// Every entry of one source and table, oldest tag first.
///
/// Held in memory for the life of the source and written through to the
/// history table, so within one process a stream that restarts from its
/// committed position finds the definitions it recorded.
#[derive(Debug, Default)]
pub(crate) struct History {
    entries: Vec<Entry>,
}

impl History {
    /// A history of `entries`, in any order.
    pub(crate) fn new(entries: Vec<Entry>) -> Self {
        let mut history = Self::default();
        for entry in entries {
            history.push(entry);
        }
        history
    }

    /// Add `entry` after every entry whose tag is no later, so of two entries
    /// with the same tag the newer one is in force.
    pub(crate) fn push(&mut self, entry: Entry) {
        let at = self
            .entries
            .partition_point(|held| rank(&held.at, &entry.at) != Ordering::Greater);
        self.entries.insert(at, entry);
    }

    /// The index of the newest entry whose tag is at or before `position`.
    fn in_force_index(&self, position: &Position) -> Option<usize> {
        self.entries
            .iter()
            .rposition(|entry| reached(&entry.at, position))
    }

    /// The definition in force at `position`.
    pub(crate) fn in_force(&self, position: &Position) -> Option<&Entry> {
        self.in_force_index(position)
            .and_then(|index| self.entries.get(index))
    }

    /// The entries after the one in force at `position`, oldest first: the
    /// definitions a stream starting there still has to reach.
    pub(crate) fn after(&self, position: &Position) -> &[Entry] {
        let from = self.in_force_index(position).map_or(0, |index| index + 1);
        &self.entries[from..]
    }

    /// Drop and return the entries before the one in force at `position`.
    pub(crate) fn prune(&mut self, position: &Position) -> Vec<Entry> {
        match self.in_force_index(position) {
            Some(keep) => self.entries.drain(..keep).collect(),
            None => Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

// --------------------------------------------------------------------- store

/// What to do about a refused write: create the table when it is missing,
/// otherwise grant the privilege.
fn write_hint(e: &mysql_async::Error) -> &'static str {
    if server_code(e) == Some(ER_NO_SUCH_TABLE) {
        "; create it, or leave mode.offset_table_autocreate #true to have it created"
    } else {
        "; grant INSERT on it"
    }
}

/// The history table of one source and table.
pub(crate) struct HistoryStore {
    /// `MariadbSource`, for error prefixes.
    what: &'static str,
    table: TableRef,
    /// `` `schema`.`table` ``.
    quoted: String,
    source_name: String,
    /// `schema.table` of the watched table, which the rows are keyed by.
    watched: String,
    autocreate: bool,
    created: bool,
}

impl HistoryStore {
    /// A store over the already resolved history `table`, for `source_name`
    /// reading `watched`.
    pub(crate) fn new(
        what: &'static str,
        table: TableRef,
        source_name: &str,
        watched: &TableRef,
        autocreate: bool,
    ) -> Self {
        Self {
            what,
            quoted: table.quoted(),
            table,
            source_name: source_name.to_string(),
            watched: watched.to_string(),
            autocreate,
            created: false,
        }
    }

    /// The table the entries are in.
    pub(crate) fn table(&self) -> &TableRef {
        &self.table
    }

    /// Point the store at the table a source reads, after the server reports
    /// the name it stores.
    pub(crate) fn watch(&mut self, watched: &TableRef) {
        self.watched = watched.to_string();
    }

    fn create_sql(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, \
             source_name VARCHAR(191) NOT NULL, \
             table_name VARCHAR(129) NOT NULL, \
             position_value TEXT NOT NULL, \
             definition_json MEDIUMTEXT NOT NULL, \
             recorded_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), \
             KEY saci_schema_history_source (source_name))",
            self.quoted
        )
    }

    fn load_sql(&self) -> String {
        format!(
            "SELECT id, position_value, definition_json FROM {} \
             WHERE source_name = ? AND table_name = ? ORDER BY id",
            self.quoted
        )
    }

    fn insert_sql(&self) -> String {
        format!(
            "INSERT INTO {} (source_name, table_name, position_value, definition_json) \
             VALUES (?, ?, ?, ?)",
            self.quoted
        )
    }

    fn delete_sql(&self, ids: usize) -> String {
        let marks = vec!["?"; ids].join(", ");
        format!(
            "DELETE FROM {} WHERE source_name = ? AND id IN ({marks})",
            self.quoted
        )
    }

    /// Create the history table if configured to and it is missing, at most
    /// once per connection. The catalog is probed first, so a table an
    /// operator created needs no `CREATE` privilege.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the probe or the `CREATE TABLE`
    /// fails, which for the latter is usually a missing `CREATE` privilege.
    pub(crate) async fn ensure(&mut self, conn: &mut Conn) -> Result<(), SaciError> {
        if self.created || !self.autocreate {
            self.created = true;
            return Ok(());
        }
        let exists: Option<u8> = conn
            .exec_first(
                "SELECT 1 FROM information_schema.TABLES \
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
                (self.table.schema.as_str(), self.table.name.as_str()),
            )
            .await
            .map_err(|e| {
                SaciError::generic(format!(
                    "{}: cannot look up schema history table '{}': {}",
                    self.what,
                    self.table,
                    mysql_detail(&e)
                ))
            })?;
        if exists.is_none() {
            conn.query_drop(self.create_sql()).await.map_err(|e| {
                SaciError::generic(format!(
                    "{}: cannot create schema history table '{}': {}; grant CREATE, or create it \
                     and set mode.offset_table_autocreate #false",
                    self.what,
                    self.table,
                    mysql_detail(&e)
                ))
            })?;
        }
        self.created = true;
        Ok(())
    }

    /// Forget that the table was created, after a reconnect.
    pub(crate) fn reset(&mut self) {
        self.created = false;
    }

    /// This source's entries for the watched table, oldest first. A missing
    /// table reads as no entries, so a source configured not to create it fails
    /// on the first write, where the message can name what to create.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the read fails, or when a row is
    /// not a position of `mode` with a definition this version reads.
    pub(crate) async fn load(
        &self,
        conn: &mut Conn,
        mode: PositionMode,
    ) -> Result<Vec<Entry>, SaciError> {
        let rows = exec_closing(
            conn,
            &self.load_sql(),
            (self.source_name.as_str(), self.watched.as_str()),
        )
        .await;
        let rows = match rows {
            Ok(rows) => rows,
            Err(e) if server_code(&e) == Some(ER_NO_SUCH_TABLE) => return Ok(Vec::new()),
            Err(e) => {
                return Err(SaciError::generic(format!(
                    "{}: cannot read schema history table '{}': {}; grant SELECT on it",
                    self.what,
                    self.table,
                    mysql_detail(&e)
                )));
            }
        };
        let mut entries = Vec::with_capacity(rows.len());
        for mut row in rows {
            let id: Option<u64> = row.take_opt(0).and_then(Result::ok);
            let position: Option<String> = row.take_opt(1).and_then(Result::ok);
            let json: Option<String> = row.take_opt(2).and_then(Result::ok);
            let (Some(id), Some(position), Some(json)) = (id, position, json) else {
                return Err(SaciError::generic(format!(
                    "{}: a row of schema history table '{}' has an unexpected type",
                    self.what, self.table
                )));
            };
            let bad = |reason: String| {
                SaciError::generic(format!(
                    "{}: row {id} of schema history table '{}' is not usable: {reason}; delete \
                     this source's rows from it",
                    self.what, self.table
                ))
            };
            let at = BinlogOffset::parse(&position)
                .and_then(|offset| Position::from_offset(offset, mode))
                .map_err(bad)?;
            let definition = Definition::from_json(&json).map_err(bad)?;
            entries.push(Entry {
                id: Some(id),
                at,
                definition: Arc::new(definition),
            });
        }
        Ok(entries)
    }

    /// Store one entry and return its `id`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the insert fails, naming the table.
    pub(crate) async fn insert(
        &self,
        conn: &mut Conn,
        at: &Position,
        definition: &Definition,
    ) -> Result<u64, SaciError> {
        let fail = |reason: String, hint: &str| {
            SaciError::generic(format!(
                "{}: cannot write schema history table '{}': {reason}{hint}",
                self.what, self.table
            ))
        };
        let json = definition.to_json().map_err(|e| fail(e.to_string(), ""))?;
        exec_closing(
            conn,
            &self.insert_sql(),
            (
                self.source_name.as_str(),
                self.watched.as_str(),
                at.to_offset().to_string(),
                json,
            ),
        )
        .await
        .map_err(|e| fail(mysql_detail(&e), write_hint(&e)))?;
        let id: Option<u64> = conn
            .query_first("SELECT LAST_INSERT_ID()")
            .await
            .map_err(|e| fail(mysql_detail(&e), ""))?;
        id.ok_or_else(|| fail("the server reported no id for the new row".to_string(), ""))
    }

    /// Delete this source's entries `ids`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the delete fails, naming the table.
    pub(crate) async fn delete(&self, conn: &mut Conn, ids: &[u64]) -> Result<(), SaciError> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut params: Vec<Value> = vec![Value::from(self.source_name.as_str())];
        params.extend(ids.iter().map(|id| Value::from(*id)));
        exec_closing(
            conn,
            &self.delete_sql(ids.len()),
            Params::Positional(params),
        )
        .await
        .map(drop)
        .map_err(|e| {
            SaciError::generic(format!(
                "{}: cannot prune schema history table '{}': {}; grant DELETE on it",
                self.what,
                self.table,
                mysql_detail(&e)
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::gtid::GtidSet;

    const A: &str = "3e11fa47-71ca-11e1-9e33-c80aa9429562";

    fn file(name: &str, pos: u64) -> Position {
        Position::File {
            name: name.to_string(),
            pos,
        }
    }

    fn gtid(text: &str) -> Position {
        Position::Gtid(text.parse::<GtidSet>().expect("gtid set"))
    }

    fn column(name: &str, ordinal: usize, data_type: &str) -> CatalogColumn {
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

    fn definition(names: &[&str]) -> Arc<Definition> {
        Arc::new(Definition {
            columns: names
                .iter()
                .enumerate()
                .map(|(ordinal, name)| column(name, ordinal, "int"))
                .collect(),
            primary_key: vec![names[0].to_string()],
        })
    }

    fn entry(at: Position, names: &[&str]) -> Entry {
        Entry {
            id: None,
            at,
            definition: definition(names),
        }
    }

    fn names(entry: Option<&Entry>) -> Vec<String> {
        entry.map_or_else(Vec::new, |entry| {
            entry
                .definition
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect()
        })
    }

    #[test]
    fn a_definition_round_trips_through_its_json() {
        let mut wide = column("d", 1, "decimal");
        wide.column_type = "decimal(10,2)".to_string();
        wide.numeric_precision = Some(10);
        wide.numeric_scale = Some(2);
        wide.charset = Some("utf8mb4".to_string());
        let original = Definition {
            columns: vec![column("id", 0, "bigint"), wide],
            primary_key: vec!["id".to_string()],
        };
        let json = original.to_json().expect("serialize");
        assert_eq!(Definition::from_json(&json).expect("parse"), original);
    }

    #[test]
    fn a_definition_of_another_format_or_no_json_is_refused() {
        let err = Definition::from_json(r#"{"format":2,"columns":[],"primary_key":[]}"#)
            .expect_err("another format");
        assert!(err.contains("format 2"), "{err}");
        assert!(Definition::from_json("not json").is_err());
        assert!(Definition::from_json(r#"{"format":1}"#).is_err());
    }

    #[test]
    fn a_column_is_found_without_regard_to_case() {
        let def = definition(&["Id", "note"]);
        assert!(def.has("id") && def.has("NOTE"));
        assert!(!def.has("extra"));
    }

    #[test]
    fn binlog_files_order_by_their_number_not_their_text() {
        assert_eq!(
            file_order("mysql-bin.000009", 4, "mysql-bin.000010", 4),
            Ordering::Less
        );
        // As text `1000000` sorts before `999999`.
        assert_eq!(
            file_order("mysql-bin.999999", 900, "mysql-bin.1000000", 4),
            Ordering::Less
        );
        assert_eq!(
            file_order("mysql-bin.000002", 100, "mysql-bin.000002", 90),
            Ordering::Greater
        );
        assert_eq!(
            file_order("mysql-bin.000002", 100, "mysql-bin.000002", 100),
            Ordering::Equal
        );
        // No number: the text decides.
        assert_eq!(file_order("a", 9, "b", 1), Ordering::Less);
    }

    #[test]
    fn a_file_tag_is_reached_by_its_own_position_and_every_later_one() {
        let tag = file("mysql-bin.000002", 500);
        assert!(!reached(&tag, &file("mysql-bin.000002", 499)));
        assert!(reached(&tag, &file("mysql-bin.000002", 500)));
        assert!(reached(&tag, &file("mysql-bin.000003", 4)));
        assert!(!reached(&tag, &file("mysql-bin.000001", 9000)));
        assert!(!reached(&tag, &gtid(&format!("{A}:1"))), "other kind");
    }

    #[test]
    fn a_gtid_tag_is_reached_by_a_set_that_contains_it() {
        let tag = gtid(&format!("{A}:1-5"));
        assert!(reached(&tag, &gtid(&format!("{A}:1-5"))));
        assert!(reached(&tag, &gtid(&format!("{A}:1-9"))));
        assert!(!reached(&tag, &gtid(&format!("{A}:1-4"))));
        assert!(reached(&gtid(""), &gtid("")));
    }

    #[test]
    fn the_definition_in_force_is_the_newest_tag_at_or_before_the_position() {
        let history = History::new(vec![
            entry(file("f.000001", 4), &["id"]),
            entry(file("f.000001", 900), &["id", "a"]),
            entry(file("f.000002", 300), &["id", "a", "b"]),
        ]);
        let at = |name: &str, pos: u64| names(history.in_force(&file(name, pos)));
        assert_eq!(at("f.000001", 4), ["id"]);
        assert_eq!(at("f.000001", 899), ["id"]);
        assert_eq!(at("f.000001", 900), ["id", "a"]);
        assert_eq!(at("f.000002", 299), ["id", "a"]);
        assert_eq!(at("f.000002", 300), ["id", "a", "b"]);
        assert_eq!(at("f.000009", 4), ["id", "a", "b"]);
        assert!(
            history.in_force(&file("f.000001", 3)).is_none(),
            "before the oldest entry"
        );
    }

    #[test]
    fn entries_are_kept_in_tag_order_whatever_order_they_arrive_in() {
        let history = History::new(vec![
            entry(file("f.000003", 4), &["c"]),
            entry(file("f.000001", 4), &["a"]),
            entry(file("f.000002", 4), &["b"]),
        ]);
        let ahead: Vec<_> = history
            .after(&file("f.000001", 4))
            .iter()
            .map(|entry| names(Some(entry)))
            .collect();
        assert_eq!(ahead, [["b"], ["c"]]);
        assert_eq!(history.after(&file("f.000000", 4)).len(), 3);
        assert!(history.after(&file("f.000003", 4)).is_empty());
    }

    #[test]
    fn of_two_entries_with_one_tag_the_newer_is_in_force() {
        let mut history = History::new(vec![entry(file("f.000001", 4), &["old"])]);
        history.push(entry(file("f.000001", 4), &["new"]));
        assert_eq!(names(history.in_force(&file("f.000001", 4))), ["new"]);
    }

    #[test]
    fn gtid_entries_sort_by_size_and_the_largest_contained_one_is_in_force() {
        let history = History::new(vec![
            entry(gtid(&format!("{A}:1-9")), &["b"]),
            entry(gtid(""), &["a"]),
        ]);
        assert_eq!(names(history.in_force(&gtid(&format!("{A}:1-5")))), ["a"]);
        assert_eq!(names(history.in_force(&gtid(&format!("{A}:1-9")))), ["b"]);
        assert_eq!(names(history.in_force(&gtid(&format!("{A}:1-20")))), ["b"]);
    }

    #[test]
    fn pruning_keeps_the_definition_in_force_and_everything_after() {
        let mut history = History::new(vec![
            entry(file("f.000001", 4), &["a"]),
            entry(file("f.000001", 900), &["b"]),
            entry(file("f.000002", 300), &["c"]),
        ]);
        let removed = history.prune(&file("f.000001", 950));
        assert_eq!(removed.len(), 1);
        assert_eq!(names(Some(&removed[0])), ["a"]);
        assert_eq!(history.len(), 2);
        assert_eq!(names(history.in_force(&file("f.000001", 950))), ["b"]);

        // Nothing is in force before the oldest, so nothing goes.
        assert!(history.prune(&file("f.000001", 5)).is_empty());
        assert_eq!(history.len(), 2);
        // The newest in force: everything before it goes.
        assert_eq!(history.prune(&file("f.000009", 4)).len(), 1);
        assert_eq!(history.len(), 1);
    }

    #[test]
    fn the_statements_name_the_table_and_the_rows_of_one_source() {
        let table = TableRef {
            schema: "shop".to_string(),
            name: "saci_source_offsets_schema".to_string(),
        };
        let watched = TableRef {
            schema: "shop".to_string(),
            name: "orders".to_string(),
        };
        let store = HistoryStore::new("MariadbSource", table, "orders-cdc", &watched, true);
        assert!(
            store
                .load_sql()
                .contains("`shop`.`saci_source_offsets_schema`")
        );
        assert!(
            store
                .load_sql()
                .contains("source_name = ? AND table_name = ?")
        );
        assert!(
            store
                .create_sql()
                .contains("definition_json MEDIUMTEXT NOT NULL")
        );
        assert_eq!(
            store.delete_sql(3),
            "DELETE FROM `shop`.`saci_source_offsets_schema` WHERE source_name = ? AND id IN (?, ?, ?)"
        );
    }
}
