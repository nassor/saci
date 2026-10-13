//! Durable offset for every source mode.
//!
//! One row per source name in a table the connector creates on demand:
//!
//! ```text
//! CREATE TABLE IF NOT EXISTS <offset_table> (
//!   source_name VARCHAR(191) NOT NULL PRIMARY KEY,
//!   cursor_value TEXT NOT NULL,
//!   tiebreak_value TEXT NULL,
//!   updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
//!     ON UPDATE CURRENT_TIMESTAMP(6)
//! )
//! ```
//!
//! A cursor source (`polling`, `cdc_trigger`) holds the cursor and tiebreak
//! values as text, so one table serves integer, timestamp, date and string
//! cursors and an operator can reset a source by editing the row. The declared
//! type of the cursor column supplies the `CAST` that compares the stored text
//! against the typed table column.
//!
//! `tiebreak_value` is a column of its own rather than a delimited pair inside
//! `cursor_value`, because a composite cursor must survive a cycle that
//! `max_batches_per_cycle` cut short: rows sharing the committed cursor value
//! but ordering after the committed tiebreak have not been emitted yet, and
//! resuming on the cursor alone would skip them.
//!
//! A `cdc_binlog` source stores where it stopped in `cursor_value` as
//! `file:<binlog name>:<position>` or `gtid:<set>`, and leaves `tiebreak_value`
//! NULL. The same text is what `mode.initial` accepts as a literal start.

use std::fmt;

use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use saci_core::error::SaciError;

use crate::connection::{
    ER_NO_SUCH_TABLE, Flavor, TableRef, exec_closing, mysql_detail, server_code, upsert_tail,
};

/// A committed cursor position: the ordering value, plus the tiebreak when one
/// is configured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Offset {
    /// The `cursor_column` value, as the server renders it in text.
    pub(crate) cursor: String,
    /// The `tiebreak_column` value, when the source declares one.
    pub(crate) tiebreak: Option<String>,
}

/// A committed binlog position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BinlogOffset {
    /// A binlog file and a byte position in it.
    File {
        /// The binlog file name, such as `mysql-bin.000003`.
        file: String,
        /// The byte position of the next event to read.
        pos: u64,
    },
    /// A GTID set, in the canonical text of `GtidSet`'s `Display`.
    Gtid(String),
}

impl BinlogOffset {
    /// Parse the stored or configured text: `file:<name>:<position>` or
    /// `gtid:<set>`.
    ///
    /// # Errors
    ///
    /// Returns the reason the text is neither form.
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        if let Some(rest) = text.strip_prefix("file:") {
            let (file, pos) = rest
                .rsplit_once(':')
                .ok_or_else(|| "a file position needs a name and a position".to_string())?;
            if file.is_empty() {
                return Err("the binlog file name is empty".to_string());
            }
            let pos = pos
                .parse()
                .map_err(|_| format!("'{pos}' is not a byte position"))?;
            return Ok(BinlogOffset::File {
                file: file.to_string(),
                pos,
            });
        }
        if let Some(set) = text.strip_prefix("gtid:") {
            // An empty set is a real position: a server that has executed
            // nothing, which a restart must resume from, not resolve again.
            return Ok(BinlogOffset::Gtid(set.to_string()));
        }
        Err("an offset starts with 'file:' or 'gtid:'".to_string())
    }
}

/// The text stored in `cursor_value`.
impl fmt::Display for BinlogOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinlogOffset::File { file, pos } => write!(f, "file:{file}:{pos}"),
            BinlogOffset::Gtid(set) => write!(f, "gtid:{set}"),
        }
    }
}

/// The one durable offset row for one source.
pub(crate) struct OffsetStore {
    /// `MariadbSource`, for error prefixes.
    what: &'static str,
    table: TableRef,
    /// `` `schema`.`table` ``.
    quoted: String,
    source_name: String,
    autocreate: bool,
    created: bool,
}

impl OffsetStore {
    /// An offset store over the already resolved `table`.
    pub(crate) fn new(
        what: &'static str,
        table: TableRef,
        source_name: &str,
        autocreate: bool,
    ) -> Self {
        Self {
            what,
            quoted: table.quoted(),
            table,
            source_name: source_name.to_string(),
            autocreate,
            created: false,
        }
    }

    /// The `CREATE TABLE IF NOT EXISTS` statement.
    fn create_sql(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             source_name VARCHAR(191) NOT NULL PRIMARY KEY, \
             cursor_value TEXT NOT NULL, \
             tiebreak_value TEXT NULL, \
             updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) \
             ON UPDATE CURRENT_TIMESTAMP(6))",
            self.quoted
        )
    }

    /// The statement that reads this source's row.
    fn load_sql(&self) -> String {
        format!(
            "SELECT cursor_value, tiebreak_value FROM {} WHERE source_name = ?",
            self.quoted
        )
    }

    /// The upsert that commits one row, shaped for the server that runs it.
    fn store_sql(&self, flavor: Flavor) -> String {
        format!(
            "INSERT INTO {} (source_name, cursor_value, tiebreak_value) VALUES (?, ?, ?){}",
            self.quoted,
            upsert_tail(flavor, &["cursor_value", "tiebreak_value"], "source_name")
        )
    }

    /// Create the offset table if configured to and it is missing, at most
    /// once per connection.
    ///
    /// The catalog is probed first, because `CREATE TABLE IF NOT EXISTS` on a
    /// table that exists still needs the `CREATE` privilege, and a source whose
    /// operator pre-created the table must not.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the probe or the `CREATE TABLE`
    /// fails, which for the latter is usually a missing `CREATE` privilege on
    /// the schema.
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
                    "{}: cannot look up offset table '{}': {}",
                    self.what,
                    self.table,
                    mysql_detail(&e)
                ))
            })?;
        if exists.is_none() {
            conn.query_drop(self.create_sql()).await.map_err(|e| {
                SaciError::generic(format!(
                    "{}: cannot create offset table '{}': {}",
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
    ///
    /// The new session may be a different server, so the check reruns.
    pub(crate) fn reset(&mut self) {
        self.created = false;
    }

    /// The stored offset, or `None` when this source has never committed one.
    ///
    /// A missing offset table reads as `None` rather than an error, so a source
    /// configured `offset_table_autocreate #false` against a table that does
    /// not exist yet fails on [`store`](Self::store), where the message can name
    /// what to create.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the `SELECT` fails for any reason
    /// other than the table not existing.
    pub(crate) async fn load(&self, conn: &mut Conn) -> Result<Option<Offset>, SaciError> {
        let rows = exec_closing(conn, &self.load_sql(), (self.source_name.as_str(),)).await;
        match rows {
            Ok(rows) => Ok(rows.into_iter().next().map(|mut row| Offset {
                cursor: row.take(0).unwrap_or_default(),
                tiebreak: row.take(1).unwrap_or_default(),
            })),
            Err(e) if server_code(&e) == Some(ER_NO_SUCH_TABLE) => Ok(None),
            Err(e) => Err(SaciError::generic(format!(
                "{}: cannot read offset table '{}': {}",
                self.what,
                self.table,
                mysql_detail(&e)
            ))),
        }
    }

    /// Commit `offset` as this source's cursor.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the upsert fails, naming the table.
    pub(crate) async fn store(
        &self,
        conn: &mut Conn,
        flavor: Flavor,
        offset: &Offset,
    ) -> Result<(), SaciError> {
        self.write(conn, flavor, &offset.cursor, offset.tiebreak.as_deref())
            .await
    }

    /// The stored binlog position, or `None` when this source has never
    /// committed one.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the read fails, or when the stored
    /// text is not a position, which means the row was edited by hand.
    pub(crate) async fn load_binlog(
        &self,
        conn: &mut Conn,
    ) -> Result<Option<BinlogOffset>, SaciError> {
        let Some(stored) = self.load(conn).await? else {
            return Ok(None);
        };
        BinlogOffset::parse(&stored.cursor)
            .map(Some)
            .map_err(|reason| {
                SaciError::generic(format!(
                    "{}: the offset row for this source in '{}' is not a binlog position: {reason}",
                    self.what, self.table
                ))
            })
    }

    /// Commit a binlog position.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the upsert fails, naming the table.
    pub(crate) async fn store_binlog(
        &self,
        conn: &mut Conn,
        flavor: Flavor,
        offset: &BinlogOffset,
    ) -> Result<(), SaciError> {
        self.write(conn, flavor, &offset.to_string(), None).await
    }

    async fn write(
        &self,
        conn: &mut Conn,
        flavor: Flavor,
        cursor: &str,
        tiebreak: Option<&str>,
    ) -> Result<(), SaciError> {
        exec_closing(
            conn,
            &self.store_sql(flavor),
            (self.source_name.as_str(), cursor, tiebreak),
        )
        .await
        .map(drop)
        .map_err(|e| {
            SaciError::generic(format!(
                "{}: cannot write offset table '{}': {}",
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

    fn store(table: &str) -> OffsetStore {
        OffsetStore::new(
            "MariadbSource",
            TableRef {
                schema: "meta".to_string(),
                name: table.to_string(),
            },
            "orders-in",
            true,
        )
    }

    #[test]
    fn the_offset_table_is_quoted_with_its_schema() {
        assert_eq!(store("saci_offsets").quoted, "`meta`.`saci_offsets`");
    }

    #[test]
    fn an_injected_table_name_is_quoted_not_interpolated() {
        let store = store("t`; DROP TABLE x; --");
        assert_eq!(store.quoted, "`meta`.`t``; DROP TABLE x; --`");
        assert!(
            store
                .create_sql()
                .contains("`meta`.`t``; DROP TABLE x; --`")
        );
        assert!(store.load_sql().contains("`meta`.`t``; DROP TABLE x; --`"));
    }

    #[test]
    fn the_offset_table_is_created_with_the_documented_shape() {
        assert_eq!(
            store("saci_offsets").create_sql(),
            "CREATE TABLE IF NOT EXISTS `meta`.`saci_offsets` (\
             source_name VARCHAR(191) NOT NULL PRIMARY KEY, \
             cursor_value TEXT NOT NULL, \
             tiebreak_value TEXT NULL, \
             updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) \
             ON UPDATE CURRENT_TIMESTAMP(6))"
        );
    }

    #[test]
    fn the_offset_row_is_read_by_source_name() {
        assert_eq!(
            store("saci_offsets").load_sql(),
            "SELECT cursor_value, tiebreak_value FROM `meta`.`saci_offsets` WHERE source_name = ?"
        );
    }

    #[test]
    fn mariadb_stores_the_offset_with_values() {
        assert_eq!(
            store("saci_offsets").store_sql(Flavor::Mariadb),
            "INSERT INTO `meta`.`saci_offsets` (source_name, cursor_value, tiebreak_value) \
             VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE `cursor_value` = VALUES(`cursor_value`), \
             `tiebreak_value` = VALUES(`tiebreak_value`)"
        );
    }

    #[test]
    fn mysql_stores_the_offset_through_a_row_alias() {
        assert_eq!(
            store("saci_offsets").store_sql(Flavor::Mysql),
            "INSERT INTO `meta`.`saci_offsets` (source_name, cursor_value, tiebreak_value) \
             VALUES (?, ?, ?) AS __saci_new ON DUPLICATE KEY UPDATE \
             `cursor_value` = __saci_new.`cursor_value`, \
             `tiebreak_value` = __saci_new.`tiebreak_value`"
        );
    }

    #[test]
    fn ensure_is_a_no_op_without_autocreate_and_reset_rearms_the_check() {
        let mut store = OffsetStore::new(
            "MariadbSource",
            TableRef {
                schema: "s".to_string(),
                name: "t".to_string(),
            },
            "s",
            false,
        );
        assert!(!store.created);
        store.reset();
        assert!(!store.created);
        let mut with_create = self::store("t");
        with_create.created = true;
        with_create.reset();
        assert!(!with_create.created);
    }

    #[test]
    fn a_file_position_round_trips_through_its_text() {
        let offset = BinlogOffset::File {
            file: "mysql-bin.000003".to_string(),
            pos: 154,
        };
        let text = offset.to_string();
        assert_eq!(text, "file:mysql-bin.000003:154");
        assert_eq!(BinlogOffset::parse(&text), Ok(offset));
    }

    #[test]
    fn a_gtid_position_round_trips_through_its_text() {
        let offset = BinlogOffset::Gtid("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5".to_string());
        let text = offset.to_string();
        assert_eq!(text, "gtid:3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5");
        assert_eq!(BinlogOffset::parse(&text), Ok(offset));
    }

    #[test]
    fn a_file_name_may_hold_a_dot_or_a_colon() {
        assert_eq!(
            BinlogOffset::parse("file:host:bin.000003:4"),
            Ok(BinlogOffset::File {
                file: "host:bin.000003".to_string(),
                pos: 4
            })
        );
    }

    #[test]
    fn a_text_that_is_not_a_position_is_refused_with_the_reason() {
        for (bad, reason) in [
            ("", "starts with"),
            ("12345", "starts with"),
            ("file:", "name and a position"),
            ("file:mysql-bin.000003", "name and a position"),
            ("file::154", "name is empty"),
            ("file:mysql-bin.000003:x", "not a byte position"),
            ("file:mysql-bin.000003:-1", "not a byte position"),
            ("gtid", "starts with"),
        ] {
            let err = BinlogOffset::parse(bad).expect_err(bad);
            assert!(err.contains(reason), "{bad:?}: {err}");
        }
    }
}
