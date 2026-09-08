//! Durable cursor shared by the `polling` and `cdc` modes.
//!
//! One row per source name in a table the connector creates on demand:
//!
//! ```text
//! CREATE TABLE IF NOT EXISTS <offset_table> (
//!     source_name    TEXT PRIMARY KEY,
//!     cursor_value   TEXT,
//!     tiebreak_value TEXT
//! )
//! ```
//!
//! Both cursor parts are held as `text` so one table serves integer, string and
//! `change_id` cursors, and an operator can reset a source by editing the row.
//! The row lives in the same database the source reads, so it is as durable as
//! the data; on a synced connection it is part of the local replica and syncs,
//! which the connector documents rather than works around.
//!
//! The offset is committed at the *start of the next* fetch, never as rows are
//! handed out, so a crash mid-cycle replays that cycle: delivery is
//! at-least-once, matching the rest of the engine.

use saci_core::error::SaciError;

/// The one durable cursor row for one source.
pub(crate) struct OffsetStore {
    table: String,
}

impl OffsetStore {
    /// A store over `table`.
    pub(crate) fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
        }
    }

    /// Create the table if it does not exist.
    async fn ensure(&self, conn: &turso::Connection, what: &str) -> Result<(), SaciError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (source_name TEXT PRIMARY KEY, \
             cursor_value TEXT, tiebreak_value TEXT)",
            self.table
        );
        conn.execute(&sql, ()).await.map_err(|e| {
            SaciError::generic(format!(
                "{what}: creating offset table '{}': {e}",
                self.table
            ))
        })?;
        Ok(())
    }

    /// The committed cursor for `source`, if any.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the offset table cannot be read.
    pub(crate) async fn load(
        &self,
        conn: &turso::Connection,
        what: &str,
        source: &str,
    ) -> Result<Option<(String, Option<String>)>, SaciError> {
        self.ensure(conn, what).await?;
        let sql = format!(
            "SELECT cursor_value, tiebreak_value FROM {} WHERE source_name = ?1",
            self.table
        );
        let mut rows = conn
            .query(
                &sql,
                turso::params_from_iter([turso::Value::Text(source.to_string())]),
            )
            .await
            .map_err(|e| {
                SaciError::generic(format!("{what}: reading offset row for '{source}': {e}"))
            })?;
        let Some(row) = rows.next().await.map_err(|e| {
            SaciError::generic(format!("{what}: reading offset row for '{source}': {e}"))
        })?
        else {
            return Ok(None);
        };
        let cursor = row
            .get_value(0)
            .map_err(|e| SaciError::generic(format!("{what}: decoding offset cursor: {e}")))?;
        let tiebreak = row
            .get_value(1)
            .map_err(|e| SaciError::generic(format!("{what}: decoding offset tiebreak: {e}")))?;
        Ok(Some((
            cursor.as_text().cloned().unwrap_or_default(),
            tiebreak.as_text().cloned(),
        )))
    }

    /// Record `cursor` (and an optional tiebreak) for `source`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the offset table cannot be written.
    pub(crate) async fn save(
        &self,
        conn: &turso::Connection,
        what: &str,
        source: &str,
        cursor: &str,
        tiebreak: Option<&str>,
    ) -> Result<(), SaciError> {
        self.ensure(conn, what).await?;
        let sql = format!(
            "INSERT INTO {} (source_name, cursor_value, tiebreak_value) VALUES (?1, ?2, ?3) \
             ON CONFLICT(source_name) DO UPDATE SET \
             cursor_value = excluded.cursor_value, tiebreak_value = excluded.tiebreak_value",
            self.table
        );
        let tiebreak = match tiebreak {
            Some(value) => turso::Value::Text(value.to_string()),
            None => turso::Value::Null,
        };
        conn.execute(
            &sql,
            turso::params_from_iter([
                turso::Value::Text(source.to_string()),
                turso::Value::Text(cursor.to_string()),
                tiebreak,
            ]),
        )
        .await
        .map_err(|e| {
            SaciError::generic(format!("{what}: writing offset row for '{source}': {e}"))
        })?;
        Ok(())
    }
}
