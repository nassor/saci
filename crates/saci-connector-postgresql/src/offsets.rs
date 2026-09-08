//! Durable cursor for the `polling` and `cdc_trigger` modes.
//!
//! One row per source name in a table the connector creates on demand:
//!
//! ```text
//! CREATE TABLE IF NOT EXISTS <offset_table> (
//!     source_name    text PRIMARY KEY,
//!     cursor_value   text NOT NULL,
//!     tiebreak_value text,
//!     updated_at     timestamptz NOT NULL DEFAULT now()
//! )
//! ```
//!
//! Both cursor parts are held as `text` so one table serves integer, timestamp,
//! date and string cursors, and an operator can reset a source by editing the
//! row. The declared [`PgFieldType`] of `cursor_column` supplies the SQL cast
//! that compares the stored text against the typed table column.
//!
//! [`PgFieldType`]: crate::config::PgFieldType
//!
//! `tiebreak_value` is a column of its own rather than a delimited pair inside
//! `cursor_value`, because a composite cursor must survive a cycle that
//! `max_batches_per_cycle` cut short: rows sharing the committed cursor value
//! but ordering after the committed tiebreak have not been emitted yet, and
//! resuming on the cursor alone would skip them.
//!
//! Table names go through
//! [`quote_qualified`](crate::connection::quote_qualified); every value is a
//! bound parameter.

use saci_core::error::SaciError;
use tokio_postgres::Client;

use crate::connection::{pg_detail, quote_qualified};

/// A committed cursor position: the ordering value, plus the tiebreak when one
/// is configured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Offset {
    /// The `cursor_column` value, as PostgreSQL renders it in text.
    pub(crate) cursor: String,
    /// The `tiebreak_column` value, when the source declares one.
    pub(crate) tiebreak: Option<String>,
}

/// The one durable cursor row for one source.
pub(crate) struct OffsetStore {
    /// Fully quoted `"schema"."table"`.
    table: String,
    /// The unquoted spelling, for error messages.
    display: String,
    source_name: String,
    autocreate: bool,
    created: bool,
}

impl OffsetStore {
    /// Resolve and quote the offset table.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `table` is not `table` or
    /// `schema.table`.
    pub(crate) fn new(
        what: &str,
        table: &str,
        source_name: &str,
        autocreate: bool,
    ) -> Result<Self, SaciError> {
        Ok(Self {
            table: quote_qualified(what, table)?,
            display: table.to_string(),
            source_name: source_name.to_string(),
            autocreate,
            created: false,
        })
    }

    /// Create the offset table if configured to, at most once per connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the `CREATE TABLE` fails, which is
    /// usually a missing `CREATE` privilege on the schema.
    pub(crate) async fn ensure(&mut self, client: &Client) -> Result<(), SaciError> {
        if self.created || !self.autocreate {
            self.created = true;
            return Ok(());
        }
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             source_name text PRIMARY KEY, \
             cursor_value text NOT NULL, \
             tiebreak_value text, \
             updated_at timestamptz NOT NULL DEFAULT now())",
            self.table
        );
        client.batch_execute(&sql).await.map_err(|e| {
            SaciError::generic(format!(
                "PostgresSource: cannot create offset table '{}': {}",
                self.display,
                pg_detail(&e)
            ))
        })?;
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
    /// configured `offset_table_autocreate = false` against a table that does
    /// not exist yet fails on [`store`](Self::store), where the message can name
    /// what to create.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the `SELECT` fails for any reason
    /// other than the table not existing.
    pub(crate) async fn load(&self, client: &Client) -> Result<Option<Offset>, SaciError> {
        let sql = format!(
            "SELECT cursor_value, tiebreak_value FROM {} WHERE source_name = $1",
            self.table
        );
        match client.query_opt(&sql, &[&self.source_name]).await {
            Ok(Some(row)) => Ok(Some(Offset {
                cursor: row.get::<_, String>(0),
                tiebreak: row.get::<_, Option<String>>(1),
            })),
            Ok(None) => Ok(None),
            Err(e) => {
                if e.code() == Some(&tokio_postgres::error::SqlState::UNDEFINED_TABLE) {
                    return Ok(None);
                }
                Err(SaciError::generic(format!(
                    "PostgresSource: cannot read offset table '{}': {}",
                    self.display,
                    pg_detail(&e)
                )))
            }
        }
    }

    /// Commit `offset` as this source's cursor.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the upsert fails, naming the table.
    pub(crate) async fn store(&self, client: &Client, offset: &Offset) -> Result<(), SaciError> {
        let sql = format!(
            "INSERT INTO {} (source_name, cursor_value, tiebreak_value, updated_at) \
             VALUES ($1, $2, $3, now()) \
             ON CONFLICT (source_name) DO UPDATE SET \
             cursor_value = EXCLUDED.cursor_value, \
             tiebreak_value = EXCLUDED.tiebreak_value, \
             updated_at = now()",
            self.table
        );
        client
            .execute(&sql, &[&self.source_name, &offset.cursor, &offset.tiebreak])
            .await
            .map_err(|e| {
                SaciError::generic(format!(
                    "PostgresSource: cannot write offset table '{}': {}",
                    self.display,
                    pg_detail(&e)
                ))
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_are_quoted_and_default_to_public() {
        let store = OffsetStore::new("PostgresSource", "saci_offsets", "s", true).unwrap();
        assert_eq!(store.table, "\"public\".\"saci_offsets\"");
        assert_eq!(store.display, "saci_offsets");

        let store = OffsetStore::new("PostgresSource", "meta.offsets", "s", true).unwrap();
        assert_eq!(store.table, "\"meta\".\"offsets\"");
    }

    #[test]
    fn an_injected_table_name_is_quoted_not_interpolated() {
        let store = OffsetStore::new("PostgresSource", "t\"; DROP TABLE x; --", "s", true).unwrap();
        assert_eq!(store.table, "\"public\".\"t\"\"; DROP TABLE x; --\"");
    }

    #[test]
    fn a_multi_dot_table_is_rejected() {
        let Err(err) = OffsetStore::new("PostgresSource", "a.b.c", "s", true) else {
            panic!("a multi-dot table must be rejected");
        };
        assert_eq!(err.category(), "configuration");
    }

    #[test]
    fn ensure_is_a_no_op_without_autocreate() {
        let mut store = OffsetStore::new("PostgresSource", "t", "s", false).unwrap();
        assert!(!store.created);
        assert!(!store.autocreate);
        store.reset();
        assert!(!store.created);
    }
}
