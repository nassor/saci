//! [`PostgresSource`]: the one public source type, over three read modes.
//!
//! The Arrow schema is built from the declared `schema_fields` once, in
//! [`PostgresSource::new`], and handed out by reference from
//! [`Source::schema`]: the trait requires a schema that does not change between
//! calls, so it is never rebuilt.
//!
//! [`new`](PostgresSource::new) opens no connection. It validates the config,
//! builds the schema and the `Connector`, and
//! returns; the first [`next_batch`](Source::next_batch) connects. That is what
//! keeps `saci-service validate` free of a database, and it is forced anyway,
//! because `SourceFactory::build` is synchronous.

pub(crate) mod cursor;
pub(crate) mod logical;
pub(crate) mod pgoutput;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use saci_core::error::SaciError;
use saci_core::io::source::Source;

use crate::config::{PostgresSourceConfig, SourceMode};
use crate::source::cursor::CursorReader;
use crate::source::logical::LogicalReader;

/// A PostgreSQL [`Source`] in one of the three read modes.
pub struct PostgresSource {
    schema: Arc<Schema>,
    /// The mode-specific reader. [`Source::request_batch_rows`] forwards into
    /// whichever variant is active, so `batch_rows` is steerable regardless
    /// of mode.
    reader: Reader,
}

/// The mode-specific half.
///
/// Both readers carry their prepared statements, builders and connection state,
/// so they are boxed: an unboxed enum would make every `PostgresSource` as large
/// as the bigger of the two.
enum Reader {
    /// `polling` and `cdc_trigger`, which differ only in retention.
    Cursor(Box<CursorReader>),
    /// `cdc_logical`.
    Logical(Box<LogicalReader>),
}

impl PostgresSource {
    /// Validate `cfg` and prepare the reader. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for any violation
    /// [`PostgresSourceConfig::validate`] reports, for a DSN that does not
    /// parse, for an unreadable `password_file`, and for a TLS configuration
    /// that cannot be built.
    pub fn new(cfg: PostgresSourceConfig) -> Result<Self, SaciError> {
        cfg.validate()?;

        let fields = cfg
            .schema_fields
            .iter()
            .map(|spec| spec.to_arrow_field())
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Arc::new(Schema::new(fields));

        #[cfg(feature = "tracing")]
        tracing::info!(
            source = %cfg.name,
            mode = cfg.mode.label(),
            columns = cfg.schema_fields.len(),
            "postgres source configured"
        );

        let reader = match &cfg.mode {
            SourceMode::Polling(_) | SourceMode::CdcTrigger(_) => {
                Reader::Cursor(Box::new(CursorReader::new(&cfg)?))
            }
            SourceMode::CdcLogical(_) => Reader::Logical(Box::new(LogicalReader::new(&cfg)?)),
        };

        Ok(Self { schema, reader })
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        match &mut self.reader {
            Reader::Cursor(reader) => reader.next_batch(&self.schema).await,
            Reader::Logical(reader) => reader.next_batch(&self.schema).await,
        }
    }

    /// A plain field write on the active reader's `batch_rows`. `batch_rows`
    /// in config seeds both readers, but `saci-service` sends the hint ahead of
    /// the first query, so the configured value is not a starting size; it is
    /// authoritative only where the host sends no hint, meaning flow control
    /// disabled for this source or a source feeding a windowed node.
    /// `max_batches_per_cycle` is untouched: it bounds batches per drain
    /// cycle, not rows per batch.
    fn request_batch_rows(&mut self, rows: usize) {
        match &mut self.reader {
            Reader::Cursor(reader) => reader.set_batch_rows(rows),
            Reader::Logical(reader) => reader.set_batch_rows(rows),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use saci_connector::from_kdl_str;
    use serde::Deserialize as _;

    const FIELDS: &str = "\nschema_fields \"id\" type=\"int64\" nullable=#false\n";

    const POLLING: &str = "name \"src\"\nbatch_rows 100\n\n\
        connection dsn=\"postgres://h:5432/d\" sslmode=\"disable\"\n\n\
        mode kind=\"polling\" table=\"public.orders\" cursor_column=\"id\"\n";

    fn config(kdl: &str) -> PostgresSourceConfig {
        let text = format!("{kdl}{FIELDS}");
        PostgresSourceConfig::deserialize(from_kdl_str(&text).expect("parse kdl")).expect("parse")
    }

    /// `PostgresSource::request_batch_rows` must reach the cursor reader's
    /// *next query*, not merely a field: the hint has to show up in the
    /// `LIMIT` the source is about to send. A hint of 0 must still clamp to a
    /// query that can advance rather than one that can never return a row.
    #[test]
    fn request_batch_rows_changes_the_cursor_readers_next_query_limit() {
        let mut source = PostgresSource::new(config(POLLING)).expect("polling source builds");

        source.request_batch_rows(64);
        let Reader::Cursor(reader) = &source.reader else {
            panic!("polling mode must build a CursorReader");
        };
        assert!(
            reader.select_sql(cursor::Shape::All).ends_with("LIMIT 64"),
            "the hint must reach the query the source actually sends"
        );

        source.request_batch_rows(0);
        let Reader::Cursor(reader) = &source.reader else {
            panic!("polling mode must build a CursorReader");
        };
        assert!(
            reader.select_sql(cursor::Shape::All).ends_with("LIMIT 1"),
            "a hint of 0 must still be clamped to a query that can advance"
        );
    }
}
