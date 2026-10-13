//! [`MariadbSource`]: the one public source type, over three read modes.
//!
//! The Arrow schema is built from the declared `schema_fields` once, in
//! [`MariadbSource::new`], and handed out by reference from [`Source::schema`]:
//! the trait requires a schema that does not change between calls, so it is
//! never rebuilt.
//!
//! [`new`](MariadbSource::new) opens no connection. It validates the config,
//! builds the schema and the mode's reader, and returns; the first
//! [`next_batch`](Source::next_batch) connects. That is what keeps
//! `saci-service validate` free of a database, and it is forced anyway, because
//! `SourceFactory::build` is synchronous.
//!
//! # Readers
//!
//! - `cursor`: `polling`, and `cdc_trigger` over an operator outbox or over the
//!   changelog the `triggers` module installs. One keyset-paginated `SELECT`
//!   shape; the `sequence` module holds back rows behind a gap in an
//!   `AUTO_INCREMENT` cursor.
//! - `binlog`: `cdc_binlog`, the row binlog by file position or by GTID (the
//!   `gtid` module), with an optional consistent `snapshot` first.

pub(crate) mod binlog;
pub(crate) mod cursor;
pub(crate) mod gtid;
pub(crate) mod history;
pub(crate) mod sequence;
pub(crate) mod snapshot;
pub(crate) mod triggers;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use saci_core::error::SaciError;
use saci_core::io::source::Source;

use crate::config::{MariadbSourceConfig, SourceMode};
use crate::source::binlog::BinlogReader;
use crate::source::cursor::CursorReader;

/// A MariaDB or MySQL [`Source`] in one of the three read modes.
pub struct MariadbSource {
    schema: Arc<Schema>,
    /// The mode-specific reader. [`Source::request_batch_rows`] forwards into
    /// whichever variant is active, so `batch_rows` is steerable regardless of
    /// mode.
    reader: Reader,
}

/// The mode-specific half.
///
/// Both readers carry their prepared statements, builders and connection state,
/// so they are boxed: an unboxed enum would make every `MariadbSource` as large
/// as the bigger of the two.
enum Reader {
    /// `polling` and `cdc_trigger`.
    Cursor(Box<CursorReader>),
    /// `cdc_binlog`.
    Binlog(Box<BinlogReader>),
}

impl MariadbSource {
    /// Validate `cfg` and prepare the reader. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for any violation
    /// [`MariadbSourceConfig::validate`] reports, for a DSN that does not parse,
    /// for an unreadable `password_file` or `ssl_ca`, for a bare table name when
    /// the DSN names no database, and for a TLS mode this build lacks.
    pub fn new(cfg: MariadbSourceConfig) -> Result<Self, SaciError> {
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
            "mariadb source configured"
        );

        let reader = match &cfg.mode {
            SourceMode::Polling(_) | SourceMode::CdcTrigger(_) => {
                Reader::Cursor(Box::new(CursorReader::new(&cfg, Arc::clone(&schema))?))
            }
            SourceMode::CdcBinlog(_) => {
                Reader::Binlog(Box::new(BinlogReader::new(&cfg, Arc::clone(&schema))?))
            }
        };

        Ok(Self { schema, reader })
    }
}

#[async_trait]
impl Source for MariadbSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        match &mut self.reader {
            Reader::Cursor(reader) => reader.next_batch().await,
            Reader::Binlog(reader) => reader.next_batch().await,
        }
    }

    /// A plain field write on the active reader's `batch_rows`, clamped to at
    /// least 1 because a `LIMIT 0` would never advance. `batch_rows` in config
    /// seeds both readers, but `saci-service` sends the hint ahead of the first
    /// query, so the configured value is not a starting size; it is
    /// authoritative only where the host sends no hint, meaning flow control
    /// disabled for this source or a source feeding a windowed node.
    /// `max_batches_per_cycle` is untouched: it bounds batches per drain cycle,
    /// not rows per batch.
    fn request_batch_rows(&mut self, rows: usize) {
        let rows = rows.max(1);
        match &mut self.reader {
            Reader::Cursor(reader) => reader.set_batch_rows(rows),
            Reader::Binlog(reader) => reader.set_batch_rows(rows),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, TimeUnit};
    use saci_connector::from_kdl_str;
    use serde::Deserialize as _;

    const FIELDS: &str = "\nschema_fields \"id\" type=\"int64\" nullable=#false\n\
        schema_fields \"label\" type=\"utf8\"\n\
        schema_fields \"at\" type=\"timestamp_micros_utc\"\n";

    const POLLING: &str = "name \"src\"\nbatch_rows 100\n\n\
        connection dsn=\"mysql://h:3306/shop\" ssl_mode=\"disabled\"\n\n\
        mode kind=\"polling\" table=\"shop.orders\" cursor_column=\"id\"\n";

    const TRIGGER: &str = "name \"src\"\n\n\
        connection dsn=\"mysql://h:3306/shop\" ssl_mode=\"disabled\"\n\n\
        mode kind=\"cdc_trigger\" table=\"outbox\" cursor_column=\"id\"\n";

    const BINLOG: &str = "name \"src\"\n\n\
        connection dsn=\"mysql://h:3306/shop\" ssl_mode=\"disabled\"\n\n\
        mode kind=\"cdc_binlog\" table=\"orders\"\n";

    fn config(kdl: &str) -> MariadbSourceConfig {
        let text = format!("{kdl}{FIELDS}");
        MariadbSourceConfig::deserialize(from_kdl_str(&text).expect("parse kdl")).expect("parse")
    }

    #[test]
    fn the_schema_follows_the_declared_fields_in_order() {
        let source = MariadbSource::new(config(POLLING)).expect("polling source builds");
        let schema = source.schema();
        let fields: Vec<_> = schema.fields().iter().collect();
        assert_eq!(
            fields.iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
            vec!["id", "label", "at"]
        );
        assert_eq!(fields[0].data_type(), &DataType::Int64);
        assert!(!fields[0].is_nullable());
        assert!(fields[1].is_nullable());
        assert_eq!(
            fields[2].data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn each_mode_builds_the_reader_that_serves_it() {
        let polling = MariadbSource::new(config(POLLING)).expect("polling");
        assert!(matches!(polling.reader, Reader::Cursor(_)));
        let trigger = MariadbSource::new(config(TRIGGER)).expect("trigger");
        assert!(matches!(trigger.reader, Reader::Cursor(_)));
        let binlog = MariadbSource::new(config(BINLOG)).expect("binlog");
        assert!(matches!(binlog.reader, Reader::Binlog(_)));
    }

    #[test]
    fn building_a_source_opens_no_connection() {
        // Nothing listens on port 1; a source that dialled at build time would
        // fail here or hang.
        let text = POLLING.replace("h:3306", "127.0.0.1:1");
        MariadbSource::new(config(&text)).expect("build touches no socket");
    }

    #[test]
    fn a_config_that_fails_validation_never_builds_a_reader() {
        let err = MariadbSource::new(config(&POLLING.replace("batch_rows 100", "batch_rows 0")))
            .err()
            .expect("batch_rows 0 is refused");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("batch_rows"), "{}", err.message());
    }

    #[test]
    fn a_bare_table_with_no_database_in_the_dsn_is_refused_at_build() {
        let text = POLLING
            .replace("h:3306/shop", "h:3306")
            .replace("shop.orders", "orders");
        let err = MariadbSource::new(config(&text))
            .err()
            .expect("a bare table needs the DSN's database");
        assert_eq!(err.category(), "configuration");
        assert!(
            err.message()
                .contains("connection.dsn names no database, so mode.table 'orders'"),
            "{}",
            err.message()
        );
    }

    /// `request_batch_rows` must reach the active reader's batch size, and a
    /// hint of 0 must still leave a size that can advance rather than a
    /// `LIMIT 0` that never returns a row.
    #[test]
    fn request_batch_rows_reaches_the_active_reader_and_clamps_zero_to_one() {
        let mut source = MariadbSource::new(config(POLLING)).expect("polling source builds");
        source.request_batch_rows(64);
        let Reader::Cursor(reader) = &source.reader else {
            panic!("polling mode must build a CursorReader");
        };
        assert_eq!(reader.batch_rows(), 64);
        source.request_batch_rows(0);
        let Reader::Cursor(reader) = &source.reader else {
            panic!("polling mode must build a CursorReader");
        };
        assert_eq!(reader.batch_rows(), 1);

        let mut source = MariadbSource::new(config(BINLOG)).expect("binlog source builds");
        source.request_batch_rows(7);
        let Reader::Binlog(reader) = &source.reader else {
            panic!("cdc_binlog mode must build a BinlogReader");
        };
        assert_eq!(reader.batch_rows(), 7);
        source.request_batch_rows(0);
        let Reader::Binlog(reader) = &source.reader else {
            panic!("cdc_binlog mode must build a BinlogReader");
        };
        assert_eq!(reader.batch_rows(), 1);
    }

    #[test]
    fn the_configured_batch_rows_seed_the_reader() {
        let source = MariadbSource::new(config(POLLING)).expect("polling source builds");
        let Reader::Cursor(reader) = &source.reader else {
            panic!("polling mode must build a CursorReader");
        };
        assert_eq!(reader.batch_rows(), 100);
    }
}
