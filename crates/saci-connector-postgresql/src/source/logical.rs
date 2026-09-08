//! The `cdc_logical` reader: `pgoutput` over the SQL replication-slot
//! interface.
//!
//! `tokio-postgres` cannot enter replication mode — its startup packet is
//! private and its backend message parser has no `CopyBothResponse` — so
//! `START_REPLICATION` is unreachable. The SQL interface is a documented peer of
//! the walsender rather than a workaround: `pgoutput` produces identical bytes
//! either way.
//!
//! # One peek per drain cycle
//!
//! `pg_logical_slot_peek_binary_changes` always resumes at the slot's
//! `confirmed_flush_lsn` and takes no lower bound, so a second peek inside one
//! cycle would return overlapping data. And advancing inside a cycle would
//! acknowledge changes before the pipeline has run them. So: one peek, chunk the
//! result into a queue, and advance at the start of the next cycle.
//!
//! `pending_lsn` tracks the highest LSN **emitted**, not the highest decoded, so
//! a cycle that `max_batches_per_cycle` cuts short leaves the undelivered
//! batches behind the slot's confirmed position and the next cycle decodes them
//! again.
//!
//! # LSN text
//!
//! `tokio-postgres` has no `FromSql`/`ToSql` for `pg_lsn`, so LSNs cross the
//! boundary as `text` in PostgreSQL's `XXXXXXXX/XXXXXXXX` form and
//! [`format_lsn`]/[`parse_lsn`] convert.
//!
//! # A column retyped mid-stream
//!
//! Column types are read from the catalog once per session, and every
//! `Relation` message is checked against them. A published type OID that no
//! longer equals the attribute's own `pg_attribute.atttypid` is refused by
//! column name, and the rule covers domains, enums and composites as much as
//! built-ins. Reconnect to pick the new type up.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use saci_core::error::SaciError;
use tokio_postgres::Row;
use tokio_postgres::error::SqlState;

use crate::config::{
    FieldSpec, LogicalMode, PostgresSourceConfig, Role, SourceMode, split_qualified,
};
use crate::connection::{Connector, PgConnection, column_types, pg_detail};
use crate::metrics::Instruments;
use crate::source::pgoutput::{Decoder, Message, Operation, Relation, TupleValue};
use crate::types::{ColumnType, resolve_wire};
use crate::values::{ColumnBuilder, TIMESTAMP_EPOCH_OFFSET_MICROS};

/// Reader over one publication's changes for one table.
pub(crate) struct LogicalReader {
    connector: Connector,
    connection: Option<PgConnection>,

    slot: String,
    publication: String,
    /// `schema.table` as the relation messages report it.
    table: String,
    slot_autocreate: bool,
    slot_ready: bool,
    max_changes_per_cycle: i32,
    batch_rows: usize,
    max_batches_per_cycle: usize,
    fields: Vec<FieldSpec>,
    /// The published table's column types, read from the catalog once per
    /// session. The `Relation` message carries OIDs but no type names, so a
    /// domain, an enum or a composite can only be identified from here.
    columns: Vec<ColumnType>,

    /// Decoded but undelivered batches. The LSN is the newest transaction the
    /// batch completes, or `None` when it ends mid-transaction.
    queue: VecDeque<(RecordBatch, Option<u64>)>,
    /// The highest LSN handed to the pipeline and therefore safe to acknowledge.
    pending_lsn: Option<u64>,
    /// Whether this cycle has already peeked.
    ///
    /// `pg_logical_slot_peek_binary_changes` always resumes at the slot's
    /// `confirmed_flush_lsn`, and the slot only advances at a cycle boundary, so
    /// a second peek inside one cycle would return the same changes again.
    peeked_this_cycle: bool,
    cycle_start: bool,
    batches_this_cycle: usize,

    decoder: Decoder,
    instruments: Instruments,
}

impl LogicalReader {
    /// Build the reader from a validated config. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the table or the connection
    /// cannot be resolved.
    pub(crate) fn new(cfg: &PostgresSourceConfig) -> Result<Self, SaciError> {
        let mode: &LogicalMode = match &cfg.mode {
            SourceMode::CdcLogical(mode) => mode,
            _ => {
                return Err(SaciError::configuration(
                    "PostgresSource: LogicalReader serves mode kind = \"cdc_logical\" only",
                ));
            }
        };
        let (namespace, name) = split_qualified("PostgresSource", &mode.table)?;

        Ok(Self {
            connector: Connector::new("PostgresSource", &cfg.connection)?,
            connection: None,
            slot: mode.slot.clone(),
            publication: mode.publication.clone(),
            table: format!("{namespace}.{name}"),
            slot_autocreate: mode.slot_autocreate,
            slot_ready: false,
            max_changes_per_cycle: mode.max_changes_per_cycle,
            batch_rows: cfg.batch_rows,
            max_batches_per_cycle: cfg.max_batches_per_cycle,
            fields: cfg.schema_fields.clone(),
            columns: Vec::new(),
            queue: VecDeque::new(),
            pending_lsn: None,
            peeked_this_cycle: false,
            cycle_start: false,
            batches_this_cycle: 0,
            decoder: Decoder::default(),
            instruments: Instruments::source(&cfg.name, "cdc_logical"),
        })
    }

    /// A plain field write: sets `batch_rows`, which `decode_rows` reads
    /// fresh off `self` to size the next builder chunk, so it takes effect on
    /// the very next batch. No reconnect, no slot restart. Clamped to at
    /// least 1: a zero-sized chunk would never flush.
    pub(crate) fn set_batch_rows(&mut self, rows: usize) {
        self.batch_rows = rows.max(1);
    }

    /// Pull the next batch, or `Ok(None)` at the end of a drain cycle.
    pub(crate) async fn next_batch(
        &mut self,
        schema: &Arc<Schema>,
    ) -> Result<Option<RecordBatch>, SaciError> {
        self.ensure_session().await?;

        if self.cycle_start {
            self.advance().await?;
            self.cycle_start = false;
            self.batches_this_cycle = 0;
            self.peeked_this_cycle = false;
        }

        if self.max_batches_per_cycle > 0 && self.batches_this_cycle >= self.max_batches_per_cycle {
            // Anything left in the queue is behind the slot's confirmed
            // position and will be decoded again next cycle.
            self.queue.clear();
            self.cycle_start = true;
            return Ok(None);
        }

        if let Some(batch) = self.take_queued() {
            return Ok(Some(batch));
        }

        if self.peeked_this_cycle {
            self.cycle_start = true;
            return Ok(None);
        }

        let started = Instant::now();
        let rows = self.peek().await?;
        self.peeked_this_cycle = true;
        self.instruments.observe(started.elapsed().as_secs_f64());

        self.decode_rows(schema, &rows)?;
        self.refresh_lag().await;

        match self.take_queued() {
            Some(batch) => {
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    slot = %self.slot,
                    table = %self.table,
                    rows = batch.num_rows(),
                    queued = self.queue.len(),
                    elapsed_us = started.elapsed().as_micros(),
                    "postgres logical batch"
                );
                Ok(Some(batch))
            }
            None => {
                self.cycle_start = true;
                Ok(None)
            }
        }
    }

    /// Pop the next queued batch and record what it makes acknowledgeable.
    ///
    /// A batch carries the `Commit` end LSN of the newest transaction it
    /// completes, or `None` when it ends mid-transaction.
    /// `pg_replication_slot_advance` acknowledges whole transactions, so acking
    /// a change's own LSN would leave the slot before that transaction's commit
    /// record and replay it in full.
    fn take_queued(&mut self) -> Option<RecordBatch> {
        let (batch, ack_lsn) = self.queue.pop_front()?;
        if let Some(lsn) = ack_lsn {
            self.pending_lsn = Some(self.pending_lsn.map_or(lsn, |current| current.max(lsn)));
        }
        self.batches_this_cycle += 1;
        self.instruments.batch(batch.num_rows() as u64);
        Some(batch)
    }

    /// Connect if needed, and create the slot on the first call.
    async fn ensure_session(&mut self) -> Result<(), SaciError> {
        if self
            .connection
            .as_ref()
            .is_some_and(|connection| !connection.is_closed())
        {
            return Ok(());
        }

        if self.connection.is_some() {
            // A new session may be a different server; re-check the slot and
            // the table's types.
            self.slot_ready = false;
            self.queue.clear();
            self.columns.clear();
        }

        self.connection = Some(match self.connector.connect_with_retry().await {
            Ok(connection) => connection,
            Err(e) => {
                self.instruments.error("connect");
                return Err(e);
            }
        });

        if !self.slot_ready {
            self.create_slot().await?;
            self.slot_ready = true;
        }
        if self.columns.is_empty() {
            let (namespace, relation) = split_qualified("PostgresSource", &self.table)?;
            let client = self.connection.as_ref().expect("session").client();
            self.columns = column_types(
                client,
                "PostgresSource",
                &namespace,
                &relation,
                &self.table,
                self.connector.target(),
            )
            .await
            .inspect_err(|_| self.instruments.error("query"))?;
        }
        Ok(())
    }

    /// `pg_create_logical_replication_slot`, treating "already exists" as done.
    async fn create_slot(&self) -> Result<(), SaciError> {
        if !self.slot_autocreate {
            return Ok(());
        }
        let client = self.connection.as_ref().expect("session").client();
        match client
            .execute(
                "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
                &[&self.slot],
            )
            .await
        {
            Ok(_) => {
                #[cfg(feature = "tracing")]
                tracing::info!(
                    slot = %self.slot,
                    target_db = %self.connector.target(),
                    "postgres logical replication slot created"
                );
                Ok(())
            }
            Err(e) if e.code() == Some(&SqlState::DUPLICATE_OBJECT) => Ok(()),
            Err(e) => {
                self.instruments.error("slot");
                Err(SaciError::generic(format!(
                    "PostgresSource: cannot create replication slot '{}' on {}: {}{}",
                    self.slot,
                    self.connector.target(),
                    pg_detail(&e),
                    slot_error_hint(&e)
                )))
            }
        }
    }

    /// Acknowledge everything emitted so far by moving the slot forward.
    async fn advance(&mut self) -> Result<(), SaciError> {
        let Some(pending) = self.pending_lsn else {
            return Ok(());
        };
        let client = self.connection.as_ref().expect("session").client();
        let requested = format_lsn(pending);
        let row = client
            .query_one(
                // `$2::text::pg_lsn`, not `$2::pg_lsn`: a bare cast makes
                // PostgreSQL infer the parameter itself as pg_lsn, and the LSN
                // is bound as text.
                "SELECT end_lsn::text FROM pg_replication_slot_advance($1, $2::text::pg_lsn)",
                &[&self.slot, &requested],
            )
            .await
            .map_err(|e| {
                self.instruments.error("slot");
                SaciError::generic(format!(
                    "PostgresSource: cannot advance replication slot '{}' to {requested}: {}",
                    self.slot,
                    pg_detail(&e)
                ))
            })?;

        // The server clamps to the current flush LSN and stops at transaction
        // boundaries, so what it returns is the real confirmed position.
        let confirmed: String = row.get(0);
        let confirmed = parse_lsn(&confirmed)?;
        self.pending_lsn = None;

        #[cfg(feature = "tracing")]
        tracing::info!(
            slot = %self.slot,
            requested = %requested,
            confirmed = %format_lsn(confirmed),
            "postgres logical slot advanced"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = confirmed;
        Ok(())
    }

    /// One peek, returning `(lsn text, xid text, data)` rows in LSN order.
    ///
    /// The `binary` option is deliberately **not** requested, so `pgoutput`
    /// sends each present tuple value as the output function's canonical text.
    /// That is what makes every PostgreSQL type readable here -- an enum, a
    /// composite, a range, a `reg*` name -- because the server renders them
    /// rather than this crate reimplementing 50 send functions in reverse. It
    /// also drops the PostgreSQL 14 floor the `binary` option carried: passing
    /// the key at all raises "unrecognized pgoutput option" on older servers,
    /// so omitting it works from PostgreSQL 10 up with one code path.
    ///
    /// `pg_logical_slot_peek_binary_changes` stays: its "binary" is the
    /// plugin's own `bytea` output, which `pgoutput` always produces, not the
    /// tuple encoding.
    async fn peek(&self) -> Result<Vec<Row>, SaciError> {
        let client = self.connection.as_ref().expect("session").client();
        client
            .query(
                "SELECT lsn::text, xid::text, data FROM pg_logical_slot_peek_binary_changes(\
                 $1, NULL::pg_lsn, $2, 'proto_version', '1', 'publication_names', $3)",
                &[&self.slot, &self.max_changes_per_cycle, &self.publication],
            )
            .await
            .map_err(|e| {
                self.instruments.error("slot");
                SaciError::generic(format!(
                    "PostgresSource: cannot read replication slot '{}' with publication '{}': \
                     {}{}",
                    self.slot,
                    self.publication,
                    pg_detail(&e),
                    slot_error_hint(&e)
                ))
            })
    }

    /// Decode one peek's rows into `batch_rows`-sized batches on the queue.
    fn decode_rows(&mut self, schema: &Arc<Schema>, rows: &[Row]) -> Result<(), SaciError> {
        // The server rebuilds its relation cache per call, so must this.
        self.decoder.reset();

        let mut plan: Option<RelationPlan> = None;
        let mut builders: Option<Vec<ColumnBuilder>> = None;
        let mut rows_in_chunk = 0usize;
        let mut commit_ts: Option<i64> = None;
        let mut skipped = 0u64;
        // Rows decoded so far, so a batch can be tied to a commit boundary.
        let mut rows_decoded = 0usize;
        // `(rows decoded when the transaction committed, its end LSN)`, in
        // ascending order. A batch ending at or after one of these row counts
        // has delivered that whole transaction.
        let mut commit_points: Vec<(usize, u64)> = Vec::new();
        // Finished batches with the cumulative row count at their end.
        let mut produced: Vec<(RecordBatch, usize)> = Vec::new();

        for row in rows {
            let lsn_text: Option<String> = row.try_get(0).map_err(|e| self.row_error("lsn", e))?;
            let lsn = match lsn_text {
                Some(text) => parse_lsn(&text)?,
                None => {
                    return Err(SaciError::generic(
                        "PostgresSource: a slot row carries a NULL lsn".to_string(),
                    ));
                }
            };
            let xid_text: Option<String> = row.try_get(1).map_err(|e| self.row_error("xid", e))?;
            let xid = match xid_text {
                Some(text) => Some(text.parse::<u32>().map(i64::from).map_err(|e| {
                    SaciError::generic(format!(
                        "PostgresSource: slot row at {} carries an unparseable xid '{text}': {e}",
                        format_lsn(lsn)
                    ))
                })?),
                None => None,
            };
            let data: &[u8] = row.try_get(2).map_err(|e| self.row_error("data", e))?;

            let message = self.decoder.decode(data).inspect_err(|_| {
                self.instruments.error("decode");
            })?;

            match message {
                Message::Begin { commit_ts: ts } => commit_ts = Some(ts),
                Message::Commit { end_lsn } => {
                    commit_points.push((rows_decoded, lsn_of_commit(end_lsn)));
                }
                Message::Metadata => {}
                Message::Truncate { relations } => {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        slot = %self.slot,
                        relations,
                        "postgres logical truncate carries no rows"
                    );
                    #[cfg(not(feature = "tracing"))]
                    let _ = relations;
                }
                Message::Relation(rel_id) => {
                    let relation = self
                        .decoder
                        .relation(rel_id)
                        .expect("the decoder just stored it");
                    if relation.qualified() == self.table {
                        plan = Some(RelationPlan::build(&self.fields, relation, &self.columns)?);
                        // A relation whose shape changed mid-peek invalidates
                        // the builders that were sized for the old plan.
                        if let Some(open) = builders.take()
                            && rows_in_chunk > 0
                        {
                            produced.push((self.finish_chunk(schema, open)?, rows_decoded));
                            rows_in_chunk = 0;
                        }
                    }
                }
                Message::Change {
                    rel_id,
                    operation,
                    tuple,
                } => {
                    let Some(relation) = self.decoder.relation(rel_id) else {
                        return Err(SaciError::generic(format!(
                            "PostgresSource: slot row at {} names relation id {rel_id}, which no \
                             Relation message declared",
                            format_lsn(lsn)
                        )));
                    };
                    if relation.qualified() != self.table {
                        skipped += 1;
                        continue;
                    }
                    let plan = plan.as_ref().ok_or_else(|| {
                        SaciError::generic(format!(
                            "PostgresSource: a change for '{}' arrived before its Relation \
                             message",
                            self.table
                        ))
                    })?;

                    let open = match builders.as_mut() {
                        Some(open) => open,
                        None => {
                            builders = Some(plan.builders(&self.fields, self.batch_rows)?);
                            builders.as_mut().expect("just assigned")
                        }
                    };

                    plan.push_row(
                        &self.fields,
                        open,
                        &tuple,
                        operation,
                        lsn,
                        xid,
                        commit_ts,
                        &self.table,
                    )
                    .inspect_err(|_| self.instruments.error("decode"))?;

                    rows_in_chunk += 1;
                    rows_decoded += 1;

                    if rows_in_chunk >= self.batch_rows {
                        let full = builders.take().expect("open builders");
                        produced.push((self.finish_chunk(schema, full)?, rows_decoded));
                        rows_in_chunk = 0;
                    }
                }
            }
        }

        if let Some(open) = builders
            && rows_in_chunk > 0
        {
            produced.push((self.finish_chunk(schema, open)?, rows_decoded));
        }

        // A batch is acknowledgeable only up to the newest transaction it
        // finished; one ending mid-transaction acknowledges nothing.
        for (batch, end_row) in produced {
            let ack_lsn = commit_points
                .iter()
                .rev()
                .find(|(rows, _)| *rows <= end_row)
                .map(|(_, lsn)| *lsn);
            self.queue.push_back((batch, ack_lsn));
        }

        if skipped > 0 {
            self.instruments.skipped(skipped);
            #[cfg(feature = "tracing")]
            tracing::debug!(
                slot = %self.slot,
                table = %self.table,
                skipped,
                "postgres logical changes for other relations skipped"
            );
        }

        Ok(())
    }

    /// Finish `builders` into one batch.
    fn finish_chunk(
        &self,
        schema: &Arc<Schema>,
        mut builders: Vec<ColumnBuilder>,
    ) -> Result<RecordBatch, SaciError> {
        let arrays = builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect::<Vec<_>>();
        RecordBatch::try_new(Arc::clone(schema), arrays).map_err(|e| {
            self.instruments.error("decode");
            SaciError::generic(format!(
                "PostgresSource: cannot assemble a batch of '{}' changes: {e}",
                self.table
            ))
        })
    }

    /// Update the WAL lag gauge. A failure here is not worth failing a batch.
    async fn refresh_lag(&self) {
        let client = self.connection.as_ref().expect("session").client();
        // pg_lsn - pg_lsn yields numeric, which has no ToSql/FromSql here.
        let row = client
            .query_opt(
                "SELECT (pg_current_wal_lsn() - confirmed_flush_lsn)::int8 \
                 FROM pg_replication_slots WHERE slot_name = $1",
                &[&self.slot],
            )
            .await;
        if let Ok(Some(row)) = row
            && let Ok(Some(lag)) = row.try_get::<_, Option<i64>>(0)
        {
            self.instruments.gauge(lag.max(0) as u64);
        }
    }

    fn row_error(&self, column: &str, e: tokio_postgres::Error) -> SaciError {
        SaciError::generic(format!(
            "PostgresSource: cannot read the '{column}' column of replication slot '{}': {}",
            self.slot,
            pg_detail(&e)
        ))
    }
}

/// How the declared fields map onto one relation's columns.
struct RelationPlan {
    /// Index into the relation's tuple for each declared field; `None` for a
    /// reserved metadata column the connector fills itself.
    column_index: Vec<Option<usize>>,
    /// The type to decode each declared field as; the reserved metadata
    /// columns carry a placeholder no server column backs.
    oids: Vec<u32>,
}

impl RelationPlan {
    /// Match declared fields against `relation` by name and check every type.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when a declared non-reserved field is
    /// absent from the relation, or when its declared type cannot hold the
    /// relation column's type.
    fn build(
        fields: &[FieldSpec],
        relation: &Relation,
        columns: &[ColumnType],
    ) -> Result<Self, SaciError> {
        let mut column_index = Vec::with_capacity(fields.len());
        let mut oids = Vec::with_capacity(fields.len());
        let mut problems = Vec::new();

        for spec in fields {
            if reserved_kind(&spec.name).is_some() {
                column_index.push(None);
                oids.push(0);
                continue;
            }
            match relation
                .columns
                .iter()
                .position(|column| column.name == spec.name)
            {
                None => {
                    problems.push(format!(
                        "column '{}' is declared but '{}' does not publish it (published \
                         columns: {})",
                        spec.name,
                        relation.qualified(),
                        relation
                            .columns
                            .iter()
                            .map(|column| column.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                    column_index.push(None);
                    oids.push(0);
                }
                Some(index) => {
                    // The `Relation` message reports the column's own OID,
                    // which for a domain is the domain's, so it compares
                    // against `attribute_oid`; the catalog row is what
                    // carries the base type to decode as and the type's name.
                    let column = columns.iter().find(|column| column.name == spec.name);
                    match column {
                        None => {
                            problems.push(format!(
                                "column '{}' is published by '{}' but the table's catalog entry \
                                 has no such column",
                                spec.name,
                                relation.qualified()
                            ));
                        }
                        Some(column) => {
                            // The published OID must equal the attribute's
                            // own for every column, whatever kind of type it
                            // is: `attribute_oid` is `pg_attribute.atttypid`,
                            // the same value the `Relation` message carries,
                            // so a domain, an enum and a composite compare as
                            // directly as a built-in. A disagreement means
                            // the column's type changed after this session
                            // read the catalog, and decoding it as the old
                            // type would misread every value.
                            let published = relation.columns[index].type_oid;
                            if published != column.attribute_oid {
                                problems.push(format!(
                                    "column '{}' of '{}' is published as oid {published} but the \
                                     catalog says {} (oid {}); the column's type changed after \
                                     this session started, so reconnect to pick it up",
                                    spec.name,
                                    relation.qualified(),
                                    column.display,
                                    column.attribute_oid
                                ));
                            }
                            if let Err(reason) = resolve_wire(spec, column, Role::Source) {
                                problems.push(format!(
                                    "column '{}' of '{}': {reason}",
                                    spec.name,
                                    relation.qualified()
                                ));
                            }
                        }
                    }
                    column_index.push(Some(index));
                    oids.push(column.map_or(0, |column| column.oid));
                }
            }
        }

        if !problems.is_empty() {
            return Err(SaciError::configuration(format!(
                "PostgresSource: declared schema does not match the published relation: {}",
                problems.join("; ")
            )));
        }

        Ok(Self { column_index, oids })
    }

    /// One builder per declared field, sized for `capacity` rows.
    fn builders(
        &self,
        fields: &[FieldSpec],
        capacity: usize,
    ) -> Result<Vec<ColumnBuilder>, SaciError> {
        fields
            .iter()
            .zip(&self.oids)
            .map(|(spec, oid)| ColumnBuilder::new(spec, capacity, *oid))
            .collect()
    }

    /// Append one change as a row.
    #[expect(
        clippy::too_many_arguments,
        reason = "one row's worth of change metadata -- the tuple plus the four reserved \
                  columns' sources -- has no grouping that is not itself used only here"
    )]
    fn push_row(
        &self,
        fields: &[FieldSpec],
        builders: &mut [ColumnBuilder],
        tuple: &[TupleValue<'_>],
        operation: Operation,
        lsn: u64,
        xid: Option<i64>,
        commit_ts: Option<i64>,
        table: &str,
    ) -> Result<(), SaciError> {
        for ((spec, builder), index) in fields
            .iter()
            .zip(builders.iter_mut())
            .zip(&self.column_index)
        {
            match reserved_kind(&spec.name) {
                Some(Reserved::Op) => builder.push_str(&spec.name, operation.as_str())?,
                Some(Reserved::Lsn) => builder.push_i64(&spec.name, lsn as i64)?,
                Some(Reserved::Xid) => match xid {
                    Some(xid) => builder.push_i64(&spec.name, xid)?,
                    None => builder.push_null(),
                },
                Some(Reserved::CommitTs) => match commit_ts {
                    Some(ts) => builder.push_i64(
                        &spec.name,
                        ts.checked_add(TIMESTAMP_EPOCH_OFFSET_MICROS)
                            .ok_or_else(|| {
                                SaciError::generic(format!(
                                    "PostgresSource: commit timestamp {ts} overflows an i64 after \
                                 rebasing to the 1970-01-01 epoch"
                                ))
                            })?,
                    )?,
                    None => builder.push_null(),
                },
                Some(Reserved::Table) => builder.push_str(&spec.name, table)?,
                None => {
                    let index = index.expect("build() proved every data column resolves");
                    let value = tuple.get(index).ok_or_else(|| {
                        SaciError::generic(format!(
                            "PostgresSource: tuple for '{table}' has {} column(s), but column \
                             '{}' sits at index {index}",
                            tuple.len(),
                            spec.name
                        ))
                    })?;
                    match value {
                        // A DELETE, and an UPDATE's old tuple, carry only
                        // replica-identity columns unless the table is REPLICA
                        // IDENTITY FULL; an unchanged TOAST value was not
                        // resent. Both read as NULL.
                        TupleValue::Null | TupleValue::Unchanged => builder.push_null(),
                        // The marker decides, not the declared type: the slot
                        // is read without the `binary` option, so a present
                        // value is the output function's canonical text, and
                        // every declared type parses it. The binary arm stays
                        // because a value whose type has no output function
                        // still arrives tagged `'b'`.
                        TupleValue::Text(bytes) => builder.push_text(&spec.name, Some(bytes))?,
                        TupleValue::Binary(bytes) => builder.push(&spec.name, Some(bytes))?,
                    }
                }
            }
        }
        Ok(())
    }
}

/// The five metadata columns the connector fills from the change stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reserved {
    /// `__op`.
    Op,
    /// `__lsn`.
    Lsn,
    /// `__xid`.
    Xid,
    /// `__commit_ts`.
    CommitTs,
    /// `__table`.
    Table,
}

fn reserved_kind(name: &str) -> Option<Reserved> {
    match name {
        "__op" => Some(Reserved::Op),
        "__lsn" => Some(Reserved::Lsn),
        "__xid" => Some(Reserved::Xid),
        "__commit_ts" => Some(Reserved::CommitTs),
        "__table" => Some(Reserved::Table),
        _ => None,
    }
}

/// Advice appended to slot errors whose SQLSTATE identifies the cause: a
/// missing role attribute, a disabled server setting, or a slot that no
/// longer exists.
fn slot_error_hint(e: &tokio_postgres::Error) -> &'static str {
    match e.code() {
        Some(code) if *code == SqlState::INSUFFICIENT_PRIVILEGE => {
            "; the connecting role needs the REPLICATION attribute or superuser \
             (pg_read_all_data is not sufficient)"
        }
        Some(code) if *code == SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE => {
            "; check that wal_level = logical and max_replication_slots is at least 1"
        }
        Some(code) if *code == SqlState::UNDEFINED_OBJECT => {
            "; this source checks for its slot once, when it connects, so it will not \
             recreate one dropped later: recreate the slot, then stop and start this \
             workflow, though changes committed while the slot was missing are gone \
             for good"
        }
        _ => "",
    }
}

/// Reinterpret a wire LSN as unsigned.
///
/// `pgoutput` writes LSNs as `int64` and PostgreSQL treats them as `uint64`, so
/// the top bit is a magnitude bit, not a sign.
fn lsn_of_commit(end_lsn: i64) -> u64 {
    end_lsn as u64
}

/// PostgreSQL's `pg_lsn` text form: two hex halves joined by `/`.
pub(crate) fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:08X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

/// Parse PostgreSQL's `pg_lsn` text form.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when `text` is not two hexadecimal halves
/// separated by a single `/`.
pub(crate) fn parse_lsn(text: &str) -> Result<u64, SaciError> {
    let malformed = || {
        SaciError::generic(format!(
            "PostgresSource: '{text}' is not a PostgreSQL LSN of the form XXXXXXXX/XXXXXXXX"
        ))
    };
    let (high, low) = text.split_once('/').ok_or_else(malformed)?;
    if low.contains('/') {
        return Err(malformed());
    }
    let high = u32::from_str_radix(high, 16).map_err(|_| malformed())?;
    let low = u32::from_str_radix(low, 16).map_err(|_| malformed())?;
    Ok((u64::from(high) << 32) | u64::from(low))
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use saci_connector::from_kdl_str;
    use serde::Deserialize as _;

    use super::*;
    use crate::source::pgoutput::fixtures;

    #[test]
    fn lsn_text_round_trips() {
        for lsn in [
            0u64,
            1,
            0xFFFF_FFFF,
            0x1_0000_0000,
            0x16B3_74C8_0000_0001,
            u64::MAX,
        ] {
            let text = format_lsn(lsn);
            assert_eq!(parse_lsn(&text).unwrap(), lsn, "text {text}");
        }
        assert_eq!(format_lsn(0), "0/00000000");
        assert_eq!(format_lsn(0x1_0000_00AB), "1/000000AB");
        assert_eq!(parse_lsn("0/0").unwrap(), 0);
        assert_eq!(parse_lsn("1A/2B").unwrap(), 0x1A_0000_002B);
    }

    #[test]
    fn a_malformed_lsn_is_rejected() {
        for text in ["", "0", "0/", "/0", "zz/1", "1/2/3"] {
            assert!(parse_lsn(text).is_err(), "{text} should be rejected");
        }
    }

    fn config(fields: &str) -> PostgresSourceConfig {
        let text = format!(
            "name \"orders\"\nbatch_rows 2\n\n\
             connection dsn=\"postgres://h/d\" sslmode=\"disable\"\n\n\
             mode kind=\"cdc_logical\" slot=\"s\" publication=\"p\" \
             table=\"public.orders\"\n{fields}"
        );
        PostgresSourceConfig::deserialize(from_kdl_str(&text).expect("parse kdl")).expect("parse")
    }

    /// The catalog view of a relation, which a live session reads from
    /// `pg_attribute`. Every fixture column is a built-in, so the published
    /// OID is also the one to decode as.
    fn catalog_columns(relation: &Relation) -> Vec<ColumnType> {
        relation
            .columns
            .iter()
            .map(|column| ColumnType::builtin(column.name.clone(), column.type_oid))
            .collect()
    }

    const FIELDS: &str = "
schema_fields \"__op\" type=\"utf8\" nullable=#false
schema_fields \"__lsn\" type=\"int64\" nullable=#false
schema_fields \"__commit_ts\" type=\"timestamp_micros_utc\"
schema_fields \"__table\" type=\"utf8\" nullable=#false
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"amount\" type=\"decimal128\" precision=18 scale=4
";

    /// Decode a hand-built message sequence the way `decode_rows` would, but
    /// without a server: the pieces under test are `RelationPlan` and the
    /// reserved-column filling.
    fn decode_fixture(
        cfg: &PostgresSourceConfig,
        messages: &[(u64, Option<u32>, Vec<u8>)],
    ) -> Result<Vec<RecordBatch>, SaciError> {
        let schema = Arc::new(arrow_schema::Schema::new(
            cfg.schema_fields
                .iter()
                .map(|spec| spec.to_arrow_field())
                .collect::<Result<Vec<_>, _>>()?,
        ));
        let mut decoder = Decoder::default();
        let mut plan: Option<RelationPlan> = None;
        let mut builders: Option<Vec<ColumnBuilder>> = None;
        let mut commit_ts = None;
        let mut batches = Vec::new();
        let mut rows_in_chunk = 0usize;

        for (lsn, xid, raw) in messages {
            match decoder.decode(raw)? {
                Message::Begin { commit_ts: ts } => commit_ts = Some(ts),
                Message::Relation(rel_id) => {
                    let relation = decoder.relation(rel_id).unwrap();
                    if relation.qualified() == "public.orders" {
                        plan = Some(RelationPlan::build(
                            &cfg.schema_fields,
                            relation,
                            &catalog_columns(relation),
                        )?);
                    }
                }
                Message::Change {
                    rel_id,
                    operation,
                    tuple,
                } => {
                    let relation = decoder
                        .relation(rel_id)
                        .ok_or_else(|| SaciError::generic("unknown relation id".to_string()))?;
                    if relation.qualified() != "public.orders" {
                        continue;
                    }
                    let plan = plan.as_ref().expect("relation first");
                    let open = match builders.as_mut() {
                        Some(open) => open,
                        None => {
                            builders = Some(plan.builders(&cfg.schema_fields, cfg.batch_rows)?);
                            builders.as_mut().unwrap()
                        }
                    };
                    plan.push_row(
                        &cfg.schema_fields,
                        open,
                        &tuple,
                        operation,
                        *lsn,
                        xid.map(i64::from),
                        commit_ts,
                        "public.orders",
                    )?;
                    rows_in_chunk += 1;
                    if rows_in_chunk >= cfg.batch_rows {
                        let mut full = builders.take().unwrap();
                        let arrays = full
                            .iter_mut()
                            .map(ColumnBuilder::finish)
                            .collect::<Vec<_>>();
                        batches.push(RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap());
                        rows_in_chunk = 0;
                    }
                }
                _ => {}
            }
        }
        if let Some(mut open) = builders
            && rows_in_chunk > 0
        {
            let arrays = open
                .iter_mut()
                .map(ColumnBuilder::finish)
                .collect::<Vec<_>>();
            batches.push(RecordBatch::try_new(schema, arrays).unwrap());
        }
        Ok(batches)
    }

    const OID_INT8: u32 = 20;
    const OID_NUMERIC: u32 = 1700;
    const OID_TEXT: u32 = 25;

    #[test]
    fn an_insert_fills_the_reserved_metadata_columns() {
        let cfg = config(FIELDS);
        let id = 7i64.to_be_bytes();
        let mut amount = bytes::BytesMut::new();
        crate::numeric::i128_to_numeric(123_400, 4, &mut amount);

        let batches = decode_fixture(
            &cfg,
            &[
                (1, None, fixtures::begin(0, 42)),
                (
                    2,
                    None,
                    fixtures::relation(
                        9,
                        "public",
                        "orders",
                        &[("id", OID_INT8), ("amount", OID_NUMERIC)],
                    ),
                ),
                (
                    0x1_0000_0002,
                    Some(42),
                    fixtures::insert(9, &[Some(&id), Some(&amount)]),
                ),
                (4, None, fixtures::commit(0)),
            ],
        )
        .expect("decode");

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);

        let op = batch
            .column_by_name("__op")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(op.value(0), "I");

        let lsn = batch
            .column_by_name("__lsn")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(lsn.value(0), 0x1_0000_0002);

        let table = batch
            .column_by_name("__table")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(table.value(0), "public.orders");

        let ts = batch
            .column_by_name("__commit_ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), TIMESTAMP_EPOCH_OFFSET_MICROS);

        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 7);

        let amounts = batch
            .column_by_name("amount")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Decimal128Array>()
            .unwrap();
        assert_eq!(amounts.value(0), 123_400);
    }

    #[test]
    fn insert_update_delete_arrive_in_order_with_their_ops() {
        let cfg = config(FIELDS);
        let one = 1i64.to_be_bytes();
        let two = 2i64.to_be_bytes();
        let relation = fixtures::relation(
            9,
            "public",
            "orders",
            &[("id", OID_INT8), ("amount", OID_NUMERIC)],
        );

        let batches = decode_fixture(
            &cfg,
            &[
                (1, None, fixtures::begin(0, 1)),
                (2, None, relation.clone()),
                (3, Some(1), fixtures::insert(9, &[Some(&one), None])),
                (4, None, fixtures::commit(0)),
                (5, None, fixtures::begin(0, 2)),
                (6, None, relation.clone()),
                (
                    7,
                    Some(2),
                    // pgoutput sends every column of the new tuple, not only
                    // the changed ones.
                    fixtures::update(9, &[Some(&one), None], &[Some(&two), None]),
                ),
                (8, None, fixtures::commit(0)),
                (9, None, fixtures::begin(0, 3)),
                (10, None, relation),
                (11, Some(3), fixtures::delete(9, &[Some(&two), None])),
                (12, None, fixtures::commit(0)),
            ],
        )
        .expect("decode");

        // batch_rows = 2, so three changes land as 2 + 1.
        assert_eq!(batches.len(), 2);
        let ops: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name("__op")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..column.len())
                    .map(|i| column.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ops, vec!["I", "U", "D"]);

        let lsns: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column_by_name("__lsn")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..column.len())
                    .map(|i| column.value(i))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(lsns, vec![3, 7, 11]);
    }

    #[test]
    fn changes_for_another_relation_are_skipped() {
        let cfg = config(FIELDS);
        let one = 1i64.to_be_bytes();
        let batches = decode_fixture(
            &cfg,
            &[
                (1, None, fixtures::begin(0, 1)),
                (
                    2,
                    None,
                    fixtures::relation(
                        9,
                        "public",
                        "orders",
                        &[("id", OID_INT8), ("amount", OID_NUMERIC)],
                    ),
                ),
                (
                    3,
                    None,
                    fixtures::relation(10, "public", "other", &[("id", OID_INT8)]),
                ),
                (4, Some(1), fixtures::insert(10, &[Some(&one)])),
                (5, Some(1), fixtures::insert(9, &[Some(&one), None])),
                (6, None, fixtures::commit(0)),
            ],
        )
        .expect("decode");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn a_change_naming_an_unknown_relation_id_is_rejected() {
        let cfg = config(FIELDS);
        let one = 1i64.to_be_bytes();
        let err = decode_fixture(&cfg, &[(1, Some(1), fixtures::insert(99, &[Some(&one)]))])
            .expect_err("should be rejected");
        assert!(
            err.message().contains("unknown relation"),
            "{}",
            err.message()
        );
    }

    /// The slot is read without the `binary` option, so this is the ordinary
    /// path rather than the error it used to be.
    #[test]
    fn a_text_tagged_value_decodes_through_its_declared_type() {
        let cfg = config(
            "
schema_fields \"id\" type=\"int64\" nullable=#false
",
        );
        let batches = decode_fixture(
            &cfg,
            &[
                (
                    1,
                    None,
                    fixtures::relation(9, "public", "orders", &[("id", OID_INT8)]),
                ),
                (2, Some(1), fixtures::insert_text(9, "42")),
            ],
        )
        .expect("a text tuple is what pgoutput sends by default");
        assert_eq!(batches.len(), 1);
        let ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("int64 column");
        assert_eq!(ids.value(0), 42);
    }

    /// An out-of-line TOAST value the server did not resend arrives as the
    /// `'u'` marker rather than as a value, and reads as NULL -- the same as a
    /// `'n'` column, because neither carries anything to decode.
    #[test]
    fn an_unchanged_toast_value_reads_as_null() {
        let cfg = config(
            "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"label\" type=\"utf8\" nullable=#true
",
        );
        let one = 1i64.to_be_bytes();
        let batches = decode_fixture(
            &cfg,
            &[
                (
                    1,
                    None,
                    fixtures::relation(
                        9,
                        "public",
                        "orders",
                        &[("id", OID_INT8), ("label", OID_TEXT)],
                    ),
                ),
                (
                    2,
                    Some(1),
                    // `label` is present in the relation but unsent.
                    fixtures::insert_unchanged(9, &[Some(&one), Some(b"ignored")], &[1]),
                ),
            ],
        )
        .expect("an unchanged column is not an error");
        assert_eq!(batches.len(), 1);
        let labels = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .expect("utf8 column");
        assert!(labels.is_null(0), "an unsent TOAST value must read as NULL");
    }

    #[test]
    fn a_declared_column_the_publication_omits_is_rejected() {
        let cfg = config(FIELDS);
        let err = decode_fixture(
            &cfg,
            &[(
                1,
                None,
                fixtures::relation(9, "public", "orders", &[("id", OID_INT8)]),
            )],
        )
        .expect_err("should be rejected");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'amount'"), "{}", err.message());
    }

    #[test]
    fn a_published_column_type_that_cannot_fill_the_declared_type_is_rejected() {
        let cfg = config(FIELDS);
        let err = decode_fixture(
            &cfg,
            &[(
                1,
                None,
                fixtures::relation(
                    9,
                    "public",
                    "orders",
                    &[("id", OID_INT8), ("amount", OID_INT8)],
                ),
            )],
        )
        .expect_err("should be rejected");
        assert_eq!(err.category(), "configuration");
        assert!(err.message().contains("'amount'"), "{}", err.message());
        assert!(err.message().contains("int8"), "{}", err.message());
    }

    /// A `Relation` message publishes a domain column's *own* OID, while the
    /// catalog row's `oid` is the base type the decoder needs, so only
    /// `attribute_oid` can tell a retyped domain from a settled one.
    #[test]
    fn a_domain_column_retyped_mid_stream_is_refused_by_name() {
        const DOMAIN_MONEY: u32 = 70_001;
        const DOMAIN_OTHER: u32 = 70_002;

        let cfg = config(FIELDS);
        let build = |published: u32| {
            let mut decoder = Decoder::default();
            decoder
                .decode(&fixtures::relation(
                    9,
                    "public",
                    "orders",
                    &[("id", OID_INT8), ("amount", published)],
                ))
                .expect("decode the relation message");
            let relation = decoder.relation(9).expect("relation 9").clone();
            let columns = vec![
                ColumnType::builtin("id".to_string(), OID_INT8),
                ColumnType {
                    name: "amount".to_string(),
                    oid: OID_NUMERIC,
                    attribute_oid: DOMAIN_MONEY,
                    display: "public.money_amt".to_string(),
                    base_display: "pg_catalog.\"numeric\"".to_string(),
                    typmod: -1,
                    catalog: Some(("public".to_string(), "money_amt".to_string())),
                },
            ];
            RelationPlan::build(&cfg.schema_fields, &relation, &columns)
        };

        if let Err(err) = build(DOMAIN_MONEY) {
            panic!(
                "the domain the catalog reported must still build: {}",
                err.message()
            );
        }

        let Err(err) = build(DOMAIN_OTHER) else {
            panic!("another domain must be refused");
        };
        assert_eq!(err.category(), "configuration");
        let message = err.message();
        assert!(message.contains("'amount'"), "{message}");
        assert!(message.contains("published as oid 70002"), "{message}");
        assert!(
            message.contains("catalog says public.money_amt (oid 70001)"),
            "{message}"
        );
        assert!(
            message.contains("type changed after this session started"),
            "{message}"
        );
    }

    #[test]
    fn a_delete_leaves_non_key_columns_null() {
        let cfg = config(FIELDS);
        let one = 1i64.to_be_bytes();
        let batches = decode_fixture(
            &cfg,
            &[
                (
                    1,
                    None,
                    fixtures::relation(
                        9,
                        "public",
                        "orders",
                        &[("id", OID_INT8), ("amount", OID_NUMERIC)],
                    ),
                ),
                (2, Some(1), fixtures::delete(9, &[Some(&one), None])),
            ],
        )
        .expect("decode");
        let amounts = batches[0].column_by_name("amount").unwrap();
        assert!(amounts.is_null(0));
    }

    #[test]
    fn reserved_names_map_to_their_columns() {
        assert_eq!(reserved_kind("__op"), Some(Reserved::Op));
        assert_eq!(reserved_kind("__lsn"), Some(Reserved::Lsn));
        assert_eq!(reserved_kind("__xid"), Some(Reserved::Xid));
        assert_eq!(reserved_kind("__commit_ts"), Some(Reserved::CommitTs));
        assert_eq!(reserved_kind("__table"), Some(Reserved::Table));
        assert_eq!(reserved_kind("id"), None);
    }

    #[test]
    fn cursor_modes_are_not_logical_modes() {
        let cfg = PostgresSourceConfig::deserialize(
            from_kdl_str(
                "name \"s\"\n\nconnection dsn=\"postgres://h/d\" sslmode=\"disable\"\n\n\
                 mode kind=\"polling\" table=\"t\" cursor_column=\"id\"\n\n\
                 schema_fields \"id\" type=\"int64\"\n",
            )
            .unwrap(),
        )
        .unwrap();
        let Err(err) = LogicalReader::new(&cfg) else {
            panic!("a cursor mode is not a logical mode");
        };
        assert_eq!(err.category(), "configuration");
    }
}
