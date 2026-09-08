//! [`TursoSource`]: one public source type over three read strategies.
//!
//! The Arrow schema is built from the declared `schema_fields` once, in
//! [`TursoSource::new`], and handed out by reference from [`Source::schema`]:
//! the trait requires a schema that does not change between calls, so it is
//! never rebuilt.
//!
//! [`TursoSource::new`] opens no connection. It validates the config and builds
//! the schema, and the first [`Source::next_batch`] opens the database and
//! connects. That is what keeps `saci-service validate` free of a database, and
//! it is forced anyway, because `SourceFactory::build` is synchronous while the
//! `turso` builders are asynchronous.
//!
//! # Read strategies
//!
//! `kind="polling"` runs an incremental query ordered by a cursor column,
//! resuming from a durable offset. It sees inserts, and updates only when the
//! cursor column is an `updated_at`-style value the writer bumps. It never sees
//! deletes.
//!
//! `kind="dump"` re-reads the whole table every cycle.
//!
//! `kind="cdc"` reads the change table the engine writes when a connection has
//! `PRAGMA capture_data_changes_conn` enabled, so inserts, updates and deletes
//! all arrive. Capture is **per connection**: only changes made through a
//! capture-enabled connection are recorded, so a missing change table is a loud
//! error naming that pragma rather than an empty stream.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use saci_core::error::SaciError;
use saci_core::io::source::Source;

use crate::cdc::{decode_cdc_field, op_of};
use crate::config::{
    CdcMode, CursorMode, DumpMode, FieldSpec, Retention, TursoFieldType, TursoSourceConfig,
};
use crate::connection::Db;
use crate::metrics::Instruments;
use crate::offsets::OffsetStore;
use crate::types::{ColBuilder, Scalar, decode_value, variant_name};

/// A Turso [`Source`] in one of the three read modes.
pub struct TursoSource {
    config: TursoSourceConfig,
    schema: Arc<Schema>,
    db: Option<Db>,
    conn: Option<turso::Connection>,
    state: State,
    metrics: Instruments,
}

/// The mode-specific half.
enum State {
    Polling(PollingState),
    Dump(DumpState),
    Cdc(CdcState),
}

/// Incremental cursor state.
struct PollingState {
    mode: CursorMode,
    store: OffsetStore,
    committed: Option<(String, Option<String>)>,
    pending: Option<(String, Option<String>)>,
    loaded: bool,
    batches: usize,
    cycle_done: bool,
    pulled: bool,
}

/// Full-table-read state. `next_offset` paginates one scan and resets when the
/// table is exhausted, so the next scan re-reads the whole table. A
/// `max_batches_per_cycle` yield keeps it, so the scan resumes where it
/// stopped.
struct DumpState {
    mode: DumpMode,
    next_offset: usize,
    batches: usize,
    cycle_done: bool,
    pulled: bool,
}

/// Change-capture state.
struct CdcState {
    mode: CdcMode,
    store: OffsetStore,
    committed: i64,
    pending: Option<i64>,
    loaded: bool,
    batches: usize,
    cycle_done: bool,
    pulled: bool,
}

/// A batch and the cursor position it advanced to.
struct Emitted {
    batch: RecordBatch,
    cursor: Option<String>,
    tiebreak: Option<String>,
}

/// One change-table read.
struct CdcFetch {
    emitted: Option<Emitted>,
    last_change_id: Option<i64>,
    caught_up: bool,
}

impl TursoSource {
    /// Validate `config` and build the Arrow schema. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for any invalid key.
    pub fn new(config: TursoSourceConfig) -> Result<Self, SaciError> {
        config.validate()?;
        let fields = config
            .schema_fields
            .iter()
            .map(FieldSpec::to_arrow_field)
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Arc::new(Schema::new(fields));
        let mode = config.mode.as_str();
        let metrics = Instruments::source(&config.name, mode);
        let state = match config.mode.clone() {
            crate::config::SourceMode::Polling(mode) => State::Polling(PollingState {
                store: OffsetStore::new(mode.offset_table.clone()),
                mode,
                committed: None,
                pending: None,
                loaded: false,
                batches: 0,
                cycle_done: false,
                pulled: false,
            }),
            crate::config::SourceMode::Dump(mode) => State::Dump(DumpState {
                mode,
                next_offset: 0,
                batches: 0,
                cycle_done: false,
                pulled: false,
            }),
            crate::config::SourceMode::Cdc(mode) => State::Cdc(CdcState {
                store: OffsetStore::new(mode.offset_table.clone()),
                mode,
                committed: 0,
                pending: None,
                loaded: false,
                batches: 0,
                cycle_done: false,
                pulled: false,
            }),
        };
        Ok(Self {
            config,
            schema,
            db: None,
            conn: None,
            state,
            metrics,
        })
    }

    /// The connector's name, for error prefixes.
    fn what(&self) -> String {
        format!("source '{}'", self.config.name)
    }

    /// Open the database and connect, once.
    async fn ensure_open(&mut self) -> Result<(), SaciError> {
        if self.conn.is_some() {
            return Ok(());
        }
        let what = self.what();
        let db = Db::open(&self.config.connection, &what).await?;
        let conn = db.connect(&self.config.connection, &what).await?;
        self.db = Some(db);
        self.conn = Some(conn);
        Ok(())
    }
}

#[async_trait]
impl Source for TursoSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        self.ensure_open().await?;
        let what = self.what();

        // A synced source pulls once at the start of each cycle, so a whole
        // drain sees one snapshot instead of a pull per batch.
        if self.config.sync.pull_before_read {
            let first = match &mut self.state {
                State::Polling(state) => {
                    let first = !state.pulled;
                    state.pulled = true;
                    first
                }
                State::Dump(state) => {
                    let first = !state.pulled;
                    state.pulled = true;
                    first
                }
                State::Cdc(state) => {
                    let first = !state.pulled;
                    state.pulled = true;
                    first
                }
            };
            if first {
                let db = self.db.as_ref().expect("opened above");
                db.pull(&self.config.connection, &what).await?;
            }
        }

        let TursoSource {
            config,
            schema,
            conn,
            state,
            metrics,
            ..
        } = self;
        let conn = conn.as_ref().expect("opened above");
        match state {
            State::Polling(state) => poll_next(config, schema, conn, state, metrics, &what).await,
            State::Dump(state) => dump_next(config, schema, conn, state, metrics, &what).await,
            State::Cdc(state) => cdc_next(config, schema, conn, state, metrics, &what).await,
        }
    }
}

/// The `polling` read: commit the previous position, then fetch the next batch.
async fn poll_next(
    config: &TursoSourceConfig,
    schema: &Arc<Schema>,
    conn: &turso::Connection,
    state: &mut PollingState,
    metrics: &Instruments,
    what: &str,
) -> Result<Option<RecordBatch>, SaciError> {
    let cap = if config.max_batches_per_cycle == 0 {
        usize::MAX
    } else {
        config.max_batches_per_cycle
    };
    if state.cycle_done {
        state.cycle_done = false;
        state.batches = 0;
        state.pulled = false;
        return Ok(None);
    }
    if !state.loaded {
        state.committed = state.store.load(conn, what, &config.name).await?;
        state.loaded = true;
    }
    // Commit the previous batch's cursor before fetching beyond it. A crash
    // before this point replays that batch: at-least-once.
    if let Some((cursor, tiebreak)) = state.pending.take() {
        state
            .store
            .save(conn, what, &config.name, &cursor, tiebreak.as_deref())
            .await?;
        state.committed = Some((cursor, tiebreak));
    }

    let fields = &config.schema_fields;
    let cursor_idx = fields
        .iter()
        .position(|f| f.name == state.mode.cursor_column);
    let tie_idx = state
        .mode
        .tiebreak_column
        .as_ref()
        .and_then(|name| fields.iter().position(|f| f.name == *name));
    let cursor_field = fields
        .iter()
        .find(|f| f.name == state.mode.cursor_column)
        .expect("validated: cursor_column is a declared field");
    let tie_field = state
        .mode
        .tiebreak_column
        .as_ref()
        .and_then(|name| fields.iter().find(|f| f.name == *name));

    let mut params = Vec::new();
    let mut sql = format!("SELECT {} FROM {}", column_list(fields), state.mode.table);
    if let Some((cursor, tiebreak)) = &state.committed {
        params.push(cursor_param(cursor_field, cursor, what)?);
        match (tie_field, tiebreak) {
            (Some(tie_field), Some(tiebreak)) => {
                params.push(cursor_param(tie_field, tiebreak, what)?);
                sql.push_str(&format!(
                    " WHERE ({} > ?1 OR ({} = ?1 AND {} > ?2))",
                    state.mode.cursor_column, state.mode.cursor_column, tie_field.name
                ));
            }
            _ => {
                sql.push_str(&format!(" WHERE {} > ?1", state.mode.cursor_column));
            }
        }
    }
    sql.push_str(&format!(" ORDER BY {}", state.mode.cursor_column));
    if let Some(tie) = &state.mode.tiebreak_column {
        sql.push_str(&format!(", {tie}"));
    }
    sql.push_str(&format!(" LIMIT {}", config.batch_rows));

    let fetched = fetch_columns(
        conn, &sql, params, fields, schema, cursor_idx, tie_idx, what,
    )
    .await?;
    let Some(emitted) = fetched else {
        state.batches = 0;
        state.pulled = false;
        return Ok(None);
    };
    let rows = emitted.batch.num_rows() as u64;
    state.pending = Some((
        emitted.cursor.clone().unwrap_or_default(),
        emitted.tiebreak.clone(),
    ));
    state.batches += 1;
    if state.batches >= cap {
        state.cycle_done = true;
    }
    metrics.source_batch(rows);
    Ok(Some(emitted.batch))
}

/// The `dump` read: paginate the whole table, restarting the scan once the
/// table is exhausted.
async fn dump_next(
    config: &TursoSourceConfig,
    schema: &Arc<Schema>,
    conn: &turso::Connection,
    state: &mut DumpState,
    metrics: &Instruments,
    what: &str,
) -> Result<Option<RecordBatch>, SaciError> {
    let cap = if config.max_batches_per_cycle == 0 {
        usize::MAX
    } else {
        config.max_batches_per_cycle
    };
    // A cap yield keeps `next_offset`, so the scan is still mid-table: leaving
    // `pulled` set holds the snapshot the scan started on. A pull here would
    // shift rows under an unordered LIMIT/OFFSET page and duplicate or skip
    // them.
    if state.cycle_done {
        state.cycle_done = false;
        state.batches = 0;
        return Ok(None);
    }
    let sql = format!(
        "SELECT {} FROM {} LIMIT {} OFFSET {}",
        column_list(&config.schema_fields),
        state.mode.table,
        config.batch_rows,
        state.next_offset
    );
    let fetched = fetch_columns(
        conn,
        &sql,
        Vec::new(),
        &config.schema_fields,
        schema,
        None,
        None,
        what,
    )
    .await?;
    let Some(emitted) = fetched else {
        state.next_offset = 0;
        state.batches = 0;
        state.pulled = false;
        return Ok(None);
    };
    state.next_offset += emitted.batch.num_rows();
    state.batches += 1;
    if state.batches >= cap {
        state.cycle_done = true;
    }
    metrics.source_batch(emitted.batch.num_rows() as u64);
    Ok(Some(emitted.batch))
}

/// The `cdc` read: commit the previous position, then read change records.
///
/// An insert, update or delete carries the changed row; a COMMIT record carries
/// none and is skipped, though its `change_id` still advances the cursor so it
/// is never re-read.
async fn cdc_next(
    config: &TursoSourceConfig,
    schema: &Arc<Schema>,
    conn: &turso::Connection,
    state: &mut CdcState,
    metrics: &Instruments,
    what: &str,
) -> Result<Option<RecordBatch>, SaciError> {
    let cap = if config.max_batches_per_cycle == 0 {
        usize::MAX
    } else {
        config.max_batches_per_cycle
    };
    if state.cycle_done {
        state.cycle_done = false;
        state.batches = 0;
        state.pulled = false;
        return Ok(None);
    }
    if !state.loaded {
        let row = state.store.load(conn, what, &config.name).await?;
        state.committed = match row {
            Some((cursor, _)) => cursor.trim().parse::<i64>().map_err(|e| {
                SaciError::configuration(format!(
                    "{what}: stored change cursor '{cursor}' is not an integer: {e}"
                ))
            })?,
            None => 0,
        };
        state.loaded = true;
        state.pending = None;
    }
    if let Some(pending) = state.pending.take() {
        state
            .store
            .save(conn, what, &config.name, &pending.to_string(), None)
            .await?;
        if state.mode.retention == Retention::DeleteAcked {
            delete_acked(
                conn,
                &state.mode.cdc_table,
                &state.mode.table,
                pending,
                what,
            )
            .await?;
        }
        state.committed = pending;
    }

    // Verify the change table exists before reading, so its absence is a named
    // configuration error rather than an engine failure.
    ensure_cdc_table(conn, &state.mode.cdc_table, what).await?;

    let mut cursor = state.committed;
    let mut fetches = 0usize;
    loop {
        fetches += 1;
        let fetch = fetch_cdc(
            conn,
            &state.mode,
            cursor,
            config.batch_rows,
            &config.schema_fields,
            schema,
            what,
        )
        .await?;
        if let Some(last) = fetch.last_change_id {
            state.pending = Some(last);
            cursor = last;
        }
        if let Some(emitted) = fetch.emitted {
            let rows = emitted.batch.num_rows() as u64;
            state.batches += 1;
            if state.batches >= cap {
                state.cycle_done = true;
            }
            metrics.changes(rows);
            metrics.source_batch(rows);
            return Ok(Some(emitted.batch));
        }
        if fetch.caught_up || fetches >= cap {
            state.batches = 0;
            state.pulled = false;
            return Ok(None);
        }
    }
}

/// Refuse a `cdc` read when the change table does not exist.
async fn ensure_cdc_table(
    conn: &turso::Connection,
    cdc_table: &str,
    what: &str,
) -> Result<(), SaciError> {
    let mut rows = conn
        .query(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            turso::params_from_iter([turso::Value::Text(cdc_table.to_string())]),
        )
        .await
        .map_err(|e| SaciError::generic(format!("{what}: looking for change table: {e}")))?;
    match rows
        .next()
        .await
        .map_err(|e| SaciError::generic(format!("{what}: looking for change table: {e}")))?
    {
        Some(_) => Ok(()),
        None => Err(SaciError::configuration(format!(
            "{what}: change table '{cdc_table}' does not exist; run \
             PRAGMA capture_data_changes_conn('full') on the connection(s) that \
             write to this database, because capture is per connection"
        ))),
    }
}

/// Delete change records the cursor has passed.
async fn delete_acked(
    conn: &turso::Connection,
    cdc_table: &str,
    table: &str,
    upto: i64,
    what: &str,
) -> Result<(), SaciError> {
    let sql = format!("DELETE FROM {cdc_table} WHERE change_id <= ?1 AND table_name = ?2");
    conn.execute(
        &sql,
        turso::params_from_iter([
            turso::Value::Integer(upto),
            turso::Value::Text(table.to_string()),
        ]),
    )
    .await
    .map_err(|e| SaciError::generic(format!("{what}: pruning acknowledged changes: {e}")))?;
    Ok(())
}

/// Read one change-table batch.
async fn fetch_cdc(
    conn: &turso::Connection,
    mode: &CdcMode,
    cursor: i64,
    limit: usize,
    fields: &[FieldSpec],
    schema: &Arc<Schema>,
    what: &str,
) -> Result<CdcFetch, SaciError> {
    let table_columns = format!("table_columns_json_array('{}')", mode.table);
    // v2 change table: `(change_id, change_time, change_txn_id, change_type,
    // table_name, id, before, after, updates)`. COMMIT rows (`change_type = 2`)
    // carry no image and are skipped below.
    let sql = format!(
        "SELECT change_id, change_time, change_txn_id, change_type, table_name, id, \
         bin_record_json_object({table_columns}, after), \
         bin_record_json_object({table_columns}, before) \
         FROM {} WHERE change_id > ?1 AND table_name = ?2 ORDER BY change_id LIMIT {limit}",
        mode.cdc_table
    );
    let mut builders = new_builders(fields)?;
    let mut count = 0usize;
    let mut fetched = 0usize;
    let mut last_change_id = None;
    let mut rows = conn
        .query(
            &sql,
            turso::params_from_iter([
                turso::Value::Integer(cursor),
                turso::Value::Text(mode.table.clone()),
            ]),
        )
        .await
        .map_err(|e| query_error(what, e))?;
    while let Some(row) = rows.next().await.map_err(|e| query_error(what, e))? {
        fetched += 1;
        let change_id = int_column(&row, 0, what)?;
        last_change_id = Some(change_id);
        let change_time = int_column(&row, 1, what)?;
        let txn_id = int_column(&row, 2, what)?;
        let change_type = int_column(&row, 3, what)?;
        let table_name = row
            .get_value(4)
            .map_err(|e| decode_error(what, e))?
            .as_text()
            .cloned()
            .unwrap_or_default();
        let rowid = match row.get_value(5).map_err(|e| decode_error(what, e))? {
            turso::Value::Integer(i) => Some(i),
            _ => None,
        };
        let Some(op) = op_of(change_type) else {
            // A COMMIT row carries no image; it still advanced the cursor
            // above, so it is never re-read.
            continue;
        };
        let after = json_image(row.get_value(6).map_err(|e| decode_error(what, e))?, what)?;
        let before = json_image(row.get_value(7).map_err(|e| decode_error(what, e))?, what)?;
        let image = if op == "D" { &before } else { &after };
        for (idx, field) in fields.iter().enumerate() {
            let scalar = if field.name.starts_with("__") {
                reserved_scalar(
                    &field.name,
                    change_id,
                    change_time,
                    txn_id,
                    &table_name,
                    rowid,
                    op,
                )
            } else {
                decode_cdc_field(field, count, image)
                    .map_err(|e| SaciError::generic(format!("{what}: {e}")))?
            };
            builders[idx]
                .append(scalar)
                .map_err(|e| SaciError::generic(format!("{what}: {e}")))?;
        }
        count += 1;
    }
    let emitted = if count == 0 {
        None
    } else {
        Some(Emitted {
            batch: finish_batch(builders, schema)?,
            cursor: None,
            tiebreak: None,
        })
    };
    Ok(CdcFetch {
        emitted,
        last_change_id,
        caught_up: fetched < limit,
    })
}

/// Read one row's column as an integer.
fn int_column(row: &turso::Row, idx: usize, what: &str) -> Result<i64, SaciError> {
    match row.get_value(idx).map_err(|e| decode_error(what, e))? {
        turso::Value::Integer(i) => Ok(i),
        other => Err(SaciError::generic(format!(
            "{what}: change column {idx} expected an integer, got {}",
            variant_name(&other)
        ))),
    }
}

/// The scalar a reserved `__`-prefixed CDC field carries.
fn reserved_scalar(
    name: &str,
    change_id: i64,
    change_time: i64,
    txn_id: i64,
    table: &str,
    rowid: Option<i64>,
    op: &str,
) -> Option<Scalar> {
    Some(match name {
        "__op" => Scalar::Utf8(op.to_string()),
        "__change_id" => Scalar::Int64(change_id),
        "__change_time" => Scalar::Int64(change_time),
        "__txn_id" => Scalar::Int64(txn_id),
        "__table" => Scalar::Utf8(table.to_string()),
        "__rowid" => Scalar::Int64(rowid?),
        _ => return None,
    })
}

/// Parse a decoded change image into a JSON object.
fn json_image(
    value: turso::Value,
    what: &str,
) -> Result<serde_json::Map<String, serde_json::Value>, SaciError> {
    let text = match value {
        turso::Value::Text(text) => text,
        turso::Value::Null => return Ok(serde_json::Map::new()),
        other => {
            return Err(SaciError::generic(format!(
                "{what}: change image expected text, got {}",
                variant_name(&other)
            )));
        }
    };
    match serde_json::from_str::<serde_json::Value>(&text) {
        Ok(serde_json::Value::Object(map)) => Ok(map),
        Ok(_) => Err(SaciError::generic(format!(
            "{what}: change image is not a JSON object"
        ))),
        Err(e) => Err(SaciError::generic(format!(
            "{what}: decoding change image: {e}"
        ))),
    }
}

/// Fetch `fields` from `sql`, building one batch and reporting the last row's
/// cursor/tiebreak text when asked.
#[allow(clippy::too_many_arguments)]
async fn fetch_columns(
    conn: &turso::Connection,
    sql: &str,
    params: Vec<turso::Value>,
    fields: &[FieldSpec],
    schema: &Arc<Schema>,
    cursor_idx: Option<usize>,
    tie_idx: Option<usize>,
    what: &str,
) -> Result<Option<Emitted>, SaciError> {
    let mut builders = new_builders(fields)?;
    let mut count = 0usize;
    let mut cursor = None;
    let mut tiebreak = None;
    let mut rows = conn
        .query(sql, turso::params_from_iter(params))
        .await
        .map_err(|e| query_error(what, e))?;
    while let Some(row) = rows.next().await.map_err(|e| query_error(what, e))? {
        for (idx, field) in fields.iter().enumerate() {
            let value = row.get_value(idx).map_err(|e| decode_error(what, e))?;
            if Some(idx) == cursor_idx {
                cursor = Some(value_to_text(&value));
            }
            if Some(idx) == tie_idx {
                tiebreak = Some(value_to_text(&value));
            }
            let scalar = decode_value(field, count, value)
                .map_err(|e| SaciError::generic(format!("{what}: {e}")))?;
            builders[idx]
                .append(scalar)
                .map_err(|e| SaciError::generic(format!("{what}: {e}")))?;
        }
        count += 1;
    }
    if count == 0 {
        return Ok(None);
    }
    Ok(Some(Emitted {
        batch: finish_batch(builders, schema)?,
        cursor,
        tiebreak,
    }))
}

/// Build one `RecordBatch` from finished column builders.
fn finish_batch(
    mut builders: Vec<ColBuilder>,
    schema: &Arc<Schema>,
) -> Result<RecordBatch, SaciError> {
    let columns = builders.iter_mut().map(ColBuilder::finish).collect();
    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| SaciError::generic(format!("building a Turso record batch: {e}")))
}

/// A fresh builder per declared field, in order.
fn new_builders(fields: &[FieldSpec]) -> Result<Vec<ColBuilder>, SaciError> {
    fields.iter().map(ColBuilder::new).collect()
}

/// The selected column list, in declared order.
fn column_list(fields: &[FieldSpec]) -> String {
    fields
        .iter()
        .map(|f| f.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// A stored cursor's text rendered back into a bindable value.
fn cursor_param(field: &FieldSpec, text: &str, what: &str) -> Result<turso::Value, SaciError> {
    let bad = |detail: String| {
        SaciError::configuration(format!(
            "{what}: stored cursor '{text}' is not a {}: {detail}",
            field.ty.as_str()
        ))
    };
    match field.ty {
        TursoFieldType::Int64 => text
            .trim()
            .parse::<i64>()
            .map(turso::Value::Integer)
            .map_err(|e| bad(e.to_string())),
        TursoFieldType::Float64 => text
            .trim()
            .parse::<f64>()
            .map(turso::Value::Real)
            .map_err(|e| bad(e.to_string())),
        TursoFieldType::Utf8 => Ok(turso::Value::Text(text.to_string())),
        other => Err(SaciError::configuration(format!(
            "{what}: '{}' cannot be a cursor column",
            other.as_str()
        ))),
    }
}

/// A value's text form, for the offset table.
fn value_to_text(value: &turso::Value) -> String {
    match value {
        turso::Value::Integer(i) => i.to_string(),
        turso::Value::Real(f) => f.to_string(),
        turso::Value::Text(s) => s.clone(),
        turso::Value::Blob(_) | turso::Value::Null => String::new(),
    }
}

/// Wrap an engine error from a query.
fn query_error(what: &str, error: turso::Error) -> SaciError {
    SaciError::generic(format!("{what}: querying: {error}"))
}

/// Wrap an engine error from reading a row.
fn decode_error(what: &str, error: turso::Error) -> SaciError {
    SaciError::generic(format!("{what}: decoding a row: {error}"))
}
