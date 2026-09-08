//! [`PostgresSink`]: bulk load through `COPY … WITH (FORMAT binary)`.
//!
//! One flush is one transaction, so a pipeline iteration lands atomically
//! downstream. `write_mode = "append"` copies straight into the target;
//! `"upsert"` and `"ignore_conflicts"` copy into an `ON COMMIT DROP` temp table
//! and then merge, which means there is no orphaned-staging-table failure mode.
//!
//! [`new`](PostgresSink::new) opens no connection, mirroring
//! [`PostgresSource::new`](crate::source::PostgresSource::new): the first
//! [`write_batch`](Sink::write_batch) connects, reads the target's real column
//! types from `pg_attribute`, and checks them against the declared schema.
//!
//! `BinaryCopyInWriter` owns the PGCOPY framing — the magic header, the flags,
//! the per-row field count and the length prefixes — so this module only
//! supplies one `ToSql` value per column. See `crate::encode`.
//!
//! # The two encode routes
//!
//! A column whose target type `crate::encode` frames exactly is copied
//! straight in, and so is a domain over such a type: a domain is framed
//! exactly as its base type is, and staging it as the domain itself makes
//! `domain_recv` apply the domain's own typmod and run its constraints during
//! the `COPY`.
//!
//! Everything else -- an enum, a composite, `citext`, a geometric, a range,
//! `tsvector`, a domain over one of those -- travels as PostgreSQL text: it is
//! copied into a `text` staging column and projected through a cast to the
//! bare base type on the way into the target, so the server's own input
//! function does the work and the target's modifier is applied by the
//! `INSERT`'s assignment coercion rather than by an explicit cast.
//! `ColumnPlan` is that decision, taken once per session.
//!
//! The cast route is total: every PostgreSQL type has an input function
//! reachable from its own text form, so the only refusals left are
//! declaration errors. It also makes a staged write out of an `append` that
//! carries one such column, inside the same single transaction.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{Kind, Type};

use crate::config::{FieldSpec, PgFieldType, PostgresSinkConfig, Role, WriteMode, split_qualified};
use crate::connection::{
    Connector, PgConnection, column_types, pg_detail, quote, quote_columns, quote_qualified,
};
use crate::encode::{PgValue, resolve_columns, row_values};
use crate::metrics::Instruments;
use crate::numeric::money_factor;
use crate::types::{
    ColumnType, OID_NUMERIC, OID_NUMERIC_ARRAY, PgTypeRef, Wire, numeric_precision, numeric_scale,
    numeric_typmod, validate_columns,
};

/// How one declared column is written.
///
/// Route 1 is `cast: None`: the value is framed in PostgreSQL's binary form
/// for the target's own type -- a domain included -- and copied straight in.
/// Route 2 carries the cast: the value is copied into a `text` staging column
/// and the server casts it into the target's base type from there, leaving the
/// modifier and any domain constraint to the `INSERT`'s assignment coercion.
#[derive(Debug, Clone)]
struct ColumnPlan {
    /// The type `COPY … WITH (FORMAT binary)` frames this column against.
    copy_type: Type,
    /// The staging table's column type, as SQL.
    stage_type: String,
    /// The bare base type the staged column is cast to, on route 2 only.
    cast: Option<String>,
}

/// A PostgreSQL [`Sink`].
pub struct PostgresSink {
    connector: Connector,
    connection: Option<PgConnection>,

    schema: Arc<Schema>,
    fields: Vec<FieldSpec>,
    /// Quoted `"schema"."table"`.
    table: String,
    /// The unquoted spelling, for error messages.
    table_display: String,
    /// Schema and table parts, for the `pg_attribute` lookup.
    namespace: String,
    relation: String,
    /// Quoted name of the staging table used by the merge modes.
    stage: String,

    write_mode: WriteMode,
    conflict_columns: Vec<String>,
    /// Resolved at construction: `update_columns` when given, every declared
    /// non-conflict column otherwise.
    update_columns: Vec<String>,
    dedupe_order_column: Option<String>,
    chunk_rows: usize,
    flush_rows: usize,
    truncate_before_first_write: bool,

    buffer: Vec<RecordBatch>,
    buffered_rows: usize,
    first_write_done: bool,
    /// One write plan per declared column, in declared order, resolved against
    /// the catalog on the first flush.
    plans: Vec<ColumnPlan>,

    instruments: Instruments,
}

impl PostgresSink {
    /// Validate `cfg` and prepare the sink. Opens no connection.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] for any violation
    /// [`PostgresSinkConfig::validate`] reports, for a DSN that does not parse,
    /// for an unreadable `password_file`, and for a TLS configuration that
    /// cannot be built.
    pub fn new(cfg: PostgresSinkConfig) -> Result<Self, SaciError> {
        cfg.validate()?;

        let what = "PostgresSink";
        let fields = cfg
            .schema_fields
            .iter()
            .map(|spec| spec.to_arrow_field())
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Arc::new(Schema::new(fields));
        let (namespace, relation) = split_qualified(what, &cfg.table)?;
        let update_columns = cfg
            .effective_update_columns()
            .into_iter()
            .map(str::to_string)
            .collect();

        let connector = Connector::new(what, &cfg.connection)?;

        #[cfg(feature = "tracing")]
        tracing::info!(
            sink = %cfg.name,
            target_db = %connector.target(),
            table = %cfg.table,
            columns = cfg.schema_fields.len(),
            "postgres sink configured"
        );

        Ok(Self {
            connector,
            connection: None,
            schema,
            fields: cfg.schema_fields.clone(),
            table: quote_qualified(what, &cfg.table)?,
            table_display: cfg.table.clone(),
            namespace,
            relation,
            // `name` is already restricted to [A-Za-z0-9_-] by validation, and
            // '-' is not legal unquoted, so it is folded to '_' before quoting.
            stage: quote(&format!("saci_stage_{}", cfg.name.replace('-', "_"))),
            write_mode: cfg.write_mode,
            conflict_columns: cfg.conflict_columns.clone(),
            update_columns,
            dedupe_order_column: cfg.dedupe_order_column.clone(),
            chunk_rows: cfg.chunk_rows,
            flush_rows: cfg.flush_rows,
            truncate_before_first_write: cfg.truncate_before_first_write,
            buffer: Vec::new(),
            buffered_rows: 0,
            first_write_done: false,
            plans: Vec::new(),
            instruments: Instruments::sink(&cfg.name),
        })
    }

    /// Connect if needed and resolve the target's real column types once.
    async fn ensure_session(&mut self) -> Result<(), SaciError> {
        if self
            .connection
            .as_ref()
            .is_some_and(|connection| !connection.is_closed())
        {
            // A failed catalog check leaves `plans` empty on an open
            // connection. Retry it here, so a missing or invisible table keeps
            // reporting the configuration error that names it instead of failing
            // deeper, as a bare 42P01 from the staging DDL or the COPY.
            if self.plans.is_empty() {
                self.resolve_target_types().await?;
            }
            return Ok(());
        }

        if self.connection.is_some() {
            // A new session may be a different server, so the catalog check and
            // the one-shot TRUNCATE both apply again.
            self.first_write_done = false;
            self.plans.clear();
        }

        self.connection = Some(match self.connector.connect_with_retry().await {
            Ok(connection) => connection,
            Err(e) => {
                self.instruments.error("connect");
                return Err(e);
            }
        });

        if self.plans.is_empty() {
            self.resolve_target_types().await?;
        }
        Ok(())
    }

    /// Read the target's columns from the catalog, check them against the
    /// declared schema, and settle each column's write plan.
    ///
    /// Only reached from [`ensure_session`](Self::ensure_session), which has
    /// just put a live connection in place.
    async fn resolve_target_types(&mut self) -> Result<(), SaciError> {
        let client = self.connection.as_ref().expect("session").client();
        let actual = column_types(
            client,
            "PostgresSink",
            &self.namespace,
            &self.relation,
            &self.table_display,
            self.connector.target(),
        )
        .await
        .inspect_err(|_| self.instruments.error("query"))?;

        let resolved = validate_columns("PostgresSink", Role::Sink, &self.fields, &actual)
            .inspect_err(|_| self.instruments.error("query"))?;

        let mut plans = Vec::with_capacity(self.fields.len());
        let mut problems: Vec<String> = Vec::new();
        for ((column, wire), spec) in resolved.iter().zip(&self.fields) {
            match plan_column(spec, column, *wire) {
                Ok(plan) => plans.push(plan),
                Err(reason) => problems.push(format!("column '{}': {reason}", spec.name)),
            }
        }
        if !problems.is_empty() {
            self.instruments.error("query");
            return Err(SaciError::configuration(format!(
                "PostgresSink: cannot write '{}': {}",
                self.table_display,
                problems.join("; ")
            )));
        }

        self.plans = plans;
        Ok(())
    }

    /// Write everything buffered in one transaction.
    async fn flush(&mut self) -> Result<(), SaciError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.ensure_session().await?;

        let started = Instant::now();
        let staged = self.needs_stage();
        let truncate = self.truncate_before_first_write && !self.first_write_done;

        // Everything below runs against `transaction`, which rolls back on drop,
        // so an error anywhere leaves the target untouched.
        let batches = std::mem::take(&mut self.buffer);
        let rows = self.buffered_rows;
        let result = self.flush_in_transaction(&batches, staged, truncate).await;

        match result {
            Ok(()) => {
                self.buffered_rows = 0;
                self.first_write_done = true;
                self.instruments.batch(rows as u64);
                self.instruments.observe(started.elapsed().as_secs_f64());
                self.instruments.gauge(0);
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    table = %self.table_display,
                    rows,
                    batches = batches.len(),
                    elapsed_us = started.elapsed().as_micros(),
                    "postgres sink flushed"
                );
                Ok(())
            }
            Err(e) => {
                // Put the batches back so `finish` can retry after a reconnect
                // rather than silently dropping rows.
                self.buffer = batches;
                self.instruments.error("copy");
                Err(e)
            }
        }
    }

    /// The whole write, inside one transaction that rolls back on drop.
    ///
    /// Every statement is built before the connection is borrowed mutably, and
    /// error mapping goes through free functions, so nothing here re-borrows
    /// `self` while the transaction is alive.
    async fn flush_in_transaction(
        &mut self,
        batches: &[RecordBatch],
        staged: bool,
        truncate: bool,
    ) -> Result<(), SaciError> {
        let types: Vec<Type> = self
            .plans
            .iter()
            .map(|plan| plan.copy_type.clone())
            .collect();
        let copy_target = if staged { &self.stage } else { &self.table };
        let copy_sql = self.copy_sql(copy_target);
        let stage_ddl = self.stage_ddl();
        let merge_sql = self.merge_sql();
        let truncate_sql = format!("TRUNCATE {}", self.table);

        // Disjoint field borrows: `connection` is mutable, the rest immutable.
        let table = self.table_display.as_str();
        let fields = self.fields.as_slice();
        let chunk_rows = self.chunk_rows;
        let connection = self.connection.as_mut().expect("session");

        let transaction = connection.client_mut().transaction().await.map_err(|e| {
            SaciError::generic(format!(
                "PostgresSink: cannot open a transaction for '{table}': {}",
                pg_detail(&e)
            ))
        })?;

        if truncate {
            transaction
                .batch_execute(&truncate_sql)
                .await
                .map_err(|e| {
                    SaciError::generic(format!(
                        "PostgresSink: cannot truncate '{table}': {}",
                        pg_detail(&e)
                    ))
                })?;
        }

        if staged {
            transaction.batch_execute(&stage_ddl).await.map_err(|e| {
                SaciError::generic(format!(
                    "PostgresSink: cannot create the staging table for '{table}': {}",
                    pg_detail(&e)
                ))
            })?;
        }

        let mut writer: Option<Pin<Box<BinaryCopyInWriter>>> = None;
        let mut rows_in_copy = 0usize;

        for batch in batches {
            let readers = resolve_columns("PostgresSink", batch, fields, &types)?;
            // Declared inside the loop because a `PgValue::List` borrows the
            // element reader that `readers` owns.
            let mut values: Vec<PgValue<'_>> = Vec::with_capacity(fields.len());
            for row in 0..batch.num_rows() {
                if writer.is_none() {
                    let sink = transaction
                        .copy_in::<_, bytes::Bytes>(&copy_sql)
                        .await
                        .map_err(|e| copy_error(table, e))?;
                    writer = Some(Box::pin(BinaryCopyInWriter::new(sink, &types)));
                }
                row_values(&readers, row, &mut values)?;
                writer
                    .as_mut()
                    .expect("writer opened above")
                    .as_mut()
                    // `values.iter()`, not `.copied()`: a by-value iterator
                    // would need `PgValue<'_>: Copy` for every lifetime, which
                    // a higher-ranked bound cannot prove. `&T: ToSql` covers it.
                    .write_raw(values.iter())
                    .await
                    .map_err(|e| copy_error(table, e))?;

                rows_in_copy += 1;
                if rows_in_copy >= chunk_rows {
                    let mut full = writer.take().expect("writer opened above");
                    full.as_mut()
                        .finish()
                        .await
                        .map_err(|e| copy_error(table, e))?;
                    rows_in_copy = 0;
                }
            }
        }
        if let Some(mut open) = writer {
            open.as_mut()
                .finish()
                .await
                .map_err(|e| copy_error(table, e))?;
        }

        if let Some(merge_sql) = merge_sql {
            transaction
                .batch_execute(&merge_sql)
                .await
                .map_err(|e| merge_error(table, e))?;
        }

        transaction.commit().await.map_err(|e| {
            SaciError::generic(format!(
                "PostgresSink: cannot commit the write to '{table}': {}",
                pg_detail(&e)
            ))
        })
    }

    /// The declared column list, quoted.
    fn column_list(&self) -> String {
        quote_columns(self.fields.iter().map(|spec| spec.name.as_str()))
    }

    /// `COPY <target> (<cols>) FROM STDIN WITH (FORMAT binary)`.
    fn copy_sql(&self, target: &str) -> String {
        format!(
            "COPY {target} ({}) FROM STDIN WITH (FORMAT binary)",
            self.column_list()
        )
    }

    /// Whether the write goes through the staging table.
    ///
    /// Every mode but `append` merges, and `append` stages as soon as one
    /// column takes the cast route.
    fn needs_stage(&self) -> bool {
        self.write_mode != WriteMode::Append || self.plans.iter().any(|plan| plan.cast.is_some())
    }

    /// The staging table DDL: one column per declared field, typed as what the
    /// `COPY` frames -- the target's own type, or `text` for a cast column --
    /// so every value arrives in a type this crate can write. `ON COMMIT DROP`
    /// is what removes the failure mode of a leftover table after a crash.
    ///
    /// The staging table carries none of the target *column's* constraints: a
    /// `NOT NULL` violation is reported by the target on the `INSERT`, and the
    /// target's defaults still apply, because the `INSERT` names only the
    /// declared columns either way. A domain's constraints are the exception,
    /// and deliberately so: they belong to the type, so a column staged as a
    /// domain runs them here.
    fn stage_ddl(&self) -> String {
        let columns = self
            .fields
            .iter()
            .zip(&self.plans)
            .map(|(spec, plan)| format!("{} {}", quote(&spec.name), plan.stage_type))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "CREATE TEMP TABLE {} ({columns}) ON COMMIT DROP",
            self.stage
        )
    }

    /// The staged column list, each cast-route column projected through its
    /// bare base type so the server's input function does the conversion and
    /// the target's modifier is left to the `INSERT`'s assignment coercion.
    fn select_list(&self) -> String {
        self.fields
            .iter()
            .zip(&self.plans)
            .map(|(spec, plan)| {
                let quoted = quote(&spec.name);
                match &plan.cast {
                    Some(target) => format!("{quoted}::{target}"),
                    None => quoted,
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The statement that moves the staged rows into the target, or `None` for
    /// an `append` that copies straight in.
    fn merge_sql(&self) -> Option<String> {
        if !self.needs_stage() {
            return None;
        }
        let columns = self.column_list();
        let target = &self.table;

        if self.write_mode == WriteMode::Append {
            // An `append` carrying a cast column stages, then inserts once.
            // Same transaction, so the atomicity story does not change.
            return Some(format!(
                "INSERT INTO {target} ({columns}) SELECT {} FROM {}",
                self.select_list(),
                self.stage
            ));
        }

        let conflict = quote_columns(self.conflict_columns.iter().map(String::as_str));
        let projection = self.select_list();

        let select = match &self.dedupe_order_column {
            Some(order) => format!(
                "SELECT DISTINCT ON ({conflict}) {projection} FROM {} ORDER BY {conflict}, {} DESC",
                self.stage,
                quote(order)
            ),
            None => format!("SELECT {projection} FROM {}", self.stage),
        };

        let action = match self.write_mode {
            WriteMode::IgnoreConflicts => "DO NOTHING".to_string(),
            _ => {
                let assignments = self
                    .update_columns
                    .iter()
                    .map(|column| {
                        let quoted = quote(column);
                        format!("{quoted} = EXCLUDED.{quoted}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("DO UPDATE SET {assignments}")
            }
        };

        Some(format!(
            "INSERT INTO {target} ({columns}) {select} ON CONFLICT ({conflict}) {action}"
        ))
    }

    /// Reject a batch whose schema is not the declared one.
    fn check_schema(&self, batch: &RecordBatch) -> Result<(), SaciError> {
        let expected = self.schema.fields();
        let actual = batch.schema_ref().fields();
        if expected.len() != actual.len() {
            return Err(SaciError::generic(format!(
                "PostgresSink: batch has {} column(s) but '{}' declares {}",
                actual.len(),
                self.table_display,
                expected.len()
            )));
        }
        for (want, got) in expected.iter().zip(actual.iter()) {
            if want.name() != got.name() {
                return Err(SaciError::generic(format!(
                    "PostgresSink: batch column '{}' does not match the declared column '{}'; \
                     schema_fields order is the batch order",
                    got.name(),
                    want.name()
                )));
            }
            if want.data_type() != got.data_type() {
                return Err(SaciError::generic(format!(
                    "PostgresSink: batch column '{}' is {:?} but '{}' declares {:?}",
                    got.name(),
                    got.data_type(),
                    self.table_display,
                    want.data_type()
                )));
            }
        }
        Ok(())
    }
}

/// Settle how one column is written.
///
/// Route 1 -- a direct binary `COPY` -- carries every target this crate frames
/// in PostgreSQL's binary form, modifier and all: `COPY … FORMAT binary`
/// hands the column's own type to that type's receive function, so
/// `character varying(3)` refuses an over-long value there rather than
/// truncating it the way an explicit `::varchar(3)` cast would. A **domain**
/// over such a type is on route 1 too, staged as the domain itself: a domain's
/// values are framed exactly as its base type's, and `domain_recv` hands the
/// base's receive function the *domain's* own typmod and then runs the
/// domain's constraints, so both are enforced while the `COPY` is still
/// running.
///
/// Route 2 -- a server-side cast from a `text` staging column -- carries every
/// `Wire::Text` pair, which is every enum, composite, extension type and
/// geometric, plus a domain over one of those. Its cast target is the
/// unconstrained base type ([`ColumnType::base_display`], quoted and
/// `pg_catalog` qualified), never the column's own spelling: an explicit cast
/// applies a `varchar(n)`/`bit(n)`/`bit varying(n)` modifier with
/// explicit-cast semantics, which pads or truncates, so the modifier and the
/// domain's constraints are left to the assignment coercion the `INSERT`
/// performs on the way into the target, which raises instead.
///
/// A modifier in `pg_type` is an *assertion*, not a coercion: the target type
/// always comes from the catalog, so a modifier the server's column does not
/// carry is a refusal.
///
/// # Errors
///
/// Returns the reason the column cannot be written, without a column prefix:
/// the caller adds one and reports every offending column at once.
fn plan_column(spec: &FieldSpec, column: &ColumnType, wire: Wire) -> Result<ColumnPlan, String> {
    let forced = spec.forced_pg_type()?;
    // A binary wire frames the target's own type, domain included, so the
    // cast route is exactly the text wire.
    let cast = wire == Wire::Text;

    let copy_type = if cast {
        // A text wire carries PostgreSQL's canonical text for the type, which
        // is exactly what the target's input function reads back.
        if spec.ty == PgFieldType::List {
            Type::TEXT_ARRAY
        } else {
            Type::TEXT
        }
    } else {
        Type::from_oid(column.oid).ok_or_else(|| {
            format!(
                "the server type {} has no binary form this connector frames; declare \
                 type = \"utf8\" with pg_type = \"{}\" to write it as text",
                column.display, column.display
            )
        })?
    };

    if let Some(forced) = &forced
        && let Some(modifier) = &forced.typmod
    {
        check_modifier(forced, modifier, column)?;
    }
    check_scale(spec, column, &copy_type)?;

    Ok(ColumnPlan {
        stage_type: if cast {
            sql_type_name(&copy_type)
        } else {
            column.display.clone()
        },
        cast: cast.then(|| column.base_display.clone()),
        copy_type,
    })
}

/// Refuse a `pg_type` modifier the server's own column does not carry.
///
/// The target type comes from the catalog either way, so a modifier that
/// disagrees means the configuration describes a column the server does not
/// have -- and a `COPY` framed for the wrong width would either refuse a value
/// the config says fits or accept one it says does not.
fn check_modifier(forced: &PgTypeRef, modifier: &str, column: &ColumnType) -> Result<(), String> {
    let mismatch = || {
        format!(
            "pg_type = \"{}\" but the server column is {}",
            forced.raw, column.display
        )
    };

    // Keyed on the column's own type, not on the type the value is framed as:
    // a `numeric` read as `type = "utf8"` travels over the text wire, and the
    // modifier still has to be compared as `numeric`'s parts.
    if column.oid == OID_NUMERIC || column.oid == OID_NUMERIC_ARRAY {
        // `numeric(12)` is `numeric(12,0)`, so the parts are compared rather
        // than the spelling.
        let (precision, scale) = numeric_typmod(modifier);
        if precision.map(u32::from) != numeric_precision(column.typmod).map(u32::from)
            || scale.map(i32::from) != numeric_scale(column.typmod)
        {
            return Err(mismatch());
        }
        return Ok(());
    }

    if display_modifier(&column.display).as_deref() != Some(modifier) {
        return Err(mismatch());
    }
    Ok(())
}

/// The parenthesised modifier of a `format_type` spelling, with its spaces
/// removed so it compares against a `pg_type`'s own normalised form.
///
/// `format_type` never nests parentheses, so the first pair is the modifier:
/// `character varying(64)` and `timestamp(3) with time zone` both yield the
/// digits between them.
fn display_modifier(display: &str) -> Option<String> {
    let open = display.find('(')?;
    let rest = &display[open + 1..];
    let close = rest.find(')')?;
    Some(rest[..close].replace(' ', ""))
}

/// Refuse a `decimal128` whose target keeps fewer fractional digits than it
/// declares.
///
/// `numeric(p,s)` and `money` both *round* a wider input rather than raising,
/// which is the one place the server would silently change a value. Checked
/// here, once per session, rather than per row. A domain's own modifier
/// arrives here too, because
/// [`column_types`](crate::connection::column_types) carries `typtypmod`
/// along the domain chain.
fn check_scale(spec: &FieldSpec, column: &ColumnType, copy_type: &Type) -> Result<(), String> {
    let decimal = spec.ty == PgFieldType::Decimal128 || spec.item == Some(PgFieldType::Decimal128);
    if !decimal {
        return Ok(());
    }
    let (_, scale) = spec.decimal_params().map_err(|e| e.message().to_string())?;

    if *copy_type == Type::MONEY || *copy_type == Type::MONEY_ARRAY {
        return money_factor(scale)
            .map(|_| ())
            .map_err(|reason| format!("the target is {}: {reason}", column.display));
    }

    if let Some(target_scale) = numeric_scale(column.typmod)
        && target_scale < i32::from(scale)
    {
        return Err(format!(
            "the target is {}, which keeps {target_scale} fractional digit(s), but the field \
             declares scale {scale}; a narrower target would round rather than error",
            column.display
        ));
    }
    Ok(())
}

/// The SQL spelling of a type, for the staging table's column list.
fn sql_type_name(ty: &Type) -> String {
    match ty.kind() {
        Kind::Array(element) => format!("{}[]", element.name()),
        _ => ty.name().to_string(),
    }
}

/// A `COPY` failure, naming the target table.
fn copy_error(table: &str, e: tokio_postgres::Error) -> SaciError {
    SaciError::generic(format!(
        "PostgresSink: COPY into '{table}' failed: {}",
        pg_detail(&e)
    ))
}

/// A merge failure. A cardinality violation means one batch repeated a conflict
/// key, which `dedupe_order_column` is what resolves.
fn merge_error(table: &str, e: tokio_postgres::Error) -> SaciError {
    if e.code() == Some(&SqlState::CARDINALITY_VIOLATION) {
        return SaciError::generic(format!(
            "PostgresSink: one batch repeats a conflict key for '{table}', so ON CONFLICT DO \
             UPDATE cannot resolve it; set dedupe_order_column to pick the winning row ({})",
            pg_detail(&e)
        ));
    }
    SaciError::generic(format!(
        "PostgresSink: cannot merge staged rows into '{table}': {}",
        pg_detail(&e)
    ))
}

#[async_trait]
impl Sink for PostgresSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.check_schema(batch)?;
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.buffered_rows += batch.num_rows();
        self.buffer.push(batch.clone());
        self.instruments.gauge(self.buffered_rows as u64);

        if self.flush_rows == 0 || self.buffered_rows >= self.flush_rows {
            self.flush().await?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        self.flush().await
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn pending_rows(&self) -> Option<usize> {
        Some(self.buffered_rows)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};

    use super::*;
    use crate::types::unconstrained_type_name;
    use saci_connector::from_kdl_str;
    use serde::Deserialize as _;

    const FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"label\" type=\"utf8\"
schema_fields \"seq\" type=\"int64\" nullable=#false
";

    fn sink(header: &str) -> PostgresSink {
        sink_with(header, FIELDS)
    }

    fn sink_with(header: &str, fields: &str) -> PostgresSink {
        let text = format!(
            "name \"out\"\n{header}\n\n\
             connection dsn=\"postgres://h/d\" sslmode=\"disable\"\n{fields}"
        );
        let cfg = PostgresSinkConfig::deserialize(from_kdl_str(&text).expect("parse kdl"))
            .expect("parse");
        PostgresSink::new(cfg).expect("sink")
    }

    /// A built-in target column, as the catalog step reports one.
    fn target(oid: u32, display: &str) -> ColumnType {
        ColumnType {
            name: String::new(),
            oid,
            attribute_oid: oid,
            display: display.to_string(),
            base_display: unconstrained_type_name(oid),
            typmod: -1,
            catalog: None,
        }
    }

    /// Fill the sink's plans the way `resolve_target_types` would, so the SQL
    /// builders see the routes `plan_column` really picks.
    fn plan(sink: &mut PostgresSink, columns: &[(ColumnType, Wire)]) {
        sink.plans = sink
            .fields
            .iter()
            .zip(columns)
            .map(|(spec, (column, wire))| {
                plan_column(spec, column, *wire).unwrap_or_else(|e| panic!("{}: {e}", spec.name))
            })
            .collect();
    }

    /// The `id`/`label`/`seq` trio, all on the direct binary route.
    fn plan_small(sink: &mut PostgresSink) {
        plan(
            sink,
            &[
                (target(Type::INT8.oid(), "bigint"), Wire::Binary),
                (target(Type::TEXT.oid(), "text"), Wire::Binary),
                (target(Type::INT8.oid(), "bigint"), Wire::Binary),
            ],
        );
    }

    #[test]
    fn append_copies_straight_into_the_target_and_has_no_merge() {
        let mut sink = sink("table \"sales.orders\"");
        plan_small(&mut sink);
        assert_eq!(
            sink.copy_sql(&sink.table),
            "COPY \"sales\".\"orders\" (\"id\", \"label\", \"seq\") FROM STDIN \
             WITH (FORMAT binary)"
        );
        assert!(!sink.needs_stage());
        assert!(sink.merge_sql().is_none());
    }

    #[test]
    fn upsert_stages_then_merges_every_non_conflict_column() {
        let mut sink = sink("table \"orders\"\nwrite_mode \"upsert\"\nconflict_columns \"id\"");
        plan_small(&mut sink);
        // The staging table is an explicit column list, so a cast column can
        // land as the type this crate frames rather than as the target's.
        assert_eq!(
            sink.stage_ddl(),
            "CREATE TEMP TABLE \"saci_stage_out\" (\"id\" bigint, \"label\" text, \
             \"seq\" bigint) ON COMMIT DROP"
        );
        assert_eq!(
            sink.copy_sql(&sink.stage),
            "COPY \"saci_stage_out\" (\"id\", \"label\", \"seq\") FROM STDIN WITH (FORMAT binary)"
        );
        assert_eq!(
            sink.merge_sql().unwrap(),
            "INSERT INTO \"public\".\"orders\" (\"id\", \"label\", \"seq\") \
             SELECT \"id\", \"label\", \"seq\" FROM \"saci_stage_out\" \
             ON CONFLICT (\"id\") DO UPDATE SET \"label\" = EXCLUDED.\"label\", \
             \"seq\" = EXCLUDED.\"seq\""
        );
    }

    #[test]
    fn explicit_update_columns_narrow_the_assignment_list() {
        let mut sink = sink(
            "table \"orders\"\nwrite_mode \"upsert\"\nconflict_columns \"id\"\n\
             update_columns \"seq\"",
        );
        plan_small(&mut sink);
        assert_eq!(
            sink.merge_sql().unwrap(),
            "INSERT INTO \"public\".\"orders\" (\"id\", \"label\", \"seq\") \
             SELECT \"id\", \"label\", \"seq\" FROM \"saci_stage_out\" \
             ON CONFLICT (\"id\") DO UPDATE SET \"seq\" = EXCLUDED.\"seq\""
        );
    }

    #[test]
    fn dedupe_order_column_adds_distinct_on_with_a_descending_tiebreak() {
        let mut sink = sink(
            "table \"orders\"\nwrite_mode \"upsert\"\nconflict_columns \"id\"\n\
             dedupe_order_column \"seq\"",
        );
        plan_small(&mut sink);
        let merge = sink.merge_sql().unwrap();
        assert!(
            merge.contains(
                "SELECT DISTINCT ON (\"id\") \"id\", \"label\", \"seq\" FROM \"saci_stage_out\" \
                 ORDER BY \"id\", \"seq\" DESC"
            ),
            "{merge}"
        );
    }

    #[test]
    fn ignore_conflicts_does_nothing_on_a_collision() {
        let mut sink =
            sink("table \"orders\"\nwrite_mode \"ignore_conflicts\"\nconflict_columns \"id\"");
        plan_small(&mut sink);
        assert!(
            sink.merge_sql()
                .unwrap()
                .ends_with("ON CONFLICT (\"id\") DO NOTHING")
        );
    }

    #[test]
    fn a_composite_conflict_key_is_quoted_column_by_column() {
        let mut sink =
            sink("table \"orders\"\nwrite_mode \"upsert\"\nconflict_columns \"id\" \"seq\"");
        plan_small(&mut sink);
        let merge = sink.merge_sql().unwrap();
        assert!(merge.contains("ON CONFLICT (\"id\", \"seq\")"), "{merge}");
        assert!(
            merge.contains("DO UPDATE SET \"label\" = EXCLUDED.\"label\""),
            "{merge}"
        );
        assert!(!merge.contains("\"seq\" = EXCLUDED"), "{merge}");
    }

    /// `numeric(12,2)`'s `atttypmod`: the precision in the high half, the
    /// scale in the low, plus the varlena header length.
    const NUMERIC_12_2: i32 = ((12 << 16) | 2) + 4;

    /// `character varying(3)`'s `atttypmod`: the declared length plus the
    /// varlena header length.
    const VARCHAR_3: i32 = 3 + 4;

    const MIXED_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"payload\" type=\"utf8\" pg_type=\"jsonb\"
schema_fields \"total\" type=\"decimal128\" precision=12 scale=2 pg_type=\"numeric(12,2)\"
";

    /// A text-wire column and a forced modifier, beside a plain `bigint`.
    fn plan_mixed(sink: &mut PostgresSink) {
        let mut numeric = target(Type::NUMERIC.oid(), "numeric(12,2)");
        numeric.typmod = NUMERIC_12_2;
        plan(
            sink,
            &[
                (target(Type::INT8.oid(), "bigint"), Wire::Binary),
                (target(Type::JSONB.oid(), "jsonb"), Wire::Text),
                (numeric, Wire::Binary),
            ],
        );
    }

    #[test]
    fn a_cast_column_makes_an_append_stage_and_project_its_target_type() {
        let mut sink = sink_with("table \"orders\"", MIXED_FIELDS);
        plan_mixed(&mut sink);

        // `append` stages as soon as one column needs the server to cast.
        // `total`'s forced modifier matches the server's, so it stages as its
        // own type and the `COPY` enforces the modifier itself.
        assert!(sink.needs_stage());
        assert_eq!(
            sink.stage_ddl(),
            "CREATE TEMP TABLE \"saci_stage_out\" (\"id\" bigint, \"payload\" text, \
             \"total\" numeric(12,2)) ON COMMIT DROP"
        );
        assert_eq!(
            sink.copy_sql(&sink.stage),
            "COPY \"saci_stage_out\" (\"id\", \"payload\", \"total\") FROM STDIN \
             WITH (FORMAT binary)"
        );
        assert_eq!(
            sink.merge_sql().unwrap(),
            "INSERT INTO \"public\".\"orders\" (\"id\", \"payload\", \"total\") \
             SELECT \"id\", \"payload\"::pg_catalog.\"jsonb\", \"total\" FROM \"saci_stage_out\""
        );
    }

    #[test]
    fn a_cast_column_projects_through_the_upsert_merge_too() {
        let mut sink = sink_with(
            "table \"orders\"\nwrite_mode \"upsert\"\nconflict_columns \"id\"",
            MIXED_FIELDS,
        );
        plan_mixed(&mut sink);
        assert_eq!(
            sink.merge_sql().unwrap(),
            "INSERT INTO \"public\".\"orders\" (\"id\", \"payload\", \"total\") \
             SELECT \"id\", \"payload\"::pg_catalog.\"jsonb\", \"total\" FROM \"saci_stage_out\" \
             ON CONFLICT (\"id\") DO UPDATE SET \"payload\" = EXCLUDED.\"payload\", \
             \"total\" = EXCLUDED.\"total\""
        );
    }

    /// A domain over a binary-framed type stages as the domain *itself*, with
    /// no cast: `domain_recv` hands the base's receive function the domain's
    /// own typmod and then runs the domain's constraints, so both are enforced
    /// during the `COPY`. Projecting `"col"::<domain>` instead would apply a
    /// `character varying(3)` base modifier with explicit-cast semantics,
    /// which truncates `'hello'` to `'hel'`.
    #[test]
    fn a_domain_over_a_binary_framed_type_stages_as_the_domain_itself() {
        let sink = sink_with(
            "table \"orders\"",
            "schema_fields \"label\" type=\"utf8\" pg_type=\"public.short\"\n",
        );
        let column = ColumnType {
            name: "label".to_string(),
            // The catalog step already replaced the domain with its base OID,
            // and substituted the domain's own `typtypmod` for the column's
            // `atttypmod`, which is always -1 for a domain.
            oid: Type::VARCHAR.oid(),
            attribute_oid: 90_001, // the domain's own OID
            display: "public.short".to_string(),
            base_display: "pg_catalog.\"varchar\"".to_string(),
            typmod: VARCHAR_3,
            catalog: Some(("public".to_string(), "short".to_string())),
        };
        let plan = plan_column(&sink.fields[0], &column, Wire::Binary).expect("plan");
        assert_eq!(plan.copy_type, Type::VARCHAR);
        assert_eq!(plan.stage_type, "public.short");
        assert_eq!(plan.cast, None);
    }

    /// A domain whose base has no binary form this crate frames stays on the
    /// cast route, and casts to the *base* type rather than to the domain, so
    /// the domain's own coercion happens on the `INSERT`.
    #[test]
    fn a_domain_over_an_enum_still_casts_and_casts_to_the_base() {
        let sink = sink_with(
            "table \"orders\"",
            "schema_fields \"mood\" type=\"utf8\" pg_type=\"public.mood_dom\"\n",
        );
        let column = ColumnType {
            name: "mood".to_string(),
            // A domain over an enum resolves to the enum's own OID, which is
            // no more static than the domain's.
            oid: 999_999,
            attribute_oid: 999_998, // the domain's own OID
            display: "public.mood_dom".to_string(),
            base_display: "public.mood".to_string(),
            typmod: -1,
            catalog: Some(("public".to_string(), "mood_dom".to_string())),
        };
        let plan = plan_column(&sink.fields[0], &column, Wire::Text).expect("plan");
        assert_eq!(plan.copy_type, Type::TEXT);
        assert_eq!(plan.stage_type, "text");
        assert_eq!(plan.cast.as_deref(), Some("public.mood"));
    }

    /// The cast target is a quoted `pg_catalog` name, which is what keeps a
    /// type *keyword*'s length default out of it: an unquoted `bit` is
    /// `bit(1)` and would leave one bit of the staged value, an unquoted
    /// `char` is `character(1)`. Quoted, the cast only retypes, and the
    /// `INSERT`'s assignment coercion checks the width.
    #[test]
    fn a_bit_target_is_cast_through_a_quoted_name_not_the_bit_keyword() {
        let sink = sink_with(
            "table \"orders\"",
            "schema_fields \"flags\" type=\"utf8\" pg_type=\"bit(4)\"\n",
        );
        let mut column = target(Type::BIT.oid(), "bit(4)");
        column.typmod = 4;
        let plan = plan_column(&sink.fields[0], &column, Wire::Text).expect("plan");
        assert_eq!(plan.stage_type, "text");
        assert_eq!(plan.cast.as_deref(), Some("pg_catalog.\"bit\""));
    }

    /// A modifier -- the server's own or one the config forces to match it --
    /// is enforced by the binary `COPY` itself, because `COPY … FORMAT binary`
    /// hands `atttypmod` to the receive function. An explicit
    /// `::character varying(3)` cast would instead *truncate*, so a matching
    /// modifier must not push the column onto the cast route.
    #[test]
    fn a_modifier_the_server_carries_stays_on_the_direct_route() {
        let sink = sink_with(
            "table \"orders\"",
            "schema_fields \"label\" type=\"utf8\"\n",
        );
        let column = target(Type::VARCHAR.oid(), "character varying(64)");
        let plan = plan_column(&sink.fields[0], &column, Wire::Binary).expect("plan");
        assert!(plan.cast.is_none());
        assert_eq!(plan.stage_type, "character varying(64)");

        // The same target, now named with its modifier by the configuration.
        let sink = sink_with(
            "table \"orders\"",
            "schema_fields \"label\" type=\"utf8\" pg_type=\"varchar(3)\"\n",
        );
        let column = target(Type::VARCHAR.oid(), "character varying(3)");
        let plan = plan_column(&sink.fields[0], &column, Wire::Binary).expect("plan");
        assert!(
            plan.cast.is_none(),
            "a cast to varchar(3) would truncate where the COPY refuses"
        );
        assert_eq!(plan.stage_type, "character varying(3)");
    }

    /// A modifier is an assertion about the server's column, so one the server
    /// does not carry is a refusal naming both spellings -- never a silent
    /// re-interpretation of the target.
    #[test]
    fn a_forced_modifier_the_server_does_not_carry_is_refused() {
        let sink = sink_with(
            "table \"orders\"",
            "schema_fields \"total\" type=\"decimal128\" precision=12 scale=2 \
             pg_type=\"numeric(12,2)\"\n",
        );
        // A wider target: `numeric(18,6)`.
        let mut column = target(Type::NUMERIC.oid(), "numeric(18,6)");
        column.typmod = ((18 << 16) | 6) + 4;
        let reason = plan_column(&sink.fields[0], &column, Wire::Binary)
            .expect_err("the modifier does not match");
        assert!(reason.contains("numeric(12,2)"), "{reason}");
        assert!(reason.contains("numeric(18,6)"), "{reason}");

        // An unconstrained `numeric` carries no modifier at all.
        let reason = plan_column(
            &sink.fields[0],
            &target(Type::NUMERIC.oid(), "numeric"),
            Wire::Binary,
        )
        .expect_err("the server column has no modifier");
        assert!(reason.contains("numeric(12,2)"), "{reason}");

        // A character width the server does not have, either way round.
        let sink = sink_with(
            "table \"orders\"",
            "schema_fields \"label\" type=\"utf8\" pg_type=\"varchar(3)\"\n",
        );
        let reason = plan_column(
            &sink.fields[0],
            &target(Type::VARCHAR.oid(), "character varying(64)"),
            Wire::Binary,
        )
        .expect_err("3 is not 64");
        assert!(reason.contains("character varying(64)"), "{reason}");

        // And the matching one still passes, so the check is not blanket.
        plan_column(
            &sink.fields[0],
            &target(Type::VARCHAR.oid(), "character varying(3)"),
            Wire::Binary,
        )
        .expect("the modifier matches");
    }

    /// A `numeric` read as `type = "utf8"` travels over the text wire, so the
    /// modifier comparison must key on the *column's* type rather than on the
    /// type the value is framed as -- otherwise `numeric(12)` is compared as a
    /// string against `format_type`'s `numeric(12,0)` and refused for nothing.
    #[test]
    fn a_text_wire_numeric_still_compares_its_modifier_as_numeric() {
        let sink = sink_with(
            "table \"orders\"",
            "schema_fields \"total\" type=\"utf8\" pg_type=\"numeric(12)\"\n",
        );
        let mut column = target(Type::NUMERIC.oid(), "numeric(12,0)");
        // `numeric(12,0)`: the scale half of the packed modifier is zero.
        column.typmod = (12 << 16) + 4;
        let plan = plan_column(&sink.fields[0], &column, Wire::Text)
            .expect("numeric(12) is numeric(12,0)");
        assert_eq!(plan.copy_type, Type::TEXT);
        // The cast target is the bare type: the `INSERT` applies the
        // modifier, so no explicit cast ever carries one.
        assert_eq!(plan.cast.as_deref(), Some("pg_catalog.\"numeric\""));

        // A real disagreement over the same wire is still refused.
        let mut column = target(Type::NUMERIC.oid(), "numeric(12,2)");
        column.typmod = NUMERIC_12_2;
        let reason =
            plan_column(&sink.fields[0], &column, Wire::Text).expect_err("scale 0 is not scale 2");
        assert!(reason.contains("numeric(12)"), "{reason}");
        assert!(reason.contains("numeric(12,2)"), "{reason}");
    }

    /// A domain column's own `atttypmod` is `-1`; the catalog step substitutes
    /// the domain's `typtypmod`, which is what lets this refusal fire at all.
    #[test]
    fn a_domain_over_a_scaled_numeric_refuses_a_wider_declared_scale() {
        let sink = sink_with(
            "table \"orders\"",
            "schema_fields \"total\" type=\"decimal128\" precision=20 scale=10 \
             pg_type=\"public.money_amt\"\n",
        );
        let column = ColumnType {
            name: "total".to_string(),
            oid: Type::NUMERIC.oid(),
            attribute_oid: 90_002, // the domain's own OID
            display: "public.money_amt".to_string(),
            base_display: "pg_catalog.\"numeric\"".to_string(),
            typmod: NUMERIC_12_2,
            catalog: Some(("public".to_string(), "money_amt".to_string())),
        };
        let reason = plan_column(&sink.fields[0], &column, Wire::Binary)
            .expect_err("the domain's numeric(12,2) would round scale 10");
        assert!(reason.contains("public.money_amt"), "{reason}");
        assert!(reason.contains("scale 10"), "{reason}");

        // Scale 2 fits, and the column stages as the domain itself, which is
        // what runs the domain's own constraints during the `COPY`.
        let sink = sink_with(
            "table \"orders\"",
            "schema_fields \"total\" type=\"decimal128\" precision=12 scale=2 \
             pg_type=\"public.money_amt\"\n",
        );
        let plan = plan_column(&sink.fields[0], &column, Wire::Binary).expect("scale 2 fits");
        assert_eq!(plan.stage_type, "public.money_amt");
        assert_eq!(plan.cast, None);
    }

    #[test]
    fn a_target_scale_narrower_than_the_declared_one_is_refused() {
        let sink = sink_with(
            "table \"orders\"",
            "schema_fields \"total\" type=\"decimal128\" precision=12 scale=4\n",
        );
        let mut column = target(Type::NUMERIC.oid(), "numeric(12,2)");
        column.typmod = NUMERIC_12_2;
        let reason = plan_column(&sink.fields[0], &column, Wire::Binary)
            .expect_err("a narrower target would round");
        assert!(reason.contains("numeric(12,2)"), "{reason}");
        assert!(reason.contains("scale 4"), "{reason}");

        // An unconstrained `numeric` keeps every digit, so it is no refusal.
        let column = target(Type::NUMERIC.oid(), "numeric");
        plan_column(&sink.fields[0], &column, Wire::Binary).expect("numeric holds any scale");

        // `money` always keeps exactly two.
        let sink = sink_with(
            "table \"orders\"",
            "schema_fields \"total\" type=\"decimal128\" precision=12 scale=1\n",
        );
        let reason = plan_column(
            &sink.fields[0],
            &target(Type::MONEY.oid(), "money"),
            Wire::Binary,
        )
        .expect_err("money keeps two digits");
        assert!(reason.contains("2 fractional digits"), "{reason}");
    }

    #[test]
    fn an_injected_table_name_is_quoted_not_interpolated() {
        let sink = sink("table \"my\\\"table\"");
        assert_eq!(sink.table, "\"public\".\"my\"\"table\"");
        let copy = sink.copy_sql(&sink.table);
        assert!(
            copy.starts_with("COPY \"public\".\"my\"\"table\" ("),
            "{copy}"
        );
        assert!(!copy.contains("COPY \"public\".\"my\"table\""), "{copy}");
    }

    #[test]
    fn a_hyphenated_name_becomes_a_legal_staging_identifier() {
        let sink = sink("table \"orders\"\nwrite_mode \"upsert\"\nconflict_columns \"id\"");
        assert_eq!(sink.stage, "\"saci_stage_out\"");

        let cfg = PostgresSinkConfig::deserialize(
            from_kdl_str(&format!(
                "name \"out-2\"\ntable \"orders\"\n\n\
                 connection dsn=\"postgres://h/d\" sslmode=\"disable\"\n{FIELDS}"
            ))
            .unwrap(),
        )
        .unwrap();
        let sink = PostgresSink::new(cfg).unwrap();
        assert_eq!(sink.stage, "\"saci_stage_out_2\"");
    }

    fn batch(fields: Vec<Field>, columns: Vec<arrow_array::ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[tokio::test]
    async fn a_matching_batch_is_accepted_and_buffered() {
        let mut sink = sink("table \"orders\"\nflush_rows 1000");
        let batch = batch(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, true),
                Field::new("seq", DataType::Int64, false),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(Int64Array::from(vec![10, 11])),
            ],
        );
        // flush_rows is above the batch size, so no connection is attempted.
        sink.write_batch(&batch).await.expect("buffered");
        assert_eq!(sink.pending_rows(), Some(2));
        assert_eq!(sink.buffer.len(), 1);
    }

    #[tokio::test]
    async fn an_empty_batch_buffers_nothing() {
        let mut sink = sink("table \"orders\"\nflush_rows 1000");
        let batch = batch(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("label", DataType::Utf8, true),
                Field::new("seq", DataType::Int64, false),
            ],
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
                Arc::new(Int64Array::from(Vec::<i64>::new())),
            ],
        );
        sink.write_batch(&batch).await.expect("accepted");
        assert_eq!(sink.pending_rows(), Some(0));
        assert!(sink.buffer.is_empty());
    }

    #[tokio::test]
    async fn a_renamed_column_is_rejected_by_name() {
        let mut sink = sink("table \"orders\"");
        let batch = batch(
            vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new("seq", DataType::Int64, false),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        );
        let err = sink.write_batch(&batch).await.unwrap_err();
        assert!(err.message().contains("'name'"), "{}", err.message());
        assert!(err.message().contains("'label'"), "{}", err.message());
    }

    #[tokio::test]
    async fn a_retyped_column_is_rejected_by_type() {
        let mut sink = sink("table \"orders\"");
        let batch = batch(
            vec![
                Field::new("id", DataType::Int32, false),
                Field::new("label", DataType::Utf8, true),
                Field::new("seq", DataType::Int64, false),
            ],
            vec![
                Arc::new(arrow_array::Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        );
        let err = sink.write_batch(&batch).await.unwrap_err();
        assert!(err.message().contains("Int32"), "{}", err.message());
        assert!(err.message().contains("Int64"), "{}", err.message());
    }

    #[tokio::test]
    async fn a_column_count_mismatch_is_rejected() {
        let mut sink = sink("table \"orders\"");
        let batch = batch(
            vec![Field::new("id", DataType::Int64, false)],
            vec![Arc::new(Int64Array::from(vec![1]))],
        );
        let err = sink.write_batch(&batch).await.unwrap_err();
        assert!(err.message().contains("1 column(s)"), "{}", err.message());
        assert!(err.message().contains("declares 3"), "{}", err.message());
    }

    #[test]
    fn the_declared_schema_is_handed_out_unchanged() {
        let sink = sink("table \"orders\"");
        let schema = sink.schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert!(!schema.field(0).is_nullable());
        assert!(schema.field(1).is_nullable());
        assert_eq!(schema.field(2).data_type(), &DataType::Int64);
        // Same Arc every call: the trait requires a stable schema.
        assert!(Arc::ptr_eq(&schema, &sink.schema()));
    }
}
