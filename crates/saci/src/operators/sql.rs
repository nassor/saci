//! Embedded zero-dependency SQL query engine ([`SqlQuery`] and [`SqlSystem`]).
//!
//! Supports declarative queries of the form:
//!
//! ```sql
//! SELECT [DISTINCT] <select_list>
//! FROM <component> [[AS] <alias>]
//! [ [INNER|LEFT|RIGHT|FULL|CROSS|SEMI|ANTI] JOIN <component> [[AS] <alias>] [ON <cond>] ]...
//! [WHERE <predicate>]
//! [GROUP BY <expr>, ...]
//! [HAVING <predicate>]
//! [ORDER BY <expr> [ASC|DESC] [NULLS FIRST|NULLS LAST], ...]
//! [LIMIT <n> [OFFSET <m>]]
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{Field, Schema, SchemaRef};
use async_trait::async_trait;

use crate::dataset::{Dataset, intern_component_name};
use crate::error::{SaciError, SaciResult};
use crate::system::{System, SystemMeta};

use super::aggregate::{AggregateOp, AggregateSpec, aggregate_output_field};
use super::deduplicate::DeduplicateSystem;
use super::expr::{BinaryOp, Expr, ExprParser, ScalarValue, Token, tokenize};
use super::group_by::GroupBySystem;
use super::join::{JoinKey, JoinKind, JoinSystem};
use super::sort::{SortKey, SortSystem};

/// Table reference in `FROM` or `JOIN`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlTableRef {
    /// Component name in [`Dataset`].
    pub component: String,
    /// Qualifier alias (`component` when omitted).
    pub alias: String,
}

/// A parsed `JOIN` clause in a [`SqlQuery`].
#[derive(Debug, Clone, PartialEq)]
pub struct SqlJoinClause {
    /// Join semantic kind (`None` represents `CROSS JOIN`).
    pub kind: Option<JoinKind>,
    /// Right-hand component reference.
    pub table: SqlTableRef,
    /// Optional `ON` join condition.
    pub on: Option<Expr>,
}

/// An item in the `SELECT` projection list.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlSelectItem {
    /// `*` or `qualifier.*`
    Wildcard {
        /// Optional table/alias qualifier before `.*`.
        qualifier: Option<String>,
    },
    /// `<expr> [AS <alias>]`
    Expr {
        /// Scalar or aggregate expression.
        expr: SqlExpr,
        /// Output column name (inferred when omitted).
        alias: Option<String>,
    },
}

/// Expression node extended with SQL aggregate calls for [`SqlQuery`].
#[derive(Debug, Clone, PartialEq)]
pub enum SqlExpr {
    /// Pure scalar expression.
    Scalar(Expr),
    /// Aggregate call such as `SUM(amount)` or `COUNT(*)`.
    Aggregate {
        /// Aggregate operation.
        op: AggregateOp,
        /// Input expression (`None` for `COUNT(*)`).
        arg: Option<Box<Expr>>,
    },
    /// Binary combination of two SQL expressions (e.g. `SUM(a) / COUNT(*)`).
    Binary {
        /// Left sub-expression.
        left: Box<SqlExpr>,
        /// Binary operator.
        op: BinaryOp,
        /// Right sub-expression.
        right: Box<SqlExpr>,
    },
}

/// Parsed SQL `ORDER BY` item.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlOrderItem {
    /// Ordering expression.
    pub expr: SqlExpr,
    /// `true` for ascending (`ASC`, default), `false` for `DESC`.
    pub ascending: bool,
    /// `Some(true)` for `NULLS FIRST`, `Some(false)` for `NULLS LAST`, `None` for Arrow default.
    pub nulls_first: Option<bool>,
}

/// Parsed AST of a single SQL `SELECT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlQuery {
    /// `SELECT DISTINCT` flag.
    pub distinct: bool,
    /// Projection items.
    pub select: Vec<SqlSelectItem>,
    /// Base `FROM` table.
    pub from: SqlTableRef,
    /// Ordered `JOIN` clauses.
    pub joins: Vec<SqlJoinClause>,
    /// Optional `WHERE` predicate.
    pub where_clause: Option<Expr>,
    /// `GROUP BY` expressions.
    pub group_by: Vec<Expr>,
    /// Optional `HAVING` predicate over grouped columns / aggregates.
    pub having: Option<SqlExpr>,
    /// `ORDER BY` specification.
    pub order_by: Vec<SqlOrderItem>,
    /// Optional `LIMIT`.
    pub limit: Option<usize>,
    /// Optional `OFFSET`.
    pub offset: usize,
}

impl SqlQuery {
    /// Parses a SQL `SELECT` query string into a [`SqlQuery`] AST.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] on syntax errors or unsupported clauses.
    pub fn parse(sql: &str) -> SaciResult<Self> {
        let tokens = tokenize(sql)?;
        let mut p = SqlParser::new(&tokens);
        let q = p.parse_query()?;
        if !p.inner.is_eof() {
            return Err(SaciError::configuration(format!(
                "unexpected trailing token '{}' in SQL query",
                p.inner.peek_display()
            )));
        }
        Ok(q)
    }

    /// Returns all unique component names read by this query (`FROM` and `JOIN`s).
    #[must_use]
    pub fn source_components(&self) -> Vec<&str> {
        let mut out = vec![self.from.component.as_str()];
        for j in &self.joins {
            if !out.contains(&j.table.component.as_str()) {
                out.push(j.table.component.as_str());
            }
        }
        out
    }

    fn is_grouped(&self) -> bool {
        !self.group_by.is_empty()
            || self.having.is_some()
            || self.select.iter().any(|item| match item {
                SqlSelectItem::Expr { expr, .. } => expr.contains_aggregate(),
                SqlSelectItem::Wildcard { .. } => false,
            })
    }
}

impl SqlExpr {
    fn contains_aggregate(&self) -> bool {
        match self {
            Self::Scalar(_) => false,
            Self::Aggregate { .. } => true,
            Self::Binary { left, right, .. } => {
                left.contains_aggregate() || right.contains_aggregate()
            }
        }
    }

    fn default_alias(&self) -> String {
        match self {
            Self::Scalar(e) => e.default_alias(),
            Self::Aggregate { op, arg } => {
                let col = arg
                    .as_ref()
                    .map_or_else(|| "all".to_string(), |e| e.default_alias());
                AggregateSpec::new(col, op.clone()).output_name()
            }
            Self::Binary { .. } => "expr".to_string(),
        }
    }

    fn to_pure_scalar(&self) -> SaciResult<Expr> {
        match self {
            Self::Scalar(e) => Ok(e.clone()),
            Self::Binary { left, op, right } => Ok(Expr::binary(
                left.to_pure_scalar()?,
                *op,
                right.to_pure_scalar()?,
            )),
            Self::Aggregate { op, .. } => Err(SaciError::configuration(format!(
                "aggregate function '{op:?}' cannot appear in this clause"
            ))),
        }
    }
}

/// Executes a parsed [`SqlQuery`] over a [`Dataset`], writing its output batch to
/// `target_component`.
#[derive(Debug, Clone)]
pub struct SqlSystem {
    query: SqlQuery,
    reads: Vec<&'static str>,
    target_component: &'static str,
}

impl SqlSystem {
    /// Parses `sql` and writes the query result to `target_component`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] on invalid SQL syntax.
    pub fn parse(target_component: impl AsRef<str>, sql: &str) -> SaciResult<Self> {
        let query = SqlQuery::parse(sql)?;
        let reads = query
            .source_components()
            .into_iter()
            .map(intern_component_name)
            .collect();
        let target_component = intern_component_name(target_component.as_ref());
        Ok(Self {
            query,
            reads,
            target_component,
        })
    }

    /// Parses `sql` and overwrites `component` in place.
    ///
    /// # Errors
    ///
    /// Same as [`Self::parse`].
    pub fn in_place(component: impl AsRef<str>, sql: &str) -> SaciResult<Self> {
        Self::parse(component, sql)
    }

    /// Returns the parsed [`SqlQuery`].
    #[must_use]
    pub fn query(&self) -> &SqlQuery {
        &self.query
    }

    /// Returns the deduplicated list of source components read by this query.
    #[must_use]
    pub fn source_components(&self) -> &[&'static str] {
        &self.reads
    }

    /// Infers the output Arrow [`SchemaRef`] given `(component_name, Schema)` pairs.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError`] if any referenced source schema or column is missing.
    pub fn infer_output_schema(&self, schemas: &[(&str, &Schema)]) -> SaciResult<SchemaRef> {
        let mut empty_batches = HashMap::with_capacity(schemas.len());
        for (name, schema) in schemas {
            empty_batches.insert(*name, RecordBatch::new_empty(Arc::new((*schema).clone())));
        }
        let result = self.execute_batches(&empty_batches)?;
        Ok(result.schema())
    }

    /// Executes the SQL query against named input [`RecordBatch`]es.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError`] when any referenced component or column is missing or an
    /// expression/aggregate fails.
    pub fn execute_batches(&self, inputs: &HashMap<&str, RecordBatch>) -> SaciResult<RecordBatch> {
        let base_raw = inputs
            .get(self.query.from.component.as_str())
            .ok_or_else(|| SaciError::component_not_registered(&self.query.from.component))?;

        let (mut working, mut col_origins) = qualify_batch(base_raw, &self.query.from.alias);

        for join in &self.query.joins {
            let right_raw = inputs
                .get(join.table.component.as_str())
                .ok_or_else(|| SaciError::component_not_registered(&join.table.component))?;
            let (right_q, right_origins) = qualify_batch(right_raw, &join.table.alias);

            working = execute_sql_join(&working, &right_q, join, &col_origins)?;
            if !matches!(join.kind, Some(JoinKind::Semi) | Some(JoinKind::Anti)) {
                col_origins.extend(right_origins);
            }
        }

        // WHERE
        if let Some(ref pred) = self.query.where_clause {
            let rewritten = qualify_expr_refs(pred, &col_origins)?;
            let mask = rewritten.evaluate_predicate(&working)?;
            working = arrow_select::filter::filter_record_batch(&working, &mask)
                .map_err(|e| SaciError::generic(format!("SQL WHERE filter: {e}")))?;
        }

        // GROUP BY + aggregates or plain SELECT
        let is_grouped = self.query.is_grouped();
        let pre_project_working = if !is_grouped && !self.query.order_by.is_empty() {
            Some(working.clone())
        } else {
            None
        };
        let (projected, _out_origins) = if is_grouped {
            execute_grouped_select(&self.query, &working, &col_origins)?
        } else {
            execute_flat_select(&self.query, &working, &col_origins)?
        };

        working = projected;

        // DISTINCT
        if self.query.distinct {
            let all_cols: Vec<String> = working
                .schema_ref()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            let dedup = DeduplicateSystem::in_place("_sql", all_cols)?.stateful(false);
            working = dedup.deduplicate_batch_stateless(&working)?;
        }

        // ORDER BY (grouped queries were already sorted inside execute_grouped_select)
        if !is_grouped && !self.query.order_by.is_empty() {
            working = execute_order_by(
                &self.query.order_by,
                &working,
                pre_project_working.as_ref(),
                &col_origins,
            )?;
        }

        // OFFSET / LIMIT
        if self.query.offset > 0 || self.query.limit.is_some() {
            let n = working.num_rows();
            let offset = self.query.offset.min(n);
            let avail = n - offset;
            let take_len = self.query.limit.map_or(avail, |l| l.min(avail));
            working = working.slice(offset, take_len);
        }

        Ok(working)
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let mut owned: Vec<(&'static str, RecordBatch)> = Vec::with_capacity(self.reads.len());
        for &comp in &self.reads {
            owned.push((comp, data.batch_named(comp)?.clone()));
        }
        let inputs: HashMap<&str, RecordBatch> =
            owned.iter().map(|(k, b)| (*k, b.clone())).collect();
        let out = self.execute_batches(&inputs)?;
        data.put_batch_named(self.target_component, out)
    }
}

#[async_trait]
impl System for SqlSystem {
    fn meta(&self) -> SystemMeta {
        let mut meta = SystemMeta::new("sql");
        for &r in &self.reads {
            meta = meta.reads_all_named(r);
        }
        meta.writes_all_named(self.target_component)
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        self.run_impl(data)
    }

    fn run_sync(&self, data: &mut Dataset) -> Option<Result<(), SaciError>> {
        Some(self.run_impl(data))
    }
}

// ---------------------------------------------------------------------------
// Query execution helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ColumnOrigin {
    internal_name: String,
    qualifier: String,
    bare_name: String,
}

fn qualify_batch(batch: &RecordBatch, qualifier: &str) -> (RecordBatch, Vec<ColumnOrigin>) {
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut origins = Vec::with_capacity(batch.num_columns());
    for (idx, f) in batch.schema_ref().fields().iter().enumerate() {
        let internal = format!("{qualifier}.{}#{idx}", f.name());
        fields.push(Field::new(&internal, f.data_type().clone(), true));
        origins.push(ColumnOrigin {
            internal_name: internal,
            qualifier: qualifier.to_string(),
            bare_name: f.name().clone(),
        });
    }
    let schema = Arc::new(Schema::new(fields));
    let out = RecordBatch::try_new(schema, batch.columns().to_vec())
        .unwrap_or_else(|_| RecordBatch::new_empty(Arc::new(Schema::empty())));
    (out, origins)
}

fn resolve_origin<'a>(origins: &'a [ColumnOrigin], ref_name: &str) -> SaciResult<&'a str> {
    if let Some((q, bare)) = ref_name.split_once('.') {
        let matches: Vec<&ColumnOrigin> = origins
            .iter()
            .filter(|o| o.qualifier == q && o.bare_name == bare)
            .collect();
        if matches.len() == 1 {
            return Ok(matches[0].internal_name.as_str());
        }
        if matches.is_empty() {
            return Err(SaciError::field_not_found("SqlSystem", ref_name));
        }
        return Err(SaciError::configuration(format!(
            "ambiguous column '{ref_name}'"
        )));
    }
    let matches: Vec<&ColumnOrigin> = origins.iter().filter(|o| o.bare_name == ref_name).collect();
    if matches.len() == 1 {
        return Ok(matches[0].internal_name.as_str());
    }
    if matches.is_empty() {
        return Err(SaciError::field_not_found("SqlSystem", ref_name));
    }
    Err(SaciError::configuration(format!(
        "ambiguous column '{ref_name}' (qualify with table alias)"
    )))
}

fn qualify_expr_refs(expr: &Expr, origins: &[ColumnOrigin]) -> SaciResult<Expr> {
    match expr {
        Expr::Column(name) => Ok(Expr::Column(resolve_origin(origins, name)?.to_string())),
        Expr::Literal(v) => Ok(Expr::Literal(v.clone())),
        Expr::Binary { left, op, right } => Ok(Expr::Binary {
            left: Box::new(qualify_expr_refs(left, origins)?),
            op: *op,
            right: Box::new(qualify_expr_refs(right, origins)?),
        }),
        Expr::Unary { op, expr } => Ok(Expr::Unary {
            op: *op,
            expr: Box::new(qualify_expr_refs(expr, origins)?),
        }),
        Expr::Cast { expr, data_type } => Ok(Expr::Cast {
            expr: Box::new(qualify_expr_refs(expr, origins)?),
            data_type: data_type.clone(),
        }),
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let op = operand
                .as_deref()
                .map(|e| qualify_expr_refs(e, origins))
                .transpose()?
                .map(Box::new);
            let mut wt = Vec::with_capacity(when_then.len());
            for (w, t) in when_then {
                wt.push((
                    qualify_expr_refs(w, origins)?,
                    qualify_expr_refs(t, origins)?,
                ));
            }
            let el = else_expr
                .as_deref()
                .map(|e| qualify_expr_refs(e, origins))
                .transpose()?
                .map(Box::new);
            Ok(Expr::Case {
                operand: op,
                when_then: wt,
                else_expr: el,
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let mut q_list = Vec::with_capacity(list.len());
            for item in list {
                q_list.push(qualify_expr_refs(item, origins)?);
            }
            Ok(Expr::InList {
                expr: Box::new(qualify_expr_refs(expr, origins)?),
                list: q_list,
                negated: *negated,
            })
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Ok(Expr::Between {
            expr: Box::new(qualify_expr_refs(expr, origins)?),
            low: Box::new(qualify_expr_refs(low, origins)?),
            high: Box::new(qualify_expr_refs(high, origins)?),
            negated: *negated,
        }),
        Expr::Call { func, args } => {
            let mut q_args = Vec::with_capacity(args.len());
            for a in args {
                q_args.push(qualify_expr_refs(a, origins)?);
            }
            Ok(Expr::Call {
                func: *func,
                args: q_args,
            })
        }
    }
}

fn execute_sql_join(
    left: &RecordBatch,
    right: &RecordBatch,
    clause: &SqlJoinClause,
    left_origins: &[ColumnOrigin],
) -> SaciResult<RecordBatch> {
    let right_origins: Vec<ColumnOrigin> = right
        .schema_ref()
        .fields()
        .iter()
        .map(|f| ColumnOrigin {
            internal_name: f.name().clone(),
            qualifier: clause.table.alias.clone(),
            bare_name: f
                .name()
                .split_once('.')
                .and_then(|(_, rest)| rest.rsplit_once('#').map(|(b, _)| b.to_string()))
                .unwrap_or_else(|| f.name().clone()),
        })
        .collect();

    let combined_origins: Vec<ColumnOrigin> = left_origins
        .iter()
        .cloned()
        .chain(right_origins.iter().cloned())
        .collect();

    // Cross join (no ON clause)
    let Some(kind) = clause.kind else {
        return cross_join_batches(left, right);
    };

    let on_expr = clause.on.as_ref().ok_or_else(|| {
        SaciError::configuration(format!("{kind:?} JOIN requires an ON condition"))
    })?;

    let mut conjuncts = Vec::new();
    split_conjuncts(on_expr, &mut conjuncts);

    let left_set: HashSet<&str> = left_origins
        .iter()
        .map(|o| o.internal_name.as_str())
        .collect();
    let right_set: HashSet<&str> = right_origins
        .iter()
        .map(|o| o.internal_name.as_str())
        .collect();

    let mut keys = Vec::new();
    let mut residual = Vec::new();

    for c in conjuncts {
        if let Expr::Binary {
            left: l,
            op: BinaryOp::Eq,
            right: r,
        } = c
            && let (Expr::Column(ln), Expr::Column(rn)) = (l.as_ref(), r.as_ref())
        {
            let li = resolve_origin(&combined_origins, ln)?;
            let ri = resolve_origin(&combined_origins, rn)?;
            if left_set.contains(li) && right_set.contains(ri) {
                keys.push(JoinKey::new(li, ri));
                continue;
            }
            if left_set.contains(ri) && right_set.contains(li) {
                keys.push(JoinKey::new(ri, li));
                continue;
            }
        }
        residual.push(qualify_expr_refs(c, &combined_origins)?);
    }

    if keys.is_empty() {
        return Err(SaciError::configuration(
            "SQL JOIN ON condition must contain at least one equality key across left and right inputs",
        ));
    }

    let join = JoinSystem::new("_l", "_r", "_out", kind, keys)?;
    let mut joined = join.join_batches(left, right)?;

    if !residual.is_empty() && !matches!(kind, JoinKind::Semi | JoinKind::Anti) {
        let mut combined_pred = residual.remove(0);
        for next in residual {
            combined_pred = Expr::binary(combined_pred, BinaryOp::And, next);
        }
        let mask = combined_pred.evaluate_predicate(&joined)?;
        joined = arrow_select::filter::filter_record_batch(&joined, &mask)
            .map_err(|e| SaciError::generic(format!("SQL JOIN ON residual filter: {e}")))?;
    }

    Ok(joined)
}

fn split_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } => {
            split_conjuncts(left, out);
            split_conjuncts(right, out);
        }
        other => out.push(other),
    }
}

fn cross_join_batches(left: &RecordBatch, right: &RecordBatch) -> SaciResult<RecordBatch> {
    let mut fields: Vec<Field> = left
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.extend(
        right
            .schema_ref()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone()),
    );
    let schema = Arc::new(Schema::new(fields));
    let ln = left.num_rows();
    let rn = right.num_rows();
    if ln == 0 || rn == 0 {
        return Ok(RecordBatch::new_empty(schema));
    }

    let mut l_indices = Vec::with_capacity(ln * rn);
    let mut r_indices = Vec::with_capacity(ln * rn);
    for li in 0..ln {
        for ri in 0..rn {
            l_indices.push(li as u32);
            r_indices.push(ri as u32);
        }
    }
    let l_idx = arrow_array::UInt32Array::from(l_indices);
    let r_idx = arrow_array::UInt32Array::from(r_indices);

    let mut cols = Vec::with_capacity(schema.fields().len());
    for c in left.columns() {
        cols.push(
            arrow_select::take::take(c.as_ref(), &l_idx, None)
                .map_err(|e| SaciError::generic(format!("CROSS JOIN left take: {e}")))?,
        );
    }
    for c in right.columns() {
        cols.push(
            arrow_select::take::take(c.as_ref(), &r_idx, None)
                .map_err(|e| SaciError::generic(format!("CROSS JOIN right take: {e}")))?,
        );
    }
    RecordBatch::try_new(schema, cols)
        .map_err(|e| SaciError::generic(format!("CROSS JOIN batch: {e}")))
}

fn execute_flat_select(
    query: &SqlQuery,
    working: &RecordBatch,
    origins: &[ColumnOrigin],
) -> SaciResult<(RecordBatch, Vec<ColumnOrigin>)> {
    let mut fields = Vec::new();
    let mut cols = Vec::new();
    let mut used_names = HashSet::new();
    let mut out_origins = Vec::new();

    for item in &query.select {
        match item {
            SqlSelectItem::Wildcard { qualifier } => {
                let mut any = false;
                for (idx, origin) in origins.iter().enumerate() {
                    if let Some(q) = qualifier
                        && origin.qualifier != *q
                    {
                        continue;
                    }
                    any = true;
                    let mut out_name = origin.bare_name.clone();
                    if !used_names.insert(out_name.clone()) {
                        out_name = format!("{}.{}", origin.qualifier, origin.bare_name);
                        used_names.insert(out_name.clone());
                    }
                    fields.push(Field::new(
                        &out_name,
                        working.column(idx).data_type().clone(),
                        true,
                    ));
                    cols.push(working.column(idx).clone());
                    out_origins.push(ColumnOrigin {
                        internal_name: out_name,
                        qualifier: origin.qualifier.clone(),
                        bare_name: origin.bare_name.clone(),
                    });
                }
                if !any && let Some(q) = qualifier {
                    return Err(SaciError::configuration(format!(
                        "unknown qualifier '{q}' in SELECT {q}.*"
                    )));
                }
            }
            SqlSelectItem::Expr { expr, alias } => {
                let scalar = expr.to_pure_scalar()?;
                let name = alias.clone().unwrap_or_else(|| scalar.default_alias());
                if !used_names.insert(name.clone()) {
                    return Err(SaciError::configuration(format!(
                        "duplicate SELECT output column '{name}'"
                    )));
                }
                let q_expr = qualify_expr_refs(&scalar, origins)?;
                let arr = q_expr.evaluate(working)?;
                fields.push(Field::new(&name, arr.data_type().clone(), true));
                cols.push(arr);
                out_origins.push(ColumnOrigin {
                    internal_name: name.clone(),
                    qualifier: String::new(),
                    bare_name: name,
                });
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, cols)
        .map_err(|e| SaciError::generic(format!("SQL projection batch: {e}")))?;
    Ok((batch, out_origins))
}

fn execute_grouped_select(
    query: &SqlQuery,
    working: &RecordBatch,
    origins: &[ColumnOrigin],
) -> SaciResult<(RecordBatch, Vec<ColumnOrigin>)> {
    let mut pre_fields: Vec<Field> = working
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut pre_cols: Vec<ArrayRef> = working.columns().to_vec();

    // Materialize GROUP BY expressions into synthetic columns.
    let mut group_col_names = Vec::with_capacity(query.group_by.len());
    let mut group_expr_map: Vec<(Expr, String)> = Vec::with_capacity(query.group_by.len());

    for (i, g_expr) in query.group_by.iter().enumerate() {
        let q_expr = qualify_expr_refs(g_expr, origins)?;
        let col_name = format!("__sql_grp_{i}");
        let arr = q_expr.evaluate(working)?;
        pre_fields.push(Field::new(&col_name, arr.data_type().clone(), true));
        pre_cols.push(arr);
        group_col_names.push(col_name.clone());
        group_expr_map.push((q_expr, col_name));
    }

    // Extract aggregate specs from SELECT, HAVING, ORDER BY.
    let mut agg_specs: Vec<AggregateSpec> = Vec::new();
    let mut agg_counter = 0usize;

    let mut rewrite = |sql_expr: &SqlExpr| -> SaciResult<Expr> {
        rewrite_sql_expr_for_grouped(
            sql_expr,
            origins,
            &group_expr_map,
            working,
            &mut pre_fields,
            &mut pre_cols,
            &mut agg_specs,
            &mut agg_counter,
        )
    };

    let mut select_rewritten = Vec::with_capacity(query.select.len());
    for item in &query.select {
        match item {
            SqlSelectItem::Wildcard { .. } => {
                return Err(SaciError::configuration(
                    "SELECT * cannot be combined with GROUP BY or aggregate functions",
                ));
            }
            SqlSelectItem::Expr { expr, alias } => {
                let re = rewrite(expr)?;
                let name = alias.clone().unwrap_or_else(|| expr.default_alias());
                select_rewritten.push((name, re));
            }
        }
    }

    let having_rewritten = query.having.as_ref().map(&mut rewrite).transpose()?;
    let mut order_rewritten = Vec::with_capacity(query.order_by.len());
    for ord in &query.order_by {
        let resolved = if let SqlExpr::Scalar(Expr::Column(col_name)) = &ord.expr
            && let Some((_, aliased_expr)) =
                select_rewritten.iter().find(|(alias, _)| alias == col_name)
        {
            aliased_expr.clone()
        } else {
            rewrite(&ord.expr)?
        };
        order_rewritten.push((resolved, ord.ascending, ord.nulls_first));
    }

    // GroupBySystem requires at least one AggregateSpec; add a dummy CountAll if query
    // had only GROUP BY columns (e.g. `SELECT region FROM orders GROUP BY region`).
    if agg_specs.is_empty() {
        agg_specs.push(AggregateSpec::count_all().alias("__sql_dummy_cnt"));
    }

    let pre_batch = RecordBatch::try_new(Arc::new(Schema::new(pre_fields)), pre_cols)
        .map_err(|e| SaciError::generic(format!("SQL pre-group batch: {e}")))?;

    let group_sys = GroupBySystem::new("_pre", "_grp", &group_col_names, agg_specs)?;
    let mut grouped = group_sys.execute_batch(&pre_batch)?;

    if let Some(having_expr) = having_rewritten
        && grouped.num_rows() > 0
    {
        let mask = having_expr.evaluate_predicate(&grouped)?;
        grouped = arrow_select::filter::filter_record_batch(&grouped, &mask)
            .map_err(|e| SaciError::generic(format!("SQL HAVING: {e}")))?;
    }

    // Apply ORDER BY before dropping hidden grouped columns so `ORDER BY SUM(amount)` works even if unprojected!
    if !order_rewritten.is_empty() {
        grouped = sort_batch_by_exprs(&grouped, &order_rewritten)?;
    }

    let mut fields = Vec::with_capacity(select_rewritten.len());
    let mut cols = Vec::with_capacity(select_rewritten.len());
    let mut used = HashSet::new();
    let mut out_origins = Vec::with_capacity(select_rewritten.len());

    for (name, expr) in select_rewritten {
        if !used.insert(name.clone()) {
            return Err(SaciError::configuration(format!(
                "duplicate SELECT output column '{name}'"
            )));
        }
        let arr = expr.evaluate(&grouped)?;
        fields.push(Field::new(&name, arr.data_type().clone(), true));
        cols.push(arr);
        out_origins.push(ColumnOrigin {
            internal_name: name.clone(),
            qualifier: String::new(),
            bare_name: name,
        });
    }

    let out = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
        .map_err(|e| SaciError::generic(format!("SQL grouped select batch: {e}")))?;
    Ok((out, out_origins))
}

#[allow(clippy::too_many_arguments)]
fn rewrite_sql_expr_for_grouped(
    sql_expr: &SqlExpr,
    origins: &[ColumnOrigin],
    group_exprs: &[(Expr, String)],
    pre_batch: &RecordBatch,
    pre_fields: &mut Vec<Field>,
    pre_cols: &mut Vec<ArrayRef>,
    agg_specs: &mut Vec<AggregateSpec>,
    agg_counter: &mut usize,
) -> SaciResult<Expr> {
    match sql_expr {
        SqlExpr::Aggregate { op, arg } => {
            let alias = format!("__sql_agg_{}", *agg_counter);
            *agg_counter += 1;

            let spec = match arg {
                None => {
                    if *op != AggregateOp::CountAll {
                        return Err(SaciError::configuration(format!(
                            "{op:?}(*) is only valid for COUNT(*)"
                        )));
                    }
                    AggregateSpec::count_all().alias(&alias)
                }
                Some(inner) => {
                    let q_inner = qualify_expr_refs(inner, origins)?;
                    let input_col = format!("__sql_agg_in_{}", *agg_counter);
                    let arr = q_inner.evaluate(pre_batch)?;
                    pre_fields.push(Field::new(&input_col, arr.data_type().clone(), true));
                    pre_cols.push(arr);
                    AggregateSpec::new(&input_col, op.clone()).alias(&alias)
                }
            };
            spec.validate()?;
            // Validate output type compatibility against pre_batch schema early.
            let tmp_schema = Schema::new(pre_fields.clone());
            let _ = aggregate_output_field(&spec, &tmp_schema)?;
            agg_specs.push(spec);
            Ok(Expr::Column(alias))
        }
        SqlExpr::Binary { left, op, right } => Ok(Expr::binary(
            rewrite_sql_expr_for_grouped(
                left,
                origins,
                group_exprs,
                pre_batch,
                pre_fields,
                pre_cols,
                agg_specs,
                agg_counter,
            )?,
            *op,
            rewrite_sql_expr_for_grouped(
                right,
                origins,
                group_exprs,
                pre_batch,
                pre_fields,
                pre_cols,
                agg_specs,
                agg_counter,
            )?,
        )),
        SqlExpr::Scalar(scalar) => rewrite_scalar_against_groups(scalar, origins, group_exprs),
    }
}

fn rewrite_scalar_against_groups(
    expr: &Expr,
    origins: &[ColumnOrigin],
    group_exprs: &[(Expr, String)],
) -> SaciResult<Expr> {
    let q_expr = qualify_expr_refs(expr, origins)?;
    for (g_expr, g_col) in group_exprs {
        if *g_expr == q_expr {
            return Ok(Expr::Column(g_col.clone()));
        }
    }
    match expr {
        Expr::Column(name) => Err(SaciError::configuration(format!(
            "column '{name}' must appear in GROUP BY or be inside an aggregate function"
        ))),
        Expr::Literal(v) => Ok(Expr::Literal(v.clone())),
        Expr::Binary { left, op, right } => Ok(Expr::Binary {
            left: Box::new(rewrite_scalar_against_groups(left, origins, group_exprs)?),
            op: *op,
            right: Box::new(rewrite_scalar_against_groups(right, origins, group_exprs)?),
        }),
        Expr::Unary { op, expr } => Ok(Expr::Unary {
            op: *op,
            expr: Box::new(rewrite_scalar_against_groups(expr, origins, group_exprs)?),
        }),
        Expr::Cast { expr, data_type } => Ok(Expr::Cast {
            expr: Box::new(rewrite_scalar_against_groups(expr, origins, group_exprs)?),
            data_type: data_type.clone(),
        }),
        Expr::Call { func, args } => {
            let mut out = Vec::with_capacity(args.len());
            for a in args {
                out.push(rewrite_scalar_against_groups(a, origins, group_exprs)?);
            }
            Ok(Expr::Call {
                func: *func,
                args: out,
            })
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let op = operand
                .as_deref()
                .map(|e| rewrite_scalar_against_groups(e, origins, group_exprs))
                .transpose()?
                .map(Box::new);
            let mut wt = Vec::with_capacity(when_then.len());
            for (w, t) in when_then {
                wt.push((
                    rewrite_scalar_against_groups(w, origins, group_exprs)?,
                    rewrite_scalar_against_groups(t, origins, group_exprs)?,
                ));
            }
            let el = else_expr
                .as_deref()
                .map(|e| rewrite_scalar_against_groups(e, origins, group_exprs))
                .transpose()?
                .map(Box::new);
            Ok(Expr::Case {
                operand: op,
                when_then: wt,
                else_expr: el,
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let mut out_list = Vec::with_capacity(list.len());
            for i in list {
                out_list.push(rewrite_scalar_against_groups(i, origins, group_exprs)?);
            }
            Ok(Expr::InList {
                expr: Box::new(rewrite_scalar_against_groups(expr, origins, group_exprs)?),
                list: out_list,
                negated: *negated,
            })
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Ok(Expr::Between {
            expr: Box::new(rewrite_scalar_against_groups(expr, origins, group_exprs)?),
            low: Box::new(rewrite_scalar_against_groups(low, origins, group_exprs)?),
            high: Box::new(rewrite_scalar_against_groups(high, origins, group_exprs)?),
            negated: *negated,
        }),
    }
}

fn execute_order_by(
    order_by: &[SqlOrderItem],
    batch: &RecordBatch,
    pre_project: Option<&RecordBatch>,
    origins: &[ColumnOrigin],
) -> SaciResult<RecordBatch> {
    if batch.num_rows() <= 1 {
        return Ok(batch.clone());
    }
    let orig_cols = batch.num_columns();
    let mut fields: Vec<Field> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
    let mut sort_keys = Vec::with_capacity(order_by.len());

    for (idx, item) in order_by.iter().enumerate() {
        let e = item.expr.to_pure_scalar()?;
        let arr = match e.evaluate(batch) {
            Ok(a) => a,
            Err(err) => {
                if let Some(pre) = pre_project
                    && pre.num_rows() == batch.num_rows()
                {
                    let qe = qualify_expr_refs(&e, origins)?;
                    qe.evaluate(pre)?
                } else {
                    return Err(err);
                }
            }
        };
        let key_name = format!("__sql_ord_{idx}");
        fields.push(Field::new(&key_name, arr.data_type().clone(), true));
        cols.push(arr);
        let mut sk = if item.ascending {
            SortKey::asc(&key_name)
        } else {
            SortKey::desc(&key_name)
        };
        if let Some(nf) = item.nulls_first {
            sk = sk.nulls_first(nf);
        }
        sort_keys.push(sk);
    }

    let ext = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
        .map_err(|e| SaciError::generic(format!("SQL sort prepare: {e}")))?;
    let sorted = SortSystem::in_place("_s", sort_keys)?.execute_batch(&ext)?;

    let out_fields: Vec<Field> = sorted.schema_ref().fields()[..orig_cols]
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let out_cols = sorted.columns()[..orig_cols].to_vec();
    RecordBatch::try_new(Arc::new(Schema::new(out_fields)), out_cols)
        .map_err(|e| SaciError::generic(format!("SQL sort finish: {e}")))
}

fn sort_batch_by_exprs(
    batch: &RecordBatch,
    items: &[(Expr, bool, Option<bool>)],
) -> SaciResult<RecordBatch> {
    if batch.num_rows() <= 1 {
        return Ok(batch.clone());
    }
    let orig_cols = batch.num_columns();
    let mut fields: Vec<Field> = batch
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
    let mut sort_keys = Vec::with_capacity(items.len());

    for (idx, (expr, asc, nulls_first)) in items.iter().enumerate() {
        let key_name = format!("__sql_ord_{idx}");
        let arr = expr.evaluate(batch)?;
        fields.push(Field::new(&key_name, arr.data_type().clone(), true));
        cols.push(arr);
        let mut sk = if *asc {
            SortKey::asc(&key_name)
        } else {
            SortKey::desc(&key_name)
        };
        if let Some(nf) = nulls_first {
            sk = sk.nulls_first(*nf);
        }
        sort_keys.push(sk);
    }

    let ext = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
        .map_err(|e| SaciError::generic(format!("SQL sort prepare: {e}")))?;
    let sorted = SortSystem::in_place("_s", sort_keys)?.execute_batch(&ext)?;

    let out_fields: Vec<Field> = sorted.schema_ref().fields()[..orig_cols]
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    let out_cols = sorted.columns()[..orig_cols].to_vec();
    RecordBatch::try_new(Arc::new(Schema::new(out_fields)), out_cols)
        .map_err(|e| SaciError::generic(format!("SQL sort finish: {e}")))
}

// ---------------------------------------------------------------------------
// SQL Statement Parser
// ---------------------------------------------------------------------------

struct SqlParser<'a> {
    inner: ExprParser<'a>,
    tokens: &'a [Token],
}

impl<'a> SqlParser<'a> {
    fn new(tokens: &'a [Token]) -> Self {
        Self {
            inner: ExprParser::new(tokens),
            tokens,
        }
    }

    fn parse_query(&mut self) -> SaciResult<SqlQuery> {
        self.inner.expect_keyword("SELECT")?;
        let distinct = self.inner.eat_keyword("DISTINCT");
        let select = self.parse_select_list()?;
        self.inner.expect_keyword("FROM")?;
        let from = self.parse_table_ref()?;

        let mut joins = Vec::new();
        while let Some(join) = self.try_parse_join()? {
            joins.push(join);
        }

        let where_clause = if self.inner.eat_keyword("WHERE") {
            Some(self.inner.parse_expr()?)
        } else {
            None
        };

        let mut group_by = Vec::new();
        if self.inner.eat_keyword("GROUP") {
            self.inner.expect_keyword("BY")?;
            loop {
                group_by.push(self.inner.parse_expr()?);
                if !self.inner.eat(&Token::Comma) {
                    break;
                }
            }
        }

        let having = if self.inner.eat_keyword("HAVING") {
            Some(self.parse_sql_expr()?)
        } else {
            None
        };

        let mut order_by = Vec::new();
        if self.inner.eat_keyword("ORDER") {
            self.inner.expect_keyword("BY")?;
            loop {
                let expr = self.parse_sql_expr()?;
                let ascending = if self.inner.eat_keyword("DESC") {
                    false
                } else {
                    let _ = self.inner.eat_keyword("ASC");
                    true
                };
                let nulls_first = if self.inner.eat_keyword("NULLS") {
                    if self.inner.eat_keyword("FIRST") {
                        Some(true)
                    } else if self.inner.eat_keyword("LAST") {
                        Some(false)
                    } else {
                        return Err(SaciError::configuration(
                            "expected FIRST or LAST after NULLS",
                        ));
                    }
                } else {
                    None
                };
                order_by.push(SqlOrderItem {
                    expr,
                    ascending,
                    nulls_first,
                });
                if !self.inner.eat(&Token::Comma) {
                    break;
                }
            }
        }

        let limit = if self.inner.eat_keyword("LIMIT") {
            Some(self.inner.expect_usize()?)
        } else {
            None
        };

        let offset = if self.inner.eat_keyword("OFFSET") {
            self.inner.expect_usize()?
        } else {
            0
        };

        Ok(SqlQuery {
            distinct,
            select,
            from,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_select_list(&mut self) -> SaciResult<Vec<SqlSelectItem>> {
        let mut items = Vec::new();
        loop {
            if self.inner.eat(&Token::Star) {
                items.push(SqlSelectItem::Wildcard { qualifier: None });
            } else if matches!(
                (
                    self.tokens.get(self.inner.pos()),
                    self.tokens.get(self.inner.pos() + 1),
                    self.tokens.get(self.inner.pos() + 2),
                ),
                (
                    Some(Token::Ident(_)) | Some(Token::QuotedIdent(_)),
                    Some(Token::Dot),
                    Some(Token::Star),
                )
            ) {
                let q = self.inner.expect_ident_name()?;
                self.inner.expect(&Token::Dot)?;
                self.inner.expect(&Token::Star)?;
                items.push(SqlSelectItem::Wildcard { qualifier: Some(q) });
            } else {
                let expr = self.parse_sql_expr()?;
                let has_alias = self.inner.eat_keyword("AS") || self.is_implicit_alias();
                let alias = if has_alias {
                    Some(self.inner.expect_ident_name()?)
                } else {
                    None
                };
                items.push(SqlSelectItem::Expr { expr, alias });
            }

            if !self.inner.eat(&Token::Comma) {
                break;
            }
        }
        Ok(items)
    }

    fn is_implicit_alias(&self) -> bool {
        match self.inner.peek() {
            Some(Token::QuotedIdent(_)) => true,
            Some(Token::Ident(s)) => !is_clause_keyword(s),
            _ => false,
        }
    }

    fn parse_table_ref(&mut self) -> SaciResult<SqlTableRef> {
        let component = self.inner.expect_ident_name()?;
        let has_alias = self.inner.eat_keyword("AS") || self.is_implicit_alias();
        let alias = if has_alias {
            self.inner.expect_ident_name()?
        } else {
            component.clone()
        };
        Ok(SqlTableRef { component, alias })
    }

    fn try_parse_join(&mut self) -> SaciResult<Option<SqlJoinClause>> {
        let kind = if self.inner.eat_keyword("JOIN") {
            Some(JoinKind::Inner)
        } else if self.inner.eat_keyword("INNER") {
            self.inner.expect_keyword("JOIN")?;
            Some(JoinKind::Inner)
        } else if self.inner.eat_keyword("LEFT") {
            let _ = self.inner.eat_keyword("OUTER");
            self.inner.expect_keyword("JOIN")?;
            Some(JoinKind::Left)
        } else if self.inner.eat_keyword("RIGHT") {
            let _ = self.inner.eat_keyword("OUTER");
            self.inner.expect_keyword("JOIN")?;
            Some(JoinKind::Right)
        } else if self.inner.eat_keyword("FULL") {
            let _ = self.inner.eat_keyword("OUTER");
            self.inner.expect_keyword("JOIN")?;
            Some(JoinKind::Full)
        } else if self.inner.eat_keyword("SEMI") {
            self.inner.expect_keyword("JOIN")?;
            Some(JoinKind::Semi)
        } else if self.inner.eat_keyword("ANTI") {
            self.inner.expect_keyword("JOIN")?;
            Some(JoinKind::Anti)
        } else if self.inner.eat_keyword("CROSS") {
            self.inner.expect_keyword("JOIN")?;
            None
        } else {
            return Ok(None);
        };

        let table = self.parse_table_ref()?;
        let on = if kind.is_some() {
            self.inner.expect_keyword("ON")?;
            Some(self.inner.parse_expr()?)
        } else {
            None
        };
        Ok(Some(SqlJoinClause { kind, table, on }))
    }

    fn parse_sql_expr(&mut self) -> SaciResult<SqlExpr> {
        self.parse_sql_or()
    }

    fn parse_sql_or(&mut self) -> SaciResult<SqlExpr> {
        let mut left = self.parse_sql_and()?;
        while self.inner.eat_keyword("OR") {
            let right = self.parse_sql_and()?;
            left = SqlExpr::Binary {
                left: Box::new(left),
                op: BinaryOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_sql_and(&mut self) -> SaciResult<SqlExpr> {
        let mut left = self.parse_sql_cmp()?;
        while self.inner.eat_keyword("AND") {
            let right = self.parse_sql_cmp()?;
            left = SqlExpr::Binary {
                left: Box::new(left),
                op: BinaryOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_sql_cmp(&mut self) -> SaciResult<SqlExpr> {
        let mut left = self.parse_sql_add()?;
        loop {
            let op = if self.inner.eat(&Token::Eq) {
                Some(BinaryOp::Eq)
            } else if self.inner.eat(&Token::Ne) {
                Some(BinaryOp::Ne)
            } else if self.inner.eat(&Token::Lt) {
                Some(BinaryOp::Lt)
            } else if self.inner.eat(&Token::Le) {
                Some(BinaryOp::Le)
            } else if self.inner.eat(&Token::Gt) {
                Some(BinaryOp::Gt)
            } else if self.inner.eat(&Token::Ge) {
                Some(BinaryOp::Ge)
            } else {
                None
            };
            if let Some(op) = op {
                let right = self.parse_sql_add()?;
                left = SqlExpr::Binary {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_sql_add(&mut self) -> SaciResult<SqlExpr> {
        let mut left = self.parse_sql_mul()?;
        loop {
            if self.inner.eat(&Token::Plus) {
                let right = self.parse_sql_mul()?;
                left = SqlExpr::Binary {
                    left: Box::new(left),
                    op: BinaryOp::Add,
                    right: Box::new(right),
                };
            } else if self.inner.eat(&Token::Minus) {
                let right = self.parse_sql_mul()?;
                left = SqlExpr::Binary {
                    left: Box::new(left),
                    op: BinaryOp::Sub,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_sql_mul(&mut self) -> SaciResult<SqlExpr> {
        let mut left = self.parse_sql_atom()?;
        loop {
            if self.inner.eat(&Token::Star) {
                let right = self.parse_sql_atom()?;
                left = SqlExpr::Binary {
                    left: Box::new(left),
                    op: BinaryOp::Mul,
                    right: Box::new(right),
                };
            } else if self.inner.eat(&Token::Slash) {
                let right = self.parse_sql_atom()?;
                left = SqlExpr::Binary {
                    left: Box::new(left),
                    op: BinaryOp::Div,
                    right: Box::new(right),
                };
            } else if self.inner.eat(&Token::Percent) {
                let right = self.parse_sql_atom()?;
                left = SqlExpr::Binary {
                    left: Box::new(left),
                    op: BinaryOp::Mod,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_sql_atom(&mut self) -> SaciResult<SqlExpr> {
        if let Some(Token::Ident(name)) = self.inner.peek().cloned()
            && matches!(self.tokens.get(self.inner.pos() + 1), Some(Token::LParen))
            && is_aggregate_func_name(&name)
        {
            return self.parse_aggregate_call(&name);
        }
        // Only fall back to pure scalar expression if it does not contain an aggregate call inside parens.
        if self.inner.eat(&Token::LParen) {
            let inner = self.parse_sql_expr()?;
            self.inner.expect(&Token::RParen)?;
            return Ok(inner);
        }
        Ok(SqlExpr::Scalar(self.inner.parse_expr()?))
    }

    fn parse_aggregate_call(&mut self, name: &str) -> SaciResult<SqlExpr> {
        let _ = self.inner.expect_ident_name()?;
        self.inner.expect(&Token::LParen)?;
        let upper = name.to_ascii_uppercase();

        if upper == "COUNT" && self.inner.eat(&Token::Star) {
            self.inner.expect(&Token::RParen)?;
            return Ok(SqlExpr::Aggregate {
                op: AggregateOp::CountAll,
                arg: None,
            });
        }

        if upper == "COUNT" && self.inner.eat_keyword("DISTINCT") {
            let arg = self.inner.parse_expr()?;
            self.inner.expect(&Token::RParen)?;
            return Ok(SqlExpr::Aggregate {
                op: AggregateOp::CountDistinct,
                arg: Some(Box::new(arg)),
            });
        }

        let arg = self.inner.parse_expr()?;
        let op = match upper.as_str() {
            "COUNT" => AggregateOp::Count,
            "SUM" => AggregateOp::Sum,
            "MIN" => AggregateOp::Min,
            "MAX" => AggregateOp::Max,
            "AVG" | "MEAN" => AggregateOp::Mean,
            "VAR" | "VARIANCE" => AggregateOp::Variance,
            "STDDEV" | "STD" => AggregateOp::StdDev,
            "FIRST" => AggregateOp::First,
            "LAST" => AggregateOp::Last,
            "LIST_AGG" | "ARRAY_AGG" => AggregateOp::ListAgg,
            "APPROX_PERCENTILE" => {
                self.inner.expect(&Token::Comma)?;
                let q_expr = self.inner.parse_expr()?;
                let q = match q_expr {
                    Expr::Literal(ScalarValue::Float64(v)) => v,
                    Expr::Literal(ScalarValue::Int64(v)) => v as f64,
                    _ => {
                        return Err(SaciError::configuration(
                            "APPROX_PERCENTILE quantile must be a numeric literal",
                        ));
                    }
                };
                AggregateOp::ApproxPercentile(q)
            }
            "STRING_AGG" => {
                let separator = if self.inner.eat(&Token::Comma) {
                    match self.inner.parse_expr()? {
                        Expr::Literal(ScalarValue::Utf8(s)) => s,
                        _ => {
                            return Err(SaciError::configuration(
                                "STRING_AGG separator must be a string literal",
                            ));
                        }
                    }
                } else {
                    ",".to_string()
                };
                AggregateOp::StringAgg { separator }
            }
            _ => unreachable!(),
        };
        self.inner.expect(&Token::RParen)?;
        Ok(SqlExpr::Aggregate {
            op,
            arg: Some(Box::new(arg)),
        })
    }
}

fn is_clause_keyword(s: &str) -> bool {
    matches!(
        s.to_ascii_uppercase().as_str(),
        "FROM"
            | "WHERE"
            | "GROUP"
            | "HAVING"
            | "ORDER"
            | "LIMIT"
            | "OFFSET"
            | "JOIN"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "CROSS"
            | "SEMI"
            | "ANTI"
            | "ON"
            | "AND"
            | "OR"
    )
}

fn is_aggregate_func_name(s: &str) -> bool {
    matches!(
        s.to_ascii_uppercase().as_str(),
        "COUNT"
            | "SUM"
            | "MIN"
            | "MAX"
            | "AVG"
            | "MEAN"
            | "VAR"
            | "VARIANCE"
            | "STDDEV"
            | "STD"
            | "FIRST"
            | "LAST"
            | "APPROX_PERCENTILE"
            | "STRING_AGG"
            | "LIST_AGG"
            | "ARRAY_AGG"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type};
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::DataType;

    fn orders_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![101, 102, 103, 104, 105])),
                Arc::new(Int64Array::from(vec![1, 2, 1, 3, 2])),
                Arc::new(StringArray::from(vec!["us", "eu", "us", "apac", "eu"])),
                Arc::new(Float64Array::from(vec![50.0, 80.0, 120.0, 25.0, 40.0])),
            ],
        )
        .unwrap()
    }

    fn customers_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("tier", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["gold", "silver"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_sql_filter_group_having_order_limit() {
        let orders = orders_batch();
        let mut ds = Dataset::new();
        ds.register_schema("orders", orders.schema()).unwrap();
        ds.set_batch_named("orders", orders).unwrap();

        let sql = SqlSystem::parse(
            "summary",
            "SELECT region, SUM(amount) AS total, COUNT(*) AS n \
             FROM orders \
             WHERE amount >= 30.0 \
             GROUP BY region \
             HAVING SUM(amount) > 100.0 \
             ORDER BY total DESC \
             LIMIT 1",
        )
        .unwrap();

        sql.run_sync(&mut ds).unwrap().unwrap();
        let out = ds.batch_named("summary").unwrap();
        assert_eq!(out.num_rows(), 1);
        assert_eq!(out.column(0).as_string::<i32>().value(0), "us");
        assert!((out.column(1).as_primitive::<Float64Type>().value(0) - 170.0).abs() < 1e-9);
        assert_eq!(out.column(2).as_primitive::<Int64Type>().value(0), 2);
    }

    #[test]
    fn test_sql_join_and_computed_projection() {
        let orders = orders_batch();
        let customers = customers_batch();

        let mut ds = Dataset::new();
        ds.register_schema("orders", orders.schema()).unwrap();
        ds.register_schema("customers", customers.schema()).unwrap();
        ds.set_batch_named("orders", orders).unwrap();
        ds.set_batch_named("customers", customers).unwrap();

        let sql = SqlSystem::parse(
            "enriched",
            "SELECT o.order_id, c.tier, o.amount * 2 AS doubled \
             FROM orders o \
             JOIN customers c ON o.customer_id = c.id \
             WHERE c.tier = 'gold' \
             ORDER BY o.order_id ASC",
        )
        .unwrap();

        sql.run_sync(&mut ds).unwrap().unwrap();
        let out = ds.batch_named("enriched").unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(
            out.column(0).as_primitive::<Int64Type>().values(),
            &[101, 103]
        );
        assert_eq!(out.column(1).as_string::<i32>().value(0), "gold");
        assert_eq!(
            out.column(2).as_primitive::<Float64Type>().values(),
            &[100.0, 240.0]
        );
    }
}
