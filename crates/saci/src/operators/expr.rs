//! Vectorized expression AST, SQL-style expression parser, [`FilterSystem`], and [`ProjectSystem`].

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use arrow_arith::boolean::{and_kleene, is_not_null, is_null, not, or_kleene};
use arrow_arith::numeric::{add, div, mul, neg, rem, sub};
use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Float64Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array,
    new_null_array,
};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_ord::cmp::{distinct, eq, gt, gt_eq, lt, lt_eq, neq, not_distinct};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow_select::filter::filter_record_batch;
use arrow_select::zip::zip;
use async_trait::async_trait;

use crate::dataset::{Dataset, intern_component_name};
use crate::error::{SaciError, SaciResult};
use crate::system::{System, SystemMeta};

/// A typed literal scalar value inside an [`Expr`].
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    /// SQL `NULL` literal.
    Null,
    /// Boolean literal (`TRUE` / `FALSE`).
    Bool(bool),
    /// 64-bit signed integer literal.
    Int64(i64),
    /// 64-bit IEEE-754 floating-point literal.
    Float64(f64),
    /// UTF-8 string literal.
    Utf8(String),
}

impl ScalarValue {
    /// Returns the Arrow [`DataType`] associated with this literal.
    #[must_use]
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Bool(_) => DataType::Boolean,
            Self::Int64(_) => DataType::Int64,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
        }
    }

    /// Materializes this scalar as a length-`len` Arrow array.
    #[must_use]
    pub fn to_array(&self, len: usize) -> ArrayRef {
        match self {
            Self::Null => new_null_array(&DataType::Null, len),
            Self::Bool(v) => Arc::new(BooleanArray::from(vec![Some(*v); len])),
            Self::Int64(v) => Arc::new(Int64Array::from(vec![*v; len])),
            Self::Float64(v) => Arc::new(Float64Array::from(vec![*v; len])),
            Self::Utf8(s) => Arc::new(StringArray::from(vec![Some(s.as_str()); len])),
        }
    }

    /// Returns a 1-row scalar `Datum` array suitable for Arrow scalar kernels.
    #[must_use]
    pub fn to_scalar_array(&self) -> ArrayRef {
        self.to_array(1)
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("NULL"),
            Self::Bool(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::Utf8(s) => write!(f, "'{}'", s.replace('\'', "''")),
        }
    }
}

/// Binary operators supported by [`Expr`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%`
    Mod,
    /// `=`
    Eq,
    /// `!=` / `<>`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
    /// `IS DISTINCT FROM`
    IsDistinctFrom,
    /// `IS NOT DISTINCT FROM`
    IsNotDistinctFrom,
    /// `AND`
    And,
    /// `OR`
    Or,
    /// `||` string concatenation
    Concat,
    /// `LIKE` (case-sensitive SQL pattern match)
    Like,
    /// `ILIKE` (case-insensitive SQL pattern match)
    ILike,
    /// `NOT LIKE`
    NotLike,
    /// `NOT ILIKE`
    NotILike,
}

impl BinaryOp {
    fn is_comparison(self) -> bool {
        matches!(
            self,
            Self::Eq
                | Self::Ne
                | Self::Lt
                | Self::Le
                | Self::Gt
                | Self::Ge
                | Self::IsDistinctFrom
                | Self::IsNotDistinctFrom
                | Self::Like
                | Self::ILike
                | Self::NotLike
                | Self::NotILike
        )
    }
}

/// Unary operators supported by [`Expr`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    /// Logical negation (`NOT`).
    Not,
    /// Arithmetic negation (`-x`).
    Neg,
    /// `IS NULL`.
    IsNull,
    /// `IS NOT NULL`.
    IsNotNull,
}

/// Built-in scalar functions supported by [`Expr`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunc {
    /// `LOWER(s)`
    Lower,
    /// `UPPER(s)`
    Upper,
    /// `TRIM(s)`
    Trim,
    /// `LENGTH(s)` / `CHAR_LENGTH(s)`
    Length,
    /// `SUBSTRING(s, start[, len])` (1-based SQL indexing)
    Substring,
    /// `CONCAT(a, b, ...)`
    Concat,
    /// `REPLACE(s, from, to)`
    Replace,
    /// `CONTAINS(s, needle)`
    Contains,
    /// `STARTS_WITH(s, prefix)`
    StartsWith,
    /// `ENDS_WITH(s, suffix)`
    EndsWith,
    /// `ABS(x)`
    Abs,
    /// `CEIL(x)`
    Ceil,
    /// `FLOOR(x)`
    Floor,
    /// `ROUND(x[, decimals])`
    Round,
    /// `SQRT(x)`
    Sqrt,
    /// `POWER(x, y)` / `POW(x, y)`
    Power,
    /// `COALESCE(a, b, ...)`
    Coalesce,
    /// `IFNULL(a, b)` / `NVL(a, b)`
    IfNull,
    /// `NULLIF(a, b)`
    NullIf,
    /// `EPOCH_MS(ts)` — extracts integer milliseconds since Unix epoch.
    EpochMs,
    /// `DATE_TRUNC(unit, ts)` — truncates timestamp/epoch-ms to `'second'|'minute'|'hour'|'day'`.
    DateTrunc,
}

impl ScalarFunc {
    /// Parses a case-insensitive function name.
    pub(crate) fn from_name(name: &str) -> SaciResult<Self> {
        match name.to_ascii_uppercase().as_str() {
            "LOWER" => Ok(Self::Lower),
            "UPPER" => Ok(Self::Upper),
            "TRIM" => Ok(Self::Trim),
            "LENGTH" | "CHAR_LENGTH" => Ok(Self::Length),
            "SUBSTRING" | "SUBSTR" => Ok(Self::Substring),
            "CONCAT" => Ok(Self::Concat),
            "REPLACE" => Ok(Self::Replace),
            "CONTAINS" => Ok(Self::Contains),
            "STARTS_WITH" => Ok(Self::StartsWith),
            "ENDS_WITH" => Ok(Self::EndsWith),
            "ABS" => Ok(Self::Abs),
            "CEIL" | "CEILING" => Ok(Self::Ceil),
            "FLOOR" => Ok(Self::Floor),
            "ROUND" => Ok(Self::Round),
            "SQRT" => Ok(Self::Sqrt),
            "POWER" | "POW" => Ok(Self::Power),
            "COALESCE" => Ok(Self::Coalesce),
            "IFNULL" | "NVL" => Ok(Self::IfNull),
            "NULLIF" => Ok(Self::NullIf),
            "EPOCH_MS" => Ok(Self::EpochMs),
            "DATE_TRUNC" => Ok(Self::DateTrunc),
            _ => Err(SaciError::configuration(format!(
                "unknown scalar function '{name}'"
            ))),
        }
    }
}

/// Vectorized expression AST evaluated against an Apache Arrow [`RecordBatch`].
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference (bare or `qualifier.column`).
    Column(String),
    /// Constant literal value.
    Literal(ScalarValue),
    /// Binary operation.
    Binary {
        /// Left operand.
        left: Box<Expr>,
        /// Binary operator.
        op: BinaryOp,
        /// Right operand.
        right: Box<Expr>,
    },
    /// Unary operation.
    Unary {
        /// Unary operator.
        op: UnaryOp,
        /// Inner expression.
        expr: Box<Expr>,
    },
    /// Explicit type cast (`CAST(expr AS type)` or `expr::type`).
    Cast {
        /// Inner expression.
        expr: Box<Expr>,
        /// Target Arrow [`DataType`].
        data_type: DataType,
    },
    /// SQL `CASE [operand] WHEN ... THEN ... [ELSE ...] END`.
    Case {
        /// Optional switch operand.
        operand: Option<Box<Expr>>,
        /// Ordered `(when, then)` branches.
        when_then: Vec<(Expr, Expr)>,
        /// Optional fallback expression.
        else_expr: Option<Box<Expr>>,
    },
    /// `expr [NOT] IN (list...)`.
    InList {
        /// Probe expression.
        expr: Box<Expr>,
        /// Candidate expressions.
        list: Vec<Expr>,
        /// True for `NOT IN`.
        negated: bool,
    },
    /// `expr [NOT] BETWEEN low AND high`.
    Between {
        /// Probe expression.
        expr: Box<Expr>,
        /// Lower bound (inclusive).
        low: Box<Expr>,
        /// Upper bound (inclusive).
        high: Box<Expr>,
        /// True for `NOT BETWEEN`.
        negated: bool,
    },
    /// Scalar function invocation.
    Call {
        /// Function kind.
        func: ScalarFunc,
        /// Positional arguments.
        args: Vec<Expr>,
    },
}

impl Expr {
    /// Column reference helper.
    #[must_use]
    pub fn col(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }

    /// Integer literal helper.
    #[must_use]
    pub fn lit_i64(v: i64) -> Self {
        Self::Literal(ScalarValue::Int64(v))
    }

    /// Float literal helper.
    #[must_use]
    pub fn lit_f64(v: f64) -> Self {
        Self::Literal(ScalarValue::Float64(v))
    }

    /// String literal helper.
    #[must_use]
    pub fn lit_str(v: impl Into<String>) -> Self {
        Self::Literal(ScalarValue::Utf8(v.into()))
    }

    /// Boolean literal helper.
    #[must_use]
    pub fn lit_bool(v: bool) -> Self {
        Self::Literal(ScalarValue::Bool(v))
    }

    /// Null literal helper.
    #[must_use]
    pub fn lit_null() -> Self {
        Self::Literal(ScalarValue::Null)
    }

    /// Binary operation builder.
    #[must_use]
    pub fn binary(left: Self, op: BinaryOp, right: Self) -> Self {
        Self::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    /// Parses a SQL-style scalar expression string into an [`Expr`].
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] on syntax errors or trailing tokens.
    pub fn parse(input: &str) -> SaciResult<Self> {
        let tokens = tokenize(input)?;
        if tokens.is_empty() {
            return Err(SaciError::configuration("empty expression"));
        }
        let mut parser = ExprParser::new(&tokens);
        let expr = parser.parse_expr()?;
        if !parser.is_eof() {
            return Err(SaciError::configuration(format!(
                "unexpected trailing token '{}' in expression '{input}'",
                parser.peek_display()
            )));
        }
        Ok(expr)
    }

    /// Returns a sensible default column alias when none is explicitly provided.
    #[must_use]
    pub fn default_alias(&self) -> String {
        match self {
            Self::Column(name) => name.rsplit('.').next().unwrap_or(name).to_string(),
            Self::Literal(v) => v.to_string(),
            Self::Cast { expr, .. } => expr.default_alias(),
            Self::Call { func, args } => {
                let fn_name = format!("{func:?}").to_ascii_lowercase();
                if let Some(first) = args.first() {
                    format!("{fn_name}_{}", first.default_alias())
                } else {
                    fn_name
                }
            }
            _ => "expr".to_string(),
        }
    }

    /// Collects every referenced column name in traversal order (deduplicated).
    pub fn referenced_columns(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        self.collect_columns(&mut out, &mut seen);
        out
    }

    fn collect_columns(&self, out: &mut Vec<String>, seen: &mut HashSet<String>) {
        match self {
            Self::Column(name) => {
                if seen.insert(name.clone()) {
                    out.push(name.clone());
                }
            }
            Self::Literal(_) => {}
            Self::Binary { left, right, .. } => {
                left.collect_columns(out, seen);
                right.collect_columns(out, seen);
            }
            Self::Unary { expr, .. } | Self::Cast { expr, .. } => {
                expr.collect_columns(out, seen);
            }
            Self::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    op.collect_columns(out, seen);
                }
                for (w, t) in when_then {
                    w.collect_columns(out, seen);
                    t.collect_columns(out, seen);
                }
                if let Some(e) = else_expr {
                    e.collect_columns(out, seen);
                }
            }
            Self::InList { expr, list, .. } => {
                expr.collect_columns(out, seen);
                for item in list {
                    item.collect_columns(out, seen);
                }
            }
            Self::Between {
                expr, low, high, ..
            } => {
                expr.collect_columns(out, seen);
                low.collect_columns(out, seen);
                high.collect_columns(out, seen);
            }
            Self::Call { args, .. } => {
                for a in args {
                    a.collect_columns(out, seen);
                }
            }
        }
    }

    /// Infers the output Arrow [`DataType`] of this expression against `schema`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::FieldNotFound`] for unknown columns or
    /// [`SaciError::Configuration`] on incompatible operands.
    pub fn data_type(&self, schema: &Schema) -> SaciResult<DataType> {
        match self {
            Self::Column(name) => {
                let idx = resolve_column_index(schema, name)?;
                Ok(schema.field(idx).data_type().clone())
            }
            Self::Literal(v) => Ok(v.data_type()),
            Self::Cast { data_type, .. } => Ok(data_type.clone()),
            Self::Unary { op, expr } => match op {
                UnaryOp::Not | UnaryOp::IsNull | UnaryOp::IsNotNull => Ok(DataType::Boolean),
                UnaryOp::Neg => expr.data_type(schema),
            },
            Self::Binary { left, op, right } => {
                if op.is_comparison() || matches!(op, BinaryOp::And | BinaryOp::Or) {
                    Ok(DataType::Boolean)
                } else if *op == BinaryOp::Concat {
                    Ok(DataType::Utf8)
                } else if *op == BinaryOp::Div {
                    let lt = left.data_type(schema)?;
                    let rt = right.data_type(schema)?;
                    if matches!(lt, DataType::Float64 | DataType::Float32)
                        || matches!(rt, DataType::Float64 | DataType::Float32)
                    {
                        Ok(DataType::Float64)
                    } else {
                        coerce_arithmetic_type(&lt, &rt)
                    }
                } else {
                    let lt = left.data_type(schema)?;
                    let rt = right.data_type(schema)?;
                    coerce_arithmetic_type(&lt, &rt)
                }
            }
            Self::InList { .. } | Self::Between { .. } => Ok(DataType::Boolean),
            Self::Case {
                when_then,
                else_expr,
                ..
            } => {
                let mut candidates = Vec::with_capacity(when_then.len() + 1);
                for (_, then_expr) in when_then {
                    let dt = then_expr.data_type(schema)?;
                    if dt != DataType::Null {
                        candidates.push(dt);
                    }
                }
                if let Some(el) = else_expr {
                    let dt = el.data_type(schema)?;
                    if dt != DataType::Null {
                        candidates.push(dt);
                    }
                }
                infer_common_type(&candidates)
            }
            Self::Call { func, args } => match func {
                ScalarFunc::Lower
                | ScalarFunc::Upper
                | ScalarFunc::Trim
                | ScalarFunc::Substring
                | ScalarFunc::Concat
                | ScalarFunc::Replace => Ok(DataType::Utf8),
                ScalarFunc::Length => Ok(DataType::Int64),
                ScalarFunc::Contains | ScalarFunc::StartsWith | ScalarFunc::EndsWith => {
                    Ok(DataType::Boolean)
                }
                ScalarFunc::Abs => {
                    let dt = args.first().map(|a| a.data_type(schema)).transpose()?;
                    Ok(dt.unwrap_or(DataType::Float64))
                }
                ScalarFunc::Ceil
                | ScalarFunc::Floor
                | ScalarFunc::Round
                | ScalarFunc::Sqrt
                | ScalarFunc::Power => Ok(DataType::Float64),
                ScalarFunc::EpochMs => Ok(DataType::Int64),
                ScalarFunc::DateTrunc => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
                ScalarFunc::NullIf => args
                    .first()
                    .map(|a| a.data_type(schema))
                    .transpose()
                    .map(|opt| opt.unwrap_or(DataType::Null)),
                ScalarFunc::Coalesce | ScalarFunc::IfNull => {
                    let mut candidates = Vec::with_capacity(args.len());
                    for a in args {
                        let dt = a.data_type(schema)?;
                        if dt != DataType::Null {
                            candidates.push(dt);
                        }
                    }
                    infer_common_type(&candidates)
                }
            },
        }
    }

    /// Evaluates this expression over `batch`, returning an [`ArrayRef`] of length
    /// `batch.num_rows()`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError`] if any referenced column is missing or a type cast /
    /// kernel execution fails.
    pub fn evaluate(&self, batch: &RecordBatch) -> SaciResult<ArrayRef> {
        let len = batch.num_rows();
        match self {
            Self::Column(name) => {
                let idx = resolve_column_index(batch.schema_ref(), name)?;
                Ok(batch.column(idx).clone())
            }
            Self::Literal(val) => Ok(val.to_array(len)),
            Self::Cast { expr, data_type } => {
                let arr = expr.evaluate(batch)?;
                if arr.data_type() == data_type {
                    return Ok(arr);
                }
                cast_array(&arr, data_type)
            }
            Self::Unary { op, expr } => eval_unary(*op, expr, batch),
            Self::Binary { left, op, right } => eval_binary(*op, left, right, batch),
            Self::Case {
                operand,
                when_then,
                else_expr,
            } => eval_case(operand.as_deref(), when_then, else_expr.as_deref(), batch),
            Self::InList {
                expr,
                list,
                negated,
            } => eval_in_list(expr, list, *negated, batch),
            Self::Between {
                expr,
                low,
                high,
                negated,
            } => eval_between(expr, low, high, *negated, batch),
            Self::Call { func, args } => eval_call(*func, args, batch),
        }
    }

    /// Evaluates this expression as a boolean predicate suitable for
    /// `arrow_select::filter::filter_record_batch`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError`] if evaluation fails or the expression does not
    /// yield a boolean array.
    pub fn evaluate_predicate(&self, batch: &RecordBatch) -> SaciResult<BooleanArray> {
        let arr = self.evaluate(batch)?;
        if let Some(b) = arr.as_any().downcast_ref::<BooleanArray>() {
            return Ok(b.clone());
        }
        if matches!(arr.data_type(), DataType::Null) {
            return Ok(BooleanArray::from(vec![None; arr.len()]));
        }
        let cast = cast_array(&arr, &DataType::Boolean)?;
        cast.as_any()
            .downcast_ref::<BooleanArray>()
            .cloned()
            .ok_or_else(|| SaciError::generic("predicate did not evaluate to BooleanArray"))
    }
}

pub(crate) fn resolve_column_index(schema: &Schema, name: &str) -> SaciResult<usize> {
    if let Ok(idx) = schema.index_of(name) {
        return Ok(idx);
    }
    if let Some((_prefix, bare)) = name.split_once('.') {
        if let Ok(idx) = schema.index_of(bare) {
            return Ok(idx);
        }
    } else {
        let suffix = format!(".{name}");
        let matches: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name().ends_with(&suffix))
            .map(|(i, _)| i)
            .collect();
        if matches.len() == 1 {
            return Ok(matches[0]);
        }
        if matches.len() > 1 {
            return Err(SaciError::configuration(format!(
                "ambiguous column reference '{name}'"
            )));
        }
    }
    Err(SaciError::field_not_found("Expr", name))
}

fn cast_array(arr: &ArrayRef, target: &DataType) -> SaciResult<ArrayRef> {
    if arr.data_type() == target {
        return Ok(arr.clone());
    }
    if matches!(arr.data_type(), DataType::Null) {
        return Ok(new_null_array(target, arr.len()));
    }
    let opts = CastOptions {
        safe: false,
        format_options: FormatOptions::default(),
    };
    cast_with_options(arr, target, &opts)
        .map_err(|e| SaciError::generic(format!("cast {:?} to {:?}: {e}", arr.data_type(), target)))
}

pub(crate) fn coerce_arithmetic_type(left: &DataType, right: &DataType) -> SaciResult<DataType> {
    if *left == DataType::Null {
        return Ok(right.clone());
    }
    if *right == DataType::Null {
        return Ok(left.clone());
    }
    if left == right {
        return Ok(left.clone());
    }
    match (left, right) {
        (DataType::Float64, _) | (_, DataType::Float64) => Ok(DataType::Float64),
        (DataType::Float32, _) | (_, DataType::Float32) => {
            if matches!(left, DataType::Int64 | DataType::UInt64)
                || matches!(right, DataType::Int64 | DataType::UInt64)
            {
                Ok(DataType::Float64)
            } else {
                Ok(DataType::Float32)
            }
        }
        (a, b) if a.is_integer() && b.is_integer() => {
            if a.is_signed_integer() || b.is_signed_integer() {
                Ok(DataType::Int64)
            } else {
                Ok(DataType::UInt64)
            }
        }
        _ => Err(SaciError::configuration(format!(
            "cannot coerce arithmetic types {left:?} and {right:?}"
        ))),
    }
}

fn infer_common_type(types: &[DataType]) -> SaciResult<DataType> {
    let mut acc: Option<DataType> = None;
    for dt in types {
        if *dt == DataType::Null {
            continue;
        }
        acc = Some(match acc {
            None => dt.clone(),
            Some(prev) if prev == *dt => prev,
            Some(prev) if prev.is_numeric() && dt.is_numeric() => {
                coerce_arithmetic_type(&prev, dt)?
            }
            Some(DataType::LargeUtf8) | Some(DataType::Utf8)
                if matches!(dt, DataType::Utf8 | DataType::LargeUtf8) =>
            {
                DataType::Utf8
            }
            Some(_) => DataType::Utf8,
        });
    }
    Ok(acc.unwrap_or(DataType::Null))
}

fn coerce_comparison_pair(left: ArrayRef, right: ArrayRef) -> SaciResult<(ArrayRef, ArrayRef)> {
    let lt = left.data_type().clone();
    let rt = right.data_type().clone();
    if lt == rt {
        return Ok((left, right));
    }
    if matches!(lt, DataType::Null) {
        return Ok((new_null_array(&rt, left.len()), right));
    }
    if matches!(rt, DataType::Null) {
        return Ok((left, new_null_array(&lt, right.len())));
    }
    if lt.is_numeric() && rt.is_numeric() {
        let target = coerce_arithmetic_type(&lt, &rt)?;
        return Ok((cast_array(&left, &target)?, cast_array(&right, &target)?));
    }
    if matches!(lt, DataType::Timestamp(_, _) | DataType::Date32) && rt.is_integer() {
        return Ok((left.clone(), cast_array(&right, &lt)?));
    }
    if matches!(rt, DataType::Timestamp(_, _) | DataType::Date32) && lt.is_integer() {
        return Ok((cast_array(&left, &rt)?, right.clone()));
    }
    let target = infer_common_type(&[lt, rt])?;
    Ok((cast_array(&left, &target)?, cast_array(&right, &target)?))
}

fn eval_unary(op: UnaryOp, inner: &Expr, batch: &RecordBatch) -> SaciResult<ArrayRef> {
    let arr = inner.evaluate(batch)?;
    match op {
        UnaryOp::Not => {
            let b = if let Some(b) = arr.as_any().downcast_ref::<BooleanArray>() {
                b.clone()
            } else if matches!(arr.data_type(), DataType::Null) {
                BooleanArray::from(vec![None; arr.len()])
            } else {
                let c = cast_array(&arr, &DataType::Boolean)?;
                c.as_boolean().clone()
            };
            let out = not(&b).map_err(|e| SaciError::generic(format!("NOT kernel: {e}")))?;
            Ok(Arc::new(out))
        }
        UnaryOp::Neg => {
            if matches!(arr.data_type(), DataType::Null) {
                return Ok(arr);
            }
            neg(&arr).map_err(|e| SaciError::generic(format!("NEG kernel: {e}")))
        }
        UnaryOp::IsNull => {
            let out = is_null(&arr).map_err(|e| SaciError::generic(format!("IS NULL: {e}")))?;
            Ok(Arc::new(out))
        }
        UnaryOp::IsNotNull => {
            let out =
                is_not_null(&arr).map_err(|e| SaciError::generic(format!("IS NOT NULL: {e}")))?;
            Ok(Arc::new(out))
        }
    }
}

fn eval_binary(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    batch: &RecordBatch,
) -> SaciResult<ArrayRef> {
    let l_arr = left.evaluate(batch)?;
    let r_arr = right.evaluate(batch)?;
    let len = batch.num_rows();

    match op {
        BinaryOp::And | BinaryOp::Or => {
            let lb = as_bool_or_cast(&l_arr)?;
            let rb = as_bool_or_cast(&r_arr)?;
            let res = if op == BinaryOp::And {
                and_kleene(&lb, &rb)
            } else {
                or_kleene(&lb, &rb)
            }
            .map_err(|e| SaciError::generic(format!("boolean kernel: {e}")))?;
            Ok(Arc::new(res))
        }
        BinaryOp::Concat => eval_concat(&[l_arr, r_arr], len),
        BinaryOp::Like | BinaryOp::ILike | BinaryOp::NotLike | BinaryOp::NotILike => {
            let case_insensitive = matches!(op, BinaryOp::ILike | BinaryOp::NotILike);
            let negated = matches!(op, BinaryOp::NotLike | BinaryOp::NotILike);
            eval_like(&l_arr, &r_arr, case_insensitive, negated)
        }
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
            if matches!(l_arr.data_type(), DataType::Null)
                || matches!(r_arr.data_type(), DataType::Null)
            {
                let target = coerce_arithmetic_type(l_arr.data_type(), r_arr.data_type())?;
                return Ok(new_null_array(&target, len));
            }
            let target = coerce_arithmetic_type(l_arr.data_type(), r_arr.data_type())?;
            let lc = cast_array(&l_arr, &target)?;
            let rc = cast_array(&r_arr, &target)?;
            match op {
                BinaryOp::Add => add(&lc, &rc),
                BinaryOp::Sub => sub(&lc, &rc),
                BinaryOp::Mul => mul(&lc, &rc),
                BinaryOp::Div => div(&lc, &rc),
                BinaryOp::Mod => rem(&lc, &rc),
                _ => unreachable!(),
            }
            .map_err(|e| SaciError::generic(format!("arithmetic kernel {op:?}: {e}")))
        }
        BinaryOp::Eq
        | BinaryOp::Ne
        | BinaryOp::Lt
        | BinaryOp::Le
        | BinaryOp::Gt
        | BinaryOp::Ge
        | BinaryOp::IsDistinctFrom
        | BinaryOp::IsNotDistinctFrom => {
            let (lc, rc) = coerce_comparison_pair(l_arr, r_arr)?;
            let res = match op {
                BinaryOp::Eq => eq(&lc, &rc),
                BinaryOp::Ne => neq(&lc, &rc),
                BinaryOp::Lt => lt(&lc, &rc),
                BinaryOp::Le => lt_eq(&lc, &rc),
                BinaryOp::Gt => gt(&lc, &rc),
                BinaryOp::Ge => gt_eq(&lc, &rc),
                BinaryOp::IsDistinctFrom => distinct(&lc, &rc),
                BinaryOp::IsNotDistinctFrom => not_distinct(&lc, &rc),
                _ => unreachable!(),
            }
            .map_err(|e| SaciError::generic(format!("comparison kernel {op:?}: {e}")))?;
            Ok(Arc::new(res))
        }
    }
}

fn as_bool_or_cast(arr: &ArrayRef) -> SaciResult<BooleanArray> {
    if let Some(b) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Ok(b.clone());
    }
    if matches!(arr.data_type(), DataType::Null) {
        return Ok(BooleanArray::from(vec![None; arr.len()]));
    }
    let c = cast_array(arr, &DataType::Boolean)?;
    Ok(c.as_boolean().clone())
}

fn eval_case(
    operand: Option<&Expr>,
    when_then: &[(Expr, Expr)],
    else_expr: Option<&Expr>,
    batch: &RecordBatch,
) -> SaciResult<ArrayRef> {
    let len = batch.num_rows();
    let op_arr = operand.map(|o| o.evaluate(batch)).transpose()?;

    if len == 0 {
        let mut types = Vec::with_capacity(when_then.len() + 1);
        for (_, then_expr) in when_then {
            types.push(then_expr.evaluate(batch)?.data_type().clone());
        }
        if let Some(el) = else_expr {
            types.push(el.evaluate(batch)?.data_type().clone());
        }
        let target_dt = infer_common_type(&types)?;
        return Ok(new_null_array(&target_dt, 0));
    }

    let empty_batch = batch.slice(0, 0);
    let mut remaining = vec![true; len];
    let mut branch_results: Vec<(Vec<usize>, ArrayRef)> = Vec::with_capacity(when_then.len() + 1);
    let mut result_types = Vec::with_capacity(when_then.len() + 1);

    let eval_masked =
        |target_expr: &Expr, indices: &[usize], mask: &BooleanArray| -> SaciResult<ArrayRef> {
            if indices.is_empty() {
                target_expr.evaluate(&empty_batch)
            } else if indices.len() == len {
                target_expr.evaluate(batch)
            } else {
                let sub_batch = filter_record_batch(batch, mask)
                    .map_err(|e| SaciError::generic(format!("CASE filter batch: {e}")))?;
                target_expr.evaluate(&sub_batch)
            }
        };

    for (when_expr, then_expr) in when_then {
        let cond = if let Some(ref lhs) = op_arr {
            let rhs = when_expr.evaluate(batch)?;
            let (lc, rc) = coerce_comparison_pair(lhs.clone(), rhs)?;
            eq(&lc, &rc).map_err(|e| SaciError::generic(format!("CASE equality: {e}")))?
        } else {
            when_expr.evaluate_predicate(batch)?
        };
        let mut active_indices = Vec::new();
        let active_mask: BooleanArray = (0..len)
            .map(|i| {
                let take_row = remaining[i] && cond.is_valid(i) && cond.value(i);
                if take_row {
                    remaining[i] = false;
                    active_indices.push(i);
                }
                take_row
            })
            .collect();

        let arr = eval_masked(then_expr, &active_indices, &active_mask)?;
        result_types.push(arr.data_type().clone());
        branch_results.push((active_indices, arr));
    }

    if let Some(el) = else_expr {
        let mut else_indices = Vec::new();
        let else_mask: BooleanArray = (0..len)
            .map(|i| {
                if remaining[i] {
                    else_indices.push(i);
                    true
                } else {
                    false
                }
            })
            .collect();
        let arr = eval_masked(el, &else_indices, &else_mask)?;
        result_types.push(arr.data_type().clone());
        branch_results.push((else_indices, arr));
    }

    let target_dt = infer_common_type(&result_types)?;
    if let Some((_, sole_arr)) = branch_results
        .iter()
        .find(|(indices, _)| indices.len() == len)
    {
        return cast_array(sole_arr, &target_dt);
    }

    let mut chunks: Vec<ArrayRef> = Vec::with_capacity(branch_results.len() + 1);
    chunks.push(new_null_array(&target_dt, 1));
    let mut scatter = vec![0u32; len];
    let mut offset = 1u32;

    for (indices, arr) in branch_results {
        if indices.is_empty() {
            continue;
        }
        let casted = cast_array(&arr, &target_dt)?;
        for (local_idx, row_idx) in indices.into_iter().enumerate() {
            scatter[row_idx] = offset + local_idx as u32;
        }
        offset += casted.len() as u32;
        chunks.push(casted);
    }

    if chunks.len() == 1 {
        return Ok(new_null_array(&target_dt, len));
    }

    let refs: Vec<&dyn Array> = chunks.iter().map(ArrayRef::as_ref).collect();
    let pool = arrow_select::concat::concat(&refs)
        .map_err(|e| SaciError::generic(format!("CASE concat: {e}")))?;
    let take_idx = UInt32Array::from(scatter);
    arrow_select::take::take(pool.as_ref(), &take_idx, None)
        .map_err(|e| SaciError::generic(format!("CASE scatter take: {e}")))
}

fn eval_in_list(
    expr: &Expr,
    list: &[Expr],
    negated: bool,
    batch: &RecordBatch,
) -> SaciResult<ArrayRef> {
    let len = batch.num_rows();
    if list.is_empty() {
        return Ok(Arc::new(BooleanArray::from(vec![Some(negated); len])));
    }
    let lhs = expr.evaluate(batch)?;
    let mut acc = BooleanArray::from(vec![Some(false); len]);
    for item in list {
        let rhs = item.evaluate(batch)?;
        let (lc, rc) = coerce_comparison_pair(lhs.clone(), rhs)?;
        let eq_arr = eq(&lc, &rc).map_err(|e| SaciError::generic(format!("IN compare: {e}")))?;
        acc = or_kleene(&acc, &eq_arr).map_err(|e| SaciError::generic(format!("IN or: {e}")))?;
    }
    if negated {
        acc = not(&acc).map_err(|e| SaciError::generic(format!("NOT IN: {e}")))?;
    }
    Ok(Arc::new(acc))
}

fn eval_between(
    expr: &Expr,
    low: &Expr,
    high: &Expr,
    negated: bool,
    batch: &RecordBatch,
) -> SaciResult<ArrayRef> {
    let val = expr.evaluate(batch)?;
    let low_arr = low.evaluate(batch)?;
    let high_arr = high.evaluate(batch)?;
    let (v1, l1) = coerce_comparison_pair(val.clone(), low_arr)?;
    let (v2, h2) = coerce_comparison_pair(val, high_arr)?;
    let ge_low = gt_eq(&v1, &l1).map_err(|e| SaciError::generic(format!("BETWEEN low: {e}")))?;
    let le_high = lt_eq(&v2, &h2).map_err(|e| SaciError::generic(format!("BETWEEN high: {e}")))?;
    let mut inside = and_kleene(&ge_low, &le_high)
        .map_err(|e| SaciError::generic(format!("BETWEEN and: {e}")))?;
    if negated {
        inside = not(&inside).map_err(|e| SaciError::generic(format!("NOT BETWEEN: {e}")))?;
    }
    Ok(Arc::new(inside))
}

fn eval_call(func: ScalarFunc, args: &[Expr], batch: &RecordBatch) -> SaciResult<ArrayRef> {
    let len = batch.num_rows();
    match func {
        ScalarFunc::Lower | ScalarFunc::Upper | ScalarFunc::Trim => {
            expect_arity(func, args, 1)?;
            let arr = args[0].evaluate(batch)?;
            let utf8 = to_utf8_array(&arr)?;
            let sa = utf8.as_string::<i32>();
            let out: StringArray = sa
                .iter()
                .map(|opt| {
                    opt.map(|s| match func {
                        ScalarFunc::Lower => s.to_lowercase(),
                        ScalarFunc::Upper => s.to_uppercase(),
                        ScalarFunc::Trim => s.trim().to_string(),
                        _ => unreachable!(),
                    })
                })
                .collect();
            Ok(Arc::new(out))
        }
        ScalarFunc::Length => {
            expect_arity(func, args, 1)?;
            let arr = args[0].evaluate(batch)?;
            let utf8 = to_utf8_array(&arr)?;
            let sa = utf8.as_string::<i32>();
            let out: Int64Array = sa
                .iter()
                .map(|opt| opt.map(|s| s.chars().count() as i64))
                .collect();
            Ok(Arc::new(out))
        }
        ScalarFunc::Substring => {
            if !(2..=3).contains(&args.len()) {
                return Err(SaciError::configuration(
                    "SUBSTRING expects 2 or 3 arguments",
                ));
            }
            let str_arr = to_utf8_array(&args[0].evaluate(batch)?)?;
            let start_arr = cast_array(&args[1].evaluate(batch)?, &DataType::Int64)?;
            let len_arr = if args.len() == 3 {
                Some(cast_array(&args[2].evaluate(batch)?, &DataType::Int64)?)
            } else {
                None
            };
            let sa = str_arr.as_string::<i32>();
            let starts = start_arr.as_primitive::<Int64Type>();
            let lens = len_arr.as_ref().map(|a| a.as_primitive::<Int64Type>());
            let mut builder: Vec<Option<String>> = Vec::with_capacity(len);
            for i in 0..len {
                if sa.is_null(i) || starts.is_null(i) || lens.as_ref().is_some_and(|l| l.is_null(i))
                {
                    builder.push(None);
                    continue;
                }
                let s = sa.value(i);
                let start_1 = starts.value(i);
                let skip = if start_1 <= 1 {
                    0
                } else {
                    (start_1 - 1) as usize
                };
                let slice = match lens {
                    None => s.chars().skip(skip).collect::<String>(),
                    Some(l) => {
                        let take_n = l.value(i).max(0) as usize;
                        s.chars().skip(skip).take(take_n).collect::<String>()
                    }
                };
                builder.push(Some(slice));
            }
            Ok(Arc::new(StringArray::from(builder)))
        }
        ScalarFunc::Concat => {
            let mut evaluated = Vec::with_capacity(args.len());
            for arg in args {
                evaluated.push(arg.evaluate(batch)?);
            }
            eval_concat(&evaluated, len)
        }
        ScalarFunc::Replace => {
            expect_arity(func, args, 3)?;
            let src = to_utf8_array(&args[0].evaluate(batch)?)?;
            let from = to_utf8_array(&args[1].evaluate(batch)?)?;
            let to = to_utf8_array(&args[2].evaluate(batch)?)?;
            let s_arr = src.as_string::<i32>();
            let f_arr = from.as_string::<i32>();
            let t_arr = to.as_string::<i32>();
            let out: StringArray = (0..len)
                .map(|i| {
                    if s_arr.is_null(i) || f_arr.is_null(i) || t_arr.is_null(i) {
                        None
                    } else {
                        Some(s_arr.value(i).replace(f_arr.value(i), t_arr.value(i)))
                    }
                })
                .collect();
            Ok(Arc::new(out))
        }
        ScalarFunc::Contains | ScalarFunc::StartsWith | ScalarFunc::EndsWith => {
            expect_arity(func, args, 2)?;
            let src = to_utf8_array(&args[0].evaluate(batch)?)?;
            let pat = to_utf8_array(&args[1].evaluate(batch)?)?;
            let s_arr = src.as_string::<i32>();
            let p_arr = pat.as_string::<i32>();
            let out: BooleanArray = (0..len)
                .map(|i| {
                    if s_arr.is_null(i) || p_arr.is_null(i) {
                        None
                    } else {
                        let s = s_arr.value(i);
                        let p = p_arr.value(i);
                        Some(match func {
                            ScalarFunc::Contains => s.contains(p),
                            ScalarFunc::StartsWith => s.starts_with(p),
                            ScalarFunc::EndsWith => s.ends_with(p),
                            _ => unreachable!(),
                        })
                    }
                })
                .collect();
            Ok(Arc::new(out))
        }
        ScalarFunc::Abs => {
            expect_arity(func, args, 1)?;
            let arr = args[0].evaluate(batch)?;
            if matches!(arr.data_type(), DataType::Null) {
                return Ok(arr);
            }
            if arr.data_type().is_integer() && arr.data_type().is_signed_integer() {
                let i_arr = cast_array(&arr, &DataType::Int64)?;
                let vals = i_arr.as_primitive::<Int64Type>();
                let out: Int64Array = vals.iter().map(|v| v.map(i64::wrapping_abs)).collect();
                cast_array(&(Arc::new(out) as ArrayRef), arr.data_type())
            } else {
                let f_arr = cast_array(&arr, &DataType::Float64)?;
                let vals = f_arr.as_primitive::<Float64Type>();
                let out: Float64Array = vals.iter().map(|v| v.map(f64::abs)).collect();
                cast_array(&(Arc::new(out) as ArrayRef), arr.data_type())
            }
        }
        ScalarFunc::Ceil | ScalarFunc::Floor | ScalarFunc::Sqrt => {
            expect_arity(func, args, 1)?;
            let arr = cast_array(&args[0].evaluate(batch)?, &DataType::Float64)?;
            let vals = arr.as_primitive::<Float64Type>();
            let out: Float64Array = vals
                .iter()
                .map(|v| {
                    v.map(|x| match func {
                        ScalarFunc::Ceil => x.ceil(),
                        ScalarFunc::Floor => x.floor(),
                        ScalarFunc::Sqrt => x.sqrt(),
                        _ => unreachable!(),
                    })
                })
                .collect();
            Ok(Arc::new(out))
        }
        ScalarFunc::Round => {
            if !(1..=2).contains(&args.len()) {
                return Err(SaciError::configuration("ROUND expects 1 or 2 arguments"));
            }
            let arr = cast_array(&args[0].evaluate(batch)?, &DataType::Float64)?;
            let scale_arr = if args.len() == 2 {
                Some(cast_array(&args[1].evaluate(batch)?, &DataType::Int64)?)
            } else {
                None
            };
            let vals = arr.as_primitive::<Float64Type>();
            let scales = scale_arr.as_ref().map(|a| a.as_primitive::<Int64Type>());
            let out: Float64Array = (0..len)
                .map(|i| {
                    if vals.is_null(i) || scales.as_ref().is_some_and(|s| s.is_null(i)) {
                        None
                    } else {
                        let x = vals.value(i);
                        let places = scales.map_or(0, |s| s.value(i));
                        if places == 0 {
                            Some(x.round())
                        } else {
                            let factor = 10f64.powi(places.clamp(-15, 15) as i32);
                            Some((x * factor).round() / factor)
                        }
                    }
                })
                .collect();
            Ok(Arc::new(out))
        }
        ScalarFunc::Power => {
            expect_arity(func, args, 2)?;
            let base = cast_array(&args[0].evaluate(batch)?, &DataType::Float64)?;
            let exp = cast_array(&args[1].evaluate(batch)?, &DataType::Float64)?;
            let b = base.as_primitive::<Float64Type>();
            let e = exp.as_primitive::<Float64Type>();
            let out: Float64Array = (0..len)
                .map(|i| {
                    if b.is_null(i) || e.is_null(i) {
                        None
                    } else {
                        Some(b.value(i).powf(e.value(i)))
                    }
                })
                .collect();
            Ok(Arc::new(out))
        }
        ScalarFunc::Coalesce | ScalarFunc::IfNull => {
            if args.is_empty() {
                return Err(SaciError::configuration(
                    "COALESCE/IFNULL expects at least 1 argument",
                ));
            }
            if func == ScalarFunc::IfNull && args.len() != 2 {
                return Err(SaciError::configuration("IFNULL expects 2 arguments"));
            }
            let mut evals = Vec::with_capacity(args.len());
            let mut dts = Vec::with_capacity(args.len());
            for a in args {
                let arr = a.evaluate(batch)?;
                dts.push(arr.data_type().clone());
                evals.push(arr);
            }
            let target_dt = infer_common_type(&dts)?;
            if target_dt == DataType::Null {
                return Ok(new_null_array(&DataType::Null, len));
            }
            let mut iter = evals.into_iter().rev();
            let mut acc = cast_array(&iter.next().unwrap(), &target_dt)?;
            for prev in iter {
                let prev_cast = cast_array(&prev, &target_dt)?;
                let present = is_not_null(&prev_cast)
                    .map_err(|e| SaciError::generic(format!("COALESCE is_not_null: {e}")))?;
                acc = zip(&present, &prev_cast, &acc)
                    .map_err(|e| SaciError::generic(format!("COALESCE zip: {e}")))?;
            }
            Ok(acc)
        }
        ScalarFunc::NullIf => {
            expect_arity(func, args, 2)?;
            let lhs = args[0].evaluate(batch)?;
            let rhs = args[1].evaluate(batch)?;
            let (lc, rc) = coerce_comparison_pair(lhs.clone(), rhs)?;
            let equal = eq(&lc, &rc).map_err(|e| SaciError::generic(format!("NULLIF eq: {e}")))?;
            let nulls = new_null_array(lhs.data_type(), len);
            zip(&equal, &nulls, &lhs).map_err(|e| SaciError::generic(format!("NULLIF zip: {e}")))
        }
        ScalarFunc::EpochMs => {
            expect_arity(func, args, 1)?;
            let arr = args[0].evaluate(batch)?;
            epoch_ms_array(&arr)
        }
        ScalarFunc::DateTrunc => {
            expect_arity(func, args, 2)?;
            let unit_arr = to_utf8_array(&args[0].evaluate(batch)?)?;
            let ts_arr = args[1].evaluate(batch)?;
            let ms_arr = epoch_ms_array(&ts_arr)?;
            let u_sa = unit_arr.as_string::<i32>();
            let ms_vals = ms_arr.as_primitive::<Int64Type>();
            let mut out_ms: Vec<Option<i64>> = Vec::with_capacity(len);
            for i in 0..len {
                if u_sa.is_null(i) || ms_vals.is_null(i) {
                    out_ms.push(None);
                    continue;
                }
                let step = match u_sa.value(i).to_ascii_lowercase().as_str() {
                    "millisecond" | "milliseconds" | "ms" => 1i64,
                    "second" | "seconds" | "s" => 1_000,
                    "minute" | "minutes" | "min" => 60_000,
                    "hour" | "hours" | "h" => 3_600_000,
                    "day" | "days" | "d" => 86_400_000,
                    other => {
                        return Err(SaciError::configuration(format!(
                            "DATE_TRUNC unsupported unit '{other}' (expected second, minute, hour, or day)"
                        )));
                    }
                };
                out_ms.push(Some(ms_vals.value(i).div_euclid(step) * step));
            }
            let raw: ArrayRef = Arc::new(Int64Array::from(out_ms));
            cast_array(&raw, &DataType::Timestamp(TimeUnit::Millisecond, None))
        }
    }
}

fn expect_arity(func: ScalarFunc, args: &[Expr], expected: usize) -> SaciResult<()> {
    if args.len() != expected {
        return Err(SaciError::configuration(format!(
            "{func:?} expects {expected} argument(s), got {}",
            args.len()
        )));
    }
    Ok(())
}

fn to_utf8_array(arr: &ArrayRef) -> SaciResult<ArrayRef> {
    match arr.data_type() {
        DataType::Utf8 => Ok(arr.clone()),
        DataType::Null => Ok(new_null_array(&DataType::Utf8, arr.len())),
        DataType::LargeUtf8 | DataType::Utf8View => cast_array(arr, &DataType::Utf8),
        _ => {
            let formatter = ArrayFormatter::try_new(arr.as_ref(), &FormatOptions::default())
                .map_err(|e| SaciError::generic(format!("format array to Utf8: {e}")))?;
            let vals: StringArray = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(formatter.value(i).to_string())
                    }
                })
                .collect();
            Ok(Arc::new(vals))
        }
    }
}

fn eval_concat(arrays: &[ArrayRef], len: usize) -> SaciResult<ArrayRef> {
    let mut utf8s = Vec::with_capacity(arrays.len());
    for a in arrays {
        utf8s.push(to_utf8_array(a)?);
    }
    let refs: Vec<&StringArray> = utf8s.iter().map(|a| a.as_string::<i32>()).collect();
    let out: StringArray = (0..len)
        .map(|i| {
            if refs.iter().any(|r| r.is_null(i)) {
                None
            } else {
                let mut s = String::new();
                for r in &refs {
                    s.push_str(r.value(i));
                }
                Some(s)
            }
        })
        .collect();
    Ok(Arc::new(out))
}

fn eval_like(
    lhs: &ArrayRef,
    rhs: &ArrayRef,
    case_insensitive: bool,
    negated: bool,
) -> SaciResult<ArrayRef> {
    let len = lhs.len();
    let l = to_utf8_array(lhs)?;
    let r = to_utf8_array(rhs)?;
    let ls = l.as_string::<i32>();
    let rs = r.as_string::<i32>();
    let out: BooleanArray = (0..len)
        .map(|i| {
            if ls.is_null(i) || rs.is_null(i) {
                None
            } else {
                let text = ls.value(i);
                let pat = rs.value(i);
                let matched = if case_insensitive {
                    sql_like_match(&text.to_lowercase(), &pat.to_lowercase())
                } else {
                    sql_like_match(text, pat)
                };
                Some(if negated { !matched } else { matched })
            }
        })
        .collect();
    Ok(Arc::new(out))
}

fn sql_like_match(text: &str, pattern: &str) -> bool {
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let (mut ti, mut pi) = (0usize, 0usize);
    let mut star_pi: Option<usize> = None;
    let mut match_ti = 0usize;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star_pi = Some(pi);
            match_ti = ti;
            pi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            match_ti += 1;
            ti = match_ti;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

fn epoch_ms_array(arr: &ArrayRef) -> SaciResult<ArrayRef> {
    let len = arr.len();
    match arr.data_type() {
        DataType::Null => Ok(new_null_array(&DataType::Int64, len)),
        DataType::Int64 | DataType::Timestamp(_, _) => Ok(Arc::new(super::time::to_ms_array(arr)?)),
        DataType::Date32 => {
            let a = arr.as_primitive::<Date32Type>();
            let out: Int64Array = a
                .iter()
                .map(|v| v.map(|d| i64::from(d) * 86_400_000))
                .collect();
            Ok(Arc::new(out))
        }
        DataType::Int32
        | DataType::Int16
        | DataType::Int8
        | DataType::UInt32
        | DataType::UInt16
        | DataType::UInt8
        | DataType::UInt64
        | DataType::Float64
        | DataType::Float32 => cast_array(arr, &DataType::Int64),
        DataType::Utf8 | DataType::LargeUtf8 => {
            let ts = cast_array(arr, &DataType::Timestamp(TimeUnit::Millisecond, None))?;
            Ok(Arc::new(super::time::to_ms_array(&ts)?))
        }
        other => Err(SaciError::configuration(format!(
            "EPOCH_MS cannot convert {other:?}"
        ))),
    }
}

/// Evaluates `CAST` target datatype names inside expressions and SQL.
pub(crate) fn parse_sql_type(name: &str) -> SaciResult<DataType> {
    match name.to_ascii_uppercase().as_str() {
        "BOOL" | "BOOLEAN" => Ok(DataType::Boolean),
        "TINYINT" | "INT8" => Ok(DataType::Int8),
        "SMALLINT" | "INT16" => Ok(DataType::Int16),
        "INT" | "INTEGER" | "INT32" => Ok(DataType::Int32),
        "BIGINT" | "INT64" => Ok(DataType::Int64),
        "UTINYINT" | "UINT8" => Ok(DataType::UInt8),
        "USMALLINT" | "UINT16" => Ok(DataType::UInt16),
        "UINT" | "UINT32" => Ok(DataType::UInt32),
        "UBIGINT" | "UINT64" => Ok(DataType::UInt64),
        "FLOAT" | "FLOAT32" | "REAL" => Ok(DataType::Float32),
        "DOUBLE" | "FLOAT64" => Ok(DataType::Float64),
        "TEXT" | "STRING" | "VARCHAR" | "UTF8" => Ok(DataType::Utf8),
        "DATE" | "DATE32" => Ok(DataType::Date32),
        "TIMESTAMP" | "TIMESTAMP_MS" => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        _ => Err(SaciError::configuration(format!(
            "unsupported SQL cast target type '{name}'"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Tokenizer & recursive-descent expression parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Token {
    Ident(String),
    QuotedIdent(String),
    String(String),
    Int(i64),
    Float(f64),
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Concat,
    DoubleColon,
    LParen,
    RParen,
    Comma,
    Dot,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ident(s) | Self::QuotedIdent(s) => f.write_str(s),
            Self::String(s) => write!(f, "'{s}'"),
            Self::Int(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Plus => f.write_str("+"),
            Self::Minus => f.write_str("-"),
            Self::Star => f.write_str("*"),
            Self::Slash => f.write_str("/"),
            Self::Percent => f.write_str("%"),
            Self::Eq => f.write_str("="),
            Self::Ne => f.write_str("!="),
            Self::Lt => f.write_str("<"),
            Self::Le => f.write_str("<="),
            Self::Gt => f.write_str(">"),
            Self::Ge => f.write_str(">="),
            Self::Concat => f.write_str("||"),
            Self::DoubleColon => f.write_str("::"),
            Self::LParen => f.write_str("("),
            Self::RParen => f.write_str(")"),
            Self::Comma => f.write_str(","),
            Self::Dot => f.write_str("."),
        }
    }
}

pub(crate) fn tokenize(input: &str) -> SaciResult<Vec<Token>> {
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    let mut tokens = Vec::new();

    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        // Single-line SQL comments `-- ...`
        if c == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            continue;
        }
        match c {
            '+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            '/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            '%' => {
                tokens.push(Token::Percent);
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            ';' => {
                i += 1;
            }
            '=' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    i += 2;
                } else {
                    i += 1;
                }
                tokens.push(Token::Eq);
            }
            '!' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Ne);
                    i += 2;
                } else {
                    return Err(SaciError::configuration(format!(
                        "unexpected character '!' at position {i}"
                    )));
                }
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Le);
                    i += 2;
                } else if i + 1 < chars.len() && chars[i + 1] == '>' {
                    tokens.push(Token::Ne);
                    i += 2;
                } else {
                    tokens.push(Token::Lt);
                    i += 1;
                }
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Ge);
                    i += 2;
                } else {
                    tokens.push(Token::Gt);
                    i += 1;
                }
            }
            '|' => {
                if i + 1 < chars.len() && chars[i + 1] == '|' {
                    tokens.push(Token::Concat);
                    i += 2;
                } else {
                    return Err(SaciError::configuration(
                        "unexpected '|' (did you mean '||')",
                    ));
                }
            }
            ':' => {
                if i + 1 < chars.len() && chars[i + 1] == ':' {
                    tokens.push(Token::DoubleColon);
                    i += 2;
                } else {
                    return Err(SaciError::configuration(
                        "unexpected ':' (did you mean '::')",
                    ));
                }
            }
            '\'' => {
                i += 1;
                let mut s = String::new();
                let mut closed = false;
                while i < chars.len() {
                    if chars[i] == '\'' {
                        if i + 1 < chars.len() && chars[i + 1] == '\'' {
                            s.push('\'');
                            i += 2;
                        } else {
                            i += 1;
                            closed = true;
                            break;
                        }
                    } else {
                        s.push(chars[i]);
                        i += 1;
                    }
                }
                if !closed {
                    return Err(SaciError::configuration("unterminated string literal"));
                }
                tokens.push(Token::String(s));
            }
            '"' | '`' => {
                let quote = c;
                i += 1;
                let mut s = String::new();
                let mut closed = false;
                while i < chars.len() {
                    if chars[i] == quote {
                        i += 1;
                        closed = true;
                        break;
                    }
                    s.push(chars[i]);
                    i += 1;
                }
                if !closed {
                    return Err(SaciError::configuration("unterminated quoted identifier"));
                }
                tokens.push(Token::QuotedIdent(s));
            }
            c if c.is_ascii_digit() => {
                let start = i;
                let mut is_float = false;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '_') {
                    i += 1;
                }
                if i < chars.len()
                    && chars[i] == '.'
                    && i + 1 < chars.len()
                    && chars[i + 1].is_ascii_digit()
                {
                    is_float = true;
                    i += 1;
                    while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '_') {
                        i += 1;
                    }
                }
                if i < chars.len() && (chars[i] == 'e' || chars[i] == 'E') {
                    is_float = true;
                    i += 1;
                    if i < chars.len() && (chars[i] == '+' || chars[i] == '-') {
                        i += 1;
                    }
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                }
                let raw: String = chars[start..i].iter().filter(|ch| **ch != '_').collect();
                if is_float {
                    let v: f64 = raw.parse().map_err(|_| {
                        SaciError::configuration(format!("invalid float literal '{raw}'"))
                    })?;
                    tokens.push(Token::Float(v));
                } else {
                    let v: i64 = raw.parse().map_err(|_| {
                        SaciError::configuration(format!("invalid integer literal '{raw}'"))
                    })?;
                    tokens.push(Token::Int(v));
                }
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let ident: String = chars[start..i].iter().collect();
                tokens.push(Token::Ident(ident));
            }
            other => {
                return Err(SaciError::configuration(format!(
                    "unexpected character '{other}' in expression"
                )));
            }
        }
    }

    Ok(tokens)
}

pub(crate) struct ExprParser<'a> {
    tokens: &'a [Token],
    pos: usize,
}

impl<'a> ExprParser<'a> {
    pub(crate) fn new(tokens: &'a [Token]) -> Self {
        Self { tokens, pos: 0 }
    }

    pub(crate) fn is_eof(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    pub(crate) fn set_pos(&mut self, pos: usize) {
        self.pos = pos;
    }

    pub(crate) fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    pub(crate) fn peek_display(&self) -> String {
        self.peek()
            .map_or_else(|| "EOF".to_string(), ToString::to_string)
    }

    pub(crate) fn peek_keyword(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Token::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }

    pub(crate) fn eat_keyword(&mut self, kw: &str) -> bool {
        if self.peek_keyword(kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    pub(crate) fn expect_keyword(&mut self, kw: &str) -> SaciResult<()> {
        if self.eat_keyword(kw) {
            Ok(())
        } else {
            Err(SaciError::configuration(format!(
                "expected keyword '{kw}', found '{}'",
                self.peek_display()
            )))
        }
    }

    pub(crate) fn eat(&mut self, expected: &Token) -> bool {
        if self.peek() == Some(expected) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    pub(crate) fn expect(&mut self, expected: &Token) -> SaciResult<()> {
        if self.eat(expected) {
            Ok(())
        } else {
            Err(SaciError::configuration(format!(
                "expected '{expected}', found '{}'",
                self.peek_display()
            )))
        }
    }

    pub(crate) fn parse_expr(&mut self) -> SaciResult<Expr> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> SaciResult<Expr> {
        let mut left = self.parse_and()?;
        while self.eat_keyword("OR") {
            let right = self.parse_and()?;
            left = Expr::binary(left, BinaryOp::Or, right);
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> SaciResult<Expr> {
        let mut left = self.parse_not()?;
        while self.eat_keyword("AND") {
            let right = self.parse_not()?;
            left = Expr::binary(left, BinaryOp::And, right);
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> SaciResult<Expr> {
        if self.eat_keyword("NOT") {
            let inner = self.parse_not()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(inner),
            });
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> SaciResult<Expr> {
        let mut left = self.parse_concat()?;
        loop {
            if self.eat(&Token::Eq) {
                let right = self.parse_concat()?;
                left = Expr::binary(left, BinaryOp::Eq, right);
            } else if self.eat(&Token::Ne) {
                let right = self.parse_concat()?;
                left = Expr::binary(left, BinaryOp::Ne, right);
            } else if self.eat(&Token::Lt) {
                let right = self.parse_concat()?;
                left = Expr::binary(left, BinaryOp::Lt, right);
            } else if self.eat(&Token::Le) {
                let right = self.parse_concat()?;
                left = Expr::binary(left, BinaryOp::Le, right);
            } else if self.eat(&Token::Gt) {
                let right = self.parse_concat()?;
                left = Expr::binary(left, BinaryOp::Gt, right);
            } else if self.eat(&Token::Ge) {
                let right = self.parse_concat()?;
                left = Expr::binary(left, BinaryOp::Ge, right);
            } else if self.eat_keyword("IS") {
                let negated = self.eat_keyword("NOT");
                if self.eat_keyword("NULL") {
                    left = Expr::Unary {
                        op: if negated {
                            UnaryOp::IsNotNull
                        } else {
                            UnaryOp::IsNull
                        },
                        expr: Box::new(left),
                    };
                } else if self.eat_keyword("DISTINCT") {
                    self.expect_keyword("FROM")?;
                    let right = self.parse_concat()?;
                    left = Expr::binary(
                        left,
                        if negated {
                            BinaryOp::IsNotDistinctFrom
                        } else {
                            BinaryOp::IsDistinctFrom
                        },
                        right,
                    );
                } else {
                    return Err(SaciError::configuration(format!(
                        "expected NULL or DISTINCT FROM after IS, found '{}'",
                        self.peek_display()
                    )));
                }
            } else if self.peek_keyword("NOT") {
                let saved = self.pos;
                self.pos += 1;
                if self.eat_keyword("IN") {
                    left = self.parse_in_list(left, true)?;
                } else if self.eat_keyword("BETWEEN") {
                    left = self.parse_between(left, true)?;
                } else if self.eat_keyword("LIKE") {
                    let right = self.parse_concat()?;
                    left = Expr::binary(left, BinaryOp::NotLike, right);
                } else if self.eat_keyword("ILIKE") {
                    let right = self.parse_concat()?;
                    left = Expr::binary(left, BinaryOp::NotILike, right);
                } else {
                    self.pos = saved;
                    break;
                }
            } else if self.eat_keyword("IN") {
                left = self.parse_in_list(left, false)?;
            } else if self.eat_keyword("BETWEEN") {
                left = self.parse_between(left, false)?;
            } else if self.eat_keyword("LIKE") {
                let right = self.parse_concat()?;
                left = Expr::binary(left, BinaryOp::Like, right);
            } else if self.eat_keyword("ILIKE") {
                let right = self.parse_concat()?;
                left = Expr::binary(left, BinaryOp::ILike, right);
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_in_list(&mut self, expr: Expr, negated: bool) -> SaciResult<Expr> {
        self.expect(&Token::LParen)?;
        let mut list = Vec::new();
        if !self.eat(&Token::RParen) {
            loop {
                list.push(self.parse_expr()?);
                if self.eat(&Token::Comma) {
                    continue;
                }
                self.expect(&Token::RParen)?;
                break;
            }
        }
        Ok(Expr::InList {
            expr: Box::new(expr),
            list,
            negated,
        })
    }

    fn parse_between(&mut self, expr: Expr, negated: bool) -> SaciResult<Expr> {
        let low = self.parse_concat()?;
        self.expect_keyword("AND")?;
        let high = self.parse_concat()?;
        Ok(Expr::Between {
            expr: Box::new(expr),
            low: Box::new(low),
            high: Box::new(high),
            negated,
        })
    }

    fn parse_concat(&mut self) -> SaciResult<Expr> {
        let mut left = self.parse_add()?;
        while self.eat(&Token::Concat) {
            let right = self.parse_add()?;
            left = Expr::binary(left, BinaryOp::Concat, right);
        }
        Ok(left)
    }

    fn parse_add(&mut self) -> SaciResult<Expr> {
        let mut left = self.parse_mul()?;
        loop {
            if self.eat(&Token::Plus) {
                let right = self.parse_mul()?;
                left = Expr::binary(left, BinaryOp::Add, right);
            } else if self.eat(&Token::Minus) {
                let right = self.parse_mul()?;
                left = Expr::binary(left, BinaryOp::Sub, right);
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_mul(&mut self) -> SaciResult<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            if self.eat(&Token::Star) {
                let right = self.parse_unary()?;
                left = Expr::binary(left, BinaryOp::Mul, right);
            } else if self.eat(&Token::Slash) {
                let right = self.parse_unary()?;
                left = Expr::binary(left, BinaryOp::Div, right);
            } else if self.eat(&Token::Percent) {
                let right = self.parse_unary()?;
                left = Expr::binary(left, BinaryOp::Mod, right);
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> SaciResult<Expr> {
        if self.eat(&Token::Minus) {
            if let Some(&Token::Int(v)) = self.peek() {
                self.pos += 1;
                return self.parse_postfix(Expr::Literal(ScalarValue::Int64(-v)));
            }
            if let Some(&Token::Float(v)) = self.peek() {
                self.pos += 1;
                return self.parse_postfix(Expr::Literal(ScalarValue::Float64(-v)));
            }
            let inner = self.parse_unary()?;
            return Ok(Expr::Unary {
                op: UnaryOp::Neg,
                expr: Box::new(inner),
            });
        }
        if self.eat(&Token::Plus) {
            return self.parse_unary();
        }
        let primary = self.parse_primary()?;
        self.parse_postfix(primary)
    }

    fn parse_postfix(&mut self, mut expr: Expr) -> SaciResult<Expr> {
        while self.eat(&Token::DoubleColon) {
            let dt = self.parse_type_name()?;
            expr = Expr::Cast {
                expr: Box::new(expr),
                data_type: dt,
            };
        }
        Ok(expr)
    }

    pub(crate) fn parse_type_name(&mut self) -> SaciResult<DataType> {
        match self.peek().cloned() {
            Some(Token::Ident(name)) => {
                self.pos += 1;
                parse_sql_type(&name)
            }
            _ => Err(SaciError::configuration(format!(
                "expected type name, found '{}'",
                self.peek_display()
            ))),
        }
    }

    fn parse_primary(&mut self) -> SaciResult<Expr> {
        match self.peek().cloned() {
            Some(Token::LParen) => {
                self.pos += 1;
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            Some(Token::Int(v)) => {
                self.pos += 1;
                Ok(Expr::Literal(ScalarValue::Int64(v)))
            }
            Some(Token::Float(v)) => {
                self.pos += 1;
                Ok(Expr::Literal(ScalarValue::Float64(v)))
            }
            Some(Token::String(s)) => {
                self.pos += 1;
                Ok(Expr::Literal(ScalarValue::Utf8(s)))
            }
            Some(Token::QuotedIdent(name)) => {
                self.pos += 1;
                if self.eat(&Token::Dot) {
                    let col = self.expect_ident_name()?;
                    Ok(Expr::Column(format!("{name}.{col}")))
                } else {
                    Ok(Expr::Column(name))
                }
            }
            Some(Token::Ident(word)) => {
                if word.eq_ignore_ascii_case("NULL") {
                    self.pos += 1;
                    return Ok(Expr::Literal(ScalarValue::Null));
                }
                if word.eq_ignore_ascii_case("TRUE") {
                    self.pos += 1;
                    return Ok(Expr::Literal(ScalarValue::Bool(true)));
                }
                if word.eq_ignore_ascii_case("FALSE") {
                    self.pos += 1;
                    return Ok(Expr::Literal(ScalarValue::Bool(false)));
                }
                if word.eq_ignore_ascii_case("CASE") {
                    self.pos += 1;
                    return self.parse_case();
                }
                if word.eq_ignore_ascii_case("CAST")
                    && matches!(self.tokens.get(self.pos + 1), Some(Token::LParen))
                {
                    self.pos += 2;
                    let inner = self.parse_expr()?;
                    self.expect_keyword("AS")?;
                    let dt = self.parse_type_name()?;
                    self.expect(&Token::RParen)?;
                    return Ok(Expr::Cast {
                        expr: Box::new(inner),
                        data_type: dt,
                    });
                }

                self.pos += 1;
                if self.eat(&Token::LParen) {
                    let mut args = Vec::new();
                    if !self.eat(&Token::RParen) {
                        loop {
                            args.push(self.parse_expr()?);
                            if self.eat(&Token::Comma) {
                                continue;
                            }
                            self.expect(&Token::RParen)?;
                            break;
                        }
                    }
                    let func = ScalarFunc::from_name(&word)?;
                    return Ok(Expr::Call { func, args });
                }
                if self.eat(&Token::Dot) {
                    let col = self.expect_ident_name()?;
                    return Ok(Expr::Column(format!("{word}.{col}")));
                }
                Ok(Expr::Column(word))
            }
            _ => Err(SaciError::configuration(format!(
                "unexpected token '{}' in expression",
                self.peek_display()
            ))),
        }
    }

    fn parse_case(&mut self) -> SaciResult<Expr> {
        let operand = if self.peek_keyword("WHEN") {
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };
        let mut when_then = Vec::new();
        while self.eat_keyword("WHEN") {
            let cond = self.parse_expr()?;
            self.expect_keyword("THEN")?;
            let val = self.parse_expr()?;
            when_then.push((cond, val));
        }
        if when_then.is_empty() {
            return Err(SaciError::configuration("CASE requires at least one WHEN"));
        }
        let else_expr = if self.eat_keyword("ELSE") {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword("END")?;
        Ok(Expr::Case {
            operand,
            when_then,
            else_expr,
        })
    }

    pub(crate) fn expect_ident_name(&mut self) -> SaciResult<String> {
        match self.peek().cloned() {
            Some(Token::Ident(s)) | Some(Token::QuotedIdent(s)) => {
                self.pos += 1;
                Ok(s)
            }
            _ => Err(SaciError::configuration(format!(
                "expected identifier, found '{}'",
                self.peek_display()
            ))),
        }
    }

    pub(crate) fn expect_usize(&mut self) -> SaciResult<usize> {
        if let Some(&Token::Int(v)) = self.peek()
            && v >= 0
        {
            self.pos += 1;
            return Ok(v as usize);
        }
        Err(SaciError::configuration(format!(
            "expected non-negative integer, found '{}'",
            self.peek_display()
        )))
    }
}

// ---------------------------------------------------------------------------
// FilterSystem
// ---------------------------------------------------------------------------

/// Filters rows in `source_component` matching a vectorized [`Expr`] predicate
/// and writes the kept rows into `target_component` (in-place by default).
#[derive(Debug, Clone)]
pub struct FilterSystem {
    source_component: &'static str,
    target_component: &'static str,
    predicate: Expr,
}

impl FilterSystem {
    /// Creates an in-place filter system over `component`.
    #[must_use]
    pub fn new(component: impl AsRef<str>, predicate: Expr) -> Self {
        let name = intern_component_name(component.as_ref());
        Self {
            source_component: name,
            target_component: name,
            predicate,
        }
    }

    /// Parses a SQL predicate string (for example `"amount > 100 AND status != 'void'"`)
    /// into a [`FilterSystem`].
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `predicate` fails to parse.
    pub fn parse(component: impl AsRef<str>, predicate: &str) -> SaciResult<Self> {
        Ok(Self::new(component, Expr::parse(predicate)?))
    }

    /// Writes the filtered batch to `target_component` instead of overwriting the
    /// source component.
    #[must_use]
    pub fn into_component(mut self, target_component: impl AsRef<str>) -> Self {
        self.target_component = intern_component_name(target_component.as_ref());
        self
    }

    /// Evaluates the filter predicate over `batch`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError`] when evaluation or Arrow record-batch filtering fails.
    pub fn filter_batch(&self, batch: &RecordBatch) -> SaciResult<RecordBatch> {
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(batch.schema()));
        }
        let mask = self.predicate.evaluate_predicate(batch)?;
        filter_record_batch(batch, &mask)
            .map_err(|e| SaciError::generic(format!("FilterSystem: {e}")))
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let input = data.batch_named(self.source_component)?.clone();
        let filtered = self.filter_batch(&input)?;
        data.put_batch_named(self.target_component, filtered)
    }
}

#[async_trait]
impl System for FilterSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("filter")
            .reads_all_named(self.source_component)
            .writes_all_named(self.target_component)
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        self.run_impl(data)
    }

    fn run_sync(&self, data: &mut Dataset) -> Option<Result<(), SaciError>> {
        Some(self.run_impl(data))
    }
}

// ---------------------------------------------------------------------------
// ProjectSystem
// ---------------------------------------------------------------------------

/// A single output specification inside a [`ProjectSystem`].
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectItem {
    /// Expands all columns of the input batch in order.
    Wildcard,
    /// Evaluates `expr` into output column `name`.
    Expr {
        /// Output column name.
        name: String,
        /// Vectorized expression.
        expr: Expr,
    },
}

/// Evaluates vectorized projection expressions over `source_component` and writes
/// the resulting [`RecordBatch`] into `target_component` (in-place by default).
#[derive(Debug, Clone)]
pub struct ProjectSystem {
    source_component: &'static str,
    target_component: &'static str,
    items: Vec<ProjectItem>,
}

impl ProjectSystem {
    /// Creates an in-place [`ProjectSystem`] from `(output_name, Expr)` pairs.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `projections` is empty or
    /// contains duplicate output column names.
    pub fn new<I, S>(component: impl AsRef<str>, projections: I) -> SaciResult<Self>
    where
        I: IntoIterator<Item = (S, Expr)>,
        S: Into<String>,
    {
        let items: Vec<ProjectItem> = projections
            .into_iter()
            .map(|(n, e)| ProjectItem::Expr {
                name: n.into(),
                expr: e,
            })
            .collect();
        Self::from_items(component, items)
    }

    /// Creates an in-place [`ProjectSystem`] from explicit [`ProjectItem`]s
    /// (allowing [`ProjectItem::Wildcard`] mixed with computed columns).
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `items` is empty or names collide.
    pub fn from_items(component: impl AsRef<str>, items: Vec<ProjectItem>) -> SaciResult<Self> {
        if items.is_empty() {
            return Err(SaciError::configuration(
                "ProjectSystem requires at least one projection",
            ));
        }
        let mut seen = HashSet::new();
        for item in &items {
            if let ProjectItem::Expr { name, .. } = item
                && !seen.insert(name.clone())
            {
                return Err(SaciError::configuration(format!(
                    "duplicate projection column '{name}'"
                )));
            }
        }
        let name = intern_component_name(component.as_ref());
        Ok(Self {
            source_component: name,
            target_component: name,
            items,
        })
    }

    /// Parses projection clauses (`"*"`, `"col"`, `"expr AS alias"`, ...) into a
    /// [`ProjectSystem`].
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when any projection string fails to
    /// parse.
    pub fn parse<S: AsRef<str>>(component: impl AsRef<str>, clauses: &[S]) -> SaciResult<Self> {
        let mut items = Vec::with_capacity(clauses.len());
        for raw in clauses {
            let trimmed = raw.as_ref().trim();
            if trimmed == "*" {
                items.push(ProjectItem::Wildcard);
                continue;
            }
            let tokens = tokenize(trimmed)?;
            if tokens.is_empty() {
                return Err(SaciError::configuration("empty projection clause"));
            }
            let mut parser = ExprParser::new(&tokens);
            let expr = parser.parse_expr()?;
            let has_explicit_or_implicit_alias = parser.eat_keyword("AS")
                || (matches!(
                    parser.peek(),
                    Some(Token::Ident(_)) | Some(Token::QuotedIdent(_))
                ) && parser.pos() + 1 == tokens.len());
            let name = if has_explicit_or_implicit_alias {
                parser.expect_ident_name()?
            } else {
                expr.default_alias()
            };
            if !parser.is_eof() {
                return Err(SaciError::configuration(format!(
                    "unexpected trailing token '{}' in projection '{trimmed}'",
                    parser.peek_display()
                )));
            }
            items.push(ProjectItem::Expr { name, expr });
        }
        Self::from_items(component, items)
    }

    /// Writes the projected batch to `target_component`.
    #[must_use]
    pub fn into_component(mut self, target_component: impl AsRef<str>) -> Self {
        self.target_component = intern_component_name(target_component.as_ref());
        self
    }

    /// Computes the projected Arrow [`SchemaRef`] given `input_schema`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError`] when any referenced input column is missing or a
    /// duplicate output column name is formed after wildcard expansion.
    pub fn output_schema(&self, input_schema: &Schema) -> SaciResult<SchemaRef> {
        let mut fields = Vec::new();
        let mut seen = HashSet::new();
        for item in &self.items {
            match item {
                ProjectItem::Wildcard => {
                    for f in input_schema.fields() {
                        if !seen.insert(f.name().clone()) {
                            return Err(SaciError::configuration(format!(
                                "duplicate projection column '{}'",
                                f.name()
                            )));
                        }
                        fields.push(f.as_ref().clone());
                    }
                }
                ProjectItem::Expr { name, expr } => {
                    if !seen.insert(name.clone()) {
                        return Err(SaciError::configuration(format!(
                            "duplicate projection column '{name}'"
                        )));
                    }
                    let dt = expr.data_type(input_schema)?;
                    fields.push(Field::new(name, dt, true));
                }
            }
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    /// Evaluates every projection over `batch`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError`] when evaluation fails or output columns collide.
    pub fn execute_batch(&self, batch: &RecordBatch) -> SaciResult<RecordBatch> {
        let mut fields = Vec::new();
        let mut cols = Vec::new();
        let mut seen = HashSet::new();

        for item in &self.items {
            match item {
                ProjectItem::Wildcard => {
                    for (idx, f) in batch.schema_ref().fields().iter().enumerate() {
                        if !seen.insert(f.name().clone()) {
                            return Err(SaciError::configuration(format!(
                                "duplicate projection column '{}'",
                                f.name()
                            )));
                        }
                        fields.push(f.as_ref().clone());
                        cols.push(batch.column(idx).clone());
                    }
                }
                ProjectItem::Expr { name, expr } => {
                    if !seen.insert(name.clone()) {
                        return Err(SaciError::configuration(format!(
                            "duplicate projection column '{name}'"
                        )));
                    }
                    let arr = expr.evaluate(batch)?;
                    fields.push(Field::new(name, arr.data_type().clone(), true));
                    cols.push(arr);
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));
        if cols.is_empty() {
            return Ok(RecordBatch::new_empty(schema));
        }
        RecordBatch::try_new(schema, cols)
            .map_err(|e| SaciError::generic(format!("ProjectSystem batch: {e}")))
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let input = data.batch_named(self.source_component)?.clone();
        let out = self.execute_batch(&input)?;
        data.put_batch_named(self.target_component, out)
    }
}

#[async_trait]
impl System for ProjectSystem {
    fn meta(&self) -> SystemMeta {
        SystemMeta::new("project")
            .reads_all_named(self.source_component)
            .writes_all_named(self.target_component)
    }

    async fn run(&self, data: &mut Dataset) -> Result<(), SaciError> {
        self.run_impl(data)
    }

    fn run_sync(&self, data: &mut Dataset) -> Option<Result<(), SaciError>> {
        Some(self.run_impl(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Float64Array;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("qty", DataType::Int64, false),
            Field::new("price", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![2, 5, 1, 0])),
                Arc::new(Float64Array::from(vec![
                    Some(10.5),
                    Some(20.0),
                    None,
                    Some(5.0),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("  Widget A "),
                    Some("Gadget B"),
                    Some("widget c"),
                    None,
                ])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_expr_arithmetic_boolean_and_functions() {
        let batch = sample_batch();
        let e = Expr::parse("qty * COALESCE(price, 0.0) + 1").unwrap();
        let out = e.evaluate(&batch).unwrap();
        let f = out.as_primitive::<Float64Type>();
        assert_eq!(f.values(), &[22.0, 101.0, 1.0, 1.0]);

        let p = Expr::parse("LOWER(TRIM(name)) LIKE 'widget%' AND qty > 1").unwrap();
        let mask = p.evaluate_predicate(&batch).unwrap();
        assert_eq!(
            mask.iter().collect::<Vec<_>>(),
            vec![Some(true), Some(false), Some(false), Some(false)]
        );
    }

    #[test]
    fn test_expr_case_in_between_cast() {
        let batch = sample_batch();
        let c = Expr::parse(
            "CASE WHEN qty BETWEEN 2 AND 5 THEN 'mid' WHEN id IN (3, 4) THEN 'tail' ELSE 'other' END",
        )
        .unwrap();
        let arr = c.evaluate(&batch).unwrap();
        let sa = arr.as_string::<i32>();
        assert_eq!(
            sa.iter().collect::<Vec<_>>(),
            vec![Some("mid"), Some("mid"), Some("tail"), Some("tail")]
        );

        let casted = Expr::parse("id::FLOAT64 + 0.5").unwrap();
        let farr = casted.evaluate(&batch).unwrap();
        assert_eq!(
            farr.as_primitive::<Float64Type>().values(),
            &[1.5, 2.5, 3.5, 4.5]
        );
    }

    #[test]
    fn test_filter_and_project_systems() {
        let batch = sample_batch();
        let schema = batch.schema();

        let mut ds = Dataset::new();
        ds.register_schema("orders", schema).unwrap();
        ds.set_batch_named("orders", batch).unwrap();

        let filter = FilterSystem::parse("orders", "price IS NOT NULL AND qty >= 2").unwrap();
        filter.run_sync(&mut ds).unwrap().unwrap();
        assert_eq!(ds.batch_named("orders").unwrap().num_rows(), 2);

        let project = ProjectSystem::parse(
            "orders",
            &[
                "id",
                "qty * price AS total",
                "UPPER(TRIM(name)) AS upper_name",
            ],
        )
        .unwrap();
        project.run_sync(&mut ds).unwrap().unwrap();
        let out = ds.batch_named("orders").unwrap();
        assert_eq!(out.num_columns(), 3);
        assert_eq!(out.schema().field(1).name(), "total");
        assert_eq!(
            out.column(1).as_primitive::<Float64Type>().values(),
            &[21.0, 100.0]
        );
        assert_eq!(
            out.column(2).as_string::<i32>().iter().collect::<Vec<_>>(),
            vec![Some("WIDGET A"), Some("GADGET B")]
        );
    }

    #[test]
    fn case_when_short_circuits_division_by_zero_and_invalid_cast() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("d", DataType::Int64, false),
            Field::new("raw", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
                Arc::new(Int64Array::from(vec![2, 0, 5])) as ArrayRef,
                Arc::new(StringArray::from(vec!["12", "not_a_num", "19"])) as ArrayRef,
            ],
        )
        .unwrap();

        let div_case = Expr::parse("CASE WHEN d != 0 THEN n / d ELSE 0 END").unwrap();
        let div_out = div_case.evaluate(&batch).unwrap();
        assert_eq!(div_out.as_primitive::<Int64Type>().values(), &[5, 0, 6]);

        let cast_case =
            Expr::parse("CASE WHEN raw LIKE '1%' THEN CAST(raw AS INT) ELSE -1 END").unwrap();
        let cast_out = cast_case.evaluate(&batch).unwrap();
        let cast_vals: Vec<i64> = (0..cast_out.len())
            .map(|i| {
                cast_array(&cast_out, &DataType::Int64)
                    .unwrap()
                    .as_primitive::<Int64Type>()
                    .value(i)
            })
            .collect();
        assert_eq!(cast_vals, vec![12, -1, 19]);
    }

    #[test]
    fn kleene_three_valued_logic_truth_tables() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![None, Some(1)])) as ArrayRef],
        )
        .unwrap();

        let eval_bool = |s: &str| -> Vec<Option<bool>> {
            Expr::parse(s)
                .unwrap()
                .evaluate_predicate(&batch)
                .unwrap()
                .iter()
                .collect()
        };

        assert_eq!(eval_bool("NULL AND FALSE"), vec![Some(false), Some(false)]);
        assert_eq!(eval_bool("NULL AND TRUE"), vec![None, None]);
        assert_eq!(eval_bool("NULL OR TRUE"), vec![Some(true), Some(true)]);
        assert_eq!(eval_bool("NULL OR FALSE"), vec![None, None]);
        assert_eq!(eval_bool("NOT (x = 1)"), vec![None, Some(false)]);
        assert_eq!(
            eval_bool("x IS DISTINCT FROM NULL"),
            vec![Some(false), Some(true)]
        );
        assert_eq!(eval_bool("x IN (1, NULL)"), vec![None, Some(true)]);
        assert_eq!(eval_bool("x IN (2, NULL)"), vec![None, None]);
    }
}
