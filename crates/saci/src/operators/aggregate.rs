//! Shared aggregation specifications and vectorized Arrow evaluation kernels
//! used by both [`GroupBySystem`](super::GroupBySystem) and
//! [`WindowedSystem`](crate::windows::WindowedSystem).

use std::collections::HashSet;
use std::sync::Arc;

use arrow_arith::aggregate::{max, min, sum};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch, StringArray, UInt32Array,
};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take;

use crate::error::{SaciError, SaciResult};

/// Built-in aggregate operations supported by [`AggregateSpec`].
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateOp {
    /// Arithmetic sum of non-null numeric values (`Float64`).
    Sum,
    /// Minimum non-null numeric value (`Float64`).
    Min,
    /// Maximum non-null numeric value (`Float64`).
    Max,
    /// Count of non-null values in the target field (`Int64`).
    Count,
    /// Count of all rows in the group including nulls (`Int64`).
    CountAll,
    /// Arithmetic mean of non-null numeric values (`Float64`).
    Mean,
    /// Exact count of distinct non-null values in the group (`Int64`).
    CountDistinct,
    /// Sample variance of non-null numeric values (`Float64`).
    Variance,
    /// Sample standard deviation of non-null numeric values (`Float64`).
    StdDev,
    /// First non-null value in group order (preserves input column type).
    First,
    /// Last non-null value in group order (preserves input column type).
    Last,
    /// Approximate quantile in `[0.0, 1.0]` over non-null numeric values (`Float64`).
    ApproxPercentile(f64),
    /// Concatenates non-null values as strings separated by `separator` (`Utf8`).
    StringAgg {
        /// Delimiter inserted between non-null values.
        separator: String,
    },
    /// Collects all values in the group into a single-element `ListArray`.
    ListAgg,
}

/// A named aggregate expression operating on a column of an Arrow `RecordBatch`.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateSpec {
    /// Input column name (ignored when `op` is [`AggregateOp::CountAll`]).
    pub field: String,
    /// Aggregate operation to evaluate.
    pub op: AggregateOp,
    /// Optional explicit output column name. Defaults to `<op>_<field>`.
    pub alias: Option<String>,
}

impl AggregateSpec {
    /// Create a new aggregate specification.
    pub fn new(field: impl Into<String>, op: AggregateOp) -> Self {
        Self {
            field: field.into(),
            op,
            alias: None,
        }
    }

    /// Arithmetic sum of `field`.
    pub fn sum(field: impl Into<String>) -> Self {
        Self::new(field, AggregateOp::Sum)
    }

    /// Minimum value of `field`.
    pub fn min(field: impl Into<String>) -> Self {
        Self::new(field, AggregateOp::Min)
    }

    /// Maximum value of `field`.
    pub fn max(field: impl Into<String>) -> Self {
        Self::new(field, AggregateOp::Max)
    }

    /// Count of non-null values in `field`.
    pub fn count(field: impl Into<String>) -> Self {
        Self::new(field, AggregateOp::Count)
    }

    /// Total row count (`COUNT(*)`).
    pub fn count_all() -> Self {
        Self::new("*", AggregateOp::CountAll)
    }

    /// Mean of non-null values in `field`.
    pub fn mean(field: impl Into<String>) -> Self {
        Self::new(field, AggregateOp::Mean)
    }

    /// Distinct non-null value count in `field`.
    pub fn count_distinct(field: impl Into<String>) -> Self {
        Self::new(field, AggregateOp::CountDistinct)
    }

    /// Sample variance of non-null values in `field`.
    pub fn variance(field: impl Into<String>) -> Self {
        Self::new(field, AggregateOp::Variance)
    }

    /// Sample standard deviation of non-null values in `field`.
    pub fn stddev(field: impl Into<String>) -> Self {
        Self::new(field, AggregateOp::StdDev)
    }

    /// First non-null value in `field`.
    pub fn first(field: impl Into<String>) -> Self {
        Self::new(field, AggregateOp::First)
    }

    /// Last non-null value in `field`.
    pub fn last(field: impl Into<String>) -> Self {
        Self::new(field, AggregateOp::Last)
    }

    /// Approximate percentile (`0.0 <= quantile <= 1.0`) of non-null values in `field`.
    pub fn approx_percentile(field: impl Into<String>, quantile: f64) -> Self {
        Self::new(field, AggregateOp::ApproxPercentile(quantile))
    }

    /// Concatenate non-null values of `field` separated by `separator`.
    pub fn string_agg(field: impl Into<String>, separator: impl Into<String>) -> Self {
        Self::new(
            field,
            AggregateOp::StringAgg {
                separator: separator.into(),
            },
        )
    }

    /// Collect values of `field` into a list column.
    pub fn list_agg(field: impl Into<String>) -> Self {
        Self::new(field, AggregateOp::ListAgg)
    }

    /// Override the output column name.
    pub fn alias(mut self, name: impl Into<String>) -> Self {
        self.alias = Some(name.into());
        self
    }

    /// Validate parameters such as percentile bounds.
    pub fn validate(&self) -> SaciResult<()> {
        if let AggregateOp::ApproxPercentile(q) = self.op
            && (!q.is_finite() || !(0.0..=1.0).contains(&q))
        {
            return Err(SaciError::configuration(format!(
                "approx_percentile quantile must be in [0.0, 1.0], got {q}"
            )));
        }
        if !matches!(self.op, AggregateOp::CountAll) && self.field.is_empty() {
            return Err(SaciError::configuration(
                "aggregate field name must not be empty",
            ));
        }
        Ok(())
    }

    /// Output column name for this aggregate.
    pub fn output_name(&self) -> String {
        if let Some(alias) = &self.alias {
            return alias.clone();
        }
        match &self.op {
            AggregateOp::Sum => format!("sum_{}", self.field),
            AggregateOp::Min => format!("min_{}", self.field),
            AggregateOp::Max => format!("max_{}", self.field),
            AggregateOp::Count => format!("count_{}", self.field),
            AggregateOp::CountAll => "count".to_string(),
            AggregateOp::Mean => format!("mean_{}", self.field),
            AggregateOp::CountDistinct => format!("count_distinct_{}", self.field),
            AggregateOp::Variance => format!("variance_{}", self.field),
            AggregateOp::StdDev => format!("stddev_{}", self.field),
            AggregateOp::First => format!("first_{}", self.field),
            AggregateOp::Last => format!("last_{}", self.field),
            AggregateOp::ApproxPercentile(q) => {
                let pct = (q * 100.0).round() as i64;
                format!("p{pct}_{}", self.field)
            }
            AggregateOp::StringAgg { .. } => format!("string_agg_{}", self.field),
            AggregateOp::ListAgg => format!("list_agg_{}", self.field),
        }
    }
}

/// Derive the Arrow output [`Field`] for `spec` against `input_schema`.
pub(crate) fn aggregate_output_field(
    spec: &AggregateSpec,
    input_schema: &Schema,
) -> SaciResult<Field> {
    spec.validate()?;
    let name = spec.output_name();
    match &spec.op {
        AggregateOp::CountAll => Ok(Field::new(name, DataType::Int64, false)),
        AggregateOp::Count | AggregateOp::CountDistinct => {
            let _ = input_schema
                .field_with_name(&spec.field)
                .map_err(|_| SaciError::field_not_found("aggregate", &spec.field))?;
            Ok(Field::new(name, DataType::Int64, false))
        }
        AggregateOp::Sum
        | AggregateOp::Min
        | AggregateOp::Max
        | AggregateOp::Mean
        | AggregateOp::Variance
        | AggregateOp::StdDev
        | AggregateOp::ApproxPercentile(_) => {
            let field = input_schema
                .field_with_name(&spec.field)
                .map_err(|_| SaciError::field_not_found("aggregate", &spec.field))?;
            ensure_numeric_type(field.data_type(), &spec.field)?;
            Ok(Field::new(name, DataType::Float64, true))
        }
        AggregateOp::First | AggregateOp::Last => {
            let field = input_schema
                .field_with_name(&spec.field)
                .map_err(|_| SaciError::field_not_found("aggregate", &spec.field))?;
            Ok(Field::new(name, field.data_type().clone(), true))
        }
        AggregateOp::StringAgg { .. } => {
            let _ = input_schema
                .field_with_name(&spec.field)
                .map_err(|_| SaciError::field_not_found("aggregate", &spec.field))?;
            Ok(Field::new(name, DataType::Utf8, true))
        }
        AggregateOp::ListAgg => {
            let field = input_schema
                .field_with_name(&spec.field)
                .map_err(|_| SaciError::field_not_found("aggregate", &spec.field))?;
            Ok(Field::new(
                name,
                DataType::List(Arc::new(Field::new(
                    "item",
                    field.data_type().clone(),
                    field.is_nullable(),
                ))),
                false,
            ))
        }
    }
}

fn ensure_numeric_type(dt: &DataType, field: &str) -> SaciResult<()> {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64 => Ok(()),
        other => Err(SaciError::generic(format!(
            "aggregate on field '{field}' requires a numeric column, got {other:?}"
        ))),
    }
}

pub(crate) fn cast_to_f64(col: &ArrayRef, field: &str) -> SaciResult<Float64Array> {
    ensure_numeric_type(col.data_type(), field)?;
    if col.data_type() == &DataType::Float64 {
        return col
            .as_any()
            .downcast_ref::<Float64Array>()
            .cloned()
            .ok_or_else(|| SaciError::generic("downcast Float64Array failed"));
    }
    let casted = cast(col.as_ref(), &DataType::Float64)
        .map_err(|e| SaciError::generic(format!("cast field '{field}' to Float64: {e}")))?;
    casted
        .as_any()
        .downcast_ref::<Float64Array>()
        .cloned()
        .ok_or_else(|| SaciError::generic("downcast casted Float64Array failed"))
}

/// Evaluate `spec` over all rows in `batch`, returning a single-element [`ArrayRef`].
pub(crate) fn evaluate_aggregate_spec(
    spec: &AggregateSpec,
    batch: &RecordBatch,
) -> SaciResult<ArrayRef> {
    spec.validate()?;
    match &spec.op {
        AggregateOp::CountAll => {
            Ok(Arc::new(Int64Array::from(vec![batch.num_rows() as i64])) as ArrayRef)
        }
        op => {
            let idx = batch
                .schema_ref()
                .index_of(&spec.field)
                .map_err(|_| SaciError::field_not_found("aggregate", &spec.field))?;
            let col = batch.column(idx);
            evaluate_column_aggregate(op, col, &spec.field)
        }
    }
}

/// Evaluate an [`AggregateOp`] on a single column slice `col`, returning a 1-element [`ArrayRef`].
pub(crate) fn evaluate_column_aggregate(
    op: &AggregateOp,
    col: &ArrayRef,
    field: &str,
) -> SaciResult<ArrayRef> {
    match op {
        AggregateOp::CountAll => Ok(Arc::new(Int64Array::from(vec![col.len() as i64])) as ArrayRef),
        AggregateOp::Count => {
            let non_null = (col.len() - col.null_count()) as i64;
            Ok(Arc::new(Int64Array::from(vec![non_null])) as ArrayRef)
        }
        AggregateOp::CountDistinct => {
            let count = count_distinct_non_null(col)?;
            Ok(Arc::new(Int64Array::from(vec![count])) as ArrayRef)
        }
        AggregateOp::Sum => {
            let arr = cast_to_f64(col, field)?;
            let val = sum(&arr).unwrap_or(0.0);
            Ok(Arc::new(Float64Array::from(vec![val])) as ArrayRef)
        }
        AggregateOp::Min => {
            let arr = cast_to_f64(col, field)?;
            let val = min(&arr).unwrap_or(f64::INFINITY);
            Ok(Arc::new(Float64Array::from(vec![val])) as ArrayRef)
        }
        AggregateOp::Max => {
            let arr = cast_to_f64(col, field)?;
            let val = max(&arr).unwrap_or(f64::NEG_INFINITY);
            Ok(Arc::new(Float64Array::from(vec![val])) as ArrayRef)
        }
        AggregateOp::Mean => {
            let arr = cast_to_f64(col, field)?;
            let non_null = arr.len() - arr.null_count();
            let val = if non_null == 0 {
                f64::NAN
            } else {
                sum(&arr).unwrap_or(0.0) / non_null as f64
            };
            Ok(Arc::new(Float64Array::from(vec![val])) as ArrayRef)
        }
        AggregateOp::Variance => {
            let arr = cast_to_f64(col, field)?;
            Ok(Arc::new(Float64Array::from(vec![welford_variance(&arr)])) as ArrayRef)
        }
        AggregateOp::StdDev => {
            let arr = cast_to_f64(col, field)?;
            Ok(Arc::new(Float64Array::from(vec![welford_variance(&arr).sqrt()])) as ArrayRef)
        }
        AggregateOp::ApproxPercentile(q) => {
            let arr = cast_to_f64(col, field)?;
            Ok(Arc::new(Float64Array::from(vec![percentile(&arr, *q)])) as ArrayRef)
        }
        AggregateOp::First => pick_non_null(col, true),
        AggregateOp::Last => pick_non_null(col, false),
        AggregateOp::StringAgg { separator } => {
            let str_col = cast(col.as_ref(), &DataType::Utf8)
                .map_err(|e| SaciError::generic(format!("string_agg cast to Utf8: {e}")))?;
            let str_arr = str_col
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| SaciError::generic("string_agg downcast StringArray failed"))?;
            let mut joined = String::new();
            let mut any = false;
            for v in str_arr.iter().flatten() {
                if any {
                    joined.push_str(separator);
                }
                joined.push_str(v);
                any = true;
            }
            let out = if any {
                StringArray::from(vec![Some(joined.as_str())])
            } else {
                StringArray::from(vec![None::<&str>])
            };
            Ok(Arc::new(out) as ArrayRef)
        }
        AggregateOp::ListAgg => {
            let item_field = Arc::new(Field::new("item", col.data_type().clone(), true));
            let offsets = OffsetBuffer::from_lengths([col.len()]);
            let nulls = NullBufferBuilder::new(1).finish();
            let list = ListArray::try_new(item_field, offsets, col.clone(), nulls)
                .map_err(|e| SaciError::generic(format!("list_agg ListArray error: {e}")))?;
            Ok(Arc::new(list) as ArrayRef)
        }
    }
}

pub(crate) fn count_distinct_non_null(col: &ArrayRef) -> SaciResult<i64> {
    let casted = cast(col.as_ref(), &DataType::Utf8)
        .map_err(|e| SaciError::generic(format!("count_distinct cast to Utf8: {e}")))?;
    let arr = casted
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| SaciError::generic("count_distinct downcast StringArray failed"))?;
    let mut set = HashSet::new();
    for v in arr.iter().flatten() {
        set.insert(v);
    }
    Ok(set.len() as i64)
}

pub(crate) fn welford_variance(arr: &Float64Array) -> f64 {
    let mut count = 0usize;
    let mut mean = 0.0f64;
    let mut m2 = 0.0f64;
    for v in arr.iter().flatten() {
        count += 1;
        let delta = v - mean;
        mean += delta / count as f64;
        let delta2 = v - mean;
        m2 += delta * delta2;
    }
    if count == 0 {
        f64::NAN
    } else if count == 1 {
        0.0
    } else {
        m2 / (count - 1) as f64
    }
}

fn percentile(arr: &Float64Array, q: f64) -> f64 {
    let mut vals: Vec<f64> = arr.iter().flatten().filter(|v| !v.is_nan()).collect();
    if vals.is_empty() {
        return f64::NAN;
    }
    vals.sort_by(|a, b| a.total_cmp(b));
    if vals.len() == 1 {
        return vals[0];
    }
    let pos = q * (vals.len() - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    let frac = pos - lo as f64;
    vals[lo] + (vals[hi] - vals[lo]) * frac
}

fn pick_non_null(col: &ArrayRef, first: bool) -> SaciResult<ArrayRef> {
    let len = col.len();
    let idx = if first {
        (0..len).find(|&i| col.is_valid(i))
    } else {
        (0..len).rev().find(|&i| col.is_valid(i))
    };
    let take_idx = UInt32Array::from(vec![idx.map(|i| i as u32)]);
    take(col.as_ref(), &take_idx, None)
        .map_err(|e| SaciError::generic(format!("first/last take error: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;

    #[test]
    fn variance_stddev_percentile_string_and_list_agg_work() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("val", DataType::Float64, true),
            Field::new("tag", DataType::Utf8, true),
            Field::new("n", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(vec![
                    Some(10.0),
                    Some(20.0),
                    None,
                    Some(30.0),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("a"),
                    None,
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 2, 3])) as ArrayRef,
            ],
        )
        .unwrap();

        let var = evaluate_aggregate_spec(&AggregateSpec::variance("val"), &batch).unwrap();
        let var_v = var
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((var_v - 100.0).abs() < 1e-9, "got {var_v}");

        let sd = evaluate_aggregate_spec(&AggregateSpec::stddev("val"), &batch).unwrap();
        let sd_v = sd.as_any().downcast_ref::<Float64Array>().unwrap().value(0);
        assert!((sd_v - 10.0).abs() < 1e-9, "got {sd_v}");

        let p50 =
            evaluate_aggregate_spec(&AggregateSpec::approx_percentile("val", 0.5), &batch).unwrap();
        let p50_v = p50
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert!((p50_v - 20.0).abs() < 1e-9, "got {p50_v}");

        let cd = evaluate_aggregate_spec(&AggregateSpec::count_distinct("tag"), &batch).unwrap();
        assert_eq!(
            cd.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            2
        );

        let sa = evaluate_aggregate_spec(&AggregateSpec::string_agg("tag", ","), &batch).unwrap();
        assert_eq!(
            sa.as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "a,b,a"
        );

        let first = evaluate_aggregate_spec(&AggregateSpec::first("tag"), &batch).unwrap();
        assert_eq!(
            first
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a"
        );
        let last = evaluate_aggregate_spec(&AggregateSpec::last("tag"), &batch).unwrap();
        assert_eq!(
            last.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "a"
        );

        let la = evaluate_aggregate_spec(&AggregateSpec::list_agg("n"), &batch).unwrap();
        let list_arr = la.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_arr.len(), 1);
        assert_eq!(list_arr.value(0).len(), 4);
    }

    #[test]
    fn invalid_percentile_rejected() {
        let err = AggregateSpec::approx_percentile("x", 1.5)
            .validate()
            .unwrap_err();
        assert_eq!(err.category(), "configuration");
    }
}
