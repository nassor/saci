//! Non-windowed relational `GROUP BY` aggregation operator.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;
use arrow_select::take::take;
use async_trait::async_trait;

use crate::dataset::{Dataset, intern_component_name};
use crate::error::{SaciError, SaciResult};
use crate::system::{System, SystemMeta};

use super::aggregate::{AggregateSpec, aggregate_output_field, evaluate_aggregate_spec};
use super::hash::compute_row_keys;

/// Groups rows of `source_component` by `key_fields`, evaluates one or more
/// [`AggregateSpec`]s per group, and writes the aggregated rows to
/// `target_component`.
///
/// When `key_fields` is empty, all rows collapse into a single global group (or
/// zero rows when the input batch is empty).
#[derive(Debug, Clone)]
pub struct GroupBySystem {
    source_component: &'static str,
    target_component: &'static str,
    key_fields: Vec<String>,
    aggregates: Vec<AggregateSpec>,
}

impl GroupBySystem {
    /// Group `source_component` by `key_fields` and write results to
    /// `target_component`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `aggregates` is empty or an
    /// aggregate spec fails validation (for example an out-of-range
    /// percentile).
    pub fn new<I, S>(
        source_component: &str,
        target_component: &str,
        key_fields: I,
        aggregates: Vec<AggregateSpec>,
    ) -> SaciResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if aggregates.is_empty() {
            return Err(SaciError::configuration(
                "GroupBySystem requires at least one AggregateSpec",
            ));
        }
        for spec in &aggregates {
            spec.validate()?;
        }
        Ok(Self {
            source_component: intern_component_name(source_component),
            target_component: intern_component_name(target_component),
            key_fields: key_fields
                .into_iter()
                .map(|s| s.as_ref().to_string())
                .collect(),
            aggregates,
        })
    }

    /// Group `component` in place, replacing its own batch with the aggregated
    /// rows.
    ///
    /// # Errors
    ///
    /// Same as [`Self::new`].
    pub fn in_place<I, S>(
        component: &str,
        key_fields: I,
        aggregates: Vec<AggregateSpec>,
    ) -> SaciResult<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        Self::new(component, component, key_fields, aggregates)
    }

    /// Derive the output Arrow [`Schema`] given `input_schema`.
    ///
    /// # Errors
    ///
    /// Returns an error if any key or aggregate field is missing from
    /// `input_schema` or has an incompatible type.
    pub fn output_schema(&self, input_schema: &Schema) -> SaciResult<Arc<Schema>> {
        let mut fields = Vec::with_capacity(self.key_fields.len() + self.aggregates.len());
        for key in &self.key_fields {
            let f = input_schema
                .field_with_name(key)
                .map_err(|_| SaciError::field_not_found(self.source_component, key))?;
            fields.push(f.clone());
        }
        for agg in &self.aggregates {
            fields.push(aggregate_output_field(agg, input_schema)?);
        }
        Ok(Arc::new(Schema::new(fields)))
    }

    /// Execute grouping against `batch` and return the aggregated [`RecordBatch`].
    pub fn execute_batch(&self, batch: &RecordBatch) -> SaciResult<RecordBatch> {
        let out_schema = self.output_schema(batch.schema_ref())?;
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(out_schema));
        }

        let groups = build_groups(batch, &self.key_fields)?;
        let mut group_batches = Vec::with_capacity(groups.len());

        for indices in groups {
            let first_idx = UInt32Array::from(vec![indices[0]]);
            let mut cols: Vec<ArrayRef> =
                Vec::with_capacity(self.key_fields.len() + self.aggregates.len());

            for key in &self.key_fields {
                let col_idx = batch
                    .schema_ref()
                    .index_of(key)
                    .map_err(|_| SaciError::field_not_found(self.source_component, key))?;
                let taken = take(batch.column(col_idx).as_ref(), &first_idx, None)
                    .map_err(|e| SaciError::generic(format!("GroupBySystem key take: {e}")))?;
                cols.push(taken);
            }

            let group_slice = if indices.len() == batch.num_rows() {
                batch.clone()
            } else {
                let idx_arr = UInt32Array::from(indices);
                let taken_cols: SaciResult<Vec<ArrayRef>> = batch
                    .columns()
                    .iter()
                    .map(|c| {
                        take(c.as_ref(), &idx_arr, None).map_err(|e| {
                            SaciError::generic(format!("GroupBySystem group take: {e}"))
                        })
                    })
                    .collect();
                RecordBatch::try_new(batch.schema(), taken_cols?)
                    .map_err(|e| SaciError::generic(format!("GroupBySystem group batch: {e}")))?
            };

            for spec in &self.aggregates {
                cols.push(evaluate_aggregate_spec(spec, &group_slice)?);
            }

            let row_batch = RecordBatch::try_new(out_schema.clone(), cols)
                .map_err(|e| SaciError::generic(format!("GroupBySystem output batch: {e}")))?;
            group_batches.push(row_batch);
        }

        concat_batches(&out_schema, group_batches.iter())
            .map_err(|e| SaciError::generic(format!("GroupBySystem concat: {e}")))
    }

    fn run_impl(&self, data: &mut Dataset) -> SaciResult<()> {
        let input = data.batch_named(self.source_component)?.clone();
        let out_batch = self.execute_batch(&input)?;
        data.put_batch_named(self.target_component, out_batch)
    }
}

fn build_groups(batch: &RecordBatch, key_fields: &[String]) -> SaciResult<Vec<Vec<u32>>> {
    let n = batch.num_rows();
    if key_fields.is_empty() {
        return Ok(vec![(0..n as u32).collect()]);
    }

    let key_cols: SaciResult<Vec<&ArrayRef>> = key_fields
        .iter()
        .map(|k| {
            let idx = batch
                .schema_ref()
                .index_of(k)
                .map_err(|_| SaciError::field_not_found("GroupBySystem", k))?;
            Ok(batch.column(idx))
        })
        .collect();
    let key_cols = key_cols?;
    let row_keys = compute_row_keys(&key_cols)?;

    let mut groups: Vec<Vec<u32>> = Vec::new();
    let mut map: HashMap<String, usize> = HashMap::new();

    for (row_idx, (_hash, key)) in row_keys.into_iter().enumerate() {
        if let Some(&group_idx) = map.get(&key) {
            groups[group_idx].push(row_idx as u32);
        } else {
            let group_idx = groups.len();
            map.insert(key, group_idx);
            groups.push(vec![row_idx as u32]);
        }
    }
    Ok(groups)
}

#[async_trait]
impl System for GroupBySystem {
    fn meta(&self) -> SystemMeta {
        let mut meta = SystemMeta::new("group_by").reads_all_named(self.source_component);
        meta = meta.writes_all_named(self.target_component);
        meta
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
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field};

    fn sample_dataset() -> Dataset {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("customer", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["us", "eu", "us", "eu", "us"])) as ArrayRef,
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0])) as ArrayRef,
                Arc::new(StringArray::from(vec!["c1", "c2", "c1", "c3", "c4"])) as ArrayRef,
            ],
        )
        .unwrap();

        let mut ds = Dataset::new();
        ds.register_schema("Order", schema).unwrap();
        ds.append_record_batch("Order", batch).unwrap();
        ds
    }

    #[tokio::test]
    async fn group_by_computes_multiple_aggregates() {
        let mut ds = sample_dataset();
        let sys = GroupBySystem::new(
            "Order",
            "RegionSummary",
            ["region"],
            vec![
                AggregateSpec::sum("amount").alias("total_amount"),
                AggregateSpec::count_distinct("customer").alias("unique_customers"),
                AggregateSpec::count_all().alias("order_count"),
            ],
        )
        .unwrap();

        let _out_schema = sys
            .output_schema(&ds.schemas().get("Order").unwrap())
            .unwrap();

        sys.run(&mut ds).await.unwrap();

        let out = ds.batch_named("RegionSummary").unwrap();
        assert_eq!(out.num_rows(), 2);

        let regions = out
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let totals = out
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let uniq = out.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let counts = out.column(3).as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(regions.value(0), "us");
        assert!((totals.value(0) - 90.0).abs() < 1e-9);
        assert_eq!(uniq.value(0), 2);
        assert_eq!(counts.value(0), 3);

        assert_eq!(regions.value(1), "eu");
        assert!((totals.value(1) - 60.0).abs() < 1e-9);
        assert_eq!(uniq.value(1), 2);
        assert_eq!(counts.value(1), 2);
    }

    #[tokio::test]
    async fn global_group_by_empty_batch_returns_zero_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        let mut ds = Dataset::new();
        ds.register_schema("Order", schema.clone()).unwrap();
        let sys = GroupBySystem::new(
            "Order",
            "Out",
            Vec::<&str>::new(),
            vec![AggregateSpec::sum("amount")],
        )
        .unwrap();
        ds.register_schema("Out", sys.output_schema(&schema).unwrap())
            .unwrap();
        sys.run(&mut ds).await.unwrap();
        assert_eq!(ds.batch_named("Out").unwrap().num_rows(), 0);
    }
}
