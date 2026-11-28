//! Parameterized ad-hoc backfills and time-window filtering.
//!
//! [`parameterize_workflow`] clones a declared [`WorkflowSpec`], forces its
//! run mode to [`RunMode::Once`], substitutes `${start_date}`, `${end_date}`,
//! and arbitrary `${KEY}` parameters into connector/codec/operator configs,
//! and injects those parameters into processor `config` tables.
//!
//! When a `time_field` is supplied (or inherited from a downstream windowed
//! processor) alongside `start_date` / `end_date`, [`apply_time_window`] wraps
//! matching sources in [`TimeWindowSource`] to filter incoming batches to
//! `[start_date, end_date]` inclusive.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Int64Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt64Type,
};
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{DataType, Schema, TimeUnit};
use async_trait::async_trait;

use crate::config::{ConfigValue, substitute_vars};
use crate::error::{SaciError, SaciResult};
use crate::inspector_wire::BackfillRequest;
use crate::io::Source;

use super::builder::{BuiltNodeKind, BuiltWorkflow};
use super::config::{RunMode, WorkflowSpec, cron::parse_timestamp_utc_ms};

/// Substitute `vars` recursively through a [`ConfigValue`] tree.
fn substitute_config_value(
    value: &mut ConfigValue,
    vars: &HashMap<String, String>,
) -> SaciResult<()> {
    match value {
        ConfigValue::String(s) => {
            *s = substitute_vars(s, vars)?;
        }
        ConfigValue::Array(items) => {
            for item in items {
                substitute_config_value(item, vars)?;
            }
        }
        ConfigValue::Object(map) => {
            for val in map.values_mut() {
                substitute_config_value(val, vars)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Clone `spec` as a one-shot backfill workflow with `req.params`, `start_date`,
/// and `end_date` substituted into every node config.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when variable substitution fails or
/// a date bound cannot be parsed.
pub(crate) fn parameterize_workflow(
    spec: &WorkflowSpec,
    req: &BackfillRequest,
) -> SaciResult<WorkflowSpec> {
    if let Some(start) = &req.start_date {
        parse_timestamp_utc_ms(start, false).map_err(SaciError::configuration)?;
    }
    if let Some(end) = &req.end_date {
        parse_timestamp_utc_ms(end, true).map_err(SaciError::configuration)?;
    }
    if let (Some(start), Some(end)) = (&req.start_date, &req.end_date) {
        let s_ms = parse_timestamp_utc_ms(start, false).map_err(SaciError::configuration)?;
        let e_ms = parse_timestamp_utc_ms(end, true).map_err(SaciError::configuration)?;
        if s_ms > e_ms {
            return Err(SaciError::configuration(format!(
                "backfill: start_date '{start}' is after end_date '{end}'"
            )));
        }
    }

    let mut vars: HashMap<String, String> = req
        .params
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if let Some(start) = &req.start_date {
        vars.insert("start_date".to_string(), start.clone());
    }
    if let Some(end) = &req.end_date {
        vars.insert("end_date".to_string(), end.clone());
    }

    let mut out = spec.clone();
    out.run = Some(RunMode::Once);

    for codec in &mut out.codecs {
        substitute_config_value(&mut codec.config, &vars)?;
    }
    for source in &mut out.sources {
        substitute_config_value(&mut source.config, &vars)?;
    }
    for sink in &mut out.sinks {
        substitute_config_value(&mut sink.config, &vars)?;
    }
    for op in &mut out.operator {
        substitute_config_value(&mut op.config, &vars)?;
    }
    for wasm in &mut out.wasm {
        for v in wasm.config.values_mut() {
            *v = substitute_vars(v, &vars)?;
        }
        for (k, v) in &vars {
            wasm.config.insert(k.clone(), v.clone());
        }
    }
    for plugin in &mut out.plugin {
        for v in plugin.config.values_mut() {
            *v = substitute_vars(v, &vars)?;
        }
        for (k, v) in &vars {
            plugin.config.insert(k.clone(), v.clone());
        }
    }

    Ok(out)
}

/// Wrap sources in [`TimeWindowSource`] when `req` specifies `[start_date, end_date]`
/// and a `time_field` (explicitly or via a processor's `window.time_field`).
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] if `req.time_field` is explicitly named
/// and a source's schema does not contain that column.
pub(crate) fn apply_time_window(
    built: &mut BuiltWorkflow,
    req: &BackfillRequest,
) -> SaciResult<()> {
    if req.start_date.is_none() && req.end_date.is_none() {
        return Ok(());
    }
    let start_ms = req
        .start_date
        .as_deref()
        .map(|s| parse_timestamp_utc_ms(s, false))
        .transpose()
        .map_err(SaciError::configuration)?;
    let end_ms = req
        .end_date
        .as_deref()
        .map(|e| parse_timestamp_utc_ms(e, true))
        .transpose()
        .map_err(SaciError::configuration)?;

    let fallback_time_field = built
        .nodes
        .iter()
        .find_map(|n| n.window.as_ref().map(|w| w.time_field.clone()));
    let explicit = req.time_field.is_some();
    let Some(time_field) = req.time_field.clone().or(fallback_time_field) else {
        return Ok(());
    };

    for node in &mut built.nodes {
        if let BuiltNodeKind::Source(src) = &mut node.kind {
            let schema = src.schema();
            if schema.index_of(&time_field).is_err() {
                if explicit {
                    return Err(SaciError::configuration(format!(
                        "backfill: source '{}' schema does not contain time_field '{time_field}'",
                        node.id
                    )));
                }
                continue;
            }
            let placeholder: Box<dyn Source> = Box::new(DummySource(Arc::clone(&schema)));
            let inner = std::mem::replace(src, placeholder);
            *src = Box::new(TimeWindowSource {
                inner,
                time_field: time_field.clone(),
                start_ms,
                end_ms,
            });
        }
    }
    Ok(())
}

struct DummySource(Arc<Schema>);

#[async_trait]
impl Source for DummySource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.0)
    }
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        Ok(None)
    }
}

/// Source adapter that filters drained [`RecordBatch`]es to `[start_ms, end_ms]`
/// inclusive on `time_field`.
struct TimeWindowSource {
    inner: Box<dyn Source>,
    time_field: String,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
}

#[async_trait]
impl Source for TimeWindowSource {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        loop {
            let Some(batch) = self.inner.next_batch().await? else {
                return Ok(None);
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let filtered =
                filter_batch_by_window(&batch, &self.time_field, self.start_ms, self.end_ms)?;
            if filtered.num_rows() > 0 {
                return Ok(Some(filtered));
            }
        }
    }

    fn estimated_rows(&self) -> Option<usize> {
        self.inner.estimated_rows()
    }

    fn request_batch_rows(&mut self, rows: usize) {
        self.inner.request_batch_rows(rows);
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        self.inner.finish().await
    }
}

fn filter_batch_by_window(
    batch: &RecordBatch,
    time_field: &str,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
) -> SaciResult<RecordBatch> {
    let col = batch.column_by_name(time_field).ok_or_else(|| {
        SaciError::generic(format!("backfill: missing time_field '{time_field}'"))
    })?;

    let in_window = |ts_ms: i64| -> bool {
        start_ms.is_none_or(|s| ts_ms >= s) && end_ms.is_none_or(|e| ts_ms <= e)
    };

    let mask: BooleanArray = match col.data_type() {
        DataType::Int64 => {
            let arr = col.as_primitive::<Int64Type>();
            (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| in_window(arr.value(i))))
                .collect()
        }
        DataType::UInt64 => {
            let arr = col.as_primitive::<UInt64Type>();
            (0..arr.len())
                .map(|i| {
                    (!arr.is_null(i)).then(|| i64::try_from(arr.value(i)).is_ok_and(in_window))
                })
                .collect()
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = col.as_primitive::<TimestampSecondType>();
            (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| in_window(arr.value(i).saturating_mul(1_000))))
                .collect()
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = col.as_primitive::<TimestampMillisecondType>();
            (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| in_window(arr.value(i))))
                .collect()
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = col.as_primitive::<TimestampMicrosecondType>();
            (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| in_window(arr.value(i) / 1_000)))
                .collect()
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col.as_primitive::<TimestampNanosecondType>();
            (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| in_window(arr.value(i) / 1_000_000)))
                .collect()
        }
        DataType::Date32 => {
            let arr = col.as_primitive::<Date32Type>();
            (0..arr.len())
                .map(|i| {
                    (!arr.is_null(i))
                        .then(|| in_window(i64::from(arr.value(i)).saturating_mul(86_400_000)))
                })
                .collect()
        }
        DataType::Date64 => {
            let arr = col.as_primitive::<Date64Type>();
            (0..arr.len())
                .map(|i| (!arr.is_null(i)).then(|| in_window(arr.value(i))))
                .collect()
        }
        DataType::Utf8 => {
            let arr = col.as_string::<i32>();
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(parse_timestamp_utc_ms(arr.value(i), false).is_ok_and(in_window))
                    }
                })
                .collect()
        }
        DataType::LargeUtf8 => {
            let arr = col.as_string::<i64>();
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(parse_timestamp_utc_ms(arr.value(i), false).is_ok_and(in_window))
                    }
                })
                .collect()
        }
        DataType::Utf8View => {
            let arr = col.as_string_view();
            (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(parse_timestamp_utc_ms(arr.value(i), false).is_ok_and(in_window))
                    }
                })
                .collect()
        }
        other => {
            return Err(SaciError::configuration(format!(
                "backfill: time_field '{time_field}' has unsupported type {other}"
            )));
        }
    };

    arrow_select::filter::filter_record_batch(batch, &mask)
        .map_err(|e| SaciError::generic(format!("backfill: filtering batch: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::Field;
    use std::collections::BTreeMap;

    #[test]
    fn parameterize_workflow_substitutes_dates_and_params_and_forces_once() {
        let mut wf = WorkflowSpec::new("etl");
        wf.run = Some(RunMode::Continuous);
        wf.sources.push(crate::service::config::SourceSpec {
            id: "s".to_string(),
            name: None,
            connector: "file".to_string(),
            codec: None,
            component: "Order".to_string(),
            retry: Default::default(),
            heal: None,
            flow_control: None,
            decrypt: None,
            schema_evolution: None,
            on_corrupt: None,
            config: serde_json::json!({
                "path": "/data/${start_date}/${region:-us-east}.csv"
            }),
        });
        wf.wasm.push(crate::service::config::WasmSpec {
            id: "p".to_string(),
            name: None,
            path: Some("p.wasm".to_string()),
            sha3_256: None,
            config: HashMap::from([(
                "window".to_string(),
                "${start_date}..${end_date}".to_string(),
            )]),
            window: None,
        });

        let mut params = BTreeMap::new();
        params.insert("region".to_string(), "eu-west".to_string());
        let req = BackfillRequest {
            start_date: Some("2026-01-01".to_string()),
            end_date: Some("2026-01-07".to_string()),
            time_field: None,
            params,
            reset_state: false,
        };

        let out = parameterize_workflow(&wf, &req).expect("parameterize");
        assert_eq!(out.run, Some(RunMode::Once));
        assert_eq!(
            out.sources[0].config["path"],
            "/data/2026-01-01/eu-west.csv"
        );
        assert_eq!(out.wasm[0].config["window"], "2026-01-01..2026-01-07");
        assert_eq!(out.wasm[0].config["region"], "eu-west");
    }

    #[test]
    fn filter_batch_by_window_filters_int64_and_utf8_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
                Arc::new(StringArray::from(vec![
                    "2026-01-01",
                    "2026-01-05",
                    "2026-01-10",
                    "2026-01-20",
                ])),
            ],
        )
        .unwrap();

        let filtered = filter_batch_by_window(&batch, "ts", Some(150), Some(350)).unwrap();
        assert_eq!(filtered.num_rows(), 2);

        let start = parse_timestamp_utc_ms("2026-01-02", false).unwrap();
        let end = parse_timestamp_utc_ms("2026-01-10", true).unwrap();
        let by_str = filter_batch_by_window(&batch, "label", Some(start), Some(end)).unwrap();
        assert_eq!(by_str.num_rows(), 2);
    }

    #[test]
    fn filter_batch_by_window_supports_all_temporal_types_and_nulls() {
        use arrow_array::{
            Date32Array, Date64Array, StringViewArray, TimestampMicrosecondArray,
            TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt64Array,
        };

        let s_sec = 1_700_000_000i64;
        let in_sec = 1_700_000_100i64;
        let out_sec = 1_700_000_500i64;
        let start_ms = Some(in_sec * 1_000);
        let end_ms = Some(in_sec * 1_000 + 500);

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "ts_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("d64", DataType::Date64, true),
            Field::new("u64", DataType::UInt64, true),
            Field::new("sv", DataType::Utf8View, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampSecondArray::from(vec![
                    Some(s_sec),
                    Some(in_sec),
                    None,
                    Some(out_sec),
                ])),
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(s_sec * 1_000),
                    Some(in_sec * 1_000),
                    None,
                    Some(out_sec * 1_000),
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(s_sec * 1_000_000),
                    Some(in_sec * 1_000_000),
                    None,
                    Some(out_sec * 1_000_000),
                ])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    Some(s_sec * 1_000_000_000),
                    Some(in_sec * 1_000_000_000),
                    None,
                    Some(out_sec * 1_000_000_000),
                ])),
                Arc::new(Date64Array::from(vec![
                    Some(s_sec * 1_000),
                    Some(in_sec * 1_000),
                    None,
                    Some(out_sec * 1_000),
                ])),
                Arc::new(UInt64Array::from(vec![
                    Some((s_sec * 1_000) as u64),
                    Some((in_sec * 1_000) as u64),
                    None,
                    Some((out_sec * 1_000) as u64),
                ])),
                Arc::new(StringViewArray::from(vec![
                    Some("2020-01-01T00:00:00Z"),
                    Some("2023-11-14T22:15:00Z"),
                    None,
                    Some("2025-01-01T00:00:00Z"),
                ])),
            ],
        )
        .unwrap();

        for col in ["ts_s", "ts_ms", "ts_us", "ts_ns", "d64", "u64"] {
            let f = filter_batch_by_window(&batch, col, start_ms, end_ms).unwrap();
            assert_eq!(f.num_rows(), 1, "column {col} must keep only row 1");
        }
        let sv_f = filter_batch_by_window(
            &batch,
            "sv",
            Some(parse_timestamp_utc_ms("2023-01-01", false).unwrap()),
            Some(parse_timestamp_utc_ms("2024-01-01", true).unwrap()),
        )
        .unwrap();
        assert_eq!(sv_f.num_rows(), 1);

        // Date32 check (days since epoch: 2026-01-05 = 20458).
        let d32_schema = Arc::new(Schema::new(vec![Field::new("d32", DataType::Date32, true)]));
        let d32_batch = RecordBatch::try_new(
            d32_schema,
            vec![Arc::new(Date32Array::from(vec![
                Some(20_450),
                Some(20_458),
                None,
                Some(20_470),
            ]))],
        )
        .unwrap();
        let d32_f = filter_batch_by_window(
            &d32_batch,
            "d32",
            Some(parse_timestamp_utc_ms("2026-01-05", false).unwrap()),
            Some(parse_timestamp_utc_ms("2026-01-06", true).unwrap()),
        )
        .unwrap();
        assert_eq!(d32_f.num_rows(), 1);
    }
}
