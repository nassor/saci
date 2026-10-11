//! [`ClickhouseSource`]: ClickHouse [`Source`] implementation with Arrow IPC streaming.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::temporal_conversions::{
    date32_to_datetime, date64_to_datetime, timestamp_ms_to_datetime, timestamp_us_to_datetime,
};
use arrow_array::*;
use arrow_cast::CastOptions;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use saci_core::error::SaciError;
use saci_core::io::cast_batch;
use saci_core::io::source::Source;

use crate::config::{ClickhouseSourceConfig, SourceMode};
use crate::connection::{build_client, format_cursor_value, quote_identifier, quote_qualified};

/// Reader state across query executions.
#[derive(Debug)]
enum SourceState {
    Polling {
        current_cursor: Option<String>,
        /// The tiebreak value of the last row the previous batch delivered,
        /// `None` until one has. It resumes polling inside a cursor value that
        /// a batch boundary fell through.
        current_tiebreak: Option<String>,
        exhausted: bool,
    },
    Dump {
        offset: usize,
        exhausted: bool,
    },
    Query {
        executed: bool,
    },
}

/// A ClickHouse [`Source`] reading data via native Arrow IPC streaming.
pub struct ClickhouseSource {
    config: ClickhouseSourceConfig,
    schema: Arc<Schema>,
    client: clickhouse::Client,
    batch_rows: usize,
    state: SourceState,
    buffered_batches: VecDeque<RecordBatch>,
    emitted_batches_this_cycle: usize,
}

impl ClickhouseSource {
    /// Creates a new [`ClickhouseSource`] from configuration.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] if configuration validation fails
    /// or Arrow schema cannot be constructed.
    pub fn new(config: ClickhouseSourceConfig) -> Result<Self, SaciError> {
        config.validate()?;

        let fields = config
            .schema_fields
            .iter()
            .map(|f| f.to_arrow_field())
            .collect::<Result<Vec<_>, _>>()?;
        let schema = Arc::new(Schema::new(fields));

        let client = build_client(&config.connection)?;
        let batch_rows = config.batch_rows;

        let state = match &config.mode {
            SourceMode::Polling(poll) => SourceState::Polling {
                current_cursor: poll.initial_cursor.clone(),
                current_tiebreak: None,
                exhausted: false,
            },
            SourceMode::Dump(_) => SourceState::Dump {
                offset: 0,
                exhausted: false,
            },
            SourceMode::Query(_) => SourceState::Query { executed: false },
        };

        Ok(Self {
            config,
            schema,
            client,
            batch_rows,
            state,
            buffered_batches: VecDeque::new(),
            emitted_batches_this_cycle: 0,
        })
    }

    /// Fetches and decodes the next batch chunk from ClickHouse.
    async fn fetch_next_batches(&mut self) -> Result<Vec<RecordBatch>, SaciError> {
        match &mut self.state {
            SourceState::Polling {
                current_cursor,
                current_tiebreak,
                exhausted,
            } => {
                if *exhausted {
                    *exhausted = false;
                    return Ok(Vec::new());
                }

                let SourceMode::Polling(poll) = &self.config.mode else {
                    unreachable!()
                };

                let cols = self
                    .config
                    .schema_fields
                    .iter()
                    .map(|f| quote_identifier(&f.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                let table = quote_qualified(&poll.table);
                let mut sql = format!("SELECT {cols} FROM {table}");

                if let Some(cursor) = current_cursor.as_deref() {
                    let cursor_ty = self
                        .config
                        .schema_fields
                        .iter()
                        .find(|f| f.name == poll.cursor_column)
                        .map(|f| f.ty)
                        .ok_or_else(|| {
                            SaciError::configuration(format!(
                                "cursor column '{}' missing from schema",
                                poll.cursor_column
                            ))
                        })?;
                    let quoted_col = quote_identifier(&poll.cursor_column);
                    let formatted_val = format_cursor_value(cursor, cursor_ty);

                    // The tiebreak widens the filter to the cursor's own value
                    // only once a previous batch fixed its position there. With
                    // just a starting cursor there is no tiebreak to resume
                    // from, so rows at the cursor value stay behind.
                    match (&poll.tiebreak_column, current_tiebreak.as_deref()) {
                        (Some(tie), Some(tie_val)) => {
                            let tie_ty = self
                                .config
                                .schema_fields
                                .iter()
                                .find(|f| f.name == *tie)
                                .map(|f| f.ty)
                                .ok_or_else(|| {
                                    SaciError::configuration(format!(
                                        "tiebreak column '{tie}' missing from schema"
                                    ))
                                })?;
                            let quoted_tie = quote_identifier(tie);
                            let formatted_tie = format_cursor_value(tie_val, tie_ty);
                            sql.push_str(&format!(
                                " WHERE ({quoted_col} > {formatted_val} OR ({quoted_col} = {formatted_val} AND {quoted_tie} > {formatted_tie}))"
                            ));
                        }
                        _ => sql.push_str(&format!(" WHERE {quoted_col} > {formatted_val}")),
                    }
                }

                sql.push_str(&format!(
                    " ORDER BY {}",
                    quote_identifier(&poll.cursor_column)
                ));
                if let Some(tie) = &poll.tiebreak_column {
                    sql.push_str(&format!(", {}", quote_identifier(tie)));
                }
                sql.push_str(&format!(" LIMIT {}", self.batch_rows));

                let bytes = self
                    .client
                    .query(&sql)
                    .fetch_bytes("ArrowStream")
                    .map_err(|e| SaciError::generic(format!("clickhouse query fetch error: {e}")))?
                    .collect()
                    .await
                    .map_err(|e| {
                        SaciError::generic(format!("clickhouse query collect error: {e}"))
                    })?;

                let batches = decode_arrow_stream(&bytes, &self.schema)?;
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

                if total_rows == 0 {
                    *exhausted = true;
                    return Ok(Vec::new());
                }

                if let Some(last_batch) = batches.last() {
                    let cursor_col =
                        last_batch
                            .column_by_name(&poll.cursor_column)
                            .ok_or_else(|| {
                                SaciError::generic(format!(
                                    "batch missing cursor column '{}'",
                                    poll.cursor_column
                                ))
                            })?;
                    let last_row = last_batch.num_rows() - 1;
                    let last_val = scalar_to_string(cursor_col, last_row)?;
                    *current_cursor = Some(last_val);

                    if let Some(tie) = &poll.tiebreak_column {
                        let tie_col = last_batch.column_by_name(tie).ok_or_else(|| {
                            SaciError::generic(format!("batch missing tiebreak column '{tie}'"))
                        })?;
                        *current_tiebreak = Some(scalar_to_string(tie_col, last_row)?);
                    }
                }

                if total_rows < self.batch_rows {
                    *exhausted = true;
                }

                Ok(batches)
            }
            SourceState::Dump { offset, exhausted } => {
                if *exhausted {
                    *exhausted = false;
                    *offset = 0;
                    return Ok(Vec::new());
                }

                let SourceMode::Dump(dump) = &self.config.mode else {
                    unreachable!()
                };

                let cols = self
                    .config
                    .schema_fields
                    .iter()
                    .map(|f| quote_identifier(&f.name))
                    .collect::<Vec<_>>()
                    .join(", ");
                let table = quote_qualified(&dump.table);
                let sql = format!(
                    "SELECT {cols} FROM {table} LIMIT {} OFFSET {}",
                    self.batch_rows, *offset
                );

                let bytes = self
                    .client
                    .query(&sql)
                    .fetch_bytes("ArrowStream")
                    .map_err(|e| SaciError::generic(format!("clickhouse dump fetch error: {e}")))?
                    .collect()
                    .await
                    .map_err(|e| {
                        SaciError::generic(format!("clickhouse dump collect error: {e}"))
                    })?;

                let batches = decode_arrow_stream(&bytes, &self.schema)?;
                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

                if total_rows == 0 {
                    *exhausted = true;
                    return Ok(Vec::new());
                }

                *offset += total_rows;
                if total_rows < self.batch_rows {
                    *exhausted = true;
                }

                Ok(batches)
            }
            SourceState::Query { executed } => {
                if *executed {
                    *executed = false;
                    return Ok(Vec::new());
                }

                let SourceMode::Query(q) = &self.config.mode else {
                    unreachable!()
                };

                let bytes = self
                    .client
                    .query(&q.query)
                    .fetch_bytes("ArrowStream")
                    .map_err(|e| SaciError::generic(format!("clickhouse query fetch error: {e}")))?
                    .collect()
                    .await
                    .map_err(|e| {
                        SaciError::generic(format!("clickhouse query collect error: {e}"))
                    })?;

                *executed = true;
                decode_arrow_stream(&bytes, &self.schema)
            }
        }
    }
}

#[async_trait]
impl Source for ClickhouseSource {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        if self.config.max_batches_per_cycle > 0
            && self.emitted_batches_this_cycle >= self.config.max_batches_per_cycle
        {
            self.emitted_batches_this_cycle = 0;
            return Ok(None);
        }

        if let Some(batch) = self.buffered_batches.pop_front() {
            self.emitted_batches_this_cycle += 1;
            return Ok(Some(batch));
        }

        let new_batches = self.fetch_next_batches().await?;
        if new_batches.is_empty() {
            self.emitted_batches_this_cycle = 0;
            return Ok(None);
        }

        self.buffered_batches.extend(new_batches);
        let batch = self.buffered_batches.pop_front();
        if batch.is_some() {
            self.emitted_batches_this_cycle += 1;
        }
        Ok(batch)
    }

    fn request_batch_rows(&mut self, rows: usize) {
        if rows > 0 {
            self.batch_rows = rows;
        }
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        Ok(())
    }
}

/// Decodes Arrow IPC stream bytes and casts them to `schema`.
fn decode_arrow_stream(bytes: &[u8], schema: &Arc<Schema>) -> Result<Vec<RecordBatch>, SaciError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let cursor = std::io::Cursor::new(bytes);
    let reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)
        .map_err(|e| SaciError::generic(format!("clickhouse source: stream header error: {e}")))?;

    let mut batches = Vec::new();
    for batch_res in reader {
        let batch = batch_res.map_err(|e| {
            SaciError::generic(format!("clickhouse source: stream decode error: {e}"))
        })?;
        let widened = widen_dates(&batch, schema)?;
        let source = widened.as_ref().unwrap_or(&batch);
        let casted = cast_batch(source, schema, &CastOptions::default())?;
        batches.push(casted);
    }
    Ok(batches)
}

/// Widens a ClickHouse `Date` column from the `UInt16` day count ArrowStream
/// sends to `Int32`, which is what the cast to a declared `Date32`/`Date64`
/// takes; `UInt16` to a date type is not a cast `arrow-cast` offers.
///
/// `None` when no column needs it, so a batch whose server types already match
/// the declared date types is handed on untouched.
fn widen_dates(batch: &RecordBatch, target: &Schema) -> Result<Option<RecordBatch>, SaciError> {
    if batch.num_columns() != target.fields().len() {
        return Ok(None);
    }

    let needs_widening =
        batch
            .columns()
            .iter()
            .zip(target.fields())
            .any(|(column, destination)| {
                matches!(destination.data_type(), DataType::Date32 | DataType::Date64)
                    && column.data_type() == &DataType::UInt16
            });
    if !needs_widening {
        return Ok(None);
    }

    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());

    for ((column, source), destination) in batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .zip(target.fields())
    {
        let declared_date = matches!(destination.data_type(), DataType::Date32 | DataType::Date64);
        if declared_date && column.data_type() == &DataType::UInt16 {
            let cast = arrow_cast::cast(column, &DataType::Int32).map_err(|e| {
                SaciError::generic(format!("clickhouse source: date widening error: {e}"))
            })?;
            columns.push(cast);
            fields.push(Field::new(
                source.name(),
                DataType::Int32,
                source.is_nullable(),
            ));
        } else {
            columns.push(Arc::clone(column));
            fields.push(source.as_ref().clone());
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map(Some)
        .map_err(|e| {
            SaciError::generic(format!(
                "clickhouse source: date widening rebuild error: {e}"
            ))
        })
}

/// Extracts a scalar value at index `row` as a string for cursor comparison.
fn scalar_to_string(array: &ArrayRef, row: usize) -> Result<String, SaciError> {
    if array.is_null(row) {
        return Err(SaciError::generic("cursor column value must not be null"));
    }

    let s = match array.data_type() {
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("downcast Int8Array")
            .value(row)
            .to_string(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .expect("downcast Int16Array")
            .value(row)
            .to_string(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("downcast Int32Array")
            .value(row)
            .to_string(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("downcast Int64Array")
            .value(row)
            .to_string(),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("downcast UInt8Array")
            .value(row)
            .to_string(),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("downcast UInt16Array")
            .value(row)
            .to_string(),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("downcast UInt32Array")
            .value(row)
            .to_string(),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("downcast UInt64Array")
            .value(row)
            .to_string(),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("downcast Float32Array")
            .value(row)
            .to_string(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("downcast Float64Array")
            .value(row)
            .to_string(),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("downcast StringArray")
            .value(row)
            .to_string(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("downcast LargeStringArray")
            .value(row)
            .to_string(),
        // A temporal cursor is compared as a ClickHouse `Date`/`DateTime64`
        // literal, which parses `YYYY-MM-DD[ HH:MM:SS[.ffffff]]` and refuses
        // the raw integer (`Cannot parse date`), so the value goes out as the
        // datetime ClickHouse reads back.
        DataType::Date32 => date32_to_datetime(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("downcast Date32Array")
                .value(row),
        )
        .ok_or_else(|| SaciError::generic("cursor date is outside the representable range"))?
        .format("%Y-%m-%d")
        .to_string(),
        DataType::Date64 => date64_to_datetime(
            array
                .as_any()
                .downcast_ref::<Date64Array>()
                .expect("downcast Date64Array")
                .value(row),
        )
        .ok_or_else(|| SaciError::generic("cursor date is outside the representable range"))?
        .format("%Y-%m-%d")
        .to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => timestamp_us_to_datetime(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("downcast TimestampMicrosecondArray")
                .value(row),
        )
        .ok_or_else(|| SaciError::generic("cursor timestamp is outside the representable range"))?
        .format("%Y-%m-%d %H:%M:%S%.6f")
        .to_string(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => timestamp_ms_to_datetime(
            array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("downcast TimestampMillisecondArray")
                .value(row),
        )
        .ok_or_else(|| SaciError::generic("cursor timestamp is outside the representable range"))?
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string(),
        DataType::Decimal128(_, scale) => {
            let v = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("downcast Decimal128Array")
                .value(row);
            format_decimal(v, *scale)
        }
        other => {
            return Err(SaciError::generic(format!(
                "unsupported cursor column data type: {other:?}"
            )));
        }
    };
    Ok(s)
}

/// Formats a 128-bit integer with given scale as decimal string.
fn format_decimal(value: i128, scale: i8) -> String {
    if scale <= 0 {
        return value.to_string();
    }
    let factor = 10_i128.pow(scale as u32);
    let int_part = value / factor;
    let frac_part = (value % factor).abs();
    format!("{int_part}.{frac_part:0width$}", width = scale as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::Field;

    #[test]
    fn format_decimal_formats_scaled_integers() {
        assert_eq!(format_decimal(12345, 2), "123.45");
        assert_eq!(format_decimal(100, 2), "1.00");
        assert_eq!(format_decimal(5, 3), "0.005");
        assert_eq!(format_decimal(42, 0), "42");
    }

    #[test]
    fn scalar_to_string_extracts_typed_values() {
        let int_arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(100), Some(200)]));
        assert_eq!(scalar_to_string(&int_arr, 0).unwrap(), "100");
        assert_eq!(scalar_to_string(&int_arr, 1).unwrap(), "200");

        let str_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")]));
        assert_eq!(scalar_to_string(&str_arr, 0).unwrap(), "alpha");

        let null_arr: ArrayRef = Arc::new(Int64Array::from(vec![None]));
        assert!(scalar_to_string(&null_arr, 0).is_err());
    }

    #[test]
    fn decode_arrow_stream_decodes_and_casts() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
            ],
        )
        .expect("batch creates");

        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).expect("writer");
            writer.write(&batch).expect("write");
            writer.finish().expect("finish");
        }

        let decoded = decode_arrow_stream(&buf, &schema).expect("decode");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 3);
        assert_eq!(decoded[0].schema(), schema);
    }

    #[test]
    fn decode_empty_arrow_stream_returns_empty_vec() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let decoded = decode_arrow_stream(&[], &schema).expect("empty decode");
        assert!(decoded.is_empty());
    }

    #[test]
    fn scalar_to_string_all_supported_types() {
        let i8_arr: ArrayRef = Arc::new(Int8Array::from(vec![-8]));
        assert_eq!(scalar_to_string(&i8_arr, 0).unwrap(), "-8");

        let i16_arr: ArrayRef = Arc::new(Int16Array::from(vec![-16]));
        assert_eq!(scalar_to_string(&i16_arr, 0).unwrap(), "-16");

        let i32_arr: ArrayRef = Arc::new(Int32Array::from(vec![-32]));
        assert_eq!(scalar_to_string(&i32_arr, 0).unwrap(), "-32");

        let u8_arr: ArrayRef = Arc::new(UInt8Array::from(vec![8]));
        assert_eq!(scalar_to_string(&u8_arr, 0).unwrap(), "8");

        let u16_arr: ArrayRef = Arc::new(UInt16Array::from(vec![16]));
        assert_eq!(scalar_to_string(&u16_arr, 0).unwrap(), "16");

        let u32_arr: ArrayRef = Arc::new(UInt32Array::from(vec![32]));
        assert_eq!(scalar_to_string(&u32_arr, 0).unwrap(), "32");

        let u64_arr: ArrayRef = Arc::new(UInt64Array::from(vec![64]));
        assert_eq!(scalar_to_string(&u64_arr, 0).unwrap(), "64");

        let f32_arr: ArrayRef = Arc::new(Float32Array::from(vec![1.25]));
        assert_eq!(scalar_to_string(&f32_arr, 0).unwrap(), "1.25");

        let f64_arr: ArrayRef = Arc::new(Float64Array::from(vec![2.5]));
        assert_eq!(scalar_to_string(&f64_arr, 0).unwrap(), "2.5");

        let large_str_arr: ArrayRef = Arc::new(LargeStringArray::from(vec!["hello"]));
        assert_eq!(scalar_to_string(&large_str_arr, 0).unwrap(), "hello");

        // Temporal values are the ClickHouse datetime literal, not the raw
        // epoch integer: `Date > '18000'` is a parse error server-side.
        let d32_arr: ArrayRef = Arc::new(Date32Array::from(vec![18000]));
        assert_eq!(scalar_to_string(&d32_arr, 0).unwrap(), "2019-04-14");

        let d64_arr: ArrayRef = Arc::new(Date64Array::from(vec![1500000000000]));
        assert_eq!(scalar_to_string(&d64_arr, 0).unwrap(), "2017-07-14");

        let ts_micros: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1600000000000000]));
        assert_eq!(
            scalar_to_string(&ts_micros, 0).unwrap(),
            "2020-09-13 12:26:40.000000"
        );

        let ts_millis: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1600000000000]));
        assert_eq!(
            scalar_to_string(&ts_millis, 0).unwrap(),
            "2020-09-13 12:26:40.000"
        );

        let dec_arr: ArrayRef = Arc::new(
            Decimal128Array::from(vec![12345])
                .with_precision_and_scale(18, 2)
                .unwrap(),
        );
        assert_eq!(scalar_to_string(&dec_arr, 0).unwrap(), "123.45");
    }

    #[test]
    fn scalar_to_string_unsupported_type_returns_error() {
        let bin_arr: ArrayRef = Arc::new(BinaryArray::from_vec(vec![b"bytes"]));
        let err = scalar_to_string(&bin_arr, 0).unwrap_err();
        assert!(
            err.message()
                .contains("unsupported cursor column data type")
        );
    }

    #[test]
    fn format_decimal_edge_cases() {
        assert_eq!(format_decimal(0, 2), "0.00");
        assert_eq!(format_decimal(0, 0), "0");
        assert_eq!(format_decimal(-12345, 2), "-123.45");
        assert_eq!(format_decimal(5, 4), "0.0005");
        assert_eq!(format_decimal(999, -1), "999");
    }

    #[test]
    fn source_request_batch_rows_updates_batch_rows() {
        use crate::config::{ClickhouseConnectionConfig, ClickhouseFieldType, DumpMode, FieldSpec};
        use std::collections::HashMap;

        let config = ClickhouseSourceConfig {
            name: "src".to_string(),
            connection: ClickhouseConnectionConfig {
                url: "http://localhost:8123".to_string(),
                user: None,
                password: None,
                database: "default".to_string(),
                query_timeout_ms: 30000,
                compression: crate::config::CompressionConfig::None,
                settings: HashMap::new(),
            },
            mode: SourceMode::Dump(DumpMode {
                table: "events".to_string(),
            }),
            schema_fields: vec![FieldSpec {
                name: "id".to_string(),
                ty: ClickhouseFieldType::Int64,
                nullable: false,
                precision: None,
                scale: None,
            }],
            batch_rows: 100,
            max_batches_per_cycle: 0,
        };

        let mut source = ClickhouseSource::new(config).expect("creates source");
        assert_eq!(source.batch_rows, 100);
        assert_eq!(source.schema().fields().len(), 1);

        source.request_batch_rows(250);
        assert_eq!(source.batch_rows, 250);

        // 0 is ignored
        source.request_batch_rows(0);
        assert_eq!(source.batch_rows, 250);
    }

    #[test]
    fn decode_arrow_stream_corrupt_bytes_returns_error() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let result = decode_arrow_stream(b"not an arrow ipc stream", &schema);
        assert!(result.is_err());
    }

    #[test]
    fn widen_dates_bridges_a_uint16_date_column() {
        let batch_schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::UInt16, false),
            Field::new("id", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&batch_schema),
            vec![
                Arc::new(UInt16Array::from(vec![18262, 18263])),
                Arc::new(UInt64Array::from(vec![1, 2])),
            ],
        )
        .expect("batch");

        let target = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, false),
            Field::new("id", DataType::UInt64, false),
        ]));

        let widened = widen_dates(&batch, &target)
            .expect("widening succeeds")
            .expect("a UInt16 date column is widened");
        assert_eq!(widened.column(0).data_type(), &DataType::Int32);

        let casted =
            cast_batch(&widened, &target, &CastOptions::default()).expect("casts to Date32");
        assert_eq!(casted.column(0).data_type(), &DataType::Date32);
        let dates = casted
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date32 column");
        assert_eq!(dates.value(0), 18262);

        // No bare `Date` column: the batch is handed on untouched.
        let undated = Arc::new(Schema::new(vec![
            Field::new("d", DataType::UInt16, false),
            Field::new("id", DataType::UInt64, false),
        ]));
        assert!(
            widen_dates(&batch, &undated)
                .expect("no widening needed")
                .is_none()
        );
    }
}
