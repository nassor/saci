//! Arrow values into MariaDB and MySQL statement parameters.
//!
//! [`ColumnReader`] resolves each declared column against a `RecordBatch`
//! once per batch, so the per-row path is a bounds-checked index into a typed
//! array with no schema lookup and no downcast. [`row_values`] appends one
//! row's [`Value`]s to a buffer the sink owns, which lets a multi-row `INSERT`
//! chunk be built by calling it once per row on the same `Vec`.
//!
//! The servers store dates and timestamps as a civil calendar value with no
//! zone, so a timestamp column is written as its UTC wall clock whatever
//! timezone the Arrow field carries. A value the servers cannot hold (a float
//! that is not finite, a year beyond 0000 to 9999, a time outside one day) is
//! refused with the column and the row rather than sent for the server to
//! clamp or reject in its own words.
//!
//! A `decimal128` is bound as its exact decimal text. The wire protocol has
//! no 128-bit integer, and text keeps every digit the declared scale carries.

use std::fmt::Display;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, StringArray,
    Time64MicrosecondArray, TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use mysql_common::value::Value;
use saci_core::error::SaciError;

use crate::config::{FieldSpec, MariadbFieldType};
use crate::values::{MICROS_PER_DAY, MICROS_PER_SECOND, civil_from_days};

/// The last year a `date`, `datetime` or `timestamp` column stores.
const MAX_YEAR: u16 = 9999;

/// Microseconds in one minute and one hour.
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;

/// A declared column resolved against one `RecordBatch`.
///
/// Holding the downcast array means the per-row cost is a bounds-checked
/// index, not a name lookup plus a downcast per value. `what` and `name` are
/// held so a per-value refusal names itself without the caller having to
/// re-derive which column produced it.
#[derive(Debug)]
pub(crate) struct ColumnReader<'a> {
    /// The connector's error prefix, e.g. `MariadbSink`.
    what: &'a str,
    /// The column's name.
    name: &'a str,
    /// The array, downcast to the type the declared type implies.
    kind: ReaderKind<'a>,
}

/// One column's array, downcast.
#[derive(Debug)]
enum ReaderKind<'a> {
    /// `boolean`.
    Boolean(&'a BooleanArray),
    /// `int8`.
    Int8(&'a Int8Array),
    /// `int16`.
    Int16(&'a Int16Array),
    /// `int32`.
    Int32(&'a Int32Array),
    /// `int64`.
    Int64(&'a Int64Array),
    /// `uint8`.
    UInt8(&'a UInt8Array),
    /// `uint16`.
    UInt16(&'a UInt16Array),
    /// `uint32`.
    UInt32(&'a UInt32Array),
    /// `uint64`.
    UInt64(&'a UInt64Array),
    /// `float32`.
    Float32(&'a Float32Array),
    /// `float64`.
    Float64(&'a Float64Array),
    /// `decimal128`, with the scale the array and the declaration agree on.
    Decimal128(&'a Decimal128Array, i8),
    /// `utf8` and `json`.
    Utf8(&'a StringArray),
    /// `binary`.
    Binary(&'a BinaryArray),
    /// `date32`.
    Date32(&'a Date32Array),
    /// `time64_micros`.
    Time64Micros(&'a Time64MicrosecondArray),
    /// `timestamp_micros` and `timestamp_micros_utc`.
    TimestampMicros(&'a TimestampMicrosecondArray),
}

impl ColumnReader<'_> {
    /// The value at `row`, or [`Value::NULL`] for a null slot.
    ///
    /// The error side is boxed so the happy path carries a `Value`-sized
    /// result and none of the formatting code; the allocation happens only on
    /// a refusal.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming the column, the row and the value
    /// for one the servers cannot hold: a non-finite float, a date or
    /// timestamp outside the years 0000 to 9999, or a time outside one day.
    pub(crate) fn value(&self, row: usize) -> Result<Value, Box<SaciError>> {
        macro_rules! read {
            ($array:expr, $v:ident => $body:expr) => {{
                let array = $array;
                if array.is_null(row) {
                    Value::NULL
                } else {
                    let $v = array.value(row);
                    $body
                }
            }};
        }

        Ok(match &self.kind {
            ReaderKind::Boolean(a) => read!(a, v => Value::Int(i64::from(v))),
            ReaderKind::Int8(a) => read!(a, v => Value::Int(i64::from(v))),
            ReaderKind::Int16(a) => read!(a, v => Value::Int(i64::from(v))),
            ReaderKind::Int32(a) => read!(a, v => Value::Int(i64::from(v))),
            ReaderKind::Int64(a) => read!(a, v => Value::Int(v)),
            ReaderKind::UInt8(a) => read!(a, v => Value::UInt(u64::from(v))),
            ReaderKind::UInt16(a) => read!(a, v => Value::UInt(u64::from(v))),
            ReaderKind::UInt32(a) => read!(a, v => Value::UInt(u64::from(v))),
            ReaderKind::UInt64(a) => read!(a, v => Value::UInt(v)),
            ReaderKind::Float32(a) => {
                read!(a, v => {
                    if !v.is_finite() {
                        return Err(self.non_finite(row, f64::from(v)));
                    }
                    Value::Float(v)
                })
            }
            ReaderKind::Float64(a) => {
                read!(a, v => {
                    if !v.is_finite() {
                        return Err(self.non_finite(row, v));
                    }
                    Value::Double(v)
                })
            }
            ReaderKind::Decimal128(a, scale) => {
                read!(a, v => Value::Bytes(decimal_text(v, *scale).into_bytes()))
            }
            ReaderKind::Utf8(a) => read!(a, v => Value::Bytes(v.as_bytes().to_vec())),
            ReaderKind::Binary(a) => read!(a, v => Value::Bytes(v.to_vec())),
            ReaderKind::Date32(a) => {
                read!(a, v => {
                    let (year, month, day) = self.civil(row, i64::from(v))?;
                    Value::Date(year, month, day, 0, 0, 0, 0)
                })
            }
            ReaderKind::Time64Micros(a) => {
                read!(a, v => {
                    if !(0..MICROS_PER_DAY).contains(&v) {
                        return Err(self.refuse(
                            row,
                            format!("time {v} is outside a time of day"),
                        ));
                    }
                    // Each part is bounded by the range check above:
                    // hours below 24, minutes and seconds below 60,
                    // microseconds below one million.
                    Value::Time(
                        false,
                        0,
                        (v / MICROS_PER_HOUR) as u8,
                        (v % MICROS_PER_HOUR / MICROS_PER_MINUTE) as u8,
                        (v % MICROS_PER_MINUTE / MICROS_PER_SECOND) as u8,
                        (v % MICROS_PER_SECOND) as u32,
                    )
                })
            }
            ReaderKind::TimestampMicros(a) => {
                read!(a, v => {
                    let (year, month, day) = self.civil(row, v.div_euclid(MICROS_PER_DAY))?;
                    // The remainder is in 0..MICROS_PER_DAY, so every part
                    // below fits its target.
                    let time = v.rem_euclid(MICROS_PER_DAY);
                    Value::Date(
                        year,
                        month,
                        day,
                        (time / MICROS_PER_HOUR) as u8,
                        (time % MICROS_PER_HOUR / MICROS_PER_MINUTE) as u8,
                        (time % MICROS_PER_MINUTE / MICROS_PER_SECOND) as u8,
                        (time % MICROS_PER_SECOND) as u32,
                    )
                })
            }
        })
    }

    /// The civil date `days` after 1970-01-01, refused outside the years the
    /// servers store.
    fn civil(&self, row: usize, days: i64) -> Result<(u16, u8, u8), Box<SaciError>> {
        let (year, month, day) = civil_from_days(days);
        match u16::try_from(year) {
            // `civil_from_days` yields a month in 1..=12 and a day in 1..=31.
            Ok(year) if year <= MAX_YEAR => Ok((year, month as u8, day as u8)),
            _ => Err(self.refuse(
                row,
                format!("date {days} is outside the years 0000 to 9999 the servers store"),
            )),
        }
    }

    /// The refusal for a float with no decimal representation.
    fn non_finite(&self, row: usize, value: f64) -> Box<SaciError> {
        let name = if value.is_nan() {
            "NaN"
        } else if value.is_sign_positive() {
            "inf"
        } else {
            "-inf"
        };
        self.refuse(
            row,
            format!("{name} has no MariaDB or MySQL representation"),
        )
    }

    /// A per-value refusal naming the connector, the column and the row.
    ///
    /// Boxed, and kept out of line, so the per-value happy path carries only a
    /// `Value`-wide result and none of the formatting code.
    #[cold]
    #[inline(never)]
    fn refuse(&self, row: usize, detail: impl Display) -> Box<SaciError> {
        Box::new(SaciError::generic(format!(
            "{}: column '{}' row {row}: {detail}",
            self.what, self.name
        )))
    }
}

/// The exact decimal text of `value` carrying `scale` fraction digits.
///
/// `value` is the unscaled integer an Arrow `Decimal128` holds. The point and
/// every fraction digit are written, including trailing zeros, so the text
/// round trips at the column's declared scale. A negative scale multiplies the
/// integer by a power of ten and writes no point.
pub(crate) fn decimal_text(value: i128, scale: i8) -> String {
    // `unsigned_abs` keeps `i128::MIN`, whose magnitude has no `i128`.
    let digits = value.unsigned_abs().to_string();
    let negative = value < 0;
    let mut text = String::with_capacity(digits.len() + usize::from(scale.unsigned_abs()) + 3);
    if negative {
        text.push('-');
    }
    if scale <= 0 {
        text.push_str(&digits);
        if value != 0 {
            text.extend(std::iter::repeat_n('0', usize::from(scale.unsigned_abs())));
        }
        return text;
    }
    let scale = usize::from(scale.unsigned_abs());
    if digits.len() > scale {
        let (whole, fraction) = digits.split_at(digits.len() - scale);
        text.push_str(whole);
        text.push('.');
        text.push_str(fraction);
    } else {
        text.push_str("0.");
        text.extend(std::iter::repeat_n('0', scale - digits.len()));
        text.push_str(&digits);
    }
    text
}

/// Resolve every declared column of `batch` once, in the order `specs` lists
/// them.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] naming the first column that `batch` does
/// not carry, that carries a different Arrow type than its declared type
/// implies, or whose `decimal128` array has a scale other than the declared
/// one.
pub(crate) fn resolve_columns<'a>(
    what: &'a str,
    batch: &'a RecordBatch,
    specs: &[&'a FieldSpec],
) -> Result<Vec<ColumnReader<'a>>, SaciError> {
    let mut readers = Vec::with_capacity(specs.len());
    for spec in specs {
        let array = batch
            .column_by_name(&spec.name)
            .ok_or_else(|| {
                SaciError::generic(format!(
                    "{what}: batch has no column '{}'; the batch schema must match the declared \
                     schema_fields",
                    spec.name
                ))
            })?
            .as_ref();
        readers.push(ColumnReader {
            what,
            name: &spec.name,
            kind: reader_kind(what, spec, array)?,
        });
    }
    Ok(readers)
}

/// The reader for one declared type over one array.
fn reader_kind<'a>(
    what: &str,
    spec: &FieldSpec,
    array: &'a dyn Array,
) -> Result<ReaderKind<'a>, SaciError> {
    macro_rules! downcast {
        ($target:ty) => {{
            array.as_any().downcast_ref::<$target>().ok_or_else(|| {
                SaciError::generic(format!(
                    "{what}: column '{}' is declared type \"{}\" but the batch holds {:?}",
                    spec.name,
                    spec.ty.as_str(),
                    array.data_type()
                ))
            })?
        }};
    }

    Ok(match spec.ty {
        MariadbFieldType::Boolean => ReaderKind::Boolean(downcast!(BooleanArray)),
        MariadbFieldType::Int8 => ReaderKind::Int8(downcast!(Int8Array)),
        MariadbFieldType::Int16 => ReaderKind::Int16(downcast!(Int16Array)),
        MariadbFieldType::Int32 => ReaderKind::Int32(downcast!(Int32Array)),
        MariadbFieldType::Int64 => ReaderKind::Int64(downcast!(Int64Array)),
        MariadbFieldType::UInt8 => ReaderKind::UInt8(downcast!(UInt8Array)),
        MariadbFieldType::UInt16 => ReaderKind::UInt16(downcast!(UInt16Array)),
        MariadbFieldType::UInt32 => ReaderKind::UInt32(downcast!(UInt32Array)),
        MariadbFieldType::UInt64 => ReaderKind::UInt64(downcast!(UInt64Array)),
        MariadbFieldType::Float32 => ReaderKind::Float32(downcast!(Float32Array)),
        MariadbFieldType::Float64 => ReaderKind::Float64(downcast!(Float64Array)),
        MariadbFieldType::Decimal128 => {
            let decimal = downcast!(Decimal128Array);
            let (_, declared) = spec.decimal_params()?;
            if decimal.scale() != declared {
                return Err(SaciError::generic(format!(
                    "{what}: column '{}' is declared with scale {declared} but the batch holds \
                     a decimal128 of scale {}",
                    spec.name,
                    decimal.scale()
                )));
            }
            ReaderKind::Decimal128(decimal, declared)
        }
        MariadbFieldType::Utf8 | MariadbFieldType::Json => ReaderKind::Utf8(downcast!(StringArray)),
        MariadbFieldType::Binary => ReaderKind::Binary(downcast!(BinaryArray)),
        MariadbFieldType::Date32 => ReaderKind::Date32(downcast!(Date32Array)),
        MariadbFieldType::Time64Micros => {
            ReaderKind::Time64Micros(downcast!(Time64MicrosecondArray))
        }
        MariadbFieldType::TimestampMicros | MariadbFieldType::TimestampMicrosUtc => {
            ReaderKind::TimestampMicros(downcast!(TimestampMicrosecondArray))
        }
    })
}

/// Append one row's values to `out`, one per reader, in reader order.
///
/// `out` is not cleared, so a multi-row `INSERT` chunk is built by calling
/// this once per row on the same `Vec`. A refused row leaves `out` as it was
/// before the call, so no half row reaches a statement.
///
/// # Errors
///
/// Propagates the first value its column cannot hold, unboxed from the
/// [`Box<SaciError>`](ColumnReader::value) the per-value path returns it in.
pub(crate) fn row_values(
    readers: &[ColumnReader<'_>],
    row: usize,
    out: &mut Vec<Value>,
) -> Result<(), SaciError> {
    let start = out.len();
    out.reserve(readers.len());
    for reader in readers {
        match reader.value(row) {
            Ok(value) => out.push(value),
            Err(refusal) => {
                out.truncate(start);
                return Err(*refusal);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::ArrayRef;
    use arrow_schema::{Field, Schema};

    use super::*;

    const WHAT: &str = "MariadbSink";

    fn spec(name: &str, ty: MariadbFieldType) -> FieldSpec {
        FieldSpec {
            name: name.to_string(),
            ty,
            nullable: true,
            precision: None,
            scale: None,
            mariadb_type: None,
        }
    }

    fn decimal_spec(name: &str, precision: u8, scale: i8) -> FieldSpec {
        FieldSpec {
            precision: Some(precision),
            scale: Some(scale),
            ..spec(name, MariadbFieldType::Decimal128)
        }
    }

    fn one_column(name: &str, array: ArrayRef) -> RecordBatch {
        let field = Field::new(name, array.data_type().clone(), true);
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).expect("batch")
    }

    /// Every row of a one-column batch, encoded through [`ColumnReader`].
    fn encode(spec: &FieldSpec, array: ArrayRef) -> Vec<Value> {
        let batch = one_column(&spec.name, array);
        let readers = resolve_columns(WHAT, &batch, &[spec]).expect("resolve");
        (0..batch.num_rows())
            .map(|row| readers[0].value(row).expect("value"))
            .collect()
    }

    /// The refusal text for `row` of a one-column batch.
    fn refusal(spec: &FieldSpec, array: ArrayRef, row: usize) -> String {
        let batch = one_column(&spec.name, array);
        let readers = resolve_columns(WHAT, &batch, &[spec]).expect("resolve");
        readers[0].value(row).expect_err("refused").message()
    }

    /// The resolve refusal for a one-column batch.
    fn resolve_refusal(spec: &FieldSpec, batch: &RecordBatch) -> String {
        resolve_columns(WHAT, batch, &[spec])
            .expect_err("refused")
            .message()
    }

    #[test]
    fn a_boolean_is_zero_or_one() {
        let array = BooleanArray::from(vec![Some(true), Some(false), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Boolean), Arc::new(array)),
            vec![Value::Int(1), Value::Int(0), Value::NULL]
        );
    }

    #[test]
    fn signed_integers_keep_their_extremes() {
        let array = Int8Array::from(vec![Some(i8::MIN), Some(i8::MAX), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Int8), Arc::new(array)),
            vec![Value::Int(-128), Value::Int(127), Value::NULL]
        );
        let array = Int16Array::from(vec![Some(i16::MIN), Some(i16::MAX), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Int16), Arc::new(array)),
            vec![Value::Int(-32_768), Value::Int(32_767), Value::NULL]
        );
        let array = Int32Array::from(vec![Some(i32::MIN), Some(i32::MAX), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Int32), Arc::new(array)),
            vec![
                Value::Int(i64::from(i32::MIN)),
                Value::Int(i64::from(i32::MAX)),
                Value::NULL
            ]
        );
        let array = Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Int64), Arc::new(array)),
            vec![Value::Int(i64::MIN), Value::Int(i64::MAX), Value::NULL]
        );
    }

    #[test]
    fn unsigned_integers_keep_their_extremes() {
        let array = UInt8Array::from(vec![Some(0), Some(u8::MAX), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::UInt8), Arc::new(array)),
            vec![Value::UInt(0), Value::UInt(255), Value::NULL]
        );
        let array = UInt16Array::from(vec![Some(0), Some(u16::MAX), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::UInt16), Arc::new(array)),
            vec![Value::UInt(0), Value::UInt(65_535), Value::NULL]
        );
        let array = UInt32Array::from(vec![Some(0), Some(u32::MAX), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::UInt32), Arc::new(array)),
            vec![Value::UInt(0), Value::UInt(4_294_967_295), Value::NULL]
        );
        let array = UInt64Array::from(vec![Some(0), Some(u64::MAX), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::UInt64), Arc::new(array)),
            vec![Value::UInt(0), Value::UInt(u64::MAX), Value::NULL]
        );
    }

    #[test]
    fn a_float32_is_bound_as_a_float() {
        let array = Float32Array::from(vec![Some(1.5_f32), Some(-0.0), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Float32), Arc::new(array)),
            vec![Value::Float(1.5), Value::Float(-0.0), Value::NULL]
        );
    }

    #[test]
    fn a_float64_is_bound_as_a_double() {
        let array = Float64Array::from(vec![Some(f64::MAX), Some(f64::MIN_POSITIVE), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Float64), Arc::new(array)),
            vec![
                Value::Double(f64::MAX),
                Value::Double(f64::MIN_POSITIVE),
                Value::NULL
            ]
        );
    }

    #[test]
    fn a_float32_that_is_not_finite_is_refused() {
        let spec = spec("c", MariadbFieldType::Float32);
        let array: ArrayRef = Arc::new(Float32Array::from(vec![
            1.0,
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
        ]));
        assert_eq!(
            refusal(&spec, array.clone(), 1),
            "MariadbSink: column 'c' row 1: NaN has no MariaDB or MySQL representation"
        );
        assert!(
            refusal(&spec, array.clone(), 2)
                .ends_with("row 2: inf has no MariaDB or MySQL representation")
        );
        assert!(
            refusal(&spec, array, 3)
                .ends_with("row 3: -inf has no MariaDB or MySQL representation")
        );
    }

    #[test]
    fn a_float64_that_is_not_finite_is_refused() {
        let spec = spec("c", MariadbFieldType::Float64);
        let array: ArrayRef = Arc::new(Float64Array::from(vec![
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ]));
        assert_eq!(
            refusal(&spec, array.clone(), 0),
            "MariadbSink: column 'c' row 0: NaN has no MariaDB or MySQL representation"
        );
        assert!(
            refusal(&spec, array.clone(), 1)
                .ends_with("row 1: inf has no MariaDB or MySQL representation")
        );
        assert!(
            refusal(&spec, array, 2)
                .ends_with("row 2: -inf has no MariaDB or MySQL representation")
        );
    }

    #[test]
    fn a_null_float_slot_is_not_checked_for_finiteness() {
        let array = Float64Array::from(vec![None, Some(2.0)]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Float64), Arc::new(array)),
            vec![Value::NULL, Value::Double(2.0)]
        );
    }

    #[test]
    fn a_decimal_below_one_has_a_leading_zero_and_every_fraction_digit() {
        assert_eq!(decimal_text(5, 2), "0.05");
        assert_eq!(decimal_text(0, 2), "0.00");
        assert_eq!(decimal_text(-5, 2), "-0.05");
        assert_eq!(decimal_text(12, 2), "0.12");
    }

    #[test]
    fn a_decimal_keeps_its_trailing_zeros() {
        assert_eq!(decimal_text(12_300, 2), "123.00");
        assert_eq!(decimal_text(100, 2), "1.00");
    }

    #[test]
    fn a_negative_decimal_carries_one_sign() {
        assert_eq!(decimal_text(-12_345, 2), "-123.45");
        assert_eq!(decimal_text(-100, 2), "-1.00");
    }

    #[test]
    fn a_decimal_of_scale_zero_has_no_point() {
        assert_eq!(decimal_text(42, 0), "42");
        assert_eq!(decimal_text(-42, 0), "-42");
        assert_eq!(decimal_text(0, 0), "0");
    }

    #[test]
    fn a_decimal_of_scale_ten_pads_the_fraction() {
        assert_eq!(decimal_text(1, 10), "0.0000000001");
        assert_eq!(decimal_text(12_345_678_901, 10), "1.2345678901");
        assert_eq!(decimal_text(123_456_789_012, 10), "12.3456789012");
    }

    #[test]
    fn a_decimal_with_a_negative_scale_is_written_as_an_integer() {
        assert_eq!(decimal_text(12, -2), "1200");
        assert_eq!(decimal_text(-12, -2), "-1200");
        assert_eq!(decimal_text(0, -2), "0");
    }

    #[test]
    fn the_smallest_decimal_does_not_overflow() {
        assert_eq!(
            decimal_text(i128::MIN, 0),
            "-170141183460469231731687303715884105728"
        );
        assert_eq!(
            decimal_text(i128::MIN, 10),
            "-17014118346046923173168730371.5884105728"
        );
        assert_eq!(
            decimal_text(i128::MAX, 38),
            "1.70141183460469231731687303715884105727"
        );
    }

    #[test]
    fn a_decimal_column_is_bound_as_text_at_the_declared_scale() {
        let array = Decimal128Array::from(vec![Some(-12_345), Some(7), None])
            .with_precision_and_scale(18, 2)
            .expect("decimal");
        assert_eq!(
            encode(&decimal_spec("c", 18, 2), Arc::new(array)),
            vec![
                Value::Bytes(b"-123.45".to_vec()),
                Value::Bytes(b"0.07".to_vec()),
                Value::NULL
            ]
        );
    }

    #[test]
    fn a_text_column_is_bound_as_its_utf8_bytes() {
        let array = StringArray::from(vec![Some("héllo"), Some(""), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Utf8), Arc::new(array)),
            vec![
                Value::Bytes("héllo".as_bytes().to_vec()),
                Value::Bytes(Vec::new()),
                Value::NULL
            ]
        );
    }

    #[test]
    fn a_json_column_is_bound_as_its_document_text() {
        let array = StringArray::from(vec![Some(r#"{"a":[1,2]}"#), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Json), Arc::new(array)),
            vec![Value::Bytes(br#"{"a":[1,2]}"#.to_vec()), Value::NULL]
        );
    }

    #[test]
    fn a_binary_column_is_bound_as_its_bytes() {
        let array = BinaryArray::from(vec![Some(&[0_u8, 255, 7][..]), Some(&[][..]), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Binary), Arc::new(array)),
            vec![
                Value::Bytes(vec![0, 255, 7]),
                Value::Bytes(Vec::new()),
                Value::NULL
            ]
        );
    }

    #[test]
    fn a_date_at_the_unix_epoch_is_1970_01_01() {
        let array = Date32Array::from(vec![Some(0), Some(19_000), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Date32), Arc::new(array)),
            vec![
                Value::Date(1970, 1, 1, 0, 0, 0, 0),
                Value::Date(2022, 1, 8, 0, 0, 0, 0),
                Value::NULL
            ]
        );
    }

    #[test]
    fn the_first_day_of_year_zero_is_accepted() {
        let array = Date32Array::from(vec![-719_528]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Date32), Arc::new(array)),
            vec![Value::Date(0, 1, 1, 0, 0, 0, 0)]
        );
    }

    #[test]
    fn the_last_day_of_year_9999_is_accepted() {
        let array = Date32Array::from(vec![2_932_896]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Date32), Arc::new(array)),
            vec![Value::Date(9999, 12, 31, 0, 0, 0, 0)]
        );
    }

    #[test]
    fn a_date_in_year_10000_is_refused() {
        let array: ArrayRef = Arc::new(Date32Array::from(vec![0, 2_932_897]));
        assert_eq!(
            refusal(&spec("c", MariadbFieldType::Date32), array, 1),
            "MariadbSink: column 'c' row 1: date 2932897 is outside the years 0000 to 9999 the \
             servers store"
        );
    }

    #[test]
    fn a_date_before_year_zero_is_refused() {
        let array: ArrayRef = Arc::new(Date32Array::from(vec![-719_529]));
        assert_eq!(
            refusal(&spec("c", MariadbFieldType::Date32), array, 0),
            "MariadbSink: column 'c' row 0: date -719529 is outside the years 0000 to 9999 the \
             servers store"
        );
    }

    #[test]
    fn midnight_is_a_time_of_zero() {
        let array = Time64MicrosecondArray::from(vec![Some(0), None]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Time64Micros), Arc::new(array)),
            vec![Value::Time(false, 0, 0, 0, 0, 0), Value::NULL]
        );
    }

    #[test]
    fn the_last_microsecond_of_a_day_is_accepted() {
        let array = Time64MicrosecondArray::from(vec![MICROS_PER_DAY - 1]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Time64Micros), Arc::new(array)),
            vec![Value::Time(false, 0, 23, 59, 59, 999_999)]
        );
    }

    #[test]
    fn a_time_splits_into_hours_minutes_seconds_and_micros() {
        let micros = 13 * MICROS_PER_HOUR + 5 * MICROS_PER_MINUTE + 9 * MICROS_PER_SECOND + 42;
        let array = Time64MicrosecondArray::from(vec![micros]);
        assert_eq!(
            encode(&spec("c", MariadbFieldType::Time64Micros), Arc::new(array)),
            vec![Value::Time(false, 0, 13, 5, 9, 42)]
        );
    }

    #[test]
    fn a_negative_time_is_refused() {
        let array: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![-1]));
        assert_eq!(
            refusal(&spec("c", MariadbFieldType::Time64Micros), array, 0),
            "MariadbSink: column 'c' row 0: time -1 is outside a time of day"
        );
    }

    #[test]
    fn a_time_of_twenty_four_hours_is_refused() {
        let array: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![MICROS_PER_DAY]));
        assert_eq!(
            refusal(&spec("c", MariadbFieldType::Time64Micros), array, 0),
            "MariadbSink: column 'c' row 0: time 86400000000 is outside a time of day"
        );
    }

    #[test]
    fn a_timestamp_at_the_unix_epoch_is_1970_01_01_midnight() {
        let array = TimestampMicrosecondArray::from(vec![Some(0), None]);
        assert_eq!(
            encode(
                &spec("c", MariadbFieldType::TimestampMicros),
                Arc::new(array)
            ),
            vec![Value::Date(1970, 1, 1, 0, 0, 0, 0), Value::NULL]
        );
    }

    #[test]
    fn a_timestamp_splits_into_date_time_and_micros() {
        // 2024-03-05 06:07:08.000009 UTC.
        let micros = 1_709_618_828_000_009_i64;
        let array = TimestampMicrosecondArray::from(vec![micros]);
        assert_eq!(
            encode(
                &spec("c", MariadbFieldType::TimestampMicros),
                Arc::new(array)
            ),
            vec![Value::Date(2024, 3, 5, 6, 7, 8, 9)]
        );
    }

    #[test]
    fn a_timestamp_one_microsecond_before_the_epoch_is_the_last_instant_of_1969() {
        let array = TimestampMicrosecondArray::from(vec![-1]);
        assert_eq!(
            encode(
                &spec("c", MariadbFieldType::TimestampMicros),
                Arc::new(array)
            ),
            vec![Value::Date(1969, 12, 31, 23, 59, 59, 999_999)]
        );
    }

    #[test]
    fn a_timestamp_one_day_before_the_epoch_is_1969_12_31_midnight() {
        let array = TimestampMicrosecondArray::from(vec![-MICROS_PER_DAY]);
        assert_eq!(
            encode(
                &spec("c", MariadbFieldType::TimestampMicros),
                Arc::new(array)
            ),
            vec![Value::Date(1969, 12, 31, 0, 0, 0, 0)]
        );
    }

    #[test]
    fn a_utc_timestamp_field_with_a_timezone_encodes_the_same_wall_clock() {
        let micros = 1_709_618_828_000_009_i64;
        let plain = TimestampMicrosecondArray::from(vec![micros]);
        let zoned = TimestampMicrosecondArray::from(vec![micros]).with_timezone("Asia/Tokyo");
        let utc = encode(
            &spec("c", MariadbFieldType::TimestampMicrosUtc),
            Arc::new(zoned),
        );
        assert_eq!(
            utc,
            encode(
                &spec("c", MariadbFieldType::TimestampMicros),
                Arc::new(plain)
            )
        );
        assert_eq!(utc, vec![Value::Date(2024, 3, 5, 6, 7, 8, 9)]);
    }

    #[test]
    fn a_timestamp_in_year_10000_is_refused() {
        let array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
            2_932_897 * MICROS_PER_DAY,
        ]));
        assert_eq!(
            refusal(&spec("c", MariadbFieldType::TimestampMicros), array, 0),
            "MariadbSink: column 'c' row 0: date 2932897 is outside the years 0000 to 9999 the \
             servers store"
        );
    }

    #[test]
    fn the_smallest_timestamp_is_refused_without_overflow() {
        let array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![i64::MIN]));
        assert!(
            refusal(&spec("c", MariadbFieldType::TimestampMicros), array, 0)
                .contains("is outside the years 0000 to 9999")
        );
    }

    #[test]
    fn a_missing_column_is_refused_with_a_hint_at_the_schema() {
        let batch = one_column("other", Arc::new(Int64Array::from(vec![1])));
        assert_eq!(
            resolve_refusal(&spec("c", MariadbFieldType::Int64), &batch),
            "MariadbSink: batch has no column 'c'; the batch schema must match the declared \
             schema_fields"
        );
    }

    #[test]
    fn a_column_of_another_arrow_type_is_refused() {
        let batch = one_column("c", Arc::new(Int32Array::from(vec![1])));
        assert_eq!(
            resolve_refusal(&spec("c", MariadbFieldType::Int64), &batch),
            "MariadbSink: column 'c' is declared type \"int64\" but the batch holds Int32"
        );
    }

    #[test]
    fn a_json_column_over_a_binary_array_is_refused() {
        let batch = one_column("c", Arc::new(BinaryArray::from(vec![&b"{}"[..]])));
        assert!(
            resolve_refusal(&spec("c", MariadbFieldType::Json), &batch)
                .contains("declared type \"json\" but the batch holds Binary")
        );
    }

    #[test]
    fn a_decimal_array_of_another_scale_is_refused() {
        let array = Decimal128Array::from(vec![1])
            .with_precision_and_scale(18, 2)
            .expect("decimal");
        let batch = one_column("c", Arc::new(array));
        assert_eq!(
            resolve_refusal(&decimal_spec("c", 18, 4), &batch),
            "MariadbSink: column 'c' is declared with scale 4 but the batch holds a decimal128 \
             of scale 2"
        );
    }

    #[test]
    fn a_decimal_declaration_without_a_scale_is_refused() {
        let array = Decimal128Array::from(vec![1])
            .with_precision_and_scale(18, 2)
            .expect("decimal");
        let batch = one_column("c", Arc::new(array));
        let bare = spec("c", MariadbFieldType::Decimal128);
        assert!(
            resolve_refusal(&bare, &batch).contains("requires 'precision'"),
            "a declaration without precision and scale names the missing key"
        );
    }

    #[test]
    fn readers_come_back_in_the_order_the_specs_list_them() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", arrow_schema::DataType::Int64, true),
            Field::new("b", arrow_schema::DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .expect("batch");
        let a = spec("a", MariadbFieldType::Int64);
        let b = spec("b", MariadbFieldType::Utf8);
        let readers = resolve_columns(WHAT, &batch, &[&b, &a]).expect("resolve");
        assert_eq!(
            readers[0].value(0).expect("value"),
            Value::Bytes(b"x".to_vec())
        );
        assert_eq!(readers[1].value(0).expect("value"), Value::Int(1));
    }

    fn two_column_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow_schema::DataType::Int64, true),
            Field::new("name", arrow_schema::DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .expect("batch")
    }

    #[test]
    fn row_values_appends_and_leaves_earlier_content_untouched() {
        let batch = two_column_batch();
        let id = spec("id", MariadbFieldType::Int64);
        let name = spec("name", MariadbFieldType::Utf8);
        let readers = resolve_columns(WHAT, &batch, &[&id, &name]).expect("resolve");

        let mut out = Vec::new();
        row_values(&readers, 0, &mut out).expect("row 0");
        assert_eq!(out, vec![Value::Int(1), Value::Bytes(b"a".to_vec())]);

        row_values(&readers, 1, &mut out).expect("row 1");
        assert_eq!(
            out,
            vec![
                Value::Int(1),
                Value::Bytes(b"a".to_vec()),
                Value::Int(2),
                Value::NULL
            ]
        );
    }

    #[test]
    fn a_refused_row_leaves_the_buffer_as_it_was() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow_schema::DataType::Int64, true),
            Field::new("x", arrow_schema::DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![0.5, f64::NAN])),
            ],
        )
        .expect("batch");
        let id = spec("id", MariadbFieldType::Int64);
        let x = spec("x", MariadbFieldType::Float64);
        let readers = resolve_columns(WHAT, &batch, &[&id, &x]).expect("resolve");

        let mut out = Vec::new();
        row_values(&readers, 0, &mut out).expect("row 0");
        let err = row_values(&readers, 1, &mut out).expect_err("row 1 is refused");

        assert!(err.message().contains("column 'x' row 1: NaN"));
        assert_eq!(out, vec![Value::Int(1), Value::Double(0.5)]);
    }
}
