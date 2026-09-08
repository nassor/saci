//! PostgreSQL values into Arrow arrays, over both wires.
//!
//! One decoder serves all three source modes. `tokio-postgres` requests binary
//! result formats unconditionally, so a query result column arrives as the
//! type's send form and [`ColumnBuilder::push`] frames it; `pgoutput` emits
//! text tuples, so a logical-decoding value arrives as the type's output form
//! and [`ColumnBuilder::push_text`] parses it. [`RawValue`] is the hook that
//! gets at the binary bytes: a `FromSql` newtype that accepts every type and
//! hands back the slice unchanged.
//!
//! Read columns as `Option<RawValue>` so `Option`'s blanket `FromSql` handles
//! SQL NULL; [`RawValue`] itself never implements `from_sql_null`.
//!
//! Every integer on the binary wire is big-endian. PostgreSQL's date and
//! timestamp epoch is 2000-01-01, Arrow's is 1970-01-01, so both gain a fixed
//! offset. The `±infinity` sentinels and the ` BC` era have no Arrow
//! representation and are rejected rather than folded into a real timestamp.

use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, IntervalMonthDayNanoBuilder, StringBuilder, Time64MicrosecondBuilder,
    TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, ListArray};
use arrow_buffer::{IntervalMonthDayNano, NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, FieldRef};
use saci_core::error::SaciError;
use tokio_postgres::types::{FromSql, Type};

use crate::config::{FieldSpec, PgFieldType};
use crate::numeric::{money_factor, numeric_text_to_i128, numeric_to_i128};
use crate::types::{OID_JSONB, array_element_oid};

/// PostgreSQL OIDs this module dispatches on. The canonical table in
/// [`crate::types`] carries the full set; these are the ones whose *decoder*
/// differs from what the declared type alone implies.
const OID_OID: u32 = 26;
const OID_XID: u32 = 28;
const OID_CID: u32 = 29;
const OID_MONEY: u32 = 790;

/// Days between the Arrow `Date32` epoch (1970-01-01) and PostgreSQL's
/// (2000-01-01).
pub(crate) const DATE_EPOCH_OFFSET_DAYS: i32 = 10_957;

/// Microseconds between the Arrow timestamp epoch (1970-01-01T00:00:00Z) and
/// PostgreSQL's (2000-01-01T00:00:00Z).
pub(crate) const TIMESTAMP_EPOCH_OFFSET_MICROS: i64 = 946_684_800_000_000;

/// Microseconds in one day, the step from a civil day count to an Arrow
/// timestamp.
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// A column value as the server sent it, with no interpretation.
///
/// Accepts every type so one read path covers every column, which is what lets
/// the pgoutput decoder share this module with the query path.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RawValue<'a>(pub(crate) &'a [u8]);

impl<'a> FromSql<'a> for RawValue<'a> {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(RawValue(raw))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

/// One Arrow builder per declared column, dispatched once at construction.
pub(crate) enum ColumnBuilder {
    /// `bool`.
    Boolean(BooleanBuilder),
    /// `int2`.
    Int16(Int16Builder),
    /// `int4`.
    Int32(Int32Builder),
    /// `int8`.
    Int64(Int64Builder),
    /// `float4`.
    Float32(Float32Builder),
    /// `float8`.
    Float64(Float64Builder),
    /// `text`, `varchar`, `bpchar`, `name`.
    Utf8(StringBuilder),
    /// `json`, or `jsonb` when the flag is set. `jsonb` prefixes the document
    /// with a version byte that `json` does not carry, and the two share an
    /// OID-indistinguishable Arrow type, so the flag comes from the server's
    /// column type rather than from a guess about the payload.
    Json(StringBuilder, bool),
    /// `bytea`.
    Binary(BinaryBuilder),
    /// `date`.
    Date32(Date32Builder),
    /// `time`.
    Time64(Time64MicrosecondBuilder),
    /// `timestamp` and `timestamptz`; the timezone lives in the data type.
    Timestamp(TimestampMicrosecondBuilder),
    /// `uuid`.
    Uuid(FixedSizeBinaryBuilder),
    /// `numeric`, rescaled to the declared scale.
    Decimal128(Decimal128Builder, i8),
    /// `money`: an `i64` of hundredths on the wire, rescaled to the declared
    /// scale.
    ///
    /// `lc_monetary = 'C'` is pinned on every session, which fixes the wire
    /// value at two fractional digits; the declared scale may be wider, and
    /// the factor carried here is `10^(scale - 2)`. A narrower scale is
    /// refused at construction rather than rounded away.
    Money(Decimal128Builder, i128, i8),
    /// `oid`, `xid`, `cid`: 4 bytes unsigned on the wire, widened to `i64`
    /// losslessly. A separate variant because the width differs from `int8`'s
    /// while the declared type does not.
    Oid(Int64Builder),
    /// `interval`: months, days and microseconds, kept apart.
    Interval(IntervalMonthDayNanoBuilder),
    /// A one-dimensional array.
    List(Box<ListState>),
}

/// The parts of a `ListArray` under construction.
///
/// Hand-assembled rather than an `arrow` `ListBuilder`, because the element
/// decoder is a [`ColumnBuilder`] -- this module's own dispatch -- and not an
/// `ArrayBuilder`.
pub(crate) struct ListState {
    /// Decodes each element into the values array.
    element: ColumnBuilder,
    /// The Arrow field the values array must carry.
    field: FieldRef,
    /// Offsets into the values array, one more than the row count.
    offsets: Vec<i32>,
    /// Whether each row's list is present.
    validity: Vec<bool>,
}

impl ColumnBuilder {
    /// A builder for `spec`, sized for `capacity` rows.
    ///
    /// `server_oid` is the PostgreSQL type OID of the column this builder is
    /// filled from, already checked by
    /// [`types::validate_columns`](crate::types::validate_columns). Pass 0 for
    /// the reserved metadata columns, which no server column backs.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `spec` declares an illegal
    /// `decimal128` precision or scale.
    pub(crate) fn new(
        spec: &FieldSpec,
        capacity: usize,
        server_oid: u32,
    ) -> Result<Self, SaciError> {
        Self::for_type(spec, spec.ty, capacity, server_oid)
    }

    /// [`new`](Self::new) for one declared type, so a `list` can build its own
    /// element decoder from the same table.
    fn for_type(
        spec: &FieldSpec,
        ty: PgFieldType,
        capacity: usize,
        server_oid: u32,
    ) -> Result<Self, SaciError> {
        Ok(match ty {
            PgFieldType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
            PgFieldType::Int16 => ColumnBuilder::Int16(Int16Builder::with_capacity(capacity)),
            PgFieldType::Int32 => ColumnBuilder::Int32(Int32Builder::with_capacity(capacity)),
            PgFieldType::Int64 if matches!(server_oid, OID_OID | OID_XID | OID_CID) => {
                ColumnBuilder::Oid(Int64Builder::with_capacity(capacity))
            }
            PgFieldType::Int64 => ColumnBuilder::Int64(Int64Builder::with_capacity(capacity)),
            PgFieldType::Float32 => ColumnBuilder::Float32(Float32Builder::with_capacity(capacity)),
            PgFieldType::Float64 => ColumnBuilder::Float64(Float64Builder::with_capacity(capacity)),
            // 16 bytes per value is a starting guess for the data buffer; the
            // builder grows it as needed.
            PgFieldType::Utf8 => {
                ColumnBuilder::Utf8(StringBuilder::with_capacity(capacity, capacity * 16))
            }
            PgFieldType::Json => ColumnBuilder::Json(
                StringBuilder::with_capacity(capacity, capacity * 16),
                server_oid == OID_JSONB,
            ),
            PgFieldType::Binary => {
                ColumnBuilder::Binary(BinaryBuilder::with_capacity(capacity, capacity * 16))
            }
            PgFieldType::Date32 => ColumnBuilder::Date32(Date32Builder::with_capacity(capacity)),
            PgFieldType::Time64Micros => {
                ColumnBuilder::Time64(Time64MicrosecondBuilder::with_capacity(capacity))
            }
            PgFieldType::TimestampMicros => {
                ColumnBuilder::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            PgFieldType::TimestampMicrosUtc => ColumnBuilder::Timestamp(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone("UTC"),
            ),
            PgFieldType::Uuid => {
                ColumnBuilder::Uuid(FixedSizeBinaryBuilder::with_capacity(capacity, 16))
            }
            PgFieldType::Decimal128 => {
                let (precision, scale) = spec.decimal_params()?;
                let builder = Decimal128Builder::with_capacity(capacity)
                    .with_precision_and_scale(precision, scale)
                    .map_err(|e| {
                        SaciError::configuration(format!(
                            "field '{}': invalid decimal128 precision/scale: {e}",
                            spec.name
                        ))
                    })?;
                if server_oid == OID_MONEY {
                    let factor = money_factor(scale).map_err(|reason| {
                        SaciError::configuration(format!("field '{}': {reason}", spec.name))
                    })?;
                    ColumnBuilder::Money(builder, factor, scale)
                } else {
                    ColumnBuilder::Decimal128(builder, scale)
                }
            }
            PgFieldType::IntervalMonthDayNano => {
                ColumnBuilder::Interval(IntervalMonthDayNanoBuilder::with_capacity(capacity))
            }
            PgFieldType::List => {
                let item = spec.item.ok_or_else(|| {
                    SaciError::configuration(format!(
                        "field '{}': type \"list\" requires 'item'",
                        spec.name
                    ))
                })?;
                if item == PgFieldType::List {
                    return Err(SaciError::configuration(format!(
                        "field '{}': 'item' must be a scalar type, not \"list\"",
                        spec.name
                    )));
                }
                // The element decoder needs the *element's* OID, not the
                // array's: `_jsonb` elements carry a version byte `_json`
                // elements do not, and `_oid`/`_money` elements are neither an
                // `int8` nor a `numeric`.
                let element_oid = array_element_oid(server_oid).unwrap_or(0);
                let element = Self::for_type(spec, item, capacity, element_oid)?;
                let data_type = spec.to_arrow_field()?.data_type().clone();
                let DataType::List(field) = data_type else {
                    return Err(SaciError::configuration(format!(
                        "field '{}': type \"list\" must map to an Arrow List",
                        spec.name
                    )));
                };
                ColumnBuilder::List(Box::new(ListState {
                    element,
                    field,
                    offsets: vec![0],
                    validity: Vec::with_capacity(capacity),
                }))
            }
        })
    }

    /// Decode one PostgreSQL binary value, or append NULL for `None`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming `column` for a value that is not
    /// this type's binary form, or that holds something the Arrow type cannot
    /// represent: a wrong length, an out-of-range discriminant, invalid UTF-8,
    /// a temporal infinity sentinel, an epoch or scale overflow, or a
    /// multi-dimensional array.
    // Inlined into the per-value loop of every source mode. Only effective because the fixed-width
    // refusal is outlined into `wrong_width`: the `format!` in `fixed`'s body would grow this frame
    // past the inlining threshold, costing every value a call plus a 32-byte `Result` return through
    // a hidden out-pointer. Removing it measured 1.2x to 1.3x per value on the fixed-width types.
    #[inline]
    pub(crate) fn push(&mut self, column: &str, raw: Option<&[u8]>) -> Result<(), SaciError> {
        let Some(raw) = raw else {
            self.push_null();
            return Ok(());
        };

        match self {
            ColumnBuilder::Boolean(builder) => {
                let byte = one(column, raw, "bool")?;
                match byte {
                    0 => builder.append_value(false),
                    1 => builder.append_value(true),
                    other => {
                        return Err(SaciError::generic(format!(
                            "column '{column}': bool value is {other}, expected 0 or 1"
                        )));
                    }
                }
            }
            ColumnBuilder::Int16(builder) => {
                builder.append_value(i16::from_be_bytes(fixed::<2>(column, raw, "int2")?));
            }
            ColumnBuilder::Int32(builder) => {
                builder.append_value(i32::from_be_bytes(fixed::<4>(column, raw, "int4")?));
            }
            ColumnBuilder::Int64(builder) => {
                builder.append_value(i64::from_be_bytes(fixed::<8>(column, raw, "int8")?));
            }
            ColumnBuilder::Float32(builder) => {
                builder.append_value(f32::from_bits(u32::from_be_bytes(fixed::<4>(
                    column, raw, "float4",
                )?)));
            }
            ColumnBuilder::Float64(builder) => {
                builder.append_value(f64::from_bits(u64::from_be_bytes(fixed::<8>(
                    column, raw, "float8",
                )?)));
            }
            ColumnBuilder::Utf8(builder) => builder.append_value(utf8(column, raw)?),
            ColumnBuilder::Json(builder, jsonb) => {
                // `jsonb` on the wire is a one-byte version followed by the
                // document; `json` is the document alone. Version 1 is the only
                // one PostgreSQL has ever emitted, and a later version would
                // have a layout this decoder does not know.
                let text = if *jsonb {
                    match raw.split_first() {
                        Some((1, rest)) => utf8(column, rest)?,
                        Some((version, _)) => {
                            return Err(SaciError::generic(format!(
                                "column '{column}': jsonb version byte is {version}, expected 1"
                            )));
                        }
                        None => {
                            return Err(SaciError::generic(format!(
                                "column '{column}': jsonb value is empty, expected at least a \
                                 version byte"
                            )));
                        }
                    }
                } else {
                    utf8(column, raw)?
                };
                builder.append_value(text);
            }
            ColumnBuilder::Binary(builder) => builder.append_value(raw),
            ColumnBuilder::Date32(builder) => {
                let days = i32::from_be_bytes(fixed::<4>(column, raw, "date")?);
                if days == i32::MIN || days == i32::MAX {
                    return Err(SaciError::generic(format!(
                        "column '{column}': date is {}infinity, which has no Date32 \
                         representation; exclude the row or select the column with a ::text cast",
                        if days == i32::MIN { "-" } else { "" }
                    )));
                }
                builder.append_value(days.checked_add(DATE_EPOCH_OFFSET_DAYS).ok_or_else(
                    || {
                        SaciError::generic(format!(
                            "column '{column}': date {days} overflows Date32 after rebasing to the \
                         1970-01-01 epoch"
                        ))
                    },
                )?);
            }
            ColumnBuilder::Time64(builder) => {
                builder.append_value(i64::from_be_bytes(fixed::<8>(column, raw, "time")?));
            }
            ColumnBuilder::Timestamp(builder) => {
                let micros = i64::from_be_bytes(fixed::<8>(column, raw, "timestamp")?);
                if micros == i64::MIN || micros == i64::MAX {
                    return Err(SaciError::generic(format!(
                        "column '{column}': timestamp is {}infinity, which has no Arrow \
                         representation; exclude the row or select the column with a ::text cast",
                        if micros == i64::MIN { "-" } else { "" }
                    )));
                }
                builder.append_value(
                    micros
                        .checked_add(TIMESTAMP_EPOCH_OFFSET_MICROS)
                        .ok_or_else(|| {
                            SaciError::generic(format!(
                                "column '{column}': timestamp {micros} overflows an i64 after \
                                 rebasing to the 1970-01-01 epoch"
                            ))
                        })?,
                );
            }
            ColumnBuilder::Uuid(builder) => {
                let bytes = fixed::<16>(column, raw, "uuid")?;
                builder.append_value(bytes).map_err(|e| {
                    SaciError::generic(format!("column '{column}': cannot append uuid: {e}"))
                })?;
            }
            ColumnBuilder::Decimal128(builder, scale) => {
                builder.append_value(numeric_to_i128(raw, *scale, column)?);
            }
            ColumnBuilder::Money(builder, factor, _) => {
                // `lc_monetary = 'C'` fixes the wire value at two fractional
                // digits; the declared scale may be wider.
                let units = i128::from(i64::from_be_bytes(fixed::<8>(column, raw, "money")?));
                builder.append_value(units.checked_mul(*factor).ok_or_else(|| {
                    SaciError::generic(format!(
                        "column '{column}': money {units} does not fit a 128-bit decimal at the \
                         declared scale"
                    ))
                })?);
            }
            ColumnBuilder::Oid(builder) => {
                let oid = u32::from_be_bytes(fixed::<4>(column, raw, "oid")?);
                builder.append_value(i64::from(oid));
            }
            ColumnBuilder::Interval(builder) => {
                let bytes = fixed::<16>(column, raw, "interval")?;
                let micros = i64::from_be_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                let days = i32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
                let months = i32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
                builder.append_value(interval_value(column, months, days, micros)?);
            }
            ColumnBuilder::List(state) => {
                let ListState { element, .. } = &mut **state;
                for_each_binary_element(column, raw, |value| element.push(column, value))?;
                state.close_present_row(column)?;
            }
        }
        Ok(())
    }

    /// Append NULL.
    ///
    /// Used for pgoutput's `'n'` and `'u'` tuple markers, and for the columns a
    /// `DELETE` old tuple does not carry. Infallible for every declared type,
    /// including a `list`: a NULL row repeats the previous offset rather than
    /// computing a new one.
    pub(crate) fn push_null(&mut self) {
        match self {
            ColumnBuilder::Boolean(b) => b.append_null(),
            ColumnBuilder::Int16(b) => b.append_null(),
            ColumnBuilder::Int32(b) => b.append_null(),
            ColumnBuilder::Int64(b) => b.append_null(),
            ColumnBuilder::Float32(b) => b.append_null(),
            ColumnBuilder::Float64(b) => b.append_null(),
            ColumnBuilder::Utf8(b) | ColumnBuilder::Json(b, _) => b.append_null(),
            ColumnBuilder::Binary(b) => b.append_null(),
            ColumnBuilder::Date32(b) => b.append_null(),
            ColumnBuilder::Time64(b) => b.append_null(),
            ColumnBuilder::Timestamp(b) => b.append_null(),
            ColumnBuilder::Uuid(b) => b.append_null(),
            ColumnBuilder::Decimal128(b, _) => b.append_null(),
            ColumnBuilder::Money(b, _, _) => b.append_null(),
            ColumnBuilder::Oid(b) => b.append_null(),
            ColumnBuilder::Interval(b) => b.append_null(),
            ColumnBuilder::List(state) => state.close_null_row(),
        }
    }

    /// Append a `&str`, for the reserved metadata columns the connector fills
    /// itself rather than reading from a tuple.
    pub(crate) fn push_str(&mut self, column: &str, value: &str) -> Result<(), SaciError> {
        match self {
            ColumnBuilder::Utf8(b) | ColumnBuilder::Json(b, _) => {
                b.append_value(value);
                Ok(())
            }
            _ => Err(SaciError::generic(format!(
                "column '{column}': expected a utf8 builder for a connector-supplied string"
            ))),
        }
    }

    /// Append an `i64`, for the reserved metadata columns.
    pub(crate) fn push_i64(&mut self, column: &str, value: i64) -> Result<(), SaciError> {
        match self {
            ColumnBuilder::Int64(b) => {
                b.append_value(value);
                Ok(())
            }
            ColumnBuilder::Timestamp(b) => {
                b.append_value(value);
                Ok(())
            }
            _ => Err(SaciError::generic(format!(
                "column '{column}': expected an int64 or timestamp builder for a \
                 connector-supplied integer"
            ))),
        }
    }

    /// Finish the builder, resetting it for the next batch.
    pub(crate) fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Boolean(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Int16(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Int32(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Int64(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Float32(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Float64(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Utf8(b) | ColumnBuilder::Json(b, _) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Binary(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Date32(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Time64(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Timestamp(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Uuid(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Decimal128(b, _) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Money(b, _, _) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Oid(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::Interval(b) => Arc::new(b.finish()) as ArrayRef,
            ColumnBuilder::List(state) => state.finish(),
        }
    }

    /// How many values the builder holds.
    fn len(&self) -> usize {
        match self {
            ColumnBuilder::Boolean(b) => b.len(),
            ColumnBuilder::Int16(b) => b.len(),
            ColumnBuilder::Int32(b) => b.len(),
            ColumnBuilder::Int64(b) | ColumnBuilder::Oid(b) => b.len(),
            ColumnBuilder::Float32(b) => b.len(),
            ColumnBuilder::Float64(b) => b.len(),
            ColumnBuilder::Utf8(b) | ColumnBuilder::Json(b, _) => b.len(),
            ColumnBuilder::Binary(b) => b.len(),
            ColumnBuilder::Date32(b) => b.len(),
            ColumnBuilder::Time64(b) => b.len(),
            ColumnBuilder::Timestamp(b) => b.len(),
            ColumnBuilder::Uuid(b) => b.len(),
            ColumnBuilder::Decimal128(b, _) | ColumnBuilder::Money(b, _, _) => b.len(),
            ColumnBuilder::Interval(b) => b.len(),
            ColumnBuilder::List(state) => state.validity.len(),
        }
    }

    /// Decode one value from PostgreSQL's canonical **text** form, or append
    /// NULL for `None`.
    ///
    /// The counterpart of [`push`](Self::push) for `pgoutput`'s text tuples and
    /// for a column the query path renders through its output function. Every
    /// session pins the output settings this parser assumes -- see
    /// `connection::OUTPUT_SETTINGS`.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming `column` when the text is not the
    /// canonical form of the declared type, or holds a value the Arrow type
    /// cannot represent.
    pub(crate) fn push_text(&mut self, column: &str, raw: Option<&[u8]>) -> Result<(), SaciError> {
        let Some(raw) = raw else {
            self.push_null();
            return Ok(());
        };
        let text = utf8(column, raw)?;

        match self {
            // Every type whose declared form is `utf8` is already canonical
            // text, which is the whole point of the text wire.
            ColumnBuilder::Utf8(builder) => builder.append_value(text),
            // `jsonb`'s text form carries no version byte, unlike its binary
            // form, so both `json` and `jsonb` are verbatim here.
            ColumnBuilder::Json(builder, _) => builder.append_value(text),
            ColumnBuilder::Boolean(builder) => match text {
                // The output function is always `t`/`f`, never `true`/`false`.
                "t" => builder.append_value(true),
                "f" => builder.append_value(false),
                other => {
                    return Err(SaciError::generic(format!(
                        "column '{column}': bool text is '{other}', expected 't' or 'f'"
                    )));
                }
            },
            ColumnBuilder::Int16(builder) => builder.append_value(parse(column, text, "int2")?),
            ColumnBuilder::Int32(builder) => builder.append_value(parse(column, text, "int4")?),
            ColumnBuilder::Int64(builder) => builder.append_value(parse(column, text, "int8")?),
            ColumnBuilder::Oid(builder) => {
                let oid: u32 = parse(column, text, "oid")?;
                builder.append_value(i64::from(oid));
            }
            ColumnBuilder::Float32(builder) => builder.append_value(parse(column, text, "float4")?),
            ColumnBuilder::Float64(builder) => builder.append_value(parse(column, text, "float8")?),
            ColumnBuilder::Binary(builder) => builder.append_value(bytea_from_text(column, text)?),
            ColumnBuilder::Uuid(builder) => {
                let bytes = uuid_from_text(column, text)?;
                builder.append_value(bytes).map_err(|e| {
                    SaciError::generic(format!("column '{column}': cannot append uuid: {e}"))
                })?;
            }
            ColumnBuilder::Date32(builder) => {
                builder.append_value(date_from_text(column, text)?);
            }
            ColumnBuilder::Time64(builder) => {
                builder.append_value(time_from_text(column, text)?);
            }
            ColumnBuilder::Timestamp(builder) => {
                builder.append_value(timestamp_from_text(column, text)?);
            }
            ColumnBuilder::Decimal128(builder, scale) => {
                builder.append_value(numeric_text_to_i128(text, *scale, column)?);
            }
            ColumnBuilder::Money(builder, _, scale) => {
                // `$1,234.56`, or `-$1,234.56`: the sign leads, then the
                // currency symbol `lc_monetary = 'C'` fixes to `$`. The parser
                // pads to the declared scale itself.
                let (sign, rest) = match text.strip_prefix('-') {
                    Some(rest) => (-1i128, rest),
                    None => (1i128, text),
                };
                let digits = rest.strip_prefix('$').unwrap_or(rest);
                builder.append_value(sign * numeric_text_to_i128(digits, *scale, column)?);
            }
            ColumnBuilder::Interval(builder) => {
                let (months, days, micros) = interval_from_text(column, text)?;
                builder.append_value(interval_value(column, months, days, micros)?);
            }
            ColumnBuilder::List(state) => {
                let ListState { element, .. } = &mut **state;
                for_each_text_element(column, text, |value| match value {
                    Some(value) => element.push_text(column, Some(value.as_bytes())),
                    None => {
                        element.push_null();
                        Ok(())
                    }
                })?;
                state.close_present_row(column)?;
            }
        }
        Ok(())
    }
}

impl ListState {
    /// Close a NULL row, which contributes no elements.
    ///
    /// Infallible by construction: the offset it repeats is the previous one,
    /// which is already an `i32`.
    fn close_null_row(&mut self) {
        let end = self.offsets.last().copied().unwrap_or(0);
        self.offsets.push(end);
        self.validity.push(false);
    }

    /// Close a present row, whose elements the caller has just pushed.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming `column` when the elements
    /// accumulated so far overflow an `i32` offset, which is the one way this
    /// module could otherwise emit a `ListArray` whose offsets do not
    /// describe its values.
    fn close_present_row(&mut self, column: &str) -> Result<(), SaciError> {
        // `element` already holds this row's values, so the offset is simply
        // wherever it now ends.
        let len = i32::try_from(self.element_len()).map_err(|_| {
            SaciError::generic(format!(
                "column '{column}': the batch's array elements overflow a 32-bit list offset; \
                 lower 'batch_rows'"
            ))
        })?;
        self.offsets.push(len);
        self.validity.push(true);
        Ok(())
    }

    /// How many elements the values builder holds so far.
    fn element_len(&self) -> usize {
        self.element.len()
    }

    /// Assemble the `ListArray` and reset for the next batch.
    fn finish(&mut self) -> ArrayRef {
        let values = self.element.finish();
        let offsets = std::mem::replace(&mut self.offsets, vec![0]);
        let validity: Vec<bool> = std::mem::take(&mut self.validity);
        let nulls = NullBuffer::from(validity);
        // Every offset is produced by `close_row` from the values array's own
        // length, monotonically, so the buffer is valid by construction.
        let array = ListArray::new(
            Arc::clone(&self.field),
            OffsetBuffer::new(offsets.into()),
            values,
            Some(nulls),
        );
        Arc::new(array) as ArrayRef
    }
}

/// Parse one scalar out of PostgreSQL's canonical text.
fn parse<T>(column: &str, text: &str, what: &str) -> Result<T, SaciError>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    text.trim().parse::<T>().map_err(|e| {
        SaciError::generic(format!(
            "column '{column}': '{text}' is not a {what} value: {e}"
        ))
    })
}

/// Refuse the renders no Arrow temporal type can hold: the `±infinity`
/// sentinels, and an era before year 1, which PostgreSQL marks with a
/// trailing ` BC`.
fn reject_unrepresentable(column: &str, text: &str, what: &str) -> Result<(), SaciError> {
    if let Some(stripped) = text.strip_suffix(" BC") {
        return Err(SaciError::generic(format!(
            "column '{column}': {what} '{stripped}' is in the BC era, which has no Arrow \
             representation; declare type = \"utf8\" to carry it as text"
        )));
    }
    if text.ends_with("infinity") {
        return Err(SaciError::generic(format!(
            "column '{column}': {what} is {text}, which has no Arrow representation; declare \
             type = \"utf8\" to carry it as text"
        )));
    }
    Ok(())
}

/// `YYYY-MM-DD` to days since 1970-01-01.
fn date_from_text(column: &str, text: &str) -> Result<i32, SaciError> {
    reject_unrepresentable(column, text, "date")?;
    let (year, month, day) = civil_from_text(text).ok_or_else(|| {
        SaciError::generic(format!(
            "column '{column}': '{text}' is not a date in the pinned ISO output style"
        ))
    })?;
    i32::try_from(days_from_civil(year, month, day)).map_err(|_| {
        SaciError::generic(format!(
            "column '{column}': date '{text}' is further from 1970-01-01 than a 32-bit day \
             count reaches"
        ))
    })
}

/// `YYYY-MM-DD HH:MM:SS[.f{1,6}]`, with a `timestamptz`'s rendered offset
/// allowed on the end, to microseconds since 1970-01-01T00:00:00Z.
///
/// Hand-parsed, like [`time_from_text`]. The obvious alternative, an ISO-8601
/// parser from the Arrow crates, routes a timestamp through `i64` nanoseconds
/// and so spans only 1677-09-21 to 2262-04-11, while `timestamp` reaches the
/// year 294276 and the binary wire reads all of it. The two source wires have
/// to agree on every value a column can hold, so the text wire parses the
/// same range.
fn timestamp_from_text(column: &str, text: &str) -> Result<i64, SaciError> {
    reject_unrepresentable(column, text, "timestamp")?;
    let bad = || {
        SaciError::generic(format!(
            "column '{column}': '{text}' is not a timestamp in the pinned ISO output style"
        ))
    };
    let (date, clock) = text.split_once(' ').ok_or_else(bad)?;
    let (year, month, day) = civil_from_text(date).ok_or_else(bad)?;
    // Neither the hours field nor the fraction carries a sign, so the first
    // `+` or `-` in the clock part opens a `timestamptz`'s UTC offset.
    let (clock, offset) = match clock.find(['+', '-']) {
        Some(at) => (&clock[..at], Some(&clock[at..])),
        None => (clock, None),
    };
    // `timezone = 'UTC'` pins that offset to zero on every session, so a
    // non-zero one means the session settings did not take. Applying it
    // silently would shift the instant by however much the server chose.
    if let Some(offset) = offset.filter(|offset| !is_zero_offset(offset)) {
        return Err(SaciError::generic(format!(
            "column '{column}': timestamp '{text}' carries the UTC offset '{offset}'; this \
             connector pins timezone = 'UTC' on every session, so a rendered offset can only \
             be zero"
        )));
    }
    let since_midnight = micros_since_midnight(clock, bad)?;
    // `24:00:00` is a `time` value, never part of a timestamp render.
    if since_midnight >= MICROS_PER_DAY {
        return Err(bad());
    }
    days_from_civil(year, month, day)
        .checked_mul(MICROS_PER_DAY)
        .and_then(|day_micros| day_micros.checked_add(since_midnight))
        .ok_or_else(|| {
            SaciError::generic(format!(
                "column '{column}': timestamp '{text}' is further from 1970-01-01 than \
                 microseconds in an i64 reach"
            ))
        })
}

/// Whether a rendered UTC offset is zero. `+00`, `+00:00` and `-00:00:00`
/// are; a malformed one is not.
fn is_zero_offset(offset: &str) -> bool {
    let Some(fields) = offset.get(1..) else {
        return false;
    };
    !fields.is_empty()
        && fields
            .split(':')
            .all(|field| matches!(field.parse::<u32>(), Ok(0)))
}

/// `YYYY-MM-DD` into its three calendar fields, validated.
///
/// The year field is variable width: PostgreSQL zero-pads it to four digits
/// and widens past that above year 9999, which both `date` (to 5874897-12-31)
/// and `timestamp` (to 294276) reach. A day past the end of its month is
/// refused here rather than wrapping into the next one.
fn civil_from_text(text: &str) -> Option<(i64, u32, u32)> {
    let mut fields = text.split('-');
    let year: i64 = fields.next()?.parse().ok()?;
    let month: u32 = fields.next()?.parse().ok()?;
    let day: u32 = fields.next()?.parse().ok()?;
    if fields.next().is_some() || !(1..=12).contains(&month) {
        return None;
    }
    (1..=days_in_month(year, month))
        .contains(&day)
        .then_some((year, month, day))
}

/// How many days a month has, in the proleptic Gregorian calendar
/// PostgreSQL and [`days_from_civil`] both use.
fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        _ => 31,
    }
}

/// Days from 1970-01-01 to a proleptic-Gregorian civil date, by Howard
/// Hinnant's `days_from_civil`.
///
/// Exact for every year an `i64` holds, where a calendar type bounded by its
/// own epoch is not. The caller has validated the fields through
/// [`civil_from_text`].
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    // A 400-year era starts in March, which puts the leap day last and makes
    // January and February belong to the preceding year.
    let shifted = year - i64::from(month <= 2);
    let era = shifted.div_euclid(400);
    let year_of_era = shifted.rem_euclid(400);
    let month = i64::from(month);
    let day_of_year =
        (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    // 719468 is the day count from 0000-03-01, era zero's first day, to
    // 1970-01-01.
    era * 146_097 + day_of_era - 719_468
}

/// `HH:MM:SS[.ffffff]` to microseconds since midnight.
///
/// Hand-parsed rather than through a wall-clock parser, because PostgreSQL's
/// `time` admits `24:00:00` and an hour field wider than two digits, both of
/// which a wall-clock parser rejects.
fn time_from_text(column: &str, text: &str) -> Result<i64, SaciError> {
    micros_since_midnight(text, || {
        SaciError::generic(format!("column '{column}': '{text}' is not a time value"))
    })
}

/// The clock part of a `time` or a `timestamp`, in microseconds since
/// midnight. `bad` reports the whole value the caller is parsing, which for a
/// timestamp is wider than this clock text.
fn micros_since_midnight<F>(text: &str, bad: F) -> Result<i64, SaciError>
where
    F: Fn() -> SaciError,
{
    let (hours, rest) = text.split_once(':').ok_or_else(&bad)?;
    let (minutes, seconds) = rest.split_once(':').ok_or_else(&bad)?;
    let (seconds, fraction) = match seconds.split_once('.') {
        Some((seconds, fraction)) => (seconds, fraction),
        None => (seconds, ""),
    };

    let hours: i64 = hours.parse().map_err(|_| bad())?;
    let minutes: i64 = minutes.parse().map_err(|_| bad())?;
    let seconds: i64 = seconds.parse().map_err(|_| bad())?;
    if !(0..60).contains(&minutes) || !(0..=60).contains(&seconds) {
        return Err(bad());
    }
    // An `interval` renders as many hours as its own microsecond count holds,
    // so the hour field is not bounded by a day and the widening has to be
    // checked.
    let fraction = fraction_micros(fraction, &bad)?;
    hours
        .checked_mul(3_600)
        .and_then(|hour_seconds| hour_seconds.checked_add(minutes * 60 + seconds))
        .and_then(|seconds| seconds.checked_mul(1_000_000))
        .and_then(|micros| micros.checked_add(fraction))
        .ok_or_else(bad)
}

/// A fractional-seconds string to microseconds, padded or refused.
fn fraction_micros<F>(fraction: &str, bad: F) -> Result<i64, SaciError>
where
    F: Fn() -> SaciError,
{
    if fraction.is_empty() {
        return Ok(0);
    }
    if fraction.len() > 6 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let mut micros: i64 = fraction.parse().map_err(|_| bad())?;
    for _ in fraction.len()..6 {
        micros *= 10;
    }
    Ok(micros)
}

/// `\x0a1b` to bytes. An empty `bytea` renders as the prefix alone.
fn bytea_from_text(column: &str, text: &str) -> Result<Vec<u8>, SaciError> {
    let hex = text.strip_prefix("\\x").ok_or_else(|| {
        SaciError::generic(format!(
            "column '{column}': bytea text '{text}' does not start with the '\\x' prefix that \
             bytea_output = 'hex' produces"
        ))
    })?;
    if hex.len() % 2 != 0 {
        return Err(SaciError::generic(format!(
            "column '{column}': bytea text has an odd number of hex digits"
        )));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for pair in bytes.chunks(2) {
        let hi = hex_digit(column, pair[0])?;
        let lo = hex_digit(column, pair[1])?;
        out.push(hi << 4 | lo);
    }
    Ok(out)
}

fn hex_digit(column: &str, byte: u8) -> Result<u8, SaciError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        other => Err(SaciError::generic(format!(
            "column '{column}': '{}' is not a hex digit",
            char::from(other)
        ))),
    }
}

/// The 36-character hyphenated form to 16 bytes.
fn uuid_from_text(column: &str, text: &str) -> Result<[u8; 16], SaciError> {
    let mut out = [0u8; 16];
    let mut nibbles = text.bytes().filter(|b| *b != b'-');
    for slot in &mut out {
        let (Some(hi), Some(lo)) = (nibbles.next(), nibbles.next()) else {
            return Err(SaciError::generic(format!(
                "column '{column}': uuid text '{text}' has fewer than 32 hex digits"
            )));
        };
        *slot = hex_digit(column, hi)? << 4 | hex_digit(column, lo)?;
    }
    if nibbles.next().is_some() {
        return Err(SaciError::generic(format!(
            "column '{column}': uuid text '{text}' has more than 32 hex digits"
        )));
    }
    Ok(out)
}

/// Combine the three interval components into Arrow's `MonthDayNano`.
///
/// The microsecond component is the only lossy edge: Arrow counts
/// nanoseconds, so a time part beyond about 292 years does not fit. That is a
/// refusal, never a truncation.
fn interval_value(
    column: &str,
    months: i32,
    days: i32,
    micros: i64,
) -> Result<IntervalMonthDayNano, SaciError> {
    let nanoseconds = micros.checked_mul(1_000).ok_or_else(|| {
        SaciError::generic(format!(
            "column '{column}': the interval's time component of {micros} microsecond(s) \
             overflows Arrow's nanosecond field; declare type = \"utf8\" to carry it as text"
        ))
    })?;
    Ok(IntervalMonthDayNano::new(months, days, nanoseconds))
}

/// Parse `intervalstyle = 'postgres'` text into `(months, days, micros)`.
///
/// The grammar is a run of signed `<n> <unit>` pairs followed by an optional
/// signed `HH:MM:SS[.ffffff]`, e.g. `1 year 2 mons 3 days 04:05:06.789` or
/// `-1 years -2 mons +3 days -04:05:06`. PostgreSQL pluralises on the signed
/// value, so `-1 years` is what a value of minus one year renders as; the unit
/// word is therefore matched on its stem.
fn interval_from_text(column: &str, text: &str) -> Result<(i32, i32, i64), SaciError> {
    let bad = || {
        SaciError::generic(format!(
            "column '{column}': '{text}' is not an interval in the pinned 'postgres' style"
        ))
    };
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i64 = 0;

    let mut tokens = text.split_whitespace();
    while let Some(token) = tokens.next() {
        if token.contains(':') {
            let (negative, clock) = match token.strip_prefix('-') {
                Some(rest) => (true, rest),
                None => (false, token.strip_prefix('+').unwrap_or(token)),
            };
            let value = time_from_text(column, clock).map_err(|_| bad())?;
            micros = micros
                .checked_add(if negative { -value } else { value })
                .ok_or_else(bad)?;
            continue;
        }

        let count: i64 = token
            .strip_prefix('+')
            .unwrap_or(token)
            .parse()
            .map_err(|_| bad())?;
        let unit = tokens.next().ok_or_else(bad)?;
        let stem = unit.trim_end_matches('s');
        match stem {
            "year" => {
                months = count
                    .checked_mul(12)
                    .and_then(|m| months.checked_add(m))
                    .ok_or_else(bad)?
            }
            "mon" | "month" => months = months.checked_add(count).ok_or_else(bad)?,
            "day" => days = days.checked_add(count).ok_or_else(bad)?,
            "hour" => {
                micros = count
                    .checked_mul(3_600_000_000)
                    .and_then(|v| micros.checked_add(v))
                    .ok_or_else(bad)?
            }
            "min" | "minute" => {
                micros = count
                    .checked_mul(60_000_000)
                    .and_then(|v| micros.checked_add(v))
                    .ok_or_else(bad)?
            }
            "sec" | "second" => {
                micros = count
                    .checked_mul(1_000_000)
                    .and_then(|v| micros.checked_add(v))
                    .ok_or_else(bad)?
            }
            _ => return Err(bad()),
        }
    }

    let months = i32::try_from(months).map_err(|_| bad())?;
    let days = i32::try_from(days).map_err(|_| bad())?;
    Ok((months, days, micros))
}

/// Hand every element of PostgreSQL's binary array form to `on_element`.
///
/// Layout: `ndim`, a flags word, the element OID, then `(length, lower_bound)`
/// per dimension, then each element as a length prefix and its bytes, with
/// `-1` for NULL.
///
/// A callback rather than a returned `Vec`: the caller pushes each element
/// straight into its own builder, so a list value costs no intermediate
/// allocation at all.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] for a truncated buffer, for an element length
/// that is neither a size nor the `-1` NULL sentinel, and for a
/// multi-dimensional array, which Arrow's `List` cannot hold. Propagates
/// whatever `on_element` returns.
fn for_each_binary_element<'a>(
    column: &str,
    raw: &'a [u8],
    mut on_element: impl FnMut(Option<&'a [u8]>) -> Result<(), SaciError>,
) -> Result<(), SaciError> {
    let bad = |what: &str| {
        SaciError::generic(format!(
            "column '{column}': array value is truncated in its {what}"
        ))
    };
    let mut reader = raw;
    let mut take = |n: usize, what: &str| -> Result<&'a [u8], SaciError> {
        if reader.len() < n {
            return Err(bad(what));
        }
        let (head, tail) = reader.split_at(n);
        reader = tail;
        Ok(head)
    };

    let ndim = i32::from_be_bytes(fixed::<4>(column, take(4, "header")?, "array ndim")?);
    let _flags = take(4, "header")?;
    let _element_oid = take(4, "header")?;
    if ndim == 0 {
        return Ok(());
    }
    if ndim != 1 {
        return Err(SaciError::generic(format!(
            "column '{column}': array has {ndim} dimensions; Arrow's List is one-dimensional, so \
             declare type = \"utf8\" to carry the literal instead"
        )));
    }
    let length = i32::from_be_bytes(fixed::<4>(column, take(4, "dimensions")?, "array length")?);
    let _lower_bound = take(4, "dimensions")?;
    let length = usize::try_from(length).map_err(|_| {
        SaciError::generic(format!(
            "column '{column}': array length {length} is negative"
        ))
    })?;

    for _ in 0..length {
        let size = i32::from_be_bytes(fixed::<4>(column, take(4, "elements")?, "array element")?);
        // `-1` is the only negative length the wire form defines; anything
        // else is a corrupt value, not another spelling of NULL.
        if size == -1 {
            on_element(None)?;
            continue;
        }
        let size = usize::try_from(size).map_err(|_| {
            SaciError::generic(format!(
                "column '{column}': array element length {size} is neither a size nor the -1 \
                 NULL marker"
            ))
        })?;
        on_element(Some(take(size, "elements")?))?;
    }
    Ok(())
}

/// Hand every element of an array literal to `on_element`.
///
/// `{a,"b,c","with \"quote\"",NULL}`: an element is bare or double-quoted,
/// backslash escapes the next character inside quotes, and a bare
/// case-insensitive `NULL` is the SQL null -- a quoted `"NULL"` is the string.
///
/// One `String` is reused across every element rather than one allocated per
/// element, because unescaping is the only reason an owned buffer is needed at
/// all and the callback is done with each element before the next begins.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] for a malformed literal and for a nested
/// brace, which is a multi-dimensional array. Propagates whatever
/// `on_element` returns.
fn for_each_text_element(
    column: &str,
    text: &str,
    mut on_element: impl FnMut(Option<&str>) -> Result<(), SaciError>,
) -> Result<(), SaciError> {
    let bad = || {
        SaciError::generic(format!(
            "column '{column}': '{text}' is not an array literal"
        ))
    };
    // A non-default lower bound is written as a `[lo:hi]=` prefix; more than
    // one bracket pair means more than one dimension.
    let body = match text.strip_prefix('[') {
        Some(rest) => {
            let (dims, rest) = rest.split_once('=').ok_or_else(bad)?;
            if dims.contains('[') {
                return Err(SaciError::generic(format!(
                    "column '{column}': array literal '{text}' is multi-dimensional; Arrow's List \
                     is one-dimensional, so declare type = \"utf8\" to carry the literal instead"
                )));
            }
            rest
        }
        None => text,
    };
    let inner = body
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .ok_or_else(bad)?;
    if inner.is_empty() {
        return Ok(());
    }

    let mut current = String::new();
    let mut quoted = false;
    let mut was_quoted = false;
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' if quoted => current.push(chars.next().ok_or_else(bad)?),
            '"' => {
                quoted = !quoted;
                was_quoted = true;
            }
            '{' if !quoted => {
                return Err(SaciError::generic(format!(
                    "column '{column}': array literal '{text}' is multi-dimensional; Arrow's List \
                     is one-dimensional, so declare type = \"utf8\" to carry the literal instead"
                )));
            }
            ',' if !quoted => {
                on_element(text_element(&current, was_quoted))?;
                current.clear();
                was_quoted = false;
            }
            other => current.push(other),
        }
    }
    if quoted {
        return Err(bad());
    }
    on_element(text_element(&current, was_quoted))
}

/// One array element: a bare `NULL` is the SQL null, a quoted one the string.
fn text_element(value: &str, was_quoted: bool) -> Option<&str> {
    if !was_quoted && value.eq_ignore_ascii_case("null") {
        return None;
    }
    Some(value)
}

/// Read a fixed-width value, naming the column and both lengths on mismatch.
///
/// The refusal itself is [`wrong_width`], out of line: a `format!` left in
/// this function's body puts its argument scratch in the frame of every
/// caller, and the callers are the per-value fixed-width arms of
/// [`ColumnBuilder::push`].
#[inline]
fn fixed<const N: usize>(column: &str, raw: &[u8], what: &str) -> Result<[u8; N], SaciError> {
    match raw.try_into() {
        Ok(bytes) => Ok(bytes),
        Err(_) => Err(wrong_width(column, raw.len(), what, N)),
    }
}

/// A fixed-width value whose length is not the type's.
#[cold]
#[inline(never)]
fn wrong_width(column: &str, len: usize, what: &str, expected: usize) -> SaciError {
    SaciError::generic(format!(
        "column '{column}': {what} value is {len} byte(s), expected {expected}"
    ))
}

fn one(column: &str, raw: &[u8], what: &str) -> Result<u8, SaciError> {
    Ok(fixed::<1>(column, raw, what)?[0])
}

fn utf8<'a>(column: &str, raw: &'a [u8]) -> Result<&'a str, SaciError> {
    std::str::from_utf8(raw).map_err(|e| {
        SaciError::generic(format!("column '{column}': value is not valid UTF-8: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
        Float32Array, Float64Array, Int64Array, StringArray, Time64MicrosecondArray,
        TimestampMicrosecondArray,
    };

    use super::*;
    use crate::config::FieldSpec;

    fn spec(ty: PgFieldType) -> FieldSpec {
        FieldSpec {
            name: "c".to_string(),
            ty,
            nullable: true,
            precision: matches!(ty, PgFieldType::Decimal128).then_some(18),
            scale: matches!(ty, PgFieldType::Decimal128).then_some(4),
            item: None,
            pg_type: None,
        }
    }

    /// The PostgreSQL type OID a declared type is normally fed from. Only the
    /// `json`/`jsonb` split actually changes decoding.
    fn oid(ty: PgFieldType) -> u32 {
        match ty {
            PgFieldType::Boolean => Type::BOOL.oid(),
            PgFieldType::Int16 => Type::INT2.oid(),
            PgFieldType::Int32 => Type::INT4.oid(),
            PgFieldType::Int64 => Type::INT8.oid(),
            PgFieldType::Float32 => Type::FLOAT4.oid(),
            PgFieldType::Float64 => Type::FLOAT8.oid(),
            PgFieldType::Utf8 => Type::TEXT.oid(),
            PgFieldType::Binary => Type::BYTEA.oid(),
            PgFieldType::Date32 => Type::DATE.oid(),
            PgFieldType::Time64Micros => Type::TIME.oid(),
            PgFieldType::TimestampMicros => Type::TIMESTAMP.oid(),
            PgFieldType::TimestampMicrosUtc => Type::TIMESTAMPTZ.oid(),
            PgFieldType::Uuid => Type::UUID.oid(),
            PgFieldType::Json => Type::JSON.oid(),
            PgFieldType::Decimal128 => Type::NUMERIC.oid(),
            PgFieldType::IntervalMonthDayNano => Type::INTERVAL.oid(),
            PgFieldType::List => Type::TEXT_ARRAY.oid(),
        }
    }

    fn builder_for(ty: PgFieldType, capacity: usize) -> ColumnBuilder {
        ColumnBuilder::new(&spec(ty), capacity, oid(ty)).expect("builder")
    }

    fn build(ty: PgFieldType, values: &[Option<&[u8]>]) -> ArrayRef {
        let mut builder = builder_for(ty, values.len());
        for value in values {
            builder.push("c", *value).expect("push");
        }
        builder.finish()
    }

    #[test]
    fn decodes_integers_and_floats() {
        let array = build(PgFieldType::Int64, &[Some(&7i64.to_be_bytes()), None]);
        let ints = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.value(0), 7);
        assert!(ints.is_null(1));

        let array = build(
            PgFieldType::Float64,
            &[Some(&2.5f64.to_bits().to_be_bytes())],
        );
        let floats = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(floats.value(0), 2.5);
    }

    #[test]
    fn decodes_booleans_and_rejects_other_bytes() {
        let array = build(PgFieldType::Boolean, &[Some(&[1]), Some(&[0])]);
        let bools = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(bools.value(0));
        assert!(!bools.value(1));

        let mut builder = builder_for(PgFieldType::Boolean, 1);
        let err = builder.push("flag", Some(&[2])).unwrap_err();
        assert!(err.message().contains("flag"), "{}", err.message());
        assert!(
            err.message().contains("expected 0 or 1"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn timestamptz_zero_is_the_year_2000_in_arrow_epoch_micros() {
        let array = build(
            PgFieldType::TimestampMicrosUtc,
            &[Some(&0i64.to_be_bytes())],
        );
        let stamps = array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(stamps.value(0), TIMESTAMP_EPOCH_OFFSET_MICROS);
        assert_eq!(stamps.value(0), 946_684_800_000_000);
    }

    #[test]
    fn timestamp_infinity_sentinels_are_rejected() {
        for sentinel in [i64::MIN, i64::MAX] {
            let mut builder = builder_for(PgFieldType::TimestampMicros, 1);
            let err = builder
                .push("created_at", Some(&sentinel.to_be_bytes()))
                .unwrap_err();
            assert!(err.message().contains("created_at"), "{}", err.message());
            assert!(err.message().contains("infinity"), "{}", err.message());
        }
    }

    #[test]
    fn date_zero_is_the_year_2000_in_arrow_epoch_days() {
        let array = build(PgFieldType::Date32, &[Some(&0i32.to_be_bytes())]);
        let dates = array.as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(dates.value(0), DATE_EPOCH_OFFSET_DAYS);
    }

    #[test]
    fn date_infinity_sentinels_are_rejected() {
        for sentinel in [i32::MIN, i32::MAX] {
            let mut builder = builder_for(PgFieldType::Date32, 1);
            let err = builder
                .push("day", Some(&sentinel.to_be_bytes()))
                .unwrap_err();
            assert!(err.message().contains("day"), "{}", err.message());
            assert!(err.message().contains("infinity"), "{}", err.message());
        }
    }

    #[test]
    fn time_is_microseconds_since_midnight_unchanged() {
        let array = build(
            PgFieldType::Time64Micros,
            &[Some(&3_600_000_000i64.to_be_bytes())],
        );
        let times = array
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();
        assert_eq!(times.value(0), 3_600_000_000);
    }

    #[test]
    fn uuid_must_be_sixteen_bytes() {
        let bytes = [7u8; 16];
        let array = build(PgFieldType::Uuid, &[Some(&bytes)]);
        let uuids = array
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(uuids.value(0), &bytes);

        let mut builder = builder_for(PgFieldType::Uuid, 1);
        let err = builder.push("uid", Some(&[0u8; 15])).unwrap_err();
        assert!(err.message().contains("uid"), "{}", err.message());
        assert!(err.message().contains("expected 16"), "{}", err.message());
    }

    #[test]
    fn invalid_utf8_in_text_is_rejected() {
        let mut builder = builder_for(PgFieldType::Utf8, 1);
        let err = builder.push("label", Some(&[0xff, 0xfe])).unwrap_err();
        assert!(err.message().contains("label"), "{}", err.message());
        assert!(err.message().contains("UTF-8"), "{}", err.message());
    }

    #[test]
    fn json_and_jsonb_both_yield_the_document_text() {
        let json = br#"{"a":1}"#;
        let array = build(PgFieldType::Json, &[Some(json)]);
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), r#"{"a":1}"#);

        let mut jsonb = vec![1u8];
        jsonb.extend_from_slice(json);
        let mut builder =
            ColumnBuilder::new(&spec(PgFieldType::Json), 1, Type::JSONB.oid()).unwrap();
        builder.push("doc", Some(&jsonb)).unwrap();
        let array = builder.finish();
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), r#"{"a":1}"#);

        // A jsonb version byte the decoder does not know is an error, not a
        // document that silently keeps its header.
        let mut builder =
            ColumnBuilder::new(&spec(PgFieldType::Json), 1, Type::JSONB.oid()).unwrap();
        let mut future = vec![2u8];
        future.extend_from_slice(json);
        let err = builder.push("doc", Some(&future)).unwrap_err();
        assert!(
            err.message().contains("version byte is 2"),
            "{}",
            err.message()
        );
        assert!(err.message().contains("doc"), "{}", err.message());
    }

    #[test]
    fn bytea_is_carried_verbatim() {
        let array = build(PgFieldType::Binary, &[Some(&[0u8, 1, 2, 255])]);
        let blobs = array.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(blobs.value(0), &[0u8, 1, 2, 255]);
    }

    #[test]
    fn numeric_reaches_the_declared_scale() {
        let mut raw = bytes::BytesMut::new();
        crate::numeric::i128_to_numeric(123_456, 4, &mut raw);
        let array = build(PgFieldType::Decimal128, &[Some(&raw)]);
        let decimals = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(decimals.value(0), 123_456);
        assert_eq!(decimals.precision(), 18);
        assert_eq!(decimals.scale(), 4);
    }

    #[test]
    fn reserved_column_helpers_reject_the_wrong_builder() {
        let mut builder = builder_for(PgFieldType::Int64, 1);
        assert!(builder.push_str("__op", "I").is_err());
        builder.push_i64("__lsn", 5).unwrap();
        let array = builder.finish();
        assert_eq!(array.len(), 1);
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            5
        );
    }

    // ------------------------------------------------- canonical text decode
    //
    // Every literal below is a render captured from a live PostgreSQL 18.6
    // under the session settings `connection::OUTPUT_SETTINGS` pins, so these
    // assert against what the server actually sends, not what a decoder wishes
    // it sent.

    /// Decode one canonical-text value into a one-row array.
    fn from_text(ty: PgFieldType, text: &str) -> Result<ArrayRef, SaciError> {
        let mut builder = builder_for(ty, 1);
        builder.push_text("c", Some(text.as_bytes()))?;
        Ok(builder.finish())
    }

    fn text_of(ty: PgFieldType, text: &str, server_oid: u32) -> Result<ArrayRef, SaciError> {
        let mut builder = ColumnBuilder::new(&spec(ty), 1, server_oid)?;
        builder.push_text("c", Some(text.as_bytes()))?;
        Ok(builder.finish())
    }

    fn list_spec(item: PgFieldType) -> FieldSpec {
        FieldSpec {
            item: Some(item),
            precision: matches!(item, PgFieldType::Decimal128).then_some(18),
            scale: matches!(item, PgFieldType::Decimal128).then_some(4),
            ..spec(PgFieldType::List)
        }
    }

    /// Decode one array literal into its single row's `ListArray`.
    fn list_from_text(item: PgFieldType, text: &str) -> Result<ListArray, SaciError> {
        let field = list_spec(item);
        let mut builder = ColumnBuilder::new(&field, 1, Type::TEXT_ARRAY.oid())?;
        builder.push_text("c", Some(text.as_bytes()))?;
        let array = builder.finish();
        Ok(array
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("a list builder yields a ListArray")
            .clone())
    }

    #[test]
    fn bool_text_is_the_single_letter_form() {
        for (text, expected) in [("t", true), ("f", false)] {
            let array = from_text(PgFieldType::Boolean, text).expect(text);
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean");
            assert_eq!(values.value(0), expected, "{text}");
        }
        // The input syntax PostgreSQL accepts is not what it renders.
        for text in ["true", "false", "yes", "1", ""] {
            from_text(PgFieldType::Boolean, text).expect_err(text);
        }
    }

    #[test]
    fn integer_text_round_trips_at_the_extremes() {
        let array = from_text(PgFieldType::Int64, "-9223372036854775808").expect("i64::MIN");
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64")
                .value(0),
            i64::MIN
        );
        from_text(PgFieldType::Int32, "12.5").expect_err("not an integer");
    }

    fn f32_at(array: &ArrayRef) -> f32 {
        array
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("f32")
            .value(0)
    }

    fn f64_at(array: &ArrayRef) -> f64 {
        array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("f64")
            .value(0)
    }

    /// `cdc_logical` decodes `pgoutput`'s text tuples where the cursor path
    /// decodes the IEEE bytes, so both have to land on the same `f32` -- bit
    /// for bit, which is what separates `-0` from `0` -- for every value the
    /// output function can render, including the three with no decimal form
    /// and the denormal at the bottom of the range. Each literal is
    /// `float4out`'s own render under `extra_float_digits = 3`.
    #[test]
    fn float4_text_decodes_to_the_bits_the_binary_wire_carries() {
        for (text, binary) in [
            ("0", 0.0f32),
            ("-0", -0.0),
            ("0.1", 0.1),
            // The shortest round-trip decimal of the smallest denormal is
            // `1e-45`, not the `1.4e-45` its full expansion suggests.
            ("1e-45", f32::from_bits(1)),
            ("1.1754944e-38", f32::MIN_POSITIVE),
            ("3.4028235e+38", f32::MAX),
            ("-3.4028235e+38", f32::MIN),
            ("Infinity", f32::INFINITY),
            ("-Infinity", f32::NEG_INFINITY),
        ] {
            let decoded = from_text(PgFieldType::Float32, text)
                .unwrap_or_else(|e| panic!("{text}: {}", e.message()));
            let wire = build(
                PgFieldType::Float32,
                &[Some(&binary.to_bits().to_be_bytes())],
            );
            assert_eq!(
                f32_at(&decoded).to_bits(),
                f32_at(&wire).to_bits(),
                "float4 '{text}'"
            );
        }
        // A NaN has no one bit pattern to compare against, so both halves owe
        // only NaN-ness -- and the text half must stay a value rather than
        // becoming the refusal `NaN` is for `decimal128`.
        assert!(
            f32_at(&from_text(PgFieldType::Float32, "NaN").expect("NaN")).is_nan(),
            "float4 'NaN'"
        );
    }

    /// The `float8` half of
    /// [`float4_text_decodes_to_the_bits_the_binary_wire_carries`].
    #[test]
    fn float8_text_decodes_to_the_bits_the_binary_wire_carries() {
        for (text, binary) in [
            ("0", 0.0f64),
            ("-0", -0.0),
            ("0.1", 0.1),
            ("5e-324", f64::from_bits(1)),
            ("2.2250738585072014e-308", f64::MIN_POSITIVE),
            ("1.7976931348623157e+308", f64::MAX),
            ("Infinity", f64::INFINITY),
            ("-Infinity", f64::NEG_INFINITY),
        ] {
            let decoded = from_text(PgFieldType::Float64, text)
                .unwrap_or_else(|e| panic!("{text}: {}", e.message()));
            let wire = build(
                PgFieldType::Float64,
                &[Some(&binary.to_bits().to_be_bytes())],
            );
            assert_eq!(
                f64_at(&decoded).to_bits(),
                f64_at(&wire).to_bits(),
                "float8 '{text}'"
            );
        }
        assert!(
            f64_at(&from_text(PgFieldType::Float64, "NaN").expect("NaN")).is_nan(),
            "float8 'NaN'"
        );
    }

    #[test]
    fn an_oid_column_widens_its_unsigned_text() {
        let array = text_of(PgFieldType::Int64, "4294967295", OID_OID).expect("u32::MAX");
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("i64")
                .value(0),
            i64::from(u32::MAX)
        );
    }

    #[test]
    fn numeric_text_reaches_the_declared_scale_and_refuses_extra_digits() {
        // The fixture spec declares scale 4.
        let array = from_text(PgFieldType::Decimal128, "123.456").expect("in scale");
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("decimal")
                .value(0),
            1_234_560
        );
        let err = from_text(PgFieldType::Decimal128, "1.23456").expect_err("five digits");
        assert!(err.message().contains("fractional"), "{}", err.message());
        for text in ["NaN", "Infinity", "-Infinity"] {
            let err = from_text(PgFieldType::Decimal128, text).expect_err(text);
            assert!(err.message().contains("utf8"), "{}", err.message());
        }
    }

    /// The wire value has two fractional digits; the Arrow column has the
    /// declared scale, and the two have to agree or `RecordBatch::try_new`
    /// rejects the batch.
    #[test]
    fn money_is_rescaled_from_its_two_wire_digits_to_the_declared_scale() {
        // The shared fixture declares `decimal128` at scale 4.
        for (text, expected) in [
            ("$1,234.56", 12_345_600i128),
            ("-$92,233,720,368,547,758.08", -922_337_203_685_477_580_800),
            ("$0.00", 0),
        ] {
            let array = text_of(PgFieldType::Decimal128, text, OID_MONEY).expect(text);
            let values = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("decimal");
            assert_eq!(values.value(0), expected, "{text}");
            // The finished column must carry the declared scale, not 2.
            assert_eq!(values.scale(), 4);
        }

        // At the natural scale the wire value passes through unscaled, and
        // both wires agree.
        let mut money = spec(PgFieldType::Decimal128);
        money.precision = Some(19);
        money.scale = Some(2);
        let mut builder = ColumnBuilder::new(&money, 2, OID_MONEY).expect("builder");
        builder
            .push("c", Some(&123_456i64.to_be_bytes()))
            .expect("binary money");
        builder
            .push_text("c", Some(b"$1,234.56"))
            .expect("text money");
        let array = builder.finish();
        let values = array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal");
        assert_eq!(values.value(0), 123_456);
        assert_eq!(values.value(1), 123_456, "both wires agree");
        assert_eq!(values.scale(), 2);

        // A scale narrower than the wire's two digits would drop them.
        money.scale = Some(1);
        let Err(err) = ColumnBuilder::new(&money, 1, OID_MONEY) else {
            panic!("scale 1 would drop a digit money always carries");
        };
        assert!(
            err.message().contains("2 fractional digits"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn temporal_text_decodes_and_names_what_arrow_cannot_hold() {
        let array = from_text(PgFieldType::Date32, "2024-01-02").expect("date");
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("date")
                .value(0),
            19_724
        );
        let array =
            from_text(PgFieldType::TimestampMicros, "2024-01-02 03:04:05.123456").expect("ts");
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp")
                .value(0),
            1_704_164_645_123_456
        );
        // `timezone = 'UTC'` makes every timestamptz render with `+00`.
        let array = from_text(
            PgFieldType::TimestampMicrosUtc,
            "2024-01-02 01:04:05.123456+00",
        )
        .expect("tstz");
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp")
                .value(0),
            1_704_157_445_123_456
        );

        // The BC era and the infinities have no Arrow representation.
        for (ty, text) in [
            (PgFieldType::Date32, "0001-01-01 BC"),
            (PgFieldType::TimestampMicros, "4713-01-01 00:00:00 BC"),
            (PgFieldType::Date32, "infinity"),
            (PgFieldType::TimestampMicros, "-infinity"),
        ] {
            let err = from_text(ty, text).expect_err(text);
            assert!(err.message().contains("utf8"), "{text}: {}", err.message());
        }
    }

    /// The one timestamp a single-row array holds.
    fn micros_of(array: &ArrayRef) -> i64 {
        array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp")
            .value(0)
    }

    /// The one date a single-row array holds.
    fn days_of(array: &ArrayRef) -> i32 {
        array
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date")
            .value(0)
    }

    #[test]
    fn timestamp_text_agrees_with_the_binary_wire_beyond_the_nanosecond_window() {
        // Both instants sit outside the i64-nanosecond span (1677-09-21 to
        // 2262-04-11) a nanosecond-based parser is bounded by, while the
        // binary wire reads them either way. The two source wires can only
        // agree here if the text parser spans the whole type.
        for (rendered, since_pg_epoch) in [
            ("9999-12-31 23:59:59.999999", 252_455_615_999_999_999i64),
            ("1000-01-01 00:00:00", -31_556_908_800_000_000),
        ] {
            for (ty, offset) in [
                (PgFieldType::TimestampMicros, ""),
                (PgFieldType::TimestampMicrosUtc, "+00"),
            ] {
                let text = format!("{rendered}{offset}");
                let binary = build(ty, &[Some(&since_pg_epoch.to_be_bytes())]);
                let decoded = from_text(ty, &text).expect(&text);
                assert_eq!(micros_of(&decoded), micros_of(&binary), "{text}");
            }
        }
    }

    #[test]
    fn date_text_agrees_with_the_binary_wire_at_the_widest_year_postgres_renders() {
        let binary = build(
            PgFieldType::Date32,
            &[Some(&2_145_031_948i32.to_be_bytes())],
        );
        let decoded = from_text(PgFieldType::Date32, "5874897-12-31").expect("the last date");
        assert_eq!(days_of(&decoded), days_of(&binary));
        assert_eq!(days_of(&decoded), 2_145_042_905);
    }

    #[test]
    fn timestamp_text_reaches_the_first_year_of_the_calendar() {
        let array =
            from_text(PgFieldType::TimestampMicros, "0001-01-01 00:00:00").expect("year one");
        assert_eq!(micros_of(&array), -62_135_596_800_000_000);
    }

    #[test]
    fn timestamp_text_pads_a_fraction_shorter_than_six_digits() {
        let array = from_text(PgFieldType::TimestampMicros, "2024-01-02 03:04:05.123")
            .expect("milliseconds");
        assert_eq!(micros_of(&array), 1_704_164_645_123_000);
    }

    #[test]
    fn timestamptz_text_accepts_both_spellings_of_a_zero_offset() {
        for text in [
            "2024-01-02 01:04:05.123456+00",
            "2024-01-02 01:04:05.123456+00:00",
        ] {
            let array = from_text(PgFieldType::TimestampMicrosUtc, text).expect(text);
            assert_eq!(micros_of(&array), 1_704_157_445_123_456, "{text}");
        }
    }

    #[test]
    fn timestamptz_text_refuses_an_offset_the_pinned_timezone_cannot_produce() {
        let err = from_text(PgFieldType::TimestampMicrosUtc, "2024-01-02 01:04:05+02")
            .expect_err("a non-UTC offset");
        assert!(err.message().contains("column 'c'"), "{}", err.message());
        assert!(err.message().contains("+02"), "{}", err.message());
    }

    #[test]
    fn time_text_accepts_the_upper_bound_postgres_allows() {
        for (text, expected) in [
            ("04:05:06.789123", 14_706_789_123i64),
            ("00:00:00", 0),
            // PostgreSQL's `time` admits 24:00:00, which a wall-clock parser
            // would reject.
            ("24:00:00", 86_400_000_000),
            ("01:02:03.5", 3_723_500_000),
        ] {
            let array = from_text(PgFieldType::Time64Micros, text).expect(text);
            assert_eq!(
                array
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .expect("time")
                    .value(0),
                expected,
                "{text}"
            );
        }
        from_text(PgFieldType::Time64Micros, "04:05").expect_err("no seconds field");
    }

    #[test]
    fn interval_text_parses_the_postgres_style_including_its_pluralisation() {
        // PostgreSQL pluralises on the signed value, so `-1 years` is what a
        // value of minus one year renders as.
        for (text, months, days, nanos) in [
            (
                "1 year 2 mons 3 days 04:05:06.789",
                14,
                3,
                14_706_789_000_000i64,
            ),
            (
                "-1 years -2 mons +3 days -04:05:06",
                -14,
                3,
                -14_706_000_000_000,
            ),
            ("00:00:00", 0, 0, 0),
            ("1 mon", 1, 0, 0),
            ("-1 days", 0, -1, 0),
        ] {
            let array = from_text(PgFieldType::IntervalMonthDayNano, text).expect(text);
            let values = array
                .as_any()
                .downcast_ref::<arrow_array::IntervalMonthDayNanoArray>()
                .expect("interval");
            let value = values.value(0);
            assert_eq!(
                (value.months, value.days, value.nanoseconds),
                (months, days, nanos),
                "{text}"
            );
        }
        from_text(PgFieldType::IntervalMonthDayNano, "3 fortnights").expect_err("unknown unit");
    }

    #[test]
    fn an_interval_whose_time_component_overflows_nanoseconds_is_refused() {
        let mut builder = builder_for(PgFieldType::IntervalMonthDayNano, 1);
        // 16 bytes: micros, days, months. i64::MAX microseconds is about
        // 292 471 years, far past what nanoseconds can hold.
        let mut raw = Vec::new();
        raw.extend_from_slice(&i64::MAX.to_be_bytes());
        raw.extend_from_slice(&0i32.to_be_bytes());
        raw.extend_from_slice(&0i32.to_be_bytes());
        let err = builder.push("c", Some(&raw)).expect_err("overflow");
        assert!(err.message().contains("overflows"), "{}", err.message());
        assert!(err.message().contains("'c'"), "{}", err.message());
    }

    #[test]
    fn bytea_text_is_hex_behind_a_prefix_that_survives_an_empty_value() {
        let array = from_text(PgFieldType::Binary, "\\x00ff").expect("hex");
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("binary")
                .value(0),
            &[0x00, 0xff]
        );
        // An empty bytea renders as the prefix alone, not as an empty string.
        let array = from_text(PgFieldType::Binary, "\\x").expect("empty");
        assert!(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("binary")
                .value(0)
                .is_empty()
        );
        from_text(PgFieldType::Binary, "00ff").expect_err("no prefix");
        from_text(PgFieldType::Binary, "\\x0f0").expect_err("odd digits");
        from_text(PgFieldType::Binary, "\\xzz").expect_err("not hex");
    }

    #[test]
    fn uuid_text_decodes_in_either_case() {
        let expected: [u8; 16] = [
            0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38,
            0x0a, 0x11,
        ];
        for text in [
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
        ] {
            let array = from_text(PgFieldType::Uuid, text).expect(text);
            assert_eq!(
                array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .expect("uuid")
                    .value(0),
                &expected,
                "{text}"
            );
        }
        from_text(PgFieldType::Uuid, "a0eebc99").expect_err("too short");
    }

    #[test]
    fn an_empty_string_is_a_value_not_a_null() {
        // A zero-length varbit, hstore or tsvector all render as an empty
        // string, which must not be mistaken for absence.
        let array = from_text(PgFieldType::Utf8, "").expect("empty string");
        let values = array.as_any().downcast_ref::<StringArray>().expect("utf8");
        assert!(!values.is_null(0));
        assert_eq!(values.value(0), "");
    }

    #[test]
    fn an_array_literal_splits_on_its_own_quoting_rules() {
        let array = list_from_text(
            PgFieldType::Utf8,
            r#"{a,"b,c","with \"quote\"","back\\slash"}"#,
        )
        .expect("quoted elements");
        let values = array.value(0);
        let values = values.as_any().downcast_ref::<StringArray>().expect("utf8");
        assert_eq!(values.len(), 4);
        assert_eq!(values.value(0), "a");
        assert_eq!(values.value(1), "b,c");
        assert_eq!(values.value(2), "with \"quote\"");
        assert_eq!(values.value(3), "back\\slash");
    }

    #[test]
    fn a_bare_null_element_is_the_sql_null_and_a_quoted_one_is_the_string() {
        let array = list_from_text(PgFieldType::Utf8, r#"{"NULL",text,NULL}"#).expect("nulls");
        let values = array.value(0);
        let values = values.as_any().downcast_ref::<StringArray>().expect("utf8");
        assert_eq!(values.value(0), "NULL");
        assert_eq!(values.value(1), "text");
        assert!(values.is_null(2));
    }

    #[test]
    fn an_empty_array_and_an_empty_string_element_are_different() {
        let empty = list_from_text(PgFieldType::Utf8, "{}").expect("empty array");
        assert_eq!(empty.value(0).len(), 0);

        let one = list_from_text(PgFieldType::Utf8, r#"{""}"#).expect("one empty string");
        let values = one.value(0);
        let values = values.as_any().downcast_ref::<StringArray>().expect("utf8");
        assert_eq!(values.len(), 1);
        assert!(!values.is_null(0));
        assert_eq!(values.value(0), "");
    }

    #[test]
    fn a_typed_array_literal_decodes_its_elements() {
        let array = list_from_text(PgFieldType::Int32, "{1,2,-2147483648}").expect("ints");
        let values = array.value(0);
        let values = values
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .expect("int32");
        assert_eq!(values.values(), &[1, 2, i32::MIN]);
    }

    #[test]
    fn a_multi_dimensional_array_literal_is_refused_by_name() {
        for text in ["{{1,2},{3,4}}", "[1:2][1:2]={{1,2},{3,4}}"] {
            let err = list_from_text(PgFieldType::Int32, text).expect_err(text);
            assert!(
                err.message().contains("one-dimensional"),
                "{text}: {}",
                err.message()
            );
        }
    }

    #[test]
    fn a_non_default_lower_bound_prefix_is_accepted() {
        let array = list_from_text(PgFieldType::Utf8, "[2:3]={x,y}").expect("lower bound");
        let values = array.value(0);
        let values = values.as_any().downcast_ref::<StringArray>().expect("utf8");
        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), "x");
    }

    #[test]
    fn a_multi_dimensional_binary_array_is_refused_by_name() {
        // ndim = 2, flags = 0, element oid = int4.
        let mut raw = Vec::new();
        raw.extend_from_slice(&2i32.to_be_bytes());
        raw.extend_from_slice(&0i32.to_be_bytes());
        raw.extend_from_slice(&23i32.to_be_bytes());
        let field = list_spec(PgFieldType::Int32);
        let mut builder = ColumnBuilder::new(&field, 1, Type::INT4_ARRAY.oid()).expect("builder");
        let err = builder.push("c", Some(&raw)).expect_err("two dimensions");
        assert!(
            err.message().contains("one-dimensional"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn a_null_list_row_holds_no_elements() {
        let field = list_spec(PgFieldType::Int32);
        let mut builder = ColumnBuilder::new(&field, 2, Type::INT4_ARRAY.oid()).expect("builder");
        builder.push_text("c", Some(b"{7}")).expect("one element");
        builder.push_text("c", None).expect("null row");
        let array = builder.finish();
        let list = array.as_any().downcast_ref::<ListArray>().expect("list");
        assert_eq!(list.len(), 2);
        assert!(!list.is_null(0));
        assert_eq!(list.value(0).len(), 1);
        assert!(list.is_null(1));
        assert_eq!(list.value(1).len(), 0);
    }

    /// `-1` is the only negative element length the wire form defines, so any
    /// other one is a corrupt value rather than another spelling of NULL.
    #[test]
    fn a_negative_element_length_that_is_not_the_null_marker_is_refused() {
        // ndim = 1, flags = 0, element oid = int4, length = 1, lower bound = 1.
        let mut raw = Vec::new();
        raw.extend_from_slice(&1i32.to_be_bytes());
        raw.extend_from_slice(&0i32.to_be_bytes());
        raw.extend_from_slice(&23i32.to_be_bytes());
        raw.extend_from_slice(&1i32.to_be_bytes());
        raw.extend_from_slice(&1i32.to_be_bytes());
        let header = raw.len();

        let field = list_spec(PgFieldType::Int32);
        let mut builder = ColumnBuilder::new(&field, 1, Type::INT4_ARRAY.oid()).expect("builder");
        raw.extend_from_slice(&(-2i32).to_be_bytes());
        let err = builder.push("c", Some(&raw)).expect_err("-2 is not NULL");
        assert!(err.message().contains("'c'"), "{}", err.message());
        assert!(err.message().contains("-2"), "{}", err.message());

        // And -1 still is: the same buffer, one length apart.
        let mut null_element = raw[..header].to_vec();
        null_element.extend_from_slice(&(-1i32).to_be_bytes());
        let mut builder = ColumnBuilder::new(&field, 1, Type::INT4_ARRAY.oid()).expect("builder");
        builder
            .push("c", Some(&null_element))
            .expect("-1 is the NULL marker");
        let array = builder.finish();
        let list = array.as_any().downcast_ref::<ListArray>().expect("list");
        assert_eq!(list.value(0).len(), 1);
        assert!(list.value(0).is_null(0));
    }
}
