//! Arrow values into PostgreSQL binary parameters for `COPY … FORMAT binary`.
//!
//! [`PgValue`] is one enum implementing `ToSql`, so a row costs one reusable
//! `Vec` rather than a boxed trait object per value, and the wire form is
//! written directly rather than through an intermediate owned type. Every
//! variant carries what it needs to frame itself, which is what lets an array
//! element -- for which the driver supplies no `Type` -- be written by exactly
//! the same code as a scalar.
//!
//! [`ColumnReader`] resolves each declared column against a `RecordBatch` and
//! against the PostgreSQL type the sink frames it as, once per batch, so the
//! per-row path is a bounds-checked index into a typed array with no schema
//! lookup and no downcast. The target type is also what selects a per-value
//! range check: a declared `int64` written into an `int2` column is checked,
//! never truncated, and the failure names the column, the row and the value.
//! Which pair needs that check is settled at resolve time too, so a pair that
//! cannot overflow -- an `int64` into an `int8`, an `int16` into an `int4` --
//! reaches its [`PgValue`] with no check and no second dispatch on the target.
//!
//! The inverse of [`crate::values`]: the same epochs, the same `jsonb` version
//! byte, the same `numeric` codec.

use std::error::Error;
use std::fmt::Display;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, IntervalMonthDayNanoArray,
    ListArray, RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use bytes::{BufMut, BytesMut};
use postgres_protocol::IsNull as WireNull;
use postgres_protocol::types::{ArrayDimension, array_to_sql};
use saci_core::error::SaciError;
use tokio_postgres::types::{IsNull, Kind, ToSql, Type, to_sql_checked};

use crate::config::{FieldSpec, PgFieldType};
use crate::numeric::{i128_to_numeric, money_factor};
use crate::values::{DATE_EPOCH_OFFSET_DAYS, TIMESTAMP_EPOCH_OFFSET_MICROS};

/// One column value, ready to be written in PostgreSQL binary format.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PgValue<'a> {
    /// SQL NULL.
    Null,
    /// `bool`.
    Bool(bool),
    /// `int2`.
    I16(i16),
    /// `int4`.
    I32(i32),
    /// `int8`.
    I64(i64),
    /// `oid`, `xid`, `cid`: four bytes unsigned.
    U32(u32),
    /// `float4`.
    F32(f32),
    /// `float8`.
    F64(f64),
    /// `text`, `varchar`, `bpchar`, `name`, and every value the server casts
    /// from text on the way into its real column type.
    Str(&'a str),
    /// `bytea`.
    Bytes(&'a [u8]),
    /// `date`, in Arrow's 1970-01-01 epoch.
    Date(i32),
    /// `time`, microseconds since midnight.
    TimeMicros(i64),
    /// `timestamp` or `timestamptz`, in Arrow's 1970-01-01 epoch.
    TimestampMicros(i64),
    /// `uuid`, exactly 16 bytes.
    Uuid(&'a [u8]),
    /// `json`, or `jsonb` when the version byte is wanted.
    Json {
        /// The document text.
        text: &'a str,
        /// Whether the target is `jsonb`, which prefixes a version byte.
        jsonb: bool,
    },
    /// `numeric`, as an unscaled `i128` plus its scale.
    Numeric {
        /// The unscaled value.
        value: i128,
        /// Decimal digits after the point.
        scale: i8,
    },
    /// `money`, in hundredths of the currency unit.
    Money(i64),
    /// `interval`: months, days and microseconds, kept apart because
    /// PostgreSQL does not treat them as interchangeable.
    Interval {
        /// Whole months.
        months: i32,
        /// Whole days.
        days: i32,
        /// The sub-day component, in microseconds.
        micros: i64,
    },
    /// One row of a one-dimensional array: the element reader plus this row's
    /// window into it.
    List {
        /// The reader that frames one element.
        elements: &'a ColumnReader<'a>,
        /// Index of this row's first element in the element array.
        offset: usize,
        /// How many elements this row holds.
        len: usize,
        /// The element type's OID, which the array header carries.
        element_oid: u32,
    },
}

impl PgValue<'_> {
    /// Write the PostgreSQL binary form of this value.
    ///
    /// Takes no `Type`: every variant already carries what the framing needs,
    /// so an array element -- which arrives with no `Type` of its own -- is
    /// written by this same function.
    fn write(&self, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match *self {
            PgValue::Null => return Ok(IsNull::Yes),
            PgValue::Bool(v) => out.put_u8(u8::from(v)),
            PgValue::I16(v) => out.put_i16(v),
            PgValue::I32(v) => out.put_i32(v),
            PgValue::I64(v) => out.put_i64(v),
            PgValue::U32(v) => out.put_u32(v),
            PgValue::F32(v) => out.put_u32(v.to_bits()),
            PgValue::F64(v) => out.put_u64(v.to_bits()),
            PgValue::Str(v) => out.put_slice(v.as_bytes()),
            PgValue::Bytes(v) | PgValue::Uuid(v) => out.put_slice(v),
            PgValue::Date(v) => {
                let days = v.checked_sub(DATE_EPOCH_OFFSET_DAYS).ok_or_else(|| {
                    format!("date {v} does not fit PostgreSQL's 2000-01-01 epoch")
                })?;
                out.put_i32(days);
            }
            PgValue::TimeMicros(v) => out.put_i64(v),
            PgValue::TimestampMicros(v) => {
                let micros = v
                    .checked_sub(TIMESTAMP_EPOCH_OFFSET_MICROS)
                    .ok_or_else(|| {
                        format!("timestamp {v} does not fit PostgreSQL's 2000-01-01 epoch")
                    })?;
                out.put_i64(micros);
            }
            PgValue::Json { text, jsonb } => {
                if jsonb {
                    out.put_u8(1);
                }
                out.put_slice(text.as_bytes());
            }
            PgValue::Numeric { value, scale } => i128_to_numeric(value, scale, out),
            PgValue::Money(v) => out.put_i64(v),
            PgValue::Interval {
                months,
                days,
                micros,
            } => {
                // interval_send's order: the time component first, then days,
                // then months.
                out.put_i64(micros);
                out.put_i32(days);
                out.put_i32(months);
            }
            PgValue::List {
                elements,
                offset,
                len,
                element_oid,
            } => {
                let dimension = ArrayDimension {
                    len: i32::try_from(len).map_err(|_| {
                        format!("an array of {len} elements is longer than PostgreSQL can hold")
                    })?,
                    lower_bound: 1,
                };
                array_to_sql(
                    [dimension],
                    element_oid,
                    offset..offset + len,
                    |index, buf| {
                        // `postgres_protocol`'s own `IsNull`, which the array
                        // writer reports element nullability through. The
                        // element's refusal arrives boxed, so unbox it here:
                        // `SaciError` is what carries the message on.
                        let element = elements.value(index).map_err(|refusal| *refusal)?;
                        Ok(match element.write(buf)? {
                            IsNull::Yes => WireNull::Yes,
                            IsNull::No => WireNull::No,
                        })
                    },
                    out,
                )?;
            }
        }
        Ok(IsNull::No)
    }
}

impl ToSql for PgValue<'_> {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.write(out)
    }

    // The target column types come from the catalog and are checked against the
    // declared schema by `types::validate_columns` before a single row is
    // encoded, so there is nothing left for a per-value type check to reject.
    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// The PostgreSQL integer a declared integer column is written into.
///
/// A target narrower than the declared Arrow width is range-checked per value
/// rather than truncated, which is what `pg_type = "int2"` on an `int64` field
/// buys; a wider one is a free widening.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntTarget {
    /// `int2`.
    I16,
    /// `int4`.
    I32,
    /// `int8`.
    I64,
    /// `oid`, `xid`, `cid`.
    U32,
}

impl IntTarget {
    /// Which integer `ty` holds, or `None` when it holds none.
    fn of(ty: &Type) -> Option<Self> {
        if *ty == Type::INT2 {
            Some(IntTarget::I16)
        } else if *ty == Type::INT4 {
            Some(IntTarget::I32)
        } else if *ty == Type::INT8 {
            Some(IntTarget::I64)
        } else if *ty == Type::OID || *ty == Type::XID || *ty == Type::CID {
            Some(IntTarget::U32)
        } else {
            None
        }
    }

    /// The PostgreSQL type's own name, for an error message.
    fn name(self) -> &'static str {
        match self {
            IntTarget::I16 => "int2",
            IntTarget::I32 => "int4",
            IntTarget::I64 => "int8",
            IntTarget::U32 => "oid",
        }
    }
}

/// A declared column resolved against one `RecordBatch` and one target type.
///
/// Holding the downcast array means the per-row cost is a bounds-checked index,
/// not a name lookup plus a downcast per value. `what` and `name` are held so a
/// per-value refusal names itself without the caller having to re-derive which
/// column produced it.
#[derive(Debug)]
pub(crate) struct ColumnReader<'a> {
    /// The connector's error prefix, e.g. `PostgresSink`.
    what: &'a str,
    /// The column's name.
    name: &'a str,
    /// How a refusal spells the index it failed at.
    position: Position,
    /// The array, and everything the target type decided.
    kind: ReaderKind<'a>,
}

/// How a per-value refusal spells the index it failed at.
///
/// An element reader is indexed into the list's *values* array, not into the
/// batch, so calling that index a row would name a row the batch may not even
/// have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Position {
    /// A batch row.
    Row,
    /// An index into a list column's values array.
    Element,
}

/// One column's array, downcast, plus what its target type settled.
#[derive(Debug)]
enum ReaderKind<'a> {
    /// `boolean`.
    Boolean(&'a BooleanArray),
    /// `int16` into an `int2`: the wire width the array already carries.
    Int16(&'a Int16Array),
    /// `int16` widened into an `int4`.
    Int16To32(&'a Int16Array),
    /// `int16` widened into an `int8`.
    Int16To64(&'a Int16Array),
    /// `int16` into the unsigned target named beside it, which a negative
    /// value cannot hold, so every value is checked.
    Int16Checked(&'a Int16Array, IntTarget),
    /// `int32` into an `int4`.
    Int32(&'a Int32Array),
    /// `int32` widened into an `int8`.
    Int32To64(&'a Int32Array),
    /// `int32` into the narrower or unsigned target named beside it, checked
    /// per value.
    Int32Checked(&'a Int32Array, IntTarget),
    /// `int64` into an `int8`.
    Int64(&'a Int64Array),
    /// `int64` into the narrower or unsigned target named beside it, checked
    /// per value.
    Int64Checked(&'a Int64Array, IntTarget),
    /// `float32`.
    Float32(&'a Float32Array),
    /// `float64`.
    Float64(&'a Float64Array),
    /// `utf8`.
    Utf8(&'a StringArray),
    /// `json`, with whether the target is `jsonb`.
    Json(&'a StringArray, bool),
    /// `binary`.
    Binary(&'a BinaryArray),
    /// `date32`.
    Date32(&'a Date32Array),
    /// `time64_micros`.
    Time64(&'a Time64MicrosecondArray),
    /// `timestamp_micros` and `timestamp_micros_utc`.
    Timestamp(&'a TimestampMicrosecondArray),
    /// `uuid`.
    Uuid(&'a FixedSizeBinaryArray),
    /// `decimal128`, carrying the declared scale.
    Decimal128(&'a Decimal128Array, i8),
    /// `decimal128` into `money`, carrying the divisor down to the two
    /// fractional digits `money` holds and the declared scale the value is
    /// expressed at.
    Money(&'a Decimal128Array, i128, i8),
    /// `interval_month_day_nano`.
    Interval(&'a IntervalMonthDayNanoArray),
    /// A one-dimensional array.
    List {
        /// The list column itself, for its offsets and validity.
        array: &'a ListArray,
        /// The element reader, over the whole child array.
        element: Box<ColumnReader<'a>>,
        /// The element type's OID, which the array header carries.
        element_oid: u32,
    },
}

impl<'a> ColumnReader<'a> {
    /// The value at `row`, or [`PgValue::Null`].
    ///
    /// The error side is boxed so that it fits the niche in [`PgValue`]'s own
    /// discriminant: `Result<PgValue<'_>, Box<SaciError>>` is exactly as wide
    /// as a `PgValue`, which is what keeps this per-value return as cheap as
    /// an infallible one. The allocation happens only on the cold refusal
    /// path; [`row_values`] and [`PgValue::write`]'s array element writer
    /// unbox it back into a [`SaciError`] at their boundary.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming the column, the row and the value
    /// for a value the target type cannot hold: a narrowed integer out of
    /// range, an `interval` with sub-microsecond precision, or a `money`
    /// amount beyond what `i64` hundredths carry.
    // Inlined into its two callers so the chosen variant is built straight
    // into the slot that receives it: a 32-byte return through a hidden
    // out-pointer, plus the call and the return, is most of what a per-value
    // read costs.
    #[inline]
    pub(crate) fn value<'s>(&'s self, row: usize) -> Result<PgValue<'s>, Box<SaciError>> {
        macro_rules! read {
            ($array:expr, $wrap:expr) => {{
                if $array.is_null(row) {
                    PgValue::Null
                } else {
                    $wrap($array.value(row))
                }
            }};
        }
        Ok(match &self.kind {
            ReaderKind::Boolean(a) => read!(a, PgValue::Bool),
            ReaderKind::Int16(a) => read!(a, PgValue::I16),
            ReaderKind::Int16To32(a) => read!(a, |v| PgValue::I32(i32::from(v))),
            ReaderKind::Int16To64(a) => read!(a, |v| PgValue::I64(i64::from(v))),
            ReaderKind::Int32(a) => read!(a, PgValue::I32),
            ReaderKind::Int32To64(a) => read!(a, |v| PgValue::I64(i64::from(v))),
            ReaderKind::Int64(a) => read!(a, PgValue::I64),
            ReaderKind::Int16Checked(a, target) => {
                if a.is_null(row) {
                    PgValue::Null
                } else {
                    self.integer(i64::from(a.value(row)), *target, row)?
                }
            }
            ReaderKind::Int32Checked(a, target) => {
                if a.is_null(row) {
                    PgValue::Null
                } else {
                    self.integer(i64::from(a.value(row)), *target, row)?
                }
            }
            ReaderKind::Int64Checked(a, target) => {
                if a.is_null(row) {
                    PgValue::Null
                } else {
                    self.integer(a.value(row), *target, row)?
                }
            }
            ReaderKind::Float32(a) => read!(a, PgValue::F32),
            ReaderKind::Float64(a) => read!(a, PgValue::F64),
            ReaderKind::Utf8(a) => read!(a, PgValue::Str),
            ReaderKind::Json(a, jsonb) => {
                if a.is_null(row) {
                    PgValue::Null
                } else {
                    PgValue::Json {
                        text: a.value(row),
                        jsonb: *jsonb,
                    }
                }
            }
            ReaderKind::Binary(a) => read!(a, PgValue::Bytes),
            ReaderKind::Date32(a) => read!(a, PgValue::Date),
            ReaderKind::Time64(a) => read!(a, PgValue::TimeMicros),
            ReaderKind::Timestamp(a) => read!(a, PgValue::TimestampMicros),
            ReaderKind::Uuid(a) => read!(a, PgValue::Uuid),
            ReaderKind::Decimal128(a, scale) => {
                if a.is_null(row) {
                    PgValue::Null
                } else {
                    PgValue::Numeric {
                        value: a.value(row),
                        scale: *scale,
                    }
                }
            }
            ReaderKind::Money(a, divisor, scale) => {
                if a.is_null(row) {
                    PgValue::Null
                } else {
                    self.money(a.value(row), *divisor, *scale, row)?
                }
            }
            ReaderKind::Interval(a) => {
                if a.is_null(row) {
                    PgValue::Null
                } else {
                    let value = a.value(row);
                    self.interval(value.months, value.days, value.nanoseconds, row)?
                }
            }
            ReaderKind::List {
                array,
                element,
                element_oid,
            } => {
                if array.is_null(row) {
                    PgValue::Null
                } else {
                    // `value_offsets` holds one offset per row plus a
                    // terminator, and `row` indexes the same batch this array
                    // was resolved from, so both reads are in bounds.
                    let offsets = array.value_offsets();
                    let (start, end) = (offsets[row], offsets[row + 1]);
                    let offset = usize::try_from(start)
                        .map_err(|_| self.refuse(row, "the array offset is negative"))?;
                    let len = usize::try_from(end - start)
                        .map_err(|_| self.refuse(row, "the array length is negative"))?;
                    PgValue::List {
                        elements: element,
                        offset,
                        len,
                        element_oid: *element_oid,
                    }
                }
            }
        })
    }

    /// One integer value, range-checked against its target's width.
    fn integer(
        &self,
        value: i64,
        target: IntTarget,
        row: usize,
    ) -> Result<PgValue<'static>, Box<SaciError>> {
        let overflow = || {
            self.refuse(
                row,
                format!("{value} does not fit a PostgreSQL {}", target.name()),
            )
        };
        Ok(match target {
            IntTarget::I64 => PgValue::I64(value),
            IntTarget::I32 => PgValue::I32(i32::try_from(value).map_err(|_| overflow())?),
            IntTarget::I16 => PgValue::I16(i16::try_from(value).map_err(|_| overflow())?),
            IntTarget::U32 => PgValue::U32(u32::try_from(value).map_err(|_| overflow())?),
        })
    }

    /// One `money` value: the declared scale down to the two fractional digits
    /// `money` holds, with no digit dropped on the way.
    fn money(
        &self,
        value: i128,
        divisor: i128,
        scale: i8,
        row: usize,
    ) -> Result<PgValue<'static>, Box<SaciError>> {
        if value % divisor != 0 {
            return Err(self.refuse(
                row,
                format!(
                    "the unscaled value {value} at scale {scale} carries more than the 2 \
                     fractional digits money holds; round it before the sink"
                ),
            ));
        }
        let hundredths = i64::try_from(value / divisor).map_err(|_| {
            self.refuse(
                row,
                format!("the unscaled value {value} is beyond the range money holds"),
            )
        })?;
        Ok(PgValue::Money(hundredths))
    }

    /// One `interval`: Arrow counts nanoseconds, PostgreSQL microseconds, so a
    /// sub-microsecond component is a refusal rather than a rounding.
    fn interval(
        &self,
        months: i32,
        days: i32,
        nanos: i64,
        row: usize,
    ) -> Result<PgValue<'static>, Box<SaciError>> {
        if nanos % 1_000 != 0 {
            return Err(self.refuse(
                row,
                format!(
                    "the interval's time component of {nanos} nanosecond(s) is finer than \
                     PostgreSQL's microsecond; declare type = \"utf8\" to carry it as text"
                ),
            ));
        }
        Ok(PgValue::Interval {
            months,
            days,
            micros: nanos / 1_000,
        })
    }

    /// A per-value refusal naming the connector, the column and the position:
    /// a batch row for a scalar column, an index into the values array for an
    /// array element.
    ///
    /// Boxed, and kept out of line, so the per-value happy path carries only a
    /// `PgValue`-wide result and none of the formatting code.
    #[cold]
    #[inline(never)]
    fn refuse(&self, index: usize, detail: impl Display) -> Box<SaciError> {
        let position = match self.position {
            Position::Row => "row",
            Position::Element => "element",
        };
        Box::new(SaciError::generic(format!(
            "{}: column '{}' {position} {index}: {detail}",
            self.what, self.name
        )))
    }
}

/// Resolve every declared column of `batch` once, in declared order.
///
/// `targets` holds the PostgreSQL type each column's values are framed
/// against, in the same order, which is what selects the `jsonb` version byte,
/// the `money` rescale, the array's element codec and the integer range check.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] naming the first column that `batch` does not
/// carry, that carries a different Arrow type than the declared one, or whose
/// target type this crate cannot frame.
pub(crate) fn resolve_columns<'a>(
    what: &'a str,
    batch: &'a RecordBatch,
    specs: &'a [FieldSpec],
    targets: &[Type],
) -> Result<Vec<ColumnReader<'a>>, SaciError> {
    if specs.len() != targets.len() {
        return Err(SaciError::generic(format!(
            "{what}: {} declared column(s) but {} resolved target type(s)",
            specs.len(),
            targets.len()
        )));
    }
    let mut readers = Vec::with_capacity(specs.len());
    for (spec, target) in specs.iter().zip(targets) {
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
            position: Position::Row,
            kind: reader_kind(what, spec, spec.ty, array, target)?,
        });
    }
    Ok(readers)
}

/// The reader for one declared type over one array and one target type.
///
/// Recurses once for a `list`: the element reader is built from the same spec's
/// `item` and from the target array's element type, so an element is framed by
/// exactly the code a scalar of that type would take.
fn reader_kind<'a>(
    what: &'a str,
    spec: &'a FieldSpec,
    ty: PgFieldType,
    array: &'a dyn Array,
    target: &Type,
) -> Result<ReaderKind<'a>, SaciError> {
    macro_rules! downcast {
        ($target:ty) => {{
            array.as_any().downcast_ref::<$target>().ok_or_else(|| {
                SaciError::generic(format!(
                    "{what}: column '{}' is declared type \"{}\" but the batch holds {:?}",
                    spec.name,
                    ty.as_str(),
                    array.data_type()
                ))
            })?
        }};
    }
    macro_rules! cast {
        ($target:ty, $variant:expr) => {{ $variant(downcast!($target)) }};
    }
    let integer = |target: &Type| {
        IntTarget::of(target).ok_or_else(|| {
            SaciError::generic(format!(
                "{what}: column '{}' is declared type \"{}\" but its target is {}, which holds no \
                 integer",
                spec.name,
                ty.as_str(),
                target.name()
            ))
        })
    };

    Ok(match ty {
        PgFieldType::Boolean => cast!(BooleanArray, ReaderKind::Boolean),
        // The target settles the wire width here, once per batch, so the
        // per-value path is one `PgValue` construction with no second-level
        // dispatch; only a pair that can actually overflow keeps its check.
        PgFieldType::Int16 => {
            let array = downcast!(Int16Array);
            match integer(target)? {
                IntTarget::I16 => ReaderKind::Int16(array),
                IntTarget::I32 => ReaderKind::Int16To32(array),
                IntTarget::I64 => ReaderKind::Int16To64(array),
                narrow @ IntTarget::U32 => ReaderKind::Int16Checked(array, narrow),
            }
        }
        PgFieldType::Int32 => {
            let array = downcast!(Int32Array);
            match integer(target)? {
                IntTarget::I32 => ReaderKind::Int32(array),
                IntTarget::I64 => ReaderKind::Int32To64(array),
                narrow @ (IntTarget::I16 | IntTarget::U32) => {
                    ReaderKind::Int32Checked(array, narrow)
                }
            }
        }
        PgFieldType::Int64 => {
            let array = downcast!(Int64Array);
            match integer(target)? {
                IntTarget::I64 => ReaderKind::Int64(array),
                narrow @ (IntTarget::I16 | IntTarget::I32 | IntTarget::U32) => {
                    ReaderKind::Int64Checked(array, narrow)
                }
            }
        }
        PgFieldType::Float32 => cast!(Float32Array, ReaderKind::Float32),
        PgFieldType::Float64 => cast!(Float64Array, ReaderKind::Float64),
        PgFieldType::Utf8 => cast!(StringArray, ReaderKind::Utf8),
        PgFieldType::Json => ReaderKind::Json(downcast!(StringArray), *target == Type::JSONB),
        PgFieldType::Binary => cast!(BinaryArray, ReaderKind::Binary),
        PgFieldType::Date32 => cast!(Date32Array, ReaderKind::Date32),
        PgFieldType::Time64Micros => cast!(Time64MicrosecondArray, ReaderKind::Time64),
        PgFieldType::TimestampMicros | PgFieldType::TimestampMicrosUtc => {
            cast!(TimestampMicrosecondArray, ReaderKind::Timestamp)
        }
        PgFieldType::Uuid => cast!(FixedSizeBinaryArray, ReaderKind::Uuid),
        PgFieldType::IntervalMonthDayNano => {
            cast!(IntervalMonthDayNanoArray, ReaderKind::Interval)
        }
        PgFieldType::Decimal128 => {
            let (_, scale) = spec.decimal_params()?;
            let typed = downcast!(Decimal128Array);
            if typed.scale() != scale {
                return Err(SaciError::generic(format!(
                    "{what}: column '{}' is declared scale {scale} but the batch holds scale {}",
                    spec.name,
                    typed.scale()
                )));
            }
            if *target == Type::MONEY {
                let divisor = money_factor(scale).map_err(|reason| {
                    SaciError::generic(format!("{what}: column '{}': {reason}", spec.name))
                })?;
                ReaderKind::Money(typed, divisor, scale)
            } else {
                ReaderKind::Decimal128(typed, scale)
            }
        }
        PgFieldType::List => {
            let item = spec.item.ok_or_else(|| {
                SaciError::configuration(format!(
                    "{what}: column '{}': type \"list\" requires 'item'",
                    spec.name
                ))
            })?;
            let Kind::Array(element_type) = target.kind() else {
                return Err(SaciError::generic(format!(
                    "{what}: column '{}' is declared type \"list\" but its target {} is not an \
                     array type",
                    spec.name,
                    target.name()
                )));
            };
            let list = downcast!(ListArray);
            let element = ColumnReader {
                what,
                name: &spec.name,
                position: Position::Element,
                kind: reader_kind(what, spec, item, list.values().as_ref(), element_type)?,
            };
            ReaderKind::List {
                array: list,
                element: Box::new(element),
                element_oid: element_type.oid(),
            }
        }
    })
}

/// Refill `out` with one row's values, reusing its allocation.
///
/// Writes through a fixed-length slice rather than pushing: `out` is sized to
/// `readers` once and then only overwritten, so the per-value cost is a store
/// instead of a store plus a capacity check plus a length update.
///
/// # Errors
///
/// Propagates the first value its target column cannot hold, unboxed from the
/// [`Box<SaciError>`](ColumnReader::value) the per-value path returns it in.
pub(crate) fn row_values<'s>(
    readers: &'s [ColumnReader<'_>],
    row: usize,
    out: &mut Vec<PgValue<'s>>,
) -> Result<(), SaciError> {
    if out.len() != readers.len() {
        out.clear();
        out.resize(readers.len(), PgValue::Null);
    }
    for (slot, reader) in out.iter_mut().zip(readers) {
        *slot = reader.value(row).map_err(|refusal| *refusal)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{FixedSizeBinaryBuilder, Int32Builder, ListBuilder, StringBuilder};
    use arrow_array::types::IntervalMonthDayNanoType;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::*;
    use crate::values::ColumnBuilder;

    fn spec(name: &str, ty: PgFieldType) -> FieldSpec {
        FieldSpec {
            name: name.to_string(),
            ty,
            nullable: true,
            precision: matches!(ty, PgFieldType::Decimal128).then_some(18),
            scale: matches!(ty, PgFieldType::Decimal128).then_some(4),
            item: None,
            pg_type: None,
        }
    }

    /// Encode with `PgValue::to_sql`, decode with `ColumnBuilder::push`, and
    /// return the length the encoder wrote so a field-width regression shows up.
    fn encoded(value: PgValue<'_>, ty: &Type) -> BytesMut {
        let mut buf = BytesMut::new();
        let is_null = value.to_sql(ty, &mut buf).expect("to_sql");
        assert!(matches!(is_null, IsNull::No));
        buf
    }

    /// One reader over a one-column batch, with the given target type.
    fn reader<'a>(
        spec: &'a [FieldSpec],
        batch: &'a RecordBatch,
        target: Type,
    ) -> Vec<ColumnReader<'a>> {
        resolve_columns("PostgresSink", batch, spec, &[target]).expect("resolve")
    }

    fn one_column(field: Field, array: arrow_array::ArrayRef) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).expect("batch")
    }

    /// The whole point of boxing the error side: `Box<SaciError>` is a non-null
    /// pointer, so it lands in the niche of `PgValue`'s own discriminant and
    /// the per-value `Result` stays exactly as wide as the value it carries.
    /// An unboxed `SaciError` makes it 48 bytes instead of 32, worth ~2 ns per
    /// 15-column row on the sink's per-row path.
    #[test]
    fn the_per_value_result_is_no_wider_than_the_value() {
        assert_eq!(
            size_of::<Result<PgValue<'_>, Box<SaciError>>>(),
            size_of::<PgValue<'_>>(),
            "the boxed refusal must fit PgValue's niche"
        );
    }

    #[test]
    fn integers_and_floats_round_trip_through_the_decoder() {
        let cases: Vec<(PgFieldType, PgValue<'static>, Type)> = vec![
            (PgFieldType::Boolean, PgValue::Bool(true), Type::BOOL),
            (PgFieldType::Int16, PgValue::I16(-3), Type::INT2),
            (PgFieldType::Int32, PgValue::I32(-70_000), Type::INT4),
            (PgFieldType::Int64, PgValue::I64(i64::MIN), Type::INT8),
            (PgFieldType::Float32, PgValue::F32(-1.5), Type::FLOAT4),
            (PgFieldType::Float64, PgValue::F64(1e100), Type::FLOAT8),
            (PgFieldType::Int64, PgValue::U32(u32::MAX), Type::OID),
        ];
        for (declared, value, ty) in cases {
            let buf = encoded(value, &ty);
            let mut builder = ColumnBuilder::new(&spec("c", declared), 1, ty.oid()).unwrap();
            builder.push("c", Some(&buf)).expect("push");
            assert_eq!(builder.finish().len(), 1, "{declared:?}");
        }
    }

    #[test]
    fn date_and_timestamp_epochs_are_symmetric() {
        // 2000-01-01 in Arrow terms, which is 0 on the wire.
        let buf = encoded(PgValue::Date(DATE_EPOCH_OFFSET_DAYS), &Type::DATE);
        assert_eq!(&buf[..], &0i32.to_be_bytes());

        let buf = encoded(
            PgValue::TimestampMicros(TIMESTAMP_EPOCH_OFFSET_MICROS),
            &Type::TIMESTAMPTZ,
        );
        assert_eq!(&buf[..], &0i64.to_be_bytes());

        // And the Unix epoch is negative on the wire.
        let buf = encoded(PgValue::TimestampMicros(0), &Type::TIMESTAMP);
        assert_eq!(
            i64::from_be_bytes(buf[..].try_into().unwrap()),
            -TIMESTAMP_EPOCH_OFFSET_MICROS
        );
    }

    #[test]
    fn json_gains_a_version_byte_only_for_jsonb() {
        let text = "{\"a\":1}";
        let batch = one_column(
            Field::new("doc", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![Some(text)])),
        );
        let specs = [spec("doc", PgFieldType::Json)];

        let plain = reader(&specs, &batch, Type::JSON);
        let buf = encoded(plain[0].value(0).unwrap(), &Type::JSON);
        assert_eq!(&buf[..], text.as_bytes());

        // The version byte follows the *target*, not the value: the same batch
        // written into a `jsonb` column carries it.
        let binary = reader(&specs, &batch, Type::JSONB);
        let buf = encoded(binary[0].value(0).unwrap(), &Type::JSONB);
        assert_eq!(buf[0], 1);
        assert_eq!(&buf[1..], text.as_bytes());
    }

    #[test]
    fn numeric_round_trips_through_the_decoder() {
        let buf = encoded(
            PgValue::Numeric {
                value: -123_456,
                scale: 4,
            },
            &Type::NUMERIC,
        );
        let mut builder =
            ColumnBuilder::new(&spec("c", PgFieldType::Decimal128), 1, Type::NUMERIC.oid())
                .unwrap();
        builder.push("c", Some(&buf)).unwrap();
        let array = builder.finish();
        let decimals = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(decimals.value(0), -123_456);
    }

    #[test]
    fn null_writes_nothing_and_reports_is_null() {
        let mut buf = BytesMut::new();
        let is_null = PgValue::Null.to_sql(&Type::INT8, &mut buf).unwrap();
        assert!(matches!(is_null, IsNull::Yes));
        assert!(buf.is_empty());
    }

    #[test]
    fn resolve_columns_reads_by_name_and_row_values_reuses_its_vec() {
        let mut uuids = FixedSizeBinaryBuilder::with_capacity(2, 16);
        uuids.append_value([9u8; 16]).unwrap();
        uuids.append_null();

        let schema = Arc::new(Schema::new(vec![
            Field::new("label", DataType::Utf8, true),
            Field::new("id", DataType::Int64, false),
            Field::new("uid", DataType::FixedSizeBinary(16), true),
            Field::new("at", DataType::Time64(TimeUnit::Microsecond), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(uuids.finish()),
                Arc::new(Time64MicrosecondArray::from(vec![Some(5), Some(6)])),
            ],
        )
        .unwrap();

        // Declared in a different order than the batch: resolution is by name.
        let specs = [
            spec("id", PgFieldType::Int64),
            spec("label", PgFieldType::Utf8),
            spec("uid", PgFieldType::Uuid),
            spec("at", PgFieldType::Time64Micros),
        ];
        let targets = [Type::INT8, Type::TEXT, Type::UUID, Type::TIME];
        let readers = resolve_columns("PostgresSink", &batch, &specs, &targets).unwrap();

        let mut row = Vec::new();
        row_values(&readers, 0, &mut row).unwrap();
        assert!(matches!(row[0], PgValue::I64(1)));
        assert!(matches!(row[1], PgValue::Str("a")));
        assert!(matches!(row[2], PgValue::Uuid(_)));
        assert!(matches!(row[3], PgValue::TimeMicros(5)));

        let capacity = row.capacity();
        row_values(&readers, 1, &mut row).unwrap();
        assert_eq!(row.capacity(), capacity, "the row vec must be reused");
        assert!(matches!(row[0], PgValue::I64(2)));
        assert!(matches!(row[1], PgValue::Null));
        assert!(matches!(row[2], PgValue::Null));
    }

    #[test]
    fn a_missing_batch_column_names_itself() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let specs = [spec("other", PgFieldType::Int64)];
        let err = resolve_columns("PostgresSink", &batch, &specs, &[Type::INT8]).unwrap_err();
        assert!(err.message().contains("'other'"), "{}", err.message());
    }

    #[test]
    fn a_decimal_scale_mismatch_is_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(18, 2),
            true,
        )]));
        let array = Decimal128Array::from(vec![Some(1i128)])
            .with_precision_and_scale(18, 2)
            .unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();
        let specs = [spec("amount", PgFieldType::Decimal128)];
        let err = resolve_columns("PostgresSink", &batch, &specs, &[Type::NUMERIC]).unwrap_err();
        assert!(err.message().contains("scale"), "{}", err.message());
    }

    #[test]
    fn a_narrowed_integer_names_the_column_the_row_and_the_value() {
        let specs = [spec("small", PgFieldType::Int64)];
        // One case per edge the width check alone would miss: past int2's
        // range, past int4's, past oid's unsigned range, and negative into an
        // unsigned oid.
        for (value, target, name) in [
            (40_000i64, Type::INT2, "int2"),
            (3_000_000_000, Type::INT4, "int4"),
            (i64::from(u32::MAX) + 1, Type::OID, "oid"),
            (-1, Type::OID, "oid"),
        ] {
            let batch = one_column(
                Field::new("small", DataType::Int64, false),
                Arc::new(Int64Array::from(vec![7, value])),
            );
            let readers = reader(&specs, &batch, target);
            readers[0].value(0).expect("7 fits every target");

            let err = readers[0].value(1).unwrap_err();
            let message = err.message();
            assert!(message.contains("'small'"), "{message}");
            assert!(message.contains("row 1"), "{message}");
            assert!(message.contains(&value.to_string()), "{message}");
            assert!(message.contains(name), "{message}");
        }

        // The widths that do fit are written unchanged.
        let batch = one_column(
            Field::new("small", DataType::Int64, false),
            Arc::new(Int64Array::from(vec![i64::from(u32::MAX)])),
        );
        let readers = reader(&specs, &batch, Type::OID);
        assert!(matches!(readers[0].value(0), Ok(PgValue::U32(u32::MAX))));
    }

    #[test]
    fn an_interval_finer_than_a_microsecond_is_refused() {
        let batch = one_column(
            Field::new(
                "span",
                DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
                true,
            ),
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                IntervalMonthDayNanoType::make_value(1, 2, 3_000),
                IntervalMonthDayNanoType::make_value(0, 0, 1),
            ])),
        );
        let specs = [spec("span", PgFieldType::IntervalMonthDayNano)];
        let readers = reader(&specs, &batch, Type::INTERVAL);

        let buf = encoded(readers[0].value(0).unwrap(), &Type::INTERVAL);
        // interval_send's order: micros, days, months.
        assert_eq!(&buf[..8], &3i64.to_be_bytes());
        assert_eq!(&buf[8..12], &2i32.to_be_bytes());
        assert_eq!(&buf[12..], &1i32.to_be_bytes());

        let err = readers[0].value(1).unwrap_err();
        assert!(err.message().contains("nanosecond"), "{}", err.message());
    }

    #[test]
    fn money_rescales_the_declared_scale_and_refuses_a_lost_digit() {
        let mut amount = spec("amount", PgFieldType::Decimal128);
        amount.precision = Some(19);
        amount.scale = Some(4);
        let array = Decimal128Array::from(vec![Some(12_345_600i128), Some(1i128)])
            .with_precision_and_scale(19, 4)
            .unwrap();
        let batch = one_column(
            Field::new("amount", DataType::Decimal128(19, 4), true),
            Arc::new(array),
        );
        let specs = [amount];
        let readers = reader(&specs, &batch, Type::MONEY);

        let buf = encoded(readers[0].value(0).unwrap(), &Type::MONEY);
        assert_eq!(i64::from_be_bytes(buf[..].try_into().unwrap()), 123_456);

        // 0.0001 has no representation in money's two digits.
        let err = readers[0].value(1).unwrap_err();
        assert!(
            err.message().contains("2 fractional digits"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn an_array_frames_its_elements_and_its_nulls() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("a,b");
        builder.values().append_null();
        builder.append(true);
        builder.append(true); // empty list
        builder.append_null();
        let array = builder.finish();

        let mut tags = spec("tags", PgFieldType::List);
        tags.item = Some(PgFieldType::Utf8);
        let batch = one_column(
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                true,
            ),
            Arc::new(array),
        );
        let specs = [tags];
        let readers = reader(&specs, &batch, Type::TEXT_ARRAY);

        let buf = encoded(readers[0].value(0).unwrap(), &Type::TEXT_ARRAY);
        // ndim, flags, element oid, then (len, lower bound).
        assert_eq!(&buf[..4], &1i32.to_be_bytes());
        assert_eq!(&buf[8..12], &Type::TEXT.oid().to_be_bytes());
        assert_eq!(&buf[12..16], &2i32.to_be_bytes());
        assert_eq!(&buf[16..20], &1i32.to_be_bytes());
        assert_eq!(&buf[20..24], &3i32.to_be_bytes());
        assert_eq!(&buf[24..27], b"a,b");
        // A NULL element is a -1 length and no bytes.
        assert_eq!(&buf[27..31], &(-1i32).to_be_bytes());
        assert_eq!(buf.len(), 31);

        // An empty list is a one-dimensional array of no elements, which
        // PostgreSQL normalises to `{}`.
        let buf = encoded(readers[0].value(1).unwrap(), &Type::TEXT_ARRAY);
        assert_eq!(&buf[12..16], &0i32.to_be_bytes());

        assert!(matches!(readers[0].value(2).unwrap(), PgValue::Null));
    }

    #[test]
    fn an_integer_array_element_is_framed_by_the_scalar_encoder() {
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(-7);
        builder.append(true);
        let array = builder.finish();

        let mut ids = spec("ids", PgFieldType::List);
        ids.item = Some(PgFieldType::Int32);
        let batch = one_column(
            Field::new(
                "ids",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            ),
            Arc::new(array),
        );
        let specs = [ids];
        let readers = reader(&specs, &batch, Type::INT4_ARRAY);
        let buf = encoded(readers[0].value(0).unwrap(), &Type::INT4_ARRAY);
        assert_eq!(&buf[8..12], &Type::INT4.oid().to_be_bytes());
        assert_eq!(&buf[20..24], &4i32.to_be_bytes());
        assert_eq!(&buf[24..28], &(-7i32).to_be_bytes());
    }

    /// An element the encoder refuses is raised from inside `array_to_sql`, so
    /// its reason has to survive being boxed: the driver's own `Display` says
    /// only "error serializing parameter N", and
    /// [`pg_detail`](crate::connection::pg_detail) recovers this text from the
    /// error's source chain.
    #[test]
    fn an_element_the_encoder_refuses_names_itself_as_an_element() {
        let mut builder =
            ListBuilder::new(arrow_array::builder::IntervalMonthDayNanoBuilder::new());
        // Row 0 is two clean elements, so the offending element sits at values
        // index 2 while its list is row 1: a message saying "row 2" would name
        // a batch row that does not exist.
        builder
            .values()
            .append_value(IntervalMonthDayNanoType::make_value(1, 0, 0));
        builder
            .values()
            .append_value(IntervalMonthDayNanoType::make_value(2, 0, 0));
        builder.append(true);
        builder
            .values()
            .append_value(IntervalMonthDayNanoType::make_value(0, 0, 1));
        builder.append(true);
        let array = builder.finish();

        let mut spans = spec("spans", PgFieldType::List);
        spans.item = Some(PgFieldType::IntervalMonthDayNano);
        let batch = one_column(
            Field::new(
                "spans",
                DataType::List(Arc::new(Field::new_list_field(
                    DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
                    true,
                ))),
                true,
            ),
            Arc::new(array),
        );
        let specs = [spans];
        let readers = reader(&specs, &batch, Type::INTERVAL_ARRAY);

        // Row 0's elements are all writable.
        let mut buf = BytesMut::new();
        readers[0]
            .value(0)
            .expect("row 0 is fine")
            .to_sql(&Type::INTERVAL_ARRAY, &mut buf)
            .expect("two whole-month elements");

        let mut buf = BytesMut::new();
        let Err(err) = readers[0]
            .value(1)
            .expect("the list itself is fine")
            .to_sql(&Type::INTERVAL_ARRAY, &mut buf)
        else {
            panic!("a sub-microsecond element cannot be written");
        };
        let message = err.to_string();
        assert!(message.contains("'spans'"), "{message}");
        // "element 2", not "row 2": the index is into the list's values array,
        // and this batch has only two rows.
        assert!(message.contains("element 2"), "{message}");
        assert!(!message.contains("row "), "{message}");
        assert!(message.contains("nanosecond"), "{message}");
    }
}
