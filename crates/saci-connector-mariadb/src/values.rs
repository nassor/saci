//! Server values into Arrow arrays, over both wires.
//!
//! A value reaches this crate in one of two forms. A `SELECT` over the binary
//! protocol (polling, a changelog read, the snapshot) hands back
//! [`mysql_common::value::Value`]s the server has already rendered. The row
//! binlog hands back [`BinlogValue`]s in storage form: a `TIMESTAMP` as epoch
//! text, an `ENUM` as its index, a `SET` as a bitmask, an unsigned integer as
//! it was stored. [`Cell`] is the one form both are normalised to, so a
//! [`ColumnBuilder`] has a single path and every mode emits identical values
//! for the same row.
//!
//! The session runs at `+00:00`, so a protocol `TIMESTAMP` and a binlog
//! `TIMESTAMP` are the same instant and a `DATETIME` is read as written.
//! Nothing here rounds, truncates or clamps: a value an Arrow type cannot hold
//! is an error naming the column.

use std::borrow::Cow;
use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder, UInt8Builder, UInt16Builder,
    UInt32Builder, UInt64Builder,
};
use mysql_common::binlog::value::BinlogValue;
use mysql_common::value::Value;
use saci_core::error::SaciError;

use crate::config::MariadbFieldType;
use crate::types::{Family, ResolvedColumn};

use MariadbFieldType as T;

/// Microseconds in one day, the step from a civil day count to an Arrow
/// timestamp.
pub(crate) const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Microseconds in one second.
pub(crate) const MICROS_PER_SECOND: i64 = 1_000_000;

/// The year a binlog `YEAR` byte of 0 is reported as. The byte holds
/// `year - 1900` and the zero year `0000`, which reads back as 1900, a year
/// `YEAR` cannot hold.
const BINLOG_ZERO_YEAR: i64 = 1900;

// ------------------------------------------------------------ civil calendar

/// The proleptic Gregorian date of a day count since 1970-01-01, as
/// `(year, month, day)` with the month in `1..=12` and the day in `1..=31`.
pub(crate) fn civil_from_days(days: i64) -> (i64, u32, u32) {
    // A 400-year era starts in March, which puts the leap day last.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    (
        if month <= 2 { year + 1 } else { year },
        month as u32,
        day as u32,
    )
}

/// The day count since 1970-01-01 of a proleptic Gregorian date, the inverse
/// of [`civil_from_days`].
pub(crate) fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let shifted_year = year - i64::from(month <= 2);
    let era = shifted_year.div_euclid(400);
    let year_of_era = shifted_year.rem_euclid(400);
    let shifted_month = (i64::from(month) + 9) % 12;
    let day_of_year = (153 * shifted_month + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// The number of days in `month` of `year`.
fn days_in_month(year: u16, month: u8) -> u8 {
    match month {
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        _ => 31,
    }
}

/// A calendar date and time of day as the server reports them, before any
/// validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Civil {
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micros: u32,
}

impl Civil {
    /// Whether this is the zero date `0000-00-00`, however the time reads.
    fn is_zero_date(&self) -> bool {
        self.year == 0 && self.month == 0 && self.day == 0
    }

    /// Whether the month and the day name a day that exists.
    fn is_real_date(&self) -> bool {
        (1..=12).contains(&self.month)
            && self.day >= 1
            && self.day <= days_in_month(self.year, self.month)
    }

    /// The day count since 1970-01-01.
    fn days(&self) -> i64 {
        days_from_civil(
            i64::from(self.year),
            u32::from(self.month),
            u32::from(self.day),
        )
    }

    /// Microseconds since 1970-01-01T00:00:00 of this instant read as UTC.
    fn epoch_micros(&self) -> i64 {
        let seconds =
            i64::from(self.hour) * 3600 + i64::from(self.minute) * 60 + i64::from(self.second);
        self.days() * MICROS_PER_DAY + seconds * MICROS_PER_SECOND + i64::from(self.micros)
    }
}

// ----------------------------------------------------------------------- Cell

/// One column value of one row in the form every mode shares.
///
/// Built by [`Cell::from_protocol`] or [`Cell::from_binlog`]; a reader that
/// fills a metadata column from the change stream builds one directly. The
/// cell is deliberately weakly typed: [`ColumnBuilder`] knows the declared type
/// and range-checks, so `Int` and `UInt` are interchangeable here.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Cell<'a> {
    /// SQL NULL.
    Null,
    /// An integer that fits `i64`.
    Int(i64),
    /// An integer above `i64::MAX`, or any value of an unsigned column.
    UInt(u64),
    /// A `float` or `double`. A `float` is widened exactly.
    Float(f64),
    /// Bytes borrowed from the row: text, a decimal's own text, a blob, the
    /// bytes of a `bit` column declared binary.
    Text(&'a [u8]),
    /// Text the normalisation had to build: a `SET` joined from its bitmask or
    /// a JSON document rendered from its binary form.
    Owned(String),
    /// A `date`.
    Date {
        /// The year, 0 to 9999.
        year: u16,
        /// The month, 1 to 12, or 0 in a zero date.
        month: u8,
        /// The day, 1 to 31, or 0 in a zero date.
        day: u8,
    },
    /// A `datetime`, or the zero `timestamp`.
    DateTime {
        /// The year, 0 to 9999.
        year: u16,
        /// The month, 1 to 12, or 0 in a zero date.
        month: u8,
        /// The day, 1 to 31, or 0 in a zero date.
        day: u8,
        /// The hour, 0 to 23.
        hour: u8,
        /// The minute, 0 to 59.
        minute: u8,
        /// The second, 0 to 59.
        second: u8,
        /// Microseconds, 0 to 999999.
        micros: u32,
    },
    /// A `time`: signed, and up to 838 hours.
    Time {
        /// Whether the interval is negative.
        negative: bool,
        /// Whole hours, days included.
        hours: u32,
        /// The minute, 0 to 59.
        minutes: u8,
        /// The second, 0 to 59.
        seconds: u8,
        /// Microseconds, 0 to 999999.
        micros: u32,
    },
    /// A `timestamp` instant as microseconds since the Unix epoch.
    EpochMicros(i64),
}

impl<'a> Cell<'a> {
    /// What the cell holds, for an error message that must not carry the value.
    fn kind(&self) -> &'static str {
        match self {
            Cell::Null => "NULL",
            Cell::Int(_) | Cell::UInt(_) => "integer",
            Cell::Float(_) => "floating point",
            Cell::Text(_) | Cell::Owned(_) => "text",
            Cell::Date { .. } => "date",
            Cell::DateTime { .. } | Cell::EpochMicros(_) => "date and time",
            Cell::Time { .. } => "time",
        }
    }

    /// Normalise a value of `column` read over the binary protocol.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming the column when the value is a
    /// kind its column type never produces, or a date that does not exist.
    pub(crate) fn from_protocol(
        value: &'a Value,
        column: &ResolvedColumn,
    ) -> Result<Self, SaciError> {
        Ok(match value {
            Value::NULL => Cell::Null,
            Value::Int(v) => Cell::Int(*v),
            Value::UInt(v) => Cell::UInt(*v),
            Value::Float(v) => Cell::Float(f64::from(*v)),
            Value::Double(v) => Cell::Float(*v),
            Value::Bytes(bytes) => Self::from_bytes(bytes, column)?,
            Value::Date(year, month, day, hour, minute, second, micros) => Self::from_civil(
                column,
                Civil {
                    year: *year,
                    month: *month,
                    day: *day,
                    hour: *hour,
                    minute: *minute,
                    second: *second,
                    micros: *micros,
                },
            )?,
            Value::Time(negative, days, hours, minutes, seconds, micros) => {
                if column.family != Family::Time {
                    return Err(unexpected(column, "time"));
                }
                Cell::Time {
                    negative: *negative,
                    hours: days.saturating_mul(24).saturating_add(u32::from(*hours)),
                    minutes: *minutes,
                    seconds: *seconds,
                    micros: *micros,
                }
            }
        })
    }

    /// Normalise a value of `column` read from a binlog row image, in the form
    /// `mysql_common` decodes it to.
    ///
    /// The decoder hands back what the storage layer holds, which differs from
    /// the protocol per type:
    ///
    /// - `TIMESTAMP` is epoch text, `sec` or `sec.micros`.
    /// - `YEAR` is text, with the zero year read back as 1900.
    /// - `ENUM` is the 1-based index of the label, 0 for the empty error value.
    /// - `SET` is a little-endian bitmask over the labels.
    /// - An unsigned integer may arrive sign-extended when the table map
    ///   carries no signedness, so it is reinterpreted at the column's width.
    /// - `JSON` is binary JSONB, rendered here as text.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming the column for a value that is
    /// malformed for its type, for an enum index or set bit beyond the
    /// column's labels, and for a partial JSON update, which only a server
    /// with `binlog_row_value_options` set sends.
    pub(crate) fn from_binlog(
        value: &'a BinlogValue<'_>,
        column: &'a ResolvedColumn,
    ) -> Result<Self, SaciError> {
        let name = column.spec.name.as_str();
        match value {
            BinlogValue::Value(value) => Self::from_binlog_value(value, column),
            BinlogValue::Jsonb(json) => {
                let document = serde_json::Value::try_from(json.clone()).map_err(|e| {
                    SaciError::generic(format!(
                        "column '{name}': cannot read the JSON value from the binlog: {e}"
                    ))
                })?;
                Ok(Cell::Owned(document.to_string()))
            }
            BinlogValue::JsonDiff(_) => Err(SaciError::generic(format!(
                "column '{name}': a partial JSON update reached the binlog; set \
                 binlog_row_value_options = '' on the server"
            ))),
        }
    }

    /// The plain-value half of [`from_binlog`](Self::from_binlog).
    fn from_binlog_value(value: &'a Value, column: &'a ResolvedColumn) -> Result<Self, SaciError> {
        let name = column.spec.name.as_str();
        match (column.family, value) {
            (_, Value::NULL) => Ok(Cell::Null),
            (Family::Integer(bits), Value::Int(v)) => Ok(if column.unsigned {
                Cell::UInt((*v as u64) & width_mask(bits))
            } else if bits < 64 && *v >= 1i64 << (bits - 1) {
                // The decoder read a signed column unsigned (it does so for a
                // MEDIUMINT): a value past the signed maximum is the two's
                // complement of a negative one.
                Cell::Int(*v - (1i64 << bits))
            } else {
                Cell::Int(*v)
            }),
            (Family::Integer(bits), Value::UInt(v)) => Ok(Cell::UInt(if column.unsigned {
                *v & width_mask(bits)
            } else {
                *v
            })),
            (Family::Year, Value::Bytes(text)) => {
                let year: i64 = std::str::from_utf8(text)
                    .ok()
                    .and_then(|text| text.parse().ok())
                    .ok_or_else(|| {
                        SaciError::generic(format!(
                            "column '{name}': the binlog YEAR is not a number"
                        ))
                    })?;
                Ok(Cell::Int(if year == BINLOG_ZERO_YEAR { 0 } else { year }))
            }
            (Family::Enum, Value::Int(index)) => {
                let index = usize::try_from(*index).unwrap_or(usize::MAX);
                if index == 0 {
                    return Ok(Cell::Text(b""));
                }
                column
                    .labels
                    .get(index - 1)
                    .map(|label| Cell::Text(label.as_bytes()))
                    .ok_or_else(|| {
                        SaciError::generic(format!(
                            "column '{name}': the binlog ENUM index {index} is beyond the \
                             column's {} label(s)",
                            column.labels.len()
                        ))
                    })
            }
            (Family::Set, Value::Bytes(mask)) => set_labels(column, mask),
            (Family::Timestamp, Value::Bytes(text)) => timestamp_text(column, text),
            _ => Self::from_protocol(value, column),
        }
    }

    /// A `Value::Bytes` of `column`, which is text for most families and a
    /// big-endian number for a `bit` read as one.
    fn from_bytes(bytes: &'a [u8], column: &ResolvedColumn) -> Result<Self, SaciError> {
        match column.family {
            Family::Bit if column.spec.ty != T::Binary => {
                if bytes.len() > 8 {
                    return Err(SaciError::generic(format!(
                        "column '{}': a bit value of {} bytes does not fit 64 bits",
                        column.spec.name,
                        bytes.len()
                    )));
                }
                Ok(Cell::UInt(
                    bytes
                        .iter()
                        .fold(0u64, |acc, byte| (acc << 8) | u64::from(*byte)),
                ))
            }
            Family::Integer(_)
            | Family::Year
            | Family::Float
            | Family::Date
            | Family::Time
            | Family::DateTime
            | Family::Timestamp => Err(unexpected(column, "text")),
            _ => Ok(Cell::Text(bytes)),
        }
    }

    /// A `Value::Date` of `column`: a date, a date and time, or a `timestamp`
    /// instant.
    fn from_civil(column: &ResolvedColumn, civil: Civil) -> Result<Self, SaciError> {
        match column.family {
            Family::Date => Ok(Cell::Date {
                year: civil.year,
                month: civil.month,
                day: civil.day,
            }),
            Family::DateTime => Ok(civil.into()),
            Family::Timestamp => {
                if civil.is_zero_date() {
                    return Ok(civil.into());
                }
                if !civil.is_real_date() {
                    return Err(SaciError::generic(format!(
                        "column '{}': the server reports a timestamp with an impossible date",
                        column.spec.name
                    )));
                }
                Ok(Cell::EpochMicros(civil.epoch_micros()))
            }
            _ => Err(unexpected(column, "date and time")),
        }
    }
}

impl From<Civil> for Cell<'_> {
    fn from(civil: Civil) -> Self {
        Cell::DateTime {
            year: civil.year,
            month: civil.month,
            day: civil.day,
            hour: civil.hour,
            minute: civil.minute,
            second: civil.second,
            micros: civil.micros,
        }
    }
}

/// A value of a kind `column`'s type never produces.
fn unexpected(column: &ResolvedColumn, kind: &str) -> SaciError {
    SaciError::generic(format!(
        "column '{}': the server sent {kind} for a {} column",
        column.spec.name,
        if column.data_type.is_empty() {
            column.spec.ty.as_str()
        } else {
            column.data_type
        }
    ))
}

/// The low `bits` bits, all of them for a width of 64 or more.
fn width_mask(bits: u8) -> u64 {
    if bits >= 64 {
        u64::MAX
    } else {
        (1u64 << bits) - 1
    }
}

/// The labels a little-endian `SET` bitmask selects, joined with `,` in
/// declaration order.
fn set_labels<'a>(column: &ResolvedColumn, mask: &[u8]) -> Result<Cell<'a>, SaciError> {
    let selected = |index: usize| {
        mask.get(index / 8)
            .is_some_and(|byte| byte >> (index % 8) & 1 == 1)
    };
    if let Some(bit) = (column.labels.len()..mask.len() * 8).find(|index| selected(*index)) {
        return Err(SaciError::generic(format!(
            "column '{}': the binlog SET has bit {bit} set, beyond the column's {} label(s)",
            column.spec.name,
            column.labels.len()
        )));
    }
    let joined = column
        .labels
        .iter()
        .enumerate()
        .filter(|(index, _)| selected(*index))
        .map(|(_, label)| label.as_str())
        .collect::<Vec<_>>()
        .join(",");
    Ok(if joined.is_empty() {
        Cell::Text(b"")
    } else {
        Cell::Owned(joined)
    })
}

/// The epoch text of a binlog `TIMESTAMP`: `sec` or `sec.micros`.
///
/// A stored zero is the zero timestamp, which the protocol reports as
/// `0000-00-00 00:00:00`, so it becomes the same zero date here.
fn timestamp_text<'a>(column: &ResolvedColumn, text: &[u8]) -> Result<Cell<'a>, SaciError> {
    let bad = || {
        SaciError::generic(format!(
            "column '{}': the binlog TIMESTAMP is not epoch seconds",
            column.spec.name
        ))
    };
    let text = std::str::from_utf8(text).map_err(|_| bad())?;
    let (seconds, micros) = match text.split_once('.') {
        None => (text, 0),
        Some((seconds, fraction)) => {
            if fraction.is_empty()
                || fraction.len() > 6
                || !fraction.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(bad());
            }
            let digits: i64 = fraction.parse().map_err(|_| bad())?;
            (seconds, digits * 10i64.pow(6 - fraction.len() as u32))
        }
    };
    let seconds: i64 = seconds.parse().map_err(|_| bad())?;
    if seconds == 0 && micros == 0 {
        return Ok(Civil {
            year: 0,
            month: 0,
            day: 0,
            hour: 0,
            minute: 0,
            second: 0,
            micros: 0,
        }
        .into());
    }
    seconds
        .checked_mul(MICROS_PER_SECOND)
        .and_then(|base| base.checked_add(micros))
        .map(Cell::EpochMicros)
        .ok_or_else(bad)
}

// ------------------------------------------------------------------- render

/// `YYYY-MM-DD`.
fn write_date(out: &mut String, year: i64, month: u32, day: u32) {
    let _ = write!(out, "{year:04}-{month:02}-{day:02}");
}

/// `.` and the first `fractional` of the six fraction digits, or nothing for a
/// column that keeps no fraction.
fn write_fraction(out: &mut String, micros: u32, fractional: u8) {
    let digits = u32::from(fractional.min(6));
    if digits == 0 {
        return;
    }
    let shown = micros / 10u32.pow(6 - digits);
    let _ = write!(out, ".{shown:0width$}", width = digits as usize);
}

/// `HH:MM:SS` and the fraction.
fn write_clock(
    out: &mut String,
    hours: u32,
    minutes: u8,
    seconds: u8,
    micros: u32,
    fractional: u8,
) {
    let _ = write!(out, "{hours:02}:{minutes:02}:{seconds:02}");
    write_fraction(out, micros, fractional);
}

/// `[-]HH:MM:SS[.f]`, the way the server prints a `TIME`.
fn time_text(
    negative: bool,
    hours: u32,
    minutes: u8,
    seconds: u8,
    micros: u32,
    fractional: u8,
) -> String {
    let mut out = String::with_capacity(16);
    if negative {
        out.push('-');
    }
    write_clock(&mut out, hours, minutes, seconds, micros, fractional);
    out
}

/// The server's text for a temporal, integer or year `cell`, appended to
/// `out`.
fn render(out: &mut String, cell: &Cell<'_>, family: Family, fractional: u8) -> Result<(), String> {
    match cell {
        Cell::Int(v) if family == Family::Year => {
            let _ = write!(out, "{v:04}");
        }
        Cell::Int(v) => {
            let _ = write!(out, "{v}");
        }
        Cell::UInt(v) => {
            let _ = write!(out, "{v}");
        }
        Cell::Date { year, month, day } => {
            write_date(out, i64::from(*year), u32::from(*month), u32::from(*day));
        }
        Cell::DateTime {
            year,
            month,
            day,
            hour,
            minute,
            second,
            micros,
        } => {
            write_date(out, i64::from(*year), u32::from(*month), u32::from(*day));
            out.push(' ');
            write_clock(out, u32::from(*hour), *minute, *second, *micros, fractional);
        }
        Cell::EpochMicros(micros) => {
            let (year, month, day) = civil_from_days(micros.div_euclid(MICROS_PER_DAY));
            let within_day = micros.rem_euclid(MICROS_PER_DAY);
            let seconds = within_day / MICROS_PER_SECOND;
            write_date(out, year, month, day);
            out.push(' ');
            write_clock(
                out,
                (seconds / 3600) as u32,
                (seconds % 3600 / 60) as u8,
                (seconds % 60) as u8,
                (within_day % MICROS_PER_SECOND) as u32,
                fractional,
            );
        }
        Cell::Time {
            negative,
            hours,
            minutes,
            seconds,
            micros,
        } => out.push_str(&time_text(
            *negative, *hours, *minutes, *seconds, *micros, fractional,
        )),
        other => {
            return Err(format!(
                "a {} value cannot be written as text",
                other.kind()
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- the builder

/// One Arrow builder per declared column, dispatched once at construction.
enum Kind {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    /// `decimal128`, with the declared scale and the first unscaled value too
    /// large for the declared precision.
    Decimal128(Decimal128Builder, i8, i128),
    /// `utf8` and `json`.
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date32(Date32Builder),
    Time64(Time64MicrosecondBuilder),
    /// `timestamp_micros` and `timestamp_micros_utc`; the zone lives in the
    /// data type.
    Timestamp(TimestampMicrosecondBuilder),
}

/// Fills one declared column of one batch from [`Cell`]s.
///
/// Built once per column per batch from the [`ResolvedColumn`], which fixes
/// everything a value is judged by: the declared type, nullability, how a
/// non-text value renders as `utf8`, and whether a JSON document is
/// canonicalised.
pub(crate) struct ColumnBuilder {
    name: String,
    nullable: bool,
    family: Family,
    /// Fraction digits a `time`, `datetime` or `timestamp` column keeps, which
    /// is how many a `utf8` rendering shows.
    fractional: u8,
    kind: Kind,
    /// Reused for the text of every value rendered, so the loop allocates once.
    scratch: String,
}

impl ColumnBuilder {
    /// A builder for `column`, sized for `capacity` rows.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when a `decimal128` declares an
    /// illegal precision or scale.
    pub(crate) fn new(column: &ResolvedColumn, capacity: usize) -> Result<Self, SaciError> {
        let spec = &column.spec;
        let kind = match spec.ty {
            T::Boolean => Kind::Boolean(BooleanBuilder::with_capacity(capacity)),
            T::Int8 => Kind::Int8(Int8Builder::with_capacity(capacity)),
            T::Int16 => Kind::Int16(Int16Builder::with_capacity(capacity)),
            T::Int32 => Kind::Int32(Int32Builder::with_capacity(capacity)),
            T::Int64 => Kind::Int64(Int64Builder::with_capacity(capacity)),
            T::UInt8 => Kind::UInt8(UInt8Builder::with_capacity(capacity)),
            T::UInt16 => Kind::UInt16(UInt16Builder::with_capacity(capacity)),
            T::UInt32 => Kind::UInt32(UInt32Builder::with_capacity(capacity)),
            T::UInt64 => Kind::UInt64(UInt64Builder::with_capacity(capacity)),
            T::Float32 => Kind::Float32(Float32Builder::with_capacity(capacity)),
            T::Float64 => Kind::Float64(Float64Builder::with_capacity(capacity)),
            T::Decimal128 => {
                let (precision, scale) = spec.decimal_params()?;
                let builder = Decimal128Builder::with_capacity(capacity)
                    .with_precision_and_scale(precision, scale)
                    .map_err(|e| {
                        SaciError::configuration(format!(
                            "field '{}': invalid decimal128 precision or scale: {e}",
                            spec.name
                        ))
                    })?;
                Kind::Decimal128(builder, scale, 10i128.pow(u32::from(precision)))
            }
            // 16 bytes per value is a starting guess for the data buffer; the
            // builder grows it as needed.
            T::Utf8 | T::Json => Kind::Utf8(StringBuilder::with_capacity(capacity, capacity * 16)),
            T::Binary => Kind::Binary(BinaryBuilder::with_capacity(capacity, capacity * 16)),
            T::Date32 => Kind::Date32(Date32Builder::with_capacity(capacity)),
            T::Time64Micros => Kind::Time64(Time64MicrosecondBuilder::with_capacity(capacity)),
            T::TimestampMicros => {
                Kind::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            T::TimestampMicrosUtc => Kind::Timestamp(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_timezone("UTC"),
            ),
        };
        Ok(Self {
            name: spec.name.clone(),
            nullable: spec.nullable,
            family: column.family,
            fractional: column.fractional,
            kind,
            scratch: String::new(),
        })
    }

    /// How many values the builder holds.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        use arrow_array::builder::ArrayBuilder;
        match &self.kind {
            Kind::Boolean(b) => b.len(),
            Kind::Int8(b) => b.len(),
            Kind::Int16(b) => b.len(),
            Kind::Int32(b) => b.len(),
            Kind::Int64(b) => b.len(),
            Kind::UInt8(b) => b.len(),
            Kind::UInt16(b) => b.len(),
            Kind::UInt32(b) => b.len(),
            Kind::UInt64(b) => b.len(),
            Kind::Float32(b) => b.len(),
            Kind::Float64(b) => b.len(),
            Kind::Decimal128(b, ..) => b.len(),
            Kind::Utf8(b) => b.len(),
            Kind::Binary(b) => b.len(),
            Kind::Date32(b) => b.len(),
            Kind::Time64(b) => b.len(),
            Kind::Timestamp(b) => b.len(),
        }
    }

    /// Append NULL.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the field is declared
    /// `nullable=#false`.
    pub(crate) fn push_null(&mut self) -> Result<(), SaciError> {
        if !self.nullable {
            return Err(SaciError::generic(format!(
                "column '{}': NULL in a field declared nullable=#false; declare the field \
                 nullable",
                self.name
            )));
        }
        self.append_null();
        Ok(())
    }

    /// Append NULL without consulting the field's nullability.
    fn append_null(&mut self) {
        match &mut self.kind {
            Kind::Boolean(b) => b.append_null(),
            Kind::Int8(b) => b.append_null(),
            Kind::Int16(b) => b.append_null(),
            Kind::Int32(b) => b.append_null(),
            Kind::Int64(b) => b.append_null(),
            Kind::UInt8(b) => b.append_null(),
            Kind::UInt16(b) => b.append_null(),
            Kind::UInt32(b) => b.append_null(),
            Kind::UInt64(b) => b.append_null(),
            Kind::Float32(b) => b.append_null(),
            Kind::Float64(b) => b.append_null(),
            Kind::Decimal128(b, ..) => b.append_null(),
            Kind::Utf8(b) => b.append_null(),
            Kind::Binary(b) => b.append_null(),
            Kind::Date32(b) => b.append_null(),
            Kind::Time64(b) => b.append_null(),
            Kind::Timestamp(b) => b.append_null(),
        }
    }

    /// Append one value, or nothing when it is refused.
    ///
    /// A zero date (`0000-00-00`) into a `date32` or a timestamp becomes NULL
    /// on a nullable field and is an error otherwise: Arrow has no such day.
    /// Into `utf8` it stays the text the server prints.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] naming the column when the value does not
    /// fit the declared type, is not valid UTF-8 for a `utf8` field, is a JSON
    /// document that does not parse, is a date that does not exist, is a
    /// `TIME` outside a time of day, or is NULL for a field declared
    /// `nullable=#false`.
    pub(crate) fn push(&mut self, cell: Cell<'_>) -> Result<(), SaciError> {
        if matches!(cell, Cell::Null) {
            return self.push_null();
        }
        let zero_date = matches!(
            &cell,
            Cell::Date {
                year: 0,
                month: 0,
                day: 0
            } | Cell::DateTime {
                year: 0,
                month: 0,
                day: 0,
                ..
            }
        );
        if zero_date && matches!(self.kind, Kind::Date32(_) | Kind::Timestamp(_)) {
            if !self.nullable {
                return Err(SaciError::generic(format!(
                    "column '{}': zero date; declare the field nullable",
                    self.name
                )));
            }
            self.append_null();
            return Ok(());
        }

        let name = self.name.as_str();
        match &mut self.kind {
            Kind::Boolean(b) => b.append_value(boolean(name, &cell)?),
            Kind::Int8(b) => b.append_value(narrow(name, T::Int8, signed(name, T::Int8, &cell)?)?),
            Kind::Int16(b) => {
                b.append_value(narrow(name, T::Int16, signed(name, T::Int16, &cell)?)?);
            }
            Kind::Int32(b) => {
                b.append_value(narrow(name, T::Int32, signed(name, T::Int32, &cell)?)?);
            }
            Kind::Int64(b) => b.append_value(signed(name, T::Int64, &cell)?),
            Kind::UInt8(b) => {
                b.append_value(narrow_unsigned(
                    name,
                    T::UInt8,
                    unsigned(name, T::UInt8, &cell)?,
                )?);
            }
            Kind::UInt16(b) => {
                b.append_value(narrow_unsigned(
                    name,
                    T::UInt16,
                    unsigned(name, T::UInt16, &cell)?,
                )?);
            }
            Kind::UInt32(b) => {
                b.append_value(narrow_unsigned(
                    name,
                    T::UInt32,
                    unsigned(name, T::UInt32, &cell)?,
                )?);
            }
            Kind::UInt64(b) => b.append_value(unsigned(name, T::UInt64, &cell)?),
            Kind::Float32(b) => b.append_value(float(name, T::Float32, &cell)? as f32),
            Kind::Float64(b) => b.append_value(float(name, T::Float64, &cell)?),
            Kind::Decimal128(b, scale, limit) => match &cell {
                Cell::Text(bytes) => b.append_value(parse_decimal(name, bytes, *scale, *limit)?),
                Cell::Owned(text) => {
                    b.append_value(parse_decimal(name, text.as_bytes(), *scale, *limit)?);
                }
                other => return Err(mismatch(name, T::Decimal128, other)),
            },
            Kind::Utf8(b) => {
                let text = text_of(name, self.family, self.fractional, &mut self.scratch, &cell)?;
                b.append_value(text);
            }
            Kind::Binary(b) => match &cell {
                Cell::Text(bytes) => b.append_value(bytes),
                Cell::Owned(text) => b.append_value(text.as_bytes()),
                other => return Err(mismatch(name, T::Binary, other)),
            },
            Kind::Date32(b) => match &cell {
                Cell::Date { year, month, day } => {
                    let civil = Civil {
                        year: *year,
                        month: *month,
                        day: *day,
                        hour: 0,
                        minute: 0,
                        second: 0,
                        micros: 0,
                    };
                    require_real_date(name, &civil)?;
                    let days = civil.days();
                    b.append_value(i32::try_from(days).map_err(|_| {
                        SaciError::generic(format!(
                            "column '{name}': date {days} days from the epoch does not fit date32"
                        ))
                    })?);
                }
                other => return Err(mismatch(name, T::Date32, other)),
            },
            Kind::Time64(b) => match &cell {
                Cell::Time {
                    negative,
                    hours,
                    minutes,
                    seconds,
                    micros,
                } => {
                    let zero = *hours == 0 && *minutes == 0 && *seconds == 0 && *micros == 0;
                    if (*negative && !zero) || *hours >= 24 {
                        return Err(SaciError::generic(format!(
                            "column '{name}': TIME value '{}' is outside a time of day; declare \
                             the field utf8",
                            time_text(
                                *negative,
                                *hours,
                                *minutes,
                                *seconds,
                                *micros,
                                self.fractional
                            )
                        )));
                    }
                    let whole =
                        (i64::from(*hours) * 60 + i64::from(*minutes)) * 60 + i64::from(*seconds);
                    b.append_value(whole * MICROS_PER_SECOND + i64::from(*micros));
                }
                other => return Err(mismatch(name, T::Time64Micros, other)),
            },
            Kind::Timestamp(b) => match &cell {
                Cell::EpochMicros(micros) => b.append_value(*micros),
                Cell::DateTime {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    micros,
                } => {
                    let civil = Civil {
                        year: *year,
                        month: *month,
                        day: *day,
                        hour: *hour,
                        minute: *minute,
                        second: *second,
                        micros: *micros,
                    };
                    require_real_date(name, &civil)?;
                    b.append_value(civil.epoch_micros());
                }
                other => return Err(mismatch(name, T::TimestampMicros, other)),
            },
        }
        Ok(())
    }

    /// Finish the array, resetting the builder for the next batch.
    pub(crate) fn finish(&mut self) -> ArrayRef {
        match &mut self.kind {
            Kind::Boolean(b) => Arc::new(b.finish()),
            Kind::Int8(b) => Arc::new(b.finish()),
            Kind::Int16(b) => Arc::new(b.finish()),
            Kind::Int32(b) => Arc::new(b.finish()),
            Kind::Int64(b) => Arc::new(b.finish()),
            Kind::UInt8(b) => Arc::new(b.finish()),
            Kind::UInt16(b) => Arc::new(b.finish()),
            Kind::UInt32(b) => Arc::new(b.finish()),
            Kind::UInt64(b) => Arc::new(b.finish()),
            Kind::Float32(b) => Arc::new(b.finish()),
            Kind::Float64(b) => Arc::new(b.finish()),
            Kind::Decimal128(b, ..) => Arc::new(b.finish()),
            Kind::Utf8(b) => Arc::new(b.finish()),
            Kind::Binary(b) => Arc::new(b.finish()),
            Kind::Date32(b) => Arc::new(b.finish()),
            Kind::Time64(b) => Arc::new(b.finish()),
            Kind::Timestamp(b) => Arc::new(b.finish()),
        }
    }
}

// ----------------------------------------------------------------- conversion

/// A cell of a kind the declared type cannot take.
fn mismatch(name: &str, ty: MariadbFieldType, cell: &Cell<'_>) -> SaciError {
    SaciError::generic(format!(
        "column '{name}': a {} value cannot fill a declared \"{}\"",
        cell.kind(),
        ty.as_str()
    ))
}

/// A value outside the declared integer type.
fn out_of_range(name: &str, value: impl std::fmt::Display, ty: MariadbFieldType) -> SaciError {
    SaciError::generic(format!(
        "column '{name}': value {value} does not fit a declared \"{}\"",
        ty.as_str()
    ))
}

/// A cell as the signed integer a declared signed type holds.
fn signed(name: &str, ty: MariadbFieldType, cell: &Cell<'_>) -> Result<i64, SaciError> {
    match cell {
        Cell::Int(v) => Ok(*v),
        Cell::UInt(v) => i64::try_from(*v).map_err(|_| out_of_range(name, v, ty)),
        other => Err(mismatch(name, ty, other)),
    }
}

/// A cell as the unsigned integer a declared unsigned type holds.
fn unsigned(name: &str, ty: MariadbFieldType, cell: &Cell<'_>) -> Result<u64, SaciError> {
    match cell {
        Cell::UInt(v) => Ok(*v),
        Cell::Int(v) => u64::try_from(*v).map_err(|_| out_of_range(name, v, ty)),
        other => Err(mismatch(name, ty, other)),
    }
}

/// `value` as the narrower signed type `N`.
fn narrow<N: TryFrom<i64>>(name: &str, ty: MariadbFieldType, value: i64) -> Result<N, SaciError> {
    N::try_from(value).map_err(|_| out_of_range(name, value, ty))
}

/// `value` as the narrower unsigned type `N`.
fn narrow_unsigned<N: TryFrom<u64>>(
    name: &str,
    ty: MariadbFieldType,
    value: u64,
) -> Result<N, SaciError> {
    N::try_from(value).map_err(|_| out_of_range(name, value, ty))
}

/// A `tinyint` flag: 0 or 1 and nothing else.
fn boolean(name: &str, cell: &Cell<'_>) -> Result<bool, SaciError> {
    match cell {
        Cell::Int(0) | Cell::UInt(0) => Ok(false),
        Cell::Int(1) | Cell::UInt(1) => Ok(true),
        Cell::Int(v) => Err(not_a_flag(name, v)),
        Cell::UInt(v) => Err(not_a_flag(name, v)),
        other => Err(mismatch(name, T::Boolean, other)),
    }
}

fn not_a_flag(name: &str, value: impl std::fmt::Display) -> SaciError {
    SaciError::generic(format!(
        "column '{name}': value {value} is not a boolean, 0 or 1; declare the field int8"
    ))
}

/// A cell as the float a declared float type holds.
fn float(name: &str, ty: MariadbFieldType, cell: &Cell<'_>) -> Result<f64, SaciError> {
    match cell {
        Cell::Float(v) => Ok(*v),
        other => Err(mismatch(name, ty, other)),
    }
}

/// A date the calendar has.
fn require_real_date(name: &str, civil: &Civil) -> Result<(), SaciError> {
    if civil.is_real_date() {
        return Ok(());
    }
    let mut shown = String::new();
    write_date(
        &mut shown,
        i64::from(civil.year),
        u32::from(civil.month),
        u32::from(civil.day),
    );
    Err(SaciError::generic(format!(
        "column '{name}': date '{shown}' does not exist, so Arrow cannot hold it; declare the \
         field utf8"
    )))
}

/// The text of a `utf8` or `json` value.
///
/// Text is checked as UTF-8 and a MySQL `json` document is canonicalised;
/// every other kind renders as the server prints it.
fn text_of<'s>(
    name: &str,
    family: Family,
    fractional: u8,
    scratch: &'s mut String,
    cell: &'s Cell<'_>,
) -> Result<Cow<'s, str>, SaciError> {
    let text: &str = match cell {
        Cell::Text(bytes) => std::str::from_utf8(bytes).map_err(|_| {
            SaciError::generic(format!(
                "column '{name}': not valid UTF-8; declare it binary"
            ))
        })?,
        Cell::Owned(text) => text,
        other => {
            scratch.clear();
            render(scratch, other, family, fractional)
                .map_err(|reason| SaciError::generic(format!("column '{name}': {reason}")))?;
            return Ok(Cow::Borrowed(scratch.as_str()));
        }
    };
    if family == Family::Json {
        return canonical_json(name, text).map(Cow::Owned);
    }
    Ok(Cow::Borrowed(text))
}

/// A JSON document as compact text with every object's keys sorted.
///
/// A MySQL `json` column prints with `", "` and `": "` separators and orders
/// its keys by length first, while the binlog hands back a binary form. Both
/// go through here, so every mode emits the same text for the same document.
fn canonical_json(name: &str, text: &str) -> Result<String, SaciError> {
    let mut document: serde_json::Value = serde_json::from_str(text).map_err(|e| {
        SaciError::generic(format!(
            "column '{name}': the JSON document does not parse: {e}"
        ))
    })?;
    document.sort_all_objects();
    Ok(document.to_string())
}

/// The unscaled integer of the exact decimal `text`, padded to `scale`
/// fraction digits.
///
/// The server renders a decimal at its column's own scale, so the text carries
/// at most `scale` fraction digits; one more is an error rather than a
/// rounding. `limit` is the first unscaled magnitude the declared precision
/// cannot hold.
fn parse_decimal(name: &str, text: &[u8], scale: i8, limit: i128) -> Result<i128, SaciError> {
    let bad = || {
        SaciError::generic(format!(
            "column '{name}': '{}' is not a decimal number",
            String::from_utf8_lossy(text)
        ))
    };
    let (negative, digits) = match text.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, text),
    };
    let scale = u32::from(scale.unsigned_abs());
    let mut value: i128 = 0;
    let mut seen_point = false;
    let mut seen_digit = false;
    let mut fraction = 0u32;
    for byte in digits {
        match byte {
            b'0'..=b'9' => {
                value = value
                    .checked_mul(10)
                    .and_then(|v| v.checked_add(i128::from(byte - b'0')))
                    .ok_or_else(bad)?;
                seen_digit = true;
                if seen_point {
                    fraction += 1;
                }
            }
            b'.' if !seen_point => seen_point = true,
            _ => return Err(bad()),
        }
    }
    if !seen_digit {
        return Err(bad());
    }
    if fraction > scale {
        return Err(SaciError::generic(format!(
            "column '{name}': '{}' has {fraction} fraction digit(s), more than the declared scale \
             {scale}",
            String::from_utf8_lossy(text)
        )));
    }
    for _ in fraction..scale {
        value = value.checked_mul(10).ok_or_else(bad)?;
    }
    if value >= limit {
        return Err(SaciError::generic(format!(
            "column '{name}': '{}' does not fit the declared precision",
            String::from_utf8_lossy(text)
        )));
    }
    Ok(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, StringArray, Time64MicrosecondArray,
        TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, TimeUnit};
    use mysql_common::binlog::jsonb;
    use mysql_common::io::ParseBuf;
    use mysql_common::proto::MyDeserialize;

    use super::*;
    use crate::config::FieldSpec;
    use crate::types::{Access, CatalogColumn, validate_columns};

    /// A catalog column of `data_type` and `column_type`, with room for the
    /// optional numbers a type carries.
    fn catalog(data_type: &str, column_type: &str) -> CatalogColumn {
        CatalogColumn {
            name: "c".to_string(),
            ordinal: 0,
            data_type: data_type.to_string(),
            column_type: column_type.to_string(),
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
            charset: None,
        }
    }

    /// `catalog` resolved against a field declared `ty` through the same check
    /// the readers run.
    fn column_of(catalog: CatalogColumn, ty: MariadbFieldType, nullable: bool) -> ResolvedColumn {
        let decimal = ty == T::Decimal128;
        let spec = FieldSpec {
            name: "c".to_string(),
            ty,
            nullable,
            precision: decimal.then(|| catalog.numeric_precision.unwrap_or(10) as u8),
            scale: decimal.then(|| catalog.numeric_scale.unwrap_or(2) as i8),
            mariadb_type: None,
        };
        validate_columns("Test", Access::Query, [&spec], &[catalog])
            .expect("the pair is legal")
            .remove(0)
    }

    fn resolved(data_type: &str, column_type: &str, ty: MariadbFieldType) -> ResolvedColumn {
        column_of(catalog(data_type, column_type), ty, true)
    }

    /// A column with `datetime_precision` fraction digits.
    fn temporal(data_type: &str, digits: u64, ty: MariadbFieldType) -> ResolvedColumn {
        let mut column = catalog(data_type, data_type);
        column.datetime_precision = Some(digits);
        column_of(column, ty, true)
    }

    fn protocol(value: &Value, column: &ResolvedColumn) -> Cell<'static> {
        // The cell borrows the value, so own what it holds for the test.
        match Cell::from_protocol(value, column).expect("protocol cell") {
            Cell::Text(bytes) => Cell::Owned(String::from_utf8_lossy(bytes).into_owned()),
            Cell::Null => Cell::Null,
            Cell::Int(v) => Cell::Int(v),
            Cell::UInt(v) => Cell::UInt(v),
            Cell::Float(v) => Cell::Float(v),
            Cell::Owned(text) => Cell::Owned(text),
            Cell::Date { year, month, day } => Cell::Date { year, month, day },
            Cell::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                micros,
            } => Cell::DateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                micros,
            },
            Cell::Time {
                negative,
                hours,
                minutes,
                seconds,
                micros,
            } => Cell::Time {
                negative,
                hours,
                minutes,
                seconds,
                micros,
            },
            Cell::EpochMicros(v) => Cell::EpochMicros(v),
        }
    }

    fn binlog(value: BinlogValue<'static>, column: &ResolvedColumn) -> Cell<'static> {
        match Cell::from_binlog(&value, column).expect("binlog cell") {
            Cell::Text(bytes) => Cell::Owned(String::from_utf8_lossy(bytes).into_owned()),
            Cell::Owned(text) => Cell::Owned(text),
            Cell::Null => Cell::Null,
            Cell::Int(v) => Cell::Int(v),
            Cell::UInt(v) => Cell::UInt(v),
            Cell::Float(v) => Cell::Float(v),
            Cell::EpochMicros(v) => Cell::EpochMicros(v),
            other => panic!("unexpected {other:?}"),
        }
    }

    fn binlog_err(value: BinlogValue<'static>, column: &ResolvedColumn) -> String {
        Cell::from_binlog(&value, column)
            .expect_err("the value must be refused")
            .message()
    }

    fn plain(value: Value) -> BinlogValue<'static> {
        BinlogValue::Value(value)
    }

    /// Build one array of `ty` from `cells`.
    fn build(column: &ResolvedColumn, cells: Vec<Cell<'_>>) -> ArrayRef {
        let mut builder = ColumnBuilder::new(column, cells.len()).expect("builder");
        for cell in cells {
            builder.push(cell).expect("push");
        }
        builder.finish()
    }

    fn push_err(column: &ResolvedColumn, cell: Cell<'_>) -> String {
        let mut builder = ColumnBuilder::new(column, 1).expect("builder");
        builder
            .push(cell)
            .expect_err("the cell must be refused")
            .message()
    }

    fn strings(array: &ArrayRef) -> Vec<Option<String>> {
        let array = array.as_any().downcast_ref::<StringArray>().expect("utf8");
        (0..array.len())
            .map(|i| (!array.is_null(i)).then(|| array.value(i).to_string()))
            .collect()
    }

    fn text(value: &'static str) -> Cell<'static> {
        Cell::Text(value.as_bytes())
    }

    // ------------------------------------------------------------- the calendar

    #[test]
    fn day_zero_is_the_unix_epoch_and_the_range_ends_are_where_the_servers_stop() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(0, 1, 1), -719_528);
        assert_eq!(civil_from_days(-719_528), (0, 1, 1));
        assert_eq!(days_from_civil(9999, 12, 31), 2_932_896);
        assert_eq!(civil_from_days(2_932_896), (9999, 12, 31));
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
    }

    #[test]
    fn every_day_between_year_zero_and_year_9999_round_trips() {
        for days in (-719_528..=2_932_896).step_by(7) {
            let (year, month, day) = civil_from_days(days);
            assert_eq!(
                days_from_civil(year, month, day),
                days,
                "{year}-{month}-{day}"
            );
        }
    }

    #[test]
    fn a_leap_day_exists_only_in_a_leap_year() {
        assert_eq!(days_in_month(2024, 2), 29);
        assert_eq!(days_in_month(2000, 2), 29);
        assert_eq!(days_in_month(1900, 2), 28);
        assert_eq!(days_in_month(2023, 2), 28);
        assert_eq!(days_in_month(2023, 4), 30);
        assert_eq!(days_in_month(2023, 12), 31);
    }

    // ------------------------------------------------------------- from_protocol

    #[test]
    fn protocol_integers_keep_their_sign_and_a_value_above_i64_stays_unsigned() {
        let column = resolved("bigint", "bigint(20) unsigned", T::UInt64);
        assert_eq!(protocol(&Value::Int(7), &column), Cell::Int(7));
        assert_eq!(
            protocol(&Value::UInt(u64::MAX), &column),
            Cell::UInt(u64::MAX)
        );
        assert_eq!(protocol(&Value::NULL, &column), Cell::Null);
    }

    #[test]
    fn protocol_floats_widen_exactly() {
        let column = resolved("float", "float", T::Float64);
        assert_eq!(protocol(&Value::Float(0.5), &column), Cell::Float(0.5));
        assert_eq!(
            protocol(&Value::Float(0.1), &column),
            Cell::Float(f64::from(0.1f32))
        );
        let column = resolved("double", "double", T::Float64);
        assert_eq!(protocol(&Value::Double(0.1), &column), Cell::Float(0.1));
    }

    #[test]
    fn protocol_text_and_blobs_are_bytes() {
        let column = resolved("varchar", "varchar(20)", T::Utf8);
        assert_eq!(
            protocol(&Value::Bytes(b"abc".to_vec()), &column),
            Cell::Owned("abc".to_string())
        );
        let column = resolved("blob", "blob", T::Binary);
        let value = Value::Bytes(vec![0, 255]);
        assert_eq!(
            Cell::from_protocol(&value, &column).expect("cell"),
            Cell::Text(&[0, 255])
        );
    }

    #[test]
    fn a_bit_value_folds_big_endian_unless_it_is_read_as_bytes() {
        let as_number = resolved("bit", "bit(12)", T::UInt64);
        let value = Value::Bytes(vec![0x0a, 0xbc]);
        assert_eq!(
            Cell::from_protocol(&value, &as_number).expect("cell"),
            Cell::UInt(0x0abc)
        );
        let as_bytes = resolved("bit", "bit(12)", T::Binary);
        assert_eq!(
            Cell::from_protocol(&value, &as_bytes).expect("cell"),
            Cell::Text(&[0x0a, 0xbc])
        );
    }

    #[test]
    fn a_bit_value_wider_than_sixty_four_bits_is_refused() {
        let column = resolved("bit", "bit(64)", T::UInt64);
        let value = Value::Bytes(vec![1; 9]);
        let err = Cell::from_protocol(&value, &column)
            .expect_err("nine bytes")
            .message();
        assert!(err.contains("column 'c'"), "{err}");
    }

    #[test]
    fn a_protocol_date_is_a_date_and_a_datetime_keeps_its_fraction() {
        let date = resolved("date", "date", T::Date32);
        assert_eq!(
            protocol(&Value::Date(2024, 2, 29, 0, 0, 0, 0), &date),
            Cell::Date {
                year: 2024,
                month: 2,
                day: 29
            }
        );
        let datetime = temporal("datetime", 6, T::TimestampMicros);
        assert_eq!(
            protocol(&Value::Date(2024, 2, 29, 23, 59, 58, 123_456), &datetime),
            Cell::DateTime {
                year: 2024,
                month: 2,
                day: 29,
                hour: 23,
                minute: 59,
                second: 58,
                micros: 123_456
            }
        );
    }

    #[test]
    fn a_protocol_timestamp_is_the_utc_instant() {
        let column = temporal("timestamp", 0, T::TimestampMicrosUtc);
        assert_eq!(
            protocol(&Value::Date(2024, 1, 2, 3, 4, 5, 6), &column),
            Cell::EpochMicros(1_704_164_645_000_006)
        );
        assert_eq!(
            protocol(&Value::Date(1970, 1, 1, 0, 0, 1, 0), &column),
            Cell::EpochMicros(1_000_000)
        );
    }

    #[test]
    fn a_zero_protocol_timestamp_becomes_the_zero_date_marker() {
        let column = temporal("timestamp", 0, T::TimestampMicrosUtc);
        assert_eq!(
            protocol(&Value::Date(0, 0, 0, 0, 0, 0, 0), &column),
            Cell::DateTime {
                year: 0,
                month: 0,
                day: 0,
                hour: 0,
                minute: 0,
                second: 0,
                micros: 0
            }
        );
    }

    #[test]
    fn an_impossible_protocol_timestamp_is_refused_by_name() {
        let column = temporal("timestamp", 0, T::TimestampMicrosUtc);
        let value = Value::Date(2024, 2, 30, 0, 0, 0, 0);
        let err = Cell::from_protocol(&value, &column)
            .expect_err("Feb 30")
            .message();
        assert!(err.contains("column 'c'"), "{err}");
    }

    #[test]
    fn a_protocol_time_counts_days_as_hours() {
        let column = temporal("time", 0, T::Utf8);
        assert_eq!(
            protocol(&Value::Time(true, 34, 22, 59, 59, 500_000), &column),
            Cell::Time {
                negative: true,
                hours: 34 * 24 + 22,
                minutes: 59,
                seconds: 59,
                micros: 500_000
            }
        );
    }

    #[test]
    fn a_value_of_a_kind_the_column_never_produces_is_refused_by_name() {
        let int = resolved("int", "int(11)", T::Int32);
        for value in [
            Value::Bytes(b"x".to_vec()),
            Value::Date(2024, 1, 1, 0, 0, 0, 0),
            Value::Time(false, 0, 0, 0, 0, 0),
        ] {
            let err = Cell::from_protocol(&value, &int)
                .expect_err("wrong kind")
                .message();
            assert!(
                err.contains("column 'c'") && err.contains("int column"),
                "{err}"
            );
        }
    }

    // --------------------------------------------------------------- from_binlog

    #[test]
    fn a_binlog_timestamp_is_epoch_text() {
        let column = temporal("timestamp", 6, T::TimestampMicrosUtc);
        assert_eq!(
            binlog(plain(Value::Bytes(b"1704164645".to_vec())), &column),
            Cell::EpochMicros(1_704_164_645_000_000)
        );
        assert_eq!(
            binlog(plain(Value::Bytes(b"1704164645.000006".to_vec())), &column),
            Cell::EpochMicros(1_704_164_645_000_006)
        );
        assert_eq!(
            binlog(plain(Value::Bytes(b"1704164645.5".to_vec())), &column),
            Cell::EpochMicros(1_704_164_645_500_000)
        );
    }

    #[test]
    fn a_stored_zero_timestamp_is_the_zero_date_like_the_protocol_reports_it() {
        let column = temporal("timestamp", 0, T::TimestampMicrosUtc);
        let value = plain(Value::Bytes(b"0".to_vec()));
        assert_eq!(
            Cell::from_binlog(&value, &column).expect("cell"),
            Cell::DateTime {
                year: 0,
                month: 0,
                day: 0,
                hour: 0,
                minute: 0,
                second: 0,
                micros: 0
            }
        );
    }

    #[test]
    fn a_malformed_binlog_timestamp_is_refused() {
        let column = temporal("timestamp", 6, T::TimestampMicrosUtc);
        for bad in [
            "",
            "x",
            "12.",
            "12.1234567",
            "12.x",
            "1.2.3",
            "12.-5",
            "12.+1",
        ] {
            let err = binlog_err(plain(Value::Bytes(bad.as_bytes().to_vec())), &column);
            assert!(err.contains("column 'c'"), "{bad}: {err}");
        }
    }

    #[test]
    fn a_binlog_year_is_text_and_the_zero_year_reads_back_as_1900() {
        let column = resolved("year", "year(4)", T::Int32);
        assert_eq!(
            binlog(plain(Value::Bytes(b"2024".to_vec())), &column),
            Cell::Int(2024)
        );
        assert_eq!(
            binlog(plain(Value::Bytes(b"1901".to_vec())), &column),
            Cell::Int(1901)
        );
        assert_eq!(
            binlog(plain(Value::Bytes(b"1900".to_vec())), &column),
            Cell::Int(0)
        );
    }

    #[test]
    fn a_binlog_enum_index_picks_the_label_and_zero_is_the_empty_string() {
        let column = resolved("enum", "enum('a','b''c','d')", T::Utf8);
        assert_eq!(
            binlog(plain(Value::Int(1)), &column),
            Cell::Owned("a".to_string())
        );
        assert_eq!(
            binlog(plain(Value::Int(2)), &column),
            Cell::Owned("b'c".to_string())
        );
        assert_eq!(
            binlog(plain(Value::Int(3)), &column),
            Cell::Owned("d".to_string())
        );
        assert_eq!(
            binlog(plain(Value::Int(0)), &column),
            Cell::Owned(String::new())
        );
    }

    #[test]
    fn a_binlog_enum_index_beyond_the_labels_is_refused() {
        let column = resolved("enum", "enum('a','b')", T::Utf8);
        let err = binlog_err(plain(Value::Int(3)), &column);
        assert!(err.contains("index 3") && err.contains("2 label"), "{err}");
    }

    #[test]
    fn a_binlog_set_bitmask_joins_the_labels_in_declaration_order() {
        let column = resolved("set", "set('a','b','c','d')", T::Utf8);
        assert_eq!(
            binlog(plain(Value::Bytes(vec![0b0101])), &column),
            Cell::Owned("a,c".to_string())
        );
        assert_eq!(
            binlog(plain(Value::Bytes(vec![0b1111])), &column),
            Cell::Owned("a,b,c,d".to_string())
        );
        assert_eq!(
            binlog(plain(Value::Bytes(vec![0b1000])), &column),
            Cell::Owned("d".to_string())
        );
        assert_eq!(
            binlog(plain(Value::Bytes(vec![0])), &column),
            Cell::Owned(String::new())
        );
    }

    #[test]
    fn a_binlog_set_mask_spans_bytes_in_little_endian_order() {
        let labels: Vec<String> = (0..10).map(|i| format!("'l{i}'")).collect();
        let column = resolved("set", &format!("set({})", labels.join(",")), T::Utf8);
        // Bit 8 is the lowest bit of the second byte.
        assert_eq!(
            binlog(plain(Value::Bytes(vec![0b1000_0000, 0b01])), &column),
            Cell::Owned("l7,l8".to_string())
        );
    }

    #[test]
    fn a_binlog_set_bit_beyond_the_labels_is_refused() {
        let column = resolved("set", "set('a','b')", T::Utf8);
        let err = binlog_err(plain(Value::Bytes(vec![0b100])), &column);
        assert!(err.contains("bit 2") && err.contains("2 label"), "{err}");
    }

    #[test]
    fn a_sign_extended_unsigned_integer_is_reinterpreted_at_the_columns_width() {
        for (data_type, column_type, bits, stored, expected) in [
            ("tinyint", "tinyint(3) unsigned", 8, -56i64, 200u64),
            ("smallint", "smallint(5) unsigned", 16, -1, 65_535),
            ("mediumint", "mediumint(8) unsigned", 24, -1, 16_777_215),
            ("int", "int(10) unsigned", 32, -1, 4_294_967_295),
            ("bigint", "bigint(20) unsigned", 64, -1, u64::MAX),
        ] {
            let column = resolved(data_type, column_type, T::UInt64);
            assert_eq!(column.family, Family::Integer(bits));
            assert_eq!(
                binlog(plain(Value::Int(stored)), &column),
                Cell::UInt(expected),
                "{data_type}"
            );
        }
    }

    #[test]
    fn a_signed_mediumint_read_unsigned_by_the_decoder_is_sign_extended() {
        let mut column = ResolvedColumn::synthetic(&FieldSpec {
            name: "m".to_string(),
            ty: MariadbFieldType::Int32,
            nullable: true,
            precision: None,
            scale: None,
            mariadb_type: None,
        });
        column.family = Family::Integer(24);
        column.unsigned = false;
        for (raw, want) in [
            (8_388_608, -8_388_608),
            (16_777_215, -1),
            (8_388_607, 8_388_607),
            (0, 0),
        ] {
            let value = BinlogValue::Value(Value::Int(raw));
            assert_eq!(
                Cell::from_binlog(&value, &column).expect("cell"),
                Cell::Int(want)
            );
        }
    }

    #[test]
    fn an_unsigned_integer_the_decoder_already_read_unsigned_is_unchanged() {
        let tiny = resolved("tinyint", "tinyint(3) unsigned", T::UInt8);
        assert_eq!(binlog(plain(Value::Int(200)), &tiny), Cell::UInt(200));
        let big = resolved("bigint", "bigint(20) unsigned", T::UInt64);
        assert_eq!(
            binlog(plain(Value::UInt(u64::MAX)), &big),
            Cell::UInt(u64::MAX)
        );
    }

    #[test]
    fn a_signed_integer_is_left_signed() {
        let column = resolved("int", "int(11)", T::Int32);
        assert_eq!(binlog(plain(Value::Int(-5)), &column), Cell::Int(-5));
    }

    #[test]
    fn a_binlog_bit_is_big_endian_bytes() {
        let column = resolved("bit", "bit(16)", T::UInt64);
        assert_eq!(
            binlog(plain(Value::Bytes(vec![0x12, 0x34])), &column),
            Cell::UInt(0x1234)
        );
    }

    #[test]
    fn a_binlog_json_document_is_rendered_as_text() {
        let column = resolved("json", "json", T::Json);
        for (value, expected) in [
            (jsonb::Value::Null, "null"),
            (jsonb::Value::Bool(true), "true"),
            (jsonb::Value::I16(-7), "-7"),
            (jsonb::Value::U64(u64::MAX), "18446744073709551615"),
            (jsonb::Value::F64(1.5), "1.5"),
            (
                jsonb::Value::String(jsonb::JsonbString::new(b"hi".to_vec())),
                "\"hi\"",
            ),
        ] {
            assert_eq!(
                binlog(BinlogValue::Jsonb(value), &column),
                Cell::Owned(expected.to_string())
            );
        }
    }

    #[test]
    fn a_binlog_json_object_is_rendered_with_every_key() {
        // `{"b": 1, "a": 2}` in the server's small-object layout: a header, a
        // key entry and a value entry per member, then the key bytes. The
        // server orders keys by length then bytes, so `a` precedes `b`.
        let bytes: Vec<u8> = vec![
            0x00, // small object
            0x02, 0x00, // two members
            0x14, 0x00, // 20 bytes in all
            0x12, 0x00, 0x01, 0x00, // key "a" at 18, length 1
            0x13, 0x00, 0x01, 0x00, // key "b" at 19, length 1
            0x05, 0x02, 0x00, // int16 2, inlined
            0x05, 0x01, 0x00, // int16 1, inlined
            b'a', b'b',
        ];
        let json = jsonb::Value::deserialize((), &mut ParseBuf(&bytes)).expect("jsonb");
        let column = resolved("json", "json", T::Json);
        assert_eq!(
            binlog(BinlogValue::Jsonb(json.into_owned()), &column),
            Cell::Owned("{\"a\":2,\"b\":1}".to_string())
        );
    }

    #[test]
    fn a_partial_json_update_names_the_server_setting_to_clear() {
        let column = resolved("json", "json", T::Json);
        let err = binlog_err(BinlogValue::JsonDiff(Vec::new()), &column);
        assert_eq!(
            err,
            "column 'c': a partial JSON update reached the binlog; set binlog_row_value_options \
             = '' on the server"
        );
    }

    #[test]
    fn every_other_binlog_value_takes_the_protocol_path() {
        let column = temporal("datetime", 0, T::TimestampMicros);
        assert_eq!(
            Cell::from_binlog(&plain(Value::Date(2024, 1, 2, 3, 4, 5, 0)), &column).expect("cell"),
            Cell::DateTime {
                year: 2024,
                month: 1,
                day: 2,
                hour: 3,
                minute: 4,
                second: 5,
                micros: 0
            }
        );
        let decimal = column_of(
            CatalogColumn {
                numeric_precision: Some(10),
                numeric_scale: Some(2),
                ..catalog("decimal", "decimal(10,2)")
            },
            T::Decimal128,
            true,
        );
        assert_eq!(
            binlog(plain(Value::Bytes(b"-1.50".to_vec())), &decimal),
            Cell::Owned("-1.50".to_string())
        );
        let null = resolved("int", "int(11)", T::Int32);
        assert_eq!(binlog(plain(Value::NULL), &null), Cell::Null);
    }

    // ----------------------------------------------------------- ColumnBuilder

    #[test]
    fn integers_fill_every_width_and_refuse_a_value_outside_it() {
        let column = resolved("tinyint", "tinyint(4)", T::Int8);
        let array = build(
            &column,
            vec![Cell::Int(i64::from(i8::MIN)), Cell::Int(127), Cell::Null],
        );
        let array = array.as_any().downcast_ref::<Int8Array>().expect("int8");
        assert_eq!(
            (array.value(0), array.value(1), array.is_null(2)),
            (-128, 127, true)
        );
        let err = push_err(&column, Cell::Int(128));
        assert!(
            err.contains("column 'c'") && err.contains("128") && err.contains("int8"),
            "{err}"
        );

        let column = resolved("smallint", "smallint(6)", T::Int16);
        let array = build(&column, vec![Cell::Int(-32_768), Cell::Int(32_767)]);
        let array = array.as_any().downcast_ref::<Int16Array>().expect("int16");
        assert_eq!((array.value(0), array.value(1)), (-32_768, 32_767));

        let column = resolved("int", "int(11)", T::Int32);
        let array = build(&column, vec![Cell::Int(i64::from(i32::MIN))]);
        let array = array.as_any().downcast_ref::<Int32Array>().expect("int32");
        assert_eq!(array.value(0), i32::MIN);

        let column = resolved("bigint", "bigint(20)", T::Int64);
        let array = build(&column, vec![Cell::Int(i64::MIN), Cell::Int(i64::MAX)]);
        let array = array.as_any().downcast_ref::<Int64Array>().expect("int64");
        assert_eq!((array.value(0), array.value(1)), (i64::MIN, i64::MAX));
        assert!(push_err(&column, Cell::UInt(u64::MAX)).contains("does not fit"));
    }

    #[test]
    fn unsigned_integers_fill_every_width_and_refuse_a_negative_value() {
        let column = resolved("tinyint", "tinyint(3) unsigned", T::UInt8);
        let array = build(&column, vec![Cell::UInt(255), Cell::Int(0)]);
        let array = array.as_any().downcast_ref::<UInt8Array>().expect("uint8");
        assert_eq!((array.value(0), array.value(1)), (255, 0));
        assert!(push_err(&column, Cell::UInt(256)).contains("256"));
        assert!(push_err(&column, Cell::Int(-1)).contains("-1"));

        let column = resolved("smallint", "smallint(5) unsigned", T::UInt16);
        let array = build(&column, vec![Cell::UInt(65_535)]);
        let array = array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("uint16");
        assert_eq!(array.value(0), 65_535);

        let column = resolved("int", "int(10) unsigned", T::UInt32);
        let array = build(&column, vec![Cell::UInt(4_294_967_295)]);
        let array = array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("uint32");
        assert_eq!(array.value(0), u32::MAX);

        let column = resolved("bigint", "bigint(20) unsigned", T::UInt64);
        let array = build(&column, vec![Cell::UInt(u64::MAX), Cell::Int(i64::MAX)]);
        let array = array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("uint64");
        assert_eq!(
            (array.value(0), array.value(1)),
            (u64::MAX, i64::MAX as u64)
        );
    }

    #[test]
    fn a_tinyint_flag_is_zero_or_one_and_nothing_else() {
        let column = resolved("tinyint", "tinyint(1)", T::Boolean);
        let array = build(
            &column,
            vec![Cell::Int(0), Cell::Int(1), Cell::UInt(1), Cell::Null],
        );
        let array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean");
        assert_eq!(
            (
                array.value(0),
                array.value(1),
                array.value(2),
                array.is_null(3)
            ),
            (false, true, true, true)
        );
        let err = push_err(&column, Cell::Int(2));
        assert!(
            err.contains("not a boolean") && err.contains("declare the field int8"),
            "{err}"
        );
    }

    #[test]
    fn floats_fill_both_widths() {
        let column = resolved("float", "float", T::Float32);
        let array = build(&column, vec![Cell::Float(f64::from(1.5f32)), Cell::Null]);
        let array = array
            .as_any()
            .downcast_ref::<Float32Array>()
            .expect("float32");
        assert_eq!(array.value(0), 1.5f32);
        assert!(array.is_null(1));

        let column = resolved("double", "double", T::Float64);
        let array = build(&column, vec![Cell::Float(-0.1)]);
        let array = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64");
        assert_eq!(array.value(0), -0.1);
    }

    #[test]
    fn decimals_parse_at_the_declared_scale() {
        let column = column_of(
            CatalogColumn {
                numeric_precision: Some(10),
                numeric_scale: Some(2),
                ..catalog("decimal", "decimal(10,2)")
            },
            T::Decimal128,
            true,
        );
        let array = build(
            &column,
            vec![
                text("12345678.90"),
                text("-0.05"),
                text("0.00"),
                text("-99999999.99"),
                text("7"),
                Cell::Null,
            ],
        );
        let array = array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal");
        assert_eq!(array.data_type(), &DataType::Decimal128(10, 2));
        let values: Vec<i128> = (0..5).map(|i| array.value(i)).collect();
        assert_eq!(values, vec![1_234_567_890, -5, 0, -9_999_999_999, 700]);
        assert!(array.is_null(5));
    }

    #[test]
    fn a_decimal_at_precision_thirty_eight_does_not_overflow() {
        let column = column_of(
            CatalogColumn {
                numeric_precision: Some(38),
                numeric_scale: Some(0),
                ..catalog("decimal", "decimal(38,0)")
            },
            T::Decimal128,
            true,
        );
        let max = "9".repeat(38);
        let array = build(
            &column,
            vec![Cell::Owned(max.clone()), Cell::Owned(format!("-{max}"))],
        );
        let array = array
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal");
        assert_eq!(array.value(0).to_string(), max);
        assert_eq!(array.value(1).to_string(), format!("-{max}"));
    }

    #[test]
    fn a_decimal_the_scale_or_precision_cannot_hold_is_refused() {
        let column = column_of(
            CatalogColumn {
                numeric_precision: Some(5),
                numeric_scale: Some(2),
                ..catalog("decimal", "decimal(5,2)")
            },
            T::Decimal128,
            true,
        );
        assert!(push_err(&column, text("1.234")).contains("fraction digit"));
        assert!(push_err(&column, text("1000.00")).contains("declared precision"));
        for bad in ["", "-", ".", "1.2.3", "1e5", "abc", "1,5"] {
            assert!(
                push_err(&column, Cell::Owned(bad.to_string())).contains("not a decimal"),
                "{bad}"
            );
        }
    }

    #[test]
    fn text_is_checked_as_utf8() {
        let column = resolved("varchar", "varchar(20)", T::Utf8);
        let array = build(&column, vec![text("café 🎉"), text(""), Cell::Null]);
        assert_eq!(
            strings(&array),
            vec![Some("café 🎉".to_string()), Some(String::new()), None]
        );
        let err = push_err(&column, Cell::Text(&[0xff, 0xfe]));
        assert_eq!(err, "column 'c': not valid UTF-8; declare it binary");
    }

    #[test]
    fn binary_columns_keep_every_byte() {
        let column = resolved("blob", "blob", T::Binary);
        let array = build(
            &column,
            vec![Cell::Text(&[0, 1, 255]), Cell::Text(&[]), Cell::Null],
        );
        let array = array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary");
        assert_eq!(array.value(0), &[0, 1, 255]);
        assert_eq!(array.value(1), &[] as &[u8]);
        assert!(array.is_null(2));
    }

    #[test]
    fn dates_count_days_from_the_epoch_and_a_zero_date_is_null() {
        let column = resolved("date", "date", T::Date32);
        let array = build(
            &column,
            vec![
                Cell::Date {
                    year: 1970,
                    month: 1,
                    day: 1,
                },
                Cell::Date {
                    year: 0,
                    month: 1,
                    day: 1,
                },
                Cell::Date {
                    year: 9999,
                    month: 12,
                    day: 31,
                },
                Cell::Date {
                    year: 0,
                    month: 0,
                    day: 0,
                },
            ],
        );
        let array = array
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date32");
        assert_eq!(
            (
                array.value(0),
                array.value(1),
                array.value(2),
                array.is_null(3)
            ),
            (0, -719_528, 2_932_896, true)
        );
    }

    #[test]
    fn a_zero_date_in_a_field_that_is_not_nullable_names_the_remedy() {
        let column = column_of(catalog("date", "date"), T::Date32, false);
        let err = push_err(
            &column,
            Cell::Date {
                year: 0,
                month: 0,
                day: 0,
            },
        );
        assert_eq!(err, "column 'c': zero date; declare the field nullable");
        let column = column_of(catalog("datetime", "datetime"), T::TimestampMicros, false);
        let zero = Cell::DateTime {
            year: 0,
            month: 0,
            day: 0,
            hour: 0,
            minute: 0,
            second: 0,
            micros: 0,
        };
        assert_eq!(
            push_err(&column, zero),
            "column 'c': zero date; declare the field nullable"
        );
    }

    #[test]
    fn a_date_with_a_zero_month_or_an_impossible_day_is_refused_not_nulled() {
        let column = resolved("date", "date", T::Date32);
        for (year, month, day) in [(2024, 0, 15), (2024, 5, 0), (2023, 2, 29), (2024, 13, 1)] {
            let err = push_err(&column, Cell::Date { year, month, day });
            assert!(
                err.contains("does not exist") && err.contains("declare the field utf8"),
                "{err}"
            );
        }
    }

    #[test]
    fn a_datetime_fills_microsecond_timestamps_and_the_zone_is_in_the_type() {
        let cell = || Cell::DateTime {
            year: 2024,
            month: 1,
            day: 2,
            hour: 3,
            minute: 4,
            second: 5,
            micros: 6,
        };
        let naive = temporal("datetime", 6, T::TimestampMicros);
        let array = build(&naive, vec![cell(), Cell::EpochMicros(-1), Cell::Null]);
        assert_eq!(
            array.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let values = array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("ts");
        assert_eq!(
            (values.value(0), values.value(1)),
            (1_704_164_645_000_006, -1)
        );

        let utc = temporal("timestamp", 6, T::TimestampMicrosUtc);
        let array = build(&utc, vec![cell()]);
        assert_eq!(
            array.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn a_time_of_day_fills_microseconds_since_midnight() {
        let column = temporal("time", 6, T::Time64Micros);
        let cell = |hours, minutes, seconds, micros| Cell::Time {
            negative: false,
            hours,
            minutes,
            seconds,
            micros,
        };
        let array = build(
            &column,
            vec![cell(0, 0, 0, 0), cell(23, 59, 59, 999_999), Cell::Null],
        );
        let array = array
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .expect("time64");
        assert_eq!((array.value(0), array.value(1)), (0, 86_399_999_999));
    }

    #[test]
    fn a_time_outside_a_time_of_day_names_the_value_and_the_remedy() {
        let column = temporal("time", 0, T::Time64Micros);
        let err = push_err(
            &column,
            Cell::Time {
                negative: true,
                hours: 12,
                minutes: 0,
                seconds: 0,
                micros: 0,
            },
        );
        assert_eq!(
            err,
            "column 'c': TIME value '-12:00:00' is outside a time of day; declare the field utf8"
        );
        let err = push_err(
            &column,
            Cell::Time {
                negative: false,
                hours: 24,
                minutes: 0,
                seconds: 0,
                micros: 0,
            },
        );
        assert!(err.contains("'24:00:00'"), "{err}");
        let err = push_err(
            &column,
            Cell::Time {
                negative: false,
                hours: 838,
                minutes: 59,
                seconds: 59,
                micros: 0,
            },
        );
        assert!(err.contains("'838:59:59'"), "{err}");
    }

    #[test]
    fn a_metadata_column_with_no_server_counterpart_builds_from_its_declared_type() {
        let synthetic = |ty| {
            ResolvedColumn::synthetic(&FieldSpec {
                name: "__meta".to_string(),
                ty,
                nullable: true,
                precision: None,
                scale: None,
                mariadb_type: None,
            })
        };

        let file = synthetic(T::Utf8);
        let array = build(&file, vec![text("mysql-bin.000003"), Cell::Null]);
        assert_eq!(
            strings(&array),
            vec![Some("mysql-bin.000003".to_string()), None]
        );

        let position = synthetic(T::UInt64);
        let array = build(&position, vec![Cell::UInt(u64::MAX), Cell::UInt(154)]);
        let array = array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("uint64");
        assert_eq!((array.value(0), array.value(1)), (u64::MAX, 154));

        let committed = synthetic(T::TimestampMicrosUtc);
        let array = build(&committed, vec![Cell::EpochMicros(1_704_164_645_000_000)]);
        assert_eq!(
            array.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        let values = array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("ts");
        assert_eq!(values.value(0), 1_704_164_645_000_000);
    }

    #[test]
    fn a_refused_value_appends_nothing_so_the_column_stays_aligned() {
        let int = resolved("tinyint", "tinyint(4)", T::Int8);
        let date = column_of(catalog("date", "date"), T::Date32, false);
        let utf8 = resolved("varchar", "varchar(20)", T::Utf8);
        let time = temporal("time", 0, T::Time64Micros);
        let refused: Vec<(&ResolvedColumn, Cell<'_>, Cell<'_>)> = vec![
            (&int, Cell::Int(1), Cell::Int(999)),
            (
                &date,
                Cell::Date {
                    year: 2024,
                    month: 1,
                    day: 1,
                },
                Cell::Date {
                    year: 0,
                    month: 0,
                    day: 0,
                },
            ),
            (&utf8, text("ok"), Cell::Text(&[0xff])),
            (
                &time,
                Cell::Time {
                    negative: false,
                    hours: 1,
                    minutes: 0,
                    seconds: 0,
                    micros: 0,
                },
                Cell::Time {
                    negative: false,
                    hours: 99,
                    minutes: 0,
                    seconds: 0,
                    micros: 0,
                },
            ),
        ];
        for (column, good, bad) in refused {
            let mut builder = ColumnBuilder::new(column, 2).expect("builder");
            builder.push(good).expect("the first value fits");
            builder.push(bad).expect_err("the second value is refused");
            assert_eq!(builder.len(), 1, "{}", column.spec.name);
        }
    }

    #[test]
    fn a_null_in_a_field_that_is_not_nullable_is_refused_by_name() {
        let column = column_of(catalog("int", "int(11)"), T::Int32, false);
        let mut builder = ColumnBuilder::new(&column, 1).expect("builder");
        let err = builder.push(Cell::Null).expect_err("NULL").message();
        assert!(
            err.contains("column 'c'") && err.contains("nullable"),
            "{err}"
        );
        assert_eq!(builder.len(), 0);
    }

    #[test]
    fn a_cell_of_the_wrong_kind_is_refused_by_name() {
        let column = resolved("int", "int(11)", T::Int32);
        let err = push_err(&column, text("x"));
        assert_eq!(
            err,
            "column 'c': a text value cannot fill a declared \"int32\""
        );
        let column = resolved("date", "date", T::Date32);
        assert!(push_err(&column, Cell::Int(1)).contains("integer value cannot fill"));
    }

    #[test]
    fn finishing_resets_the_builder_for_the_next_batch() {
        let column = resolved("int", "int(11)", T::Int32);
        let mut builder = ColumnBuilder::new(&column, 2).expect("builder");
        builder.push(Cell::Int(1)).expect("push");
        assert_eq!(builder.len(), 1);
        assert_eq!(builder.finish().len(), 1);
        assert_eq!(builder.len(), 0);
        builder.push(Cell::Int(2)).expect("push");
        assert_eq!(builder.finish().len(), 1);
    }

    // ------------------------------------------------------------- utf8 rendering

    #[test]
    fn integers_render_in_decimal_and_a_year_keeps_four_digits() {
        let column = resolved("bigint", "bigint(20)", T::Utf8);
        let array = build(&column, vec![Cell::Int(-5), Cell::UInt(u64::MAX)]);
        assert_eq!(
            strings(&array),
            vec![
                Some("-5".to_string()),
                Some("18446744073709551615".to_string())
            ]
        );
        let year = resolved("year", "year(4)", T::Utf8);
        let array = build(&year, vec![Cell::Int(2024), Cell::Int(0)]);
        assert_eq!(
            strings(&array),
            vec![Some("2024".to_string()), Some("0000".to_string())]
        );
    }

    #[test]
    fn a_date_renders_as_the_server_prints_it_zero_date_included() {
        let column = resolved("date", "date", T::Utf8);
        let array = build(
            &column,
            vec![
                Cell::Date {
                    year: 2024,
                    month: 2,
                    day: 9,
                },
                Cell::Date {
                    year: 0,
                    month: 0,
                    day: 0,
                },
            ],
        );
        assert_eq!(
            strings(&array),
            vec![
                Some("2024-02-09".to_string()),
                Some("0000-00-00".to_string())
            ]
        );
    }

    #[test]
    fn a_datetime_renders_exactly_its_columns_fraction_digits() {
        let datetime = |micros| Cell::DateTime {
            year: 2024,
            month: 1,
            day: 2,
            hour: 3,
            minute: 4,
            second: 5,
            micros,
        };
        for (digits, expected) in [
            (0, "2024-01-02 03:04:05"),
            (1, "2024-01-02 03:04:05.1"),
            (3, "2024-01-02 03:04:05.123"),
            (6, "2024-01-02 03:04:05.123456"),
        ] {
            let column = temporal("datetime", digits, T::Utf8);
            let array = build(&column, vec![datetime(123_456)]);
            assert_eq!(
                strings(&array),
                vec![Some(expected.to_string())],
                "{digits} digits"
            );
        }
        let column = temporal("datetime", 3, T::Utf8);
        let array = build(&column, vec![datetime(0)]);
        assert_eq!(
            strings(&array),
            vec![Some("2024-01-02 03:04:05.000".to_string())]
        );
    }

    #[test]
    fn a_timestamp_renders_like_a_datetime() {
        let column = temporal("timestamp", 6, T::Utf8);
        let array = build(
            &column,
            vec![
                Cell::EpochMicros(1_704_164_645_000_006),
                Cell::EpochMicros(-1),
            ],
        );
        assert_eq!(
            strings(&array),
            vec![
                Some("2024-01-02 03:04:05.000006".to_string()),
                Some("1969-12-31 23:59:59.999999".to_string())
            ]
        );
    }

    #[test]
    fn a_time_renders_signed_with_hours_past_twenty_four() {
        let column = temporal("time", 3, T::Utf8);
        let array = build(
            &column,
            vec![
                Cell::Time {
                    negative: true,
                    hours: 12,
                    minutes: 0,
                    seconds: 0,
                    micros: 0,
                },
                Cell::Time {
                    negative: false,
                    hours: 838,
                    minutes: 59,
                    seconds: 59,
                    micros: 999_000,
                },
                Cell::Time {
                    negative: false,
                    hours: 1,
                    minutes: 2,
                    seconds: 3,
                    micros: 4_000,
                },
            ],
        );
        assert_eq!(
            strings(&array),
            vec![
                Some("-12:00:00.000".to_string()),
                Some("838:59:59.999".to_string()),
                Some("01:02:03.004".to_string())
            ]
        );
    }

    #[test]
    fn a_decimal_read_as_text_keeps_the_servers_digits() {
        let column = column_of(
            CatalogColumn {
                numeric_precision: Some(65),
                numeric_scale: Some(30),
                ..catalog("decimal", "decimal(65,30)")
            },
            T::Utf8,
            true,
        );
        let digits = "12345678901234567890123456789012345.123456789012345678901234567890";
        let array = build(&column, vec![Cell::Owned(digits.to_string())]);
        assert_eq!(strings(&array), vec![Some(digits.to_string())]);
    }

    #[test]
    fn a_float_cannot_render_as_text() {
        let column = resolved("varchar", "varchar(20)", T::Utf8);
        assert!(push_err(&column, Cell::Float(1.0)).contains("cannot be written as text"));
    }

    // ------------------------------------------------------------ JSON documents

    #[test]
    fn a_mysql_json_document_is_canonicalised_compact_with_sorted_keys() {
        let column = resolved("json", "json", T::Json);
        for source in [
            r#"{"b": 1, "a": [1, 2, {"d": null, "c": true}]}"#,
            r#"{"a":[1,2,{"c":true,"d":null}],"b":1}"#,
            "{\n  \"a\" : [1, 2, {\"d\": null, \"c\": true}],\n  \"b\" : 1\n}",
        ] {
            let array = build(&column, vec![Cell::Text(source.as_bytes())]);
            assert_eq!(
                strings(&array),
                vec![Some(r#"{"a":[1,2,{"c":true,"d":null}],"b":1}"#.to_string())],
                "{source}"
            );
        }
    }

    #[test]
    fn a_json_document_read_as_utf8_is_canonicalised_too() {
        let column = resolved("json", "json", T::Utf8);
        let array = build(&column, vec![text(r#"{"z": 1, "y": 2}"#)]);
        assert_eq!(strings(&array), vec![Some(r#"{"y":2,"z":1}"#.to_string())]);
    }

    #[test]
    fn json_scalars_survive_canonicalisation() {
        let column = resolved("json", "json", T::Json);
        let array = build(
            &column,
            vec![
                text("1"),
                text("\"x\""),
                text("null"),
                text("[3, 1]"),
                text("1.5"),
            ],
        );
        assert_eq!(
            strings(&array),
            ["1", "\"x\"", "null", "[3,1]", "1.5"]
                .map(|s| Some(s.to_string()))
                .to_vec()
        );
    }

    #[test]
    fn a_json_document_that_does_not_parse_is_refused() {
        let column = resolved("json", "json", T::Json);
        let err = push_err(&column, text("{not json"));
        assert!(
            err.contains("column 'c'") && err.contains("does not parse"),
            "{err}"
        );
    }

    #[test]
    fn a_mariadb_json_column_is_longtext_and_keeps_its_text_as_stored() {
        let column = resolved("longtext", "longtext", T::Json);
        let array = build(&column, vec![text(r#"{"b": 1, "a": 2}"#)]);
        assert_eq!(
            strings(&array),
            vec![Some(r#"{"b": 1, "a": 2}"#.to_string())]
        );
    }

    #[test]
    fn the_binlog_and_the_protocol_agree_on_the_same_json_document() {
        let column = resolved("json", "json", T::Json);
        let from_protocol = build(&column, vec![text(r#"{"b": [1, 2], "a": "x"}"#)]);
        let from_binlog = build(
            &column,
            vec![Cell::Owned(
                serde_json::json!({"a": "x", "b": [1, 2]}).to_string(),
            )],
        );
        assert_eq!(strings(&from_protocol), strings(&from_binlog));
    }
}
