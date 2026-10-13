//! The canonical server type table and the load-time check that applies it.
//!
//! [`TYPES`] is the one mapping in this crate: one row per
//! `information_schema.COLUMNS.DATA_TYPE`, carrying the storage [`Family`] a
//! value is decoded as, the declared types the column can fill with the
//! default first, and whether the row binlog can carry the type at all.
//!
//! The connector never coerces. A declared `int32` over a `bigint` column is a
//! configuration error, not a narrowing cast, and reading a number as text
//! takes an explicit `utf8` declaration. [`validate_columns`] runs once per
//! connection against the real catalog and reports every mismatch at once, so
//! one restart fixes the whole configuration.

use saci_core::error::SaciError;
use serde::{Deserialize, Serialize};

use crate::config::{FieldSpec, MariadbFieldType};

use MariadbFieldType as T;

/// How a column's rows reach the connector, which decides what it may be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Access {
    /// A `SELECT` over the binary protocol: polling, a changelog read and the
    /// snapshot. The server renders every value.
    Query,
    /// The row images of the binlog, which carry each value in its storage
    /// form, in the column's own character set.
    Binlog,
    /// The sink's `INSERT`. A declared decimal may be narrower than the
    /// column, never wider.
    Write,
}

/// The storage form a column's value arrives in, which picks its decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Family {
    /// `tinyint` (8 bits) to `bigint` (64 bits); `mediumint` is 24.
    Integer(u8),
    /// `year`: a number whose zero value is year 0000.
    Year,
    /// `bit(n)`: big-endian bytes read as one number.
    Bit,
    /// `float` and `double`.
    Float,
    /// `decimal(p,s)`: the server's own text.
    Decimal,
    /// `char`, `varchar`, the text types, and `uuid`, `inet4` and `inet6`,
    /// which arrive as text.
    Text,
    /// `binary`, `varbinary`, the blobs, geometries and `vector`: bytes.
    Blob,
    /// `enum`: one label.
    Enum,
    /// `set`: labels joined with `,`.
    Set,
    /// MySQL's native `json`.
    Json,
    /// `date`.
    Date,
    /// `time`, with a sign and hours past 24.
    Time,
    /// `datetime`: a calendar date and time with no zone.
    DateTime,
    /// `timestamp`: an instant, shown in the session's UTC time zone.
    Timestamp,
}

/// One row of the canonical mapping.
struct TypeRow {
    /// `information_schema.COLUMNS.DATA_TYPE`, lower case.
    data_type: &'static str,
    /// How a value of this type arrives.
    family: Family,
    /// The declared type a column of this type maps to when nothing else is
    /// said. An unsigned integer column takes the unsigned variant.
    default: MariadbFieldType,
    /// Every declared type this type fills, [`default`](Self::default) first,
    /// listed as for a signed column. [`fills`] derives the unsigned form.
    accepts: &'static [MariadbFieldType],
    /// Whether the row binlog carries this type in a form [`crate::values`]
    /// decodes.
    binlog: bool,
}

const fn row(
    data_type: &'static str,
    family: Family,
    default: MariadbFieldType,
    accepts: &'static [MariadbFieldType],
    binlog: bool,
) -> TypeRow {
    TypeRow {
        data_type,
        family,
        default,
        accepts,
        binlog,
    }
}

/// `char`, `varchar` and the text types read as text, a JSON document, or
/// bytes.
const TEXT: &[MariadbFieldType] = &[T::Utf8, T::Json, T::Binary];
/// Bytes only.
const BYTES: &[MariadbFieldType] = &[T::Binary];

/// The canonical table, sorted by `data_type` so a lookup is a binary search.
#[rustfmt::skip]
static TYPES: &[TypeRow] = &[
    row("bigint", Family::Integer(64), T::Int64, &[T::Int64, T::Utf8], true),
    row("binary", Family::Blob, T::Binary, BYTES, true),
    row("bit", Family::Bit, T::UInt64, &[T::UInt64, T::Binary], true),
    row("blob", Family::Blob, T::Binary, BYTES, true),
    row("char", Family::Text, T::Utf8, TEXT, true),
    row("date", Family::Date, T::Date32, &[T::Date32, T::Utf8], true),
    row("datetime", Family::DateTime, T::TimestampMicros, &[T::TimestampMicros, T::Utf8], true),
    row("decimal", Family::Decimal, T::Decimal128, &[T::Decimal128, T::Utf8], true),
    row("double", Family::Float, T::Float64, &[T::Float64], true),
    row("enum", Family::Enum, T::Utf8, &[T::Utf8], true),
    row("float", Family::Float, T::Float32, &[T::Float32, T::Float64], true),
    row("geomcollection", Family::Blob, T::Binary, BYTES, true),
    row("geometry", Family::Blob, T::Binary, BYTES, true),
    row("geometrycollection", Family::Blob, T::Binary, BYTES, true),
    row("inet4", Family::Text, T::Utf8, &[T::Utf8], false),
    row("inet6", Family::Text, T::Utf8, &[T::Utf8], false),
    row("int", Family::Integer(32), T::Int32, &[T::Int32, T::Int64, T::Utf8], true),
    row("json", Family::Json, T::Json, &[T::Json, T::Utf8], true),
    row("linestring", Family::Blob, T::Binary, BYTES, true),
    row("longblob", Family::Blob, T::Binary, BYTES, true),
    row("longtext", Family::Text, T::Utf8, TEXT, true),
    row("mediumblob", Family::Blob, T::Binary, BYTES, true),
    row("mediumint", Family::Integer(24), T::Int32, &[T::Int32, T::Int64, T::Utf8], true),
    row("mediumtext", Family::Text, T::Utf8, TEXT, true),
    row("multilinestring", Family::Blob, T::Binary, BYTES, true),
    row("multipoint", Family::Blob, T::Binary, BYTES, true),
    row("multipolygon", Family::Blob, T::Binary, BYTES, true),
    row("point", Family::Blob, T::Binary, BYTES, true),
    row("polygon", Family::Blob, T::Binary, BYTES, true),
    row("set", Family::Set, T::Utf8, &[T::Utf8], true),
    row("smallint", Family::Integer(16), T::Int16, &[T::Int16, T::Int32, T::Int64, T::Utf8], true),
    row("text", Family::Text, T::Utf8, TEXT, true),
    row("time", Family::Time, T::Time64Micros, &[T::Time64Micros, T::Utf8], true),
    row("timestamp", Family::Timestamp, T::TimestampMicrosUtc, &[T::TimestampMicrosUtc, T::Utf8], true),
    row("tinyblob", Family::Blob, T::Binary, BYTES, true),
    row("tinyint", Family::Integer(8), T::Int8, &[T::Int8, T::Boolean, T::Int16, T::Int32, T::Int64, T::Utf8], true),
    row("tinytext", Family::Text, T::Utf8, TEXT, true),
    row("uuid", Family::Text, T::Utf8, &[T::Utf8], false),
    row("varbinary", Family::Blob, T::Binary, BYTES, true),
    row("varchar", Family::Text, T::Utf8, TEXT, true),
    row("vector", Family::Blob, T::Binary, BYTES, true),
    row("year", Family::Year, T::Int16, &[T::Int16, T::Int32, T::Int64, T::Utf8], true),
];

/// Character sets a binlog row carries as bytes this connector can read as
/// UTF-8 text unchanged.
const BINLOG_TEXT_CHARSETS: [&str; 4] = ["utf8mb4", "utf8mb3", "utf8", "ascii"];

/// The row for a `DATA_TYPE`, matched without regard to case.
fn lookup(data_type: &str) -> Option<&'static TypeRow> {
    let key = data_type.to_ascii_lowercase();
    TYPES
        .binary_search_by(|row| row.data_type.cmp(key.as_str()))
        .ok()
        .map(|index| &TYPES[index])
}

/// Whether `data_type` names a column type this connector reads or writes.
pub(crate) fn data_type_is_known(data_type: &str) -> bool {
    lookup(data_type).is_some()
}

/// The unsigned variant of a signed integer type, the type itself otherwise.
fn unsigned_variant(ty: MariadbFieldType) -> MariadbFieldType {
    match ty {
        T::Int8 => T::UInt8,
        T::Int16 => T::UInt16,
        T::Int32 => T::UInt32,
        T::Int64 => T::UInt64,
        other => other,
    }
}

/// Whether a column of `row`'s type, signed or `unsigned`, can fill a field
/// declared `declared`.
///
/// An integer column fills every integer type that holds all of its values: a
/// signed or unsigned type at least as wide for its own signedness, and for an
/// unsigned column also a signed type strictly wider. A signed column never
/// fills an unsigned type, which a negative value would not fit.
fn fills(row: &TypeRow, unsigned: bool, declared: MariadbFieldType) -> bool {
    let Family::Integer(bits) = row.family else {
        return row.accepts.contains(&declared);
    };
    match declared {
        T::Utf8 => true,
        T::Boolean => bits == 8,
        other => match other.int_bits() {
            Some(width) if other.is_unsigned_int() => unsigned && width >= bits,
            Some(width) if unsigned => width > bits,
            Some(width) => width >= bits,
            None => false,
        },
    }
}

/// The declared types a column of `row`'s type fills, default first, for an
/// error message.
fn options(row: &TypeRow, unsigned: bool) -> String {
    let default = if unsigned {
        unsigned_variant(row.default)
    } else {
        row.default
    };
    let mut names = vec![default];
    names.extend(
        MariadbFieldType::ALL
            .into_iter()
            .filter(|ty| *ty != default && fills(row, unsigned, *ty)),
    );
    names
        .iter()
        .map(|ty| format!("\"{}\"", ty.as_str()))
        .collect::<Vec<_>>()
        .join(", ")
}

/// One `information_schema.COLUMNS` row of the table being read or written.
///
/// Serialized into the schema history of a `cdc_binlog` source, so a field
/// added here is read back as absent from an older row: give it a default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CatalogColumn {
    /// `COLUMN_NAME`.
    pub(crate) name: String,
    /// `ORDINAL_POSITION - 1`: the column's index in a row image, counting
    /// from 0.
    pub(crate) ordinal: usize,
    /// `DATA_TYPE`, lower case.
    pub(crate) data_type: String,
    /// `COLUMN_TYPE`: `int(10) unsigned`, `decimal(10,2)`, `enum('a','b')`.
    pub(crate) column_type: String,
    /// `NUMERIC_PRECISION`: the digits of a `decimal`, the bits of a `bit`.
    pub(crate) numeric_precision: Option<u64>,
    /// `NUMERIC_SCALE`: the fraction digits of a `decimal`.
    pub(crate) numeric_scale: Option<u64>,
    /// `DATETIME_PRECISION`: the fraction digits of a `time`, `datetime` or
    /// `timestamp`.
    pub(crate) datetime_precision: Option<u64>,
    /// `CHARACTER_SET_NAME`, for the text types.
    pub(crate) charset: Option<String>,
}

impl CatalogColumn {
    /// Whether `COLUMN_TYPE` carries the `unsigned` attribute.
    pub(crate) fn is_unsigned(&self) -> bool {
        self.column_type.to_ascii_lowercase().contains("unsigned")
    }

    /// The column as a DBA writes it, for an error message: `COLUMN_TYPE`
    /// unless that is as long as an enum's label list, then `DATA_TYPE`.
    fn describe(&self) -> &str {
        if self.column_type.len() <= 48 {
            &self.column_type
        } else {
            &self.data_type
        }
    }
}

/// A declared field resolved against the server column that fills it.
///
/// What a decoder, a builder and a binlog reader need to know about the column
/// beyond the declared type, collected once per connection so no per-value
/// path consults the catalog.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedColumn {
    /// The declared field.
    pub(crate) spec: FieldSpec,
    /// The column's index in a binlog row image, counting from 0.
    pub(crate) ordinal: usize,
    /// How the column's values arrive.
    pub(crate) family: Family,
    /// The server's `DATA_TYPE`, lower case.
    pub(crate) data_type: &'static str,
    /// Whether an integer column is `unsigned`.
    pub(crate) unsigned: bool,
    /// Fraction digits a `time`, `datetime` or `timestamp` column keeps, 0 to 6.
    pub(crate) fractional: u8,
    /// The labels of an `enum` or `set` column in declaration order.
    pub(crate) labels: Vec<String>,
}

impl ResolvedColumn {
    /// A column that has no server counterpart, for a metadata field a change
    /// stream fills: only the declared type is known.
    pub(crate) fn synthetic(spec: &FieldSpec) -> Self {
        let family = match spec.ty {
            T::Utf8 | T::Json => Family::Text,
            T::Binary => Family::Blob,
            T::Boolean | T::Int8 | T::UInt8 => Family::Integer(8),
            T::Int16 | T::UInt16 => Family::Integer(16),
            T::Int32 | T::UInt32 => Family::Integer(32),
            T::Int64 | T::UInt64 => Family::Integer(64),
            T::Float32 | T::Float64 => Family::Float,
            T::Decimal128 => Family::Decimal,
            T::Date32 => Family::Date,
            T::Time64Micros => Family::Time,
            T::TimestampMicros => Family::DateTime,
            T::TimestampMicrosUtc => Family::Timestamp,
        };
        Self {
            spec: spec.clone(),
            ordinal: 0,
            family,
            data_type: "",
            unsigned: spec.ty.is_unsigned_int(),
            fractional: 6,
            labels: Vec::new(),
        }
    }
}

/// Reject a declared schema the server's actual columns cannot fill, and
/// return what it resolved to.
///
/// Matches by name, ignoring case because the servers do, and never by
/// position. Collects every problem before returning so a misconfigured schema
/// is fixed in one pass. Columns the server has but the config does not
/// declare are ignored. On success the result holds one entry per declared
/// field, in declared order.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] listing every declared field that is
/// absent from `catalog` or whose type the column cannot fill.
pub(crate) fn validate_columns<'a>(
    what: &str,
    access: Access,
    declared: impl IntoIterator<Item = &'a FieldSpec>,
    catalog: &[CatalogColumn],
) -> Result<Vec<ResolvedColumn>, SaciError> {
    let mut resolved = Vec::new();
    let mut problems: Vec<String> = Vec::new();

    for spec in declared {
        let column = catalog
            .iter()
            .find(|column| column.name == spec.name)
            .or_else(|| {
                catalog
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&spec.name))
            });
        let Some(column) = column else {
            problems.push(format!(
                "column '{}' is declared but the server has no such column (server columns: {})",
                spec.name,
                if catalog.is_empty() {
                    "none".to_string()
                } else {
                    catalog
                        .iter()
                        .map(|column| column.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            ));
            continue;
        };

        match resolve(spec, column, access) {
            Ok(column) => resolved.push(column),
            Err(reason) => problems.push(format!("column '{}': {reason}", spec.name)),
        }
    }

    if problems.is_empty() {
        return Ok(resolved);
    }
    Err(SaciError::configuration(format!(
        "{what}: declared schema does not match the server: {}",
        problems.join("; ")
    )))
}

/// Resolve one declared field against the column named for it.
///
/// Returns the reason the pair is illegal, without a column prefix.
fn resolve(
    spec: &FieldSpec,
    column: &CatalogColumn,
    access: Access,
) -> Result<ResolvedColumn, String> {
    if let Some(asserted) = &spec.mariadb_type
        && !asserted.eq_ignore_ascii_case(&column.data_type)
    {
        return Err(format!(
            "mariadb_type '{asserted}' is declared but the server column is '{}'",
            column.data_type
        ));
    }
    let Some(row) = lookup(&column.data_type) else {
        return Err(format!(
            "the server type '{}' is not supported by this connector",
            column.describe()
        ));
    };

    if access == Access::Binlog {
        if !row.binlog {
            return Err(format!(
                "a '{}' column cannot be read from the binlog, which carries it as raw bytes; \
                 leave it out of schema_fields or read with mode kind \"polling\"",
                row.data_type
            ));
        }
        if row.family == Family::Text
            && let Some(charset) = column.charset.as_deref()
            && !BINLOG_TEXT_CHARSETS
                .iter()
                .any(|known| known.eq_ignore_ascii_case(charset))
        {
            return Err(format!(
                "the binlog carries '{}' text in the column's character set '{charset}', which \
                 is not UTF-8; convert the column to utf8mb4 or read with mode kind \"polling\"",
                row.data_type
            ));
        }
    }

    let unsigned = matches!(row.family, Family::Integer(_)) && column.is_unsigned();
    if !fills(row, unsigned, spec.ty) {
        return Err(format!(
            "the server type '{}' cannot fill a declared \"{}\"; declare one of {}",
            column.describe(),
            spec.ty.as_str(),
            options(row, unsigned)
        ));
    }

    if row.family == Family::Decimal && spec.ty == MariadbFieldType::Decimal128 {
        check_decimal(spec, column, access)?;
    }

    let labels = match row.family {
        Family::Enum | Family::Set => enum_labels(&column.column_type).map_err(|reason| {
            format!(
                "cannot read the labels of '{}': {reason}",
                column.describe()
            )
        })?,
        _ => Vec::new(),
    };
    let fractional = match row.family {
        Family::Time | Family::DateTime | Family::Timestamp => {
            clamp_u8(column.datetime_precision.unwrap_or(0)).min(6)
        }
        _ => 0,
    };

    Ok(ResolvedColumn {
        spec: spec.clone(),
        ordinal: column.ordinal,
        family: row.family,
        data_type: row.data_type,
        unsigned,
        fractional,
        labels,
    })
}

/// A catalog number as the small integer it is, saturating at `u8::MAX`.
fn clamp_u8(value: u64) -> u8 {
    u8::try_from(value).unwrap_or(u8::MAX)
}

/// Compare a declared `decimal128` with the `decimal(p,s)` column.
///
/// A source reads exactly what the column holds, so the two must agree and
/// the column must fit 38 digits. A sink writes the declared value as exact
/// text, so the column only has to be at least as wide on both sides of the
/// point.
fn check_decimal(spec: &FieldSpec, column: &CatalogColumn, access: Access) -> Result<(), String> {
    let (Some(precision), Some(scale)) = (column.numeric_precision, column.numeric_scale) else {
        return Err(format!(
            "the server does not report the precision and scale of '{}'",
            column.describe()
        ));
    };
    let (declared_precision, declared_scale) = spec.decimal_params().map_err(|e| e.message())?;
    let (declared_precision, declared_scale) = (
        u64::from(declared_precision),
        u64::from(declared_scale.unsigned_abs()),
    );

    if access == Access::Write {
        let integer_digits = precision.saturating_sub(scale);
        let declared_integer_digits = declared_precision.saturating_sub(declared_scale);
        if scale < declared_scale || integer_digits < declared_integer_digits {
            return Err(format!(
                "declared decimal128({declared_precision}, {declared_scale}) does not fit the \
                 server column '{}', which holds {integer_digits} digit(s) before the point \
                 and {scale} after it",
                column.describe()
            ));
        }
        return Ok(());
    }

    if precision > 38 {
        return Err(format!(
            "the server column '{}' is wider than the 38 digits of decimal128; declare it \
             \"utf8\"",
            column.describe()
        ));
    }
    if (declared_precision, declared_scale) != (precision, scale) {
        return Err(format!(
            "declared decimal128({declared_precision}, {declared_scale}) but the server column \
             is '{}'; declare precision {precision} and scale {scale}",
            column.describe()
        ));
    }
    Ok(())
}

/// The labels of an `enum('a','b')` or `set('x','y')` `COLUMN_TYPE`, in
/// declaration order.
///
/// The server writes a label in single quotes with a quote doubled and `\0`,
/// `\n`, `\r`, `\Z` and `\\` backslash-escaped, so a label may hold a comma or
/// a parenthesis.
///
/// # Errors
///
/// Returns the reason the text is not a quoted label list.
pub(crate) fn enum_labels(column_type: &str) -> Result<Vec<String>, String> {
    let open = column_type
        .find('(')
        .ok_or_else(|| "no opening parenthesis".to_string())?;
    let mut chars = column_type[open + 1..].chars().peekable();
    let mut labels = Vec::new();
    loop {
        match chars.next() {
            Some('\'') => {}
            Some(')') if labels.is_empty() => return Ok(labels),
            _ => return Err("a label does not start with a single quote".to_string()),
        }
        let mut label = String::new();
        loop {
            match chars.next() {
                None => return Err("a label is not closed".to_string()),
                Some('\'') if chars.peek() == Some(&'\'') => {
                    chars.next();
                    label.push('\'');
                }
                Some('\'') => break,
                Some('\\') => match chars.next() {
                    Some('0') => label.push('\0'),
                    Some('n') => label.push('\n'),
                    Some('r') => label.push('\r'),
                    Some('Z' | 'z') => label.push('\u{1a}'),
                    Some(other) => label.push(other),
                    None => return Err("a label ends in a backslash".to_string()),
                },
                Some(c) => label.push(c),
            }
        }
        labels.push(label);
        match chars.next() {
            Some(',') => {}
            Some(')') => return Ok(labels),
            _ => return Err("labels are not separated by commas".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str, ty: MariadbFieldType) -> FieldSpec {
        FieldSpec {
            name: name.to_string(),
            ty,
            nullable: true,
            precision: matches!(ty, T::Decimal128).then_some(10),
            scale: matches!(ty, T::Decimal128).then_some(2),
            mariadb_type: None,
        }
    }

    /// A catalog column with the given `DATA_TYPE` and `COLUMN_TYPE`.
    fn col(name: &str, data_type: &str, column_type: &str) -> CatalogColumn {
        CatalogColumn {
            name: name.to_string(),
            ordinal: 0,
            data_type: data_type.to_string(),
            column_type: column_type.to_string(),
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
            charset: None,
        }
    }

    fn decimal(precision: u64, scale: u64) -> CatalogColumn {
        CatalogColumn {
            numeric_precision: Some(precision),
            numeric_scale: Some(scale),
            ..col("c", "decimal", &format!("decimal({precision},{scale})"))
        }
    }

    fn text(charset: &str) -> CatalogColumn {
        CatalogColumn {
            charset: Some(charset.to_string()),
            ..col("c", "varchar", "varchar(20)")
        }
    }

    /// Resolve one declared type over one column.
    fn check(
        ty: MariadbFieldType,
        column: &CatalogColumn,
        access: Access,
    ) -> Result<ResolvedColumn, SaciError> {
        let declared = spec(&column.name, ty);
        validate_columns(
            "MariadbSource",
            access,
            [&declared],
            std::slice::from_ref(column),
        )
        .map(|mut resolved| resolved.remove(0))
    }

    fn fills_all(column: &CatalogColumn, accepted: &[MariadbFieldType], access: Access) {
        for ty in MariadbFieldType::ALL {
            let result = check(ty, column, access);
            assert_eq!(
                result.is_ok(),
                accepted.contains(&ty),
                "{} over {}: {:?}",
                ty.as_str(),
                column.column_type,
                result.err().map(|e| e.message())
            );
        }
    }

    // --------------------------------------------------------------- the table

    #[test]
    fn the_table_is_sorted_and_keyed_uniquely_in_lower_case() {
        assert!(
            TYPES.windows(2).all(|w| w[0].data_type < w[1].data_type),
            "TYPES must be sorted by data_type for the binary search"
        );
        for row in TYPES {
            assert_eq!(row.data_type, row.data_type.to_ascii_lowercase());
        }
    }

    #[test]
    fn every_row_lists_its_default_first_and_accepts_something() {
        for row in TYPES {
            assert_eq!(
                row.accepts.first(),
                Some(&row.default),
                "{} must list its default first",
                row.data_type
            );
        }
    }

    #[test]
    fn every_type_the_plan_names_has_a_row() {
        for name in [
            "tinyint",
            "smallint",
            "mediumint",
            "int",
            "bigint",
            "year",
            "bit",
            "float",
            "double",
            "decimal",
            "char",
            "varchar",
            "tinytext",
            "text",
            "mediumtext",
            "longtext",
            "binary",
            "varbinary",
            "tinyblob",
            "blob",
            "mediumblob",
            "longblob",
            "enum",
            "set",
            "json",
            "date",
            "time",
            "datetime",
            "timestamp",
            "geometry",
            "point",
            "linestring",
            "polygon",
            "multipoint",
            "multilinestring",
            "multipolygon",
            "geometrycollection",
            "vector",
            "uuid",
            "inet4",
            "inet6",
        ] {
            assert!(data_type_is_known(name), "{name} has no row");
        }
    }

    #[test]
    fn mysql_spells_the_geometry_collection_its_own_way() {
        assert!(data_type_is_known("geomcollection"));
    }

    #[test]
    fn a_lookup_ignores_case_and_an_unknown_type_has_no_row() {
        assert!(data_type_is_known("VARCHAR"));
        assert!(data_type_is_known("BigInt"));
        assert!(!data_type_is_known("vachar"));
        assert!(!data_type_is_known(""));
    }

    #[test]
    fn only_the_three_text_shaped_types_are_closed_to_the_binlog() {
        let closed: Vec<&str> = TYPES
            .iter()
            .filter(|row| !row.binlog)
            .map(|row| row.data_type)
            .collect();
        assert_eq!(closed, vec!["inet4", "inet6", "uuid"]);
    }

    // ------------------------------------------------------------- acceptance

    #[test]
    fn a_signed_integer_column_fills_every_signed_type_at_least_as_wide() {
        fills_all(
            &col("c", "tinyint", "tinyint(4)"),
            &[T::Int8, T::Boolean, T::Int16, T::Int32, T::Int64, T::Utf8],
            Access::Query,
        );
        fills_all(
            &col("c", "smallint", "smallint(6)"),
            &[T::Int16, T::Int32, T::Int64, T::Utf8],
            Access::Query,
        );
        fills_all(
            &col("c", "mediumint", "mediumint(9)"),
            &[T::Int32, T::Int64, T::Utf8],
            Access::Query,
        );
        fills_all(
            &col("c", "int", "int(11)"),
            &[T::Int32, T::Int64, T::Utf8],
            Access::Query,
        );
        fills_all(
            &col("c", "bigint", "bigint(20)"),
            &[T::Int64, T::Utf8],
            Access::Query,
        );
    }

    #[test]
    fn an_unsigned_integer_column_fills_the_unsigned_types_and_the_wider_signed_ones() {
        fills_all(
            &col("c", "tinyint", "tinyint(3) unsigned"),
            &[
                T::UInt8,
                T::Boolean,
                T::UInt16,
                T::UInt32,
                T::UInt64,
                T::Int16,
                T::Int32,
                T::Int64,
                T::Utf8,
            ],
            Access::Query,
        );
        fills_all(
            &col("c", "mediumint", "mediumint(8) unsigned"),
            &[T::UInt32, T::UInt64, T::Int32, T::Int64, T::Utf8],
            Access::Query,
        );
        fills_all(
            &col("c", "int", "int(10) unsigned"),
            &[T::UInt32, T::UInt64, T::Int64, T::Utf8],
            Access::Query,
        );
        fills_all(
            &col("c", "bigint", "bigint(20) unsigned"),
            &[T::UInt64, T::Utf8],
            Access::Query,
        );
    }

    #[test]
    fn the_integer_rule_agrees_with_the_listed_types_of_a_signed_column() {
        for row in TYPES
            .iter()
            .filter(|row| matches!(row.family, Family::Integer(_)))
        {
            let derived: Vec<MariadbFieldType> = MariadbFieldType::ALL
                .into_iter()
                .filter(|ty| fills(row, false, *ty))
                .collect();
            let mut listed = row.accepts.to_vec();
            let mut derived_sorted = derived.clone();
            listed.sort_by_key(|ty| ty.as_str());
            derived_sorted.sort_by_key(|ty| ty.as_str());
            assert_eq!(listed, derived_sorted, "{}", row.data_type);
        }
    }

    #[test]
    fn an_integer_column_declared_too_narrow_names_what_it_can_fill() {
        let err = check(T::Int32, &col("c", "bigint", "bigint(20)"), Access::Query).unwrap_err();
        let message = err.message();
        assert!(message.contains("'bigint(20)'"), "{message}");
        assert!(message.contains("declared \"int32\""), "{message}");
        assert!(message.contains("\"int64\", \"utf8\""), "{message}");
    }

    #[test]
    fn an_unsigned_default_leads_the_suggestion_for_an_unsigned_column() {
        let err = check(
            T::Float64,
            &col("c", "int", "int(10) unsigned"),
            Access::Query,
        )
        .unwrap_err();
        assert!(
            err.message().contains("declare one of \"uint32\""),
            "{}",
            err.message()
        );
    }

    #[test]
    fn year_fills_signed_integers_and_text() {
        fills_all(
            &col("c", "year", "year(4)"),
            &[T::Int16, T::Int32, T::Int64, T::Utf8],
            Access::Query,
        );
    }

    #[test]
    fn bit_fills_an_unsigned_number_or_bytes() {
        fills_all(
            &col("c", "bit", "bit(8)"),
            &[T::UInt64, T::Binary],
            Access::Query,
        );
    }

    #[test]
    fn a_float_widens_to_float64_and_a_double_never_narrows() {
        fills_all(
            &col("c", "float", "float"),
            &[T::Float32, T::Float64],
            Access::Query,
        );
        fills_all(&col("c", "double", "double"), &[T::Float64], Access::Query);
    }

    #[test]
    fn text_columns_read_as_text_json_or_bytes() {
        for data_type in [
            "char",
            "varchar",
            "tinytext",
            "text",
            "mediumtext",
            "longtext",
        ] {
            fills_all(
                &col("c", data_type, data_type),
                &[T::Utf8, T::Json, T::Binary],
                Access::Query,
            );
        }
    }

    #[test]
    fn blobs_geometries_and_vectors_read_only_as_bytes() {
        for data_type in [
            "binary",
            "varbinary",
            "tinyblob",
            "blob",
            "mediumblob",
            "longblob",
            "geometry",
            "point",
            "linestring",
            "polygon",
            "multipoint",
            "multilinestring",
            "multipolygon",
            "geometrycollection",
            "geomcollection",
            "vector",
        ] {
            fills_all(&col("c", data_type, data_type), &[T::Binary], Access::Query);
        }
    }

    #[test]
    fn enum_and_set_read_as_text_and_json_as_a_document_or_text() {
        fills_all(
            &col("c", "enum", "enum('a','b')"),
            &[T::Utf8],
            Access::Query,
        );
        fills_all(&col("c", "set", "set('a','b')"), &[T::Utf8], Access::Query);
        fills_all(
            &col("c", "json", "json"),
            &[T::Json, T::Utf8],
            Access::Query,
        );
    }

    #[test]
    fn each_temporal_type_fills_its_own_arrow_type_and_text() {
        fills_all(
            &col("c", "date", "date"),
            &[T::Date32, T::Utf8],
            Access::Query,
        );
        fills_all(
            &col("c", "time", "time"),
            &[T::Time64Micros, T::Utf8],
            Access::Query,
        );
        fills_all(
            &col("c", "datetime", "datetime"),
            &[T::TimestampMicros, T::Utf8],
            Access::Query,
        );
        fills_all(
            &col("c", "timestamp", "timestamp"),
            &[T::TimestampMicrosUtc, T::Utf8],
            Access::Query,
        );
    }

    #[test]
    fn uuid_and_the_inet_types_fill_text_only() {
        for data_type in ["uuid", "inet4", "inet6"] {
            fills_all(&col("c", data_type, data_type), &[T::Utf8], Access::Query);
        }
    }

    // ---------------------------------------------------------------- decimals

    #[test]
    fn a_source_decimal_must_equal_the_column() {
        let column = decimal(10, 2);
        check(T::Decimal128, &column, Access::Query).expect("equal precision and scale");
        let mut wrong = spec("c", T::Decimal128);
        wrong.precision = Some(12);
        let err =
            validate_columns("MariadbSource", Access::Query, [&wrong], &[column]).unwrap_err();
        let message = err.message();
        assert!(message.contains("decimal128(12, 2)"), "{message}");
        assert!(
            message.contains("declare precision 10 and scale 2"),
            "{message}"
        );
    }

    #[test]
    fn a_source_decimal_wider_than_thirty_eight_digits_must_be_utf8() {
        let column = decimal(65, 30);
        let err = check(T::Decimal128, &column, Access::Query).unwrap_err();
        assert!(
            err.message().contains("declare it \"utf8\""),
            "{}",
            err.message()
        );
        check(T::Utf8, &column, Access::Query).expect("text always fits");
    }

    #[test]
    fn a_sink_decimal_may_be_narrower_than_the_column_never_wider() {
        let column = decimal(12, 4);
        for (precision, scale, fits) in [
            (12, 4, true),
            (10, 2, true),
            (8, 4, true),
            (12, 5, false),
            (11, 0, false),
            (13, 4, false),
        ] {
            let mut declared = spec("c", T::Decimal128);
            declared.precision = Some(precision);
            declared.scale = Some(scale);
            let result = validate_columns(
                "MariadbSink",
                Access::Write,
                [&declared],
                std::slice::from_ref(&column),
            );
            assert_eq!(result.is_ok(), fits, "decimal128({precision}, {scale})");
        }
    }

    #[test]
    fn a_sink_may_write_a_decimal_into_a_column_wider_than_thirty_eight_digits() {
        check(T::Decimal128, &decimal(65, 30), Access::Write).expect("the sink widens");
    }

    // ------------------------------------------------------------------ binlog

    #[test]
    fn a_text_column_in_a_non_utf8_charset_is_readable_by_query_only() {
        check(T::Utf8, &text("latin1"), Access::Query).expect("the server converts");
        let err = check(T::Utf8, &text("latin1"), Access::Binlog).unwrap_err();
        assert!(err.message().contains("'latin1'"), "{}", err.message());
        assert!(err.message().contains("utf8mb4"), "{}", err.message());
    }

    #[test]
    fn the_utf8_family_of_charsets_is_readable_from_the_binlog() {
        for charset in ["utf8mb4", "utf8mb3", "utf8", "ascii", "UTF8MB4"] {
            check(T::Utf8, &text(charset), Access::Binlog).unwrap_or_else(|e| {
                panic!("{charset}: {}", e.message());
            });
        }
    }

    #[test]
    fn binary_and_numeric_columns_have_no_charset_rule() {
        check(T::Binary, &col("c", "blob", "blob"), Access::Binlog).expect("blob");
        check(T::Int32, &col("c", "int", "int(11)"), Access::Binlog).expect("int");
    }

    #[test]
    fn a_uuid_column_cannot_be_read_from_the_binlog() {
        let uuid = col("c", "uuid", "uuid");
        check(T::Utf8, &uuid, Access::Query).expect("polling reads the text form");
        let err = check(T::Utf8, &uuid, Access::Binlog).unwrap_err();
        assert!(err.message().contains("'uuid'"), "{}", err.message());
        assert!(err.message().contains("binlog"), "{}", err.message());
    }

    // ---------------------------------------------------------- validate_columns

    #[test]
    fn missing_and_mismatched_columns_are_reported_together() {
        let declared = [
            spec("id", T::Int64),
            spec("label", T::Int32),
            spec("gone", T::Utf8),
        ];
        let catalog = vec![
            col("id", "bigint", "bigint(20)"),
            col("label", "varchar", "varchar(20)"),
        ];
        let err =
            validate_columns("MariadbSource", Access::Query, &declared, &catalog).unwrap_err();
        assert_eq!(err.category(), "configuration");
        let message = err.message();
        assert!(
            message.starts_with("MariadbSource: declared schema does not match the server: "),
            "{message}"
        );
        assert!(
            message.contains("column 'gone' is declared but the server has no such column"),
            "{message}"
        );
        assert!(message.contains("(server columns: id, label)"), "{message}");
        assert!(message.contains("column 'label'"), "{message}");
        assert!(!message.contains("column 'id'"), "{message}");
    }

    #[test]
    fn a_table_with_no_columns_lists_none() {
        let declared = [spec("id", T::Int64)];
        let err = validate_columns("MariadbSink", Access::Write, &declared, &[]).unwrap_err();
        assert!(
            err.message().contains("(server columns: none)"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn undeclared_server_columns_are_ignored() {
        let declared = [spec("id", T::Int64)];
        let catalog = vec![
            col("id", "bigint", "bigint(20)"),
            col("extra", "varchar", "varchar(20)"),
        ];
        let resolved =
            validate_columns("MariadbSource", Access::Query, &declared, &catalog).unwrap();
        assert_eq!(resolved.len(), 1);
    }

    #[test]
    fn column_names_match_without_regard_to_case() {
        let declared = [spec("ID", T::Int64)];
        let catalog = vec![col("id", "bigint", "bigint(20)")];
        let resolved =
            validate_columns("MariadbSource", Access::Query, &declared, &catalog).unwrap();
        assert_eq!(resolved[0].spec.name, "ID");
    }

    #[test]
    fn an_exact_name_match_wins_over_a_case_insensitive_one() {
        let mut first = col("Id", "varchar", "varchar(4)");
        first.ordinal = 0;
        let mut second = col("id", "bigint", "bigint(20)");
        second.ordinal = 1;
        let declared = [spec("id", T::Int64)];
        let resolved =
            validate_columns("MariadbSource", Access::Query, &declared, &[first, second]).unwrap();
        assert_eq!(resolved[0].ordinal, 1);
    }

    #[test]
    fn the_resolved_columns_follow_the_declared_order() {
        let declared = [spec("b", T::Int64), spec("a", T::Int64)];
        let mut a = col("a", "bigint", "bigint(20)");
        a.ordinal = 0;
        let mut b = col("b", "bigint", "bigint(20)");
        b.ordinal = 1;
        let resolved =
            validate_columns("MariadbSource", Access::Query, &declared, &[a, b]).unwrap();
        let ordinals: Vec<usize> = resolved.iter().map(|c| c.ordinal).collect();
        assert_eq!(ordinals, vec![1, 0]);
    }

    #[test]
    fn mariadb_type_asserts_the_server_type_in_any_case() {
        let column = col("c", "varchar", "varchar(20)");
        let mut declared = spec("c", T::Utf8);
        declared.mariadb_type = Some("VARCHAR".to_string());
        validate_columns(
            "MariadbSource",
            Access::Query,
            [&declared],
            std::slice::from_ref(&column),
        )
        .expect("same type, any case");
        declared.mariadb_type = Some("text".to_string());
        let err =
            validate_columns("MariadbSource", Access::Query, [&declared], &[column]).unwrap_err();
        let message = err.message();
        assert!(message.contains("mariadb_type 'text'"), "{message}");
        assert!(message.contains("'varchar'"), "{message}");
    }

    #[test]
    fn a_server_type_with_no_row_is_refused_by_name() {
        let err = check(
            T::Utf8,
            &col("c", "hierarchyid", "hierarchyid"),
            Access::Query,
        )
        .unwrap_err();
        assert!(
            err.message().contains("'hierarchyid' is not supported"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn a_resolved_column_carries_what_its_decoders_need() {
        let mut column = col("c", "int", "int(10) unsigned");
        column.ordinal = 4;
        let resolved = check(T::UInt32, &column, Access::Query).unwrap();
        assert_eq!(resolved.ordinal, 4);
        assert_eq!(resolved.family, Family::Integer(32));
        assert!(resolved.unsigned);
        assert_eq!(resolved.data_type, "int");

        let mut bit = col("c", "bit", "bit(12)");
        bit.numeric_precision = Some(12);
        let resolved = check(T::UInt64, &bit, Access::Query).unwrap();
        assert_eq!(resolved.family, Family::Bit);

        let mut datetime = col("c", "datetime", "datetime(3)");
        datetime.datetime_precision = Some(3);
        let resolved = check(T::TimestampMicros, &datetime, Access::Query).unwrap();
        assert_eq!(resolved.fractional, 3);

        let resolved = check(
            T::Utf8,
            &col("c", "enum", "enum('a','b''c')"),
            Access::Query,
        )
        .unwrap();
        assert_eq!(resolved.labels, vec!["a", "b'c"]);
    }

    #[test]
    fn a_signed_integer_column_is_not_unsigned_and_a_decimal_has_no_sign_to_carry() {
        assert!(
            !check(T::Int32, &col("c", "int", "int(11)"), Access::Query)
                .unwrap()
                .unsigned
        );
        let mut column = decimal(10, 2);
        column.column_type = "decimal(10,2) unsigned".to_string();
        assert!(
            !check(T::Decimal128, &column, Access::Query)
                .unwrap()
                .unsigned
        );
    }

    #[test]
    fn a_synthetic_column_takes_its_family_from_the_declared_type() {
        for (ty, family) in [
            (T::Utf8, Family::Text),
            (T::UInt64, Family::Integer(64)),
            (T::TimestampMicrosUtc, Family::Timestamp),
            (T::Binary, Family::Blob),
        ] {
            assert_eq!(ResolvedColumn::synthetic(&spec("c", ty)).family, family);
        }
    }

    // --------------------------------------------------------------- enum_labels

    #[test]
    fn enum_labels_split_a_plain_list() {
        assert_eq!(
            enum_labels("enum('small','medium','large')").unwrap(),
            vec!["small", "medium", "large"]
        );
        assert_eq!(enum_labels("set('a','b')").unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn enum_labels_undouble_a_quote() {
        assert_eq!(enum_labels("enum('a','b''c')").unwrap(), vec!["a", "b'c"]);
        assert_eq!(enum_labels("enum('''')").unwrap(), vec!["'"]);
    }

    #[test]
    fn enum_labels_keep_commas_and_parentheses_inside_a_label() {
        assert_eq!(
            enum_labels("enum('a,b','c)d','(e')").unwrap(),
            vec!["a,b", "c)d", "(e"]
        );
    }

    #[test]
    fn enum_labels_decode_the_servers_backslash_escapes() {
        assert_eq!(
            enum_labels(r"enum('a\\b','c\nd','e\0f','g\rh','i\Zj')").unwrap(),
            vec!["a\\b", "c\nd", "e\0f", "g\rh", "i\u{1a}j"]
        );
    }

    #[test]
    fn an_empty_label_is_a_label() {
        assert_eq!(enum_labels("enum('','a')").unwrap(), vec!["", "a"]);
    }

    #[test]
    fn enum_labels_keep_non_ascii_text() {
        assert_eq!(
            enum_labels("enum('café','日本')").unwrap(),
            vec!["café", "日本"]
        );
    }

    #[test]
    fn enum_labels_refuse_text_that_is_not_a_quoted_list() {
        for bad in [
            "enum",
            "enum(a,b)",
            "enum('a'",
            "enum('a' 'b')",
            "enum('a\\",
            "enum('a',)",
        ] {
            assert!(enum_labels(bad).is_err(), "{bad} should be refused");
        }
    }
}
