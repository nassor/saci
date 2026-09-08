//! The canonical PostgreSQL/Arrow type table, the type-name resolver, and the
//! load-time checks that enforce both.
//!
//! [`ROWS`] is the one mapping in this crate: one entry per PostgreSQL base
//! type, carrying the OID, the OID of its one-dimensional array type, the
//! [`PgFieldType`] a column of it maps to by default, and every declared type
//! it can fill over the binary wire. A PostgreSQL type with no Arrow
//! counterpart maps to `utf8` in PostgreSQL's canonical text form, which is
//! what makes the table total: every type is readable and writable, and the
//! server's own output and input functions do the rendering.
//!
//! The connector never coerces. A declared `int32` fed by an `int8` column is a
//! configuration error, not a narrowing cast, and reading a type whose default
//! is not `utf8` as text takes an explicit `pg_type` on the field.
//! [`validate_columns`] runs once per connection against the real catalog --
//! the statement's result columns for the source, `pg_attribute` for the sink
//! -- and reports every mismatch at once so one restart fixes the whole config.

use saci_core::error::SaciError;
use tokio_postgres::types::Type;

use crate::config::{FieldSpec, PgFieldType, Role};

/// `jsonb`'s OID, which the encoder checks to add the version byte.
pub(crate) const OID_JSONB: u32 = 3802;
/// `numeric`'s OID, which the sink checks to compare a forced modifier's
/// precision and scale rather than its spelling.
pub(crate) const OID_NUMERIC: u32 = 1700;
/// `numeric[]`'s OID, for the same comparison over an array column.
pub(crate) const OID_NUMERIC_ARRAY: u32 = 1231;

/// How one (PostgreSQL type, declared type) pair moves a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Wire {
    /// A codec frames the type's binary send/recv form.
    Binary,
    /// The value travels as PostgreSQL canonical text: the type's output
    /// function on the query path (`format('%s', col)`) and on the logical
    /// path (`pgoutput`'s text tuples), and a cast from a text-typed staging
    /// column on the sink path.
    Text,
}

/// One row of the canonical mapping.
struct TypeRow {
    /// `Type::name()`, the canonical PostgreSQL spelling.
    pg: &'static str,
    /// The base type's OID, or 0 for a type whose OID is installation-dependent.
    oid: u32,
    /// The OID of the one-dimensional array of this type, or 0 when it has none.
    array_oid: u32,
    /// The declared type a column of this type maps to when nothing forces it.
    default_arrow: PgFieldType,
    /// Every declared type this PostgreSQL type fills over [`Wire::Binary`].
    ///
    /// `utf8` over [`Wire::Text`] is admitted by rule rather than listed here,
    /// so it is not repeated in all 84 rows: see [`resolve_wire`].
    binary: &'static [PgFieldType],
}

const fn row(
    pg: &'static str,
    oid: u32,
    array_oid: u32,
    default_arrow: PgFieldType,
    binary: &'static [PgFieldType],
) -> TypeRow {
    TypeRow {
        pg,
        oid,
        array_oid,
        default_arrow,
        binary,
    }
}

/// The canonical table, sorted by `pg` so a name is a binary search.
///
/// Every `Kind::Simple`, `Kind::Range` and `Kind::Multirange` built-in, plus the
/// three extension types whose OIDs are installation-dependent. `Kind::Pseudo`
/// types are absent: they cannot be column types. Enums, domains and composites
/// are absent too -- they have no static OID and are resolved through the
/// catalog.
#[rustfmt::skip]
static ROWS: &[TypeRow] = &[
    row("aclitem", 1033, 1034, PgFieldType::Utf8, &[]),
    row("bit", 1560, 1561, PgFieldType::Utf8, &[]),
    row("bool", 16, 1000, PgFieldType::Boolean, &[PgFieldType::Boolean]),
    row("box", 603, 1020, PgFieldType::Utf8, &[]),
    row("bpchar", 1042, 1014, PgFieldType::Utf8, &[PgFieldType::Utf8]),
    row("bytea", 17, 1001, PgFieldType::Binary, &[PgFieldType::Binary]),
    row("char", 18, 1002, PgFieldType::Utf8, &[]),
    row("cid", 29, 1012, PgFieldType::Int64, &[PgFieldType::Int64]),
    row("cidr", 650, 651, PgFieldType::Utf8, &[]),
    row("circle", 718, 719, PgFieldType::Utf8, &[]),
    row("citext", 0, 0, PgFieldType::Utf8, &[PgFieldType::Utf8]),
    row("date", 1082, 1182, PgFieldType::Date32, &[PgFieldType::Date32]),
    row("datemultirange", 4535, 6155, PgFieldType::Utf8, &[]),
    row("daterange", 3912, 3913, PgFieldType::Utf8, &[]),
    row("float4", 700, 1021, PgFieldType::Float32, &[PgFieldType::Float32]),
    row("float8", 701, 1022, PgFieldType::Float64, &[PgFieldType::Float64]),
    row("gtsvector", 3642, 3644, PgFieldType::Utf8, &[]),
    row("hstore", 0, 0, PgFieldType::Utf8, &[]),
    row("inet", 869, 1041, PgFieldType::Utf8, &[]),
    row("int2", 21, 1005, PgFieldType::Int16, &[PgFieldType::Int16]),
    row("int4", 23, 1007, PgFieldType::Int32, &[PgFieldType::Int32]),
    row("int4multirange", 4451, 6150, PgFieldType::Utf8, &[]),
    row("int4range", 3904, 3905, PgFieldType::Utf8, &[]),
    row("int8", 20, 1016, PgFieldType::Int64, &[PgFieldType::Int64]),
    row("int8multirange", 4536, 6157, PgFieldType::Utf8, &[]),
    row("int8range", 3926, 3927, PgFieldType::Utf8, &[]),
    row("interval", 1186, 1187, PgFieldType::IntervalMonthDayNano, &[PgFieldType::IntervalMonthDayNano]),
    row("json", 114, 199, PgFieldType::Json, &[PgFieldType::Json]),
    row("jsonb", 3802, 3807, PgFieldType::Json, &[PgFieldType::Json]),
    row("jsonpath", 4072, 4073, PgFieldType::Utf8, &[]),
    row("line", 628, 629, PgFieldType::Utf8, &[]),
    row("lseg", 601, 1018, PgFieldType::Utf8, &[]),
    row("ltree", 0, 0, PgFieldType::Utf8, &[]),
    row("macaddr", 829, 1040, PgFieldType::Utf8, &[]),
    row("macaddr8", 774, 775, PgFieldType::Utf8, &[]),
    row("money", 790, 791, PgFieldType::Decimal128, &[PgFieldType::Decimal128]),
    row("name", 19, 1003, PgFieldType::Utf8, &[PgFieldType::Utf8]),
    row("numeric", 1700, 1231, PgFieldType::Utf8, &[PgFieldType::Decimal128]),
    row("nummultirange", 4532, 6151, PgFieldType::Utf8, &[]),
    row("numrange", 3906, 3907, PgFieldType::Utf8, &[]),
    row("oid", 26, 1028, PgFieldType::Int64, &[PgFieldType::Int64]),
    row("path", 602, 1019, PgFieldType::Utf8, &[]),
    row("pg_brin_bloom_summary", 4600, 0, PgFieldType::Utf8, &[]),
    row("pg_brin_minmax_multi_summary", 4601, 0, PgFieldType::Utf8, &[]),
    row("pg_dependencies", 3402, 0, PgFieldType::Utf8, &[]),
    row("pg_lsn", 3220, 3221, PgFieldType::Utf8, &[]),
    row("pg_mcv_list", 5017, 0, PgFieldType::Utf8, &[]),
    row("pg_ndistinct", 3361, 0, PgFieldType::Utf8, &[]),
    row("pg_node_tree", 194, 0, PgFieldType::Utf8, &[]),
    row("pg_snapshot", 5038, 5039, PgFieldType::Utf8, &[]),
    row("point", 600, 1017, PgFieldType::Utf8, &[]),
    row("polygon", 604, 1027, PgFieldType::Utf8, &[]),
    row("refcursor", 1790, 2201, PgFieldType::Utf8, &[]),
    row("regclass", 2205, 2210, PgFieldType::Utf8, &[]),
    row("regcollation", 4191, 4192, PgFieldType::Utf8, &[]),
    row("regconfig", 3734, 3735, PgFieldType::Utf8, &[]),
    row("regdictionary", 3769, 3770, PgFieldType::Utf8, &[]),
    row("regnamespace", 4089, 4090, PgFieldType::Utf8, &[]),
    row("regoper", 2203, 2208, PgFieldType::Utf8, &[]),
    row("regoperator", 2204, 2209, PgFieldType::Utf8, &[]),
    row("regproc", 24, 1008, PgFieldType::Utf8, &[]),
    row("regprocedure", 2202, 2207, PgFieldType::Utf8, &[]),
    row("regrole", 4096, 4097, PgFieldType::Utf8, &[]),
    row("regtype", 2206, 2211, PgFieldType::Utf8, &[]),
    row("text", 25, 1009, PgFieldType::Utf8, &[PgFieldType::Utf8]),
    row("tid", 27, 1010, PgFieldType::Utf8, &[]),
    row("time", 1083, 1183, PgFieldType::Time64Micros, &[PgFieldType::Time64Micros]),
    row("timestamp", 1114, 1115, PgFieldType::TimestampMicros, &[PgFieldType::TimestampMicros]),
    row("timestamptz", 1184, 1185, PgFieldType::TimestampMicrosUtc, &[PgFieldType::TimestampMicrosUtc]),
    row("timetz", 1266, 1270, PgFieldType::Utf8, &[]),
    row("tsmultirange", 4533, 6152, PgFieldType::Utf8, &[]),
    row("tsquery", 3615, 3645, PgFieldType::Utf8, &[]),
    row("tsrange", 3908, 3909, PgFieldType::Utf8, &[]),
    row("tstzmultirange", 4534, 6153, PgFieldType::Utf8, &[]),
    row("tstzrange", 3910, 3911, PgFieldType::Utf8, &[]),
    row("tsvector", 3614, 3643, PgFieldType::Utf8, &[]),
    row("txid_snapshot", 2970, 2949, PgFieldType::Utf8, &[]),
    row("unknown", 705, 0, PgFieldType::Utf8, &[PgFieldType::Utf8]),
    row("uuid", 2950, 2951, PgFieldType::Uuid, &[PgFieldType::Uuid]),
    row("varbit", 1562, 1563, PgFieldType::Utf8, &[]),
    row("varchar", 1043, 1015, PgFieldType::Utf8, &[PgFieldType::Utf8]),
    row("xid", 28, 1011, PgFieldType::Int64, &[PgFieldType::Int64]),
    row("xid8", 5069, 271, PgFieldType::Utf8, &[]),
    row("xml", 142, 143, PgFieldType::Utf8, &[]),
];

/// `oid` to [`ROWS`] index, sorted by OID.
#[rustfmt::skip]
static BY_OID: &[(u32, u16)] = &[
    (16, 2), (17, 5), (18, 6), (19, 36), (20, 23), (21, 19), (23, 20), (24, 60), (25, 64),
    (26, 40), (27, 65), (28, 81), (29, 7), (114, 27), (142, 83), (194, 48), (600, 50),
    (601, 31), (602, 41), (603, 3), (604, 51), (628, 30), (650, 8), (700, 14), (701, 15),
    (705, 77), (718, 9), (774, 34), (790, 35), (829, 33), (869, 18), (1033, 0), (1042, 4),
    (1043, 80), (1082, 11), (1083, 66), (1114, 67), (1184, 68), (1186, 26), (1266, 69),
    (1560, 1), (1562, 79), (1700, 37), (1790, 52), (2202, 61), (2203, 58), (2204, 59),
    (2205, 53), (2206, 63), (2950, 78), (2970, 76), (3220, 45), (3361, 47), (3402, 44),
    (3614, 75), (3615, 71), (3642, 16), (3734, 55), (3769, 56), (3802, 28), (3904, 22),
    (3906, 39), (3908, 72), (3910, 74), (3912, 13), (3926, 25), (4072, 29), (4089, 57),
    (4096, 62), (4191, 54), (4451, 21), (4532, 38), (4533, 70), (4534, 73), (4535, 12),
    (4536, 24), (4600, 42), (4601, 43), (5017, 46), (5038, 49), (5069, 82),
];

/// Array-type OID to the [`ROWS`] index of its *element*, sorted by OID.
#[rustfmt::skip]
static BY_ARRAY_OID: &[(u32, u16)] = &[
    (143, 83), (199, 27), (271, 82), (629, 30), (651, 8), (719, 9), (775, 34), (791, 35),
    (1000, 2), (1001, 5), (1002, 6), (1003, 36), (1005, 19), (1007, 20), (1008, 60), (1009, 64),
    (1010, 65), (1011, 81), (1012, 7), (1014, 4), (1015, 80), (1016, 23), (1017, 50),
    (1018, 31), (1019, 41), (1020, 3), (1021, 14), (1022, 15), (1027, 51), (1028, 40),
    (1034, 0), (1040, 33), (1041, 18), (1115, 67), (1182, 11), (1183, 66), (1185, 68),
    (1187, 26), (1231, 37), (1270, 69), (1561, 1), (1563, 79), (2201, 52), (2207, 61),
    (2208, 58), (2209, 59), (2210, 53), (2211, 63), (2949, 76), (2951, 78), (3221, 45),
    (3643, 75), (3644, 16), (3645, 71), (3735, 55), (3770, 56), (3807, 28), (3905, 22),
    (3907, 39), (3909, 72), (3911, 74), (3913, 13), (3927, 25), (4073, 29), (4090, 57),
    (4097, 62), (4192, 54), (5039, 49), (6150, 21), (6151, 38), (6152, 70), (6153, 73),
    (6155, 12), (6157, 24),
];

/// SQL spellings that are not `Type::name()`, sorted by alias.
///
/// `serial` and friends are not types at all; they resolve to the integer type
/// they store, which is what a column declared `serial` actually has.
#[rustfmt::skip]
static ALIASES: &[(&str, &str)] = &[
    ("bigint", "int8"),
    ("bigserial", "int8"),
    ("bit varying", "varbit"),
    ("boolean", "bool"),
    ("char", "bpchar"),
    ("character", "bpchar"),
    ("character varying", "varchar"),
    ("decimal", "numeric"),
    ("double precision", "float8"),
    ("float", "float8"),
    ("int", "int4"),
    ("integer", "int4"),
    ("real", "float4"),
    ("serial", "int4"),
    ("smallint", "int2"),
    ("smallserial", "int2"),
    ("time with time zone", "timetz"),
    ("time without time zone", "time"),
    ("timestamp with time zone", "timestamptz"),
    ("timestamp without time zone", "timestamp"),
];

fn row_at(index: u16) -> &'static TypeRow {
    // Every index in BY_OID and BY_ARRAY_OID is generated from ROWS itself.
    &ROWS[index as usize]
}

fn row_by_name(name: &str) -> Option<&'static TypeRow> {
    ROWS.binary_search_by(|row| row.pg.cmp(name))
        .ok()
        .map(row_at_usize)
}

fn row_at_usize(index: usize) -> &'static TypeRow {
    // Only ever reached from a successful `binary_search` over `ROWS`, whose
    // contract is an in-bounds index.
    &ROWS[index]
}

fn row_by_oid(oid: u32) -> Option<&'static TypeRow> {
    BY_OID
        .binary_search_by_key(&oid, |(key, _)| *key)
        .ok()
        .map(|i| row_at(BY_OID[i].1))
}

/// The *element* row of an array type's OID.
fn row_by_array_oid(oid: u32) -> Option<&'static TypeRow> {
    BY_ARRAY_OID
        .binary_search_by_key(&oid, |(key, _)| *key)
        .ok()
        .map(|i| row_at(BY_ARRAY_OID[i].1))
}

/// The server-side name of a type OID, for error messages.
///
/// The table covers every built-in that can be a column type, and an array OID
/// renders as `elem[]`. `Type::from_oid` still backs the pseudo and vector
/// types the table omits; anything else -- a domain, an enum, a composite, an
/// extension type -- has no static name and is reported by OID alone.
pub(crate) fn type_name(oid: u32) -> String {
    if let Some(row) = row_by_oid(oid) {
        return row.pg.to_string();
    }
    if let Some(element) = row_by_array_oid(oid) {
        return format!("{}[]", element.pg);
    }
    match Type::from_oid(oid) {
        Some(ty) => ty.name().to_string(),
        None => format!("oid {oid}"),
    }
}

/// The SQL spelling of a type that constrains nothing, so a cast to it can
/// neither pad nor truncate the value it carries.
///
/// This is what the sink's staging route casts to, leaving the target's own
/// modifier to the assignment coercion the `INSERT` performs -- which raises
/// where an explicit cast would resize.
///
/// The name is quoted, because two type *keywords* carry a length default the
/// generic identifier does not: an unquoted `char` is `character(1)`, so
/// `'\310'::char` keeps one character of a four-character render, and an
/// unquoted `bit` is `bit(1)`, so `'1010'::bit` keeps one bit.
/// `pg_catalog."char"` and `pg_catalog."bit"` are the types themselves, at
/// typmod -1. The `pg_catalog.` qualifier is there for the same reason it is
/// on the cursor path's `pg_catalog.format`: a type on the role's
/// `search_path` must not be able to shadow a built-in one.
pub(crate) fn unconstrained_type_name(oid: u32) -> String {
    if let Some(element) = row_by_array_oid(oid) {
        return format!("pg_catalog.\"{}\"[]", element.pg);
    }
    format!("pg_catalog.\"{}\"", type_name(oid))
}

/// The element type's OID for an array type's OID.
pub(crate) fn array_element_oid(oid: u32) -> Option<u32> {
    row_by_array_oid(oid).map(|row| row.oid)
}

/// Whether the canonical table knows this OID, as a base type or as an array.
///
/// A `false` means the catalog has to be consulted: a domain, an enum, a
/// composite or an extension type.
pub(crate) fn is_static_type(oid: u32) -> bool {
    row_by_oid(oid).is_some() || row_by_array_oid(oid).is_some()
}

/// A column's PostgreSQL type, after catalog resolution.
///
/// [`resolve_wire`] sees only OIDs, and a domain, an enum, a composite or an
/// extension type has no static row to look one up in. The callers therefore
/// resolve those against `pg_type` first -- see
/// [`connection::column_types`](crate::connection::column_types) -- and hand
/// the result here.
#[derive(Debug, Clone)]
pub(crate) struct ColumnType {
    /// Column name, matched against declared fields by name.
    pub(crate) name: String,
    /// The OID to decode as: a domain's base type, otherwise the column's own.
    pub(crate) oid: u32,
    /// `pg_attribute.atttypid` exactly as the catalog reports it: the
    /// column's own type OID, before a domain chain is walked. Equal to
    /// [`Self::oid`] for every kind of type but a domain, where it is the
    /// domain's OID and [`Self::oid`] the base's.
    ///
    /// A `pgoutput` `Relation` message publishes this OID, so it is the
    /// comparand that catches an `ALTER TABLE … ALTER COLUMN … TYPE` landing
    /// after a `cdc_logical` session read the catalog.
    pub(crate) attribute_oid: u32,
    /// How the server spells this type (`format_type`), modifier included,
    /// which is what an error message shows.
    pub(crate) display: String,
    /// The unconstrained SQL spelling -- no modifier -- of the type this
    /// column's wire form actually belongs to: the *base* type for a domain,
    /// the type itself otherwise. A built-in arrives here through
    /// [`unconstrained_type_name`], so it is quoted and `pg_catalog`
    /// qualified; a catalog type keeps its `format_type` spelling, which
    /// resolves in the same session's `search_path` and is therefore a valid
    /// cast target as it stands.
    ///
    /// This is the sink's cast target on the staging route. It carries no
    /// modifier and never names a domain on purpose: an explicit cast applies
    /// a `varchar(n)`/`bit varying(n)` modifier with explicit-cast semantics,
    /// which *truncates*, so the modifier and the domain's constraints are
    /// left to the `INSERT`'s own assignment coercion, which raises instead.
    pub(crate) base_display: String,
    /// `pg_attribute.atttypmod`, or `-1` when the column declares no modifier.
    /// Only the sink reads it, to compare a `numeric(p,s)` target against a
    /// declared `decimal128` scale.
    pub(crate) typmod: i32,
    /// `(schema, typname)` of the column's own type, when it has no static
    /// row. This is what a forced `pg_type` asserts identity against.
    pub(crate) catalog: Option<(String, String)>,
}

impl ColumnType {
    /// A built-in column type, which needs no catalog round-trip.
    #[cfg(test)]
    pub(crate) fn builtin(name: String, oid: u32) -> Self {
        Self {
            name,
            display: type_name(oid),
            base_display: unconstrained_type_name(oid),
            oid,
            attribute_oid: oid,
            typmod: -1,
            catalog: None,
        }
    }
}

/// Which wire form fills `spec` from a column of PostgreSQL type `oid`.
///
/// `role` decides one rule: a sink may encode a declared integer into a
/// narrower PostgreSQL integer, with a per-value range check, when `pg_type`
/// names that target explicitly. A source never widens on read.
///
/// # Errors
///
/// Returns the reason the pair is illegal, without a column prefix:
/// [`validate_columns`] adds one.
pub(crate) fn resolve_wire(
    spec: &FieldSpec,
    column: &ColumnType,
    role: Role,
) -> Result<Wire, String> {
    let forced = spec.forced_pg_type()?;
    let oid = column.oid;

    if let Some(forced) = &forced {
        match (forced.builtin_oid, &column.catalog) {
            // A built-in name asserts against the resolved OID.
            (Some(want), _) if want != oid => {
                return Err(format!(
                    "pg_type = \"{}\" is {} (oid {want}) but the server type is {} (oid {oid})",
                    forced.raw,
                    type_name(want),
                    column.display
                ));
            }
            (Some(_), _) => {}
            // A name the table does not know asserts against the catalog's own
            // `schema.typname`, so `pg_type = "public.mood"` proves *this*
            // enum rather than merely "some type the table has no row for".
            (None, Some((schema, typname))) => {
                let name_matches = forced.base == *typname;
                let schema_matches = forced
                    .schema
                    .as_ref()
                    .is_none_or(|declared| declared == schema);
                if !name_matches || !schema_matches {
                    return Err(format!(
                        "pg_type = \"{}\" but the server type is {schema}.{typname}",
                        forced.raw
                    ));
                }
            }
            (None, None) => {
                return Err(format!(
                    "pg_type = \"{}\" is not a built-in type, and the server type {} is",
                    forced.raw, column.display
                ));
            }
        }
    }

    if let Some(element) = row_by_array_oid(oid) {
        let Some(item) = spec.item else {
            return Err(format!(
                "the server type is {}[] (oid {oid}); declare type = \"list\" with item = \"{}\"",
                element.pg,
                element.default_arrow.as_str()
            ));
        };
        if element.binary.contains(&item) {
            return Ok(Wire::Binary);
        }
        // Same intent rule as a scalar: reading elements as text is fine when
        // text is what the element type maps to anyway, and takes an explicit
        // `pg_type` naming the array otherwise.
        if item == PgFieldType::Utf8 {
            if element.default_arrow == PgFieldType::Utf8
                || forced.as_ref().is_some_and(|f| f.builtin_oid == Some(oid))
            {
                return Ok(Wire::Text);
            }
            return Err(format!(
                "item = \"utf8\" reads an element of {} (oid {oid}) as PostgreSQL text; add \
                 pg_type = \"{}[]\" to say so deliberately, or declare item = \"{}\"",
                element.pg,
                element.pg,
                element.default_arrow.as_str()
            ));
        }
        return Err(format!(
            "item = \"{}\" cannot hold an element of {} (oid {oid}); {}",
            item.as_str(),
            element.pg,
            options(element)
        ));
    }

    if spec.ty == PgFieldType::List {
        return Err(format!(
            "declared type \"list\" but the server type is {} (oid {oid}), which is not an array \
             type",
            column.display
        ));
    }

    let Some(row) = row_by_oid(oid) else {
        // No static row even after the catalog step: an enum, a composite, or
        // an extension type. Its canonical text is the only form this
        // connector can read without knowing the type, and naming it with
        // `pg_type` is what proves the user meant this type. (A domain never
        // reaches here: the catalog step replaced it with its base OID.)
        return match (&forced, spec.ty) {
            (Some(_), PgFieldType::Utf8) => Ok(Wire::Text),
            (Some(forced), ty) => Err(format!(
                "pg_type = \"{}\" is {}, which can only be read as text; declare type = \"utf8\" \
                 rather than \"{}\"",
                forced.raw,
                column.display,
                ty.as_str()
            )),
            (None, ty) => Err(format!(
                "declared type \"{}\" but the server type {} (oid {oid}) has no built-in type; \
                 name it with pg_type = \"{}\" and declare type = \"utf8\" to read its text form",
                ty.as_str(),
                column.display,
                column.display
            )),
        };
    };

    if row.binary.contains(&spec.ty) {
        return Ok(Wire::Binary);
    }
    let named_exactly = forced.as_ref().is_some_and(|f| f.builtin_oid == Some(oid));
    // The text form of a type whose default is not `utf8` is available, but
    // only on purpose: `pg_type` naming that exact type is the intent.
    if spec.ty == PgFieldType::Utf8 && (row.default_arrow == PgFieldType::Utf8 || named_exactly) {
        return Ok(Wire::Text);
    }
    // A sink narrows an integer width with a per-value range check. Gated on
    // the explicit `pg_type`, so a declared `int64` over a plain `int4` column
    // stays the configuration error it has always been.
    if role == Role::Sink
        && named_exactly
        && is_integer(spec.ty)
        && row.binary.iter().copied().any(is_integer)
    {
        return Ok(Wire::Binary);
    }
    Err(format!(
        "declared type \"{}\" cannot hold {} (oid {oid}); {}",
        spec.ty.as_str(),
        row.pg,
        options(row)
    ))
}

/// The declarations that *can* hold a column of this type, for an error message.
fn options(row: &TypeRow) -> String {
    let mut parts: Vec<String> = row
        .binary
        .iter()
        .map(|ty| match ty {
            PgFieldType::Decimal128 => {
                "declare type = \"decimal128\" with explicit 'precision' and 'scale'".to_string()
            }
            other => format!("declare type = \"{}\"", other.as_str()),
        })
        .collect();
    parts.push(if row.default_arrow == PgFieldType::Utf8 {
        "declare type = \"utf8\" to read it as PostgreSQL text (its output function)".to_string()
    } else {
        format!(
            "declare type = \"utf8\" with pg_type = \"{}\" to read it as PostgreSQL text (its \
             output function)",
            row.pg
        )
    });
    parts.join(", or ")
}

/// The scale a `numeric` column's `atttypmod` packs, when it declares one.
///
/// PostgreSQL stores `numeric`'s precision and scale in one `atttypmod`, with
/// `-1` for an unconstrained column; the scale field is signed as of
/// PostgreSQL 15. The sink compares it against a declared `decimal128` scale,
/// because a narrower target rounds on input rather than erroring.
pub(crate) fn numeric_scale(typmod: i32) -> Option<i32> {
    // 4 is `VARHDRSZ`, which every varlena typmod carries.
    let packed = typmod.checked_sub(4).filter(|packed| *packed >= 0)?;
    Some(((packed & 0x7ff) ^ 1024) - 1024)
}

/// The precision a `numeric` column's `atttypmod` packs, when it declares one.
///
/// The sink compares it against a forced `pg_type = "numeric(p,s)"`, so a
/// modifier the configuration states and the server does not carry is a
/// refusal rather than a silent re-interpretation.
pub(crate) fn numeric_precision(typmod: i32) -> Option<u8> {
    let packed = typmod.checked_sub(4).filter(|packed| *packed >= 0)?;
    u8::try_from((packed >> 16) & 0xffff).ok()
}

/// Reject a declared schema the server's actual column types cannot fill, and
/// return what it resolved to.
///
/// Matches by name, never by position, and collects every problem before
/// returning so a misconfigured schema is fixed in one pass. Columns the server
/// has but the config does not declare are ignored. On success the result holds
/// one entry per declared field, in declared order, so a caller needs neither a
/// second lookup nor an `expect` to recover what was just proved.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] listing every declared field that is
/// absent from `actual` or whose type `actual` cannot fill.
pub(crate) fn validate_columns(
    what: &str,
    role: Role,
    declared: &[FieldSpec],
    actual: &[ColumnType],
) -> Result<Vec<(ColumnType, Wire)>, SaciError> {
    let mut resolved = Vec::with_capacity(declared.len());
    let mut problems: Vec<String> = Vec::new();

    for spec in declared {
        let Some(column) = actual.iter().find(|column| column.name == spec.name) else {
            problems.push(format!(
                "column '{}' is declared but the server has no such column (server columns: {})",
                spec.name,
                if actual.is_empty() {
                    "none".to_string()
                } else {
                    actual
                        .iter()
                        .map(|column| column.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            ));
            continue;
        };

        match resolve_wire(spec, column, role) {
            Err(reason) => problems.push(format!("column '{}': {reason}", spec.name)),
            Ok(wire) => resolved.push((column.clone(), wire)),
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

// ------------------------------------------------------------------ resolver

/// A PostgreSQL type name from a `pg_type` key, parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgTypeRef {
    /// The spelling from the configuration, for error messages.
    pub(crate) raw: String,
    /// The schema qualifier, when the name carried one. Asserted against the
    /// catalog's `nspname` for a type the table does not know.
    pub(crate) schema: Option<String>,
    /// The canonical base type name, aliases resolved.
    pub(crate) base: String,
    /// The parenthesised modifier without its parentheses, e.g. `12,2`.
    pub(crate) typmod: Option<String>,
    /// Whether the name named the one-dimensional array of `base`.
    pub(crate) array: bool,
    /// The OID this name resolves to, when the base type is built in.
    ///
    /// `None` for an enum, a domain, a composite, an extension type or a
    /// schema-qualified name: those need the catalog, which
    /// `SourceFactory::build` cannot reach.
    pub(crate) builtin_oid: Option<u32>,
}

/// Parse a `pg_type` spelling.
///
/// Accepts every `Type::name()`, the SQL aliases in [`ALIASES`], both array
/// spellings (`text[]` and `_text`), a parenthesised modifier anywhere after
/// the type name (`numeric(12,2)` and `format_type`'s own
/// `timestamp(3) with time zone`), a `schema.type` qualifier, and a quoted
/// name (`"char"`, which is a different type from `char`, and
/// `public."Mood"`, whose case is kept as written).
///
/// # Errors
///
/// Returns the reason the spelling is not a type name.
pub(crate) fn resolve_pg_type(spelling: &str) -> Result<PgTypeRef, String> {
    let raw = spelling.trim();
    if raw.is_empty() {
        return Err("pg_type must not be empty".to_string());
    }
    let mut text: String = collapse_whitespace(raw);

    let mut array = false;
    while let Some(rest) = text.strip_suffix("[]") {
        if array {
            return Err(format!(
                "pg_type = \"{raw}\" is multi-dimensional; Arrow's List is one-dimensional, so \
                 only a single '[]' is supported"
            ));
        }
        array = true;
        text = rest.trim_end().to_string();
    }

    let mut typmod = None;
    match outside_quotes(&text, '(').0 {
        Some(open) => {
            if text[..open].trim().is_empty() {
                return Err(format!(
                    "pg_type = \"{raw}\" has a modifier with no type name before it"
                ));
            }
            // The '(' sits outside every quoted identifier, so the rest of the
            // text starts outside one too and its own scan is in step.
            let Some(close) = outside_quotes(&text[open + 1..], ')')
                .0
                .map(|i| open + 1 + i)
            else {
                return Err(format!("pg_type = \"{raw}\" has a '(' with no ')'"));
            };
            let inside = text[open + 1..close].replace(' ', "");
            let parts: Vec<&str> = inside.split(',').collect();
            // A leading '-' is legal on the scale alone: PostgreSQL 15 admits
            // `numeric(10,-2)`, which rounds to hundreds.
            let digits = |part: &str| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit());
            let legal = match parts.as_slice() {
                [precision] => digits(precision),
                [precision, scale] => {
                    digits(precision) && digits(scale.strip_prefix('-').unwrap_or(scale))
                }
                _ => false,
            };
            if !legal {
                return Err(format!(
                    "pg_type = \"{raw}\" has a modifier that is not '(digits)' or \
                     '(digits,[-]digits)'"
                ));
            }
            typmod = Some(inside);
            // `format_type` puts the modifier before the trailing words, so
            // cutting it out of the middle is what leaves an alias the table
            // knows: `timestamp(3) with time zone` -> `timestamp with time
            // zone`.
            let mut rest = text[..open].to_string();
            rest.push_str(&text[close + 1..]);
            text = collapse_whitespace(&rest);
            if outside_quotes(&text, '(').1 > 0 || outside_quotes(&text, ')').1 > 0 {
                return Err(format!(
                    "pg_type = \"{raw}\" has more than one modifier; a type carries one"
                ));
            }
        }
        None => {
            if outside_quotes(&text, ')').1 > 0 {
                return Err(format!("pg_type = \"{raw}\" has a ')' with no '('"));
            }
        }
    }

    // The schema is split off before quoting is decided, so a quoted name
    // after a qualifier keeps its case exactly as an unqualified one does.
    let mut schema: Option<String> = None;
    let mut qualified = false;
    match outside_quotes(&text, '.') {
        (_, 0) => {}
        (Some(dot), 1) => {
            let (schema_part, name) = (&text[..dot], &text[dot + 1..]);
            if schema_part.is_empty() || name.is_empty() {
                return Err(format!(
                    "pg_type = \"{raw}\" has an empty schema or type part"
                ));
            }
            let Some(schema_name) = unquote_or_fold(schema_part) else {
                return Err(format!("pg_type = \"{raw}\" has an unterminated quote"));
            };
            qualified = schema_part_is_foreign(&schema_name);
            schema = Some(schema_name);
            text = name.to_string();
        }
        _ => {
            return Err(format!(
                "pg_type = \"{raw}\" must be 'type' or 'schema.type', not a longer path"
            ));
        }
    }

    let quoted = text.starts_with('"');
    let Some(mut base) = unquote_or_fold(&text) else {
        return Err(format!("pg_type = \"{raw}\" has an unterminated quote"));
    };

    if !quoted
        && let Some(stripped) = base.strip_prefix('_')
        && row_by_name(stripped).is_some()
    {
        if array {
            return Err(format!(
                "pg_type = \"{raw}\" names an array twice ('_' and '[]'); Arrow's List is \
                 one-dimensional"
            ));
        }
        array = true;
        base = stripped.to_string();
    }

    if !quoted && let Ok(i) = ALIASES.binary_search_by_key(&base.as_str(), |(alias, _)| *alias) {
        base = ALIASES[i].1.to_string();
    }

    let builtin_oid = match row_by_name(&base) {
        Some(row) if !qualified && row.oid != 0 => {
            if array {
                if row.array_oid == 0 {
                    return Err(format!("pg_type = \"{raw}\": {} has no array type", row.pg));
                }
                Some(row.array_oid)
            } else {
                Some(row.oid)
            }
        }
        _ => None,
    };

    Ok(PgTypeRef {
        raw: raw.to_string(),
        schema,
        base,
        typmod,
        array,
        builtin_oid,
    })
}

/// Whether a schema qualifier means "not the built-in of this name".
///
/// `pg_catalog` is where the built-ins live, so `pg_catalog.int4` still
/// resolves statically; any other schema names a different type that only the
/// catalog can resolve.
fn schema_part_is_foreign(schema: &str) -> bool {
    !schema.eq_ignore_ascii_case("pg_catalog")
}

/// An identifier part with its quotes removed, or folded to lower case when it
/// carries none.
///
/// PostgreSQL folds an unquoted identifier to lower case and keeps a quoted
/// one exactly, so `public.mood` and `public."Mood"` name different types.
/// `None` for a part that opens a quote and does not close it.
fn unquote_or_fold(part: &str) -> Option<String> {
    if !part.starts_with('"') {
        return Some(part.to_ascii_lowercase());
    }
    match part.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(inner) if !inner.is_empty() => Some(inner.to_string()),
        _ => None,
    }
}

/// The byte index of the first `needle` outside a quoted identifier, and how
/// many such occurrences there are.
///
/// A quoted name may hold a `.` or a `(` of its own, which is neither a schema
/// separator nor a modifier.
fn outside_quotes(text: &str, needle: char) -> (Option<usize>, usize) {
    let mut quoted = false;
    let mut first = None;
    let mut count = 0;
    for (i, c) in text.char_indices() {
        if c == '"' {
            quoted = !quoted;
        } else if !quoted && c == needle {
            count += 1;
            first = first.or(Some(i));
        }
    }
    (first, count)
}

fn collapse_whitespace(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut space = false;
    for c in text.chars() {
        if c.is_whitespace() {
            space = true;
            continue;
        }
        if space && !out.is_empty() {
            out.push(' ');
        }
        space = false;
        out.push(c);
    }
    out
}

/// Reject a `pg_type` the declared type cannot be encoded to or decoded from.
///
/// Runs at load time, so it sees only what the table knows: a name needing the
/// catalog is checked when the connection opens, by [`resolve_wire`].
///
/// # Errors
///
/// Returns the reason the pair is illegal, without a column prefix.
pub(crate) fn validate_forced(
    spec: &FieldSpec,
    forced: &PgTypeRef,
    role: Role,
) -> Result<(), String> {
    // A foreign schema qualifier means "not the built-in of this name", which
    // is exactly what `resolve_pg_type` records by leaving `builtin_oid`
    // empty. Matching `myext.int4` against the real `int4` row here would
    // refuse a pair the catalog may well admit -- `myext.int4` could be a
    // domain over anything -- and then disagree with `resolve_wire`.
    let foreign = forced.builtin_oid.is_none()
        && forced.schema.as_deref().is_some_and(schema_part_is_foreign);
    let Some(row) = row_by_name(&forced.base).filter(|_| !foreign) else {
        // A name the table does not know needs the catalog, and what it turns
        // out to be decides which declared types can carry it: a domain
        // decodes as whatever it is built over, so `type = "int32"` with
        // `pg_type = "public.posint"` is right, while an enum or a composite
        // can only be text. Neither is knowable here, so only the shape the
        // *name* fixes is checked now and `resolve_wire` enforces the rest
        // once the connection is open.
        return match (forced.array, spec.ty) {
            (true, PgFieldType::List) => Ok(()),
            (true, ty) => Err(format!(
                "pg_type = \"{}\" names an array type, so declare type = \"list\" with an \
                 'item', not \"{}\"",
                forced.raw,
                ty.as_str()
            )),
            (false, PgFieldType::List) => Err(format!(
                "type = \"list\" needs an array pg_type such as \"{}[]\", not \"{}\"",
                forced.base, forced.raw
            )),
            (false, _) => Ok(()),
        };
    };

    if forced.array {
        if spec.ty != PgFieldType::List {
            return Err(format!(
                "pg_type = \"{}\" names an array type, so declare type = \"list\" with an 'item', \
                 not \"{}\"",
                forced.raw,
                spec.ty.as_str()
            ));
        }
        let item = spec.item.ok_or_else(|| {
            format!(
                "pg_type = \"{}\" names an array type and type = \"list\" requires 'item'",
                forced.raw
            )
        })?;
        if row.binary.contains(&item) || item == PgFieldType::Utf8 {
            return Ok(());
        }
        return Err(format!(
            "pg_type = \"{}\" has elements of {}, which item = \"{}\" cannot hold; {}",
            forced.raw,
            row.pg,
            item.as_str(),
            options(row)
        ));
    }

    if spec.ty == PgFieldType::List {
        return Err(format!(
            "type = \"list\" needs an array pg_type such as \"{}[]\", not \"{}\"",
            row.pg, forced.raw
        ));
    }

    if row.pg == "numeric"
        && spec.ty == PgFieldType::Decimal128
        && let Some(modifier) = &forced.typmod
    {
        let (precision, scale) = numeric_typmod(modifier);
        let (declared_precision, declared_scale) =
            spec.decimal_params().map_err(|e| e.message().to_string())?;
        if let Some(scale) = scale
            && i16::from(declared_scale) > i16::from(scale)
        {
            return Err(format!(
                "pg_type = \"{}\" keeps {scale} fractional digit(s) but the field declares scale \
                 {declared_scale}; a narrower target would round rather than error",
                forced.raw
            ));
        }
        if let Some(precision) = precision
            && declared_precision > precision
        {
            return Err(format!(
                "pg_type = \"{}\" holds {precision} digit(s) but the field declares precision \
                 {declared_precision}",
                forced.raw
            ));
        }
    }

    if row.binary.contains(&spec.ty) || spec.ty == PgFieldType::Utf8 {
        return Ok(());
    }
    // A sink narrows integer widths with a per-value range check, so a declared
    // `int64` may target `int2`; the source never widens on read.
    if role == Role::Sink && is_integer(spec.ty) && row.binary.iter().copied().any(is_integer) {
        return Ok(());
    }
    Err(format!(
        "pg_type = \"{}\" cannot carry a declared \"{}\"; {}",
        forced.raw,
        spec.ty.as_str(),
        options(row)
    ))
}

fn is_integer(ty: PgFieldType) -> bool {
    matches!(
        ty,
        PgFieldType::Int16 | PgFieldType::Int32 | PgFieldType::Int64
    )
}

/// `(precision, scale)` from a forced `numeric` modifier's text.
///
/// A modifier with no comma is PostgreSQL's own `NUMERIC(p)`, which *is*
/// `NUMERIC(p, 0)` -- not an unconstrained scale. Reading it as "no scale
/// constraint" would let `pg_type = "numeric(12)"` pass beside a declared
/// scale the target rounds away.
pub(crate) fn numeric_typmod(modifier: &str) -> (Option<u8>, Option<i8>) {
    let mut parts = modifier.split(',');
    let precision = parts.next().and_then(|p| p.parse::<u8>().ok());
    let scale = match parts.next() {
        Some(scale) => scale.parse::<i8>().ok(),
        None => Some(0),
    };
    (precision, scale)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn forced(name: &str, ty: PgFieldType, pg_type: &str) -> FieldSpec {
        FieldSpec {
            pg_type: Some(pg_type.to_string()),
            ..spec(name, ty)
        }
    }

    fn list(name: &str, item: PgFieldType) -> FieldSpec {
        FieldSpec {
            item: Some(item),
            ..spec(name, PgFieldType::List)
        }
    }

    /// A built-in column of type `oid`, which is what most cases need.
    fn col(oid: u32) -> ColumnType {
        ColumnType::builtin("c".to_string(), oid)
    }

    /// A column whose type has no static row: an enum, a composite or an
    /// extension type, as the catalog step reports it.
    fn catalog_col(oid: u32, schema: &str, name: &str) -> ColumnType {
        ColumnType {
            name: "c".to_string(),
            oid,
            // Not a domain, so the attribute's own type is this one.
            attribute_oid: oid,
            display: format!("{schema}.{name}"),
            base_display: format!("{schema}.{name}"),
            typmod: -1,
            catalog: Some((schema.to_string(), name.to_string())),
        }
    }

    fn oid_of(name: &str) -> u32 {
        row_by_name(name)
            .unwrap_or_else(|| panic!("{name} should be in the table"))
            .oid
    }

    fn array_oid_of(name: &str) -> u32 {
        row_by_name(name)
            .unwrap_or_else(|| panic!("{name} should be in the table"))
            .array_oid
    }

    // ------------------------------------------------------------ the table

    #[test]
    fn the_table_and_its_indexes_are_sorted_and_consistent() {
        assert!(
            ROWS.windows(2).all(|w| w[0].pg < w[1].pg),
            "ROWS must be sorted by name for the binary search"
        );
        assert!(BY_OID.windows(2).all(|w| w[0].0 < w[1].0));
        assert!(BY_ARRAY_OID.windows(2).all(|w| w[0].0 < w[1].0));
        assert!(ALIASES.windows(2).all(|w| w[0].0 < w[1].0));
        for (oid, index) in BY_OID {
            assert_eq!(row_at(*index).oid, *oid);
        }
        for (array_oid, index) in BY_ARRAY_OID {
            assert_eq!(row_at(*index).array_oid, *array_oid);
        }
        // Complete, not merely correct: a row with a real OID and no index
        // entry would silently degrade that type to "needs the catalog"
        // instead of failing anything.
        assert_eq!(
            BY_OID.len(),
            ROWS.iter().filter(|row| row.oid != 0).count(),
            "every row with an OID needs a BY_OID entry"
        );
        assert_eq!(
            BY_ARRAY_OID.len(),
            ROWS.iter().filter(|row| row.array_oid != 0).count(),
            "every row with an array type needs a BY_ARRAY_OID entry"
        );
        for (_, canonical) in ALIASES {
            assert!(
                row_by_name(canonical).is_some(),
                "alias target {canonical} must be a row"
            );
        }
    }

    /// [`resolve_wire`] looks an array OID up first, so no base type may share
    /// an OID with some other type's array.
    #[test]
    fn no_base_oid_is_also_an_array_oid() {
        for (oid, _) in BY_OID {
            assert!(
                row_by_array_oid(*oid).is_none(),
                "oid {oid} is both a base type and an array type"
            );
        }
    }

    #[test]
    fn every_binary_pair_in_the_table_resolves_to_the_binary_wire() {
        for row in ROWS {
            if row.oid == 0 {
                continue;
            }
            for ty in row.binary {
                assert_eq!(
                    resolve_wire(&spec("c", *ty), &col(row.oid), Role::Source),
                    Ok(Wire::Binary),
                    "{} should fill {} over the binary wire",
                    row.pg,
                    ty.as_str()
                );
            }
        }
    }

    #[test]
    fn a_type_with_no_arrow_counterpart_resolves_to_text() {
        for name in [
            "inet",
            "cidr",
            "macaddr",
            "point",
            "polygon",
            "circle",
            "tsvector",
            "tsquery",
            "xml",
            "jsonpath",
            "pg_lsn",
            "txid_snapshot",
            "int4range",
            "nummultirange",
            "regclass",
            "timetz",
            "varbit",
            "char",
            "numeric",
        ] {
            let oid = oid_of(name);
            assert_eq!(
                resolve_wire(&spec("c", PgFieldType::Utf8), &col(oid), Role::Source),
                Ok(Wire::Text),
                "{name} should be readable as text with no pg_type"
            );
        }
    }

    #[test]
    fn integer_widths_are_not_interchangeable() {
        for (ty, name) in [
            (PgFieldType::Int32, "int8"),
            (PgFieldType::Int64, "int4"),
            (PgFieldType::TimestampMicros, "timestamptz"),
            (PgFieldType::TimestampMicrosUtc, "timestamp"),
            (PgFieldType::Float32, "float8"),
        ] {
            let reason = resolve_wire(&spec("c", ty), &col(oid_of(name)), Role::Source)
                .expect_err("widths and time zones are not interchangeable");
            assert!(reason.contains(name), "{reason}");
        }
    }

    #[test]
    fn text_over_a_non_text_default_needs_an_explicit_pg_type() {
        let reason = resolve_wire(
            &spec("c", PgFieldType::Utf8),
            &col(oid_of("int8")),
            Role::Source,
        )
        .expect_err("reading an int8 as text must be deliberate");
        assert!(reason.contains("int8"), "{reason}");
        assert_eq!(
            resolve_wire(
                &forced("c", PgFieldType::Utf8, "int8"),
                &col(oid_of("int8")),
                Role::Source
            ),
            Ok(Wire::Text)
        );
    }

    #[test]
    fn a_forced_type_must_equal_the_server_type() {
        let reason = resolve_wire(
            &forced("c", PgFieldType::Int32, "int4"),
            &col(oid_of("int8")),
            Role::Source,
        )
        .expect_err("the assertion must fail");
        assert!(reason.contains("int4"), "{reason}");
        assert!(reason.contains("int8"), "{reason}");
    }

    #[test]
    fn an_array_column_needs_a_list_declaration() {
        let reason = resolve_wire(
            &spec("c", PgFieldType::Utf8),
            &col(array_oid_of("text")),
            Role::Source,
        )
        .expect_err("an array is not a scalar");
        assert!(reason.contains("text[]"), "{reason}");
        assert!(reason.contains("list"), "{reason}");

        assert_eq!(
            resolve_wire(
                &list("c", PgFieldType::Utf8),
                &col(array_oid_of("text")),
                Role::Source
            ),
            Ok(Wire::Binary)
        );
        assert_eq!(
            resolve_wire(
                &list("c", PgFieldType::Int32),
                &col(array_oid_of("int4")),
                Role::Source
            ),
            Ok(Wire::Binary)
        );
    }

    /// `item = "utf8"` follows the same intent rule as a scalar `utf8`: free
    /// when the element type maps to text anyway, deliberate otherwise.
    #[test]
    fn a_list_of_text_needs_an_explicit_pg_type_over_a_non_text_element() {
        // `inet` already maps to `utf8`, so reading its elements as text is
        // not a reinterpretation.
        assert_eq!(
            resolve_wire(
                &list("c", PgFieldType::Utf8),
                &col(array_oid_of("inet")),
                Role::Source
            ),
            Ok(Wire::Text)
        );

        // `int4` does not, so it takes `pg_type` naming the array.
        let reason = resolve_wire(
            &list("c", PgFieldType::Utf8),
            &col(array_oid_of("int4")),
            Role::Source,
        )
        .expect_err("reading int4 elements as text must be deliberate");
        assert!(reason.contains("int4"), "{reason}");
        assert!(reason.contains("pg_type"), "{reason}");

        let mut forced_list = list("c", PgFieldType::Utf8);
        forced_list.pg_type = Some("int4[]".to_string());
        assert_eq!(
            resolve_wire(&forced_list, &col(array_oid_of("int4")), Role::Source),
            Ok(Wire::Text)
        );
        // Both array spellings name the same type.
        forced_list.pg_type = Some("_int4".to_string());
        assert_eq!(
            resolve_wire(&forced_list, &col(array_oid_of("int4")), Role::Source),
            Ok(Wire::Text)
        );
    }

    #[test]
    fn an_element_type_the_array_cannot_hold_is_rejected() {
        let reason = resolve_wire(
            &list("c", PgFieldType::Boolean),
            &col(array_oid_of("int4")),
            Role::Source,
        )
        .expect_err("a bool item cannot hold an int4 element");
        assert!(reason.contains("int4"), "{reason}");
    }

    #[test]
    fn a_list_declared_over_a_scalar_column_is_rejected() {
        let reason = resolve_wire(
            &list("c", PgFieldType::Int32),
            &col(oid_of("int4")),
            Role::Source,
        )
        .expect_err("int4 is not an array type");
        assert!(reason.contains("not an array type"), "{reason}");
    }

    #[test]
    fn a_type_with_no_static_oid_needs_a_pg_type() {
        // What the catalog step reports for an enum: its own OID, plus the
        // `schema.typname` a forced `pg_type` is asserted against.
        let mood = catalog_col(99_999, "public", "mood");

        let reason = resolve_wire(&spec("c", PgFieldType::Utf8), &mood, Role::Source)
            .expect_err("an enum cannot be decoded blind");
        assert!(reason.contains("pg_type"), "{reason}");

        assert_eq!(
            resolve_wire(
                &forced("c", PgFieldType::Utf8, "public.mood"),
                &mood,
                Role::Source
            ),
            Ok(Wire::Text)
        );
        // The unqualified spelling resolves through the search path.
        assert_eq!(
            resolve_wire(&forced("c", PgFieldType::Utf8, "mood"), &mood, Role::Source),
            Ok(Wire::Text)
        );
        let reason = resolve_wire(
            &forced("c", PgFieldType::Int64, "public.mood"),
            &mood,
            Role::Source,
        )
        .expect_err("only text can carry an enum");
        assert!(reason.contains("utf8"), "{reason}");
    }

    /// The identity assertion is against the catalog's own `schema.typname`,
    /// so a `pg_type` naming a different type is refused even though both are
    /// types the table has no row for.
    #[test]
    fn a_forced_non_builtin_name_must_match_the_catalog() {
        let mood = catalog_col(99_999, "public", "mood");
        for (spelling, fragment) in [("public.moood", "moood"), ("other.mood", "other.mood")] {
            let reason = resolve_wire(
                &forced("c", PgFieldType::Utf8, spelling),
                &mood,
                Role::Source,
            )
            .expect_err(spelling);
            assert!(reason.contains(fragment), "{spelling}: {reason}");
            assert!(reason.contains("public.mood"), "{spelling}: {reason}");
        }
    }

    /// A built-in column with a non-built-in `pg_type` is a mismatch, not a
    /// free pass: the catalog step proved the column's type is built in.
    #[test]
    fn a_non_builtin_pg_type_over_a_builtin_column_is_refused() {
        let reason = resolve_wire(
            &forced("c", PgFieldType::Utf8, "public.mood"),
            &col(oid_of("text")),
            Role::Source,
        )
        .expect_err("text is not an enum");
        assert!(reason.contains("not a built-in type"), "{reason}");
    }

    #[test]
    fn type_name_renders_base_types_arrays_and_unknown_oids() {
        assert_eq!(type_name(oid_of("timestamptz")), "timestamptz");
        assert_eq!(type_name(array_oid_of("text")), "text[]");
        assert_eq!(type_name(99_999), "oid 99999");
    }

    /// The sink casts a staged value to this spelling, so the quoting is
    /// load-bearing: an unquoted `char` is `character(1)` and an unquoted
    /// `bit` is `bit(1)`, either of which would silently resize the value the
    /// cast is only meant to retype.
    #[test]
    fn an_unconstrained_type_name_is_a_quoted_pg_catalog_name() {
        assert_eq!(
            unconstrained_type_name(oid_of("char")),
            "pg_catalog.\"char\""
        );
        assert_eq!(unconstrained_type_name(oid_of("bit")), "pg_catalog.\"bit\"");
        assert_eq!(
            unconstrained_type_name(oid_of("varchar")),
            "pg_catalog.\"varchar\""
        );
        assert_eq!(
            unconstrained_type_name(array_oid_of("text")),
            "pg_catalog.\"text\"[]"
        );
    }

    // ------------------------------------------------------- validate_columns

    #[test]
    fn missing_and_mismatched_columns_are_reported_together() {
        let declared = [
            spec("id", PgFieldType::Int64),
            spec("label", PgFieldType::Int32),
            spec("gone", PgFieldType::Utf8),
        ];
        let actual = vec![
            ColumnType::builtin("id".to_string(), oid_of("int8")),
            ColumnType::builtin("label".to_string(), oid_of("text")),
        ];
        let err = validate_columns("PostgresSource", Role::Source, &declared, &actual).unwrap_err();
        assert_eq!(err.category(), "configuration");
        let message = err.message();
        assert!(message.contains("'gone'"), "{message}");
        assert!(message.contains("'label'"), "{message}");
        assert!(message.contains("text"), "{message}");
        assert!(!message.contains("'id'"), "{message}");
    }

    #[test]
    fn numeric_into_a_non_decimal_field_suggests_the_alternatives() {
        let declared = [spec("amount", PgFieldType::Float64)];
        let actual = vec![ColumnType::builtin("amount".to_string(), oid_of("numeric"))];
        let err = validate_columns("PostgresSink", Role::Sink, &declared, &actual).unwrap_err();
        let message = err.message();
        assert!(message.contains("decimal128"), "{message}");
        // The advice a user would grep for, not the SQL that implements it.
        assert!(message.contains("type = \"utf8\""), "{message}");
        assert!(message.contains("numeric"), "{message}");
    }

    #[test]
    fn undeclared_server_columns_are_ignored() {
        let declared = [spec("id", PgFieldType::Int64)];
        let actual = vec![
            ColumnType::builtin("id".to_string(), oid_of("int8")),
            ColumnType::builtin("extra".to_string(), oid_of("text")),
        ];
        validate_columns("PostgresSource", Role::Source, &declared, &actual).unwrap();
    }

    // ------------------------------------------------------------- resolver

    #[test]
    fn canonical_names_resolve_to_their_oid() {
        for name in ["int4", "timestamptz", "bpchar", "varbit", "jsonb", "point"] {
            let resolved = resolve_pg_type(name).expect("canonical name");
            assert_eq!(resolved.base, name);
            assert_eq!(resolved.builtin_oid, Some(oid_of(name)));
            assert!(!resolved.array);
            assert_eq!(resolved.typmod, None);
        }
    }

    #[test]
    fn sql_aliases_resolve_to_their_canonical_type() {
        for (alias, canonical) in [
            ("int", "int4"),
            ("INTEGER", "int4"),
            ("bigint", "int8"),
            ("smallint", "int2"),
            ("double precision", "float8"),
            ("real", "float4"),
            ("decimal", "numeric"),
            ("boolean", "bool"),
            ("character varying", "varchar"),
            ("character", "bpchar"),
            ("char", "bpchar"),
            ("timestamp with time zone", "timestamptz"),
            ("timestamp without time zone", "timestamp"),
            ("time with time zone", "timetz"),
            ("bit varying", "varbit"),
            ("bigserial", "int8"),
        ] {
            let resolved = resolve_pg_type(alias).unwrap_or_else(|e| panic!("{alias}: {e}"));
            assert_eq!(resolved.base, canonical, "{alias}");
            assert_eq!(resolved.builtin_oid, Some(oid_of(canonical)), "{alias}");
        }
    }

    #[test]
    fn whitespace_in_a_multi_word_alias_is_collapsed() {
        let resolved = resolve_pg_type("  timestamp   with  time zone ").expect("collapsed");
        assert_eq!(resolved.base, "timestamptz");
    }

    #[test]
    fn both_array_spellings_resolve_to_the_array_oid() {
        for spelling in ["text[]", "_text", "TEXT[]"] {
            let resolved = resolve_pg_type(spelling).unwrap_or_else(|e| panic!("{spelling}: {e}"));
            assert!(resolved.array, "{spelling}");
            assert_eq!(resolved.base, "text");
            assert_eq!(
                resolved.builtin_oid,
                Some(array_oid_of("text")),
                "{spelling}"
            );
        }
        let resolved = resolve_pg_type("int[]").expect("alias plus array");
        assert_eq!(resolved.builtin_oid, Some(array_oid_of("int4")));
    }

    #[test]
    fn a_modifier_is_captured_and_not_evaluated() {
        let resolved = resolve_pg_type("numeric(12,2)").expect("modifier");
        assert_eq!(resolved.base, "numeric");
        assert_eq!(resolved.typmod.as_deref(), Some("12,2"));
        assert_eq!(resolved.builtin_oid, Some(oid_of("numeric")));

        let resolved = resolve_pg_type("varchar(64)").expect("modifier");
        assert_eq!(resolved.typmod.as_deref(), Some("64"));

        let resolved = resolve_pg_type("numeric(12, 2)[]").expect("modifier and array");
        assert_eq!(resolved.typmod.as_deref(), Some("12,2"));
        assert!(resolved.array);
    }

    #[test]
    fn a_quoted_char_is_not_bpchar() {
        let quoted = resolve_pg_type("\"char\"").expect("quoted");
        assert_eq!(quoted.base, "char");
        assert_eq!(quoted.builtin_oid, Some(oid_of("char")));

        let unquoted = resolve_pg_type("char").expect("unquoted");
        assert_eq!(unquoted.base, "bpchar");
        assert_ne!(quoted.builtin_oid, unquoted.builtin_oid);
    }

    #[test]
    fn a_schema_qualified_name_needs_the_catalog() {
        let resolved = resolve_pg_type("public.mood").expect("qualified");
        assert_eq!(resolved.base, "mood");
        assert_eq!(resolved.builtin_oid, None);

        // pg_catalog is where the built-ins live, so it still resolves.
        let resolved = resolve_pg_type("pg_catalog.int4").expect("qualified built-in");
        assert_eq!(resolved.builtin_oid, Some(oid_of("int4")));

        // A built-in name in another schema is a different type.
        let resolved = resolve_pg_type("myext.int4").expect("shadowed name");
        assert_eq!(resolved.builtin_oid, None);
    }

    /// A quoted name after a schema keeps the case the catalog stores, which
    /// is what `resolve_wire` compares against `pg_type.typname`.
    #[test]
    fn a_quoted_name_after_a_schema_keeps_its_case() {
        let resolved = resolve_pg_type("public.\"Mood\"").expect("qualified quoted name");
        assert_eq!(resolved.schema.as_deref(), Some("public"));
        assert_eq!(resolved.base, "Mood");
        assert_eq!(resolved.builtin_oid, None);
    }

    #[test]
    fn a_quoted_schema_keeps_its_case() {
        let resolved = resolve_pg_type("\"MySchema\".mood").expect("quoted schema");
        assert_eq!(resolved.schema.as_deref(), Some("MySchema"));
        assert_eq!(resolved.base, "mood");
    }

    #[test]
    fn a_qualified_quoted_char_is_still_the_built_in() {
        let resolved = resolve_pg_type("pg_catalog.\"char\"").expect("qualified quoted built-in");
        assert_eq!(resolved.base, "char");
        assert_eq!(resolved.builtin_oid, Some(oid_of("char")));
    }

    #[test]
    fn an_unterminated_quote_after_a_schema_is_rejected() {
        let reason = resolve_pg_type("public.\"mood").expect_err("unterminated");
        assert!(reason.contains("unterminated quote"), "{reason}");
    }

    /// `format_type` puts the modifier before the trailing words, so the
    /// spelling the server itself prints has to resolve to the same type as
    /// the bare alias.
    #[test]
    fn a_modifier_before_the_trailing_words_resolves_the_alias() {
        for (spelling, canonical, modifier) in [
            ("timestamp(3) with time zone", "timestamptz", "3"),
            ("time(0) without time zone", "time", "0"),
            ("bit varying(8)", "varbit", "8"),
        ] {
            let resolved = resolve_pg_type(spelling).unwrap_or_else(|e| panic!("{spelling}: {e}"));
            assert_eq!(resolved.base, canonical, "{spelling}");
            assert_eq!(resolved.typmod.as_deref(), Some(modifier), "{spelling}");
            assert_eq!(resolved.builtin_oid, Some(oid_of(canonical)), "{spelling}");
        }
    }

    #[test]
    fn two_modifier_groups_are_rejected() {
        let reason = resolve_pg_type("timestamp(3) with time zone(6)")
            .expect_err("a type carries one modifier");
        assert!(reason.contains("more than one modifier"), "{reason}");
    }

    #[test]
    fn an_extension_type_parses_but_needs_the_catalog() {
        let resolved = resolve_pg_type("citext").expect("extension type");
        assert_eq!(resolved.base, "citext");
        assert_eq!(resolved.builtin_oid, None);
    }

    #[test]
    fn a_multi_dimensional_name_is_rejected() {
        for spelling in ["int4[][]", "_int4[]", "text [] []"] {
            let reason = resolve_pg_type(spelling)
                .expect_err("Arrow's List is one-dimensional")
                .to_lowercase();
            assert!(reason.contains("one-dimensional"), "{spelling}: {reason}");
        }
    }

    #[test]
    fn a_malformed_name_is_rejected() {
        for spelling in [
            "",
            "   ",
            "numeric(x)",
            "numeric(1,2,3)",
            "numeric()",
            "a.b.c",
            ".int4",
            "int4)",
        ] {
            resolve_pg_type(spelling).expect_err(spelling);
        }
    }

    #[test]
    fn a_builtin_without_an_array_type_rejects_the_array_spelling() {
        // `unknown` has no array type in the catalog.
        let reason = resolve_pg_type("unknown[]").expect_err("no array type");
        assert!(reason.contains("no array type"), "{reason}");
    }

    // -------------------------------------------------------- validate_forced

    #[test]
    fn a_forced_type_the_declared_type_cannot_carry_is_rejected() {
        let field = forced("c", PgFieldType::Float64, "float4");
        let forced_ref = resolve_pg_type("float4").expect("float4");
        let reason = validate_forced(&field, &forced_ref, Role::Sink)
            .expect_err("float64 into float4 loses precision");
        assert!(reason.contains("float32"), "{reason}");
    }

    #[test]
    fn a_sink_may_narrow_an_integer_but_a_source_may_not() {
        let field = forced("c", PgFieldType::Int64, "int2");
        let forced_ref = resolve_pg_type("int2").expect("int2");
        validate_forced(&field, &forced_ref, Role::Sink).expect("a sink range-checks per value");
        let reason = validate_forced(&field, &forced_ref, Role::Source)
            .expect_err("a source never widens on read");
        assert!(reason.contains("int16"), "{reason}");
    }

    /// The two checks must agree on every pair whose type name the static
    /// table can resolve: a pair `validate_forced` admits at load time has to
    /// survive `resolve_wire` when the connection opens, or the config passes
    /// `saci-service validate` and then fails on the first flush.
    #[test]
    fn the_load_time_and_connect_time_checks_agree_on_a_static_forced_pair() {
        let list_of = |item, name| FieldSpec {
            item: Some(item),
            ..forced("c", PgFieldType::List, name)
        };
        let cases = [
            // A deliberate sink narrowing, admitted by both.
            (forced("c", PgFieldType::Int64, "int2"), col(oid_of("int2"))),
            // A widening the table does not offer, refused by both.
            (
                forced("c", PgFieldType::Float64, "float4"),
                col(oid_of("float4")),
            ),
            // An array name against a scalar declaration, refused by both.
            (
                forced("c", PgFieldType::Utf8, "text[]"),
                col(oid_of("text")),
            ),
            // A list over an element the table admits, taken by both.
            (
                list_of(PgFieldType::Int32, "int4[]"),
                col(array_oid_of("int4")),
            ),
            // A forced modifier, which neither half evaluates.
            (
                FieldSpec {
                    precision: Some(12),
                    scale: Some(2),
                    ..forced("c", PgFieldType::Decimal128, "numeric(12,2)")
                },
                col(oid_of("numeric")),
            ),
        ];

        for (field, column) in cases {
            let reference = field
                .forced_pg_type()
                .expect("the spelling parses")
                .expect("the field carries a pg_type");
            let load = validate_forced(&field, &reference, Role::Sink);
            let connect = resolve_wire(&field, &column, Role::Sink);
            assert_eq!(
                load.is_ok(),
                connect.is_ok(),
                "pg_type = {:?} with type = {:?}: load said {load:?}, connect said {connect:?}",
                reference.raw,
                field.ty.as_str()
            );
        }
    }

    /// A name qualified with another schema is a *different* type from the
    /// built-in of the same name, so the load-time check must defer to the
    /// catalog rather than validate against that built-in's row.
    #[test]
    fn a_foreign_schema_name_is_not_matched_against_the_built_in_of_that_name() {
        // `myext.int4` may be a domain over anything, so pairing it with
        // float64 is only knowable once the catalog is read.
        let field = forced("c", PgFieldType::Float64, "myext.int4");
        let reference = resolve_pg_type("myext.int4").expect("qualified");
        validate_forced(&field, &reference, Role::Sink)
            .expect("a foreign-schema name defers to the catalog");

        // The unqualified built-in is still settled at load time.
        let field = forced("c", PgFieldType::Float64, "int4");
        let reference = resolve_pg_type("int4").expect("built-in");
        validate_forced(&field, &reference, Role::Sink)
            .expect_err("int4 cannot carry a declared float64");
    }

    /// Without an explicit `pg_type` a width mismatch stays the configuration
    /// error it has always been, on both halves. This is what keeps
    /// `tests/sink.rs`'s `int64`-over-`integer` refusal true.
    #[test]
    fn an_unforced_width_mismatch_is_refused_on_a_sink_too() {
        for role in [Role::Source, Role::Sink] {
            let reason = resolve_wire(&spec("seq", PgFieldType::Int64), &col(oid_of("int4")), role)
                .expect_err("narrowing must be deliberate");
            assert!(reason.contains("int4"), "{reason}");
        }
    }

    #[test]
    fn a_forced_numeric_modifier_narrower_than_the_declared_scale_is_rejected() {
        let field = FieldSpec {
            precision: Some(18),
            scale: Some(4),
            ..forced("amount", PgFieldType::Decimal128, "numeric(12,2)")
        };
        let forced_ref = resolve_pg_type("numeric(12,2)").expect("numeric");
        let reason = validate_forced(&field, &forced_ref, Role::Sink)
            .expect_err("a narrower target would round");
        assert!(reason.contains("scale"), "{reason}");

        let field = FieldSpec {
            precision: Some(10),
            scale: Some(2),
            ..field
        };
        validate_forced(&field, &forced_ref, Role::Sink).expect("scale and precision both fit");
    }

    /// PostgreSQL's `NUMERIC(p)` *is* `NUMERIC(p, 0)`, so a comma-less
    /// modifier constrains the scale to zero rather than leaving it open.
    #[test]
    fn a_comma_less_numeric_modifier_means_scale_zero() {
        assert_eq!(numeric_typmod("12"), (Some(12), Some(0)));
        assert_eq!(numeric_typmod("12,2"), (Some(12), Some(2)));
        assert_eq!(numeric_typmod("10,-2"), (Some(10), Some(-2)));

        let field = FieldSpec {
            precision: Some(12),
            scale: Some(4),
            ..forced("amount", PgFieldType::Decimal128, "numeric(12)")
        };
        let forced_ref = resolve_pg_type("numeric(12)").expect("numeric(12)");
        let reason = validate_forced(&field, &forced_ref, Role::Sink)
            .expect_err("numeric(12) keeps no fractional digit");
        assert!(reason.contains("scale"), "{reason}");

        let field = FieldSpec {
            scale: Some(0),
            ..field
        };
        validate_forced(&field, &forced_ref, Role::Sink).expect("scale 0 fits numeric(12)");
    }

    #[test]
    fn an_array_pg_type_needs_a_list_declaration_and_the_reverse() {
        let scalar_over_array = forced("c", PgFieldType::Utf8, "text[]");
        let array_ref = resolve_pg_type("text[]").expect("array");
        let reason = validate_forced(&scalar_over_array, &array_ref, Role::Source)
            .expect_err("an array pg_type needs type=list");
        assert!(reason.contains("list"), "{reason}");

        let list_over_scalar = FieldSpec {
            pg_type: Some("text".to_string()),
            ..list("c", PgFieldType::Utf8)
        };
        let scalar_ref = resolve_pg_type("text").expect("scalar");
        let reason = validate_forced(&list_over_scalar, &scalar_ref, Role::Source)
            .expect_err("a list needs an array pg_type");
        assert!(reason.contains("text[]"), "{reason}");
    }

    /// A name the table does not know could be an enum (text only) or a domain
    /// (decoded as its base type), so the declared-type pairing is settled at
    /// connect time and load time checks only the array shape.
    #[test]
    fn a_non_builtin_forced_type_defers_its_pairing_to_connect_time() {
        let enum_ref = resolve_pg_type("public.mood").expect("enum");
        for ty in [PgFieldType::Utf8, PgFieldType::Int64] {
            validate_forced(&forced("c", ty, "public.mood"), &enum_ref, Role::Source)
                .expect("the catalog decides, not the name");
        }
        // `type = "int32" pg_type = "public.posint"` is the domain case that
        // has to survive load time: a domain over `int4` decodes as `int32`.
        let domain_ref = resolve_pg_type("public.posint").expect("domain");
        validate_forced(
            &forced("c", PgFieldType::Int32, "public.posint"),
            &domain_ref,
            Role::Source,
        )
        .expect("a domain decodes as its base type");

        // The shape the name itself fixes is still checked here.
        let array_ref = resolve_pg_type("public.mood[]").expect("enum array");
        let reason = validate_forced(
            &forced("c", PgFieldType::Utf8, "public.mood[]"),
            &array_ref,
            Role::Source,
        )
        .expect_err("an array name needs a list declaration");
        assert!(reason.contains("list"), "{reason}");

        let mut field = list("c", PgFieldType::Utf8);
        field.pg_type = Some("public.mood[]".to_string());
        validate_forced(&field, &array_ref, Role::Source).expect("a list of text carries it");
    }
}
