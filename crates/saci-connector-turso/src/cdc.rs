//! Decoding Turso's change-data-capture records.
//!
//! The `cdc` source mode reads the change table the engine writes when a
//! connection has `PRAGMA capture_data_changes_conn` enabled. Each record's
//! `before`/`after` columns are a binary image of the row, which the engine's
//! own `bin_record_json_object(table_columns_json_array(<table>), <blob>)`
//! renders as a JSON object; this module coerces that object's values to the
//! declared Arrow types.
//!
//! Capture is **per connection**: a connection records only the changes made
//! through that same connection. A source therefore only ever sees records some
//! writer opted into, which is why a missing change table is a loud error
//! rather than an empty stream.

use serde_json::Value as Json;

use crate::config::{FieldSpec, TursoFieldType};
use crate::types::{Scalar, parse_decimal, rescale};

/// The operation letter a `change_type` denotes, or `None` for the COMMIT
/// record (`2`), which carries no row.
pub(crate) fn op_of(change_type: i64) -> Option<&'static str> {
    match change_type {
        1 => Some("I"),
        0 => Some("U"),
        -1 => Some("D"),
        _ => None,
    }
}

/// The JSON kind of a value, for error messages.
fn json_kind(value: &Json) -> &'static str {
    match value {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "array",
        Json::Object(_) => "object",
    }
}

/// Read one field of a decoded change image as the declared type.
///
/// A field the image does not carry is null, which is what a DELETE recorded in
/// `id` mode (no image at all) or a column absent from the change mode's capture
/// yields.
///
/// # Errors
///
/// Returns the reason the image value does not satisfy `spec`.
pub(crate) fn decode_cdc_field(
    spec: &FieldSpec,
    row: usize,
    image: &serde_json::Map<String, Json>,
) -> Result<Option<Scalar>, String> {
    let Some(value) = image.get(&spec.name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let mismatch = |value: &Json| {
        format!(
            "column '{}' row {row}: expected {}, got {}",
            spec.name,
            spec.ty.as_str(),
            json_kind(value)
        )
    };
    match spec.ty {
        TursoFieldType::Int64 => match value.as_i64() {
            Some(i) => Ok(Some(Scalar::Int64(i))),
            None => Err(mismatch(value)),
        },
        TursoFieldType::Float64 => match value.as_f64() {
            Some(f) => Ok(Some(Scalar::Float64(f))),
            None => Err(mismatch(value)),
        },
        TursoFieldType::Utf8 => match value.as_str() {
            Some(s) => Ok(Some(Scalar::Utf8(s.to_string()))),
            None => Err(mismatch(value)),
        },
        TursoFieldType::Bool => match value.as_i64() {
            Some(0) => Ok(Some(Scalar::Bool(false))),
            Some(1) => Ok(Some(Scalar::Bool(true))),
            _ => Err(format!(
                "column '{}' row {row}: bool expects 0 or 1, got {}",
                spec.name,
                json_kind(value)
            )),
        },
        TursoFieldType::Binary => match value.as_str() {
            Some(s) => decode_blob(s)
                .map(|b| Some(Scalar::Binary(b)))
                .ok_or_else(|| {
                    format!(
                        "column '{}' row {row}: binary value is not a base64 or x'..' hex string",
                        spec.name
                    )
                }),
            None => Err(mismatch(value)),
        },
        TursoFieldType::Decimal128 => {
            let (_, scale) = spec.decimal_params().map_err(|e| e.message())?;
            match value {
                Json::String(s) => parse_decimal(s, scale)
                    .map(|d| Some(Scalar::Decimal128(d)))
                    .map_err(|e| format!("column '{}' row {row}: {e}", spec.name)),
                Json::Number(n) => n
                    .as_i64()
                    .map(|i| Some(Scalar::Decimal128(rescale(i, scale))))
                    .ok_or_else(|| mismatch(value)),
                _ => Err(mismatch(value)),
            }
        }
    }
}

/// Decode a BLOB's JSON text form back to bytes.
///
/// The engine renders a blob either as an `x'..'` hex literal or as base64; both
/// are accepted so the connector does not depend on which one an engine version
/// chooses.
fn decode_blob(text: &str) -> Option<Vec<u8>> {
    let trimmed = text.trim();
    if let Some(hex) = trimmed
        .strip_prefix("x'")
        .and_then(|rest| rest.strip_suffix('\''))
    {
        return decode_hex(hex);
    }
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(trimmed)
        .ok()
}

/// Decode a run of hex digit pairs.
fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    let bytes = hex.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let (pairs, _) = bytes.as_chunks::<2>();
    let mut out = Vec::with_capacity(pairs.len());
    for pair in pairs {
        let high = (pair[0] as char).to_digit(16)?;
        let low = (pair[1] as char).to_digit(16)?;
        out.push(((high << 4) | low) as u8);
    }
    Some(out)
}
