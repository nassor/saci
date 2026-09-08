//! The `turso::Value` ↔ Arrow coercion.
//!
//! `turso::Value` is a five-way union of `Null`, `Integer(i64)`, `Real(f64)`,
//! `Text(String)` and `Blob(Vec<u8>)`, so a column's declared [`TursoFieldType`]
//! decides how each value is read, and a value that does not fit is a loud error
//! naming the column, the row inside the batch, and the value's own kind.
//! Nothing is widened silently on read.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Decimal128Builder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float64Array, Int64Array,
    StringArray,
};

use crate::config::{FieldSpec, TursoFieldType};

/// One decoded scalar, tagged by the declared type it satisfies.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Scalar {
    /// A `Bool` column.
    Bool(bool),
    /// An `Int64` column.
    Int64(i64),
    /// A `Float64` column.
    Float64(f64),
    /// A `Utf8` column.
    Utf8(String),
    /// A `Binary` column.
    Binary(Vec<u8>),
    /// A `Decimal128` column, already scaled.
    Decimal128(i128),
}

/// The name of a value's variant, for error messages.
pub(crate) fn variant_name(value: &turso::Value) -> &'static str {
    match value {
        turso::Value::Null => "null",
        turso::Value::Integer(_) => "integer",
        turso::Value::Real(_) => "real",
        turso::Value::Text(_) => "text",
        turso::Value::Blob(_) => "blob",
    }
}

/// Read one engine value as the declared type, or refuse it.
///
/// # Errors
///
/// Returns the reason `value` does not satisfy `spec`, without a connector
/// prefix: the caller adds one.
pub(crate) fn decode_value(
    spec: &FieldSpec,
    row: usize,
    value: turso::Value,
) -> Result<Option<Scalar>, String> {
    use turso::Value;
    let mismatch = |got: &str| {
        format!(
            "column '{}' row {row}: expected {}, got {got}",
            spec.name,
            spec.ty.as_str()
        )
    };
    match spec.ty {
        TursoFieldType::Int64 => match value {
            Value::Null => Ok(None),
            Value::Integer(i) => Ok(Some(Scalar::Int64(i))),
            other => Err(mismatch(variant_name(&other))),
        },
        TursoFieldType::Float64 => match value {
            Value::Null => Ok(None),
            Value::Real(f) => Ok(Some(Scalar::Float64(f))),
            Value::Integer(i) => Ok(Some(Scalar::Float64(i as f64))),
            other => Err(mismatch(variant_name(&other))),
        },
        TursoFieldType::Utf8 => match value {
            Value::Null => Ok(None),
            Value::Text(s) => Ok(Some(Scalar::Utf8(s))),
            other => Err(mismatch(variant_name(&other))),
        },
        TursoFieldType::Bool => match value {
            Value::Null => Ok(None),
            Value::Integer(0) => Ok(Some(Scalar::Bool(false))),
            Value::Integer(1) => Ok(Some(Scalar::Bool(true))),
            Value::Integer(i) => Err(format!(
                "column '{}' row {row}: bool expects 0 or 1, got {i}",
                spec.name
            )),
            other => Err(mismatch(variant_name(&other))),
        },
        TursoFieldType::Binary => match value {
            Value::Null => Ok(None),
            Value::Blob(b) => Ok(Some(Scalar::Binary(b))),
            other => Err(mismatch(variant_name(&other))),
        },
        TursoFieldType::Decimal128 => {
            let (_, scale) = spec.decimal_params().map_err(|e| e.message())?;
            match value {
                Value::Null => Ok(None),
                Value::Text(s) => parse_decimal(&s, scale)
                    .map(|d| Some(Scalar::Decimal128(d)))
                    .map_err(|e| format!("column '{}' row {row}: {e}", spec.name)),
                Value::Integer(i) => Ok(Some(Scalar::Decimal128(rescale(i, scale)))),
                other => Err(mismatch(variant_name(&other))),
            }
        }
    }
}

/// Encode one row of an Arrow array as an engine value.
///
/// # Errors
///
/// Returns the reason the array does not match `spec`'s declared type.
pub(crate) fn encode_value(
    spec: &FieldSpec,
    array: &ArrayRef,
    row: usize,
) -> Result<turso::Value, String> {
    if array.is_null(row) {
        return Ok(turso::Value::Null);
    }
    let wrong_type = |ty: &str| format!("column '{}': array is not {ty}", spec.name);
    Ok(match spec.ty {
        TursoFieldType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| wrong_type("Int64"))?;
            turso::Value::Integer(array.value(row))
        }
        TursoFieldType::Float64 => {
            let array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| wrong_type("Float64"))?;
            turso::Value::Real(array.value(row))
        }
        TursoFieldType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| wrong_type("Utf8"))?;
            turso::Value::Text(array.value(row).to_string())
        }
        TursoFieldType::Bool => {
            let array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| wrong_type("Boolean"))?;
            turso::Value::Integer(i64::from(array.value(row)))
        }
        TursoFieldType::Binary => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| wrong_type("Binary"))?;
            turso::Value::Blob(array.value(row).to_vec())
        }
        TursoFieldType::Decimal128 => {
            let array = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| wrong_type("Decimal128"))?;
            let (_, scale) = spec.decimal_params().map_err(|e| e.message())?;
            turso::Value::Text(render_decimal(array.value(row), scale))
        }
    })
}

/// Scale an integer value up to the declared `scale`.
pub(crate) fn rescale(value: i64, scale: i8) -> i128 {
    let scale = scale.max(0) as u32;
    i128::from(value) * 10i128.pow(scale)
}

/// Parse a decimal string into its scaled integer form.
///
/// A value carrying more fractional digits than `scale` is refused rather than
/// rounded, and one whose digits overflow `i128` is refused rather than
/// truncated.
///
/// # Errors
///
/// Returns the reason the text is not a decimal of the declared scale.
pub(crate) fn parse_decimal(text: &str, scale: i8) -> Result<i128, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("empty decimal".to_string());
    }
    let (negative, body) = match trimmed.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };
    let (integer_part, fraction_part) = match body.split_once('.') {
        Some((int, frac)) => (int, frac),
        None => (body, ""),
    };
    if integer_part.is_empty() && fraction_part.is_empty() {
        return Err(format!("invalid decimal '{text}'"));
    }
    if !integer_part.chars().all(|c| c.is_ascii_digit())
        || !fraction_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("invalid decimal '{text}'"));
    }
    let scale = scale.max(0) as usize;
    if fraction_part.len() > scale {
        return Err(format!(
            "decimal '{text}' has more fractional digits than the declared scale {scale}"
        ));
    }
    let mut digits = String::with_capacity(integer_part.len() + scale);
    digits.push_str(integer_part);
    digits.push_str(fraction_part);
    for _ in fraction_part.len()..scale {
        digits.push('0');
    }
    if digits.is_empty() {
        digits.push('0');
    }
    let magnitude: i128 = digits
        .parse()
        .map_err(|_| format!("decimal '{text}' does not fit a 128-bit scaled integer"))?;
    Ok(if negative { -magnitude } else { magnitude })
}

/// Render a scaled integer as a fixed-scale decimal string.
pub(crate) fn render_decimal(value: i128, scale: i8) -> String {
    let scale = scale.max(0) as usize;
    if scale == 0 {
        return value.to_string();
    }
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let (integer_part, fraction_part) = if digits.len() > scale {
        let split = digits.len() - scale;
        (digits[..split].to_string(), digits[split..].to_string())
    } else {
        (
            "0".to_string(),
            format!("{:0>width$}", digits, width = scale),
        )
    };
    format!(
        "{}{integer_part}.{fraction_part}",
        if negative { "-" } else { "" }
    )
}

/// One column's Arrow builder, chosen by the declared type.
pub(crate) enum ColBuilder {
    Bool(BooleanBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Decimal128(Decimal128Builder),
}

impl ColBuilder {
    /// A builder producing the declared type.
    ///
    /// # Errors
    ///
    /// Returns [`saci_core::error::SaciError::Configuration`] when a
    /// `decimal128` field's precision/scale are missing or illegal.
    pub(crate) fn new(spec: &FieldSpec) -> Result<Self, saci_core::error::SaciError> {
        Ok(match spec.ty {
            TursoFieldType::Bool => ColBuilder::Bool(BooleanBuilder::new()),
            TursoFieldType::Int64 => ColBuilder::Int64(Int64Builder::new()),
            TursoFieldType::Float64 => ColBuilder::Float64(Float64Builder::new()),
            TursoFieldType::Utf8 => ColBuilder::Utf8(StringBuilder::new()),
            TursoFieldType::Binary => ColBuilder::Binary(BinaryBuilder::new()),
            TursoFieldType::Decimal128 => {
                let (precision, scale) = spec.decimal_params()?;
                ColBuilder::Decimal128(
                    Decimal128Builder::new()
                        .with_precision_and_scale(precision, scale)
                        .map_err(|e| {
                            saci_core::error::SaciError::configuration(format!(
                                "field '{}': {e}",
                                spec.name
                            ))
                        })?,
                )
            }
        })
    }

    /// Append one decoded value, or a null.
    ///
    /// # Errors
    ///
    /// Returns the reason the scalar does not match this builder's type.
    pub(crate) fn append(&mut self, value: Option<Scalar>) -> Result<(), String> {
        match (self, value) {
            (ColBuilder::Bool(builder), None) => builder.append_null(),
            (ColBuilder::Bool(builder), Some(Scalar::Bool(v))) => builder.append_value(v),
            (ColBuilder::Int64(builder), None) => builder.append_null(),
            (ColBuilder::Int64(builder), Some(Scalar::Int64(v))) => builder.append_value(v),
            (ColBuilder::Float64(builder), None) => builder.append_null(),
            (ColBuilder::Float64(builder), Some(Scalar::Float64(v))) => builder.append_value(v),
            (ColBuilder::Utf8(builder), None) => builder.append_null(),
            (ColBuilder::Utf8(builder), Some(Scalar::Utf8(v))) => builder.append_value(v),
            (ColBuilder::Binary(builder), None) => builder.append_null(),
            (ColBuilder::Binary(builder), Some(Scalar::Binary(v))) => builder.append_value(v),
            (ColBuilder::Decimal128(builder), None) => builder.append_null(),
            (ColBuilder::Decimal128(builder), Some(Scalar::Decimal128(v))) => {
                builder.append_value(v)
            }
            _ => return Err("decoded scalar does not match its column builder".to_string()),
        }
        Ok(())
    }

    /// Finish the column into an Arrow array.
    pub(crate) fn finish(&mut self) -> ArrayRef {
        match self {
            ColBuilder::Bool(builder) => Arc::new(builder.finish()),
            ColBuilder::Int64(builder) => Arc::new(builder.finish()),
            ColBuilder::Float64(builder) => Arc::new(builder.finish()),
            ColBuilder::Utf8(builder) => Arc::new(builder.finish()),
            ColBuilder::Binary(builder) => Arc::new(builder.finish()),
            ColBuilder::Decimal128(builder) => Arc::new(builder.finish()),
        }
    }
}
