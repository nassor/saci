//! Rendering one batch column into per-row strings.
//!
//! Three sink keys need the same rendering: `subject_field`, every value in
//! `header_fields`, and `message_id_field`.

use arrow_array::{Array, RecordBatch};
use arrow_cast::display::{ArrayFormatter, FormatOptions};

use saci_core::error::SaciError;

/// Render `field` for every row, `None` for a null cell.
///
/// `key` names the config key that asked for the column, so an error points at
/// the configuration rather than at the batch.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when the batch has no such column, or when
/// Arrow cannot format its type.
pub(crate) fn render_column(
    batch: &RecordBatch,
    field: &str,
    what: &str,
    key: &str,
) -> Result<Vec<Option<String>>, SaciError> {
    let column = batch.column_by_name(field).ok_or_else(|| {
        SaciError::generic(format!(
            "{what}: {key} '{field}' is not a column in the batch"
        ))
    })?;
    let formatter = ArrayFormatter::try_new(column.as_ref(), &FormatOptions::default())
        .map_err(|e| SaciError::generic(format!("{what}: formatting {key} '{field}': {e}")))?;
    Ok((0..column.len())
        .map(|i| {
            if column.is_null(i) {
                None
            } else {
                Some(formatter.value(i).to_string())
            }
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ],
        )
        .expect("valid batch")
    }

    #[test]
    fn a_missing_column_names_the_key_that_asked_for_it() {
        let err = render_column(&batch(), "nope", "NatsSink", "subject_field")
            .expect_err("there is no 'nope' column");
        assert_eq!(err.category(), "generic");
        assert!(err.message().contains("subject_field 'nope'"), "got: {err}");
    }

    #[test]
    fn a_null_cell_renders_none() {
        let rendered = render_column(&batch(), "name", "NatsSink", "subject_field")
            .expect("the column exists");
        assert_eq!(rendered, vec![Some("a".to_string()), None]);
    }

    #[test]
    fn a_non_string_column_renders_through_arrow() {
        let rendered = render_column(&batch(), "id", "NatsSink", "message_id_field")
            .expect("the column exists");
        assert_eq!(rendered, vec![Some("1".to_string()), Some("2".to_string())]);
    }
}
