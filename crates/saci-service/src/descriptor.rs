//! Turning a runtime's declared component schemas into a template [`Dataset`].
//!
//! Both pipeline runtimes that live outside this crate's process boundary need
//! this. A WebAssembly processor reports `component-descriptor` records with
//! `arrow-schema-ipc` bytes; a native plugin reports the same pairs as
//! base64 in its JSON manifest. Once decoded they are the same
//! `(name, schema ipc bytes)` list, and both runtimes build their
//! `template_dataset` from it the same way.

use std::sync::Arc;

use arrow_ipc::reader::StreamReader;
use arrow_schema::Schema;
use saci_core::{Dataset, SaciError, SaciResult};

/// Decode an Arrow IPC schema-message into an Arrow [`Schema`].
///
/// The producer writes a `StreamWriter` with no batches, so `StreamReader` reads
/// the schema straight from the stream header.
pub(crate) fn parse_ipc_schema(ipc_bytes: &[u8]) -> SaciResult<Arc<Schema>> {
    StreamReader::try_new(ipc_bytes, None)
        .map(|reader| reader.schema())
        .map_err(|e| SaciError::configuration(format!("component schema parse error: {e}")))
}

/// Build a schema-only [`Dataset`] from a runtime's declared components.
///
/// Every component whose schema parses is registered; one that does not is
/// skipped with a warning, because a runtime that cannot describe one component
/// can still serve the rest, and the load-time IO coverage check reports the
/// missing name with far better context than a parse error here would.
///
/// Callers pass a freshly built list: registration asserts the dataset holds no
/// rows, and this always starts from [`Dataset::new`].
///
/// Names are interned by [`Dataset::register_named_component`], so reloading a
/// runtime reuses one allocation per distinct component name rather than
/// growing memory per load.
pub(crate) fn template_dataset_from<'a>(
    components: impl Iterator<Item = (&'a str, &'a [u8])>,
) -> Dataset {
    let mut dataset = Dataset::new();

    for (name, schema_ipc) in components {
        let schema = match parse_ipc_schema(schema_ipc) {
            Ok(schema) => schema,
            Err(_e) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    component = %name,
                    error = %_e,
                    "template_dataset: schema parse failed, skipping component"
                );
                continue;
            }
        };
        dataset.register_named_component(name, schema);
    }

    dataset
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field};
    use saci_core::sdk::schema_to_ipc_bytes;

    fn ipc_for(fields: Vec<Field>) -> Vec<u8> {
        schema_to_ipc_bytes(&Schema::new(fields)).expect("schema ipc")
    }

    #[test]
    fn a_schema_round_trips_through_ipc() {
        let bytes = ipc_for(vec![Field::new("a", DataType::Int64, false)]);
        let schema = parse_ipc_schema(&bytes).expect("parse");
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "a");
    }

    #[test]
    fn empty_and_garbage_schema_bytes_are_rejected() {
        let empty = parse_ipc_schema(&[]).expect_err("empty must not parse");
        assert!(empty.to_string().contains("schema parse error"));
        parse_ipc_schema(&[0xde, 0xad, 0xbe, 0xef]).expect_err("garbage must not parse");
    }

    #[test]
    fn every_parseable_component_lands_in_the_template() {
        let order = ipc_for(vec![Field::new("id", DataType::Int64, false)]);
        let price = ipc_for(vec![Field::new("value", DataType::Float64, false)]);

        let dataset = template_dataset_from(
            [("Order", order.as_slice()), ("Price", price.as_slice())].into_iter(),
        );

        assert_eq!(dataset.rows(), 0);
        assert!(dataset.schemas().contains("Order"));
        assert!(dataset.schemas().contains("Price"));
        assert_eq!(dataset.schemas().len(), 2);
    }

    #[test]
    fn an_unparseable_component_is_skipped_not_fatal() {
        let good = ipc_for(vec![Field::new("id", DataType::Int64, false)]);

        let dataset =
            template_dataset_from([("Good", good.as_slice()), ("Bad", [].as_slice())].into_iter());

        assert!(dataset.schemas().contains("Good"));
        assert!(
            !dataset.schemas().contains("Bad"),
            "an unparseable schema must not register a component"
        );
        assert_eq!(dataset.schemas().len(), 1);
    }
}
