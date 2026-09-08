use crate::error::SaciError;

use super::Dataset;

impl Dataset {
    /// Append every component `dst` declares and `self` also holds into `dst`.
    ///
    /// This is how one processor's output reaches the next one's input: the
    /// receiving dataset decides which components move, so a component the
    /// downstream processor does not declare is not copied.
    ///
    /// Returns the number of rows appended.
    ///
    /// # Errors
    ///
    /// Propagates [`Dataset::append_record_batch`]'s schema-mismatch error.
    pub fn forward_into(&self, dst: &mut Dataset) -> Result<usize, SaciError> {
        // Collected first so the shared borrow of `dst` ends before the loop
        // below borrows it mutably; the registry already hands out `&'static
        // str`, so nothing here is interned twice.
        let names: Vec<&'static str> = dst.schemas().iter().map(|(name, _)| *name).collect();

        let mut appended = 0usize;
        for name in names {
            if !self.schemas().contains(name) {
                continue;
            }
            let batch = self
                .batch_for(name)
                .expect("component present in schemas() must have a batch")
                .clone();
            appended += batch.num_rows();
            dst.append_record_batch(name, batch)?;
        }
        Ok(appended)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::component::Component;
    use crate::dataset::Dataset;

    #[derive(Serialize, Deserialize)]
    struct Order {
        id: i64,
    }
    impl Component for Order {
        fn name() -> &'static str {
            "Order"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Audit {
        note: String,
    }
    impl Component for Audit {
        fn name() -> &'static str {
            "Audit"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new("note", DataType::Utf8, false)]))
        }
    }

    #[test]
    fn forward_into_copies_only_components_the_destination_declares() {
        let mut src = Dataset::new();
        src.register_component::<Order>().unwrap();
        src.register_component::<Audit>().unwrap();
        src.append::<Order>(&[Order { id: 1 }, Order { id: 2 }])
            .unwrap();
        src.append::<Audit>(&[
            Audit {
                note: "a".to_string(),
            },
            Audit {
                note: "b".to_string(),
            },
        ])
        .unwrap();

        let mut dst = Dataset::new();
        dst.register_component::<Order>().unwrap();

        let appended = src.forward_into(&mut dst).unwrap();
        assert_eq!(appended, 2);
        assert_eq!(dst.rows(), 2);
        assert!(
            dst.batch_for("Audit").is_none(),
            "a component the destination never registered must not appear"
        );
    }

    #[test]
    fn forward_into_is_a_no_op_when_nothing_overlaps() {
        let mut src = Dataset::new();
        src.register_component::<Audit>().unwrap();
        src.append::<Audit>(&[Audit {
            note: "x".to_string(),
        }])
        .unwrap();

        let mut dst = Dataset::new();
        dst.register_component::<Order>().unwrap();

        let appended = src.forward_into(&mut dst).unwrap();
        assert_eq!(appended, 0);
        assert_eq!(dst.rows(), 0);
    }

    #[test]
    fn forward_into_propagates_schema_mismatch() {
        let mut src = Dataset::new();
        src.register_component::<Order>().unwrap();
        src.append::<Order>(&[Order { id: 1 }]).unwrap();

        let mut dst = Dataset::new();
        // Registered under the same name but an incompatible schema.
        dst.register_raw_component(
            "Order",
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
        );

        let err = src.forward_into(&mut dst).unwrap_err();
        assert_eq!(err.category(), "generic");
    }
}
