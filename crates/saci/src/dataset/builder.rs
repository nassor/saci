use crate::component::Component;

use super::{Dataset, DatasetBuilder};

impl Dataset {
    /// Create a [`DatasetBuilder`] for fluent dataset construction.
    pub fn builder() -> DatasetBuilder {
        DatasetBuilder(Dataset::new())
    }
}

impl DatasetBuilder {
    /// Register component `C` in the dataset being built.
    ///
    /// # Panics
    ///
    /// Panics if `C` was already registered.
    pub fn with<C: Component>(mut self) -> Self {
        self.0
            .register_component::<C>()
            .expect("component already registered");
        self
    }

    /// Insert a resource into the dataset being built.
    pub fn with_resource<R: Send + Sync + 'static>(mut self, r: R) -> Self {
        self.0.insert_resource(r);
        self
    }

    /// Consume the builder and return the configured [`Dataset`].
    pub fn build(self) -> Dataset {
        self.0
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
    struct BldPrice {
        value: f64,
    }

    impl Component for BldPrice {
        fn name() -> &'static str {
            "BldPrice"
        }
        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Float64,
                false,
            )]))
        }
    }

    struct Limit(u32);

    #[test]
    fn test_builder_basic() {
        let mut ds = Dataset::builder().with::<BldPrice>().build();
        ds.append::<BldPrice>(&[BldPrice { value: 1.0 }]).unwrap();
        assert_eq!(ds.rows(), 1);
    }

    #[test]
    fn test_builder_with_resource() {
        let ds = Dataset::builder().with_resource(Limit(42)).build();
        assert_eq!(ds.get_resource::<Limit>().unwrap().0, 42);
    }
}
