use super::Dataset;

impl Dataset {
    /// Insert (or replace) a global resource singleton.
    pub fn insert_resource<R: Send + Sync + 'static>(&mut self, resource: R) {
        self.resources.insert(resource);
    }

    /// Return a shared reference to resource `R`, or `None` if not present.
    pub fn get_resource<R: 'static>(&self) -> Option<&R> {
        self.resources.get::<R>()
    }

    /// Return a mutable reference to resource `R`, or `None` if not present.
    pub fn get_resource_mut<R: 'static>(&mut self) -> Option<&mut R> {
        self.resources.get_mut::<R>()
    }
}

#[cfg(test)]
mod tests {
    use crate::dataset::Dataset;

    struct Cfg {
        threshold: f64,
    }

    #[test]
    fn test_insert_and_get() {
        let mut ds = Dataset::new();
        ds.insert_resource(Cfg { threshold: 0.9 });
        assert!((ds.get_resource::<Cfg>().unwrap().threshold - 0.9).abs() < 1e-9);
    }

    #[test]
    fn test_get_mut() {
        let mut ds = Dataset::new();
        ds.insert_resource(Cfg { threshold: 0.5 });
        ds.get_resource_mut::<Cfg>().unwrap().threshold = 1.0;
        assert!((ds.get_resource::<Cfg>().unwrap().threshold - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_missing_returns_none() {
        let ds = Dataset::new();
        assert!(ds.get_resource::<Cfg>().is_none());
    }

    #[test]
    fn test_replace_resource() {
        let mut ds = Dataset::new();
        ds.insert_resource(Cfg { threshold: 0.1 });
        ds.insert_resource(Cfg { threshold: 0.9 });
        assert!((ds.get_resource::<Cfg>().unwrap().threshold - 0.9).abs() < 1e-9);
    }
}
