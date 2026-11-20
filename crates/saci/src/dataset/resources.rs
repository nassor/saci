use crate::error::{SaciError, SaciResult};

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

    /// Resource `R`.
    ///
    /// # Errors
    ///
    /// [`SaciError::ResourceNotFound`] naming `R` when it is absent.
    pub fn resource<R: 'static>(&self) -> SaciResult<&R> {
        self.resources
            .get::<R>()
            .ok_or_else(|| SaciError::resource_not_found(std::any::type_name::<R>()))
    }

    /// Resource `R`, mutably.
    ///
    /// # Errors
    ///
    /// [`SaciError::ResourceNotFound`] naming `R` when it is absent.
    pub fn resource_mut<R: 'static>(&mut self) -> SaciResult<&mut R> {
        self.resources
            .get_mut::<R>()
            .ok_or_else(|| SaciError::resource_not_found(std::any::type_name::<R>()))
    }

    /// Remove resource `R` and return it, or `None` if it was absent.
    pub fn take_resource<R: 'static>(&mut self) -> Option<R> {
        self.resources.remove::<R>()
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
        assert!((ds.resource::<Cfg>().unwrap().threshold - 0.9).abs() < 1e-9);
    }

    #[test]
    fn test_get_mut() {
        let mut ds = Dataset::new();
        ds.insert_resource(Cfg { threshold: 0.5 });
        ds.resource_mut::<Cfg>().unwrap().threshold = 1.0;
        assert!((ds.get_resource::<Cfg>().unwrap().threshold - 1.0).abs() < 1e-9);
    }

    #[test]
    fn test_missing_is_resource_not_found() {
        let ds = Dataset::new();
        assert!(ds.get_resource::<Cfg>().is_none());
        let err = ds.resource::<Cfg>().err().unwrap();
        assert_eq!(err.category(), "resource_not_found");
        assert!(err.to_string().contains("Cfg"), "got: {err}");
    }

    #[test]
    fn test_take_resource_removes_it() {
        let mut ds = Dataset::new();
        ds.insert_resource(Cfg { threshold: 0.3 });
        let taken = ds.take_resource::<Cfg>().unwrap();
        assert!((taken.threshold - 0.3).abs() < 1e-9);
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
