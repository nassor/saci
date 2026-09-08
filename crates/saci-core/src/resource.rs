//! Resource container for [`Pipeline`](super::pipeline::Pipeline).
//!
//! Resources are global singletons keyed by [`TypeId`]. Unlike components they
//! are not columnar: each stays a plain Rust value behind `Box<dyn Any>`.

use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

/// A type-erased map of global resource singletons.
///
/// Insert any `Send + Sync + 'static` value; retrieve it by type parameter.
/// Inserting a type a second time replaces the previous value.
#[derive(Debug, Default)]
pub struct ResourceMap {
    inner: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl ResourceMap {
    /// Create an empty resource map.
    pub fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Insert (or replace) a resource.
    pub fn insert<R: Send + Sync + 'static>(&mut self, resource: R) {
        self.inner.insert(TypeId::of::<R>(), Box::new(resource));
    }

    /// Return a shared reference to the resource of type `R`, if present.
    pub fn get<R: 'static>(&self) -> Option<&R> {
        self.inner
            .get(&TypeId::of::<R>())
            .and_then(|b| b.downcast_ref::<R>())
    }

    /// Return a mutable reference to the resource of type `R`, if present.
    pub fn get_mut<R: 'static>(&mut self) -> Option<&mut R> {
        self.inner
            .get_mut(&TypeId::of::<R>())
            .and_then(|b| b.downcast_mut::<R>())
    }

    /// Remove and return the resource of type `R`, if present.
    pub fn remove<R: 'static>(&mut self) -> Option<R> {
        self.inner
            .remove(&TypeId::of::<R>())
            .and_then(|b| b.downcast::<R>().ok())
            .map(|b| *b)
    }

    /// Return `true` if a resource of type `R` is registered.
    pub fn contains<R: 'static>(&self) -> bool {
        self.inner.contains_key(&TypeId::of::<R>())
    }

    /// Number of resources stored.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` when no resources are stored.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Remove all resources.
    pub fn clear(&mut self) {
        self.inner.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Config {
        max_retries: u32,
    }

    struct Counter(i64);

    #[test]
    fn test_insert_and_get() {
        let mut map = ResourceMap::new();
        map.insert(Config { max_retries: 3 });
        let cfg = map.get::<Config>().unwrap();
        assert_eq!(cfg.max_retries, 3);
    }

    #[test]
    fn test_get_mut() {
        let mut map = ResourceMap::new();
        map.insert(Counter(0));
        map.get_mut::<Counter>().unwrap().0 += 10;
        assert_eq!(map.get::<Counter>().unwrap().0, 10);
    }

    #[test]
    fn test_insert_replaces() {
        let mut map = ResourceMap::new();
        map.insert(Config { max_retries: 1 });
        map.insert(Config { max_retries: 5 });
        assert_eq!(map.get::<Config>().unwrap().max_retries, 5);
    }

    #[test]
    fn test_get_missing_returns_none() {
        let map = ResourceMap::new();
        assert!(map.get::<Config>().is_none());
    }

    #[test]
    fn test_contains() {
        let mut map = ResourceMap::new();
        assert!(!map.contains::<Config>());
        map.insert(Config { max_retries: 0 });
        assert!(map.contains::<Config>());
    }

    #[test]
    fn test_remove() {
        let mut map = ResourceMap::new();
        map.insert(Counter(42));
        let removed = map.remove::<Counter>().unwrap();
        assert_eq!(removed.0, 42);
        assert!(map.get::<Counter>().is_none());
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut map = ResourceMap::new();
        assert!(map.is_empty());
        map.insert(Counter(0));
        assert_eq!(map.len(), 1);
        map.insert(Config { max_retries: 0 });
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_clear() {
        let mut map = ResourceMap::new();
        map.insert(Counter(1));
        map.insert(Config { max_retries: 1 });
        map.clear();
        assert!(map.is_empty());
    }

    #[test]
    fn test_multiple_types_independent() {
        let mut map = ResourceMap::new();
        map.insert(Counter(7));
        map.insert(Config { max_retries: 99 });
        assert_eq!(map.get::<Counter>().unwrap().0, 7);
        assert_eq!(map.get::<Config>().unwrap().max_retries, 99);
    }
}
