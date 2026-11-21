use std::any::TypeId;

use crate::component::Component;

use super::field::{FieldAccess, FieldRef};

/// Metadata describing a [`System`](super::System)'s field-level data access.
///
/// Declare reads and writes at field granularity with [`reads`](Self::reads)
/// and [`writes`](Self::writes), or for every field of a component with
/// [`reads_all`](Self::reads_all) and [`writes_all`](Self::writes_all). The
/// `_named` forms take the component and field as strings, for a component
/// whose schema exists only at runtime.
///
/// Resource access is keyed by `TypeId`, because resources are Rust singletons
/// rather than Arrow columns.
///
/// # Example
///
/// ```rust
/// use saci::SystemMeta;
///
/// struct MyResource;
///
/// let meta = SystemMeta::new("enrichment")
///     .reads_named("Order", "id")
///     .writes_named("Order", "total")
///     .reads_resource::<MyResource>();
///
/// assert_eq!(meta.name(), "enrichment");
/// ```
#[derive(Debug, Clone, Default)]
pub struct SystemMeta {
    /// Human-readable system name used in diagnostics and scheduling.
    pub(crate) name: &'static str,
    /// Specific fields this system reads (shared access).
    pub(crate) reads: Vec<FieldAccess>,
    /// Specific fields this system writes (exclusive access).
    pub(crate) writes: Vec<FieldAccess>,
    /// Whole-component reads: expand to all fields of the named component
    /// at validation time.
    pub(crate) reads_components: Vec<&'static str>,
    /// Whole-component writes: expand to all fields of the named component
    /// at validation time.
    pub(crate) writes_components: Vec<&'static str>,
    /// Resource types this system reads (shared access, TypeId-keyed).
    pub(crate) reads_resources: Vec<TypeId>,
    /// Resource types this system writes (exclusive access, TypeId-keyed).
    pub(crate) writes_resources: Vec<TypeId>,
}

impl SystemMeta {
    /// Create a new `SystemMeta` with the given name and empty access sets.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            ..Self::default()
        }
    }

    /// The system's name, used in diagnostics and scheduling.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The fields declared with [`reads`](Self::reads) and [`reads_named`](Self::reads_named).
    pub fn read_fields(&self) -> &[FieldAccess] {
        &self.reads
    }

    /// The fields declared with [`writes`](Self::writes) and [`writes_named`](Self::writes_named).
    pub fn write_fields(&self) -> &[FieldAccess] {
        &self.writes
    }

    /// The component names declared with [`reads_all`](Self::reads_all) and
    /// [`reads_all_named`](Self::reads_all_named).
    pub fn read_components(&self) -> &[&'static str] {
        &self.reads_components
    }

    /// The component names declared with [`writes_all`](Self::writes_all) and
    /// [`writes_all_named`](Self::writes_all_named).
    pub fn write_components(&self) -> &[&'static str] {
        &self.writes_components
    }

    /// The resource types declared with [`reads_resource`](Self::reads_resource).
    pub fn read_resources(&self) -> &[TypeId] {
        &self.reads_resources
    }

    /// The resource types declared with [`writes_resource`](Self::writes_resource).
    pub fn write_resources(&self) -> &[TypeId] {
        &self.writes_resources
    }

    /// Declare that this system reads one field of `C`.
    pub fn reads<C: Component>(self, f: FieldRef<C>) -> Self {
        self.reads_named(C::name(), f.name())
    }

    /// Declare that this system writes one field of `C`.
    pub fn writes<C: Component>(self, f: FieldRef<C>) -> Self {
        self.writes_named(C::name(), f.name())
    }

    /// Declare that this system reads every field of `C`.
    pub fn reads_all<C: Component>(self) -> Self {
        self.reads_all_named(C::name())
    }

    /// Declare that this system writes every field of `C`.
    pub fn writes_all<C: Component>(self) -> Self {
        self.writes_all_named(C::name())
    }

    /// Declare that this system reads `field` within the component named
    /// `component`.
    pub fn reads_named(mut self, component: &'static str, field: &'static str) -> Self {
        self.reads.push(FieldAccess { component, field });
        self
    }

    /// Declare that this system writes `field` within the component named
    /// `component`.
    pub fn writes_named(mut self, component: &'static str, field: &'static str) -> Self {
        self.writes.push(FieldAccess { component, field });
        self
    }

    /// Declare that this system reads every field of the component named
    /// `component`. Resolved to one access per field during pipeline
    /// validation.
    pub fn reads_all_named(mut self, component: &'static str) -> Self {
        self.reads_components.push(component);
        self
    }

    /// Declare that this system writes every field of the component named
    /// `component`.
    pub fn writes_all_named(mut self, component: &'static str) -> Self {
        self.writes_components.push(component);
        self
    }

    /// Declare that this system reads resource type `R`.
    pub fn reads_resource<R: 'static>(mut self) -> Self {
        self.reads_resources.push(TypeId::of::<R>());
        self
    }

    /// Declare that this system writes resource type `R`.
    pub fn writes_resource<R: 'static>(mut self) -> Self {
        self.writes_resources.push(TypeId::of::<R>());
        self
    }
}
