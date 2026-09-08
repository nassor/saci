use std::any::TypeId;

use crate::component::Component;

use super::field::{FieldAccess, FieldRef};

/// Metadata describing a [`System`](super::System)'s field-level data access.
///
/// Declare reads and writes at field granularity via [`read`](Self::read) and
/// [`write`](Self::write), or at component granularity via
/// [`read_component`](Self::read_component) and
/// [`write_component`](Self::write_component) for systems that touch every
/// field.
///
/// Resource access is keyed by `TypeId`, because resources are Rust singletons
/// rather than Arrow columns.
///
/// # Example
///
/// ```rust
/// #
/// # {
/// use saci_core::system::SystemMeta;
///
/// struct MyResource;
///
/// let meta = SystemMeta::new("enrichment")
///     .read("Order", "id")
///     .write("Order", "total")
///     .read_resource::<MyResource>();
///
/// assert_eq!(meta.name, "enrichment");
/// assert_eq!(meta.reads.len(), 1);
/// assert_eq!(meta.writes.len(), 1);
/// assert_eq!(meta.reads_resources.len(), 1);
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct SystemMeta {
    /// Human-readable system name used in diagnostics and scheduling.
    pub name: &'static str,
    /// Specific fields this system reads (shared access).
    pub reads: Vec<FieldAccess>,
    /// Specific fields this system writes (exclusive access).
    pub writes: Vec<FieldAccess>,
    /// Whole-component reads: expand to all fields of the named component
    /// at validation time. Shortcut for systems that access every field.
    pub reads_components: Vec<&'static str>,
    /// Whole-component writes: expand to all fields of the named component
    /// at validation time.
    pub writes_components: Vec<&'static str>,
    /// Resource types this system reads (shared access, TypeId-keyed).
    pub reads_resources: Vec<TypeId>,
    /// Resource types this system writes (exclusive access, TypeId-keyed).
    pub writes_resources: Vec<TypeId>,
}

impl SystemMeta {
    /// Create a new `SystemMeta` with the given name and empty access sets.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            reads: Vec::new(),
            writes: Vec::new(),
            reads_components: Vec::new(),
            writes_components: Vec::new(),
            reads_resources: Vec::new(),
            writes_resources: Vec::new(),
        }
    }

    /// Declare that this system reads `field` within `component`.
    pub fn read(mut self, component: &'static str, field: &'static str) -> Self {
        self.reads.push(FieldAccess { component, field });
        self
    }

    /// Declare that this system writes `field` within `component`.
    pub fn write(mut self, component: &'static str, field: &'static str) -> Self {
        self.writes.push(FieldAccess { component, field });
        self
    }

    /// Declare that this system reads *all* fields of `component`.
    ///
    /// Equivalent to calling [`read`](Self::read) for every field in the
    /// component's schema. Resolved to concrete `FieldAccess` entries during
    /// pipeline validation.
    pub fn read_component(mut self, component: &'static str) -> Self {
        self.reads_components.push(component);
        self
    }

    /// Declare that this system writes *all* fields of `component`.
    pub fn write_component(mut self, component: &'static str) -> Self {
        self.writes_components.push(component);
        self
    }

    /// Declare that this system reads resource type `R`.
    pub fn read_resource<R: 'static>(mut self) -> Self {
        self.reads_resources.push(TypeId::of::<R>());
        self
    }

    /// Declare that this system writes resource type `R`.
    pub fn write_resource<R: 'static>(mut self) -> Self {
        self.writes_resources.push(TypeId::of::<R>());
        self
    }

    /// Declare a read dependency on a typed field reference.
    ///
    /// Equivalent to `.read(C::name(), f.field)`, with the component name
    /// taken from `C::name()` instead of a raw string.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use saci_core::system::{FieldRef, SystemMeta};
    /// # use saci_core::component::Component;
    /// # use std::sync::Arc;
    /// # use arrow_schema::Schema;
    /// # struct Order;
    /// # impl Component for Order {
    /// #     fn name() -> &'static str { "Order" }
    /// #     fn schema() -> Arc<Schema> { Arc::new(Schema::empty()) }
    /// # }
    /// impl Order {
    ///     pub const TOTAL: FieldRef<Order> = FieldRef::new("total");
    /// }
    ///
    /// let meta = SystemMeta::new("calc").reads(Order::TOTAL);
    /// assert_eq!(meta.reads[0].component, "Order");
    /// assert_eq!(meta.reads[0].field, "total");
    /// ```
    pub fn reads<C: Component>(self, f: FieldRef<C>) -> Self {
        self.read(C::name(), f.field)
    }

    /// Declare a write dependency on a typed field reference.
    ///
    /// Equivalent to `.write(C::name(), f.field)`, with the component name
    /// taken from `C::name()` instead of a raw string.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use saci_core::system::{FieldRef, SystemMeta};
    /// # use saci_core::component::Component;
    /// # use std::sync::Arc;
    /// # use arrow_schema::Schema;
    /// # struct Order;
    /// # impl Component for Order {
    /// #     fn name() -> &'static str { "Order" }
    /// #     fn schema() -> Arc<Schema> { Arc::new(Schema::empty()) }
    /// # }
    /// impl Order {
    ///     pub const TOTAL: FieldRef<Order> = FieldRef::new("total");
    /// }
    ///
    /// let meta = SystemMeta::new("calc").writes(Order::TOTAL);
    /// assert_eq!(meta.writes[0].component, "Order");
    /// assert_eq!(meta.writes[0].field, "total");
    /// ```
    pub fn writes<C: Component>(self, f: FieldRef<C>) -> Self {
        self.write(C::name(), f.field)
    }
}
