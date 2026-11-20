use crate::component::Component;

/// A field-level data access declaration: `(component_name, field_name)`.
///
/// [`SystemMeta`](super::SystemMeta) records one per declared field, so two
/// systems writing different fields of the same component can share a
/// pipeline stage.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct FieldAccess {
    /// Name of the component (must match [`Component::name()`](crate::component::Component::name)).
    pub component: &'static str,
    /// Name of the field within that component's Arrow schema.
    pub field: &'static str,
}

/// A typed reference to a named field of component `C`.
///
/// `#[derive(Component)]` declares one per field as an associated constant
/// named after the field in upper case (`Order::AMOUNT`). A hand-written
/// [`Component`] impl declares its own:
///
/// ```rust
/// # use saci::FieldRef;
/// # use saci::Component;
/// # use std::sync::Arc;
/// # use arrow_schema::Schema;
/// # struct Order;
/// # impl Component for Order {
/// #     fn name() -> &'static str { "Order" }
/// #     fn schema() -> Arc<Schema> { Arc::new(Schema::empty()) }
/// # }
/// impl Order {
///     pub const AMOUNT: FieldRef<Order> = FieldRef::new("amount");
/// }
/// assert_eq!(Order::AMOUNT.name(), "amount");
/// assert_eq!(Order::AMOUNT.component(), "Order");
/// ```
pub struct FieldRef<C: Component> {
    field: &'static str,
    _p: std::marker::PhantomData<fn() -> C>,
}

impl<C: Component> FieldRef<C> {
    /// Create a new `FieldRef` for the given field name.
    pub const fn new(field: &'static str) -> Self {
        Self {
            field,
            _p: std::marker::PhantomData,
        }
    }

    /// The field name within the component's Arrow schema.
    pub const fn name(&self) -> &'static str {
        self.field
    }

    /// The name of the component the field belongs to.
    pub fn component(&self) -> &'static str {
        C::name()
    }
}

impl<C: Component> Copy for FieldRef<C> {}

impl<C: Component> Clone for FieldRef<C> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<C: Component> AsRef<str> for FieldRef<C> {
    fn as_ref(&self) -> &str {
        self.field
    }
}

impl<C: Component> std::fmt::Debug for FieldRef<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldRef")
            .field("component", &C::name())
            .field("field", &self.field)
            .finish()
    }
}
