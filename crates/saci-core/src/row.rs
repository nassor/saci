//! Positional row handle into a columnar [`Dataset`](super::dataset::Dataset).
//!
//! A `Row` carries no generation counter. Validity lives in the `__alive`
//! boolean column the dataset maintains. A `Row` obtained before a
//! [`mark_dead`](super::dataset::Dataset::mark_dead) call is still accepted by
//! the API, so the caller must check
//! [`Dataset::is_alive`](super::dataset::Dataset::is_alive).
//!
//! # Example
//!
//! ```rust
//! use saci_core::row::Row;
//!
//! let row = Row::new(42);
//! assert_eq!(row.index(), 42);
//! ```

/// A positional index into a [`Dataset`](super::dataset::Dataset)'s columnar storage.
///
/// Rows are returned by [`Dataset::append`](super::dataset::Dataset::append) and
/// identify a position in every component's `RecordBatch`. The inner `u32` is
/// public so callers can pattern-match and construct rows directly.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Row(pub u32);

impl Row {
    /// Construct a new `Row` from a raw positional index.
    ///
    /// # Example
    ///
    /// ```rust
    /// use saci_core::row::Row;
    /// let r = Row::new(0);
    /// assert_eq!(r.0, 0);
    /// ```
    #[inline]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Return the row's index as a `usize`, suitable for array indexing.
    ///
    /// # Example
    ///
    /// ```rust
    /// use saci_core::row::Row;
    /// let r = Row::new(7);
    /// assert_eq!(r.index(), 7);
    /// ```
    #[inline]
    pub const fn index(self) -> usize {
        self.0 as usize
    }
}

impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Row({})", self.0)
    }
}

impl From<u32> for Row {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl From<Row> for usize {
    fn from(r: Row) -> Self {
        r.index()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_new_and_index() {
        let r = Row::new(5);
        assert_eq!(r.0, 5);
        assert_eq!(r.index(), 5);
    }

    #[test]
    fn test_row_ordering() {
        let a = Row::new(1);
        let b = Row::new(2);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, Row::new(1));
    }

    #[test]
    fn test_row_from_u32() {
        let r: Row = 10u32.into();
        assert_eq!(r.index(), 10);
    }

    #[test]
    fn test_row_into_usize() {
        let r = Row::new(3);
        let u: usize = r.into();
        assert_eq!(u, 3);
    }

    #[test]
    fn test_row_display() {
        assert_eq!(format!("{}", Row::new(42)), "Row(42)");
    }

    #[test]
    fn test_row_copy() {
        let a = Row::new(99);
        let b = a; // copy
        assert_eq!(a, b);
    }
}
