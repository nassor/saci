//! The error type author-written transforms and folds return.

/// The error type for processor-authored functions.
///
/// A message and nothing else. `?` works on anything printable because of the
/// blanket [`From`] impl below, so an [`ArrowError`](arrow_schema::ArrowError),
/// a [`SaciError`](saci_core::SaciError), a `String`, a `&str` from
/// `.ok_or("...")?`, or any third-party error propagates without a `map_err`
/// closure:
///
/// ```ignore
/// #[transform(component = Order)]
/// pub fn settle(row: &mut Order) -> saci_processor::Result<()> {
///     let tier = TIERS.get(row.review_tier as usize).ok_or("unknown review tier")?;
///     row.settlement = tier.parse::<Settlement>()?.to_string();
///     Ok(())
/// }
/// ```
///
/// # Why it implements neither `Display` nor `std::error::Error`
///
/// The blanket `impl<E: Display> From<E> for Error` is what makes `?` work on
/// every foreign error. It is legal only while `Error` itself is not
/// `Display`: the moment it were, that impl would overlap the reflexive
/// `impl<T> From<T> for T` in core. The message is read back with
/// [`message`](Self::message) or [`into_message`](Self::into_message) instead,
/// and the macro-generated system body converts it into a
/// `SaciError::SystemExecution`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(String);

impl Error {
    /// Build an error from a message.
    pub fn new<S: Into<String>>(message: S) -> Self {
        Self(message.into())
    }

    /// Borrow the message.
    pub fn message(&self) -> &str {
        &self.0
    }

    /// Take the message, consuming the error.
    pub fn into_message(self) -> String {
        self.0
    }
}

impl<E: std::fmt::Display> From<E> for Error {
    fn from(err: E) -> Self {
        Self(err.to_string())
    }
}

/// `Result` alias with [`Error`] as the default error type.
///
/// The return type of every `#[transform]` and `#[fold]` function.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;
    use saci_core::SaciError;

    #[test]
    fn a_saci_error_propagates_without_a_map_err() {
        fn call() -> Result<()> {
            let upstream: Result<(), SaciError> = Err(SaciError::configuration("bad config"));
            upstream?;
            Ok(())
        }
        assert_eq!(
            call().unwrap_err().message(),
            "Configuration error: bad config"
        );
    }

    #[test]
    fn an_arrow_error_propagates_without_a_map_err() {
        fn call() -> Result<()> {
            let upstream: Result<(), arrow_schema::ArrowError> = Err(
                arrow_schema::ArrowError::SchemaError("no such field".into()),
            );
            upstream?;
            Ok(())
        }
        assert!(call().unwrap_err().message().contains("no such field"));
    }

    #[test]
    fn ok_or_on_a_string_slice_propagates() {
        fn call() -> Result<u8> {
            let missing: Option<u8> = None;
            Ok(missing.ok_or("value missing")?)
        }
        assert_eq!(call().unwrap_err().message(), "value missing");
    }

    #[test]
    fn propagating_an_error_of_the_same_type_uses_the_reflexive_conversion() {
        fn inner() -> Result<()> {
            Err(Error::new("inner failed"))
        }
        fn outer() -> Result<()> {
            inner()?;
            Ok(())
        }
        assert_eq!(outer().unwrap_err().into_message(), "inner failed");
    }
}
