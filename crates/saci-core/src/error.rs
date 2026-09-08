//! Error types for the SACI engine.
//!
//! [`SaciError`] is the single error enum and [`SaciResult`] the matching
//! `Result` alias. Every variant implements `std::error::Error`, so `?`
//! propagates them into other error types.
//!
//! | Error type | When it occurs | Remedy |
//! |------------|----------------|--------|
//! | `SystemExecution` | System processing fails | Check the system logic |
//! | `ComponentNotFound` | Entity missing a component | Add the component first |
//! | `EntityNotFound` | Entity does not exist or is dead | Verify entity IDs |
//! | `ResourceNotFound` | A global resource is missing | Register the resource |
//! | `Store` | Store operations fail | Check store state and keys |
//! | `Scheduler` | Scheduler orchestration fails | Verify system registration |
//! | `Configuration` | Invalid system or pipeline config | Check the parameters |
//! | `RetryExhausted` | All retries failed | Raise the budget or fix the cause |
//! | `Generic` | Everything else | Read the error message |

/// Error type for SACI workflows.
///
/// Each variant carries enough context to say what happened; the table in the
/// [module docs](self) maps every variant to its cause and remedy.
///
/// `From` impls exist for `&str`, `String`, `std::io::Error`,
/// `arrow_schema::ArrowError`, and `Box<dyn std::error::Error>`, so `?` works
/// against those sources directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaciError {
    /// Failure inside a system's own processing logic.
    SystemExecution(String),

    /// A system accessed a component the entity does not have.
    ComponentNotFound { entity_id: u32, type_name: String },

    /// The referenced entity does not exist or has been despawned.
    EntityNotFound(u32),

    /// A required global resource was never registered.
    ResourceNotFound(String),

    /// Store operation failed: missing key, type mismatch, backend error.
    Store(String),

    /// Scheduler orchestration failed: unregistered system, invalid routing.
    Scheduler(String),

    /// Invalid system or pipeline configuration.
    Configuration(String),

    /// Every configured retry attempt has been used up.
    RetryExhausted {
        /// The source error from the final failed attempt.
        source: Box<SaciError>,
        /// The total number of attempts that were made.
        attempts: usize,
    },

    /// General-purpose error for cases outside the other categories.
    Generic(String),

    /// Distributed coordination failure: partitioning, consensus, networking.
    #[cfg(feature = "distributed")]
    Distributed(String),

    /// A batch lease expired before processing completed. Another instance may
    /// reclaim the batch.
    #[cfg(feature = "distributed")]
    LeaseExpired {
        /// The batch whose lease expired.
        batch_id: u64,
    },
}

impl SaciError {
    /// Create a new system execution error
    pub fn system_execution<S: Into<String>>(msg: S) -> Self {
        SaciError::SystemExecution(msg.into())
    }

    /// Create a new component not found error
    pub fn component_not_found(entity_id: u32, type_name: &str) -> Self {
        SaciError::ComponentNotFound {
            entity_id,
            type_name: type_name.to_string(),
        }
    }

    /// Create a new entity not found error
    pub fn entity_not_found(entity_id: u32) -> Self {
        SaciError::EntityNotFound(entity_id)
    }

    /// Create a new resource not found error
    pub fn resource_not_found<S: Into<String>>(name: S) -> Self {
        SaciError::ResourceNotFound(name.into())
    }

    /// Create a new store error
    pub fn store<S: Into<String>>(msg: S) -> Self {
        SaciError::Store(msg.into())
    }

    /// Create a new scheduler error
    pub fn scheduler<S: Into<String>>(msg: S) -> Self {
        SaciError::Scheduler(msg.into())
    }

    /// Create a new configuration error
    pub fn configuration<S: Into<String>>(msg: S) -> Self {
        SaciError::Configuration(msg.into())
    }

    /// Create a new retry exhausted error
    pub fn retry_exhausted(source: SaciError, attempts: usize) -> Self {
        SaciError::RetryExhausted {
            source: Box::new(source),
            attempts,
        }
    }

    /// Create a new generic error
    pub fn generic<S: Into<String>>(msg: S) -> Self {
        SaciError::Generic(msg.into())
    }

    /// Create a new distributed error
    #[cfg(feature = "distributed")]
    pub fn distributed<S: Into<String>>(msg: S) -> Self {
        SaciError::Distributed(msg.into())
    }

    /// Create a new lease expired error
    #[cfg(feature = "distributed")]
    pub fn lease_expired(batch_id: u64) -> Self {
        SaciError::LeaseExpired { batch_id }
    }

    /// Get the error message as a string
    pub fn message(&self) -> String {
        match self {
            SaciError::SystemExecution(msg) => msg.clone(),
            SaciError::ComponentNotFound { type_name, .. } => type_name.clone(),
            SaciError::EntityNotFound(id) => id.to_string(),
            SaciError::ResourceNotFound(name) => name.clone(),
            SaciError::Store(msg) => msg.clone(),
            SaciError::Scheduler(msg) => msg.clone(),
            SaciError::Configuration(msg) => msg.clone(),
            SaciError::RetryExhausted { source, attempts } => {
                format!("after {attempts} attempt(s): {source}")
            }
            SaciError::Generic(msg) => msg.clone(),
            #[cfg(feature = "distributed")]
            SaciError::Distributed(msg) => msg.clone(),
            #[cfg(feature = "distributed")]
            SaciError::LeaseExpired { batch_id } => {
                format!("lease expired for batch {batch_id}")
            }
        }
    }

    /// Get the error category as a string
    pub fn category(&self) -> &'static str {
        match self {
            SaciError::SystemExecution(_) => "system_execution",
            SaciError::ComponentNotFound { .. } => "component_not_found",
            SaciError::EntityNotFound(_) => "entity_not_found",
            SaciError::ResourceNotFound(_) => "resource_not_found",
            SaciError::Store(_) => "store",
            SaciError::Scheduler(_) => "scheduler",
            SaciError::Configuration(_) => "configuration",
            SaciError::RetryExhausted { .. } => "retry_exhausted",
            SaciError::Generic(_) => "generic",
            #[cfg(feature = "distributed")]
            SaciError::Distributed(_) => "distributed",
            #[cfg(feature = "distributed")]
            SaciError::LeaseExpired { .. } => "lease_expired",
        }
    }
}

impl std::fmt::Display for SaciError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SaciError::SystemExecution(msg) => write!(f, "System execution error: {msg}"),
            SaciError::ComponentNotFound {
                entity_id,
                type_name,
            } => write!(
                f,
                "Component not found: entity {entity_id} missing {type_name}"
            ),
            SaciError::EntityNotFound(id) => write!(f, "Entity not found: {id}"),
            SaciError::ResourceNotFound(name) => write!(f, "Resource not found: {name}"),
            SaciError::Store(msg) => write!(f, "Store error: {msg}"),
            SaciError::Scheduler(msg) => write!(f, "Scheduler error: {msg}"),
            SaciError::Configuration(msg) => write!(f, "Configuration error: {msg}"),
            SaciError::RetryExhausted { source, attempts } => {
                write!(f, "Retry exhausted after {attempts} attempt(s): {source}")
            }
            SaciError::Generic(msg) => write!(f, "Error: {msg}"),
            #[cfg(feature = "distributed")]
            SaciError::Distributed(msg) => write!(f, "Distributed error: {msg}"),
            #[cfg(feature = "distributed")]
            SaciError::LeaseExpired { batch_id } => {
                write!(f, "Lease expired for batch {batch_id}")
            }
        }
    }
}

impl std::error::Error for SaciError {}

impl From<Box<dyn std::error::Error + Send + Sync>> for SaciError {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        SaciError::Generic(err.to_string())
    }
}

impl From<&str> for SaciError {
    fn from(err: &str) -> Self {
        SaciError::Generic(err.to_string())
    }
}

impl From<String> for SaciError {
    fn from(err: String) -> Self {
        SaciError::Generic(err)
    }
}

impl From<std::io::Error> for SaciError {
    fn from(err: std::io::Error) -> Self {
        SaciError::Generic(format!("IO error: {err}"))
    }
}

/// Arrow's own error type, so `?` propagates a schema, cast or IPC failure
/// straight into a [`SaciError`].
///
/// Every Arrow call a system makes — `RecordBatch::try_new`, `Schema::index_of`,
/// a cast, an IPC read — returns this, and wrapping each one in
/// `.map_err(|e| SaciError::generic(format!(...)))` is the same closure written
/// once per call site.
impl From<arrow_schema::ArrowError> for SaciError {
    fn from(err: arrow_schema::ArrowError) -> Self {
        SaciError::Generic(format!("Arrow error: {err}"))
    }
}

/// `Result` alias with [`SaciError`] as the error type.
pub type SaciResult<TState> = Result<TState, SaciError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_execution_error_creation() {
        let error = SaciError::system_execution("system failed");
        assert_eq!(error.message(), "system failed");
        assert_eq!(error.category(), "system_execution");
    }

    #[test]
    fn test_system_execution_display() {
        let error = SaciError::SystemExecution("bad state".to_string());
        assert_eq!(format!("{error}"), "System execution error: bad state");
    }

    #[test]
    fn test_component_not_found_error_creation() {
        let error = SaciError::component_not_found(42, "Health");
        assert_eq!(error.category(), "component_not_found");
        assert_eq!(
            format!("{error}"),
            "Component not found: entity 42 missing Health"
        );
    }

    #[test]
    fn test_component_not_found_message_contains_type_name() {
        let error = SaciError::component_not_found(1, "Transform");
        assert!(error.message().contains("Transform"));
    }

    #[test]
    fn test_entity_not_found_error_creation() {
        let error = SaciError::entity_not_found(99);
        assert_eq!(error.category(), "entity_not_found");
        assert_eq!(format!("{error}"), "Entity not found: 99");
    }

    #[test]
    fn test_entity_not_found_message_contains_id() {
        let error = SaciError::entity_not_found(7);
        assert!(error.message().contains("7"));
    }

    #[test]
    fn test_resource_not_found_error_creation() {
        let error = SaciError::resource_not_found("GameConfig");
        assert_eq!(error.category(), "resource_not_found");
        assert_eq!(format!("{error}"), "Resource not found: GameConfig");
    }

    #[test]
    fn test_scheduler_error_creation() {
        let error = SaciError::scheduler("missing system");
        assert_eq!(error.message(), "missing system");
        assert_eq!(error.category(), "scheduler");
    }

    #[test]
    fn test_scheduler_display() {
        let error = SaciError::Scheduler("cycle detected".to_string());
        assert_eq!(format!("{error}"), "Scheduler error: cycle detected");
    }

    #[test]
    fn test_error_conversions() {
        let error1: SaciError = "Test error".into();
        let error2: SaciError = "Test error".to_string().into();

        match (&error1, &error2) {
            (SaciError::Generic(msg1), SaciError::Generic(msg2)) => {
                assert_eq!(msg1, msg2);
            }
            _ => panic!("Expected Generic errors"),
        }
    }

    #[test]
    fn test_error_categories() {
        assert_eq!(
            SaciError::SystemExecution("".to_string()).category(),
            "system_execution"
        );
        assert_eq!(
            SaciError::ComponentNotFound {
                entity_id: 0,
                type_name: "".to_string()
            }
            .category(),
            "component_not_found"
        );
        assert_eq!(SaciError::EntityNotFound(0).category(), "entity_not_found");
        assert_eq!(
            SaciError::ResourceNotFound("".to_string()).category(),
            "resource_not_found"
        );
        assert_eq!(SaciError::store("").category(), "store");
        assert_eq!(SaciError::Scheduler("".to_string()).category(), "scheduler");
        assert_eq!(SaciError::configuration("").category(), "configuration");
        assert_eq!(
            SaciError::retry_exhausted(SaciError::generic(""), 0).category(),
            "retry_exhausted"
        );
        assert_eq!(SaciError::generic("").category(), "generic");
    }

    #[test]
    fn test_io_error_maps_to_generic() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let saci_err: SaciError = io_err.into();
        assert_eq!(saci_err.category(), "generic");
        assert!(saci_err.message().contains("IO error"));
        assert!(saci_err.message().contains("file missing"));
    }

    #[test]
    fn test_partial_eq_system_execution_same_message() {
        let a = SaciError::SystemExecution("oops".to_string());
        let b = SaciError::SystemExecution("oops".to_string());
        assert_eq!(a, b);
    }

    #[test]
    fn test_partial_eq_system_execution_different_message() {
        let a = SaciError::SystemExecution("a".to_string());
        let b = SaciError::SystemExecution("b".to_string());
        assert_ne!(a, b);
    }

    #[test]
    fn test_partial_eq_component_not_found() {
        let a = SaciError::ComponentNotFound {
            entity_id: 1,
            type_name: "Health".to_string(),
        };
        let b = SaciError::ComponentNotFound {
            entity_id: 1,
            type_name: "Health".to_string(),
        };
        let c = SaciError::ComponentNotFound {
            entity_id: 2,
            type_name: "Health".to_string(),
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_partial_eq_entity_not_found() {
        let a = SaciError::EntityNotFound(5);
        let b = SaciError::EntityNotFound(5);
        let c = SaciError::EntityNotFound(6);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_partial_eq_different_variants() {
        let a = SaciError::SystemExecution("msg".to_string());
        let b = SaciError::Scheduler("msg".to_string());
        assert_ne!(a, b);
    }

    #[test]
    fn test_partial_eq_resource_not_found() {
        let a = SaciError::ResourceNotFound("Config".to_string());
        let b = SaciError::ResourceNotFound("Config".to_string());
        let c = SaciError::ResourceNotFound("Other".to_string());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn an_arrow_error_converts_into_a_generic_saci_error() {
        let err: SaciError =
            arrow_schema::ArrowError::SchemaError("field 'settlement' not found".to_string())
                .into();
        assert_eq!(err.category(), "generic");
        assert!(
            err.message().contains("field 'settlement' not found"),
            "the Arrow message must survive: {err}"
        );
    }

    #[test]
    fn an_arrow_error_propagates_with_the_question_mark_operator() {
        fn index_of(name: &str) -> SaciResult<usize> {
            let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int64,
                false,
            )]);
            Ok(schema.index_of(name)?)
        }
        assert_eq!(index_of("id").unwrap(), 0);
        assert!(
            index_of("missing")
                .unwrap_err()
                .message()
                .contains("missing")
        );
    }
}
