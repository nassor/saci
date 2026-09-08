//! The two factory traits a connector crate implements.

use saci_config::ConfigValue;
use saci_core::error::SaciError;
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;

use crate::context::ConnectorContext;

/// The reason [`SourceFactory::rebuildable`] and [`SinkFactory::rebuildable`]
/// give until a connector overrides them.
pub const REBUILD_UNDECLARED: &str =
    "the connector does not declare that building it a second time is sound";

/// Factory for building a [`Source`] from config.
///
/// Implement this trait for each source type you want to expose to the
/// configuration file. The `type_name` must match the `type` property of a
/// `source` node.
pub trait SourceFactory: Send + Sync + 'static {
    /// The type name that appears in config as `type="<name>"`.
    fn type_name(&self) -> &'static str;

    /// Build a source instance from the user-supplied config value.
    ///
    /// `ctx` carries the transformer registry, so a connector that moves bytes
    /// resolves its byte format through
    /// [`ConnectorContext::transformer`] instead of owning a format list.
    ///
    /// # Errors
    ///
    /// Return [`SaciError::Configuration`] if required config fields are
    /// missing or have invalid values.
    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError>;

    /// Whether building this connector a second time in the same process,
    /// after the first instance has been dropped, is sound for `config`: it
    /// loses nothing the first instance had accepted, and it re-delivers
    /// nothing the first instance had already delivered, beyond what the
    /// connector's own at-least-once contract already allows.
    ///
    /// The host heals a failing connector by rebuilding it, and heals by
    /// default, so the default answer is `Err(REBUILD_UNDECLARED)`: a
    /// connector opts in with `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns the reason a second build is unsound, in one phrase the host
    /// reports verbatim when a `heal` block asks for what this connector
    /// cannot give.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Err(REBUILD_UNDECLARED)
    }
}

/// Factory for building a [`Sink`] from config.
///
/// Implement this trait for each sink type you want to expose to the
/// configuration file. The `type_name` must match the `type` property of a
/// `sink` node.
pub trait SinkFactory: Send + Sync + 'static {
    /// The type name that appears in config as `type="<name>"`.
    fn type_name(&self) -> &'static str;

    /// Build a sink instance from the user-supplied config value.
    ///
    /// `ctx` carries the transformer registry, so a connector that moves bytes
    /// resolves its byte format through
    /// [`ConnectorContext::transformer`] instead of owning a format list.
    ///
    /// # Errors
    ///
    /// Return [`SaciError::Configuration`] if required config fields are
    /// missing or have invalid values.
    fn build(
        &self,
        config: &ConfigValue,
        ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError>;

    /// Whether building this connector a second time in the same process,
    /// after the first instance has been dropped, is sound for `config`: it
    /// loses nothing the first instance had accepted, and it re-delivers
    /// nothing the first instance had already delivered, beyond what the
    /// connector's own at-least-once contract already allows.
    ///
    /// The host heals a failing connector by rebuilding it, and heals by
    /// default, so the default answer is `Err(REBUILD_UNDECLARED)`: a
    /// connector opts in with `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns the reason a second build is unsound, in one phrase the host
    /// reports verbatim when a `heal` block asks for what this connector
    /// cannot give.
    fn rebuildable(&self, _config: &ConfigValue) -> Result<(), &'static str> {
        Err(REBUILD_UNDECLARED)
    }
}
