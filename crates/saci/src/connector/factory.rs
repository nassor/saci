//! The [`Connector`] trait a connector crate implements.

use std::fmt;

use crate::config::ConfigValue;
use crate::connector::context::ConnectorContext;
use crate::error::{SaciError, SaciResult};
use crate::io::{Sink, Source};

/// The reason [`Connector::rebuild`] gives until a connector overrides it.
pub const REBUILD_UNDECLARED: &str =
    "the connector does not declare that building it a second time is sound";

/// Which end of a workflow a connector instance serves: a `source` node or a
/// `sink` node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// The connector reads data into the workflow.
    Source,
    /// The connector writes data out of the workflow.
    Sink,
}

impl Role {
    /// The word a config uses for this role: `"source"` or `"sink"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Source => "source",
            Role::Sink => "sink",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether the host may build a connector a second time to heal it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rebuild {
    /// A second build is sound.
    Yes,
    /// A second build is unsound, for the reason given in one phrase.
    No(&'static str),
}

impl Rebuild {
    /// True for [`Rebuild::Yes`].
    pub fn is_yes(self) -> bool {
        matches!(self, Rebuild::Yes)
    }

    /// True for [`Rebuild::No`].
    pub fn is_no(self) -> bool {
        matches!(self, Rebuild::No(_))
    }
}

/// A named connector that builds [`Source`]s, [`Sink`]s or both from config.
///
/// Implement this trait once per connector you want to expose to the
/// configuration file and register it with
/// [`ServiceBuilder::connector`](crate::service::ServiceBuilder::connector).
/// [`name`](Connector::name) is the value of the `connector` property of a
/// `source` or `sink` node. [`roles`](Connector::roles) lists the nodes it
/// serves; override [`source`](Connector::source) for the roles that include
/// [`Role::Source`] and [`sink`](Connector::sink) for the roles that include
/// [`Role::Sink`].
pub trait Connector: Send + Sync + 'static {
    /// The name that appears in config as `connector="<name>"`.
    fn name(&self) -> &'static str;

    /// The roles this connector serves. Both by default.
    fn roles(&self) -> &'static [Role] {
        &[Role::Source, Role::Sink]
    }

    /// Build a source instance from the user-supplied config value.
    ///
    /// `ctx` carries the codec registry, so a connector that moves bytes
    /// resolves its byte format through [`ConnectorContext::codec`] instead of
    /// owning a format list.
    ///
    /// # Errors
    ///
    /// Return [`SaciError::Configuration`] if required config fields are
    /// missing or have invalid values. The default answers that the connector
    /// has no source.
    fn source(
        &self,
        _config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> SaciResult<Box<dyn Source>> {
        Err(SaciError::configuration(format!(
            "connector '{}' has no source",
            self.name()
        )))
    }

    /// Build a sink instance from the user-supplied config value.
    ///
    /// `ctx` carries the codec registry, as for [`source`](Connector::source).
    ///
    /// # Errors
    ///
    /// Return [`SaciError::Configuration`] if required config fields are
    /// missing or have invalid values. The default answers that the connector
    /// has no sink.
    fn sink(&self, _config: &ConfigValue, _ctx: &ConnectorContext) -> SaciResult<Box<dyn Sink>> {
        Err(SaciError::configuration(format!(
            "connector '{}' has no sink",
            self.name()
        )))
    }

    /// Whether building this connector a second time in the same process,
    /// after the first instance has been dropped, is sound for `config`: it
    /// loses nothing the first instance had accepted, and it re-delivers
    /// nothing the first instance had already delivered, beyond what the
    /// connector's own at-least-once contract already allows.
    ///
    /// The host heals a failing connector by rebuilding it, and heals by
    /// default, so the default answer is
    /// `Rebuild::No(REBUILD_UNDECLARED)`: a connector opts in with
    /// [`Rebuild::Yes`]. The reason in [`Rebuild::No`] is one phrase the host
    /// reports verbatim when a `heal` block asks for what this connector
    /// cannot give.
    fn rebuild(&self, _role: Role, _config: &ConfigValue) -> Rebuild {
        Rebuild::No(REBUILD_UNDECLARED)
    }

    /// True when a source built from `config` never reaches end of input.
    ///
    /// Only the stream runner can drive a live source: any other run mode
    /// would wait on it forever. A workflow that declares one under a run mode
    /// other than `run "stream"` is refused when the config validates, which
    /// asks the connectors this crate ships, and again when the service builds
    /// the workflow, which asks the registry a connector was registered in.
    fn live(&self, _config: &ConfigValue) -> bool {
        false
    }

    /// Config keys (dotted) shown in the topology detail of a node of `role`.
    ///
    /// Dotted keys walk nested tables (`mode.subject` reads the `mode` table,
    /// then `subject`). A key outside this list is dropped from the
    /// dashboard, never masked, so nothing in the response hints at a secret's
    /// shape or length. A connector that lists nothing shows no detail at all.
    fn detail_keys(&self, _role: Role) -> &'static [&'static str] {
        &[]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Bare;

    impl Connector for Bare {
        fn name(&self) -> &'static str {
            "bare"
        }
    }

    #[test]
    fn role_words_are_the_node_names() {
        assert_eq!(Role::Source.to_string(), "source");
        assert_eq!(Role::Sink.to_string(), "sink");
    }

    #[test]
    fn a_connector_serves_both_roles_and_declares_nothing_by_default() {
        let bare = Bare;
        assert_eq!(bare.roles(), &[Role::Source, Role::Sink]);
        assert_eq!(
            bare.rebuild(Role::Source, &ConfigValue::Null),
            Rebuild::No(REBUILD_UNDECLARED)
        );
        assert!(!bare.live(&ConfigValue::Null));
        assert!(bare.detail_keys(Role::Sink).is_empty());
    }

    #[test]
    fn the_default_builders_say_which_role_is_missing() {
        let bare = Bare;
        let ctx = ConnectorContext::new(None);
        let source = bare
            .source(&ConfigValue::Null, &ctx)
            .err()
            .expect("no source");
        assert!(
            source
                .to_string()
                .contains("connector 'bare' has no source")
        );
        let sink = bare.sink(&ConfigValue::Null, &ctx).err().expect("no sink");
        assert!(sink.to_string().contains("connector 'bare' has no sink"));
    }
}
