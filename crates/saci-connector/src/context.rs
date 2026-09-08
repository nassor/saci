//! [`ConnectorContext`]: what a factory can reach while it builds.
//!
//! The context carries the transformer, if any, the host bound to this
//! connector instance through a declared `transformer` id. Resolution against
//! the `TransformerRegistry` happens in the host, once per workflow build, so
//! every byte-carrying connector uses what it is handed instead of resolving a
//! `format` key itself.
//!
//! It also carries the optional [`ChannelBridge`], the shared registry a
//! `ChannelSource`/`ChannelSink` factory resolves its named half through, and
//! the [`NodeIdentity`] the host binds to every connector it builds, which a
//! connector that names itself to a peer or labels its own series reads.

use std::sync::Arc;

use saci_core::error::SaciError;
use saci_transformer::Transformer;

use crate::ChannelBridge;

/// Where a connector instance sits: the service that runs it, the workflow
/// it is declared in and its own node id.
///
/// Bound by the host, read by a connector that names itself to a peer or
/// labels its own series.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeIdentity {
    /// `node.name` from the service config, or `node.id` as a decimal string
    /// when unnamed.
    pub service: String,
    /// The declared workflow id.
    pub workflow: String,
    /// The declared source or sink id.
    pub node: String,
}

/// What a factory can reach while it builds: the transformer the config
/// bound to this connector instance, if any, the channel bridge, if one
/// was registered, and where this node sits.
pub struct ConnectorContext {
    transformer: Option<Arc<dyn Transformer>>,
    channels: Option<Arc<dyn ChannelBridge>>,
    identity: Option<NodeIdentity>,
}

impl ConnectorContext {
    /// Wrap the transformer the host resolved for this connector instance.
    ///
    /// `None` for a connector declared with no `transformer` key, which is
    /// valid for a connector that produces `RecordBatch`es directly (for
    /// example `PostgresSource`) and an error for one that moves bytes.
    pub fn new(transformer: Option<Arc<dyn Transformer>>) -> Self {
        Self {
            transformer,
            channels: None,
            identity: None,
        }
    }

    /// Attach the shared channel bridge every `ChannelSource`/`ChannelSink`
    /// node resolves its named half through.
    pub fn with_channels(mut self, channels: Arc<dyn ChannelBridge>) -> Self {
        self.channels = Some(channels);
        self
    }

    /// Attach the [`NodeIdentity`] of the node this connector was declared on.
    pub fn with_identity(mut self, identity: NodeIdentity) -> Self {
        self.identity = Some(identity);
        self
    }

    /// Where this node sits, or a configuration error naming `what`.
    ///
    /// `what` prefixes the error, so a connector that needs an identity and
    /// was built without one names itself.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the host bound no identity,
    /// which is the case for a connector built outside a workflow node.
    pub fn identity(&self, what: &str) -> Result<&NodeIdentity, SaciError> {
        self.identity.as_ref().ok_or_else(|| {
            SaciError::configuration(format!(
                "{what} needs the node identity (service, workflow, node) the host binds to every connector it builds"
            ))
        })
    }

    /// The channel bridge, if one was registered; `None` otherwise.
    pub fn channel_bridge(&self) -> Option<&Arc<dyn ChannelBridge>> {
        self.channels.as_ref()
    }

    /// The bound transformer, or a configuration error naming `what`.
    ///
    /// `what` prefixes the error, so a bad key names the connector that
    /// rejected it.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the source or sink declared no
    /// `transformer` key.
    pub fn transformer(&self, what: &str) -> Result<Arc<dyn Transformer>, SaciError> {
        self.transformer.clone().ok_or_else(|| {
            SaciError::configuration(format!(
                "{what} moves bytes and needs a 'transformer' key naming a declared transformer"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoTransformer(&'static str);

    impl Transformer for EchoTransformer {
        fn format(&self) -> &'static str {
            self.0
        }
    }

    #[test]
    fn a_bound_transformer_resolves() {
        let ctx = ConnectorContext::new(Some(Arc::new(EchoTransformer("csv"))));
        assert_eq!(ctx.transformer("FileSource").unwrap().format(), "csv");
    }

    #[test]
    fn an_unbound_transformer_is_a_configuration_error_naming_the_connector() {
        let ctx = ConnectorContext::new(None);
        let err = match ctx.transformer("FileSource") {
            Ok(_) => panic!("no transformer was bound"),
            Err(e) => e,
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "FileSource moves bytes and needs a 'transformer' key naming a declared transformer"
        );
    }

    #[test]
    fn a_bound_identity_resolves() {
        let ctx = ConnectorContext::new(None).with_identity(NodeIdentity {
            service: "svc-a".to_string(),
            workflow: "ticks".to_string(),
            node: "out".to_string(),
        });
        let identity = ctx.identity("SaciSink").expect("identity was bound");
        assert_eq!(identity.service, "svc-a");
        assert_eq!(identity.workflow, "ticks");
        assert_eq!(identity.node, "out");
    }

    #[test]
    fn an_unbound_identity_is_a_configuration_error_naming_the_connector() {
        let ctx = ConnectorContext::new(None);
        let err = match ctx.identity("SaciSink") {
            Ok(_) => panic!("no identity was bound"),
            Err(e) => e,
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "SaciSink needs the node identity (service, workflow, node) the host binds to every connector it builds"
        );
    }
}
