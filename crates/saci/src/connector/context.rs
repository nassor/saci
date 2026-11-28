//! [`ConnectorContext`]: what a factory can reach while it builds.
//!
//! The context carries the codec, if any, the host bound to this
//! connector instance through a declared `codec` id. Resolution against
//! the `CodecRegistry` happens in the host, once per workflow build, so
//! every byte-carrying connector uses what it is handed instead of resolving a
//! `format` key itself.
//!
//! It also carries the optional [`ChannelBridge`], the shared registry a
//! `ChannelSource`/`ChannelSink` factory resolves its named half through, and
//! the [`NodeIdentity`] the host binds to every connector it builds, which a
//! connector that names itself to a peer or labels its own series reads.

use std::sync::Arc;

use crate::codec::Codec;
use crate::error::SaciError;

use crate::connector::ChannelBridge;

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

/// What a factory can reach while it builds: the codec the config
/// bound to this connector instance, if any, the channel bridge, if one
/// was registered, and where this node sits.
pub struct ConnectorContext {
    codec: Option<Arc<dyn Codec>>,
    channels: Option<Arc<dyn ChannelBridge>>,
    identity: Option<NodeIdentity>,
    skip_corrupt: bool,
}

impl ConnectorContext {
    /// Wrap the codec the host resolved for this connector instance.
    ///
    /// `None` for a connector declared with no `codec` key, which is
    /// valid for a connector that produces `RecordBatch`es directly (for
    /// example `PostgresSource`) and an error for one that moves bytes.
    pub fn new(codec: Option<Arc<dyn Codec>>) -> Self {
        Self {
            codec,
            channels: None,
            identity: None,
            skip_corrupt: false,
        }
    }

    /// Configure whether byte readers/decoders isolate malformed source records
    /// rather than failing the batch.
    pub fn with_skip_corrupt(mut self, skip: bool) -> Self {
        self.skip_corrupt = skip;
        self
    }

    /// Whether sources built under this context should isolate malformed input
    /// records rather than failing the batch.
    pub fn skip_corrupt(&self) -> bool {
        self.skip_corrupt
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
    pub fn channels(&self) -> Option<&Arc<dyn ChannelBridge>> {
        self.channels.as_ref()
    }

    /// The bound codec, or a configuration error naming `what`.
    ///
    /// `what` prefixes the error, so a bad key names the connector that
    /// rejected it.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when the source or sink declared no
    /// `codec` key.
    pub fn codec(&self, what: &str) -> Result<Arc<dyn Codec>, SaciError> {
        self.codec.clone().ok_or_else(|| {
            SaciError::configuration(format!(
                "{what} moves bytes and needs a 'codec' key naming a declared codec"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct EchoCodec(&'static str);

    impl Codec for EchoCodec {
        fn format(&self) -> &'static str {
            self.0
        }
    }

    #[test]
    fn a_bound_codec_resolves() {
        let ctx = ConnectorContext::new(Some(Arc::new(EchoCodec("csv"))));
        assert_eq!(ctx.codec("FileSource").unwrap().format(), "csv");
    }

    #[test]
    fn an_unbound_codec_is_a_configuration_error_naming_the_connector() {
        let ctx = ConnectorContext::new(None);
        let err = match ctx.codec("FileSource") {
            Ok(_) => panic!("no codec was bound"),
            Err(e) => e,
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(
            err.message(),
            "FileSource moves bytes and needs a 'codec' key naming a declared codec"
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
