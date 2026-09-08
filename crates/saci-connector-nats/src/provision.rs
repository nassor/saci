//! JetStream stream provisioning shared by [`crate::source`] and
//! [`crate::sink`].

use async_nats::jetstream::{self, stream::Stream};

use saci_core::error::SaciError;

use crate::config::StreamProvision;

/// Resolve the stream `name`, creating it first when `provision.create` is set.
///
/// `fallback_subjects` is what the calling half writes or reads, used as the new
/// stream's subject list when `provision.subjects` is empty.
///
/// `create = false` still fetches the stream, which turns a stream typo into a
/// startup error rather than a silent black hole.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when the stream cannot be created or does not
/// exist.
pub(crate) async fn resolve_stream(
    context: &jetstream::Context,
    name: &str,
    provision: &StreamProvision,
    fallback_subjects: &[String],
    what: &str,
) -> Result<Stream, SaciError> {
    if provision.create {
        // Idempotent by design: concurrent SACI instances race to create the
        // same stream, and only one needs to win. An existing stream is
        // returned as it stands, never reconfigured.
        return context
            .get_or_create_stream(provision.to_stream_config(name, fallback_subjects))
            .await
            .map_err(|e| {
                SaciError::generic(format!("{what}: cannot resolve stream '{name}': {e}"))
            });
    }
    context.get_stream(name).await.map_err(|e| {
        SaciError::generic(format!(
            "{what}: cannot resolve stream '{name}': {e}; set \
             mode.stream_provision.create = true to have SACI create it"
        ))
    })
}
