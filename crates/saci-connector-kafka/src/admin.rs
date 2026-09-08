//! Topic provisioning shared by [`crate::source::KafkaSource`] and
//! [`crate::sink::KafkaSink`].

use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;

use saci_core::error::SaciError;

use crate::config::TopicProvision;

/// Create `topics` when `provision.create` is set, treating an existing topic
/// as success.
///
/// `provision.create == false` returns `Ok(())` immediately with no metadata
/// call: a missing topic then surfaces as a broker error from the consumer or
/// producer, which is the point of the opt-out.
///
/// # Errors
///
/// Returns [`SaciError::Generic`] when the admin client cannot be built, the
/// create-topics request fails outright, or any topic fails to create for a
/// reason other than already existing.
pub(crate) async fn ensure_topics(
    config: &ClientConfig,
    topics: &[String],
    provision: &TopicProvision,
) -> Result<(), SaciError> {
    if !provision.create {
        return Ok(());
    }

    let admin: AdminClient<DefaultClientContext> = config.create().map_err(|e| {
        SaciError::generic(format!(
            "KafkaSource: cannot create admin client for topic provisioning: {e}"
        ))
    })?;

    let specs: Vec<NewTopic<'_>> = topics
        .iter()
        .map(|name| {
            let mut spec = NewTopic::new(
                name,
                provision.partitions,
                TopicReplication::Fixed(provision.replication_factor),
            );
            for (key, value) in &provision.config {
                spec = spec.set(key.as_str(), value.as_str());
            }
            spec
        })
        .collect();

    let opts =
        AdminOptions::new().operation_timeout(Some(Duration::from_millis(provision.timeout_ms)));

    let results = admin.create_topics(&specs, &opts).await.map_err(|e| {
        SaciError::generic(format!("KafkaSource: create_topics request failed: {e}"))
    })?;

    for result in results {
        match result {
            Ok(_) => {}
            // Provisioning is idempotent by design: concurrent SACI instances
            // race to create the same topic, and only one needs to win.
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((name, code)) => {
                return Err(SaciError::generic(format!(
                    "KafkaSource: cannot create topic '{name}': {code}"
                )));
            }
        }
    }

    Ok(())
}
