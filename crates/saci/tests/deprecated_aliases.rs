//! The 0.2 names that still compile, as deprecated aliases of their 0.3 spelling.
//!
//! Each alias must resolve and forward to the new item without changing what it
//! does. The file is the record of which names are aliased; the next release
//! deletes it with them.
#![cfg(all(feature = "service", feature = "connector-channel", feature = "connector-http"))]
#![allow(deprecated)]

use std::sync::Arc;

use saci::SaciError;
use saci::connector::ChannelBridge;
use saci::connector::channel::{ChannelSink, ChannelSource};
use saci::connector::http::{HttpClientConfig, HttpClientOptions};
use saci::dataset::Dataset;
use saci::service::{ServiceBuilder, ServiceConfig, ServiceFactory, StandaloneStats, WorkflowFactory, WorkflowStats};
use saci::system::SystemMeta;
use saci::window::WindowSpec;
use saci::Component;
use serde::{Deserialize, Serialize};

#[derive(Component, Serialize, Deserialize)]
struct Price {
    value: f64,
}

#[test]
fn type_aliases_name_the_same_type() {
    let _: StandaloneStats = WorkflowStats::default();
    let _: Option<ServiceFactory> = None::<WorkflowFactory>;
    let _: HttpClientOptions = HttpClientConfig::default();
}

#[test]
fn dataset_register_component_registers() -> Result<(), SaciError> {
    let mut data = Dataset::new();
    data.register_component::<Price>()?;
    assert!(data.batch::<Price>()?.num_rows() == 0);
    Ok(())
}

#[test]
fn system_meta_old_declarations_match_new() {
    let old = SystemMeta::new("s")
        .read("Price", "value")
        .write("Price", "value")
        .read_component("Price")
        .write_component("Price")
        .read_resource::<u8>()
        .write_resource::<u8>();
    let new = SystemMeta::new("s")
        .reads_named("Price", "value")
        .writes_named("Price", "value")
        .reads_all_named("Price")
        .writes_all_named("Price")
        .reads_resource::<u8>()
        .writes_resource::<u8>();
    assert_eq!(old.read_fields().len(), new.read_fields().len());
    assert_eq!(old.write_fields().len(), new.write_fields().len());
    assert_eq!(old.read_components(), new.read_components());
    assert_eq!(old.write_components(), new.write_components());
    assert_eq!(old.read_resources(), new.read_resources());
    assert_eq!(old.write_resources(), new.write_resources());
}

#[test]
fn window_spec_old_path_and_tumbling_id() {
    let spec: saci::window_spec::WindowSpec = WindowSpec::tumbling(1000);
    assert!(spec.validate().is_ok());
    assert_eq!(WindowSpec::assign_tumbling(-1500, 1000, 0), saci::window::tumbling_id(-1500, 1000, 0));
}

#[tokio::test]
async fn channel_halves_keep_the_old_constructors() {
    use arrow_schema::Schema;
    let schema = Arc::new(Schema::empty());
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let _source = ChannelSource::from_receiver(schema.clone(), rx);
    let _sink = ChannelSink::from_sender(schema, 2, tx);
}

#[test]
fn service_builder_old_chain_compiles() {
    let bridge: Arc<dyn ChannelBridge> = Arc::new(saci::connector::channel::ChannelRegistry::default());
    let builder = ServiceBuilder::new().with_channel_bridge(bridge);
    // `build_all` forwards to `build`; naming it proves it resolves with the same shape.
    let build_all: fn(ServiceBuilder, &ServiceConfig) -> Result<Vec<saci::service::BuiltWorkflow>, SaciError> =
        ServiceBuilder::build_all;
    let _ = (builder, build_all);
}
