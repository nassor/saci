//! Compile-and-run checks for the four macro expansions.
//!
//! The unit tests inside `saci-macros` inspect generated tokens; these compile
//! the generated code against the real SDK and run it. `#[processor]` is not
//! exercised here: its output is `#[cfg(target_arch = "wasm32")]`, so the gate
//! that proves it is the `wasm32-wasip2` build of a processor crate.

use saci_processor::__rt::pollster;
use saci_processor::arrow_schema::{DataType, Field, Schema};
use saci_processor::prelude::*;

/// Stand-in for the `saci_config_get` that `#[processor]` emits into a real
/// processor crate; a two-parameter `#[transform]` reads it through
/// `crate::saci_config_get`.
pub fn saci_config_get(key: &str) -> Option<String> {
    match key {
        "fee_rate" => Some("0.10".to_string()),
        "mangled" => Some("not-a-number".to_string()),
        _ => None,
    }
}

/// One row, covering the four Arrow types the polyglot schema uses.
#[derive(Component, serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct Row {
    pub id: i64,
    pub label: String,
    pub amount: f64,
    pub valid: bool,
}

/// Cross-batch state for the `#[fold]`, with the component renamed.
#[derive(Component, serde::Serialize, serde::Deserialize, Default, Debug, PartialEq)]
#[saci(name = "RunningTotals")]
pub struct Totals {
    pub rows_seen: i64,
    pub amount_sum: f64,
}

#[transform(component = Row)]
pub fn stamp(row: &mut Row) -> saci_processor::Result<()> {
    row.label = format!("id-{}", row.id);
    Ok(())
}

#[transform(component = Row)]
pub fn apply_fee(row: &mut Row, config: &Config) -> saci_processor::Result<()> {
    let rate: f64 = config.get("fee_rate", 0.0)?;
    row.amount -= row.amount * rate;
    Ok(())
}

#[transform(component = Row)]
pub fn reject_invalid(row: &mut Row) -> saci_processor::Result<()> {
    if !row.valid {
        return Err(format!("row {} is invalid", row.id).into());
    }
    Ok(())
}

#[fold(reads = Row, state = Totals)]
pub fn totals(rows: &[Row], state: &mut Totals) -> saci_processor::Result<()> {
    state.rows_seen += rows.len() as i64;
    state.amount_sum += rows.iter().map(|row| row.amount).sum::<f64>();
    Ok(())
}

fn seeded(rows: Vec<Row>) -> Dataset {
    let mut data = Dataset::new();
    data.register_component::<Row>().expect("register Row");
    data.append(&rows).expect("append rows");
    data.insert_resource(saci_processor::ProcessorState::<Totals>::default());
    data
}

fn rows_of(data: &Dataset) -> Vec<Row> {
    <Row as Component>::from_record_batch(data.columns::<Row>().expect("Row registered"))
        .expect("decode rows")
}

/// Drive a pipeline whose systems are going to fail.
///
/// `Pipeline::run_on` retries a failed system through `saci_core::retry`, which
/// waits on `tokio::time::sleep` in every host build of `saci-core` — its
/// `runtime` feature is on here. The `wasm32-wasip2` processor build has no
/// timer and retries immediately, which is why the happy paths above can use
/// `pollster` exactly as `#[processor]` does; an error path cannot, because
/// `pollster` supplies no reactor for that sleep. The backoff between attempts
/// is real time, which is why only the two failing cases pay for a runtime.
fn block_on_retrying<T>(fut: impl Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime with a timer")
        .block_on(fut)
}

#[test]
fn the_derived_schema_matches_a_hand_written_field_list() {
    let expected = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("valid", DataType::Boolean, false),
    ]);
    assert_eq!(*<Row as Component>::schema(), expected);
}

#[test]
fn a_string_field_traces_to_utf8_not_large_utf8() {
    let schema = <Row as Component>::schema();
    assert_eq!(
        schema.field_with_name("label").unwrap().data_type(),
        &DataType::Utf8,
        "serde_arrow's default is LargeUtf8, which no non-Rust SACI codec reads"
    );
}

#[test]
fn field_order_is_declaration_order() {
    let schema = <Row as Component>::schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, ["id", "label", "amount", "valid"]);
}

#[test]
fn the_component_name_is_the_struct_identifier_unless_overridden() {
    assert_eq!(<Row as Component>::name(), "Row");
    assert_eq!(<Totals as Component>::name(), "RunningTotals");
}

#[test]
fn the_derived_version_stays_at_one() {
    assert_eq!(<Row as Component>::version(), 1);
}

#[test]
fn a_transform_rewrites_every_row_in_place() {
    let mut data = seeded(vec![
        Row {
            id: 1,
            label: String::new(),
            amount: 100.0,
            valid: true,
        },
        Row {
            id: 2,
            label: String::new(),
            amount: 50.0,
            valid: true,
        },
    ]);

    let pipeline = Pipeline::builder("transform")
        .with::<Row>()
        .with_system(stamp_system())
        .build();
    pollster::block_on(pipeline.run_on(&mut data)).expect("run");

    let labels: Vec<String> = rows_of(&data).into_iter().map(|r| r.label).collect();
    assert_eq!(labels, ["id-1", "id-2"]);
}

#[test]
fn a_two_parameter_transform_reads_the_host_config() {
    let mut data = seeded(vec![Row {
        id: 1,
        label: String::new(),
        amount: 200.0,
        valid: true,
    }]);

    let pipeline = Pipeline::builder("config")
        .with::<Row>()
        .with_system(apply_fee_system())
        .build();
    pollster::block_on(pipeline.run_on(&mut data)).expect("run");

    assert_eq!(rows_of(&data)[0].amount, 180.0);
}

#[test]
fn a_transform_error_fails_the_batch_as_a_retryable_system_execution() {
    let mut data = seeded(vec![Row {
        id: 7,
        label: String::new(),
        amount: 1.0,
        valid: false,
    }]);

    let pipeline = Pipeline::builder("reject")
        .with::<Row>()
        .with_system(reject_invalid_system())
        .build();
    let err = block_on_retrying(pipeline.run_on(&mut data)).expect_err("invalid row");

    let (retryable, message) = saci_processor::__rt::classify_run_error(&err);
    assert!(retryable, "a failing transform releases the claim: {err}");
    assert!(message.contains("row 7 is invalid"), "{message}");
}

#[test]
fn a_fold_accumulates_into_the_processor_state_across_batches() {
    let pipeline = Pipeline::builder("fold")
        .with::<Row>()
        .with_system(totals_system())
        .build();

    let mut state = saci_processor::ProcessorState::<Totals>::default();
    for amount in [10.0, 32.0] {
        let mut data = Dataset::new();
        data.register_component::<Row>().expect("register Row");
        data.append(&[Row {
            id: 1,
            label: String::new(),
            amount,
            valid: true,
        }])
        .expect("append");
        data.insert_resource(state);

        pollster::block_on(pipeline.run_on(&mut data)).expect("run");
        // Stands in for the checkpoint the `#[processor]` expansion captures
        // and the host hands back as the next batch's `prior`.
        let rows = std::mem::take(
            &mut data
                .get_resource_mut::<saci_processor::ProcessorState<Totals>>()
                .expect("state survives the batch")
                .rows,
        );
        state = saci_processor::ProcessorState::new(rows);
    }

    assert_eq!(
        state.rows,
        vec![Totals {
            rows_seen: 2,
            amount_sum: 42.0,
        }]
    );
}

#[test]
fn a_fold_without_the_state_resource_is_an_error_not_silent_accumulation() {
    let mut data = Dataset::new();
    data.register_component::<Row>().expect("register Row");
    data.append(&[Row {
        id: 1,
        label: String::new(),
        amount: 1.0,
        valid: true,
    }])
    .expect("append");

    let pipeline = Pipeline::builder("fold")
        .with::<Row>()
        .with_system(totals_system())
        .build();
    let err = block_on_retrying(pipeline.run_on(&mut data)).expect_err("no state resource");
    assert!(err.message().contains("Totals"), "got: {err}");
}

#[test]
fn a_transform_declares_a_whole_component_read_and_write() {
    let meta = stamp_system().meta();
    assert_eq!(meta.name, "stamp");
    assert_eq!(meta.reads_components, ["Row"]);
    assert_eq!(meta.writes_components, ["Row"]);
}

#[test]
fn a_fold_declares_a_component_read_and_a_state_resource_write() {
    let meta = totals_system().meta();
    assert_eq!(meta.name, "totals");
    assert_eq!(meta.reads_components, ["Row"]);
    assert!(meta.writes_components.is_empty());
    assert_eq!(
        meta.writes_resources,
        [std::any::TypeId::of::<saci_processor::ProcessorState<Totals>>()]
    );
}

#[test]
fn the_generated_system_takes_the_synchronous_fast_path() {
    let mut data = seeded(vec![Row {
        id: 1,
        label: String::new(),
        amount: 1.0,
        valid: true,
    }]);
    assert!(
        stamp_system().run_sync(&mut data).is_some(),
        "a row transform never awaits, so it must not build a boxed future"
    );
}
