//! End to end smoke test against a live PostgreSQL server.
//!
//! Creates two tables, inserts rows, drains the first through a
//! [`PostgresSource`] into a [`Dataset`], drains that dataset back out through a
//! [`PostgresSink`] into the second, and compares the two.
//!
//! ```bash
//! export SACI_PG_DSN='postgres://postgres:saci@localhost:5432/postgres'
//! cargo run -p saci-connector-postgresql --example postgres_roundtrip
//! ```
//!
//! Exits non-zero when the row counts or the values disagree.

use std::process::ExitCode;

use arrow_array::{Array, Decimal128Array, Int64Array, StringArray};
use saci_connector::from_kdl_str;
use saci_connector_postgresql::{
    PostgresSink, PostgresSinkConfig, PostgresSource, PostgresSourceConfig,
};
use saci_core::dataset::Dataset;
use saci_core::io::sink::Sink;
use saci_core::io::source::{Source, drain_into_dataset};

/// Rows inserted into the input table.
const ROWS: i64 = 250;

const COMPONENT: &str = "Order";

fn dsn() -> Option<String> {
    std::env::var("SACI_PG_DSN").ok()
}

#[tokio::main]
async fn main() -> ExitCode {
    let Some(dsn) = dsn() else {
        eprintln!(
            "SKIP: set SACI_PG_DSN to a PostgreSQL connection string, for example\n  \
             export SACI_PG_DSN='postgres://postgres:saci@localhost:5432/postgres'"
        );
        return ExitCode::SUCCESS;
    };

    match run(&dsn).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("FAIL: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(dsn: &str) -> Result<(), Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
    let driver = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    client
        .batch_execute(
            "DROP TABLE IF EXISTS saci_roundtrip_in; \
             DROP TABLE IF EXISTS saci_roundtrip_out; \
             DROP TABLE IF EXISTS saci_roundtrip_offsets; \
             CREATE TABLE saci_roundtrip_in ( \
                 id bigint PRIMARY KEY, \
                 label text, \
                 amount numeric(18,4) \
             ); \
             CREATE TABLE saci_roundtrip_out ( \
                 id bigint PRIMARY KEY, \
                 label text, \
                 amount numeric(18,4) \
             )",
        )
        .await?;

    let statement = client
        // `$3::text::numeric`: a bare `$3::numeric` would make the server infer the
        // parameter itself as numeric, and the amount is bound as text.
        .prepare(
            "INSERT INTO saci_roundtrip_in (id, label, amount) \
             VALUES ($1, $2, $3::text::numeric)",
        )
        .await?;
    for id in 1..=ROWS {
        let label = if id % 7 == 0 {
            None
        } else {
            Some(format!("row-{id}"))
        };
        let amount = format!("{}.{:04}", id, id % 10_000);
        client.execute(&statement, &[&id, &label, &amount]).await?;
    }
    println!("inserted {ROWS} row(s) into saci_roundtrip_in");

    // --- read ------------------------------------------------------------
    let source_cfg: PostgresSourceConfig = parse(&format!(
        r#"
name "roundtrip_in"
batch_rows 64

connection dsn={dsn} sslmode="disable"

mode kind="polling" table="saci_roundtrip_in" cursor_column="id" offset_table="saci_roundtrip_offsets"

schema_fields "id" type="int64" nullable=#false
schema_fields "label" type="utf8"
schema_fields "amount" type="decimal128" precision=18 scale=4
"#,
        dsn = quoted(dsn)
    ))?;

    let mut source = PostgresSource::new(source_cfg)?;
    let schema = source.schema();
    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema.clone());
    let read = drain_into_dataset(&mut source, &mut dataset, COMPONENT).await?;
    println!("source drained {read} row(s) into the dataset");

    // --- write -----------------------------------------------------------
    let sink_cfg: PostgresSinkConfig = parse(&format!(
        r#"
name "roundtrip_out"
table "saci_roundtrip_out"
write_mode "upsert"
conflict_columns "id"
chunk_rows 100

connection dsn={dsn} sslmode="disable"

schema_fields "id" type="int64" nullable=#false
schema_fields "label" type="utf8"
schema_fields "amount" type="decimal128" precision=18 scale=4
"#,
        dsn = quoted(dsn)
    ))?;

    let mut sink = PostgresSink::new(sink_cfg)?;
    let batch = dataset
        .batch_for(COMPONENT)
        .ok_or("the dataset lost its component")?;
    sink.write_batch(batch).await?;
    sink.finish().await?;
    println!(
        "sink wrote {} row(s) to saci_roundtrip_out",
        batch.num_rows()
    );

    // --- compare ---------------------------------------------------------
    print_head(batch);

    let out_rows: i64 = client
        .query_one("SELECT count(*) FROM saci_roundtrip_out", &[])
        .await?
        .get(0);
    println!("saci_roundtrip_out holds {out_rows} row(s)");

    let mismatches: i64 = client
        .query_one(
            "SELECT count(*) FROM saci_roundtrip_in i \
             FULL OUTER JOIN saci_roundtrip_out o USING (id) \
             WHERE i.id IS NULL OR o.id IS NULL \
                OR i.label IS DISTINCT FROM o.label \
                OR i.amount IS DISTINCT FROM o.amount",
            &[],
        )
        .await?
        .get(0);

    client
        .batch_execute(
            "DROP TABLE saci_roundtrip_in; \
             DROP TABLE saci_roundtrip_out; \
             DROP TABLE saci_roundtrip_offsets",
        )
        .await?;
    driver.abort();

    if read as i64 != ROWS {
        return Err(format!("source read {read} row(s), expected {ROWS}").into());
    }
    if out_rows != ROWS {
        return Err(format!("sink wrote {out_rows} row(s), expected {ROWS}").into());
    }
    if mismatches != 0 {
        return Err(format!("{mismatches} row(s) differ between the two tables").into());
    }

    println!("OK: {ROWS} row(s) round-tripped with every column equal");
    Ok(())
}

/// Print the first three rows so the run shows real values, not just counts.
fn print_head(batch: &arrow_array::RecordBatch) {
    let ids = batch
        .column_by_name("id")
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>());
    let labels = batch
        .column_by_name("label")
        .and_then(|column| column.as_any().downcast_ref::<StringArray>());
    let amounts = batch
        .column_by_name("amount")
        .and_then(|column| column.as_any().downcast_ref::<Decimal128Array>());

    let (Some(ids), Some(labels), Some(amounts)) = (ids, labels, amounts) else {
        return;
    };
    println!(
        "first {} row(s) as Arrow saw them:",
        3.min(batch.num_rows())
    );
    for row in 0..batch.num_rows().min(3) {
        let label = if labels.is_null(row) {
            "NULL".to_string()
        } else {
            labels.value(row).to_string()
        };
        println!(
            "  id={} label={label} amount={} (unscaled, scale {})",
            ids.value(row),
            amounts.value(row),
            amounts.scale()
        );
    }
}

/// Quote a value as a configuration string.
fn quoted(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

/// Parse a KDL fragment into one of the connector's typed configs.
fn parse<T: serde::de::DeserializeOwned>(kdl: &str) -> Result<T, Box<dyn std::error::Error>> {
    Ok(T::deserialize(from_kdl_str(kdl)?)?)
}
