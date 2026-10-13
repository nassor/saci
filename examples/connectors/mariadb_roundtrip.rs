//! End to end smoke test against a live MariaDB or MySQL server.
//!
//! Creates two tables, inserts rows, drains the first through a
//! [`MariadbSource`] into a [`Dataset`], drains that dataset back out through a
//! [`MariadbSink`] into the second, and compares the two.
//!
//! ```bash
//! docker run -d --name saci-mariadb -p 3306:3306 -e MARIADB_ROOT_PASSWORD=saci \
//!     -e MARIADB_DATABASE=saci mariadb:11.8
//! export SACI_MARIADB_DSN='mysql://root:saci@127.0.0.1:3306/saci'
//! cargo run -p saci-connector-mariadb --example mariadb_roundtrip
//! ```
//!
//! Any MariaDB or MySQL DSN works. Exits non-zero when the row counts or the
//! values disagree.

use std::process::ExitCode;

use arrow_array::{Array, Decimal128Array, Int64Array, StringArray};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};
use saci_connector::from_kdl_str;
use saci_connector_mariadb::{MariadbSink, MariadbSinkConfig, MariadbSource, MariadbSourceConfig};
use saci_core::dataset::Dataset;
use saci_core::io::sink::Sink;
use saci_core::io::source::{Source, drain_into_dataset};

/// Rows inserted into the input table.
const ROWS: i64 = 250;

const COMPONENT: &str = "Order";

fn dsn() -> Option<String> {
    std::env::var("SACI_MARIADB_DSN").ok()
}

#[tokio::main]
async fn main() -> ExitCode {
    let Some(dsn) = dsn() else {
        eprintln!(
            "SKIP: set SACI_MARIADB_DSN to a MariaDB or MySQL URL, for example\n  \
             export SACI_MARIADB_DSN='mysql://root:saci@127.0.0.1:3306/saci'"
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
    let mut conn = Conn::new(Opts::from_url(dsn)?).await?;

    for statement in [
        "DROP TABLE IF EXISTS saci_roundtrip_in",
        "DROP TABLE IF EXISTS saci_roundtrip_out",
        "DROP TABLE IF EXISTS saci_roundtrip_offsets",
        "CREATE TABLE saci_roundtrip_in (id BIGINT PRIMARY KEY, label VARCHAR(64), \
         amount DECIMAL(18,4))",
        "CREATE TABLE saci_roundtrip_out (id BIGINT PRIMARY KEY, label VARCHAR(64), \
         amount DECIMAL(18,4))",
    ] {
        conn.query_drop(statement).await?;
    }

    for start in (1..=ROWS).step_by(50) {
        let values: Vec<String> = (start..(start + 50).min(ROWS + 1))
            .map(|id| {
                let label = if id % 7 == 0 {
                    "NULL".to_string()
                } else {
                    format!("'row-{id}'")
                };
                format!("({id}, {label}, '{}.{:04}')", id, id % 10_000)
            })
            .collect();
        conn.query_drop(format!(
            "INSERT INTO saci_roundtrip_in (id, label, amount) VALUES {}",
            values.join(", ")
        ))
        .await?;
    }
    println!("inserted {ROWS} row(s) into saci_roundtrip_in");

    // --- read ------------------------------------------------------------
    let source_cfg: MariadbSourceConfig = parse(&format!(
        r#"
name "roundtrip_in"
batch_rows 64

connection dsn={dsn} ssl_mode="disabled"

mode kind="polling" table="saci_roundtrip_in" cursor_column="id" offset_table="saci_roundtrip_offsets"

schema_fields "id" type="int64" nullable=#false
schema_fields "label" type="utf8"
schema_fields "amount" type="decimal128" precision=18 scale=4
"#,
        dsn = quoted(dsn)
    ))?;

    let mut source = MariadbSource::new(source_cfg)?;
    let schema = source.schema();
    let mut dataset = Dataset::new();
    dataset.register_raw_component(COMPONENT, schema.clone());
    let read = drain_into_dataset(&mut source, &mut dataset, COMPONENT).await?;
    println!("source drained {read} row(s) into the dataset");

    // --- write -----------------------------------------------------------
    let sink_cfg: MariadbSinkConfig = parse(&format!(
        r#"
name "roundtrip_out"
table "saci_roundtrip_out"
write_mode "upsert"
conflict_columns "id"
chunk_rows 100

connection dsn={dsn} ssl_mode="disabled"

schema_fields "id" type="int64" nullable=#false
schema_fields "label" type="utf8"
schema_fields "amount" type="decimal128" precision=18 scale=4
"#,
        dsn = quoted(dsn)
    ))?;

    let mut sink = MariadbSink::new(sink_cfg)?;
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

    let out_rows: i64 = conn
        .query_first("SELECT COUNT(*) FROM saci_roundtrip_out")
        .await?
        .unwrap_or(-1);
    println!("saci_roundtrip_out holds {out_rows} row(s)");

    // A LEFT JOIN each way finds a row missing on either side or differing in
    // any column; `<=>` is the NULL-safe equality.
    let mismatches: i64 = conn
        .query_first(
            "SELECT \
               (SELECT COUNT(*) FROM saci_roundtrip_in i LEFT JOIN saci_roundtrip_out o \
                  ON o.id = i.id WHERE o.id IS NULL \
                  OR NOT (i.label <=> o.label) OR NOT (i.amount <=> o.amount)) \
             + (SELECT COUNT(*) FROM saci_roundtrip_out o LEFT JOIN saci_roundtrip_in i \
                  ON i.id = o.id WHERE i.id IS NULL \
                  OR NOT (i.label <=> o.label) OR NOT (i.amount <=> o.amount))",
        )
        .await?
        .unwrap_or(-1);

    for table in [
        "saci_roundtrip_in",
        "saci_roundtrip_out",
        "saci_roundtrip_offsets",
    ] {
        conn.query_drop(format!("DROP TABLE {table}")).await?;
    }
    conn.disconnect().await?;

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
