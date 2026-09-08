//! A round trip over a synced replica, against a live endpoint.
//!
//! The endpoint is whatever `SACI_TURSO_URL` and `SACI_TURSO_TOKEN` name, so
//! `default` skips this binary by name in `.config/nextest.toml`; run it with
//! `--profile ci`. The variables come from the environment, falling back to a
//! repo-root `.env` the way the service binary reads one. Unset variables, or an
//! endpoint that does not answer within the probe budget, soft-skip; every step
//! after the seed panics instead, because the sync engine's HTTP client sets no
//! timeout of its own and a stall there must fail rather than hang.

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use saci_connector_turso::{TursoSink, TursoSinkConfig, TursoSource, TursoSourceConfig};
use saci_core::io::{sink::Sink, source::Source};

const TABLE: &str = "saci_turso_synced_roundtrip";

/// How long the endpoint has to accept the seed before the test soft-skips.
///
/// `turso::sync` builds its hyper client with no connect or request timeout and
/// drives every operation from one IO worker thread, so a stalled endpoint holds
/// the operation — and this test, and the suite — open indefinitely. The seed is
/// the reachability gate, so a budget overrun here means "no endpoint", which is
/// the same condition as the variables being unset.
const PROBE_BUDGET: Duration = Duration::from_secs(30);

/// How long the round trip itself may take once the seed has landed. The
/// endpoint answered by then, so overrunning this is a failure, not a skip.
const ROUND_TRIP_BUDGET: Duration = Duration::from_secs(60);

fn sink(path: &str, url: &str, token: &str) -> TursoSink {
    let body = format!(
        "name \"synced\"\n\
         table \"{TABLE}\"\n\
         write_mode \"append\"\n\
         connection path=\"{}\" {{\n\
         \x20   remote url=\"{url}\" token=\"{token}\"\n\
         }}\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"total\" type=\"float64\" nullable=#true\n",
        common::kdl_path(path)
    );
    let config: TursoSinkConfig = common::config_from_kdl(&body);
    TursoSink::new(config).expect("sink builds")
}

fn source(path: &str, url: &str, token: &str) -> TursoSource {
    let body = format!(
        "name \"synced\"\n\
         batch_rows 1024\n\
         connection path=\"{}\" {{\n\
         \x20   remote url=\"{url}\" token=\"{token}\"\n\
         }}\n\
         mode kind=\"dump\" table=\"{TABLE}\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"total\" type=\"float64\" nullable=#true\n",
        common::kdl_path(path)
    );
    let config: TursoSourceConfig = common::config_from_kdl(&body);
    TursoSource::new(config).expect("source builds")
}

/// Seed the table on the replica through the sync builder itself, which is also
/// what proves the endpoint and token are reachable.
async fn seed(path: &str, url: &str, token: &str) -> Result<(), String> {
    let database = turso::sync::Builder::new_remote(path)
        .with_remote_url(url)
        .with_auth_token(token)
        .bootstrap_if_empty(true)
        .build()
        .await
        .map_err(|e| e.to_string())?;
    let conn = database.connect().await.map_err(|e| e.to_string())?;
    conn.execute(
        &format!("CREATE TABLE IF NOT EXISTS {TABLE} (id INTEGER PRIMARY KEY, total REAL)"),
        (),
    )
    .await
    .map_err(|e| e.to_string())?;
    conn.execute(&format!("DELETE FROM {TABLE}"), ())
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[tokio::test]
async fn a_synced_replica_round_trips() {
    // Credentials normally live in the repo-root `.env` the workspace's
    // `dotenvy` entry exists for. An absent file leaves the variables unset,
    // and the test skips below.
    let _ = dotenvy::dotenv();
    let (Ok(url), Ok(token)) = (
        std::env::var("SACI_TURSO_URL"),
        std::env::var("SACI_TURSO_TOKEN"),
    ) else {
        eprintln!("SKIP: SACI_TURSO_URL/SACI_TURSO_TOKEN unset");
        return;
    };

    let db = common::temp_db();
    // The engine's synced builder takes only `http(s)://`, so the aliases Turso
    // hands out are rewritten before either the raw seed or the connector sees
    // them.
    let https = url
        .replacen("turso://", "https://", 1)
        .replacen("libsql://", "https://", 1);

    match tokio::time::timeout(PROBE_BUDGET, seed(&db.path_str(), &https, &token)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            eprintln!("SKIP: turso endpoint unavailable: {error}");
            return;
        }
        Err(_) => {
            eprintln!("SKIP: turso endpoint did not answer within {PROBE_BUDGET:?}");
            return;
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("total", DataType::Float64, true),
    ]));
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1i64, 2]));
    let totals: ArrayRef = Arc::new(Float64Array::from(vec![1.5f64, 2.5]));
    let batch = RecordBatch::try_new(schema, vec![ids, totals]).expect("batch");

    let round_trip = async {
        let mut sink = sink(&db.path_str(), &https, &token);
        sink.write_batch(&batch).await.expect("write");
        sink.finish().await.expect("finish");
        drop(sink);

        let mut source = source(&db.path_str(), &https, &token);
        let mut rows = 0;
        while let Some(batch) = source.next_batch().await.expect("next_batch") {
            rows += batch.num_rows();
        }
        assert_eq!(rows, 2);
    };
    tokio::time::timeout(ROUND_TRIP_BUDGET, round_trip)
        .await
        .unwrap_or_else(|_| panic!("the round trip did not return within {ROUND_TRIP_BUDGET:?}"));
}
