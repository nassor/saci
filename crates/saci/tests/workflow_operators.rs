//! End-to-end integration tests for declarative `operator` nodes inside
//! KDL workflows (`ServiceConfig::from_kdl` -> `ServiceBuilder::build` -> `BuiltWorkflow::run`).

#![cfg(feature = "service")]

use std::sync::{Arc, Mutex};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Float64Array, Int64Array, ListArray, RecordBatch, StringArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use saci::connector::{ConfigValue, Connector, ConnectorContext, Role};
use saci::error::SaciError;
use saci::io::{Sink, Source};
use saci::service::{ServiceBuilder, ServiceConfig};
use tokio_util::sync::CancellationToken;

struct MemBatchSource {
    schema: Arc<Schema>,
    batches: std::collections::VecDeque<RecordBatch>,
}

#[async_trait]
impl Source for MemBatchSource {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, SaciError> {
        Ok(self.batches.pop_front())
    }
}

struct MemBatchSink {
    schema: Arc<Schema>,
    out: Arc<Mutex<Vec<RecordBatch>>>,
}

#[async_trait]
impl Sink for MemBatchSink {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), SaciError> {
        self.out.lock().unwrap().push(batch.clone());
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), SaciError> {
        Ok(())
    }
}

type SourceEntry = (Arc<Schema>, Vec<RecordBatch>);
type SinkBuffer = Arc<Mutex<Vec<RecordBatch>>>;
type SinkEntry = (Arc<Schema>, SinkBuffer);

struct MemConnector {
    sources: Mutex<std::collections::HashMap<String, SourceEntry>>,
    sinks: Mutex<std::collections::HashMap<String, SinkEntry>>,
}

impl MemConnector {
    fn new() -> Self {
        Self {
            sources: Mutex::new(Default::default()),
            sinks: Mutex::new(Default::default()),
        }
    }

    fn add_source(&self, key: &str, schema: Arc<Schema>, batches: Vec<RecordBatch>) {
        self.sources
            .lock()
            .unwrap()
            .insert(key.to_string(), (schema, batches));
    }

    fn add_sink(&self, key: &str, schema: Arc<Schema>) -> Arc<Mutex<Vec<RecordBatch>>> {
        let out = Arc::new(Mutex::new(Vec::new()));
        self.sinks
            .lock()
            .unwrap()
            .insert(key.to_string(), (schema, out.clone()));
        out
    }
}

impl Connector for MemConnector {
    fn name(&self) -> &'static str {
        "mem"
    }

    fn roles(&self) -> &'static [Role] {
        &[Role::Source, Role::Sink]
    }

    fn source(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Source>, SaciError> {
        let key = config["key"]
            .as_str()
            .ok_or_else(|| SaciError::configuration("mem source missing key"))?;
        let guard = self.sources.lock().unwrap();
        let (schema, batches) = guard
            .get(key)
            .ok_or_else(|| SaciError::configuration(format!("unknown mem source '{key}'")))?;
        Ok(Box::new(MemBatchSource {
            schema: schema.clone(),
            batches: batches.iter().cloned().collect(),
        }))
    }

    fn sink(
        &self,
        config: &ConfigValue,
        _ctx: &ConnectorContext,
    ) -> Result<Box<dyn Sink>, SaciError> {
        let key = config["key"]
            .as_str()
            .ok_or_else(|| SaciError::configuration("mem sink missing key"))?;
        let guard = self.sinks.lock().unwrap();
        let (schema, out) = guard
            .get(key)
            .ok_or_else(|| SaciError::configuration(format!("unknown mem sink '{key}'")))?;
        Ok(Box::new(MemBatchSink {
            schema: schema.clone(),
            out: out.clone(),
        }))
    }
}

#[tokio::test]
async fn kdl_filter_project_group_by_and_sort_chain() {
    let dir = tempfile::tempdir().unwrap();
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("qty", DataType::Int64, false),
        Field::new("price", DataType::Float64, false),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, true),
        Field::new("rev", DataType::Float64, true),
        Field::new("cnt", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        in_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["us", "eu", "us", "apac"])),
            Arc::new(Int64Array::from(vec![2, 5, 3, 1])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 5.0])),
        ],
    )
    .unwrap();

    let conn = MemConnector::new();
    conn.add_source("orders", in_schema, vec![batch]);
    let captured = conn.add_sink("summary", out_schema);

    let kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{dir}"
run "once"

workflow "analytics" {{
    source "in" connector="mem" component="Orders" {{
        config key="orders"
    }}
    operator "f" kind="filter" component="Orders" {{
        config where="qty > 1"
    }}
    operator "p" kind="project" component="Orders" into="LineTotal" {{
        config {{
            select "region"
            select "qty * price AS revenue"
        }}
    }}
    operator "g" kind="group_by" component="LineTotal" into="Summary" {{
        config keys="region" {{
            aggregates "sum(revenue) AS rev"
            aggregates "count(*) AS cnt"
        }}
    }}
    operator "s" kind="sort" component="Summary" {{
        config order_by="rev DESC"
    }}
    sink "out" connector="mem" component="Summary" {{
        config key="summary"
    }}
    link from="in" to="f"
    link from="f" to="p"
    link from="p" to="g"
    link from="g" to="s"
    link from="s" to="out"
}}
"#,
        dir = dir.path().display()
    );

    let cfg = ServiceConfig::from_kdl(&kdl).unwrap();
    cfg.validate().unwrap();
    let built = ServiceBuilder::empty().connector(conn).build(&cfg).unwrap();
    built
        .into_iter()
        .next()
        .unwrap()
        .run(CancellationToken::new())
        .await
        .unwrap();

    let batches = captured.lock().unwrap();
    assert_eq!(batches.len(), 1);
    let out = &batches[0];
    assert_eq!(out.num_rows(), 2);
    // Row 0: us = 2*10 + 3*30 = 110.0, cnt = 2
    // Row 1: eu = 5*20 = 100.0, cnt = 1
    assert_eq!(out.column(0).as_string::<i32>().value(0), "us");
    assert!((out.column(1).as_primitive::<Float64Type>().value(0) - 110.0).abs() < 1e-9);
    assert_eq!(out.column(2).as_primitive::<Int64Type>().value(0), 2);
    assert_eq!(out.column(0).as_string::<i32>().value(1), "eu");
}

#[tokio::test]
async fn kdl_sql_join_operator_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("cust_id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let cust_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tier", DataType::Utf8, false),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("tier", DataType::Utf8, true),
        Field::new("total", DataType::Float64, true),
    ]));

    let orders = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 10])),
            Arc::new(Float64Array::from(vec![40.0, 25.0, 60.0])),
        ],
    )
    .unwrap();
    let customers = RecordBatch::try_new(
        cust_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["gold", "silver"])),
        ],
    )
    .unwrap();

    let conn = MemConnector::new();
    conn.add_source("o", orders_schema, vec![orders]);
    conn.add_source("c", cust_schema, vec![customers]);
    let captured = conn.add_sink("res", out_schema);

    let kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{dir}"
run "once"

workflow "sql_join" {{
    source "src_o" connector="mem" component="Orders" {{ config key="o" }}
    source "src_c" connector="mem" component="Customers" {{ config key="c" }}
    operator "q" kind="sql" into="TierTotals" {{
        config query="SELECT c.tier, SUM(o.amount) AS total FROM Orders o JOIN Customers c ON o.cust_id = c.id GROUP BY c.tier ORDER BY total DESC"
    }}
    sink "out" connector="mem" component="TierTotals" {{ config key="res" }}
    link from="src_o" to="q"
    link from="src_c" to="q"
    link from="q" to="out"
}}
"#,
        dir = dir.path().display()
    );

    let cfg = ServiceConfig::from_kdl(&kdl).unwrap();
    cfg.validate().unwrap();
    let built = ServiceBuilder::empty().connector(conn).build(&cfg).unwrap();
    built
        .into_iter()
        .next()
        .unwrap()
        .run(CancellationToken::new())
        .await
        .unwrap();

    let batches = captured.lock().unwrap();
    assert_eq!(batches.len(), 1);
    let out = &batches[0];
    assert_eq!(out.num_rows(), 2);
    assert_eq!(out.column(0).as_string::<i32>().value(0), "gold");
    assert!((out.column(1).as_primitive::<Float64Type>().value(0) - 100.0).abs() < 1e-9);
}

#[tokio::test]
async fn kdl_stateful_dedup_and_unnest_top_n() {
    let dir = tempfile::tempdir().unwrap();
    let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
        Field::new("tags", DataType::List(item_field.clone()), false),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, true),
    ]));

    let b1 = {
        let vals = Arc::new(StringArray::from(vec!["x", "y"])) as arrow_array::ArrayRef;
        let list = ListArray::try_new(
            item_field.clone(),
            OffsetBuffer::from_lengths([2usize]),
            vals,
            None,
        )
        .unwrap();
        RecordBatch::try_new(
            in_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![50])),
                Arc::new(list),
            ],
        )
        .unwrap()
    };
    let b2 = {
        // Duplicate id=1 must be dropped across batches; id=2 with score=90 kept and wins top_n.
        let vals = Arc::new(StringArray::from(vec!["dup", "z"])) as arrow_array::ArrayRef;
        let list = ListArray::try_new(
            item_field,
            OffsetBuffer::from_lengths([1usize, 1]),
            vals,
            None,
        )
        .unwrap();
        RecordBatch::try_new(
            in_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![999, 90])),
                Arc::new(list),
            ],
        )
        .unwrap()
    };

    let conn = MemConnector::new();
    conn.add_source("events", in_schema, vec![b1, b2]);
    let captured = conn.add_sink("top_tags", out_schema);

    let kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{dir}"
run "stream"

workflow "stream_ops" {{
    source "src" connector="mem" component="Events" {{ config key="events" }}
    operator "dedup" kind="deduplicate" component="Events" {{
        config keys="id"
    }}
    operator "explode" kind="unnest" component="Events" into="Exploded" {{
        config field="tags" alias="tag"
    }}
    operator "top" kind="top_n" component="Exploded" {{
        config order_by="score DESC" limit=1
    }}
    sink "out" connector="mem" component="Exploded" {{ config key="top_tags" }}
    link from="src" to="dedup"
    link from="dedup" to="explode"
    link from="explode" to="top"
    link from="top" to="out"
}}
"#,
        dir = dir.path().display()
    );

    let cfg = ServiceConfig::from_kdl(&kdl).unwrap();
    cfg.validate().unwrap();
    let built = ServiceBuilder::empty().connector(conn).build(&cfg).unwrap();
    let cancel = CancellationToken::new();
    built.into_iter().next().unwrap().run(cancel).await.unwrap();

    let batches = captured.lock().unwrap();
    // Item 1 yields row (id=1, score=50, tag="x") after top_n(1).
    // Item 2 drops duplicate id=1, yields (id=2, score=90, tag="z").
    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_primitive::<Int64Type>()
                .values()
                .iter()
                .copied()
        })
        .collect();
    assert_eq!(ids, vec![1, 2]);
}

#[tokio::test]
async fn kdl_stateful_lookup_join_enriches_across_items() {
    let dir = tempfile::tempdir().unwrap();
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int64, false),
        Field::new("cust_id", DataType::Int64, false),
    ]));
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("tier", DataType::Utf8, false),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int64, false),
        Field::new("cust_id", DataType::Int64, false),
        Field::new("id", DataType::Int64, true),
        Field::new("tier", DataType::Utf8, true),
    ]));

    let dim1 = RecordBatch::try_new(
        dim_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(StringArray::from(vec!["gold"])),
        ],
    )
    .unwrap();
    let fact2 = RecordBatch::try_new(
        fact_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![7])),
            Arc::new(Int64Array::from(vec![10])),
        ],
    )
    .unwrap();

    let conn = MemConnector::new();
    // Item 1 seeds dim table; Item 2 sends fact batch only.
    conn.add_source("dim", dim_schema.clone(), vec![dim1]);
    conn.add_source(
        "fact",
        fact_schema.clone(),
        vec![RecordBatch::new_empty(fact_schema), fact2],
    );
    let captured = conn.add_sink("enriched", out_schema);

    let kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{dir}"
run "stream"

workflow "lookup" {{
    source "src_f" connector="mem" component="Fact" {{ config key="fact" }}
    source "src_d" connector="mem" component="Dim" {{ config key="dim" }}
    operator "lj" kind="lookup_join" left="Fact" right="Dim" into="Enriched" {{
        config on="cust_id=id" join_type="left"
    }}
    sink "out" connector="mem" component="Enriched" {{ config key="enriched" }}
    link from="src_f" to="lj"
    link from="src_d" to="lj"
    link from="lj" to="out"
}}
"#,
        dir = dir.path().display()
    );

    let cfg = ServiceConfig::from_kdl(&kdl).unwrap();
    cfg.validate().unwrap();
    let built = ServiceBuilder::empty().connector(conn).build(&cfg).unwrap();
    built
        .into_iter()
        .next()
        .unwrap()
        .run(CancellationToken::new())
        .await
        .unwrap();

    let batches = captured.lock().unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 1);
    assert_eq!(batches[0].column(3).as_string::<i32>().value(0), "gold");
}

#[tokio::test]
async fn kdl_stateful_interval_join_and_lookup_join() {
    let dir = tempfile::tempdir().unwrap();
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("pay_ok", DataType::Utf8, false),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("oid", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("right_oid", DataType::Int64, false),
        Field::new("right_ts", DataType::Int64, false),
        Field::new("pay_ok", DataType::Utf8, false),
    ]));

    let lb1 = RecordBatch::try_new(
        left_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(Int64Array::from(vec![1000])),
        ],
    )
    .unwrap();
    let rb2 = RecordBatch::try_new(
        right_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![42])),
            Arc::new(Int64Array::from(vec![1050])),
            Arc::new(StringArray::from(vec!["paid"])),
        ],
    )
    .unwrap();

    let conn = MemConnector::new();
    // Batch 1 has left only; Batch 2 has right only — stateful carry must match across passes.
    conn.add_source(
        "l",
        left_schema.clone(),
        vec![lb1, RecordBatch::new_empty(left_schema)],
    );
    conn.add_source(
        "r",
        right_schema.clone(),
        vec![RecordBatch::new_empty(right_schema), rb2],
    );
    let captured = conn.add_sink("matched", out_schema);

    let kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{dir}"
run "continuous"

workflow "interval" {{
    source "src_l" connector="mem" component="Left" {{ config key="l" }}
    source "src_r" connector="mem" component="Right" {{ config key="r" }}
    operator "ij" kind="interval_join" left="Left" right="Right" into="Matched" {{
        config on="oid" left_time_field="ts" right_time_field="ts" lower_bound_ms=0 upper_bound_ms=100
    }}
    sink "out" connector="mem" component="Matched" {{ config key="matched" }}
    link from="src_l" to="ij"
    link from="src_r" to="ij"
    link from="ij" to="out"
}}
"#,
        dir = dir.path().display()
    );

    let cfg = ServiceConfig::from_kdl(&kdl).unwrap();
    cfg.validate().unwrap();
    let built = ServiceBuilder::empty().connector(conn).build(&cfg).unwrap();
    let cancel = CancellationToken::new();
    let cancel_child = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        cancel_child.cancel();
    });
    built.into_iter().next().unwrap().run(cancel).await.unwrap();

    let batches = captured.lock().unwrap();
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(total_rows, 1);
    assert_eq!(batches[0].column(4).as_string::<i32>().value(0), "paid");
}

#[tokio::test]
async fn workflow_in_place_project_and_sql_operators_validate_and_run_across_multiple_passes() {
    let dir = tempfile::tempdir().unwrap();
    let in_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let final_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("doubled", DataType::Float64, true),
    ]));

    let make_in = |id: i64, amount: f64, region: &str| {
        RecordBatch::try_new(
            in_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(Float64Array::from(vec![amount])),
                Arc::new(StringArray::from(vec![region])),
            ],
        )
        .unwrap()
    };

    let conn = MemConnector::new();
    let in_batches = vec![make_in(1, 10.0, "us"), make_in(2, 25.0, "eu")];
    conn.add_source("orders", in_schema, in_batches);
    let captured = conn.add_sink("projected", final_schema);

    // `p` transforms Orders in place (3 cols -> 2 cols) and `q` also writes Orders in place across 2 passes.
    let kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{dir}"
run "stream"

workflow "in_place_ops" {{
    source "in" connector="mem" component="Orders" {{ config key="orders" }}
    operator "p" kind="project" component="Orders" {{
        config {{
            select "id"
            select "amount * 2 AS doubled"
        }}
    }}
    operator "q" kind="sql" into="Orders" {{
        config query="SELECT id, doubled FROM Orders WHERE doubled >= 20.0"
    }}
    sink "out" connector="mem" component="Orders" {{ config key="projected" }}
    link from="in" to="p"
    link from="p" to="q"
    link from="q" to="out"
}}
"#,
        dir = dir.path().display()
    );

    let cfg = ServiceConfig::from_kdl(&kdl).unwrap();
    cfg.validate().unwrap();
    let built = ServiceBuilder::empty().connector(conn).build(&cfg).unwrap();
    built
        .into_iter()
        .next()
        .unwrap()
        .run(CancellationToken::new())
        .await
        .unwrap();

    let batches = captured.lock().unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].column(0).as_primitive::<Int64Type>().value(0), 1);
    assert!((batches[0].column(1).as_primitive::<Float64Type>().value(0) - 20.0).abs() < 1e-9);
    assert_eq!(batches[1].column(0).as_primitive::<Int64Type>().value(0), 2);
    assert!((batches[1].column(1).as_primitive::<Float64Type>().value(0) - 50.0).abs() < 1e-9);
}

#[test]
fn workflow_validate_rejects_unknown_operator_config_keys_and_invalid_sql() {
    let bad_key_kdl = r#"
mode "standalone"
node id=1 data_dir="/tmp/saci"
workflow "w" {
    operator "f" kind="filter" component="Orders" {
        config wher="amount > 0"
    }
}
"#;
    let err_key = ServiceConfig::from_kdl(bad_key_kdl)
        .and_then(|c| c.validate())
        .unwrap_err()
        .to_string();
    assert!(err_key.contains("wher"), "unexpected error: {err_key}");

    let bad_sql_kdl = r#"
mode "standalone"
node id=1 data_dir="/tmp/saci"
workflow "w" {
    operator "q" kind="sql" into="Out" {
        config query="SELECT FROM"
    }
}
"#;
    let err_sql = ServiceConfig::from_kdl(bad_sql_kdl)
        .and_then(|c| c.validate())
        .unwrap_err();
    assert_eq!(err_sql.category(), "configuration");
}

#[test]
fn workflow_operator_to_kdl_roundtrip() {
    let kdl = r#"
mode "standalone"
node id=1 data_dir="/tmp/saci"
run "once"

workflow "rt" {
    codec "ndjson" format="ndjson"
    source "src" connector="file" component="Orders" codec="ndjson" {
        config path="/tmp/orders_in.ndjson" {
            field "id" type="int64" nullable=#false
            field "amount" type="float64" nullable=#false
        }
    }
    operator "f" kind="filter" component="Orders" {
        config where="amount > 10.0"
    }
    operator "q" kind="sql" into="Totals" {
        config query="SELECT COUNT(*) AS n, SUM(amount) AS total FROM Orders"
    }
    sink "out" connector="file" component="Totals" codec="ndjson" {
        config path="/tmp/totals_out.ndjson" {
            field "n" type="int64" nullable=#true
            field "total" type="float64" nullable=#true
        }
    }
    link from="src" to="f"
    link from="f" to="q"
    link from="q" to="out"
}
"#;
    let cfg = ServiceConfig::from_kdl(kdl).unwrap();
    cfg.validate().unwrap();
    let serialized = cfg.to_kdl().unwrap();
    let reparsed = ServiceConfig::from_kdl(&serialized).unwrap();
    reparsed.validate().unwrap();
    assert_eq!(reparsed.workflows[0].operator.len(), 2);
}

#[cfg(feature = "windows")]
#[tokio::test]
async fn kdl_declarative_tumbling_window_fan_in_across_batches() {
    let dir = tempfile::tempdir().unwrap();
    let sale_schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("window_id", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("count", DataType::Int64, false),
        Field::new("sum", DataType::Float64, true),
    ]));

    let make_sales = |rows: &[(i64, &str, f64)]| {
        let ts: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let sym: Vec<&str> = rows.iter().map(|r| r.1).collect();
        let amt: Vec<f64> = rows.iter().map(|r| r.2).collect();
        RecordBatch::try_new(
            sale_schema.clone(),
            vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(sym)),
                Arc::new(Float64Array::from(amt)),
            ],
        )
        .unwrap()
    };

    let conn = MemConnector::new();
    conn.add_source(
        "sales_a",
        sale_schema.clone(),
        vec![
            make_sales(&[(29_000, "AAPL", 10.0)]),
            make_sales(&[(31_000, "AAPL", 5.0)]),
        ],
    );
    conn.add_source(
        "sales_b",
        sale_schema.clone(),
        vec![
            make_sales(&[(29_500, "AAPL", 7.0)]),
            make_sales(&[(61_000, "GOOG", 2.0)]),
        ],
    );
    let captured = conn.add_sink("totals", out_schema);

    let kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{dir}"
run "stream"

workflow "window_fanin" {{
    source "in_a" connector="mem" component="Sale" {{ config key="sales_a" }}
    source "in_b" connector="mem" component="Sale" {{ config key="sales_b" }}

    operator "win" kind="window" component="Sale" into="WindowTotal" {{
        window kind="tumbling" size_ms=30000 time_field="timestamp_ms" allowed_lateness_ms=5000 {{
            key_fields "symbol"
        }}
        config window_id_field="window_id" emit_bounds=#false {{
            aggregates "count(*) AS count"
            aggregates "sum(amount) AS sum"
        }}
    }}

    sink "out" connector="mem" component="WindowTotal" {{ config key="totals" }}

    link from="in_a" to="win"
    link from="in_b" to="win"
    link from="win" to="out"
}}
"#,
        dir = dir.path().display()
    );

    let cfg = ServiceConfig::from_kdl(&kdl).unwrap();
    cfg.validate().unwrap();
    let built = ServiceBuilder::empty().connector(conn).build(&cfg).unwrap();
    built
        .into_iter()
        .next()
        .unwrap()
        .run(CancellationToken::new())
        .await
        .unwrap();

    let batches = captured.lock().unwrap();
    let mut rows: Vec<(i64, String, i64, f64)> = Vec::new();
    for b in batches.iter() {
        let wid = b.column(0).as_primitive::<Int64Type>();
        let sym = b.column(1).as_string::<i32>();
        let cnt = b.column(2).as_primitive::<Int64Type>();
        let sum = b.column(3).as_primitive::<Float64Type>();
        for r in 0..b.num_rows() {
            rows.push((
                wid.value(r),
                sym.value(r).to_string(),
                cnt.value(r),
                sum.value(r),
            ));
        }
    }
    // Window 0 receives 29_000 (10.0) from in_a and 29_500 (7.0) from in_b before 31_000 closes it,
    // and window 1 receives 31_000 (5.0) before 61_000 closes window 1.
    assert_eq!(rows.len(), 2, "got rows: {rows:?}");
    assert_eq!(rows[0].0, 0);
    assert_eq!(rows[0].1, "AAPL");
    assert_eq!(rows[0].2, 2);
    assert!((rows[0].3 - 17.0).abs() < 1e-9);
    assert_eq!(rows[1].0, 1);
    assert_eq!(rows[1].1, "AAPL");
    assert_eq!(rows[1].2, 1);
    assert!((rows[1].3 - 5.0).abs() < 1e-9);
}

#[cfg(feature = "windows")]
#[tokio::test]
async fn kdl_declarative_group_by_with_session_window_block() {
    let dir = tempfile::tempdir().unwrap();
    let sale_schema = Arc::new(Schema::new(vec![
        Field::new("timestamp_ms", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("session_start_ms", DataType::Int64, false),
        Field::new("session_end_ms", DataType::Int64, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("count", DataType::Int64, false),
        Field::new("sum", DataType::Float64, true),
    ]));

    let make_sales = |rows: &[(i64, &str, f64)]| {
        let ts: Vec<i64> = rows.iter().map(|r| r.0).collect();
        let sym: Vec<&str> = rows.iter().map(|r| r.1).collect();
        let amt: Vec<f64> = rows.iter().map(|r| r.2).collect();
        RecordBatch::try_new(
            sale_schema.clone(),
            vec![
                Arc::new(Int64Array::from(ts)),
                Arc::new(StringArray::from(sym)),
                Arc::new(Float64Array::from(amt)),
            ],
        )
        .unwrap()
    };

    let conn = MemConnector::new();
    conn.add_source(
        "sales",
        sale_schema.clone(),
        vec![
            make_sales(&[(1_000, "AAPL", 10.0), (6_000, "AAPL", 5.0)]),
            make_sales(&[(25_000, "GOOG", 1.0)]),
        ],
    );
    let captured = conn.add_sink("sessions", out_schema);

    let kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{dir}"
run "stream"

workflow "session_win" {{
    source "in" connector="mem" component="Sale" {{ config key="sales" }}

    operator "sess" kind="group_by" component="Sale" into="SessionTotal" {{
        window kind="session" gap_ms=10000 time_field="timestamp_ms" {{
            key_fields "symbol"
        }}
        config window_start_field="session_start_ms" window_end_field="session_end_ms" {{
            aggregates "count(*) AS count"
            aggregates "sum(amount) AS sum"
        }}
    }}

    sink "out" connector="mem" component="SessionTotal" {{ config key="sessions" }}
    link from="in" to="sess"
    link from="sess" to="out"
}}
"#,
        dir = dir.path().display()
    );

    let cfg = ServiceConfig::from_kdl(&kdl).unwrap();
    cfg.validate().unwrap();
    let built = ServiceBuilder::empty().connector(conn).build(&cfg).unwrap();
    built
        .into_iter()
        .next()
        .unwrap()
        .run(CancellationToken::new())
        .await
        .unwrap();

    let batches = captured.lock().unwrap();
    assert_eq!(batches.len(), 1);
    let b = &batches[0];
    assert_eq!(b.num_rows(), 1);
    assert_eq!(b.column(0).as_primitive::<Int64Type>().value(0), 1_000);
    assert_eq!(b.column(1).as_primitive::<Int64Type>().value(0), 16_000);
    assert_eq!(b.column(2).as_string::<i32>().value(0), "AAPL");
    assert_eq!(b.column(3).as_primitive::<Int64Type>().value(0), 2);
    assert!((b.column(4).as_primitive::<Float64Type>().value(0) - 15.0).abs() < 1e-9);
}

#[tokio::test]
async fn kdl_declarative_route_splits_rows_and_feeds_downstream_operators() {
    let dir = tempfile::tempdir().unwrap();
    let order_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("priority", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
    ]));
    let high_proj_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("fee", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        order_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["high", "high", "low", "other"])),
            Arc::new(Float64Array::from(vec![15_000.0, 200.0, 50.0, 10.0])),
        ],
    )
    .unwrap();

    let conn = MemConnector::new();
    conn.add_source("orders", order_schema.clone(), vec![batch]);
    let vip_out = conn.add_sink("vip", order_schema.clone());
    let high_out = conn.add_sink("high", high_proj_schema);
    let std_out = conn.add_sink("standard", order_schema);

    let kdl = format!(
        r#"
mode "standalone"
node id=1 data_dir="{dir}"
run "once"

workflow "declarative_branching" {{
    source "in" connector="mem" component="Order" {{ config key="orders" }}

    operator "router" kind="route" component="Order" {{
        config mode="row" exclusive=#true default="standard" {{
            branches {{
                vip  "amount >= 10000.0 AND priority = 'high'"
                high "priority = 'high'"
            }}
        }}
    }}

    operator "high_fee" kind="project" component="Order" into="HighFee" {{
        config {{
            select "id"
            select "amount * 0.05 AS fee"
        }}
    }}

    sink "out_vip"      connector="mem" component="Order"   {{ config key="vip" }}
    sink "out_high"     connector="mem" component="HighFee" {{ config key="high" }}
    sink "out_standard" connector="mem" component="Order"   {{ config key="standard" }}

    link from="in" to="router"
    link from="router" to="out_vip"      branch="vip"
    link from="router" to="high_fee"     branch="high"
    link from="high_fee" to="out_high"
    link from="router" to="out_standard" branch="standard"
}}
"#,
        dir = dir.path().display()
    );

    let cfg = ServiceConfig::from_kdl(&kdl).unwrap();
    cfg.validate().unwrap();
    let built = ServiceBuilder::empty().connector(conn).build(&cfg).unwrap();
    built
        .into_iter()
        .next()
        .unwrap()
        .run(CancellationToken::new())
        .await
        .unwrap();

    let vip = vip_out.lock().unwrap();
    assert_eq!(vip.len(), 1);
    assert_eq!(vip[0].column(0).as_primitive::<Int64Type>().values(), &[1]);

    let high = high_out.lock().unwrap();
    assert_eq!(high.len(), 1);
    assert_eq!(high[0].column(0).as_primitive::<Int64Type>().values(), &[2]);
    assert!((high[0].column(1).as_primitive::<Float64Type>().value(0) - 10.0).abs() < 1e-9);

    let standard = std_out.lock().unwrap();
    assert_eq!(standard.len(), 1);
    assert_eq!(
        standard[0].column(0).as_primitive::<Int64Type>().values(),
        &[3, 4]
    );
}

#[test]
fn kdl_window_and_route_validation_rejects_bad_configs_and_roundtrips() {
    let missing_window_kdl = r#"
mode "standalone"
node id=1 data_dir="/tmp/saci"
workflow "w" {
    operator "win" kind="window" component="Sale" into="Totals" {
        config aggregates="count(*) AS n"
    }
}
"#;
    let err = ServiceConfig::from_kdl(missing_window_kdl)
        .and_then(|c| c.validate())
        .unwrap_err()
        .to_string();
    assert!(err.contains("window"), "got: {err}");

    let route_kdl = r#"
mode "standalone"
node id=1 data_dir="/tmp/saci"
workflow "w" {
    operator "r" kind="branch" component="Order" {
        config mode="first_row" default="other" {
            branches {
                high "priority = 'high'"
            }
        }
    }
}
"#;
    let cfg = ServiceConfig::from_kdl(route_kdl).unwrap();
    cfg.validate().unwrap();
    let serialized = cfg.to_kdl().unwrap();
    let reparsed = ServiceConfig::from_kdl(&serialized).unwrap();
    reparsed.validate().unwrap();
    assert_eq!(reparsed.workflows[0].operator[0].kind, "branch");
}
