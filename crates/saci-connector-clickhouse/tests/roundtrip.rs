//! Round-trip integration tests between [`ClickhouseSink`] and [`ClickhouseSource`].

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array, Date32Array, Decimal128Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use saci_connector_clickhouse::config::{
    ClickhouseConnectionConfig, ClickhouseFieldType, ClickhouseSinkConfig, ClickhouseSourceConfig,
    CompressionConfig, DumpMode, FieldSpec, PollingMode, QueryMode, SourceMode, WriteMode,
};
use saci_connector_clickhouse::{ClickhouseSink, ClickhouseSource};
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;

#[tokio::test]
async fn clickhouse_roundtrip_sink_and_source() {
    let Some(ch) = common::try_start().await else {
        return;
    };

    let client = ch.client();
    let table = "test_roundtrip";

    // Setup table
    client
        .query(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                id UInt64,
                name String,
                score Float64
            ) ENGINE = MergeTree ORDER BY id"
        ))
        .execute()
        .await
        .expect("table created");

    let conn_cfg = ClickhouseConnectionConfig {
        url: ch.url.clone(),
        user: None,
        password: None,
        database: "default".to_string(),
        query_timeout_ms: 30000,
        compression: CompressionConfig::Lz4,
        settings: HashMap::new(),
    };

    let schema_fields = vec![
        FieldSpec {
            name: "id".to_string(),
            ty: ClickhouseFieldType::UInt64,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "name".to_string(),
            ty: ClickhouseFieldType::Utf8,
            nullable: true,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "score".to_string(),
            ty: ClickhouseFieldType::Float64,
            nullable: true,
            precision: None,
            scale: None,
        },
    ];

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
    ]));

    // 1. Write data using ClickhouseSink
    let sink_cfg = ClickhouseSinkConfig {
        name: "sink_roundtrip".to_string(),
        connection: conn_cfg.clone(),
        table: table.to_string(),
        schema_fields: schema_fields.clone(),
        write_mode: WriteMode::Append,
        chunk_rows: 1000,
        flush_rows: 2,
        truncate_before_first_write: true,
    };

    let mut sink = ClickhouseSink::new(sink_cfg).expect("creates sink");

    let batch1 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("Alice"), Some("Bob")])),
            Arc::new(Float64Array::from(vec![Some(95.5), Some(88.0)])),
        ],
    )
    .expect("batch1");

    let batch2 = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![3])),
            Arc::new(StringArray::from(vec![Some("Charlie")])),
            Arc::new(Float64Array::from(vec![Some(72.3)])),
        ],
    )
    .expect("batch2");

    sink.write_batch(&batch1).await.expect("writes batch1");
    sink.write_batch(&batch2).await.expect("writes batch2");
    sink.finish().await.expect("finishes sink");

    // 2. Read data using ClickhouseSource in Polling mode
    let source_cfg = ClickhouseSourceConfig {
        name: "source_polling".to_string(),
        connection: conn_cfg.clone(),
        mode: SourceMode::Polling(PollingMode {
            table: table.to_string(),
            cursor_column: "id".to_string(),
            initial_cursor: None,
            tiebreak_column: None,
        }),
        schema_fields: schema_fields.clone(),
        batch_rows: 2,
        max_batches_per_cycle: 0,
    };

    let mut source = ClickhouseSource::new(source_cfg).expect("creates source");

    let mut total_rows = 0;
    while let Some(batch) = source.next_batch().await.expect("reads batch") {
        total_rows += batch.num_rows();
    }
    assert_eq!(total_rows, 3, "polling mode should read all 3 rows");

    // Next drain cycle yields None
    assert!(source.next_batch().await.expect("next batch").is_none());

    // 3. Read data using ClickhouseSource in Dump mode
    let dump_cfg = ClickhouseSourceConfig {
        name: "source_dump".to_string(),
        connection: conn_cfg.clone(),
        mode: SourceMode::Dump(DumpMode {
            table: table.to_string(),
        }),
        schema_fields: schema_fields.clone(),
        batch_rows: 2,
        max_batches_per_cycle: 0,
    };

    let mut dump_source = ClickhouseSource::new(dump_cfg).expect("creates dump source");
    let mut dump_rows = 0;
    while let Some(batch) = dump_source.next_batch().await.expect("reads dump batch") {
        dump_rows += batch.num_rows();
    }
    assert_eq!(dump_rows, 3, "dump mode should read all 3 rows");

    // 4. Read data using ClickhouseSource in Query mode
    let query_cfg = ClickhouseSourceConfig {
        name: "source_query".to_string(),
        connection: conn_cfg.clone(),
        mode: SourceMode::Query(QueryMode {
            query: format!("SELECT id, name, score FROM {table} WHERE score > 80 ORDER BY id"),
        }),
        schema_fields: schema_fields.clone(),
        batch_rows: 10,
        max_batches_per_cycle: 0,
    };

    let mut query_source = ClickhouseSource::new(query_cfg).expect("creates query source");
    let mut filtered_rows = 0;
    while let Some(batch) = query_source.next_batch().await.expect("reads query batch") {
        filtered_rows += batch.num_rows();
    }
    assert_eq!(
        filtered_rows, 2,
        "query mode with filter should return 2 rows"
    );
}

#[tokio::test]
async fn clickhouse_types_roundtrip() {
    let Some(ch) = common::try_start().await else {
        return;
    };

    let client = ch.client();
    let table = "test_types_roundtrip";

    client
        .query(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                c_i8 Int8,
                c_i16 Int16,
                c_i32 Int32,
                c_i64 Int64,
                c_u8 UInt8,
                c_u16 UInt16,
                c_u32 UInt32,
                c_u64 UInt64,
                c_f32 Float32,
                c_f64 Float64,
                c_str Nullable(String),
                c_date Nullable(Date32),
                c_dec Nullable(Decimal(18, 4))
            ) ENGINE = MergeTree ORDER BY c_i64"
        ))
        .execute()
        .await
        .expect("table created");

    let conn_cfg = ClickhouseConnectionConfig {
        url: ch.url.clone(),
        user: None,
        password: None,
        database: "default".to_string(),
        query_timeout_ms: 30000,
        compression: CompressionConfig::None,
        settings: HashMap::new(),
    };

    let schema_fields = vec![
        FieldSpec {
            name: "c_i8".into(),
            ty: ClickhouseFieldType::Int8,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "c_i16".into(),
            ty: ClickhouseFieldType::Int16,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "c_i32".into(),
            ty: ClickhouseFieldType::Int32,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "c_i64".into(),
            ty: ClickhouseFieldType::Int64,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "c_u8".into(),
            ty: ClickhouseFieldType::UInt8,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "c_u16".into(),
            ty: ClickhouseFieldType::UInt16,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "c_u32".into(),
            ty: ClickhouseFieldType::UInt32,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "c_u64".into(),
            ty: ClickhouseFieldType::UInt64,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "c_f32".into(),
            ty: ClickhouseFieldType::Float32,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "c_f64".into(),
            ty: ClickhouseFieldType::Float64,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "c_str".into(),
            ty: ClickhouseFieldType::Utf8,
            nullable: true,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "c_date".into(),
            ty: ClickhouseFieldType::Date32,
            nullable: true,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "c_dec".into(),
            ty: ClickhouseFieldType::Decimal128,
            nullable: true,
            precision: Some(18),
            scale: Some(4),
        },
    ];

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("c_i8", DataType::Int8, false),
        Field::new("c_i16", DataType::Int16, false),
        Field::new("c_i32", DataType::Int32, false),
        Field::new("c_i64", DataType::Int64, false),
        Field::new("c_u8", DataType::UInt8, false),
        Field::new("c_u16", DataType::UInt16, false),
        Field::new("c_u32", DataType::UInt32, false),
        Field::new("c_u64", DataType::UInt64, false),
        Field::new("c_f32", DataType::Float32, false),
        Field::new("c_f64", DataType::Float64, false),
        Field::new("c_str", DataType::Utf8, true),
        Field::new("c_date", DataType::Date32, true),
        Field::new("c_dec", DataType::Decimal128(18, 4), true),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&arrow_schema),
        vec![
            Arc::new(Int8Array::from(vec![-8, 12])),
            Arc::new(Int16Array::from(vec![-160, 320])),
            Arc::new(Int32Array::from(vec![-32000, 64000])),
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(UInt8Array::from(vec![8, 250])),
            Arc::new(UInt16Array::from(vec![160, 65000])),
            Arc::new(UInt32Array::from(vec![32000, 4000000])),
            Arc::new(UInt64Array::from(vec![1000, 2000])),
            Arc::new(Float32Array::from(vec![1.5, -2.5])),
            Arc::new(Float64Array::from(vec![10.125, -20.25])),
            Arc::new(StringArray::from(vec![Some("non-null"), None])),
            Arc::new(Date32Array::from(vec![Some(19000), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(1234500), None])
                    .with_precision_and_scale(18, 4)
                    .unwrap(),
            ),
        ],
    )
    .expect("batch creates");

    let sink_cfg = ClickhouseSinkConfig {
        name: "types_sink".into(),
        connection: conn_cfg.clone(),
        table: table.into(),
        schema_fields: schema_fields.clone(),
        write_mode: WriteMode::Append,
        chunk_rows: 1000,
        flush_rows: 10,
        truncate_before_first_write: true,
    };

    let mut sink = ClickhouseSink::new(sink_cfg).expect("creates sink");
    sink.write_batch(&batch).await.expect("writes batch");
    sink.finish().await.expect("finishes sink");

    let source_cfg = ClickhouseSourceConfig {
        name: "types_source".into(),
        connection: conn_cfg,
        mode: SourceMode::Dump(DumpMode {
            table: table.into(),
        }),
        schema_fields,
        batch_rows: 10,
        max_batches_per_cycle: 0,
    };

    let mut source = ClickhouseSource::new(source_cfg).expect("creates source");
    let read_batch = source
        .next_batch()
        .await
        .expect("reads batch")
        .expect("has batch");
    assert_eq!(read_batch.num_rows(), 2);

    let i64_col = read_batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(i64_col.value(0), 1);
    assert_eq!(i64_col.value(1), 2);

    let str_col = read_batch
        .column(10)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(str_col.value(0), "non-null");
    assert!(str_col.is_null(1));

    let dec_col = read_batch
        .column(12)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(dec_col.value(0), 1234500);
    assert!(dec_col.is_null(1));
}

#[tokio::test]
async fn clickhouse_polling_with_tiebreak_and_initial_cursor() {
    let Some(ch) = common::try_start().await else {
        return;
    };

    let client = ch.client();
    let table = "test_polling_tiebreak";

    client
        .query(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                ts Int64,
                seq UInt64,
                val String
            ) ENGINE = MergeTree ORDER BY (ts, seq)"
        ))
        .execute()
        .await
        .expect("table created");

    client
        .query(&format!(
            "INSERT INTO {table} (ts, seq, val) VALUES (10, 1, 'a'), (10, 2, 'b'), (20, 1, 'c'), (30, 1, 'd')"
        ))
        .execute()
        .await
        .expect("seed data");

    let conn_cfg = ClickhouseConnectionConfig {
        url: ch.url.clone(),
        user: None,
        password: None,
        database: "default".to_string(),
        query_timeout_ms: 30000,
        compression: CompressionConfig::None,
        settings: HashMap::new(),
    };

    let schema_fields = vec![
        FieldSpec {
            name: "ts".into(),
            ty: ClickhouseFieldType::Int64,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "seq".into(),
            ty: ClickhouseFieldType::UInt64,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "val".into(),
            ty: ClickhouseFieldType::Utf8,
            nullable: false,
            precision: None,
            scale: None,
        },
    ];

    let source_cfg = ClickhouseSourceConfig {
        name: "polling_tiebreak_source".into(),
        connection: conn_cfg,
        mode: SourceMode::Polling(PollingMode {
            table: table.into(),
            cursor_column: "ts".into(),
            initial_cursor: Some("10".into()),
            tiebreak_column: Some("seq".into()),
        }),
        schema_fields,
        batch_rows: 10,
        max_batches_per_cycle: 0,
    };

    let mut source = ClickhouseSource::new(source_cfg).expect("creates source");
    let mut rows_read = 0;
    while let Some(batch) = source.next_batch().await.expect("reads batch") {
        rows_read += batch.num_rows();
        let ts_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            assert!(ts_col.value(i) >= 10);
        }
    }
    assert_eq!(
        rows_read, 2,
        "initial_cursor=10 with tiebreak should read ts > 10 rows"
    );
}

#[tokio::test]
async fn clickhouse_sink_buffering_and_truncate() {
    let Some(ch) = common::try_start().await else {
        return;
    };

    let client = ch.client();
    let table = "test_sink_truncate_fresh";

    client
        .query(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                id UInt64,
                val String
            ) ENGINE = MergeTree ORDER BY id"
        ))
        .execute()
        .await
        .expect("table created");

    // Seed 5 existing rows
    client
        .query(&format!(
            "INSERT INTO {table} (id, val) VALUES (1, 'old1'), (2, 'old2'), (3, 'old3'), (4, 'old4'), (5, 'old5')"
        ))
        .execute()
        .await
        .expect("seed old rows");

    let conn_cfg = ClickhouseConnectionConfig {
        url: ch.url.clone(),
        user: None,
        password: None,
        database: "default".to_string(),
        query_timeout_ms: 30000,
        compression: CompressionConfig::None,
        settings: HashMap::new(),
    };

    let schema_fields = vec![
        FieldSpec {
            name: "id".into(),
            ty: ClickhouseFieldType::UInt64,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "val".into(),
            ty: ClickhouseFieldType::Utf8,
            nullable: false,
            precision: None,
            scale: None,
        },
    ];

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("val", DataType::Utf8, false),
    ]));

    let sink_cfg = ClickhouseSinkConfig {
        name: "truncate_sink".into(),
        connection: conn_cfg.clone(),
        table: table.into(),
        schema_fields: schema_fields.clone(),
        write_mode: WriteMode::Append,
        chunk_rows: 1000,
        flush_rows: 50,
        truncate_before_first_write: true,
    };

    let mut sink = ClickhouseSink::new(sink_cfg).expect("creates sink");

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(UInt64Array::from(vec![100, 200])),
            Arc::new(StringArray::from(vec!["new1", "new2"])),
        ],
    )
    .expect("batch");

    sink.write_batch(&batch).await.expect("writes batch");
    assert_eq!(sink.pending_rows(), Some(2));
    sink.finish().await.expect("finishes and flushes sink");
    assert_eq!(sink.pending_rows(), Some(0));

    // Dump table and verify only 2 new rows exist
    let source_cfg = ClickhouseSourceConfig {
        name: "verify_source".into(),
        connection: conn_cfg,
        mode: SourceMode::Dump(DumpMode {
            table: table.into(),
        }),
        schema_fields,
        batch_rows: 10,
        max_batches_per_cycle: 0,
    };

    let mut source = ClickhouseSource::new(source_cfg).expect("creates source");
    let dump = source
        .next_batch()
        .await
        .expect("reads batch")
        .expect("batch exists");
    assert_eq!(dump.num_rows(), 2, "old rows should have been truncated");
}

#[tokio::test]
async fn clickhouse_source_batch_size_and_limits() {
    let Some(ch) = common::try_start().await else {
        return;
    };

    let client = ch.client();
    let table = "test_source_batch_limits";

    client
        .query(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                id UInt64
            ) ENGINE = MergeTree ORDER BY id"
        ))
        .execute()
        .await
        .expect("table created");

    // Seed 25 rows
    let vals: Vec<String> = (1..=25).map(|i| format!("({i})")).collect();
    client
        .query(&format!(
            "INSERT INTO {table} (id) VALUES {}",
            vals.join(", ")
        ))
        .execute()
        .await
        .expect("seed 25 rows");

    let conn_cfg = ClickhouseConnectionConfig {
        url: ch.url.clone(),
        user: None,
        password: None,
        database: "default".to_string(),
        query_timeout_ms: 30000,
        compression: CompressionConfig::None,
        settings: HashMap::new(),
    };

    let schema_fields = vec![FieldSpec {
        name: "id".into(),
        ty: ClickhouseFieldType::UInt64,
        nullable: false,
        precision: None,
        scale: None,
    }];

    let source_cfg = ClickhouseSourceConfig {
        name: "limits_source".into(),
        connection: conn_cfg,
        mode: SourceMode::Dump(DumpMode {
            table: table.into(),
        }),
        schema_fields,
        batch_rows: 10,
        max_batches_per_cycle: 2,
    };

    let mut source = ClickhouseSource::new(source_cfg).expect("creates source");

    // Cycle 1: should return at most 2 batches
    let b1 = source.next_batch().await.expect("b1").expect("has b1");
    assert_eq!(b1.num_rows(), 10);
    let b2 = source.next_batch().await.expect("b2").expect("has b2");
    assert_eq!(b2.num_rows(), 10);
    let cycle1_end = source.next_batch().await.expect("cycle1 end");
    assert!(cycle1_end.is_none(), "cycle limit reached, returns None");

    // Cycle 2: resumes and returns remaining 5 rows
    let b3 = source.next_batch().await.expect("b3").expect("has b3");
    assert_eq!(b3.num_rows(), 5);
    let cycle2_end = source.next_batch().await.expect("cycle2 end");
    assert!(cycle2_end.is_none(), "table exhausted, returns None");
}

#[tokio::test]
async fn clickhouse_sink_chunks_a_write_larger_than_chunk_rows() {
    let Some(ch) = common::try_start().await else {
        return;
    };

    let client = ch.client();
    let table = "test_sink_chunk_rows";

    client
        .query(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                id UInt64,
                val String
            ) ENGINE = MergeTree ORDER BY id"
        ))
        .execute()
        .await
        .expect("table created");

    let conn_cfg = ClickhouseConnectionConfig {
        url: ch.url.clone(),
        user: None,
        password: None,
        database: "default".to_string(),
        query_timeout_ms: 30000,
        compression: CompressionConfig::None,
        settings: HashMap::new(),
    };

    let schema_fields = vec![
        FieldSpec {
            name: "id".into(),
            ty: ClickhouseFieldType::UInt64,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "val".into(),
            ty: ClickhouseFieldType::Utf8,
            nullable: false,
            precision: None,
            scale: None,
        },
    ];

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("val", DataType::Utf8, false),
    ]));

    // `flush_rows 0` flushes on the first write, so the single 10-row batch is
    // split by `chunk_rows 3` into 3 + 3 + 3 + 1 INSERTs before it lands.
    let sink_cfg = ClickhouseSinkConfig {
        name: "chunked_sink".into(),
        connection: conn_cfg.clone(),
        table: table.into(),
        schema_fields: schema_fields.clone(),
        write_mode: WriteMode::Append,
        chunk_rows: 3,
        flush_rows: 0,
        truncate_before_first_write: true,
    };

    let mut sink = ClickhouseSink::new(sink_cfg).expect("creates sink");
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(UInt64Array::from((1..=10).collect::<Vec<u64>>())),
            Arc::new(StringArray::from(
                (1..=10).map(|i| format!("v{i}")).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch");

    sink.write_batch(&batch).await.expect("writes batch");
    sink.finish().await.expect("finishes");

    let source_cfg = ClickhouseSourceConfig {
        name: "chunk_verify".into(),
        connection: conn_cfg,
        mode: SourceMode::Dump(DumpMode {
            table: table.into(),
        }),
        schema_fields,
        batch_rows: 100,
        max_batches_per_cycle: 0,
    };

    let mut source = ClickhouseSource::new(source_cfg).expect("creates source");
    let mut total = 0usize;
    while let Some(batch) = source.next_batch().await.expect("reads batch") {
        total += batch.num_rows();
    }
    assert_eq!(
        total, 10,
        "every row lands exactly once across the chunked inserts"
    );
}

#[tokio::test]
async fn clickhouse_polling_resumes_a_timestamp_cursor_across_batches() {
    let Some(ch) = common::try_start().await else {
        return;
    };

    let client = ch.client();
    let table = "test_polling_timestamp_cursor";

    client
        .query(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                ts DateTime64(3),
                id UInt64
            ) ENGINE = MergeTree ORDER BY (ts, id)"
        ))
        .execute()
        .await
        .expect("table created");

    client
        .query(&format!(
            "INSERT INTO {table} (ts, id) VALUES
                ('2020-01-01 00:00:00', 1),
                ('2020-01-02 00:00:00', 2),
                ('2020-01-03 00:00:00', 3),
                ('2020-01-04 00:00:00', 4)"
        ))
        .execute()
        .await
        .expect("seed data");

    let conn_cfg = ClickhouseConnectionConfig {
        url: ch.url.clone(),
        user: None,
        password: None,
        database: "default".to_string(),
        query_timeout_ms: 30000,
        compression: CompressionConfig::None,
        settings: HashMap::new(),
    };

    let schema_fields = vec![
        FieldSpec {
            name: "ts".into(),
            ty: ClickhouseFieldType::TimestampMillis,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "id".into(),
            ty: ClickhouseFieldType::UInt64,
            nullable: false,
            precision: None,
            scale: None,
        },
    ];

    // `batch_rows 2` over three matching rows forces a second poll whose cursor
    // comes from the first batch rather than the config. A raw epoch integer
    // there is read by ClickHouse as micros, so a millisecond cursor re-reads
    // from 1970: this is the regression the datetime literal pins.
    let source_cfg = ClickhouseSourceConfig {
        name: "timestamp_cursor_source".into(),
        connection: conn_cfg,
        mode: SourceMode::Polling(PollingMode {
            table: table.into(),
            cursor_column: "ts".into(),
            initial_cursor: Some("2020-01-01 00:00:00".into()),
            tiebreak_column: None,
        }),
        schema_fields,
        batch_rows: 2,
        max_batches_per_cycle: 0,
    };

    let mut source = ClickhouseSource::new(source_cfg).expect("creates source");
    let mut ids: Vec<u64> = Vec::new();
    while let Some(batch) = source.next_batch().await.expect("reads batch") {
        let id_col = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("uint64 id");
        ids.extend((0..batch.num_rows()).map(|i| id_col.value(i)));
    }
    assert_eq!(
        ids,
        vec![2, 3, 4],
        "every poll resumes from the datetime cursor, not the raw epoch integer"
    );
}

#[tokio::test]
async fn clickhouse_polling_resumes_a_date_cursor_across_batches() {
    let Some(ch) = common::try_start().await else {
        return;
    };

    let client = ch.client();
    let table = "test_polling_date_cursor";

    client
        .query(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                d Date,
                id UInt64
            ) ENGINE = MergeTree ORDER BY (d, id)"
        ))
        .execute()
        .await
        .expect("table created");

    client
        .query(&format!(
            "INSERT INTO {table} (d, id) VALUES
                ('2020-01-01', 1),
                ('2020-01-02', 2),
                ('2020-01-03', 3)"
        ))
        .execute()
        .await
        .expect("seed data");

    let conn_cfg = ClickhouseConnectionConfig {
        url: ch.url.clone(),
        user: None,
        password: None,
        database: "default".to_string(),
        query_timeout_ms: 30000,
        compression: CompressionConfig::None,
        settings: HashMap::new(),
    };

    let schema_fields = vec![
        FieldSpec {
            name: "d".into(),
            ty: ClickhouseFieldType::Date32,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "id".into(),
            ty: ClickhouseFieldType::UInt64,
            nullable: false,
            precision: None,
            scale: None,
        },
    ];

    // A `Date` cursor as a raw day count (`'18262'`) is a server-side parse
    // error, so every poll after the first needs the date literal.
    let source_cfg = ClickhouseSourceConfig {
        name: "date_cursor_source".into(),
        connection: conn_cfg,
        mode: SourceMode::Polling(PollingMode {
            table: table.into(),
            cursor_column: "d".into(),
            initial_cursor: Some("2020-01-01".into()),
            tiebreak_column: None,
        }),
        schema_fields,
        batch_rows: 1,
        max_batches_per_cycle: 0,
    };

    let mut source = ClickhouseSource::new(source_cfg).expect("creates source");
    let mut ids: Vec<u64> = Vec::new();
    while let Some(batch) = source.next_batch().await.expect("reads batch") {
        let id_col = batch
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("uint64 id");
        ids.extend((0..batch.num_rows()).map(|i| id_col.value(i)));
    }
    assert_eq!(ids, vec![2, 3], "every poll resumes from the date literal");
}

#[tokio::test]
async fn clickhouse_date_column_round_trips() {
    let Some(ch) = common::try_start().await else {
        return;
    };

    let client = ch.client();
    let table = "test_date_column";

    client
        .query(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                d Date,
                id UInt64
            ) ENGINE = MergeTree ORDER BY id"
        ))
        .execute()
        .await
        .expect("table created");

    let conn_cfg = ClickhouseConnectionConfig {
        url: ch.url.clone(),
        user: None,
        password: None,
        database: "default".to_string(),
        query_timeout_ms: 30000,
        compression: CompressionConfig::None,
        settings: HashMap::new(),
    };

    let schema_fields = vec![
        FieldSpec {
            name: "d".into(),
            ty: ClickhouseFieldType::Date32,
            nullable: false,
            precision: None,
            scale: None,
        },
        FieldSpec {
            name: "id".into(),
            ty: ClickhouseFieldType::UInt64,
            nullable: false,
            precision: None,
            scale: None,
        },
    ];

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("d", DataType::Date32, false),
        Field::new("id", DataType::UInt64, false),
    ]));

    let sink_cfg = ClickhouseSinkConfig {
        name: "date_sink".into(),
        connection: conn_cfg.clone(),
        table: table.into(),
        schema_fields: schema_fields.clone(),
        write_mode: WriteMode::Append,
        chunk_rows: 1000,
        flush_rows: 0,
        truncate_before_first_write: true,
    };

    let mut sink = ClickhouseSink::new(sink_cfg).expect("creates sink");
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Date32Array::from(vec![18262, 18263])),
            Arc::new(UInt64Array::from(vec![1, 2])),
        ],
    )
    .expect("batch");

    sink.write_batch(&batch)
        .await
        .expect("writes a Date column");
    sink.finish().await.expect("finishes");

    // ArrowStream sends the column back as `UInt16`; the source widens it to the
    // declared `date32`.
    let source_cfg = ClickhouseSourceConfig {
        name: "date_source".into(),
        connection: conn_cfg,
        mode: SourceMode::Dump(DumpMode {
            table: table.into(),
        }),
        schema_fields,
        batch_rows: 100,
        max_batches_per_cycle: 0,
    };

    let mut source = ClickhouseSource::new(source_cfg).expect("creates source");
    let read_batch = source
        .next_batch()
        .await
        .expect("reads batch")
        .expect("has batch");
    assert_eq!(read_batch.num_rows(), 2);
    let dates = read_batch
        .column(0)
        .as_any()
        .downcast_ref::<Date32Array>()
        .expect("date32 column");
    assert_eq!(dates.value(0), 18262);
    assert_eq!(dates.value(1), 18263);
}
