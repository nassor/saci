//! `polling` and `cdc_trigger` against a real PostgreSQL server.
//!
//! Soft-skips without Docker; see `common::try_start`.

mod common;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use saci_connector::from_kdl_str;
use saci_connector_postgresql::{PostgresSource, PostgresSourceConfig};
use saci_core::io::source::Source;
use serde::Deserialize as _;

/// Build a source config from a KDL fragment plus the container's DSN.
fn source(dsn: &str, body: &str) -> PostgresSource {
    let text = format!(
        "{body}\n\nconnection dsn={} sslmode=\"disable\"\n",
        common::quoted(dsn)
    );
    let cfg = PostgresSourceConfig::deserialize(from_kdl_str(&text).expect("parse kdl"))
        .expect("parse config");
    PostgresSource::new(cfg).expect("build source")
}

/// Drain one cycle, returning every batch until `Ok(None)`.
async fn drain(source: &mut PostgresSource) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    while let Some(batch) = source.next_batch().await.expect("next_batch") {
        batches.push(batch);
    }
    batches
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column_by_name("id")
                .expect("id column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 id");
            (0..column.len())
                .map(|row| column.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

const PRICES_FIELDS: &str = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"label\" type=\"utf8\"
";

#[tokio::test]
async fn polling_batches_the_table_then_resumes_from_the_persisted_offset() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE prices (id bigint PRIMARY KEY, label text); \
             INSERT INTO prices SELECT g, 'p' || g FROM generate_series(1, 10) g",
        )
        .await
        .expect("fixture");

    let mut source = source(
        &pg.dsn(),
        &format!(
            "name \"prices\"\nbatch_rows 4\n\n\
             mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n\
             {PRICES_FIELDS}"
        ),
    );

    let batches = drain(&mut source).await;
    assert_eq!(
        batches
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        vec![4, 4, 2],
        "batch_rows = 4 over 10 rows"
    );
    assert_eq!(ids(&batches), (1..=10).collect::<Vec<_>>());

    let labels = batches[0]
        .column_by_name("label")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(labels.value(0), "p1");

    // A second cycle sees only the new rows: the in-memory cursor survived, and
    // the offset was committed at the start of this cycle.
    client
        .batch_execute("INSERT INTO prices SELECT g, 'p' || g FROM generate_series(11, 13) g")
        .await
        .expect("insert more");

    let batches = drain(&mut source).await;
    assert_eq!(ids(&batches), vec![11, 12, 13]);

    // The offset table now holds the last emitted cursor.
    let stored: String = client
        .query_one(
            "SELECT cursor_value FROM saci_source_offsets WHERE source_name = 'prices'",
            &[],
        )
        .await
        .expect("offset row")
        .get(0);
    assert_eq!(stored, "10", "the offset is committed one cycle late");
}

#[tokio::test]
async fn a_new_source_resumes_from_the_offset_table() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE prices (id bigint PRIMARY KEY, label text); \
             INSERT INTO prices SELECT g, 'p' || g FROM generate_series(1, 5) g",
        )
        .await
        .expect("fixture");

    let config = format!(
        "name \"prices\"\nbatch_rows 100\n\n\
         mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n\
         {PRICES_FIELDS}"
    );

    {
        let mut first = source(&pg.dsn(), &config);
        assert_eq!(ids(&drain(&mut first).await), (1..=5).collect::<Vec<_>>());
        // A second cycle commits the offset, then finds nothing.
        assert!(drain(&mut first).await.is_empty());
    }

    client
        .batch_execute("INSERT INTO prices SELECT g, 'p' || g FROM generate_series(6, 7) g")
        .await
        .expect("insert more");

    let mut second = source(&pg.dsn(), &config);
    assert_eq!(
        ids(&drain(&mut second).await),
        vec![6, 7],
        "a fresh source must resume from the persisted offset, not replay"
    );
}

#[tokio::test]
async fn initial_now_skips_the_existing_rows() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE prices (id bigint PRIMARY KEY, label text); \
             INSERT INTO prices SELECT g, 'p' || g FROM generate_series(1, 5) g",
        )
        .await
        .expect("fixture");

    let mut source = source(
        &pg.dsn(),
        &format!(
            "name \"prices\"\n\n\
             mode kind=\"polling\" table=\"prices\" cursor_column=\"id\" \
             initial=\"now\"\n{PRICES_FIELDS}"
        ),
    );
    assert!(
        drain(&mut source).await.is_empty(),
        "initial = \"now\" must start after the current maximum"
    );

    client
        .batch_execute("INSERT INTO prices VALUES (6, 'p6')")
        .await
        .expect("insert");
    assert_eq!(ids(&drain(&mut source).await), vec![6]);
}

#[tokio::test]
async fn a_tiebreak_column_keeps_a_non_unique_cursor_lossless() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    // Five rows sharing one updated_at, read two at a time: without a tiebreak
    // the second query's `updated_at > $1` predicate excludes the rest of the
    // run, and rows are lost.
    client
        .batch_execute(
            "CREATE TABLE events (id bigint PRIMARY KEY, updated_at timestamptz NOT NULL); \
             INSERT INTO events SELECT g, '2024-01-01 00:00:00+00'::timestamptz \
             FROM generate_series(1, 5) g",
        )
        .await
        .expect("fixture");

    let fields = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"updated_at\" type=\"timestamp_micros_utc\" nullable=#false
";

    let with_tiebreak = source(
        &pg.dsn(),
        &format!(
            "name \"with_tie\"\nbatch_rows 2\n\n\
             mode kind=\"polling\" table=\"events\" \
             cursor_column=\"updated_at\" tiebreak_column=\"id\"\n{fields}"
        ),
    );
    let mut with_tiebreak = with_tiebreak;
    let mut got = ids(&drain(&mut with_tiebreak).await);
    got.sort_unstable();
    assert_eq!(got, vec![1, 2, 3, 4, 5], "the tiebreak must lose no rows");

    // The documented failure the option exists to prevent.
    let mut without = source(
        &pg.dsn(),
        &format!(
            "name \"no_tie\"\nbatch_rows 2\n\n\
             mode kind=\"polling\" table=\"events\" \
             cursor_column=\"updated_at\"\n{fields}"
        ),
    );
    let lost = ids(&drain(&mut without).await);
    assert_eq!(
        lost.len(),
        2,
        "without a tiebreak the first batch is all that survives the boundary"
    );
}

#[tokio::test]
async fn a_where_clause_narrows_every_query() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE prices (id bigint PRIMARY KEY, label text); \
             INSERT INTO prices SELECT g, CASE WHEN g % 2 = 0 THEN 'keep' ELSE 'drop' END \
             FROM generate_series(1, 10) g",
        )
        .await
        .expect("fixture");

    let mut source = source(
        &pg.dsn(),
        &format!(
            "name \"prices\"\nbatch_rows 3\n\n\
             mode kind=\"polling\" table=\"prices\" cursor_column=\"id\" \
             where_clause=\"label = 'keep'\"\n{PRICES_FIELDS}"
        ),
    );
    assert_eq!(ids(&drain(&mut source).await), vec![2, 4, 6, 8, 10]);
}

#[tokio::test]
async fn max_batches_per_cycle_ends_the_cycle_early_and_resumes() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE prices (id bigint PRIMARY KEY, label text); \
             INSERT INTO prices SELECT g, 'p' || g FROM generate_series(1, 10) g",
        )
        .await
        .expect("fixture");

    let mut source = source(
        &pg.dsn(),
        &format!(
            "name \"prices\"\nbatch_rows 3\nmax_batches_per_cycle 2\n\n\
             mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n\
             {PRICES_FIELDS}"
        ),
    );
    assert_eq!(ids(&drain(&mut source).await), vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(ids(&drain(&mut source).await), vec![7, 8, 9, 10]);
}

#[tokio::test]
async fn cdc_trigger_with_delete_acked_prunes_the_outbox() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE outbox (seq bigserial PRIMARY KEY, payload text); \
             INSERT INTO outbox (payload) SELECT 'p' || g FROM generate_series(1, 6) g",
        )
        .await
        .expect("fixture");

    let mut source = source(
        &pg.dsn(),
        "name \"outbox\"\nbatch_rows 10\n\n\
         mode kind=\"cdc_trigger\" table=\"outbox\" cursor_column=\"seq\" \
         retention=\"delete_acked\"\n\n\
         schema_fields \"seq\" type=\"int64\" nullable=#false\n\
         schema_fields \"payload\" type=\"utf8\"\n",
    );

    let batches = drain(&mut source).await;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
    let remaining: i64 = client
        .query_one("SELECT count(*) FROM outbox", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(remaining, 6, "the prune runs at the next cycle's start");

    // The second cycle commits the offset and prunes what it acknowledged.
    assert!(drain(&mut source).await.is_empty());
    let remaining: i64 = client
        .query_one("SELECT count(*) FROM outbox", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn notify_wakes_the_source_well_inside_its_timeout() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute("CREATE TABLE prices (id bigint PRIMARY KEY, label text)")
        .await
        .expect("fixture");

    let mut source = source(
        &pg.dsn(),
        &format!(
            "name \"prices\"\nbatch_rows 10\n\n\
             mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n\n\
             notify channel=\"prices_changed\" timeout_ms=15000\n{PRICES_FIELDS}"
        ),
    );

    let writer = pg.connect().await;
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        writer
            .batch_execute("INSERT INTO prices VALUES (1, 'p1'); NOTIFY prices_changed")
            .await
            .expect("insert and notify");
    });

    let batch = source
        .next_batch()
        .await
        .expect("next_batch")
        .expect("the notification must deliver a batch");
    assert_eq!(batch.num_rows(), 1);
}

#[tokio::test]
async fn a_declared_type_the_column_cannot_fill_is_a_loud_error() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute("CREATE TABLE prices (id bigint PRIMARY KEY, label text)")
        .await
        .expect("fixture");

    let mut source = source(
        &pg.dsn(),
        "name \"prices\"\n\n\
         mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"label\" type=\"int32\"\n",
    );
    let err = source
        .next_batch()
        .await
        .expect_err("an int32 column cannot be filled by text");
    assert_eq!(err.category(), "configuration");
    assert!(err.message().contains("'label'"), "{}", err.message());
    assert!(err.message().contains("text"), "{}", err.message());
}

#[tokio::test]
async fn a_declared_column_the_table_lacks_names_itself() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute("CREATE TABLE prices (id bigint PRIMARY KEY)")
        .await
        .expect("fixture");

    let mut source = source(
        &pg.dsn(),
        &format!(
            "name \"prices\"\n\n\
             mode kind=\"polling\" table=\"prices\" cursor_column=\"id\"\n\
             {PRICES_FIELDS}"
        ),
    );
    let err = source.next_batch().await.expect_err("missing column");
    // The prepare fails in the server before the schema check runs, so the
    // message comes from PostgreSQL and still names the column.
    assert!(err.message().contains("label"), "{}", err.message());
}

/// A text-wire column carries its own **output function**'s rendering, not a
/// `::text` cast's.
///
/// The two are different functions for `inet`, `cidr`, `bool` and `bpchar`,
/// which each carry a cast-to-`text` of their own: `'::1'::inet::text` is
/// `::1/128` where `inet_out` is `::1`, `true::text` is `true` where
/// `boolout` is `t`, and `bpchar`'s cast rtrims where its output function
/// pads. Only the output function is reachable on the `cdc_logical` path,
/// which reads `pgoutput`'s text tuples, so it is the one form the two source
/// paths can agree on -- and this test is what keeps the cursor path on it.
#[tokio::test]
async fn a_text_wire_column_carries_its_output_functions_rendering() {
    let Some(pg) = common::try_start().await else {
        return;
    };
    let client = pg.connect().await;
    client
        .batch_execute(
            "CREATE TABLE outfn (id bigint PRIMARY KEY, ip inet, flag boolean, \
             pad char(5), ips inet[]);
             INSERT INTO outfn VALUES
               (1, '::1', true, 'x', ARRAY['::1','10.0.0.0/8']::inet[]),
               (2, '192.168.1.5/32', false, 'ab', ARRAY[]::inet[]),
               (3, NULL, NULL, NULL, NULL)",
        )
        .await
        .expect("fixture");

    let fields = "
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"ip\" type=\"utf8\" pg_type=\"inet\" nullable=#true
schema_fields \"flag\" type=\"utf8\" pg_type=\"bool\" nullable=#true
schema_fields \"pad\" type=\"utf8\" pg_type=\"bpchar\" nullable=#true
schema_fields \"ips\" type=\"list\" item=\"utf8\" pg_type=\"inet[]\" nullable=#true
";
    let mut source = source(
        &pg.dsn(),
        &format!(
            "name \"outfn\"\nbatch_rows 100\n\n\
             mode kind=\"polling\" table=\"outfn\" cursor_column=\"id\"\n{fields}"
        ),
    );
    let batches = drain(&mut source).await;
    let batch = batches.first().expect("one batch");
    assert_eq!(batch.num_rows(), 3);

    // What the connector must return, measured against a live server rather
    // than re-derived from the same expression the connector emits: an
    // oracle of `format('%s', col)` would only prove the plumbing.
    let expected: [(&str, [Option<&str>; 3]); 3] = [
        ("ip", [Some("::1"), Some("192.168.1.5"), None]),
        ("flag", [Some("t"), Some("f"), None]),
        // `char(5)` is blank-padded by its output function.
        ("pad", [Some("x    "), Some("ab   "), None]),
    ];
    for (column, want) in expected {
        let got = batch
            .column_by_name(column)
            .expect(column)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        for (row, want) in want.iter().enumerate() {
            let have = (!got.is_null(row)).then(|| got.value(row));
            assert_eq!(have, *want, "column {column} row {row}");
        }
    }

    // And the `::text` cast really does disagree, so the literals above are
    // the output function's and not merely "whatever the server said".
    let cast = client
        .query_one(
            "SELECT ip::text, flag::text, pad::text FROM outfn WHERE id = 1",
            &[],
        )
        .await
        .expect("the cast for contrast");
    assert_eq!(cast.get::<_, String>(0), "::1/128");
    assert_eq!(cast.get::<_, String>(1), "true");
    assert_eq!(cast.get::<_, String>(2), "x");

    // An array arrives as its own literal and decodes element by element.
    let lists = batch
        .column_by_name("ips")
        .expect("ips")
        .as_any()
        .downcast_ref::<arrow_array::ListArray>()
        .expect("list");
    let values = lists.value(0);
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 elements");
    let elements: Vec<&str> = (0..values.len()).map(|i| values.value(i)).collect();
    assert_eq!(elements, ["::1", "10.0.0.0/8"]);
    assert_eq!(lists.value(1).len(), 0, "an empty array has no elements");
    assert!(lists.is_null(2), "a NULL array stays NULL");
}
