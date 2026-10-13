//! `cdc_binlog` against real MariaDB and MySQL servers.
//!
//! Every shared case runs on both servers; the cases at the bottom are specific
//! to one. A source resolves `initial "now"` on its first `next_batch`, so each
//! case drains one empty cycle before it makes the changes it expects to read.

mod common;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, TimestampMicrosecondType, UInt64Type};
use arrow_array::{Array as _, RecordBatch};
use common::{Flavor, Server, drain, source};
use mysql_async::prelude::Queryable;
use saci_core::io::source::Source as _;

/// The metadata and data fields most cases declare.
const ORDER_FIELDS: &str = r#"
schema_fields "__op" type="utf8" nullable=#false
schema_fields "__table" type="utf8" nullable=#false
schema_fields "__binlog_file" type="utf8" nullable=#false
schema_fields "__binlog_pos" type="uint64" nullable=#false
schema_fields "__commit_ts" type="timestamp_micros_utc" nullable=#false
schema_fields "id" type="int64" nullable=#false
schema_fields "note" type="utf8"
"#;

/// One change as a test reads it.
#[derive(Debug, Clone, PartialEq)]
struct Change {
    op: String,
    table: String,
    file: String,
    pos: u64,
    commit_ts: i64,
    id: i64,
    note: Option<String>,
}

fn changes(batches: &[RecordBatch]) -> Vec<Change> {
    let mut out = Vec::new();
    for batch in batches {
        let op = batch
            .column_by_name("__op")
            .expect("__op")
            .as_string::<i32>();
        let table = batch
            .column_by_name("__table")
            .expect("__table")
            .as_string::<i32>();
        let file = batch
            .column_by_name("__binlog_file")
            .expect("__binlog_file")
            .as_string::<i32>();
        let pos = batch
            .column_by_name("__binlog_pos")
            .expect("__binlog_pos")
            .as_primitive::<UInt64Type>();
        let ts = batch
            .column_by_name("__commit_ts")
            .expect("__commit_ts")
            .as_primitive::<TimestampMicrosecondType>();
        let id = batch
            .column_by_name("id")
            .expect("id")
            .as_primitive::<Int64Type>();
        let note = batch
            .column_by_name("note")
            .expect("note")
            .as_string::<i32>();
        for row in 0..batch.num_rows() {
            out.push(Change {
                op: op.value(row).to_string(),
                table: table.value(row).to_string(),
                file: file.value(row).to_string(),
                pos: pos.value(row),
                commit_ts: ts.value(row),
                id: id.value(row),
                note: (!note.is_null(row)).then(|| note.value(row).to_string()),
            });
        }
    }
    out
}

/// `(op, id)` of each change, for order assertions.
fn ops(changes: &[Change]) -> Vec<(String, i64)> {
    changes.iter().map(|c| (c.op.clone(), c.id)).collect()
}

fn pair(op: &str, id: i64) -> (String, i64) {
    (op.to_string(), id)
}

fn batch_sizes(batches: &[RecordBatch]) -> Vec<usize> {
    batches.iter().map(RecordBatch::num_rows).collect()
}

/// A `cdc_binlog` config over `saci.<table>` with the order fields.
fn config(name: &str, table: &str, mode: &str, top: &str) -> String {
    format!(
        "name \"{name}\"\n{top}\n\nmode kind=\"cdc_binlog\" table=\"{table}\" {mode}\n{ORDER_FIELDS}"
    )
}

async fn run(server: &Server, sql: &[&str]) {
    let mut conn = server.conn().await;
    for statement in sql {
        conn.query_drop(*statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
}

async fn stored_offset(server: &Server, name: &str) -> Option<String> {
    let mut conn = server.conn().await;
    conn.exec_first(
        "SELECT cursor_value FROM saci_source_offsets WHERE source_name = ?",
        (name,),
    )
    .await
    .expect("read the offset row")
}

async fn orders(server: &Server) {
    run(
        server,
        &[
            "DROP TABLE IF EXISTS orders",
            "CREATE TABLE orders (id BIGINT PRIMARY KEY, note VARCHAR(40)) ENGINE=InnoDB",
        ],
    )
    .await;
}

// ----------------------------------------------------------------------------
// Shared cases
// ----------------------------------------------------------------------------

async fn insert_update_delete_arrive_with_their_ops_and_positions_advance(server: &Server) {
    orders(server).await;
    let mut src = source(
        &server.dsn(),
        &config("orders_cdc", "orders", "", "batch_rows 100"),
    );
    assert!(drain(&mut src).await.is_empty(), "nothing has changed yet");

    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a'), (2, 'b')",
            "UPDATE orders SET note = 'c' WHERE id = 1",
            "DELETE FROM orders WHERE id = 2",
        ],
    )
    .await;

    let batches = drain(&mut src).await;
    let rows = changes(&batches);
    assert_eq!(
        ops(&rows),
        vec![pair("I", 1), pair("I", 2), pair("U", 1), pair("D", 2)]
    );
    assert_eq!(rows[0].note.as_deref(), Some("a"));
    assert_eq!(rows[2].note.as_deref(), Some("c"));
    assert_eq!(
        rows[3].note.as_deref(),
        Some("b"),
        "a delete carries the old row"
    );

    for row in &rows {
        assert_eq!(row.table, "saci.orders");
        assert!(row.commit_ts > 1_600_000_000_000_000, "{}", row.commit_ts);
        assert!(row.file.contains("-bin."), "{}", row.file);
    }
    // One transaction shares one position; later transactions are later.
    assert_eq!(rows[0].pos, rows[1].pos);
    assert!(
        rows[1].pos < rows[2].pos && rows[2].pos < rows[3].pos,
        "{rows:?}"
    );

    // The position is stored one cycle late.
    let last = rows.last().expect("rows");
    assert!(drain(&mut src).await.is_empty());
    let stored = stored_offset(server, "orders_cdc")
        .await
        .expect("an offset row");
    let (file, pos) = stored
        .strip_prefix("file:")
        .and_then(|rest| rest.rsplit_once(':'))
        .expect("a file offset");
    assert_eq!(file, last.file);
    assert!(
        pos.parse::<u64>().expect("a position") >= last.pos,
        "{stored}"
    );
}

async fn changes_to_other_tables_are_skipped(server: &Server) {
    orders(server).await;
    run(
        server,
        &[
            "DROP TABLE IF EXISTS noise",
            "CREATE TABLE noise (id BIGINT PRIMARY KEY, note VARCHAR(40))",
        ],
    )
    .await;
    let mut src = source(
        &server.dsn(),
        &config("skip_cdc", "orders", "", "batch_rows 100"),
    );
    assert!(drain(&mut src).await.is_empty());

    run(
        server,
        &[
            "INSERT INTO noise VALUES (1, 'x'), (2, 'y')",
            "INSERT INTO orders VALUES (10, 'mine')",
            "UPDATE noise SET note = 'z'",
            "DELETE FROM noise WHERE id = 1",
        ],
    )
    .await;
    let rows = changes(&drain(&mut src).await);
    assert_eq!(ops(&rows), vec![pair("I", 10)]);
}

async fn a_primary_key_update_arrives_as_delete_then_insert(server: &Server) {
    orders(server).await;
    run(server, &["INSERT INTO orders VALUES (1, 'a'), (2, 'b')"]).await;
    let mut src = source(
        &server.dsn(),
        &config("key_cdc", "orders", "", "batch_rows 100"),
    );
    assert!(drain(&mut src).await.is_empty());

    run(
        server,
        &[
            "UPDATE orders SET id = 10 WHERE id = 1",
            "UPDATE orders SET note = 'bb' WHERE id = 2",
        ],
    )
    .await;
    let rows = changes(&drain(&mut src).await);
    assert_eq!(ops(&rows), vec![pair("D", 1), pair("I", 10), pair("U", 2)]);
    assert_eq!(rows[0].note.as_deref(), Some("a"));
    assert_eq!(rows[1].note.as_deref(), Some("a"));
    assert_eq!(rows[0].pos, rows[1].pos, "the pair is one transaction");
}

async fn snapshot_initial_reads_existing_rows_as_r_then_streams_changes_made_during_it(
    server: &Server,
) {
    orders(server).await;
    run(
        server,
        &["INSERT INTO orders VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')"],
    )
    .await;
    let mut src = source(
        &server.dsn(),
        &config("snap_cdc", "orders", "snapshot=\"initial\"", "batch_rows 2"),
    );
    // Made after the source exists but before the snapshot starts: part of it.
    run(server, &["INSERT INTO orders VALUES (5, 'five')"]).await;

    let first = src
        .next_batch()
        .await
        .expect("first snapshot page")
        .expect("a page");
    assert_eq!(batch_sizes(std::slice::from_ref(&first)), vec![2]);

    // Made while the snapshot is open: after its pinned position, so it comes
    // from the stream, once, and the snapshot rows keep their old values.
    run(
        server,
        &[
            "INSERT INTO orders VALUES (6, 'six')",
            "UPDATE orders SET note = 'changed' WHERE id = 2",
        ],
    )
    .await;

    let mut all = changes(std::slice::from_ref(&first));
    all.extend(changes(&drain(&mut src).await));
    assert_eq!(
        ops(&all),
        vec![
            pair("R", 1),
            pair("R", 2),
            pair("R", 3),
            pair("R", 4),
            pair("R", 5),
            pair("I", 6),
            pair("U", 2)
        ]
    );
    assert_eq!(
        all[1].note.as_deref(),
        Some("two"),
        "the snapshot row is as of its position"
    );
    assert_eq!(all[6].note.as_deref(), Some("changed"));
    // Every snapshot row carries the one position the snapshot was pinned at.
    assert!(
        all[..5]
            .iter()
            .all(|r| r.pos == all[0].pos && r.file == all[0].file)
    );
    assert!(all[5].pos > all[0].pos);

    // Nothing is delivered twice by the next cycle, and the offset is stored.
    assert!(drain(&mut src).await.is_empty());
    assert!(stored_offset(server, "snap_cdc").await.is_some());
    let mut again = source(
        &server.dsn(),
        &config("snap_cdc", "orders", "snapshot=\"initial\"", "batch_rows 2"),
    );
    assert!(
        drain(&mut again).await.is_empty(),
        "a stored offset means no second snapshot"
    );
}

async fn snapshot_without_a_declared_key_is_refused(server: &Server) {
    orders(server).await;
    run(
        server,
        &[
            "DROP TABLE IF EXISTS nopk",
            "CREATE TABLE nopk (a INT, note VARCHAR(10))",
        ],
    )
    .await;

    let fields_without_id = r#"
schema_fields "__op" type="utf8" nullable=#false
schema_fields "note" type="utf8"
"#;
    let mut undeclared = source(
        &server.dsn(),
        &format!(
            "name \"nokey\"\n\nmode kind=\"cdc_binlog\" table=\"orders\" snapshot=\"initial\"\n{fields_without_id}"
        ),
    );
    let err = undeclared.next_batch().await.expect_err("refused");
    assert_eq!(err.category(), "configuration", "{}", err.message());
    assert!(
        err.message().contains(
            "snapshot \"initial\" pages 'saci.orders' by its primary key, so every key column \
             must be declared in schema_fields (missing: 'id')"
        ),
        "{}",
        err.message()
    );

    let mut keyless = source(
        &server.dsn(),
        &format!(
            "name \"nopk\"\n\nmode kind=\"cdc_binlog\" table=\"nopk\" snapshot=\"initial\"\n{fields_without_id}"
        ),
    );
    let err = keyless.next_batch().await.expect_err("refused");
    assert!(
        err.message()
            .contains("snapshot \"initial\" needs 'saci.nopk' to have a primary key"),
        "{}",
        err.message()
    );
}

async fn max_changes_per_cycle_ends_on_a_transaction_boundary(server: &Server) {
    orders(server).await;
    let mut src = source(
        &server.dsn(),
        &config(
            "cap_cdc",
            "orders",
            "max_changes_per_cycle=3",
            "batch_rows 100",
        ),
    );
    assert!(drain(&mut src).await.is_empty());

    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a'), (2, 'b')",
            "INSERT INTO orders VALUES (3, 'c'), (4, 'd')",
            "INSERT INTO orders VALUES (5, 'e'), (6, 'f')",
        ],
    )
    .await;

    // Three changes are reached inside the second transaction, which is read
    // whole; the third waits for the next cycle.
    let first = drain(&mut src).await;
    assert_eq!(
        ops(&changes(&first)),
        vec![pair("I", 1), pair("I", 2), pair("I", 3), pair("I", 4)]
    );
    let second = drain(&mut src).await;
    assert_eq!(ops(&changes(&second)), vec![pair("I", 5), pair("I", 6)]);
    assert!(drain(&mut src).await.is_empty());
}

async fn request_batch_rows_overrides_the_chunk_size(server: &Server) {
    orders(server).await;
    let mut src = source(
        &server.dsn(),
        &config("chunk_cdc", "orders", "", "batch_rows 100"),
    );
    assert!(drain(&mut src).await.is_empty());
    src.request_batch_rows(2);

    run(
        server,
        &["INSERT INTO orders VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')"],
    )
    .await;
    let batches = drain(&mut src).await;
    assert_eq!(batch_sizes(&batches), vec![2, 2, 1]);
    assert_eq!(changes(&batches).len(), 5);

    // A hint of zero still advances.
    src.request_batch_rows(0);
    run(server, &["INSERT INTO orders VALUES (6,'f'),(7,'g')"]).await;
    assert_eq!(batch_sizes(&drain(&mut src).await), vec![1, 1]);
}

async fn a_new_source_resumes_from_the_offset_table(server: &Server) {
    orders(server).await;
    let config = config("resume_cdc", "orders", "", "batch_rows 100");
    let mut first = source(&server.dsn(), &config);
    assert!(drain(&mut first).await.is_empty());
    run(server, &["INSERT INTO orders VALUES (1, 'a')"]).await;
    assert_eq!(ops(&changes(&drain(&mut first).await)), vec![pair("I", 1)]);
    // The next cycle commits the position.
    assert!(drain(&mut first).await.is_empty());
    drop(first);

    run(server, &["INSERT INTO orders VALUES (2, 'b')"]).await;
    let mut second = source(&server.dsn(), &config);
    assert_eq!(
        ops(&changes(&drain(&mut second).await)),
        vec![pair("I", 2)],
        "only what happened after the stored position"
    );
}

async fn a_cycle_cut_by_max_batches_per_cycle_loses_nothing(server: &Server) {
    orders(server).await;
    let mut src = source(
        &server.dsn(),
        &config(
            "cut_cdc",
            "orders",
            "",
            "batch_rows 2\nmax_batches_per_cycle 1",
        ),
    );
    assert!(drain(&mut src).await.is_empty());
    // One transaction per row, so the cut always falls on a boundary.
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1,'a')",
            "INSERT INTO orders VALUES (2,'b')",
            "INSERT INTO orders VALUES (3,'c')",
            "INSERT INTO orders VALUES (4,'d')",
            "INSERT INTO orders VALUES (5,'e')",
        ],
    )
    .await;

    let mut seen = Vec::new();
    for _ in 0..6 {
        let batches = drain(&mut src).await;
        assert!(batches.len() <= 1, "one batch per cycle: {}", batches.len());
        seen.extend(ops(&changes(&batches)));
        if seen.len() >= 5 {
            break;
        }
    }
    assert_eq!(
        seen,
        (1..=5).map(|id| pair("I", id)).collect::<Vec<_>>(),
        "every change once and in order"
    );
}

async fn an_added_column_is_picked_up_mid_stream(server: &Server) {
    orders(server).await;
    let fields = r#"
schema_fields "__op" type="utf8" nullable=#false
schema_fields "id" type="int64" nullable=#false
schema_fields "note" type="utf8"
schema_fields "extra" type="int32"
"#;
    // `extra` is declared before the server has it; the first read after the
    // ALTER finds it.
    let mut src = source(
        &server.dsn(),
        &format!(
            "name \"alter_cdc\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"orders\"\n{}",
            fields.replace("schema_fields \"extra\" type=\"int32\"\n", "")
        ),
    );
    assert!(drain(&mut src).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a')",
            "ALTER TABLE orders ADD COLUMN extra INT",
            "INSERT INTO orders (id, note, extra) VALUES (2, 'b', 7)",
        ],
    )
    .await;
    let batches = drain(&mut src).await;
    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .expect("id")
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, vec![1, 2], "rows from before and after the DDL");
}

async fn binlog_row_image_minimal_is_refused(server: &Server) {
    orders(server).await;
    run(server, &["SET GLOBAL binlog_row_image = 'MINIMAL'"]).await;
    let mut src = source(
        &server.dsn(),
        &config("min_cdc", "orders", "", "batch_rows 100"),
    );
    let result = src.next_batch().await;
    run(server, &["SET GLOBAL binlog_row_image = 'FULL'"]).await;
    let err = result.expect_err("refused");
    assert_eq!(err.category(), "configuration", "{}", err.message());
    assert!(
        err.message().contains("binlog_row_image = FULL"),
        "{}",
        err.message()
    );
}

async fn a_user_without_replication_privileges_is_told_what_it_needs(server: &Server) {
    orders(server).await;
    run(
        server,
        &[
            "DROP USER IF EXISTS 'reader'@'%'",
            "CREATE USER 'reader'@'%' IDENTIFIED BY 'reader'",
            "GRANT SELECT, INSERT, UPDATE, CREATE ON saci.* TO 'reader'@'%'",
            "DROP USER IF EXISTS 'client'@'%'",
            "CREATE USER 'client'@'%' IDENTIFIED BY 'client'",
            "GRANT SELECT, INSERT, UPDATE, CREATE ON saci.* TO 'client'@'%'",
        ],
    )
    .await;
    let client_grant = match server.flavor {
        Flavor::Mariadb => "GRANT BINLOG MONITOR ON *.* TO 'client'@'%'",
        Flavor::Mysql => "GRANT REPLICATION CLIENT ON *.* TO 'client'@'%'",
    };
    run(server, &[client_grant]).await;

    let needed = match server.flavor {
        Flavor::Mariadb => "grant REPLICATION SLAVE and BINLOG MONITOR",
        Flavor::Mysql => "grant REPLICATION SLAVE and REPLICATION CLIENT",
    };

    // Cannot even read the position.
    let mut reader = source(
        &server.dsn_as("reader", "reader"),
        &config("priv_cdc", "orders", "", "batch_rows 100"),
    );
    let err = reader.next_batch().await.expect_err("refused");
    assert!(err.message().contains(needed), "{}", err.message());

    // Can read the position, cannot dump.
    let mut client = source(
        &server.dsn_as("client", "client"),
        &config("priv_cdc2", "orders", "", "batch_rows 100"),
    );
    let err = client
        .next_batch()
        .await
        .expect_err("first cycle resolves now");
    // The first cycle resolves `now` and succeeds only when it can open the
    // stream; a refusal there carries the same hint.
    assert!(err.message().contains(needed), "{}", err.message());
}

async fn a_purged_position_names_the_remedy(server: &Server) {
    orders(server).await;
    let config = config("purge_cdc", "orders", "", "batch_rows 100");
    let mut first = source(&server.dsn(), &config);
    assert!(drain(&mut first).await.is_empty());
    drop(first);
    run(
        server,
        &[
            "UPDATE saci_source_offsets SET cursor_value = 'file:nope.000001:4' \
           WHERE source_name = 'purge_cdc'",
        ],
    )
    .await;

    let mut second = source(&server.dsn(), &config);
    let err = second
        .next_batch()
        .await
        .expect_err("the file does not exist");
    assert!(
        err.message().contains(
            "the stored position is no longer on the server (purged?): delete the source's row \
             from 'saci.saci_source_offsets' and set mode.initial"
        ),
        "{}",
        err.message()
    );
}

/// The columns whose binlog and protocol forms differ, read both ways.
async fn enum_set_bit_year_timestamp_and_unsigned_decode_like_polling(server: &Server) {
    run(
        server,
        &[
            "DROP TABLE IF EXISTS kinds",
            "CREATE TABLE kinds (id BIGINT PRIMARY KEY, e ENUM('a','b','it''s') NOT NULL, \
             s SET('x','y','z') NOT NULL, b BIT(8) NOT NULL, y YEAR NOT NULL, \
             ts TIMESTAMP(3) NULL, u TINYINT UNSIGNED NOT NULL, w INT UNSIGNED NOT NULL, \
             m MEDIUMINT UNSIGNED NOT NULL, dt DATETIME(6) NULL, d DECIMAL(10,2) NOT NULL, \
             f DOUBLE NOT NULL, j TEXT, t TIME(3) NULL)",
        ],
    )
    .await;
    let data_fields = r#"
schema_fields "id" type="int64" nullable=#false
schema_fields "e" type="utf8" nullable=#false
schema_fields "s" type="utf8" nullable=#false
schema_fields "b" type="uint64" nullable=#false
schema_fields "y" type="int16" nullable=#false
schema_fields "ts" type="timestamp_micros_utc"
schema_fields "u" type="uint8" nullable=#false
schema_fields "w" type="uint32" nullable=#false
schema_fields "m" type="uint32" nullable=#false
schema_fields "dt" type="timestamp_micros"
schema_fields "d" type="decimal128" precision=10 scale=2 nullable=#false
schema_fields "f" type="float64" nullable=#false
schema_fields "j" type="utf8"
schema_fields "t" type="time64_micros"
"#;
    let mut binlog = source(
        &server.dsn(),
        &format!(
            "name \"kinds_cdc\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"kinds\"\n{data_fields}"
        ),
    );
    assert!(drain(&mut binlog).await.is_empty());

    run(
        server,
        &[
            "INSERT INTO kinds VALUES (1, 'b', 'x,z', b'10100101', 2024, \
             '2024-02-29 12:34:56.789', 200, 4000000000, 16777215, \
             '2023-12-31 23:59:59.123456', 12345678.90, -0.5, 'plain', '12:34:56.789')",
            "INSERT INTO kinds VALUES (2, 'it''s', '', b'0', 0, NULL, 255, 0, 0, NULL, \
             -0.01, 1e300, NULL, NULL)",
        ],
    )
    .await;
    let from_binlog = drain(&mut binlog).await;

    let mut polling = source(
        &server.dsn(),
        &format!(
            "name \"kinds_poll\"\nbatch_rows 100\n\n\
             mode kind=\"polling\" table=\"kinds\" cursor_column=\"id\" initial=\"beginning\"\n{data_fields}"
        ),
    );
    let from_polling = drain(&mut polling).await;

    let names = [
        "id", "e", "s", "b", "y", "ts", "u", "w", "m", "dt", "d", "f", "j", "t",
    ];
    for name in names {
        assert_eq!(
            row_slices(&from_binlog, name),
            row_slices(&from_polling, name),
            "column {name} differs between the binlog and polling"
        );
    }
    assert_eq!(
        from_binlog.iter().map(RecordBatch::num_rows).sum::<usize>(),
        2
    );
}

/// One single-row slice per row of column `name`, so two sources that batch
/// differently still compare row by row.
fn row_slices(batches: &[RecordBatch], name: &str) -> Vec<arrow_array::ArrayRef> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name(name).expect("column");
            (0..batch.num_rows())
                .map(|row| column.slice(row, 1))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Read `table` through the binlog and through polling and require every
/// listed column to match row by row. `inserts` run with an empty `sql_mode`,
/// so zero dates are legal.
async fn binlog_and_polling_agree(
    server: &Server,
    table: &str,
    create: &str,
    fields: &str,
    names: &[&str],
    inserts: &[&str],
) {
    let mut conn = server.conn().await;
    for statement in [
        "SET SESSION sql_mode = ''".to_string(),
        format!("DROP TABLE IF EXISTS {table}"),
        create.to_string(),
    ] {
        conn.query_drop(statement.as_str())
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    let mut binlog = source(
        &server.dsn(),
        &format!(
            "name \"{table}_cdc\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"{table}\"\n{fields}"
        ),
    );
    assert!(drain(&mut binlog).await.is_empty());
    for statement in inserts {
        conn.query_drop(*statement)
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    let from_binlog = drain(&mut binlog).await;

    let mut polling = source(
        &server.dsn(),
        &format!(
            "name \"{table}_poll\"\nbatch_rows 100\n\n\
             mode kind=\"polling\" table=\"{table}\" cursor_column=\"id\" initial=\"beginning\"\n{fields}"
        ),
    );
    let from_polling = drain(&mut polling).await;
    assert_eq!(
        from_binlog.iter().map(RecordBatch::num_rows).sum::<usize>(),
        from_polling
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        "both paths read the same rows"
    );
    for name in names {
        assert_eq!(
            row_slices(&from_binlog, name),
            row_slices(&from_polling, name),
            "column {name} differs between the binlog and polling"
        );
    }
}

/// Every temporal, integer, decimal and JSON edge, rendered as text so the
/// comparison sees exactly what each path decoded.
async fn extreme_values_render_the_same_text_from_the_binlog_and_polling(server: &Server) {
    let names = [
        "tm", "tm0", "d", "dt", "dt0", "ts", "ts0", "mi", "umi", "si", "ti", "bi", "ubi", "yr",
        "dec1", "dec2", "dec3", "js",
    ];
    let fields: String =
        std::iter::once("schema_fields \"id\" type=\"int64\" nullable=#false\n".to_string())
            .chain(
                names
                    .iter()
                    .map(|name| format!("schema_fields \"{name}\" type=\"utf8\"\n")),
            )
            .collect();
    binlog_and_polling_agree(
        server,
        "edge_text",
        "CREATE TABLE edge_text (id BIGINT PRIMARY KEY, tm TIME(3), tm0 TIME, d DATE, \
         dt DATETIME(3), dt0 DATETIME, ts TIMESTAMP(6) NULL, ts0 TIMESTAMP NULL, \
         mi MEDIUMINT, umi MEDIUMINT UNSIGNED, si SMALLINT, ti TINYINT, bi BIGINT, \
         ubi BIGINT UNSIGNED, yr YEAR, dec1 DECIMAL(38,10), dec2 DECIMAL(5,0), \
         dec3 DECIMAL(10,2), js JSON)",
        &fields,
        &names,
        &[
            "INSERT INTO edge_text VALUES (1, '-838:59:59.999', '838:59:59', '0000-00-00', \
             '0000-00-00 00:00:00.000', '0000-00-00 00:00:00', '0000-00-00 00:00:00.000000', \
             '0000-00-00 00:00:00', -8388608, 16777215, -32768, -128, -9223372036854775808, \
             18446744073709551615, 2155, -9999999999999999999999999999.9999999999, -99999, \
             -99999999.99, '{\"b\":1,\"a\":[1,2.5,{\"c\":null}],\"s\":\"é\\\\\"q\"}')",
            "INSERT INTO edge_text VALUES (2, '00:00:00.001', '00:00:00', '9999-12-31', \
             '9999-12-31 23:59:59.999', '1000-01-01 00:00:00', '1970-01-01 00:00:01.000001', \
             '2038-01-19 03:14:07', 8388607, 0, 32767, 127, 9223372036854775807, 0, 1901, \
             9999999999999999999999999999.9999999999, 99999, 99999999.99, '[]')",
            "INSERT INTO edge_text VALUES (3, '-00:00:00.500', '-00:00:01', '2024-02-29', \
             '2024-02-29 12:00:00.5', '1969-12-31 23:59:59', '2024-02-29 12:00:00.5', \
             '2024-02-29 12:00:00', -1, 1, -1, -1, -1, 1, 0, 0.0000000001, 0, 0.01, \
             '{\"x\":1e2,\"y\":12345678901234567890,\"z\":-0.1,\"w\":true}')",
            "INSERT INTO edge_text VALUES (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        ],
    )
    .await;
}

/// The typed decoders, with the byte, bit and float edges the text case cannot
/// reach.
async fn extreme_values_decode_the_same_typed_value_from_the_binlog_and_polling(server: &Server) {
    let names = [
        "tm", "d", "dt", "ts", "mi", "si", "ti", "flag", "bi", "ubi", "f", "dbl", "dec1", "dec2",
        "bin4", "vb", "bl", "txt", "c", "bt", "ubit", "yr",
    ];
    let fields = r#"
schema_fields "id" type="int64" nullable=#false
schema_fields "tm" type="time64_micros"
schema_fields "d" type="date32"
schema_fields "dt" type="timestamp_micros"
schema_fields "ts" type="timestamp_micros_utc"
schema_fields "mi" type="int32" nullable=#false
schema_fields "si" type="int16" nullable=#false
schema_fields "ti" type="int8" nullable=#false
schema_fields "flag" type="boolean" nullable=#false
schema_fields "bi" type="int64" nullable=#false
schema_fields "ubi" type="uint64" nullable=#false
schema_fields "f" type="float32" nullable=#false
schema_fields "dbl" type="float64" nullable=#false
schema_fields "dec1" type="decimal128" precision=38 scale=10 nullable=#false
schema_fields "dec2" type="decimal128" precision=5 scale=0 nullable=#false
schema_fields "bin4" type="binary"
schema_fields "vb" type="binary"
schema_fields "bl" type="binary"
schema_fields "txt" type="utf8"
schema_fields "c" type="utf8"
schema_fields "bt" type="uint64"
schema_fields "ubit" type="uint64"
schema_fields "yr" type="int16"
"#;
    binlog_and_polling_agree(
        server,
        "edge_typed",
        "CREATE TABLE edge_typed (id BIGINT PRIMARY KEY, tm TIME(6), d DATE, dt DATETIME(6), \
         ts TIMESTAMP(6) NULL, mi MEDIUMINT NOT NULL, si SMALLINT NOT NULL, ti TINYINT NOT NULL, \
         flag TINYINT(1) NOT NULL, bi BIGINT NOT NULL, ubi BIGINT UNSIGNED NOT NULL, \
         f FLOAT NOT NULL, dbl DOUBLE NOT NULL, dec1 DECIMAL(38,10) NOT NULL, \
         dec2 DECIMAL(5,0) NOT NULL, bin4 BINARY(4), vb VARBINARY(8), bl BLOB, txt MEDIUMTEXT, \
         c CHAR(5), bt BIT(20), ubit BIT(64), yr YEAR)",
        fields,
        &names,
        &[
            "INSERT INTO edge_typed VALUES (1, '23:59:59.999999', '0001-01-01', \
             '1000-01-01 00:00:00.000001', '1970-01-01 00:00:01.000001', -8388608, -32768, -128, \
             1, -9223372036854775808, 18446744073709551615, -3.4028235e38, \
             1.7976931348623157e308, -9999999999999999999999999999.9999999999, -99999, \
             0x00, X'', 0x00ff00, 'multi\\nline é 😀', 'ab', b'11111111111111111111', \
             b'1111111111111111111111111111111111111111111111111111111111111111', 2155)",
            "INSERT INTO edge_typed VALUES (2, '00:00:00', '9999-12-31', \
             '9999-12-31 23:59:59.999999', '2038-01-19 03:14:07.999999', 8388607, 32767, 127, \
             0, 9223372036854775807, 0, 1.17549435e-38, 2.2250738585072014e-308, \
             9999999999999999999999999999.9999999999, 99999, 0xAB, 0x00, X'', '', 'x y  ', b'0', \
             b'0', 1901)",
            "INSERT INTO edge_typed VALUES (3, NULL, NULL, NULL, NULL, -1, -1, -1, 1, -1, 1, \
             -0.0, -0.0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
            "INSERT INTO edge_typed VALUES (4, '12:00:00.5', '2000-02-29', '2000-02-29 12:00:00.5', \
             '2000-02-29 12:00:00.5', 1, 1, 1, 1, 1, 1, 1.5, 1.5, 1.5, 1, 0x01020304, \
             0x0102030405060708, 0xff, 'x', 'abcde', b'101', b'101', 2000)",
        ],
    )
    .await;
}

/// A two-byte ENUM index, a SET wider than 32 members, a geometry, a JSON
/// document and values larger than a network packet's comfortable size.
async fn wide_enum_set_geometry_json_and_large_values_decode_alike(server: &Server) {
    let labels = |prefix: &str, count: usize| {
        (0..count)
            .map(|n| format!("'{prefix}{n}'"))
            .collect::<Vec<_>>()
            .join(",")
    };
    let create = format!(
        "CREATE TABLE edge_wide (id BIGINT PRIMARY KEY, e300 ENUM({}), s40 SET({}), \
         g POINT, jd JSON, big LONGTEXT, blob2 LONGBLOB)",
        labels("v", 300),
        labels("m", 40)
    );
    let fields = r#"
schema_fields "id" type="int64" nullable=#false
schema_fields "e300" type="utf8"
schema_fields "s40" type="utf8"
schema_fields "g" type="binary"
schema_fields "jd" type="json"
schema_fields "big" type="utf8"
schema_fields "blob2" type="binary"
"#;
    binlog_and_polling_agree(
        server,
        "edge_wide",
        &create,
        fields,
        &["e300", "s40", "g", "jd", "big", "blob2"],
        &[
            "INSERT INTO edge_wide VALUES (1, 'v299', 'm0,m7,m8,m39', \
             ST_GeomFromText('POINT(1.5 -2.5)'), '{\"b\":[1,2,3],\"a\":{\"z\":null,\"y\":1.5e0}}', \
             REPEAT('é', 40000), UNHEX(REPEAT('AB', 70000)))",
            "INSERT INTO edge_wide VALUES (2, 'v0', 'm39', NULL, '[]', '', X'')",
            "INSERT INTO edge_wide VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL)",
        ],
    )
    .await;
}

/// A DDL that is the last event a cycle reads leaves the catalog stale for the
/// next one. The column count is unchanged, so nothing but the DDL says the
/// table changed: a stale `INT` plan reads a `BIGINT` value above 2^31 as a
/// negative one and hands back a wrong number without an error.
async fn a_type_changed_by_the_last_event_of_a_cycle_is_decoded_with_its_new_type(server: &Server) {
    run(
        server,
        &[
            "DROP TABLE IF EXISTS retyped",
            "CREATE TABLE retyped (id BIGINT PRIMARY KEY, n INT)",
        ],
    )
    .await;
    let mut src = source(
        &server.dsn(),
        "name \"retyped_cdc\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"retyped\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"n\" type=\"int64\"",
    );
    assert!(drain(&mut src).await.is_empty());

    run(
        server,
        &[
            "INSERT INTO retyped VALUES (1, 7)",
            "ALTER TABLE retyped MODIFY n BIGINT",
        ],
    )
    .await;
    let first = drain(&mut src).await;
    assert_eq!(first.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

    run(
        server,
        &["INSERT INTO retyped VALUES (2, 5000000000), (3, -5000000000)"],
    )
    .await;
    let second = drain(&mut src).await;
    let values: Vec<i64> = second
        .iter()
        .flat_map(|b| {
            b.column_by_name("n")
                .expect("n")
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(values, vec![5_000_000_000, -5_000_000_000]);
}

/// One transaction of five rows, a two-row batch and a one-batch cap: the cap
/// may not cut inside the transaction, or every cycle re-reads its first batch
/// and the source never gets past it.
async fn a_cap_smaller_than_one_transaction_still_makes_progress(server: &Server) {
    orders(server).await;
    let mut src = source(
        &server.dsn(),
        &config(
            "wide_tx_cdc",
            "orders",
            "",
            "batch_rows 2\nmax_batches_per_cycle 1",
        ),
    );
    assert!(drain(&mut src).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')",
            "INSERT INTO orders VALUES (6,'f')",
        ],
    )
    .await;

    let mut seen = Vec::new();
    for _ in 0..4 {
        seen.extend(ops(&changes(&drain(&mut src).await)));
        if seen.len() >= 6 {
            break;
        }
    }
    assert_eq!(
        seen,
        (1..=6).map(|id| pair("I", id)).collect::<Vec<_>>(),
        "every change once and in order"
    );
}

/// The binlog carries a stored generated column, and a virtual one too on
/// MySQL, so the row image and the catalog count the same columns.
async fn a_table_with_generated_columns_is_read(server: &Server) {
    run(
        server,
        &[
            "DROP TABLE IF EXISTS gen",
            "CREATE TABLE gen (id BIGINT PRIMARY KEY, a INT NOT NULL, \
             v INT AS (a * 2) VIRTUAL, s INT AS (a + 1) STORED, z INT NOT NULL DEFAULT 9)",
        ],
    )
    .await;
    let mut src = source(
        &server.dsn(),
        "name \"gen_cdc\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"gen\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"a\" type=\"int32\" nullable=#false\n\
         schema_fields \"s\" type=\"int32\"\n\
         schema_fields \"z\" type=\"int32\" nullable=#false",
    );
    assert!(drain(&mut src).await.is_empty());
    run(server, &["INSERT INTO gen (id, a) VALUES (1, 5)"]).await;
    let batches = drain(&mut src).await;
    let column = |name: &str| -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                b.column_by_name(name)
                    .expect("column")
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .to_vec()
            })
            .collect()
    };
    assert_eq!(column("a"), vec![5]);
    assert_eq!(column("s"), vec![6]);
    assert_eq!(column("z"), vec![9]);
}

/// A rotated log is followed across the rotation, and the offset it leaves
/// resumes in the newest file once the older ones are gone.
async fn a_rotated_log_is_followed_and_the_new_file_resumes(server: &Server) {
    orders(server).await;
    let text = config("rotate_cdc", "orders", "", "batch_rows 100");
    let mut src = source(&server.dsn(), &text);
    assert!(drain(&mut src).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a')",
            "FLUSH BINARY LOGS",
            "INSERT INTO orders VALUES (2, 'b')",
            "FLUSH BINARY LOGS",
        ],
    )
    .await;
    let rows = changes(&drain(&mut src).await);
    assert_eq!(ops(&rows), vec![pair("I", 1), pair("I", 2)]);
    assert_ne!(rows[0].file, rows[1].file, "the rows come from two files");
    assert!(drain(&mut src).await.is_empty());
    drop(src);

    let mut conn = server.conn().await;
    let logs: Vec<mysql_async::Row> = conn.query("SHOW BINARY LOGS").await.expect("list the logs");
    let newest: String = logs.last().and_then(|row| row.get(0)).expect("a log name");
    conn.query_drop(format!("PURGE BINARY LOGS TO '{newest}'"))
        .await
        .expect("purge the older logs");
    let stored = stored_offset(server, "rotate_cdc")
        .await
        .expect("an offset row");
    assert!(
        stored.starts_with(&format!("file:{newest}:")),
        "the offset names the newest file, {stored}"
    );

    run(server, &["INSERT INTO orders VALUES (3, 'c')"]).await;
    let mut again = source(&server.dsn(), &text);
    assert_eq!(
        ops(&changes(&drain(&mut again).await)),
        vec![pair("I", 3)],
        "only what happened after the stored position"
    );
}

/// An `INVISIBLE` column is in the row image like any other.
async fn an_invisible_column_is_counted_in_the_row_image(server: &Server) {
    run(
        server,
        &[
            "DROP TABLE IF EXISTS hidden",
            "CREATE TABLE hidden (id BIGINT PRIMARY KEY, secret INT INVISIBLE, note VARCHAR(40))",
        ],
    )
    .await;
    let mut src = source(
        &server.dsn(),
        "name \"hidden_cdc\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"hidden\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"secret\" type=\"int32\"\n\
         schema_fields \"note\" type=\"utf8\"",
    );
    assert!(drain(&mut src).await.is_empty());
    run(
        server,
        &["INSERT INTO hidden (id, secret, note) VALUES (1, 7, 'n')"],
    )
    .await;
    let batches = drain(&mut src).await;
    assert_eq!(
        batches[0]
            .column_by_name("secret")
            .expect("secret")
            .as_primitive::<arrow_array::types::Int32Type>()
            .value(0),
        7
    );
    assert_eq!(
        batches[0]
            .column_by_name("note")
            .expect("note")
            .as_string::<i32>()
            .value(0),
        "n"
    );
}

/// The stream runner drops a `next_batch` on cancellation. Dropped at any
/// point of a first call (connect, preflight, catalog, position, dump, events)
/// the next call must still read every change since the stored position.
async fn a_next_batch_dropped_mid_flight_loses_nothing(server: &Server) {
    orders(server).await;
    let text = config("dropped_cdc", "orders", "", "batch_rows 100");
    // Stores `now` at once.
    assert!(drain(&mut source(&server.dsn(), &text)).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a')",
            "INSERT INTO orders VALUES (2, 'b')",
            "INSERT INTO orders VALUES (3, 'c')",
        ],
    )
    .await;

    for micros in (300..60_000).step_by(1_100) {
        let mut src = source(&server.dsn(), &text);
        let cancelled =
            tokio::time::timeout(std::time::Duration::from_micros(micros), src.next_batch()).await;
        let mut batches = Vec::new();
        match cancelled {
            Ok(first) => batches.extend(first.expect("first batch")),
            Err(_elapsed) => {}
        }
        loop {
            match src.next_batch().await {
                Ok(Some(batch)) => batches.push(batch),
                Ok(None) => break,
                Err(e) => panic!("after a drop at {micros}us: {}", e.message()),
            }
        }
        assert_eq!(
            ops(&changes(&batches)),
            vec![pair("I", 1), pair("I", 2), pair("I", 3)],
            "after a drop at {micros}us"
        );
    }
}

/// A snapshot page dropped mid-flight leaves a connection and a read view that
/// cannot be trusted, so the snapshot starts over and still delivers every
/// row once, then streams.
async fn a_snapshot_whose_next_batch_was_dropped_starts_over(server: &Server) {
    orders(server).await;
    run(
        server,
        &["INSERT INTO orders VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')"],
    )
    .await;
    for (round, micros) in (300..60_000).step_by(2_300).enumerate() {
        let mut src = source(
            &server.dsn(),
            &config(
                &format!("snap_drop_{round}"),
                "orders",
                "snapshot=\"initial\"",
                "batch_rows 2",
            ),
        );
        let cancelled =
            tokio::time::timeout(std::time::Duration::from_micros(micros), src.next_batch()).await;
        let mut batches = Vec::new();
        if let Ok(first) = cancelled {
            batches.extend(first.unwrap_or_else(|e| panic!("{}", e.message())));
        }
        loop {
            match src.next_batch().await {
                Ok(Some(batch)) => batches.push(batch),
                Ok(None) => break,
                Err(e) => panic!("after a drop at {micros}us: {}", e.message()),
            }
        }
        assert_eq!(
            ops(&changes(&batches)),
            (1..=5).map(|id| pair("R", id)).collect::<Vec<_>>(),
            "after a drop at {micros}us"
        );
    }
}

/// The server's current binlog position.
async fn binlog_position(server: &Server) -> u64 {
    let mut conn = server.conn().await;
    let row: mysql_async::Row = conn
        .query_first(if server.flavor == Flavor::Mysql {
            "SHOW BINARY LOG STATUS"
        } else {
            "SHOW BINLOG STATUS"
        })
        .await
        .expect("status")
        .expect("a row");
    row.get(1).expect("a position")
}

/// The source's own offset write is a transaction in the binlog it reads. If
/// reading it counted as news, every cycle would store a position, and that
/// store would be news to the next cycle: an idle source would write forever.
async fn an_idle_source_does_not_keep_writing_its_own_offset(server: &Server) {
    orders(server).await;
    let mut src = source(
        &server.dsn(),
        &config("idle_cdc", "orders", "", "batch_rows 100"),
    );
    // Resolves `now` and stores it, once.
    assert!(drain(&mut src).await.is_empty());
    let settled = binlog_position(server).await;
    for _ in 0..6 {
        assert!(drain(&mut src).await.is_empty());
    }
    assert_eq!(
        binlog_position(server).await,
        settled,
        "six idle cycles wrote to the binlog"
    );
}

/// What other tables do is news: the position follows it, so a quiet table on
/// a busy server does not leave its stored position behind, and a rotation of
/// the log moves the stored file too, so an idle source never names a purged
/// one.
async fn the_stored_position_follows_other_tables_and_a_rotation_but_not_its_own_writes(
    server: &Server,
) {
    orders(server).await;
    run(server, &["CREATE TABLE other_news (a INT)"]).await;
    let mut src = source(
        &server.dsn(),
        &config("news_cdc", "orders", "", "batch_rows 100"),
    );
    assert!(drain(&mut src).await.is_empty());
    assert!(drain(&mut src).await.is_empty());
    let first = stored_offset(server, "news_cdc").await.expect("an offset");

    run(server, &["INSERT INTO other_news VALUES (1)"]).await;
    assert!(drain(&mut src).await.is_empty(), "this cycle reads it");
    assert!(drain(&mut src).await.is_empty(), "this one stores it");
    let second = stored_offset(server, "news_cdc").await.expect("an offset");
    assert_ne!(
        second, first,
        "the position moved past the other table's write"
    );
    let settled = binlog_position(server).await;
    assert!(drain(&mut src).await.is_empty());
    assert_eq!(binlog_position(server).await, settled);

    run(server, &["FLUSH BINARY LOGS"]).await;
    assert!(
        drain(&mut src).await.is_empty(),
        "this cycle reads the rotation"
    );
    assert!(drain(&mut src).await.is_empty(), "this one stores it");
    let third = stored_offset(server, "news_cdc").await.expect("an offset");
    let file = |offset: &str| {
        offset
            .strip_prefix("file:")
            .and_then(|rest| rest.rsplit_once(':'))
            .map(|(file, _)| file.to_string())
            .expect("a file offset")
    };
    assert_ne!(
        file(&third),
        file(&second),
        "the stored file is the new one"
    );
}

// ----------------------------------------------------------------------------
// Schema history: a restart from a stored position decodes every row with the
// definition of the table that was in force when the row was written
// ----------------------------------------------------------------------------

/// A `cdc_binlog` config declaring `__op`, `id` and `columns` (name, type);
/// every column but `id` is nullable.
fn history_config(name: &str, table: &str, mode: &str, columns: &[(&str, &str)]) -> String {
    let mut text = format!(
        "name \"{name}\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"{table}\" {mode}\n\
         schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n"
    );
    for (column, ty) in columns {
        text.push_str(&format!("schema_fields \"{column}\" type=\"{ty}\"\n"));
    }
    text
}

/// `<op><id>` of every row, in order: `I1`, `U1`, `D2`.
fn op_ids(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for batch in batches {
        let op = batch
            .column_by_name("__op")
            .expect("__op")
            .as_string::<i32>();
        let id = batch
            .column_by_name("id")
            .expect("id")
            .as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            out.push(format!("{}{}", op.value(row), id.value(row)));
        }
    }
    out
}

/// One `int64` column of every row, `None` for NULL.
fn ints(batches: &[RecordBatch], column: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for batch in batches {
        let values = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column {column}"))
            .as_primitive::<Int64Type>();
        out.extend((0..values.len()).map(|row| (!values.is_null(row)).then(|| values.value(row))));
    }
    out
}

/// One `utf8` column of every row, `None` for NULL.
fn texts(batches: &[RecordBatch], column: &str) -> Vec<Option<String>> {
    let mut out = Vec::new();
    for batch in batches {
        let values = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column {column}"))
            .as_string::<i32>();
        out.extend(
            (0..values.len())
                .map(|row| (!values.is_null(row)).then(|| values.value(row).to_string())),
        );
    }
    out
}

/// One `timestamp_micros` column of every row, `None` for NULL.
fn micros(batches: &[RecordBatch], column: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for batch in batches {
        let values = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column {column}"))
            .as_primitive::<TimestampMicrosecondType>();
        out.extend((0..values.len()).map(|row| (!values.is_null(row)).then(|| values.value(row))));
    }
    out
}

fn strs(items: &[&str]) -> Vec<Option<String>> {
    items.iter().map(|item| Some((*item).to_string())).collect()
}

/// How many definitions the schema history of source `name` holds.
async fn history_rows(server: &Server, name: &str) -> i64 {
    let mut conn = server.conn().await;
    conn.exec_first(
        "SELECT COUNT(*) FROM saci_source_offsets_schema WHERE source_name = ?",
        (name,),
    )
    .await
    .expect("count the history rows")
    .expect("a count")
}

/// `ALTER TABLE ... ADD COLUMN` while the source is stopped: rows written
/// before it have one column fewer than the table now has. Declared as it was,
/// the source reads them as it always did; declaring the new column reads NULL
/// for them. Two sources share the table and the history table.
async fn a_column_added_while_the_source_was_stopped_is_read_with_each_rows_own_definition(
    server: &Server,
) {
    orders(server).await;
    let same = history_config("added_same", "orders", "", &[("note", "utf8")]);
    let narrow = history_config("added_wide", "orders", "", &[("note", "utf8")]);
    for text in [&same, &narrow] {
        assert!(drain(&mut source(&server.dsn(), text)).await.is_empty());
    }
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a'), (2, 'b')",
            "ALTER TABLE orders ADD COLUMN extra INT",
            "INSERT INTO orders VALUES (3, 'c', 30)",
            "UPDATE orders SET note = 'a2' WHERE id = 1",
        ],
    )
    .await;

    let rows = drain(&mut source(&server.dsn(), &same)).await;
    assert_eq!(op_ids(&rows), ["I1", "I2", "I3", "U1"]);
    assert_eq!(texts(&rows, "note"), strs(&["a", "b", "c", "a2"]));

    let wide = history_config(
        "added_wide",
        "orders",
        "",
        &[("note", "utf8"), ("extra", "int64")],
    );
    let mut src = source(&server.dsn(), &wide);
    let rows = drain(&mut src).await;
    assert_eq!(op_ids(&rows), ["I1", "I2", "I3", "U1"]);
    assert_eq!(ints(&rows, "extra"), [None, None, Some(30), None]);

    // The next start resumes after the ALTER, from the recorded definition.
    assert!(drain(&mut src).await.is_empty());
    drop(src);
    run(server, &["INSERT INTO orders VALUES (4, 'd', 40)"]).await;
    let rows = drain(&mut source(&server.dsn(), &wide)).await;
    assert_eq!(op_ids(&rows), ["I4"]);
    assert_eq!(ints(&rows, "extra"), [Some(40)]);

    assert!(history_rows(server, "added_same").await >= 1);
    assert!(history_rows(server, "added_wide").await >= 1);
}

/// A type changed while the source was stopped: the rows before it carry the
/// old wire type, the rows after it the new one, and the declared type fills
/// both. Decimal precision and the fractional digits of a datetime are wire
/// shapes too.
async fn a_column_retyped_while_the_source_was_stopped_decodes_the_rows_of_both_definitions(
    server: &Server,
) {
    // INT to BIGINT, with values after it that an INT cannot hold.
    run(
        server,
        &[
            "DROP TABLE IF EXISTS shaped",
            "CREATE TABLE shaped (id BIGINT PRIMARY KEY, n INT, d DECIMAL(10,2))",
        ],
    )
    .await;
    let text = history_config("shaped_int", "shaped", "", &[("n", "int64"), ("d", "utf8")]);
    assert!(drain(&mut source(&server.dsn(), &text)).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO shaped VALUES (1, 7, 1.50)",
            "ALTER TABLE shaped MODIFY n BIGINT",
            "INSERT INTO shaped VALUES (2, 5000000000, 2.25), (3, -5000000000, -0.50)",
        ],
    )
    .await;
    let rows = drain(&mut source(&server.dsn(), &text)).await;
    assert_eq!(op_ids(&rows), ["I1", "I2", "I3"]);
    assert_eq!(
        ints(&rows, "n"),
        [Some(7), Some(5_000_000_000), Some(-5_000_000_000)]
    );
    assert_eq!(texts(&rows, "d"), strs(&["1.50", "2.25", "-0.50"]));

    // DECIMAL(10,2) to DECIMAL(12,4).
    run(
        server,
        &[
            "DROP TABLE shaped",
            "CREATE TABLE shaped (id BIGINT PRIMARY KEY, n BIGINT, d DECIMAL(10,2))",
        ],
    )
    .await;
    let text = history_config("shaped_dec", "shaped", "", &[("d", "utf8")]);
    assert!(drain(&mut source(&server.dsn(), &text)).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO shaped VALUES (1, 7, 1.50)",
            "ALTER TABLE shaped MODIFY d DECIMAL(12,4)",
            "INSERT INTO shaped VALUES (2, 8, 2.2500)",
        ],
    )
    .await;
    let rows = drain(&mut source(&server.dsn(), &text)).await;
    assert_eq!(texts(&rows, "d"), strs(&["1.50", "2.2500"]));

    // DATETIME to DATETIME(6): the fractional digits are in the table map.
    run(
        server,
        &[
            "DROP TABLE shaped",
            "CREATE TABLE shaped (id BIGINT PRIMARY KEY, t DATETIME)",
        ],
    )
    .await;
    let text = history_config("shaped_t", "shaped", "", &[("t", "timestamp_micros")]);
    assert!(drain(&mut source(&server.dsn(), &text)).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO shaped VALUES (1, '2024-01-30 10:20:30')",
            "ALTER TABLE shaped MODIFY t DATETIME(6)",
            "INSERT INTO shaped VALUES (2, '2024-01-31 11:22:33.123456')",
        ],
    )
    .await;
    let rows = drain(&mut source(&server.dsn(), &text)).await;
    assert_eq!(
        micros(&rows, "t"),
        [Some(1_706_610_030_000_000), Some(1_706_700_153_123_456)]
    );
}

/// A column dropped from the middle of the table shifts the ones after it:
/// the rows written before the DROP still carry it, at its old index.
async fn a_column_dropped_while_the_source_was_stopped_still_decodes_the_older_rows(
    server: &Server,
) {
    run(
        server,
        &[
            "DROP TABLE IF EXISTS dropped",
            "CREATE TABLE dropped (id BIGINT PRIMARY KEY, a INT, b INT, c INT)",
        ],
    )
    .await;
    let text = history_config(
        "dropped_cdc",
        "dropped",
        "",
        &[("a", "int64"), ("b", "int64"), ("c", "int64")],
    );
    assert!(drain(&mut source(&server.dsn(), &text)).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO dropped VALUES (1, 10, 20, 30)",
            "ALTER TABLE dropped DROP COLUMN b",
            "INSERT INTO dropped VALUES (2, 11, 31)",
        ],
    )
    .await;
    let rows = drain(&mut source(&server.dsn(), &text)).await;
    assert_eq!(op_ids(&rows), ["I1", "I2"]);
    assert_eq!(ints(&rows, "a"), [Some(10), Some(11)]);
    assert_eq!(
        ints(&rows, "b"),
        [Some(20), None],
        "the dropped column is NULL after the DROP"
    );
    assert_eq!(ints(&rows, "c"), [Some(30), Some(31)]);
}

/// A declared column that no definition in range has is a typo, refused as
/// before; one the definition at the stored position lacks is NULL, but not for
/// a field declared `nullable=#false`.
async fn a_declared_column_missing_from_a_definition_is_null_or_refused_by_its_nullability(
    server: &Server,
) {
    orders(server).await;
    let narrow = history_config("nullable_cdc", "orders", "", &[("note", "utf8")]);
    assert!(drain(&mut source(&server.dsn(), &narrow)).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a')",
            "ALTER TABLE orders ADD COLUMN extra INT NOT NULL DEFAULT 0",
        ],
    )
    .await;

    let required = format!(
        "{}schema_fields \"extra\" type=\"int64\" nullable=#false\n",
        narrow
    );
    let err = source(&server.dsn(), &required)
        .next_batch()
        .await
        .expect_err("a row written before the column cannot fill it");
    assert!(
        err.message().contains("column 'extra'") && err.message().contains("nullable=#false"),
        "{}",
        err.message()
    );

    let typo = format!("{narrow}schema_fields \"exra\" type=\"int64\"\n");
    let err = source(&server.dsn(), &typo)
        .next_batch()
        .await
        .expect_err("a column no definition has");
    assert!(
        err.message()
            .contains("column 'exra' is declared but the server has no such column"),
        "{}",
        err.message()
    );
}

/// A position stored before the history existed (or one whose rows an operator
/// deleted) has no definition behind it: the catalog is taken to describe the
/// rows, and the table map says when it does not.
async fn rows_written_under_another_definition_are_refused_when_no_history_was_recorded(
    server: &Server,
) {
    run(
        server,
        &[
            "DROP TABLE IF EXISTS shaped",
            "CREATE TABLE shaped (id BIGINT PRIMARY KEY, n INT)",
        ],
    )
    .await;
    let text = history_config("nohist_int", "shaped", "", &[("n", "int64")]);
    assert!(drain(&mut source(&server.dsn(), &text)).await.is_empty());
    run(
        server,
        &[
            "DELETE FROM saci_source_offsets_schema",
            "INSERT INTO shaped VALUES (1, 7)",
            "ALTER TABLE shaped MODIFY n BIGINT",
        ],
    )
    .await;
    let err = source(&server.dsn(), &text)
        .next_batch()
        .await
        .expect_err("rows of the old definition are not decoded");
    assert!(
        err.message()
            .contains("column 'n' is a 32-bit integer in the binlog")
            && err.message().contains("a 64-bit integer in the catalog")
            && err.message().contains("schema history"),
        "{}",
        err.message()
    );

    run(
        server,
        &[
            "DROP TABLE shaped",
            "CREATE TABLE shaped (id BIGINT PRIMARY KEY, d DECIMAL(10,2))",
        ],
    )
    .await;
    let text = history_config("nohist_dec", "shaped", "", &[("d", "utf8")]);
    assert!(drain(&mut source(&server.dsn(), &text)).await.is_empty());
    run(
        server,
        &[
            "DELETE FROM saci_source_offsets_schema",
            "INSERT INTO shaped VALUES (1, 1.50)",
            "ALTER TABLE shaped MODIFY d DECIMAL(12,4)",
        ],
    )
    .await;
    let err = source(&server.dsn(), &text)
        .next_batch()
        .await
        .expect_err("a decimal of another scale is not decoded");
    assert!(
        err.message().contains("column 'd' is a decimal(10,2)"),
        "{}",
        err.message()
    );

    run(
        server,
        &[
            "DROP TABLE shaped",
            "CREATE TABLE shaped (id BIGINT PRIMARY KEY, t DATETIME)",
        ],
    )
    .await;
    let text = history_config("nohist_t", "shaped", "", &[("t", "timestamp_micros")]);
    assert!(drain(&mut source(&server.dsn(), &text)).await.is_empty());
    run(
        server,
        &[
            "DELETE FROM saci_source_offsets_schema",
            "INSERT INTO shaped VALUES (1, '2024-01-30 10:20:30')",
            "ALTER TABLE shaped MODIFY t DATETIME(6)",
        ],
    )
    .await;
    let err = source(&server.dsn(), &text)
        .next_batch()
        .await
        .expect_err("a datetime of other digits is not decoded");
    assert!(
        err.message().contains("column 't' is a datetime(0)"),
        "{}",
        err.message()
    );

    // An added column changes the count, which is refused the same way.
    orders(server).await;
    let text = history_config("nohist_add", "orders", "", &[("note", "utf8")]);
    assert!(drain(&mut source(&server.dsn(), &text)).await.is_empty());
    run(
        server,
        &[
            "DELETE FROM saci_source_offsets_schema",
            "INSERT INTO orders VALUES (1, 'a')",
            "ALTER TABLE orders ADD COLUMN extra INT",
        ],
    )
    .await;
    let err = source(&server.dsn(), &text)
        .next_batch()
        .await
        .expect_err("a row of fewer columns is not decoded");
    assert!(
        err.message()
            .contains("changed shape under the binlog (binlog has 2 columns, catalog has 3)")
            && err.message().contains("schema history"),
        "{}",
        err.message()
    );
}

/// `schema_history #false` keeps no table and no history: the catalog is the
/// only definition, as before.
async fn schema_history_off_keeps_no_table_and_refuses_as_before(server: &Server) {
    orders(server).await;
    let text = history_config(
        "nohistory_cdc",
        "orders",
        "schema_history=#false",
        &[("note", "utf8")],
    );
    assert!(drain(&mut source(&server.dsn(), &text)).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a')",
            "ALTER TABLE orders ADD COLUMN extra INT",
        ],
    )
    .await;
    let err = source(&server.dsn(), &text)
        .next_batch()
        .await
        .expect_err("a row of fewer columns is not decoded");
    assert!(
        err.message()
            .contains("changed shape under the binlog (binlog has 2 columns, catalog has 3)"),
        "{}",
        err.message()
    );
    let mut conn = server.conn().await;
    let tables: i64 = conn
        .query_first(
            "SELECT COUNT(*) FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'saci_source_offsets_schema'",
        )
        .await
        .expect("count the tables")
        .expect("a count");
    assert_eq!(tables, 0, "no history table is created");
}

/// The snapshot pins its start position: the definition it read is the one in
/// force there, so a restart from it decodes the rows written before a later
/// ALTER.
async fn a_snapshot_records_the_definition_in_force_at_its_position(server: &Server) {
    orders(server).await;
    run(server, &["INSERT INTO orders VALUES (1, 'a'), (2, 'b')"]).await;
    let narrow = history_config(
        "snap_hist",
        "orders",
        "snapshot=\"initial\"",
        &[("note", "utf8")],
    );
    let mut first = source(&server.dsn(), &narrow);
    let rows = drain(&mut first).await;
    assert_eq!(op_ids(&rows), ["R1", "R2"]);
    // The next cycle stores the position the snapshot pinned.
    assert!(drain(&mut first).await.is_empty());
    drop(first);
    assert_eq!(history_rows(server, "snap_hist").await, 1);

    run(
        server,
        &[
            "INSERT INTO orders VALUES (3, 'c')",
            "ALTER TABLE orders ADD COLUMN extra INT",
            "INSERT INTO orders VALUES (4, 'd', 40)",
        ],
    )
    .await;
    let wide = history_config(
        "snap_hist",
        "orders",
        "snapshot=\"initial\"",
        &[("note", "utf8"), ("extra", "int64")],
    );
    let rows = drain(&mut source(&server.dsn(), &wide)).await;
    assert_eq!(op_ids(&rows), ["I3", "I4"]);
    assert_eq!(ints(&rows, "extra"), [None, Some(40)]);
}

/// Binlog files are ordered by their number, not by when a definition was
/// read: a restart from the first file finds the definition recorded in the
/// second, and one from the third finds it as the newest at or before it.
async fn definitions_are_ordered_across_a_rotated_log(server: &Server) {
    orders(server).await;
    let narrow = history_config("rotated_hist", "orders", "", &[("note", "utf8")]);
    assert!(drain(&mut source(&server.dsn(), &narrow)).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a')",
            "FLUSH BINARY LOGS",
            "ALTER TABLE orders ADD COLUMN extra INT",
            "INSERT INTO orders VALUES (2, 'b', 7)",
            "FLUSH BINARY LOGS",
            "INSERT INTO orders VALUES (3, 'c', 8)",
        ],
    )
    .await;
    let wide = history_config(
        "rotated_hist",
        "orders",
        "",
        &[("note", "utf8"), ("extra", "int64")],
    );
    let mut src = source(&server.dsn(), &wide);
    let rows = drain(&mut src).await;
    assert_eq!(op_ids(&rows), ["I1", "I2", "I3"]);
    assert_eq!(ints(&rows, "extra"), [None, Some(7), Some(8)]);
    assert!(drain(&mut src).await.is_empty());
    drop(src);

    run(server, &["INSERT INTO orders VALUES (4, 'd', 9)"]).await;
    let rows = drain(&mut source(&server.dsn(), &wide)).await;
    assert_eq!(op_ids(&rows), ["I4"]);
    assert_eq!(ints(&rows, "extra"), [Some(9)]);
}

/// The definition a restart can no longer need, the one before the stored
/// position's, is deleted once the position is stored.
async fn history_behind_the_stored_position_is_pruned(server: &Server) {
    orders(server).await;
    let text = history_config("pruned_cdc", "orders", "", &[("note", "utf8")]);
    let mut src = source(&server.dsn(), &text);
    assert!(drain(&mut src).await.is_empty());
    assert_eq!(
        history_rows(server, "pruned_cdc").await,
        1,
        "the definition at the start"
    );
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a')",
            "ALTER TABLE orders ADD COLUMN extra INT",
            "INSERT INTO orders VALUES (2, 'b', 7)",
        ],
    )
    .await;
    assert_eq!(op_ids(&drain(&mut src).await), ["I1", "I2"]);
    assert_eq!(
        history_rows(server, "pruned_cdc").await,
        2,
        "the definition after the ALTER is recorded as it is read"
    );
    // The next cycle stores the position, which is past the first definition.
    assert!(drain(&mut src).await.is_empty());
    assert_eq!(
        history_rows(server, "pruned_cdc").await,
        1,
        "nothing before the stored position's definition is kept"
    );
}

/// A definition recorded for a DDL is authoritative when that DDL is read
/// again, however much newer the catalog is: the second ALTER ran while the
/// source was stopped, and its rows follow the recorded first one.
async fn a_recorded_definition_is_used_when_its_ddl_is_replayed(server: &Server) {
    orders(server).await;
    let first = history_config("replay_hist", "orders", "", &[("note", "utf8")]);
    let mut src = source(&server.dsn(), &first);
    assert!(drain(&mut src).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a')",
            "ALTER TABLE orders ADD COLUMN c1 INT",
            "INSERT INTO orders VALUES (2, 'b', 10)",
        ],
    )
    .await;
    // Read live, so the first ALTER's definition is recorded; dropped before
    // the next cycle stores the position, so the restart replays all of it.
    assert_eq!(op_ids(&drain(&mut src).await), ["I1", "I2"]);
    drop(src);
    assert_eq!(history_rows(server, "replay_hist").await, 2);

    run(
        server,
        &[
            "ALTER TABLE orders ADD COLUMN c2 INT",
            "INSERT INTO orders VALUES (3, 'c', 11, 21)",
        ],
    )
    .await;
    let wide = history_config(
        "replay_hist",
        "orders",
        "",
        &[("note", "utf8"), ("c1", "int64"), ("c2", "int64")],
    );
    let rows = drain(&mut source(&server.dsn(), &wide)).await;
    assert_eq!(op_ids(&rows), ["I1", "I2", "I3"]);
    assert_eq!(ints(&rows, "c1"), [None, Some(10), Some(11)]);
    assert_eq!(ints(&rows, "c2"), [None, None, Some(21)]);
}

/// Two ALTERs ran while the source was stopped, and a row was written between
/// them: the catalog is the second definition and no history holds the first,
/// so those rows cannot be decoded. The error says so and names the remedy.
async fn two_changes_made_while_the_source_was_stopped_are_refused_with_the_remedy(
    server: &Server,
) {
    orders(server).await;
    let text = history_config("twice_hist", "orders", "", &[("note", "utf8")]);
    assert!(drain(&mut source(&server.dsn(), &text)).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a')",
            "ALTER TABLE orders ADD COLUMN c1 INT",
            "INSERT INTO orders VALUES (2, 'b', 10)",
            "ALTER TABLE orders ADD COLUMN c2 INT",
            "INSERT INTO orders VALUES (3, 'c', 11, 21)",
        ],
    )
    .await;
    let err = source(&server.dsn(), &text)
        .next_batch()
        .await
        .expect_err("the first ALTER's definition is not recorded");
    assert!(
        err.message()
            .contains("changed shape under the binlog (binlog has 3 columns, catalog has 4)")
            && err.message().contains("schema history"),
        "{}",
        err.message()
    );
}

/// A source with no stored position starts from the table as it is: a
/// definition recorded by an earlier run may be older than a change made since,
/// and a column renamed in between is a change the table map cannot show.
async fn a_source_started_afresh_reads_the_table_as_it_is_now(server: &Server) {
    orders(server).await;
    let first = history_config("fresh_hist", "orders", "", &[("note", "utf8")]);
    assert!(drain(&mut source(&server.dsn(), &first)).await.is_empty());
    run(
        server,
        &[
            "DELETE FROM saci_source_offsets WHERE source_name = 'fresh_hist'",
            "ALTER TABLE orders CHANGE note memo VARCHAR(40)",
        ],
    )
    .await;

    let renamed = history_config("fresh_hist", "orders", "", &[("memo", "utf8")]);
    let mut src = source(&server.dsn(), &renamed);
    assert!(drain(&mut src).await.is_empty());
    run(server, &["INSERT INTO orders VALUES (1, 'a')"]).await;
    let rows = drain(&mut src).await;
    assert_eq!(op_ids(&rows), ["I1"]);
    assert_eq!(texts(&rows, "memo"), strs(&["a"]));
    assert_eq!(
        history_rows(server, "fresh_hist").await,
        1,
        "the new start's definition replaced the older one, which its position outdates"
    );
}

/// The history table needs `SELECT` and `INSERT`; `DELETE` only prunes, so a
/// user without it keeps the source running and the table grows. Both tables
/// are pre-created, so no `CREATE` is needed either.
async fn a_user_without_delete_on_the_history_table_keeps_the_source_running(server: &Server) {
    orders(server).await;
    let seed = history_config("grants_seed", "orders", "", &[("note", "utf8")]);
    assert!(drain(&mut source(&server.dsn(), &seed)).await.is_empty());
    let replication = match server.flavor {
        Flavor::Mariadb => "GRANT REPLICATION SLAVE, BINLOG MONITOR ON *.* TO 'nodelete'@'%'",
        Flavor::Mysql => "GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'nodelete'@'%'",
    };
    run(
        server,
        &[
            "DROP USER IF EXISTS 'nodelete'@'%'",
            "CREATE USER 'nodelete'@'%' IDENTIFIED BY 'nodelete'",
            "GRANT SELECT ON saci.* TO 'nodelete'@'%'",
            "GRANT INSERT, UPDATE ON saci.saci_source_offsets TO 'nodelete'@'%'",
            "GRANT INSERT ON saci.saci_source_offsets_schema TO 'nodelete'@'%'",
            replication,
        ],
    )
    .await;

    let text = history_config(
        "nodelete_cdc",
        "orders",
        "offset_table_autocreate=#false",
        &[("note", "utf8")],
    );
    let mut src = source(&server.dsn_as("nodelete", "nodelete"), &text);
    assert!(drain(&mut src).await.is_empty());
    run(
        server,
        &[
            "INSERT INTO orders VALUES (1, 'a')",
            "ALTER TABLE orders ADD COLUMN extra INT",
            "INSERT INTO orders VALUES (2, 'b', 7)",
        ],
    )
    .await;
    assert_eq!(op_ids(&drain(&mut src).await), ["I1", "I2"]);
    // The next cycle stores the position and tries to prune, which is refused.
    assert!(drain(&mut src).await.is_empty());
    assert_eq!(
        history_rows(server, "nodelete_cdc").await,
        2,
        "the refused delete pruned nothing"
    );
}

common::on_every_server!(
    insert_update_delete_arrive_with_their_ops_and_positions_advance,
    changes_to_other_tables_are_skipped,
    a_primary_key_update_arrives_as_delete_then_insert,
    snapshot_initial_reads_existing_rows_as_r_then_streams_changes_made_during_it,
    snapshot_without_a_declared_key_is_refused,
    max_changes_per_cycle_ends_on_a_transaction_boundary,
    request_batch_rows_overrides_the_chunk_size,
    a_new_source_resumes_from_the_offset_table,
    a_cycle_cut_by_max_batches_per_cycle_loses_nothing,
    an_added_column_is_picked_up_mid_stream,
    binlog_row_image_minimal_is_refused,
    a_user_without_replication_privileges_is_told_what_it_needs,
    a_purged_position_names_the_remedy,
    enum_set_bit_year_timestamp_and_unsigned_decode_like_polling,
    extreme_values_render_the_same_text_from_the_binlog_and_polling,
    extreme_values_decode_the_same_typed_value_from_the_binlog_and_polling,
    wide_enum_set_geometry_json_and_large_values_decode_alike,
    a_type_changed_by_the_last_event_of_a_cycle_is_decoded_with_its_new_type,
    a_column_added_while_the_source_was_stopped_is_read_with_each_rows_own_definition,
    a_column_retyped_while_the_source_was_stopped_decodes_the_rows_of_both_definitions,
    a_column_dropped_while_the_source_was_stopped_still_decodes_the_older_rows,
    a_declared_column_missing_from_a_definition_is_null_or_refused_by_its_nullability,
    rows_written_under_another_definition_are_refused_when_no_history_was_recorded,
    schema_history_off_keeps_no_table_and_refuses_as_before,
    a_snapshot_records_the_definition_in_force_at_its_position,
    definitions_are_ordered_across_a_rotated_log,
    history_behind_the_stored_position_is_pruned,
    a_recorded_definition_is_used_when_its_ddl_is_replayed,
    two_changes_made_while_the_source_was_stopped_are_refused_with_the_remedy,
    a_source_started_afresh_reads_the_table_as_it_is_now,
    a_user_without_delete_on_the_history_table_keeps_the_source_running,
    a_cap_smaller_than_one_transaction_still_makes_progress,
    a_table_with_generated_columns_is_read,
    a_rotated_log_is_followed_and_the_new_file_resumes,
    an_invisible_column_is_counted_in_the_row_image,
    a_next_batch_dropped_mid_flight_loses_nothing,
    a_snapshot_whose_next_batch_was_dropped_starts_over,
    an_idle_source_does_not_keep_writing_its_own_offset,
    the_stored_position_follows_other_tables_and_a_rotation_but_not_its_own_writes,
);

// ----------------------------------------------------------------------------
// MySQL only
// ----------------------------------------------------------------------------

const GTID_FIELDS: &str = r#"
schema_fields "__op" type="utf8" nullable=#false
schema_fields "__table" type="utf8" nullable=#false
schema_fields "__binlog_file" type="utf8" nullable=#false
schema_fields "__binlog_pos" type="uint64" nullable=#false
schema_fields "__gtid" type="utf8"
schema_fields "__commit_ts" type="timestamp_micros_utc" nullable=#false
schema_fields "id" type="int64" nullable=#false
schema_fields "note" type="utf8"
"#;

#[tokio::test]
async fn gtid_position_resumes_from_the_stored_gtid_set() {
    let Some(server) = common::try_start(Flavor::Mysql).await else {
        return;
    };
    orders(&server).await;
    let text = format!(
        "name \"gtid_cdc\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"orders\" position=\"gtid\"\n{ORDER_FIELDS}"
    );
    let mut first = source(&server.dsn(), &text);
    assert!(drain(&mut first).await.is_empty());
    run(&server, &["INSERT INTO orders VALUES (1, 'a')"]).await;
    assert_eq!(ops(&changes(&drain(&mut first).await)), vec![pair("I", 1)]);
    assert!(drain(&mut first).await.is_empty());
    drop(first);

    let stored = stored_offset(&server, "gtid_cdc")
        .await
        .expect("an offset row");
    assert!(stored.starts_with("gtid:"), "{stored}");

    run(&server, &["INSERT INTO orders VALUES (2, 'b')"]).await;
    let mut second = source(&server.dsn(), &text);
    assert_eq!(
        ops(&changes(&drain(&mut second).await)),
        vec![pair("I", 2)],
        "only what the stored set has not executed"
    );
    // Quiet again: the set keeps up through transactions of other tables.
    run(
        &server,
        &[
            "CREATE TABLE IF NOT EXISTS other (a INT)",
            "INSERT INTO other VALUES (1)",
        ],
    )
    .await;
    assert!(drain(&mut second).await.is_empty());
    assert!(drain(&mut second).await.is_empty());
}

#[tokio::test]
async fn gtid_column_carries_each_transactions_gtid() {
    let Some(server) = common::try_start(Flavor::Mysql).await else {
        return;
    };
    orders(&server).await;
    let mut conn = server.conn().await;
    let uuid: String = conn
        .query_first("SELECT @@server_uuid")
        .await
        .expect("uuid")
        .expect("a row");
    let text = format!(
        "name \"gtid_col\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"orders\"\n{GTID_FIELDS}"
    );
    let mut src = source(&server.dsn(), &text);
    assert!(drain(&mut src).await.is_empty());

    run(
        &server,
        &[
            "INSERT INTO orders VALUES (1, 'a')",
            "INSERT INTO orders VALUES (2, 'b')",
        ],
    )
    .await;
    let batches = drain(&mut src).await;
    let gtids: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let column = b
                .column_by_name("__gtid")
                .expect("__gtid")
                .as_string::<i32>();
            (0..b.num_rows())
                .map(|row| column.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(gtids.len(), 2);
    let number = |gtid: &str| -> u64 {
        gtid.strip_prefix(&format!("{uuid}:"))
            .unwrap_or_else(|| panic!("{gtid} is not {uuid}:N"))
            .parse()
            .expect("a transaction number")
    };
    assert_eq!(number(&gtids[1]), number(&gtids[0]) + 1);
}

#[tokio::test]
async fn a_tagged_gtid_is_read_and_resumed() {
    let Some(server) = common::try_start(Flavor::Mysql).await else {
        return;
    };
    orders(&server).await;
    let text = format!(
        "name \"gtid_tag\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"orders\" position=\"gtid\"\n{GTID_FIELDS}"
    );
    let mut src = source(&server.dsn(), &text);
    assert!(drain(&mut src).await.is_empty());

    let mut conn = server.conn().await;
    conn.query_drop("SET @@session.gtid_next = 'AUTOMATIC:blue'")
        .await
        .expect("a tagged gtid_next");
    conn.query_drop("INSERT INTO orders VALUES (1, 'tagged')")
        .await
        .expect("insert");
    conn.query_drop("SET @@session.gtid_next = 'AUTOMATIC'")
        .await
        .expect("reset");
    conn.query_drop("INSERT INTO orders VALUES (2, 'plain')")
        .await
        .expect("insert");

    let batches = drain(&mut src).await;
    let rows = changes(&batches);
    assert_eq!(ops(&rows), vec![pair("I", 1), pair("I", 2)]);
    let first_gtid = batches[0]
        .column_by_name("__gtid")
        .expect("__gtid")
        .as_string::<i32>()
        .value(0)
        .to_string();
    assert!(first_gtid.contains(":blue:"), "{first_gtid}");

    // The stored set carries the tag, and resuming from it replays nothing.
    assert!(drain(&mut src).await.is_empty());
    drop(src);
    let stored = stored_offset(&server, "gtid_tag")
        .await
        .expect("an offset row");
    assert!(stored.contains(":blue:"), "{stored}");
    let mut again = source(&server.dsn(), &text);
    assert!(drain(&mut again).await.is_empty());
}

#[tokio::test]
async fn a_compressed_transaction_is_read_with_its_outer_position() {
    let Some(server) = common::try_start(Flavor::Mysql).await else {
        return;
    };
    orders(&server).await;
    let mut src = source(
        &server.dsn(),
        &config("zip_cdc", "orders", "", "batch_rows 100"),
    );
    assert!(drain(&mut src).await.is_empty());

    let mut conn = server.conn().await;
    conn.query_drop("SET SESSION binlog_transaction_compression = ON")
        .await
        .expect("compression");
    conn.query_drop("START TRANSACTION").await.expect("begin");
    conn.query_drop("INSERT INTO orders VALUES (1, 'a'), (2, 'b')")
        .await
        .expect("insert");
    conn.query_drop("UPDATE orders SET note = 'z' WHERE id = 1")
        .await
        .expect("update");
    conn.query_drop("COMMIT").await.expect("commit");

    let rows = changes(&drain(&mut src).await);
    assert_eq!(ops(&rows), vec![pair("I", 1), pair("I", 2), pair("U", 1)]);
    assert!(
        rows[0].pos > 0 && rows.iter().all(|r| r.pos == rows[0].pos),
        "{rows:?}"
    );

    // The position it left resumes cleanly.
    assert!(drain(&mut src).await.is_empty());
    assert!(drain(&mut src).await.is_empty());
}

/// The executed GTID set, as the server writes it.
async fn gtid_executed(server: &Server) -> String {
    let mut conn = server.conn().await;
    conn.query_first("SELECT @@global.gtid_executed")
        .await
        .expect("gtid_executed")
        .expect("a value")
}

/// The GTID counterpart of the idle-source case: the executed set must stop
/// growing once the source has stored its start.
#[tokio::test]
async fn an_idle_gtid_source_does_not_keep_writing_its_own_offset() {
    let Some(server) = common::try_start(Flavor::Mysql).await else {
        return;
    };
    orders(&server).await;
    let mut src = source(
        &server.dsn(),
        &format!(
            "name \"gtid_idle\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"orders\" position=\"gtid\"\n{ORDER_FIELDS}"
        ),
    );
    assert!(drain(&mut src).await.is_empty());
    let settled = gtid_executed(&server).await;
    for _ in 0..6 {
        assert!(drain(&mut src).await.is_empty());
    }
    assert_eq!(
        gtid_executed(&server).await,
        settled,
        "six idle cycles wrote"
    );
}

/// A server that has executed nothing resolves `now` to the empty set. Stored
/// like any other start, so a restart before the first transaction resumes
/// from it instead of resolving `now` again and skipping what ran in between.
#[tokio::test]
async fn an_empty_gtid_start_is_stored_so_a_restart_reads_what_ran_since() {
    let Some(server) = common::try_start(Flavor::Mysql).await else {
        return;
    };
    orders(&server).await;
    let body = |name: &str| {
        format!(
            "name \"{name}\"\nbatch_rows 100\n\nmode kind=\"cdc_binlog\" table=\"orders\" position=\"gtid\"\n{ORDER_FIELDS}"
        )
    };
    // Creates the offset table, so the source under test creates nothing.
    assert!(
        drain(&mut source(&server.dsn(), &body("gtid_seed")))
            .await
            .is_empty()
    );
    run(&server, &["RESET BINARY LOGS AND GTIDS"]).await;
    assert_eq!(gtid_executed(&server).await, "", "nothing executed");

    let text = body("gtid_empty");
    let mut first = source(&server.dsn(), &text);
    assert!(drain(&mut first).await.is_empty());
    assert_eq!(
        stored_offset(&server, "gtid_empty").await.as_deref(),
        Some("gtid:"),
        "the empty start is stored at once"
    );
    run(&server, &["INSERT INTO orders VALUES (1, 'a')"]).await;
    drop(first);

    let mut second = source(&server.dsn(), &text);
    assert_eq!(
        ops(&changes(&drain(&mut second).await)),
        vec![pair("I", 1)],
        "the restart resumes from the stored empty set"
    );
}

/// Definitions are tagged with the executed GTID set after their DDL, and a
/// restart takes the newest one that set contains. Two rounds, each stopped
/// across one ALTER: an added column, then a wider type with values the old
/// one cannot hold.
#[tokio::test]
async fn a_gtid_source_decodes_rows_across_altered_definitions() {
    let Some(server) = common::try_start(Flavor::Mysql).await else {
        return;
    };
    run(
        &server,
        &[
            "DROP TABLE IF EXISTS gtid_hist",
            "CREATE TABLE gtid_hist (id BIGINT PRIMARY KEY, n INT)",
        ],
    )
    .await;
    let narrow = history_config(
        "gtid_hist_cdc",
        "gtid_hist",
        "position=\"gtid\"",
        &[("n", "int64")],
    );
    assert!(drain(&mut source(&server.dsn(), &narrow)).await.is_empty());
    run(
        &server,
        &[
            "INSERT INTO gtid_hist VALUES (1, 7)",
            "ALTER TABLE gtid_hist ADD COLUMN extra INT",
            "INSERT INTO gtid_hist VALUES (2, 8, 80)",
        ],
    )
    .await;
    let wide = history_config(
        "gtid_hist_cdc",
        "gtid_hist",
        "position=\"gtid\"",
        &[("n", "int64"), ("extra", "int64")],
    );
    let mut src = source(&server.dsn(), &wide);
    let rows = drain(&mut src).await;
    assert_eq!(op_ids(&rows), ["I1", "I2"]);
    assert_eq!(ints(&rows, "n"), [Some(7), Some(8)]);
    assert_eq!(ints(&rows, "extra"), [None, Some(80)]);
    assert!(drain(&mut src).await.is_empty());
    drop(src);
    let stored = stored_offset(&server, "gtid_hist_cdc")
        .await
        .expect("an offset row");
    assert!(stored.starts_with("gtid:"), "{stored}");

    run(
        &server,
        &[
            "INSERT INTO gtid_hist VALUES (3, 9, 90)",
            "ALTER TABLE gtid_hist MODIFY n BIGINT",
            "INSERT INTO gtid_hist VALUES (4, 5000000000, 100)",
        ],
    )
    .await;
    let rows = drain(&mut source(&server.dsn(), &wide)).await;
    assert_eq!(op_ids(&rows), ["I3", "I4"]);
    assert_eq!(ints(&rows, "n"), [Some(9), Some(5_000_000_000)]);
    assert_eq!(ints(&rows, "extra"), [Some(90), Some(100)]);
}
// ----------------------------------------------------------------------------
// MariaDB only
// ----------------------------------------------------------------------------

/// `log_bin_compress` writes a long enough row event compressed, which the
/// driver cannot decode. Skipping it would drop its changes and move the
/// position past them, so a compressed event of the table is an error, one of
/// another table is skipped, and a server that compresses is refused at the
/// start.
#[tokio::test]
async fn a_row_event_compressed_by_mariadb_is_an_error_not_a_silent_skip() {
    let Some(server) = common::try_start(Flavor::Mariadb).await else {
        return;
    };
    orders(&server).await;
    run(
        &server,
        &[
            "DROP TABLE IF EXISTS other",
            "CREATE TABLE other (id BIGINT PRIMARY KEY, note VARCHAR(80))",
        ],
    )
    .await;
    let mut src = source(
        &server.dsn(),
        &config("zip_maria", "orders", "", "batch_rows 100"),
    );
    assert!(drain(&mut src).await.is_empty());

    run(
        &server,
        &[
            "SET GLOBAL log_bin_compress = ON",
            "SET GLOBAL log_bin_compress_min_len = 10",
        ],
    )
    .await;
    // The setting reaches sessions opened after it. The statement is long
    // enough to be a compressed query event too.
    let long = "a".repeat(40);
    let mut conn = server.conn().await;
    for statement in [
        "ALTER TABLE other ADD COLUMN extra_column_for_the_compressed_query INT".to_string(),
        format!("INSERT INTO other (id, note) VALUES (1, '{long}')"),
    ] {
        conn.query_drop(statement.as_str())
            .await
            .unwrap_or_else(|e| panic!("{statement}: {e}"));
    }
    assert!(
        drain(&mut src).await.is_empty(),
        "a compressed event of another table, and a compressed statement, are skipped"
    );

    conn.query_drop(format!("INSERT INTO orders VALUES (1, '{long}')"))
        .await
        .expect("insert");
    let err = src
        .next_batch()
        .await
        .expect_err("a compressed row is refused");
    assert!(
        err.message().contains("log_bin_compress") && err.message().contains("cannot decode"),
        "{}",
        err.message()
    );

    let mut fresh = source(
        &server.dsn(),
        &config("zip_maria_fresh", "orders", "", "batch_rows 100"),
    );
    let err = fresh
        .next_batch()
        .await
        .expect_err("a compressing server is refused");
    assert_eq!(err.category(), "configuration", "{}", err.message());
    assert!(
        err.message().contains("log_bin_compress = OFF"),
        "{}",
        err.message()
    );

    // Switched off, a new source starts after the compressed rows and reads on.
    run(&server, &["SET GLOBAL log_bin_compress = OFF"]).await;
    let mut after = source(
        &server.dsn(),
        &config("zip_maria_after", "orders", "", "batch_rows 100"),
    );
    assert!(drain(&mut after).await.is_empty());
    run(&server, &["INSERT INTO orders VALUES (2, 'plain')"]).await;
    assert_eq!(ops(&changes(&drain(&mut after).await)), vec![pair("I", 2)]);
}

#[tokio::test]
async fn position_gtid_is_refused_on_mariadb_naming_position_file() {
    let Some(server) = common::try_start(Flavor::Mariadb).await else {
        return;
    };
    orders(&server).await;
    let mut src = source(
        &server.dsn(),
        &format!(
            "name \"maria_gtid\"\n\nmode kind=\"cdc_binlog\" table=\"orders\" position=\"gtid\"\n{ORDER_FIELDS}"
        ),
    );
    let err = src.next_batch().await.expect_err("refused");
    assert_eq!(err.category(), "configuration", "{}", err.message());
    assert!(
        err.message().contains(
            "position \"gtid\" streams MySQL GTIDs only; use position \"file\" on MariaDB"
        ),
        "{}",
        err.message()
    );
}

#[tokio::test]
async fn mariadb_uuid_column_is_refused_in_binlog_mode() {
    let Some(server) = common::try_start(Flavor::Mariadb).await else {
        return;
    };
    run(
        &server,
        &[
            "DROP TABLE IF EXISTS ids",
            "CREATE TABLE ids (id BIGINT PRIMARY KEY, u UUID)",
        ],
    )
    .await;
    let mut src = source(
        &server.dsn(),
        "name \"uuid_cdc\"\n\nmode kind=\"cdc_binlog\" table=\"ids\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"u\" type=\"utf8\"",
    );
    let err = src.next_batch().await.expect_err("refused");
    assert_eq!(err.category(), "configuration", "{}", err.message());
    assert!(err.message().contains("column 'u'"), "{}", err.message());
    assert!(
        err.message().to_ascii_lowercase().contains("binlog"),
        "{}",
        err.message()
    );
}
