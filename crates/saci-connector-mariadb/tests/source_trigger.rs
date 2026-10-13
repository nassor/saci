//! `cdc_trigger` against a real MariaDB and a real MySQL server: an operator's
//! outbox, and the changelog and triggers the connector installs.
//!
//! Soft-skips a flavour whose container cannot start; see `common::try_start`.

mod common;

use std::time::Duration;

use arrow_array::{
    Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt64Array,
};
use common::Server;
use mysql_async::Conn;
use mysql_async::prelude::Queryable;
use saci_connector_mariadb::MariadbSource;
use saci_core::io::source::Source;

common::on_every_server!(
    outbox_mode_reads_the_operator_table_and_delete_acked_prunes_it,
    a_delete_acked_outbox_over_an_unsigned_cursor_above_two_to_the_63_prunes_exactly,
    installed_triggers_capture_insert_update_and_delete,
    a_primary_key_change_arrives_as_delete_then_insert,
    installing_twice_reuses_the_triggers_and_changelog,
    a_foreign_trigger_with_the_same_name_is_refused,
    a_changelog_that_no_longer_matches_the_table_is_refused,
    installed_mode_prunes_acknowledged_changelog_rows_by_default,
    retention_keep_leaves_the_changelog_rows_in_place,
    a_user_without_the_trigger_privilege_is_told_what_it_needs,
    a_changelog_in_a_latin1_schema_still_captures_an_emoji_row,
    a_zero_date_row_does_not_fail_the_writers_statement,
    a_column_widened_under_a_running_source_is_refused_not_truncated,
    a_column_added_under_a_running_source_does_not_stop_it,
    a_row_committed_after_a_later_one_is_still_delivered,
    delete_acked_never_prunes_past_the_delivered_point,
    a_rolled_back_hole_is_given_up_after_the_timeout_without_innodb_trx,
    a_rolled_back_hole_is_released_at_once_when_innodb_trx_is_readable,
    a_transaction_open_longer_than_the_timeout_is_still_delivered_when_innodb_trx_is_readable,
    a_transaction_open_longer_than_the_timeout_is_lost_without_innodb_trx,
    a_hole_a_transaction_could_fill_is_given_up_at_gap_max_wait_ms,
    a_capped_hole_whose_transaction_is_still_open_does_not_stall_the_prune,
    a_stale_transaction_list_never_settles_a_hole_that_is_still_open,
    a_hole_open_across_a_restart_is_still_filled_by_the_late_commit,
    a_restart_waits_a_rolled_back_hole_out_again_without_innodb_trx,
    a_sequence_that_steps_by_more_than_one_has_no_false_holes,
    an_outbox_row_committed_after_a_later_one_is_delivered_and_never_pruned_early,
    rows_before_a_hole_go_out_and_the_ones_behind_it_wait_across_batches,
    an_open_writer_at_the_tail_does_not_block_the_prune,
);

const ORDERS_FIELDS: &str = "
schema_fields \"__op\" type=\"utf8\" nullable=#false
schema_fields \"__seq\" type=\"uint64\" nullable=#false
schema_fields \"__commit_ts\" type=\"timestamp_micros_utc\" nullable=#false
schema_fields \"__table\" type=\"utf8\" nullable=#false
schema_fields \"id\" type=\"int64\" nullable=#false
schema_fields \"label\" type=\"utf8\"
schema_fields \"qty\" type=\"int32\"
";

/// The installed-trigger source `name` over `orders`, with `mode_extra` added to
/// the `mode` line.
fn installed(dsn: &str, name: &str, mode_extra: &str) -> MariadbSource {
    common::source(
        dsn,
        &format!(
            "name \"{name}\"\nbatch_rows 100\n\n\
             mode kind=\"cdc_trigger\" table=\"orders\" install_triggers=#true {mode_extra}\n\
             {ORDERS_FIELDS}"
        ),
    )
}

async fn orders(server: &Server) {
    let mut conn = server.conn().await;
    conn.query_drop(
        "CREATE TABLE orders (id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(20), qty INT)",
    )
    .await
    .expect("create orders");
}

fn strings(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column_by_name(name)
                .expect("column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 column");
            (0..column.len())
                .map(|row| (!column.is_null(row)).then(|| column.value(row).to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn ops(batches: &[RecordBatch]) -> Vec<String> {
    strings(batches, "__op").into_iter().flatten().collect()
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column_by_name("id")
                .expect("id")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64");
            (0..column.len())
                .map(|row| column.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn seqs(batches: &[RecordBatch]) -> Vec<u64> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column_by_name("__seq")
                .expect("__seq")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("uint64");
            (0..column.len())
                .map(|row| column.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn count(server: &Server, table: &str) -> i64 {
    let mut conn = server.conn().await;
    conn.query_first(format!("SELECT COUNT(*) FROM {table}"))
        .await
        .expect("count")
        .expect("one row")
}

async fn trigger_names(server: &Server) -> Vec<String> {
    let mut conn = server.conn().await;
    conn.query(
        "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS \
         WHERE TRIGGER_SCHEMA = 'saci' ORDER BY TRIGGER_NAME",
    )
    .await
    .expect("list triggers")
}

async fn outbox_mode_reads_the_operator_table_and_delete_acked_prunes_it(server: &Server) {
    let mut conn = server.conn().await;
    conn.query_drop(
        "CREATE TABLE outbox (id BIGINT AUTO_INCREMENT PRIMARY KEY, \
         __op CHAR(1) NOT NULL, payload VARCHAR(20))",
    )
    .await
    .expect("create outbox");
    conn.query_drop(
        "INSERT INTO outbox (__op, payload) VALUES ('I','a'),('U','b'),('D','c'),('I','d'),('I','e'),('I','f')",
    )
    .await
    .expect("seed");

    let mut source = common::source(
        &server.dsn(),
        "name \"outbox\"\nbatch_rows 2\n\n\
         mode kind=\"cdc_trigger\" table=\"outbox\" cursor_column=\"id\" retention=\"delete_acked\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
         schema_fields \"payload\" type=\"utf8\"\n",
    );
    let batches = common::drain(&mut source).await;
    assert_eq!(ids(&batches), vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(
        ops(&batches),
        vec!["I", "U", "D", "I", "I", "I"],
        "an outbox may carry __op as an ordinary column"
    );
    assert_eq!(
        count(server, "outbox").await,
        6,
        "nothing is pruned before the next cycle"
    );

    assert!(common::drain(&mut source).await.is_empty());
    assert_eq!(
        count(server, "outbox").await,
        0,
        "acknowledged rows are pruned"
    );

    conn.query_drop("INSERT INTO outbox (__op, payload) VALUES ('I','g')")
        .await
        .expect("insert");
    assert_eq!(ids(&common::drain(&mut source).await), vec![7]);
    assert!(common::drain(&mut source).await.is_empty());
    assert_eq!(count(server, "outbox").await, 0);
}

async fn a_delete_acked_outbox_over_an_unsigned_cursor_above_two_to_the_63_prunes_exactly(
    server: &Server,
) {
    let base: u64 = 9_223_372_036_854_775_808;
    let mut conn = server.conn().await;
    conn.query_drop("CREATE TABLE ubox (id BIGINT UNSIGNED PRIMARY KEY, payload VARCHAR(20))")
        .await
        .expect("create");
    let values = (1..=4)
        .map(|n| format!("({}, 'p{n}')", base + n))
        .collect::<Vec<_>>()
        .join(", ");
    conn.query_drop(format!("INSERT INTO ubox VALUES {values}"))
        .await
        .expect("seed");

    let mut source = common::source(
        &server.dsn(),
        "name \"ubox\"\nbatch_rows 2\nmax_batches_per_cycle 1\n\n\
         mode kind=\"cdc_trigger\" table=\"ubox\" cursor_column=\"id\" retention=\"delete_acked\"\n\
         schema_fields \"id\" type=\"uint64\" nullable=#false\n",
    );
    assert_eq!(common::drain(&mut source).await[0].num_rows(), 2);
    // The next cycle commits ids 1 and 2 and prunes exactly them.
    let second = common::drain(&mut source).await;
    assert_eq!(second.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert_eq!(
        count(server, "ubox").await,
        2,
        "the first two rows are gone, the rest stay"
    );
    let mut remaining: Vec<u64> = conn
        .query("SELECT id FROM ubox ORDER BY id")
        .await
        .expect("remaining");
    remaining.sort_unstable();
    assert_eq!(remaining, vec![base + 3, base + 4]);
}

async fn installed_triggers_capture_insert_update_and_delete(server: &Server) {
    orders(server).await;
    let mut conn = server.conn().await;
    let mut source = installed(&server.dsn(), "orders-cdc", "");

    assert!(
        common::drain(&mut source).await.is_empty(),
        "installing finds nothing yet"
    );
    assert_eq!(
        trigger_names(server).await,
        vec![
            "saci_orders_cdc_ad",
            "saci_orders_cdc_ai",
            "saci_orders_cdc_au"
        ]
    );

    conn.query_drop("INSERT INTO orders VALUES (1, 'new', 5)")
        .await
        .expect("insert");
    conn.query_drop("UPDATE orders SET label = 'changed' WHERE id = 1")
        .await
        .expect("update");
    conn.query_drop("DELETE FROM orders WHERE id = 1")
        .await
        .expect("delete");

    let batches = common::drain(&mut source).await;
    assert_eq!(ops(&batches), vec!["I", "U", "D"]);
    assert_eq!(ids(&batches), vec![1, 1, 1]);
    assert_eq!(
        strings(&batches, "label"),
        vec![
            Some("new".to_string()),
            Some("changed".to_string()),
            Some("changed".to_string())
        ],
        "a delete carries the old image"
    );
    assert!(
        strings(&batches, "__table")
            .iter()
            .all(|t| t.as_deref() == Some("saci.orders")),
        "{:?}",
        strings(&batches, "__table")
    );
    let seq = seqs(&batches);
    assert!(seq.windows(2).all(|w| w[0] < w[1]), "{seq:?}");

    let stamps = batches[0]
        .column_by_name("__commit_ts")
        .expect("__commit_ts")
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("timestamp");
    assert!(
        stamps.value(0) > 1_600_000_000_000_000,
        "{}",
        stamps.value(0)
    );
}

async fn a_primary_key_change_arrives_as_delete_then_insert(server: &Server) {
    orders(server).await;
    let mut conn = server.conn().await;
    conn.query_drop("INSERT INTO orders VALUES (1, 'a', 1)")
        .await
        .expect("seed");
    let mut source = installed(&server.dsn(), "orders-cdc", "");
    assert!(common::drain(&mut source).await.is_empty());

    conn.query_drop("UPDATE orders SET id = 100 WHERE id = 1")
        .await
        .expect("key change");
    let batches = common::drain(&mut source).await;
    assert_eq!(ops(&batches), vec!["D", "I"]);
    assert_eq!(ids(&batches), vec![1, 100]);

    conn.query_drop("UPDATE orders SET qty = 9 WHERE id = 100")
        .await
        .expect("plain update");
    assert_eq!(ops(&common::drain(&mut source).await), vec!["U"]);
}

async fn installing_twice_reuses_the_triggers_and_changelog(server: &Server) {
    orders(server).await;
    let mut conn = server.conn().await;
    let mut first = installed(&server.dsn(), "orders-cdc", "");
    assert!(common::drain(&mut first).await.is_empty());
    let mut second = installed(&server.dsn(), "orders-cdc", "");
    assert!(common::drain(&mut second).await.is_empty());

    assert_eq!(
        trigger_names(server).await.len(),
        3,
        "no second set of triggers"
    );
    conn.query_drop("INSERT INTO orders VALUES (1, 'a', 1)")
        .await
        .expect("insert");
    assert_eq!(
        count(server, "saci_cdc_orders_cdc").await,
        1,
        "one write is one changelog row"
    );
    assert_eq!(ops(&common::drain(&mut second).await), vec!["I"]);
}

async fn a_foreign_trigger_with_the_same_name_is_refused(server: &Server) {
    orders(server).await;
    let mut conn = server.conn().await;
    conn.query_drop("CREATE TABLE audit (n INT)")
        .await
        .expect("audit");
    conn.query_drop(
        "CREATE TRIGGER saci_orders_cdc_ai AFTER INSERT ON orders FOR EACH ROW \
         INSERT INTO audit VALUES (NEW.id)",
    )
    .await
    .expect("foreign trigger");

    let mut source = installed(&server.dsn(), "orders-cdc", "");
    let err = source.next_batch().await.expect_err("refused");
    let message = err.message();
    assert!(
        message.contains("trigger 'saci_orders_cdc_ai' exists with a different body"),
        "{message}"
    );
    assert!(message.contains("install_triggers #false"), "{message}");
}

async fn a_changelog_that_no_longer_matches_the_table_is_refused(server: &Server) {
    orders(server).await;
    let mut conn = server.conn().await;
    let mut first = installed(&server.dsn(), "orders-cdc", "");
    assert!(common::drain(&mut first).await.is_empty());
    drop(first);

    conn.query_drop("ALTER TABLE orders ADD COLUMN extra INT")
        .await
        .expect("alter");
    let mut second = installed(&server.dsn(), "orders-cdc", "");
    let err = second.next_batch().await.expect_err("refused");
    let message = err.message();
    assert!(message.contains("no longer matches"), "{message}");
    assert!(message.contains("'extra'"), "{message}");
    assert!(message.contains("drop it and its triggers"), "{message}");
}

async fn installed_mode_prunes_acknowledged_changelog_rows_by_default(server: &Server) {
    orders(server).await;
    let mut conn = server.conn().await;
    let mut source = installed(&server.dsn(), "orders-cdc", "");
    assert!(
        common::drain(&mut source).await.is_empty(),
        "installs the triggers"
    );
    conn.query_drop("INSERT INTO orders VALUES (1, 'a', 1), (2, 'b', 2)")
        .await
        .expect("insert");

    assert_eq!(ops(&common::drain(&mut source).await), vec!["I", "I"]);
    assert_eq!(count(server, "saci_cdc_orders_cdc").await, 2);
    assert!(common::drain(&mut source).await.is_empty());
    assert_eq!(count(server, "saci_cdc_orders_cdc").await, 0);

    conn.query_drop("INSERT INTO orders VALUES (3, 'c', 3)")
        .await
        .expect("insert");
    assert_eq!(ids(&common::drain(&mut source).await), vec![3]);
}

async fn retention_keep_leaves_the_changelog_rows_in_place(server: &Server) {
    orders(server).await;
    let mut conn = server.conn().await;
    let mut source = installed(&server.dsn(), "orders-cdc", "retention=\"keep\"");
    assert!(
        common::drain(&mut source).await.is_empty(),
        "installs the triggers"
    );
    conn.query_drop("INSERT INTO orders VALUES (1, 'a', 1)")
        .await
        .expect("insert");
    assert_eq!(ids(&common::drain(&mut source).await), vec![1]);
    assert!(common::drain(&mut source).await.is_empty());
    assert_eq!(count(server, "saci_cdc_orders_cdc").await, 1);
}

async fn a_user_without_the_trigger_privilege_is_told_what_it_needs(server: &Server) {
    orders(server).await;
    let mut root = server.conn().await;
    root.query_drop("CREATE USER 'noTrig'@'%' IDENTIFIED BY 'pw'")
        .await
        .expect("user");
    root.query_drop("GRANT SELECT, INSERT, CREATE ON saci.* TO 'noTrig'@'%'")
        .await
        .expect("grant");

    let mut source = installed(&server.dsn_as("noTrig", "pw"), "orders-cdc", "");
    let err = source.next_batch().await.expect_err("refused");
    let message = err.message();
    assert!(
        message.contains("TRIGGER privilege on 'saci.orders'"),
        "{message}"
    );
    assert!(
        message.contains("INSERT on 'saci.saci_cdc_orders_cdc'"),
        "{message}"
    );
    assert!(
        !message.contains("pw"),
        "the password never reaches an error: {message}"
    );
}

async fn a_changelog_in_a_latin1_schema_still_captures_an_emoji_row(server: &Server) {
    let mut conn = server.conn().await;
    conn.query_drop("CREATE DATABASE latin_db DEFAULT CHARACTER SET latin1")
        .await
        .expect("database");
    conn.query_drop(
        "CREATE TABLE latin_db.w (id BIGINT NOT NULL PRIMARY KEY, \
         label VARCHAR(40) CHARACTER SET utf8mb4)",
    )
    .await
    .expect("table");

    let mut source = common::source(
        &server.dsn(),
        "name \"latin\"\nbatch_rows 100\n\n\
         mode kind=\"cdc_trigger\" table=\"latin_db.w\" install_triggers=#true\n\
         schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
         schema_fields \"__seq\" type=\"uint64\" nullable=#false\n\
         schema_fields \"__commit_ts\" type=\"timestamp_micros_utc\" nullable=#false\n\
         schema_fields \"__table\" type=\"utf8\" nullable=#false\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"label\" type=\"utf8\"\n",
    );
    assert!(common::drain(&mut source).await.is_empty());

    conn.exec_drop(
        "INSERT INTO latin_db.w VALUES (?, ?)",
        (1_i64, "emoji \u{1F600} caf\u{e9}"),
    )
    .await
    .expect("the application's own write must not fail");

    let batches = common::drain(&mut source).await;
    assert_eq!(
        strings(&batches, "label"),
        vec![Some("emoji \u{1F600} caf\u{e9}".to_string())]
    );
    assert_eq!(
        strings(&batches, "__table"),
        vec![Some("latin_db.w".to_string())]
    );
}

async fn a_zero_date_row_does_not_fail_the_writers_statement(server: &Server) {
    let mut conn = server.conn().await;
    conn.query_drop("CREATE TABLE zd (id INT PRIMARY KEY, d DATE, n INT)")
        .await
        .expect("table");
    conn.query_drop("SET SESSION sql_mode = ''")
        .await
        .expect("lax session");
    conn.query_drop("INSERT INTO zd VALUES (1, '0000-00-00', 0)")
        .await
        .expect("zero date");

    let mut source = common::source(
        &server.dsn(),
        "name \"zd\"\nbatch_rows 100\n\n\
         mode kind=\"cdc_trigger\" table=\"zd\" install_triggers=#true\n\
         schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
         schema_fields \"__seq\" type=\"uint64\" nullable=#false\n\
         schema_fields \"__commit_ts\" type=\"timestamp_micros_utc\" nullable=#false\n\
         schema_fields \"__table\" type=\"utf8\" nullable=#false\n\
         schema_fields \"id\" type=\"int32\" nullable=#false\n\
         schema_fields \"d\" type=\"date32\"\n\
         schema_fields \"n\" type=\"int32\"\n",
    );
    assert!(common::drain(&mut source).await.is_empty());

    // The writer runs strict; the trigger carries the mode it was made under.
    conn.query_drop(
        "SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,\
         ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'",
    )
    .await
    .expect("strict session");
    conn.query_drop("UPDATE zd SET n = 1 WHERE id = 1")
        .await
        .expect("the trigger must not fail the application's UPDATE");

    let batches = common::drain(&mut source).await;
    assert_eq!(ops(&batches), vec!["U"]);
    let d = batches[0].column_by_name("d").expect("d");
    assert!(
        d.is_null(0),
        "a zero date reads as NULL on a nullable field"
    );
}

/// The triggers copy under an empty `sql_mode`, so a column widened after the
/// install would be truncated into the changelog's older, narrower one with
/// only a warning. A source that stays connected never reruns the install, so
/// the rows are checked before they go out.
async fn a_column_widened_under_a_running_source_is_refused_not_truncated(server: &Server) {
    orders(server).await;
    let mut source = installed(&server.dsn(), "widened", "");
    assert!(common::drain(&mut source).await.is_empty());

    let mut conn = server.conn().await;
    conn.query_drop("ALTER TABLE orders MODIFY label VARCHAR(100)")
        .await
        .expect("widen");
    let long = "x".repeat(60);
    conn.query_drop(format!("INSERT INTO orders VALUES (1, '{long}', 1)"))
        .await
        .expect("insert");

    let err = source
        .next_batch()
        .await
        .expect_err("the captured row is truncated");
    assert_eq!(err.category(), "configuration", "{}", err.message());
    assert!(
        err.message().contains("column 'label' is varchar(20)")
            && err.message().contains("varchar(100)"),
        "{}",
        err.message()
    );
    // Still refused on the next call: nothing is delivered.
    assert!(source.next_batch().await.is_err());
}

/// A column added to the table is not a problem for a running source: the
/// triggers go on writing the columns they name.
async fn a_column_added_under_a_running_source_does_not_stop_it(server: &Server) {
    orders(server).await;
    let mut source = installed(&server.dsn(), "added", "");
    assert!(common::drain(&mut source).await.is_empty());

    let mut conn = server.conn().await;
    conn.query_drop("ALTER TABLE orders ADD COLUMN extra INT")
        .await
        .expect("add");
    conn.query_drop("INSERT INTO orders (id, label, qty, extra) VALUES (1, 'a', 1, 9)")
        .await
        .expect("insert");
    assert_eq!(ids(&common::drain(&mut source).await), vec![1]);
}

// ------------------------------------------------- commit order and sequence holes

const CHANGELOG: &str = "saci_cdc_late";

async fn commit(conn: &mut Conn) {
    conn.query_drop("COMMIT").await.expect("commit");
}

async fn rollback(conn: &mut Conn) {
    conn.query_drop("ROLLBACK").await.expect("rollback");
}

/// A writer whose transaction has inserted order `id` and not committed: the
/// trigger has taken the next `__seq`, and nobody can read it yet.
async fn open_order(server: &Server, id: i64) -> Conn {
    common::open_transaction(
        server,
        &format!("INSERT INTO orders VALUES ({id}, 'open', {id})"),
    )
    .await
}

/// Insert order `id` and commit at once, from a connection of its own.
async fn insert_order(server: &Server, id: i64) {
    let mut conn = server.conn().await;
    conn.query_drop(format!("INSERT INTO orders VALUES ({id}, 'done', {id})"))
        .await
        .expect("insert");
}

/// The cursor the offset table holds for the source `name`.
async fn stored_cursor(server: &Server, name: &str) -> Option<String> {
    let mut conn = server.conn().await;
    conn.exec_first(
        "SELECT cursor_value FROM saci_source_offsets WHERE source_name = ?",
        (name,),
    )
    .await
    .expect("read the offset row")
}

/// Sleep until a hole first seen before this call is older than `timeout_ms`.
async fn past(timeout_ms: u64) {
    tokio::time::sleep(Duration::from_millis(timeout_ms + 500)).await;
}

/// Transaction A takes the first `__seq` and stays open, B takes the second and
/// commits, a drain reads, and A commits. The changelog's `__seq` is allocated
/// by the trigger, not at commit, so A's row appears after a later one is
/// visible.
async fn a_row_committed_after_a_later_one_is_still_delivered(server: &Server) {
    orders(server).await;
    let mut source = installed(&server.dsn(), "late", "");
    assert!(
        common::drain(&mut source).await.is_empty(),
        "installing finds nothing yet"
    );

    let mut a = open_order(server, 1).await;
    insert_order(server, 2).await;
    let before = common::drain(&mut source).await;

    commit(&mut a).await;
    let after = common::drain(&mut source).await;

    let all: Vec<RecordBatch> = before.iter().chain(&after).cloned().collect();
    assert_eq!(
        seqs(&all),
        vec![1, 2],
        "A's row, committed after B's, must arrive"
    );
    assert_eq!(ids(&all), vec![1, 2]);
    assert!(
        before.is_empty(),
        "a row behind an open hole is held back, not handed over: {:?}",
        seqs(&before)
    );
}

async fn delete_acked_never_prunes_past_the_delivered_point(server: &Server) {
    orders(server).await;
    let mut source = installed(&server.dsn(), "late", "");
    assert!(common::drain(&mut source).await.is_empty());

    insert_order(server, 1).await;
    assert_eq!(seqs(&common::drain(&mut source).await), vec![1]);

    let mut a = open_order(server, 2).await;
    insert_order(server, 3).await;
    // This cycle stores offset 1 and prunes through it, then meets the hole.
    assert!(common::drain(&mut source).await.is_empty());
    assert_eq!(
        count(server, CHANGELOG).await,
        1,
        "seq 1 is pruned; seq 3 waits behind the hole and stays"
    );

    commit(&mut a).await;
    assert_eq!(count(server, CHANGELOG).await, 2);
    assert_eq!(seqs(&common::drain(&mut source).await), vec![2, 3]);
    assert_eq!(
        count(server, CHANGELOG).await,
        2,
        "delivered rows are pruned by the next cycle, not this one"
    );

    assert!(common::drain(&mut source).await.is_empty());
    assert_eq!(count(server, CHANGELOG).await, 0);
    assert_eq!(stored_cursor(server, "late").await.as_deref(), Some("3"));
}

/// Without the `PROCESS` privilege `INNODB_TRX` is refused, so the source cannot
/// tell an open transaction from a rolled-back one: the rows behind a hole wait
/// `gap_timeout_ms`, then go out.
async fn a_rolled_back_hole_is_given_up_after_the_timeout_without_innodb_trx(server: &Server) {
    orders(server).await;
    let dsn = common::user_without_process(server, "plain").await;
    let mut source = installed(&dsn, "rolled", "gap_timeout_ms=1500");
    assert!(common::drain(&mut source).await.is_empty());
    insert_order(server, 1).await;
    assert_eq!(seqs(&common::drain(&mut source).await), vec![1]);

    let mut a = open_order(server, 2).await;
    insert_order(server, 3).await;
    rollback(&mut a).await;

    assert!(
        common::drain(&mut source).await.is_empty(),
        "the hole is waited for before it is given up"
    );
    past(1500).await;
    let released = common::drain(&mut source).await;
    assert_eq!(seqs(&released), vec![3]);
    assert_eq!(ids(&released), vec![3]);

    insert_order(server, 4).await;
    assert_eq!(
        seqs(&common::drain(&mut source).await),
        vec![4],
        "no hole is left, so the next row goes straight out"
    );
}

/// The offset stops before the hole, so a process that restarts while A is
/// still open reads the hole again and still delivers A's row when it commits.
async fn a_hole_open_across_a_restart_is_still_filled_by_the_late_commit(server: &Server) {
    orders(server).await;
    let mode = "gap_timeout_ms=30000";
    let mut first = installed(&server.dsn(), "restart", mode);
    assert!(common::drain(&mut first).await.is_empty());
    insert_order(server, 1).await;
    assert_eq!(seqs(&common::drain(&mut first).await), vec![1]);

    let mut a = open_order(server, 2).await;
    insert_order(server, 3).await;
    assert!(common::drain(&mut first).await.is_empty());
    drop(first);
    assert_eq!(
        stored_cursor(server, "restart").await.as_deref(),
        Some("1"),
        "the stored offset never passes an open hole"
    );

    let mut second = installed(&server.dsn(), "restart", mode);
    assert!(
        common::drain(&mut second).await.is_empty(),
        "the restarted source meets the same hole and waits"
    );
    commit(&mut a).await;
    assert_eq!(seqs(&common::drain(&mut second).await), vec![2, 3]);
}

/// Without `INNODB_TRX` a hole's age is this process's own: a restarted source
/// has not watched the hole for a timeout yet, so it holds the rows back again
/// rather than guess.
async fn a_restart_waits_a_rolled_back_hole_out_again_without_innodb_trx(server: &Server) {
    orders(server).await;
    let dsn = common::user_without_process(server, "plain").await;
    let mode = "gap_timeout_ms=1500";
    let mut first = installed(&dsn, "again", mode);
    assert!(common::drain(&mut first).await.is_empty());
    insert_order(server, 1).await;
    assert_eq!(seqs(&common::drain(&mut first).await), vec![1]);

    let mut a = open_order(server, 2).await;
    insert_order(server, 3).await;
    rollback(&mut a).await;
    assert!(common::drain(&mut first).await.is_empty());
    past(1500).await;
    drop(first);
    assert_eq!(stored_cursor(server, "again").await.as_deref(), Some("1"));

    let mut second = installed(&dsn, "again", mode);
    assert!(
        common::drain(&mut second).await.is_empty(),
        "the new process starts the hole's clock again"
    );
    past(1500).await;
    assert_eq!(seqs(&common::drain(&mut second).await), vec![3]);
}

/// `auto_increment_increment` 3 leaves two unused numbers between consecutive
/// rows, which are not holes; a missing step is.
async fn a_sequence_that_steps_by_more_than_one_has_no_false_holes(server: &Server) {
    let mut root = server.conn().await;
    root.query_drop("SET GLOBAL auto_increment_increment = 3")
        .await
        .expect("step");
    root.query_drop("SET GLOBAL auto_increment_offset = 2")
        .await
        .expect("offset");

    orders(server).await;
    let mut source = installed(&server.dsn(), "step", "gap_timeout_ms=30000");
    assert!(common::drain(&mut source).await.is_empty());
    for id in 1..=3 {
        insert_order(server, id).await;
    }
    let seq = seqs(&common::drain(&mut source).await);
    assert_eq!(seq.len(), 3, "a step is not a hole: {seq:?}");
    assert!(seq.windows(2).all(|w| w[1] - w[0] == 3), "{seq:?}");
    let last = seq[2];

    let mut a = open_order(server, 4).await;
    insert_order(server, 5).await;
    assert!(
        common::drain(&mut source).await.is_empty(),
        "a missing step is a hole"
    );
    commit(&mut a).await;
    assert_eq!(
        seqs(&common::drain(&mut source).await),
        vec![last + 3, last + 6]
    );
}

/// The same race over an operator's own `AUTO_INCREMENT` outbox with
/// `delete_acked`: the late row is delivered, and the row behind the hole is
/// not pruned before it is.
async fn an_outbox_row_committed_after_a_later_one_is_delivered_and_never_pruned_early(
    server: &Server,
) {
    let mut conn = server.conn().await;
    conn.query_drop(
        "CREATE TABLE outbox (id BIGINT AUTO_INCREMENT PRIMARY KEY, \
         __op CHAR(1) NOT NULL, payload VARCHAR(20))",
    )
    .await
    .expect("create outbox");
    let mut source = common::source(
        &server.dsn(),
        "name \"gapbox\"\nbatch_rows 100\n\n\
         mode kind=\"cdc_trigger\" table=\"outbox\" cursor_column=\"id\" \
         retention=\"delete_acked\"\n\
         schema_fields \"id\" type=\"int64\" nullable=#false\n\
         schema_fields \"__op\" type=\"utf8\" nullable=#false\n\
         schema_fields \"payload\" type=\"utf8\"\n",
    );
    assert!(common::drain(&mut source).await.is_empty());

    let mut a = common::open_transaction(
        server,
        "INSERT INTO outbox (__op, payload) VALUES ('I', 'a')",
    )
    .await;
    conn.query_drop("INSERT INTO outbox (__op, payload) VALUES ('I', 'b')")
        .await
        .expect("b commits first");

    let mut delivered = ids(&common::drain(&mut source).await);
    // The second cycle prunes whatever the first handed over.
    delivered.extend(ids(&common::drain(&mut source).await));
    assert_eq!(
        count(server, "outbox").await,
        1,
        "the committed row behind the hole is neither delivered nor pruned yet"
    );

    commit(&mut a).await;
    delivered.extend(ids(&common::drain(&mut source).await));
    assert_eq!(delivered, vec![1, 2], "the late row arrives, in order");

    assert!(common::drain(&mut source).await.is_empty());
    assert_eq!(count(server, "outbox").await, 0);
}

/// The rows before the first hole go out, a batch at a time; the rows from the
/// hole on wait, including the ones that would start the next batch.
async fn rows_before_a_hole_go_out_and_the_ones_behind_it_wait_across_batches(server: &Server) {
    orders(server).await;
    let mut source = common::source(
        &server.dsn(),
        &format!(
            "name \"late\"\nbatch_rows 2\n\n\
             mode kind=\"cdc_trigger\" table=\"orders\" install_triggers=#true\n\
             {ORDERS_FIELDS}"
        ),
    );
    assert!(common::drain(&mut source).await.is_empty());

    for id in 1..=2 {
        insert_order(server, id).await;
    }
    let mut a = open_order(server, 3).await;
    for id in 4..=6 {
        insert_order(server, id).await;
    }
    let before = common::drain(&mut source).await;
    assert_eq!(seqs(&before), vec![1, 2]);

    commit(&mut a).await;
    let after = common::drain(&mut source).await;
    assert_eq!(seqs(&after), vec![3, 4, 5, 6]);
    assert_eq!(
        after.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
        vec![2, 2]
    );
}

/// A writer that has inserted and not committed owns the changelog's last
/// record, right after the offset. A bare range `DELETE` through the offset
/// would wait for that writer; the prune must not.
async fn an_open_writer_at_the_tail_does_not_block_the_prune(server: &Server) {
    orders(server).await;
    let mut source = installed(&server.dsn(), "late", "");
    assert!(common::drain(&mut source).await.is_empty());
    insert_order(server, 1).await;
    assert_eq!(seqs(&common::drain(&mut source).await), vec![1]);

    let mut a = open_order(server, 2).await;
    let pruning = tokio::time::timeout(Duration::from_secs(10), common::drain(&mut source));
    let drained = pruning
        .await
        .expect("the prune waited for the writer that is still open");
    assert!(drained.is_empty());
    assert_eq!(count(server, CHANGELOG).await, 0, "seq 1 is pruned");

    commit(&mut a).await;
    assert_eq!(seqs(&common::drain(&mut source).await), vec![2]);
}

/// With `INNODB_TRX` readable, a rolled-back transaction is no longer running,
/// so nothing can fill its hole and the rows behind it go out at once, however
/// long `gap_timeout_ms` is.
async fn a_rolled_back_hole_is_released_at_once_when_innodb_trx_is_readable(server: &Server) {
    orders(server).await;
    let mut source = installed(&server.dsn(), "rolled", "gap_timeout_ms=30000");
    assert!(common::drain(&mut source).await.is_empty());
    insert_order(server, 1).await;
    assert_eq!(seqs(&common::drain(&mut source).await), vec![1]);

    let mut a = open_order(server, 2).await;
    insert_order(server, 3).await;
    rollback(&mut a).await;
    assert_eq!(
        seqs(&common::drain(&mut source).await),
        vec![3],
        "no running transaction can fill the hole, so nothing waits"
    );
}

/// A transaction that stays open past `gap_timeout_ms` is still waited for
/// while `INNODB_TRX` shows it, and its row arrives in order.
async fn a_transaction_open_longer_than_the_timeout_is_still_delivered_when_innodb_trx_is_readable(
    server: &Server,
) {
    orders(server).await;
    let mut source = installed(&server.dsn(), "slow", "gap_timeout_ms=1000");
    assert!(common::drain(&mut source).await.is_empty());

    let mut a = open_order(server, 1).await;
    insert_order(server, 2).await;
    assert!(common::drain(&mut source).await.is_empty());
    past(1000).await;
    assert!(
        common::drain(&mut source).await.is_empty(),
        "the open transaction is still running, so the hole is still held"
    );

    commit(&mut a).await;
    assert_eq!(seqs(&common::drain(&mut source).await), vec![1, 2]);
}

/// The fallback has no evidence to wait on: past `gap_timeout_ms` the hole is
/// given up, and the row the slow transaction commits later is lost.
async fn a_transaction_open_longer_than_the_timeout_is_lost_without_innodb_trx(server: &Server) {
    orders(server).await;
    let dsn = common::user_without_process(server, "plain").await;
    let mut source = installed(&dsn, "slow", "gap_timeout_ms=1000");
    assert!(common::drain(&mut source).await.is_empty());

    let mut a = open_order(server, 1).await;
    insert_order(server, 2).await;
    assert!(common::drain(&mut source).await.is_empty());
    past(1000).await;
    assert_eq!(seqs(&common::drain(&mut source).await), vec![2]);

    commit(&mut a).await;
    assert!(
        common::drain(&mut source).await.is_empty(),
        "the late row is behind the offset"
    );
}

/// A transaction `INNODB_TRX` shows is waited for, but not forever: an idle
/// connection with a transaction left open would stop the source for good.
/// `gap_max_wait_ms` ends the wait.
async fn a_hole_a_transaction_could_fill_is_given_up_at_gap_max_wait_ms(server: &Server) {
    orders(server).await;
    let mut source = installed(
        &server.dsn(),
        "capped",
        "gap_timeout_ms=500 gap_max_wait_ms=2000",
    );
    assert!(common::drain(&mut source).await.is_empty());

    let mut a = open_order(server, 1).await;
    insert_order(server, 2).await;
    assert!(common::drain(&mut source).await.is_empty());
    past(500).await;
    assert!(
        common::drain(&mut source).await.is_empty(),
        "past gap_timeout_ms but inside gap_max_wait_ms"
    );
    past(1500).await;
    assert_eq!(seqs(&common::drain(&mut source).await), vec![2]);
    rollback(&mut a).await;
}

/// A hole passed at the cap while its transaction is still open sits below the
/// offset, and the transaction's uncommitted record is in the range the prune
/// walks. The prune must give up on that wait and try again later, not fail the
/// cycle or hold it for the statement timeout.
async fn a_capped_hole_whose_transaction_is_still_open_does_not_stall_the_prune(server: &Server) {
    orders(server).await;
    let mut source = installed(
        &server.dsn(),
        "capprune",
        "gap_timeout_ms=500 gap_max_wait_ms=1500",
    );
    assert!(common::drain(&mut source).await.is_empty());

    let mut a = open_order(server, 1).await;
    insert_order(server, 2).await;
    assert!(common::drain(&mut source).await.is_empty());
    past(1500).await;
    assert_eq!(seqs(&common::drain(&mut source).await), vec![2]);

    // This cycle prunes through seq 2, and A's record is in that range.
    let started = std::time::Instant::now();
    let drained = tokio::time::timeout(Duration::from_secs(20), common::drain(&mut source))
        .await
        .expect("the prune waited on the open transaction");
    assert!(drained.is_empty());
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );

    rollback(&mut a).await;
    tokio::time::sleep(Duration::from_millis(6000)).await;
    assert!(common::drain(&mut source).await.is_empty());
    assert_eq!(
        count(server, CHANGELOG_CAPPRUNE).await,
        0,
        "the prune is retried once the writer is gone"
    );
}

const CHANGELOG_CAPPRUNE: &str = "saci_cdc_capprune";

/// A monitor reading `INNODB_TRX` faster than the server rebuilds it keeps the
/// list as it was before the writer started. A source that trusted that list
/// would find no transaction for the hole and pass it. The source proves the
/// list current with a transaction of its own, so it never settles on a stale
/// one, and the late row still arrives.
async fn a_stale_transaction_list_never_settles_a_hole_that_is_still_open(server: &Server) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    orders(server).await;
    let mut source = installed(&server.dsn(), "stale", "gap_timeout_ms=30000");
    assert!(common::drain(&mut source).await.is_empty());

    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let mut monitor = server.conn().await;
    let poller = tokio::spawn(async move {
        while !flag.load(Ordering::Relaxed) {
            monitor
                .query_drop("SELECT COUNT(*) FROM information_schema.INNODB_TRX")
                .await
                .expect("poll");
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    });
    // The list is built once here, before the writer below starts.
    tokio::time::sleep(Duration::from_millis(400)).await;

    let mut a = open_order(server, 1).await;
    insert_order(server, 2).await;
    let before = common::drain(&mut source).await;
    commit(&mut a).await;
    let after = common::drain(&mut source).await;
    stop.store(true, Ordering::Relaxed);
    poller.await.expect("the monitor stops");

    let all: Vec<RecordBatch> = before.iter().chain(&after).cloned().collect();
    assert_eq!(seqs(&all), vec![1, 2], "A's row must arrive");
    assert!(
        before.is_empty(),
        "a hole whose writer the list cannot show is not passed: {:?}",
        seqs(&before)
    );
}
