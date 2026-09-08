//! End-to-end behaviour of the redb source and sink against a real file.
//!
//! No Docker and no external service: redb is embedded, so each test owns a
//! fresh `tempfile::tempdir()` and the file inside it.

use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use redb::{Database, ReadableDatabase, TableDefinition};

use saci_connector_redb::{DurabilityMode, RedbSink, RedbSinkConfig, RedbSource, RedbSourceConfig};
use saci_core::io::sink::Sink;
use saci_core::io::source::Source;
use saci_transformer::Transformer;
use saci_transformer_csv::CsvTransformer;
use saci_transformer_parquet::ParquetTransformer;

const TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("records");

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("amount", DataType::Float64, true),
    ]))
}

fn batch(ids: &[i64]) -> RecordBatch {
    let amounts: Vec<f64> = ids.iter().map(|&i| i as f64 * 1.5).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(Float64Array::from(amounts)),
        ],
    )
    .expect("build batch")
}

fn sink_config(dir: &std::path::Path) -> RedbSinkConfig {
    RedbSinkConfig {
        directory: dir.to_path_buf(),
        file: "saci.redb".to_string(),
        table: "records".to_string(),
        key_prefix: String::new(),
        key_suffix: String::new(),
        check_integrity: false,
        cache_size_bytes: None,
        compact: true,
        durability: DurabilityMode::Immediate,
        two_phase_commit: true,
        quick_repair: true,
        schema_fields: Vec::new(),
    }
}

fn source_config(dir: &std::path::Path) -> RedbSourceConfig {
    RedbSourceConfig {
        directory: dir.to_path_buf(),
        file: "saci.redb".to_string(),
        table: "records".to_string(),
        key_prefix: String::new(),
        key_suffix: String::new(),
        check_integrity: false,
        cache_size_bytes: None,
        consume: false,
        schema_fields: Vec::new(),
    }
}

/// Every id the source yields, in the order it yielded them.
async fn drain(source: &mut RedbSource) -> Vec<i64> {
    let mut ids = Vec::new();
    while let Some(b) = source.next_batch().await.expect("next batch") {
        let column = b
            .column_by_name("id")
            .expect("id column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 id column")
            .clone();
        ids.extend(column.values().iter().copied());
    }
    ids
}

/// Write `values` under `key` with a raw redb handle, then release the lock.
fn seed(dir: &std::path::Path, entries: &[(&str, Vec<u8>)]) {
    let db = Database::create(dir.join("saci.redb")).expect("create file");
    let txn = db.begin_write().expect("begin_write");
    {
        let mut table = txn.open_table(TABLE).expect("open table");
        for (key, bytes) in entries {
            table.insert(*key, bytes.as_slice()).expect("insert");
        }
    }
    txn.commit().expect("commit");
}

/// Build a redb file in `dir` that was never shut down cleanly.
///
/// The allocator state table is written either by a two-phase commit (which
/// `quick_repair` forces on, so the sink's defaults always write it) or by
/// `Database::drop`, which skips that write while the thread is panicking.
/// Committing with both knobs off and then unwinding past the drop therefore
/// leaves exactly what a killed process leaves: a valid file that only a
/// read-write open can recover. Copying a live file instead is not an option
/// on Windows, where redb's lock blocks the read.
fn seed_unclean(dir: &std::path::Path, entries: &[(&str, Vec<u8>)]) {
    let path = dir.join("saci.redb");
    let owned: Vec<(String, Vec<u8>)> = entries
        .iter()
        .map(|(key, bytes)| ((*key).to_string(), bytes.clone()))
        .collect();

    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let unwound = std::panic::catch_unwind(|| {
        let db = Database::create(&path).expect("create file");
        let mut txn = db.begin_write().expect("begin_write");
        txn.set_two_phase_commit(false);
        txn.set_quick_repair(false);
        {
            let mut table = txn.open_table(TABLE).expect("open table");
            for (key, bytes) in &owned {
                table
                    .insert(key.as_str(), bytes.as_slice())
                    .expect("insert");
            }
        }
        txn.commit().expect("commit");
        panic!("unwind past Database::drop");
    });
    std::panic::set_hook(hook);
    assert!(unwound.is_err(), "the seed must unwind past the drop");
}

/// Every key in the file, in key order.
fn keys(dir: &std::path::Path) -> Vec<String> {
    let db = Database::open(dir.join("saci.redb")).expect("open file");
    let txn = db.begin_read().expect("begin_read");
    let table = txn.open_table(TABLE).expect("open table");
    table
        .range::<&str>(..)
        .expect("range")
        .map(|entry| entry.expect("entry").0.value().to_string())
        .collect()
}

/// One csv document holding `ids`, as the sink would have written it.
///
/// `Transformer::open_writer` takes ownership of a `'static` handle, so the
/// bytes live behind an `Arc` this function keeps a second handle on.
fn csv_document(ids: &[i64]) -> Vec<u8> {
    #[derive(Clone)]
    struct SharedBuf(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("buffer lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let buffer = SharedBuf(Arc::new(std::sync::Mutex::new(Vec::new())));
    let transformer = CsvTransformer::new(true);
    let mut writer = transformer
        .open_writer(Box::new(buffer.clone()), schema())
        .expect("open csv writer");
    writer.write_batch(&batch(ids)).expect("write batch");
    writer.finish().expect("finish");
    buffer.0.lock().expect("buffer lock").clone()
}

#[tokio::test]
async fn sink_then_source_round_trip_through_parquet() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut sink = RedbSink::open(
        sink_config(dir.path()),
        schema(),
        Arc::new(ParquetTransformer::new()),
    )
    .expect("open sink");
    for ids in [vec![1i64, 2], vec![3], vec![4, 5, 6]] {
        sink.write_batch(&batch(&ids)).await.expect("write batch");
    }
    sink.finish().await.expect("finish");

    let mut source = RedbSource::new(
        source_config(dir.path()),
        schema(),
        Arc::new(ParquetTransformer::new()),
    )
    .expect("open source");
    assert_eq!(drain(&mut source).await, vec![1, 2, 3, 4, 5, 6]);
}

#[tokio::test]
async fn durability_none_still_leaves_the_entries_in_the_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = RedbSinkConfig {
        durability: DurabilityMode::None,
        ..sink_config(dir.path())
    };
    let mut sink =
        RedbSink::open(config, schema(), Arc::new(CsvTransformer::new(true))).expect("open sink");
    sink.write_batch(&batch(&[1, 2]))
        .await
        .expect("first batch");
    sink.write_batch(&batch(&[3])).await.expect("second batch");
    // `finish` runs the immediate commit that makes the two above durable.
    sink.finish().await.expect("finish");

    let mut source = RedbSource::new(
        source_config(dir.path()),
        schema(),
        Arc::new(CsvTransformer::new(true)),
    )
    .expect("open source");
    assert_eq!(drain(&mut source).await, vec![1, 2, 3]);
}

#[tokio::test]
async fn keys_resume_from_the_file_across_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = RedbSinkConfig {
        key_prefix: "orders/".to_string(),
        key_suffix: ".parquet".to_string(),
        ..sink_config(dir.path())
    };

    let mut sink = RedbSink::open(
        config.clone(),
        schema(),
        Arc::new(ParquetTransformer::new()),
    )
    .expect("open sink");
    sink.write_batch(&batch(&[1])).await.expect("first batch");
    sink.write_batch(&batch(&[2])).await.expect("second batch");
    sink.finish().await.expect("finish");

    // The second open proves `finish` released redb's exclusive file lock.
    let mut sink =
        RedbSink::open(config, schema(), Arc::new(ParquetTransformer::new())).expect("reopen sink");
    sink.write_batch(&batch(&[3])).await.expect("third batch");
    sink.finish().await.expect("finish again");

    assert_eq!(
        keys(dir.path()),
        vec![
            "orders/00000000000000000000.parquet".to_string(),
            "orders/00000000000000000001.parquet".to_string(),
            "orders/00000000000000000002.parquet".to_string(),
        ]
    );
}

#[tokio::test]
async fn a_foreign_key_sorting_above_the_prefix_does_not_reset_the_sequence() {
    let dir = tempfile::tempdir().expect("tempdir");
    // `zzz` sorts above every `orders/...` key, so a descending scan meets it
    // first. It must be skipped, not treated as the end of the prefix region.
    seed(
        dir.path(),
        &[
            ("orders/00000000000000000000.csv", csv_document(&[1])),
            ("zzz-foreign", csv_document(&[9])),
        ],
    );

    let config = RedbSinkConfig {
        key_prefix: "orders/".to_string(),
        key_suffix: ".csv".to_string(),
        ..sink_config(dir.path())
    };
    let mut sink =
        RedbSink::open(config, schema(), Arc::new(CsvTransformer::new(true))).expect("open sink");
    sink.write_batch(&batch(&[2])).await.expect("write batch");
    sink.finish().await.expect("finish");

    assert_eq!(
        keys(dir.path()),
        vec![
            "orders/00000000000000000000.csv".to_string(),
            "orders/00000000000000000001.csv".to_string(),
            "zzz-foreign".to_string(),
        ]
    );
}

#[tokio::test]
async fn two_sources_read_one_file_at_the_same_time() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed(
        dir.path(),
        &[("00000000000000000000.csv", csv_document(&[1, 2]))],
    );

    // The source's handle takes a shared lock, so the second open succeeds
    // while the first still holds the file.
    let mut first = RedbSource::new(
        source_config(dir.path()),
        schema(),
        Arc::new(CsvTransformer::new(true)),
    )
    .expect("build first source");
    let held = first
        .next_batch()
        .await
        .expect("first batch")
        .expect("the entry yields one batch");
    assert_eq!(held.num_rows(), 2);

    let mut second = RedbSource::new(
        source_config(dir.path()),
        schema(),
        Arc::new(CsvTransformer::new(true)),
    )
    .expect("build second source");
    assert_eq!(drain(&mut second).await, vec![1, 2]);
}

#[tokio::test]
async fn an_unclean_file_is_refused_by_name_and_check_integrity_recovers_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed_unclean(
        dir.path(),
        &[("00000000000000000000.csv", csv_document(&[1, 2]))],
    );

    let mut source = RedbSource::new(
        source_config(dir.path()),
        schema(),
        Arc::new(CsvTransformer::new(true)),
    )
    .expect("build source");
    let Err(err) = source.next_batch().await else {
        panic!("a read-only open cannot repair an unclean file");
    };
    assert!(
        err.message().contains("was not shut down cleanly")
            && err.message().contains("check_integrity"),
        "message was {}",
        err.message()
    );

    // `check_integrity` opens read-write, which repairs, so the same config
    // with the knob on reads the file.
    let config = RedbSourceConfig {
        check_integrity: true,
        ..source_config(dir.path())
    };
    let mut source = RedbSource::new(config, schema(), Arc::new(CsvTransformer::new(true)))
        .expect("build source");
    assert_eq!(drain(&mut source).await, vec![1, 2]);
}

#[tokio::test]
async fn source_reads_only_entries_matching_prefix_and_suffix() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed(
        dir.path(),
        &[
            ("archive/00000000000000000000.csv", csv_document(&[10])),
            ("orders/00000000000000000000.csv", csv_document(&[1, 2])),
            ("orders/00000000000000000001.other", csv_document(&[20])),
        ],
    );

    let config = RedbSourceConfig {
        key_prefix: "orders/".to_string(),
        key_suffix: ".csv".to_string(),
        ..source_config(dir.path())
    };
    let mut source = RedbSource::new(config, schema(), Arc::new(CsvTransformer::new(true)))
        .expect("open source");
    assert_eq!(drain(&mut source).await, vec![1, 2]);
}

#[tokio::test]
async fn source_reports_a_missing_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut source = RedbSource::new(
        source_config(dir.path()),
        schema(),
        Arc::new(CsvTransformer::new(true)),
    )
    .expect("build source");
    let Err(err) = source.next_batch().await else {
        panic!("an absent file must fail rather than report EOF");
    };
    assert!(
        err.message().contains("cannot open"),
        "message was {}",
        err.message()
    );
}

#[tokio::test]
async fn write_after_finish_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut sink = RedbSink::open(
        sink_config(dir.path()),
        schema(),
        Arc::new(CsvTransformer::new(true)),
    )
    .expect("open sink");
    sink.finish().await.expect("finish");
    let Err(err) = sink.write_batch(&batch(&[1])).await else {
        panic!("a write after finish must fail");
    };
    assert!(
        err.message().contains("called after finish"),
        "message was {}",
        err.message()
    );
}

#[tokio::test]
async fn consume_deletes_every_entry_a_full_drain_yielded() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed(
        dir.path(),
        &[
            ("00000000000000000000", csv_document(&[1, 2])),
            ("00000000000000000001", csv_document(&[3])),
        ],
    );

    let config = RedbSourceConfig {
        consume: true,
        ..source_config(dir.path())
    };
    let mut source = RedbSource::new(config, schema(), Arc::new(CsvTransformer::new(true)))
        .expect("open source");
    assert_eq!(drain(&mut source).await, vec![1, 2, 3]);
    source.finish().await.expect("finish");
    assert!(keys(dir.path()).is_empty(), "finish must delete both keys");

    let config = RedbSourceConfig {
        consume: true,
        ..source_config(dir.path())
    };
    let mut second = RedbSource::new(config, schema(), Arc::new(CsvTransformer::new(true)))
        .expect("open source");
    assert!(
        drain(&mut second).await.is_empty(),
        "a consumed file has nothing left to yield"
    );
}

/// Only an entry whose batch stream ended is deleted.
///
/// A caller that stops mid-file has been handed the first entry whole and
/// nothing of the second, so a `finish` there must leave the second for the
/// next run. This is what makes the mode safe for a reader that aborts.
#[tokio::test]
async fn consume_leaves_an_entry_the_drain_never_finished() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed(
        dir.path(),
        &[
            ("00000000000000000000", csv_document(&[1, 2])),
            ("00000000000000000001", csv_document(&[3])),
        ],
    );

    let config = RedbSourceConfig {
        consume: true,
        ..source_config(dir.path())
    };
    let mut source = RedbSource::new(config, schema(), Arc::new(CsvTransformer::new(true)))
        .expect("open source");

    // The first call yields the first entry's only batch; the second ends
    // that entry's stream, which is what records its key, and opens the next
    // one. So two calls have consumed exactly the first entry.
    source.next_batch().await.expect("first batch");
    source.next_batch().await.expect("second batch");
    source.finish().await.expect("finish");

    assert_eq!(
        keys(dir.path()),
        vec!["00000000000000000001".to_string()],
        "the entry whose stream never ended stays"
    );
}

#[tokio::test]
async fn a_consuming_source_dropped_without_finish_deletes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed(
        dir.path(),
        &[
            ("00000000000000000000", csv_document(&[1, 2])),
            ("00000000000000000001", csv_document(&[3])),
        ],
    );

    let config = RedbSourceConfig {
        consume: true,
        ..source_config(dir.path())
    };
    let mut source = RedbSource::new(config, schema(), Arc::new(CsvTransformer::new(true)))
        .expect("open source");
    assert_eq!(drain(&mut source).await, vec![1, 2, 3]);
    drop(source);

    let config = RedbSourceConfig {
        consume: true,
        ..source_config(dir.path())
    };
    let mut second = RedbSource::new(config, schema(), Arc::new(CsvTransformer::new(true)))
        .expect("open source");
    assert_eq!(
        drain(&mut second).await,
        vec![1, 2, 3],
        "an unfinished instance leaves every entry for the next one"
    );
}

#[tokio::test]
async fn a_second_finish_after_a_successful_one_is_a_no_op() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed(
        dir.path(),
        &[
            ("00000000000000000000", csv_document(&[1, 2])),
            ("00000000000000000001", csv_document(&[3])),
        ],
    );

    let config = RedbSourceConfig {
        consume: true,
        ..source_config(dir.path())
    };
    let mut source = RedbSource::new(config, schema(), Arc::new(CsvTransformer::new(true)))
        .expect("open source");
    assert_eq!(drain(&mut source).await, vec![1, 2, 3]);
    source.finish().await.expect("first finish");
    assert!(
        keys(dir.path()).is_empty(),
        "the first finish deletes both keys"
    );

    // Nothing left to record, so a second call has nothing to delete either.
    source
        .finish()
        .await
        .expect("a second finish is not an error");
    assert!(
        keys(dir.path()).is_empty(),
        "the second finish deletes nothing further"
    );

    // The file itself is still sound: a fresh open still reads through it.
    let config = RedbSourceConfig {
        consume: true,
        ..source_config(dir.path())
    };
    let mut reopened = RedbSource::new(config, schema(), Arc::new(CsvTransformer::new(true)))
        .expect("the file still opens after the extra finish");
    assert!(drain(&mut reopened).await.is_empty());
}

#[tokio::test]
async fn finish_before_any_next_batch_deletes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed(
        dir.path(),
        &[
            ("00000000000000000000", csv_document(&[1, 2])),
            ("00000000000000000001", csv_document(&[3])),
        ],
    );

    let config = RedbSourceConfig {
        consume: true,
        ..source_config(dir.path())
    };
    let mut source = RedbSource::new(config, schema(), Arc::new(CsvTransformer::new(true)))
        .expect("open source");
    // No `next_batch` call at all: nothing was handed over whole, so there
    // is nothing for `finish` to record.
    source
        .finish()
        .await
        .expect("finish with nothing drained is not an error");

    assert_eq!(
        keys(dir.path()),
        vec![
            "00000000000000000000".to_string(),
            "00000000000000000001".to_string(),
        ],
        "a finish with no prior drain deletes nothing"
    );
}

#[tokio::test]
async fn consume_false_leaves_every_entry_after_finish() {
    let dir = tempfile::tempdir().expect("tempdir");
    seed(
        dir.path(),
        &[
            ("00000000000000000000", csv_document(&[1, 2])),
            ("00000000000000000001", csv_document(&[3])),
        ],
    );

    // `source_config`'s default is `consume: false`.
    let mut source = RedbSource::new(
        source_config(dir.path()),
        schema(),
        Arc::new(CsvTransformer::new(true)),
    )
    .expect("open source");
    assert_eq!(drain(&mut source).await, vec![1, 2, 3]);
    source.finish().await.expect("finish");

    assert_eq!(
        keys(dir.path()),
        vec![
            "00000000000000000000".to_string(),
            "00000000000000000001".to_string(),
        ],
        "a non-consuming source's finish deletes nothing, even after a full drain"
    );
}
