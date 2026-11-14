//! [`ParquetCheckpointStore`]: cold-storage archival checkpoint store.
//!
//! Persists each checkpoint as a Parquet file on disk, one file per
//! `(claim_id, stage_idx)` pair, with a JSON sidecar for metadata that cannot
//! be round-tripped through Parquet.
//!
//! ## Purpose
//!
//! This store is **not** a primary operational checkpoint store. Use it for:
//!
//! - Archiving completed-pipeline checkpoints for later inspection or replay.
//! - Human-readable, tool-inspectable snapshots (Parquet is self-describing).
//! - Long-lived cold storage that outlives a single pipeline run.
//!
//! Live distributed operation needs crash recovery and at-least-once semantics, so
//! there [`RedbSharedStore`](crate::distributed::RedbSharedStore) is the primary
//! store and this one is only an archival sink.
//!
//! ## Limitations
//!
//! - **Single-writer per checkpoint**: no file locking is performed. Concurrent writers
//!   to the same `(claim_id, stage_idx)` corrupt the Parquet file, so only one writer
//!   may operate per checkpoint path at a time.
//!
//! - **Payload encoding**: the Arrow IPC `payload` field on [`Checkpoint`] is decoded
//!   from IPC, written to Parquet, then re-encoded to IPC on read. Data round-trips
//!   losslessly, but Arrow schema metadata carried in IPC custom fields may not.
//!
//! ## Sidecar metadata
//!
//! Each Parquet file `<uuid>-stage<NNNN>.parquet` has a sidecar
//! `<uuid>-stage<NNNN>.meta.json` containing:
//!
//! ```json
//! { "batch_id": 42, "schema_id": 1, "created_at": 1700000000000 }
//! ```
//!
//! The Parquet file is written first, the sidecar second. [`read_checkpoint`] returns
//! `Ok(None)` if either file is missing.
//!
//! [`read_checkpoint`]: CheckpointStore::load_checkpoint

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use async_trait::async_trait;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::SaciError;
use crate::SaciResult;
use crate::distributed::checkpoint::{Checkpoint, CheckpointStore};

/// JSON sidecar that accompanies each Parquet checkpoint file.
///
/// Parquet cannot store arbitrary scalar metadata beyond what Arrow schema metadata
/// supports, so the fields that cannot be recovered from the batch itself live in a
/// small JSON file.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CheckpointMeta {
    batch_id: u64,
    schema_id: u32,
    created_at: u64,
}

/// Derive a deterministic `u64` from the Parquet file path.
///
/// Used as the checkpoint ID returned from [`CheckpointStore::save_checkpoint`].
/// The value is stable for a given path but not globally unique: a best-effort ID for
/// idempotency checks, not a cryptographic hash.
fn hash_path_to_u64(path: &Path) -> u64 {
    let mut h = DefaultHasher::new();
    path.hash(&mut h);
    h.finish()
}

/// Archival checkpoint store that writes each checkpoint to a Parquet file.
///
/// See the [module documentation](self) for usage guidelines and limitations.
pub struct ParquetCheckpointStore {
    root: PathBuf,
}

impl ParquetCheckpointStore {
    /// Create a new store rooted at `root`.
    ///
    /// The directory is created (including parents) if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` if the directory cannot be created.
    pub fn new(root: impl Into<PathBuf>) -> SaciResult<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(|e| {
            SaciError::generic(format!(
                "ParquetCheckpointStore: cannot create root dir {root:?}: {e}"
            ))
        })?;
        Ok(Self { root })
    }

    /// Absolute path for the Parquet data file.
    fn parquet_path(&self, claim_id: Uuid, stage_idx: u32) -> PathBuf {
        self.root
            .join(format!("{claim_id}-stage{stage_idx:04}.parquet"))
    }

    /// Absolute path for the JSON metadata sidecar.
    fn meta_path(&self, claim_id: Uuid, stage_idx: u32) -> PathBuf {
        self.root
            .join(format!("{claim_id}-stage{stage_idx:04}.meta.json"))
    }

    /// Decode Arrow IPC bytes into a list of [`RecordBatch`]es plus the schema.
    fn decode_ipc(ipc_bytes: &[u8]) -> SaciResult<(Arc<arrow_schema::Schema>, Vec<RecordBatch>)> {
        let cursor = std::io::Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).map_err(|e| {
            SaciError::generic(format!("ParquetCheckpointStore: IPC decode error: {e}"))
        })?;
        let schema = reader.schema();
        let batches: Vec<RecordBatch> = reader.filter_map(|r| r.ok()).collect();
        Ok((schema, batches))
    }

    /// Encode a list of [`RecordBatch`]es to Arrow IPC stream bytes.
    fn encode_ipc(schema: &arrow_schema::Schema, batches: &[RecordBatch]) -> SaciResult<Vec<u8>> {
        let mut buf = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buf, schema).map_err(|e| {
            SaciError::generic(format!("ParquetCheckpointStore: IPC encode error: {e}"))
        })?;
        for batch in batches {
            writer.write(batch).map_err(|e| {
                SaciError::generic(format!(
                    "ParquetCheckpointStore: IPC write batch error: {e}"
                ))
            })?;
        }
        writer.finish().map_err(|e| {
            SaciError::generic(format!("ParquetCheckpointStore: IPC finish error: {e}"))
        })?;
        Ok(buf)
    }
}

#[async_trait]
impl CheckpointStore for ParquetCheckpointStore {
    /// Persist a checkpoint by writing a Parquet data file and a JSON sidecar.
    ///
    /// The Arrow IPC payload in `ipc_bytes` is decoded to [`RecordBatch`]es and written
    /// as Parquet (Snappy-compressed). The metadata fields (`batch_id`, `schema_id`,
    /// `created_at`) go to the JSON sidecar.
    ///
    /// Returns a deterministic `u64` checkpoint ID derived from the Parquet
    /// file path.
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` if the IPC payload is malformed, the file
    /// cannot be created, or the Parquet/JSON write fails.
    async fn save_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
        ipc_bytes: Vec<u8>,
        schema_id: u32,
    ) -> SaciResult<()> {
        let created_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            let store = ParquetCheckpointStore { root };
            store.write_checkpoint_internal(
                claim_id,
                stage_idx,
                &ipc_bytes,
                CheckpointMeta {
                    batch_id: 0,
                    schema_id,
                    created_at,
                },
            )
        })
        .await
        .map_err(|e| SaciError::generic(format!("save_checkpoint join error: {e}")))?
    }

    /// Load a checkpoint from Parquet + sidecar, re-encoding to Arrow IPC.
    ///
    /// Returns `Ok(None)` if no Parquet file or sidecar exists for the given
    /// `(claim_id, stage_idx)`.
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` if the files exist but cannot be read or
    /// decoded.
    async fn load_checkpoint(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
    ) -> SaciResult<Option<Checkpoint>> {
        let pq_path = self.parquet_path(claim_id, stage_idx);
        let meta_path = self.meta_path(claim_id, stage_idx);

        tokio::task::spawn_blocking(move || Self::load_blocking(pq_path, meta_path, stage_idx))
            .await
            .map_err(|e| SaciError::generic(format!("load_checkpoint join error: {e}")))?
    }
}

impl ParquetCheckpointStore {
    /// Blocking path for `load_checkpoint`; called inside `spawn_blocking`.
    fn load_blocking(
        pq_path: PathBuf,
        meta_path: PathBuf,
        stage_idx: u32,
    ) -> SaciResult<Option<Checkpoint>> {
        if !pq_path.exists() || !meta_path.exists() {
            return Ok(None);
        }

        let meta_bytes = std::fs::read(&meta_path).map_err(|e| {
            SaciError::generic(format!(
                "ParquetCheckpointStore: cannot read sidecar {meta_path:?}: {e}"
            ))
        })?;
        let meta: CheckpointMeta = serde_json::from_slice(&meta_bytes).map_err(|e| {
            SaciError::generic(format!(
                "ParquetCheckpointStore: sidecar JSON parse error: {e}"
            ))
        })?;

        let file = std::fs::File::open(&pq_path).map_err(|e| {
            SaciError::generic(format!(
                "ParquetCheckpointStore: cannot open Parquet {pq_path:?}: {e}"
            ))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            SaciError::generic(format!(
                "ParquetCheckpointStore: Parquet builder error: {e}"
            ))
        })?;
        let schema = builder.schema().clone();
        let reader = builder.build().map_err(|e| {
            SaciError::generic(format!(
                "ParquetCheckpointStore: Parquet reader build error: {e}"
            ))
        })?;
        let batches: Vec<RecordBatch> = reader.filter_map(|r| r.ok()).collect();

        let payload = Self::encode_ipc(&schema, &batches)?;

        Ok(Some(Checkpoint {
            batch_id: meta.batch_id,
            stage_idx,
            payload,
            schema_id: meta.schema_id,
            created_at: meta.created_at,
        }))
    }
}

impl ParquetCheckpointStore {
    /// Internal helper: write Parquet file then sidecar, atomically.
    ///
    /// Write order:
    /// 1. Write Parquet to `{path}.tmp`, fsync the file.
    /// 2. Rename `.tmp` to final path (atomic on POSIX).
    /// 3. fsync the parent directory to flush the directory entry.
    /// 4. Write the JSON sidecar using the same tmp-rename pattern.
    ///
    /// A crash between steps 1 and 2 leaves a `.tmp` file. On load, the final
    /// path does not exist so `load_checkpoint` returns `Ok(None)`.
    fn write_checkpoint_internal(
        &self,
        claim_id: Uuid,
        stage_idx: u32,
        ipc_bytes: &[u8],
        meta: CheckpointMeta,
    ) -> SaciResult<()> {
        let pq_path = self.parquet_path(claim_id, stage_idx);
        let meta_path = self.meta_path(claim_id, stage_idx);
        let pq_tmp = pq_path.with_extension("parquet.tmp");
        let meta_tmp = meta_path.with_extension("meta.json.tmp");

        let (schema, batches) = Self::decode_ipc(ipc_bytes)?;

        // Write Parquet to .tmp, fsync, then rename atomically.
        {
            let file = std::fs::File::create(&pq_tmp).map_err(|e| {
                SaciError::generic(format!(
                    "ParquetCheckpointStore: cannot create tmp {pq_tmp:?}: {e}"
                ))
            })?;
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            let mut writer =
                ArrowWriter::try_new(&file, schema.clone(), Some(props)).map_err(|e| {
                    SaciError::generic(format!(
                        "ParquetCheckpointStore: ArrowWriter init error: {e}"
                    ))
                })?;
            for batch in &batches {
                writer.write(batch).map_err(|e| {
                    SaciError::generic(format!("ParquetCheckpointStore: Parquet write error: {e}"))
                })?;
            }
            writer.close().map_err(|e| {
                SaciError::generic(format!("ParquetCheckpointStore: Parquet close error: {e}"))
            })?;
            file.sync_all().map_err(|e| {
                SaciError::generic(format!(
                    "ParquetCheckpointStore: fsync failed for {pq_tmp:?}: {e}"
                ))
            })?;
        }
        std::fs::rename(&pq_tmp, &pq_path).map_err(|e| {
            SaciError::generic(format!(
                "ParquetCheckpointStore: rename {pq_tmp:?} to {pq_path:?} failed: {e}"
            ))
        })?;
        // fsync parent dir to persist the new directory entry.
        if let Some(parent) = pq_path.parent()
            && let Ok(dir) = std::fs::File::open(parent)
            && let Err(_e) = dir.sync_all()
        {
            #[cfg(feature = "tracing")]
            tracing::debug!(error = %_e, "parent dir fsync failed");
        }

        // Write JSON sidecar via tmp → rename.
        {
            let meta_json = serde_json::to_vec(&meta).map_err(|e| {
                SaciError::generic(format!(
                    "ParquetCheckpointStore: sidecar serialise error: {e}"
                ))
            })?;
            let mut file = std::fs::File::create(&meta_tmp).map_err(|e| {
                SaciError::generic(format!(
                    "ParquetCheckpointStore: cannot create tmp {meta_tmp:?}: {e}"
                ))
            })?;
            file.write_all(&meta_json).map_err(|e| {
                SaciError::generic(format!(
                    "ParquetCheckpointStore: sidecar write error {meta_tmp:?}: {e}"
                ))
            })?;
            file.sync_all().map_err(|e| {
                SaciError::generic(format!(
                    "ParquetCheckpointStore: fsync failed for {meta_tmp:?}: {e}"
                ))
            })?;
        }
        std::fs::rename(&meta_tmp, &meta_path).map_err(|e| {
            SaciError::generic(format!(
                "ParquetCheckpointStore: rename {meta_tmp:?} to {meta_path:?} failed: {e}"
            ))
        })?;
        if let Some(parent) = meta_path.parent()
            && let Ok(dir) = std::fs::File::open(parent)
            && let Err(_e) = dir.sync_all()
        {
            #[cfg(feature = "tracing")]
            tracing::debug!(error = %_e, "parent dir fsync failed");
        }

        let _ = hash_path_to_u64(&pq_path);
        Ok(())
    }

    /// Write a complete [`Checkpoint`] to disk.
    ///
    /// Unlike [`CheckpointStore::save_checkpoint`], this method preserves all
    /// metadata fields (`batch_id`, `schema_id`, `created_at`) in the sidecar.
    /// Use this when archiving full [`Checkpoint`] structs produced by a
    /// [`DistributedRunner`](crate::distributed::runner::DistributedRunner).
    pub fn archive_checkpoint(&self, claim_id: Uuid, checkpoint: &Checkpoint) -> SaciResult<()> {
        self.write_checkpoint_internal(
            claim_id,
            checkpoint.stage_idx,
            &checkpoint.payload,
            CheckpointMeta {
                batch_id: checkpoint.batch_id,
                schema_id: checkpoint.schema_id,
                created_at: checkpoint.created_at,
            },
        )
    }

    /// Delete the Parquet file and sidecar for `(claim_id, stage_idx)`.
    ///
    /// A no-op if neither file exists.
    ///
    /// # Errors
    ///
    /// Returns `SaciError::Generic` if an existing file cannot be removed.
    pub fn delete_checkpoint(&self, claim_id: Uuid, stage_idx: u32) -> SaciResult<()> {
        let pq_path = self.parquet_path(claim_id, stage_idx);
        let meta_path = self.meta_path(claim_id, stage_idx);

        for path in &[&pq_path, &meta_path] {
            if path.exists() {
                std::fs::remove_file(path).map_err(|e| {
                    SaciError::generic(format!(
                        "ParquetCheckpointStore: cannot delete {path:?}: {e}"
                    ))
                })?;
            }
        }
        Ok(())
    }
}

/// Build a small Arrow IPC byte stream from a single [`RecordBatch`].
///
/// Useful in tests to avoid depending on the full distributed runner.
#[cfg(test)]
pub(crate) fn build_ipc_payload(batch: &RecordBatch) -> Vec<u8> {
    let schema = batch.schema();
    let mut buf = Vec::new();
    let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
    w.write(batch).unwrap();
    w.finish().unwrap();
    buf
}

#[cfg(all(test, feature = "parquet-checkpoint"))]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int32Array};
    use arrow_schema::{DataType, Field, Schema};
    use tempfile::TempDir;

    fn simple_schema() -> Arc<arrow_schema::Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Float64, false),
        ]))
    }

    fn simple_batch(schema: Arc<arrow_schema::Schema>, n: i32) -> RecordBatch {
        let ids = Arc::new(Int32Array::from_iter_values(0..n));
        let vals = Arc::new(Float64Array::from_iter_values(
            (0..n).map(|i| i as f64 * 2.5),
        ));
        RecordBatch::try_new(schema, vec![ids, vals]).unwrap()
    }

    fn make_checkpoint(_claim_id: Uuid, stage_idx: u32, n: i32) -> Checkpoint {
        let schema = simple_schema();
        let batch = simple_batch(schema, n);
        let payload = build_ipc_payload(&batch);
        Checkpoint {
            batch_id: 99,
            stage_idx,
            payload,
            schema_id: 7,
            created_at: 1_700_000_000_000,
        }
    }

    // Write + read round-trip including sidecar metadata.
    #[tokio::test]
    async fn test_write_read_round_trip() {
        let dir = TempDir::new().unwrap();
        let store = ParquetCheckpointStore::new(dir.path()).unwrap();

        let claim_id = Uuid::now_v7();
        let stage_idx = 3u32;
        let cp = make_checkpoint(claim_id, stage_idx, 10);

        store.archive_checkpoint(claim_id, &cp).unwrap();

        let loaded = store.load_checkpoint(claim_id, stage_idx).await.unwrap();
        assert!(loaded.is_some(), "expected Some(checkpoint)");
        let loaded = loaded.unwrap();

        // Metadata round-trips.
        assert_eq!(loaded.batch_id, 99);
        assert_eq!(loaded.stage_idx, stage_idx);
        assert_eq!(loaded.schema_id, 7);
        assert_eq!(loaded.created_at, 1_700_000_000_000);

        // Payload re-encodes to valid IPC with same row count.
        let cursor = std::io::Cursor::new(&loaded.payload);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let batches: Vec<RecordBatch> = reader.filter_map(|r| r.ok()).collect();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10);
    }

    // Reading a non-existent checkpoint returns None.
    #[tokio::test]
    async fn test_read_nonexistent_returns_none() {
        let dir = TempDir::new().unwrap();
        let store = ParquetCheckpointStore::new(dir.path()).unwrap();
        let result = store.load_checkpoint(Uuid::now_v7(), 0).await.unwrap();
        assert!(result.is_none(), "expected None for missing checkpoint");
    }

    // Delete removes both files.
    #[tokio::test]
    async fn test_delete_removes_both_files() {
        let dir = TempDir::new().unwrap();
        let store = ParquetCheckpointStore::new(dir.path()).unwrap();

        let claim_id = Uuid::now_v7();
        let stage_idx = 1u32;
        let cp = make_checkpoint(claim_id, stage_idx, 5);
        store.archive_checkpoint(claim_id, &cp).unwrap();

        // Both files must exist.
        assert!(store.parquet_path(claim_id, stage_idx).exists());
        assert!(store.meta_path(claim_id, stage_idx).exists());

        store.delete_checkpoint(claim_id, stage_idx).unwrap();

        // Neither file should remain.
        assert!(!store.parquet_path(claim_id, stage_idx).exists());
        assert!(!store.meta_path(claim_id, stage_idx).exists());

        // Subsequent load returns None (no file to read).
        let result = store.load_checkpoint(claim_id, stage_idx).await.unwrap();
        assert!(result.is_none());
    }

    // A large payload (10k rows) round-trips with the correct row count.
    #[tokio::test]
    async fn test_large_payload_round_trip() {
        let dir = TempDir::new().unwrap();
        let store = ParquetCheckpointStore::new(dir.path()).unwrap();

        let claim_id = Uuid::now_v7();
        let stage_idx = 0u32;
        let cp = make_checkpoint(claim_id, stage_idx, 10_000);

        store.archive_checkpoint(claim_id, &cp).unwrap();

        let loaded = store
            .load_checkpoint(claim_id, stage_idx)
            .await
            .unwrap()
            .unwrap();

        let cursor = std::io::Cursor::new(&loaded.payload);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let total: usize = reader.filter_map(|r| r.ok()).map(|b| b.num_rows()).sum();
        assert_eq!(total, 10_000);
    }

    // Multiple stages for the same claim_id each round-trip correctly.
    #[tokio::test]
    async fn test_multiple_stages_same_claim() {
        let dir = TempDir::new().unwrap();
        let store = ParquetCheckpointStore::new(dir.path()).unwrap();

        let claim_id = Uuid::now_v7();

        // Write stages 0, 1, 2 with different row counts.
        for stage in 0u32..3 {
            let row_count = (stage as i32 + 1) * 7;
            let cp = make_checkpoint(claim_id, stage, row_count);
            store.archive_checkpoint(claim_id, &cp).unwrap();
        }

        // Read each back and verify row counts.
        for stage in 0u32..3 {
            let expected_rows = ((stage as i32 + 1) * 7) as usize;
            let loaded = store
                .load_checkpoint(claim_id, stage)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(loaded.stage_idx, stage);

            let cursor = std::io::Cursor::new(&loaded.payload);
            let reader = StreamReader::try_new(cursor, None).unwrap();
            let total: usize = reader.filter_map(|r| r.ok()).map(|b| b.num_rows()).sum();
            assert_eq!(total, expected_rows, "stage {stage} row count mismatch");
        }
    }

    // save_checkpoint (CheckpointStore trait) writes recoverable data.
    #[tokio::test]
    async fn test_save_and_load_via_trait() {
        let dir = TempDir::new().unwrap();
        let store = ParquetCheckpointStore::new(dir.path()).unwrap();

        let claim_id = Uuid::now_v7();
        let schema = simple_schema();
        let batch = simple_batch(schema, 20);
        let ipc = build_ipc_payload(&batch);

        store.save_checkpoint(claim_id, 0, ipc, 3).await.unwrap();

        let loaded = store.load_checkpoint(claim_id, 0).await.unwrap().unwrap();
        assert_eq!(loaded.schema_id, 3);

        let cursor = std::io::Cursor::new(&loaded.payload);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let total: usize = reader.filter_map(|r| r.ok()).map(|b| b.num_rows()).sum();
        assert_eq!(total, 20);
    }
    // Atomic write: no partial file at the final path when renamed from .tmp.
    #[tokio::test]
    async fn test_parquet_save_atomic_tmp_rename() {
        let dir = TempDir::new().unwrap();
        let store = ParquetCheckpointStore::new(dir.path()).unwrap();

        let claim_id = Uuid::now_v7();
        let schema = simple_schema();
        let batch = simple_batch(schema, 5);
        let ipc = build_ipc_payload(&batch);

        // Successful save: the .tmp file must not remain, only the final file.
        store.save_checkpoint(claim_id, 0, ipc, 1).await.unwrap();

        let final_path = store.parquet_path(claim_id, 0);
        let tmp_path = final_path.with_extension("parquet.tmp");
        assert!(
            final_path.exists(),
            "final Parquet path must exist after save"
        );
        assert!(
            !tmp_path.exists(),
            ".tmp file must be removed after atomic rename"
        );
    }

    // A truncated Parquet file returns an error, not None and not a panic.
    #[tokio::test]
    async fn test_parquet_load_rejects_truncated() {
        let dir = TempDir::new().unwrap();
        let store = ParquetCheckpointStore::new(dir.path()).unwrap();

        let claim_id = Uuid::now_v7();
        let schema = simple_schema();
        let batch = simple_batch(schema, 10);
        let ipc = build_ipc_payload(&batch);

        // Write a valid checkpoint first.
        store.save_checkpoint(claim_id, 0, ipc, 1).await.unwrap();

        // Overwrite with truncated content, leaving no valid Parquet footer.
        let pq_path = store.parquet_path(claim_id, 0);
        let original = std::fs::read(&pq_path).unwrap();
        let half = original.len() / 2;
        std::fs::write(&pq_path, &original[..half]).unwrap();

        // load_checkpoint must return an error, not Ok(None) or panic.
        let result = store.load_checkpoint(claim_id, 0).await;
        assert!(result.is_err(), "truncated Parquet must return Err, not Ok");
    }
}
