//! Raft snapshot builder and installer for the Arrow-IPC state machine.
//!
//! ## Snapshot format
//!
//! The snapshot payload is a sequence of length-prefixed postcard chunks, one
//! per table. The format is:
//!
//! ```text
//! Header (16 bytes):
//!   [8 bytes: ARROWSNA]  magic
//!   [4 bytes LE u32: 2]  version
//!   [4 bytes LE u32]     CRC-32 (IEEE) of the body that follows
//! Body (repeated):
//!   [4 bytes LE u32 = chunk length]
//!   [chunk_length bytes of postcard-encoded SnapshotChunk]
//! Sentinel: [4 bytes of 0x00]
//! ```
//!
//! Postcard is used here, consistent with log entries, because the tables store
//! metadata with heterogeneous record shapes. The Arrow IPC bytes that represent
//! RecordBatch data sit inside `MasterBatchRecord.ipc_bytes` as opaque binary fields
//! of the postcard rows.
//!
//! Version 2 is the current format. Version 1 snapshots used JSON chunks and are not
//! readable here.
//!
//! ## Install procedure
//!
//! 1. Deserialize the payload into in-memory records.
//! 2. Call `restore_state` on the target database.
//! 3. Update `last_applied` and `last_membership` in memory.

#[cfg(feature = "distributed-raft")]
pub(crate) mod raft_impl {
    use std::io::Cursor;
    use std::io::{Read, Write};
    use std::sync::{Arc, Mutex};

    use openraft::type_config::alias::{LogIdOf, SnapshotOf, StoredMembershipOf};
    use openraft::{RaftSnapshotBuilder, Snapshot, SnapshotMeta};
    use redb::Database;
    use serde::{Deserialize, Serialize};

    use crate::SaciError;
    use crate::distributed::consensus::state_machine::{
        CheckpointRecord, ClaimRecord, InstanceRecord, MasterBatchRecord, dump_state, restore_state,
    };
    use crate::distributed::consensus::types::SaciTypeConfig;

    #[derive(Serialize, Deserialize)]
    enum SnapshotChunk {
        MasterBatches(Vec<MasterBatchRecord>),
        Claims(Vec<(String, ClaimRecord)>), // uuid string → record
        Checkpoints(Vec<(String, CheckpointRecord)>), // hex key → record
        Instances(Vec<(String, InstanceRecord)>), // uuid string → record
    }

    const SNAPSHOT_MAGIC: &[u8; 8] = b"ARROWSNA";
    const SNAPSHOT_VERSION: u32 = 2;
    const HEADER_LEN: usize = 16; // 8 + 4 + 4

    /// CRC-32 (IEEE 802.3 polynomial 0xEDB88320), hand-rolled to avoid a dependency.
    fn crc32(data: &[u8]) -> u32 {
        const POLY: u32 = 0xEDB8_8320;
        let mut crc: u32 = 0xFFFF_FFFF;
        for byte in data {
            crc ^= u32::from(*byte);
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ POLY;
                } else {
                    crc >>= 1;
                }
            }
        }
        crc ^ 0xFFFF_FFFF
    }

    fn add_header(body: Vec<u8>) -> Vec<u8> {
        let checksum = crc32(&body);
        let mut out = Vec::with_capacity(HEADER_LEN + body.len());
        out.extend_from_slice(SNAPSHOT_MAGIC);
        out.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
        out.extend_from_slice(&checksum.to_le_bytes());
        out.extend_from_slice(&body);
        out
    }

    /// Strip and validate the 16-byte header. Returns the body slice on success.
    fn strip_header(data: &[u8]) -> Result<&[u8], std::io::Error> {
        if data.len() < HEADER_LEN {
            return Err(std::io::Error::other(format!(
                "snapshot too short: {} bytes (need at least {HEADER_LEN})",
                data.len()
            )));
        }
        let magic = &data[..8];
        if magic != SNAPSHOT_MAGIC {
            return Err(std::io::Error::other(format!(
                "snapshot magic mismatch: got {:?}",
                magic
            )));
        }
        let version = u32::from_le_bytes(data[8..12].try_into().unwrap());
        if version != SNAPSHOT_VERSION {
            return Err(std::io::Error::other(format!(
                "snapshot version {version} not supported (expected {SNAPSHOT_VERSION})"
            )));
        }
        let stored_crc = u32::from_le_bytes(data[12..16].try_into().unwrap());
        let body = &data[HEADER_LEN..];
        let computed_crc = crc32(body);
        if stored_crc != computed_crc {
            return Err(std::io::Error::other(format!(
                "snapshot CRC-32 mismatch: stored {stored_crc:#010x}, computed {computed_crc:#010x}"
            )));
        }
        Ok(body)
    }

    fn write_chunk<W: Write>(writer: &mut W, chunk: &SnapshotChunk) -> Result<(), std::io::Error> {
        let bytes = postcard::to_allocvec(chunk)
            .map_err(|e| std::io::Error::other(format!("snapshot chunk encode: {e}")))?;
        let len = bytes.len() as u32;
        writer.write_all(&len.to_le_bytes())?;
        writer.write_all(&bytes)?;
        Ok(())
    }

    fn read_chunks(body: &[u8]) -> Result<Vec<SnapshotChunk>, std::io::Error> {
        let mut reader = std::io::Cursor::new(body);
        let mut chunks = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let len = u32::from_le_bytes(len_buf) as usize;
            if len == 0 {
                break;
            }
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf)?;
            let chunk: SnapshotChunk = postcard::from_bytes(&buf)
                .map_err(|e| std::io::Error::other(format!("snapshot chunk decode: {e}")))?;
            chunks.push(chunk);
        }
        Ok(chunks)
    }

    pub fn build_snapshot_bytes(db: &Database) -> Result<Vec<u8>, SaciError> {
        let (batches, claims, checkpoints, instances) = dump_state(db)?;

        let claims_str: Vec<(String, ClaimRecord)> = claims
            .into_iter()
            .map(|(id, rec)| (id.to_string(), rec))
            .collect();
        let cp_str: Vec<(String, CheckpointRecord)> = checkpoints
            .into_iter()
            .map(|(key, rec)| (hex::encode(key), rec))
            .collect();
        let inst_str: Vec<(String, InstanceRecord)> = instances
            .into_iter()
            .map(|(id, rec)| (id.to_string(), rec))
            .collect();

        let mut body: Vec<u8> = Vec::new();
        write_chunk(&mut body, &SnapshotChunk::MasterBatches(batches))
            .map_err(|e| SaciError::generic(format!("snapshot write: {e}")))?;
        write_chunk(&mut body, &SnapshotChunk::Claims(claims_str))
            .map_err(|e| SaciError::generic(format!("snapshot write: {e}")))?;
        write_chunk(&mut body, &SnapshotChunk::Checkpoints(cp_str))
            .map_err(|e| SaciError::generic(format!("snapshot write: {e}")))?;
        write_chunk(&mut body, &SnapshotChunk::Instances(inst_str))
            .map_err(|e| SaciError::generic(format!("snapshot write: {e}")))?;
        // Sentinel.
        body.extend_from_slice(&0u32.to_le_bytes());
        Ok(add_header(body))
    }

    /// Install a snapshot payload into `db`.
    ///
    /// `sm_meta`, when `Some`, writes `(last_applied_bytes, last_membership_bytes)`
    /// into `arrow_sm_meta` in the same write transaction as the table content, so
    /// data and watermark share one commit and one fsync.
    pub fn install_snapshot_bytes(
        db: &Database,
        data: &[u8],
        sm_meta: Option<(&[u8], &[u8])>,
    ) -> Result<(), SaciError> {
        let body = strip_header(data)
            .map_err(|e| SaciError::generic(format!("snapshot header invalid: {e}")))?;
        let chunks =
            read_chunks(body).map_err(|e| SaciError::generic(format!("snapshot read: {e}")))?;

        let mut batches = Vec::new();
        let mut claims = Vec::new();
        let mut checkpoints = Vec::new();
        let mut instances = Vec::new();

        for chunk in chunks {
            match chunk {
                SnapshotChunk::MasterBatches(b) => batches = b,
                SnapshotChunk::Claims(c) => {
                    for (id_str, rec) in c {
                        let id = uuid::Uuid::parse_str(&id_str)
                            .map_err(|e| SaciError::generic(format!("snapshot claim uuid: {e}")))?;
                        claims.push((id, rec));
                    }
                }
                SnapshotChunk::Checkpoints(c) => {
                    for (hex_str, rec) in c {
                        let key_vec = hex::decode(&hex_str).map_err(|e| {
                            SaciError::generic(format!("snapshot checkpoint key: {e}"))
                        })?;
                        let key: [u8; 20] = key_vec.try_into().map_err(|_| {
                            SaciError::generic("snapshot checkpoint key not 20 bytes")
                        })?;
                        checkpoints.push((key, rec));
                    }
                }
                SnapshotChunk::Instances(i) => {
                    for (id_str, rec) in i {
                        let id = uuid::Uuid::parse_str(&id_str).map_err(|e| {
                            SaciError::generic(format!("snapshot instance uuid: {e}"))
                        })?;
                        instances.push((id, rec));
                    }
                }
            }
        }

        restore_state(db, batches, claims, checkpoints, instances, sm_meta)
    }

    /// openraft `RaftSnapshotBuilder` that serializes the redb state as a
    /// length-prefixed postcard chunk stream.
    pub struct ArrowSnapshotBuilder {
        pub db: Arc<Mutex<Database>>,
        pub last_applied: Option<LogIdOf<SaciTypeConfig>>,
        pub last_membership: StoredMembershipOf<SaciTypeConfig>,
    }

    impl RaftSnapshotBuilder<SaciTypeConfig> for ArrowSnapshotBuilder {
        type SnapshotData = Cursor<Vec<u8>>;

        async fn build_snapshot(
            &mut self,
        ) -> Result<SnapshotOf<SaciTypeConfig, Self::SnapshotData>, std::io::Error> {
            let db = self.db.lock().unwrap();
            let payload = build_snapshot_bytes(&db)
                .map_err(|e| std::io::Error::other(format!("build_snapshot: {e}")))?;

            let meta = SnapshotMeta {
                last_log_id: self.last_applied,
                last_membership: self.last_membership.clone(),
            };
            Ok(Snapshot {
                meta,
                snapshot: Cursor::new(payload),
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::distributed::consensus::state_machine::{apply, read_master_batch};
        use crate::distributed::consensus::types::ConsensusCommand;

        fn temp_db() -> (Database, tempfile::TempPath) {
            let file = tempfile::NamedTempFile::new().expect("tempfile");
            let path = file.into_temp_path();
            let db = Database::create(&path).expect("redb create");
            (db, path)
        }

        fn small_ipc() -> Vec<u8> {
            vec![0xAB; 64]
        }

        #[test]
        fn test_snapshot_build_install_round_trip() {
            let (db, _path) = temp_db();

            for i in 0u64..3 {
                apply(
                    &db,
                    ConsensusCommand::RegisterMasterBatch {
                        batch_id: i,
                        component: format!("comp_{i}"),
                        schema_id: 1,
                        ipc_bytes: small_ipc(),
                        total_rows: 100,
                        now_at_propose: 0,
                    },
                )
                .unwrap();
            }

            let claim_id = uuid::Uuid::now_v7();
            let inst = uuid::Uuid::now_v7();
            apply(
                &db,
                ConsensusCommand::ClaimRowRange {
                    batch_id: 0,
                    row_range_start: 0,
                    row_range_end: 50,
                    claim_id,
                    instance_id: inst,
                    lease_ttl_millis: 30_000,
                    now_at_propose: 0,
                },
            )
            .unwrap();

            let snapshot_bytes = build_snapshot_bytes(&db).unwrap();
            assert!(!snapshot_bytes.is_empty());

            let (db2, _path2) = temp_db();
            install_snapshot_bytes(&db2, &snapshot_bytes, None).unwrap();

            for i in 0u64..3 {
                let batch = read_master_batch(&db2, i).unwrap().unwrap();
                assert_eq!(batch.component, format!("comp_{i}"));
            }

            use crate::distributed::consensus::state_machine::read_claim;
            let claim = read_claim(&db2, claim_id).unwrap().unwrap();
            assert_eq!(claim.row_range_start, 0);
            assert_eq!(claim.row_range_end, 50);
        }

        #[test]
        fn snapshot_format_magic_version_crc_valid_round_trip() {
            let (db, _path) = temp_db();
            // Empty state still produces a valid header.
            let snap = build_snapshot_bytes(&db).unwrap();
            assert!(snap.len() >= HEADER_LEN, "snapshot must have header");
            let body = strip_header(&snap).expect("header must be valid");
            assert!(
                !body.is_empty(),
                "body must have at least the sentinel chunk"
            );
        }

        #[test]
        fn snapshot_format_wrong_magic_rejected() {
            let (db, _path) = temp_db();
            let mut snap = build_snapshot_bytes(&db).unwrap();
            // Corrupt magic bytes.
            snap[0] = b'X';
            let err = strip_header(&snap).unwrap_err();
            assert!(
                err.to_string().contains("magic mismatch"),
                "expected magic mismatch, got: {err}"
            );
        }

        #[test]
        fn snapshot_format_wrong_version_rejected() {
            let (db, _path) = temp_db();
            let mut snap = build_snapshot_bytes(&db).unwrap();
            // Bump version to 99.
            snap[8..12].copy_from_slice(&99u32.to_le_bytes());
            let err = strip_header(&snap).unwrap_err();
            assert!(
                err.to_string().contains("version 99 not supported"),
                "expected version error, got: {err}"
            );
        }

        #[test]
        fn snapshot_format_crc_mismatch_rejected() {
            let (db, _path) = temp_db();
            let mut snap = build_snapshot_bytes(&db).unwrap();
            // Flip a bit in the body (after the 16-byte header).
            if snap.len() > HEADER_LEN {
                snap[HEADER_LEN] ^= 0xFF;
            }
            let err = strip_header(&snap).unwrap_err();
            assert!(
                err.to_string().contains("CRC-32 mismatch"),
                "expected CRC mismatch, got: {err}"
            );
        }

        #[test]
        fn snapshot_format_truncated_rejected() {
            // Too short to have a header.
            let err = strip_header(&[0u8; 10]).unwrap_err();
            assert!(
                err.to_string().contains("too short"),
                "expected too-short error, got: {err}"
            );
        }

        #[test]
        fn install_snapshot_with_tampered_data_rejected() {
            let (db_src, _p1) = temp_db();
            let mut snap = build_snapshot_bytes(&db_src).unwrap();
            // Flip body bit → CRC mismatch.
            if snap.len() > HEADER_LEN {
                snap[HEADER_LEN] ^= 0x01;
            }
            let (db_dst, _p2) = temp_db();
            let err = install_snapshot_bytes(&db_dst, &snap, None).unwrap_err();
            assert!(
                err.to_string().contains("CRC"),
                "install must reject tampered snapshot: {err}"
            );
        }
    }
}

#[cfg(feature = "distributed-raft")]
pub use raft_impl::{ArrowSnapshotBuilder, build_snapshot_bytes, install_snapshot_bytes};
