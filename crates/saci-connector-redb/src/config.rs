//! Config structs for the redb source and sink.
//!
//! Both halves name the same four things: the directory the file lives in, the
//! file name inside it, the table name inside the file, and the key prefix and
//! suffix the generated keys carry. The sink adds the write-safety knobs redb
//! itself exposes.

use std::path::PathBuf;

use serde::Deserialize;

use saci_connector::ConfigValue;
use saci_core::error::SaciError;

/// The file name used when the config names none.
fn default_file() -> String {
    "saci.redb".to_string()
}

/// The table name used when the config names none.
fn default_table() -> String {
    "records".to_string()
}

/// `true`, for the three write-safety knobs and `compact`.
fn default_true() -> bool {
    true
}

/// How hard a commit tries before it reports success.
///
/// Maps onto [`redb::Durability`], which carries exactly these two levels.
#[derive(Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityMode {
    /// The commit has reached the disk when it returns. The default: an entry
    /// this sink reported as written survives a crash.
    #[default]
    Immediate,
    /// The commit does not reach the disk until a later `Immediate` one does.
    /// [`RedbSink::finish`](crate::RedbSink) runs that later commit itself, so
    /// this trades per-batch durability for throughput within one run, not
    /// durability of the finished file.
    None,
}

impl From<DurabilityMode> for redb::Durability {
    fn from(mode: DurabilityMode) -> Self {
        match mode {
            DurabilityMode::Immediate => redb::Durability::Immediate,
            DurabilityMode::None => redb::Durability::None,
        }
    }
}

/// Configuration for [`RedbSource`](crate::RedbSource).
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct RedbSourceConfig {
    /// Directory the redb file lives in. Required.
    pub directory: PathBuf,
    /// File name inside `directory`.
    #[serde(default = "default_file")]
    pub file: String,
    /// Table name inside the file.
    #[serde(default = "default_table")]
    pub table: String,
    /// Only entries whose key starts with this are read. Empty reads the whole
    /// table.
    #[serde(default)]
    pub key_prefix: String,
    /// Only entries whose key ends with this are read.
    #[serde(default)]
    pub key_suffix: String,
    /// Walk the whole file at open and refuse a corrupted one. Off by default:
    /// the check costs a full pass every time, and with the sink's
    /// write-safety knobs on there is nothing to repair.
    ///
    /// It has a second job on a source. A read-only open never repairs, so a
    /// file a sink left without an allocator state table (both
    /// `quick_repair` and `two_phase_commit` off, and the process killed
    /// before `finish`) is refused; turning this on opens the file
    /// read-write, which repairs it. That open takes an exclusive lock for
    /// the check's duration and needs a writable file.
    #[serde(default)]
    pub check_integrity: bool,
    /// Page cache budget in bytes. `None` leaves redb's own default.
    #[serde(default)]
    pub cache_size_bytes: Option<usize>,
    /// Delete the entries this instance yielded at
    /// [`Source::finish`](saci_core::io::source::Source::finish). Off by
    /// default: a source is a reader, and a second run over the same file
    /// reads the same entries.
    ///
    /// With it on, the file is a queue rather than a table: what one run
    /// handed over is gone once that run finished, and an instance dropped
    /// without `finish` deletes nothing, so the entries are delivered again.
    /// Deleting needs a read-write open, which is exclusive, so `finish`
    /// takes the file for the duration of one delete transaction.
    #[serde(default)]
    pub consume: bool,
    /// Declared Arrow schema. Required: `Source::schema()` is read at load
    /// time by `validate_workflow_graph`, before the file is opened.
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub schema_fields: Vec<ConfigValue>,
}

/// Configuration for [`RedbSink`](crate::RedbSink).
#[derive(Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct RedbSinkConfig {
    /// Directory the redb file lives in, created if absent. Required.
    pub directory: PathBuf,
    /// File name inside `directory`.
    #[serde(default = "default_file")]
    pub file: String,
    /// Table name inside the file.
    #[serde(default = "default_table")]
    pub table: String,
    /// Prepended to every generated key.
    #[serde(default)]
    pub key_prefix: String,
    /// Appended to every generated key, `.csv` style. The format is never
    /// inferred, so nothing is appended by default.
    #[serde(default)]
    pub key_suffix: String,
    /// Walk the whole file at open and refuse a corrupted one. Off by default,
    /// for the reason on [`RedbSourceConfig::check_integrity`].
    #[serde(default)]
    pub check_integrity: bool,
    /// Page cache budget in bytes. `None` leaves redb's own default.
    #[serde(default)]
    pub cache_size_bytes: Option<usize>,
    /// Reclaim the file's free space at `finish`. On by default: a run that
    /// rewrote entries leaves the file larger than its contents.
    #[serde(default = "default_true")]
    pub compact: bool,
    /// How hard each commit tries before it reports success.
    #[serde(default)]
    pub durability: DurabilityMode,
    /// Two-phase commit. On by default: it costs one extra fsync per commit
    /// and leaves the file in a committed state at every instant.
    #[serde(default = "default_true")]
    pub two_phase_commit: bool,
    /// Quick repair. On by default: commits carry page checksums, so recovery
    /// after a crash is instant instead of a full-file walk.
    #[serde(default = "default_true")]
    pub quick_repair: bool,
    /// Declared Arrow schema. Required, whatever the transformer: it is the
    /// schema the rows are written with.
    #[serde(default, deserialize_with = "saci_connector::one_or_many")]
    pub schema_fields: Vec<ConfigValue>,
}

/// Shared validation for the keys both halves declare.
///
/// `what` is the connector type name, so the message names the node's own
/// `type` string.
fn validate_common(
    what: &str,
    file: &str,
    table: &str,
    cache_size_bytes: Option<usize>,
) -> Result<(), SaciError> {
    if file.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: 'file' must not be empty"
        )));
    }
    if table.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: 'table' must not be empty"
        )));
    }
    if cache_size_bytes == Some(0) {
        return Err(SaciError::configuration(format!(
            "{what}: 'cache_size_bytes' must be greater than zero"
        )));
    }
    Ok(())
}

impl RedbSourceConfig {
    /// Reject a config that parsed but cannot open a file.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `file` or `table` is empty, or
    /// when `cache_size_bytes` is zero.
    pub fn validate(&self, what: &str) -> Result<(), SaciError> {
        validate_common(what, &self.file, &self.table, self.cache_size_bytes)
    }

    /// The full path of the redb file this config names.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.directory.join(&self.file)
    }
}

impl RedbSinkConfig {
    /// Reject a config that parsed but cannot open a file.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Configuration`] when `file` or `table` is empty, or
    /// when `cache_size_bytes` is zero.
    pub fn validate(&self, what: &str) -> Result<(), SaciError> {
        validate_common(what, &self.file, &self.table, self.cache_size_bytes)
    }

    /// The full path of the redb file this config names.
    #[must_use]
    pub fn path(&self) -> PathBuf {
        self.directory.join(&self.file)
    }
}

#[cfg(test)]
mod tests {
    use saci_connector::from_kdl_str;

    use super::*;

    fn sink(raw: &str) -> RedbSinkConfig {
        RedbSinkConfig::deserialize(from_kdl_str(raw).expect("parse test config"))
            .expect("deserialize sink config")
    }

    #[test]
    fn the_sink_defaults_are_the_safe_ones() {
        let cfg = sink(r#"directory "/tmp/x""#);
        assert_eq!(cfg.file, "saci.redb");
        assert_eq!(cfg.table, "records");
        assert_eq!(cfg.key_prefix, "");
        assert_eq!(cfg.key_suffix, "");
        assert!(cfg.compact);
        assert_eq!(cfg.durability, DurabilityMode::Immediate);
        assert!(cfg.two_phase_commit);
        assert!(cfg.quick_repair);
        assert!(!cfg.check_integrity);
        assert_eq!(cfg.cache_size_bytes, None);
    }

    #[test]
    fn an_empty_file_or_table_name_is_refused() {
        let cfg = sink(
            r#"directory "/tmp/x"
file ""
"#,
        );
        let Err(err) = cfg.validate("RedbSink") else {
            panic!("an empty file name must fail");
        };
        assert_eq!(err.category(), "configuration");
        assert_eq!(err.message(), "RedbSink: 'file' must not be empty");

        let cfg = sink(
            r#"directory "/tmp/x"
table ""
"#,
        );
        assert_eq!(
            cfg.validate("RedbSink").unwrap_err().message(),
            "RedbSink: 'table' must not be empty"
        );
    }

    #[test]
    fn a_zero_cache_budget_is_refused() {
        let cfg = sink(
            r#"directory "/tmp/x"
cache_size_bytes 0
"#,
        );
        assert_eq!(
            cfg.validate("RedbSink").unwrap_err().message(),
            "RedbSink: 'cache_size_bytes' must be greater than zero"
        );
    }

    #[test]
    fn the_path_joins_the_directory_and_the_file() {
        let cfg = sink(
            r#"directory "/tmp/x"
file "orders.redb"
"#,
        );
        assert_eq!(cfg.path(), PathBuf::from("/tmp/x").join("orders.redb"));
    }
}
