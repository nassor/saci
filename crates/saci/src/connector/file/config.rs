//! The `config` tables of the file source and sink.

use std::path::PathBuf;

use serde::Deserialize;

/// The `config` table of a [`FileSource`](crate::connector::file::FileSource).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSourceConfig {
    /// The file to read.
    pub path: PathBuf,
}

impl FileSourceConfig {
    /// A config reading `path`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

/// The `config` table of a [`FileSink`](crate::connector::file::FileSink).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileSinkConfig {
    /// The file to write.
    pub path: PathBuf,
    /// Replace the file rather than append to it. The footer formats,
    /// `parquet` and `avro`, cannot be appended to a non-empty file and stay
    /// readable, so they want this or a path of their own. Default `false`.
    #[serde(default)]
    pub truncate: bool,
}

impl FileSinkConfig {
    /// A config appending to `path`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            truncate: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::from_kdl_str;
    use crate::connector::schema::deserialize_config;

    #[test]
    fn a_source_table_needs_a_path() {
        let table = from_kdl_str("path \"/data/in.csv\"\nfield \"v\" type=\"int64\"\n").unwrap();
        let cfg: FileSourceConfig = deserialize_config(&table, "FileSource").unwrap();
        assert_eq!(cfg, FileSourceConfig::new("/data/in.csv"));

        let empty = from_kdl_str("field \"v\" type=\"int64\"\n").unwrap();
        let err = deserialize_config::<FileSourceConfig>(&empty, "FileSource").unwrap_err();
        assert_eq!(err.category(), "configuration");
        assert!(err.to_string().contains("path"), "got: {err}");
    }

    #[test]
    fn a_sink_table_defaults_to_appending() {
        let table = from_kdl_str("path \"/data/out.ndjson\"\n").unwrap();
        let cfg: FileSinkConfig = deserialize_config(&table, "FileSink").unwrap();
        assert_eq!(cfg, FileSinkConfig::new("/data/out.ndjson"));

        let truncating = from_kdl_str("path \"/data/out.parquet\"\ntruncate #true\n").unwrap();
        let cfg: FileSinkConfig = deserialize_config(&truncating, "FileSink").unwrap();
        assert!(cfg.truncate);
    }

    #[test]
    fn a_wrong_typed_truncate_names_the_key() {
        let table = from_kdl_str("path \"/data/out\"\ntruncate \"yes\"\n").unwrap();
        let err = deserialize_config::<FileSinkConfig>(&table, "FileSink").unwrap_err();
        assert!(err.to_string().contains("truncate"), "got: {err}");
    }
}
