//! The `store` block.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Persistent store declaration, tagged by `store`.
///
/// ```kdl
/// store "redb" {
///     path "/var/lib/saci/state.redb"
///     batch_resume #true
/// }
/// ```
///
/// With a `store` block present, the pipeline config is persisted to the
/// store before the pipeline builds and stream-mode resume state (processor
/// priors and source cursors) is written back as work progresses.
/// Interval/one-shot batch state carry is opt-in per store via
/// `batch_resume`.
///
/// This is standalone-mode persistence only: the file is local and
/// unreplicated. Cluster mode takes no `store` block, because its
/// application state is the raft-replicated `cluster-app.redb` under
/// `node.data_dir`.
#[derive(Debug, Clone)]
pub enum StoreConfig {
    /// A local embedded redb file.
    Redb {
        /// Path to the redb file. Created on first use.
        path: PathBuf,
        /// Opt-in interval/one-shot processor state carry across iterations.
        batch_resume: bool,
    },
}

impl Serialize for StoreConfig {
    /// The inverse of [`Deserialize`]: the kind is the `id` entry, which is
    /// the leading argument when written as KDL (`store "redb" { path .. }`).
    /// `batch_resume` is left out at its default, false.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap as _;

        match self {
            Self::Redb { path, batch_resume } => {
                let mut table = serializer.serialize_map(Some(2 + usize::from(*batch_resume)))?;
                table.serialize_entry("id", "redb")?;
                table.serialize_entry("path", path)?;
                if *batch_resume {
                    table.serialize_entry("batch_resume", batch_resume)?;
                }
                table.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for StoreConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // The KDL parser stores a node's leading argument under `id` (see
        // `crate::config` rules), so the tag arrives as an `id` entry rather than
        // a `store` key. Read it first, then validate the remaining keys
        // against the variant's shape.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct RedbInner {
            id: String,
            path: PathBuf,
            #[serde(default)]
            batch_resume: bool,
        }

        let inner = RedbInner::deserialize(deserializer)?;
        if inner.id != "redb" {
            return Err(serde::de::Error::custom(format!(
                "unknown store kind '{}' (expected \"redb\")",
                inner.id
            )));
        }
        Ok(StoreConfig::Redb {
            path: inner.path,
            batch_resume: inner.batch_resume,
        })
    }
}

#[cfg(all(test, feature = "service"))]
mod tests {
    use super::*;
    use crate::service::config::test_support::{TRIVIAL_WORKFLOW, parse};

    /// A `store "redb"` block parses into [`StoreConfig::Redb`] with
    /// `batch_resume` defaulting to false.
    #[test]
    fn test_store_redb_parses_and_validates() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

store "redb" {{
    path "/var/lib/saci/state.redb"
}}
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse");
        match &cfg.store {
            Some(StoreConfig::Redb { path, batch_resume }) => {
                assert_eq!(path, &PathBuf::from("/var/lib/saci/state.redb"));
                assert!(!batch_resume, "batch resume is opt-in, default false");
            }
            other => panic!("expected a redb store config, got {other:?}"),
        }
        cfg.validate()
            .expect("a well-formed redb store should validate");
    }

    /// `batch_resume` is read off the block when declared.
    #[test]
    fn test_store_redb_batch_resume_opt_in() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

store "redb" {{
    path "state.redb"
    batch_resume #true
}}
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("parse");
        assert!(matches!(
            cfg.store,
            Some(StoreConfig::Redb {
                batch_resume: true,
                ..
            })
        ));
    }

    /// Only `redb` is a known store kind, so any other tag is a parse error
    /// rather than a store the service silently ignores.
    #[test]
    fn test_store_unknown_kind_is_a_parse_error() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

store "sqlite" {{
    path "state.redb"
}}
{TRIVIAL_WORKFLOW}
"#
        );
        let err = parse(&raw).expect_err("an unknown store kind must not parse");
        assert!(
            err.contains("unknown store kind"),
            "error should name the kind: {err}"
        );
    }

    /// `path` is required: a redb store with no file has nowhere to persist.
    #[test]
    fn test_store_redb_requires_path() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

store "redb" {{
    batch_resume #true
}}
{TRIVIAL_WORKFLOW}
"#
        );
        let err = parse(&raw).expect_err("a redb store without a path must not parse");
        assert!(err.contains("path"), "error should name the field: {err}");
    }

    /// A declared but empty `path` parses and is caught by `validate`, which
    /// is the only place that can tell an empty string from an absent key.
    #[test]
    fn test_store_redb_rejects_empty_path() {
        let raw = format!(
            r#"
mode "standalone"

node id=1 data_dir="/tmp/saci"

store "redb" {{
    path ""
}}
{TRIVIAL_WORKFLOW}
"#
        );
        let cfg = parse(&raw).expect("an empty path still parses");
        let err = cfg
            .validate()
            .expect_err("an empty store path must not validate");
        assert!(
            err.to_string()
                .contains("store redb: path must not be empty"),
            "error should name the empty path: {err}"
        );
    }
}
