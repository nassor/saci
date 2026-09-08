//! Shared helpers for the connector's integration tests.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use tempfile::TempDir;

/// A temporary directory and the database file inside it.
pub struct TestDb {
    _dir: TempDir,
    /// The database file's path.
    pub path: PathBuf,
}

/// A fresh database path in a directory that is removed on drop.
pub fn temp_db() -> TestDb {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("saci-turso.db");
    TestDb { _dir: dir, path }
}

impl TestDb {
    /// The path as a string, for a config.
    pub fn path_str(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }
}

/// A KDL-safe spelling of a path.
///
/// A quoted KDL string reads backslashes as escapes, so a Windows path goes in
/// with forward slashes.
pub fn kdl_path(path: &str) -> String {
    path.replace('\\', "/")
}

/// Deserialise a connector config from a KDL fragment.
pub fn config_from_kdl<T: serde::de::DeserializeOwned>(body: &str) -> T {
    let value = saci_connector::from_kdl_str(body).expect("kdl parses");
    serde_json::from_value(value).unwrap_or_else(|e| panic!("config deserializes: {e}\n{body}"))
}

/// Open a raw connection to the database.
pub async fn connect(path: &Path) -> turso::Connection {
    let db = turso::Builder::new_local(path.to_str().expect("utf8 path"))
        .build()
        .await
        .expect("open the database");
    db.connect().expect("connect")
}

/// Run one statement, panicking on failure.
pub async fn exec(conn: &turso::Connection, sql: &str) {
    conn.execute(sql, ())
        .await
        .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
}

/// Read one integer from a query.
pub async fn scalar_i64(conn: &turso::Connection, sql: &str) -> i64 {
    let mut rows = conn.query(sql, ()).await.expect("query");
    let row = rows.next().await.expect("step").expect("one row");
    match row.get_value(0).expect("value") {
        turso::Value::Integer(i) => i,
        other => panic!("expected an integer, got {other:?}"),
    }
}

/// Read one text value from a query.
pub async fn scalar_text(conn: &turso::Connection, sql: &str) -> String {
    let mut rows = conn.query(sql, ()).await.expect("query");
    let row = rows.next().await.expect("step").expect("one row");
    match row.get_value(0).expect("value") {
        turso::Value::Text(text) => text,
        other => panic!("expected text, got {other:?}"),
    }
}

/// Run a source to EOF, returning the number of rows seen.
pub async fn drain(source: &mut dyn saci_core::io::source::Source) -> usize {
    let mut total = 0;
    while let Some(batch) = source.next_batch().await.expect("next_batch") {
        total += batch.num_rows();
    }
    total
}
