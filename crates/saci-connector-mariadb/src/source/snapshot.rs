//! The consistent initial snapshot of a `cdc_binlog` source with
//! `snapshot "initial"`.
//!
//! Reads every existing row of the table once as an `R` row, from one
//! repeatable-read transaction opened at a known binlog position, then hands the
//! stream that position so no change is missed and none is read twice.
//!
//! # Sequence
//!
//! On a connection of its own:
//!
//! 1. `FLUSH TABLES WITH READ LOCK`, so no commit can land between the next two
//!    steps;
//! 2. read the binlog position (file, position and, on MySQL, the executed GTID
//!    set, all from one statement);
//! 3. `SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ` and
//!    `START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY`, which pins the
//!    read view at that position;
//! 4. `UNLOCK TABLES`. The transaction stays open.
//!
//! The lock is held for a few statements only. The transaction then reads every
//! row as of the pinned position while writers carry on, and the stream starts
//! at the same position, so a change committed during the snapshot arrives from
//! the stream and not twice.
//!
//! # Paging
//!
//! The table is paged by its primary key, so every key column must be a declared
//! field. A composite key uses the expanded predicate
//! `(k1 > ?) OR (k1 = ? AND k2 > ?) ...` and not the row constructor
//! `(k1, k2) > (?, ?)`: both servers turn the first into a range scan of the
//! key and the second into a scan of the whole index, which makes every page of
//! a large table cost as much as the first. Decimal and floating point columns
//! cannot be keys, because the previous page's last value is bound back as text
//! or a float and would compare differently from the stored value.
//!
//! Rows decode through [`Cell::from_protocol`](crate::values::Cell::from_protocol),
//! because the snapshot reads over the binary protocol, and are validated with
//! `types::Access::Binlog`, the stricter of the two row sources, so a column the
//! stream could not carry fails here and not when the stream starts. Progress is
//! held in memory only: a source dropped mid-snapshot snapshots again from the
//! start, which `upsert` and `apply_changes` absorb.

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params, Row, Value};
use saci_core::error::SaciError;

use super::binlog::{Status, grant_hint, read_status};
use crate::connection::{
    ER_ACCESS_DENIED_ERROR, ER_SPECIFIC_ACCESS_DENIED_ERROR, Flavor, Session, TableRef,
    mysql_detail, quote_columns, quote_ident, server_code,
};
use crate::types::{Family, ResolvedColumn};

/// What the snapshot reads and how it pages.
#[derive(Debug, Clone)]
pub(super) struct SnapshotSpec {
    /// The table, with the names the server reports.
    pub(super) table: TableRef,
    /// The declared data columns in `SELECT` order.
    pub(super) columns: Vec<String>,
    /// Indexes into `columns` of the primary key, in key order.
    pub(super) key: Vec<usize>,
}

/// Work out which of the selected columns form the primary key.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when the table has no primary key, when
/// a key column is not declared, or when one is a decimal, float or double.
pub(super) fn plan_keys(
    what: &str,
    table: &TableRef,
    primary_key: &[String],
    data: &[ResolvedColumn],
) -> Result<Vec<usize>, SaciError> {
    if primary_key.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: snapshot \"initial\" needs '{table}' to have a primary key"
        )));
    }
    let mut key = Vec::with_capacity(primary_key.len());
    let mut missing = Vec::new();
    for name in primary_key {
        match data
            .iter()
            .position(|column| column.spec.name.eq_ignore_ascii_case(name))
        {
            Some(index) => key.push(index),
            None => missing.push(format!("'{name}'")),
        }
    }
    if !missing.is_empty() {
        return Err(SaciError::configuration(format!(
            "{what}: snapshot \"initial\" pages '{table}' by its primary key, so every key \
             column must be declared in schema_fields (missing: {})",
            missing.join(", ")
        )));
    }
    for &index in &key {
        let column = &data[index];
        if matches!(column.family, Family::Float | Family::Decimal) {
            return Err(SaciError::configuration(format!(
                "{what}: snapshot \"initial\" pages '{table}' by its primary key, so key column \
                 '{}' must not be decimal, float or double",
                column.spec.name
            )));
        }
    }
    Ok(key)
}

/// The `WHERE` predicate selecting the rows after a key, as `(k1 > ?) OR
/// (k1 = ? AND k2 > ?) ...`.
fn after_predicate(keys: &[String]) -> String {
    let mut terms = Vec::with_capacity(keys.len());
    for (i, key) in keys.iter().enumerate() {
        let mut term: Vec<String> = keys[..i]
            .iter()
            .map(|earlier| format!("{} = ?", quote_ident(earlier)))
            .collect();
        term.push(format!("{} > ?", quote_ident(key)));
        terms.push(format!("({})", term.join(" AND ")));
    }
    terms.join(" OR ")
}

impl SnapshotSpec {
    fn key_names(&self) -> Vec<String> {
        self.key
            .iter()
            .map(|&index| self.columns[index].clone())
            .collect()
    }

    /// One page of the table: the first when `after` is false, else the rows
    /// after the previous page's last key.
    fn page_sql(&self, after: bool, limit: usize) -> String {
        let keys = self.key_names();
        let mut sql = format!(
            "SELECT {} FROM {}",
            quote_columns(self.columns.iter().map(String::as_str)),
            self.table.quoted()
        );
        if after {
            sql.push_str(" WHERE ");
            sql.push_str(&after_predicate(&keys));
        }
        sql.push_str(" ORDER BY ");
        sql.push_str(&quote_columns(keys.iter().map(String::as_str)));
        sql.push_str(&format!(" LIMIT {limit}"));
        sql
    }
}

/// The bind values of the `after` predicate for the last key `last`: the
/// earlier keys are repeated for each term.
fn after_params(last: &[Value]) -> Vec<Value> {
    let mut params = Vec::with_capacity(last.len() * (last.len() + 1) / 2);
    for i in 0..last.len() {
        params.extend(last[..=i].iter().cloned());
    }
    params
}

/// An open snapshot: the transaction, and where it left off.
pub(super) struct Snapshot {
    conn: Conn,
    spec: SnapshotSpec,
    /// The binlog position the read view is pinned to.
    pub(super) status: Status,
    /// When the snapshot began, in microseconds since the Unix epoch.
    pub(super) started_micros: i64,
    /// The key of the last row handed out, absent before the first page.
    last_key: Option<Vec<Value>>,
}

impl Snapshot {
    /// Pin a read view and the binlog position it matches.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when a statement fails, naming the
    /// privilege `FLUSH TABLES WITH READ LOCK` needs when that is the cause.
    pub(super) async fn open(
        session: Session,
        what: &str,
        target: &str,
        spec: SnapshotSpec,
    ) -> Result<Self, SaciError> {
        let Session {
            mut conn, flavor, ..
        } = session;
        match pin(&mut conn, flavor, what, target).await {
            Ok(status) => Ok(Self {
                conn,
                spec,
                status,
                started_micros: now_micros(),
                last_key: None,
            }),
            Err(e) => {
                // Dropping the connection releases the lock too; this just
                // does not leave it to the driver's background cleanup.
                let _ = conn.query_drop("UNLOCK TABLES").await;
                Err(e)
            }
        }
    }

    /// The next page of at most `limit` rows. Fewer than `limit` rows is the
    /// last page.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] when the query fails.
    pub(super) async fn next_page(
        &mut self,
        what: &str,
        limit: usize,
    ) -> Result<Vec<Row>, SaciError> {
        let limit = limit.max(1);
        let (sql, params) = match &self.last_key {
            None => (self.spec.page_sql(false, limit), Params::Empty),
            Some(last) => (
                self.spec.page_sql(true, limit),
                Params::Positional(after_params(last)),
            ),
        };
        let rows: Vec<Row> = self.conn.exec(sql, params).await.map_err(|e| {
            SaciError::generic(format!(
                "{what}: cannot read a snapshot page of '{}': {}",
                self.spec.table,
                mysql_detail(&e)
            ))
        })?;
        if let Some(last) = rows.last() {
            let mut key = Vec::with_capacity(self.spec.key.len());
            for &index in &self.spec.key {
                let value = last.as_ref(index).cloned().ok_or_else(|| {
                    SaciError::generic(format!(
                        "{what}: a snapshot row of '{}' has no value for key column '{}'",
                        self.spec.table, self.spec.columns[index]
                    ))
                })?;
                key.push(value);
            }
            self.last_key = Some(key);
        }
        Ok(rows)
    }

    /// End the transaction and the connection.
    pub(super) async fn close(mut self) {
        let _ = self.conn.query_drop("COMMIT").await;
        let _ = self.conn.disconnect().await;
    }
}

/// The lock, position, view, unlock sequence.
async fn pin(
    conn: &mut Conn,
    flavor: Flavor,
    what: &str,
    target: &str,
) -> Result<Status, SaciError> {
    conn.query_drop("FLUSH TABLES WITH READ LOCK")
        .await
        .map_err(|e| {
            let hint = match server_code(&e) {
                Some(ER_SPECIFIC_ACCESS_DENIED_ERROR | ER_ACCESS_DENIED_ERROR) => {
                    "; snapshot \"initial\" needs the RELOAD privilege for a brief global read lock"
                }
                _ => "",
            };
            SaciError::generic(format!(
                "{what}: cannot lock {target} for the snapshot: {}{hint}",
                mysql_detail(&e)
            ))
        })?;
    let status = read_status(conn, flavor, what, target).await?;
    for statement in [
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
        "UNLOCK TABLES",
    ] {
        conn.query_drop(statement).await.map_err(|e| {
            SaciError::generic(format!(
                "{what}: snapshot statement '{statement}' failed on {target}: {}{}",
                mysql_detail(&e),
                grant_hint(flavor, server_code(&e))
            ))
        })?;
    }
    Ok(status)
}

/// Microseconds since the Unix epoch.
fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_micros()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{FieldSpec, MariadbFieldType};

    fn table() -> TableRef {
        TableRef {
            schema: "shop".to_string(),
            name: "orders".to_string(),
        }
    }

    fn resolved(name: &str, ty: MariadbFieldType, family: Family) -> ResolvedColumn {
        let mut column = ResolvedColumn::synthetic(&FieldSpec {
            name: name.to_string(),
            ty,
            nullable: true,
            precision: None,
            scale: None,
            mariadb_type: None,
        });
        column.family = family;
        column
    }

    fn spec(columns: &[&str], key: &[usize]) -> SnapshotSpec {
        SnapshotSpec {
            table: table(),
            columns: columns.iter().map(|c| (*c).to_string()).collect(),
            key: key.to_vec(),
        }
    }

    #[test]
    fn the_first_page_has_no_predicate() {
        assert_eq!(
            spec(&["id", "total"], &[0]).page_sql(false, 100),
            "SELECT `id`, `total` FROM `shop`.`orders` ORDER BY `id` LIMIT 100"
        );
    }

    #[test]
    fn a_single_key_pages_with_a_plain_comparison() {
        assert_eq!(
            spec(&["id", "total"], &[0]).page_sql(true, 5),
            "SELECT `id`, `total` FROM `shop`.`orders` WHERE (`id` > ?) ORDER BY `id` LIMIT 5"
        );
    }

    #[test]
    fn a_composite_key_pages_with_the_expanded_predicate_never_a_row_constructor() {
        let sql = spec(&["a", "x", "b", "c"], &[0, 2, 3]).page_sql(true, 7);
        assert_eq!(
            sql,
            "SELECT `a`, `x`, `b`, `c` FROM `shop`.`orders` WHERE (`a` > ?) OR (`a` = ? AND \
             `b` > ?) OR (`a` = ? AND `b` = ? AND `c` > ?) ORDER BY `a`, `b`, `c` LIMIT 7"
        );
        assert!(!sql.contains("(`a`, `b`"));
    }

    #[test]
    fn the_bind_values_repeat_the_earlier_keys_for_each_term() {
        let last = [Value::Int(1), Value::Int(2), Value::Int(3)];
        assert_eq!(
            after_params(&last),
            vec![
                Value::Int(1),
                Value::Int(1),
                Value::Int(2),
                Value::Int(1),
                Value::Int(2),
                Value::Int(3)
            ]
        );
        assert_eq!(after_params(&[Value::Int(9)]), vec![Value::Int(9)]);
    }

    #[test]
    fn a_key_identifier_with_a_backtick_is_quoted() {
        assert_eq!(after_predicate(&["a`b".to_string()]), "(`a``b` > ?)");
    }

    #[test]
    fn keys_resolve_to_their_select_positions_whatever_the_case() {
        let data = [
            resolved("note", MariadbFieldType::Utf8, Family::Text),
            resolved("ID", MariadbFieldType::Int64, Family::Integer(64)),
        ];
        let key = plan_keys("MariadbSource", &table(), &["id".to_string()], &data).expect("key");
        assert_eq!(key, vec![1]);
    }

    #[test]
    fn a_table_with_no_primary_key_is_refused() {
        let err = plan_keys("MariadbSource", &table(), &[], &[]).expect_err("refused");
        assert_eq!(
            err.message(),
            "MariadbSource: snapshot \"initial\" needs 'shop.orders' to \
             have a primary key"
        );
    }

    #[test]
    fn an_undeclared_key_column_is_named() {
        let data = [resolved("note", MariadbFieldType::Utf8, Family::Text)];
        let err = plan_keys(
            "MariadbSource",
            &table(),
            &["id".to_string(), "region".to_string()],
            &data,
        )
        .expect_err("refused");
        let text = err.message();
        assert!(
            text.contains(
                "every key column must be declared in schema_fields (missing: 'id', 'region')"
            ),
            "{text}"
        );
    }

    #[test]
    fn a_decimal_float_or_double_key_is_refused() {
        for (family, ty) in [
            (Family::Decimal, MariadbFieldType::Utf8),
            (Family::Float, MariadbFieldType::Float64),
        ] {
            let data = [resolved("k", ty, family)];
            let err = plan_keys("MariadbSource", &table(), &["k".to_string()], &data)
                .expect_err("refused");
            assert!(
                err.message()
                    .contains("key column 'k' must not be decimal, float or double"),
                "{}",
                err.message()
            );
        }
    }
}
