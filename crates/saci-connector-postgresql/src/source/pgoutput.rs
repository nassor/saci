//! The `pgoutput` protocol version 1 message set.
//!
//! The bytes come from the `data` column of
//! `pg_logical_slot_peek_binary_changes`, which is byte-identical to what a
//! walsender would send: `pgoutput` has no walsender awareness, and the output
//! plugin is driven the same way through either interface.
//!
//! `proto_version` is pinned to `1` and `streaming`, `two_phase`, `messages`
//! and `binary` are never requested, so the message set here is complete
//! rather than a subset, and every present tuple value is a `'t'` field
//! carrying the type's output-function text, which
//! [`ColumnBuilder::push_text`](crate::values::ColumnBuilder::push_text)
//! parses. The cursor path reaches the same output-function text a different
//! way -- it casts server-side to `text`, so the value arrives as an ordinary
//! binary `text` column and goes through `push` instead. Both lean on
//! PostgreSQL's own output function; neither shares the other's decoder.
//!
//! # Relation caching
//!
//! Every `peek`/`get` call builds a fresh `LogicalDecodingContext` and destroys
//! it before returning, and `pgoutput` ties its relation cache to that context's
//! memory. So a `Relation` message arrives again on every call, and repeatedly
//! for the same relation id within a call. [`Decoder::reset`] must run before
//! each call's messages, and a repeated `Relation` is a normal event, not a
//! protocol error.
//!
//! All integers are big-endian; strings are NUL-terminated.

use std::collections::HashMap;

use saci_core::error::SaciError;

/// One column of a `Relation` message.
#[derive(Debug, Clone)]
pub(crate) struct RelationColumn {
    /// Column name, matched against declared fields by name.
    pub(crate) name: String,
    /// PostgreSQL type OID, checked with [`crate::types::resolve_wire`].
    pub(crate) type_oid: u32,
}

/// A relation the publication decodes.
#[derive(Debug, Clone)]
pub(crate) struct Relation {
    /// Schema name.
    pub(crate) namespace: String,
    /// Table name.
    pub(crate) name: String,
    /// Columns in wire order, which is the order every tuple follows.
    pub(crate) columns: Vec<RelationColumn>,
}

impl Relation {
    /// `schema.table`.
    pub(crate) fn qualified(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }
}

/// One column of a tuple.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TupleValue<'a> {
    /// `'n'`: SQL NULL.
    Null,
    /// `'u'`: an unchanged out-of-line (TOAST) value the server did not resend.
    Unchanged,
    /// `'b'`: the type's binary send format.
    Binary(&'a [u8]),
    /// `'t'`: the type's text output format.
    Text(&'a [u8]),
}

/// The operation a change carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operation {
    /// `INSERT`.
    Insert,
    /// `UPDATE`; only the new tuple is emitted.
    Update,
    /// `DELETE`; the tuple holds the replica-identity columns.
    Delete,
}

impl Operation {
    /// The single-letter form the `__op` metadata column carries.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Operation::Insert => "I",
            Operation::Update => "U",
            Operation::Delete => "D",
        }
    }
}

/// One decoded `pgoutput` message.
#[derive(Debug)]
pub(crate) enum Message<'a> {
    /// `'B'`: transaction start, carrying the commit timestamp every change in
    /// the transaction reports.
    Begin {
        /// Microseconds since 2000-01-01, as the wire carries it.
        commit_ts: i64,
    },
    /// `'C'`: transaction end.
    ///
    /// `end_lsn` is the LSN just past the commit record, which is exactly what
    /// `pg_replication_slot_advance` takes to acknowledge the transaction.
    Commit {
        /// LSN just past the commit record, as the wire carries it.
        end_lsn: i64,
    },
    /// `'R'`: relation metadata, already stored in the decoder.
    Relation(u32),
    /// `'I'`, `'U'` or `'D'`.
    Change {
        /// Which relation the tuple belongs to.
        rel_id: u32,
        /// Insert, update or delete.
        operation: Operation,
        /// Column values in the relation's column order.
        tuple: Vec<TupleValue<'a>>,
    },
    /// `'T'`: `TRUNCATE`, which carries no rows.
    Truncate {
        /// How many relations the statement truncated.
        relations: usize,
    },
    /// `'Y'` type metadata and `'O'` replication origin: parsed and discarded.
    Metadata,
}

/// Stateful decoder holding the relation table for one slot call.
#[derive(Default)]
pub(crate) struct Decoder {
    relations: HashMap<u32, Relation>,
}

impl Decoder {
    /// Drop the relation table before a new slot call's messages.
    pub(crate) fn reset(&mut self) {
        self.relations.clear();
    }

    /// The relation a change refers to, if a `Relation` message declared it.
    pub(crate) fn relation(&self, rel_id: u32) -> Option<&Relation> {
        self.relations.get(&rel_id)
    }

    /// Decode one message.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] for an unknown message tag, an unknown
    /// tuple marker, a truncated buffer, or trailing bytes the message layout
    /// does not account for.
    pub(crate) fn decode<'a>(&mut self, raw: &'a [u8]) -> Result<Message<'a>, SaciError> {
        let mut reader = Reader::new(raw);
        let tag = reader.u8()?;
        let message = match tag {
            b'B' => {
                let _final_lsn = reader.i64()?;
                let commit_ts = reader.i64()?;
                let _xid = reader.i32()?;
                Message::Begin { commit_ts }
            }
            b'C' => {
                let _flags = reader.u8()?;
                let _commit_lsn = reader.i64()?;
                let end_lsn = reader.i64()?;
                let _commit_ts = reader.i64()?;
                Message::Commit { end_lsn }
            }
            b'R' => {
                let rel_id = reader.u32()?;
                let namespace = reader.cstring()?;
                let name = reader.cstring()?;
                let _replica_identity = reader.u8()?;
                let ncols = reader.i16()?;
                if ncols < 0 {
                    return Err(SaciError::generic(format!(
                        "pgoutput: Relation message for '{namespace}.{name}' declares {ncols} \
                         columns"
                    )));
                }
                let mut columns = Vec::with_capacity(ncols as usize);
                for _ in 0..ncols {
                    let _flags = reader.u8()?;
                    let column_name = reader.cstring()?;
                    let type_oid = reader.u32()?;
                    let _type_modifier = reader.i32()?;
                    columns.push(RelationColumn {
                        name: column_name,
                        type_oid,
                    });
                }
                // A repeated Relation for the same id is expected: the server
                // rebuilds its cache on every slot call.
                self.relations.insert(
                    rel_id,
                    Relation {
                        // An empty namespace means the relation is in the
                        // search path's default schema, which pgoutput reports
                        // as "public" for ordinary tables.
                        namespace: if namespace.is_empty() {
                            "public".to_string()
                        } else {
                            namespace
                        },
                        name,
                        columns,
                    },
                );
                Message::Relation(rel_id)
            }
            b'Y' => {
                let _type_id = reader.u32()?;
                let _namespace = reader.cstring()?;
                let _name = reader.cstring()?;
                Message::Metadata
            }
            b'O' => {
                let _commit_lsn = reader.i64()?;
                let _origin = reader.cstring()?;
                Message::Metadata
            }
            b'I' => {
                let rel_id = reader.u32()?;
                expect_marker(&mut reader, b'N', "Insert")?;
                let tuple = reader.tuple()?;
                Message::Change {
                    rel_id,
                    operation: Operation::Insert,
                    tuple,
                }
            }
            b'U' => {
                let rel_id = reader.u32()?;
                let mut marker = reader.u8()?;
                if marker == b'K' || marker == b'O' {
                    // The old tuple. Only the new tuple is emitted, so its
                    // values are parsed and dropped.
                    let _old = reader.tuple()?;
                    marker = reader.u8()?;
                }
                if marker != b'N' {
                    return Err(SaciError::generic(format!(
                        "pgoutput: Update message has tuple marker {:?}, expected 'N'",
                        marker as char
                    )));
                }
                let tuple = reader.tuple()?;
                Message::Change {
                    rel_id,
                    operation: Operation::Update,
                    tuple,
                }
            }
            b'D' => {
                let rel_id = reader.u32()?;
                let marker = reader.u8()?;
                if marker != b'K' && marker != b'O' {
                    return Err(SaciError::generic(format!(
                        "pgoutput: Delete message has tuple marker {:?}, expected 'K' or 'O'",
                        marker as char
                    )));
                }
                let tuple = reader.tuple()?;
                Message::Change {
                    rel_id,
                    operation: Operation::Delete,
                    tuple,
                }
            }
            b'T' => {
                let nrels = reader.i32()?;
                let _flags = reader.u8()?;
                if nrels < 0 {
                    return Err(SaciError::generic(format!(
                        "pgoutput: Truncate message declares {nrels} relations"
                    )));
                }
                for _ in 0..nrels {
                    let _rel_id = reader.u32()?;
                }
                Message::Truncate {
                    relations: nrels as usize,
                }
            }
            other => {
                return Err(SaciError::generic(format!(
                    "pgoutput: unknown message tag {:?} (0x{other:02X}); this connector requests \
                     proto_version 1 without streaming, two-phase commit or logical messages",
                    other as char
                )));
            }
        };

        if !reader.is_empty() {
            return Err(SaciError::generic(format!(
                "pgoutput: message tag {:?} left {} undecoded byte(s)",
                tag as char,
                reader.remaining()
            )));
        }
        Ok(message)
    }
}

fn expect_marker(reader: &mut Reader<'_>, expected: u8, what: &str) -> Result<(), SaciError> {
    let marker = reader.u8()?;
    if marker != expected {
        return Err(SaciError::generic(format!(
            "pgoutput: {what} message has tuple marker {:?}, expected {:?}",
            marker as char, expected as char
        )));
    }
    Ok(())
}

/// Big-endian cursor over one message.
struct Reader<'a> {
    raw: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn new(raw: &'a [u8]) -> Self {
        Self { raw, at: 0 }
    }

    fn remaining(&self) -> usize {
        self.raw.len() - self.at
    }

    fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], SaciError> {
        let end = self.at.checked_add(n).ok_or_else(|| self.truncated(n))?;
        if end > self.raw.len() {
            return Err(self.truncated(n));
        }
        let slice = &self.raw[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn truncated(&self, wanted: usize) -> SaciError {
        SaciError::generic(format!(
            "pgoutput: message is truncated: wanted {wanted} byte(s) at offset {}, {} remain",
            self.at,
            self.remaining()
        ))
    }

    fn u8(&mut self) -> Result<u8, SaciError> {
        Ok(self.take(1)?[0])
    }

    fn i16(&mut self) -> Result<i16, SaciError> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().expect("two bytes"),
        ))
    }

    fn i32(&mut self) -> Result<i32, SaciError> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }

    fn u32(&mut self) -> Result<u32, SaciError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("four bytes"),
        ))
    }

    fn i64(&mut self) -> Result<i64, SaciError> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().expect("eight bytes"),
        ))
    }

    fn cstring(&mut self) -> Result<String, SaciError> {
        let start = self.at;
        let end = self.raw[start..]
            .iter()
            .position(|byte| *byte == 0)
            .map(|offset| start + offset)
            .ok_or_else(|| {
                SaciError::generic(format!("pgoutput: unterminated string at offset {start}"))
            })?;
        let text = std::str::from_utf8(&self.raw[start..end]).map_err(|e| {
            SaciError::generic(format!(
                "pgoutput: string at offset {start} is not valid UTF-8: {e}"
            ))
        })?;
        self.at = end + 1;
        Ok(text.to_string())
    }

    fn tuple(&mut self) -> Result<Vec<TupleValue<'a>>, SaciError> {
        let ncols = self.i16()?;
        if ncols < 0 {
            return Err(SaciError::generic(format!(
                "pgoutput: TupleData declares {ncols} columns"
            )));
        }
        let mut values = Vec::with_capacity(ncols as usize);
        for index in 0..ncols {
            let kind = self.u8()?;
            values.push(match kind {
                b'n' => TupleValue::Null,
                b'u' => TupleValue::Unchanged,
                b't' | b'b' => {
                    let len = self.i32()?;
                    if len < 0 {
                        return Err(SaciError::generic(format!(
                            "pgoutput: TupleData column {index} declares length {len}"
                        )));
                    }
                    let bytes = self.take(len as usize)?;
                    if kind == b'b' {
                        TupleValue::Binary(bytes)
                    } else {
                        TupleValue::Text(bytes)
                    }
                }
                other => {
                    return Err(SaciError::generic(format!(
                        "pgoutput: TupleData column {index} has marker {:?} (0x{other:02X})",
                        other as char
                    )));
                }
            });
        }
        Ok(values)
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    //! Byte builders so the decoder is tested against the wire layout rather
    //! than against itself.

    /// A `Begin` message with `commit_ts` in PostgreSQL epoch microseconds.
    pub(crate) fn begin(commit_ts: i64, xid: i32) -> Vec<u8> {
        let mut out = vec![b'B'];
        out.extend_from_slice(&0i64.to_be_bytes());
        out.extend_from_slice(&commit_ts.to_be_bytes());
        out.extend_from_slice(&xid.to_be_bytes());
        out
    }

    /// A `Commit` message whose `end_lsn` is `end_lsn`.
    pub(crate) fn commit(end_lsn: i64) -> Vec<u8> {
        let mut out = vec![b'C', 0];
        out.extend_from_slice(&0i64.to_be_bytes());
        out.extend_from_slice(&end_lsn.to_be_bytes());
        out.extend_from_slice(&0i64.to_be_bytes());
        out
    }

    /// A `Relation` message for `namespace.name` with `(column, oid)` columns.
    pub(crate) fn relation(
        rel_id: u32,
        namespace: &str,
        name: &str,
        columns: &[(&str, u32)],
    ) -> Vec<u8> {
        let mut out = vec![b'R'];
        out.extend_from_slice(&rel_id.to_be_bytes());
        out.extend_from_slice(namespace.as_bytes());
        out.push(0);
        out.extend_from_slice(name.as_bytes());
        out.push(0);
        out.push(b'd'); // replica identity default
        out.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for (column, oid) in columns {
            out.push(0); // flags
            out.extend_from_slice(column.as_bytes());
            out.push(0);
            out.extend_from_slice(&oid.to_be_bytes());
            out.extend_from_slice(&(-1i32).to_be_bytes());
        }
        out
    }

    /// A tuple body: `None` is `'n'`, `Some(bytes)` is `'b'`, and every index
    /// listed in `unchanged` is `'u'` -- the marker for an out-of-line TOAST
    /// value the server did not resend.
    pub(crate) fn tuple_with_unchanged(values: &[Option<&[u8]>], unchanged: &[usize]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(values.len() as i16).to_be_bytes());
        for (index, value) in values.iter().enumerate() {
            if unchanged.contains(&index) {
                out.push(b'u');
                continue;
            }
            match value {
                None => out.push(b'n'),
                Some(bytes) => {
                    out.push(b'b');
                    out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    out.extend_from_slice(bytes);
                }
            }
        }
        out
    }

    /// A tuple body with no unchanged column.
    pub(crate) fn tuple(values: &[Option<&[u8]>]) -> Vec<u8> {
        tuple_with_unchanged(values, &[])
    }

    /// An `Insert` whose column at `index` is the `'u'` unchanged marker.
    pub(crate) fn insert_unchanged(
        rel_id: u32,
        values: &[Option<&[u8]>],
        unchanged: &[usize],
    ) -> Vec<u8> {
        let mut out = vec![b'I'];
        out.extend_from_slice(&rel_id.to_be_bytes());
        out.push(b'N');
        out.extend_from_slice(&tuple_with_unchanged(values, unchanged));
        out
    }

    /// An `Insert` message.
    pub(crate) fn insert(rel_id: u32, values: &[Option<&[u8]>]) -> Vec<u8> {
        let mut out = vec![b'I'];
        out.extend_from_slice(&rel_id.to_be_bytes());
        out.push(b'N');
        out.extend_from_slice(&tuple(values));
        out
    }

    /// An `Update` message with an old key tuple followed by the new tuple.
    pub(crate) fn update(rel_id: u32, old: &[Option<&[u8]>], new: &[Option<&[u8]>]) -> Vec<u8> {
        let mut out = vec![b'U'];
        out.extend_from_slice(&rel_id.to_be_bytes());
        out.push(b'K');
        out.extend_from_slice(&tuple(old));
        out.push(b'N');
        out.extend_from_slice(&tuple(new));
        out
    }

    /// A `Delete` message carrying the replica-identity tuple.
    pub(crate) fn delete(rel_id: u32, key: &[Option<&[u8]>]) -> Vec<u8> {
        let mut out = vec![b'D'];
        out.extend_from_slice(&rel_id.to_be_bytes());
        out.push(b'K');
        out.extend_from_slice(&tuple(key));
        out
    }

    /// A `Truncate` message.
    pub(crate) fn truncate(rel_ids: &[u32]) -> Vec<u8> {
        let mut out = vec![b'T'];
        out.extend_from_slice(&(rel_ids.len() as i32).to_be_bytes());
        out.push(0);
        for rel_id in rel_ids {
            out.extend_from_slice(&rel_id.to_be_bytes());
        }
        out
    }

    /// An `Insert` whose single column is `'t'`-tagged text.
    pub(crate) fn insert_text(rel_id: u32, value: &str) -> Vec<u8> {
        let mut out = vec![b'I'];
        out.extend_from_slice(&rel_id.to_be_bytes());
        out.push(b'N');
        out.extend_from_slice(&1i16.to_be_bytes());
        out.push(b't');
        out.extend_from_slice(&(value.len() as i32).to_be_bytes());
        out.extend_from_slice(value.as_bytes());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;

    const OID_INT8: u32 = 20;
    const OID_NUMERIC: u32 = 1700;

    #[test]
    fn a_transaction_decodes_begin_relation_insert_commit() {
        let mut decoder = Decoder::default();

        let Message::Begin { commit_ts } = decoder.decode(&begin(1_700_000, 42)).unwrap() else {
            panic!("expected Begin");
        };
        assert_eq!(commit_ts, 1_700_000);

        let relation_bytes = relation(
            7,
            "public",
            "orders",
            &[("id", OID_INT8), ("amount", OID_NUMERIC)],
        );
        assert!(matches!(
            decoder.decode(&relation_bytes).unwrap(),
            Message::Relation(7)
        ));

        let relation = decoder.relation(7).expect("relation stored");
        assert_eq!(relation.qualified(), "public.orders");
        assert_eq!(relation.columns.len(), 2);
        assert_eq!(relation.columns[0].name, "id");
        assert_eq!(relation.columns[0].type_oid, OID_INT8);
        assert_eq!(relation.columns[1].type_oid, OID_NUMERIC);

        let id = 7i64.to_be_bytes();
        let insert_bytes = insert(7, &[Some(&id), None]);
        let message = decoder.decode(&insert_bytes).unwrap();
        let Message::Change {
            rel_id,
            operation,
            tuple,
        } = message
        else {
            panic!("expected Change");
        };
        assert_eq!(rel_id, 7);
        assert_eq!(operation, Operation::Insert);
        assert_eq!(tuple.len(), 2);
        assert!(matches!(tuple[0], TupleValue::Binary(bytes) if bytes == id));
        assert!(matches!(tuple[1], TupleValue::Null));

        let commit_bytes = commit(0x2A);
        assert!(matches!(
            decoder.decode(&commit_bytes).unwrap(),
            Message::Commit { end_lsn: 0x2A }
        ));
    }

    #[test]
    fn a_repeated_relation_for_the_same_id_is_accepted() {
        let mut decoder = Decoder::default();
        decoder
            .decode(&relation(7, "public", "orders", &[("id", OID_INT8)]))
            .unwrap();
        // The server rebuilds its cache per slot call, so the same id arrives
        // again; the second message must replace the first, not be rejected.
        decoder
            .decode(&relation(
                7,
                "public",
                "orders",
                &[("id", OID_INT8), ("extra", OID_INT8)],
            ))
            .unwrap();
        assert_eq!(decoder.relation(7).unwrap().columns.len(), 2);
    }

    #[test]
    fn an_update_emits_only_the_new_tuple() {
        let mut decoder = Decoder::default();
        decoder
            .decode(&relation(1, "public", "t", &[("id", OID_INT8)]))
            .unwrap();
        let old = 1i64.to_be_bytes();
        let new = 2i64.to_be_bytes();
        let update_bytes = update(1, &[Some(&old)], &[Some(&new)]);
        let Message::Change {
            operation, tuple, ..
        } = decoder.decode(&update_bytes).unwrap()
        else {
            panic!("expected Change");
        };
        assert_eq!(operation, Operation::Update);
        assert!(matches!(tuple[0], TupleValue::Binary(bytes) if bytes == new));
    }

    #[test]
    fn an_update_without_an_old_tuple_is_accepted() {
        let mut decoder = Decoder::default();
        let new = 2i64.to_be_bytes();
        let mut raw = vec![b'U'];
        raw.extend_from_slice(&1u32.to_be_bytes());
        raw.push(b'N');
        raw.extend_from_slice(&tuple(&[Some(&new)]));
        let Message::Change { operation, .. } = decoder.decode(&raw).unwrap() else {
            panic!("expected Change");
        };
        assert_eq!(operation, Operation::Update);
    }

    #[test]
    fn a_delete_carries_the_key_tuple() {
        let mut decoder = Decoder::default();
        let id = 3i64.to_be_bytes();
        let delete_bytes = delete(1, &[Some(&id), None]);
        let Message::Change {
            operation, tuple, ..
        } = decoder.decode(&delete_bytes).unwrap()
        else {
            panic!("expected Change");
        };
        assert_eq!(operation, Operation::Delete);
        assert_eq!(tuple.len(), 2);
        assert!(matches!(tuple[1], TupleValue::Null));
    }

    #[test]
    fn truncate_reports_its_relation_count_and_emits_no_rows() {
        let mut decoder = Decoder::default();
        let Message::Truncate { relations } = decoder.decode(&truncate(&[1, 2, 3])).unwrap() else {
            panic!("expected Truncate");
        };
        assert_eq!(relations, 3);
    }

    #[test]
    fn type_and_origin_messages_are_discarded() {
        let mut decoder = Decoder::default();
        let mut type_message = vec![b'Y'];
        type_message.extend_from_slice(&1234u32.to_be_bytes());
        type_message.extend_from_slice(b"public\0");
        type_message.extend_from_slice(b"mood\0");
        assert!(matches!(
            decoder.decode(&type_message).unwrap(),
            Message::Metadata
        ));

        let mut origin = vec![b'O'];
        origin.extend_from_slice(&0i64.to_be_bytes());
        origin.extend_from_slice(b"upstream\0");
        assert!(matches!(
            decoder.decode(&origin).unwrap(),
            Message::Metadata
        ));
    }

    #[test]
    fn an_unknown_tag_names_itself() {
        let mut decoder = Decoder::default();
        let err = decoder.decode(&[b'Z', 0, 0]).unwrap_err();
        assert!(err.message().contains("'Z'"), "{}", err.message());
        assert!(err.message().contains("0x5A"), "{}", err.message());
    }

    #[test]
    fn a_text_tagged_tuple_value_is_preserved_for_the_caller_to_reject() {
        let mut decoder = Decoder::default();
        let insert_bytes = insert_text(1, "42");
        let Message::Change { tuple, .. } = decoder.decode(&insert_bytes).unwrap() else {
            panic!("expected Change");
        };
        assert!(matches!(tuple[0], TupleValue::Text(b"42")));
    }

    #[test]
    fn an_unknown_tuple_marker_is_rejected() {
        let mut raw = vec![b'I'];
        raw.extend_from_slice(&1u32.to_be_bytes());
        raw.push(b'N');
        raw.extend_from_slice(&1i16.to_be_bytes());
        raw.push(b'x');
        let err = Decoder::default().decode(&raw).unwrap_err();
        assert!(err.message().contains("'x'"), "{}", err.message());
    }

    #[test]
    fn a_truncated_message_is_rejected() {
        let err = Decoder::default().decode(&[b'B', 0, 0]).unwrap_err();
        assert!(err.message().contains("truncated"), "{}", err.message());
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut raw = commit(0);
        raw.push(0xff);
        let err = Decoder::default().decode(&raw).unwrap_err();
        assert!(err.message().contains("undecoded"), "{}", err.message());
    }

    #[test]
    fn an_unterminated_string_is_rejected() {
        let mut raw = vec![b'R'];
        raw.extend_from_slice(&1u32.to_be_bytes());
        raw.extend_from_slice(b"public");
        let err = Decoder::default().decode(&raw).unwrap_err();
        assert!(err.message().contains("unterminated"), "{}", err.message());
    }

    #[test]
    fn an_empty_namespace_reads_as_public() {
        let mut decoder = Decoder::default();
        decoder
            .decode(&relation(1, "", "orders", &[("id", OID_INT8)]))
            .unwrap();
        assert_eq!(decoder.relation(1).unwrap().qualified(), "public.orders");
    }

    #[test]
    fn reset_drops_the_relation_table() {
        let mut decoder = Decoder::default();
        decoder
            .decode(&relation(1, "public", "t", &[("id", OID_INT8)]))
            .unwrap();
        assert!(decoder.relation(1).is_some());
        decoder.reset();
        assert!(decoder.relation(1).is_none());
    }
}
