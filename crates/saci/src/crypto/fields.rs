//! Whole-batch payload and field-level Arrow [`RecordBatch`] encryption.

use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};

use crate::crypto::cell;
use crate::crypto::envelope::Cipher;
use crate::error::SaciError;
use crate::io::CorruptRecord;

/// Column name used on the wire when [`EncryptionMode::Payload`] is active.
pub const PAYLOAD_COLUMN: &str = "__saci_encrypted_payload";

/// Wire representation of encrypted columns or payloads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FieldEncoding {
    /// Store ciphertext envelopes as Arrow `Binary` (`DataType::Binary`).
    #[default]
    Binary,
    /// Store base64-encoded ciphertext envelopes as Arrow `Utf8` (`DataType::Utf8`).
    Base64Utf8,
}

impl FieldEncoding {
    pub(crate) const fn wire_type(self) -> DataType {
        match self {
            Self::Binary => DataType::Binary,
            Self::Base64Utf8 => DataType::Utf8,
        }
    }
}

/// How [`EncryptingSink`](crate::crypto::EncryptingSink) and
/// [`DecryptingSource`](crate::crypto::DecryptingSource) transform batches.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncryptionMode {
    /// Encrypt only the named fields per row, leaving all other columns untouched.
    Fields {
        /// Plaintext column names to encrypt.
        fields: Vec<String>,
        /// Wire encoding for encrypted columns.
        encoding: FieldEncoding,
    },
    /// Encrypt the entire [`RecordBatch`] (as Arrow IPC stream bytes) into a
    /// single ciphertext envelope column named [`PAYLOAD_COLUMN`].
    Payload {
        /// Wire encoding for the ciphertext payload column.
        encoding: FieldEncoding,
    },
}

impl EncryptionMode {
    /// Encrypt `fields` as [`FieldEncoding::Binary`] columns.
    pub fn fields<I, S>(fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Fields {
            fields: fields.into_iter().map(Into::into).collect(),
            encoding: FieldEncoding::Binary,
        }
    }

    /// Encrypt `fields` with an explicit [`FieldEncoding`].
    pub fn fields_with_encoding<I, S>(fields: I, encoding: FieldEncoding) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Fields {
            fields: fields.into_iter().map(Into::into).collect(),
            encoding,
        }
    }

    /// Encrypt the whole batch as [`FieldEncoding::Binary`].
    pub const fn payload() -> Self {
        Self::Payload {
            encoding: FieldEncoding::Binary,
        }
    }

    /// Encrypt the whole batch with an explicit [`FieldEncoding`].
    pub const fn payload_with_encoding(encoding: FieldEncoding) -> Self {
        Self::Payload { encoding }
    }
}

/// Derive the wire Arrow [`Schema`] corresponding to `plain` and `mode`.
///
/// # Errors
///
/// Returns [`SaciError::Configuration`] when `EncryptionMode::Fields` names no
/// fields, names a duplicate field, or names a field absent from `plain`.
pub fn wire_schema(plain: &Schema, mode: &EncryptionMode) -> Result<Arc<Schema>, SaciError> {
    match mode {
        EncryptionMode::Payload { encoding } => Ok(Arc::new(Schema::new(vec![Field::new(
            PAYLOAD_COLUMN,
            encoding.wire_type(),
            false,
        )]))),
        EncryptionMode::Fields { fields, encoding } => {
            let targets = validate_field_targets(plain, fields)?;
            let mapped: Vec<Field> = plain
                .fields()
                .iter()
                .map(|field| {
                    if targets.contains(field.name().as_str()) {
                        Field::new(field.name(), encoding.wire_type(), field.is_nullable())
                    } else {
                        field.as_ref().clone()
                    }
                })
                .collect();
            Ok(Arc::new(Schema::new(mapped)))
        }
    }
}

fn validate_field_targets<'a>(
    plain: &Schema,
    fields: &'a [String],
) -> Result<HashSet<&'a str>, SaciError> {
    if fields.is_empty() {
        return Err(SaciError::configuration(
            "crypto: EncryptionMode::Fields requires at least one field name",
        ));
    }
    let mut targets = HashSet::with_capacity(fields.len());
    for name in fields {
        if !targets.insert(name.as_str()) {
            return Err(SaciError::configuration(format!(
                "crypto: duplicate encrypted field '{name}'"
            )));
        }
        if plain.index_of(name).is_err() {
            return Err(SaciError::configuration(format!(
                "crypto: encrypted field '{name}' does not exist in schema"
            )));
        }
    }
    Ok(targets)
}

/// Encrypt `batch` according to `mode`, producing a [`RecordBatch`] conforming to `wire_schema`.
///
/// # Errors
///
/// Returns [`SaciError`] if `mode` validation fails, cell encoding fails, or
/// AEAD sealing fails.
pub fn encrypt_batch(
    batch: &RecordBatch,
    wire_schema: &Arc<Schema>,
    cipher: &Cipher,
    mode: &EncryptionMode,
) -> Result<RecordBatch, SaciError> {
    match mode {
        EncryptionMode::Payload { encoding } => {
            if batch.num_rows() == 0 {
                return Ok(RecordBatch::new_empty(Arc::clone(wire_schema)));
            }
            let mut ipc = Vec::new();
            let mut writer = StreamWriter::try_new(&mut ipc, &batch.schema())
                .map_err(|e| SaciError::generic(format!("crypto: batch IPC writer: {e}")))?;
            writer
                .write(batch)
                .map_err(|e| SaciError::generic(format!("crypto: batch IPC write: {e}")))?;
            writer
                .finish()
                .map_err(|e| SaciError::generic(format!("crypto: batch IPC finish: {e}")))?;

            let col: ArrayRef = match encoding {
                FieldEncoding::Binary => {
                    let sealed = cipher.encrypt(&ipc, b"saci:batch")?;
                    Arc::new(BinaryArray::from(vec![Some(sealed.as_slice())]))
                }
                FieldEncoding::Base64Utf8 => {
                    let sealed = cipher.encrypt_base64(&ipc, b"saci:batch")?;
                    Arc::new(StringArray::from(vec![Some(sealed.as_str())]))
                }
            };
            RecordBatch::try_new(Arc::clone(wire_schema), vec![col])
                .map_err(|e| SaciError::generic(format!("crypto: build encrypted batch: {e}")))
        }
        EncryptionMode::Fields { fields, encoding } => {
            let targets = validate_field_targets(&batch.schema(), fields)?;
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
            for (idx, field) in batch.schema().fields().iter().enumerate() {
                let col = batch.column(idx);
                if !targets.contains(field.name().as_str()) {
                    columns.push(Arc::clone(col));
                    continue;
                }
                let aad = field_aad(field.name());
                let encrypted_col = encrypt_column(col.as_ref(), cipher, &aad, *encoding)?;
                columns.push(encrypted_col);
            }
            RecordBatch::try_new(Arc::clone(wire_schema), columns)
                .map_err(|e| SaciError::generic(format!("crypto: build encrypted batch: {e}")))
        }
    }
}

/// Decrypt `batch` according to `mode`, restoring `plain_schema`.
///
/// # Errors
///
/// Returns [`SaciError`] if the wire payload is malformed, fails AEAD
/// authentication, or violates non-null constraints in `plain_schema`.
pub fn decrypt_batch(
    batch: &RecordBatch,
    plain_schema: &Arc<Schema>,
    cipher: &Cipher,
    mode: &EncryptionMode,
) -> Result<RecordBatch, SaciError> {
    match mode {
        EncryptionMode::Payload { encoding } => {
            if batch.num_rows() == 0 {
                return Ok(RecordBatch::new_empty(Arc::clone(plain_schema)));
            }
            if batch.num_columns() != 1 || batch.schema().field(0).name() != PAYLOAD_COLUMN {
                return Err(SaciError::generic(format!(
                    "crypto: expected single '{PAYLOAD_COLUMN}' column in encrypted payload batch"
                )));
            }
            let mut decoded: Vec<RecordBatch> = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let env_bytes = read_wire_cell(batch.column(0).as_ref(), row, *encoding)?
                    .ok_or_else(|| SaciError::generic("crypto: encrypted payload cell is null"))?;
                let ipc = cipher.decrypt(&env_bytes, b"saci:batch")?;
                let mut reader = StreamReader::try_new(Cursor::new(ipc), None)
                    .map_err(|e| SaciError::generic(format!("crypto: batch IPC reader: {e}")))?;
                for item in &mut reader {
                    let b = item.map_err(|e| {
                        SaciError::generic(format!("crypto: batch IPC decode: {e}"))
                    })?;
                    if b.schema().fields() != plain_schema.fields() {
                        return Err(SaciError::generic(
                            "crypto: decrypted payload schema does not match declared schema",
                        ));
                    }
                    decoded.push(b);
                }
            }
            if decoded.is_empty() {
                return Ok(RecordBatch::new_empty(Arc::clone(plain_schema)));
            }
            if decoded.len() == 1 {
                return Ok(decoded.into_iter().next().expect("len == 1"));
            }
            arrow_select::concat::concat_batches(plain_schema, &decoded)
                .map_err(|e| SaciError::generic(format!("crypto: concat decrypted batches: {e}")))
        }
        EncryptionMode::Fields { fields, encoding } => {
            let targets = validate_field_targets(plain_schema, fields)?;
            if batch.num_columns() != plain_schema.fields().len() {
                return Err(SaciError::generic(
                    "crypto: encrypted batch column count does not match plain schema",
                ));
            }
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
            for (idx, field) in plain_schema.fields().iter().enumerate() {
                let col = batch.column(idx);
                if !targets.contains(field.name().as_str()) {
                    columns.push(Arc::clone(col));
                    continue;
                }
                let aad = field_aad(field.name());
                let decrypted = decrypt_column(
                    col.as_ref(),
                    field.data_type(),
                    field.is_nullable(),
                    field.name(),
                    cipher,
                    &aad,
                    *encoding,
                )?;
                columns.push(decrypted);
            }
            RecordBatch::try_new(Arc::clone(plain_schema), columns)
                .map_err(|e| SaciError::generic(format!("crypto: build decrypted batch: {e}")))
        }
    }
}

/// Decrypt `batch` while isolating individual rows that fail AEAD authentication,
/// base64 decoding, or non-null checks into [`CorruptRecord`]s.
///
/// Structural schema errors (wrong column count or missing encrypted columns)
/// still return `Err`. When all rows fail decryption, an empty batch matching
/// `plain_schema` is returned alongside the collected corrupt records.
///
/// # Errors
///
/// Returns [`SaciError`] when `batch` lacks the expected wire schema structure or
/// concatenating surviving decrypted rows fails.
pub fn decrypt_batch_isolating_corrupt(
    batch: &RecordBatch,
    plain_schema: &Arc<Schema>,
    cipher: &Cipher,
    mode: &EncryptionMode,
) -> Result<(RecordBatch, Vec<CorruptRecord>), SaciError> {
    match decrypt_batch(batch, plain_schema, cipher, mode) {
        Ok(decrypted) => Ok((decrypted, Vec::new())),
        Err(batch_err) => {
            if batch.num_rows() == 0 {
                return Err(batch_err);
            }
            match mode {
                EncryptionMode::Payload { .. } => {
                    if batch.num_columns() != 1 || batch.schema().field(0).name() != PAYLOAD_COLUMN
                    {
                        return Err(batch_err);
                    }
                }
                EncryptionMode::Fields { fields, .. } => {
                    validate_field_targets(plain_schema, fields)?;
                    if batch.num_columns() != plain_schema.fields().len() {
                        return Err(batch_err);
                    }
                }
            }
            let mut good: Vec<RecordBatch> = Vec::with_capacity(batch.num_rows());
            let mut corrupt: Vec<CorruptRecord> = Vec::new();
            for row in 0..batch.num_rows() {
                let row_batch = batch.slice(row, 1);
                match decrypt_batch(&row_batch, plain_schema, cipher, mode) {
                    Ok(dec) => good.push(dec),
                    Err(e) => corrupt.push(CorruptRecord {
                        format: "crypto",
                        payload: extract_row_wire_bytes(batch, row, mode),
                        error: e.message().to_string(),
                    }),
                }
            }
            let out = if good.is_empty() {
                RecordBatch::new_empty(Arc::clone(plain_schema))
            } else if good.len() == 1 {
                good.into_iter().next().expect("len == 1")
            } else {
                arrow_select::concat::concat_batches(plain_schema, &good).map_err(|e| {
                    SaciError::generic(format!("crypto: concat decrypted batches: {e}"))
                })?
            };
            Ok((out, corrupt))
        }
    }
}

fn extract_row_wire_bytes(batch: &RecordBatch, row: usize, mode: &EncryptionMode) -> Vec<u8> {
    let raw_cell_bytes = |col: &dyn arrow_array::Array| -> Vec<u8> {
        if col.is_null(row) {
            return Vec::new();
        }
        if let Some(arr) = col.as_any().downcast_ref::<BinaryArray>() {
            arr.value(row).to_vec()
        } else if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            arr.value(row).as_bytes().to_vec()
        } else {
            Vec::new()
        }
    };
    match mode {
        EncryptionMode::Payload { .. } => raw_cell_bytes(batch.column(0).as_ref()),
        EncryptionMode::Fields { fields, .. } => {
            if fields.len() == 1
                && let Ok(idx) = batch.schema().index_of(&fields[0])
            {
                return raw_cell_bytes(batch.column(idx).as_ref());
            }
            let mut out = Vec::new();
            for name in fields {
                if let Ok(idx) = batch.schema().index_of(name) {
                    if !out.is_empty() {
                        out.push(b';');
                    }
                    out.extend_from_slice(name.as_bytes());
                    out.push(b'=');
                    out.extend_from_slice(&raw_cell_bytes(batch.column(idx).as_ref()));
                }
            }
            out
        }
    }
}

fn field_aad(field_name: &str) -> Vec<u8> {
    format!("saci:field:{field_name}").into_bytes()
}

fn encrypt_column(
    col: &dyn arrow_array::Array,
    cipher: &Cipher,
    aad: &[u8],
    encoding: FieldEncoding,
) -> Result<ArrayRef, SaciError> {
    match encoding {
        FieldEncoding::Binary => {
            let mut cells: Vec<Option<Vec<u8>>> = Vec::with_capacity(col.len());
            for row in 0..col.len() {
                if col.is_null(row) {
                    cells.push(None);
                } else {
                    let plain = cell::encode_cell(col, row)?;
                    cells.push(Some(cipher.encrypt(&plain, aad)?));
                }
            }
            let slices: Vec<Option<&[u8]>> = cells.iter().map(|c| c.as_deref()).collect();
            Ok(Arc::new(BinaryArray::from(slices)))
        }
        FieldEncoding::Base64Utf8 => {
            let mut cells: Vec<Option<String>> = Vec::with_capacity(col.len());
            for row in 0..col.len() {
                if col.is_null(row) {
                    cells.push(None);
                } else {
                    let plain = cell::encode_cell(col, row)?;
                    cells.push(Some(cipher.encrypt_base64(&plain, aad)?));
                }
            }
            Ok(Arc::new(StringArray::from(cells)))
        }
    }
}

fn decrypt_column(
    col: &dyn arrow_array::Array,
    target_type: &DataType,
    nullable: bool,
    field_name: &str,
    cipher: &Cipher,
    aad: &[u8],
    encoding: FieldEncoding,
) -> Result<ArrayRef, SaciError> {
    let mut cells: Vec<Option<Vec<u8>>> = Vec::with_capacity(col.len());
    for row in 0..col.len() {
        match read_wire_cell(col, row, encoding)? {
            None => {
                if !nullable {
                    return Err(SaciError::generic(format!(
                        "crypto: non-nullable encrypted field '{field_name}' received null at row {row}"
                    )));
                }
                cells.push(None);
            }
            Some(env) => {
                cells.push(Some(cipher.decrypt(&env, aad)?));
            }
        }
    }
    cell::build_column(target_type, cells)
}

fn read_wire_cell(
    col: &dyn arrow_array::Array,
    row: usize,
    encoding: FieldEncoding,
) -> Result<Option<Vec<u8>>, SaciError> {
    if col.is_null(row) {
        return Ok(None);
    }
    match encoding {
        FieldEncoding::Binary => {
            let arr = col.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                SaciError::generic(format!(
                    "crypto: expected Binary column for encrypted field, got {:?}",
                    col.data_type()
                ))
            })?;
            Ok(Some(arr.value(row).to_vec()))
        }
        FieldEncoding::Base64Utf8 => {
            use base64::Engine as _;
            let arr = col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                SaciError::generic(format!(
                    "crypto: expected Utf8 column for base64 encrypted field, got {:?}",
                    col.data_type()
                ))
            })?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(arr.value(row).trim())
                .map_err(|e| {
                    SaciError::generic(format!("crypto: invalid base64 cell ciphertext: {e}"))
                })?;
            Ok(Some(bytes))
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
        ListArray, StringArray, TimestampMicrosecondArray,
    };
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::TimeUnit;

    use super::*;
    use crate::crypto::keys::{KeyMaterial, StaticKeyProvider};

    fn test_cipher() -> Cipher {
        let provider =
            Arc::new(StaticKeyProvider::single("k1", KeyMaterial::generate().unwrap()).unwrap());
        Cipher::aes_256_gcm(provider)
    }

    #[test]
    fn field_encryption_round_trips_scalar_nested_and_nulls() {
        let cipher = test_cipher();
        let list_field = Arc::new(Field::new("item", DataType::Int32, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("flag", DataType::Boolean, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("day", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
            Field::new("tags", DataType::List(Arc::clone(&list_field)), true),
        ]));

        let list_values = Int32Array::from(vec![Some(1), Some(2), None]);
        let offsets = OffsetBuffer::new(vec![0i32, 2, 2, 3].into());
        let list_arr = ListArray::new(list_field, offsets, Arc::new(list_values), None);

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
                Arc::new(StringArray::from(vec![Some("alice"), None, Some("carol")])),
                Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])),
                Arc::new(Date32Array::from(vec![Some(19_000), None, Some(19_001)])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(1_000_000),
                    None,
                    Some(2_000_000),
                ])),
                Arc::new(
                    Decimal128Array::from(vec![Some(12345), None, Some(-99)])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(list_arr),
            ],
        )
        .unwrap();

        let targets = ["flag", "name", "score", "day", "ts", "amount", "tags"];
        for encoding in [FieldEncoding::Binary, FieldEncoding::Base64Utf8] {
            let mode = EncryptionMode::fields_with_encoding(targets, encoding);
            let wire = wire_schema(&schema, &mode).unwrap();
            let encrypted = encrypt_batch(&batch, &wire, &cipher, &mode).unwrap();
            assert_eq!(encrypted.schema().fields(), wire.fields());
            let decrypted = decrypt_batch(&encrypted, &schema, &cipher, &mode).unwrap();
            assert_eq!(decrypted, batch);
        }
    }

    #[test]
    fn column_swap_is_rejected_by_aad_binding() {
        let cipher = test_cipher();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["alpha"])),
                Arc::new(StringArray::from(vec!["beta"])),
            ],
        )
        .unwrap();
        let mode = EncryptionMode::fields(["a", "b"]);
        let wire = wire_schema(&schema, &mode).unwrap();
        let enc = encrypt_batch(&batch, &wire, &cipher, &mode).unwrap();

        // Swap columns `a` and `b` in the ciphertext batch.
        let swapped = RecordBatch::try_new(
            wire,
            vec![Arc::clone(enc.column(1)), Arc::clone(enc.column(0))],
        )
        .unwrap();
        assert!(decrypt_batch(&swapped, &schema, &cipher, &mode).is_err());
    }

    #[test]
    fn payload_mode_round_trips_whole_batch() {
        let cipher = test_cipher();
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![10, 20]))],
        )
        .unwrap();
        let mode = EncryptionMode::payload_with_encoding(FieldEncoding::Base64Utf8);
        let wire = wire_schema(&schema, &mode).unwrap();
        let enc = encrypt_batch(&batch, &wire, &cipher, &mode).unwrap();
        assert_eq!(enc.num_rows(), 1);
        let dec = decrypt_batch(&enc, &schema, &cipher, &mode).unwrap();
        assert_eq!(dec, batch);

        // Concatenating two encrypted 1-row payload batches into a 2-row wire batch restores all rows.
        let two_row_wire = arrow_select::concat::concat_batches(&wire, [&enc, &enc]).unwrap();
        let dec_two = decrypt_batch(&two_row_wire, &schema, &cipher, &mode).unwrap();
        let expected_two = arrow_select::concat::concat_batches(&schema, [&batch, &batch]).unwrap();
        assert_eq!(dec_two, expected_two);

        // Zero-row batch round-trips in both Payload and Fields modes.
        let empty = RecordBatch::new_empty(Arc::clone(&schema));
        let empty_enc = encrypt_batch(&empty, &wire, &cipher, &mode).unwrap();
        assert_eq!(empty_enc.num_rows(), 0);
        let empty_dec = decrypt_batch(&empty_enc, &schema, &cipher, &mode).unwrap();
        assert_eq!(empty_dec, empty);

        let f_mode = EncryptionMode::fields(["v"]);
        let f_wire = wire_schema(&schema, &f_mode).unwrap();
        let empty_f_enc = encrypt_batch(&empty, &f_wire, &cipher, &f_mode).unwrap();
        assert_eq!(
            decrypt_batch(&empty_f_enc, &schema, &cipher, &f_mode).unwrap(),
            empty
        );
    }

    #[test]
    fn malformed_wire_batches_and_types_are_rejected() {
        let cipher = test_cipher();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("secret", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["top-secret"])),
            ],
        )
        .unwrap();

        // 1. Null cell in non-nullable encrypted field must fail.
        let b64_mode = EncryptionMode::fields_with_encoding(["secret"], FieldEncoding::Base64Utf8);
        let nullable_wire = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("secret", DataType::Utf8, true),
        ]));
        let null_wire = RecordBatch::try_new(
            Arc::clone(&nullable_wire),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();
        assert!(decrypt_batch(&null_wire, &schema, &cipher, &b64_mode).is_err());

        // 2. Invalid base64 ciphertext must fail.
        let bad_b64_wire = RecordBatch::try_new(
            nullable_wire,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["!!!not-base64!!!"])),
            ],
        )
        .unwrap();
        assert!(decrypt_batch(&bad_b64_wire, &schema, &cipher, &b64_mode).is_err());

        // 3. Wrong wire column type (Int64 where Binary expected) must fail.
        let bin_mode = EncryptionMode::fields(["secret"]);
        let wrong_col_type_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("secret", DataType::Int64, false),
        ]));
        let wrong_col_type = RecordBatch::try_new(
            wrong_col_type_schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![42])),
            ],
        )
        .unwrap();
        assert!(decrypt_batch(&wrong_col_type, &schema, &cipher, &bin_mode).is_err());

        // 4. Extra column on Payload wire batch must fail.
        let extra_schema = Arc::new(Schema::new(vec![
            Field::new(PAYLOAD_COLUMN, DataType::Binary, false),
            Field::new("extra", DataType::Int64, false),
        ]));
        let p_mode = EncryptionMode::payload();
        let p_wire = wire_schema(&schema, &p_mode).unwrap();
        let p_enc = encrypt_batch(&batch, &p_wire, &cipher, &p_mode).unwrap();
        let extra_wire = RecordBatch::try_new(
            extra_schema,
            vec![
                Arc::clone(p_enc.column(0)),
                Arc::new(Int64Array::from(vec![99])),
            ],
        )
        .unwrap();
        assert!(decrypt_batch(&extra_wire, &schema, &cipher, &p_mode).is_err());

        // 5. Malformed IPC bytes sealed inside a valid AEAD envelope must fail.
        let bogus_sealed = cipher
            .encrypt(b"not-valid-arrow-ipc", b"saci:batch")
            .unwrap();
        let bogus_wire = RecordBatch::try_new(
            p_wire,
            vec![Arc::new(arrow_array::BinaryArray::from(vec![Some(
                bogus_sealed.as_slice(),
            )]))],
        )
        .unwrap();
        assert!(decrypt_batch(&bogus_wire, &schema, &cipher, &p_mode).is_err());
    }
}
