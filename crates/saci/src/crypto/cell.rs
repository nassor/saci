//! Lossless per-cell encoding and decoding for arbitrary Arrow column types.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::types::IntervalMonthDayNanoType;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, IntervalMonthDayNanoArray, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, new_null_array,
};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};

use crate::error::SaciError;

/// Encode a single non-null cell `(array, row)` into canonical plaintext bytes.
pub(crate) fn encode_cell(array: &dyn Array, row: usize) -> Result<Vec<u8>, SaciError> {
    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(vec![u8::from(a.value(row))])
        }
        DataType::Int8 => {
            let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::UInt8 => {
            let a = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::UInt16 => {
            let a = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(a.value(row).as_bytes().to_vec())
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(a.value(row).as_bytes().to_vec())
        }
        DataType::Binary => {
            let a = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            Ok(a.value(row).to_vec())
        }
        DataType::LargeBinary => {
            let a = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Ok(a.value(row).to_vec())
        }
        DataType::FixedSizeBinary(_) => {
            let a = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();
            Ok(a.value(row).to_vec())
        }
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Date64 => {
            let a = array.as_any().downcast_ref::<Date64Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let a = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let a = array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Decimal128(_, _) => {
            let a = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(a.value(row).to_le_bytes().to_vec())
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let a = array
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .unwrap();
            let v = a.value(row);
            let mut bytes = [0u8; 16];
            bytes[0..4].copy_from_slice(&v.months.to_le_bytes());
            bytes[4..8].copy_from_slice(&v.days.to_le_bytes());
            bytes[8..16].copy_from_slice(&v.nanoseconds.to_le_bytes());
            Ok(bytes.to_vec())
        }
        other => {
            let single = array.slice(row, 1);
            let schema = Arc::new(Schema::new(vec![Field::new("v", other.clone(), true)]));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![single]).map_err(|e| {
                SaciError::generic(format!("crypto: cannot wrap cell for IPC: {e}"))
            })?;
            let mut bytes = Vec::new();
            let mut writer = StreamWriter::try_new(&mut bytes, &schema)
                .map_err(|e| SaciError::generic(format!("crypto: cell IPC writer: {e}")))?;
            writer
                .write(&batch)
                .map_err(|e| SaciError::generic(format!("crypto: cell IPC write: {e}")))?;
            writer
                .finish()
                .map_err(|e| SaciError::generic(format!("crypto: cell IPC finish: {e}")))?;
            Ok(bytes)
        }
    }
}

/// Reconstruct an Arrow array of `data_type` from decrypted per-row cells.
pub(crate) fn build_column(
    data_type: &DataType,
    cells: Vec<Option<Vec<u8>>>,
) -> Result<ArrayRef, SaciError> {
    match data_type {
        DataType::Boolean => {
            let vals: Vec<Option<bool>> = cells
                .into_iter()
                .map(|c| {
                    c.map(|b| match b.as_slice() {
                        [0] => Ok(false),
                        [1] => Ok(true),
                        _ => Err(SaciError::generic("crypto: invalid boolean cell payload")),
                    })
                    .transpose()
                })
                .collect::<Result<_, _>>()?;
            Ok(Arc::new(BooleanArray::from(vals)))
        }
        DataType::Int8 => {
            build_prim::<i8, 1>(cells, i8::from_le_bytes, |v| Arc::new(Int8Array::from(v)))
        }
        DataType::Int16 => {
            build_prim::<i16, 2>(cells, i16::from_le_bytes, |v| Arc::new(Int16Array::from(v)))
        }
        DataType::Int32 => {
            build_prim::<i32, 4>(cells, i32::from_le_bytes, |v| Arc::new(Int32Array::from(v)))
        }
        DataType::Int64 => {
            build_prim::<i64, 8>(cells, i64::from_le_bytes, |v| Arc::new(Int64Array::from(v)))
        }
        DataType::UInt8 => {
            build_prim::<u8, 1>(cells, u8::from_le_bytes, |v| Arc::new(UInt8Array::from(v)))
        }
        DataType::UInt16 => build_prim::<u16, 2>(cells, u16::from_le_bytes, |v| {
            Arc::new(UInt16Array::from(v))
        }),
        DataType::UInt32 => build_prim::<u32, 4>(cells, u32::from_le_bytes, |v| {
            Arc::new(UInt32Array::from(v))
        }),
        DataType::UInt64 => build_prim::<u64, 8>(cells, u64::from_le_bytes, |v| {
            Arc::new(UInt64Array::from(v))
        }),
        DataType::Float32 => build_prim::<f32, 4>(cells, f32::from_le_bytes, |v| {
            Arc::new(Float32Array::from(v))
        }),
        DataType::Float64 => build_prim::<f64, 8>(cells, f64::from_le_bytes, |v| {
            Arc::new(Float64Array::from(v))
        }),
        DataType::Utf8 => {
            let vals: Vec<Option<String>> = cells
                .into_iter()
                .map(|c| {
                    c.map(|b| {
                        String::from_utf8(b)
                            .map_err(|_| SaciError::generic("crypto: invalid UTF-8 cell payload"))
                    })
                    .transpose()
                })
                .collect::<Result<_, _>>()?;
            Ok(Arc::new(StringArray::from(vals)))
        }
        DataType::LargeUtf8 => {
            let vals: Vec<Option<String>> = cells
                .into_iter()
                .map(|c| {
                    c.map(|b| {
                        String::from_utf8(b)
                            .map_err(|_| SaciError::generic("crypto: invalid UTF-8 cell payload"))
                    })
                    .transpose()
                })
                .collect::<Result<_, _>>()?;
            Ok(Arc::new(LargeStringArray::from(vals)))
        }
        DataType::Binary => {
            let slices: Vec<Option<&[u8]>> = cells.iter().map(|c| c.as_deref()).collect();
            Ok(Arc::new(BinaryArray::from(slices)))
        }
        DataType::LargeBinary => {
            let slices: Vec<Option<&[u8]>> = cells.iter().map(|c| c.as_deref()).collect();
            Ok(Arc::new(LargeBinaryArray::from(slices)))
        }
        DataType::FixedSizeBinary(width) => {
            let expected = usize::try_from(*width).map_err(|_| {
                SaciError::generic(format!("crypto: invalid FixedSizeBinary width {width}"))
            })?;
            let vals: Vec<Option<Vec<u8>>> = cells
                .into_iter()
                .map(|c| {
                    c.map(|b| {
                        if b.len() != expected {
                            Err(SaciError::generic(format!(
                                "crypto: expected {expected} bytes for FixedSizeBinary cell, got {}",
                                b.len()
                            )))
                        } else {
                            Ok(b)
                        }
                    })
                    .transpose()
                })
                .collect::<Result<_, _>>()?;
            let arr = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                vals.iter().map(|v| v.as_deref()),
                *width,
            )
            .map_err(|e| SaciError::generic(format!("crypto: FixedSizeBinary build: {e}")))?;
            Ok(Arc::new(arr))
        }
        DataType::Date32 => build_prim::<i32, 4>(cells, i32::from_le_bytes, |v| {
            Arc::new(Date32Array::from(v))
        }),
        DataType::Date64 => build_prim::<i64, 8>(cells, i64::from_le_bytes, |v| {
            Arc::new(Date64Array::from(v))
        }),
        DataType::Time64(TimeUnit::Microsecond) => {
            build_prim::<i64, 8>(cells, i64::from_le_bytes, |v| {
                Arc::new(Time64MicrosecondArray::from(v))
            })
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            build_prim::<i64, 8>(cells, i64::from_le_bytes, |v| {
                Arc::new(Time64NanosecondArray::from(v))
            })
        }
        DataType::Timestamp(TimeUnit::Second, tz) => {
            let tz = tz.clone();
            build_prim::<i64, 8>(cells, i64::from_le_bytes, |v| {
                Arc::new(TimestampSecondArray::from(v).with_timezone_opt(tz))
            })
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let tz = tz.clone();
            build_prim::<i64, 8>(cells, i64::from_le_bytes, |v| {
                Arc::new(TimestampMillisecondArray::from(v).with_timezone_opt(tz))
            })
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let tz = tz.clone();
            build_prim::<i64, 8>(cells, i64::from_le_bytes, |v| {
                Arc::new(TimestampMicrosecondArray::from(v).with_timezone_opt(tz))
            })
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let tz = tz.clone();
            build_prim::<i64, 8>(cells, i64::from_le_bytes, |v| {
                Arc::new(TimestampNanosecondArray::from(v).with_timezone_opt(tz))
            })
        }
        DataType::Decimal128(precision, scale) => {
            let (p, s) = (*precision, *scale);
            let arr = build_prim_vec::<i128, 16>(cells, i128::from_le_bytes)?;
            let dec = Decimal128Array::from(arr)
                .with_precision_and_scale(p, s)
                .map_err(|e| SaciError::generic(format!("crypto: Decimal128 build: {e}")))?;
            Ok(Arc::new(dec))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let vals = cells
                .into_iter()
                .map(|c| {
                    c.map(|b| {
                        let arr: [u8; 16] = b.try_into().map_err(|_| {
                            SaciError::generic("crypto: invalid IntervalMonthDayNano cell payload")
                        })?;
                        let months = i32::from_le_bytes(arr[0..4].try_into().unwrap());
                        let days = i32::from_le_bytes(arr[4..8].try_into().unwrap());
                        let nanos = i64::from_le_bytes(arr[8..16].try_into().unwrap());
                        Ok::<_, SaciError>(IntervalMonthDayNanoType::make_value(
                            months, days, nanos,
                        ))
                    })
                    .transpose()
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(IntervalMonthDayNanoArray::from(vals)))
        }
        other => {
            if cells.is_empty() {
                return Ok(new_null_array(other, 0));
            }
            let mut slices: Vec<ArrayRef> = Vec::with_capacity(cells.len());
            for cell in cells {
                match cell {
                    None => slices.push(new_null_array(other, 1)),
                    Some(bytes) => {
                        let mut reader =
                            StreamReader::try_new(Cursor::new(bytes), None).map_err(|e| {
                                SaciError::generic(format!("crypto: cell IPC reader: {e}"))
                            })?;
                        let batch = reader
                            .next()
                            .transpose()
                            .map_err(|e| {
                                SaciError::generic(format!("crypto: cell IPC decode: {e}"))
                            })?
                            .ok_or_else(|| SaciError::generic("crypto: empty IPC cell payload"))?;
                        if batch.num_rows() != 1 || batch.num_columns() != 1 {
                            return Err(SaciError::generic(
                                "crypto: unexpected shape in IPC cell payload",
                            ));
                        }
                        if batch.column(0).data_type() != other {
                            return Err(SaciError::generic(format!(
                                "crypto: decrypted IPC cell type {:?} does not match expected {:?}",
                                batch.column(0).data_type(),
                                other
                            )));
                        }
                        if reader.next().is_some() {
                            return Err(SaciError::generic(
                                "crypto: unexpected extra batch in IPC cell payload",
                            ));
                        }
                        slices.push(Arc::clone(batch.column(0)));
                    }
                }
            }
            let refs: Vec<&dyn Array> = slices.iter().map(AsRef::as_ref).collect();
            arrow_select::concat::concat(&refs)
                .map_err(|e| SaciError::generic(format!("crypto: concat cell arrays: {e}")))
        }
    }
}

fn build_prim_vec<T, const N: usize>(
    cells: Vec<Option<Vec<u8>>>,
    from_le: fn([u8; N]) -> T,
) -> Result<Vec<Option<T>>, SaciError> {
    cells
        .into_iter()
        .map(|c| {
            c.map(|b| {
                let arr: [u8; N] = b.try_into().map_err(|v: Vec<u8>| {
                    SaciError::generic(format!(
                        "crypto: expected {N} bytes for cell, got {}",
                        v.len()
                    ))
                })?;
                Ok(from_le(arr))
            })
            .transpose()
        })
        .collect()
}

fn build_prim<T, const N: usize>(
    cells: Vec<Option<Vec<u8>>>,
    from_le: fn([u8; N]) -> T,
    finish: impl FnOnce(Vec<Option<T>>) -> ArrayRef,
) -> Result<ArrayRef, SaciError> {
    Ok(finish(build_prim_vec::<T, N>(cells, from_le)?))
}
