use crate::array::validity::concat_validities;
use crate::array::{
    Array, BooleanArray, BooleanValuesBuffer, DecimalArray, NullArray, OffsetIndex, PrimitiveArray,
    VarlenArray, VarlenType, VarlenValuesBuffer,
};
use crate::field::DataType;
use rayexec_error::{RayexecError, Result};

use super::macros::collect_arrays_of_type;

/// Concat multiple arrays into a single array.
///
/// All arrays must be of the same type.
pub fn concat(arrays: &[&Array]) -> Result<Array> {
    if arrays.is_empty() {
        return Err(RayexecError::new("Cannot concat zero arrays"));
    }

    let datatype = arrays[0].datatype();

    match datatype {
        DataType::Null => {
            let arrs = collect_arrays_of_type!(arrays, Null, datatype)?;
            Ok(Array::Null(NullArray::new(
                arrs.iter().map(|arr| arr.len()).sum(),
            )))
        }

        DataType::Boolean => {
            let arrs = collect_arrays_of_type!(arrays, Boolean, datatype)?;
            Ok(Array::Boolean(concat_boolean(arrs.as_slice())))
        }
        DataType::Float32 => {
            let arrs = collect_arrays_of_type!(arrays, Float32, datatype)?;
            Ok(Array::Float32(concat_primitive(arrs.as_slice())))
        }
        DataType::Float64 => {
            let arrs = collect_arrays_of_type!(arrays, Float64, datatype)?;
            Ok(Array::Float64(concat_primitive(arrs.as_slice())))
        }
        DataType::Int8 => {
            let arrs = collect_arrays_of_type!(arrays, Int8, datatype)?;
            Ok(Array::Int8(concat_primitive(arrs.as_slice())))
        }
        DataType::Int16 => {
            let arrs = collect_arrays_of_type!(arrays, Int16, datatype)?;
            Ok(Array::Int16(concat_primitive(arrs.as_slice())))
        }
        DataType::Int32 => {
            let arrs = collect_arrays_of_type!(arrays, Int32, datatype)?;
            Ok(Array::Int32(concat_primitive(arrs.as_slice())))
        }
        DataType::Int64 => {
            let arrs = collect_arrays_of_type!(arrays, Int64, datatype)?;
            Ok(Array::Int64(concat_primitive(arrs.as_slice())))
        }
        DataType::UInt8 => {
            let arrs = collect_arrays_of_type!(arrays, UInt8, datatype)?;
            Ok(Array::UInt8(concat_primitive(arrs.as_slice())))
        }
        DataType::UInt16 => {
            let arrs = collect_arrays_of_type!(arrays, UInt16, datatype)?;
            Ok(Array::UInt16(concat_primitive(arrs.as_slice())))
        }
        DataType::UInt32 => {
            let arrs = collect_arrays_of_type!(arrays, UInt32, datatype)?;
            Ok(Array::UInt32(concat_primitive(arrs.as_slice())))
        }
        DataType::UInt64 => {
            let arrs = collect_arrays_of_type!(arrays, UInt64, datatype)?;
            Ok(Array::UInt64(concat_primitive(arrs.as_slice())))
        }
        DataType::Decimal64(p, s) => {
            let arrs = collect_arrays_of_type!(arrays, Decimal64, datatype)?;
            let arrs: Vec<_> = arrs.iter().map(|arr| arr.get_primitive()).collect();
            Ok(Array::Decimal64(DecimalArray::new(
                p,
                s,
                concat_primitive(arrs.as_slice()),
            )))
        }
        DataType::Decimal128(p, s) => {
            let arrs = collect_arrays_of_type!(arrays, Decimal128, datatype)?;
            let arrs: Vec<_> = arrs.iter().map(|arr| arr.get_primitive()).collect();
            Ok(Array::Decimal128(DecimalArray::new(
                p,
                s,
                concat_primitive(arrs.as_slice()),
            )))
        }
        DataType::Date32 => {
            let arrs = collect_arrays_of_type!(arrays, Date32, datatype)?;
            Ok(Array::Date32(concat_primitive(arrs.as_slice())))
        }
        DataType::Date64 => {
            let arrs = collect_arrays_of_type!(arrays, Date64, datatype)?;
            Ok(Array::Date64(concat_primitive(arrs.as_slice())))
        }
        DataType::Timestamp(unit) => {
            // TODO: Need to worry about unit?
            let arrs = arrays
                .iter()
                .map(|arr| match arr {
                    Array::Timestamp(_, arr) => Ok(arr),
                    other => Err(RayexecError::new(format!(
                        "Array is not of the expected type. Expected {}, got {}",
                        DataType::Timestamp(unit),
                        other.datatype()
                    ))),
                })
                .collect::<rayexec_error::Result<Vec<_>>>()?;
            Ok(Array::Timestamp(unit, concat_primitive(arrs.as_slice())))
        }
        DataType::Interval => {
            let arrs = collect_arrays_of_type!(arrays, Interval, datatype)?;
            Ok(Array::Interval(concat_primitive(arrs.as_slice())))
        }
        DataType::Utf8 => {
            let arrs = collect_arrays_of_type!(arrays, Utf8, datatype)?;
            Ok(Array::Utf8(concat_varlen(arrs.as_slice())))
        }
        DataType::LargeUtf8 => {
            let arrs = collect_arrays_of_type!(arrays, LargeUtf8, datatype)?;
            Ok(Array::LargeUtf8(concat_varlen(arrs.as_slice())))
        }
        DataType::Binary => {
            let arrs = collect_arrays_of_type!(arrays, Binary, datatype)?;
            Ok(Array::Binary(concat_varlen(arrs.as_slice())))
        }
        DataType::LargeBinary => {
            let arrs = collect_arrays_of_type!(arrays, LargeBinary, datatype)?;
            Ok(Array::LargeBinary(concat_varlen(arrs.as_slice())))
        }
        DataType::Struct { .. } => unimplemented!(),
    }
}

pub fn concat_boolean(arrays: &[&BooleanArray]) -> BooleanArray {
    let validity = concat_validities(arrays.iter().map(|arr| (arr.len(), arr.validity())));
    let values_iters = arrays.iter().map(|arr| arr.values());
    let values: BooleanValuesBuffer = values_iters.flat_map(|v| v.iter()).collect();
    BooleanArray::new(values, validity)
}

pub fn concat_primitive<T: Copy>(arrays: &[&PrimitiveArray<T>]) -> PrimitiveArray<T> {
    let validity = concat_validities(arrays.iter().map(|arr| (arr.len(), arr.validity())));
    let values_iters = arrays.iter().map(|arr| arr.values().as_ref());
    let values: Vec<T> = values_iters.flat_map(|v| v.iter().copied()).collect();
    PrimitiveArray::new(values, validity)
}

pub fn concat_varlen<T, O>(arrays: &[&VarlenArray<T, O>]) -> VarlenArray<T, O>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    let validity = concat_validities(arrays.iter().map(|arr| (arr.len(), arr.validity())));
    let values_iters = arrays.iter().map(|arr| arr.values_iter());
    let values: VarlenValuesBuffer<_> = values_iters.flatten().collect();
    VarlenArray::new(values, validity)
}

#[cfg(test)]
mod tests {
    use crate::array::{Int64Array, Utf8Array};

    use super::*;

    #[test]
    fn concat_primitive() {
        let arrs = [
            &Array::Int64(Int64Array::from_iter([1])),
            &Array::Int64(Int64Array::from_iter([2, 3])),
            &Array::Int64(Int64Array::from_iter([4, 5, 6])),
        ];

        let got = concat(&arrs).unwrap();
        let expected = Array::Int64(Int64Array::from_iter([1, 2, 3, 4, 5, 6]));

        assert_eq!(expected, got);
    }

    #[test]
    fn concat_varlen() {
        let arrs = [
            &Array::Utf8(Utf8Array::from_iter(["a"])),
            &Array::Utf8(Utf8Array::from_iter(["bb", "ccc"])),
            &Array::Utf8(Utf8Array::from_iter(["dddd", "eeeee", "ffffff"])),
        ];

        let got = concat(&arrs).unwrap();
        let expected = Array::Utf8(Utf8Array::from_iter([
            "a", "bb", "ccc", "dddd", "eeeee", "ffffff",
        ]));

        assert_eq!(expected, got);
    }
}
