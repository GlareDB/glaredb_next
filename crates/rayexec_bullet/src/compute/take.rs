use crate::{
    array::{
        Array2, BooleanArray, DecimalArray, NullArray, OffsetIndex, PrimitiveArray, TimestampArray,
        VarlenArray, VarlenType2, VarlenValuesBuffer,
    },
    bitmap::Bitmap,
};
use rayexec_error::{not_implemented, RayexecError, Result};

/// Take values from an array at the provided indices, and return a new array.
///
/// An index may appear multiple times.
pub fn take(arr: &Array2, indices: &[usize]) -> Result<Array2> {
    Ok(match arr {
        Array2::Null(_) => Array2::Null(NullArray::new(indices.len())),
        Array2::Boolean(arr) => Array2::Boolean(take_boolean(arr, indices)?),
        Array2::Float32(arr) => Array2::Float32(take_primitive(arr, indices)?),
        Array2::Float64(arr) => Array2::Float64(take_primitive(arr, indices)?),
        Array2::Int8(arr) => Array2::Int8(take_primitive(arr, indices)?),
        Array2::Int16(arr) => Array2::Int16(take_primitive(arr, indices)?),
        Array2::Int32(arr) => Array2::Int32(take_primitive(arr, indices)?),
        Array2::Int64(arr) => Array2::Int64(take_primitive(arr, indices)?),
        Array2::UInt8(arr) => Array2::UInt8(take_primitive(arr, indices)?),
        Array2::UInt16(arr) => Array2::UInt16(take_primitive(arr, indices)?),
        Array2::UInt32(arr) => Array2::UInt32(take_primitive(arr, indices)?),
        Array2::UInt64(arr) => Array2::UInt64(take_primitive(arr, indices)?),
        Array2::Decimal64(arr) => {
            let new_primitive = take_primitive(arr.get_primitive(), indices)?;
            Array2::Decimal64(DecimalArray::new(
                arr.precision(),
                arr.scale(),
                new_primitive,
            ))
        }
        Array2::Decimal128(arr) => {
            let new_primitive = take_primitive(arr.get_primitive(), indices)?;
            Array2::Decimal128(DecimalArray::new(
                arr.precision(),
                arr.scale(),
                new_primitive,
            ))
        }
        Array2::Date32(arr) => Array2::Date32(take_primitive(arr, indices)?),
        Array2::Date64(arr) => Array2::Date64(take_primitive(arr, indices)?),
        Array2::Timestamp(arr) => {
            let primitive = take_primitive(arr.get_primitive(), indices)?;
            Array2::Timestamp(TimestampArray::new(arr.unit(), primitive))
        }
        Array2::Utf8(arr) => Array2::Utf8(take_varlen(arr, indices)?),
        Array2::LargeUtf8(arr) => Array2::LargeUtf8(take_varlen(arr, indices)?),
        Array2::Binary(arr) => Array2::Binary(take_varlen(arr, indices)?),
        Array2::LargeBinary(arr) => Array2::LargeBinary(take_varlen(arr, indices)?),
        other => not_implemented!("other: {}", other.datatype()),
    })
}

pub fn take_boolean(arr: &BooleanArray, indices: &[usize]) -> Result<BooleanArray> {
    if !indices.iter().all(|&idx| idx < arr.len()) {
        return Err(RayexecError::new("Index out of bounds"));
    }

    let values = arr.values();
    let new_values = Bitmap::from_iter(indices.iter().map(|idx| values.value_unchecked(*idx)));

    let validity = arr.validity().map(|validity| {
        Bitmap::from_iter(indices.iter().map(|idx| validity.value_unchecked(*idx)))
    });

    Ok(BooleanArray::new(new_values, validity))
}

pub fn take_primitive<T: Copy>(
    arr: &PrimitiveArray<T>,
    indices: &[usize],
) -> Result<PrimitiveArray<T>> {
    if !indices.iter().all(|&idx| idx < arr.len()) {
        return Err(RayexecError::new("Index out of bounds"));
    }

    let values = arr.values();
    let new_values: Vec<_> = indices
        .iter()
        .map(|idx| *values.as_ref().get(*idx).unwrap())
        .collect();

    let validity = arr.validity().map(|validity| {
        Bitmap::from_iter(indices.iter().map(|idx| validity.value_unchecked(*idx)))
    });

    let taken = PrimitiveArray::new(new_values, validity);

    Ok(taken)
}

pub fn take_varlen<T: VarlenType2 + ?Sized, O: OffsetIndex>(
    arr: &VarlenArray<T, O>,
    indices: &[usize],
) -> Result<VarlenArray<T, O>> {
    if !indices.iter().all(|&idx| idx < arr.len()) {
        return Err(RayexecError::new("Index out of bounds"));
    }

    let new_values: VarlenValuesBuffer<_> =
        indices.iter().map(|idx| arr.value(*idx).unwrap()).collect();

    let validity = arr.validity().map(|validity| {
        Bitmap::from_iter(indices.iter().map(|idx| validity.value_unchecked(*idx)))
    });

    let taken = VarlenArray::new(new_values, validity);

    Ok(taken)
}

#[cfg(test)]
mod tests {
    use crate::array::{Int32Array, Utf8Array};

    use super::*;

    #[test]
    fn simple_take_primitive() {
        let arr = Int32Array::from_iter([6, 7, 8, 9]);
        let indices = [1, 1, 3, 0];
        let out = take_primitive(&arr, &indices).unwrap();

        let expected = Int32Array::from_iter([7, 7, 9, 6]);
        assert_eq!(expected, out);
    }

    #[test]
    fn take_primitive_out_of_bounds() {
        let arr = Int32Array::from_iter([6, 7, 8, 9]);
        let indices = [1, 1, 3, 4];

        let _ = take_primitive(&arr, &indices).unwrap_err();
    }

    #[test]
    fn simple_take_varlen() {
        let arr = Utf8Array::from_iter(["aaa", "bbb", "ccc", "ddd"]);
        let indices = [1, 1, 3, 0];
        let out = take_varlen(&arr, &indices).unwrap();

        let expected = Utf8Array::from_iter(["bbb", "bbb", "ddd", "aaa"]);
        assert_eq!(expected, out);
    }

    #[test]
    fn take_varlen_out_of_bounds() {
        let arr = Utf8Array::from_iter(["aaa", "bbb", "ccc", "ddd"]);
        let indices = [1, 1, 3, 4];

        let _ = take_varlen(&arr, &indices).unwrap_err();
    }
}
