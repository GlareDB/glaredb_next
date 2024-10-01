use crate::{
    array::{
        Array2, ArrayAccessor, BooleanArray, BooleanValuesBuffer, Decimal128Array, Decimal64Array,
        OffsetIndex, PrimitiveArray, TimestampArray, ValuesBuffer, VarlenArray, VarlenType,
        VarlenValuesBuffer,
    },
    bitmap::Bitmap,
};
use rayexec_error::{not_implemented, RayexecError, Result};

/// Slice an array at the given range.
///
/// Not zero-copy.
///
/// A full zero-copy implementation will come in the future and may make use of
/// "view" type arrays.
pub fn slice(arr: &Array2, start: usize, count: usize) -> Result<Array2> {
    Ok(match arr {
        Array2::Null(_) => not_implemented!("slice null array"), // TODO
        Array2::Boolean(arr) => Array2::Boolean(slice_boolean(arr, start, count)?),
        Array2::Float32(arr) => Array2::Float32(slice_primitive(arr, start, count)?),
        Array2::Float64(arr) => Array2::Float64(slice_primitive(arr, start, count)?),
        Array2::Int8(arr) => Array2::Int8(slice_primitive(arr, start, count)?),
        Array2::Int16(arr) => Array2::Int16(slice_primitive(arr, start, count)?),
        Array2::Int32(arr) => Array2::Int32(slice_primitive(arr, start, count)?),
        Array2::Int64(arr) => Array2::Int64(slice_primitive(arr, start, count)?),
        Array2::Int128(arr) => Array2::Int128(slice_primitive(arr, start, count)?),
        Array2::UInt8(arr) => Array2::UInt8(slice_primitive(arr, start, count)?),
        Array2::UInt16(arr) => Array2::UInt16(slice_primitive(arr, start, count)?),
        Array2::UInt32(arr) => Array2::UInt32(slice_primitive(arr, start, count)?),
        Array2::UInt64(arr) => Array2::UInt64(slice_primitive(arr, start, count)?),
        Array2::UInt128(arr) => Array2::UInt128(slice_primitive(arr, start, count)?),
        Array2::Decimal64(arr) => {
            let primitive = slice_primitive(arr.get_primitive(), start, count)?;
            Array2::Decimal64(Decimal64Array::new(arr.precision(), arr.scale(), primitive))
        }
        Array2::Decimal128(arr) => {
            let primitive = slice_primitive(arr.get_primitive(), start, count)?;
            Array2::Decimal128(Decimal128Array::new(
                arr.precision(),
                arr.scale(),
                primitive,
            ))
        }
        Array2::Date32(arr) => Array2::Date32(slice_primitive(arr, start, count)?),
        Array2::Date64(arr) => Array2::Date64(slice_primitive(arr, start, count)?),
        Array2::Timestamp(arr) => {
            let sliced = slice_primitive(arr.get_primitive(), start, count)?;
            Array2::Timestamp(TimestampArray::new(arr.unit(), sliced))
        }
        Array2::Utf8(arr) => Array2::Utf8(slice_varlen(arr, start, count)?),
        Array2::LargeUtf8(arr) => Array2::LargeUtf8(slice_varlen(arr, start, count)?),
        Array2::Binary(arr) => Array2::Binary(slice_varlen(arr, start, count)?),
        Array2::LargeBinary(arr) => Array2::LargeBinary(slice_varlen(arr, start, count)?),
        other => not_implemented!("slice array {}", other.datatype()),
    })
}

pub fn slice_boolean(arr: &BooleanArray, start: usize, count: usize) -> Result<BooleanArray> {
    if start + count > arr.len() {
        return Err(RayexecError::new(format!(
            "Range end out of bounds, start: {start}, count: {count}, len: {}",
            arr.len()
        )));
    }

    let mut buffer = BooleanValuesBuffer::with_capacity(count);
    arr.values_iter()
        .skip(start)
        .take(count)
        .for_each(|val| buffer.push_value(val));

    let validity = arr
        .validity()
        .map(|validity| Bitmap::from_iter(validity.iter().skip(start).take(count)));

    Ok(BooleanArray::new(buffer, validity))
}

pub fn slice_primitive<T: Copy + Default>(
    arr: &PrimitiveArray<T>,
    start: usize,
    count: usize,
) -> Result<PrimitiveArray<T>> {
    if start + count > arr.len() {
        return Err(RayexecError::new(format!(
            "Range end out of bounds, start: {start}, count: {count}, len: {}",
            arr.len()
        )));
    }

    let vals = arr.values_iter();

    let mut buffer = Vec::with_capacity(arr.len());
    vals.skip(start)
        .take(count)
        .for_each(|val| buffer.push_value(val));

    let validity = arr
        .validity()
        .map(|validity| Bitmap::from_iter(validity.iter().skip(start).take(count)));

    Ok(PrimitiveArray::new(buffer, validity))
}

pub fn slice_varlen<T: VarlenType + ?Sized, O: OffsetIndex>(
    arr: &VarlenArray<T, O>,
    start: usize,
    count: usize,
) -> Result<VarlenArray<T, O>> {
    if start + count > arr.len() {
        return Err(RayexecError::new(format!(
            "Range end out of bounds, start: {start}, count: {count}, len: {}",
            arr.len()
        )));
    }

    let vals = arr.values_iter();

    let mut buffer = VarlenValuesBuffer::default();
    vals.skip(start)
        .take(count)
        .for_each(|val| buffer.push_value(val));

    let validity = arr
        .validity()
        .map(|validity| Bitmap::from_iter(validity.iter().skip(start).take(count)));

    Ok(VarlenArray::new(buffer, validity))
}

#[cfg(test)]
mod tests {
    use crate::array::{Int32Array, Utf8Array};

    use super::*;

    #[test]
    fn slice_primitive_from_middle() {
        let arr = Int32Array::from_iter([1, 2, 3, 4]);
        let out = slice_primitive(&arr, 1, 2).unwrap();

        let expected = Int32Array::from_iter([2, 3]);
        assert_eq!(expected, out);
    }

    #[test]
    fn slice_varlen_from_middle() {
        let arr = Utf8Array::from_iter(["hello", "world", "goodbye", "world"]);
        let out = slice_varlen(&arr, 1, 2).unwrap();

        let expected = Utf8Array::from_iter(["world", "goodbye"]);
        assert_eq!(expected, out);
    }
}
