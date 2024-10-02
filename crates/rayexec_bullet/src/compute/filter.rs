use crate::{
    array::{
        Array2, BooleanArray, Decimal128Array, Decimal64Array, NullArray, OffsetIndex,
        PrimitiveArray, TimestampArray, VarlenArray, VarlenType2, VarlenValuesBuffer,
    },
    bitmap::Bitmap,
};
use rayexec_error::{not_implemented, RayexecError, Result};

use super::util::IntoExtactSizeIterator;

/// A trait for determining which rows should be selected during a filter.
///
/// This implements `Copy`, as the iterator needs to be ran twice, once for the
/// values and once for the validity. The `Copy` essentially enforces that we
/// only pass in a reference which we can use to create two separate iterators
/// for the same data.
pub trait FilterSelection: IntoExtactSizeIterator<Item = bool> + Copy {
    /// Returns the exact size of the iterator that will be created after a call
    /// to `into_iter`.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Bitmap can be used to filter.
impl FilterSelection for &Bitmap {
    fn len(&self) -> usize {
        Bitmap::len(self)
    }
}

// TODO: Probably make this just accept an iterator.
pub fn filter(arr: &Array2, selection: impl FilterSelection) -> Result<Array2> {
    Ok(match arr {
        Array2::Null(_) => {
            let len = selection.into_iter().filter(|&b| b).count();
            Array2::Null(NullArray::new(len))
        }
        Array2::Boolean(arr) => Array2::Boolean(filter_boolean(arr, selection)?),
        Array2::Float32(arr) => Array2::Float32(filter_primitive(arr, selection)?),
        Array2::Float64(arr) => Array2::Float64(filter_primitive(arr, selection)?),
        Array2::Int8(arr) => Array2::Int8(filter_primitive(arr, selection)?),
        Array2::Int16(arr) => Array2::Int16(filter_primitive(arr, selection)?),
        Array2::Int32(arr) => Array2::Int32(filter_primitive(arr, selection)?),
        Array2::Int64(arr) => Array2::Int64(filter_primitive(arr, selection)?),
        Array2::Int128(arr) => Array2::Int128(filter_primitive(arr, selection)?),
        Array2::UInt8(arr) => Array2::UInt8(filter_primitive(arr, selection)?),
        Array2::UInt16(arr) => Array2::UInt16(filter_primitive(arr, selection)?),
        Array2::UInt32(arr) => Array2::UInt32(filter_primitive(arr, selection)?),
        Array2::UInt64(arr) => Array2::UInt64(filter_primitive(arr, selection)?),
        Array2::UInt128(arr) => Array2::UInt128(filter_primitive(arr, selection)?),
        Array2::Decimal64(arr) => {
            let primitive = filter_primitive(arr.get_primitive(), selection)?;
            Array2::Decimal64(Decimal64Array::new(arr.precision(), arr.scale(), primitive))
        }
        Array2::Decimal128(arr) => {
            let primitive = filter_primitive(arr.get_primitive(), selection)?;
            Array2::Decimal128(Decimal128Array::new(
                arr.precision(),
                arr.scale(),
                primitive,
            ))
        }
        Array2::Date32(arr) => Array2::Date32(filter_primitive(arr, selection)?),
        Array2::Date64(arr) => Array2::Date64(filter_primitive(arr, selection)?),
        Array2::Timestamp(arr) => {
            let primitive = filter_primitive(arr.get_primitive(), selection)?;
            Array2::Timestamp(TimestampArray::new(arr.unit(), primitive))
        }
        Array2::Interval(arr) => Array2::Interval(filter_primitive(arr, selection)?),
        Array2::Utf8(arr) => Array2::Utf8(filter_varlen(arr, selection)?),
        Array2::LargeUtf8(arr) => Array2::LargeUtf8(filter_varlen(arr, selection)?),
        Array2::Binary(arr) => Array2::Binary(filter_varlen(arr, selection)?),
        Array2::LargeBinary(arr) => Array2::LargeBinary(filter_varlen(arr, selection)?),
        Array2::List(_) => not_implemented!("list filter"),
        Array2::Struct(_) => not_implemented!("struct filter"),
    })
}

pub fn filter_boolean(arr: &BooleanArray, selection: impl FilterSelection) -> Result<BooleanArray> {
    if arr.len() != selection.len() {
        return Err(RayexecError::new(format!(
            "Selection array length doesn't equal array length, got {}, want {}",
            selection.len(),
            arr.len()
        )));
    }

    let values_iter = arr.values().iter();

    let values: Bitmap = values_iter
        .zip(selection.into_iter())
        .filter_map(|(v, take)| if take { Some(v) } else { None })
        .collect();

    let validity = filter_validity(arr.validity(), selection);

    Ok(BooleanArray::new(values, validity))
}

pub fn filter_primitive<T: Copy>(
    arr: &PrimitiveArray<T>,
    selection: impl FilterSelection,
) -> Result<PrimitiveArray<T>> {
    if arr.len() != selection.len() {
        return Err(RayexecError::new(format!(
            "Selection array length doesn't equal array length, got {}, want {}",
            selection.len(),
            arr.len()
        )));
    }

    let values_iter = arr.values().as_ref().iter();

    let values: Vec<_> = values_iter
        .zip(selection.into_iter())
        .filter_map(|(v, take)| if take { Some(*v) } else { None })
        .collect();

    let validity = filter_validity(arr.validity(), selection);

    let arr = PrimitiveArray::new(values, validity);

    Ok(arr)
}

pub fn filter_varlen<T: VarlenType2 + ?Sized, O: OffsetIndex>(
    arr: &VarlenArray<T, O>,
    selection: impl FilterSelection,
) -> Result<VarlenArray<T, O>> {
    if arr.len() != selection.len() {
        return Err(RayexecError::new(format!(
            "Selection array length doesn't equal array length, got {}, want {}",
            selection.len(),
            arr.len()
        )));
    }

    let values_iter = arr.values_iter();

    let values: VarlenValuesBuffer<O> = values_iter
        .zip(selection.into_iter())
        .filter_map(|(v, take)| if take { Some(v) } else { None })
        .collect();

    let validity = filter_validity(arr.validity(), selection);

    let arr = VarlenArray::new(values, validity);

    Ok(arr)
}

fn filter_validity(validity: Option<&Bitmap>, selection: impl FilterSelection) -> Option<Bitmap> {
    validity.map(|validity| {
        validity
            .iter()
            .zip(selection.into_iter())
            .filter_map(|(v, take)| if take { Some(v) } else { None })
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use crate::array::{Int32Array, Utf8Array};

    use super::*;

    #[test]
    fn simple_filter_primitive() {
        let arr = Int32Array::from_iter([6, 7, 8, 9]);
        let selection = Bitmap::from_iter([true, false, true, false]);

        let filtered = filter_primitive(&arr, &selection).unwrap();
        let expected = Int32Array::from_iter([6, 8]);
        assert_eq!(expected, filtered);
    }

    #[test]
    fn simple_filter_varlen() {
        let arr = Utf8Array::from_iter(["aaa", "bbb", "ccc", "ddd"]);
        let selection = Bitmap::from_iter([true, false, true, false]);

        let filtered = filter_varlen(&arr, &selection).unwrap();
        let expected = Utf8Array::from_iter(["aaa", "ccc"]);
        assert_eq!(expected, filtered);
    }

    #[test]
    fn filter_primitive_with_nulls() {
        let arr = Int32Array::from_iter([Some(6), Some(7), None, None]);
        let selection = Bitmap::from_iter([true, false, true, false]);

        let filtered = filter_primitive(&arr, &selection).unwrap();
        let expected = Int32Array::from_iter([Some(6), None]);
        assert_eq!(expected, filtered);
    }
}
