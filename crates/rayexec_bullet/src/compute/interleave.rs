use crate::{
    array::{Array, ArrayBuilder, PrimitiveArray, PrimitiveArrayBuilder},
    bitmap::Bitmap,
};
use rayexec_error::{RayexecError, Result};

pub fn interleave(arrays: &[&Array], indices: &[(usize, usize)]) -> Result<Array> {
    unimplemented!()
}

pub fn interleave_primitive<T: Copy>(
    arrays: &[&PrimitiveArray<T>],
    indices: &[(usize, usize)],
) -> Result<PrimitiveArray<T>> {
    // Build out validities bitmap.

    // Merge arrays values.
    let mut builder = PrimitiveArrayBuilder::with_capacity(indices.len());
    for (arr_idx, row_idx) in indices {
        let v = arrays[*arr_idx].value(*row_idx).expect("row to exist");
        builder.push_value(*v);
    }

    let validities: Vec<_> = arrays.iter().map(|arr| arr.validity()).collect();
    if let Some(validity) = interleave_validities(&validities, indices) {
        builder.put_validity(validity);
    }

    Ok(builder.into_typed_array())
}

fn interleave_validities(
    validities: &[Option<&Bitmap>],
    indices: &[(usize, usize)],
) -> Option<Bitmap> {
    let all_none = validities.iter().all(|v| v.is_none());
    if all_none {
        return None;
    }

    let mut validity = Bitmap::default();
    for (arr_idx, row_idx) in indices {
        let v = validities[*arr_idx]
            .map(|bm| bm.value(*row_idx))
            .unwrap_or(true);

        validity.push(v);
    }

    Some(validity)
}

#[cfg(test)]
mod tests {
    use crate::array::Int32Array;

    use super::*;

    #[test]
    fn simple_interleave_primitivie() {
        let arr1 = Int32Array::from_iter([1, 2, 3]);
        let arr2 = Int32Array::from_iter([4, 5, 6, 7]);

        #[rustfmt::skip]
        let indices = vec![
            (0, 0),
            (1, 2),
            (1, 3),
            (0, 2),
            (0, 1),
        ];

        let out = interleave_primitive(&[&arr1, &arr2], &indices).unwrap();

        let expected = Int32Array::from_iter([1, 6, 7, 3, 2]);
        assert_eq!(expected, out);
    }
}
