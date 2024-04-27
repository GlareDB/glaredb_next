use crate::{
    array::{Array, PrimitiveArray, PrimitiveNumeric, Utf8Array},
    datatype::DataType,
    scalar::ScalarValue,
};
use rayexec_error::{RayexecError, Result};

pub fn cast(arr: Array, to: DataType) -> Result<Array> {
    if arr.datatype() == to {
        return Ok(arr);
    }

    Ok(match (arr, to) {
        // Nulls casted to anything always returns nulls.
        (Array::Null(arr), _) => Array::Null(arr),
        (Array::Utf8(arr), to) if to.is_numeric() => match to {
            DataType::Float32 => Array::Float32(utf8_array_to_numeric(arr)?),
            DataType::Float64 => Array::Float64(utf8_array_to_numeric(arr)?),
            DataType::Int8 => Array::Int8(utf8_array_to_numeric(arr)?),
            DataType::Int16 => Array::Int16(utf8_array_to_numeric(arr)?),
            DataType::Int32 => Array::Int32(utf8_array_to_numeric(arr)?),
            DataType::Int64 => Array::Int64(utf8_array_to_numeric(arr)?),
            DataType::UInt8 => Array::UInt8(utf8_array_to_numeric(arr)?),
            DataType::UInt16 => Array::UInt16(utf8_array_to_numeric(arr)?),
            DataType::UInt32 => Array::UInt32(utf8_array_to_numeric(arr)?),
            DataType::UInt64 => Array::UInt64(utf8_array_to_numeric(arr)?),
            _ => unreachable!(),
        },
        (arr, to) => {
            return Err(RayexecError::new(format!(
                "Unhandled cast for array of type {:?} to {:?}",
                arr.datatype(),
                to
            )))
        }
    })
}

pub fn cast_scalar(scalar: ScalarValue, to: DataType) -> Result<ScalarValue> {
    if scalar.datatype() == to {
        return Ok(scalar);
    }

    Ok(match (scalar, to) {
        (ScalarValue::Null, _) => ScalarValue::Null,
        (ScalarValue::Utf8(val), to) if to.is_numeric() => match to {
            DataType::Float32 => ScalarValue::Float32(utf8_scalar_to_numeric(val)?),
            DataType::Float64 => ScalarValue::Float64(utf8_scalar_to_numeric(val)?),
            DataType::Int8 => ScalarValue::Int8(utf8_scalar_to_numeric(val)?),
            DataType::Int16 => ScalarValue::Int16(utf8_scalar_to_numeric(val)?),
            DataType::Int32 => ScalarValue::Int32(utf8_scalar_to_numeric(val)?),
            DataType::Int64 => ScalarValue::Int64(utf8_scalar_to_numeric(val)?),
            DataType::UInt8 => ScalarValue::UInt8(utf8_scalar_to_numeric(val)?),
            DataType::UInt16 => ScalarValue::UInt16(utf8_scalar_to_numeric(val)?),
            DataType::UInt32 => ScalarValue::UInt32(utf8_scalar_to_numeric(val)?),
            DataType::UInt64 => ScalarValue::UInt64(utf8_scalar_to_numeric(val)?),
            _ => unreachable!(),
        },
        (arr, to) => {
            return Err(RayexecError::new(format!(
                "Unhandled cast for scalar of type {:?} to {:?}",
                arr.datatype(),
                to
            )))
        }
    })
}

fn utf8_scalar_to_numeric<T: PrimitiveNumeric>(val: impl AsRef<str>) -> Result<T> {
    let val = val.as_ref();
    T::from_str(val).ok_or_else(|| RayexecError::new(format!("Unable to cast '{val}' to a number")))
}

/// Cast an array of utf strings to a numeric array.
// TODO: Change types to support large utf8 as well.
fn utf8_array_to_numeric<T: PrimitiveNumeric>(arr: Utf8Array) -> Result<PrimitiveArray<T>> {
    let mut values: Vec<T> = Vec::with_capacity(arr.len());
    for val in arr.values_iter() {
        // TODO: If null...
        // TODO: Allow ignoring error? (implicit cast to null?)

        let val = T::from_str(val)
            .ok_or_else(|| RayexecError::new(format!("Unable to cast '{val}' to a number")))?;

        values.push(val);
    }

    // TODO: Nulls

    Ok(PrimitiveArray::from(values))
}

#[cfg(test)]
mod tests {
    use crate::array::{Float32Array, Int32Array};

    use super::*;

    #[test]
    fn utf8_arr_to_float() {
        let arr = Array::Utf8(Utf8Array::from_iter(["1", "2.1", "3.5"]));
        let out = cast(arr.into(), DataType::Float32).unwrap();

        assert_eq!(
            Array::Float32(Float32Array::from_iter([1.0, 2.1, 3.5])),
            out
        )
    }

    #[test]
    fn utf8_arr_to_int() {
        let arr = Array::Utf8(Utf8Array::from_iter(["1", "2", "3"]));
        let out = cast(arr.into(), DataType::Int32).unwrap();

        assert_eq!(Array::Int32(Int32Array::from_iter([1, 2, 3])), out)
    }
}
