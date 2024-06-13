use crate::{
    array::{Array, PrimitiveArray},
    field::DataType,
};
use num::{NumCast, ToPrimitive};
use rayexec_error::{RayexecError, Result};
use std::fmt;

pub fn cast(arr: &Array, to: &DataType) -> Result<Array> {
    Ok(match (arr, to) {
        // Primitive numeric casts
        // From UInt8
        (Array::UInt8(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::UInt8(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        // From UInt16
        (Array::UInt16(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::UInt16(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        // From UInt32
        (Array::UInt32(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::UInt32(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        // From UInt64
        (Array::UInt64(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::UInt64(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        // From Int8
        (Array::Int8(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::Int8(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        // From Int16
        (Array::Int16(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::Int16(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        // From Int32
        (Array::Int32(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::Int32(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        // From Int64
        (Array::Int64(arr), DataType::Int8) => Array::Int8(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::Int16) => Array::Int16(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::Int32) => Array::Int32(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::Int64) => Array::Int64(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::Float32) => Array::Float32(cast_primitive_numeric(arr)?),
        (Array::Int64(arr), DataType::Float64) => Array::Float64(cast_primitive_numeric(arr)?),
        (arr, to) => {
            return Err(RayexecError::new(format!(
                "Unable to cast from {} to {to}",
                arr.datatype(),
            )))
        }
    })
}

/// Fallibly cast from primitive type A to primitive type B.
pub fn cast_primitive_numeric<A, B>(arr: &PrimitiveArray<A>) -> Result<PrimitiveArray<B>>
where
    A: Copy + ToPrimitive + fmt::Display,
    B: NumCast,
{
    let mut new_vals = Vec::with_capacity(arr.len());
    for val in arr.values().as_ref().iter() {
        new_vals
            .push(B::from(*val).ok_or_else(|| RayexecError::new(format!("Failed to cast {val}")))?);
    }

    Ok(match arr.validity() {
        Some(validity) => PrimitiveArray::new_from_values_and_validity(new_vals, validity.clone()),
        None => PrimitiveArray::from(new_vals),
    })
}
