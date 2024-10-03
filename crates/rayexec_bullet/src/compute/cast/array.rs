use crate::{
    array::{
        Array, Array2, ArrayAccessor, ArrayData, BooleanArray, BooleanValuesBuffer,
        Decimal128Array, Decimal64Array, DecimalArray, OffsetIndex, PrimitiveArray, ValuesBuffer,
        VarlenArray, VarlenValuesBuffer,
    },
    datatype::{DataType, TimeUnit},
    executor::{
        builder::{ArrayBuilder, BooleanBuffer, GermanVarlenBuffer, PrimitiveBuffer},
        physical_type::{
            PhysicalBool, PhysicalF32, PhysicalF64, PhysicalI128, PhysicalI16, PhysicalI32,
            PhysicalI64, PhysicalI8, PhysicalStorage, PhysicalU128, PhysicalU16, PhysicalU32,
            PhysicalU64, PhysicalU8, PhysicalUtf8,
        },
        scalar::{UnaryExecutor, UnaryExecutor2},
    },
    scalar::decimal::{Decimal128Type, Decimal64Type, DecimalPrimitive, DecimalType},
    storage::{AddressableStorage, PrimitiveStorage},
};
use num::{CheckedDiv, CheckedMul, Float, NumCast, PrimInt, ToPrimitive};
use rayexec_error::{RayexecError, Result};
use std::{
    borrow::Cow,
    fmt::{self, Display},
    ops::{Div, Mul},
};

use super::{
    behavior::CastFailBehavior,
    format::{
        BoolFormatter, Decimal128Formatter, Decimal64Formatter, Float32Formatter, Float64Formatter,
        Formatter, Int128Formatter, Int16Formatter, Int32Formatter, Int64Formatter, Int8Formatter,
        TimestampMicrosecondsFormatter, TimestampMillisecondsFormatter,
        TimestampNanosecondsFormatter, TimestampSecondsFormatter, UInt128Formatter,
        UInt16Formatter, UInt32Formatter, UInt64Formatter, UInt8Formatter,
    },
    parse::{
        BoolParser, Date32Parser, Decimal128Parser, Decimal64Parser, Float32Parser, Float64Parser,
        Int128Parser, Int16Parser, Int32Parser, Int64Parser, Int8Parser, IntervalParser, Parser,
        UInt128Parser, UInt16Parser, UInt32Parser, UInt64Parser, UInt8Parser,
    },
};

pub fn cast_array<'a>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Cow<'a, Array>> {
    if arr.datatype() == &to {
        return Ok(Cow::Borrowed(arr));
    }

    let arr = match arr.datatype() {
        // String to anything else.
        DataType::Utf8 | DataType::LargeUtf8 => cast_from_utf8(arr, to, behavior)?,

        // Primitive numerics to other primitive numerics.
        DataType::Int8 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI8>(arr, to, behavior)?
        }
        DataType::Int16 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI16>(arr, to, behavior)?
        }
        DataType::Int32 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI32>(arr, to, behavior)?
        }
        DataType::Int64 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI64>(arr, to, behavior)?
        }
        DataType::Int128 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalI128>(arr, to, behavior)?
        }
        DataType::UInt8 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU8>(arr, to, behavior)?
        }
        DataType::UInt16 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU16>(arr, to, behavior)?
        }
        DataType::UInt32 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU32>(arr, to, behavior)?
        }
        DataType::UInt64 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU64>(arr, to, behavior)?
        }
        DataType::UInt128 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalU128>(arr, to, behavior)?
        }
        DataType::Float32 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalF32>(arr, to, behavior)?
        }
        DataType::Float64 if to.is_primitive_numeric() => {
            cast_primitive_numeric_helper::<PhysicalF64>(arr, to, behavior)?
        }

        // Int to decimal.
        DataType::Int8 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI8>(arr, to, behavior)?
        }
        DataType::Int16 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI16>(arr, to, behavior)?
        }
        DataType::Int32 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI32>(arr, to, behavior)?
        }
        DataType::Int64 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI64>(arr, to, behavior)?
        }
        DataType::Int128 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalI128>(arr, to, behavior)?
        }
        DataType::UInt8 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU8>(arr, to, behavior)?
        }
        DataType::UInt16 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU16>(arr, to, behavior)?
        }
        DataType::UInt32 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU32>(arr, to, behavior)?
        }
        DataType::UInt64 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU64>(arr, to, behavior)?
        }
        DataType::UInt128 if to.is_decimal() => {
            cast_int_to_decimal_helper::<PhysicalU128>(arr, to, behavior)?
        }

        // Float to decimal.
        DataType::Float32 if to.is_decimal() => {
            cast_float_to_decimal_helper::<PhysicalF32>(arr, to, behavior)?
        }
        DataType::Float64 if to.is_decimal() => {
            cast_float_to_decimal_helper::<PhysicalF64>(arr, to, behavior)?
        }

        // Decimal to decimal
        DataType::Decimal64(_) if to.is_decimal() => {
            decimal_rescale_helper::<PhysicalI64>(arr, to, behavior)?
        }
        DataType::Decimal128(_) if to.is_decimal() => {
            decimal_rescale_helper::<PhysicalI128>(arr, to, behavior)?
        }

        // Anything to string.
        _ if to.is_utf8() => cast_to_utf8(arr, behavior)?,

        other => {
            return Err(RayexecError::new(format!(
                "Casting from {other} to {to} not implemented"
            )))
        }
    };

    Ok(Cow::Owned(arr))
}

fn decimal_rescale_helper<'a, S>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage<'a>,
    <S::Storage as AddressableStorage>::T: PrimInt,
{
    match to {
        DataType::Decimal64(_) => decimal_rescale::<S, Decimal64Type>(arr, to, behavior),
        DataType::Decimal128(_) => decimal_rescale::<S, Decimal128Type>(arr, to, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

pub fn decimal_rescale<'a, S, D>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage<'a>,
    D: DecimalType,
    <S::Storage as AddressableStorage>::T: PrimInt,
    ArrayData: From<PrimitiveStorage<D::Primitive>>,
{
    let new_meta = to.try_get_decimal_type_meta()?;
    let arr_meta = arr.datatype().try_get_decimal_type_meta()?;

    let scale_amount = <D::Primitive as NumCast>::from(
        10.pow((arr_meta.scale - new_meta.scale).unsigned_abs() as u32),
    )
    .expect("to be in range");

    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<S, _, _>(
        arr,
        ArrayBuilder {
            datatype: to,
            buffer: PrimitiveBuffer::with_len(arr.logical_len()),
        },
        |v, buf| {
            // Convert to decimal primitive.
            let v = match <D::Primitive as NumCast>::from(v) {
                Some(v) => v,
                None => {
                    fail_state.set_did_fail(buf.idx);
                    return;
                }
            };

            if arr_meta.scale < new_meta.scale {
                match v.checked_mul(&scale_amount) {
                    Some(v) => buf.put(&v),
                    None => fail_state.set_did_fail(buf.idx),
                }
            } else {
                match v.checked_div(&scale_amount) {
                    Some(v) => buf.put(&v),
                    None => fail_state.set_did_fail(buf.idx),
                }
            }
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

fn cast_float_to_decimal_helper<'a, S>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage<'a>,
    <S::Storage as AddressableStorage>::T: Float,
{
    match to {
        DataType::Decimal64(_) => cast_float_to_decimal::<S, Decimal64Type>(arr, to, behavior),
        DataType::Decimal128(_) => cast_float_to_decimal::<S, Decimal128Type>(arr, to, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

fn cast_float_to_decimal<'a, S, D>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage<'a>,
    D: DecimalType,
    <S::Storage as AddressableStorage>::T: Float,
    ArrayData: From<PrimitiveStorage<D::Primitive>>,
{
    let scale = to.try_get_decimal_type_meta()?.scale;
    let scale = <<S::Storage as AddressableStorage>::T as NumCast>::from(
        10.pow(scale.unsigned_abs() as u32),
    )
    .ok_or_else(|| RayexecError::new(format!("Failed to cast scale {scale} to float")))?;

    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<S, _, _>(
        arr,
        ArrayBuilder {
            datatype: to,
            buffer: PrimitiveBuffer::with_len(arr.logical_len()),
        },
        |v, buf| {
            // TODO: Properly handle negative scale.
            let scaled_value = v.mul(scale).round();

            match <D::Primitive as NumCast>::from(scaled_value) {
                Some(v) => buf.put(&v),
                None => fail_state.set_did_fail(buf.idx),
            }
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

fn cast_int_to_decimal_helper<'a, S>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage<'a>,
    <S::Storage as AddressableStorage>::T: PrimInt,
{
    match to {
        DataType::Decimal64(_) => cast_int_to_decimal::<S, Decimal64Type>(arr, to, behavior),
        DataType::Decimal128(_) => cast_int_to_decimal::<S, Decimal128Type>(arr, to, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

fn cast_int_to_decimal<'a, S, D>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage<'a>,
    D: DecimalType,
    <S::Storage as AddressableStorage>::T: PrimInt,
    ArrayData: From<PrimitiveStorage<D::Primitive>>,
{
    let scale = to.try_get_decimal_type_meta()?.scale;
    let scale_amount = <D::Primitive as NumCast>::from(10.pow(scale.unsigned_abs() as u32))
        .expect("to be in range");

    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<S, _, _>(
        arr,
        ArrayBuilder {
            datatype: to,
            buffer: PrimitiveBuffer::with_len(arr.logical_len()),
        },
        |v, buf| {
            // Convert to decimal primitive.
            let v = match <D::Primitive as NumCast>::from(v) {
                Some(v) => v,
                None => {
                    fail_state.set_did_fail(buf.idx);
                    return;
                }
            };

            // Scale.
            if scale > 0 {
                match v.checked_mul(&scale_amount) {
                    Some(v) => buf.put(&v),
                    None => fail_state.set_did_fail(buf.idx),
                }
            } else {
                match v.checked_div(&scale_amount) {
                    Some(v) => buf.put(&v),
                    None => fail_state.set_did_fail(buf.idx),
                }
            }
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

fn cast_primitive_numeric_helper<'a, S>(
    arr: &'a Array,
    to: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage<'a>,
    <S::Storage as AddressableStorage>::T: ToPrimitive,
{
    match to {
        DataType::Int8 => cast_primitive_numeric::<S, i8>(arr, to, behavior),
        DataType::Int16 => cast_primitive_numeric::<S, i16>(arr, to, behavior),
        DataType::Int32 => cast_primitive_numeric::<S, i32>(arr, to, behavior),
        DataType::Int64 => cast_primitive_numeric::<S, i64>(arr, to, behavior),
        DataType::Int128 => cast_primitive_numeric::<S, i128>(arr, to, behavior),
        DataType::UInt8 => cast_primitive_numeric::<S, u8>(arr, to, behavior),
        DataType::UInt16 => cast_primitive_numeric::<S, u16>(arr, to, behavior),
        DataType::UInt32 => cast_primitive_numeric::<S, u32>(arr, to, behavior),
        DataType::UInt64 => cast_primitive_numeric::<S, u64>(arr, to, behavior),
        DataType::UInt128 => cast_primitive_numeric::<S, u128>(arr, to, behavior),
        DataType::Float32 => cast_primitive_numeric::<S, f32>(arr, to, behavior),
        DataType::Float64 => cast_primitive_numeric::<S, f64>(arr, to, behavior),
        other => Err(RayexecError::new(format!("Unhandled data type: {other}"))),
    }
}

pub fn cast_primitive_numeric<'a, S, T>(
    arr: &'a Array,
    datatype: DataType,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage<'a>,
    <S::Storage as AddressableStorage>::T: ToPrimitive,
    T: NumCast + Default + Copy,
    ArrayData: From<PrimitiveStorage<T>>,
{
    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<S, _, _>(
        arr,
        ArrayBuilder {
            datatype,
            buffer: PrimitiveBuffer::with_len(arr.logical_len()),
        },
        |v, buf| match T::from(v) {
            Some(v) => buf.put(&v),
            None => fail_state.set_did_fail(buf.idx),
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

pub fn cast_from_utf8(
    arr: &Array,
    datatype: DataType,
    behavior: CastFailBehavior,
) -> Result<Array> {
    match datatype {
        DataType::Boolean => cast_parse_bool(arr, behavior),
        DataType::Int8 => cast_parse_primitive(arr, datatype, behavior, Int8Parser::default()),
        DataType::Int16 => cast_parse_primitive(arr, datatype, behavior, Int16Parser::default()),
        DataType::Int32 => cast_parse_primitive(arr, datatype, behavior, Int32Parser::default()),
        DataType::Int64 => cast_parse_primitive(arr, datatype, behavior, Int64Parser::default()),
        DataType::Int128 => cast_parse_primitive(arr, datatype, behavior, Int128Parser::default()),
        DataType::UInt8 => cast_parse_primitive(arr, datatype, behavior, UInt8Parser::default()),
        DataType::UInt16 => cast_parse_primitive(arr, datatype, behavior, UInt16Parser::default()),
        DataType::UInt32 => cast_parse_primitive(arr, datatype, behavior, UInt32Parser::default()),
        DataType::UInt64 => cast_parse_primitive(arr, datatype, behavior, UInt64Parser::default()),
        DataType::UInt128 => {
            cast_parse_primitive(arr, datatype, behavior, UInt128Parser::default())
        }
        DataType::Float32 => {
            cast_parse_primitive(arr, datatype, behavior, Float32Parser::default())
        }
        DataType::Float64 => {
            cast_parse_primitive(arr, datatype, behavior, Float64Parser::default())
        }
        DataType::Decimal64(m) => cast_parse_primitive(
            arr,
            datatype,
            behavior,
            Decimal64Parser::new(m.precision, m.scale),
        ),
        DataType::Decimal128(m) => cast_parse_primitive(
            arr,
            datatype,
            behavior,
            Decimal128Parser::new(m.precision, m.scale),
        ),
        DataType::Date32 => cast_parse_primitive(arr, datatype, behavior, Date32Parser),
        DataType::Interval => {
            cast_parse_primitive(arr, datatype, behavior, IntervalParser::default())
        }
        other => {
            return Err(RayexecError::new(format!(
                "Unable to cast utf8 array to {other}"
            )))
        }
    }
}

pub fn cast_to_utf8(arr: &Array, behavior: CastFailBehavior) -> Result<Array> {
    match arr.datatype() {
        DataType::Boolean => {
            cast_format::<PhysicalBool, _>(arr, BoolFormatter::default(), behavior)
        }
        DataType::Int8 => cast_format::<PhysicalI8, _>(arr, Int8Formatter::default(), behavior),
        DataType::Int16 => cast_format::<PhysicalI16, _>(arr, Int16Formatter::default(), behavior),
        DataType::Int32 => cast_format::<PhysicalI32, _>(arr, Int32Formatter::default(), behavior),
        DataType::Int64 => cast_format::<PhysicalI64, _>(arr, Int64Formatter::default(), behavior),
        DataType::Int128 => {
            cast_format::<PhysicalI128, _>(arr, Int128Formatter::default(), behavior)
        }
        DataType::UInt8 => cast_format::<PhysicalU8, _>(arr, UInt8Formatter::default(), behavior),
        DataType::UInt16 => {
            cast_format::<PhysicalU16, _>(arr, UInt16Formatter::default(), behavior)
        }
        DataType::UInt32 => {
            cast_format::<PhysicalU32, _>(arr, UInt32Formatter::default(), behavior)
        }
        DataType::UInt64 => {
            cast_format::<PhysicalU64, _>(arr, UInt64Formatter::default(), behavior)
        }
        DataType::UInt128 => {
            cast_format::<PhysicalU128, _>(arr, UInt128Formatter::default(), behavior)
        }
        DataType::Float32 => {
            cast_format::<PhysicalF32, _>(arr, Float32Formatter::default(), behavior)
        }
        DataType::Float64 => {
            cast_format::<PhysicalF64, _>(arr, Float64Formatter::default(), behavior)
        }
        DataType::Decimal64(m) => cast_format::<PhysicalI64, _>(
            arr,
            Decimal64Formatter::new(m.precision, m.scale),
            behavior,
        ),
        DataType::Decimal128(m) => cast_format::<PhysicalI128, _>(
            arr,
            Decimal128Formatter::new(m.precision, m.scale),
            behavior,
        ),
        DataType::Timestamp(m) => match m.unit {
            TimeUnit::Second => {
                cast_format::<PhysicalI64, _>(arr, TimestampSecondsFormatter::default(), behavior)
            }
            TimeUnit::Millisecond => cast_format::<PhysicalI64, _>(
                arr,
                TimestampMillisecondsFormatter::default(),
                behavior,
            ),
            TimeUnit::Microsecond => cast_format::<PhysicalI64, _>(
                arr,
                TimestampMicrosecondsFormatter::default(),
                behavior,
            ),
            TimeUnit::Nanosecond => cast_format::<PhysicalI64, _>(
                arr,
                TimestampNanosecondsFormatter::default(),
                behavior,
            ),
        },
        other => {
            return Err(RayexecError::new(format!(
                "Unable to cast {other} array to utf8"
            )))
        }
    }
}

fn cast_format<'a, S, F>(
    arr: &'a Array,
    mut formatter: F,
    behavior: CastFailBehavior,
) -> Result<Array>
where
    S: PhysicalStorage<'a>,
    F: Formatter<Type = <S::Storage as AddressableStorage>::T>,
{
    let mut fail_state = behavior.new_state_for_array(arr);
    let mut string_buf = String::new();

    let output = UnaryExecutor::execute::<S, _, _>(
        arr,
        ArrayBuilder {
            datatype: DataType::Utf8,
            buffer: GermanVarlenBuffer::with_len(arr.logical_len()),
        },
        |v, buf| {
            string_buf.clear();
            match formatter.write(&v, &mut string_buf) {
                Ok(_) => buf.put(string_buf.as_str()),
                Err(_) => fail_state.set_did_fail(buf.idx),
            }
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

fn cast_parse_bool(arr: &Array, behavior: CastFailBehavior) -> Result<Array> {
    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<PhysicalUtf8, _, _>(
        arr,
        ArrayBuilder {
            datatype: DataType::Boolean,
            buffer: BooleanBuffer::with_len(arr.logical_len()),
        },
        |v, buf| match BoolParser.parse(v) {
            Some(v) => buf.put(&v),
            None => fail_state.set_did_fail(buf.idx),
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

fn cast_parse_primitive<P, T>(
    arr: &Array,
    datatype: DataType,
    behavior: CastFailBehavior,
    mut parser: P,
) -> Result<Array>
where
    T: Default + Copy,
    P: Parser<Type = T>,
    ArrayData: From<PrimitiveStorage<T>>,
{
    let mut fail_state = behavior.new_state_for_array(arr);
    let output = UnaryExecutor::execute::<PhysicalUtf8, _, _>(
        arr,
        ArrayBuilder {
            datatype: datatype.clone(),
            buffer: PrimitiveBuffer::<T>::with_len(arr.logical_len()),
        },
        |v, buf| match parser.parse(v) {
            Some(v) => buf.put(&v),
            None => fail_state.set_did_fail(buf.idx),
        },
    )?;

    fail_state.check_and_apply(arr, output)
}

/// Cast an array to some other data type.
pub fn cast_array2(arr: &Array2, to: &DataType) -> Result<Array2> {
    Ok(match (arr, to) {
        // Null to whatever.
        (Array2::Null(arr), datatype) => cast_from_null(arr.len(), datatype)?,

        // Primitive numeric casts
        // From UInt8
        (Array2::UInt8(arr), DataType::Int8) => Array2::Int8(cast_primitive_numeric2(arr)?),
        (Array2::UInt8(arr), DataType::Int16) => Array2::Int16(cast_primitive_numeric2(arr)?),
        (Array2::UInt8(arr), DataType::Int32) => Array2::Int32(cast_primitive_numeric2(arr)?),
        (Array2::UInt8(arr), DataType::Int64) => Array2::Int64(cast_primitive_numeric2(arr)?),
        (Array2::UInt8(arr), DataType::UInt8) => Array2::UInt8(cast_primitive_numeric2(arr)?),
        (Array2::UInt8(arr), DataType::UInt16) => Array2::UInt16(cast_primitive_numeric2(arr)?),
        (Array2::UInt8(arr), DataType::UInt32) => Array2::UInt32(cast_primitive_numeric2(arr)?),
        (Array2::UInt8(arr), DataType::UInt64) => Array2::UInt64(cast_primitive_numeric2(arr)?),
        (Array2::UInt8(arr), DataType::Float32) => Array2::Float32(cast_primitive_numeric2(arr)?),
        (Array2::UInt8(arr), DataType::Float64) => Array2::Float64(cast_primitive_numeric2(arr)?),
        (Array2::UInt8(arr), DataType::Decimal64(meta)) => Array2::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal2::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array2::UInt8(arr), DataType::Decimal128(meta)) => {
            Array2::Decimal128(Decimal128Array::new(
                meta.precision,
                meta.scale,
                cast_int_to_decimal2::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
            ))
        }
        // From UInt16
        (Array2::UInt16(arr), DataType::Int8) => Array2::Int8(cast_primitive_numeric2(arr)?),
        (Array2::UInt16(arr), DataType::Int16) => Array2::Int16(cast_primitive_numeric2(arr)?),
        (Array2::UInt16(arr), DataType::Int32) => Array2::Int32(cast_primitive_numeric2(arr)?),
        (Array2::UInt16(arr), DataType::Int64) => Array2::Int64(cast_primitive_numeric2(arr)?),
        (Array2::UInt16(arr), DataType::UInt8) => Array2::UInt8(cast_primitive_numeric2(arr)?),
        (Array2::UInt16(arr), DataType::UInt16) => Array2::UInt16(cast_primitive_numeric2(arr)?),
        (Array2::UInt16(arr), DataType::UInt32) => Array2::UInt32(cast_primitive_numeric2(arr)?),
        (Array2::UInt16(arr), DataType::UInt64) => Array2::UInt64(cast_primitive_numeric2(arr)?),
        (Array2::UInt16(arr), DataType::Float32) => Array2::Float32(cast_primitive_numeric2(arr)?),
        (Array2::UInt16(arr), DataType::Float64) => Array2::Float64(cast_primitive_numeric2(arr)?),
        (Array2::UInt16(arr), DataType::Decimal64(meta)) => Array2::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal2::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array2::UInt16(arr), DataType::Decimal128(meta)) => {
            Array2::Decimal128(Decimal128Array::new(
                meta.precision,
                meta.scale,
                cast_int_to_decimal2::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
            ))
        }

        // From UInt32
        (Array2::UInt32(arr), DataType::Int8) => Array2::Int8(cast_primitive_numeric2(arr)?),
        (Array2::UInt32(arr), DataType::Int16) => Array2::Int16(cast_primitive_numeric2(arr)?),
        (Array2::UInt32(arr), DataType::Int32) => Array2::Int32(cast_primitive_numeric2(arr)?),
        (Array2::UInt32(arr), DataType::Int64) => Array2::Int64(cast_primitive_numeric2(arr)?),
        (Array2::UInt32(arr), DataType::UInt8) => Array2::UInt8(cast_primitive_numeric2(arr)?),
        (Array2::UInt32(arr), DataType::UInt16) => Array2::UInt16(cast_primitive_numeric2(arr)?),
        (Array2::UInt32(arr), DataType::UInt32) => Array2::UInt32(cast_primitive_numeric2(arr)?),
        (Array2::UInt32(arr), DataType::UInt64) => Array2::UInt64(cast_primitive_numeric2(arr)?),
        (Array2::UInt32(arr), DataType::Float32) => Array2::Float32(cast_primitive_numeric2(arr)?),
        (Array2::UInt32(arr), DataType::Float64) => Array2::Float64(cast_primitive_numeric2(arr)?),
        (Array2::UInt32(arr), DataType::Decimal64(meta)) => Array2::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal2::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array2::UInt32(arr), DataType::Decimal128(meta)) => {
            Array2::Decimal128(Decimal128Array::new(
                meta.precision,
                meta.scale,
                cast_int_to_decimal2::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
            ))
        }

        // From UInt64
        (Array2::UInt64(arr), DataType::Int8) => Array2::Int8(cast_primitive_numeric2(arr)?),
        (Array2::UInt64(arr), DataType::Int16) => Array2::Int16(cast_primitive_numeric2(arr)?),
        (Array2::UInt64(arr), DataType::Int32) => Array2::Int32(cast_primitive_numeric2(arr)?),
        (Array2::UInt64(arr), DataType::Int64) => Array2::Int64(cast_primitive_numeric2(arr)?),
        (Array2::UInt64(arr), DataType::UInt8) => Array2::UInt8(cast_primitive_numeric2(arr)?),
        (Array2::UInt64(arr), DataType::UInt16) => Array2::UInt16(cast_primitive_numeric2(arr)?),
        (Array2::UInt64(arr), DataType::UInt32) => Array2::UInt32(cast_primitive_numeric2(arr)?),
        (Array2::UInt64(arr), DataType::UInt64) => Array2::UInt64(cast_primitive_numeric2(arr)?),
        (Array2::UInt64(arr), DataType::Float32) => Array2::Float32(cast_primitive_numeric2(arr)?),
        (Array2::UInt64(arr), DataType::Float64) => Array2::Float64(cast_primitive_numeric2(arr)?),
        (Array2::UInt64(arr), DataType::Decimal64(meta)) => Array2::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal2::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array2::UInt64(arr), DataType::Decimal128(meta)) => {
            Array2::Decimal128(Decimal128Array::new(
                meta.precision,
                meta.scale,
                cast_int_to_decimal2::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
            ))
        }

        // From Int8
        (Array2::Int8(arr), DataType::Int8) => Array2::Int8(cast_primitive_numeric2(arr)?),
        (Array2::Int8(arr), DataType::Int16) => Array2::Int16(cast_primitive_numeric2(arr)?),
        (Array2::Int8(arr), DataType::Int32) => Array2::Int32(cast_primitive_numeric2(arr)?),
        (Array2::Int8(arr), DataType::Int64) => Array2::Int64(cast_primitive_numeric2(arr)?),
        (Array2::Int8(arr), DataType::UInt8) => Array2::UInt8(cast_primitive_numeric2(arr)?),
        (Array2::Int8(arr), DataType::UInt16) => Array2::UInt16(cast_primitive_numeric2(arr)?),
        (Array2::Int8(arr), DataType::UInt32) => Array2::UInt32(cast_primitive_numeric2(arr)?),
        (Array2::Int8(arr), DataType::UInt64) => Array2::UInt64(cast_primitive_numeric2(arr)?),
        (Array2::Int8(arr), DataType::Float32) => Array2::Float32(cast_primitive_numeric2(arr)?),
        (Array2::Int8(arr), DataType::Float64) => Array2::Float64(cast_primitive_numeric2(arr)?),
        (Array2::Int8(arr), DataType::Decimal64(meta)) => Array2::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal2::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array2::Int8(arr), DataType::Decimal128(meta)) => {
            Array2::Decimal128(Decimal128Array::new(
                meta.precision,
                meta.scale,
                cast_int_to_decimal2::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
            ))
        }

        // From Int16
        (Array2::Int16(arr), DataType::Int8) => Array2::Int8(cast_primitive_numeric2(arr)?),
        (Array2::Int16(arr), DataType::Int16) => Array2::Int16(cast_primitive_numeric2(arr)?),
        (Array2::Int16(arr), DataType::Int32) => Array2::Int32(cast_primitive_numeric2(arr)?),
        (Array2::Int16(arr), DataType::Int64) => Array2::Int64(cast_primitive_numeric2(arr)?),
        (Array2::Int16(arr), DataType::UInt8) => Array2::UInt8(cast_primitive_numeric2(arr)?),
        (Array2::Int16(arr), DataType::UInt16) => Array2::UInt16(cast_primitive_numeric2(arr)?),
        (Array2::Int16(arr), DataType::UInt32) => Array2::UInt32(cast_primitive_numeric2(arr)?),
        (Array2::Int16(arr), DataType::UInt64) => Array2::UInt64(cast_primitive_numeric2(arr)?),
        (Array2::Int16(arr), DataType::Float32) => Array2::Float32(cast_primitive_numeric2(arr)?),
        (Array2::Int16(arr), DataType::Float64) => Array2::Float64(cast_primitive_numeric2(arr)?),
        (Array2::Int16(arr), DataType::Decimal64(meta)) => Array2::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal2::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array2::Int16(arr), DataType::Decimal128(meta)) => {
            Array2::Decimal128(Decimal128Array::new(
                meta.precision,
                meta.scale,
                cast_int_to_decimal2::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
            ))
        }

        // From Int32
        (Array2::Int32(arr), DataType::Int8) => Array2::Int8(cast_primitive_numeric2(arr)?),
        (Array2::Int32(arr), DataType::Int16) => Array2::Int16(cast_primitive_numeric2(arr)?),
        (Array2::Int32(arr), DataType::Int32) => Array2::Int32(cast_primitive_numeric2(arr)?),
        (Array2::Int32(arr), DataType::Int64) => Array2::Int64(cast_primitive_numeric2(arr)?),
        (Array2::Int32(arr), DataType::UInt8) => Array2::UInt8(cast_primitive_numeric2(arr)?),
        (Array2::Int32(arr), DataType::UInt16) => Array2::UInt16(cast_primitive_numeric2(arr)?),
        (Array2::Int32(arr), DataType::UInt32) => Array2::UInt32(cast_primitive_numeric2(arr)?),
        (Array2::Int32(arr), DataType::UInt64) => Array2::UInt64(cast_primitive_numeric2(arr)?),
        (Array2::Int32(arr), DataType::Float32) => Array2::Float32(cast_primitive_numeric2(arr)?),
        (Array2::Int32(arr), DataType::Float64) => Array2::Float64(cast_primitive_numeric2(arr)?),
        (Array2::Int32(arr), DataType::Decimal64(meta)) => Array2::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal2::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array2::Int32(arr), DataType::Decimal128(meta)) => {
            Array2::Decimal128(Decimal128Array::new(
                meta.precision,
                meta.scale,
                cast_int_to_decimal2::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
            ))
        }

        // From Int64
        (Array2::Int64(arr), DataType::Int8) => Array2::Int8(cast_primitive_numeric2(arr)?),
        (Array2::Int64(arr), DataType::Int16) => Array2::Int16(cast_primitive_numeric2(arr)?),
        (Array2::Int64(arr), DataType::Int32) => Array2::Int32(cast_primitive_numeric2(arr)?),
        (Array2::Int64(arr), DataType::Int64) => Array2::Int64(cast_primitive_numeric2(arr)?),
        (Array2::Int64(arr), DataType::UInt8) => Array2::UInt8(cast_primitive_numeric2(arr)?),
        (Array2::Int64(arr), DataType::UInt16) => Array2::UInt16(cast_primitive_numeric2(arr)?),
        (Array2::Int64(arr), DataType::UInt32) => Array2::UInt32(cast_primitive_numeric2(arr)?),
        (Array2::Int64(arr), DataType::UInt64) => Array2::UInt64(cast_primitive_numeric2(arr)?),
        (Array2::Int64(arr), DataType::Float32) => Array2::Float32(cast_primitive_numeric2(arr)?),
        (Array2::Int64(arr), DataType::Float64) => Array2::Float64(cast_primitive_numeric2(arr)?),
        (Array2::Int64(arr), DataType::Decimal64(meta)) => Array2::Decimal64(Decimal64Array::new(
            meta.precision,
            meta.scale,
            cast_int_to_decimal2::<_, Decimal64Type>(arr, meta.precision, meta.scale)?,
        )),
        (Array2::Int64(arr), DataType::Decimal128(meta)) => {
            Array2::Decimal128(Decimal128Array::new(
                meta.precision,
                meta.scale,
                cast_int_to_decimal2::<_, Decimal128Type>(arr, meta.precision, meta.scale)?,
            ))
        }

        // From Float32
        (Array2::Float32(arr), DataType::Int8) => Array2::Int8(cast_primitive_numeric2(arr)?),
        (Array2::Float32(arr), DataType::Int16) => Array2::Int16(cast_primitive_numeric2(arr)?),
        (Array2::Float32(arr), DataType::Int32) => Array2::Int32(cast_primitive_numeric2(arr)?),
        (Array2::Float32(arr), DataType::Int64) => Array2::Int64(cast_primitive_numeric2(arr)?),
        (Array2::Float32(arr), DataType::UInt8) => Array2::UInt8(cast_primitive_numeric2(arr)?),
        (Array2::Float32(arr), DataType::UInt16) => Array2::UInt16(cast_primitive_numeric2(arr)?),
        (Array2::Float32(arr), DataType::UInt32) => Array2::UInt32(cast_primitive_numeric2(arr)?),
        (Array2::Float32(arr), DataType::UInt64) => Array2::UInt64(cast_primitive_numeric2(arr)?),
        (Array2::Float32(arr), DataType::Float32) => Array2::Float32(cast_primitive_numeric2(arr)?),
        (Array2::Float32(arr), DataType::Float64) => Array2::Float64(cast_primitive_numeric2(arr)?),
        (Array2::Float32(arr), DataType::Decimal64(m)) => {
            let prim = cast_float_to_decimal2::<_, Decimal64Type>(arr, m.precision, m.scale)?;
            Array2::Decimal64(Decimal64Array::new(m.precision, m.scale, prim))
        }
        (Array2::Float32(arr), DataType::Decimal128(m)) => {
            let prim = cast_float_to_decimal2::<_, Decimal128Type>(arr, m.precision, m.scale)?;
            Array2::Decimal128(Decimal128Array::new(m.precision, m.scale, prim))
        }
        // From FLoat64
        (Array2::Float64(arr), DataType::Int8) => Array2::Int8(cast_primitive_numeric2(arr)?),
        (Array2::Float64(arr), DataType::Int16) => Array2::Int16(cast_primitive_numeric2(arr)?),
        (Array2::Float64(arr), DataType::Int32) => Array2::Int32(cast_primitive_numeric2(arr)?),
        (Array2::Float64(arr), DataType::Int64) => Array2::Int64(cast_primitive_numeric2(arr)?),
        (Array2::Float64(arr), DataType::UInt8) => Array2::UInt8(cast_primitive_numeric2(arr)?),
        (Array2::Float64(arr), DataType::UInt16) => Array2::UInt16(cast_primitive_numeric2(arr)?),
        (Array2::Float64(arr), DataType::UInt32) => Array2::UInt32(cast_primitive_numeric2(arr)?),
        (Array2::Float64(arr), DataType::UInt64) => Array2::UInt64(cast_primitive_numeric2(arr)?),
        (Array2::Float64(arr), DataType::Float32) => Array2::Float32(cast_primitive_numeric2(arr)?),
        (Array2::Float64(arr), DataType::Float64) => Array2::Float64(cast_primitive_numeric2(arr)?),
        (Array2::Float64(arr), DataType::Decimal64(m)) => {
            let prim = cast_float_to_decimal2::<_, Decimal64Type>(arr, m.precision, m.scale)?;
            Array2::Decimal64(Decimal64Array::new(m.precision, m.scale, prim))
        }
        (Array2::Float64(arr), DataType::Decimal128(m)) => {
            let prim = cast_float_to_decimal2::<_, Decimal128Type>(arr, m.precision, m.scale)?;
            Array2::Decimal128(Decimal128Array::new(m.precision, m.scale, prim))
        }

        // From Decimal
        (Array2::Decimal64(arr), DataType::Float32) => {
            Array2::Float32(cast_decimal_to_float::<_, Decimal64Type>(arr)?)
        }
        (Array2::Decimal64(arr), DataType::Float64) => {
            Array2::Float64(cast_decimal_to_float::<_, Decimal64Type>(arr)?)
        }
        (Array2::Decimal128(arr), DataType::Float32) => {
            Array2::Float32(cast_decimal_to_float::<_, Decimal128Type>(arr)?)
        }
        (Array2::Decimal128(arr), DataType::Float64) => {
            Array2::Float64(cast_decimal_to_float::<_, Decimal128Type>(arr)?)
        }

        // From Utf8
        (Array2::Utf8(arr), datatype) => cast_from_utf8_array2(arr, datatype)?,
        (Array2::LargeUtf8(arr), datatype) => cast_from_utf8_array2(arr, datatype)?,

        // To Utf8
        (arr, DataType::Utf8) => Array2::Utf8(cast_to_utf8_array2(arr)?),
        (arr, DataType::LargeUtf8) => Array2::Utf8(cast_to_utf8_array2(arr)?),

        (arr, to) => {
            return Err(RayexecError::new(format!(
                "Unable to cast from {} to {to}",
                arr.datatype(),
            )))
        }
    })
}

pub fn cast_from_null(len: usize, datatype: &DataType) -> Result<Array2> {
    // TODO: Since the null array already contains a bitmap, we should maybe
    // change these array constructors to accept the bitmap, and not the length.
    Ok(match datatype {
        DataType::Boolean => Array2::Boolean(BooleanArray::new_nulls(len)),
        DataType::Int8 => Array2::Int8(PrimitiveArray::new_nulls(len)),
        DataType::Int16 => Array2::Int16(PrimitiveArray::new_nulls(len)),
        DataType::Int32 => Array2::Int32(PrimitiveArray::new_nulls(len)),
        DataType::Int64 => Array2::Int64(PrimitiveArray::new_nulls(len)),
        DataType::UInt8 => Array2::UInt8(PrimitiveArray::new_nulls(len)),
        DataType::UInt16 => Array2::UInt16(PrimitiveArray::new_nulls(len)),
        DataType::UInt32 => Array2::UInt32(PrimitiveArray::new_nulls(len)),
        DataType::UInt64 => Array2::UInt64(PrimitiveArray::new_nulls(len)),
        DataType::Float32 => Array2::Float32(PrimitiveArray::new_nulls(len)),
        DataType::Float64 => Array2::Float64(PrimitiveArray::new_nulls(len)),
        DataType::Decimal64(meta) => {
            let primitive = PrimitiveArray::new_nulls(len);
            Array2::Decimal64(DecimalArray::new(meta.precision, meta.scale, primitive))
        }
        DataType::Decimal128(meta) => {
            let primitive = PrimitiveArray::new_nulls(len);
            Array2::Decimal128(DecimalArray::new(meta.precision, meta.scale, primitive))
        }
        DataType::Date32 => Array2::Date32(PrimitiveArray::new_nulls(len)),
        DataType::Interval => Array2::Interval(PrimitiveArray::new_nulls(len)),
        DataType::Utf8 => Array2::Utf8(VarlenArray::new_nulls(len)),
        DataType::LargeUtf8 => Array2::LargeUtf8(VarlenArray::new_nulls(len)),
        DataType::Binary => Array2::Binary(VarlenArray::new_nulls(len)),
        DataType::LargeBinary => Array2::LargeBinary(VarlenArray::new_nulls(len)),
        other => {
            return Err(RayexecError::new(format!(
                "Unable to cast null array to {other}"
            )))
        }
    })
}

pub fn cast_from_utf8_array2<O>(arr: &VarlenArray<str, O>, datatype: &DataType) -> Result<Array2>
where
    O: OffsetIndex,
{
    Ok(match datatype {
        DataType::Boolean => Array2::Boolean(cast_parse_boolean(arr)?),
        DataType::Int8 => Array2::Int8(cast_parse_primitive2(arr, Int8Parser::default())?),
        DataType::Int16 => Array2::Int16(cast_parse_primitive2(arr, Int16Parser::default())?),
        DataType::Int32 => Array2::Int32(cast_parse_primitive2(arr, Int32Parser::default())?),
        DataType::Int64 => Array2::Int64(cast_parse_primitive2(arr, Int64Parser::default())?),
        DataType::UInt8 => Array2::UInt8(cast_parse_primitive2(arr, UInt8Parser::default())?),
        DataType::UInt16 => Array2::UInt16(cast_parse_primitive2(arr, UInt16Parser::default())?),
        DataType::UInt32 => Array2::UInt32(cast_parse_primitive2(arr, UInt32Parser::default())?),
        DataType::UInt64 => Array2::UInt64(cast_parse_primitive2(arr, UInt64Parser::default())?),
        DataType::Float32 => Array2::Float32(cast_parse_primitive2(arr, Float32Parser::default())?),
        DataType::Float64 => Array2::Float64(cast_parse_primitive2(arr, Float64Parser::default())?),
        DataType::Decimal64(meta) => {
            let primitive =
                cast_parse_primitive2(arr, Decimal64Parser::new(meta.precision, meta.scale))?;
            Array2::Decimal64(DecimalArray::new(meta.precision, meta.scale, primitive))
        }
        DataType::Decimal128(meta) => {
            let primitive =
                cast_parse_primitive2(arr, Decimal128Parser::new(meta.precision, meta.scale))?;
            Array2::Decimal128(DecimalArray::new(meta.precision, meta.scale, primitive))
        }
        DataType::Date32 => Array2::Date32(cast_parse_primitive2(arr, Date32Parser)?),
        DataType::Interval => {
            Array2::Interval(cast_parse_primitive2(arr, IntervalParser::default())?)
        }
        other => {
            return Err(RayexecError::new(format!(
                "Unable to cast utf8 array to {other}"
            )))
        }
    })
}

pub fn cast_to_utf8_array2<O>(arr: &Array2) -> Result<VarlenArray<str, O>>
where
    O: OffsetIndex,
{
    Ok(match arr {
        Array2::Boolean(arr) => format_values_into_varlen(arr, BoolFormatter::default())?,
        Array2::Int8(arr) => format_values_into_varlen(arr, Int8Formatter::default())?,
        Array2::Int16(arr) => format_values_into_varlen(arr, Int16Formatter::default())?,
        Array2::Int32(arr) => format_values_into_varlen(arr, Int32Formatter::default())?,
        Array2::Int64(arr) => format_values_into_varlen(arr, Int64Formatter::default())?,
        Array2::UInt8(arr) => format_values_into_varlen(arr, UInt8Formatter::default())?,
        Array2::UInt16(arr) => format_values_into_varlen(arr, UInt16Formatter::default())?,
        Array2::UInt32(arr) => format_values_into_varlen(arr, UInt32Formatter::default())?,
        Array2::UInt64(arr) => format_values_into_varlen(arr, UInt64Formatter::default())?,
        Array2::Float32(arr) => format_values_into_varlen(arr, Float32Formatter::default())?,
        Array2::Float64(arr) => format_values_into_varlen(arr, Float64Formatter::default())?,
        Array2::Decimal64(arr) => format_values_into_varlen(
            arr.get_primitive(),
            Decimal64Formatter::new(arr.precision(), arr.scale()),
        )?,
        Array2::Decimal128(arr) => format_values_into_varlen(
            arr.get_primitive(),
            Decimal128Formatter::new(arr.precision(), arr.scale()),
        )?,
        Array2::Timestamp(arr) => match arr.unit() {
            TimeUnit::Second => format_values_into_varlen(
                arr.get_primitive(),
                TimestampSecondsFormatter::default(),
            )?,
            TimeUnit::Millisecond => format_values_into_varlen(
                arr.get_primitive(),
                TimestampMillisecondsFormatter::default(),
            )?,
            TimeUnit::Microsecond => format_values_into_varlen(
                arr.get_primitive(),
                TimestampMicrosecondsFormatter::default(),
            )?,
            TimeUnit::Nanosecond => format_values_into_varlen(
                arr.get_primitive(),
                TimestampNanosecondsFormatter::default(),
            )?,
        },
        _ => unimplemented!(),
    })
}

/// Helper for taking an arbitrary array and producing a varlen array with the
/// formatted values.
fn format_values_into_varlen<O, F, T, A, I>(
    array: A,
    mut formatter: F,
) -> Result<VarlenArray<str, O>>
where
    T: Display,
    O: OffsetIndex,
    A: ArrayAccessor<T, ValueIter = I>,
    I: Iterator<Item = T>,
    F: Formatter<Type = T>,
{
    let mut buffer = VarlenValuesBuffer::default();
    let mut string_buf = String::new();

    match array.validity() {
        Some(validity) => {
            for (value, valid) in array.values_iter().zip(validity.iter()) {
                if valid {
                    string_buf.clear();
                    formatter
                        .write(&value, &mut string_buf)
                        .map_err(|_| RayexecError::new(format!("Failed to format {value}")))?;
                    buffer.push_value(string_buf.as_str());
                } else {
                    buffer.push_value("");
                }
            }
        }
        None => {
            for value in array.values_iter() {
                string_buf.clear();
                formatter
                    .write(&value, &mut string_buf)
                    .map_err(|_| RayexecError::new(format!("Failed to format {value}")))?;
                buffer.push_value(string_buf.as_str());
            }
        }
    }

    let out = VarlenArray::new(buffer, array.validity().cloned());

    Ok(out)
}

/// Cast from a utf8 array to a primitive array by parsing the utf8 values.
fn cast_parse_primitive2<O, T, P>(
    arr: &VarlenArray<str, O>,
    mut parser: P,
) -> Result<PrimitiveArray<T>>
where
    T: Default + Display,
    O: OffsetIndex,
    P: Parser<Type = T>,
{
    let mut new_values = Vec::with_capacity(arr.len());
    let operation = |val| {
        parser
            .parse(val)
            .ok_or_else(|| RayexecError::new(format!("Failed to parse '{val}'")))
    };
    UnaryExecutor2::try_execute(arr, operation, &mut new_values)?;

    Ok(PrimitiveArray::new(new_values, arr.validity().cloned()))
}

fn cast_parse_boolean<O>(arr: &VarlenArray<str, O>) -> Result<BooleanArray>
where
    O: OffsetIndex,
{
    let mut buf = BooleanValuesBuffer::with_capacity(arr.len());
    let operation = |val| {
        BoolParser
            .parse(val)
            .ok_or_else(|| RayexecError::new(format!("Failed to parse '{val}'")))
    };
    UnaryExecutor2::try_execute(arr, operation, &mut buf)?;

    Ok(BooleanArray::new(buf, arr.validity().cloned()))
}

/// Fallibly cast from primitive type A to primitive type B.
fn cast_primitive_numeric2<A, B>(arr: &PrimitiveArray<A>) -> Result<PrimitiveArray<B>>
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
        Some(validity) => PrimitiveArray::new(new_vals, Some(validity.clone())),
        None => PrimitiveArray::from(new_vals),
    })
}

pub fn cast_decimal_to_float<F, D>(arr: &DecimalArray<D::Primitive>) -> Result<PrimitiveArray<F>>
where
    F: Float + fmt::Display,
    D: DecimalType,
{
    let mut new_vals: Vec<F> = Vec::with_capacity(arr.len());

    let scale = <F as NumCast>::from((10.0).powi(arr.scale() as i32)).ok_or_else(|| {
        RayexecError::new(format!("Failed to cast scale {} to float", arr.scale()))
    })?;

    for val in arr.get_primitive().values().as_ref().iter() {
        let val = <F as NumCast>::from(*val)
            .ok_or_else(|| RayexecError::new(format!("Failed to convert {val} to float")))?;

        let scale = val.div(scale);

        new_vals.push(scale);
    }

    Ok(PrimitiveArray::new(
        new_vals,
        arr.get_primitive().validity().cloned(),
    ))
}

fn cast_float_to_decimal2<F, D>(
    arr: &PrimitiveArray<F>,
    precision: u8,
    scale: i8,
) -> Result<PrimitiveArray<D::Primitive>>
where
    F: Float + fmt::Display,
    D: DecimalType,
{
    if scale.is_negative() {
        return Err(RayexecError::new(
            "Casting to decimal with negative scale not yet supported",
        ));
    }

    let mut new_vals: Vec<D::Primitive> = Vec::with_capacity(arr.len());

    let scale = <F as NumCast>::from(10.pow(scale.unsigned_abs() as u32))
        .ok_or_else(|| RayexecError::new(format!("Failed to cast scale {scale} to float")))?;

    for val in arr.values().as_ref().iter() {
        // TODO: Properly handle negative scale.
        let scaled_value = val.mul(scale).round();

        new_vals.push(
            <D::Primitive as NumCast>::from(scaled_value).ok_or_else(|| {
                RayexecError::new(format!("Failed to cast {val} to decimal primitive"))
            })?,
        );
    }

    // Validate precision.
    // TODO: Skip nulls
    for v in &new_vals {
        D::validate_precision(*v, precision)?;
    }

    Ok(PrimitiveArray::new(new_vals, arr.validity().cloned()))
}

pub fn cast_decimal_to_new_precision_and_scale2<D>(
    arr: &DecimalArray<D::Primitive>,
    new_precision: u8,
    new_scale: i8,
) -> Result<DecimalArray<D::Primitive>>
where
    D: DecimalType,
{
    let scale_amount =
        <D::Primitive as NumCast>::from(10.pow((arr.scale() - new_scale).unsigned_abs() as u32))
            .expect("to be in range");

    let mut new_vals: Vec<D::Primitive> = arr.get_primitive().values().as_ref().to_vec();
    if arr.scale() < new_scale {
        new_vals.iter_mut().for_each(|v| *v = v.mul(scale_amount))
    } else {
        new_vals.iter_mut().for_each(|v| *v = v.div(scale_amount))
    }

    // Validate precision.
    // TODO: Skip nulls
    for v in &new_vals {
        D::validate_precision(*v, new_precision)?;
    }

    Ok(DecimalArray::new(
        new_precision,
        new_scale,
        PrimitiveArray::new(new_vals, arr.get_primitive().validity().cloned()),
    ))
}

/// Cast a primitive int type to the primitive representation of a decimal.
fn cast_int_to_decimal2<I, D>(
    arr: &PrimitiveArray<I>,
    precision: u8,
    scale: i8,
) -> Result<PrimitiveArray<D::Primitive>>
where
    I: PrimInt + fmt::Display,
    D: DecimalType,
{
    let mut new_vals: Vec<D::Primitive> = Vec::with_capacity(arr.len());

    // Convert everything to the primitive.
    for val in arr.values().as_ref().iter() {
        new_vals.push(<D::Primitive as NumCast>::from(*val).ok_or_else(|| {
            RayexecError::new(format!("Failed to cast {val} to decimal primitive"))
        })?);
    }

    // Scale everything.
    let scale_amount = <D::Primitive as NumCast>::from(10.pow(scale.unsigned_abs() as u32))
        .expect("to be in range");
    if scale > 0 {
        new_vals.iter_mut().for_each(|v| *v = v.mul(scale_amount))
    } else {
        new_vals.iter_mut().for_each(|v| *v = v.div(scale_amount))
    }

    // Validate precision.
    // TODO: Skip nulls
    for v in &new_vals {
        D::validate_precision(*v, precision)?;
    }

    Ok(PrimitiveArray::new(new_vals, arr.validity().cloned()))
}

#[cfg(test)]
mod tests {
    use crate::scalar::ScalarValue;

    use super::*;

    #[test]
    fn array_cast_utf8_to_i32() {
        let arr = Array::from_iter(["13", "18", "123456789"]);

        let got = cast_array(&arr, DataType::Int32, CastFailBehavior::Error).unwrap();

        assert_eq!(ScalarValue::from(13), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(18), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::from(123456789), got.logical_value(2).unwrap());
    }

    #[test]
    fn array_cast_utf8_to_i32_overflow_error() {
        let arr = Array::from_iter(["13", "18", "123456789000000"]);
        cast_array(&arr, DataType::Int32, CastFailBehavior::Error).unwrap_err();
    }

    #[test]
    fn array_cast_utf8_to_i32_overflow_null() {
        let arr = Array::from_iter(["13", "18", "123456789000000"]);

        let got = cast_array(&arr, DataType::Int32, CastFailBehavior::Null).unwrap();

        assert_eq!(ScalarValue::from(13), got.logical_value(0).unwrap());
        assert_eq!(ScalarValue::from(18), got.logical_value(1).unwrap());
        assert_eq!(ScalarValue::Null, got.logical_value(2).unwrap());
    }
}
