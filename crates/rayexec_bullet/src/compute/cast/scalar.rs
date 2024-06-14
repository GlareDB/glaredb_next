use num::{NumCast, ToPrimitive};
use rayexec_error::{RayexecError, Result};
use std::fmt;

use crate::{
    compute::cast::parse::{BoolParser, Date32Parser},
    field::DataType,
    scalar::{DecimalScalar, OwnedScalarValue, ScalarValue},
};

use super::parse::{
    Decimal128Parser, Decimal64Parser, Float32Parser, Float64Parser, Int16Parser, Int32Parser,
    Int64Parser, Int8Parser, Parser, UInt16Parser, UInt32Parser, UInt64Parser, UInt8Parser,
};

pub fn cast_scalar(scalar: ScalarValue, to: &DataType) -> Result<OwnedScalarValue> {
    if &scalar.datatype() == to {
        return Ok(scalar.into_owned());
    }

    Ok(match (scalar, to) {
        (ScalarValue::UInt8(v), DataType::Int8) => ScalarValue::Int8(cast_primitive_numeric(v)?),
        (ScalarValue::UInt8(v), DataType::Int16) => ScalarValue::Int16(cast_primitive_numeric(v)?),
        (ScalarValue::UInt8(v), DataType::Int32) => ScalarValue::Int32(cast_primitive_numeric(v)?),
        (ScalarValue::UInt8(v), DataType::Int64) => ScalarValue::Int64(cast_primitive_numeric(v)?),
        (ScalarValue::UInt8(v), DataType::UInt8) => ScalarValue::UInt8(cast_primitive_numeric(v)?),
        (ScalarValue::UInt8(v), DataType::UInt16) => {
            ScalarValue::UInt16(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt8(v), DataType::UInt32) => {
            ScalarValue::UInt32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt8(v), DataType::UInt64) => {
            ScalarValue::UInt64(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt8(v), DataType::Float32) => {
            ScalarValue::Float32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt8(v), DataType::Float64) => {
            ScalarValue::Float64(cast_primitive_numeric(v)?)
        }
        // From UInt16
        (ScalarValue::UInt16(v), DataType::Int8) => ScalarValue::Int8(cast_primitive_numeric(v)?),
        (ScalarValue::UInt16(v), DataType::Int16) => ScalarValue::Int16(cast_primitive_numeric(v)?),
        (ScalarValue::UInt16(v), DataType::Int32) => ScalarValue::Int32(cast_primitive_numeric(v)?),
        (ScalarValue::UInt16(v), DataType::Int64) => ScalarValue::Int64(cast_primitive_numeric(v)?),
        (ScalarValue::UInt16(v), DataType::UInt8) => ScalarValue::UInt8(cast_primitive_numeric(v)?),
        (ScalarValue::UInt16(v), DataType::UInt16) => {
            ScalarValue::UInt16(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt16(v), DataType::UInt32) => {
            ScalarValue::UInt32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt16(v), DataType::UInt64) => {
            ScalarValue::UInt64(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt16(v), DataType::Float32) => {
            ScalarValue::Float32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt16(v), DataType::Float64) => {
            ScalarValue::Float64(cast_primitive_numeric(v)?)
        }
        // From UInt32
        (ScalarValue::UInt32(v), DataType::Int8) => ScalarValue::Int8(cast_primitive_numeric(v)?),
        (ScalarValue::UInt32(v), DataType::Int16) => ScalarValue::Int16(cast_primitive_numeric(v)?),
        (ScalarValue::UInt32(v), DataType::Int32) => ScalarValue::Int32(cast_primitive_numeric(v)?),
        (ScalarValue::UInt32(v), DataType::Int64) => ScalarValue::Int64(cast_primitive_numeric(v)?),
        (ScalarValue::UInt32(v), DataType::UInt8) => ScalarValue::UInt8(cast_primitive_numeric(v)?),
        (ScalarValue::UInt32(v), DataType::UInt16) => {
            ScalarValue::UInt16(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt32(v), DataType::UInt32) => {
            ScalarValue::UInt32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt32(v), DataType::UInt64) => {
            ScalarValue::UInt64(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt32(v), DataType::Float32) => {
            ScalarValue::Float32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt32(v), DataType::Float64) => {
            ScalarValue::Float64(cast_primitive_numeric(v)?)
        }
        // From UInt64
        (ScalarValue::UInt64(v), DataType::Int8) => ScalarValue::Int8(cast_primitive_numeric(v)?),
        (ScalarValue::UInt64(v), DataType::Int16) => ScalarValue::Int16(cast_primitive_numeric(v)?),
        (ScalarValue::UInt64(v), DataType::Int32) => ScalarValue::Int32(cast_primitive_numeric(v)?),
        (ScalarValue::UInt64(v), DataType::Int64) => ScalarValue::Int64(cast_primitive_numeric(v)?),
        (ScalarValue::UInt64(v), DataType::UInt8) => ScalarValue::UInt8(cast_primitive_numeric(v)?),
        (ScalarValue::UInt64(v), DataType::UInt16) => {
            ScalarValue::UInt16(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt64(v), DataType::UInt32) => {
            ScalarValue::UInt32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt64(v), DataType::UInt64) => {
            ScalarValue::UInt64(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt64(v), DataType::Float32) => {
            ScalarValue::Float32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::UInt64(v), DataType::Float64) => {
            ScalarValue::Float64(cast_primitive_numeric(v)?)
        }
        // From Int8
        (ScalarValue::Int8(v), DataType::Int8) => ScalarValue::Int8(cast_primitive_numeric(v)?),
        (ScalarValue::Int8(v), DataType::Int16) => ScalarValue::Int16(cast_primitive_numeric(v)?),
        (ScalarValue::Int8(v), DataType::Int32) => ScalarValue::Int32(cast_primitive_numeric(v)?),
        (ScalarValue::Int8(v), DataType::Int64) => ScalarValue::Int64(cast_primitive_numeric(v)?),
        (ScalarValue::Int8(v), DataType::UInt8) => ScalarValue::UInt8(cast_primitive_numeric(v)?),
        (ScalarValue::Int8(v), DataType::UInt16) => ScalarValue::UInt16(cast_primitive_numeric(v)?),
        (ScalarValue::Int8(v), DataType::UInt32) => ScalarValue::UInt32(cast_primitive_numeric(v)?),
        (ScalarValue::Int8(v), DataType::UInt64) => ScalarValue::UInt64(cast_primitive_numeric(v)?),
        (ScalarValue::Int8(v), DataType::Float32) => {
            ScalarValue::Float32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int8(v), DataType::Float64) => {
            ScalarValue::Float64(cast_primitive_numeric(v)?)
        }
        // From Int16
        (ScalarValue::Int16(v), DataType::Int8) => ScalarValue::Int8(cast_primitive_numeric(v)?),
        (ScalarValue::Int16(v), DataType::Int16) => ScalarValue::Int16(cast_primitive_numeric(v)?),
        (ScalarValue::Int16(v), DataType::Int32) => ScalarValue::Int32(cast_primitive_numeric(v)?),
        (ScalarValue::Int16(v), DataType::Int64) => ScalarValue::Int64(cast_primitive_numeric(v)?),
        (ScalarValue::Int16(v), DataType::UInt8) => ScalarValue::UInt8(cast_primitive_numeric(v)?),
        (ScalarValue::Int16(v), DataType::UInt16) => {
            ScalarValue::UInt16(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int16(v), DataType::UInt32) => {
            ScalarValue::UInt32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int16(v), DataType::UInt64) => {
            ScalarValue::UInt64(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int16(v), DataType::Float32) => {
            ScalarValue::Float32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int16(v), DataType::Float64) => {
            ScalarValue::Float64(cast_primitive_numeric(v)?)
        }
        // From Int32
        (ScalarValue::Int32(v), DataType::Int8) => ScalarValue::Int8(cast_primitive_numeric(v)?),
        (ScalarValue::Int32(v), DataType::Int16) => ScalarValue::Int16(cast_primitive_numeric(v)?),
        (ScalarValue::Int32(v), DataType::Int32) => ScalarValue::Int32(cast_primitive_numeric(v)?),
        (ScalarValue::Int32(v), DataType::Int64) => ScalarValue::Int64(cast_primitive_numeric(v)?),
        (ScalarValue::Int32(v), DataType::UInt8) => ScalarValue::UInt8(cast_primitive_numeric(v)?),
        (ScalarValue::Int32(v), DataType::UInt16) => {
            ScalarValue::UInt16(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int32(v), DataType::UInt32) => {
            ScalarValue::UInt32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int32(v), DataType::UInt64) => {
            ScalarValue::UInt64(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int32(v), DataType::Float32) => {
            ScalarValue::Float32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int32(v), DataType::Float64) => {
            ScalarValue::Float64(cast_primitive_numeric(v)?)
        }
        // From Int64
        (ScalarValue::Int64(v), DataType::Int8) => ScalarValue::Int8(cast_primitive_numeric(v)?),
        (ScalarValue::Int64(v), DataType::Int16) => ScalarValue::Int16(cast_primitive_numeric(v)?),
        (ScalarValue::Int64(v), DataType::Int32) => ScalarValue::Int32(cast_primitive_numeric(v)?),
        (ScalarValue::Int64(v), DataType::Int64) => ScalarValue::Int64(cast_primitive_numeric(v)?),
        (ScalarValue::Int64(v), DataType::UInt8) => ScalarValue::UInt8(cast_primitive_numeric(v)?),
        (ScalarValue::Int64(v), DataType::UInt16) => {
            ScalarValue::UInt16(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int64(v), DataType::UInt32) => {
            ScalarValue::UInt32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int64(v), DataType::UInt64) => {
            ScalarValue::UInt64(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int64(v), DataType::Float32) => {
            ScalarValue::Float32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Int64(v), DataType::Float64) => {
            ScalarValue::Float64(cast_primitive_numeric(v)?)
        }
        // From Float32
        (ScalarValue::Float32(v), DataType::Int8) => ScalarValue::Int8(cast_primitive_numeric(v)?),
        (ScalarValue::Float32(v), DataType::Int16) => {
            ScalarValue::Int16(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float32(v), DataType::Int32) => {
            ScalarValue::Int32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float32(v), DataType::Int64) => {
            ScalarValue::Int64(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float32(v), DataType::UInt8) => {
            ScalarValue::UInt8(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float32(v), DataType::UInt16) => {
            ScalarValue::UInt16(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float32(v), DataType::UInt32) => {
            ScalarValue::UInt32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float32(v), DataType::UInt64) => {
            ScalarValue::UInt64(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float32(v), DataType::Float32) => {
            ScalarValue::Float32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float32(v), DataType::Float64) => {
            ScalarValue::Float64(cast_primitive_numeric(v)?)
        }
        // From Float64
        (ScalarValue::Float64(v), DataType::Int8) => ScalarValue::Int8(cast_primitive_numeric(v)?),
        (ScalarValue::Float64(v), DataType::Int16) => {
            ScalarValue::Int16(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float64(v), DataType::Int32) => {
            ScalarValue::Int32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float64(v), DataType::Int64) => {
            ScalarValue::Int64(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float64(v), DataType::UInt8) => {
            ScalarValue::UInt8(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float64(v), DataType::UInt16) => {
            ScalarValue::UInt16(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float64(v), DataType::UInt32) => {
            ScalarValue::UInt32(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float64(v), DataType::UInt64) => {
            ScalarValue::UInt64(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float64(v), DataType::Float32) => {
            ScalarValue::Float64(cast_primitive_numeric(v)?)
        }
        (ScalarValue::Float64(v), DataType::Float64) => {
            ScalarValue::Float64(cast_primitive_numeric(v)?)
        }

        // From Utf8
        (ScalarValue::Utf8(v), datatype) => cast_from_utf8_scalar(v.as_ref(), datatype)?,
        (ScalarValue::LargeUtf8(v), datatype) => cast_from_utf8_scalar(v.as_ref(), datatype)?,

        // To Utf8
        (v, DataType::Utf8) => ScalarValue::Utf8(v.to_string().into()),
        (v, DataType::LargeUtf8) => ScalarValue::Utf8(v.to_string().into()),

        (scalar, to) => {
            return Err(RayexecError::new(format!(
                "Unable to cast from {} to {to}",
                scalar.datatype(),
            )))
        }
    })
}

fn cast_from_utf8_scalar(v: &str, datatype: &DataType) -> Result<OwnedScalarValue> {
    fn parse<T, P: Parser<Type = T>>(mut parser: P, v: &str, datatype: &DataType) -> Result<T> {
        parser
            .parse(v)
            .ok_or_else(|| RayexecError::new(format!("Failed to parse as 'v' as {datatype}")))
    }

    Ok(match datatype {
        DataType::Boolean => ScalarValue::Boolean(parse(BoolParser, v, datatype)?),
        DataType::Int8 => ScalarValue::Int8(parse(Int8Parser::default(), v, datatype)?),
        DataType::Int16 => ScalarValue::Int16(parse(Int16Parser::default(), v, datatype)?),
        DataType::Int32 => ScalarValue::Int32(parse(Int32Parser::default(), v, datatype)?),
        DataType::Int64 => ScalarValue::Int64(parse(Int64Parser::default(), v, datatype)?),
        DataType::UInt8 => ScalarValue::UInt8(parse(UInt8Parser::default(), v, datatype)?),
        DataType::UInt16 => ScalarValue::UInt16(parse(UInt16Parser::default(), v, datatype)?),
        DataType::UInt32 => ScalarValue::UInt32(parse(UInt32Parser::default(), v, datatype)?),
        DataType::UInt64 => ScalarValue::UInt64(parse(UInt64Parser::default(), v, datatype)?),
        DataType::Float32 => ScalarValue::Float32(parse(Float32Parser::default(), v, datatype)?),
        DataType::Float64 => ScalarValue::Float64(parse(Float64Parser::default(), v, datatype)?),
        DataType::Decimal64(p, s) => ScalarValue::Decimal64(DecimalScalar {
            precision: *p,
            scale: *s,
            value: parse(Decimal64Parser::new(*p, *s), v, datatype)?,
        }),
        DataType::Decimal128(p, s) => ScalarValue::Decimal128(DecimalScalar {
            precision: *p,
            scale: *s,
            value: parse(Decimal128Parser::new(*p, *s), v, datatype)?,
        }),
        DataType::Date32 => ScalarValue::Date32(parse(Date32Parser, v, datatype)?),
        other => {
            return Err(RayexecError::new(format!(
                "Unable to cast utf8 scalar to {other}"
            )))
        }
    })
}

fn cast_primitive_numeric<A, B>(v: A) -> Result<B>
where
    A: Copy + ToPrimitive + fmt::Display,
    B: NumCast,
{
    B::from(v).ok_or_else(|| RayexecError::new(format!("Failed to cast {v}")))
}
