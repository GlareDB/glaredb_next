use crate::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal64Array,
    Float32Array, Float64Array, Int128Array, Int16Array, Int32Array, Int64Array, Int8Array,
    IntervalDayTime, IntervalDayTimeArray, IntervalYearMonth, IntervalYearMonthArray,
    LargeBinaryArray, LargeUtf8Array, NullArray, TimestampArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array, Utf8Array,
};
use crate::compute::cast::format::{
    BoolFormatter, Date32Formatter, Date64Formatter, Decimal128Formatter, Decimal64Formatter,
    Float32Formatter, Float64Formatter, Formatter, Int16Formatter, Int32Formatter, Int64Formatter,
    Int8Formatter, TimestampMicrosecondsFormatter, TimestampMillisecondsFormatter,
    TimestampNanosecondsFormatter, TimestampSecondsFormatter, UInt16Formatter, UInt32Formatter,
    UInt64Formatter, UInt8Formatter,
};
use crate::field::{DataType, IntervalUnit, TimeUnit};
use rayexec_error::{RayexecError, Result};
use std::borrow::Cow;
use std::fmt;

pub const DECIMAL_64_MAX_PRECISION: u8 = 18;
pub const DECIMAL_128_MAX_PRECISION: u8 = 38;
pub const DECIMAL_DEFUALT_SCALE: i8 = 9;

/// A single scalar value.
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue<'a> {
    /// Represents `DataType::Null` (castable to/from any other type)
    Null,

    /// True or false value
    Boolean(bool),

    /// 32bit float
    Float32(f32),

    /// 64bit float
    Float64(f64),

    /// Signed 8bit int
    Int8(i8),

    /// Signed 16bit int
    Int16(i16),

    /// Signed 32bit int
    Int32(i32),

    /// Signed 64bit int
    Int64(i64),

    /// Unsigned 8bit int
    UInt8(u8),

    /// Unsigned 16bit int
    UInt16(u16),

    /// Unsigned 32bit int
    UInt32(u32),

    /// Unsigned 64bit int
    UInt64(u64),

    Decimal64(DecimalScalar<i64>),
    Decimal128(DecimalScalar<i128>),

    /// A Date32 value.
    Date32(i32),

    /// A Date64 value.
    Date64(i64),

    /// Timestamp value
    Timestamp(TimeUnit, i64),

    /// A YearMonth interval
    IntervalYearMonth(IntervalYearMonth),

    /// A DayTime interval
    IntervalDayTime(IntervalDayTime),

    /// Utf-8 encoded string.
    Utf8(Cow<'a, str>),

    /// Utf-8 encoded string representing a LargeString's arrow type.
    LargeUtf8(Cow<'a, str>),

    /// Binary
    Binary(Cow<'a, [u8]>),

    /// Large binary
    LargeBinary(Cow<'a, [u8]>),

    /// A struct.
    Struct(Vec<ScalarValue<'a>>),
}

pub type OwnedScalarValue = ScalarValue<'static>;

impl<'a> ScalarValue<'a> {
    pub fn datatype(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Decimal64(v) => DataType::Decimal64(v.precision, v.scale),
            ScalarValue::Decimal128(v) => DataType::Decimal64(v.precision, v.scale),
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Date64(_) => DataType::Date64,
            ScalarValue::Timestamp(unit, _) => DataType::Timestamp(*unit),
            ScalarValue::IntervalYearMonth(_) => DataType::Interval(IntervalUnit::YearMonth),
            ScalarValue::IntervalDayTime(_) => DataType::Interval(IntervalUnit::DayTime),
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::LargeUtf8(_) => DataType::LargeUtf8,
            ScalarValue::Binary(_) => DataType::Binary,
            ScalarValue::LargeBinary(_) => DataType::LargeBinary,
            ScalarValue::Struct(fields) => DataType::Struct {
                fields: fields.iter().map(|f| f.datatype()).collect(),
            },
        }
    }

    pub fn into_owned(self) -> OwnedScalarValue {
        match self {
            Self::Null => OwnedScalarValue::Null,
            Self::Boolean(v) => OwnedScalarValue::Boolean(v),
            Self::Float32(v) => OwnedScalarValue::Float32(v),
            Self::Float64(v) => OwnedScalarValue::Float64(v),
            Self::Int8(v) => OwnedScalarValue::Int8(v),
            Self::Int16(v) => OwnedScalarValue::Int16(v),
            Self::Int32(v) => OwnedScalarValue::Int32(v),
            Self::Int64(v) => OwnedScalarValue::Int64(v),
            Self::UInt8(v) => OwnedScalarValue::UInt8(v),
            Self::UInt16(v) => OwnedScalarValue::UInt16(v),
            Self::UInt32(v) => OwnedScalarValue::UInt32(v),
            Self::UInt64(v) => OwnedScalarValue::UInt64(v),
            Self::Decimal64(v) => OwnedScalarValue::Decimal64(v),
            Self::Decimal128(v) => OwnedScalarValue::Decimal128(v),
            Self::Date32(v) => OwnedScalarValue::Date32(v),
            Self::Date64(v) => OwnedScalarValue::Date64(v),
            Self::Timestamp(unit, v) => OwnedScalarValue::Timestamp(unit, v),
            Self::IntervalYearMonth(v) => OwnedScalarValue::IntervalYearMonth(v),
            Self::IntervalDayTime(v) => OwnedScalarValue::IntervalDayTime(v),
            Self::Utf8(v) => OwnedScalarValue::Utf8(v.into_owned().into()),
            Self::LargeUtf8(v) => OwnedScalarValue::LargeUtf8(v.into_owned().into()),
            Self::Binary(v) => OwnedScalarValue::Binary(v.into_owned().into()),
            Self::LargeBinary(v) => OwnedScalarValue::LargeBinary(v.into_owned().into()),
            Self::Struct(v) => {
                OwnedScalarValue::Struct(v.into_iter().map(|v| v.into_owned()).collect())
            }
        }
    }

    /// Create an array of size `n` using the scalar value.
    pub fn as_array(&self, n: usize) -> Array {
        match self {
            Self::Null => Array::Null(NullArray::new(n)),
            Self::Boolean(v) => {
                Array::Boolean(BooleanArray::from_iter(std::iter::repeat(*v).take(n)))
            }
            Self::Float32(v) => {
                Array::Float32(Float32Array::from_iter(std::iter::repeat(*v).take(n)))
            }
            Self::Float64(v) => {
                Array::Float64(Float64Array::from_iter(std::iter::repeat(*v).take(n)))
            }
            Self::Int8(v) => Array::Int8(Int8Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::Int16(v) => Array::Int16(Int16Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::Int32(v) => Array::Int32(Int32Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::Int64(v) => Array::Int64(Int64Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::UInt8(v) => Array::UInt8(UInt8Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::UInt16(v) => Array::UInt16(UInt16Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::UInt32(v) => Array::UInt32(UInt32Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::UInt64(v) => Array::UInt64(UInt64Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::Decimal64(v) => {
                let primitive = Int64Array::from_iter(std::iter::repeat(v.value).take(n));
                Array::Decimal64(Decimal64Array::new(v.precision, v.scale, primitive))
            }
            Self::Decimal128(v) => {
                let primitive = Int128Array::from_iter(std::iter::repeat(v.value).take(n));
                Array::Decimal128(Decimal128Array::new(v.precision, v.scale, primitive))
            }
            Self::Date32(v) => Array::Date32(Date32Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::Date64(v) => Array::Date64(Date64Array::from_iter(std::iter::repeat(*v).take(n))),
            Self::Timestamp(unit, v) => Array::Timestamp(
                *unit,
                TimestampArray::from_iter(std::iter::repeat(*v).take(n)),
            ),
            Self::IntervalYearMonth(v) => Array::IntervalYearMonth(
                IntervalYearMonthArray::from_iter(std::iter::repeat(*v).take(n)),
            ),
            Self::IntervalDayTime(v) => Array::IntervalDayTime(IntervalDayTimeArray::from_iter(
                std::iter::repeat(*v).take(n),
            )),
            Self::Utf8(v) => {
                Array::Utf8(Utf8Array::from_iter(std::iter::repeat(v.as_ref()).take(n)))
            }
            Self::LargeUtf8(v) => Array::LargeUtf8(LargeUtf8Array::from_iter(
                std::iter::repeat(v.as_ref()).take(n),
            )),
            Self::Binary(v) => Array::Binary(BinaryArray::from_iter(
                std::iter::repeat(v.as_ref()).take(n),
            )),
            Self::LargeBinary(v) => Array::LargeBinary(LargeBinaryArray::from_iter(
                std::iter::repeat(v.as_ref()).take(n),
            )),
            Self::Struct(_) => unimplemented!("struct into array"),
        }
    }

    pub fn try_as_bool(&self) -> Result<bool> {
        match self {
            Self::Boolean(b) => Ok(*b),
            other => Err(RayexecError::new(format!("Not a bool: {other}"))),
        }
    }

    pub fn try_as_usize(&self) -> Result<usize> {
        match self {
            Self::Int8(i) => Ok((*i).try_into()?),
            Self::Int16(i) => Ok((*i).try_into()?),
            Self::Int32(i) => Ok((*i).try_into()?),
            Self::Int64(i) => Ok((*i).try_into()?),
            Self::UInt8(i) => Ok(*i as usize),
            Self::UInt16(i) => Ok(*i as usize),
            Self::UInt32(i) => Ok(*i as usize),
            Self::UInt64(i) => Ok(*i as usize),
            other => Err(RayexecError::new(format!("Not an integer: {other}"))),
        }
    }

    pub fn try_as_i64(&self) -> Result<i64> {
        match self {
            Self::Int8(i) => Ok(*i as i64),
            Self::Int16(i) => Ok(*i as i64),
            Self::Int32(i) => Ok(*i as i64),
            Self::Int64(i) => Ok(*i),
            Self::UInt8(i) => Ok(*i as i64),
            Self::UInt16(i) => Ok(*i as i64),
            Self::UInt32(i) => Ok(*i as i64),
            Self::UInt64(i) => {
                if *i < i64::MAX as u64 {
                    Ok(*i as i64)
                } else {
                    Err(RayexecError::new("u64 too large to fit into an i64"))
                }
            }
            other => Err(RayexecError::new(format!("Not an integer: {other}"))),
        }
    }

    pub fn try_as_i32(&self) -> Result<i32> {
        match self {
            Self::Int8(i) => Ok(*i as i32),
            Self::Int16(i) => Ok(*i as i32),
            Self::Int32(i) => Ok(*i),
            Self::Int64(i) => {
                if *i < i32::MAX as i64 {
                    Ok(*i as i32)
                } else {
                    Err(RayexecError::new("i64 too large to fit into an i32"))
                }
            }
            Self::UInt8(i) => Ok(*i as i32),
            Self::UInt16(i) => Ok(*i as i32),
            Self::UInt32(i) => {
                if *i < i32::MAX as u32 {
                    Ok(*i as i32)
                } else {
                    Err(RayexecError::new("u32 too large to fit into an i32"))
                }
            }
            Self::UInt64(i) => {
                if *i < i32::MAX as u64 {
                    Ok(*i as i32)
                } else {
                    Err(RayexecError::new("u64 too large to fit into an i32"))
                }
            }
            other => Err(RayexecError::new(format!("Not an integer: {other}"))),
        }
    }

    pub fn try_as_str(&self) -> Result<&str> {
        match self {
            Self::Utf8(v) | Self::LargeUtf8(v) => Ok(v.as_ref()),
            other => Err(RayexecError::new(format!("Not a string: {other}"))),
        }
    }

    pub fn try_into_string(self) -> Result<String> {
        match self {
            Self::Utf8(v) | Self::LargeUtf8(v) => Ok(v.to_string()),
            other => Err(RayexecError::new(format!("Not a string: {other}"))),
        }
    }
}

impl fmt::Display for ScalarValue<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "NULL"),
            Self::Boolean(v) => BoolFormatter::default().write(v, f),
            Self::Float32(v) => Float32Formatter::default().write(v, f),
            Self::Float64(v) => Float64Formatter::default().write(v, f),
            Self::Int8(v) => Int8Formatter::default().write(v, f),
            Self::Int16(v) => Int16Formatter::default().write(v, f),
            Self::Int32(v) => Int32Formatter::default().write(v, f),
            Self::Int64(v) => Int64Formatter::default().write(v, f),
            Self::UInt8(v) => UInt8Formatter::default().write(v, f),
            Self::UInt16(v) => UInt16Formatter::default().write(v, f),
            Self::UInt32(v) => UInt32Formatter::default().write(v, f),
            Self::UInt64(v) => UInt64Formatter::default().write(v, f),
            Self::Decimal64(v) => Decimal64Formatter::new(v.precision, v.scale).write(&v.value, f),
            Self::Decimal128(v) => {
                Decimal128Formatter::new(v.precision, v.scale).write(&v.value, f)
            }
            Self::Date32(v) => Date32Formatter.write(v, f),
            Self::Date64(v) => Date64Formatter.write(v, f),
            Self::Timestamp(unit, v) => match unit {
                TimeUnit::Second => TimestampSecondsFormatter::default().write(v, f),
                TimeUnit::Millisecond => TimestampMillisecondsFormatter::default().write(v, f),
                TimeUnit::Microsecond => TimestampMicrosecondsFormatter::default().write(v, f),
                TimeUnit::Nanosecond => TimestampNanosecondsFormatter::default().write(v, f),
            },
            Self::IntervalYearMonth(v) => write!(f, "{} months", v.months), // TODO
            Self::IntervalDayTime(v) => write!(f, "{} day {} ms", v.days, v.milliseconds), // TODO
            Self::Utf8(v) => write!(f, "{}", v),
            Self::LargeUtf8(v) => write!(f, "{}", v),
            Self::Binary(v) => write!(f, "{:X?}", v),
            Self::LargeBinary(v) => write!(f, "{:X?}", v),
            Self::Struct(fields) => write!(
                f,
                "{{{}}}",
                fields
                    .iter()
                    .map(|typ| format!("{typ}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DecimalScalar<T: Copy> {
    pub precision: u8,
    pub scale: i8,
    pub value: T,
}

impl<'a> From<bool> for ScalarValue<'a> {
    fn from(value: bool) -> Self {
        ScalarValue::Boolean(value)
    }
}

impl<'a> From<f32> for ScalarValue<'a> {
    fn from(value: f32) -> Self {
        ScalarValue::Float32(value)
    }
}

impl<'a> From<f64> for ScalarValue<'a> {
    fn from(value: f64) -> Self {
        ScalarValue::Float64(value)
    }
}

impl<'a> From<i8> for ScalarValue<'a> {
    fn from(value: i8) -> Self {
        ScalarValue::Int8(value)
    }
}

impl<'a> From<i16> for ScalarValue<'a> {
    fn from(value: i16) -> Self {
        ScalarValue::Int16(value)
    }
}

impl<'a> From<i32> for ScalarValue<'a> {
    fn from(value: i32) -> Self {
        ScalarValue::Int32(value)
    }
}

impl<'a> From<i64> for ScalarValue<'a> {
    fn from(value: i64) -> Self {
        ScalarValue::Int64(value)
    }
}

impl<'a> From<u8> for ScalarValue<'a> {
    fn from(value: u8) -> Self {
        ScalarValue::UInt8(value)
    }
}

impl<'a> From<u16> for ScalarValue<'a> {
    fn from(value: u16) -> Self {
        ScalarValue::UInt16(value)
    }
}

impl<'a> From<u32> for ScalarValue<'a> {
    fn from(value: u32) -> Self {
        ScalarValue::UInt32(value)
    }
}

impl<'a> From<u64> for ScalarValue<'a> {
    fn from(value: u64) -> Self {
        ScalarValue::UInt64(value)
    }
}

impl<'a> From<&'a str> for ScalarValue<'a> {
    fn from(value: &'a str) -> Self {
        ScalarValue::Utf8(Cow::Borrowed(value))
    }
}

impl<'a> From<&'a [u8]> for ScalarValue<'a> {
    fn from(value: &'a [u8]) -> Self {
        ScalarValue::Binary(Cow::Borrowed(value))
    }
}

impl<'a, T: Into<ScalarValue<'a>>> From<Option<T>> for ScalarValue<'a> {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => ScalarValue::Null,
        }
    }
}
