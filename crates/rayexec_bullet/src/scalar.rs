use std::borrow::Cow;

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

    /// Utf-8 encoded string.
    Utf8(Cow<'a, str>),

    /// Utf-8 encoded string representing a LargeString's arrow type.
    LargeUtf8(Cow<'a, str>),

    /// Binary
    Binary(Cow<'a, [u8]>),

    /// Large binary
    LargeBinary(Cow<'a, [u8]>),
}

pub type OwnedScalarValue = ScalarValue<'static>;

impl<'a> ScalarValue<'a> {
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
            Self::Utf8(v) => OwnedScalarValue::Utf8(v.into_owned().into()),
            Self::LargeUtf8(v) => OwnedScalarValue::LargeUtf8(v.into_owned().into()),
            Self::Binary(v) => OwnedScalarValue::Binary(v.into_owned().into()),
            Self::LargeBinary(v) => OwnedScalarValue::LargeBinary(v.into_owned().into()),
        }
    }
}
