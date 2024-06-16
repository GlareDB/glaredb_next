use std::fmt;

use rayexec_error::{RayexecError, Result};

use crate::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType, DECIMAL_DEFUALT_SCALE};

/// Some types may optionally contain metadata to further refine the type. For
/// example, the decimal types might have additional precision/scale
/// information.
///
/// However there are some context where we don't care or want this metadata.
/// For example, a function accepting a decimal as an argument doens't care
/// about the precision or scale of that decimal, and so would define its type
/// signature to not include any type metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypeMeta<T> {
    Some(T),
    None,
}

impl<T> TypeMeta<T> {
    /// Get the type metadata if it's some, erroring if it's None.
    pub fn try_get_meta(&self) -> Result<&T> {
        match self {
            Self::Some(m) => Ok(m),
            Self::None => Err(RayexecError::new("No metadata on data type")),
        }
    }

    pub const fn is_some(&self) -> bool {
        matches!(self, TypeMeta::Some(_))
    }
}

impl<T> From<T> for TypeMeta<T> {
    fn from(value: T) -> Self {
        TypeMeta::Some(value)
    }
}

/// Metadata for the any type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AnyTypeMeta {}

/// Metadata associated with decimals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DecimalTypeMeta {
    pub precision: u8,
    pub scale: i8,
}

impl DecimalTypeMeta {
    pub const fn new(precision: u8, scale: i8) -> Self {
        DecimalTypeMeta { precision, scale }
    }
}

/// Metadata associated with structs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructTypeMeta {
    pub fields: Vec<(String, DataType)>,
}

/// Metadata associated with lists.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ListTypeMeta {
    pub datatype: Box<DataType>,
}

/// Supported data types.
///
/// This generally follows Arrow's type system, but is not restricted to it.
///
/// Some types may include additional metadata, and so will have an attached
/// `TypeMeta` field. This will act to refine the type even further. For
/// example, in cases where we care to act on _any_ list, and not just a list of
/// specific type, we use `List(TypeMeta::None)` to represent that.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    /// Any datatype.
    ///
    /// This is used for functions that can accept any input. Like all other
    /// variants, this variant must be explicitly matched on. Checking equality
    /// with any other data type will always return false.
    Any(TypeMeta<AnyTypeMeta>),
    /// Constant null columns.
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Float32,
    Float64,
    /// 64-bit decimal.
    Decimal64(TypeMeta<DecimalTypeMeta>),
    /// 128-bit decimal.
    Decimal128(TypeMeta<DecimalTypeMeta>),
    /// Timestamp in seconds.
    TimestampSeconds,
    /// Timestamp in milliseconds.
    TimestampMilliseconds,
    /// Timestamp in microseconds.
    TimestampMicroseconds,
    /// Timestamp in nanoseconds.
    TimestampNanoseconds,
    /// Days since epoch.
    Date32,
    /// Milliseconds since epoch.
    Date64,
    /// Some time interval with nanosecond resolution.
    Interval,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    /// A struct of different types.
    Struct(TypeMeta<StructTypeMeta>),
    /// A list of values all of the same type.
    List(TypeMeta<ListTypeMeta>),
}

impl DataType {
    /// Fill the datatype with default type meta if it's missing.
    pub fn fill_default_type_meta(self) -> Self {
        match self {
            Self::Any(TypeMeta::None) => Self::Any(TypeMeta::Some(AnyTypeMeta {})),
            Self::Decimal64(TypeMeta::None) => Self::Decimal64(TypeMeta::Some(DecimalTypeMeta {
                precision: Decimal64Type::MAX_PRECISION,
                scale: DECIMAL_DEFUALT_SCALE,
            })),
            Self::Decimal128(TypeMeta::None) => Self::Decimal128(TypeMeta::Some(DecimalTypeMeta {
                precision: Decimal128Type::MAX_PRECISION,
                scale: DECIMAL_DEFUALT_SCALE,
            })),
            other => other,
        }
    }

    /// Return if this datatype is any.
    pub const fn is_any(&self) -> bool {
        matches!(self, DataType::Any(_))
    }

    /// Return if this datatype is null.
    pub const fn is_null(&self) -> bool {
        matches!(self, DataType::Null)
    }

    /// Return if this datatype is a list.
    pub const fn is_list(&self) -> bool {
        matches!(self, DataType::List(_))
    }

    /// For types that might hold a `TypeMeta`, check if it's Some.
    ///
    /// All other types will return false.
    pub const fn type_meta_is_some(&self) -> bool {
        match self {
            Self::Decimal64(meta) => meta.is_some(),
            Self::Decimal128(meta) => meta.is_some(),
            Self::Struct(meta) => meta.is_some(),
            Self::List(meta) => meta.is_some(),
            _ => false,
        }
    }

    /// Compare the equality of two data types without comparing any type
    /// metadata.
    pub const fn eq_no_meta(&self, other: &Self) -> bool {
        match (self, other) {
            (DataType::Any(_), DataType::Any(_)) => true,
            (DataType::Null, DataType::Null) => true,
            (DataType::Boolean, DataType::Boolean) => true,
            (DataType::Int8, DataType::Int8) => true,
            (DataType::Int16, DataType::Int16) => true,
            (DataType::Int32, DataType::Int32) => true,
            (DataType::Int64, DataType::Int64) => true,
            (DataType::UInt8, DataType::UInt8) => true,
            (DataType::UInt16, DataType::UInt16) => true,
            (DataType::UInt32, DataType::UInt32) => true,
            (DataType::UInt64, DataType::UInt64) => true,
            (DataType::Float32, DataType::Float32) => true,
            (DataType::Float64, DataType::Float64) => true,
            (DataType::Decimal64(_), DataType::Decimal64(_)) => true,
            (DataType::Decimal128(_), DataType::Decimal128(_)) => true,
            (DataType::TimestampSeconds, DataType::TimestampSeconds) => true,
            (DataType::TimestampMilliseconds, DataType::TimestampMilliseconds) => true,
            (DataType::TimestampMicroseconds, DataType::TimestampMicroseconds) => true,
            (DataType::TimestampNanoseconds, DataType::TimestampNanoseconds) => true,
            (DataType::Date32, DataType::Date32) => true,
            (DataType::Date64, DataType::Date64) => true,
            (DataType::Interval, DataType::Interval) => true,
            (DataType::Utf8, DataType::Utf8) => true,
            (DataType::LargeUtf8, DataType::LargeUtf8) => true,
            (DataType::Binary, DataType::Binary) => true,
            (DataType::LargeBinary, DataType::LargeBinary) => true,
            (DataType::Struct(_), DataType::Struct(_)) => true,
            (DataType::List(_), DataType::List(_)) => true,
            _ => false,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any(_) => write!(f, "Any"),
            Self::Null => write!(f, "Null"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Int8 => write!(f, "Int8"),
            Self::Int16 => write!(f, "Int16"),
            Self::Int32 => write!(f, "Int32"),
            Self::Int64 => write!(f, "Int64"),
            Self::Int128 => write!(f, "Int128"),
            Self::UInt8 => write!(f, "UInt8"),
            Self::UInt16 => write!(f, "UInt16"),
            Self::UInt32 => write!(f, "UInt32"),
            Self::UInt64 => write!(f, "UInt64"),
            Self::UInt128 => write!(f, "UInt128"),
            Self::Float32 => write!(f, "Float32"),
            Self::Float64 => write!(f, "Float64"),
            Self::Decimal64(meta) => match meta {
                TypeMeta::Some(meta) => write!(f, "Decimal64({}, {})", meta.precision, meta.scale),
                TypeMeta::None => write!(f, "Decimal64"),
            },
            Self::Decimal128(meta) => match meta {
                TypeMeta::Some(meta) => write!(f, "Decimal128({}, {})", meta.precision, meta.scale),
                TypeMeta::None => write!(f, "Decimal128"),
            },
            Self::TimestampSeconds => write!(f, "Timestamp(s)"),
            Self::TimestampMilliseconds => write!(f, "Timestamp(ms)"),
            Self::TimestampMicroseconds => write!(f, "Timestamp(μs)"),
            Self::TimestampNanoseconds => write!(f, "Timestamp(ns)"),
            Self::Date32 => write!(f, "Date32"),
            Self::Date64 => write!(f, "Date64"),
            Self::Interval => write!(f, "Interval"),
            Self::Utf8 => write!(f, "Utf8"),
            Self::LargeUtf8 => write!(f, "LargeUtf8"),
            Self::Binary => write!(f, "Binary"),
            Self::LargeBinary => write!(f, "LargeBinary"),
            Self::Struct(meta) => match meta {
                TypeMeta::Some(meta) => {
                    write!(
                        f,
                        "Struct {{{}}}",
                        meta.fields
                            .iter()
                            .map(|(name, typ)| format!("{name}: {typ}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
                TypeMeta::None => write!(f, "Struct"),
            },
            DataType::List(meta) => match meta {
                TypeMeta::Some(meta) => write!(f, "List[{}]", meta.datatype),
                TypeMeta::None => write!(f, "List"),
            },
        }
    }
}
