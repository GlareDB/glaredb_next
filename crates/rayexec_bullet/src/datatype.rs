use std::fmt::{self, Debug};

use rayexec_error::{RayexecError, Result};

use crate::scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType, DECIMAL_DEFUALT_SCALE};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataTypeId {
    /// Any datatype.
    ///
    /// This is used for functions that can accept any input. Like all other
    /// variants, this variant must be explicitly matched on. Checking equality
    /// with any other data type will always return false.
    ///
    /// This is mostly useful for a saying a UDF can accept any type.
    Any,
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
    Decimal64,
    Decimal128,
    TimestampSeconds,
    TimestampMilliseconds,
    TimestampMicroseconds,
    TimestampNanoseconds,
    Date32,
    Date64,
    Interval,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Struct,
    List,
}

impl fmt::Display for DataTypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Any => write!(f, "Any"),
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
            Self::Decimal64 => write!(f, "Decimal64"),
            Self::Decimal128 => write!(f, "Decimal128"),
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
            Self::Struct => write!(f, "Struct"),
            Self::List => write!(f, "List"),
        }
    }
}

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
/// Some types may include additional metadata, which acts to refine the type
/// even further.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
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
    Decimal64(DecimalTypeMeta),
    /// 128-bit decimal.
    Decimal128(DecimalTypeMeta),
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
    Struct(StructTypeMeta),
    /// A list of values all of the same type.
    List(ListTypeMeta),
}

impl DataType {
    /// Try to create a default data type from the the data type id.
    ///
    /// Errors on attempts to create a data type from an id that we either don't
    /// have enough information about (struct, list) or can never be represented
    /// as a concrete data type (any).
    pub fn try_default_datatype(id: DataTypeId) -> Result<Self> {
        Ok(match id {
            DataTypeId::Any => {
                return Err(RayexecError::new("Cannot create a default Any datatype"))
            }
            DataTypeId::Null => DataType::Null,
            DataTypeId::Boolean => DataType::Boolean,
            DataTypeId::Int8 => DataType::Int8,
            DataTypeId::Int16 => DataType::Int16,
            DataTypeId::Int32 => DataType::Int32,
            DataTypeId::Int64 => DataType::Int64,
            DataTypeId::Int128 => DataType::Int128,
            DataTypeId::UInt8 => DataType::UInt8,
            DataTypeId::UInt16 => DataType::UInt16,
            DataTypeId::UInt32 => DataType::UInt32,
            DataTypeId::UInt64 => DataType::UInt64,
            DataTypeId::UInt128 => DataType::UInt128,
            DataTypeId::Float32 => DataType::Float32,
            DataTypeId::Float64 => DataType::Float64,
            DataTypeId::Decimal64 => DataType::Decimal64(DecimalTypeMeta::new(
                Decimal64Type::MAX_PRECISION,
                DECIMAL_DEFUALT_SCALE,
            )),
            DataTypeId::Decimal128 => DataType::Decimal128(DecimalTypeMeta::new(
                Decimal128Type::MAX_PRECISION,
                DECIMAL_DEFUALT_SCALE,
            )),
            DataTypeId::TimestampSeconds => DataType::TimestampSeconds,
            DataTypeId::TimestampMilliseconds => DataType::TimestampMilliseconds,
            DataTypeId::TimestampMicroseconds => DataType::TimestampMicroseconds,
            DataTypeId::TimestampNanoseconds => DataType::TimestampNanoseconds,
            DataTypeId::Date32 => DataType::Date32,
            DataTypeId::Date64 => DataType::Date64,
            DataTypeId::Interval => DataType::Interval,
            DataTypeId::Utf8 => DataType::Utf8,
            DataTypeId::LargeUtf8 => DataType::LargeUtf8,
            DataTypeId::Binary => DataType::Binary,
            DataTypeId::LargeBinary => DataType::LargeBinary,
            DataTypeId::Struct => {
                return Err(RayexecError::new("Cannot create a default Struct datatype"))
            }
            DataTypeId::List => {
                return Err(RayexecError::new("Cannot create a default List datatype"))
            }
        })
    }

    /// Get the data type id from the data type.
    pub const fn datatype_id(&self) -> DataTypeId {
        match self {
            DataType::Null => DataTypeId::Null,
            DataType::Boolean => DataTypeId::Boolean,
            DataType::Int8 => DataTypeId::Int8,
            DataType::Int16 => DataTypeId::Int16,
            DataType::Int32 => DataTypeId::Int32,
            DataType::Int64 => DataTypeId::Int64,
            DataType::Int128 => DataTypeId::Int128,
            DataType::UInt8 => DataTypeId::UInt8,
            DataType::UInt16 => DataTypeId::UInt16,
            DataType::UInt32 => DataTypeId::UInt32,
            DataType::UInt64 => DataTypeId::UInt64,
            DataType::UInt128 => DataTypeId::UInt128,
            DataType::Float32 => DataTypeId::Float32,
            DataType::Float64 => DataTypeId::Float64,
            DataType::Decimal64(_) => DataTypeId::Decimal64,
            DataType::Decimal128(_) => DataTypeId::Decimal128,
            DataType::TimestampSeconds => DataTypeId::TimestampSeconds,
            DataType::TimestampMilliseconds => DataTypeId::TimestampMilliseconds,
            DataType::TimestampMicroseconds => DataTypeId::TimestampMicroseconds,
            DataType::TimestampNanoseconds => DataTypeId::TimestampNanoseconds,
            DataType::Date32 => DataTypeId::Date32,
            DataType::Date64 => DataTypeId::Date64,
            DataType::Interval => DataTypeId::Interval,
            DataType::Utf8 => DataTypeId::Utf8,
            DataType::LargeUtf8 => DataTypeId::LargeUtf8,
            DataType::Binary => DataTypeId::Binary,
            DataType::LargeBinary => DataTypeId::LargeBinary,
            DataType::Struct(_) => DataTypeId::Struct,
            DataType::List(_) => DataTypeId::List,
        }
    }

    /// Return if this datatype is null.
    pub const fn is_null(&self) -> bool {
        matches!(self, DataType::Null)
    }

    /// Return if this datatype is a list.
    pub const fn is_list(&self) -> bool {
        matches!(self, DataType::List(_))
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
            Self::Decimal64(meta) => write!(f, "Decimal64({}, {})", meta.precision, meta.scale),
            Self::Decimal128(meta) => write!(f, "Decimal128({}, {})", meta.precision, meta.scale),
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
            Self::Struct(meta) => {
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
            DataType::List(meta) => write!(f, "List[{}]", meta.datatype),
        }
    }
}

pub trait PrimitiveType: Debug + Clone + Copy + Sync + Send + 'static {
    const DATATYPE: DataType;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeBoolean;

impl PrimitiveType for PrimitiveTypeBoolean {
    const DATATYPE: DataType = DataType::Boolean;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeInt8;

impl PrimitiveType for PrimitiveTypeInt8 {
    const DATATYPE: DataType = DataType::Int8;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeInt16;

impl PrimitiveType for PrimitiveTypeInt16 {
    const DATATYPE: DataType = DataType::Int16;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeInt32;

impl PrimitiveType for PrimitiveTypeInt32 {
    const DATATYPE: DataType = DataType::Int32;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeInt64;

impl PrimitiveType for PrimitiveTypeInt64 {
    const DATATYPE: DataType = DataType::Int64;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeInt128;

impl PrimitiveType for PrimitiveTypeInt128 {
    const DATATYPE: DataType = DataType::Int128;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeUInt8;

impl PrimitiveType for PrimitiveTypeUInt8 {
    const DATATYPE: DataType = DataType::UInt8;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeUInt16;

impl PrimitiveType for PrimitiveTypeUInt16 {
    const DATATYPE: DataType = DataType::UInt16;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeUInt32;

impl PrimitiveType for PrimitiveTypeUInt32 {
    const DATATYPE: DataType = DataType::UInt32;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeUInt64;

impl PrimitiveType for PrimitiveTypeUInt64 {
    const DATATYPE: DataType = DataType::UInt64;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeUInt128;

impl PrimitiveType for PrimitiveTypeUInt128 {
    const DATATYPE: DataType = DataType::UInt128;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeFloat32;

impl PrimitiveType for PrimitiveTypeFloat32 {
    const DATATYPE: DataType = DataType::Float32;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeFloat64;

impl PrimitiveType for PrimitiveTypeFloat64 {
    const DATATYPE: DataType = DataType::Float64;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeTimestampSeconds;

impl PrimitiveType for PrimitiveTypeTimestampSeconds {
    const DATATYPE: DataType = DataType::TimestampSeconds;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeTimestampMilliseconds;

impl PrimitiveType for PrimitiveTypeTimestampMilliseconds {
    const DATATYPE: DataType = DataType::TimestampMilliseconds;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeTimestampMicroseconds;

impl PrimitiveType for PrimitiveTypeTimestampMicroseconds {
    const DATATYPE: DataType = DataType::TimestampMicroseconds;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeTimestampNanoseconds;

impl PrimitiveType for PrimitiveTypeTimestampNanoseconds {
    const DATATYPE: DataType = DataType::TimestampNanoseconds;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeDate32;

impl PrimitiveType for PrimitiveTypeDate32 {
    const DATATYPE: DataType = DataType::Date32;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrimitiveTypeDate64;

impl PrimitiveType for PrimitiveTypeDate64 {
    const DATATYPE: DataType = DataType::Date64;
}
