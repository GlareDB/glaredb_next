use std::array::TryFromSliceError;
use std::fmt::Debug;

use rayexec_error::{RayexecError, Result};

use crate::thrift_gen;

/// Physical types representable in parquet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalType {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray,
}

impl TryFrom<thrift_gen::Type> for PhysicalType {
    type Error = RayexecError;

    fn try_from(value: thrift_gen::Type) -> Result<Self> {
        Ok(match value {
            thrift_gen::Type::BOOLEAN => PhysicalType::Boolean,
            thrift_gen::Type::INT32 => PhysicalType::Int32,
            thrift_gen::Type::INT64 => PhysicalType::Int64,
            thrift_gen::Type::INT96 => PhysicalType::Int96,
            thrift_gen::Type::FLOAT => PhysicalType::Float,
            thrift_gen::Type::DOUBLE => PhysicalType::Double,
            thrift_gen::Type::BYTE_ARRAY => PhysicalType::ByteArray,
            thrift_gen::Type::FIXED_LEN_BYTE_ARRAY => PhysicalType::FixedLenByteArray,
            _ => return Err(RayexecError::new("invalid physical type")),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repetition {
    Required,
    Optional,
    Repeated,
}

impl TryFrom<thrift_gen::FieldRepetitionType> for Repetition {
    type Error = RayexecError;
    fn try_from(value: thrift_gen::FieldRepetitionType) -> Result<Self> {
        Ok(match value {
            thrift_gen::FieldRepetitionType::REQUIRED => Self::Required,
            thrift_gen::FieldRepetitionType::OPTIONAL => Self::Optional,
            thrift_gen::FieldRepetitionType::REPEATED => Self::Repeated,
            _ => return Err(RayexecError::new("invalid field repetition")),
        })
    }
}

/// Parquet converted type, deprecated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvertedType {
    Utf8,
    Map,
    MapKeyValue,
    List,
    Enum,
    Decimal,
    Date,
    TimeMillis,
    TimeMicros,
    TimestampMillis,
    TimestampMicros,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Int8,
    Int16,
    Int32,
    Int64,
    Json,
    Bson,
    Interval,
}

impl TryFrom<thrift_gen::ConvertedType> for ConvertedType {
    type Error = RayexecError;
    fn try_from(value: thrift_gen::ConvertedType) -> Result<Self> {
        Ok(match value {
            thrift_gen::ConvertedType::UTF8 => ConvertedType::Utf8,
            thrift_gen::ConvertedType::MAP => ConvertedType::Map,
            thrift_gen::ConvertedType::MAP_KEY_VALUE => ConvertedType::MapKeyValue,
            thrift_gen::ConvertedType::LIST => ConvertedType::List,
            thrift_gen::ConvertedType::ENUM => ConvertedType::Enum,
            thrift_gen::ConvertedType::DECIMAL => ConvertedType::Decimal,
            thrift_gen::ConvertedType::DATE => ConvertedType::Date,
            thrift_gen::ConvertedType::TIME_MILLIS => ConvertedType::TimeMillis,
            thrift_gen::ConvertedType::TIME_MICROS => ConvertedType::TimeMicros,
            thrift_gen::ConvertedType::TIMESTAMP_MILLIS => ConvertedType::TimestampMillis,
            thrift_gen::ConvertedType::TIMESTAMP_MICROS => ConvertedType::TimestampMicros,
            thrift_gen::ConvertedType::UINT_8 => ConvertedType::Uint8,
            thrift_gen::ConvertedType::UINT_16 => ConvertedType::Uint16,
            thrift_gen::ConvertedType::UINT_32 => ConvertedType::Uint32,
            thrift_gen::ConvertedType::UINT_64 => ConvertedType::Uint64,
            thrift_gen::ConvertedType::INT_8 => ConvertedType::Int8,
            thrift_gen::ConvertedType::INT_16 => ConvertedType::Int16,
            thrift_gen::ConvertedType::INT_32 => ConvertedType::Int32,
            thrift_gen::ConvertedType::INT_64 => ConvertedType::Int64,
            thrift_gen::ConvertedType::JSON => ConvertedType::Json,
            thrift_gen::ConvertedType::BSON => ConvertedType::Bson,
            thrift_gen::ConvertedType::INTERVAL => ConvertedType::Interval,
            _ => return Err(RayexecError::new("invalid converted type")),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Millis,
    Micros,
    Nanos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalType {
    String,
    Map,
    List,
    Enum,
    Decimal {
        scale: i32,
        precision: i32,
    },
    Date,
    Time {
        is_adjusted_to_u_t_c: bool,
        unit: TimeUnit,
    },
    Timestamp {
        is_adjusted_to_u_t_c: bool,
        unit: TimeUnit,
    },
    Integer {
        bit_width: i8,
        is_signed: bool,
    },
    Unknown,
    Json,
    Bson,
    Uuid,
    Float16,
}

impl From<thrift_gen::LogicalType> for LogicalType {
    fn from(value: thrift_gen::LogicalType) -> Self {
        match value {
            thrift_gen::LogicalType::STRING(_) => LogicalType::String,
            thrift_gen::LogicalType::MAP(_) => LogicalType::Map,
            thrift_gen::LogicalType::LIST(_) => LogicalType::List,
            thrift_gen::LogicalType::ENUM(_) => LogicalType::Enum,
            thrift_gen::LogicalType::DECIMAL(t) => LogicalType::Decimal {
                scale: t.scale,
                precision: t.precision,
            },
            thrift_gen::LogicalType::DATE(_) => LogicalType::Date,
            thrift_gen::LogicalType::TIME(t) => LogicalType::Time {
                is_adjusted_to_u_t_c: t.is_adjusted_to_u_t_c,
                unit: match t.unit {
                    thrift_gen::TimeUnit::MILLIS(_) => TimeUnit::Millis,
                    thrift_gen::TimeUnit::MICROS(_) => TimeUnit::Micros,
                    thrift_gen::TimeUnit::NANOS(_) => TimeUnit::Nanos,
                },
            },
            thrift_gen::LogicalType::TIMESTAMP(t) => LogicalType::Timestamp {
                is_adjusted_to_u_t_c: t.is_adjusted_to_u_t_c,
                unit: match t.unit {
                    thrift_gen::TimeUnit::MILLIS(_) => TimeUnit::Millis,
                    thrift_gen::TimeUnit::MICROS(_) => TimeUnit::Micros,
                    thrift_gen::TimeUnit::NANOS(_) => TimeUnit::Nanos,
                },
            },
            thrift_gen::LogicalType::INTEGER(t) => LogicalType::Integer {
                bit_width: t.bit_width,
                is_signed: t.is_signed,
            },
            thrift_gen::LogicalType::UNKNOWN(_) => LogicalType::Unknown,
            thrift_gen::LogicalType::JSON(_) => LogicalType::Json,
            thrift_gen::LogicalType::BSON(_) => LogicalType::Bson,
            thrift_gen::LogicalType::UUID(_) => LogicalType::Uuid,
            thrift_gen::LogicalType::FLOAT16(_) => LogicalType::Float16,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeInfo {
    pub name: String,
    pub repetition: Repetition,
    pub converted_type: Option<ConvertedType>,
    pub logical_type: Option<LogicalType>,
    pub id: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParquetType {
    Primitive(PrimitiveType),
    Group(GroupType),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimitiveType {
    pub info: TypeInfo,
    pub physical_type: PhysicalType,
    pub type_length: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupType {
    pub info: TypeInfo,
    /// Indices in the schema list for child fields for this group type.
    pub fields: Vec<usize>,
}

/// Parquet Int96 type. Deprecated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Int96([u32; 3]);

pub trait ParquetFixedWidthType: Debug + Send + Sync + Copy + 'static {
    const PHYSICAL_TYPE: PhysicalType;
    type Bytes: Sized + AsRef<[u8]> + for<'a> TryFrom<&'a [u8], Error = TryFromSliceError>;

    /// Convert self to little endian bytes.
    fn to_le_bytes(&self) -> Self::Bytes;

    /// Convert little endian bytes to self.
    fn from_le_bytes(bytes: Self::Bytes) -> Self;
}

impl ParquetFixedWidthType for i32 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32;
    type Bytes = [u8; 4];

    fn to_le_bytes(&self) -> Self::Bytes {
        i32::to_le_bytes(*self)
    }

    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        i32::from_le_bytes(bytes)
    }
}

impl ParquetFixedWidthType for i64 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int64;
    type Bytes = [u8; 8];

    fn to_le_bytes(&self) -> Self::Bytes {
        i64::to_le_bytes(*self)
    }

    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        i64::from_le_bytes(bytes)
    }
}

impl ParquetFixedWidthType for f32 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Float;
    type Bytes = [u8; 4];

    fn to_le_bytes(&self) -> Self::Bytes {
        f32::to_le_bytes(*self)
    }

    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        f32::from_le_bytes(bytes)
    }
}

impl ParquetFixedWidthType for f64 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Double;
    type Bytes = [u8; 8];

    fn to_le_bytes(&self) -> Self::Bytes {
        f64::to_le_bytes(*self)
    }

    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        f64::from_le_bytes(bytes)
    }
}

impl ParquetFixedWidthType for Int96 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int96;
    type Bytes = [u8; 12];

    fn to_le_bytes(&self) -> Self::Bytes {
        let mut buf = [0; 12];
        buf[0..4].copy_from_slice(&u32::to_le_bytes(self.0[0]));
        buf[4..8].copy_from_slice(&u32::to_le_bytes(self.0[1]));
        buf[8..12].copy_from_slice(&u32::to_le_bytes(self.0[2]));
        buf
    }

    fn from_le_bytes(bytes: Self::Bytes) -> Self {
        let a = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let b = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let c = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        Int96([a, b, c])
    }
}
