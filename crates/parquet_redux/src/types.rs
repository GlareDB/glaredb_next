use std::array::TryFromSliceError;
use std::fmt::Debug;

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
    FixedLenByteArray(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repetition {
    Required,
    Optional,
    Repeated,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeInfo {
    pub name: String,
    pub repetition: Option<Repetition>,
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
    pub fields: Vec<ParquetType>,
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
