use std::fmt;

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

/// Metadata associated with decimals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DecimalTypMeta {
    pub precision: u8,
    pub scale: i8,
}

/// Metadata associated with structs.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct StructTypeMeta {
    pub fields: Vec<(String, DataType)>,
}

/// Metadata associated with lists.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ListTypeMeta {
    pub datatype: Box<DataType>,
}

/// Supported data types.
///
/// This generally follows Arrow's type system, but is not restricted to it.
///
/// Some types may include additional metadata, and so will have an attached
/// `TypeMeta` field.
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum DataType {
    /// Constant null columns.
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    /// 64-bit decimal.
    Decimal64(TypeMeta<DecimalTypMeta>),
    /// 128-bit decimal.
    Decimal128(TypeMeta<DecimalTypMeta>),
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
    pub const fn is_null(&self) -> bool {
        match self {
            Self::Null => true,
            _ => false,
        }
    }

    /// Compare the equality of two data types without comparing any type
    /// metadata.
    pub const fn eq_no_meta(&self, other: &Self) -> bool {
        match (self, other) {
            (DataType::Null, DataType::Null) => true,
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
            Self::Null => write!(f, "Null"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Int8 => write!(f, "Int8"),
            Self::Int16 => write!(f, "Int16"),
            Self::Int32 => write!(f, "Int32"),
            Self::Int64 => write!(f, "Int64"),
            Self::UInt8 => write!(f, "UInt8"),
            Self::UInt16 => write!(f, "UInt16"),
            Self::UInt32 => write!(f, "UInt32"),
            Self::UInt64 => write!(f, "UInt64"),
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
