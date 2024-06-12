use std::fmt;

/// Data types with extended metadata.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DataType {
    Null,
    Boolean,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    /// 64-bit decimal.
    Decimal64(u8, i8),
    /// 128-bit decimal.
    Decimal128(u8, i8),
    /// Timestamp at a specific time.
    Timestamp(TimeUnit),
    /// Days since epoch.
    Date32,
    /// Millisecond since epoch.
    Date64,
    Interval(IntervalUnit),
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Struct {
        fields: Vec<DataType>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntervalUnit {
    /// YEAR_MONTH
    ///
    /// '1-2' => 1 year 2 months
    ///
    /// Store as whole months in a 4 byte int.
    YearMonth,
    /// DAY_TIME
    ///
    /// '3 4:05:06' -> 3 days 4:05:06
    ///
    /// Stored as two contiguous 4 byte integers representing days and
    /// milliseconds.
    DayTime,
}

impl fmt::Display for IntervalUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::YearMonth => write!(f, "ym"),
            Self::DayTime => write!(f, "dt"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Nanosecond => write!(f, "ns"),
            Self::Microsecond => write!(f, "μs"),
            Self::Millisecond => write!(f, "ms"),
            Self::Second => write!(f, "s"),
        }
    }
}

impl DataType {
    pub const fn is_numeric(&self) -> bool {
        matches!(
            self,
            Self::Float32
                | Self::Float64
                | Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::UInt8
                | Self::UInt16
                | Self::UInt32
                | Self::UInt64
                | Self::Decimal64(_, _)
                | Self::Decimal128(_, _)
        )
    }

    pub const fn is_decimal(&self) -> bool {
        matches!(self, Self::Decimal64(_, _) | Self::Decimal128(_, _))
    }

    pub const fn is_integer(&self) -> bool {
        matches!(
            self,
            Self::Int8
                | Self::Int16
                | Self::Int32
                | Self::Int64
                | Self::UInt8
                | Self::UInt16
                | Self::UInt32
                | Self::UInt64
        )
    }

    pub const fn is_float(&self) -> bool {
        matches!(self, Self::Float32 | Self::Float64)
    }

    pub const fn is_signed_integer(&self) -> bool {
        matches!(self, Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64)
    }

    pub const fn is_unsigned_integer(&self) -> bool {
        matches!(
            self,
            Self::UInt8 | Self::UInt16 | Self::UInt32 | Self::UInt64
        )
    }

    pub const fn is_string(&self) -> bool {
        matches!(self, Self::Utf8 | Self::LargeUtf8)
    }

    pub const fn is_temporal(&self) -> bool {
        matches!(
            self,
            Self::Timestamp(_) | Self::Date32 | Self::Date64 | Self::Interval(_)
        )
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => write!(f, "Null"),
            Self::Boolean => write!(f, "Boolean"),
            Self::Float32 => write!(f, "Float32"),
            Self::Float64 => write!(f, "Float64"),
            Self::Int8 => write!(f, "Int8"),
            Self::Int16 => write!(f, "Int16"),
            Self::Int32 => write!(f, "Int32"),
            Self::Int64 => write!(f, "Int64"),
            Self::UInt8 => write!(f, "UInt8"),
            Self::UInt16 => write!(f, "UInt16"),
            Self::UInt32 => write!(f, "UInt32"),
            Self::UInt64 => write!(f, "UInt64"),
            Self::Decimal64(p, s) => write!(f, "Decimal64({p}, {s})"),
            Self::Decimal128(p, s) => write!(f, "Decimal128({p}, {s})"),
            Self::Timestamp(unit) => write!(f, "Timestamp({unit})"),
            Self::Date32 => write!(f, "Date32"),
            Self::Date64 => write!(f, "Date64"),
            Self::Interval(unit) => write!(f, "Interval({unit})"),
            Self::Utf8 => write!(f, "Utf8"),
            Self::LargeUtf8 => write!(f, "LargeUtf8"),
            Self::Binary => write!(f, "Binary"),
            Self::LargeBinary => write!(f, "LargeBinary"),
            Self::Struct { fields } => write!(
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

/// A named field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
}

impl Field {
    pub fn new(name: impl Into<String>, datatype: DataType, nullable: bool) -> Self {
        Field {
            name: name.into(),
            datatype,
            nullable,
        }
    }
}

/// Represents the full schema of an output batch.
///
/// Includes the names and nullability of each of the columns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    /// Create an empty schema.
    pub const fn empty() -> Self {
        Schema { fields: Vec::new() }
    }

    pub fn new(fields: impl IntoIterator<Item = Field>) -> Self {
        Schema {
            fields: fields.into_iter().collect(),
        }
    }

    pub fn merge(self, other: Schema) -> Self {
        Schema {
            fields: self.fields.into_iter().chain(other.fields).collect(),
        }
    }

    /// Return an iterator over all the fields in the schema.
    pub fn iter(&self) -> impl Iterator<Item = &Field> {
        self.fields.iter()
    }

    pub fn type_schema(&self) -> TypeSchema {
        TypeSchema {
            types: self
                .fields
                .iter()
                .map(|field| field.datatype.clone())
                .collect(),
        }
    }

    /// Convert the schema into a type schema.
    pub fn into_type_schema(self) -> TypeSchema {
        TypeSchema {
            types: self
                .fields
                .into_iter()
                .map(|field| field.datatype)
                .collect(),
        }
    }
}

/// Represents the output types of a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeSchema {
    pub types: Vec<DataType>,
}

impl TypeSchema {
    /// Create an empty type schema.
    pub const fn empty() -> Self {
        TypeSchema { types: Vec::new() }
    }

    pub fn new(types: impl IntoIterator<Item = DataType>) -> Self {
        TypeSchema {
            types: types.into_iter().collect(),
        }
    }

    pub fn merge(self, other: TypeSchema) -> Self {
        TypeSchema {
            types: self.types.into_iter().chain(other.types).collect(),
        }
    }
}
