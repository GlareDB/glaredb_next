/// All possible data types.
// TODO: Additional types (compound, decimal, timestamp, etc)
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
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
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
        )
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn new(fields: impl IntoIterator<Item = Field>) -> Self {
        Schema {
            fields: fields.into_iter().collect(),
        }
    }
}

/// Represents the output types of a batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeSchema {
    pub types: Vec<DataType>,
}

impl TypeSchema {
    pub fn new(types: impl IntoIterator<Item = DataType>) -> Self {
        TypeSchema {
            types: types.into_iter().collect(),
        }
    }
}
