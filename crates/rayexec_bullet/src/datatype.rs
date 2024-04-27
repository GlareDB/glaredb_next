use std::hash::Hash;

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
