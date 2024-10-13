use std::fmt::Debug;

use crate::physical_type::PhysicalType;

/// Parquet Int96 type. Deprecated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Int96([u32; 3]);

pub trait ParquetPrimitiveType: Debug + Send + Sync + Copy + 'static {
    const PHYSICAL_TYPE: PhysicalType;
    type AsBytes: Sized + AsRef<[u8]>;

    /// Convert self to little endian bytes.
    fn to_le_bytes(&self) -> Self::AsBytes;

    /// Convert little endian bytes to self.
    fn from_le_bytes(bytes: Self::AsBytes) -> Self;
}

impl ParquetPrimitiveType for i32 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int32;
    type AsBytes = [u8; 4];

    fn to_le_bytes(&self) -> Self::AsBytes {
        i32::to_le_bytes(*self)
    }

    fn from_le_bytes(bytes: Self::AsBytes) -> Self {
        i32::from_le_bytes(bytes)
    }
}

impl ParquetPrimitiveType for i64 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int64;
    type AsBytes = [u8; 8];

    fn to_le_bytes(&self) -> Self::AsBytes {
        i64::to_le_bytes(*self)
    }

    fn from_le_bytes(bytes: Self::AsBytes) -> Self {
        i64::from_le_bytes(bytes)
    }
}

impl ParquetPrimitiveType for f32 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Float;
    type AsBytes = [u8; 4];

    fn to_le_bytes(&self) -> Self::AsBytes {
        f32::to_le_bytes(*self)
    }

    fn from_le_bytes(bytes: Self::AsBytes) -> Self {
        f32::from_le_bytes(bytes)
    }
}

impl ParquetPrimitiveType for f64 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Double;
    type AsBytes = [u8; 8];

    fn to_le_bytes(&self) -> Self::AsBytes {
        f64::to_le_bytes(*self)
    }

    fn from_le_bytes(bytes: Self::AsBytes) -> Self {
        f64::from_le_bytes(bytes)
    }
}

impl ParquetPrimitiveType for Int96 {
    const PHYSICAL_TYPE: PhysicalType = PhysicalType::Int96;
    type AsBytes = [u8; 12];

    fn to_le_bytes(&self) -> Self::AsBytes {
        let mut buf = [0; 12];
        buf[0..4].copy_from_slice(&u32::to_le_bytes(self.0[0]));
        buf[4..8].copy_from_slice(&u32::to_le_bytes(self.0[1]));
        buf[8..12].copy_from_slice(&u32::to_le_bytes(self.0[2]));
        buf
    }

    fn from_le_bytes(bytes: Self::AsBytes) -> Self {
        let a = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let b = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        let c = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        Int96([a, b, c])
    }
}
