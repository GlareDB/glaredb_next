use rayexec_error::{RayexecError, Result};

use crate::{
    array::{ArrayData, BinaryData},
    scalar::interval::Interval,
    storage::{
        AddressableStorage, BooleanStorage, BooleanStorageRef, ContiguousVarlenStorageSlice,
        GermanVarlenStorageSlice, PrimitiveStorageSlice, SharedHeapStorageSlice,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalType {
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
    Interval,
    Binary,
    Utf8,
}

pub trait VarlenType {
    fn as_bytes(&self) -> &[u8];
}

impl VarlenType for str {
    fn as_bytes(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl VarlenType for [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

/// Helper trait for getting the underlying data for an array.
pub trait PhysicalStorage<'a> {
    type Storage: AddressableStorage;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage>;
}

pub struct PhysicalBool;

impl<'a> PhysicalStorage<'a> for PhysicalBool {
    type Storage = BooleanStorageRef<'a>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Boolean(storage) => Ok(storage.as_boolean_storage_ref()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalI8;

impl<'a> PhysicalStorage<'a> for PhysicalI8 {
    type Storage = PrimitiveStorageSlice<'a, i8>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Int8(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalI16;

impl<'a> PhysicalStorage<'a> for PhysicalI16 {
    type Storage = PrimitiveStorageSlice<'a, i16>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Int16(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalI32;

impl<'a> PhysicalStorage<'a> for PhysicalI32 {
    type Storage = PrimitiveStorageSlice<'a, i32>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Int32(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalI64;

impl<'a> PhysicalStorage<'a> for PhysicalI64 {
    type Storage = PrimitiveStorageSlice<'a, i64>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Int64(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalI128;

impl<'a> PhysicalStorage<'a> for PhysicalI128 {
    type Storage = PrimitiveStorageSlice<'a, i128>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Int128(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalU8;

impl<'a> PhysicalStorage<'a> for PhysicalU8 {
    type Storage = PrimitiveStorageSlice<'a, u8>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::UInt8(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalU16;

impl<'a> PhysicalStorage<'a> for PhysicalU16 {
    type Storage = PrimitiveStorageSlice<'a, u16>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::UInt16(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalU32;

impl<'a> PhysicalStorage<'a> for PhysicalU32 {
    type Storage = PrimitiveStorageSlice<'a, u32>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::UInt32(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalU64;

impl<'a> PhysicalStorage<'a> for PhysicalU64 {
    type Storage = PrimitiveStorageSlice<'a, u64>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::UInt64(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalU128;

impl<'a> PhysicalStorage<'a> for PhysicalU128 {
    type Storage = PrimitiveStorageSlice<'a, u128>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::UInt128(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalF32;

impl<'a> PhysicalStorage<'a> for PhysicalF32 {
    type Storage = PrimitiveStorageSlice<'a, f32>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Float32(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalF64;

impl<'a> PhysicalStorage<'a> for PhysicalF64 {
    type Storage = PrimitiveStorageSlice<'a, f64>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Float64(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalInterval;

impl<'a> PhysicalStorage<'a> for PhysicalInterval {
    type Storage = PrimitiveStorageSlice<'a, Interval>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Interval(storage) => Ok(storage.as_primitive_storage_slice()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalBinary;

impl<'a> PhysicalStorage<'a> for PhysicalBinary {
    type Storage = BinaryDataStorage<'a>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Binary(binary) => match binary {
                BinaryData::Binary(b) => {
                    Ok(BinaryDataStorage::Binary(b.as_contiguous_storage_slice()))
                }
                BinaryData::LargeBinary(b) => Ok(BinaryDataStorage::LargeBinary(
                    b.as_contiguous_storage_slice(),
                )),
                BinaryData::SharedHeap(b) => Ok(BinaryDataStorage::SharedHeap(
                    b.as_shared_heap_storage_slice(),
                )),
                BinaryData::German(b) => Ok(BinaryDataStorage::German(b.as_german_storage_slice())),
            },
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

pub struct PhysicalUtf8;

impl<'a> PhysicalStorage<'a> for PhysicalUtf8 {
    type Storage = StrDataStorage<'a>;

    fn get_storage(data: &'a ArrayData) -> Result<Self::Storage> {
        match data {
            ArrayData::Binary(binary) => match binary {
                BinaryData::Binary(b) => {
                    Ok(BinaryDataStorage::Binary(b.as_contiguous_storage_slice()).into())
                }
                BinaryData::LargeBinary(b) => {
                    Ok(BinaryDataStorage::LargeBinary(b.as_contiguous_storage_slice()).into())
                }
                BinaryData::SharedHeap(b) => {
                    Ok(BinaryDataStorage::SharedHeap(b.as_shared_heap_storage_slice()).into())
                }
                BinaryData::German(b) => {
                    Ok(BinaryDataStorage::German(b.as_german_storage_slice()).into())
                }
            },
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

#[derive(Debug)]
pub enum BinaryDataStorage<'a> {
    Binary(ContiguousVarlenStorageSlice<'a, i32>),
    LargeBinary(ContiguousVarlenStorageSlice<'a, i64>),
    SharedHeap(SharedHeapStorageSlice<'a>),
    German(GermanVarlenStorageSlice<'a>),
}

impl<'a> AddressableStorage for BinaryDataStorage<'a> {
    type T = &'a [u8];

    fn len(&self) -> usize {
        match self {
            Self::Binary(s) => s.len(),
            Self::LargeBinary(s) => s.len(),
            Self::SharedHeap(s) => s.len(),
            Self::German(s) => s.len(),
        }
    }

    fn get(&self, idx: usize) -> Option<Self::T> {
        match self {
            Self::Binary(s) => s.get(idx),
            Self::LargeBinary(s) => s.get(idx),
            Self::SharedHeap(s) => s.get(idx),
            Self::German(s) => s.get(idx),
        }
    }

    #[inline]
    unsafe fn get_unchecked(&self, idx: usize) -> Self::T {
        match self {
            Self::Binary(s) => s.get_unchecked(idx),
            Self::LargeBinary(s) => s.get_unchecked(idx),
            Self::SharedHeap(s) => s.get_unchecked(idx),
            Self::German(s) => s.get_unchecked(idx),
        }
    }
}

#[derive(Debug)]
pub struct StrDataStorage<'a> {
    inner: BinaryDataStorage<'a>,
}

impl<'a> AddressableStorage for StrDataStorage<'a> {
    type T = &'a str;

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn get(&self, idx: usize) -> Option<Self::T> {
        let b = self.inner.get(idx)?;
        // SAFETY: Construction of the vector should have already validated the data.
        let s = unsafe { std::str::from_utf8_unchecked(b) };
        Some(s)
    }

    #[inline]
    unsafe fn get_unchecked(&self, idx: usize) -> Self::T {
        let b = self.inner.get_unchecked(idx);
        unsafe { std::str::from_utf8_unchecked(b) } // See above
    }
}

impl<'a> From<BinaryDataStorage<'a>> for StrDataStorage<'a> {
    fn from(value: BinaryDataStorage<'a>) -> Self {
        StrDataStorage { inner: value }
    }
}
