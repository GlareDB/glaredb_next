use rayexec_error::{RayexecError, Result};

use crate::{
    array::{ArrayData, BinaryData},
    storage::{ContiguousVarlenIter, SharedHeapIter},
};

/// Helper trait for getting the underlying data for an array.
pub trait PhysicalType<'a> {
    type IterItem: 'a;
    type StorageIter: Iterator<Item = Self::IterItem> + 'a;

    fn get_storage_iter(data: &'a ArrayData) -> Result<Self::StorageIter>;
}

impl<'a> PhysicalType<'a> for i8 {
    type IterItem = &'a i8;
    type StorageIter = std::slice::Iter<'a, i8>;

    fn get_storage_iter(data: &'a ArrayData) -> Result<Self::StorageIter> {
        match data {
            ArrayData::Int8(storage) => Ok(storage.iter()),
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

impl<'a> PhysicalType<'a> for [u8] {
    type IterItem = &'a [u8];
    type StorageIter = BinaryDataIter<'a>;

    fn get_storage_iter(data: &'a ArrayData) -> Result<Self::StorageIter> {
        match data {
            ArrayData::Binary(binary) => match binary {
                BinaryData::Binary(b) => Ok(BinaryDataIter::Binary(b.iter())),
                BinaryData::LargeBinary(b) => Ok(BinaryDataIter::LargeBinary(b.iter())),
                BinaryData::SharedHeap(b) => Ok(BinaryDataIter::SharedHeap(b.iter())),
            },
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

impl<'a> PhysicalType<'a> for str {
    type IterItem = &'a str;
    type StorageIter = StrDataIter<'a>;

    fn get_storage_iter(data: &'a ArrayData) -> Result<Self::StorageIter> {
        match data {
            ArrayData::Binary(binary) => match binary {
                BinaryData::Binary(b) => Ok(BinaryDataIter::Binary(b.iter()).into()),
                BinaryData::LargeBinary(b) => Ok(BinaryDataIter::LargeBinary(b.iter()).into()),
                BinaryData::SharedHeap(b) => Ok(BinaryDataIter::SharedHeap(b.iter()).into()),
            },
            _ => return Err(RayexecError::new("invalid storage")),
        }
    }
}

// TODO: Don't love this. But it might not matter.
#[derive(Debug)]
pub enum BinaryDataIter<'a> {
    Binary(ContiguousVarlenIter<'a, i32>),
    LargeBinary(ContiguousVarlenIter<'a, i64>),
    SharedHeap(SharedHeapIter<'a>),
}

impl<'a> Iterator for BinaryDataIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Binary(it) => it.next(),
            Self::LargeBinary(it) => it.next(),
            Self::SharedHeap(it) => it.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Binary(it) => it.size_hint(),
            Self::LargeBinary(it) => it.size_hint(),
            Self::SharedHeap(it) => it.size_hint(),
        }
    }
}

impl<'a> ExactSizeIterator for BinaryDataIter<'a> {}

#[derive(Debug)]
pub struct StrDataIter<'a> {
    inner: BinaryDataIter<'a>,
}

impl<'a> Iterator for StrDataIter<'a> {
    type Item = &'a str;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let bs = self.inner.next()?;
        // SAFETY: String data should be verified when constructing the backing
        // storage, not when reading.
        let s = unsafe { std::str::from_utf8_unchecked(bs) };
        Some(s)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a> ExactSizeIterator for StrDataIter<'a> {}

impl<'a> From<BinaryDataIter<'a>> for StrDataIter<'a> {
    fn from(value: BinaryDataIter<'a>) -> Self {
        StrDataIter { inner: value }
    }
}
