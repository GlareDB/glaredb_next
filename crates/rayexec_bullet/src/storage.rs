use std::ops::{Deref, DerefMut};

/// Backing storage for primitive values.
///
/// Currently this contains only a single variant, but should be extension point
/// for working with externally managed data (Arrow arrays from arrow-rs, shared
/// memory regions, CUDA, etc).
#[derive(Debug, PartialEq)]
pub enum PrimitiveStorage<T> {
    Vec(Vec<T>),

    // UNUSED DO NOT USE.
    //
    // Added this variant just to make sure derefs work as expected.
    SlicedVec { offset: usize, data: Vec<T> },
}

impl<T> PrimitiveStorage<T> {}

impl<T> From<Vec<T>> for PrimitiveStorage<T> {
    fn from(value: Vec<T>) -> Self {
        PrimitiveStorage::Vec(value)
    }
}

impl<T> AsRef<[T]> for PrimitiveStorage<T> {
    fn as_ref(&self) -> &[T] {
        self
    }
}

impl<T> AsMut<[T]> for PrimitiveStorage<T> {
    fn as_mut(&mut self) -> &mut [T] {
        self
    }
}

impl<T> Deref for PrimitiveStorage<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Vec(v) => v,
            Self::SlicedVec { offset, data } => &data[*offset..],
        }
    }
}

impl<T> DerefMut for PrimitiveStorage<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Vec(v) => v,
            Self::SlicedVec { offset, data } => &mut data[*offset..],
        }
    }
}
