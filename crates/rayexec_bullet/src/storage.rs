use rayexec_error::{RayexecError, Result};
use std::ops::Deref;

/// Backing storage for primitive values.
///
/// Currently this contains only a single variant, but should be extension point
/// for working with externally managed data (Arrow arrays from arrow-rs, shared
/// memory regions, CUDA, etc).
#[derive(Debug, PartialEq)]
pub enum PrimitiveStorage<T> {
    /// A basic vector of data.
    Vec(Vec<T>),

    /// Pointer to a raw slice of data that's potentially been externally
    /// allocated.
    // TODO: Don't use, just thinking about ffi.
    Raw { ptr: *const T, len: usize },
}

impl<T> PrimitiveStorage<T> {
    /// A potentially failable conversion to a mutable slice reference.
    pub fn try_as_mut(&mut self) -> Result<&mut [T]> {
        match self {
            Self::Vec(v) => Ok(v),
            PrimitiveStorage::Raw { .. } => Err(RayexecError::new(
                "Cannot get a mutable reference to raw value storage",
            )),
        }
    }
}

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

impl<T> Deref for PrimitiveStorage<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Vec(v) => v,
            Self::Raw { ptr, len } => unsafe { std::slice::from_raw_parts(*ptr, *len) },
        }
    }
}
