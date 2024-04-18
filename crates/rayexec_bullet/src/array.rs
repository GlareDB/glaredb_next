use crate::bitmap::Bitmap;
use crate::scalar::ScalarValue;
use crate::storage::PrimitiveStorage;

#[derive(Debug)]
pub enum Array {
    Null(NullArray),
    Boolean(BooleanArray),
    Float32(Float32Array),
    Float64(Float64Array),
    Int32(Int32Array),
    Int64(Int64Array),
    UInt32(UInt32Array),
    UInt64(UInt64Array),
}

impl Array {
    /// Get a scalar value at the given index.
    pub fn scalar(&self, idx: usize) -> Option<ScalarValue> {
        if !self.is_valid(idx)? {
            return Some(ScalarValue::Null);
        }

        Some(match self {
            Self::Null(_) => panic!("nulls should be handled by validity check"),
            Self::Boolean(arr) => ScalarValue::Boolean(arr.raw_value(idx)?),
            Self::Float32(arr) => ScalarValue::Float32(*arr.raw_value(idx)?),
            Self::Float64(arr) => ScalarValue::Float64(*arr.raw_value(idx)?),
            Self::Int32(arr) => ScalarValue::Int32(*arr.raw_value(idx)?),
            Self::Int64(arr) => ScalarValue::Int64(*arr.raw_value(idx)?),
            Self::UInt32(arr) => ScalarValue::UInt32(*arr.raw_value(idx)?),
            Self::UInt64(arr) => ScalarValue::UInt64(*arr.raw_value(idx)?),
        })
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        match self {
            Self::Null(arr) => arr.is_valid(idx),
            Self::Boolean(arr) => arr.is_valid(idx),
            Self::Float32(arr) => arr.is_valid(idx),
            Self::Float64(arr) => arr.is_valid(idx),
            Self::Int32(arr) => arr.is_valid(idx),
            Self::Int64(arr) => arr.is_valid(idx),
            Self::UInt32(arr) => arr.is_valid(idx),
            Self::UInt64(arr) => arr.is_valid(idx),
        }
    }
}

/// A logical array for representing some number of Nulls.
#[derive(Debug, PartialEq)]
pub struct NullArray {
    len: usize,
}

impl NullArray {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len {
            return None;
        }
        Some(false)
    }
}

/// A logical array for representing bools.
#[derive(Debug, PartialEq)]
pub struct BooleanArray {
    validity: Bitmap,
    values: Bitmap,
}

impl BooleanArray {
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(self.validity.value(idx))
    }

    pub fn raw_value(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(self.values.value(idx))
    }
}

/// Array for storing primitive values.
#[derive(Debug, PartialEq)]
pub struct PrimitiveArray<T> {
    /// Validity bitmap.
    ///
    /// "True" values indicate the value at index is valid, "false" indicates
    /// null.
    validity: Bitmap,

    /// Underlying primitive values.
    values: PrimitiveStorage<T>,
}

pub type Int32Array = PrimitiveArray<i32>;
pub type Int64Array = PrimitiveArray<i64>;
pub type UInt32Array = PrimitiveArray<u32>;
pub type UInt64Array = PrimitiveArray<u64>;
pub type Float32Array = PrimitiveArray<f32>;
pub type Float64Array = PrimitiveArray<f64>;

impl<T> PrimitiveArray<T> {
    pub fn len(&self) -> usize {
        self.validity.len()
    }

    /// Get the raw value at the given index.
    ///
    /// This does not take validity into account.
    pub fn raw_value(&self, idx: usize) -> Option<&T> {
        if idx >= self.len() {
            return None;
        }

        self.values.get(idx)
    }

    /// Get the validity at the given index.
    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(self.validity.value(idx))
    }
}
