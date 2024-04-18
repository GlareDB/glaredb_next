use crate::bitmap::Bitmap;
use crate::scalar::ScalarValue;
use crate::storage::PrimitiveStorage;
use std::fmt::Debug;
use std::marker::PhantomData;

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
    Utf8(Utf8Array),
    LargeUtf8(LargeUtf8Array),
    Binary(BinaryArray),
    LargeBinary(LargeBinaryArray),
}

impl Array {
    /// Get a scalar value at the given index.
    pub fn scalar(&self, idx: usize) -> Option<ScalarValue> {
        if !self.is_valid(idx)? {
            return Some(ScalarValue::Null);
        }

        Some(match self {
            Self::Null(_) => panic!("nulls should be handled by validity check"),
            Self::Boolean(arr) => ScalarValue::Boolean(arr.value(idx)?),
            Self::Float32(arr) => ScalarValue::Float32(*arr.value(idx)?),
            Self::Float64(arr) => ScalarValue::Float64(*arr.value(idx)?),
            Self::Int32(arr) => ScalarValue::Int32(*arr.value(idx)?),
            Self::Int64(arr) => ScalarValue::Int64(*arr.value(idx)?),
            Self::UInt32(arr) => ScalarValue::UInt32(*arr.value(idx)?),
            Self::UInt64(arr) => ScalarValue::UInt64(*arr.value(idx)?),
            Self::Utf8(arr) => ScalarValue::Utf8(arr.value(idx)?.into()),
            Self::LargeUtf8(arr) => ScalarValue::Utf8(arr.value(idx)?.into()),
            Self::Binary(arr) => ScalarValue::Binary(arr.value(idx)?.into()),
            Self::LargeBinary(arr) => ScalarValue::LargeBinary(arr.value(idx)?.into()),
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
            Self::Utf8(arr) => arr.is_valid(idx),
            Self::LargeUtf8(arr) => arr.is_valid(idx),
            Self::Binary(arr) => arr.is_valid(idx),
            Self::LargeBinary(arr) => arr.is_valid(idx),
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

    pub fn value(&self, idx: usize) -> Option<bool> {
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

    /// Get the value at the given index.
    ///
    /// This does not take validity into account.
    pub fn value(&self, idx: usize) -> Option<&T> {
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

/// Trait for determining how to interpret binary data stored in a variable
/// length array.
pub trait VarlenType {
    /// The user-facing type.
    type Output: ?Sized;

    /// Interpret some binary input into an output type.
    fn interpret(input: &[u8]) -> &Self::Output;
}

/// Interpret binary as... binary.
#[derive(Debug, Clone, Copy)]
pub struct VarlenBinary;

impl VarlenType for VarlenBinary {
    type Output = [u8];

    fn interpret(input: &[u8]) -> &Self::Output {
        input
    }
}

/// Interpret binary as a utf8 string.
#[derive(Debug, Clone, Copy)]
pub struct VarlenUtf8;

impl VarlenType for VarlenUtf8 {
    type Output = str;

    fn interpret(input: &[u8]) -> &Self::Output {
        std::str::from_utf8(input).expect("input should be valid utf8")
    }
}

pub trait OffsetIndex {
    fn as_usize(&self) -> usize;
}

impl OffsetIndex for i32 {
    fn as_usize(&self) -> usize {
        (*self).try_into().expect("index to be greater than zero")
    }
}

impl OffsetIndex for i64 {
    fn as_usize(&self) -> usize {
        (*self).try_into().expect("index to be greater than zero")
    }
}

#[derive(Debug)]
pub struct VarlenArray<T: VarlenType, O: OffsetIndex> {
    /// Value validities.
    validity: Bitmap,

    /// Offsets into the data buffer.
    ///
    /// Length should be one more than the number of values being held in this
    /// array.
    offsets: PrimitiveStorage<O>,

    /// The raw data.
    data: PrimitiveStorage<u8>,

    /// How to interpret the binary data.
    varlen_type: PhantomData<T>,
}

pub type Utf8Array = VarlenArray<VarlenUtf8, i32>;
pub type LargeUtf8Array = VarlenArray<VarlenUtf8, i64>;
pub type BinaryArray = VarlenArray<VarlenBinary, i32>;
pub type LargeBinaryArray = VarlenArray<VarlenBinary, i64>;

impl<T, O> VarlenArray<T, O>
where
    T: VarlenType,
    O: OffsetIndex,
{
    pub fn len(&self) -> usize {
        self.validity.len()
    }

    pub fn value(&self, idx: usize) -> Option<&T::Output> {
        if idx >= self.len() {
            return None;
        }

        let offset = self
            .offsets
            .get(idx)
            .expect("offset for idx to exist")
            .as_usize();
        let len: usize = self
            .offsets
            .get(idx + 1)
            .expect("offset for idx+1 to exist")
            .as_usize();

        let val = self
            .data
            .get_slice(offset, len)
            .expect("value to exist in data array");
        let val = T::interpret(val);

        Some(val)
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(self.validity.value(idx))
    }
}
