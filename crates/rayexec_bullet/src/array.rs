use crate::bitmap::Bitmap;
use crate::scalar::ScalarValue;
use crate::storage::PrimitiveStorage;
use crate::validity::Validity;
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
    validity: Validity,
    values: Bitmap,
}

impl BooleanArray {
    pub fn from_iter(iter: impl IntoIterator<Item = bool>) -> Self {
        Self::new_with_values(Bitmap::from_bool_iter(iter))
    }

    pub fn new_with_values(values: Bitmap) -> Self {
        BooleanArray {
            validity: Validity::default(),
            values,
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(self.validity.is_valid(idx))
    }

    pub fn value(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(self.values.value(idx))
    }

    pub(crate) fn values(&self) -> &Bitmap {
        &self.values
    }
}

pub trait PrimitiveNumeric {
    const MIN_VALUE: Self;
    const MAX_VALUE: Self;
    const ZERO_VALUE: Self;
}

impl PrimitiveNumeric for i8 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;
}

impl PrimitiveNumeric for i16 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;
}

impl PrimitiveNumeric for i32 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;
}

impl PrimitiveNumeric for i64 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;
}

impl PrimitiveNumeric for u8 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;
}

impl PrimitiveNumeric for u16 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;
}

impl PrimitiveNumeric for u32 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;
}

impl PrimitiveNumeric for u64 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;
}

impl PrimitiveNumeric for f32 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0.0;
}

impl PrimitiveNumeric for f64 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0.0;
}

/// Array for storing primitive values.
#[derive(Debug, PartialEq)]
pub struct PrimitiveArray<T> {
    /// Validity bitmap.
    ///
    /// "True" values indicate the value at index is valid, "false" indicates
    /// null.
    validity: Validity,

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
        self.values.len()
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

        Some(self.validity.is_valid(idx))
    }

    /// Get a reference to the underlying validity bitmap.
    pub(crate) fn validity(&self) -> &Validity {
        &self.validity
    }

    /// Get a reference to the underlying primitive values.
    pub(crate) fn values(&self) -> &PrimitiveStorage<T> {
        &self.values
    }

    /// Get a mutable reference to the underlying primitive values.
    pub(crate) fn values_mut(&mut self) -> &mut PrimitiveStorage<T> {
        &mut self.values
    }
}

impl<A> FromIterator<A> for PrimitiveArray<A> {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let values = PrimitiveStorage::from(iter.into_iter().collect::<Vec<_>>());
        PrimitiveArray {
            validity: Validity::default(),
            values,
        }
    }
}

impl<A: Default> FromIterator<Option<A>> for PrimitiveArray<A> {
    fn from_iter<T: IntoIterator<Item = Option<A>>>(iter: T) -> Self {
        let mut validity = Bitmap::default();
        let mut values = Vec::new();

        for item in iter {
            match item {
                Some(value) => {
                    validity.push(true);
                    values.push(value);
                }
                None => {
                    validity.push(false);
                    values.push(A::default());
                }
            }
        }

        PrimitiveArray {
            validity: Validity(Some(validity)),
            values: values.into(),
        }
    }
}

/// Trait for determining how to interpret binary data stored in a variable
/// length array.
pub trait VarlenType: PartialEq {
    /// Interpret some binary input into an output type.
    fn interpret(input: &[u8]) -> &Self;

    /// Convert self into binary.
    fn as_binary(&self) -> &[u8];
}

impl VarlenType for [u8] {
    fn interpret(input: &[u8]) -> &Self {
        input
    }

    fn as_binary(&self) -> &[u8] {
        self
    }
}

impl VarlenType for str {
    fn interpret(input: &[u8]) -> &Self {
        std::str::from_utf8(input).expect("input should be valid utf8")
    }

    fn as_binary(&self) -> &[u8] {
        self.as_bytes()
    }
}

pub trait OffsetIndex {
    fn as_usize(&self) -> usize;
    fn from_usize(u: usize) -> Self;
}

impl OffsetIndex for i32 {
    fn as_usize(&self) -> usize {
        (*self).try_into().expect("index to be greater than zero")
    }

    fn from_usize(u: usize) -> Self {
        u as Self
    }
}

impl OffsetIndex for i64 {
    fn as_usize(&self) -> usize {
        (*self).try_into().expect("index to be greater than zero")
    }

    fn from_usize(u: usize) -> Self {
        u as Self
    }
}

#[derive(Debug)]
pub struct VarlenArray<T: VarlenType + ?Sized, O: OffsetIndex> {
    /// Value validities.
    validity: Validity,

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

pub type Utf8Array = VarlenArray<str, i32>;
pub type LargeUtf8Array = VarlenArray<str, i64>;
pub type BinaryArray = VarlenArray<[u8], i32>;
pub type LargeBinaryArray = VarlenArray<[u8], i64>;

impl<T, O> VarlenArray<T, O>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    /// Create a new varlen array from an iterator of values.
    // TODO: The lifetime bound works, but could probably be a bit confusing to
    // read. See if there's a good way to remove it.
    pub fn from_iter<'a>(iter: impl IntoIterator<Item = &'a T>) -> Self
    where
        T: 'a,
    {
        let mut offsets: Vec<O> = vec![O::from_usize(0)];
        let mut data: Vec<u8> = Vec::new();

        for item in iter.into_iter() {
            data.extend(item.as_binary());
            let offset = data.len();
            offsets.push(O::from_usize(offset));
        }

        let offsets = PrimitiveStorage::from(offsets);
        let data = PrimitiveStorage::from(data);

        VarlenArray {
            validity: Validity::default(),
            offsets,
            data,
            varlen_type: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn value(&self, idx: usize) -> Option<&T> {
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

        Some(self.validity.is_valid(idx))
    }

    /// Get a reference to the underlying validity bitmap.
    pub(crate) fn validity(&self) -> &Validity {
        &self.validity
    }

    pub fn values_iter(&self) -> VarlenArrayIter<'_, T, O> {
        VarlenArrayIter { idx: 0, arr: self }
    }
}

#[derive(Debug)]
pub struct VarlenArrayIter<'a, T: VarlenType + ?Sized, O: OffsetIndex> {
    idx: usize,
    arr: &'a VarlenArray<T, O>,
}

impl<'a, T, O> Iterator for VarlenArrayIter<'a, T, O>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        let val = self.arr.value(self.idx);
        self.idx += 1;
        val
    }
}

impl<T, O> PartialEq for VarlenArray<T, O>
where
    T: VarlenType + ?Sized,
    O: OffsetIndex,
{
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        // TODO: Validity check

        let left = self.values_iter();
        let right = other.values_iter();

        left.zip(right).all(|(left, right)| left == right)
    }
}
