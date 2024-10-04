pub mod null;
pub use null::*;
pub mod boolean;
pub mod struct_array;
pub use boolean::*;
pub use struct_array::*;
pub mod primitive;
pub use primitive::*;
pub mod varlen;
pub use varlen::*;
pub mod list;
pub use list::*;

pub mod validity;

use crate::bitmap::Bitmap;
use crate::datatype::{DataType, DecimalTypeMeta, TimestampTypeMeta};
use crate::executor::physical_type::PhysicalType;
use crate::scalar::interval::Interval;
use crate::scalar::timestamp::TimestampScalar;
use crate::scalar::{
    decimal::{Decimal128Scalar, Decimal64Scalar},
    ScalarValue,
};
use crate::selection::SelectionVector;
use crate::storage::{
    AddressableStorage, BooleanStorage, ContiguousVarlenStorage, GermanVarlenStorage,
    PrimitiveStorage, SharedHeapStorage, UntypedNullStorage,
};
use rayexec_error::{not_implemented, RayexecError, Result, ResultExt};
use std::fmt::Debug;
use std::sync::Arc;

/// Wrapper around a selection vector allowing for owned or shared vectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    Owned(SelectionVector),
    Shared(Arc<SelectionVector>),
}

impl AsRef<SelectionVector> for Selection {
    fn as_ref(&self) -> &SelectionVector {
        match self {
            Selection::Owned(v) => &v,
            Self::Shared(v) => v.as_ref(),
        }
    }
}

impl From<SelectionVector> for Selection {
    fn from(value: SelectionVector) -> Self {
        Selection::Owned(value)
    }
}

impl From<Arc<SelectionVector>> for Selection {
    fn from(value: Arc<SelectionVector>) -> Self {
        Selection::Shared(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Array {
    /// Data type of the array.
    pub(crate) datatype: DataType,
    /// Selection of rows for the array.
    ///
    /// If set, this provides logical row mapping on top of the underlying data.
    /// If not set, then there's a one-to-one mapping between the logical row
    /// and and row in the underlying data.
    pub(crate) selection: Option<Selection>,
    /// Option validity mask.
    ///
    /// This indicates the validity of the underlying data. This does not take
    /// into account the selection vector, and always maps directly to the data.
    pub(crate) validity: Option<Bitmap>,
    /// The physical data.
    pub(crate) data: ArrayData,
}

impl Array {
    pub fn new_untyped_null_array(len: usize) -> Self {
        let data = UntypedNullStorage(len);

        Array {
            datatype: DataType::Null,
            selection: None,
            validity: None,
            data: data.into(),
        }
    }

    /// Creates a new typed array with all values being set to null.
    pub fn new_typed_null_array(datatype: DataType, len: usize) -> Result<Self> {
        // Create physical array data of length 1, and use a selection vector to
        // extend it out to the desired size.
        let data = datatype.physical_type()?.zeroed_array_data(1);
        let validity = Bitmap::new_with_all_false(1);
        let selection = SelectionVector::constant(len, 0);

        Ok(Array {
            datatype,
            selection: Some(selection.into()),
            validity: Some(validity),
            data,
        })
    }

    pub fn new_with_array_data(datatype: DataType, data: impl Into<ArrayData>) -> Self {
        Array {
            datatype,
            selection: None,
            validity: None,
            data: data.into(),
        }
    }

    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }

    pub fn selection_vector(&self) -> Option<&SelectionVector> {
        self.selection.as_ref().map(|v| v.as_ref())
    }

    /// Sets the validity for a value at a given physical index.
    pub fn set_physical_validity(&mut self, idx: usize, valid: bool) {
        match &mut self.validity {
            Some(validity) => validity.set_unchecked(idx, valid),
            None => {
                // Initialize validity.
                let len = self.data.len();
                let mut validity = Bitmap::new_with_all_true(len);
                validity.set_unchecked(idx, valid);

                self.validity = Some(validity)
            }
        }
    }

    // TODO: Validating variant too.
    pub fn put_selection(&mut self, selection: impl Into<Selection>) {
        self.selection = Some(selection.into())
    }

    /// Updates this array's selection vector.
    ///
    /// Takes into account any existing selection. This allows for repeated
    /// selection (filtering) against the same array.
    pub fn select_mut(&mut self, selection: &Selection) {
        match self.selection_vector() {
            Some(existing) => {
                // Existing selection, need to create a new vector that selects
                // from the existing vector.
                let input_sel = selection.as_ref();
                let mut new_sel = SelectionVector::with_capacity(input_sel.num_rows());

                for input_loc in input_sel.iter_locations() {
                    new_sel.push_location(existing.get_unchecked(input_loc));
                }
            }
            None => {
                // No existing selection, we can just use the provided vector
                // directly.
                self.selection = Some(selection.clone())
            }
        }
    }

    pub fn logical_len(&self) -> usize {
        match self.selection_vector() {
            Some(v) => v.num_rows(),
            None => self.data.len(),
        }
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    pub fn array_data(&self) -> &ArrayData {
        &self.data
    }

    pub fn physical_type(&self) -> PhysicalType {
        match self.data.physical_type() {
            PhysicalType::Binary => match self.datatype {
                DataType::Utf8 | DataType::LargeUtf8 => PhysicalType::Utf8,
                _ => PhysicalType::Binary,
            },
            other => other,
        }
    }

    /// Get the value at a logical index.
    ///
    /// Takes into account the validity and selection vector.
    pub fn logical_value(&self, idx: usize) -> Result<ScalarValue> {
        let idx = match self.selection_vector() {
            Some(v) => v
                .get(idx)
                .ok_or_else(|| RayexecError::new(format!("Logical index {idx} out of bounds")))?,
            None => idx,
        };

        if let Some(validity) = &self.validity {
            if !validity.value_unchecked(idx) {
                return Ok(ScalarValue::Null);
            }
        }

        self.physical_scalar(idx)
    }

    /// Gets the scalar value at the physical index.
    ///
    /// Ignores validity and selectivitity.
    pub fn physical_scalar(&self, idx: usize) -> Result<ScalarValue> {
        Ok(match &self.datatype {
            DataType::Null => match &self.data {
                ArrayData::UntypedNull(_) => ScalarValue::Null,
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Boolean => match &self.data {
                ArrayData::Boolean(arr) => arr.as_ref().as_ref().value_unchecked(idx).into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Float32 => match &self.data {
                ArrayData::Float32(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Float64 => match &self.data {
                ArrayData::Float64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int8 => match &self.data {
                ArrayData::Int8(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int16 => match &self.data {
                ArrayData::Int16(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int32 => match &self.data {
                ArrayData::Int32(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int64 => match &self.data {
                ArrayData::Int64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Int128 => match &self.data {
                ArrayData::Int64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt8 => match &self.data {
                ArrayData::UInt8(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt16 => match &self.data {
                ArrayData::UInt16(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt32 => match &self.data {
                ArrayData::UInt32(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt64 => match &self.data {
                ArrayData::UInt64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::UInt128 => match &self.data {
                ArrayData::UInt64(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Decimal64(m) => match &self.data {
                ArrayData::Int64(arr) => ScalarValue::Decimal64(Decimal64Scalar {
                    precision: m.precision,
                    scale: m.scale,
                    value: arr.as_ref().as_ref()[idx],
                }),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Decimal128(m) => match &self.data {
                ArrayData::Int128(arr) => ScalarValue::Decimal128(Decimal128Scalar {
                    precision: m.precision,
                    scale: m.scale,
                    value: arr.as_ref().as_ref()[idx],
                }),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Timestamp(m) => match &self.data {
                ArrayData::Int64(arr) => ScalarValue::Timestamp(TimestampScalar {
                    unit: m.unit.clone(),
                    value: arr.as_ref().as_ref()[idx],
                }),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Interval => match &self.data {
                ArrayData::Interval(arr) => arr.as_ref().as_ref()[idx].into(),
                _other => return Err(array_not_valid_for_type_err(&self.datatype)),
            },
            DataType::Utf8 | DataType::LargeUtf8 => {
                let v = match &self.data {
                    ArrayData::Binary(BinaryData::Binary(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData::Binary(BinaryData::LargeBinary(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData::Binary(BinaryData::SharedHeap(arr)) => arr
                        .get(idx)
                        .map(|b| b.as_ref())
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData::Binary(BinaryData::German(arr)) => arr
                        .get(idx)
                        .map(|b| b.as_ref())
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    _other => return Err(array_not_valid_for_type_err(&self.datatype)),
                };
                let s = std::str::from_utf8(v).context("binary data not valid utf8")?;
                s.into()
            }
            DataType::Binary | DataType::LargeBinary => {
                let v = match &self.data {
                    ArrayData::Binary(BinaryData::Binary(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData::Binary(BinaryData::LargeBinary(arr)) => arr
                        .get(idx)
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData::Binary(BinaryData::SharedHeap(arr)) => arr
                        .get(idx)
                        .map(|b| b.as_ref())
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    ArrayData::Binary(BinaryData::German(arr)) => arr
                        .get(idx)
                        .map(|b| b.as_ref())
                        .ok_or_else(|| RayexecError::new("missing data"))?,
                    _other => return Err(array_not_valid_for_type_err(&self.datatype)),
                };
                v.into()
            }
            other => not_implemented!("get value: {other}"),
        })
    }

    pub fn try_slice(&self, offset: usize, count: usize) -> Result<Self> {
        if offset + count > self.logical_len() {
            return Err(RayexecError::new("Slice out of bounds"));
        }
        Ok(self.slice(offset, count))
    }

    pub fn slice(&self, offset: usize, count: usize) -> Self {
        let selection = match self.selection_vector() {
            Some(sel) => sel.slice_unchecked(offset, count),
            None => SelectionVector::with_range(offset..(offset + count)),
        };

        Array {
            datatype: self.datatype.clone(),
            selection: Some(selection.into()),
            validity: self.validity.clone(),
            data: self.data.clone(),
        }
    }
}

fn array_not_valid_for_type_err(datatype: &DataType) -> RayexecError {
    RayexecError::new(format!("Array data not valid for data type: {datatype}"))
}

impl<'a> FromIterator<&'a str> for Array {
    fn from_iter<T: IntoIterator<Item = &'a str>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut german = GermanVarlenStorage::with_metadata_capacity(lower);

        for s in iter {
            german.try_push(s.as_bytes()).unwrap();
        }

        Array {
            datatype: DataType::Utf8,
            selection: None,
            validity: None,
            data: ArrayData::Binary(BinaryData::German(Arc::new(german))),
        }
    }
}

impl FromIterator<i32> for Array {
    fn from_iter<T: IntoIterator<Item = i32>>(iter: T) -> Self {
        let vals: Vec<_> = iter.into_iter().collect();
        Array {
            datatype: DataType::Int32,
            selection: None,
            validity: None,
            data: ArrayData::Int32(Arc::new(vals.into())),
        }
    }
}

impl FromIterator<bool> for Array {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        let vals: Bitmap = iter.into_iter().collect();
        Array {
            datatype: DataType::Boolean,
            selection: None,
            validity: None,
            data: ArrayData::Boolean(Arc::new(vals.into())),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ArrayData {
    UntypedNull(UntypedNullStorage),
    Boolean(Arc<BooleanStorage>),
    Float32(Arc<PrimitiveStorage<f32>>),
    Float64(Arc<PrimitiveStorage<f64>>),
    Int8(Arc<PrimitiveStorage<i8>>),
    Int16(Arc<PrimitiveStorage<i16>>),
    Int32(Arc<PrimitiveStorage<i32>>),
    Int64(Arc<PrimitiveStorage<i64>>),
    Int128(Arc<PrimitiveStorage<i128>>),
    UInt8(Arc<PrimitiveStorage<u8>>),
    UInt16(Arc<PrimitiveStorage<u16>>),
    UInt32(Arc<PrimitiveStorage<u32>>),
    UInt64(Arc<PrimitiveStorage<u64>>),
    UInt128(Arc<PrimitiveStorage<u128>>),
    Interval(Arc<PrimitiveStorage<Interval>>),
    Binary(BinaryData),
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryData {
    Binary(Arc<ContiguousVarlenStorage<i32>>),
    LargeBinary(Arc<ContiguousVarlenStorage<i64>>),
    SharedHeap(SharedHeapStorage), // TODO: Arc?
    German(Arc<GermanVarlenStorage>),
}

impl ArrayData {
    pub fn physical_type(&self) -> PhysicalType {
        match self {
            Self::UntypedNull(_) => PhysicalType::UntypedNull,
            Self::Boolean(_) => PhysicalType::Boolean,
            Self::Float32(_) => PhysicalType::Float32,
            Self::Float64(_) => PhysicalType::Float64,
            Self::Int8(_) => PhysicalType::Int8,
            Self::Int16(_) => PhysicalType::Int16,
            Self::Int32(_) => PhysicalType::Int32,
            Self::Int64(_) => PhysicalType::Int64,
            Self::Int128(_) => PhysicalType::Int128,
            Self::UInt8(_) => PhysicalType::UInt8,
            Self::UInt16(_) => PhysicalType::UInt16,
            Self::UInt32(_) => PhysicalType::UInt32,
            Self::UInt64(_) => PhysicalType::UInt64,
            Self::UInt128(_) => PhysicalType::UInt128,
            Self::Interval(_) => PhysicalType::Interval,
            Self::Binary(_) => PhysicalType::Binary,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::UntypedNull(s) => s.len(),
            Self::Boolean(s) => s.len(),
            Self::Float32(s) => s.len(),
            Self::Float64(s) => s.len(),
            Self::Int8(s) => s.len(),
            Self::Int16(s) => s.len(),
            Self::Int32(s) => s.len(),
            Self::Int64(s) => s.len(),
            Self::Int128(s) => s.len(),
            Self::UInt8(s) => s.len(),
            Self::UInt16(s) => s.len(),
            Self::UInt32(s) => s.len(),
            Self::UInt64(s) => s.len(),
            Self::UInt128(s) => s.len(),
            Self::Interval(s) => s.len(),
            Self::Binary(bin) => match bin {
                BinaryData::Binary(s) => s.len(),
                BinaryData::LargeBinary(s) => s.len(),
                BinaryData::SharedHeap(s) => s.len(),
                BinaryData::German(s) => s.len(),
            },
        }
    }
}

impl From<UntypedNullStorage> for ArrayData {
    fn from(value: UntypedNullStorage) -> Self {
        ArrayData::UntypedNull(value)
    }
}

impl From<BooleanStorage> for ArrayData {
    fn from(value: BooleanStorage) -> Self {
        ArrayData::Boolean(value.into())
    }
}

impl From<PrimitiveStorage<f32>> for ArrayData {
    fn from(value: PrimitiveStorage<f32>) -> Self {
        ArrayData::Float32(value.into())
    }
}

impl From<PrimitiveStorage<f64>> for ArrayData {
    fn from(value: PrimitiveStorage<f64>) -> Self {
        ArrayData::Float64(value.into())
    }
}

impl From<PrimitiveStorage<i8>> for ArrayData {
    fn from(value: PrimitiveStorage<i8>) -> Self {
        ArrayData::Int8(value.into())
    }
}

impl From<PrimitiveStorage<i16>> for ArrayData {
    fn from(value: PrimitiveStorage<i16>) -> Self {
        ArrayData::Int16(value.into())
    }
}

impl From<PrimitiveStorage<i32>> for ArrayData {
    fn from(value: PrimitiveStorage<i32>) -> Self {
        ArrayData::Int32(value.into())
    }
}

impl From<PrimitiveStorage<i64>> for ArrayData {
    fn from(value: PrimitiveStorage<i64>) -> Self {
        ArrayData::Int64(value.into())
    }
}

impl From<PrimitiveStorage<i128>> for ArrayData {
    fn from(value: PrimitiveStorage<i128>) -> Self {
        ArrayData::Int128(value.into())
    }
}

impl From<PrimitiveStorage<u8>> for ArrayData {
    fn from(value: PrimitiveStorage<u8>) -> Self {
        ArrayData::UInt8(value.into())
    }
}

impl From<PrimitiveStorage<u16>> for ArrayData {
    fn from(value: PrimitiveStorage<u16>) -> Self {
        ArrayData::UInt16(value.into())
    }
}

impl From<PrimitiveStorage<u32>> for ArrayData {
    fn from(value: PrimitiveStorage<u32>) -> Self {
        ArrayData::UInt32(value.into())
    }
}

impl From<PrimitiveStorage<u64>> for ArrayData {
    fn from(value: PrimitiveStorage<u64>) -> Self {
        ArrayData::UInt64(value.into())
    }
}

impl From<PrimitiveStorage<u128>> for ArrayData {
    fn from(value: PrimitiveStorage<u128>) -> Self {
        ArrayData::UInt128(value.into())
    }
}

impl From<PrimitiveStorage<Interval>> for ArrayData {
    fn from(value: PrimitiveStorage<Interval>) -> Self {
        ArrayData::Interval(value.into())
    }
}

impl From<GermanVarlenStorage> for ArrayData {
    fn from(value: GermanVarlenStorage) -> Self {
        ArrayData::Binary(BinaryData::German(Arc::new(value)))
    }
}

#[derive(Debug, PartialEq)]
pub enum Array2 {
    Null(NullArray),
    Boolean(BooleanArray),
    Float32(Float32Array),
    Float64(Float64Array),
    Int8(Int8Array),
    Int16(Int16Array),
    Int32(Int32Array),
    Int64(Int64Array),
    Int128(Int128Array),
    UInt8(UInt8Array),
    UInt16(UInt16Array),
    UInt32(UInt32Array),
    UInt64(UInt64Array),
    UInt128(UInt128Array),
    Decimal64(Decimal64Array),
    Decimal128(Decimal128Array),
    Date32(Date32Array),
    Date64(Date64Array),
    Timestamp(TimestampArray),
    Interval(IntervalArray),
    Utf8(Utf8Array),
    LargeUtf8(LargeUtf8Array),
    Binary(BinaryArray),
    LargeBinary(LargeBinaryArray),
    Struct(StructArray),
    List(ListArray),
}

impl Array2 {
    pub fn new_nulls(datatype: &DataType, len: usize) -> Self {
        match datatype {
            DataType::Null => Array2::Null(NullArray::new(len)),
            DataType::Boolean => Array2::Boolean(BooleanArray::new_nulls(len)),
            DataType::Int8 => Array2::Int8(PrimitiveArray::new_nulls(len)),
            DataType::Int16 => Array2::Int16(PrimitiveArray::new_nulls(len)),
            DataType::Int32 => Array2::Int32(PrimitiveArray::new_nulls(len)),
            DataType::Int64 => Array2::Int64(PrimitiveArray::new_nulls(len)),
            DataType::Int128 => Array2::Int128(PrimitiveArray::new_nulls(len)),
            DataType::UInt8 => Array2::UInt8(PrimitiveArray::new_nulls(len)),
            DataType::UInt16 => Array2::UInt16(PrimitiveArray::new_nulls(len)),
            DataType::UInt32 => Array2::UInt32(PrimitiveArray::new_nulls(len)),
            DataType::UInt64 => Array2::UInt64(PrimitiveArray::new_nulls(len)),
            DataType::UInt128 => Array2::UInt128(PrimitiveArray::new_nulls(len)),
            DataType::Float32 => Array2::Float32(PrimitiveArray::new_nulls(len)),
            DataType::Float64 => Array2::Float64(PrimitiveArray::new_nulls(len)),
            DataType::Decimal64(m) => Array2::Decimal64(DecimalArray::new(
                m.precision,
                m.scale,
                PrimitiveArray::new_nulls(len),
            )),
            DataType::Decimal128(m) => Array2::Decimal128(DecimalArray::new(
                m.precision,
                m.scale,
                PrimitiveArray::new_nulls(len),
            )),
            DataType::Date32 => Array2::Date32(PrimitiveArray::new_nulls(len)),
            DataType::Date64 => Array2::Date64(PrimitiveArray::new_nulls(len)),
            DataType::Timestamp(m) => {
                Array2::Timestamp(TimestampArray::new(m.unit, PrimitiveArray::new_nulls(len)))
            }
            DataType::Interval => Array2::Interval(PrimitiveArray::new_nulls(len)),
            DataType::Utf8 => Array2::Utf8(VarlenArray::new_nulls(len)),
            DataType::LargeUtf8 => Array2::LargeUtf8(VarlenArray::new_nulls(len)),
            DataType::Binary => Array2::Binary(VarlenArray::new_nulls(len)),
            DataType::LargeBinary => Array2::LargeBinary(VarlenArray::new_nulls(len)),
            DataType::Struct(m) => Array2::Struct(StructArray::new_nulls(&m.fields, len)),
            // TODO: Revisit this to ensure the list actually doesn't need any
            // type info.
            DataType::List(_m) => Array2::List(ListArray::new_nulls(len)),
        }
    }

    pub fn datatype(&self) -> DataType {
        match self {
            Array2::Null(_) => DataType::Null,
            Array2::Boolean(_) => DataType::Boolean,
            Array2::Float32(_) => DataType::Float32,
            Array2::Float64(_) => DataType::Float64,
            Array2::Int8(_) => DataType::Int8,
            Array2::Int16(_) => DataType::Int16,
            Array2::Int32(_) => DataType::Int32,
            Array2::Int64(_) => DataType::Int64,
            Array2::Int128(_) => DataType::Int128,
            Array2::UInt8(_) => DataType::UInt8,
            Array2::UInt16(_) => DataType::UInt16,
            Array2::UInt32(_) => DataType::UInt32,
            Array2::UInt64(_) => DataType::UInt64,
            Array2::UInt128(_) => DataType::UInt128,
            Self::Decimal64(arr) => {
                DataType::Decimal64(DecimalTypeMeta::new(arr.precision(), arr.scale()))
            }
            Self::Decimal128(arr) => {
                DataType::Decimal128(DecimalTypeMeta::new(arr.precision(), arr.scale()))
            }
            Array2::Date32(_) => DataType::Date32,
            Array2::Date64(_) => DataType::Date64,
            Array2::Timestamp(arr) => DataType::Timestamp(TimestampTypeMeta::new(arr.unit())),
            Array2::Interval(_) => DataType::Interval,
            Array2::Utf8(_) => DataType::Utf8,
            Array2::LargeUtf8(_) => DataType::LargeUtf8,
            Array2::Binary(_) => DataType::Binary,
            Array2::LargeBinary(_) => DataType::LargeBinary,
            Self::Struct(arr) => arr.datatype(),
            Self::List(arr) => arr.data_type(),
        }
    }

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
            Self::Int8(arr) => ScalarValue::Int8(*arr.value(idx)?),
            Self::Int16(arr) => ScalarValue::Int16(*arr.value(idx)?),
            Self::Int32(arr) => ScalarValue::Int32(*arr.value(idx)?),
            Self::Int64(arr) => ScalarValue::Int64(*arr.value(idx)?),
            Self::Int128(arr) => ScalarValue::Int128(*arr.value(idx)?),
            Self::UInt8(arr) => ScalarValue::UInt8(*arr.value(idx)?),
            Self::UInt16(arr) => ScalarValue::UInt16(*arr.value(idx)?),
            Self::UInt32(arr) => ScalarValue::UInt32(*arr.value(idx)?),
            Self::UInt64(arr) => ScalarValue::UInt64(*arr.value(idx)?),
            Self::UInt128(arr) => ScalarValue::UInt128(*arr.value(idx)?),
            Self::Decimal64(arr) => ScalarValue::Decimal64(Decimal64Scalar {
                precision: arr.precision(),
                scale: arr.scale(),
                value: *arr.get_primitive().value(idx)?,
            }),
            Self::Decimal128(arr) => ScalarValue::Decimal128(Decimal128Scalar {
                precision: arr.precision(),
                scale: arr.scale(),
                value: *arr.get_primitive().value(idx)?,
            }),
            Self::Date32(arr) => ScalarValue::Date32(*arr.value(idx)?),
            Self::Date64(arr) => ScalarValue::Date64(*arr.value(idx)?),
            Self::Timestamp(arr) => ScalarValue::Timestamp(TimestampScalar {
                unit: arr.unit(),
                value: *arr.get_primitive().value(idx)?,
            }),
            Self::Interval(arr) => ScalarValue::Interval(*arr.value(idx)?),
            Self::Utf8(arr) => ScalarValue::Utf8(arr.value(idx)?.into()),
            Self::LargeUtf8(arr) => ScalarValue::Utf8(arr.value(idx)?.into()),
            Self::Binary(arr) => ScalarValue::Binary(arr.value(idx)?.into()),
            Self::LargeBinary(arr) => unimplemented!(),
            Self::Struct(arr) => arr.scalar(idx)?,
            Self::List(arr) => arr.scalar(idx)?,
        })
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        match self {
            Self::Null(arr) => arr.is_valid(idx),
            Self::Boolean(arr) => arr.is_valid(idx),
            Self::Float32(arr) => arr.is_valid(idx),
            Self::Float64(arr) => arr.is_valid(idx),
            Self::Int8(arr) => arr.is_valid(idx),
            Self::Int16(arr) => arr.is_valid(idx),
            Self::Int32(arr) => arr.is_valid(idx),
            Self::Int64(arr) => arr.is_valid(idx),
            Self::Int128(arr) => arr.is_valid(idx),
            Self::UInt8(arr) => arr.is_valid(idx),
            Self::UInt16(arr) => arr.is_valid(idx),
            Self::UInt32(arr) => arr.is_valid(idx),
            Self::UInt64(arr) => arr.is_valid(idx),
            Self::UInt128(arr) => arr.is_valid(idx),
            Self::Decimal64(arr) => arr.get_primitive().is_valid(idx),
            Self::Decimal128(arr) => arr.get_primitive().is_valid(idx),
            Self::Date32(arr) => arr.is_valid(idx),
            Self::Date64(arr) => arr.is_valid(idx),
            Self::Timestamp(arr) => arr.get_primitive().is_valid(idx),
            Self::Interval(arr) => arr.is_valid(idx),
            Self::Utf8(arr) => arr.is_valid(idx),
            Self::LargeUtf8(arr) => arr.is_valid(idx),
            Self::Binary(arr) => arr.is_valid(idx),
            Self::LargeBinary(arr) => arr.is_valid(idx),
            Self::Struct(arr) => arr.is_valid(idx),
            Self::List(arr) => arr.is_valid(idx),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Null(arr) => arr.len(),
            Self::Boolean(arr) => arr.len(),
            Self::Float32(arr) => arr.len(),
            Self::Float64(arr) => arr.len(),
            Self::Int8(arr) => arr.len(),
            Self::Int16(arr) => arr.len(),
            Self::Int32(arr) => arr.len(),
            Self::Int64(arr) => arr.len(),
            Self::Int128(arr) => arr.len(),
            Self::UInt8(arr) => arr.len(),
            Self::UInt16(arr) => arr.len(),
            Self::UInt32(arr) => arr.len(),
            Self::UInt64(arr) => arr.len(),
            Self::UInt128(arr) => arr.len(),
            Self::Decimal64(arr) => arr.get_primitive().len(),
            Self::Decimal128(arr) => arr.get_primitive().len(),
            Self::Date32(arr) => arr.len(),
            Self::Date64(arr) => arr.len(),
            Self::Timestamp(arr) => arr.get_primitive().len(),
            Self::Interval(arr) => arr.len(),
            Self::Utf8(arr) => arr.len(),
            Self::LargeUtf8(arr) => arr.len(),
            Self::Binary(arr) => arr.len(),
            Self::LargeBinary(arr) => arr.len(),
            Self::Struct(arr) => arr.len(),
            Self::List(arr) => arr.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        match self {
            Self::Null(arr) => Some(arr.validity()),
            Self::Boolean(arr) => arr.validity(),
            Self::Float32(arr) => arr.validity(),
            Self::Float64(arr) => arr.validity(),
            Self::Int8(arr) => arr.validity(),
            Self::Int16(arr) => arr.validity(),
            Self::Int32(arr) => arr.validity(),
            Self::Int64(arr) => arr.validity(),
            Self::Int128(arr) => arr.validity(),
            Self::UInt8(arr) => arr.validity(),
            Self::UInt16(arr) => arr.validity(),
            Self::UInt32(arr) => arr.validity(),
            Self::UInt64(arr) => arr.validity(),
            Self::UInt128(arr) => arr.validity(),
            Self::Decimal64(arr) => arr.get_primitive().validity(),
            Self::Decimal128(arr) => arr.get_primitive().validity(),
            Self::Date32(arr) => arr.validity(),
            Self::Date64(arr) => arr.validity(),
            Self::Timestamp(arr) => arr.get_primitive().validity(),
            Self::Interval(arr) => arr.validity(),
            Self::Utf8(arr) => arr.validity(),
            Self::LargeUtf8(arr) => arr.validity(),
            Self::Binary(arr) => arr.validity(),
            Self::LargeBinary(arr) => arr.validity(),
            Self::Struct(_arr) => unimplemented!(),
            Self::List(arr) => arr.validity(),
        }
    }

    /// Try to convert an iterator of scalars of a given datatype into an array.
    ///
    /// Errors if any of the scalars are a different type than the provided
    /// datatype.
    pub fn try_from_scalars<'a>(
        datatype: DataType,
        scalars: impl Iterator<Item = ScalarValue<'a>>,
    ) -> Result<Array2> {
        /// Helper for iterating over scalars and producing a single type of
        /// array.
        ///
        /// `builder` is the array builder we're pushing values to.
        ///
        /// `default` is the default value to use if the we want to push a "null".
        ///
        /// `variant` is the enum variant for the Array and ScalarValue.
        macro_rules! iter_scalars_for_type {
            ($buffer:expr, $variant:ident, $array:ident, $null:expr) => {{
                let mut bitmap = Bitmap::default();
                let mut buffer = $buffer;
                for scalar in scalars {
                    match scalar {
                        ScalarValue::Null => {
                            bitmap.push(false);
                            buffer.push_value($null);
                        }
                        ScalarValue::$variant(v) => {
                            bitmap.push(true);
                            buffer.push_value(v);
                        }
                        other => {
                            return Err(RayexecError::new(format!(
                                "Unexpected scalar value: {other:?}, want: {}",
                                datatype,
                            )))
                        }
                    }
                }
                let arr = $array::new(buffer, Some(bitmap));
                Ok(Array2::$variant(arr))
            }};
        }

        let (cap, _) = scalars.size_hint();

        match datatype {
            DataType::Null => {
                let mut len = 0;
                for scalar in scalars {
                    match scalar {
                        ScalarValue::Null => len += 1,
                        other => {
                            return Err(RayexecError::new(format!(
                                "Unexpected non-null scalar: {other}"
                            )))
                        }
                    }
                }
                Ok(Array2::Null(NullArray::new(len)))
            }
            DataType::Boolean => iter_scalars_for_type!(
                BooleanValuesBuffer::with_capacity(cap),
                Boolean,
                BooleanArray,
                false
            ),
            DataType::Float32 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), Float32, PrimitiveArray, 0.0)
            }
            DataType::Float64 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), Float64, PrimitiveArray, 0.0)
            }
            DataType::Int8 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), Int8, PrimitiveArray, 0)
            }
            DataType::Int16 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), Int16, PrimitiveArray, 0)
            }
            DataType::Int32 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), Int32, PrimitiveArray, 0)
            }
            DataType::Int64 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), Int64, PrimitiveArray, 0)
            }
            DataType::Int128 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), Int128, PrimitiveArray, 0)
            }
            DataType::UInt8 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), UInt8, PrimitiveArray, 0)
            }
            DataType::UInt16 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), UInt16, PrimitiveArray, 0)
            }
            DataType::UInt32 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), UInt32, PrimitiveArray, 0)
            }
            DataType::UInt64 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), UInt64, PrimitiveArray, 0)
            }
            DataType::UInt128 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), UInt128, PrimitiveArray, 0)
            }
            DataType::Decimal64(meta) => {
                let mut bitmap = Bitmap::default();
                let mut buffer = Vec::with_capacity(cap);
                for scalar in scalars {
                    match scalar {
                        ScalarValue::Null => {
                            bitmap.push(false);
                            buffer.push_value(0);
                        }
                        ScalarValue::Decimal64(v) => {
                            // TODO: Assert prec/scale
                            bitmap.push(true);
                            buffer.push_value(v.value);
                        }
                        other => {
                            return Err(RayexecError::new(format!(
                                "Unexpected scalar value: {other:?}, want: {}",
                                datatype,
                            )))
                        }
                    }
                }
                let prim = PrimitiveArray::new(buffer, Some(bitmap));
                Ok(Array2::Decimal64(Decimal64Array::new(
                    meta.precision,
                    meta.scale,
                    prim,
                )))
            }
            DataType::Decimal128(meta) => {
                // TODO: Reduce duplication
                let mut bitmap = Bitmap::default();
                let mut buffer = Vec::with_capacity(cap);
                for scalar in scalars {
                    match scalar {
                        ScalarValue::Null => {
                            bitmap.push(false);
                            buffer.push_value(0);
                        }
                        ScalarValue::Decimal128(v) => {
                            // TODO: Assert prec/scale
                            bitmap.push(true);
                            buffer.push_value(v.value);
                        }
                        other => {
                            return Err(RayexecError::new(format!(
                                "Unexpected scalar value: {other:?}, want: {}",
                                datatype,
                            )))
                        }
                    }
                }
                let prim = PrimitiveArray::new(buffer, Some(bitmap));
                Ok(Array2::Decimal128(Decimal128Array::new(
                    meta.precision,
                    meta.scale,
                    prim,
                )))
            }
            DataType::Date32 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), Date32, PrimitiveArray, 0)
            }
            DataType::Date64 => {
                iter_scalars_for_type!(Vec::with_capacity(cap), Date64, PrimitiveArray, 0)
            }
            DataType::Timestamp(_) => {
                unimplemented!()
            }
            DataType::Interval => {
                iter_scalars_for_type!(
                    Vec::with_capacity(cap),
                    Interval,
                    PrimitiveArray,
                    Interval::default()
                )
            }
            DataType::Utf8 => {
                iter_scalars_for_type!(VarlenValuesBuffer::default(), Utf8, VarlenArray, "")
            }
            DataType::LargeUtf8 => {
                unimplemented!()
            }
            DataType::Binary => {
                iter_scalars_for_type!(
                    VarlenValuesBuffer::default(),
                    Binary,
                    VarlenArray,
                    &[] as &[u8]
                )
            }
            DataType::LargeBinary => {
                unimplemented!()
            }
            DataType::Struct(_) => Err(RayexecError::new(
                "Cannot build a struct array from struct scalars",
            )), // yet
            DataType::List(_) => Err(RayexecError::new(
                "Cannot build a list array from struct scalars",
            )), // yet
        }
    }
}

impl From<Decimal64Array> for Array2 {
    fn from(value: Decimal64Array) -> Self {
        Array2::Decimal64(value)
    }
}

impl From<Decimal128Array> for Array2 {
    fn from(value: Decimal128Array) -> Self {
        Array2::Decimal128(value)
    }
}

impl From<Utf8Array> for Array2 {
    fn from(value: Utf8Array) -> Self {
        Array2::Utf8(value)
    }
}

impl From<LargeUtf8Array> for Array2 {
    fn from(value: LargeUtf8Array) -> Self {
        Array2::LargeUtf8(value)
    }
}

impl From<ListArray> for Array2 {
    fn from(value: ListArray) -> Self {
        Array2::List(value)
    }
}

/// Utility trait for iterating over arrays.
pub trait ArrayAccessor<T: ?Sized> {
    type ValueIter: Iterator<Item = T>;

    /// Return the length of the array.
    fn len(&self) -> usize;

    /// If this array is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return an iterator over the values in the array.
    ///
    /// This should iterate over values even if the validity of the value is
    /// false.
    fn values_iter(&self) -> Self::ValueIter;

    /// Return a reference to the validity bitmap if this array has one.
    fn validity(&self) -> Option<&Bitmap>;
}

/// Storage for result values when executing operations on an array.
pub trait ValuesBuffer<T: ?Sized> {
    /// Put a computed value onto the buffer.
    fn push_value(&mut self, value: T);

    /// Push a dummy value for null.
    fn push_null(&mut self);
}

impl<T: Default> ValuesBuffer<T> for Vec<T> {
    fn push_value(&mut self, value: T) {
        self.push(value);
    }

    fn push_null(&mut self) {
        self.push(T::default())
    }
}

/// An implementation of an accessor that just returns unit values for
/// everything.
///
/// This is useful for when we care about iterating over arrays, but don't care
/// about the actual values. The primary use case for this is COUNT, as it
/// doesn't care about its input, other than if it's null which the validity
/// bitmap provides us.
pub struct UnitArrayAccessor<'a> {
    inner: &'a Array2,
}

impl<'a> UnitArrayAccessor<'a> {
    pub fn new(arr: &'a Array2) -> Self {
        UnitArrayAccessor { inner: arr }
    }
}

impl<'a> ArrayAccessor<()> for UnitArrayAccessor<'a> {
    type ValueIter = UnitIterator;

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn values_iter(&self) -> Self::ValueIter {
        UnitIterator {
            idx: 0,
            len: self.inner.len(),
        }
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.inner.validity()
    }
}

#[derive(Debug)]
pub struct UnitIterator {
    idx: usize,
    len: usize,
}

impl Iterator for UnitIterator {
    type Item = ();
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.len {
            None
        } else {
            Some(())
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.len - self.idx;
        (rem, Some(rem))
    }
}

/// Helper for determining if a value at a given index should be considered
/// valid.
///
/// If the bitmap is None, it's assumed that all values, regardless of the
/// index, are valid.
///
/// Panics if index is out of bounds.
fn is_valid(validity: Option<&Bitmap>, idx: usize) -> bool {
    validity.map(|bm| bm.value_unchecked(idx)).unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use crate::datatype::TimeUnit;

    use super::*;

    fn datatypes() -> Vec<DataType> {
        vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Int128,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::UInt128,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal64(DecimalTypeMeta::new(18, 9)),
            DataType::Decimal128(DecimalTypeMeta::new(38, 9)),
            DataType::Timestamp(TimestampTypeMeta::new(TimeUnit::Millisecond)),
            DataType::Date32,
            DataType::Date64,
            DataType::Interval,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            // TODO: Struct, list
        ]
    }

    #[test]
    fn new_nulls_empty() {
        for datatype in datatypes() {
            let arr = Array2::new_nulls(&datatype, 0);
            assert_eq!(0, arr.len(), "datatype: {datatype}");
        }
    }

    #[test]
    fn new_nulls_not_empty() {
        for datatype in datatypes() {
            let arr = Array2::new_nulls(&datatype, 3);
            assert_eq!(3, arr.len(), "datatype: {datatype}");
        }
    }
}
