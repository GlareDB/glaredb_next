use std::fmt;

use bytes::Bytes;

use crate::basic::Type;
use crate::column::page::PageReader;
use crate::column::reader::{ColumnReader, GenericColumnReader};
use crate::data_type::{ByteArray, FixedLenByteArray, Int96, ParquetValueType};
use crate::encodings::decoding::get_decoder::GetDecoder;
use crate::encodings::decoding::PlainDecoderState;
use crate::errors::{ParquetError, Result};

pub trait DecodeBuffer: Sized + Send + Default + fmt::Debug {
    /// Value type stored in the buffer.
    type Value;

    fn with_len(len: usize) -> Self;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn swap(&mut self, a: usize, b: usize);

    fn grow(&mut self, additional: usize);

    fn put_value(&mut self, idx: usize, val: &Self::Value);

    fn get_value(&self, idx: usize) -> &Self::Value;
}

impl<T> DecodeBuffer for Vec<T>
where
    T: Send + Clone + Default + fmt::Debug,
{
    type Value = T;

    fn with_len(len: usize) -> Self {
        vec![T::default(); len]
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn swap(&mut self, a: usize, b: usize) {
        self.as_mut_slice().swap(a, b)
    }

    fn grow(&mut self, additional: usize) {
        self.resize(additional + self.len(), T::default());
    }

    fn put_value(&mut self, idx: usize, val: &Self::Value) {
        self[idx] = val.clone();
    }

    fn get_value(&self, idx: usize) -> &Self::Value {
        &self[idx]
    }
}

pub trait ValueDecoder: Sized + Send + fmt::Debug + 'static {
    type ValueType: ParquetValueType;
    type DecodeBuffer: DecodeBuffer;

    // TODO: Remove
    fn get_physical_type() -> Type {
        Self::ValueType::PHYSICAL_TYPE
    }

    /// Establish the data that will be decoded in a buffer
    fn set_data2(decoder: &mut PlainDecoderState, data: Bytes, num_values: usize);

    /// Decode the value from a given buffer for a higher level decoder
    fn decode2(
        offset: usize,
        buffer: &mut Self::DecodeBuffer,
        decoder: &mut PlainDecoderState,
    ) -> Result<usize>;

    fn skip2(decoder: &mut PlainDecoderState, num_values: usize) -> Result<usize>;
}

/// Implements the value decoder for base parquet types.
///
/// The decode buffer will be `Vec<T>` where `T` is the type.
///
/// This provides good decoding defaults for primitives. Varlen types are less
/// efficient here, and so custom `ValueDecoder` implementations should be used
/// for maximum perf if needed.
///
/// Note that this implements directly on the type instead of on `T:
/// ParquetValue` as I (Sean) could not figure out how to slice into the buffer
/// even when it was set to `Vec<T>`. It was a weird type error.
macro_rules! impl_value_decoder {
    ($ty:ty) => {
        impl ValueDecoder for $ty {
            type ValueType = $ty;
            type DecodeBuffer = Vec<$ty>;

            fn set_data2(decoder: &mut PlainDecoderState, data: Bytes, num_values: usize) {
                <$ty as ParquetValueType>::set_data(decoder, data, num_values)
            }

            fn decode2(
                offset: usize,
                buffer: &mut Self::DecodeBuffer,
                decoder: &mut PlainDecoderState,
            ) -> Result<usize> {
                let buf = &mut buffer[offset..];
                <$ty as ParquetValueType>::decode(buf, decoder)
            }

            fn skip2(decoder: &mut PlainDecoderState, num_values: usize) -> Result<usize> {
                <$ty as ParquetValueType>::skip(decoder, num_values)
            }
        }
    };
}

impl_value_decoder!(bool);
impl_value_decoder!(i32);
impl_value_decoder!(i64);
impl_value_decoder!(Int96);
impl_value_decoder!(f32);
impl_value_decoder!(f64);
impl_value_decoder!(ByteArray);
impl_value_decoder!(FixedLenByteArray);

// TODO: REMOVE (at some point). Currently just a workaround to get the
// `get_column_reader` and `get_typed_column_reader` functions working. Those
// are just for tests.
pub trait TypedColumnReader: ValueDecoder + GetDecoder {
    fn get_typed_reader<P: PageReader>(
        column_reader: ColumnReader<P>,
    ) -> Option<GenericColumnReader<Self, P>>;
}

macro_rules! impl_typed_column_reader {
    ($ty:ty, $variant:ident) => {
        impl TypedColumnReader for $ty {
            fn get_typed_reader<P: PageReader>(
                column_reader: ColumnReader<P>,
            ) -> Option<GenericColumnReader<Self, P>> {
                match column_reader {
                    ColumnReader::$variant(reader) => Some(reader),
                    _ => None,
                }
            }
        }
    };
}

impl_typed_column_reader!(bool, BoolColumnReader);
impl_typed_column_reader!(i32, Int32ColumnReader);
impl_typed_column_reader!(i64, Int64ColumnReader);
impl_typed_column_reader!(Int96, Int96ColumnReader);
impl_typed_column_reader!(f32, FloatColumnReader);
impl_typed_column_reader!(f64, DoubleColumnReader);
impl_typed_column_reader!(ByteArray, ByteArrayColumnReader);
impl_typed_column_reader!(FixedLenByteArray, FixedLenByteArrayColumnReader);
