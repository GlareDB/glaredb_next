use std::fmt;

use bytes::Bytes;

use crate::basic::Type;
use crate::column::page::PageReader;
use crate::column::reader::{ColumnReader, GenericColumnReader};
use crate::data_type::{ByteArray, FixedLenByteArray, Int96, ParquetValueType};
use crate::encodings::decoding::get_decoder::GetDecoder;
use crate::encodings::decoding::PlainDecoderState;
use crate::errors::{ParquetError, Result};
use crate::util::bit_util::read_num_bytes;

pub trait DecodeBuffer: Sized + Send + fmt::Debug {
    /// Value that's passed to and from the buffer.
    ///
    /// The internal representation of the items may differ.
    type Value: ?Sized;

    /// Create a new buffer with `len` items.
    ///
    /// This should initialize the buffer with some default values, but
    /// meaningless values.
    fn with_len(len: usize) -> Self;

    /// Return the length of the buffer.
    fn len(&self) -> usize;

    /// Return if the buffer is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Swap values between two positions in the buffer.
    ///
    /// `a` and `b` are both guaranteed to be in bound according to `len`.
    fn swap(&mut self, a: usize, b: usize);

    /// Grow the buffer to hold `additional` items.
    ///
    /// This should increase the length of the buffer and initialize the new
    /// values to some default.
    fn grow(&mut self, additional: usize);

    /// Put a value in the buffer.
    ///
    /// `idx` guaranteed to be in bounds according to length.
    fn put_value(&mut self, idx: usize, val: &Self::Value);

    /// Get a value from the buffer.
    ///
    /// `idx` guaranteed to be in bounds according to length.
    fn get_value(&self, idx: usize) -> &Self::Value;
}

/// Default implementation on Vec. Suitable for most primitives.
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

/// Default decode buffer for byte arrays.
///
/// Internally this is just a vec of `Bytes`. This is needed over just the
/// `Vec<T>` above because we want the delta decoders to be able to pass in
/// `&[u8]` when storing things in the buffer.
///
/// The eventual goal is to remove `Bytes` from most places as it makes buffer
/// reuse more difficult.
///
/// This can be the basis for more efficient implementations for byte array
/// buffers.
#[derive(Debug, PartialEq, Eq)]
pub struct ByteArrayDecodeBuffer {
    pub(crate) values: Vec<Bytes>,
}

impl DecodeBuffer for ByteArrayDecodeBuffer {
    type Value = [u8];

    fn with_len(len: usize) -> Self {
        ByteArrayDecodeBuffer {
            values: vec![Bytes::new(); len],
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn swap(&mut self, a: usize, b: usize) {
        self.values.swap(a, b)
    }

    fn grow(&mut self, additional: usize) {
        self.values
            .resize(additional + self.values.len(), Bytes::new());
    }

    fn put_value(&mut self, idx: usize, val: &Self::Value) {
        self.values[idx] = Bytes::copy_from_slice(val)
    }

    fn get_value(&self, idx: usize) -> &Self::Value {
        &self.values[idx]
    }
}

impl From<Vec<Bytes>> for ByteArrayDecodeBuffer {
    fn from(values: Vec<Bytes>) -> Self {
        ByteArrayDecodeBuffer { values }
    }
}

impl AsRef<[Bytes]> for ByteArrayDecodeBuffer {
    fn as_ref(&self) -> &[Bytes] {
        self.values.as_slice()
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
impl_value_decoder!(FixedLenByteArray);

impl ValueDecoder for ByteArray {
    type ValueType = ByteArray;
    type DecodeBuffer = ByteArrayDecodeBuffer;

    fn set_data2(decoder: &mut PlainDecoderState, data: Bytes, num_values: usize) {
        decoder.data.replace(data);
        decoder.start = 0;
        decoder.num_values = num_values;
    }

    fn decode2(
        offset: usize,
        buffer: &mut Self::DecodeBuffer,
        decoder: &mut PlainDecoderState,
    ) -> Result<usize> {
        let buf = &mut buffer.values[offset..];

        let data = decoder
            .data
            .as_mut()
            .expect("set_data should have been called");
        let num_values = std::cmp::min(buf.len(), decoder.num_values);
        for val in buf.iter_mut().take(num_values) {
            let len: usize =
                read_num_bytes::<u32>(4, data.slice(decoder.start..).as_ref()) as usize;
            decoder.start += std::mem::size_of::<u32>();

            if data.len() < decoder.start + len {
                return Err(eof_err!("Not enough bytes to decode"));
            }

            *val = data.slice(decoder.start..decoder.start + len);
            decoder.start += len;
        }
        decoder.num_values -= num_values;

        Ok(num_values)
    }

    fn skip2(decoder: &mut PlainDecoderState, num_values: usize) -> Result<usize> {
        let data = decoder
            .data
            .as_mut()
            .expect("set_data should have been called");
        let num_values = num_values.min(decoder.num_values);

        for _ in 0..num_values {
            let len: usize =
                read_num_bytes::<u32>(4, data.slice(decoder.start..).as_ref()) as usize;
            decoder.start += std::mem::size_of::<u32>() + len;
        }
        decoder.num_values -= num_values;

        Ok(num_values)
    }
}

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
