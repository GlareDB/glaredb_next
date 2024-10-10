use super::byte_stream_split_decoder::ByteStreamSplitDecoder;
use super::{
    ByteArray,
    Decoder,
    DeltaBitPackDecoder,
    DeltaByteArrayDecoder,
    DeltaLengthByteArrayDecoder,
    Encoding,
    FixedLenByteArray,
    Int96,
    PlainDecoder,
    RleValueDecoder,
    ValueDecoder,
};
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;

/// Gets a decoder for the column descriptor `descr` and encoding type `encoding`.
///
/// NOTE: the primitive type in `descr` MUST match the data type `T::ValueType`,
/// otherwise disastrous consequence could occur.
pub fn get_decoder<T: ValueDecoder + GetDecoder>(
    descr: ColumnDescPtr,
    encoding: Encoding,
) -> Result<Box<dyn Decoder<T>>> {
    T::get_decoder(descr, encoding)
}

/// A trait that allows getting a [`Decoder`] implementation for a
/// [`DataType`] with the corresponding [`ParquetValueType`].
///
/// This is necessary to support [`Decoder`] implementations that may not be
/// applicable for all [`DataType`] and by extension all
/// [`ParquetValueType`]
pub trait GetDecoder: ValueDecoder {
    fn get_decoder(descr: ColumnDescPtr, encoding: Encoding) -> Result<Box<dyn Decoder<Self>>> {
        get_decoder_default::<Self>(descr, encoding)
    }
}

pub fn get_decoder_default<T: ValueDecoder>(
    descr: ColumnDescPtr,
    encoding: Encoding,
) -> Result<Box<dyn Decoder<T>>> {
    match encoding {
        Encoding::PLAIN => Ok(Box::new(PlainDecoder::new(descr.type_length()))),
        Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => Err(general_err!(
            "Cannot initialize this encoding through this function"
        )),
        Encoding::RLE
        | Encoding::DELTA_BINARY_PACKED
        | Encoding::DELTA_BYTE_ARRAY
        | Encoding::DELTA_LENGTH_BYTE_ARRAY => Err(general_err!(
            "Encoding {} is not supported for type",
            encoding
        )),
        e => Err(nyi_err!("Encoding {} is not supported", e)),
    }
}

impl GetDecoder for bool {
    fn get_decoder(descr: ColumnDescPtr, encoding: Encoding) -> Result<Box<dyn Decoder<Self>>> {
        match encoding {
            Encoding::RLE => Ok(Box::new(RleValueDecoder::new())),
            _ => get_decoder_default(descr, encoding),
        }
    }
}

impl GetDecoder for i32 {
    fn get_decoder(descr: ColumnDescPtr, encoding: Encoding) -> Result<Box<dyn Decoder<Self>>> {
        match encoding {
            Encoding::DELTA_BINARY_PACKED => Ok(Box::new(DeltaBitPackDecoder::new())),
            _ => get_decoder_default(descr, encoding),
        }
    }
}

impl GetDecoder for i64 {
    fn get_decoder(descr: ColumnDescPtr, encoding: Encoding) -> Result<Box<dyn Decoder<Self>>> {
        match encoding {
            Encoding::DELTA_BINARY_PACKED => Ok(Box::new(DeltaBitPackDecoder::new())),
            _ => get_decoder_default(descr, encoding),
        }
    }
}

impl GetDecoder for f32 {
    fn get_decoder(descr: ColumnDescPtr, encoding: Encoding) -> Result<Box<dyn Decoder<Self>>> {
        match encoding {
            Encoding::BYTE_STREAM_SPLIT => Ok(Box::new(ByteStreamSplitDecoder::new())),
            _ => get_decoder_default(descr, encoding),
        }
    }
}
impl GetDecoder for f64 {
    fn get_decoder(descr: ColumnDescPtr, encoding: Encoding) -> Result<Box<dyn Decoder<Self>>> {
        match encoding {
            Encoding::BYTE_STREAM_SPLIT => Ok(Box::new(ByteStreamSplitDecoder::new())),
            _ => get_decoder_default(descr, encoding),
        }
    }
}

impl GetDecoder for ByteArray {
    fn get_decoder(descr: ColumnDescPtr, encoding: Encoding) -> Result<Box<dyn Decoder<Self>>> {
        match encoding {
            Encoding::DELTA_BYTE_ARRAY => Ok(Box::new(DeltaByteArrayDecoder::new())),
            Encoding::DELTA_LENGTH_BYTE_ARRAY => Ok(Box::new(DeltaLengthByteArrayDecoder::new())),
            _ => get_decoder_default(descr, encoding),
        }
    }
}

impl GetDecoder for FixedLenByteArray {
    fn get_decoder(descr: ColumnDescPtr, encoding: Encoding) -> Result<Box<dyn Decoder<Self>>> {
        match encoding {
            Encoding::DELTA_BYTE_ARRAY => Ok(Box::new(DeltaByteArrayDecoder::new())),
            _ => get_decoder_default(descr, encoding),
        }
    }
}

impl GetDecoder for Int96 {}
