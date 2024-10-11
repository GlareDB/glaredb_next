use core::fmt;

use crate::basic::Type;
use crate::data_type::{ByteArray, FixedLenByteArray, Int96, ParquetValueType};
use crate::errors::Result;
use crate::util::bit_util::BitWriter;

pub trait ValueEncoder: Sized + Send + fmt::Debug + 'static {
    type ValueType: ParquetValueType;

    // TODO: Remove
    fn get_physical_type() -> Type {
        Self::ValueType::PHYSICAL_TYPE
    }

    fn encode<W>(
        values: &[Self::ValueType],
        writer: &mut W,
        bit_writer: &mut BitWriter,
    ) -> Result<()>
    where
        W: std::io::Write;

    fn dict_encoding_size(&self) -> (usize, usize);
}

macro_rules! impl_value_encoder {
    ($ty:ty) => {
        impl ValueEncoder for $ty {
            type ValueType = $ty;

            fn encode<W>(
                values: &[Self::ValueType],
                writer: &mut W,
                bit_writer: &mut BitWriter,
            ) -> Result<()>
            where
                W: std::io::Write,
            {
                <$ty as ParquetValueType>::encode(values, writer, bit_writer)
            }

            fn dict_encoding_size(&self) -> (usize, usize) {
                <$ty as ParquetValueType>::dict_encoding_size(self)
            }
        }
    };
}

impl_value_encoder!(bool);
impl_value_encoder!(i32);
impl_value_encoder!(i64);
impl_value_encoder!(Int96);
impl_value_encoder!(f32);
impl_value_encoder!(f64);
impl_value_encoder!(ByteArray);
impl_value_encoder!(FixedLenByteArray);
