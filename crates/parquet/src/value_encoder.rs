use core::fmt;

use crate::basic::Type;
use crate::column::page::PageWriter;
use crate::column::writer::{ColumnWriter, GenericColumnWriter};
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

// TODO: Try to remove this as well.
pub trait TypedColumnWriter: ValueEncoder {
    fn get_typed_writer<P: PageWriter>(
        column_writer: ColumnWriter<P>,
    ) -> Option<GenericColumnWriter<Self, P>>;

    fn get_typed_writer_mut<P: PageWriter>(
        column_writer: &mut ColumnWriter<P>,
    ) -> Option<&mut GenericColumnWriter<Self, P>>;
}

macro_rules! impl_typed_column_writer {
    ($ty:ty, $variant:ident) => {
        impl TypedColumnWriter for $ty {
            fn get_typed_writer<P: PageWriter>(
                column_writer: ColumnWriter<P>,
            ) -> Option<GenericColumnWriter<Self, P>> {
                match column_writer {
                    ColumnWriter::$variant(writer) => Some(writer),
                    _ => None,
                }
            }

            fn get_typed_writer_mut<P: PageWriter>(
                column_writer: &mut ColumnWriter<P>,
            ) -> Option<&mut GenericColumnWriter<Self, P>> {
                match column_writer {
                    ColumnWriter::$variant(writer) => Some(writer),
                    _ => None,
                }
            }
        }
    };
}

impl_typed_column_writer!(bool, BoolColumnWriter);
impl_typed_column_writer!(i32, Int32ColumnWriter);
impl_typed_column_writer!(i64, Int64ColumnWriter);
impl_typed_column_writer!(Int96, Int96ColumnWriter);
impl_typed_column_writer!(f32, FloatColumnWriter);
impl_typed_column_writer!(f64, DoubleColumnWriter);
impl_typed_column_writer!(ByteArray, ByteArrayColumnWriter);
impl_typed_column_writer!(FixedLenByteArray, FixedLenByteArrayColumnWriter);
