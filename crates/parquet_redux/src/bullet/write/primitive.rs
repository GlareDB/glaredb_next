use std::borrow::Cow;

use num::cast::AsPrimitive;
use rayexec_bullet::array::Array;
use rayexec_bullet::executor::physical_type::PhysicalStorage;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::storage::AddressableStorage;
use rayexec_error::{not_implemented, Result};

use crate::encoding::Encoding;
use crate::page::{DataPage, DataPageHeader, DataPageHeaderV2};
use crate::types::ParquetFixedWidthType;

/// Plain encodes a primitive array and produces a data page.
pub fn plain_encode_primitive_array_page<'a, S, P>(array: &'a Array) -> Result<DataPage<'static>>
where
    S: PhysicalStorage<'a>,
    P: ParquetFixedWidthType,
    <S::Storage as AddressableStorage>::T: AsPrimitive<P>,
{
    if array.validity().is_some() {
        not_implemented!("Encoding with validity");
    }

    // TODO: Stats and stuff.

    // TODO: Could reuse.
    let mut buf = Vec::new();
    plain_encode_primitive_array::<S, P>(array, &mut buf)?;

    Ok(DataPage {
        header: DataPageHeader::V2(DataPageHeaderV2 {
            num_values: array.logical_len() as i32,
            num_nulls: 0,
            num_rows: array.logical_len() as i32,
            encoding: Encoding::Plain,
            definition_levels_byte_length: 0,
            repetition_levels_byte_length: 0,
            is_compressed: Some(false),
            statistics: None,
        }),
        buffer: Cow::Owned(buf),
    })
}

/// Plain encodes a primitive array.
pub fn plain_encode_primitive_array<'a, S, P>(array: &'a Array, buf: &mut Vec<u8>) -> Result<()>
where
    S: PhysicalStorage<'a>,
    P: ParquetFixedWidthType,
    <S::Storage as AddressableStorage>::T: AsPrimitive<P>,
{
    if array.validity().is_some() {
        not_implemented!("Encoding with validity");
    }

    buf.reserve(std::mem::size_of::<P::Bytes>() * array.logical_len());

    UnaryExecutor::for_each::<S, _>(array, |_idx, val| {
        if let Some(val) = val {
            // Note that for conversions like i32 -> u32 we can end up with
            // under/overflow. This is expected and follows upstream parquet.
            let val: P = val.as_();
            buf.extend_from_slice(val.to_le_bytes().as_ref());
        }
        // Nulls don't get encoded.
    })
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::executor::physical_type::{PhysicalI16, PhysicalI32};

    use super::*;

    #[test]
    fn plain_encode_simple_i32() {
        let a = Array::from_iter([2, 3, 4]);
        let page = plain_encode_primitive_array_page::<PhysicalI32, i32>(&a).unwrap();

        assert_eq!(3, page.header.get_v2().unwrap().num_values);
    }

    #[test]
    fn plain_encode_simple_i16_as_i32() {
        let a = Array::from_iter([2_i16, 3, 4]);
        let page = plain_encode_primitive_array_page::<PhysicalI16, i32>(&a).unwrap();

        assert_eq!(3, page.header.get_v2().unwrap().num_values);
    }
}
