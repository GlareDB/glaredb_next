use std::borrow::Cow;

use rayexec_bullet::array::Array;
use rayexec_bullet::executor::physical_type::{AsBytes, PhysicalStorage};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::storage::AddressableStorage;
use rayexec_error::{not_implemented, Result};

use crate::encoding::Encoding;
use crate::page::{DataPage, DataPageHeader, DataPageHeaderV2};

pub fn plain_encode_binary_array_page<'a, S>(array: &'a Array) -> Result<DataPage<'static>>
where
    S: PhysicalStorage<'a>,
    <S::Storage as AddressableStorage>::T: AsBytes,
{
    if array.validity().is_some() {
        not_implemented!("Encoding with validity");
    }

    // TODO: Stats and stuff.

    let mut buf = Vec::new();
    plain_encode_binary_array::<S>(array, &mut buf)?;

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

pub fn plain_encode_binary_array<'a, S>(array: &'a Array, buf: &mut Vec<u8>) -> Result<()>
where
    S: PhysicalStorage<'a>,
    <S::Storage as AddressableStorage>::T: AsBytes,
{
    if array.validity().is_some() {
        not_implemented!("Encoding with validity");
    }

    // TODO: Tbd if this is worth it.
    let mut num_bytes = 0;
    UnaryExecutor::for_each::<S, _>(array, |_, val| {
        if let Some(val) = val {
            num_bytes += val.as_bytes().len();
        }
    })?;

    buf.reserve(num_bytes + (std::mem::size_of::<u32>() * array.logical_len()));

    UnaryExecutor::for_each::<S, _>(array, |_idx, val| {
        if let Some(val) = val {
            let val = val.as_bytes();
            let len = val.len();

            buf.extend_from_slice(&u32::to_le_bytes(len as u32));
            buf.extend_from_slice(val);
        }
        // Nulls don't get encoded.
    })
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::executor::physical_type::PhysicalUtf8;

    use super::*;

    #[test]
    fn plain_encode_simple_str() {
        let a = Array::from_iter(["hello", "world"]);
        let page = plain_encode_binary_array_page::<PhysicalUtf8>(&a).unwrap();

        assert_eq!(2, page.header.get_v2().unwrap().num_values);
    }
}
