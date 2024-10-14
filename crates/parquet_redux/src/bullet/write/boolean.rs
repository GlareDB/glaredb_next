use std::borrow::Cow;

use rayexec_bullet::array::Array;
use rayexec_bullet::executor::physical_type::PhysicalBool;
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_error::{not_implemented, Result};

use crate::encoding::bitpack::boolean::bitpack_bools;
use crate::encoding::Encoding;
use crate::page::{DataPage, DataPageHeader, DataPageHeaderV2};

pub fn plain_encode_boolean_array_page(array: &Array) -> Result<DataPage<'static>> {
    if array.validity().is_some() {
        not_implemented!("Encoding with validity");
    }

    // TODO: Stats and stuff.

    // TODO: Could reuse.
    let mut buf = Vec::new();
    plain_encode_boolean_array(array, &mut buf)?;

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

/// Plain encodes (bitpacks) a boolean array.
pub fn plain_encode_boolean_array(array: &Array, buf: &mut Vec<u8>) -> Result<()> {
    if array.validity().is_some() {
        not_implemented!("Encoding with validity");
    }

    // TODO: Try not to do this.
    let mut bools = Vec::with_capacity(array.logical_len());
    UnaryExecutor::for_each::<PhysicalBool, _>(array, |_, b| {
        if let Some(b) = b {
            bools.push(b);
        }
    })?;

    bitpack_bools(buf, bools.iter().copied())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_encode_simple_bools() {
        let a = Array::from_iter([true, false, true, true]);
        let page = plain_encode_boolean_array_page(&a).unwrap();
        assert_eq!(4, page.header.get_v2().unwrap().num_values);
    }
}
