use rayexec_bullet::array::Array;
use rayexec_bullet::executor::physical_type::{
    PhysicalBinary,
    PhysicalBool,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalInterval,
    PhysicalStorage,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use rayexec_bullet::executor::scalar::UnaryExecutor;
use rayexec_bullet::selection::SelectionVector;
use rayexec_bullet::storage::AddressableStorage;
use rayexec_error::Result;

use super::chunk::GroupChunk;
use super::hash_table::GroupAddress;

pub fn group_values_eq(
    inputs: &[Array],
    input_hashes: &[u64],
    input_sel: &SelectionVector,
    chunks: &[GroupChunk],
    addresses: &[GroupAddress],
    not_eq_sel: &mut SelectionVector,
) -> Result<()> {
    for row_idx in input_sel.iter_locations() {
        let addr = &addresses[row_idx];
        let chunk = &chunks[addr.chunk_idx as usize];

        if !group_rows_eq(row_idx, inputs, input_hashes, chunk, addr)? {
            not_eq_sel.push_location(row_idx);
        }
    }

    Ok(())
}

fn group_rows_eq(
    row_idx: usize,
    inputs: &[Array],
    input_hashes: &[u64],
    chunk: &GroupChunk,
    addr: &GroupAddress,
) -> Result<bool> {
    if input_hashes[row_idx] != chunk.hashes[addr.row_idx as usize] {
        return Ok(false);
    }

    for col_idx in 0..inputs.len() {
        let left = &inputs[col_idx];
        let right = &chunk.arrays[col_idx];

        let eq = match left.physical_type() {
            PhysicalType::UntypedNull => group_rows_eq_inner::<PhysicalUntypedNull>(
                left,
                row_idx,
                right,
                addr.row_idx as usize,
            )?,
            PhysicalType::Boolean => {
                group_rows_eq_inner::<PhysicalBool>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::Int8 => {
                group_rows_eq_inner::<PhysicalI8>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::Int16 => {
                group_rows_eq_inner::<PhysicalI16>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::Int32 => {
                group_rows_eq_inner::<PhysicalI32>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::Int64 => {
                group_rows_eq_inner::<PhysicalI64>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::Int128 => {
                group_rows_eq_inner::<PhysicalI128>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::UInt8 => {
                group_rows_eq_inner::<PhysicalU8>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::UInt16 => {
                group_rows_eq_inner::<PhysicalU16>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::UInt32 => {
                group_rows_eq_inner::<PhysicalU32>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::UInt64 => {
                group_rows_eq_inner::<PhysicalU64>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::UInt128 => {
                group_rows_eq_inner::<PhysicalU128>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::Float32 => {
                group_rows_eq_inner::<PhysicalF32>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::Float64 => {
                group_rows_eq_inner::<PhysicalF64>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::Interval => group_rows_eq_inner::<PhysicalInterval>(
                left,
                row_idx,
                right,
                addr.row_idx as usize,
            )?,
            PhysicalType::Binary => {
                group_rows_eq_inner::<PhysicalBinary>(left, row_idx, right, addr.row_idx as usize)?
            }
            PhysicalType::Utf8 => {
                group_rows_eq_inner::<PhysicalUtf8>(left, row_idx, right, addr.row_idx as usize)?
            }
        };

        if !eq {
            return Ok(false);
        }
    }

    Ok(true)
}

fn group_rows_eq_inner<'a, S>(
    left: &'a Array,
    left_idx: usize,
    right: &'a Array,
    right_idx: usize,
) -> Result<bool>
where
    S: PhysicalStorage<'a>,
    <S::Storage as AddressableStorage>::T: PartialEq,
{
    let left = UnaryExecutor::value_at_unchecked::<S>(left, left_idx)?;
    let right = UnaryExecutor::value_at_unchecked::<S>(right, right_idx)?;

    Ok(left == right)
}
