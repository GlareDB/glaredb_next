//! Decoder for delta encoding.
//!
//! See <https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5>

use num::{FromPrimitive, PrimInt};
use rayexec_error::{RayexecError, Result};

use crate::encoding::delta_binary::uleb128::decode_uleb128;
use crate::encoding::delta_binary::zigzag::decode_zigzag_uleb128;

#[derive(Debug)]
pub struct Decoder<'a, T> {
    buf: &'a [u8],

    /// Number of values in each block.
    block_size: usize,
    /// Number of miniblocks per block.
    miniblocks_per_block: usize,

    /// Number of values left to read.
    values_remaining: usize,

    /// First value in the page.
    first_value: T,
    /// If we've already read the first value.
    first_read: bool,

    /// Last value for computing deltas for.
    last_value: T,

    /// State for the current block we're reading.
    block_state: Option<BlockState<'a, T>>,
}

#[derive(Debug)]
struct BlockState<'a, T> {
    min_delta: T,
    bit_widths: &'a [u8],
    miniblock_idx: usize,
    miniblock_remaining_values: usize,
}

impl<'a, T> Decoder<'a, T>
where
    T: PrimInt + FromPrimitive + Default,
{
    /// Try to create a new decoder, initializing the decoder state by reading
    /// the header from `buf`.
    ///
    /// `buf` should be the buffer of the entire page.
    pub fn try_new(mut buf: &'a [u8]) -> Result<Self> {
        // Read header (all uleb128)
        // <block size in values> <number of miniblocks in a block> <total value count> <first value>

        let (block_size, num_read) = decode_uleb128(buf)?;
        buf = &buf[num_read..];

        let (miniblocks_per_block, num_read) = decode_uleb128(buf)?;
        buf = &buf[num_read..];

        let (total_vals, num_read) = decode_uleb128(buf)?;
        buf = &buf[num_read..];

        let (first_value, num_read) = decode_uleb128(buf)?;
        buf = &buf[num_read..];

        if block_size % 128 != 0 {
            return Err(RayexecError::new(
                "Expected block size to be multiple of 128",
            ));
        }

        if miniblocks_per_block % 32 != 0 {
            return Err(RayexecError::new(
                "Expected miniblocks per block to be multiple of 32",
            ));
        }

        if block_size % miniblocks_per_block != 0 {
            return Err(RayexecError::new(
                "Expected block size to be a multiple of miniblocks per block",
            ));
        }

        let first_value = T::from_u64(first_value).ok_or_else(|| {
            RayexecError::new(format!(
                "Unable to cast first value {first_value} to correct type"
            ))
        })?;

        Ok(Decoder {
            buf,
            block_size: block_size as usize,
            miniblocks_per_block: miniblocks_per_block as usize,
            values_remaining: total_vals as usize,
            first_value,
            first_read: false,
            block_state: None,
            last_value: T::default(),
        })
    }

    fn read_next_block(&mut self) -> Result<()> {
        let (min_delta, num_read) = decode_uleb128(&self.buf)?;
        self.buf = &self.buf[num_read..];

        let min_delta = T::from_u64(min_delta).ok_or_else(|| {
            RayexecError::new(format!(
                "Unable to cast min delta {min_delta} to correct type"
            ))
        })?;

        let bit_widths = &self.buf[..self.miniblocks_per_block];
        self.buf = &self.buf[self.miniblocks_per_block..];

        let miniblock_remaining_values = self.block_size / self.miniblocks_per_block;

        self.block_state = Some(BlockState {
            min_delta,
            bit_widths,
            miniblock_idx: 0,
            miniblock_remaining_values: miniblock_remaining_values as usize,
        });

        Ok(())
    }

    pub fn read_values(&mut self, out: &mut Vec<T>) -> Result<usize> {
        if self.values_remaining == 0 {
            return Ok(0);
        }

        if self.block_state.is_none() {
            self.read_next_block()?;
        }

        if !self.first_read {
            out.push(self.first_value);
            self.first_read = true;
            self.last_value = self.first_value;
        }

        let block_state = self.block_state.as_mut().unwrap();
        let remaining_read = block_state.miniblock_remaining_values;

        let bit_width = block_state.bit_widths[block_state.miniblock_idx];
        for _ in 0..remaining_read {
            // let v =
        }

        unimplemented!()
    }
}
