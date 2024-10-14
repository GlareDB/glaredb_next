//! Decoder for delta encoding.
//!
//! See <https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5>

use std::fmt::{Debug, Display};

use num::traits::WrappingAdd;
use num::{FromPrimitive, PrimInt};
use rayexec_error::{RayexecError, Result};

use crate::encoding::bitpack::BitPackable;
use crate::encoding::delta_binary::uleb128::decode_uleb128;
use crate::encoding::delta_binary::zigzag::decode_zigzag_uleb128;

#[derive(Debug)]
pub struct Decoder<'a, T> {
    buf: &'a [u8],
    /// Number of values in each block.
    block_size: usize,
    /// Number of miniblocks per block.
    miniblocks_per_block: usize,
    /// Total number of values.
    total_values: usize,
    /// First value in the page.
    first_value: T,
}

impl<'a, T> Decoder<'a, T>
where
    T: PrimInt + FromPrimitive + Default + BitPackable + WrappingAdd + Debug + Display,
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

        let (first_value, num_read) = decode_zigzag_uleb128(buf)?;
        buf = &buf[num_read..];

        if block_size % 128 != 0 {
            return Err(RayexecError::new(
                "Expected block size to be multiple of 128",
            ));
        }

        if (block_size / miniblocks_per_block) % 32 != 0 {
            return Err(RayexecError::new(
                "Expected miniblocks per block to be multiple of 32",
            ));
        }

        if block_size % miniblocks_per_block != 0 {
            return Err(RayexecError::new(
                "Expected block size to be a multiple of miniblocks per block",
            ));
        }

        let first_value = T::from_i64(first_value).ok_or_else(|| {
            RayexecError::new(format!(
                "Unable to cast first value {first_value} to correct type"
            ))
        })?;

        Ok(Decoder {
            buf,
            block_size: block_size as usize,
            miniblocks_per_block: miniblocks_per_block as usize,
            total_values: total_vals as usize,
            first_value,
        })
    }

    /// Decode a block of values from delta encoding
    fn decode_block(
        &mut self,
        mut previous_value: T,
        mut remaining_values: i64,
        out: &mut Vec<T>,
    ) -> Result<usize> {
        out.reserve(self.block_size);

        // Read the minimum delta
        let (min_delta, num_bytes) = decode_zigzag_uleb128(&self.buf)?;
        self.buf = &self.buf[num_bytes..];

        let min_delta = T::from_i64(min_delta).ok_or_else(|| {
            RayexecError::new(format!(
                "Unable to cast min delta {min_delta} to correct type"
            ))
        })?;

        // Read the bit widths for each miniblock
        let bit_widths = &self.buf[..self.miniblocks_per_block];
        self.buf = &self.buf[self.miniblocks_per_block..];

        let values_per_miniblock = self.block_size / self.miniblocks_per_block;

        // Used to compute total number decoded.
        let out_start = out.len();

        // Decode each miniblock
        for &bit_width in bit_widths {
            // Decoder needs to handle the case where block has additional bit
            // widths but not more values to read.
            if remaining_values <= 0 {
                break;
            }

            // Number of values remaining in this mini block.
            let mut miniblock_remaining_values =
                std::cmp::min(remaining_values, values_per_miniblock as i64);

            let bit_width = bit_width as usize;

            // Read bit-packed data for this miniblock
            let byte_count = (bit_width * values_per_miniblock + 7) / 8;
            let packed_data = &self.buf[..byte_count];
            self.buf = &self.buf[byte_count..];

            // Index in out where this miniblock starts.
            let block_start = out.len();

            // Unpack based on the byte width of the type we're decoding to.
            //
            // Unpacking here works by ensuring that the chunks we read are
            // always aligned to a byte, hence the zero data arrays.
            match std::mem::size_of::<T>() {
                1 => {
                    let mut buf = T::zero_packed_array();
                    while miniblock_remaining_values > 0 {
                        T::unpack(packed_data, bit_width, &mut buf);
                        out.extend_from_slice(buf.as_ref());
                        miniblock_remaining_values -= 8;
                    }
                }
                2 => {
                    let mut buf = T::zero_packed_array();
                    while miniblock_remaining_values > 0 {
                        T::unpack(packed_data, bit_width, &mut buf);
                        out.extend_from_slice(buf.as_ref());
                        miniblock_remaining_values -= 16;
                    }
                }
                4 => {
                    let mut buf = T::zero_packed_array();
                    while miniblock_remaining_values > 0 {
                        T::unpack(packed_data, bit_width, &mut buf);
                        out.extend_from_slice(buf.as_ref());
                        miniblock_remaining_values -= 32;
                    }
                }
                8 => {
                    // TODO: Double check that we can do this an remain
                    // inbounds to the original buffer. We might need to
                    // allocate a buffer if we have <64 values left to read.
                    let mut buf = T::zero_packed_array();
                    while miniblock_remaining_values > 0 {
                        T::unpack(packed_data, bit_width, &mut buf);
                        out.extend_from_slice(buf.as_ref());
                        miniblock_remaining_values -= 64;
                    }
                }
                other => panic!("Invalid type size: {other}"),
            }

            // Adjust total remaining values.
            //
            // Note that we're not using `miniblock_remaining_values` here since
            // that's mostly for early stoppage above. We always want to track
            // the remaining values relative to the size of the mini blocks.
            remaining_values -= values_per_miniblock as i64;

            // The above unpacking works on fixed sized arrays that we just copy
            // direclty to `out`. We may end up with more values than we
            // actually want, so truncate the vec to the correct len to remove
            // those values.
            if remaining_values < 0 {
                let new_len = out.len() - remaining_values.abs() as usize;
                out.truncate(new_len);
            }

            // We stored delta in the output vector, now adjust them
            // according to previous value.
            for v in &mut out[block_start..] {
                *v = v.wrapping_add(&min_delta).wrapping_add(&previous_value);

                // Every delta is relative to the immediately preceding value.
                previous_value = *v;
            }
        }

        let num_decoded = out.len() - out_start;

        Ok(num_decoded)
    }

    pub fn decode_values(&mut self, out: &mut Vec<T>) -> Result<()> {
        out.push(self.first_value);
        let mut num_decoded = 1;

        let mut previous_value = self.first_value;

        while num_decoded < self.total_values {
            let remaining = self.total_values - num_decoded;

            let count = self.decode_block(previous_value, remaining as i64, out)?;
            previous_value = out.last().copied().unwrap();
            num_decoded += count;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Some tests taken from parquet2.

    #[test]
    fn single_value() {
        // Generated by parquet-rs
        //
        // header: [128, 1, 4, 1, 2]
        // block size: 128, 1
        // mini-blocks: 4
        // elements: 1
        // first_value: 2 <=z> 1
        let data = &[128, 1, 4, 1, 2];

        let mut decoder = Decoder::<i64>::try_new(data).unwrap();
        let mut out = Vec::new();
        decoder.decode_values(&mut out).unwrap();

        assert_eq!(&[1], &out[..]);
    }

    #[test]
    fn test_from_spec() {
        // VALIDATED FROM SPARK==3.1.1
        // header: [128, 1, 4, 5, 2]
        // block size: 128, 1
        // mini-blocks: 4
        // elements: 5
        // first_value: 2 <=z> 1
        // block1: [2, 0, 0, 0, 0]
        // min_delta: 2 <=z> 1
        // bit_width: 0
        let data = &[128, 1, 4, 5, 2, 2, 0, 0, 0, 0];

        let mut decoder = Decoder::try_new(data).unwrap();
        let mut out = Vec::new();
        decoder.decode_values(&mut out).unwrap();

        let expected = [1, 2, 3, 4, 5];

        assert_eq!(&expected, &out[..]);
    }

    #[test]
    fn case2() {
        // VALIDATED FROM SPARK==3.1.1
        // header: [128, 1, 4, 6, 2]
        // block size: 128, 1 <=u> 128
        // mini-blocks: 4     <=u> 4
        // elements: 6        <=u> 6
        // first_value: 2     <=z> 1
        // block1: [7, 3, 0, 0, 0]
        // min_delta: 7       <=z> -4
        // bit_widths: [3, 0, 0, 0]
        // values: [
        //      0b01101101
        //      0b00001011
        //      ...
        // ]                  <=b> [3, 3, 3, 3, 0]
        let data = &[
            128, 1, 4, 6, 2, 7, 3, 0, 0, 0, 0b01101101, 0b00001011, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            // these should not be consumed
            1, 2, 3,
        ];

        let mut decoder = Decoder::try_new(data).unwrap();
        let mut out = Vec::new();
        decoder.decode_values(&mut out).unwrap();

        let expected = vec![1, 2, 3, 4, 5, 1];

        assert_eq!(&expected, &out[..]);
    }

    #[test]
    fn multiple_miniblocks() {
        #[rustfmt::skip]
        let data = &[
            // Header: [128, 1, 4, 65, 100]
            128, 1, // block size <=u> 128
            4,      // number of mini-blocks <=u> 4
            65,     // number of elements <=u> 65
            100,    // first_value <=z> 50

            // Block 1 header: [7, 3, 4, 0, 0]
            7,            // min_delta <=z> -4
            3, 4, 255, 0, // bit_widths (255 should not be used as only two miniblocks are needed)

            // 32 3-bit values of 0 for mini-block 1 (12 bytes)
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,

            // 32 4-bit values of 8 for mini-block 2 (16 bytes)
            0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88,
            0x88, 0x88,

            // these should not be consumed
            1, 2, 3,
        ];

        #[rustfmt::skip]
        let expected = [
            // First value
            50,

            // Mini-block 1: 32 deltas of -4
            46, 42, 38, 34, 30, 26, 22, 18, 14, 10, 6, 2, -2, -6, -10, -14, -18, -22, -26, -30, -34,
            -38, -42, -46, -50, -54, -58, -62, -66, -70, -74, -78,

            // Mini-block 2: 32 deltas of 4
            -74, -70, -66, -62, -58, -54, -50, -46, -42, -38, -34, -30, -26, -22, -18, -14, -10, -6,
            -2, 2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46, 50,
        ];

        let mut decoder = Decoder::try_new(data).unwrap();
        let mut out = Vec::new();
        decoder.decode_values(&mut out).unwrap();

        assert_eq!(&expected[..], &out[..]);
    }
}
