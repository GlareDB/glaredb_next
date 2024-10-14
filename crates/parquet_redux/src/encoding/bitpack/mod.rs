pub mod boolean;
pub mod unpack;

use num::PrimInt;
use unpack::{unpack32, unpack64};

pub trait BitPackable: PrimInt + Default {
    type PackedArray;

    fn zero_packed_array() -> Self::PackedArray;

    /// Unpacks bitpacked values into `output`.
    fn unpack(input: &[u8], num_bits: usize, output: &mut Self::PackedArray);
}

impl BitPackable for u32 {
    type PackedArray = [u32; 32];

    fn zero_packed_array() -> Self::PackedArray {
        [0; 32]
    }

    fn unpack(input: &[u8], num_bits: usize, output: &mut Self::PackedArray) {
        unpack32(input, output, num_bits)
    }
}

impl BitPackable for u64 {
    type PackedArray = [u64; 64];

    fn zero_packed_array() -> Self::PackedArray {
        [0; 64]
    }

    fn unpack(input: &[u8], num_bits: usize, output: &mut Self::PackedArray) {
        unpack64(input, output, num_bits)
    }
}
