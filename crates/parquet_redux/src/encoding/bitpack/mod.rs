pub mod boolean;
pub mod unpack;

use num::PrimInt;
use unpack::{unpack16, unpack32, unpack64, unpack8};

pub trait BitPackable: PrimInt + Default {
    type PackedArray: AsRef<[Self]>;

    fn zero_packed_array() -> Self::PackedArray;

    /// Unpacks bitpacked values into `output`.
    fn unpack(input: &[u8], num_bits: usize, output: &mut Self::PackedArray);
}

macro_rules! impl_unsigned {
    ($ty:ty, $bits:expr, $fn:ident) => {
        impl BitPackable for $ty {
            type PackedArray = [$ty; $bits];

            fn zero_packed_array() -> Self::PackedArray {
                [0; $bits]
            }

            fn unpack(input: &[u8], num_bits: usize, output: &mut Self::PackedArray) {
                $fn(input, output, num_bits)
            }
        }
    };
}

impl_unsigned!(u8, 8, unpack8);
impl_unsigned!(u16, 16, unpack16);
impl_unsigned!(u32, 32, unpack32);
impl_unsigned!(u64, 64, unpack64);

macro_rules! impl_unsigned {
    ($ty:ty, $bits:expr, $fn:ident, $unsigned_ty:ty) => {
        impl BitPackable for $ty {
            type PackedArray = [$ty; $bits];

            fn zero_packed_array() -> Self::PackedArray {
                [0; $bits]
            }

            fn unpack(input: &[u8], num_bits: usize, output: &mut Self::PackedArray) {
                // SAFETY: We transmuting to a same size int. While we only implement
                // unpacked for unsigned ints, all deltas should be positive.
                let unsigned =
                    unsafe { std::mem::transmute::<_, &mut [$unsigned_ty; $bits]>(output) };
                $fn(input, unsigned, num_bits)
            }
        }
    };
}

impl_unsigned!(i8, 8, unpack8, u8);
impl_unsigned!(i16, 16, unpack16, u16);
impl_unsigned!(i32, 32, unpack32, u32);
impl_unsigned!(i64, 64, unpack64, u64);
