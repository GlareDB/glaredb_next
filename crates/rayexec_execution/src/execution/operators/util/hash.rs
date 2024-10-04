use ahash::RandomState;
use rayexec_bullet::{
    array::{Array2, BooleanArray, OffsetIndex, PrimitiveArray, VarlenArray, VarlenType2},
    row::ScalarRow,
    scalar::{interval::Interval, ScalarValue},
};
use rayexec_error::{RayexecError, Result};

/// State used for all hashing operations during physical execution.
pub const HASH_RANDOM_STATE: RandomState = RandomState::with_seeds(0, 0, 0, 0);

/// Get the partition to use for a hash.
///
/// This should be used for hash repartitions, hash joins, hash aggregates, and
/// whatever else requires consistent hash to partition mappings.
pub const fn partition_for_hash(hash: u64, partitions: usize) -> usize {
    hash as usize % partitions
}

pub trait ArrayHasher {
    /// Hash every row in the provided arrays, writing the values to `hashes`.
    ///
    /// All arrays provided must be of the same length, and the provided hash
    /// buffer must equal that length.
    fn hash_arrays2<'a>(arrays: &[&Array2], hashes: &'a mut [u64]) -> Result<&'a mut [u64]>;
}

#[derive(Debug, Clone, Copy)]
pub struct ForcedCollisionHasher;

impl ArrayHasher for ForcedCollisionHasher {
    fn hash_arrays2<'a>(_arrays: &[&Array2], hashes: &'a mut [u64]) -> Result<&'a mut [u64]> {
        for hash in hashes.iter_mut() {
            *hash = 0;
        }
        Ok(hashes)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AhashHasher;

impl ArrayHasher for AhashHasher {
    fn hash_arrays2<'a>(arrays: &[&Array2], hashes: &'a mut [u64]) -> Result<&'a mut [u64]> {
        for (idx, array) in arrays.iter().enumerate() {
            let combine_hash = idx > 0;

            match array {
                Array2::Null(_) => hash_null(hashes, combine_hash),
                Array2::Boolean(arr) => hash_bool(arr, hashes, combine_hash),
                Array2::Float32(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::Float64(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::Int8(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::Int16(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::Int32(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::Int64(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::Int128(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::UInt8(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::UInt16(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::UInt32(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::UInt64(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::UInt128(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::Decimal64(arr) => hash_primitive(arr.get_primitive(), hashes, combine_hash),
                Array2::Decimal128(arr) => {
                    hash_primitive(arr.get_primitive(), hashes, combine_hash)
                }
                Array2::Date32(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::Date64(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::Timestamp(arr) => hash_primitive(arr.get_primitive(), hashes, combine_hash),
                Array2::Interval(arr) => hash_primitive(arr, hashes, combine_hash),
                Array2::Utf8(arr) => hash_varlen(arr, hashes, combine_hash),
                Array2::LargeUtf8(arr) => hash_varlen(arr, hashes, combine_hash),
                Array2::Binary(arr) => hash_varlen(arr, hashes, combine_hash),
                Array2::LargeBinary(arr) => hash_varlen(arr, hashes, combine_hash),
                Array2::Struct(_) => {
                    // Yet
                    return Err(RayexecError::new("hashing struct arrays not supported"));
                }
                Array2::List(_) => {
                    // Yet
                    return Err(RayexecError::new("hashing list arrays not supported"));
                }
            }
        }

        Ok(hashes)
    }
}

/// Helper trait for hashing values.
///
/// This is mostly for floats since they don't automatically implement `Hash`.
trait HashValue {
    fn hash_one(&self) -> u64;
}

macro_rules! impl_hash_value {
    ($typ:ty) => {
        impl HashValue for $typ {
            fn hash_one(&self) -> u64 {
                HASH_RANDOM_STATE.hash_one(self)
            }
        }
    };
}

impl_hash_value!(bool);
impl_hash_value!(i8);
impl_hash_value!(i16);
impl_hash_value!(i32);
impl_hash_value!(i64);
impl_hash_value!(i128);
impl_hash_value!(u8);
impl_hash_value!(u16);
impl_hash_value!(u32);
impl_hash_value!(u64);
impl_hash_value!(u128);
impl_hash_value!(str);
impl_hash_value!([u8]);
impl_hash_value!(Interval);

impl HashValue for f32 {
    fn hash_one(&self) -> u64 {
        HASH_RANDOM_STATE.hash_one(self.to_ne_bytes())
    }
}

impl HashValue for f64 {
    fn hash_one(&self) -> u64 {
        HASH_RANDOM_STATE.hash_one(self.to_ne_bytes())
    }
}

/// Combines two hashes into one hash
///
/// This implementation came from datafusion.
const fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}

/// All nulls should hash to the same value.
///
/// _What_ that value is is arbitrary, but it needs to be consistent.
fn null_hash_value() -> u64 {
    HASH_RANDOM_STATE.hash_one(1)
}

fn hash_null(hashes: &mut [u64], combine: bool) {
    let null_hash = null_hash_value();

    if combine {
        for hash in hashes.iter_mut() {
            *hash = combine_hashes(null_hash, *hash);
        }
    } else {
        for hash in hashes.iter_mut() {
            *hash = null_hash;
        }
    }
}

fn hash_bool(array: &BooleanArray, hashes: &mut [u64], combine: bool) {
    assert_eq!(
        array.len(),
        hashes.len(),
        "Hashes buffer should be same length as array"
    );

    let values = array.values();
    match array.validity() {
        Some(_bitmap) => {
            // TODO: Nulls
            unimplemented!()
        }
        None => {
            if combine {
                for (val, hash) in values.iter().zip(hashes.iter_mut()) {
                    *hash = combine_hashes(val.hash_one(), *hash);
                }
            } else {
                for (val, hash) in values.iter().zip(hashes.iter_mut()) {
                    *hash = val.hash_one();
                }
            }
        }
    }
}

/// Hash a primitive array.
fn hash_primitive<T: HashValue>(array: &PrimitiveArray<T>, hashes: &mut [u64], combine: bool) {
    assert_eq!(
        array.len(),
        hashes.len(),
        "Hashes buffer should be same length as array"
    );

    let values = array.values();
    match array.validity() {
        Some(bitmap) => {
            if combine {
                for ((val, hash), valid) in values
                    .as_ref()
                    .iter()
                    .zip(hashes.iter_mut())
                    .zip(bitmap.iter())
                {
                    if valid {
                        *hash = combine_hashes(val.hash_one(), *hash);
                    } else {
                        *hash = combine_hashes(null_hash_value(), *hash);
                    }
                }
            } else {
                for ((val, hash), valid) in values
                    .as_ref()
                    .iter()
                    .zip(hashes.iter_mut())
                    .zip(bitmap.iter())
                {
                    if valid {
                        *hash = val.hash_one();
                    } else {
                        *hash = null_hash_value();
                    }
                }
            }
        }
        None => {
            if combine {
                for (val, hash) in values.as_ref().iter().zip(hashes.iter_mut()) {
                    *hash = combine_hashes(val.hash_one(), *hash);
                }
            } else {
                for (val, hash) in values.as_ref().iter().zip(hashes.iter_mut()) {
                    *hash = val.hash_one();
                }
            }
        }
    }
}

/// Hash a varlen array.
fn hash_varlen<T, O>(array: &VarlenArray<T, O>, hashes: &mut [u64], combine: bool)
where
    T: VarlenType2 + HashValue + ?Sized,
    O: OffsetIndex,
{
    assert_eq!(
        array.len(),
        hashes.len(),
        "Hashes buffer should be same length as array"
    );

    let values_iter = array.values_iter();
    match array.validity() {
        Some(bitmap) => {
            if combine {
                for ((val, hash), valid) in values_iter.zip(hashes.iter_mut()).zip(bitmap.iter()) {
                    if valid {
                        *hash = combine_hashes(val.hash_one(), *hash);
                    } else {
                        *hash = combine_hashes(null_hash_value(), *hash);
                    }
                }
            } else {
                for ((val, hash), valid) in values_iter.zip(hashes.iter_mut()).zip(bitmap.iter()) {
                    if valid {
                        *hash = val.hash_one();
                    } else {
                        *hash = null_hash_value();
                    }
                }
            }
        }
        None => {
            if combine {
                for (val, hash) in values_iter.zip(hashes.iter_mut()) {
                    *hash = combine_hashes(val.hash_one(), *hash);
                }
            } else {
                for (val, hash) in values_iter.zip(hashes.iter_mut()) {
                    *hash = val.hash_one();
                }
            }
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use rayexec_bullet::array::{Int32Array, Utf8Array};

//     use super::*;

//     #[test]
//     fn array_hash_row_hash_equivalent() {
//         // Hashing a slice of a arrays should produce the same hashes as the row
//         // representation of those same values.

//         let arrays = [
//             &Array2::Utf8(Utf8Array::from_iter(["a", "b", "c"])),
//             &Array2::Int32(Int32Array::from_iter([1, 2, 3])),
//         ];
//         let mut hashes = vec![0; 3];

//         // Hash the arrays.
//         AhashHasher::hash_arrays2(&arrays, &mut hashes).unwrap();

//         // Sanity check just to make sure we're hashing.
//         assert_ne!(vec![0; 3], hashes);

//         // Now hash the row representations.
//         let mut row_hashes = vec![0; 3];
//         for idx in 0..3 {
//             let row = ScalarRow::try_new_from_arrays2(&arrays, idx).unwrap();
//             row_hashes[idx] = hash_row(&row).unwrap();
//         }

//         assert_eq!(hashes, row_hashes);
//     }

//     #[test]
//     fn nulls_produce_different_values() {
//         let arr1 = Array2::Utf8(Utf8Array::from_iter([Some("a"), Some("b"), Some("c")]));
//         let mut hashes1 = vec![0; 3];
//         AhashHasher::hash_arrays2(&[&arr1], &mut hashes1).unwrap();

//         let arr2 = Array2::Utf8(Utf8Array::from_iter([Some("a"), None, Some("c")]));
//         let mut hashes2 = vec![0; 3];
//         AhashHasher::hash_arrays2(&[&arr2], &mut hashes2).unwrap();

//         assert_ne!(hashes1, hashes2);
//     }
// }
