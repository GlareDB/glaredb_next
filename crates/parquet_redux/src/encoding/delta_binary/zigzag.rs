//! Signed alternative to ULEB that zigzags before ULEBing.
use rayexec_error::Result;

use super::uleb128::{decode_uleb128, encode_uleb128};

pub fn encode_zigzag_uleb128(value: i64, buf: &mut [u8]) -> usize {
    let value = ((value << 1) ^ (value >> (64 - 1))) as u64;
    encode_uleb128(value, buf)
}

pub fn decode_zigzag_uleb128(buf: &[u8]) -> Result<(i64, usize)> {
    let (v, num_bytes) = decode_uleb128(buf)?;
    let v = (v >> 1) as i64 ^ -((v & 1) as i64);
    Ok((v, num_bytes))
}
