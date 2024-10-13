//! ULEB128 things.
//!
//! <https://en.wikipedia.org/wiki/LEB128#Unsigned_LEB128>
use rayexec_error::{RayexecError, Result};

/// ULEB128 encode a u64.
///
/// Requires `buf` to have a length of at least 10.
///
/// Returns number of bytes written to.
pub fn encode_uleb128(mut value: u64, buf: &mut [u8]) -> usize {
    let mut idx = 0;

    loop {
        let mut byte = (value & 0x7F) as u8; // Take the least significant 7 bits
        value >>= 7; // Shift the value by 7 bits

        if value != 0 {
            byte |= 0x80; // Set the continuation bit if more bytes are needed
        }

        buf[idx] = byte;
        idx += 1;

        if value == 0 {
            break;
        }
    }

    idx
}

/// Decodes as u64 from `buf`.
///
/// Returns (val, offset) pair.
pub fn decode_uleb128(buf: &[u8]) -> Result<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0;
    let mut consumed = 0;

    for &byte in buf {
        let low_bits = (byte & 0x7F) as u64;
        value |= low_bits << shift;

        consumed += 1;
        shift += 7;

        // If the continuation bit (MSB) is not set, we're done
        if byte & 0x80 == 0 {
            return Ok((value, consumed));
        }

        // Overflow detection for safety, ULEB128 can only represent values up to 2^64-1
        if shift > 64 {
            return Err(RayexecError::new("ULEB128 value is too large"));
        }
    }

    Err(RayexecError::new("Invalid ULEB128 sequence"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode() {
        let v = 624485;
        let mut buf = [0; 10];
        encode_uleb128(v, &mut buf);

        assert_eq!(buf, [0xE5, 0x8E, 0x26, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn decode() {
        let buf = [0xE5, 0x8E, 0x26, 0, 0, 0, 0, 0, 0, 0];
        let (v, num_bytes) = decode_uleb128(&buf).unwrap();

        assert_eq!(v, 624485);
        assert_eq!(3, num_bytes);
    }

    #[test]
    fn decode_extra_data() {
        let buf = [0xE5, 0x8E, 0x26, 0x53, 0x21, 0, 0, 0, 0, 0];
        let (v, num_bytes) = decode_uleb128(&buf).unwrap();

        assert_eq!(624485, v);
        assert_eq!(3, num_bytes);
    }

    #[test]
    fn encode_max() {
        let mut buf = [0; 10];
        encode_uleb128(u64::MAX, &mut buf);

        assert_eq!(buf, [255, 255, 255, 255, 255, 255, 255, 255, 255, 1]);
    }

    #[test]
    fn decode_max() {
        let buf = [255, 255, 255, 255, 255, 255, 255, 255, 255, 1];
        let (v, num_bytes) = decode_uleb128(&buf).unwrap();

        assert_eq!(u64::MAX, v);
        assert_eq!(10, num_bytes);
    }
}
