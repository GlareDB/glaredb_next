pub mod unpack;

use std::io::Write;

use rayexec_bullet::bitmap::SET_MASKS;
use rayexec_error::Result;

/// LSB encodes bools to a writer.
pub fn bitpack_bools<W, I>(writer: &mut W, mut iter: I) -> Result<()>
where
    W: Write,
    I: Iterator<Item = bool> + ExactSizeIterator,
{
    let len = iter.len();

    let chunks = len / 8;
    let rem = len % 8;

    for _ in 0..chunks {
        let mut b: u8 = 0;
        (0..8).for_each(|bit| {
            if iter.next().unwrap() {
                b |= SET_MASKS[bit];
            }
        });
        writer.write_all(&[b])?;
    }

    if rem != 0 {
        let mut b: u8 = 0;
        iter.enumerate().for_each(|(bit, v)| {
            if v {
                b |= SET_MASKS[bit];
            }
        });
        writer.write_all(&[b])?;
    }

    Ok(())
}
