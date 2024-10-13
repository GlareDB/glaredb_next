use num::cast::AsPrimitive;
use rayexec_error::{RayexecError, Result};

use crate::types::ParquetPrimitiveType;

pub fn plain_decode_primitive_data<T, P>(num_values: usize, buf: &[u8]) -> Result<Vec<T>>
where
    T: Copy + 'static,
    P: ParquetPrimitiveType + AsPrimitive<T>,
{
    let val_size = std::mem::size_of::<P>();
    let min_buf_len = num_values * val_size;
    if buf.len() < min_buf_len {
        return Err(RayexecError::new(format!(
            "Buffer too small, expected at least {}, got {}",
            min_buf_len,
            buf.len()
        )));
    }

    let mut out = Vec::with_capacity(num_values);

    for idx in 0..num_values {
        let start = val_size * idx;
        let val = P::from_le_bytes(buf[start..start + val_size].try_into().unwrap());
        let val: T = val.as_();
        out.push(val)
    }

    Ok(out)
}
