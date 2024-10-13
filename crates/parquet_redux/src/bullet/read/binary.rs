use rayexec_bullet::executor::builder::{ArrayDataBuffer, GermanVarlenBuffer};
use rayexec_bullet::executor::physical_type::VarlenType;
use rayexec_error::{RayexecError, Result};

pub fn plain_decode_binary_data<T>(num_values: usize, buf: &[u8]) -> Result<GermanVarlenBuffer<T>>
where
    T: VarlenType + ?Sized,
{
    let min_buf_len = num_values * std::mem::size_of::<u32>();
    if buf.len() < min_buf_len {
        return Err(RayexecError::new(format!(
            "Buffer too small, expected at least {}, got {}",
            min_buf_len,
            buf.len()
        )));
    }

    // TODO: Could use better heuristics here since there's the case that a lot
    // of the actual data could be inlined in the stored metadata.
    let mut data = GermanVarlenBuffer::with_len_and_data_capacity(num_values, buf.len());

    let mut buf_start = 0;
    for idx in 0..num_values {
        let len = u32::from_le_bytes(buf[buf_start..buf_start + 4].try_into().unwrap()) as usize;
        buf_start += 4;

        let val = &buf[buf_start..buf_start + len];
        buf_start += len;

        data.put(idx, T::try_from_bytes(val)?);
    }

    Ok(data)
}
