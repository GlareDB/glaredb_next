use bytes::Bytes;
use rayexec_bullet::executor::builder::GermanVarlenBuffer;

use super::decoder::ColumnValueDecoder;
use super::Encoding;
use crate::errors::Result;

/// Column value decoder for byte arrays that stores bytes in a contiguous
/// buffer with "views" slicing into the buffer for the actual values.
///
/// The "views" correspond to Arrow's string view concept (and to our "german"
/// buffers).
#[derive(Debug)]
pub struct ViewColumnValueDecoder {
    /// If binary data that we read should be utf8-validated.
    validate_utf8: bool,
}

impl ViewColumnValueDecoder {}

impl ColumnValueDecoder for ViewColumnValueDecoder {
    /// Stores bytes in a contiguous array.
    ///
    /// Note that this will use [u8] for strings as well, string validation is
    /// handled separately, but the underlying storage is the same.
    type Buffer = GermanVarlenBuffer<[u8]>;

    fn set_dict(
        &mut self,
        buf: bytes::Bytes,
        num_values: u32,
        encoding: super::Encoding,
        _is_sorted: bool,
    ) -> Result<()> {
        unimplemented!()
    }

    fn set_data(
        &mut self,
        encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<()> {
        unimplemented!()
    }

    fn read(&mut self, out: &mut Self::Buffer, num_values: usize) -> Result<usize> {
        unimplemented!()
    }

    fn skip_values(&mut self, num_values: usize) -> Result<usize> {
        unimplemented!()
    }
}
