//! Specialized decoders for reading into "view" buffers.
//!
//! These should be used with the `ViewColumnValueDecoder` and are only valid
//! for byte arrays. Note also that none of these implement the `Decoder` trait
//! since that's hard to work with.

use bytes::Bytes;
use rayexec_bullet::array::ArrayData;
use rayexec_bullet::executor::builder::{ArrayDataBuffer, GermanVarlenBuffer};

use super::Encoding;
use crate::encodings::rle::RleDecoder;
use crate::errors::{ParquetError, Result};

#[derive(Debug)]
pub struct ViewBuffer {
    /// Current index we're writing to.
    ///
    /// This is also the currently "length" of the buffer.
    current_idx: usize,
    /// The actual buffer, should be initialized to the max length we expect to
    /// read.
    buffer: GermanVarlenBuffer<[u8]>,
}

impl ViewBuffer {
    pub fn new(len: usize) -> Self {
        ViewBuffer {
            current_idx: 0,
            buffer: GermanVarlenBuffer::with_len(len),
        }
    }

    pub fn len(&self) -> usize {
        self.current_idx
    }

    pub fn push(&mut self, data: &[u8]) {
        self.buffer.put(self.current_idx, data);
        self.current_idx += 1;
    }

    pub fn get(&self, idx: usize) -> Option<&[u8]> {
        if idx >= self.current_idx {
            return None;
        }

        self.buffer.get(idx)
    }

    pub fn validate_utf8(&self) -> Result<()> {
        for val in self.buffer.iter().take(self.current_idx) {
            let _ = std::str::from_utf8(val)?;
        }
        Ok(())
    }

    pub fn into_array_data(mut self) -> ArrayData {
        self.buffer.truncate(self.current_idx);
        self.buffer.into_data()
    }
}

#[derive(Debug)]
pub enum ViewDecoder {
    Plain(PlainViewDecoder),
    Dictionary(DictionaryViewDecoder),
}

impl ViewDecoder {
    pub fn new(
        encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<Self> {
        let decoder = match encoding {
            Encoding::PLAIN => Self::Plain(PlainViewDecoder::new(data, num_levels, num_values)),
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
                Self::Dictionary(DictionaryViewDecoder::new(data, num_levels, num_values))
            }
            // Encoding::DELTA_LENGTH_BYTE_ARRAY => ByteArrayDecoder::DeltaLength(
            //     ByteArrayDecoderDeltaLength::new(data, validate_utf8)?,
            // ),
            // Encoding::DELTA_BYTE_ARRAY => {
            //     ByteArrayDecoder::DeltaByteArray(ByteArrayDecoderDelta::new(data, validate_utf8)?)
            // }
            _ => {
                return Err(general_err!(
                    "unsupported encoding for byte array: {}",
                    encoding
                ))
            }
        };

        Ok(decoder)
    }

    pub fn read(
        &mut self,
        buffer: &mut ViewBuffer,
        num_values: usize,
        dict: Option<&ViewBuffer>,
    ) -> Result<usize> {
        match self {
            Self::Plain(d) => d.read(buffer, num_values),
            Self::Dictionary(d) => {
                let dict =
                    dict.ok_or_else(|| general_err!("missing dictionary page for column"))?;

                d.read(buffer, dict, num_values)
            }
        }
    }

    pub fn skip(&mut self, num_values: usize, dict: Option<&ViewBuffer>) -> Result<usize> {
        match self {
            Self::Plain(d) => d.skip(num_values),
            Self::Dictionary(d) => {
                let dict =
                    dict.ok_or_else(|| general_err!("missing dictionary page for column"))?;

                d.skip(dict, num_values)
            }
        }
    }
}

/// Decoder for PLAIN.
#[derive(Debug)]
pub struct PlainViewDecoder {
    /// Currently set page data.
    buf: Bytes,
    /// Current offset into data.
    offset: usize,
    /// This is a maximum as the null count is not always known, e.g. value data
    /// from a v1 data page
    max_remaining_values: usize,
}

impl PlainViewDecoder {
    pub fn new(buf: Bytes, num_levels: usize, num_values: Option<usize>) -> Self {
        PlainViewDecoder {
            buf,
            offset: 0,
            max_remaining_values: num_values.unwrap_or(num_levels),
        }
    }

    pub fn read(&mut self, buffer: &mut ViewBuffer, num_vals: usize) -> Result<usize> {
        let to_read = usize::min(num_vals, self.max_remaining_values);

        if self.buf.len() - self.offset == 0 {
            return Ok(0);
        }

        let mut num_read = 0;
        let buf = &self.buf;
        while self.offset < self.buf.len() && num_read != to_read {
            if self.offset + 4 > buf.len() {
                return Err(ParquetError::EOF("eof decoding byte array".into()));
            }
            let len_bytes: [u8; 4] = buf[self.offset..self.offset + 4].try_into().unwrap();
            let len = u32::from_le_bytes(len_bytes) as usize;
            self.offset += 4;

            let data = &self.buf[self.offset..self.offset + len];
            self.offset += len;

            buffer.push(data);

            num_read += 1;
        }

        self.max_remaining_values -= to_read;

        Ok(num_read)
    }

    pub fn skip(&mut self, num_vals: usize) -> Result<usize> {
        let to_skip = usize::min(num_vals, self.max_remaining_values);

        let mut skip = 0;
        let buf = &self.buf;
        while self.offset < self.buf.len() && skip != to_skip {
            if self.offset + 4 > buf.len() {
                return Err(ParquetError::EOF("eof decoding byte array".into()));
            }
            let len_bytes: [u8; 4] = buf[self.offset..self.offset + 4].try_into().unwrap();
            let len = u32::from_le_bytes(len_bytes) as usize;
            self.offset += 4 + len as usize;

            skip += 1;
        }

        self.max_remaining_values -= skip;

        Ok(skip)
    }
}

/// Decoder for PLAIN_DICTIONARY/RLE_DICTIONARY.
#[derive(Debug)]
pub struct DictionaryViewDecoder {
    decoder: DictIndexDecoder,
}

impl DictionaryViewDecoder {
    pub fn new(data: Bytes, num_levels: usize, num_values: Option<usize>) -> Self {
        DictionaryViewDecoder {
            decoder: DictIndexDecoder::new(data, num_levels, num_values),
        }
    }

    pub fn read(
        &mut self,
        buffer: &mut ViewBuffer,
        dict: &ViewBuffer,
        num_vals: usize,
    ) -> Result<usize> {
        // TODO: What would be _real_ cool is if `dict` was an array and we just
        // created selection vectors on top of the dictionary (data is behind an
        // arc).

        self.decoder.read(num_vals, |keys| {
            for &key in keys {
                let val = dict
                    .get(key as usize)
                    .ok_or_else(|| general_err!("Missing dictionary value at index {key}"))?;
                buffer.push(val);
            }
            Ok(())
        })
    }

    pub fn skip(&mut self, _dict: &ViewBuffer, num_vals: usize) -> Result<usize> {
        self.decoder.skip(num_vals)
    }
}

/// Decoder for `RLE_DICTIONARY` indices
#[derive(Debug)]
pub struct DictIndexDecoder {
    /// Decoder for the dictionary offsets array
    decoder: RleDecoder,
    /// We want to decode the offsets in chunks so we will maintain an internal
    /// buffer of decoded offsets
    index_buf: Box<[i32; 1024]>,
    /// Current length of `index_buf`
    index_buf_len: usize,
    /// Current offset into `index_buf`. If `index_buf_offset` ==
    /// `index_buf_len` then we've consumed the entire buffer and need to decode
    /// another chunk of offsets.
    index_offset: usize,
    /// This is a maximum as the null count is not always known, e.g. value data from
    /// a v1 data page
    max_remaining_values: usize,
}

impl DictIndexDecoder {
    /// Create a new [`DictIndexDecoder`] with the provided data page, the number of levels
    /// associated with this data page, and the number of non-null values (if known)
    pub fn new(data: Bytes, num_levels: usize, num_values: Option<usize>) -> Self {
        let bit_width = data[0];
        let mut decoder = RleDecoder::new(bit_width);
        decoder.set_data(data.slice(1..));

        Self {
            decoder,
            index_buf: Box::new([0; 1024]),
            index_buf_len: 0,
            index_offset: 0,
            max_remaining_values: num_values.unwrap_or(num_levels),
        }
    }

    /// Read up to `len` values, returning the number of values read
    /// and calling `f` with each decoded dictionary index
    ///
    /// Will short-circuit and return on error
    pub fn read<F: FnMut(&[i32]) -> Result<()>>(&mut self, len: usize, mut f: F) -> Result<usize> {
        let mut values_read = 0;

        while values_read != len && self.max_remaining_values != 0 {
            if self.index_offset == self.index_buf_len {
                // We've consumed the entire index buffer so we need to reload it before proceeding
                let read = self.decoder.get_batch(self.index_buf.as_mut())?;
                if read == 0 {
                    break;
                }
                self.index_buf_len = read;
                self.index_offset = 0;
            }

            let to_read = (len - values_read)
                .min(self.index_buf_len - self.index_offset)
                .min(self.max_remaining_values);

            f(&self.index_buf[self.index_offset..self.index_offset + to_read])?;

            self.index_offset += to_read;
            values_read += to_read;
            self.max_remaining_values -= to_read;
        }
        Ok(values_read)
    }

    /// Skip up to `to_skip` values, returning the number of values skipped
    pub fn skip(&mut self, to_skip: usize) -> Result<usize> {
        let to_skip = to_skip.min(self.max_remaining_values);

        let mut values_skip = 0;
        while values_skip < to_skip {
            if self.index_offset == self.index_buf_len {
                // Instead of reloading the buffer, just skip in the decoder
                let skip = self.decoder.skip(to_skip - values_skip)?;

                if skip == 0 {
                    break;
                }

                self.max_remaining_values -= skip;
                values_skip += skip;
            } else {
                // We still have indices buffered, so skip within the buffer
                let skip = (to_skip - values_skip).min(self.index_buf_len - self.index_offset);

                self.index_offset += skip;
                self.max_remaining_values -= skip;
                values_skip += skip;
            }
        }
        Ok(values_skip)
    }
}
