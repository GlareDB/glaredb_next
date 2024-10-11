// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Contains all supported decoders for Parquet.

pub mod get_decoder;

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::{cmp, mem};

use bytes::Bytes;
use num::traits::WrappingAdd;
use num::FromPrimitive;

use super::rle::RleDecoder;
use crate::basic::*;
use crate::data_type::{ParquetValueType, *};
use crate::errors::{ParquetError, Result};
use crate::util::bit_util::{self, BitReader};
use crate::value_decoder::{DecodeBuffer, ValueDecoder};

mod byte_stream_split_decoder;

// ----------------------------------------------------------------------
// Decoders

/// A Parquet decoder for the data type `T`.
pub trait Decoder<T: ValueDecoder>: Send + Debug {
    /// Sets the data to decode to be `data`, which should contain `num_values` of values
    /// to decode.
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()>;

    /// Consumes values from this decoder and write the results to `buffer`.
    /// This will try to fill up `buffer` starting at `offset`.
    ///
    /// Returns the actual number of values decoded, which should be equal to
    /// `buffer.len() - offset` unless the remaining number of values is less
    /// than `buffer.len() - offset`.
    fn read(&mut self, offset: usize, buffer: &mut T::DecodeBuffer) -> Result<usize>;

    /// Consume values from this decoder and write the results to `buffer`, leaving
    /// "spaces" for null values.
    ///
    /// `null_count` is the number of nulls we expect to see in `buffer`, after reading.
    /// `valid_bits` stores the valid bit for each value in the buffer. It should contain
    ///   at least number of bits that equal to `buffer.len()`.
    ///
    /// Returns the actual number of values decoded.
    ///
    /// # Panics
    ///
    /// Panics if `null_count` is greater than `buffer.len()`.
    fn read_spaced(
        &mut self,
        offset: usize,
        buffer: &mut T::DecodeBuffer,
        null_count: usize,
        valid_bits: &[u8],
    ) -> Result<usize> {
        assert!(buffer.len() >= null_count);

        // TODO: check validity of the input arguments?
        if null_count == 0 {
            return self.read(offset, buffer);
        }

        let num_values = buffer.len();
        let values_to_read = num_values - null_count;
        let values_read = self.read(offset, buffer)?;
        if values_read != values_to_read {
            return Err(general_err!(
                "Number of values read: {}, doesn't match expected: {}",
                values_read,
                values_to_read
            ));
        }
        let mut values_to_move = values_read;
        for i in (0..num_values).rev() {
            if bit_util::get_bit(valid_bits, i) {
                values_to_move -= 1;
                buffer.swap(i, values_to_move);
            }
        }

        Ok(num_values)
    }

    /// Returns the number of values left in this decoder stream.
    fn values_left(&self) -> usize;

    /// Returns the encoding for this decoder.
    fn encoding(&self) -> Encoding;

    /// Skip the specified number of values in this decoder stream.
    fn skip(&mut self, num_values: usize) -> Result<usize>;
}

// ----------------------------------------------------------------------
// PLAIN Decoding

#[derive(Debug, Default)]
pub struct PlainDecoderState {
    // The remaining number of values in the byte array
    pub(crate) num_values: usize,

    // The current starting index in the byte array. Not used when `T` is bool.
    pub(crate) start: usize,

    // The length for the type `T`. Only used when `T` is `FixedLenByteArrayType`
    pub(crate) type_length: i32,

    // The byte array to decode from. Not set if `T` is bool.
    pub(crate) data: Option<Bytes>,

    // Read `data` bit by bit. Only set if `T` is bool.
    pub(crate) bit_reader: Option<BitReader>,
}

/// Plain decoding that supports all types.
/// Values are encoded back to back. For native types, data is encoded as little endian.
/// Floating point types are encoded in IEEE.
/// See [`PlainEncoder`](crate::encoding::PlainEncoder) for more information.
#[derive(Debug)]
pub struct PlainDecoder<T: ValueDecoder> {
    /// The binary details needed for decoding
    state: PlainDecoderState,
    _phantom: PhantomData<T>,
}

impl<T: ValueDecoder> PlainDecoder<T> {
    /// Creates new plain decoder.
    pub fn new(type_length: i32) -> Self {
        PlainDecoder {
            state: PlainDecoderState {
                type_length,
                num_values: 0,
                start: 0,
                data: None,
                bit_reader: None,
            },
            _phantom: PhantomData,
        }
    }
}

impl<T: ValueDecoder> Decoder<T> for PlainDecoder<T> {
    #[inline]
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        T::set_data2(&mut self.state, data, num_values);
        Ok(())
    }

    #[inline]
    fn values_left(&self) -> usize {
        self.state.num_values
    }

    #[inline]
    fn encoding(&self) -> Encoding {
        Encoding::PLAIN
    }

    #[inline]
    fn read(&mut self, offset: usize, buffer: &mut T::DecodeBuffer) -> Result<usize> {
        T::decode2(offset, buffer, &mut self.state)
    }

    #[inline]
    fn skip(&mut self, num_values: usize) -> Result<usize> {
        T::skip2(&mut self.state, num_values)
    }
}

// ----------------------------------------------------------------------
// RLE_DICTIONARY/PLAIN_DICTIONARY Decoding

/// Dictionary decoder.
/// The dictionary encoding builds a dictionary of values encountered in a given column.
/// The dictionary is be stored in a dictionary page per column chunk.
/// See [`DictEncoder`](crate::encoding::DictEncoder) for more information.
#[derive(Debug)]
pub struct DictDecoder<T: ValueDecoder> {
    // The dictionary, which maps ids to the values
    dictionary: T::DecodeBuffer,

    // Whether `dictionary` has been initialized
    has_dictionary: bool,

    // The decoder for the value ids
    rle_decoder: Option<RleDecoder>,

    // Number of values left in the data stream
    num_values: usize,
}

impl<T: ValueDecoder> Default for DictDecoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ValueDecoder> DictDecoder<T> {
    /// Creates new dictionary decoder.
    pub fn new() -> Self {
        Self {
            dictionary: T::DecodeBuffer::default(),
            has_dictionary: false,
            rle_decoder: None,
            num_values: 0,
        }
    }

    /// Decodes and sets values for dictionary using `decoder` decoder.
    pub fn set_dict(&mut self, mut decoder: Box<dyn Decoder<T>>) -> Result<()> {
        let num_values = decoder.values_left();
        self.dictionary.grow(num_values);
        let _ = decoder.read(0, &mut self.dictionary)?;
        self.has_dictionary = true;
        Ok(())
    }
}

impl<T: ValueDecoder> Decoder<T> for DictDecoder<T> {
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        // First byte in `data` is bit width
        let bit_width = data.as_ref()[0];
        let mut rle_decoder = RleDecoder::new(bit_width);
        rle_decoder.set_data(data.slice(1..));
        self.num_values = num_values;
        self.rle_decoder = Some(rle_decoder);
        Ok(())
    }

    fn read(&mut self, offset: usize, buffer: &mut T::DecodeBuffer) -> Result<usize> {
        assert!(self.rle_decoder.is_some());
        assert!(self.has_dictionary, "Must call set_dict() first!");

        let rle = self.rle_decoder.as_mut().unwrap();
        let num_values = cmp::min(buffer.len() - offset, self.num_values);
        rle.get_batch_with_dict(&self.dictionary, offset, buffer, num_values)
    }

    /// Number of values left in this decoder stream
    fn values_left(&self) -> usize {
        self.num_values
    }

    fn encoding(&self) -> Encoding {
        Encoding::RLE_DICTIONARY
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        assert!(self.rle_decoder.is_some());
        assert!(self.has_dictionary, "Must call set_dict() first!");

        let rle = self.rle_decoder.as_mut().unwrap();
        let num_values = cmp::min(num_values, self.num_values);
        rle.skip(num_values)
    }
}

// ----------------------------------------------------------------------
// RLE Decoding

/// RLE/Bit-Packing hybrid decoding for values.
/// Currently is used only for data pages v2 and supports boolean types.
/// See [`RleValueEncoder`](crate::encoding::RleValueEncoder) for more information.
#[derive(Debug)]
pub struct RleValueDecoder<T: ValueDecoder> {
    values_left: usize,
    decoder: RleDecoder,
    _phantom: PhantomData<T>,
}

impl<T> Default for RleValueDecoder<T>
where
    T: ValueDecoder,
    T::ValueType: Copy,
    T::DecodeBuffer: DerefMut<Target = [<T as ValueDecoder>::ValueType]>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> RleValueDecoder<T>
where
    T: ValueDecoder,
    T::ValueType: Copy,
    T::DecodeBuffer: DerefMut<Target = [<T as ValueDecoder>::ValueType]>,
{
    pub fn new() -> Self {
        Self {
            values_left: 0,
            decoder: RleDecoder::new(1),
            _phantom: PhantomData,
        }
    }
}

impl<T> Decoder<T> for RleValueDecoder<T>
where
    T: ValueDecoder,
    T::ValueType: Copy,
    T::DecodeBuffer: DerefMut<Target = [<T as ValueDecoder>::ValueType]>,
{
    #[inline]
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        // Only support RLE value reader for boolean values with bit width of 1.
        ensure_phys_ty!(Type::BOOLEAN, "RleValueDecoder only supports BoolType");

        // We still need to remove prefix of i32 from the stream.
        const I32_SIZE: usize = mem::size_of::<i32>();
        let data_size = bit_util::read_num_bytes::<i32>(I32_SIZE, data.as_ref()) as usize;
        self.decoder = RleDecoder::new(1);
        self.decoder
            .set_data(data.slice(I32_SIZE..I32_SIZE + data_size));
        self.values_left = num_values;
        Ok(())
    }

    #[inline]
    fn values_left(&self) -> usize {
        self.values_left
    }

    #[inline]
    fn encoding(&self) -> Encoding {
        Encoding::RLE
    }

    #[inline]
    fn read(&mut self, offset: usize, buffer: &mut T::DecodeBuffer) -> Result<usize> {
        // TODO: ADD TEST FOR THIS
        let num_values = cmp::min(buffer.len() - offset, self.values_left);

        let buf = &mut buffer[offset..num_values + offset];

        let values_read = self.decoder.get_batch(buf)?;
        self.values_left -= values_read;
        Ok(values_read)
    }

    #[inline]
    fn skip(&mut self, num_values: usize) -> Result<usize> {
        let num_values = cmp::min(num_values, self.values_left);
        let values_skipped = self.decoder.skip(num_values)?;
        self.values_left -= values_skipped;
        Ok(values_skipped)
    }
}

// ----------------------------------------------------------------------
// DELTA_BINARY_PACKED Decoding

/// Delta binary packed decoder.
/// Supports INT32 and INT64 types.
/// See [`DeltaBitPackEncoder`](crate::encoding::DeltaBitPackEncoder) for more
/// information.
#[derive(Debug)]
pub struct DeltaBitPackDecoder<T: ValueDecoder> {
    bit_reader: BitReader,
    initialized: bool,

    // Header info
    /// The number of values in each block
    block_size: usize,
    /// The number of values that remain to be read in the current page
    values_left: usize,
    /// The number of mini-blocks in each block
    mini_blocks_per_block: usize,
    /// The number of values in each mini block
    values_per_mini_block: usize,

    // Per block info
    /// The minimum delta in the block
    min_delta: T::ValueType,
    /// The byte offset of the end of the current block
    block_end_offset: usize,
    /// The index on the current mini block
    mini_block_idx: usize,
    /// The bit widths of each mini block in the current block
    mini_block_bit_widths: Vec<u8>,
    /// The number of values remaining in the current mini block
    mini_block_remaining: usize,

    /// The first value from the block header if not consumed
    first_value: Option<T::ValueType>,
    /// The last value to compute offsets from
    last_value: T::ValueType,
}

impl<T: ValueDecoder> Default for DeltaBitPackDecoder<T>
where
    T::ValueType: Default + FromPrimitive + WrappingAdd + Copy,
    T::DecodeBuffer: DerefMut<Target = [T::ValueType]>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ValueDecoder> DeltaBitPackDecoder<T>
where
    T::ValueType: Default + FromPrimitive + WrappingAdd + Copy,
    T::DecodeBuffer: DerefMut<Target = [T::ValueType]>,
{
    /// Creates new delta bit packed decoder.
    pub fn new() -> Self {
        Self {
            bit_reader: BitReader::from(vec![]),
            initialized: false,
            block_size: 0,
            values_left: 0,
            mini_blocks_per_block: 0,
            values_per_mini_block: 0,
            min_delta: Default::default(),
            mini_block_idx: 0,
            mini_block_bit_widths: vec![],
            mini_block_remaining: 0,
            block_end_offset: 0,
            first_value: None,
            last_value: Default::default(),
        }
    }

    /// Returns the current offset
    pub fn get_offset(&self) -> usize {
        assert!(self.initialized, "Bit reader is not initialized");
        match self.values_left {
            // If we've exhausted this page report the end of the current block
            // as we may not have consumed the trailing padding
            //
            // The max is necessary to handle pages which don't contain more than
            // one value and therefore have no blocks, but still contain a page header
            0 => self.bit_reader.get_byte_offset().max(self.block_end_offset),
            _ => self.bit_reader.get_byte_offset(),
        }
    }

    /// Initializes the next block and the first mini block within it
    #[inline]
    fn next_block(&mut self) -> Result<()> {
        let min_delta = self
            .bit_reader
            .get_zigzag_vlq_int()
            .ok_or_else(|| eof_err!("Not enough data to decode 'min_delta'"))?;

        self.min_delta = T::ValueType::from_i64(min_delta)
            .ok_or_else(|| general_err!("'min_delta' too large"))?;

        self.mini_block_bit_widths.clear();
        self.bit_reader
            .get_aligned_bytes(&mut self.mini_block_bit_widths, self.mini_blocks_per_block);

        let mut offset = self.bit_reader.get_byte_offset();
        let mut remaining = self.values_left;

        // Compute the end offset of the current block
        for b in &mut self.mini_block_bit_widths {
            if remaining == 0 {
                // Specification requires handling arbitrary bit widths
                // for trailing mini blocks
                *b = 0;
            }
            remaining = remaining.saturating_sub(self.values_per_mini_block);
            offset += *b as usize * self.values_per_mini_block / 8;
        }
        self.block_end_offset = offset;

        if self.mini_block_bit_widths.len() != self.mini_blocks_per_block {
            return Err(eof_err!("insufficient mini block bit widths"));
        }

        self.mini_block_remaining = self.values_per_mini_block;
        self.mini_block_idx = 0;

        Ok(())
    }

    /// Initializes the next mini block
    #[inline]
    fn next_mini_block(&mut self) -> Result<()> {
        if self.mini_block_idx + 1 < self.mini_block_bit_widths.len() {
            self.mini_block_idx += 1;
            self.mini_block_remaining = self.values_per_mini_block;
            Ok(())
        } else {
            self.next_block()
        }
    }
}

impl<T: ValueDecoder> Decoder<T> for DeltaBitPackDecoder<T>
where
    T::ValueType: Default + FromPrimitive + WrappingAdd + Copy,
    T::DecodeBuffer: DerefMut<Target = [T::ValueType]>,
{
    // # of total values is derived from encoding
    #[inline]
    fn set_data(&mut self, data: Bytes, _index: usize) -> Result<()> {
        self.bit_reader = BitReader::new(data);
        self.initialized = true;

        // Read header information
        self.block_size = self
            .bit_reader
            .get_vlq_int()
            .ok_or_else(|| eof_err!("Not enough data to decode 'block_size'"))?
            .try_into()
            .map_err(|_| general_err!("invalid 'block_size'"))?;

        self.mini_blocks_per_block = self
            .bit_reader
            .get_vlq_int()
            .ok_or_else(|| eof_err!("Not enough data to decode 'mini_blocks_per_block'"))?
            .try_into()
            .map_err(|_| general_err!("invalid 'mini_blocks_per_block'"))?;

        self.values_left = self
            .bit_reader
            .get_vlq_int()
            .ok_or_else(|| eof_err!("Not enough data to decode 'values_left'"))?
            .try_into()
            .map_err(|_| general_err!("invalid 'values_left'"))?;

        let first_value = self
            .bit_reader
            .get_zigzag_vlq_int()
            .ok_or_else(|| eof_err!("Not enough data to decode 'first_value'"))?;

        self.first_value = Some(
            T::ValueType::from_i64(first_value)
                .ok_or_else(|| general_err!("first value too large"))?,
        );

        if self.block_size % 128 != 0 {
            return Err(general_err!(
                "'block_size' must be a multiple of 128, got {}",
                self.block_size
            ));
        }

        if self.block_size % self.mini_blocks_per_block != 0 {
            return Err(general_err!(
                "'block_size' must be a multiple of 'mini_blocks_per_block' got {} and {}",
                self.block_size,
                self.mini_blocks_per_block
            ));
        }

        // Reset decoding state
        self.mini_block_idx = 0;
        self.values_per_mini_block = self.block_size / self.mini_blocks_per_block;
        self.mini_block_remaining = 0;
        self.mini_block_bit_widths.clear();

        if self.values_per_mini_block % 32 != 0 {
            return Err(general_err!(
                "'values_per_mini_block' must be a multiple of 32 got {}",
                self.values_per_mini_block
            ));
        }

        Ok(())
    }

    fn read(&mut self, offset: usize, buffer: &mut T::DecodeBuffer) -> Result<usize> {
        let buffer = &mut buffer[offset..];

        assert!(self.initialized, "Bit reader is not initialized");
        if buffer.is_empty() {
            return Ok(0);
        }

        let mut read = 0;
        let to_read = std::cmp::min(buffer.len(), self.values_left); // TODO: ADD TEST FOR THIS

        if let Some(value) = self.first_value.take() {
            self.last_value = value;
            buffer[0] = value;
            read += 1;
            self.values_left -= 1;
        }

        while read != to_read {
            if self.mini_block_remaining == 0 {
                self.next_mini_block()?;
            }

            let bit_width = self.mini_block_bit_widths[self.mini_block_idx] as usize;
            let batch_to_read = self.mini_block_remaining.min(to_read - read);

            let batch_read = self
                .bit_reader
                .get_batch(&mut buffer[read..read + batch_to_read], bit_width);

            if batch_read != batch_to_read {
                return Err(general_err!(
                    "Expected to read {} values from miniblock got {}",
                    batch_to_read,
                    batch_read
                ));
            }

            // At this point we have read the deltas to `buffer` we now need to offset
            // these to get back to the original values that were encoded
            for v in &mut buffer[read..read + batch_read] {
                // It is OK for deltas to contain "overflowed" values after encoding,
                // e.g. i64::MAX - i64::MIN, so we use `wrapping_add` to "overflow" again and
                // restore original value.
                *v = v
                    .wrapping_add(&self.min_delta)
                    .wrapping_add(&self.last_value);

                self.last_value = *v;
            }

            read += batch_read;
            self.mini_block_remaining -= batch_read;
            self.values_left -= batch_read;
        }

        Ok(to_read)
    }

    fn values_left(&self) -> usize {
        self.values_left
    }

    fn encoding(&self) -> Encoding {
        Encoding::DELTA_BINARY_PACKED
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        let mut skip = 0;
        let to_skip = num_values.min(self.values_left);
        if to_skip == 0 {
            return Ok(0);
        }

        // try to consume first value in header.
        if let Some(value) = self.first_value.take() {
            self.last_value = value;
            skip += 1;
            self.values_left -= 1;
        }

        let mini_block_batch_size = match T::ValueType::PHYSICAL_TYPE {
            Type::INT32 => 32,
            Type::INT64 => 64,
            _ => unreachable!(),
        };

        let mut skip_buffer = vec![T::ValueType::default(); mini_block_batch_size];
        while skip < to_skip {
            if self.mini_block_remaining == 0 {
                self.next_mini_block()?;
            }

            let bit_width = self.mini_block_bit_widths[self.mini_block_idx] as usize;
            let mini_block_to_skip = self.mini_block_remaining.min(to_skip - skip);
            let mini_block_should_skip = mini_block_to_skip;

            let skip_count = self
                .bit_reader
                .get_batch(&mut skip_buffer[0..mini_block_to_skip], bit_width);

            if skip_count != mini_block_to_skip {
                return Err(general_err!(
                    "Expected to skip {} values from mini block got {}.",
                    mini_block_batch_size,
                    skip_count
                ));
            }

            for v in &mut skip_buffer[0..skip_count] {
                *v = v
                    .wrapping_add(&self.min_delta)
                    .wrapping_add(&self.last_value);

                self.last_value = *v;
            }

            skip += mini_block_should_skip;
            self.mini_block_remaining -= mini_block_should_skip;
            self.values_left -= mini_block_should_skip;
        }

        Ok(to_skip)
    }
}

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY Decoding

/// Delta length byte array decoder.
/// Only applied to byte arrays to separate the length values and the data, the lengths
/// are encoded using DELTA_BINARY_PACKED encoding.
/// See [`DeltaLengthByteArrayEncoder`](crate::encoding::DeltaLengthByteArrayEncoder)
/// for more information.
#[derive(Debug)]
pub struct DeltaLengthByteArrayDecoder<T: ValueDecoder> {
    // Lengths for each byte array in `data`
    // TODO: add memory tracker to this
    lengths: Vec<i32>,

    // Current index into `lengths`
    current_idx: usize,

    // Concatenated byte array data
    data: Option<Bytes>,

    // Offset into `data`, always point to the beginning of next byte array.
    offset: usize,

    // Number of values left in this decoder stream
    num_values: usize,

    // Placeholder to allow `T` as generic parameter
    _phantom: PhantomData<T>,
}

impl<T: ValueDecoder> Default for DeltaLengthByteArrayDecoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ValueDecoder> DeltaLengthByteArrayDecoder<T> {
    /// Creates new delta length byte array decoder.
    pub fn new() -> Self {
        Self {
            lengths: vec![],
            current_idx: 0,
            data: None,
            offset: 0,
            num_values: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T> Decoder<T> for DeltaLengthByteArrayDecoder<T>
where
    T: ValueDecoder,
    T::DecodeBuffer: DecodeBuffer<Value = ByteArray>, // TODO: AsRef<[u8]>
{
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        match T::ValueType::PHYSICAL_TYPE {
            Type::BYTE_ARRAY => {
                let mut len_decoder = DeltaBitPackDecoder::<i32>::new();
                len_decoder.set_data(data.clone(), num_values)?;
                let num_lengths = len_decoder.values_left();
                self.lengths.resize(num_lengths, 0);
                len_decoder.read(0, &mut self.lengths)?;

                self.data = Some(data.slice(len_decoder.get_offset()..));
                self.offset = 0;
                self.current_idx = 0;
                self.num_values = num_lengths;
                Ok(())
            }
            _ => Err(general_err!(
                "DeltaLengthByteArrayDecoder only support ByteArrayType"
            )),
        }
    }

    fn read(&mut self, offset: usize, buffer: &mut T::DecodeBuffer) -> Result<usize> {
        match T::ValueType::PHYSICAL_TYPE {
            Type::BYTE_ARRAY => {
                assert!(self.data.is_some());

                let data = self.data.as_ref().unwrap();
                let num_values = cmp::min(buffer.len() - offset, self.num_values);

                for idx in offset..offset + num_values {
                    let len = self.lengths[self.current_idx] as usize;

                    buffer.put_value(idx, &data.slice(self.offset..self.offset + len).into());

                    self.offset += len;
                    self.current_idx += 1;
                }

                self.num_values -= num_values;

                Ok(num_values)
            }
            _ => Err(general_err!(
                "DeltaLengthByteArrayDecoder only support ByteArrayType"
            )),
        }
    }

    fn values_left(&self) -> usize {
        self.num_values
    }

    fn encoding(&self) -> Encoding {
        Encoding::DELTA_LENGTH_BYTE_ARRAY
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        match T::ValueType::PHYSICAL_TYPE {
            Type::BYTE_ARRAY => {
                let num_values = cmp::min(num_values, self.num_values);

                let next_offset: i32 = self.lengths
                    [self.current_idx..self.current_idx + num_values]
                    .iter()
                    .sum();

                self.current_idx += num_values;
                self.offset += next_offset as usize;

                self.num_values -= num_values;
                Ok(num_values)
            }
            other_type => Err(general_err!(
                "DeltaLengthByteArrayDecoder not support {}, only support byte array",
                other_type
            )),
        }
    }
}

// ----------------------------------------------------------------------
// DELTA_BYTE_ARRAY Decoding

/// Delta byte array decoder.
/// Prefix lengths are encoded using `DELTA_BINARY_PACKED` encoding, Suffixes are stored
/// using `DELTA_LENGTH_BYTE_ARRAY` encoding.
/// See [`DeltaByteArrayEncoder`](crate::encoding::DeltaByteArrayEncoder) for more
/// information.
#[derive(Debug)]
pub struct DeltaByteArrayDecoder<T: ValueDecoder> {
    // Prefix lengths for each byte array
    // TODO: add memory tracker to this
    prefix_lengths: Vec<i32>,

    // The current index into `prefix_lengths`,
    current_idx: usize,

    // Decoder for all suffixes, the # of which should be the same as
    // `prefix_lengths.len()`
    suffix_decoder: Option<DeltaLengthByteArrayDecoder<ByteArray>>,

    // The last byte array, used to derive the current prefix
    previous_value: Vec<u8>,

    // Number of values left
    num_values: usize,

    // Placeholder to allow `T` as generic parameter
    _phantom: PhantomData<T>,
}

impl<T: ValueDecoder> Default for DeltaByteArrayDecoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ValueDecoder> DeltaByteArrayDecoder<T> {
    /// Creates new delta byte array decoder.
    pub fn new() -> Self {
        Self {
            prefix_lengths: vec![],
            current_idx: 0,
            suffix_decoder: None,
            previous_value: vec![],
            num_values: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T> Decoder<T> for DeltaByteArrayDecoder<T>
where
    T: ValueDecoder,
{
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        match T::ValueType::PHYSICAL_TYPE {
            Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY => {
                let mut prefix_len_decoder = DeltaBitPackDecoder::<i32>::new();
                prefix_len_decoder.set_data(data.clone(), num_values)?;
                let num_prefixes = prefix_len_decoder.values_left();
                self.prefix_lengths.resize(num_prefixes, 0);
                prefix_len_decoder.read(0, &mut self.prefix_lengths)?;

                let mut suffix_decoder = DeltaLengthByteArrayDecoder::new();
                suffix_decoder
                    .set_data(data.slice(prefix_len_decoder.get_offset()..), num_values)?;
                self.suffix_decoder = Some(suffix_decoder);
                self.num_values = num_prefixes;
                self.current_idx = 0;
                self.previous_value.clear();
                Ok(())
            }
            _ => Err(general_err!(
                "DeltaByteArrayDecoder only supports ByteArrayType and FixedLenByteArrayType"
            )),
        }
    }

    fn read(&mut self, offset: usize, buffer: &mut T::DecodeBuffer) -> Result<usize> {
        match T::ValueType::PHYSICAL_TYPE {
            ty @ Type::BYTE_ARRAY | ty @ Type::FIXED_LEN_BYTE_ARRAY => {
                let num_values = cmp::min(buffer.len() - offset, self.num_values);
                let mut v = vec![ByteArray::new()];

                for idx in offset..offset + num_values {
                    // Process suffix
                    // TODO: this is awkward - maybe we should add a non-vectorized API?
                    let suffix_decoder = self
                        .suffix_decoder
                        .as_mut()
                        .expect("decoder not initialized");
                    suffix_decoder.read(0, &mut v)?;
                    let suffix = &v[0].data();

                    // Extract current prefix length, can be 0
                    let prefix_len = self.prefix_lengths[self.current_idx] as usize;

                    // Concatenate prefix with suffix
                    let mut result = Vec::with_capacity(prefix_len + suffix.len());
                    result.extend_from_slice(&self.previous_value[0..prefix_len]);
                    result.extend_from_slice(suffix);

                    let data = Bytes::from(result.clone());

                    unimplemented!("TODO");
                    // match ty {
                    //     Type::BYTE_ARRAY => {
                    //         buffer.put_value(idx, data.into());
                    //     }
                    //     Type::FIXED_LEN_BYTE_ARRAY => item
                    //         .as_mut_any()
                    //         .downcast_mut::<FixedLenByteArray>()
                    //         .unwrap()
                    //         .set_data(data),
                    //     _ => unreachable!(),
                    // };

                    self.previous_value = result;
                    self.current_idx += 1;
                }

                self.num_values -= num_values;
                Ok(num_values)
            }
            _ => Err(general_err!(
                "DeltaByteArrayDecoder only supports ByteArrayType and FixedLenByteArrayType"
            )),
        }
    }

    fn values_left(&self) -> usize {
        self.num_values
    }

    fn encoding(&self) -> Encoding {
        Encoding::DELTA_BYTE_ARRAY
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        let mut buffer = T::DecodeBuffer::with_len(num_values);
        self.read(0, &mut buffer)
    }
}

#[cfg(test)]
mod tests {
    use std::f32::consts::PI as PI_f32;
    use std::f64::consts::PI as PI_f64;
    use std::sync::Arc;

    use get_decoder::GetDecoder;

    use super::super::encoding::*;
    use super::get_decoder::get_decoder;
    use super::*;
    use crate::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, Type as SchemaType};
    use crate::util::test_common::rand_gen::RandGen;

    #[test]
    fn test_get_decoders() {
        // supported encodings
        create_and_check_decoder::<i32>(Encoding::PLAIN, None);
        create_and_check_decoder::<i32>(Encoding::DELTA_BINARY_PACKED, None);
        create_and_check_decoder::<ByteArray>(Encoding::DELTA_LENGTH_BYTE_ARRAY, None);
        create_and_check_decoder::<ByteArray>(Encoding::DELTA_BYTE_ARRAY, None);
        create_and_check_decoder::<bool>(Encoding::RLE, None);

        // error when initializing
        create_and_check_decoder::<i32>(
            Encoding::RLE_DICTIONARY,
            Some(general_err!(
                "Cannot initialize this encoding through this function"
            )),
        );
        create_and_check_decoder::<i32>(
            Encoding::PLAIN_DICTIONARY,
            Some(general_err!(
                "Cannot initialize this encoding through this function"
            )),
        );
        create_and_check_decoder::<i32>(
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
            Some(general_err!(
                "Encoding DELTA_LENGTH_BYTE_ARRAY is not supported for type"
            )),
        );
        create_and_check_decoder::<i32>(
            Encoding::DELTA_BYTE_ARRAY,
            Some(general_err!(
                "Encoding DELTA_BYTE_ARRAY is not supported for type"
            )),
        );

        // unsupported
        #[allow(deprecated)]
        create_and_check_decoder::<i32>(
            Encoding::BIT_PACKED,
            Some(nyi_err!("Encoding BIT_PACKED is not supported")),
        );
    }

    #[test]
    fn test_plain_decode_int32() {
        let data = [42, 18, 52];
        let data_bytes = Int32Type::to_byte_array(&data[..]);
        let mut buffer = vec![0; 3];
        test_plain_decode::<i32>(Bytes::from(data_bytes), 3, -1, &mut buffer, &data[..]);
    }

    #[test]
    fn test_plain_skip_int32() {
        let data = [42, 18, 52];
        let data_bytes = Int32Type::to_byte_array(&data[..]);
        test_plain_skip::<i32>(Bytes::from(data_bytes), 3, 1, -1, &data[1..]);
    }

    #[test]
    fn test_plain_skip_all_int32() {
        let data = [42, 18, 52];
        let data_bytes = Int32Type::to_byte_array(&data[..]);
        test_plain_skip::<i32>(Bytes::from(data_bytes), 3, 5, -1, &[]);
    }

    #[test]
    fn test_plain_decode_int32_spaced() {
        let data = [42, 18, 52];
        let expected_data = [0, 42, 0, 18, 0, 0, 52, 0];
        let data_bytes = Int32Type::to_byte_array(&data[..]);
        let mut buffer = vec![0; 8];
        let num_nulls = 5;
        let valid_bits = [0b01001010];
        test_plain_decode_spaced::<i32>(
            Bytes::from(data_bytes),
            3,
            -1,
            &mut buffer,
            num_nulls,
            &valid_bits,
            &expected_data[..],
        );
    }

    #[test]
    fn test_plain_decode_int64() {
        let data = [42, 18, 52];
        let data_bytes = Int64Type::to_byte_array(&data[..]);
        let mut buffer = vec![0; 3];
        test_plain_decode::<i64>(Bytes::from(data_bytes), 3, -1, &mut buffer, &data[..]);
    }

    #[test]
    fn test_plain_skip_int64() {
        let data = [42, 18, 52];
        let data_bytes = Int64Type::to_byte_array(&data[..]);
        test_plain_skip::<i64>(Bytes::from(data_bytes), 3, 2, -1, &data[2..]);
    }

    #[test]
    fn test_plain_skip_all_int64() {
        let data = [42, 18, 52];
        let data_bytes = Int64Type::to_byte_array(&data[..]);
        test_plain_skip::<i64>(Bytes::from(data_bytes), 3, 3, -1, &[]);
    }

    #[test]
    fn test_plain_decode_float() {
        let data = [PI_f32, 2.414, 12.51];
        let data_bytes = FloatType::to_byte_array(&data[..]);
        let mut buffer = vec![0.0; 3];
        test_plain_decode::<f32>(Bytes::from(data_bytes), 3, -1, &mut buffer, &data[..]);
    }

    #[test]
    fn test_plain_skip_float() {
        let data = [PI_f32, 2.414, 12.51];
        let data_bytes = FloatType::to_byte_array(&data[..]);
        test_plain_skip::<f32>(Bytes::from(data_bytes), 3, 1, -1, &data[1..]);
    }

    #[test]
    fn test_plain_skip_all_float() {
        let data = [PI_f32, 2.414, 12.51];
        let data_bytes = FloatType::to_byte_array(&data[..]);
        test_plain_skip::<f32>(Bytes::from(data_bytes), 3, 4, -1, &[]);
    }

    #[test]
    fn test_plain_skip_double() {
        let data = [PI_f64, 2.414f64, 12.51f64];
        let data_bytes = DoubleType::to_byte_array(&data[..]);
        test_plain_skip::<f64>(Bytes::from(data_bytes), 3, 1, -1, &data[1..]);
    }

    #[test]
    fn test_plain_skip_all_double() {
        let data = [PI_f64, 2.414f64, 12.51f64];
        let data_bytes = DoubleType::to_byte_array(&data[..]);
        test_plain_skip::<f64>(Bytes::from(data_bytes), 3, 5, -1, &[]);
    }

    #[test]
    fn test_plain_decode_double() {
        let data = [PI_f64, 2.414f64, 12.51f64];
        let data_bytes = DoubleType::to_byte_array(&data[..]);
        let mut buffer = vec![0.0f64; 3];
        test_plain_decode::<f64>(Bytes::from(data_bytes), 3, -1, &mut buffer, &data[..]);
    }

    #[test]
    fn test_plain_decode_int96() {
        let mut data = [Int96::new(); 4];
        data[0].set_data(11, 22, 33);
        data[1].set_data(44, 55, 66);
        data[2].set_data(10, 20, 30);
        data[3].set_data(40, 50, 60);
        let data_bytes = Int96Type::to_byte_array(&data[..]);
        let mut buffer = vec![Int96::new(); 4];
        test_plain_decode::<Int96>(Bytes::from(data_bytes), 4, -1, &mut buffer, &data[..]);
    }

    #[test]
    fn test_plain_skip_int96() {
        let mut data = [Int96::new(); 4];
        data[0].set_data(11, 22, 33);
        data[1].set_data(44, 55, 66);
        data[2].set_data(10, 20, 30);
        data[3].set_data(40, 50, 60);
        let data_bytes = Int96Type::to_byte_array(&data[..]);
        test_plain_skip::<Int96>(Bytes::from(data_bytes), 4, 2, -1, &data[2..]);
    }

    #[test]
    fn test_plain_skip_all_int96() {
        let mut data = [Int96::new(); 4];
        data[0].set_data(11, 22, 33);
        data[1].set_data(44, 55, 66);
        data[2].set_data(10, 20, 30);
        data[3].set_data(40, 50, 60);
        let data_bytes = Int96Type::to_byte_array(&data[..]);
        test_plain_skip::<Int96>(Bytes::from(data_bytes), 4, 8, -1, &[]);
    }

    #[test]
    fn test_plain_decode_bool() {
        let data = [
            false, true, false, false, true, false, true, true, false, true,
        ];
        let data_bytes = BoolType::to_byte_array(&data[..]);
        let mut buffer = vec![false; 10];
        test_plain_decode::<bool>(Bytes::from(data_bytes), 10, -1, &mut buffer, &data[..]);
    }

    #[test]
    fn test_plain_skip_bool() {
        let data = [
            false, true, false, false, true, false, true, true, false, true,
        ];
        let data_bytes = BoolType::to_byte_array(&data[..]);
        test_plain_skip::<bool>(Bytes::from(data_bytes), 10, 5, -1, &data[5..]);
    }

    #[test]
    fn test_plain_skip_all_bool() {
        let data = [
            false, true, false, false, true, false, true, true, false, true,
        ];
        let data_bytes = BoolType::to_byte_array(&data[..]);
        test_plain_skip::<bool>(Bytes::from(data_bytes), 10, 20, -1, &[]);
    }

    #[test]
    fn test_plain_decode_byte_array() {
        let mut data = vec![ByteArray::new(); 2];
        data[0].set_data(Bytes::from(String::from("hello")));
        data[1].set_data(Bytes::from(String::from("parquet")));
        let data_bytes = ByteArrayType::to_byte_array(&data[..]);
        let mut buffer = vec![ByteArray::new(); 2];
        test_plain_decode::<ByteArray>(Bytes::from(data_bytes), 2, -1, &mut buffer, &data[..]);
    }

    #[test]
    fn test_plain_skip_byte_array() {
        let mut data = vec![ByteArray::new(); 2];
        data[0].set_data(Bytes::from(String::from("hello")));
        data[1].set_data(Bytes::from(String::from("parquet")));
        let data_bytes = ByteArrayType::to_byte_array(&data[..]);
        test_plain_skip::<ByteArray>(Bytes::from(data_bytes), 2, 1, -1, &data[1..]);
    }

    #[test]
    fn test_plain_skip_all_byte_array() {
        let mut data = vec![ByteArray::new(); 2];
        data[0].set_data(Bytes::from(String::from("hello")));
        data[1].set_data(Bytes::from(String::from("parquet")));
        let data_bytes = ByteArrayType::to_byte_array(&data[..]);
        test_plain_skip::<ByteArray>(Bytes::from(data_bytes), 2, 2, -1, &[]);
    }

    #[test]
    fn test_plain_decode_fixed_len_byte_array() {
        let mut data = vec![FixedLenByteArray::default(); 3];
        data[0].set_data(Bytes::from(String::from("bird")));
        data[1].set_data(Bytes::from(String::from("come")));
        data[2].set_data(Bytes::from(String::from("flow")));
        let data_bytes = FixedLenByteArrayType::to_byte_array(&data[..]);
        let mut buffer = vec![FixedLenByteArray::default(); 3];
        test_plain_decode::<FixedLenByteArray>(
            Bytes::from(data_bytes),
            3,
            4,
            &mut buffer,
            &data[..],
        );
    }

    #[test]
    fn test_plain_skip_fixed_len_byte_array() {
        let mut data = vec![FixedLenByteArray::default(); 3];
        data[0].set_data(Bytes::from(String::from("bird")));
        data[1].set_data(Bytes::from(String::from("come")));
        data[2].set_data(Bytes::from(String::from("flow")));
        let data_bytes = FixedLenByteArrayType::to_byte_array(&data[..]);
        test_plain_skip::<FixedLenByteArray>(Bytes::from(data_bytes), 3, 1, 4, &data[1..]);
    }

    #[test]
    fn test_plain_skip_all_fixed_len_byte_array() {
        let mut data = vec![FixedLenByteArray::default(); 3];
        data[0].set_data(Bytes::from(String::from("bird")));
        data[1].set_data(Bytes::from(String::from("come")));
        data[2].set_data(Bytes::from(String::from("flow")));
        let data_bytes = FixedLenByteArrayType::to_byte_array(&data[..]);
        test_plain_skip::<FixedLenByteArray>(Bytes::from(data_bytes), 3, 6, 4, &[]);
    }

    fn test_plain_decode<T>(
        data: Bytes,
        num_values: usize,
        type_length: i32,
        buffer: &mut Vec<T>,
        expected: &[T],
    ) where
        T: ValueDecoder<DecodeBuffer = Vec<T>> + ParquetValueType,
    {
        let mut decoder: PlainDecoder<T> = PlainDecoder::new(type_length);
        let result = decoder.set_data(data, num_values);
        assert!(result.is_ok());
        let result = decoder.read(0, buffer);
        assert!(result.is_ok());
        assert_eq!(decoder.values_left(), 0);
        assert_eq!(buffer, expected);
    }

    fn test_plain_skip<T>(
        data: Bytes,
        num_values: usize,
        skip: usize,
        type_length: i32,
        expected: &[T],
    ) where
        T: ValueDecoder<DecodeBuffer = Vec<T>> + ParquetValueType,
    {
        let mut decoder: PlainDecoder<T> = PlainDecoder::new(type_length);
        let result = decoder.set_data(data, num_values);
        assert!(result.is_ok());
        let skipped = decoder.skip(skip).expect("skipping values");

        if skip >= num_values {
            assert_eq!(skipped, num_values);

            let mut buffer = vec![T::default(); 1];
            let remaining = decoder
                .read(0, &mut buffer)
                .expect("getting remaining values");
            assert_eq!(remaining, 0);
        } else {
            assert_eq!(skipped, skip);
            let mut buffer = vec![T::default(); num_values - skip];
            let remaining = decoder
                .read(0, &mut buffer)
                .expect("getting remaining values");
            assert_eq!(remaining, num_values - skip);
            assert_eq!(decoder.values_left(), 0);
            assert_eq!(buffer, expected);
        }
    }

    fn test_plain_decode_spaced<T>(
        data: Bytes,
        num_values: usize,
        type_length: i32,
        buffer: &mut Vec<T>,
        num_nulls: usize,
        valid_bits: &[u8],
        expected: &[T],
    ) where
        T: ValueDecoder<DecodeBuffer = Vec<T>> + ParquetValueType,
    {
        let mut decoder: PlainDecoder<T> = PlainDecoder::new(type_length);
        let result = decoder.set_data(data, num_values);
        assert!(result.is_ok());
        let result = decoder.read_spaced(0, buffer, num_nulls, valid_bits);
        assert!(result.is_ok());
        assert_eq!(num_values + num_nulls, result.unwrap());
        assert_eq!(decoder.values_left(), 0);
        assert_eq!(buffer, expected);
    }

    #[test]
    #[should_panic(expected = "RleValueEncoder only supports BoolType")]
    fn test_rle_value_encode_int32_not_supported() {
        let mut encoder = RleValueEncoder::<Int32Type>::new();
        encoder.put(&[1, 2, 3, 4]).unwrap();
    }

    #[test]
    #[should_panic(expected = "RleValueDecoder only supports BoolType")]
    fn test_rle_value_decode_int32_not_supported() {
        let mut decoder = RleValueDecoder::<i32>::new();
        decoder.set_data(Bytes::from(vec![5, 0, 0, 0]), 1).unwrap();
    }

    #[test]
    fn test_rle_value_decode_bool_decode() {
        // Test multiple 'put' calls on the same encoder
        let data = vec![
            BoolType::gen_vec(-1, 256),
            BoolType::gen_vec(-1, 257),
            BoolType::gen_vec(-1, 126),
        ];
        test_rle_value_decode::<BoolType>(data);
    }

    #[test]
    #[should_panic(expected = "Bit reader is not initialized")]
    fn test_delta_bit_packed_not_initialized_offset() {
        // Fail if set_data() is not called before get_offset()
        let decoder = DeltaBitPackDecoder::<i32>::new();
        decoder.get_offset();
    }

    #[test]
    #[should_panic(expected = "Bit reader is not initialized")]
    fn test_delta_bit_packed_not_initialized_get() {
        // Fail if set_data() is not called before get()
        let mut decoder = DeltaBitPackDecoder::<i32>::new();
        let mut buffer = vec![];
        decoder.read(0, &mut buffer).unwrap();
    }

    #[test]
    fn test_delta_bit_packed_int32_empty() {
        let data = vec![vec![0; 0]];
        test_delta_bit_packed_decode::<Int32Type>(data);
    }

    #[test]
    fn test_delta_bit_packed_int32_repeat() {
        let block_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5,
            6, 7, 8,
        ];
        test_delta_bit_packed_decode::<Int32Type>(vec![block_data]);
    }

    #[test]
    fn test_skip_delta_bit_packed_int32_repeat() {
        let block_data = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5,
            6, 7, 8,
        ];
        test_skip::<Int32Type, i32>(block_data.clone(), Encoding::DELTA_BINARY_PACKED, 10);
        test_skip::<Int32Type, i32>(block_data, Encoding::DELTA_BINARY_PACKED, 100);
    }

    #[test]
    fn test_delta_bit_packed_int32_uneven() {
        let block_data = vec![1, -2, 3, -4, 5, 6, 7, 8, 9, 10, 11];
        test_delta_bit_packed_decode::<Int32Type>(vec![block_data]);
    }

    #[test]
    fn test_skip_delta_bit_packed_int32_uneven() {
        let block_data = vec![1, -2, 3, -4, 5, 6, 7, 8, 9, 10, 11];
        test_skip::<Int32Type, i32>(block_data.clone(), Encoding::DELTA_BINARY_PACKED, 5);
        test_skip::<Int32Type, i32>(block_data, Encoding::DELTA_BINARY_PACKED, 100);
    }

    #[test]
    fn test_delta_bit_packed_int32_same_values() {
        let block_data = vec![
            127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127,
        ];
        test_delta_bit_packed_decode::<Int32Type>(vec![block_data]);

        let block_data = vec![
            -127, -127, -127, -127, -127, -127, -127, -127, -127, -127, -127, -127, -127, -127,
            -127, -127,
        ];
        test_delta_bit_packed_decode::<Int32Type>(vec![block_data]);
    }

    #[test]
    fn test_skip_delta_bit_packed_int32_same_values() {
        let block_data = vec![
            127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127, 127,
        ];
        test_skip::<Int32Type, i32>(block_data.clone(), Encoding::DELTA_BINARY_PACKED, 5);
        test_skip::<Int32Type, i32>(block_data, Encoding::DELTA_BINARY_PACKED, 100);

        let block_data = vec![
            -127, -127, -127, -127, -127, -127, -127, -127, -127, -127, -127, -127, -127, -127,
            -127, -127,
        ];
        test_skip::<Int32Type, i32>(block_data.clone(), Encoding::DELTA_BINARY_PACKED, 5);
        test_skip::<Int32Type, i32>(block_data, Encoding::DELTA_BINARY_PACKED, 100);
    }

    #[test]
    fn test_delta_bit_packed_int32_min_max() {
        let block_data = vec![
            i32::MIN,
            i32::MIN,
            i32::MIN,
            i32::MAX,
            i32::MIN,
            i32::MAX,
            i32::MIN,
            i32::MAX,
        ];
        test_delta_bit_packed_decode::<Int32Type>(vec![block_data]);
    }

    #[test]
    fn test_skip_delta_bit_packed_int32_min_max() {
        let block_data = vec![
            i32::MIN,
            i32::MIN,
            i32::MIN,
            i32::MAX,
            i32::MIN,
            i32::MAX,
            i32::MIN,
            i32::MAX,
        ];
        test_skip::<Int32Type, i32>(block_data.clone(), Encoding::DELTA_BINARY_PACKED, 5);
        test_skip::<Int32Type, i32>(block_data, Encoding::DELTA_BINARY_PACKED, 100);
    }

    #[test]
    fn test_delta_bit_packed_int32_multiple_blocks() {
        // Test multiple 'put' calls on the same encoder
        let data = vec![
            Int32Type::gen_vec(-1, 64),
            Int32Type::gen_vec(-1, 128),
            Int32Type::gen_vec(-1, 64),
        ];
        test_delta_bit_packed_decode::<Int32Type>(data);
    }

    #[test]
    fn test_delta_bit_packed_int32_data_across_blocks() {
        // Test multiple 'put' calls on the same encoder
        let data = vec![Int32Type::gen_vec(-1, 256), Int32Type::gen_vec(-1, 257)];
        test_delta_bit_packed_decode::<Int32Type>(data);
    }

    #[test]
    fn test_delta_bit_packed_int32_with_empty_blocks() {
        let data = vec![
            Int32Type::gen_vec(-1, 128),
            vec![0; 0],
            Int32Type::gen_vec(-1, 64),
        ];
        test_delta_bit_packed_decode::<Int32Type>(data);
    }

    #[test]
    fn test_delta_bit_packed_int64_empty() {
        let data = vec![vec![0; 0]];
        test_delta_bit_packed_decode::<Int64Type>(data);
    }

    #[test]
    fn test_delta_bit_packed_int64_min_max() {
        let block_data = vec![
            i64::min_value(),
            i64::max_value(),
            i64::min_value(),
            i64::max_value(),
            i64::min_value(),
            i64::max_value(),
            i64::min_value(),
            i64::max_value(),
        ];
        test_delta_bit_packed_decode::<Int64Type>(vec![block_data]);
    }

    #[test]
    fn test_delta_bit_packed_int64_multiple_blocks() {
        // Test multiple 'put' calls on the same encoder
        let data = vec![
            Int64Type::gen_vec(-1, 64),
            Int64Type::gen_vec(-1, 128),
            Int64Type::gen_vec(-1, 64),
        ];
        test_delta_bit_packed_decode::<Int64Type>(data);
    }

    #[test]
    fn test_delta_bit_packed_decoder_sample() {
        let data_bytes = vec![
            128, 1, 4, 3, 58, 28, 6, 0, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
        ];
        let mut decoder: DeltaBitPackDecoder<i32> = DeltaBitPackDecoder::new();
        decoder.set_data(data_bytes.into(), 3).unwrap();
        // check exact offsets, because when reading partial values we end up with
        // some data not being read from bit reader
        assert_eq!(decoder.get_offset(), 5);
        let mut result = vec![0, 0, 0];
        decoder.read(0, &mut result).unwrap();
        assert_eq!(decoder.get_offset(), 34);
        assert_eq!(result, vec![29, 43, 89]);
    }

    #[test]
    fn test_delta_bit_packed_padding() {
        // Page header
        let header = vec![
            // Page Header

            // Block Size - 256
            128,
            2,
            // Miniblocks in block,
            4,
            // Total value count - 419
            128 + 35,
            3,
            // First value - 7
            7,
        ];

        // Block Header
        let block1_header = vec![
            0, // Min delta
            0, 1, 0, 0, // Bit widths
        ];

        // Mini-block 1 - bit width 0 => 0 bytes
        // Mini-block 2 - bit width 1 => 8 bytes
        // Mini-block 3 - bit width 0 => 0 bytes
        // Mini-block 4 - bit width 0 => 0 bytes
        let block1 = vec![0xFF; 8];

        // Block Header
        let block2_header = vec![
            0, // Min delta
            0, 1, 2, 0xFF, // Bit widths, including non-zero padding
        ];

        // Mini-block 1 - bit width 0 => 0 bytes
        // Mini-block 2 - bit width 1 => 8 bytes
        // Mini-block 3 - bit width 2 => 16 bytes
        // Mini-block 4 - padding => no bytes
        let block2 = vec![0xFF; 24];

        let data: Vec<u8> = header
            .into_iter()
            .chain(block1_header)
            .chain(block1)
            .chain(block2_header)
            .chain(block2)
            .collect();

        let length = data.len();

        let ptr = Bytes::from(data);
        let mut reader = BitReader::new(ptr.clone());
        assert_eq!(reader.get_vlq_int().unwrap(), 256);
        assert_eq!(reader.get_vlq_int().unwrap(), 4);
        assert_eq!(reader.get_vlq_int().unwrap(), 419);
        assert_eq!(reader.get_vlq_int().unwrap(), 7);

        // Test output buffer larger than needed and not exact multiple of block size
        let mut output = vec![0_i32; 420];

        let mut decoder = DeltaBitPackDecoder::<i32>::new();
        decoder.set_data(ptr.clone(), 0).unwrap();
        assert_eq!(decoder.read(0, &mut output).unwrap(), 419);
        assert_eq!(decoder.get_offset(), length);

        // Test with truncated buffer
        decoder.set_data(ptr.slice(..12), 0).unwrap();
        let err = decoder.read(0, &mut output).unwrap_err().to_string();
        assert!(
            err.contains("Expected to read 64 values from miniblock got 8"),
            "{}",
            err
        );
    }

    #[test]
    fn test_delta_byte_array_same_arrays() {
        let data = vec![
            vec![ByteArray::from(vec![1, 2, 3, 4, 5, 6])],
            vec![
                ByteArray::from(vec![1, 2, 3, 4, 5, 6]),
                ByteArray::from(vec![1, 2, 3, 4, 5, 6]),
            ],
            vec![
                ByteArray::from(vec![1, 2, 3, 4, 5, 6]),
                ByteArray::from(vec![1, 2, 3, 4, 5, 6]),
            ],
        ];
        test_delta_byte_array_decode(data);
    }

    #[test]
    fn test_delta_byte_array_unique_arrays() {
        let data = vec![
            vec![ByteArray::from(vec![1])],
            vec![ByteArray::from(vec![2, 3]), ByteArray::from(vec![4, 5, 6])],
            vec![
                ByteArray::from(vec![7, 8]),
                ByteArray::from(vec![9, 0, 1, 2]),
            ],
        ];
        test_delta_byte_array_decode(data);
    }

    #[test]
    fn test_delta_byte_array_single_array() {
        let data = vec![vec![ByteArray::from(vec![1, 2, 3, 4, 5, 6])]];
        test_delta_byte_array_decode(data);
    }

    #[test]
    fn test_byte_stream_split_multiple_f32() {
        let data = vec![
            vec![
                f32::from_le_bytes([0xAA, 0xBB, 0xCC, 0xDD]),
                f32::from_le_bytes([0x00, 0x11, 0x22, 0x33]),
            ],
            vec![f32::from_le_bytes([0xA3, 0xB4, 0xC5, 0xD6])],
        ];
        test_byte_stream_split_decode::<FloatType>(data);
    }

    #[test]
    fn test_byte_stream_split_f64() {
        let data = vec![vec![
            f64::from_le_bytes([0, 1, 2, 3, 4, 5, 6, 7]),
            f64::from_le_bytes([8, 9, 10, 11, 12, 13, 14, 15]),
        ]];
        test_byte_stream_split_decode::<DoubleType>(data);
    }

    #[test]
    fn test_skip_byte_stream_split() {
        let block_data = vec![0.3, 0.4, 0.1, 4.10];
        test_skip::<FloatType, f32>(block_data.clone(), Encoding::BYTE_STREAM_SPLIT, 2);
        test_skip::<DoubleType, f64>(
            block_data.into_iter().map(|x| x as f64).collect(),
            Encoding::BYTE_STREAM_SPLIT,
            100,
        );
    }

    fn test_rle_value_decode<T>(data: Vec<Vec<T::T>>)
    where
        T: DataType,
        T::T: ValueDecoder<DecodeBuffer = Vec<T::T>>,
    {
        test_encode_decode::<T>(data, Encoding::RLE);
    }

    fn test_delta_bit_packed_decode<T>(data: Vec<Vec<T::T>>)
    where
        T: DataType,
        T::T: ValueDecoder<DecodeBuffer = Vec<T::T>>,
    {
        test_encode_decode::<T>(data, Encoding::DELTA_BINARY_PACKED);
    }

    fn test_byte_stream_split_decode<T>(data: Vec<Vec<T::T>>)
    where
        T: DataType,
        T::T: ValueDecoder<DecodeBuffer = Vec<T::T>>,
    {
        test_encode_decode::<T>(data, Encoding::BYTE_STREAM_SPLIT);
    }

    fn test_delta_byte_array_decode(data: Vec<Vec<ByteArray>>) {
        test_encode_decode::<ByteArrayType>(data, Encoding::DELTA_BYTE_ARRAY);
    }

    // Input data represents vector of data slices to write (test multiple `put()` calls)
    // For example,
    //   vec![vec![1, 2, 3]] invokes `put()` once and writes {1, 2, 3}
    //   vec![vec![1, 2], vec![3]] invokes `put()` twice and writes {1, 2, 3}
    fn test_encode_decode<T>(data: Vec<Vec<T::T>>, encoding: Encoding)
    where
        T: DataType,
        T::T: ValueDecoder<DecodeBuffer = Vec<T::T>>,
    {
        // Type length should not really matter for encode/decode test,
        // otherwise change it based on type
        let col_descr = create_test_col_desc_ptr(-1, T::get_physical_type());

        // Encode data
        let mut encoder = get_encoder::<T>(encoding).expect("get encoder");

        for v in &data[..] {
            encoder.put(&v[..]).expect("ok to encode");
        }
        let bytes = encoder.flush_buffer().expect("ok to flush buffer");

        // Flatten expected data as contiguous array of values
        let expected: Vec<T::T> = data.iter().flat_map(|s| s.clone()).collect();

        // Decode data and compare with original
        let mut decoder = get_decoder::<T::T>(col_descr, encoding).expect("get decoder");

        let mut result = vec![T::T::default(); expected.len()];
        decoder
            .set_data(bytes, expected.len())
            .expect("ok to set data");
        let mut result_num_values = 0;
        while decoder.values_left() > 0 {
            result_num_values += decoder
                .read(result_num_values, &mut result) // TODO: CHANGE OFFSET
                .expect("ok to decode");
        }
        assert_eq!(result_num_values, expected.len());
        assert_eq!(result, expected);
    }

    // TODO: Update encoder to not need datatype.
    fn test_skip<D, T>(data: Vec<T>, encoding: Encoding, skip: usize)
    where
        T: ValueDecoder<DecodeBuffer = Vec<T>> + ParquetValueType,
        D: DataType<T = T>,
    {
        // Type length should not really matter for encode/decode test,
        // otherwise change it based on type
        let col_descr = create_test_col_desc_ptr(-1, T::PHYSICAL_TYPE);

        // Encode data
        let mut encoder = get_encoder::<D>(encoding).expect("get encoder");

        encoder.put(&data).expect("ok to encode");

        let bytes = encoder.flush_buffer().expect("ok to flush buffer");

        let mut decoder = get_decoder::<T>(col_descr, encoding).expect("get decoder");
        decoder.set_data(bytes, data.len()).expect("ok to set data");

        if skip >= data.len() {
            let skipped = decoder.skip(skip).expect("ok to skip");
            assert_eq!(skipped, data.len());

            let skipped_again = decoder.skip(skip).expect("ok to skip again");
            assert_eq!(skipped_again, 0);
        } else {
            let skipped = decoder.skip(skip).expect("ok to skip");
            assert_eq!(skipped, skip);

            let remaining = data.len() - skip;

            let expected = &data[skip..];
            let mut buffer = vec![T::default(); remaining];
            let fetched = decoder.read(0, &mut buffer).expect("ok to decode");
            assert_eq!(remaining, fetched);
            assert_eq!(&buffer, expected);
        }
    }

    fn create_and_check_decoder<T>(encoding: Encoding, err: Option<ParquetError>)
    where
        T: ValueDecoder + GetDecoder,
    {
        let descr = create_test_col_desc_ptr(-1, T::ValueType::PHYSICAL_TYPE);
        let decoder = get_decoder::<T>(descr, encoding);
        match err {
            Some(parquet_error) => {
                assert_eq!(
                    decoder.err().unwrap().to_string(),
                    parquet_error.to_string()
                );
            }
            None => {
                assert_eq!(decoder.unwrap().encoding(), encoding);
            }
        }
    }

    // Creates test column descriptor.
    fn create_test_col_desc_ptr(type_len: i32, t: Type) -> ColumnDescPtr {
        let ty = SchemaType::primitive_type_builder("t", t)
            .with_length(type_len)
            .build()
            .unwrap();
        Arc::new(ColumnDescriptor::new(
            Arc::new(ty),
            0,
            0,
            ColumnPath::new(vec![]),
        ))
    }

    fn usize_to_bytes(v: usize) -> [u8; 4] {
        (v as u32).to_ne_bytes()
    }

    /// A util trait to convert slices of different types to byte arrays
    trait ToByteArray<T: DataType> {
        #[allow(clippy::wrong_self_convention)]
        fn to_byte_array(data: &[T::T]) -> Vec<u8>;
    }

    macro_rules! to_byte_array_impl {
        ($ty: ty) => {
            impl ToByteArray<$ty> for $ty {
                #[allow(clippy::wrong_self_convention)]
                fn to_byte_array(data: &[<$ty as DataType>::T]) -> Vec<u8> {
                    <$ty as DataType>::T::slice_as_bytes(data).to_vec()
                }
            }
        };
    }

    to_byte_array_impl!(Int32Type);
    to_byte_array_impl!(Int64Type);
    to_byte_array_impl!(FloatType);
    to_byte_array_impl!(DoubleType);

    impl ToByteArray<BoolType> for BoolType {
        #[allow(clippy::wrong_self_convention)]
        fn to_byte_array(data: &[bool]) -> Vec<u8> {
            let mut v = vec![];
            for (i, item) in data.iter().enumerate() {
                if i % 8 == 0 {
                    v.push(0);
                }
                if *item {
                    v[i / 8] |= 1 << (i % 8);
                }
            }
            v
        }
    }

    impl ToByteArray<Int96Type> for Int96Type {
        #[allow(clippy::wrong_self_convention)]
        fn to_byte_array(data: &[Int96]) -> Vec<u8> {
            let mut v = vec![];
            for d in data {
                v.extend_from_slice(d.as_bytes());
            }
            v
        }
    }

    impl ToByteArray<ByteArrayType> for ByteArrayType {
        #[allow(clippy::wrong_self_convention)]
        fn to_byte_array(data: &[ByteArray]) -> Vec<u8> {
            let mut v = vec![];
            for d in data {
                let buf = d.data();
                let len = &usize_to_bytes(buf.len());
                v.extend_from_slice(len);
                v.extend(buf);
            }
            v
        }
    }

    impl ToByteArray<FixedLenByteArrayType> for FixedLenByteArrayType {
        #[allow(clippy::wrong_self_convention)]
        fn to_byte_array(data: &[FixedLenByteArray]) -> Vec<u8> {
            let mut v = vec![];
            for d in data {
                let buf = d.data();
                v.extend(buf);
            }
            v
        }
    }
}