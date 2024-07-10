use csv_core::{Reader, ReaderBuilder};
use rayexec_error::{RayexecError, Result, ResultExt};

use crate::reader::DialectOptions;

const DATA_BUFFER_SIZE: usize = 4 * 1024;
const END_BUFFER_SIZE: usize = 1024;

#[derive(Debug)]
pub struct DecoderState {
    /// Buffer containing decoded records.
    buffer: Vec<u8>,

    /// Length of decoded data in `buffer`.
    buffer_len: usize,

    /// End offsets for fields in `buffer`.
    ends: Vec<usize>,

    /// Length of end offsets in `ends`.
    ends_len: usize,

    /// Field index in a record we're currently decoding.
    current_field: usize,

    /// Number of fields we've detected during decoding.
    ///
    /// Only set when we've completed our first record.
    num_fields: Option<usize>,
}

impl Default for DecoderState {
    fn default() -> Self {
        DecoderState {
            buffer: vec![0; DATA_BUFFER_SIZE],
            buffer_len: 0,
            ends: vec![0; END_BUFFER_SIZE],
            ends_len: 0,
            current_field: 0,
            num_fields: None,
        }
    }
}

impl DecoderState {
    /// Get the number of complete records we've decoded so far.
    pub fn num_records(&self) -> usize {
        let fields = match self.num_fields {
            Some(n) => n,
            None => return 0,
        };

        self.ends_len / fields
    }

    pub fn num_fields(&self) -> Option<usize> {
        self.num_fields
    }

    pub fn clear_completed(&mut self) {
        let num_completed = self.num_records();
        let num_fields = match self.num_fields {
            Some(n) => n,
            None => return, // No completed records to clear.
        };

        let start_data_offset = self.ends[num_completed * num_fields];

        let ends_idx = num_completed * num_fields + 1;
        self.ends
            .copy_within(ends_idx..(ends_idx + self.current_field), 0);

        let end_data_offset = self.ends[self.current_field];

        self.buffer
            .copy_within(start_data_offset..end_data_offset, 0);

        self.buffer_len = end_data_offset - 1;
        self.ends_len = self.current_field;
    }

    /// Resets the state to as if we've never decoded anything.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.buffer_len = 0;
        self.ends.clear();
        self.ends_len = 0;
        self.current_field = 0;
        self.num_fields = None;
    }

    pub fn completed_records(&self) -> CompletedRecords {
        CompletedRecords { state: self }
    }
}

#[derive(Debug)]
pub struct CompletedRecords<'a> {
    state: &'a DecoderState,
}

impl<'a> CompletedRecords<'a> {
    pub fn num_completed(&self) -> usize {
        self.state.num_records()
    }

    pub fn num_fields(&self) -> Option<usize> {
        self.state.num_fields()
    }

    pub fn get_record(&self, idx: usize) -> Option<CompletedRecord<'a>> {
        let num_fields = self.state.num_fields?;
        if idx >= self.state.num_records() {
            return None;
        }

        let ends = &self.state.ends[(idx * num_fields)..(idx * num_fields + num_fields)];
        let data_start = if idx == 0 {
            0
        } else {
            let ends_idx = idx * num_fields;
            self.state.ends[ends_idx]
        };
        let data_end = *ends.last()?;
        let data = &self.state.buffer[data_start..data_end];

        Some(CompletedRecord {
            line: idx + 1,
            data,
            ends,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = CompletedRecord> {
        (0..self.state.num_records()).map(|idx| self.get_record(idx).unwrap())
    }
}

#[derive(Debug)]
pub struct CompletedRecord<'a> {
    line: usize,
    data: &'a [u8],
    ends: &'a [usize],
}

impl<'a> CompletedRecord<'a> {
    pub fn get_field(&self, idx: usize) -> Result<&'a str> {
        let start = if idx == 0 { 0 } else { self.ends[idx - 1] };
        let end = self.ends[idx];

        std::str::from_utf8(&self.data[start..end]).context_fn(|| {
            format!(
                "Field '{idx}' on line '{}' contains invalid UTF-8 data",
                self.line
            )
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<&str>> {
        (0..self.ends.len()).map(|idx| self.get_field(idx))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecoderResult {
    /// Input was completely exhausted.
    InputExhuasted,

    /// Buffer was full but input was not exhuasted. Call `decode` again with a
    /// sliced input beginning at offset to resume.
    BufferFull { input_offset: usize },
}

#[derive(Debug)]
pub struct CsvDecoder {
    reader: Reader,
}

impl CsvDecoder {
    pub fn new(dialect: DialectOptions) -> Self {
        CsvDecoder {
            reader: dialect.csv_core_reader(),
        }
    }

    /// Decode an input buffer writing decoded fields to `state`.
    pub fn decode(&mut self, input: &[u8], state: &mut DecoderState) -> Result<DecoderResult> {
        let mut input_offset = 0;

        // Read as many records as we can.
        loop {
            let input = &input[input_offset..];
            let output = &mut state.buffer[state.buffer_len..];
            let ends = &mut state.ends[state.ends_len..];

            let (result, bytes_read, bytes_written, ends_written) =
                self.reader.read_record(input, output, ends);

            input_offset += bytes_read;
            state.buffer_len += bytes_written;
            state.ends_len += ends_written;
            state.current_field += ends_written;

            match result {
                csv_core::ReadRecordResult::InputEmpty => return Ok(DecoderResult::InputExhuasted),
                csv_core::ReadRecordResult::OutputFull => {
                    return Ok(DecoderResult::BufferFull { input_offset })
                }
                csv_core::ReadRecordResult::OutputEndsFull => {
                    return Ok(DecoderResult::BufferFull { input_offset })
                }
                csv_core::ReadRecordResult::Record => {
                    match state.num_fields {
                        Some(num) => {
                            if state.current_field != num {
                                return Err(RayexecError::new(format!(
                                    "Invalid number of fields in record. Got {}, expected {}",
                                    state.current_field, num
                                )));
                            }
                        }
                        None => state.num_fields = Some(state.current_field),
                    }

                    state.current_field = 0;
                    // Continue reading records.
                }
                csv_core::ReadRecordResult::End => return Ok(DecoderResult::InputExhuasted),
            }
        }
    }
}
