use parquet::column::page::PageReader;
use parquet::data_type::{AsBytes, ByteArray, ByteArrayType, DataType as ParquetDataType};
use parquet::{basic::Type as PhysicalType, schema::types::ColumnDescPtr};
use rayexec_bullet::array::{Array2, ValuesBuffer};
use rayexec_bullet::array::{BinaryArray, VarlenArray, VarlenValuesBuffer};
use rayexec_bullet::datatype::DataType;
use rayexec_error::{RayexecError, Result};

use super::{def_levels_into_bitmap, ArrayBuilder, ValuesReader};

#[derive(Debug)]
pub struct VarlenArrayReader<P: PageReader> {
    datatype: DataType,
    values_reader: ValuesReader<ByteArrayType, P>,
    values_buffer: Vec<ByteArray>,
}

impl<P> VarlenArrayReader<P>
where
    P: PageReader,
{
    pub fn new(batch_size: usize, datatype: DataType, desc: ColumnDescPtr) -> Self {
        VarlenArrayReader {
            datatype,
            values_reader: ValuesReader::new(desc),
            values_buffer: Vec::with_capacity(batch_size),
        }
    }

    pub fn take_array(&mut self) -> Result<Array2> {
        let def_levels = self.values_reader.take_def_levels();
        let _rep_levels = self.values_reader.take_rep_levels();

        let arr = match (ByteArrayType::get_physical_type(), &self.datatype) {
            (PhysicalType::BYTE_ARRAY, DataType::Utf8) => {
                // TODO: Ideally we change the decoding to write directly into a buffer
                // we can use for constructing the array instead of needing to copy it.
                //
                // TODO: Byte array methods panic on no data except for
                // `num_bytes` which I added. Make that clearer and/or fix it.
                let data_cap: usize = self.values_buffer.iter().map(|a| a.num_bytes().unwrap_or(0)).sum();
                let mut buffer = VarlenValuesBuffer::with_data_and_offset_caps(data_cap, self.values_buffer.len());

                let validity = match def_levels {
                    Some(levels) => {
                        let bitmap = def_levels_into_bitmap(levels);
                        let mut values_iter = self.values_buffer.iter();

                        for valid in bitmap.iter() {
                            if valid {
                                let value = values_iter.next().expect("value to exist");
                                let s = unsafe { std::str::from_utf8_unchecked(value.as_bytes()) };
                                buffer.push_value(s);
                            } else {
                                buffer.push_value("");
                            }
                        }

                        Some(bitmap)
                    }
                    None => {
                        for buf in &self.values_buffer {
                            let s = unsafe { std::str::from_utf8_unchecked(buf.as_bytes()) };
                            buffer.push_value(s);
                        }
                        None
                    }
                };

                Array2::Utf8(VarlenArray::new(buffer, validity))

            }
            (PhysicalType::BYTE_ARRAY, DataType::Binary) => {
                let data_cap: usize = self.values_buffer.iter().map(|a| a.num_bytes().unwrap_or(0)).sum();
                let mut buffer = VarlenValuesBuffer::with_data_and_offset_caps(data_cap, self.values_buffer.len());

                let validity = match def_levels {
                    Some(levels) => {
                        let bitmap = def_levels_into_bitmap(levels);
                        let mut values_iter = self.values_buffer.iter();

                        for valid in bitmap.iter() {
                            if valid {
                                let value = values_iter.next().expect("value to exist");
                                buffer.push_value(value.as_bytes());
                            } else {
                                let null: &[u8] = &[];
                                buffer.push_value(null);
                            }
                        }
                        Some(bitmap)
                    }
                    None => {
                        for buf in &self.values_buffer {
                            buffer.push_value(buf.as_bytes());
                        }
                        None
                    }
                };

                Array2::Binary(BinaryArray::new(buffer, validity))
            }
            (p_other, d_other) => return Err(RayexecError::new(format!("Unknown conversion from parquet to bullet type in varlen reader; parqet: {p_other}, bullet: {d_other}")))
        };

        self.values_buffer.clear();

        Ok(arr)
    }
}

impl<P> ArrayBuilder<P> for VarlenArrayReader<P>
where
    P: PageReader,
{
    fn build(&mut self) -> Result<Array2> {
        self.take_array()
    }

    fn set_page_reader(&mut self, page_reader: P) -> Result<()> {
        self.values_reader.set_page_reader(page_reader)
    }

    fn read_rows(&mut self, n: usize) -> Result<usize> {
        self.values_reader.read_records(n, &mut self.values_buffer)
    }
}
