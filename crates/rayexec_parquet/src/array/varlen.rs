use parquet::column::page::PageReader;
use parquet::data_type::{AsBytes, ByteArray, DataType as ParquetDataType};
use parquet::{basic::Type as PhysicalType, schema::types::ColumnDescPtr};
use rayexec_bullet::{
    array::{Array, ArrayBuilder as _, VarlenArrayBuilder},
    field::DataType,
};
use rayexec_error::{RayexecError, Result};

use super::{def_levels_into_bitmap, ArrayBuilder, IntoArray, ValuesReader};

#[derive(Debug)]
pub struct VarlenArrayReader<T: ParquetDataType, P: PageReader> {
    datatype: DataType,
    values_reader: ValuesReader<T, P>,
}

impl<T, P> VarlenArrayReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
    Vec<T::T>: IntoArray,
{
    pub fn new(datatype: DataType, desc: ColumnDescPtr) -> Self {
        VarlenArrayReader {
            datatype,
            values_reader: ValuesReader::new(desc),
        }
    }

    pub fn take_array(&mut self) -> Result<Array> {
        let data = self.values_reader.take_values();
        let def_levels = self.values_reader.take_def_levels();
        let _rep_levels = self.values_reader.take_rep_levels();

        let arr = match (T::get_physical_type(), &self.datatype) {
            (PhysicalType::BYTE_ARRAY, DataType::Utf8) => {
                data.into_array(def_levels)
            }
            (PhysicalType::BYTE_ARRAY, DataType::Binary) => {
                let mut builder = VarlenArrayBuilder::new();

                match def_levels {
                    Some(levels) => {
                        let bitmap = def_levels_into_bitmap(levels);
                        let mut values_iter = data.iter();

                        for valid in bitmap.iter() {
                            if valid {
                                let value = values_iter.next().expect("value to exist");
                                builder.push_value(value.as_bytes());
                            } else {
                                builder.push_value(&[]);
                            }
                        }

                        builder.put_validity(bitmap);
                    }
                    None => {
                        for buf in &data {
                            builder.push_value(buf.as_bytes());
                        }
                    }
                }

                let arr = builder.into_typed_array();
                Array::Binary(arr)
            }
            (p_other, d_other) => return Err(RayexecError::new(format!("Unknown conversion from parquet to bullet type in varlen reader; parqet: {p_other}, bullet: {d_other}")))
        };

        Ok(arr)
    }
}

impl<T, P> ArrayBuilder<P> for VarlenArrayReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
    Vec<T::T>: IntoArray,
{
    fn build(&mut self) -> Result<Array> {
        self.take_array()
    }

    fn set_page_reader(&mut self, page_reader: P) -> Result<()> {
        self.values_reader.set_page_reader(page_reader)
    }

    fn read_rows(&mut self, n: usize) -> Result<usize> {
        self.values_reader.read_records(n)
    }
}

impl IntoArray for Vec<ByteArray> {
    fn into_array(self, def_levels: Option<Vec<i16>>) -> Array {
        let mut builder = VarlenArrayBuilder::new();

        match def_levels {
            Some(levels) => {
                let bitmap = def_levels_into_bitmap(levels);
                let mut values_iter = self.iter();

                for valid in bitmap.iter() {
                    if valid {
                        let value = values_iter.next().expect("value to exist");
                        let s = unsafe { std::str::from_utf8_unchecked(value.as_bytes()) };
                        builder.push_value(s);
                    } else {
                        builder.push_value("");
                    }
                }

                builder.put_validity(bitmap);
            }
            None => {
                for buf in &self {
                    let s = unsafe { std::str::from_utf8_unchecked(buf.as_bytes()) };
                    builder.push_value(s);
                }
            }
        }

        let arr = builder.into_typed_array();
        Array::Utf8(arr)
    }
}
