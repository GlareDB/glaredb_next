use parquet::column::page::PageReader;
use parquet::data_type::{AsBytes, ByteArray, DataType as ParquetDataType};
use parquet::{basic::Type as PhysicalType, schema::types::ColumnDescPtr};
use rayexec_bullet::{
    array::{Array, ArrayBuilder as _, VarlenArrayBuilder},
    field::DataType,
};
use rayexec_error::{RayexecError, Result};

use super::{ArrayBuilder, IntoArray, ValuesReader};

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
        // TODO: Nulls
        let arr = match (T::get_physical_type(), &self.datatype) {
            (PhysicalType::BYTE_ARRAY, DataType::Utf8) => {
                data.into_array()
            }
            (PhysicalType::BYTE_ARRAY, DataType::Binary) => {
                let mut builder = VarlenArrayBuilder::new();
                for buf in &data {
                    builder.push_value(buf.as_bytes());
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
    fn into_array(self) -> Array {
        let mut builder = VarlenArrayBuilder::new();
        for buf in &self {
            let s = unsafe { std::str::from_utf8_unchecked(buf.as_bytes()) };
            builder.push_value(s);
        }
        let arr = builder.into_typed_array();
        Array::Utf8(arr)
    }
}
