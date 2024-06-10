use parquet::basic::Type as PhysicalType;
use parquet::column::page::PageReader;
use parquet::data_type::DataType as ParquetDataType;
use rayexec_bullet::{
    array::{Array, OffsetIndex, VarlenBuffer},
    field::DataType,
};
use rayexec_error::{RayexecError, Result};

use super::ValuesReader;

#[derive(Debug)]
pub struct VarlenArrayReader<T: ParquetDataType, P: PageReader> {
    datatype: DataType,
    values_reader: ValuesReader<T, P>,
}

impl<T, P> VarlenArrayReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
{
    pub fn take_array(&mut self) -> Result<Array> {
        let data = self.values_reader.take_values();
        // TODO: Nulls
        let arr = match (T::get_physical_type(), &self.datatype) {
            (p_other, d_other) => return Err(RayexecError::new(format!("Unknown conversion from parquet to bullet type in varlen reader; parqet: {p_other}, bullet: {d_other}")))
        };

        Ok(arr)
    }
}
