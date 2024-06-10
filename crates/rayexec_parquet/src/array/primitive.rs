use bytes::Bytes;
use parquet::column::page::PageReader;
use parquet::data_type::{DataType as ParquetDataType, Int96};
use parquet::schema::types::ColumnDescPtr;
use parquet::{basic::Type as PhysicalType, file::reader::SerializedPageReader};
use rayexec_bullet::array::TimestampArray;
use rayexec_bullet::field::TimeUnit;
use rayexec_bullet::{
    array::{
        Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    field::DataType,
};
use rayexec_error::{RayexecError, Result};

use super::{ArrayBuilder, ValuesReader};

pub struct PrimitiveArrayReader<T: ParquetDataType, P: PageReader> {
    datatype: DataType,
    values_reader: ValuesReader<T, P>,
}

impl<T, P> PrimitiveArrayReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
    Vec<T::T>: IntoArray,
{
    pub fn new(datatype: DataType, desc: ColumnDescPtr) -> Self {
        PrimitiveArrayReader {
            datatype,
            values_reader: ValuesReader::new(desc),
        }
    }
}

impl<T, P> PrimitiveArrayReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
    Vec<T::T>: IntoArray,
{
    /// Take the currently read values and convert into an array.
    pub fn take_array(&mut self) -> Result<Array> {
        let data = self.values_reader.take_values();
        // TODO: Nulls

        // TODO: Other types.
        let arr = match (T::get_physical_type(), &self.datatype) {
            (PhysicalType::BOOLEAN, DataType::Boolean) => data.into_array(),
            (PhysicalType::INT32, DataType::Int32) => data.into_array(),
            (PhysicalType::INT64, DataType::Int64) => data.into_array(),
            (PhysicalType::INT96, DataType::Timestamp(TimeUnit::Nanosecond)) => data.into_array(),
            (PhysicalType::FLOAT, DataType::Float32) => data.into_array(),
            (PhysicalType::DOUBLE, DataType::Float64) => data.into_array(),
            (p_other, d_other) => return Err(RayexecError::new(format!("Unknown conversion from parquet to bullet type in primitive reader; parqet: {p_other}, bullet: {d_other}")))
        };

        Ok(arr)
    }
}

impl<T, P> ArrayBuilder<P> for PrimitiveArrayReader<T, P>
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

pub trait IntoArray {
    fn into_array(self) -> Array;
}

impl IntoArray for Vec<bool> {
    fn into_array(self) -> Array {
        Array::Boolean(BooleanArray::from_iter(self))
    }
}

impl IntoArray for Vec<Int96> {
    fn into_array(self) -> Array {
        let vals = self.into_iter().map(|v| v.to_nanos());
        Array::Timestamp(TimeUnit::Nanosecond, TimestampArray::from_iter(vals))
    }
}

macro_rules! into_array_prim {
    ($prim:ty, $variant:ident, $array:ty) => {
        impl IntoArray for Vec<$prim> {
            fn into_array(self) -> Array {
                Array::$variant(<$array>::from(self))
            }
        }
    };
}

into_array_prim!(i8, Int8, Int8Array);
into_array_prim!(i16, Int16, Int16Array);
into_array_prim!(i32, Int32, Int32Array);
into_array_prim!(i64, Int64, Int64Array);
into_array_prim!(u8, UInt8, UInt8Array);
into_array_prim!(u16, UInt16, UInt16Array);
into_array_prim!(u32, UInt32, UInt32Array);
into_array_prim!(u64, UInt64, UInt64Array);
into_array_prim!(f32, Float32, Float32Array);
into_array_prim!(f64, Float64, Float64Array);
