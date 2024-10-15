use parquet::basic::Type as PhysicalType;
use parquet::column::page::PageReader;
use parquet::column::reader::basic::BasicColumnValueDecoder;
use parquet::data_type::{DataType as ParquetDataType, Int96};
use parquet::schema::types::ColumnDescPtr;
use rayexec_bullet::array::{Array, ArrayData};
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::selection::SelectionVector;
use rayexec_bullet::storage::{BooleanStorage, PrimitiveStorage};
use rayexec_error::{RayexecError, Result};

use super::{def_levels_into_bitmap, ArrayBuilder, IntoArrayData, ValuesReader};

pub struct PrimitiveArrayReader<T: ParquetDataType, P: PageReader> {
    batch_size: usize,
    datatype: DataType,
    values_reader: ValuesReader<BasicColumnValueDecoder<T>, P>,
    values_buffer: Vec<T::T>,
}

impl<T, P> PrimitiveArrayReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
    Vec<T::T>: IntoArrayData,
{
    pub fn new(batch_size: usize, datatype: DataType, desc: ColumnDescPtr) -> Self {
        PrimitiveArrayReader {
            batch_size,
            datatype,
            values_reader: ValuesReader::new(desc),
            values_buffer: Vec::with_capacity(batch_size + 1), // +1 for possible null value.
        }
    }

    /// Take the currently read values and convert into an array.
    pub fn take_array(&mut self) -> Result<Array> {
        let def_levels = self.values_reader.take_def_levels();
        let _rep_levels = self.values_reader.take_rep_levels();

        // Basis of the array.
        let mut data = std::mem::replace(
            &mut self.values_buffer,
            Vec::with_capacity(self.batch_size + 1), // +1 for possible null value.
        );

        // Insert dummy null into array data if needed.
        let null_idx = data.len();
        if def_levels.is_some() {
            data.push(T::T::default());
        }

        let phys_len = data.len();

        let array_data = match (T::get_physical_type(), &self.datatype) {
            (PhysicalType::BOOLEAN, DataType::Boolean) => data.into_array_data(),
            (PhysicalType::INT32, DataType::Int32) => data.into_array_data(),
            (PhysicalType::INT32, DataType::Date32) => data.into_array_data(),
            (PhysicalType::INT64, DataType::Int64) => data.into_array_data(),
            (PhysicalType::INT64, DataType::Decimal64(_)) => data.into_array_data(),
            (PhysicalType::INT64, DataType::Timestamp(_)) => data.into_array_data(),
            (PhysicalType::INT96, DataType::Timestamp(_)) => data.into_array_data(),
            (PhysicalType::FLOAT, DataType::Float32) => data.into_array_data(),
            (PhysicalType::DOUBLE, DataType::Float64) => data.into_array_data(),
            (p_other, d_other) => return Err(RayexecError::new(format!("Unknown conversion from parquet to bullet type in primitive reader; parquet: {p_other}, bullet: {d_other}")))
        };

        match def_levels {
            Some(levels) => {
                // Produce a selection vector that maps the logical nulls to
                // where the physical null is.

                // Logical validities, won't be the final bitmap we use.
                let logical_bitmap = def_levels_into_bitmap(levels);

                // Null index defined above.
                let mut selection = vec![null_idx; logical_bitmap.len()];

                let mut phys_idx = 0;
                for (row_idx, valid) in logical_bitmap.iter().enumerate() {
                    if valid {
                        selection[row_idx] = phys_idx;
                        phys_idx += 1;
                    } else {
                        // Selection already intialized to the null index.
                    }
                }

                // Actual bitmap, last index set to null (null_idx).
                let mut final_bitmap = Bitmap::new_with_all_true(phys_len);
                final_bitmap.set_unchecked(null_idx, false);

                Ok(Array::new_with_validity_selection_and_array_data(
                    self.datatype.clone(),
                    final_bitmap,
                    SelectionVector::from(selection),
                    array_data,
                ))
            }
            None => {
                // Nothing else to do, return array data as-is.
                Ok(Array::new_with_array_data(
                    self.datatype.clone(),
                    array_data,
                ))
            }
        }
    }
}

impl<T, P> ArrayBuilder<P> for PrimitiveArrayReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
    Vec<T::T>: IntoArrayData,
{
    fn build(&mut self) -> Result<Array> {
        self.take_array()
    }

    fn set_page_reader(&mut self, page_reader: P) -> Result<()> {
        let decoder = BasicColumnValueDecoder::new(&self.values_reader.descr);
        self.values_reader.set_page_reader(decoder, page_reader)
    }

    fn read_rows(&mut self, n: usize) -> Result<usize> {
        self.values_reader.read_records(n, &mut self.values_buffer)
    }
}

impl IntoArrayData for Vec<bool> {
    fn into_array_data(self) -> ArrayData {
        let values = Bitmap::from_iter(self);
        BooleanStorage::from(values).into()
    }
}

macro_rules! impl_into_array_primitive {
    ($prim:ty) => {
        impl IntoArrayData for Vec<$prim> {
            fn into_array_data(self) -> ArrayData {
                PrimitiveStorage::from(self).into()
            }
        }
    };
}

impl_into_array_primitive!(i8);
impl_into_array_primitive!(i16);
impl_into_array_primitive!(i32);
impl_into_array_primitive!(i64);
impl_into_array_primitive!(i128);
impl_into_array_primitive!(u8);
impl_into_array_primitive!(u16);
impl_into_array_primitive!(u32);
impl_into_array_primitive!(u64);
impl_into_array_primitive!(u128);
impl_into_array_primitive!(f32);
impl_into_array_primitive!(f64);

impl IntoArrayData for Vec<Int96> {
    fn into_array_data(self) -> ArrayData {
        let values: Vec<_> = self.into_iter().map(|v| v.to_nanos()).collect();
        PrimitiveStorage::from(values).into()
    }
}

/// Insert null (meaningless) values into the vec according to the validity
/// bitmap.
///
/// The resulting vec will have its length equal to the bitmap's length.
fn insert_null_values<T: Copy + Default>(mut values: Vec<T>, bitmap: &Bitmap) -> Vec<T> {
    values.resize(bitmap.len(), T::default());

    for (current_idx, new_idx) in (0..values.len()).rev().zip(bitmap.index_iter().rev()) {
        if current_idx <= new_idx {
            break;
        }
        values[new_idx] = values[current_idx];
    }

    values
}
