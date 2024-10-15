use parquet::basic::Type as PhysicalType;
use parquet::column::page::PageReader;
use parquet::column::reader::view::ViewColumnValueDecoder;
use parquet::data_type::{ByteArray, DataType as ParquetDataType};
use parquet::decoding::view::ViewBuffer;
use parquet::schema::types::ColumnDescPtr;
use rayexec_bullet::array::Array;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::selection::SelectionVector;
use rayexec_error::{RayexecError, Result, ResultExt};

use super::{def_levels_into_bitmap, ArrayBuilder, ValuesReader};

#[derive(Debug)]
pub struct VarlenArrayReader<P: PageReader> {
    batch_size: usize,
    datatype: DataType,
    values_reader: ValuesReader<ViewColumnValueDecoder, P>,
    values_buffer: ViewBuffer,
}

impl<P> VarlenArrayReader<P>
where
    P: PageReader,
{
    pub fn new(batch_size: usize, datatype: DataType, desc: ColumnDescPtr) -> Self {
        VarlenArrayReader {
            batch_size,
            datatype,
            values_reader: ValuesReader::new(desc),
            values_buffer: ViewBuffer::new(batch_size + 1), // +1 to accomodate possible null at the end.
        }
    }

    pub fn take_array(&mut self) -> Result<Array> {
        let def_levels = self.values_reader.take_def_levels();
        let _rep_levels = self.values_reader.take_rep_levels();

        // Replace the view buffer, what we take is the basis for the array.
        let mut view_buffer = std::mem::replace(
            &mut self.values_buffer,
            ViewBuffer::new(self.batch_size + 1), // +1 to accommodate possible null at the end.
        );

        let arr = match (ByteArray::get_physical_type(), &self.datatype) {
            (PhysicalType::BYTE_ARRAY, _) => {
                match def_levels {
                    Some(levels) => {
                        // Do a utf8 check if needed.
                        if matches!(self.datatype, DataType::Utf8 | DataType::LargeUtf8) {
                            view_buffer.validate_utf8().context("Failed to validate utf8")?;
                        }

                        // Logical validities, won't be the final bitmap we use.
                        let logical_bitmap = def_levels_into_bitmap(levels);

                        // Ensure we have a dummy null value at the end. This
                        // will be the index that all null values in the
                        // selection vector will point to.
                        //
                        // We do this because parquet only stores non-null
                        // values, and we want to avoid having to reorder the
                        // array to insert null values at the proper indices.
                        // Instead we can just use a selection vector to get the
                        // same effect.
                        let null_idx = view_buffer.len();
                        view_buffer.push(&[]);

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
                        let mut final_bitmap = Bitmap::new_with_all_true(view_buffer.len());
                        final_bitmap.set_unchecked(null_idx, false);

                        let data = view_buffer.into_array_data();

                        Array::new_with_validity_selection_and_array_data(self.datatype.clone(), final_bitmap, SelectionVector::from(selection), data)
                    }
                    None => {
                        Array::new_with_array_data(self.datatype.clone(), view_buffer.into_array_data())
                    }
                }
            }
            (p_other, d_other) => return Err(RayexecError::new(format!("Unknown conversion from parquet to bullet type in varlen reader; parqet: {p_other}, bullet: {d_other}")))
        };

        Ok(arr)
    }
}

impl<P> ArrayBuilder<P> for VarlenArrayReader<P>
where
    P: PageReader,
{
    fn build(&mut self) -> Result<Array> {
        self.take_array()
    }

    fn set_page_reader(&mut self, page_reader: P) -> Result<()> {
        let decoder = ViewColumnValueDecoder::new(&self.values_reader.descr);
        self.values_reader.set_page_reader(decoder, page_reader)
    }

    fn read_rows(&mut self, n: usize) -> Result<usize> {
        self.values_reader.read_records(n, &mut self.values_buffer)
    }
}
