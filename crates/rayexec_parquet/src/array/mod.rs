pub mod primitive;

use parquet::column::page::PageReader;
use parquet::column::reader::GenericColumnReader;
use parquet::data_type::DataType as ParquetDataType;
use parquet::schema::types::ColumnDescPtr;
use rayexec_error::{RayexecError, Result, ResultExt};

#[derive(Debug)]
pub struct ValuesReader<T: ParquetDataType, P: PageReader> {
    desc: ColumnDescPtr,
    reader: Option<GenericColumnReader<T, P>>,

    values: Vec<T::T>,
    def_levels: Option<Vec<i16>>,
    rep_levels: Option<Vec<i16>>,
}

impl<T, P> ValuesReader<T, P>
where
    T: ParquetDataType,
    P: PageReader,
{
    pub fn new(desc: ColumnDescPtr) -> Self {
        let def_levels = if desc.max_def_level() > 0 {
            Some(Vec::new())
        } else {
            None
        };
        let rep_levels = if desc.max_rep_level() > 0 {
            Some(Vec::new())
        } else {
            None
        };

        Self {
            desc,
            reader: None,
            values: Vec::new(),
            def_levels,
            rep_levels,
        }
    }

    pub fn set_page_reader(&mut self, page_reader: P) -> Result<()> {
        let reader = GenericColumnReader::new(self.desc.clone(), page_reader);
        self.reader = Some(reader);

        Ok(())
    }

    pub fn read_records(&mut self, num_records: usize) -> Result<usize> {
        let reader = match &mut self.reader {
            Some(reader) => reader,
            None => return Err(RayexecError::new("Expected reader to be Some")),
        };

        let mut num_read = 0;
        loop {
            let to_read = num_records - num_read;
            let (records_read, values_read, levels_read) = reader
                .read_records(
                    to_read,
                    self.def_levels.as_mut(),
                    self.rep_levels.as_mut(),
                    &mut self.values,
                )
                .context("read records")?;

            // Pad nulls.
            if values_read < levels_read {
                unimplemented!()
            }

            num_read += records_read;

            if num_read == num_records || !reader.has_next().context("check next page")? {
                break;
            }
        }

        Ok(num_read)
    }

    pub fn take_values(&mut self) -> Vec<T::T> {
        std::mem::take(&mut self.values)
    }
}
