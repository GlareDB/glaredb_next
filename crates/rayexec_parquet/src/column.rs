use parquet::schema::types::ColumnDescPtr;

pub struct RecordReader<T> {
    desc: ColumnDescPtr,
    values: Vec<T>,
}

impl<T> RecordReader<T> {}
