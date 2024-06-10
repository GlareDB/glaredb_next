use parquet::file::{
    footer::{decode_footer, decode_metadata},
    metadata::ParquetMetaData,
};
use rayexec_error::{RayexecError, Result, ResultExt};
use std::ops::Range;

pub trait AsyncReadRange {
    async fn read_range(&mut self, range: Range<usize>, buf: &mut [u8]) -> Result<()>;
}

#[derive(Debug)]
pub struct Metadata {
    pub parquet_metadata: ParquetMetaData,
}

impl Metadata {
    /// Loads parquet metadata from an async source.
    pub async fn load_from<R>(mut reader: R, size: usize) -> Result<Self>
    where
        R: AsyncReadRange,
    {
        if size < 8 {
            return Err(RayexecError::new("File size is too small"));
        }

        let mut footer = [0; 8];
        let footer_start = size - 8;
        reader.read_range(footer_start..size, &mut footer).await?;

        let len = decode_footer(&footer).context("failed to decode footer")?;
        if size < len + 8 {
            return Err(RayexecError::new(format!(
                "File size of {size} is less than footer + metadata {}",
                len + 8
            )));
        }

        let metadata_start = size - len - 8;
        let mut metadata = vec![0; len];
        reader
            .read_range(metadata_start..size - 8, &mut metadata)
            .await?;

        let metadata = decode_metadata(&metadata).context("failed to decode metadata")?;

        Ok(Metadata {
            parquet_metadata: metadata,
        })
    }
}
