//! Column chunk metadata

use super::column::ColumnDescriptor;
use crate::compression::CompressionCodec;
use crate::encoding::Encoding;
use crate::page::PageEncodingStats;
use crate::statistics::Statistics;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnChunkMetadata {
    pub column_descr: ColumnDescriptor,
    pub encodings: Vec<Encoding>,
    pub file_path: Option<String>,
    pub file_offset: i64,
    pub num_values: i64,
    pub compression: CompressionCodec,
    pub total_compressed_size: i64,
    pub total_uncompressed_size: i64,
    pub data_page_offset: i64,
    pub index_page_offset: Option<i64>,
    pub dictionary_page_offset: Option<i64>,
    pub statistics: Option<Statistics>,
    pub encoding_stats: Option<Vec<PageEncodingStats>>,
    pub bloom_filter_offset: Option<i64>,
    pub bloom_filter_length: Option<i32>,
    pub offset_index_offset: Option<i64>,
    pub offset_index_length: Option<i32>,
    pub column_index_offset: Option<i64>,
    pub column_index_length: Option<i32>,
    pub unencoded_byte_array_data_bytes: Option<i64>,
    pub repetition_level_histogram: Option<Histogram>,
    pub definition_level_histogram: Option<Histogram>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Histogram {
    pub values: Vec<i64>,
}
