#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
    Lz4Raw,
}
