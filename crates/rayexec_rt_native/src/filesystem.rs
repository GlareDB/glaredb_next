use std::fs::{File, OpenOptions};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::future::{self, BoxFuture, FutureExt};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use rayexec_error::{RayexecError, Result};
use rayexec_io::{
    filesystem::{FileReader, FileSystemProvider},
    AsyncReader,
};
use std::io::{Read, Seek, SeekFrom};

/// Standard file system access, nothing special.
#[derive(Debug, Clone, Copy)]
pub struct LocalFileSystemProvider;

impl FileSystemProvider for LocalFileSystemProvider {
    fn reader(&self, path: &Path) -> Result<Box<dyn FileReader>> {
        let file = OpenOptions::new().read(true).open(path).map_err(|e| {
            RayexecError::with_source(
                format!(
                    "Failed to open file at location: {}",
                    path.to_string_lossy()
                ),
                Box::new(e),
            )
        })?;

        let len = file.metadata()?.len() as usize;

        Ok(Box::new(LocalFile { len, file }))
    }
}

#[derive(Debug)]
pub struct LocalFile {
    len: usize,
    file: File,
}

impl FileReader for LocalFile {
    fn size(&mut self) -> BoxFuture<Result<usize>> {
        async move { Ok(self.len) }.boxed()
    }
}

/// Implementation of async reading on top of a file.
///
/// Note that we're not using tokio's async read+sync traits as the
/// implementation for files will attempt to spawn the read on a block thread.
impl AsyncReader for LocalFile {
    fn read_range(&mut self, start: usize, len: usize) -> BoxFuture<Result<Bytes>> {
        let mut buf = vec![0; len];
        let result = read_at(&mut self.file, start, &mut buf);
        let bytes = Bytes::from(buf);
        future::ready(result.map(|_| bytes)).boxed()
    }

    fn read_stream(&mut self) -> BoxStream<'static, Result<Bytes>> {
        // TODO: Remove these when this function is changed to
        // `into_read_stream`. This shares the same file handle which isn't
        // good. We should be taking full ownership of it.
        let mut file = self.file.try_clone().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();

        FileStream {
            file,
            curr: 0,
            len: self.len,
        }
        .boxed()
    }
}

struct FileStream {
    file: File,
    curr: usize,
    len: usize,
}

impl FileStream {
    fn read_next(&mut self) -> Result<Bytes> {
        const FILE_STREAM_BUF_SIZE: usize = 4 * 1024;

        let rem = self.len - self.curr;
        let to_read = usize::min(rem, FILE_STREAM_BUF_SIZE);

        // TODO: Reuse buffer. This might be tricky just given that we're
        // requiring the future to be static.
        let mut buf = vec![0; to_read];

        self.file.read_exact(&mut buf)?;
        self.curr += buf.len();

        Ok(buf.into())
    }
}

impl Stream for FileStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.curr >= self.len {
            return Poll::Ready(None);
        }

        let result = self.read_next();
        Poll::Ready(Some(result))
    }
}

/// Helper for synchronously reading into a buffer.
fn read_at<R>(mut reader: R, start: usize, buf: &mut [u8]) -> Result<()>
where
    R: Read + Seek,
{
    reader.seek(SeekFrom::Start(start as u64))?;
    reader.read_exact(buf)?;
    Ok(())
}
