// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::Range;
use std::sync::Arc;

use futures::future::BoxFuture;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;

/// Wrapper for [`AsyncFileReader`] that only reads its metadata the first time it is requested
pub struct CachingParquetFileReader<R: AsyncFileReader> {
    inner: R,
    metadata: Arc<ParquetMetaData>,
}

impl<R: AsyncFileReader> CachingParquetFileReader<R> {
    pub fn new(inner: R, metadata: Arc<ParquetMetaData>) -> Self {
        Self { inner, metadata }
    }
}

impl<R: AsyncFileReader> AsyncFileReader for CachingParquetFileReader<R> {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        self.inner.get_bytes(range)
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(std::future::ready(Ok(Arc::clone(&self.metadata))))
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        self.inner.get_byte_ranges(ranges)
    }
}
