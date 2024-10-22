// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::Range;
use std::sync::{Arc, Weak};

use futures::future::BoxFuture;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;

/// A collection of [`AsyncFileReader`] that gets the reader back before they are dropped
///
/// This allows reusing readers across requests, so they can cache the metadata.
pub struct ParquetFileReaderPool<R: Send>(Arc<crossbeam_queue::SegQueue<R>>);

impl<R: Send> Default for ParquetFileReaderPool<R> {
    fn default() -> Self {
        ParquetFileReaderPool(Arc::default())
    }
}

impl<R: Send> ParquetFileReaderPool<R> {
    pub fn try_get_reader<E>(
        &self,
        reader_init: impl FnOnce() -> Result<R, E>,
    ) -> Result<PooledParquetFileReader<R>, E> {
        let inner_reader = match self.0.pop() {
            None => reader_init()?,
            Some(reader) => reader,
        };
        Ok(PooledParquetFileReader::new(
            inner_reader,
            Arc::downgrade(&self.0), // downgrade so orphan readers don't keep the pool in memory
        ))
    }
}

/// Wrapper for [`AsyncFileReader`] that puts its wrapped reader back into a pool when dropped.
pub struct PooledParquetFileReader<R: Send> {
    inner: Option<R>,
    pool: Weak<crossbeam_queue::SegQueue<R>>,
}

impl<R: Send> PooledParquetFileReader<R> {
    fn new(inner: R, pool: Weak<crossbeam_queue::SegQueue<R>>) -> Self {
        Self {
            inner: Some(inner),
            pool,
        }
    }
}

impl<R: AsyncFileReader + Send> AsyncFileReader for PooledParquetFileReader<R> {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        self.inner.as_mut().unwrap().get_bytes(range)
    }
    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        self.inner.as_mut().unwrap().get_metadata()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        self.inner.as_mut().unwrap().get_byte_ranges(ranges)
    }
}

impl<R: Send> Drop for PooledParquetFileReader<R> {
    fn drop(&mut self) {
        // If the pool still exists, put the wrapped reader back into the pool
        if let Some(pool) = self.pool.upgrade() {
            pool.push(self.inner.take().unwrap());
        }
    }
}
