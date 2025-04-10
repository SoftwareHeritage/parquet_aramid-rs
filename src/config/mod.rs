// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

mod binary;
pub use binary::*;
mod primitive;
pub use primitive::*;

pub trait Configurator: Send + Sync + 'static {
    fn configure_stream_builder<R: AsyncFileReader>(
        &self,
        reader_builder: ParquetRecordBatchStreamBuilder<R>,
    ) -> anyhow::Result<ParquetRecordBatchStreamBuilder<R>>;
}

/// An implementation of [`Configurator`] that does nothing
///
/// In particular, it does not filter rows at all.
pub struct NoopConfigurator;

impl Configurator for NoopConfigurator {
    fn configure_stream_builder<R: AsyncFileReader>(
        &self,
        reader_builder: ParquetRecordBatchStreamBuilder<R>,
    ) -> Result<ParquetRecordBatchStreamBuilder<R>> {
        Ok(reader_builder)
    }
}
