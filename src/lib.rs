// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![doc = include_str!("../README.md")]

pub use parquet;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

mod caching_parquet_reader;
pub mod metrics;
mod pooled_reader;
mod reader;
pub mod reader_builder_config;
mod table;
pub use table::*;
pub mod types;

pub trait ReaderBuilderConfigurator: Send + Sync + 'static {
    fn configure<R: AsyncFileReader>(
        &self,
        reader_builder: ParquetRecordBatchStreamBuilder<R>,
    ) -> anyhow::Result<ParquetRecordBatchStreamBuilder<R>>;
}
