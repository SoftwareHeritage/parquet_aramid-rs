// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use arrow::array::*;
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};

use crate::types::FixedSizeBinary;
use crate::ReaderBuilderConfigurator;

/// An implementation of [`ReaderBuilderConfigurator`] that does nothing
///
/// In particular, it does not filter rows at all.
pub struct NoopConfigurator;

impl ReaderBuilderConfigurator for NoopConfigurator {
    fn configure<R: AsyncFileReader>(
        &self,
        reader_builder: ParquetRecordBatchStreamBuilder<R>,
    ) -> Result<ParquetRecordBatchStreamBuilder<R>> {
        Ok(reader_builder)
    }
}

/// A [`ReaderBuilderConfigurator`] that filters out rows whose value for a given
/// [`FixedSizeBinary`] column is not in the given set of allowed values
///
/// `BINARY_SIZE` must fit in a `i32`, as Arrow does not support larger arrays.
pub struct FilterFixedSizeBinaryConfigurator<const BINARY_SIZE: usize> {
    column_name: &'static str,
    keys: Arc<Vec<FixedSizeBinary<BINARY_SIZE>>>,
}

impl<const BINARY_SIZE: usize> FilterFixedSizeBinaryConfigurator<BINARY_SIZE> {
    pub fn new(column_name: &'static str, mut keys: Vec<FixedSizeBinary<BINARY_SIZE>>) -> Self {
        keys.sort_unstable();
        Self::with_sorted_keys(column_name, Arc::new(keys))
    }

    /// Same as [`Self::new`] but assumes keys are already sorted
    pub fn with_sorted_keys(
        column_name: &'static str,
        keys: Arc<Vec<FixedSizeBinary<BINARY_SIZE>>>,
    ) -> Self {
        Self { column_name, keys }
    }
}

impl<const BINARY_SIZE: usize> ReaderBuilderConfigurator
    for FilterFixedSizeBinaryConfigurator<BINARY_SIZE>
{
    fn configure<R: AsyncFileReader>(
        &self,
        reader_builder: ParquetRecordBatchStreamBuilder<R>,
    ) -> Result<ParquetRecordBatchStreamBuilder<R>> {
        let (column_idx, column) = reader_builder
            .schema()
            .column_with_name(self.column_name)
            .with_context(|| format!("No column names {}", self.column_name))?;
        ensure!(
            *column.data_type()
                == DataType::FixedSizeBinary(
                    i32::try_from(BINARY_SIZE).context("BINARY_SIZE overflows i32")?
                ),
            "Expected type FixedSizeBinary for column {}, got {:?}",
            self.column_name,
            column.data_type()
        );
        let keys = Arc::clone(&self.keys);
        let row_filter = RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
            // Only read the column we need for filtering
            ProjectionMask::roots(reader_builder.parquet_schema(), [column_idx]),
            move |batch| {
                // TODO: check 'type' column
                let mut matches =
                    arrow::array::builder::BooleanBufferBuilder::new(batch.num_rows());
                for key in batch
                    .column(column_idx)
                    .as_fixed_size_binary_opt()
                    .expect("key column is not a FixedSizeBinaryArray")
                {
                    // Can't panic because we check the schema before applying this row
                    // filter
                    let key: [u8; BINARY_SIZE] = key
                        .expect("key column contains a null")
                        .try_into()
                        .expect("unexpected FixedSizeBinary length");
                    matches.append(keys.binary_search(&FixedSizeBinary::from(key)).is_ok());
                }
                Ok(arrow::array::BooleanArray::new(matches.finish(), None))
            },
        ))]);
        Ok(reader_builder.with_row_filter(row_filter))
    }
}