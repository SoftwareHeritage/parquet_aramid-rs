// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use arrow::array::*;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use rdst::RadixSort;

use super::Configurator;

/// A [`ReaderBuilderConfigurator`] that filters out rows whose value for a given
/// [primitive](ArrowPrimitiveType) column is not in the given set of allowed values
pub struct FilterPrimitiveConfigurator<K: ArrowPrimitiveType<Native: Ord> + Send + Sync + 'static> {
    column_name: &'static str,
    keys: Arc<Vec<K::Native>>,
}

impl<K: ArrowPrimitiveType<Native: Ord> + Send + Sync + 'static> FilterPrimitiveConfigurator<K> {
    pub fn new(column_name: &'static str, mut keys: Vec<K::Native>) -> Self
    where
        K::Native: rdst::RadixKey,
    {
        keys.radix_sort_unstable();
        Self::with_sorted_keys(column_name, Arc::new(keys))
    }

    /// Less efficient implementation of [`Self::new`] for types that don't implement
    /// [`rdst::RadixKey`]
    pub fn slow_new(column_name: &'static str, mut keys: Vec<K::Native>) -> Self {
        keys.sort_unstable();
        Self::with_sorted_keys(column_name, Arc::new(keys))
    }

    /// Same as [`Self::new`] but assumes keys are already sorted
    pub fn with_sorted_keys(column_name: &'static str, keys: Arc<Vec<K::Native>>) -> Self {
        Self { column_name, keys }
    }
}

impl<K: ArrowPrimitiveType<Native: Ord> + Send + Sync + 'static> Configurator
    for FilterPrimitiveConfigurator<K>
{
    fn configure_stream_builder<R: AsyncFileReader>(
        &self,
        reader_builder: ParquetRecordBatchStreamBuilder<R>,
    ) -> Result<ParquetRecordBatchStreamBuilder<R>> {
        let (column_idx, column) = reader_builder
            .schema()
            .column_with_name(self.column_name)
            .with_context(|| format!("No column names {}", self.column_name))?;
        ensure!(
            *column.data_type() == K::DATA_TYPE,
            "Expected primitive type {} for column {}, got {:?}",
            K::DATA_TYPE,
            self.column_name,
            column.data_type()
        );
        let needles = Arc::clone(&self.keys);
        let row_filter = RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
            // Only read the column we need for filtering
            ProjectionMask::roots(reader_builder.parquet_schema(), [column_idx]),
            move |batch| {
                let mut matches =
                    arrow::array::builder::BooleanBufferBuilder::new(batch.num_rows());
                let haystack = batch
                    .column(0) // we selected a single column
                    .as_primitive_opt::<K>()
                    .unwrap_or_else(|| {
                        panic!("key column is not a primitive array of {}", K::DATA_TYPE)
                    });
                for key in haystack {
                    // Can't panic because we check the schema before applying this row
                    // filter
                    let key: K::Native = key.expect("key column contains a null");
                    matches.append(needles.binary_search(&key).is_ok());
                }
                Ok(arrow::array::BooleanArray::new(matches.finish(), None))
            },
        ))]);
        Ok(reader_builder.with_row_filter(row_filter))
    }
}
