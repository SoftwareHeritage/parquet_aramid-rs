// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use arrow::array::*;
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};

use super::Configurator;
use crate::types::{Binary, FixedSizeBinary};

/// A [`ReaderBuilderConfigurator`] that filters out rows whose value for a given
/// [`Binary`] or [`LargeBinaryArray|] column is not in the given set of allowed values
///
/// `BINARY_SIZE` must fit in a `i32`, as Arrow does not support larger arrays.
pub struct FilterBinaryConfigurator<
    O: OffsetSizeTrait,
    Key: AsRef<[u8]> + Clone + Sync + Send + 'static,
> {
    column_name: &'static str,
    keys: Arc<Vec<Binary<Key>>>,
    _marker: PhantomData<O>,
}

impl<O: OffsetSizeTrait, Key: AsRef<[u8]> + Clone + Ord + Sync + Send + 'static>
    FilterBinaryConfigurator<O, Key>
{
    pub fn new(column_name: &'static str, mut keys: Vec<Binary<Key>>) -> Self {
        keys.sort_unstable();
        Self::with_sorted_keys(column_name, Arc::new(keys))
    }
}

impl<O: OffsetSizeTrait, Key: AsRef<[u8]> + Clone + Sync + Send + 'static>
    FilterBinaryConfigurator<O, Key>
{
    /// Same as [`Self::new`] but assumes keys are already sorted
    pub fn with_sorted_keys(column_name: &'static str, keys: Arc<Vec<Binary<Key>>>) -> Self {
        Self {
            column_name,
            keys,
            _marker: PhantomData,
        }
    }
}

impl<O: OffsetSizeTrait, Key: AsRef<[u8]> + Clone + Sync + Send + 'static> Configurator
    for FilterBinaryConfigurator<O, Key>
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
            *column.data_type() == GenericBinaryType::<O>::DATA_TYPE,
            "Expected type {:?} for column {}, got {:?}",
            GenericBinaryType::<O>::DATA_TYPE,
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
                    .as_binary_opt::<O>()
                    .expect("key column is not a BinaryArray");
                for key in haystack {
                    // Can't panic because we check the schema before applying this row
                    // filter
                    let key = key.expect("key column contains a null");
                    matches.append(
                        needles
                            .binary_search_by_key(&key, |needle| needle.as_ref())
                            .is_ok(),
                    );
                }
                Ok(arrow::array::BooleanArray::new(matches.finish(), None))
            },
        ))]);
        Ok(reader_builder.with_row_filter(row_filter))
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

impl<const BINARY_SIZE: usize> Configurator for FilterFixedSizeBinaryConfigurator<BINARY_SIZE> {
    fn configure_stream_builder<R: AsyncFileReader>(
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
        let needles = Arc::clone(&self.keys);
        let row_filter = RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
            // Only read the column we need for filtering
            ProjectionMask::roots(reader_builder.parquet_schema(), [column_idx]),
            move |batch| {
                let mut matches =
                    arrow::array::builder::BooleanBufferBuilder::new(batch.num_rows());
                let haystack = batch
                    .column(0) // we selected a single column
                    .as_fixed_size_binary_opt()
                    .expect("key column is not a FixedSizeBinaryArray");
                for key in haystack {
                    // Can't panic because we check the schema before applying this row
                    // filter
                    let key: [u8; BINARY_SIZE] = key
                        .expect("key column contains a null")
                        .try_into()
                        .expect("unexpected FixedSizeBinary length");
                    matches.append(needles.binary_search(&FixedSizeBinary::from(key)).is_ok());
                }
                Ok(arrow::array::BooleanArray::new(matches.finish(), None))
            },
        ))]);
        Ok(reader_builder.with_row_filter(row_filter))
    }
}
