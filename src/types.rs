// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::*;
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::data_type::AsBytes;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::metadata::{ParquetColumnIndex, ParquetOffsetIndex};
use rdst::RadixSort;

pub type KeysFilteredByColumnChunk<K> = Box<[Arc<[K]>]>;

/// Trait of types that can be used to select rows with `parquet_aramid`.
///
/// This implies support support for (in call order):
///
/// * checking file-level Elias-Fano indexes, using `as_ef_key` (optional)
/// * checking row group statistics, in [`IndexKey::check_column_chunk`] (optional)
/// * checking Bloom Filter, implemented through [`AsBytes`] (required)
/// * checking the page index, in [`IndexKey::check_page_index`] (optional)
pub trait IndexKey: AsBytes + Clone + Send + Sync + 'static {
    /// Returns this key as a value in the file-level Elias-Fano index, if it supports it.
    fn as_ef_key(&self) -> Option<usize>;

    /// For each row group in `row_groups_metadata`, returns the set of keys that may
    /// be contained in that group.
    ///
    /// Returns `None` when it cannot prune (ie. when all rows would be selected)
    fn check_column_chunk(
        keys: &Arc<[Self]>,
        statistics_converter: &StatisticsConverter,
        row_groups_metadata: &[RowGroupMetaData],
    ) -> Result<Option<KeysFilteredByColumnChunk<Self>>>;

    /// Given a page index, returns page ids within the index that may contain this key, as a
    /// boolean array.
    ///
    /// Returns `None` when it cannot prune (ie. when all rows would be selected)
    fn check_page_index<'a, I: IntoIterator<Item = &'a usize> + Copy>(
        keys: &[Self],
        statistics_converter: &StatisticsConverter<'a>,
        column_page_index: &ParquetColumnIndex,
        column_offset_index: &ParquetOffsetIndex,
        row_group_indices: I,
    ) -> Result<Option<BooleanBuffer>>;
}

/// Newtype for `[u8; N]` that implements [`AsBytes`]
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Copy)]
pub struct FixedSizeBinary<const N: usize>(pub [u8; N]);

impl<const N: usize> From<[u8; N]> for FixedSizeBinary<N> {
    fn from(value: [u8; N]) -> Self {
        FixedSizeBinary(value)
    }
}

impl<const N: usize> std::ops::Deref for FixedSizeBinary<N> {
    type Target = [u8; N];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const N: usize> AsBytes for FixedSizeBinary<N> {
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl<const N: usize> IndexKey for FixedSizeBinary<N> {
    #[inline(always)]
    fn as_ef_key(&self) -> Option<usize> {
        None
    }

    fn check_column_chunk(
        _keys: &Arc<[Self]>,
        _statistics_converter: &StatisticsConverter,
        _row_groups_metadata: &[RowGroupMetaData],
    ) -> Result<Option<Box<[Arc<[Self]>]>>> {
        // TODO
        Ok(None)
    }
    fn check_page_index<'a, I: IntoIterator<Item = &'a usize> + Copy>(
        _keys: &[Self],
        _statistics_converter: &StatisticsConverter<'a>,
        _column_page_index: &ParquetColumnIndex,
        _column_offset_index: &ParquetOffsetIndex,
        _row_group_indices: I,
    ) -> Result<Option<BooleanBuffer>> {
        // TODO
        Ok(None)
    }
}

/// Newtype for `impl AsRef<[u8]>` that implements [`AsBytes`]
#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Copy)]
pub struct Binary<T: AsRef<[u8]> + Clone + Sync + Send + 'static>(pub T);

impl<T: AsRef<[u8]> + Clone + Sync + Send + 'static> From<T> for Binary<T> {
    fn from(value: T) -> Self {
        Binary(value)
    }
}

impl<T: AsRef<[u8]> + Clone + Sync + Send + 'static> std::ops::Deref for Binary<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: AsRef<[u8]> + Clone + Sync + Send + 'static> AsBytes for Binary<T> {
    fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<T: AsRef<[u8]> + Clone + Sync + Send + 'static> IndexKey for Binary<T> {
    #[inline(always)]
    fn as_ef_key(&self) -> Option<usize> {
        None
    }

    fn check_column_chunk(
        _keys: &Arc<[Self]>,
        _statistics_converter: &StatisticsConverter,
        _row_groups_metadata: &[RowGroupMetaData],
    ) -> Result<Option<KeysFilteredByColumnChunk<Self>>> {
        // TODO
        Ok(None)
    }
    fn check_page_index<'a, I: IntoIterator<Item = &'a usize> + Copy>(
        _keys: &[Self],
        _statistics_converter: &StatisticsConverter<'a>,
        _column_page_index: &ParquetColumnIndex,
        _column_offset_index: &ParquetOffsetIndex,
        _row_group_indices: I,
    ) -> Result<Option<BooleanBuffer>> {
        // TODO
        Ok(None)
    }
}

macro_rules! impl_type {
    ($arrow_type:ty, $rust_type:ty) => {
        impl IndexKey for $rust_type {
            #[inline(always)]
            fn as_ef_key(&self) -> Option<usize> {
                usize::try_from(*self).ok()
            }

            fn check_column_chunk(
                keys: &Arc<[Self]>,
                statistics_converter: &StatisticsConverter,
                row_groups_metadata: &[RowGroupMetaData],
            ) -> Result<Option<KeysFilteredByColumnChunk<Self>>> {
                let mut sorted_keys: Vec<Self> = keys.iter().copied().collect();
                sorted_keys.radix_sort_unstable();
                let min_key = sorted_keys
                    .first()
                    .cloned()
                    .context("check_column_chunk got empty set of keys")?;
                let max_key = sorted_keys
                    .last()
                    .cloned()
                    .context("check_column_chunk got empty set of keys")?;

                let row_group_mins = statistics_converter
                    .row_group_mins(row_groups_metadata)
                    .context("Could not get row group statistics")?;
                let row_group_mins = row_group_mins
                    .as_primitive_opt::<$arrow_type>()
                    .with_context(|| {
                        format!(
                            "Could not interpret statistics as {} array",
                            std::any::type_name::<$arrow_type>()
                        )
                    })?;
                let row_group_maxes = statistics_converter
                    .row_group_maxes(row_groups_metadata)
                    .context("Could not get row group statistics")?;
                let row_group_maxes = row_group_maxes
                    .as_primitive_opt::<$arrow_type>()
                    .with_context(|| {
                        format!(
                            "Could not interpret statistics as {} array",
                            std::any::type_name::<$arrow_type>()
                        )
                    })?;
                if row_group_mins.nulls().is_some() || row_group_maxes.nulls().is_some() {
                    unimplemented!("missing column statistics for u64 column chunk")
                }

                Ok(Some(
                    std::iter::zip(row_group_mins.into_iter(), row_group_maxes.into_iter())
                        .map(|(row_group_min, row_group_max)| -> Arc<[Self]> {
                            // Can't fail, we checked before the arrays contain no nulls
                            let row_group_min = row_group_min.unwrap();
                            let row_group_max = row_group_max.unwrap();

                            if min_key >= row_group_min && max_key <= row_group_max {
                                // Shortcut, no need to bisect
                                return Arc::clone(keys);
                            }
                            if max_key < row_group_min || min_key > row_group_max {
                                // Shortcut, no need to bisect
                                return Arc::new([]);
                            }

                            // Discard keys too small to be in the row group
                            let filtered_keys = &sorted_keys
                                [sorted_keys.partition_point(|key| *key < row_group_min)..];
                            // Discard keys too large to be in the row group
                            let filtered_keys = &filtered_keys
                                [..filtered_keys.partition_point(|key| *key <= row_group_max)];

                            filtered_keys.into()
                        })
                        .collect(),
                ))
            }
            fn check_page_index<'a, I: IntoIterator<Item = &'a usize> + Copy>(
                keys: &[Self],
                statistics_converter: &StatisticsConverter<'a>,
                column_page_index: &ParquetColumnIndex,
                column_offset_index: &ParquetOffsetIndex,
                row_group_indices: I,
            ) -> Result<Option<BooleanBuffer>> {
                let min_key = keys
                    .iter()
                    .min()
                    .cloned()
                    .context("check_page_index got empty set of keys")?;
                let max_key = keys
                    .iter()
                    .max()
                    .cloned()
                    .context("check_page_index got empty set of keys")?;

                let data_page_mins = statistics_converter
                    .data_page_mins(column_page_index, column_offset_index, row_group_indices)
                    .context("Could not get row group statistics")?;
                let data_page_maxes = statistics_converter
                    .data_page_maxes(column_page_index, column_offset_index, row_group_indices)
                    .context("Could not get row group statistics")?;
                if data_page_mins.nulls().is_some() || data_page_maxes.nulls().is_some() {
                    unimplemented!("missing column statistics for u64 page")
                }
                Ok(Some(
                    arrow::compute::and(
                        // Discard row groups whose smallest value is greater than the largest key
                        &BooleanArray::from_unary(
                            data_page_mins
                                .as_primitive_opt::<$arrow_type>()
                                .with_context(|| {
                                    format!(
                                        "Could not interpret statistics as {} array",
                                        std::any::type_name::<$arrow_type>()
                                    )
                                })?,
                            |data_page_min| data_page_min <= max_key,
                        ),
                        // Discard row groups whose largest value is less than the smallest key
                        &BooleanArray::from_unary(
                            data_page_maxes
                                .as_primitive_opt::<$arrow_type>()
                                .with_context(|| {
                                    format!(
                                        "Could not interpret statistics as {} array",
                                        std::any::type_name::<$arrow_type>()
                                    )
                                })?,
                            |data_page_max| data_page_max >= min_key,
                        ),
                    )
                    .context("Could not build boolean array")?
                    .into_parts()
                    .0,
                ))
            }
        }
    };
}

impl_type!(UInt64Type, u64);
impl_type!(UInt32Type, u32);
impl_type!(UInt16Type, u16);
impl_type!(UInt8Type, u8);

impl_type!(Int64Type, i64);
impl_type!(Int32Type, i32);
impl_type!(Int16Type, i16);
impl_type!(Int8Type, i8);

// TODO: impl for all the date and time types
