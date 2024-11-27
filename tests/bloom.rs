// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Tests Bloom Filter evaluation for correctness and optimality

use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow::datatypes::*;

use parquet_aramid::metrics::*;
use parquet_aramid::reader_builder_config::*;
use parquet_aramid::types::IndexKey;
use parquet_aramid::Table;

mod common;
use common::*;

const PAGES_PER_GROUP: u64 = 10;
const NUM_FILES: u64 = 10;
const ROWS_PER_GROUP: u64 = ROWS_PER_PAGE * PAGES_PER_GROUP;
const ROWS_PER_FILE: u64 = ROWS_PER_GROUP * ROW_GROUPS_PER_FILE;
const NUM_ROW_GROUPS: u64 = ROW_GROUPS_PER_FILE * NUM_FILES;

async fn make_table(db_path: &Path, ef_indexes_path: &Path) -> Result<Table> {
    // generates [[0, 4, 8, 12], [1, 5, 9, 13], [2, 6, 10, 14], [3, 7, 11, 15]]
    let haystack = (0..NUM_ROW_GROUPS).map(|i| {
        (0..ROWS_PER_GROUP)
            .map(|j| j * ROWS_PER_GROUP + i)
            .collect()
    });

    let with_bloom_filters = true;
    write_database(haystack, db_path, with_bloom_filters).context("Could not write DB")?;

    let url = url::Url::from_file_path(db_path)
        .map_err(|()| anyhow!("Could not convert temp dir path to URL"))?;
    let (store, path) = object_store::parse_url(&url).context("Could not parse temp dir URL")?;
    let store = store.into();

    let table = Table::new(store, path, ef_indexes_path.to_owned())
        .await
        .context("Could not open table")?;

    Ok(table)
}

fn make_row<Needle>(_needle: Needle, key: u64) -> Row {
    // Inverts the order built in make_table()'s haystack
    let row_group_id = key % ROWS_PER_GROUP;
    let row_id = row_group_id * ROWS_PER_GROUP + key / ROWS_PER_GROUP;

    Row {
        key_u64: key,
        key_u64_bloom: key,
        key_binary_bloom: format!("item{:>21}", key).into_bytes().into(),
        key_fixedsizebinary_bloom: ar_row::FixedSizeBinary(
            format!("item{:>21}", key).into_bytes()[..]
                .try_into()
                .unwrap(),
        ),
        row_id,
        page_id: row_id % ROWS_PER_FILE % ROWS_PER_GROUP / ROWS_PER_PAGE,
        batch_id: row_id / ROWS_PER_GROUP,
        row_group_id: row_id % ROWS_PER_FILE / ROWS_PER_GROUP,
        file_id: row_id / ROWS_PER_FILE,
    }
}

/// Tests u64, Binary, and FixedSizeBinary.
///
/// They are all tested in a single test because generating the data file takes a while,
/// so we should avoid doing it more than once.
#[tokio::test]
async fn test_bloom() -> Result<()> {
    let db_dir = tempfile::tempdir().context("Could not get tempdir")?;
    let indexes_dir = tempfile::tempdir().context("Could not get tempdir")?;
    let ef_indexes_path = indexes_dir.path().join("ef");

    let table = make_table(db_dir.path(), &ef_indexes_path).await?;

    test_bloom_inner::<u64, _>(|keys: Vec<u64>| {
        let configurator = FilterPrimitiveConfigurator::<UInt64Type>::with_sorted_keys(
            "key_u64_bloom",
            Arc::new(keys.clone()),
        );
        check_results::<u64>(
            &table,
            keys.clone(),
            "key_u64_bloom",
            keys,
            configurator,
            make_row::<u64>,
        )
    })
    .await?;
    test_bloom_inner::<parquet_aramid::types::Binary<Box<[u8]>>, _>(|keys: Vec<u64>| {
        let make_needle = |key: u64| {
            parquet_aramid::types::Binary(
                format!("item{:>21}", key).into_bytes().into_boxed_slice(),
            )
        };
        let configurator = FilterBinaryConfigurator::<i32, _>::with_sorted_keys(
            "key_binary_bloom",
            Arc::new(keys.iter().copied().map(make_needle).collect()),
        );
        check_results::<parquet_aramid::types::Binary<Box<[u8]>>>(
            &table,
            keys.iter().copied().map(make_needle).collect(),
            "key_binary_bloom",
            keys.clone(),
            configurator,
            make_row::<parquet_aramid::types::Binary<Box<[u8]>>>,
        )
    })
    .await?;
    test_bloom_inner::<parquet_aramid::types::FixedSizeBinary<{ FIXEDSIZEBINARY_SIZE as _ }>, _>(
        |keys: Vec<u64>| {
            let make_needle = |key| parquet_aramid::types::FixedSizeBinary::<{FIXEDSIZEBINARY_SIZE as _}>(
                format!("item{:>21}", key).into_bytes()[..]
                    .try_into()
                    .unwrap()
            );
            let configurator = FilterFixedSizeBinaryConfigurator::<{ FIXEDSIZEBINARY_SIZE as _ }>::with_sorted_keys(
                "key_fixedsizebinary_bloom",
                Arc::new(keys.iter().copied().map(make_needle).collect()),
            );
            check_results::<parquet_aramid::types::FixedSizeBinary::<{ FIXEDSIZEBINARY_SIZE as _ }>>(
                &table,
                keys.iter()
                    .copied()
                    .map(make_needle)
                    .collect(),
                "key_fixedsizebinary_bloom",
                keys.clone(),
                configurator,
                make_row::<parquet_aramid::types::FixedSizeBinary<{FIXEDSIZEBINARY_SIZE as _ }>>,
            )
        },
    )
    .await?;

    Ok(())
}

async fn test_bloom_inner<
    Needle: IndexKey + Ord + Clone + std::fmt::Debug,
    F: Future<Output = Result<(Vec<Needle>, TableScanInitMetrics)>>,
>(
    check_results: impl Fn(Vec<u64>) -> F,
) -> Result<()> {
    // All results in the same page, and every row group matches the stats
    let metrics1 = check_results(vec![1000]).await?;
    let metrics2 = check_results(vec![1001]).await?;
    let metrics3 = check_results(vec![1234]).await?;
    let metrics4 = check_results(vec![1000, 1001]).await?;
    // All results in the same row group, but different pages
    let metrics5 = check_results(vec![1000, 1001, 1050]).await?;
    for (needles, metrics) in [metrics1, metrics2, metrics3, metrics4, metrics5] {
        match metrics {
            TableScanInitMetrics {
                files_pruned_by_ef_index: 0,
                files_selected_by_ef_index: 0,
                row_groups_selection:
                    RowGroupsSelectionMetrics {
                        row_groups_pruned_by_statistics: 0,
                        row_groups_selected_by_statistics: NUM_ROW_GROUPS,
                        row_groups_pruned_by_bloom_filters,
                        row_groups_selected_by_bloom_filters,
                        eval_row_groups_statistics_time: _,
                        filter_by_row_groups_statistics_time: _,
                        read_bloom_filter_time: _,
                        eval_bloom_filter_time: _,
                    },
                rows_selection:
                    RowsSelectionMetrics {
                        rows_pruned_by_page_index: _,
                        rows_selected_by_page_index: _,
                        row_groups_pruned_by_page_index: 0, // not implemented yet
                        row_groups_selected_by_page_index: 0,
                        eval_page_index_time: _,
                    },
                ef_file_index_eval_time: _,
                open_builder_time: _,
                read_metadata_time: _,
                total_time: _,
            } => {
                assert_eq!(
                    row_groups_pruned_by_bloom_filters + row_groups_selected_by_bloom_filters,
                    NUM_ROW_GROUPS,
                    "Sum of number of row groups pruned + selected by Bloom Filters does not match the total number of row groups for needles {:?}, {:#?}",
                    needles,
                    metrics
                );

                // Bloom Filters are probabilistic, so we can't use equality.
                assert!(
                    row_groups_selected_by_bloom_filters >= 1,
                    "No row group was selected by bloom filters for needles {:?}, {:#?}",
                    needles,
                    metrics
                );
                assert!(
                    row_groups_selected_by_bloom_filters <= 5,
                    "Surprisingly high number of Bloom Filter false positives for needles {:?}, {:#?}",
                    needles,
                    metrics
                );
            }
            _ => panic!(
                "Mismatched metrics for needles {:?}: {:#?}",
                needles, metrics
            ),
        }
    }

    // All results in different row groups
    let (needles, metrics) = check_results(vec![0, 1, ROWS_PER_GROUP]).await?;
    match metrics {
        TableScanInitMetrics {
            files_pruned_by_ef_index: 0,
            files_selected_by_ef_index: 0,
            row_groups_selection:
                RowGroupsSelectionMetrics {
                    row_groups_pruned_by_statistics: 0,
                    row_groups_selected_by_statistics: NUM_ROW_GROUPS,
                    row_groups_pruned_by_bloom_filters,
                    row_groups_selected_by_bloom_filters,
                    eval_row_groups_statistics_time: _,
                    filter_by_row_groups_statistics_time: _,
                    read_bloom_filter_time: _,
                    eval_bloom_filter_time: _,
                },
            rows_selection:
                RowsSelectionMetrics {
                    rows_pruned_by_page_index: _,
                    rows_selected_by_page_index: _,
                    row_groups_pruned_by_page_index: 0, // not implemented yet
                    row_groups_selected_by_page_index: 0,
                    eval_page_index_time: _,
                },
            ef_file_index_eval_time: _,
            open_builder_time: _,
            read_metadata_time: _,
            total_time: _,
        } => {
            assert_eq!(
                row_groups_pruned_by_bloom_filters + row_groups_selected_by_bloom_filters,
                NUM_ROW_GROUPS,
                "Sum of number of row groups pruned + selected by Bloom Filters does not match the total number of row groups for needles {:?}, {:#?}",
                needles,
                metrics
            );

            // Bloom Filters are probabilistic, so we can't use equality.
            assert!(
                row_groups_selected_by_bloom_filters >= 1,
                "No row group was selected by bloom filters for needles {:?}, {:#?}",
                needles,
                metrics
            );
            assert!(
                row_groups_selected_by_bloom_filters <= 5,
                "Surprisingly high number of Bloom Filter false positives for needles {:?}, {:#?}",
                needles,
                metrics
            );
        }
        _ => panic!(
            "Mismatched metrics for same-file needles {:?}: {:#?}",
            needles, metrics
        ),
    }

    Ok(())
}
