// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Every key in the database is unique, and keys are `u64` from 0 to 1_000_000, in that order.

use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow::datatypes::*;

use parquet_aramid::config::FilterPrimitiveConfigurator;
use parquet_aramid::metrics::*;
use parquet_aramid::Table;

mod common;
use common::*;

const PAGES_PER_GROUP: u64 = 10;
const NUM_FILES: u64 = 10;
const ROWS_PER_GROUP: u64 = ROWS_PER_PAGE * PAGES_PER_GROUP;
const ROWS_PER_FILE: u64 = ROWS_PER_GROUP * ROW_GROUPS_PER_FILE;
const NUM_ROW_GROUPS: u64 = ROW_GROUPS_PER_FILE * NUM_FILES;

async fn make_table(db_path: &Path, ef_indexes_path: &Path) -> Result<Table> {
    // generates [[0, 1, 2, 3], [4, 5, 6, 7], ...]
    let haystack =
        (0..NUM_ROW_GROUPS).map(|i| (i * ROWS_PER_GROUP..(i + 1) * ROWS_PER_GROUP).collect());

    let with_bloom_filters = false; // too slow in debug build
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

pub async fn check_u64_results(
    table: &Table,
    needles: Vec<u64>,
    make_row: impl FnMut(u64, u64) -> Row,
) -> Result<(Vec<u64>, TableScanInitMetrics)> {
    let configurator = FilterPrimitiveConfigurator::<UInt64Type>::with_sorted_keys(
        "key_u64",
        Arc::new(needles.clone()),
    );
    check_results::<u64>(
        table,
        needles.clone(),
        "key_u64",
        needles,
        configurator,
        make_row,
    )
    .await
}

fn make_row<Needle>(_needle: Needle, key: u64) -> Row {
    let row_id = key;
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

#[tokio::test]
async fn test_without_ef() -> Result<()> {
    let db_dir = tempfile::tempdir().context("Could not get tempdir")?;
    let indexes_dir = tempfile::tempdir().context("Could not get tempdir")?;
    let ef_indexes_path = indexes_dir.path().join("ef");

    let table = make_table(db_dir.path(), &ef_indexes_path).await?;

    // All results in the same page
    let metrics1 = check_u64_results(&table, vec![0], &make_row::<u64>).await?;
    let metrics2 = check_u64_results(&table, vec![1], &make_row::<u64>).await?;
    let metrics3 = check_u64_results(&table, vec![1234], &make_row::<u64>).await?;
    let metrics4 = check_u64_results(&table, vec![0, 1], &make_row::<u64>).await?;
    for (needles, metrics) in [metrics1, metrics2, metrics3, metrics4] {
        match metrics {
            TableScanInitMetrics {
                files_pruned_by_ef_index: 0,
                files_selected_by_ef_index: 0,
                row_groups_selection:
                    RowGroupsSelectionMetrics {
                        row_groups_pruned_by_statistics,
                        row_groups_selected_by_statistics: 1,
                        row_groups_pruned_by_bloom_filters: 0, // no BF on the 'keys' column
                        row_groups_selected_by_bloom_filters: 0,
                        eval_row_groups_statistics_time: _,
                        filter_by_row_groups_statistics_time: _,
                        read_bloom_filter_time: _,
                        eval_bloom_filter_time: _,
                    },
                rows_selection:
                    RowsSelectionMetrics {
                        rows_pruned_by_page_index,
                        rows_selected_by_page_index,
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
                    row_groups_pruned_by_statistics,
                    NUM_ROW_GROUPS - 1,
                    "mismatched row_groups_pruned_by_statistics for same-page needles {:?}, {:#?}",
                    needles,
                    metrics
                );
                assert_eq!(
                    rows_selected_by_page_index as u64, ROWS_PER_PAGE,
                    "mismatched rows_selected_by_page_index for same-page needles {:?}, {:#?}",
                    needles, metrics
                );
                assert_eq!(
                    rows_pruned_by_page_index as u64,
                    (PAGES_PER_GROUP - 1) * ROWS_PER_PAGE,
                    "mismatched rows_pruned_by_page_index for same-page needles {:?}, {:#?}",
                    needles,
                    metrics
                );
            }
            _ => panic!(
                "Mismatched metrics for same-page needles {:?}: {:#?}",
                needles, metrics
            ),
        }
    }

    // All results in the same row group, but different pages
    let (needles, metrics) = check_u64_results(&table, vec![0, 1, 50], &make_row::<u64>).await?;
    match metrics {
        TableScanInitMetrics {
            files_pruned_by_ef_index: 0,
            files_selected_by_ef_index: 0,
            row_groups_selection:
                RowGroupsSelectionMetrics {
                    row_groups_pruned_by_statistics,
                    row_groups_selected_by_statistics: 1,
                    row_groups_pruned_by_bloom_filters: 0, // write_database() does not build BFs
                    row_groups_selected_by_bloom_filters: 0,
                    eval_row_groups_statistics_time: _,
                    filter_by_row_groups_statistics_time: _,
                    read_bloom_filter_time: _,
                    eval_bloom_filter_time: _,
                },
            rows_selection:
                RowsSelectionMetrics {
                    rows_pruned_by_page_index,
                    rows_selected_by_page_index,
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
                row_groups_pruned_by_statistics,
                NUM_ROW_GROUPS - 1,
                "mismatched row_groups_pruned_by_statistics for same-group needles {:?}, {:#?}",
                needles,
                metrics
            );
            assert_eq!(
                rows_selected_by_page_index as u64,
                ROWS_PER_PAGE * 6, // pages 0 and 5 are expected results, pages 1 to 4 are
                // false positives
                "mismatched rows_selected_by_page_index for same-group needles {:?}, {:#?}",
                needles,
                metrics
            );
            assert_eq!(
                rows_pruned_by_page_index as u64,
                (PAGES_PER_GROUP - 6) * ROWS_PER_PAGE,
                "mismatched rows_pruned_by_page_index for same-group needles {:?}, {:#?}",
                needles,
                metrics
            );
        }
        _ => panic!(
            "Mismatched metrics for same-group needles {:?}: {:#?}",
            needles, metrics
        ),
    }

    // All results in the same file, but two different row groups
    let (needles, metrics) =
        check_u64_results(&table, vec![0, 1, ROWS_PER_GROUP], &make_row::<u64>).await?;
    match metrics {
        TableScanInitMetrics {
            files_pruned_by_ef_index: 0,
            files_selected_by_ef_index: 0,
            row_groups_selection:
                RowGroupsSelectionMetrics {
                    row_groups_pruned_by_statistics,
                    row_groups_selected_by_statistics: 2,
                    row_groups_pruned_by_bloom_filters: 0, // write_database() does not build BFs
                    row_groups_selected_by_bloom_filters: 0,
                    eval_row_groups_statistics_time: _,
                    filter_by_row_groups_statistics_time: _,
                    read_bloom_filter_time: _,
                    eval_bloom_filter_time: _,
                },
            rows_selection:
                RowsSelectionMetrics {
                    rows_pruned_by_page_index,
                    rows_selected_by_page_index,
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
                row_groups_pruned_by_statistics,
                NUM_ROW_GROUPS - 2,
                "mismatched row_groups_pruned_by_statistics for same-file needles {:?}, {:#?}",
                needles,
                metrics
            );
            assert_eq!(
                rows_selected_by_page_index as u64,
                (PAGES_PER_GROUP + 1) * ROWS_PER_PAGE, // pages 0 and 10 are expected results,
                // pages pages 1 to 9 are false
                // positives
                "mismatched rows_selected_by_page_index for same-file needles {:?}, {:#?}",
                needles,
                metrics
            );
            assert_eq!(
                rows_pruned_by_page_index as u64,
                (PAGES_PER_GROUP - 1) * ROWS_PER_PAGE, // pages 11 to 19 (inclusive)
                "mismatched rows_pruned_by_page_index for same-file needles {:?}, {:#?}",
                needles,
                metrics
            );
        }
        _ => panic!(
            "Mismatched metrics for same-file needles {:?}: {:#?}",
            needles, metrics
        ),
    }

    // Results from different files
    let (needles, metrics) = check_u64_results(&table, vec![12, 1234], &make_row::<u64>).await?;
    match metrics {
        TableScanInitMetrics {
            files_pruned_by_ef_index: 0,
            files_selected_by_ef_index: 0,
            row_groups_selection:
                RowGroupsSelectionMetrics {
                    row_groups_pruned_by_statistics,
                    row_groups_selected_by_statistics: 2,
                    row_groups_pruned_by_bloom_filters: 0, // write_database() does not build BFs
                    row_groups_selected_by_bloom_filters: 0,
                    eval_row_groups_statistics_time: _,
                    filter_by_row_groups_statistics_time: _,
                    read_bloom_filter_time: _,
                    eval_bloom_filter_time: _,
                },
            rows_selection:
                RowsSelectionMetrics {
                    rows_pruned_by_page_index,
                    rows_selected_by_page_index,
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
                row_groups_pruned_by_statistics,
                NUM_ROW_GROUPS - 2,
                "mismatched row_groups_pruned_by_statistics for different-file needles {:?}, {:#?}",
                needles,
                metrics
            );
            assert_eq!(
                rows_selected_by_page_index as u64,
                130, // most pages in the 2 selected groups (excludes the first one,
                // and the last handful)
                "mismatched rows_selected_by_page_index for different-file needles {:?}, {:#?}",
                needles,
                metrics
            );
            assert_eq!(
                rows_pruned_by_page_index as u64,
                10 + 60, // pages 0 and 123 to 129 (inclusive)
                "mismatched rows_pruned_by_page_index for different-file needles {:?}, {:#?}",
                needles,
                metrics
            );
        }
        _ => panic!(
            "Mismatched metrics for different-file needles {:?}: {:#?}",
            needles, metrics
        ),
    }

    Ok(())
}

#[tokio::test]
async fn test_with_ef() -> Result<()> {
    use epserde::ser::Serialize;
    let db_dir = tempfile::tempdir().context("Could not get tempdir")?;
    let indexes_dir = tempfile::tempdir().context("Could not get tempdir")?;
    let ef_indexes_path = indexes_dir.path().join("ef");

    let table = make_table(db_dir.path(), &ef_indexes_path).await?;

    std::fs::create_dir_all(&ef_indexes_path).unwrap();

    for file in &table.files {
        // Build index
        let ef_values = file
            .build_ef_index("key_u64")
            .await
            .expect("Could not build Elias-Fano index");

        // Write index to disk
        let index_path = file.ef_index_path("key_u64");
        let mut ef_file = std::fs::File::create_new(&index_path).unwrap();
        ef_values
            .serialize(&mut ef_file)
            .expect("Could not serialize Elias-Fano index");
    }

    table
        .mmap_ef_index("key_u64")
        .context("Could not mmap EF indexes")?;

    // All results in the same page
    let metrics1 = check_u64_results(&table, vec![0], &make_row::<u64>).await?;
    let metrics2 = check_u64_results(&table, vec![1], &make_row::<u64>).await?;
    let metrics3 = check_u64_results(&table, vec![1234], &make_row::<u64>).await?;
    let metrics4 = check_u64_results(&table, vec![0, 1], &make_row::<u64>).await?;
    for (needles, metrics) in [metrics1, metrics2, metrics3, metrics4] {
        match metrics {
            TableScanInitMetrics {
                files_pruned_by_ef_index: 9,
                files_selected_by_ef_index: 1,
                row_groups_selection:
                    RowGroupsSelectionMetrics {
                        row_groups_pruned_by_statistics,
                        row_groups_selected_by_statistics: 1,
                        row_groups_pruned_by_bloom_filters: 0, // write_database() does not build BFs
                        row_groups_selected_by_bloom_filters: 0,
                        eval_row_groups_statistics_time: _,
                        filter_by_row_groups_statistics_time: _,
                        read_bloom_filter_time: _,
                        eval_bloom_filter_time: _,
                    },
                rows_selection:
                    RowsSelectionMetrics {
                        rows_pruned_by_page_index,
                        rows_selected_by_page_index,
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
                    row_groups_pruned_by_statistics,
                    ROW_GROUPS_PER_FILE - 1,
                    "mismatched row_groups_pruned_by_statistics for same-page needles {:?}, {:#?}",
                    needles,
                    metrics
                );
                assert_eq!(
                    rows_selected_by_page_index as u64, ROWS_PER_PAGE,
                    "mismatched rows_selected_by_page_index for same-page needles {:?}, {:#?}",
                    needles, metrics
                );
                assert_eq!(
                    rows_pruned_by_page_index as u64,
                    (PAGES_PER_GROUP - 1) * ROWS_PER_PAGE,
                    "mismatched rows_pruned_by_page_index for same-page needles {:?}, {:#?}",
                    needles,
                    metrics
                );
            }
            _ => panic!(
                "Mismatched metrics for same-page needles {:?}: {:#?}",
                needles, metrics
            ),
        }
    }

    // Results from different files
    let (needles, metrics) = check_u64_results(&table, vec![12, 1234], &make_row::<u64>).await?;
    match metrics {
        TableScanInitMetrics {
            files_pruned_by_ef_index: 8,
            files_selected_by_ef_index: 2,
            row_groups_selection:
                RowGroupsSelectionMetrics {
                    row_groups_pruned_by_statistics,
                    row_groups_selected_by_statistics: 2, // one in each file
                    row_groups_pruned_by_bloom_filters: 0, // write_database() does not build BFs
                    row_groups_selected_by_bloom_filters: 0,
                    eval_row_groups_statistics_time: _,
                    filter_by_row_groups_statistics_time: _,
                    read_bloom_filter_time: _,
                    eval_bloom_filter_time: _,
                },
            rows_selection:
                RowsSelectionMetrics {
                    rows_pruned_by_page_index,
                    rows_selected_by_page_index,
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
                row_groups_pruned_by_statistics,
                ROW_GROUPS_PER_FILE * 2 - 2, // two files opened, all but one in each file was pruned
                "mismatched row_groups_pruned_by_statistics for different-file needles {:?}, {:#?}",
                needles,
                metrics
            );
            assert_eq!(
                rows_selected_by_page_index as u64,
                20, // no false positive, there is only one needle per file so no issue with
                // the "convex hull" matching other pages
                "mismatched rows_selected_by_page_index for different-file needles {:?}, {:#?}",
                needles,
                metrics
            );
            assert_eq!(
                rows_pruned_by_page_index as u64,
                (PAGES_PER_GROUP * 2 - 2) * ROWS_PER_PAGE,
                "mismatched rows_pruned_by_page_index for different-file needles {:?}, {:#?}",
                needles,
                metrics
            );
        }
        _ => panic!(
            "Mismatched metrics for different-file needles {:?}: {:#?}",
            needles, metrics
        ),
    }

    Ok(())
}
