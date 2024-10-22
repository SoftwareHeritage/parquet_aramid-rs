// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::atomic::AtomicU64;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Returned by [`TableScanInitMetrics`] methods
pub struct Timer<'a> {
    metric: &'a RwLock<Duration>,
    started_at: Instant,
}

impl<'a> Timer<'a> {
    fn new(metric: &'a RwLock<Duration>) -> Self {
        Timer {
            metric,
            started_at: Instant::now(),
        }
    }
}

impl<'a> Drop for Timer<'a> {
    fn drop(&mut self) {
        *self.metric.write().unwrap() += self.started_at.elapsed();
    }
}

#[derive(Default)]
pub struct Timing(RwLock<Duration>);

impl Timing {
    pub fn timer(&self) -> Timer<'_> {
        Timer::new(&self.0)
    }

    pub fn get(&self) -> Duration {
        *self.0.read().unwrap()
    }

    pub fn add(&self, other: Duration) {
        *self.0.write().unwrap() += other;
    }
}

impl std::fmt::Debug for Timing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.get().fmt(f)
    }
}

#[derive(Debug, Default)]
pub struct TableScanInitMetrics {
    pub files_pruned_by_ef_index: u64,
    pub files_selected_by_ef_index: u64,
    pub row_groups_pruned_by_statistics: u64,
    pub row_groups_selected_by_statistics: u64,
    pub row_groups_pruned_by_bloom_filters: u64,
    pub row_groups_selected_by_bloom_filters: u64,
    pub row_groups_pruned_by_page_index: u64,
    pub row_groups_selected_by_page_index: u64,
    pub rows_pruned_by_page_index: usize,
    pub rows_selected_by_page_index: usize,

    pub ef_file_index_eval_time: Timing,
    pub open_builder_time: Timing,
    pub read_metadata_time: Timing,
    pub eval_row_groups_statistics_time: Timing,
    pub filter_by_row_groups_statistics_time: Timing,
    pub read_bloom_filter_time: Timing,
    pub eval_bloom_filter_time: Timing,
    pub eval_page_index_time: Timing,
    pub total_time: Timing,
}

impl std::iter::Sum for TableScanInitMetrics {
    fn sum<I: std::iter::Iterator<Item = TableScanInitMetrics>>(it: I) -> Self {
        let mut sum = Self::default();
        {
            for item in it {
                sum.files_pruned_by_ef_index += item.files_pruned_by_ef_index;
                sum.files_selected_by_ef_index += item.files_selected_by_ef_index;
                sum.row_groups_pruned_by_statistics += item.row_groups_pruned_by_statistics;
                sum.row_groups_selected_by_statistics += item.row_groups_selected_by_statistics;
                sum.row_groups_pruned_by_bloom_filters += item.row_groups_pruned_by_bloom_filters;
                sum.row_groups_selected_by_bloom_filters +=
                    item.row_groups_selected_by_bloom_filters;
                sum.row_groups_pruned_by_page_index += item.row_groups_pruned_by_page_index;
                sum.row_groups_selected_by_page_index += item.row_groups_selected_by_page_index;
                sum.rows_pruned_by_page_index += item.rows_pruned_by_page_index;
                sum.rows_selected_by_page_index += item.rows_selected_by_page_index;
                sum.open_builder_time.add(item.open_builder_time.get());
                sum.read_metadata_time.add(item.read_metadata_time.get());
                sum.ef_file_index_eval_time
                    .add(item.ef_file_index_eval_time.get());
                sum.eval_row_groups_statistics_time
                    .add(item.eval_row_groups_statistics_time.get());
                sum.filter_by_row_groups_statistics_time
                    .add(item.filter_by_row_groups_statistics_time.get());
                sum.read_bloom_filter_time
                    .add(item.read_bloom_filter_time.get());
                sum.eval_bloom_filter_time
                    .add(item.eval_bloom_filter_time.get());
                sum.eval_page_index_time
                    .add(item.eval_page_index_time.get());
                sum.total_time.add(item.total_time.get());
            }
        }
        sum
    }
}

#[derive(Debug, Default)]
pub struct TableScanMetrics {
    pub rows_pruned_by_row_filter: AtomicU64,
    pub rows_selected_by_row_filter: AtomicU64,

    pub row_filter_eval_time: Timing,
    pub row_filter_eval_loop_time: Timing,
}
