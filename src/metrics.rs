// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

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

impl Drop for Timer<'_> {
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

    pub row_groups_selection: RowGroupsSelectionMetrics,
    pub rows_selection: RowsSelectionMetrics,

    pub ef_file_index_eval_time: Timing,
    pub open_builder_time: Timing,
    pub read_metadata_time: Timing,
    pub total_time: Timing,
}

impl std::ops::AddAssign for TableScanInitMetrics {
    fn add_assign(&mut self, rhs: Self) {
        self.files_pruned_by_ef_index += rhs.files_pruned_by_ef_index;
        self.files_selected_by_ef_index += rhs.files_selected_by_ef_index;
        self.row_groups_selection += rhs.row_groups_selection;
        self.rows_selection += rhs.rows_selection;
        self.open_builder_time.add(rhs.open_builder_time.get());
        self.read_metadata_time.add(rhs.read_metadata_time.get());
        self.ef_file_index_eval_time
            .add(rhs.ef_file_index_eval_time.get());
        self.total_time.add(rhs.total_time.get());
    }
}
impl std::ops::Add for TableScanInitMetrics {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}
impl std::iter::Sum for TableScanInitMetrics {
    fn sum<I: std::iter::Iterator<Item = Self>>(it: I) -> Self {
        let mut sum = Self::default();
        for item in it {
            sum += item;
        }
        sum
    }
}

#[derive(Debug, Default)]
pub struct RowGroupsSelectionMetrics {
    pub row_groups_pruned_by_statistics: u64,
    pub row_groups_selected_by_statistics: u64,
    pub row_groups_pruned_by_bloom_filters: u64,
    pub row_groups_selected_by_bloom_filters: u64,

    pub eval_row_groups_statistics_time: Timing,
    pub filter_by_row_groups_statistics_time: Timing,
    pub read_bloom_filter_time: Timing,
    pub eval_bloom_filter_time: Timing,
}

impl std::ops::AddAssign for RowGroupsSelectionMetrics {
    fn add_assign(&mut self, rhs: Self) {
        self.row_groups_pruned_by_statistics += rhs.row_groups_pruned_by_statistics;
        self.row_groups_selected_by_statistics += rhs.row_groups_selected_by_statistics;
        self.row_groups_pruned_by_bloom_filters += rhs.row_groups_pruned_by_bloom_filters;
        self.row_groups_selected_by_bloom_filters += rhs.row_groups_selected_by_bloom_filters;

        self.eval_row_groups_statistics_time
            .add(rhs.eval_row_groups_statistics_time.get());
        self.filter_by_row_groups_statistics_time
            .add(rhs.filter_by_row_groups_statistics_time.get());
        self.read_bloom_filter_time
            .add(rhs.read_bloom_filter_time.get());
        self.eval_bloom_filter_time
            .add(rhs.eval_bloom_filter_time.get());
    }
}
impl std::ops::Add for RowGroupsSelectionMetrics {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}
impl std::iter::Sum for RowGroupsSelectionMetrics {
    fn sum<I: std::iter::Iterator<Item = Self>>(it: I) -> Self {
        let mut sum = Self::default();
        for item in it {
            sum += item;
        }
        sum
    }
}

#[derive(Debug, Default)]
pub struct RowsSelectionMetrics {
    pub rows_pruned_by_page_index: usize,
    pub rows_selected_by_page_index: usize,
    pub row_groups_pruned_by_page_index: u64,
    pub row_groups_selected_by_page_index: u64,

    pub eval_page_index_time: Timing,
}

impl std::ops::AddAssign for RowsSelectionMetrics {
    fn add_assign(&mut self, rhs: Self) {
        self.row_groups_pruned_by_page_index += rhs.row_groups_pruned_by_page_index;
        self.row_groups_selected_by_page_index += rhs.row_groups_selected_by_page_index;
        self.rows_pruned_by_page_index += rhs.rows_pruned_by_page_index;
        self.rows_selected_by_page_index += rhs.rows_selected_by_page_index;

        self.eval_page_index_time
            .add(rhs.eval_page_index_time.get());
    }
}
impl std::ops::Add for RowsSelectionMetrics {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self += rhs;
        self
    }
}
impl std::iter::Sum for RowsSelectionMetrics {
    fn sum<I: std::iter::Iterator<Item = Self>>(it: I) -> Self {
        let mut sum = Self::default();
        for item in it {
            sum += item;
        }
        sum
    }
}
