// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::iter::repeat_n;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use ar_row::deserialize::{ArRowDeserialize, CheckableDataType};
use ar_row_derive::ArRowDeserialize;
use arrow::array::*;
use arrow::datatypes::DataType::*;
use arrow::datatypes::*;
use futures::stream::TryStreamExt;
use parquet::arrow::*;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use parquet_aramid::metrics::*;
use parquet_aramid::types::IndexKey;
use parquet_aramid::ReaderBuilderConfigurator;
use parquet_aramid::Table;

pub const ROWS_PER_PAGE: u64 = 10;
pub const ROW_GROUPS_PER_FILE: u64 = 10;
pub const FIXEDSIZEBINARY_SIZE: i32 = 25;

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("key_u64", UInt64, false),
        Field::new("key_u64_bloom", UInt64, false),
        Field::new("key_binary_bloom", Binary, false),
        Field::new(
            "key_fixedsizebinary_bloom",
            FixedSizeBinary(FIXEDSIZEBINARY_SIZE),
            false,
        ),
        Field::new("row_id", UInt64, false),
        Field::new("page_id", UInt64, false),
        Field::new("batch_id", UInt64, false),
        Field::new("row_group_id", UInt64, false),
        Field::new("file_id", UInt64, false),
    ])
}

#[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Row {
    pub key_u64: u64,
    pub key_u64_bloom: u64,
    pub key_binary_bloom: Box<[u8]>,
    pub key_fixedsizebinary_bloom: ar_row::FixedSizeBinary<{ FIXEDSIZEBINARY_SIZE as _ }>,
    pub row_id: u64,
    pub page_id: u64,
    pub batch_id: u64,
    pub row_group_id: u64,
    pub file_id: u64,
}

/// Writes the given keys to Parquet files
///
/// The files are organized so that there are:
///
/// - 10 rows per page
/// - 1 key batch per row group
/// - 10 row groups per file
///
/// The last page of each row group may have fewer rows if the row group does not have a multiple
/// of 10 rows.
///
/// The last file may have fewer row groups if the number of key batches is not a multiple of 10.
pub fn write_database(
    key_batches: impl IntoIterator<Item = UInt64Array>,
    path: impl AsRef<Path>,
    with_bloom_filters: bool,
) -> Result<()> {
    let path = path.as_ref();
    let schema = Arc::new(schema());

    // WriterProperties can be used to set Parquet file options
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_data_page_row_count_limit(ROWS_PER_PAGE as usize)
        .set_write_batch_size(ROWS_PER_PAGE as usize)
        .set_bloom_filter_ndv(100)
        .set_bloom_filter_enabled(false)
        .set_column_bloom_filter_enabled("key_u64_bloom".into(), with_bloom_filters)
        .set_column_bloom_filter_enabled("key_binary_bloom".into(), with_bloom_filters)
        .set_column_bloom_filter_enabled("key_fixedsizebinary_bloom".into(), with_bloom_filters)
        .set_max_row_group_size(usize::MAX) // we control it manually on a per-rowgroup basis
        .build();

    let mut key_batches = key_batches.into_iter().enumerate().peekable();
    let mut row_ids = 0..;

    for file_id in 0.. {
        if key_batches.peek().is_none() {
            break;
        }
        let file = std::fs::File::create_new(path.join(format!("{}.parquet", file_id)))
            .context("Could not create file")?;
        let mut writer =
            ArrowWriter::try_new(file, schema.clone(), Some(properties.clone())).unwrap();

        for row_group_id in 0..ROW_GROUPS_PER_FILE {
            let Some((batch_id, keys)) = key_batches.next() else {
                break;
            };
            let num_rows = keys.len();
            let page_ids =
                UInt64Array::from_iter((0..num_rows).map(|row_id| row_id as u64 / ROWS_PER_PAGE));
            let batch =
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(keys.clone()),
                        Arc::new(keys.clone()),
                        Arc::new(BinaryArray::from_iter(keys.iter().map(|key| {
                            key.map(|key| format!("item{:>21}", key).into_bytes())
                        }))),
                        Arc::new(
                            FixedSizeBinaryArray::try_from_iter(keys.iter().map(|key| {
                                format!("item{:>21}", key.expect("null key")).into_bytes()
                            }))
                            .context("Could not build FixedSizeBinaryArray")?,
                        ),
                        Arc::new(UInt64Array::from_iter(
                            (0..num_rows).map(|_| row_ids.next().unwrap()),
                        )),
                        Arc::new(page_ids),
                        Arc::new(UInt64Array::from_iter(repeat_n(batch_id as u64, num_rows))),
                        Arc::new(UInt64Array::from_iter(repeat_n(row_group_id, num_rows))),
                        Arc::new(UInt64Array::from_iter(repeat_n(file_id as u64, num_rows))),
                    ],
                )
                .context("Could not build RecordBatch")?;
            writer.write(&batch).context("Could not write batch")?;

            if key_batches.peek().is_some() {
                writer.flush().context("Could not start next row group")?;
            }
        }

        writer.close().context("Could not close writer")?;
    }

    Ok(())
}

pub async fn check_results<Needle: IndexKey + Ord + Copy>(
    table: &Table,
    needles: Vec<Needle>,
    key_column: &'static str,
    expected_keys: Vec<u64>,
    configurator: impl ReaderBuilderConfigurator,
    mut make_row: impl FnMut(Needle, u64) -> Row,
) -> Result<(Vec<Needle>, TableScanInitMetrics)> {
    Row::check_schema(&schema())
        .map_err(|e| anyhow::anyhow!("Cannot deserialize to given Row type: {}", e))?;
    let needles = Arc::new(needles);
    let (metrics, stream) = table
        .stream_for_keys(key_column, needles.clone(), Arc::new(configurator))
        .await
        .context("Could not stream from table")?;

    let result_batches: Vec<RecordBatch> = stream
        .try_collect()
        .await
        .context("Could not collect stream")?;
    let mut results: Vec<_> = result_batches
        .into_iter()
        .flat_map(|batch| Row::from_record_batch(batch).expect("Could not deserialize batch"))
        .collect();
    results.sort_unstable();
    let expected_results: Vec<_> = needles
        .iter()
        .zip(expected_keys.iter())
        .map(|(&needle, &key)| make_row(needle, key))
        .collect();
    assert_eq!(
        results, expected_results,
        "Unexpected results when searching for {:?}. Metrics: {:#?}",
        expected_keys, metrics
    );
    Ok(((*needles).clone(), metrics))
}
