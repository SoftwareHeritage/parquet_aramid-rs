// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::iter::repeat_n;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use ar_row_derive::ArRowDeserialize;
use arrow::array::*;
use arrow::datatypes::DataType::*;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::*;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub const ROWS_PER_PAGE: u64 = 10;
pub const ROW_GROUPS_PER_FILE: u64 = 10;

fn schema(key_type: DataType) -> Schema {
    Schema::new(vec![
        Field::new("key", key_type, false),
        Field::new("row_id", UInt64, false),
        Field::new("page_id", UInt64, false),
        Field::new("batch_id", UInt64, false),
        Field::new("row_group_id", UInt64, false),
        Field::new("file_id", UInt64, false),
    ])
}

#[derive(ArRowDeserialize, Clone, Default, Debug, PartialEq, Eq, Ord, PartialOrd)]
pub struct Row {
    pub key: u64,
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
pub fn write_database<KeyArray: Array + 'static>(
    key_batches: impl IntoIterator<Item = KeyArray>,
    key_type: DataType,
    path: impl AsRef<Path>,
) -> Result<()> {
    let path = path.as_ref();
    let schema = Arc::new(schema(key_type));

    // WriterProperties can be used to set Parquet file options
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_data_page_row_count_limit(ROWS_PER_PAGE as usize)
        .set_write_batch_size(ROWS_PER_PAGE as usize)
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
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(keys),
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
