# parquet_aramid

Query engine using Parquet tables as a Key-Value store, for Rust applications.

## Motivation and use-cases

[Parquet](https://parquet.apache.org/) is column-oriented file format, meaning
that it is designed to efficiently read of large chunks of a column at once
(aka. OLAP workflows).

This also means that it is generally badly suited to queries that expect a few
rows at once (aka. OLTP workflows), or relational joins.

This crate is for the few queries of this kind where Parquet can shine.
This means:

1. selection based on a column where values are either low-cardinality or
   somewhat sorted,
2. non-selection columns can be large, and benefit from data locality and
   good compression across close rows,
3. datasets do not change after they were generated

If your queries do not match point 1, you need a more traditional Parquet
query engine, like [Datafusion](https://datafusion.apache.org/).
If your queries do not match points 2 or 3, you should probably use a
SQL database (like [sqlite](https://www.sqlite.org/)), or
[RocksDB](https://rocksdb.org/).

## Example usage

```rust,no_run
# tokio_test::block_on(async {
use std::path::PathBuf;
use std::sync::Arc;

use futures::stream::StreamExt;
use parquet_aramid::Table;
use parquet_aramid::types::FixedSizeBinary;
use parquet_aramid::reader_builder_config::FilterFixedSizeBinaryConfigurator;

// Location of the Parquet table
let url = url::Url::parse("file:///srv/data/my_table/").unwrap();
let (store, path) = object_store::parse_url(&url).unwrap();
let store = store.into();

// Location of external indexes to speed-up lookup
let ef_indexes_path = PathBuf::from("/tmp/my_table_catalog/");

// Initialize table, read metadata to RAM
let table = Table::new(store, path, ef_indexes_path).await.unwrap();

// Load external indexes from disk (see below for how to generate them)
table.mmap_ef_index("key_column").unwrap();

// Filter out rows whose key is not b"abcd"
let keys = Arc::new(vec![FixedSizeBinary(*b"abcd")]);
let configurator = Arc::new(
    FilterFixedSizeBinaryConfigurator::with_sorted_keys("key_column", Arc::clone(&keys))
);

// Read the table
table
    .stream_for_keys("key_column", Arc::clone(&keys), configurator)
    .await
    .unwrap()
    .for_each(|batch| async move {
        // Iterates on every row with b"abcd" as its value in "key_column"
        println!("{:?}", batch);
    });

# })
```

## Generating external indexes

```rust,no_run
# tokio_test::block_on(async {
use std::path::PathBuf;
use std::sync::Arc;

use epserde::ser::Serialize;
use futures::stream::StreamExt;
use parquet_aramid::Table;
use parquet_aramid::types::FixedSizeBinary;
use parquet_aramid::reader_builder_config::FilterFixedSizeBinaryConfigurator;
use tokio::task::JoinSet;

// Location of the Parquet table
let url = url::Url::parse("file:///srv/data/my_table/").unwrap();
let (store, path) = object_store::parse_url(&url).unwrap();
let store = store.into();

// Location where to write external indexes
let ef_indexes_path = PathBuf::from("/tmp/my_table_catalog/");

// Initialize table
let table = Table::new(store, path, ef_indexes_path.clone()).await.unwrap();

// Create directory to store indexes
std::fs::create_dir_all(&ef_indexes_path).unwrap();

let mut tasks = Vec::new();
for file in table.files {
    tasks.push(tokio::task::spawn(async move {
        // Build index
        let ef_values = file
            .build_ef_index("key_column")
            .await
            .expect("Could not build Elias-Fano index");

        // Write index to disk
        let index_path = file.ef_index_path("key_column");
        let mut ef_file =std::fs::File::create_new(&index_path).unwrap();
        ef_values
            .serialize(&mut ef_file)
            .expect("Could not serialize Elias-Fano index");
    }));
}

tasks
    .into_iter()
    .collect::<JoinSet<_>>()
    .join_all()
    .await
    .into_iter()
    .collect::<Result<Vec<_>, tokio::task::JoinError>>()
    .expect("Could not join task");
# })
```

## Architecture

[`Table::new`] reads all metadata from Parquet files in the table into RAM,
and keeps it until it is dropped, in order to significantly reduce time
spent on each query.
This can be significant for some tables, but is no more than Datafusion's peak
memory usage while a query is running.

[`Table::stream_for_keys`] is designed very similary to any Parquet query
engine's `SELECT xxx FROM table WHERE key = yyy`:

1. If `Table::mmap_ef_index` was called for this column, use said index to
   filter out files which do not contain any of the given keys.
   For kept files, remember which of the input keys are present in the file
   (to reduce false positives in the next three filters).
   This filter does not have false positives.
   This is a `parquet_aramid`-specific design/format, built on
   [Sebastiano Vigna's Elias-Fano implementation](https://docs.rs/sux/latest/sux/dict/elias_fano/struct.EliasFano.html)
2. For each selected file, look at
   [statistics of each row group](https://parquet.apache.org/docs/file-format/metadata/)
   and filter out row groups whose statistics do not match any of the keys.
   This leaves many false positives, but reduces the cost of the next step.
3. For each row group selected by the previous filter, use its
   [Bloom Filter](https://parquet.apache.org/docs/file-format/bloomfilter/) to
   filter it out if it was a false positives.
   This may leave more false positives.
4. For each page in a row group selected by the previous filter, use the
   [Page Index](https://parquet.apache.org/docs/file-format/pageindex/) to
   check if it may contain one of the keys.

So far, none of the keys or data in the table was read.
At this point, every matching page (of the key column, and potentially others
depending on the `configurator`) is decompressed, and the `configurator`
checks individual values, to tell `parquet_aramid` which rows × columns should
bne returned.

See the [`ParquetRecordBatchStreamBuilder`] documentation for an exhaustive
overview of all the filtering the `configurator` can do.
