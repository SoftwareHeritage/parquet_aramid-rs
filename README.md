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

```no_run
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

// Load external indexes from disk
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
