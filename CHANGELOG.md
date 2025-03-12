# v0.2.0

*2025-03-12*

Performance improvements:

* Check row group statistics against every key, instead of only the min/max
* Use Bloom Filters even when statistics matching is not implemented
* Share cached ParquetMetaData between readers of the same pool

New features:

* Add support for Binary keys
* Implement Add, AddAssign, and Sum for all metrics traits
* Add FilterPrimitiveConfigurator
* Re-export arrow and parquet
* Return metrics instead of logging them
* Improve errors when a file cannot be read

Bug fixes:

* Count EF index evaluation toward total time
* Fix filtering predicate on non-first column
* Ignore non-Parquet files in a directory
* README: Fix syntax highlighting on crates.io

Breaking changes:

* Move ReaderBuilderConfigurator to config::Configurator along with its impls

Internal:

* Add tests for Bloom Filters
* rustfmt
* Add end-to-end tests

# v0.1.0

*2024-10-22*

Initial release.

Provides a very low-level API to filter on integers, and a bare-bones
example `ReaderBuilderConfigurator` that only supports filtering out rows
based on a `FixedSizeBinary` key.
