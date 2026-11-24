# v0.4.0

*2026-11-24*

Breaking changes:

* Switch from arrow v53 to v55-v58
* Bump MSRV to 1.85

Internal:

* Switch to released ar_row and ar_row_derive in dev-dependencies
* Fix doctest

# v0.3.0

*2026-06-30*

Breaking changes:

* Update epserde from 0.6.1 to 0.12.0 and sux from 0.4.1 to 0.12.0

Internal:

* Remove Cargo.lock from gitignore
* Fix Clippy warnings

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
