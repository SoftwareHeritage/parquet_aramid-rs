// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![doc = include_str!("../README.md")]

pub use arrow;
pub use parquet;

mod caching_parquet_reader;
pub mod config;
pub mod metrics;
mod pooled_reader;
mod reader;
mod table;
pub use table::*;
pub mod types;
