//! Bin for running the SLTs.

use rayexec_execution::engine::Engine;
use std::path::Path;

/// Path to slts directory relative to this crate's root.
const SLTS_PATH: &str = "slts/";

pub fn main() {
    let paths = rayexec_sqltest::find_files(Path::new(SLTS_PATH)).unwrap();
    rayexec_sqltest::run(paths, || Engine::try_new(), "standard").unwrap();
}
