use rayexec_execution::engine::Engine;
use std::path::Path;

pub fn main() {
    let paths = rayexec_sqltest::find_files(Path::new("slts/")).unwrap();
    rayexec_sqltest::run(paths, || Engine::try_new(), "standard_slt").unwrap();
}
