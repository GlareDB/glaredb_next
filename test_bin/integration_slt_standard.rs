use rayexec_execution::{engine::Engine, runtime::NopScheduler};
use rayexec_slt::RunConfig;
use std::path::Path;

pub fn main() {
    let paths = rayexec_slt::find_files(Path::new("../slt/standard")).unwrap();
    rayexec_slt::run(
        paths,
        |rt, _| Engine::new(rt),
        RunConfig::default(),
        "slt_standard",
    )
    .unwrap();
}
