use rayexec_csv::CsvDataSource;
use rayexec_execution::{datasource::DataSourceRegistry, engine::Engine};
use std::path::Path;

pub fn main() {
    let paths = rayexec_slt::find_files(Path::new("../slt/csv")).unwrap();
    rayexec_slt::run(
        paths,
        |rt| {
            Engine::new_with_registry(
                rt,
                DataSourceRegistry::default().with_datasource("csv", Box::new(CsvDataSource))?,
            )
        },
        "slt_datasource_csv",
    )
    .unwrap();
}