use rayexec_csv::CsvDataSource;
use rayexec_execution::{datasource::DataSourceRegistry, engine::Engine, runtime::NopScheduler};
use rayexec_slt::{ReplacementVars, RunConfig};
use std::{path::Path, sync::Arc};

pub fn main() {
    let paths = rayexec_slt::find_files(Path::new("../slt/csv")).unwrap();
    rayexec_slt::run(
        paths,
        |rt, _| {
            Engine::new_with_registry(
                rt,
                DataSourceRegistry::default().with_datasource("csv", Box::new(CsvDataSource))?,
            )
        },
        RunConfig {
            vars: ReplacementVars::default(),
            create_slt_tmp: true,
        },
        "slt_datasource_csv",
    )
    .unwrap();
}
