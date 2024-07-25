use rayexec_execution::{datasource::DataSourceRegistry, engine::Engine, runtime::NopScheduler};
use rayexec_postgres::PostgresDataSource;
use rayexec_slt::RunConfig;
use std::path::Path;
use std::sync::Arc;

pub fn main() {
    let paths = rayexec_slt::find_files(Path::new("../slt/postgres")).unwrap();
    rayexec_slt::run(
        paths,
        |rt, _| {
            Engine::new_with_registry(
                rt,
                DataSourceRegistry::default()
                    .with_datasource("postgres", Box::new(PostgresDataSource))?,
            )
        },
        RunConfig::default(),
        "slt_datasource_postgres",
    )
    .unwrap();
}
