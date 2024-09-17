use rayexec_delta::DeltaDataSource;
use rayexec_error::Result;
use rayexec_execution::{
    datasource::{DataSourceBuilder, DataSourceRegistry},
    engine::Engine,
};
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_slt::{ReplacementVars, RunConfig, VarValue};
use std::{path::Path, sync::Arc, time::Duration};

pub fn main() -> Result<()> {
    let rt = NativeRuntime::with_default_tokio()?;
    let engine = Arc::new(Engine::new_with_registry(
        ThreadedNativeExecutor::try_new()?,
        rt.clone(),
        DataSourceRegistry::default().with_datasource("delta", DeltaDataSource::initialize(rt))?,
    )?);

    let paths = rayexec_slt::find_files(Path::new("../slt/delta")).unwrap();
    rayexec_slt::run(
        paths,
        move || {
            let aws_key = VarValue::sensitive_from_env("AWS_KEY");
            let aws_secret = VarValue::sensitive_from_env("AWS_SECRET");

            let mut vars = ReplacementVars::default();
            vars.add_var("AWS_KEY", aws_key);
            vars.add_var("AWS_SECRET", aws_secret);

            let session = engine.new_session()?;

            Ok(RunConfig {
                session,
                vars,
                create_slt_tmp: true,
                query_timeout: Duration::from_secs(5),
            })
        },
        "slt_datasource_delta",
    )
}
