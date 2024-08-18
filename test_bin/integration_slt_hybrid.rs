use rayexec_error::Result;
use rayexec_execution::{
    engine::Engine,
    hybrid::client::{HybridClient, HybridConnectConfig},
    runtime::{Runtime, TokioHandlerProvider},
};
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_server::serve_with_engine;
use rayexec_slt::{ReplacementVars, RunConfig};
use std::{path::Path, sync::Arc};

pub fn main() -> Result<()> {
    const PORT: u16 = 8085;

    {
        // Server engine.
        let rt = NativeRuntime::with_default_tokio()?;
        let tokio_handle = rt.tokio_handle().handle().expect("tokio to be configured");
        let engine = Engine::new(ThreadedNativeExecutor::try_new()?, rt.clone())?;

        tokio_handle.spawn(async move { serve_with_engine(engine, PORT).await });
    }

    // Client engine.
    let rt = NativeRuntime::with_default_tokio()?;
    let engine = Arc::new(Engine::new(ThreadedNativeExecutor::try_new()?, rt.clone())?);

    let paths = rayexec_slt::find_files(Path::new("../slt/hybrid")).unwrap();
    rayexec_slt::run(
        paths,
        move || {
            let mut session = engine.new_session()?;

            // TODO: This is duplicated with `connect_hybrid` in `rayexec_shell`.

            let connection_string = format!("http://localhost:{}", PORT);
            let config = HybridConnectConfig::try_from_connection_string(&connection_string)?;
            let client = rt.http_client();
            let hybrid = HybridClient::new(client, config);

            session.set_hybrid(hybrid);

            Ok(RunConfig {
                session,
                vars: ReplacementVars::default(),
                create_slt_tmp: false,
            })
        },
        "slt_hybrid",
    )
}
