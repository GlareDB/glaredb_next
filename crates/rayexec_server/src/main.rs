use axum::{extract::State, routing::get, Router};
use clap::Parser;
use rayexec_csv::CsvDataSource;
use rayexec_error::{Result, ResultExt};
use rayexec_execution::{
    datasource::{DataSourceRegistry, MemoryDataSource},
    engine::Engine,
    runtime::ExecutionRuntime,
};
use rayexec_parquet::ParquetDataSource;
use rayexec_postgres::PostgresDataSource;
use rayexec_rt_native::runtime::ThreadedExecutionRuntime;
use std::sync::Arc;
use tracing::info;

#[derive(Parser)]
#[clap(name = "rayexec_server")]
struct Arguments {
    /// Port to start the server on.
    #[clap(short, long, default_value_t = 8080)]
    port: u16,
}

fn main() {
    let args = Arguments::parse();
    logutil::configure_global_logger(tracing::Level::DEBUG);

    let runtime = Arc::new(
        ThreadedExecutionRuntime::try_new()
            .unwrap()
            .with_default_tokio()
            .unwrap(),
    );
    let tokio_handle = runtime.tokio_handle().expect("tokio to be configured");

    let runtime_clone = runtime.clone();
    let result = tokio_handle.block_on(async move { inner(args, runtime_clone).await });

    if let Err(e) = result {
        println!("ERROR: {e}");
        std::process::exit(1);
    }
}

async fn inner(args: Arguments, runtime: Arc<dyn ExecutionRuntime>) -> Result<()> {
    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("postgres", Box::new(PostgresDataSource))?
        .with_datasource("parquet", Box::new(ParquetDataSource))?
        .with_datasource("csv", Box::new(CsvDataSource))?;
    let engine = Engine::new_with_registry(runtime.clone(), registry)?;

    let state = Arc::new(ServerState { engine });

    let app = Router::new()
        .route("/healthz", get(healthz))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port))
        .await
        .context("failed to bind port")?;

    info!(port = %args.port, "starting server");

    axum::serve(listener, app)
        .await
        .context("failed to begin serving")?;

    Ok(())
}

#[derive(Debug)]
struct ServerState {
    engine: Engine,
}

async fn healthz(State(_): State<Arc<ServerState>>) -> &'static str {
    "OK"
}
