mod editor;
mod shell;
mod vt100;

use std::sync::Arc;

use crossterm::event::{self, Event, KeyEvent};
use futures::StreamExt;
use rayexec_bullet::format::ugly::ugly_print;
use rayexec_error::Result;
use rayexec_execution::datasource::{DataSourceRegistry, MemoryDataSource};
use rayexec_execution::engine::{Engine, EngineRuntime};
use rayexec_parquet::ParquetDataSource;
use rayexec_postgres::PostgresDataSource;
use shell::{KeyCode, Shell};
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::FmtSubscriber;

/// Simple binary for quickly running arbitrary queries.
fn main() {
    let env_filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::ERROR.into())
        .from_env_lossy()
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap())
        .add_directive("sqllogictest=info".parse().unwrap());
    let subscriber = FmtSubscriber::builder()
        .with_test_writer() // TODO: Actually capture
        .with_env_filter(env_filter)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let runtime = EngineRuntime::try_new_shared().unwrap();
    runtime.clone().tokio.block_on(async move {
        if let Err(e) = inner(runtime).await {
            println!("----");
            println!("ERROR");
            println!("{e}");
            std::process::exit(1);
        }
    })
}

fn from_crossterm_keycode(code: crossterm::event::KeyCode) -> KeyCode {
    match code {
        crossterm::event::KeyCode::Backspace => KeyCode::Backspace,
        crossterm::event::KeyCode::Enter => KeyCode::Enter,
        crossterm::event::KeyCode::Left => KeyCode::Left,
        crossterm::event::KeyCode::Right => KeyCode::Right,
        crossterm::event::KeyCode::Up => KeyCode::Up,
        crossterm::event::KeyCode::Down => KeyCode::Down,
        crossterm::event::KeyCode::Home => KeyCode::Home,
        crossterm::event::KeyCode::End => KeyCode::End,
        crossterm::event::KeyCode::Tab => KeyCode::Tab,
        crossterm::event::KeyCode::BackTab => KeyCode::BackTab,
        crossterm::event::KeyCode::Delete => KeyCode::Delete,
        crossterm::event::KeyCode::Insert => KeyCode::Insert,
        crossterm::event::KeyCode::Char(c) => KeyCode::Char(c),
        key => unimplemented!("{key:?}"),
    }
}

async fn inner(runtime: Arc<EngineRuntime>) -> Result<()> {
    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("postgres", Box::new(PostgresDataSource))?
        .with_datasource("parquet", Box::new(ParquetDataSource))?;
    let engine = Engine::new_with_registry(runtime, registry)?;

    crossterm::terminal::enable_raw_mode()?;

    let stdout = std::io::stdout();
    let shell = Shell::new(stdout, engine);

    loop {
        match event::read()? {
            Event::Key(KeyEvent { code, .. }) => {
                shell.on_key(from_crossterm_keycode(code)).await;
            }
            Event::Resize(width, _) => shell.set_width(width as usize),
            event => println!("{event:?}"),
        }
    }

    // let args: Vec<_> = std::env::args().collect();

    // let mut session = engine.new_session()?;

    // let query = args[1].clone();

    // let outputs = session.simple(&query).await?;

    // for mut output in outputs {
    //     println!("----");
    //     println!("INPUT: {query}");
    //     println!("OUTPUT SCHEMA: {:?}", output.output_schema);

    //     while let Some(result) = output.stream.next().await {
    //         let batch = result?;
    //         let out = ugly_print(&output.output_schema, &[batch])?;
    //         println!("{out}");
    //     }

    //     let dump = output.handle.query_dump();
    //     println!("----");
    //     println!("DUMP");
    //     println!("{dump}");
    // }

    // Ok(())
}
