mod editor;
mod shell;
mod vt100;

use std::sync::Arc;

use crossterm::event::{self, Event, KeyModifiers};
use futures::StreamExt;
use rayexec_bullet::format::ugly::ugly_print;
use rayexec_error::Result;
use rayexec_execution::datasource::{DataSourceRegistry, MemoryDataSource};
use rayexec_execution::engine::{Engine, EngineRuntime};
use rayexec_parquet::ParquetDataSource;
use rayexec_postgres::PostgresDataSource;
use rayexec_shell::lineedit::{KeyEvent, LineEditor, Signal};
use rayexec_shell::shell::{Shell, ShellSignal};
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

fn from_crossterm_keycode(code: crossterm::event::KeyCode) -> KeyEvent {
    match code {
        crossterm::event::KeyCode::Backspace => KeyEvent::Backspace,
        crossterm::event::KeyCode::Enter => KeyEvent::Enter,
        crossterm::event::KeyCode::Left => KeyEvent::Left,
        crossterm::event::KeyCode::Right => KeyEvent::Right,
        crossterm::event::KeyCode::Up => KeyEvent::Up,
        crossterm::event::KeyCode::Down => KeyEvent::Down,
        crossterm::event::KeyCode::Home => KeyEvent::Home,
        crossterm::event::KeyCode::End => KeyEvent::End,
        crossterm::event::KeyCode::Tab => KeyEvent::Tab,
        crossterm::event::KeyCode::BackTab => KeyEvent::BackTab,
        crossterm::event::KeyCode::Delete => KeyEvent::Delete,
        crossterm::event::KeyCode::Insert => KeyEvent::Insert,
        crossterm::event::KeyCode::Char(c) => KeyEvent::Char(c),
        _ => KeyEvent::Unknown,
    }
}

async fn inner(runtime: Arc<EngineRuntime>) -> Result<()> {
    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("postgres", Box::new(PostgresDataSource))?
        .with_datasource("parquet", Box::new(ParquetDataSource))?;
    let engine = Engine::new_with_registry(runtime, registry)?;
    let session = engine.new_session()?;

    let (cols, _rows) = crossterm::terminal::size()?;
    let stdout = std::io::stdout();

    crossterm::terminal::enable_raw_mode()?;

    let shell = Shell::new(stdout);
    shell.set_cols(cols as usize);
    shell.attach(session, "Rayexec Shell")?;

    let inner_loop = || async move {
        loop {
            match event::read()? {
                Event::Key(event::KeyEvent {
                    code, modifiers, ..
                }) => {
                    let key = if modifiers.contains(KeyModifiers::CONTROL) {
                        match code {
                            event::KeyCode::Char('c') => KeyEvent::CtrlC,
                            _ => KeyEvent::Unknown,
                        }
                    } else {
                        from_crossterm_keycode(code)
                    };

                    match shell.consume_key(key).await? {
                        ShellSignal::Continue => (),
                        ShellSignal::Exit => break,
                    }
                }
                Event::Resize(cols, _) => shell.set_cols(cols as usize),
                _event => (),
            }
        }
        Ok(())
    };

    let result = inner_loop().await;
    crossterm::terminal::disable_raw_mode()?;

    result
}
