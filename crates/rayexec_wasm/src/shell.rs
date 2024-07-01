use crate::{errors::Result, runtime::WasmExecutionRuntime};
use js_sys::{Function, RegExp};
use rayexec_execution::engine::{session::Session, Engine};
use rayexec_shell::shell::ShellSignal;
use rayexec_shell::{lineedit::KeyEvent, shell::Shell};
use std::io::{self, BufWriter};
use std::sync::Arc;
use tracing::{debug, error, warn};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use web_sys::KeyboardEvent;

#[wasm_bindgen]
extern "C" {
    /// Structurally typed terminal interface for outputting data including
    /// terminal codes.
    ///
    /// xterm.js mostly.
    #[derive(Debug)]
    pub type Terminal;

    #[wasm_bindgen(method)]
    pub fn write(this: &Terminal, data: &[u8]);

    #[wasm_bindgen(js_name = "IDisposable")]
    pub type Disposable;

    #[wasm_bindgen(method, js_name = "dispose")]
    pub fn dispose(this: &Disposable);

    pub type OnKeyEvent;

    #[wasm_bindgen(method, getter, js_name = "key")]
    pub fn key(this: &OnKeyEvent) -> String;

    #[wasm_bindgen(method, getter, js_name = "domEvent")]
    pub fn dom_event(this: &OnKeyEvent) -> KeyboardEvent;

    #[wasm_bindgen(method, js_name = "onKey")]
    pub fn on_key(
        this: &Terminal,
        f: &Function, // Event<{key: &str, dom_event: KeyboardEvent}>
    ) -> Disposable;
}

#[derive(Debug)]
pub struct TerminalWrapper {
    terminal: Terminal,
}

impl TerminalWrapper {
    pub fn new(terminal: Terminal) -> Self {
        TerminalWrapper { terminal }
    }
}

impl io::Write for TerminalWrapper {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = buf.len();
        self.terminal.write(buf);

        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Wrapper around a database session and the engine that created it.
#[wasm_bindgen]
#[derive(Debug)]
pub struct WasmShell {
    pub(crate) engine: Engine,
    // TODO: For some reason, without the buf writer, the output gets all messed
    // up. The buf writer is a good thing since we're calling flush where
    // appropriate, but it'd be nice to know what's going wrong when it's not
    // used.
    pub(crate) shell: Arc<Shell<BufWriter<TerminalWrapper>>>,
}

#[wasm_bindgen]
impl WasmShell {
    pub fn try_new(terminal: Terminal) -> Result<WasmShell> {
        let runtime = Arc::new(WasmExecutionRuntime::try_new()?);
        let engine = Engine::new(runtime)?;

        let terminal = TerminalWrapper::new(terminal);
        let shell = Arc::new(Shell::new(BufWriter::new(terminal)));

        let session = engine.new_session()?;
        shell.attach(session, "Rayexec WASM Shell")?;

        Ok(WasmShell { engine, shell })
    }

    pub fn on_data(&self, text: String) -> Result<()> {
        self.shell.consume_text(&text)?;
        Ok(())
    }

    pub fn on_key(&self, event: KeyboardEvent) -> Result<()> {
        if event.type_() != "keydown" {
            return Ok(());
        }

        let key = event.key();

        let key = match key.as_str() {
            "Backspace" => KeyEvent::Backspace,
            "Enter" => KeyEvent::Enter,
            other if other.chars().count() != 1 => {
                warn!(%other, "unhandled input");
                return Ok(());
            }
            other => match other.chars().next() {
                Some(ch) => {
                    if event.ctrl_key() {
                        match ch {
                            'c' => KeyEvent::CtrlC,
                            other => {
                                warn!(%other, "unhandled input with ctrl modifier");
                                return Ok(());
                            }
                        }
                    } else if event.meta_key() {
                        warn!(%other, "unhandled input with meta modifier");
                        return Ok(());
                    } else {
                        KeyEvent::Char(ch)
                    }
                }
                None => {
                    warn!("key event with no key");
                    return Ok(());
                }
            },
        };

        match self.shell.consume_key(key)? {
            ShellSignal::Continue => (), // Continue with normal editing.
            ShellSignal::ExecutePending => {
                let shell = self.shell.clone();
                spawn_local(async move {
                    if let Err(e) = shell.execute_pending().await {
                        error!(%e, "error executing pending query");
                    }
                });
            }
            ShellSignal::Exit => (), // Can't exit out of the web shell.
        }

        Ok(())
    }
}
