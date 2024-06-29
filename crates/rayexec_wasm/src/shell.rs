use crate::errors::Result;
use js_sys::{Function, RegExp};
use rayexec_execution::engine::{session::Session, Engine, EngineRuntime};
use rayexec_shell::{lineedit::KeyEvent, shell::Shell};
use std::io;
use tracing::{debug, warn};
use wasm_bindgen::prelude::*;
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
    pub(crate) shell: Shell<TerminalWrapper>,
}

#[wasm_bindgen]
impl WasmShell {
    pub fn try_new(terminal: Terminal) -> Result<WasmShell> {
        let runtime = EngineRuntime::try_new_shared()?;
        let engine = Engine::new(runtime)?;

        let terminal = TerminalWrapper::new(terminal);
        let shell = Shell::new(terminal);

        let session = engine.new_session()?;
        shell.attach(session, "Rayexec WASM Shell")?;

        Ok(WasmShell { engine, shell })
    }

    pub async fn on_key(&self, event: OnKeyEvent) -> Result<()> {
        let event = event.dom_event();
        event.prevent_default();

        if event.type_() != "keydown" && event.type_() != "keypress" {
            return Ok(());
        }

        let key = event.key();
        debug!(%key, "keyboard event");

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

        self.shell.consume_key(key).await?;

        Ok(())
    }

    pub async fn put_text(&self, text: &str) -> Result<()> {
        for ch in text.chars() {
            self.shell.consume_key(KeyEvent::Char(ch)).await?;
        }
        Ok(())
    }

    pub async fn ctrl_c(&self) -> Result<()> {
        self.shell.consume_key(KeyEvent::CtrlC).await?;
        Ok(())
    }
}
