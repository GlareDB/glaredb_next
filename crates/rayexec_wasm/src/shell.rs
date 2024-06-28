use crate::{errors::Result, session::WasmSession};
use futures::StreamExt;
use rayexec_bullet::format::ugly::ugly_print;
use rayexec_error::RayexecError;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    /// Structurally typed terminal interface for outputting data including
    /// terminal codes.
    #[derive(Debug)]
    pub type Terminal;

    #[wasm_bindgen(method)]
    pub fn write(this: &Terminal, data: &[u8]);
}

#[derive(Debug)]
pub struct TerminalWrapper {
    terminal: Terminal,
}

impl TerminalWrapper {
    pub fn new(terminal: Terminal) -> Self {
        TerminalWrapper { terminal }
    }

    pub fn write_str(&self, s: &str) {
        self.terminal.write(s.as_bytes())
    }

    pub fn write_bytes(&self, data: &[u8]) {
        self.terminal.write(data)
    }

    pub fn newline(&self) {
        self.terminal.write("\r\n".as_bytes())
    }
}

/// A simple shell wrapper around a wasm session.
#[wasm_bindgen]
#[derive(Debug)]
pub struct WasmShell {
    /// Buffered input.
    input: String,

    /// Current terminal width.
    term_width: usize,

    /// The session we'll be interacting with.
    session: WasmSession,

    /// Terminal we'll be writing to.
    terminal: TerminalWrapper,
}

#[wasm_bindgen]
impl WasmShell {
    pub fn try_new(terminal: Terminal) -> Result<WasmShell> {
        let terminal = TerminalWrapper::new(terminal);

        terminal.write_str("Rayexec WASM");
        terminal.newline();
        terminal.write_str("Initializing...");
        terminal.newline();

        let session = WasmSession::try_new()?;
        Ok(Self {
            input: String::new(),
            term_width: 80,
            session,
            terminal,
        })
    }

    pub fn set_term_width(&mut self, width: usize) {
        self.term_width = width
    }

    pub fn put_text(&mut self, text: String) {
        self.input.push_str(text.trim())
    }

    pub async fn submit(&mut self) -> Result<()> {
        let results = self.session.session.simple(&self.input).await?;
        self.input.clear();

        self.terminal.newline();
        for result in results {
            let batches = result
                .stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
            let s = ugly_print(&result.output_schema, batches.iter())?;
            self.terminal.write_str(&s);
        }

        Ok(())
    }
}
