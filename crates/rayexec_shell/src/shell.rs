use std::{
    cell::RefCell,
    io::{self, Write},
};

use futures::StreamExt;
use rayexec_bullet::format::pretty::table::{pretty_format_batches, PrettyTable};
use rayexec_error::{RayexecError, Result};
use rayexec_execution::engine::{result::ExecutionResult, session::Session, Engine};

use crate::lineedit::{KeyEvent, LineEditor, Signal};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellSignal {
    Continue,
    Exit,
}

#[derive(Debug)]
pub struct Shell<W: io::Write> {
    editor: RefCell<LineEditor<W>>,
    session: RefCell<Option<Session>>,
}

impl<W: io::Write> Shell<W> {
    pub fn new(writer: W) -> Self {
        let editor = LineEditor::new(writer, ">> ", 80);
        Shell {
            editor: RefCell::new(editor),
            session: RefCell::new(None),
        }
    }

    pub fn attach(&self, session: Session, shell_msg: &str) -> Result<()> {
        let mut current = self.session.borrow_mut();
        *current = Some(session);

        let mut editor = self.editor.borrow_mut();
        writeln!(editor.raw_writer(), "{}", shell_msg)?;
        editor.edit_start()?;

        Ok(())
    }

    pub fn set_cols(&self, cols: usize) {
        let mut editor = self.editor.borrow_mut();
        editor.set_cols(cols);
    }

    pub async fn consume_key(&self, key: KeyEvent) -> Result<ShellSignal> {
        let mut editor = self.editor.borrow_mut();

        match editor.consume_key(key)? {
            Signal::KeepEditing => Ok(ShellSignal::Continue),
            Signal::InputCompleted(query) => {
                let query = query.to_string();
                let mut session = self.session.borrow_mut();

                match session.as_mut() {
                    Some(session) => {
                        let width = editor.get_cols();
                        let mut writer = editor.raw_writer();
                        writer.write(&[b'\n'])?;

                        match session.simple(&query).await {
                            Ok(results) => {
                                for result in results {
                                    match Self::format_execution_stream(result, width).await {
                                        Ok(table) => {
                                            writeln!(writer, "{table}")?;
                                        }
                                        Err(e) => {
                                            // Same as below, the error is
                                            // related to executing a query.
                                            writeln!(writer, "{e}")?;

                                            return Ok(ShellSignal::Continue);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                // We're not returning the error here since it's
                                // related to the user input. We want to show
                                // the error to the user.
                                writeln!(writer, "{e}")?;
                            }
                        }

                        editor.edit_start()?;

                        Ok(ShellSignal::Continue)
                    }
                    None => {
                        return Err(RayexecError::new(
                            "Attempted to run query without attached session",
                        ))
                    }
                }
            }
            Signal::Exit => Ok(ShellSignal::Exit),
        }
    }

    /// Collects the entire stream in memory and creates a pretty table from the
    /// stream.
    async fn format_execution_stream(result: ExecutionResult, width: usize) -> Result<PrettyTable> {
        let batches = result
            .stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let table = PrettyTable::try_new(&result.output_schema, &batches, width, None)?;

        Ok(table)
    }
}
