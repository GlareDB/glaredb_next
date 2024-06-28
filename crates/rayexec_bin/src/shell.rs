use std::cell::RefCell;
use std::io;

use futures::StreamExt;
use rayexec_bullet::format::pretty::table::PrettyTable;
use rayexec_bullet::format::ugly::ugly_print;
use rayexec_error::Result;
use rayexec_execution::engine::session::Session;
use rayexec_execution::engine::Engine;

use crate::editor::LineEditor;
use crate::vt100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyCode {
    Backspace,
    Enter,
    Left,
    Right,
    Up,
    Down,
    Home,
    End,
    Tab,
    BackTab,
    Delete,
    Insert,
    Char(char),
}

#[derive(Debug)]
pub struct Shell<W> {
    inner: RefCell<ShellInner<W>>,
}

#[derive(Debug)]
struct ShellInner<W> {
    prompt: LineEditor,
    writer: W,
    session: Session,
}

impl<W: io::Write> Shell<W> {
    pub fn new(mut writer: W, engine: Engine) -> Self {
        let mut editor = LineEditor::new();
        editor.reset();

        vt100::write_bold("Rayexec Shell", &mut writer);
        vt100::writeln(&mut writer);

        let shell = Shell {
            inner: RefCell::new(ShellInner {
                prompt: editor,
                writer,
                session: engine.new_session().unwrap(),
            }),
        };
        shell.inner.borrow_mut().flush_pending();

        shell
    }

    pub fn set_width(&self, width: usize) {
        let mut inner = self.inner.borrow_mut();
        inner.prompt.set_width(width);
    }

    pub async fn on_key(&self, key: KeyCode) {
        let mut inner = self.inner.borrow_mut();
        match key {
            KeyCode::Char(ch) => {
                inner.prompt.insert_char(ch);
                inner.flush_pending();
            }
            KeyCode::Enter => {
                let input = inner.prompt.current_input().trim().to_string();
                if input.ends_with(";") {
                    // Run the query.
                    vt100::writeln(&mut inner.writer);

                    match inner.session.simple(&input).await {
                        Ok(outputs) => {
                            for output in outputs {
                                let batches = output
                                    .stream
                                    .collect::<Vec<_>>()
                                    .await
                                    .into_iter()
                                    .collect::<Result<Vec<_>>>()
                                    .unwrap();

                                let table = PrettyTable::try_new(
                                    &output.output_schema,
                                    &batches,
                                    inner.prompt.width(),
                                    None,
                                )
                                .unwrap();
                                writeln!(inner.writer, "{table}").unwrap();
                            }
                        }
                        Err(e) => {
                            writeln!(inner.writer, "{}", e.to_string()).unwrap();
                        }
                    }

                    inner.prompt.reset();
                } else {
                    // Just insert a new line.
                    inner.prompt.insert_newline();
                }

                inner.flush_pending();
            }
            other => println!("{other:?}"),
        }
    }
}

impl<W: io::Write> ShellInner<W> {
    fn flush_pending(&mut self) {
        let writer = &mut self.writer;
        write!(writer, "{}", self.prompt.pending()).unwrap();
        writer.flush().unwrap();
        self.prompt.clear_pending();
    }
}
