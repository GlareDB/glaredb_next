use std::fmt::Write;

use crate::vt100;

const PROMPT_START: &'static str = ">> ";
const PROMPT_CONTINUATION: &'static str = ".. ";

const PROMPT_WIDTH: usize = PROMPT_START.len();

pub trait Renderer {
    fn write_char(&mut self, ch: char);
    fn write_str(&mut self, s: &str);
}

/// Line editor.
#[derive(Debug)]
pub struct LineEditor {
    /// Pending output.
    output: String,

    /// All text written so far (for this prompt).
    text: String,

    /// Current cursor position.
    pos: usize,

    /// Current width of the terminal.
    width: usize,
}

impl LineEditor {
    pub fn new() -> Self {
        LineEditor {
            output: String::new(),
            text: String::new(),
            pos: 0,
            width: 80,
        }
    }

    pub fn set_width(&mut self, width: usize) {
        self.width = width;
    }

    pub fn width(&self) -> usize {
        self.width
    }

    pub fn reset(&mut self) {
        self.output.clear();
        self.output.push_str(PROMPT_START);
        self.text.clear();
        self.pos = 0;
    }

    pub fn current_input(&self) -> &str {
        &self.text
    }

    pub fn pending(&self) -> &str {
        &self.output
    }

    pub fn clear_pending(&mut self) {
        self.output.clear();
    }

    pub fn insert_char(&mut self, ch: char) {
        if self.chars_len() + ch.len_utf8() >= self.width {
            self.output.push_str(vt100::CRLF);
            self.output.push_str(PROMPT_CONTINUATION);
        }
        self.output.push(ch);
        self.text.push(ch);
        self.pos += 1;
    }

    pub fn erase_char(&mut self) {}

    pub fn insert_newline(&mut self) {
        self.text.push('\n');
        self.output.push_str(vt100::CRLF);
        self.output.push_str(PROMPT_CONTINUATION);
    }

    fn chars_len(&self) -> usize {
        self.text.chars().map(|c| c.len_utf8()).sum()
    }
}
