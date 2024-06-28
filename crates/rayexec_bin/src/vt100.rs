//! VT100 escape codes.
//!
//! https://espterm.github.io/docs/VT100%20escape%20codes.html
#![allow(dead_code)]

use std::io;

pub fn cursor_up_n(n: usize, w: &mut impl io::Write) {
    write!(w, "\x1b[{}A", n).unwrap();
}

pub fn cursor_down_n(n: usize, w: &mut impl io::Write) {
    write!(w, "\x1b[{}B", n).unwrap();
}

pub fn cursor_left_n(n: usize, w: &mut impl io::Write) {
    write!(w, "\x1b[{}D", n).unwrap();
}

pub fn cursor_right_n(n: usize, w: &mut impl io::Write) {
    write!(w, "\x1b[{}C", n).unwrap();
}

fn write_code_then_reset(code: &str, value: &str, w: &mut impl io::Write) {
    write!(w, "{code}{value}{}", MODES_OFF).unwrap();
}

pub fn write_bold(value: &str, w: &mut impl io::Write) {
    write_code_then_reset(MODE_BOLD, value, w)
}

pub fn writeln(w: &mut impl io::Write) {
    write!(w, "{}", CRLF).unwrap()
}

pub const KEY_TAB: u32 = 9;
pub const KEY_ENTER: u32 = 13;
pub const KEY_BACKSPACE: u32 = 8;
pub const KEY_ARROW_LEFT: u32 = 37;
pub const KEY_ARROW_UP: u32 = 38;
pub const KEY_ARROW_RIGHT: u32 = 39;
pub const KEY_ARROW_DOWN: u32 = 40;

pub const COLOR_FG_BLACK: &str = "\x1b[30m";
pub const COLOR_FG_RED: &str = "\x1b[31m";
pub const COLOR_FG_GREEN: &str = "\x1b[32m";
pub const COLOR_FG_YELLOW: &str = "\x1b[33m";
pub const COLOR_FG_BLUE: &str = "\x1b[34m";
pub const COLOR_FG_MAGENTA: &str = "\x1b[35m";
pub const COLOR_FG_CYAN: &str = "\x1b[36m";
pub const COLOR_FG_WHITE: &str = "\x1b[37m";
pub const COLOR_FG_BRIGHT_BLACK: &str = "\x1b[90m";
pub const COLOR_FG_BRIGHT_YELLOW: &str = "\x1b[93m";
pub const COLOR_BG_BLACK: &str = "\x1b[40m";
pub const COLOR_BG_RED: &str = "\x1b[41m";
pub const COLOR_BG_GREEN: &str = "\x1b[42m";
pub const COLOR_BG_YELLOW: &str = "\x1b[43m";
pub const COLOR_BG_MAGENTA: &str = "\x1b[44m";
pub const COLOR_BG_CYAN: &str = "\x1b[46m";
pub const COLOR_BG_WHITE: &str = "\x1b[47m";
pub const COLOR_BG_BRIGHT_BLACK: &str = "\x1b[100m";
pub const COLOR_BG_BRIGHT_RED: &str = "\x1b[101m";
pub const COLOR_BG_BRIGHT_YELLOW: &str = "\x1b[103m";
pub const COLOR_BG_BRIGHT_WHITE: &str = "\x1b[107m";

pub const CURSOR_UP: &str = "\x1b[A";
pub const CURSOR_DOWN: &str = "\x1b[B";
pub const CURSOR_LEFT: &str = "\x1b[D";
pub const CURSOR_RIGHT: &str = "\x1b[C";
pub const CR: char = '\r';
pub const CRLF: &str = "\r\n";
pub const PARAGRAPH_SEPERATOR: char = '\u{2029}';
pub const NEXT_LINE: char = '\u{0085}';

pub const CLEAR_LINE_CURSOR_RIGHT: &str = "\x1b[0K";
pub const CLEAR_LINE_CURSOR_LEFT: &str = "\x1b[1K";
pub const CLEAR_LINE: &str = "\x1b[2K";
pub const CLEAR_SCREEN_CURSOR_DOWN: &str = "\x1b[0J";
pub const CLEAR_SCREEN_CURSOR_UP: &str = "\x1b[1J";
pub const CLEAR_SCREEN: &str = "\x1b[2J";

pub const MODES_OFF: &str = "\x1b[0m";
pub const MODE_BOLD: &str = "\x1b[1m";
pub const MODE_UNDERLINE: &str = "\x1b[4m";
pub const MODE_BLINK: &str = "\x1b[6m";
pub const MODE_REVERSE: &str = "\x1b[7m";

pub const CURSOR_HOME: &str = "\x1b[H";
