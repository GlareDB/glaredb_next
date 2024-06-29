//! VT100 related functionality.
//!
//! https://espterm.github.io/docs/VT100%20escape%20codes.html
#![allow(dead_code)]

use std::io;

pub fn cursor_up(w: &mut impl io::Write, n: usize) -> io::Result<()> {
    write!(w, "\x1b[{}A", n)
}

pub fn cursor_down(w: &mut impl io::Write, n: usize) -> io::Result<()> {
    write!(w, "\x1b[{}B", n)
}

pub fn cursor_left(w: &mut impl io::Write, n: usize) -> io::Result<()> {
    write!(w, "\x1b[{}D", n)
}

pub fn cursor_right(w: &mut impl io::Write, n: usize) -> io::Result<()> {
    write!(w, "\x1b[{}C", n)
}

pub const CLEAR_LINE_CURSOR_RIGHT: &str = "\x1b[0K";
