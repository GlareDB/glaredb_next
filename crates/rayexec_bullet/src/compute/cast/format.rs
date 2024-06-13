//! Utilities for writing values into strings (and other buffers).
use std::{
    fmt::{self, Display, Write as _},
    marker::PhantomData,
};

use chrono::{DateTime, Utc};

use crate::compute::cast::parse::SECONDS_IN_DAY;

/// Logic for formatting and writing a type to a buffer.
pub trait Formatter {
    /// Type we're formatting.
    type Type;

    /// Write the value to the buffer.
    fn write<W: fmt::Write>(&mut self, val: &Self::Type, buf: &mut W) -> fmt::Result;
}

/// Formatter that uses the type's `Display` implmentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DisplayFormatter<T: Display> {
    _type: PhantomData<T>,
}

impl<T: Display> Formatter for DisplayFormatter<T> {
    type Type = T;
    fn write<W: fmt::Write>(&mut self, val: &Self::Type, buf: &mut W) -> fmt::Result {
        write!(buf, "{val}")
    }
}

pub type BoolFormatter = DisplayFormatter<bool>;
pub type Int8Formatter = DisplayFormatter<i8>;
pub type Int16Formatter = DisplayFormatter<i16>;
pub type Int32Formatter = DisplayFormatter<i32>;
pub type Int64Formatter = DisplayFormatter<i64>;
pub type UInt8Formatter = DisplayFormatter<u8>;
pub type UInt16Formatter = DisplayFormatter<u16>;
pub type UInt32Formatter = DisplayFormatter<u32>;
pub type UInt64Formatter = DisplayFormatter<u64>;
pub type Float32Formatter = DisplayFormatter<f32>;
pub type Float64Formatter = DisplayFormatter<f64>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalFormatter<T: Display> {
    precision: u8,
    scale: i8,
    buf: String,
    _type: PhantomData<T>,
}

pub type Decimal64Formatter = DecimalFormatter<i64>;
pub type Decimal128Formatter = DecimalFormatter<i128>;

impl<T: Display> DecimalFormatter<T> {
    pub fn new(precision: u8, scale: i8) -> Self {
        DecimalFormatter {
            precision,
            scale,
            buf: String::new(),
            _type: PhantomData,
        }
    }
}

impl<T: Display> Formatter for DecimalFormatter<T> {
    type Type = T;
    fn write<W: fmt::Write>(&mut self, val: &Self::Type, buf: &mut W) -> fmt::Result {
        self.buf.clear();
        if self.scale > 0 {
            write!(&mut self.buf, "{val}").expect("string write to not fail");
            if self.buf.len() <= self.scale as usize {
                let pad = self.scale.unsigned_abs() as usize;
                write!(buf, "0.{val:0>pad$}")
            } else {
                self.buf.insert(self.buf.len() - self.scale as usize, '.');
                write!(buf, "{}", self.buf)
            }
        } else if self.scale < 0 {
            write!(&mut self.buf, "{val}").expect("string write to not fail");
            let pad = self.buf.len() + self.scale.unsigned_abs() as usize;
            write!(buf, "{val:0<pad$}")
        } else {
            write!(buf, "{val}")
        }
    }
}

/// Trait for converting an i64 to a Chrono DateTime;
pub trait DateTimeFromTimestamp {
    fn from(val: i64) -> Option<DateTime<Utc>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DateTimeFromSeconds;
impl DateTimeFromTimestamp for DateTimeFromSeconds {
    fn from(val: i64) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp(val, 0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DateTimeFromMilliseconds;
impl DateTimeFromTimestamp for DateTimeFromMilliseconds {
    fn from(val: i64) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_millis(val)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DateTimeFromMicroseconds;
impl DateTimeFromTimestamp for DateTimeFromMicroseconds {
    fn from(val: i64) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_micros(val)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DateTimeFromNanoseconds;
impl DateTimeFromTimestamp for DateTimeFromNanoseconds {
    fn from(val: i64) -> Option<DateTime<Utc>> {
        Some(DateTime::from_timestamp_nanos(val))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimestampFormatter<T: DateTimeFromTimestamp> {
    _type: PhantomData<T>,
}

pub type TimestampSecondsFormatter = TimestampFormatter<DateTimeFromSeconds>;
pub type TimestampMillisecondsFormatter = TimestampFormatter<DateTimeFromMilliseconds>;
pub type TimestampMicrosecondsFormatter = TimestampFormatter<DateTimeFromMicroseconds>;
pub type TimestampNanosecondsFormatter = TimestampFormatter<DateTimeFromNanoseconds>;

impl<T: DateTimeFromTimestamp> Formatter for TimestampFormatter<T> {
    type Type = i64;
    fn write<W: fmt::Write>(&mut self, val: &Self::Type, buf: &mut W) -> fmt::Result {
        let datetime = T::from(*val).ok_or_else(|| fmt::Error)?;
        write!(buf, "{datetime}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date32Formatter;

impl Formatter for Date32Formatter {
    type Type = i32;
    fn write<W: fmt::Write>(&mut self, val: &Self::Type, buf: &mut W) -> fmt::Result {
        let datetime = DateTime::from_timestamp((*val as i64) * SECONDS_IN_DAY, 0)
            .ok_or_else(|| fmt::Error)?;
        write!(buf, "{}", datetime.format("%Y-%m-%d"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date64Formatter;

impl Formatter for Date64Formatter {
    type Type = i64;
    fn write<W: fmt::Write>(&mut self, val: &Self::Type, buf: &mut W) -> fmt::Result {
        let datetime = DateTime::from_timestamp_millis(*val).ok_or_else(|| fmt::Error)?;
        write!(buf, "{datetime}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_positive_scale() {
        let mut formatter = Decimal64Formatter::new(6, 3);
        let mut buf = String::new();
        formatter.write(&123450, &mut buf).unwrap();
        assert_eq!("123.450", buf);

        let mut buf = String::new();
        formatter.write(&123, &mut buf).unwrap();
        assert_eq!("0.123", buf);

        let mut buf = String::new();
        formatter.write(&12, &mut buf).unwrap();
        assert_eq!("0.012", buf);
    }

    #[test]
    fn decimal_negative_scale() {
        let mut formatter = Decimal64Formatter::new(6, -3);
        let mut buf = String::new();
        formatter.write(&123450, &mut buf).unwrap();
        assert_eq!("123450000", buf);

        let mut buf = String::new();
        formatter.write(&23, &mut buf).unwrap();
        assert_eq!("23000", buf);
    }

    #[test]
    fn decimal_zero_scale() {
        let mut formatter = Decimal64Formatter::new(6, 0);
        let mut buf = String::new();
        formatter.write(&123450, &mut buf).unwrap();
        assert_eq!("123450", buf);

        let mut buf = String::new();
        formatter.write(&23, &mut buf).unwrap();
        assert_eq!("23", buf);
    }

    #[test]
    fn date32_basic() {
        let mut formatter = Date32Formatter;
        let mut buf = String::new();
        formatter.write(&8319, &mut buf).unwrap();
        assert_eq!("1992-10-11", buf);
    }
}
