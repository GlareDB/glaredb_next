//! Parsing related utilities for casting from a string to other types.
use chrono::{Datelike, NaiveDate};
use num::PrimInt;
use rayexec_error::RayexecError;
use std::{marker::PhantomData, str::FromStr};

pub const EPOCH_NAIVE_DATE: NaiveDate = match NaiveDate::from_ymd_opt(1970, 01, 01) {
    Some(date) => date,
    _ => unreachable!(),
};

pub const EPOCH_DAYS_FROM_CE: i32 = 719163;

pub const SECONDS_IN_DAY: i64 = 86400;

/// Logic for parsing a string into some type.
pub trait Parser {
    /// The type we'll be producing.
    type Type;

    /// Parse a string into `Type`, returning None if the parse cannot be done.
    fn parse(&mut self, s: &str) -> Option<Self::Type>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoolParser;

impl Parser for BoolParser {
    type Type = bool;
    fn parse(&mut self, s: &str) -> Option<Self::Type> {
        match s {
            "t" | "true" | "TRUE" | "T" => Some(true),
            "f" | "false" | "FALSE" | "F" => Some(false),
            _ => None,
        }
    }
}

/// Parser that uses the stdlib `FromStr` trait.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FromStrParser<T: FromStr> {
    _type: PhantomData<T>,
}

impl<T: FromStr> Parser for FromStrParser<T> {
    type Type = T;
    fn parse(&mut self, s: &str) -> Option<Self::Type> {
        T::from_str(s).ok()
    }
}

pub type Int8Parser = FromStrParser<i8>;
pub type Int16Parser = FromStrParser<i16>;
pub type Int32Parser = FromStrParser<i32>;
pub type Int64Parser = FromStrParser<i64>;
pub type UInt8Parser = FromStrParser<u8>;
pub type UInt16Parser = FromStrParser<u16>;
pub type UInt32Parser = FromStrParser<u32>;
pub type UInt64Parser = FromStrParser<u64>;
pub type Float32Parser = FromStrParser<f32>;
pub type Float64Parser = FromStrParser<f64>;

/// Parse a string date into a number of days since epoch.
///
/// Example formats:
///
/// '1992-10-11'
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date32Parser;

impl Parser for Date32Parser {
    type Type = i32;
    fn parse(&mut self, s: &str) -> Option<Self::Type> {
        let date = NaiveDate::from_str(s).ok()?;
        Some(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimalParser<T: PrimInt> {
    precision: u8,
    scale: i8,
    _type: PhantomData<T>,
}

pub type Decimal64Parser = DecimalParser<i64>;
pub type Decimal128Parser = DecimalParser<i128>;

impl<T: PrimInt> DecimalParser<T> {
    pub fn new(precision: u8, scale: i8) -> Self {
        DecimalParser {
            precision,
            scale,
            _type: PhantomData,
        }
    }
}

impl<T: PrimInt> Parser for DecimalParser<T> {
    type Type = T;
    fn parse(&mut self, s: &str) -> Option<Self::Type> {
        let bs = s.as_bytes();
        let (neg, bs) = match bs.first() {
            Some(b'-') => (true, &bs[1..]),
            Some(b'+') => (false, &bs[1..]),
            _ => (false, bs),
        };

        let mut val = T::zero();
        let mut digits: u8 = 0; // Total number of digits.
        let mut decimals: i8 = 0; // Digits to right of decimal point.

        let ten = T::from(10).unwrap();

        let mut iter = bs.iter();

        // Leading digits.
        while let Some(b) = iter.next() {
            match b {
                b'0'..=b'9' => {
                    // Leading zero.
                    if digits == 0 && *b == b'0' {
                        continue;
                    }
                    digits += 1;
                    val = val.mul(ten);
                    val = val.add(T::from(b - b'0').unwrap());
                }
                b'.' => {
                    break;
                }
                _ => return None,
            }
        }

        // Digits after decimal.
        for b in iter {
            match b {
                b'0'..=b'9' => {
                    if decimals == self.scale {
                        continue;
                    }

                    decimals += 1;
                    digits += 1;
                    val = val.mul(ten);
                    val = val.add(T::from(b - b'0').unwrap());
                }
                b'e' | b'E' => return None,
                _ => return None,
            }
        }

        if self.scale < 0 {
            digits -= self.scale.abs() as u8;
            let exp = self.scale.abs() as u32;
            val = val.div(ten.pow(exp));
        }

        if digits > self.precision {
            return None;
        }

        if (decimals as i8) < self.scale {
            let exp = (self.scale - (decimals as i8)) as u32;
            val = val.mul(ten.pow(exp));
        }

        if neg {
            val = T::zero().sub(val);
        }

        Some(val)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_days() {
        assert_eq!(EPOCH_DAYS_FROM_CE, EPOCH_NAIVE_DATE.num_days_from_ce());
    }

    #[test]
    fn test_parse_date32() {
        assert_eq!(8319, Date32Parser.parse("1992-10-11").unwrap());
        assert_eq!(-1, Date32Parser.parse("1969-12-31").unwrap());
    }

    #[test]
    fn parse_decimal() {
        // Can parse
        assert_eq!(123, Decimal64Parser::new(5, 1).parse("12.3").unwrap());
        assert_eq!(12, Decimal64Parser::new(5, 0).parse("12.3").unwrap());
        assert_eq!(1230, Decimal64Parser::new(5, 1).parse("123").unwrap());
        assert_eq!(-1230, Decimal64Parser::new(5, 1).parse("-123").unwrap());
        assert_eq!(1230, Decimal64Parser::new(5, 2).parse("12.3").unwrap());
        assert_eq!(123, Decimal64Parser::new(3, 1).parse("12.3").unwrap());
        assert_eq!(123, Decimal64Parser::new(3, 0).parse("123.4").unwrap());
        assert_eq!(-1230, Decimal64Parser::new(5, 2).parse("-12.3").unwrap());
        assert_eq!(123, Decimal64Parser::new(5, -2).parse("12300").unwrap());

        // Can't parse
        assert_eq!(None, Decimal64Parser::new(5, 1).parse("1four2.3"));
        assert_eq!(None, Decimal64Parser::new(5, 1).parse("12.3a"));
        assert_eq!(None, Decimal64Parser::new(3, 1).parse("123.4")); // "overflow"
    }
}
