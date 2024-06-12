//! Parsing related utilities for casting from a string to other types.
use rayexec_error::{RayexecError, Result};
use std::str::FromStr;

use chrono::{Datelike, NaiveDate};

pub const EPOCH_NAIVE_DATE: NaiveDate = match NaiveDate::from_ymd_opt(1970, 01, 01) {
    Some(date) => date,
    _ => unreachable!(),
};

pub const EPOCH_DAYS_FROM_CE: i32 = 719163;

/// Parse a string date into a number of days since epoch.
///
/// Example formats:
///
/// '1992-10-11'
pub fn parse_date32(s: &str) -> Result<i32> {
    let date = NaiveDate::from_str(s).map_err(|e| {
        RayexecError::with_source(format!("Unable to parse 's' into a date"), Box::new(e))
    })?;

    Ok(date.num_days_from_ce() - EPOCH_DAYS_FROM_CE)
}

/// Parse a bool.
pub fn parse_bool(s: &str) -> Result<bool> {
    match s {
        "t" | "true" | "TRUE" | "T" => Ok(true),
        "f" | "false" | "FALSE" | "F" => Ok(false),
        other => Err(RayexecError::new(format!(
            "Unable to convert '{other}' to a bool"
        ))),
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
        assert_eq!(8319, parse_date32("1992-10-11").unwrap());
        assert_eq!(-1, parse_date32("1969-12-31").unwrap());
    }
}
