use crate::bitmap::Bitmap;
use crate::compute::cast::format::IntervalFormatter;
use crate::storage::PrimitiveStorage;
use std::fmt::{self, Debug};
use std::hash::Hash;

use super::{is_valid, ArrayAccessor};

pub trait PrimitiveNumeric: Sized {
    const MIN_VALUE: Self;
    const MAX_VALUE: Self;
    const ZERO_VALUE: Self;

    fn from_str(s: &str) -> Option<Self>;
}

impl PrimitiveNumeric for i8 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for i16 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for i32 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for i64 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for u8 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for u16 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for u32 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for u64 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for f32 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0.0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

impl PrimitiveNumeric for f64 {
    const MIN_VALUE: Self = Self::MIN;
    const MAX_VALUE: Self = Self::MAX;
    const ZERO_VALUE: Self = 0.0;

    fn from_str(s: &str) -> Option<Self> {
        s.parse().ok()
    }
}

/// A representation of an interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub nanos: i64,
}

impl Interval {
    pub const ASSUMED_DAYS_IN_MONTH: i32 = 30; // Matches Postgres
    pub const ASSUMED_HOURS_IN_DAY: i32 = 24; // Matches Postgres

    pub const NANOSECONDS_IN_MICROSECOND: i64 = 1_000;
    pub const NANOSECONDS_IN_MILLISECOND: i64 = 1_000_000;
    pub const NANOSECONDS_IN_SECOND: i64 = 1_000_000_000;
    pub const NANOSECONDS_IN_MINUTE: i64 = 60 * Self::NANOSECONDS_IN_SECOND;
    pub const NANOSECONDS_IN_HOUR: i64 = 60 * Self::NANOSECONDS_IN_MINUTE;

    pub const fn new(months: i32, days: i32, nanos: i64) -> Self {
        Interval {
            months,
            days,
            nanos,
        }
    }

    pub fn add_microseconds(&mut self, microseconds: i64) {
        self.nanos += microseconds * Self::NANOSECONDS_IN_MICROSECOND
    }

    pub fn add_milliseconds(&mut self, milliseconds: i64) {
        self.nanos += milliseconds * Self::NANOSECONDS_IN_MILLISECOND
    }

    pub fn add_seconds(&mut self, seconds: i64) {
        self.nanos += seconds * Self::NANOSECONDS_IN_SECOND
    }

    pub fn add_minutes(&mut self, minutes: i64) {
        self.nanos += minutes * Self::NANOSECONDS_IN_MINUTE
    }

    pub fn add_hours(&mut self, hours: i64) {
        self.nanos += hours * Self::NANOSECONDS_IN_HOUR
    }

    pub fn add_days(&mut self, days: i32) {
        self.days += days
    }

    pub fn add_weeks(&mut self, weeks: i32) {
        self.days += weeks * 7
    }

    pub fn add_months(&mut self, months: i32) {
        self.months += months
    }

    pub fn add_years(&mut self, years: i32) {
        self.months += years * 12
    }

    pub fn add_decades(&mut self, decades: i32) {
        self.add_years(decades * 10)
    }

    pub fn add_centuries(&mut self, centuries: i32) {
        self.add_years(centuries * 100)
    }

    pub fn add_millenium(&mut self, millenium: i32) {
        self.add_years(millenium * 1000)
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::compute::cast::format::Formatter;
        IntervalFormatter.write(self, f)
    }
}

/// Array for storing primitive values.
#[derive(Debug, PartialEq)]
pub struct PrimitiveArray<T> {
    /// Validity bitmap.
    ///
    /// "True" values indicate the value at index is valid, "false" indicates
    /// null.
    validity: Option<Bitmap>,

    /// Underlying primitive values.
    values: PrimitiveStorage<T>,
}

pub type Int8Array = PrimitiveArray<i8>;
pub type Int16Array = PrimitiveArray<i16>;
pub type Int32Array = PrimitiveArray<i32>;
pub type Int64Array = PrimitiveArray<i64>;
pub type Int128Array = PrimitiveArray<i128>;
pub type UInt8Array = PrimitiveArray<u8>;
pub type UInt16Array = PrimitiveArray<u16>;
pub type UInt32Array = PrimitiveArray<u32>;
pub type UInt64Array = PrimitiveArray<u64>;
pub type Float32Array = PrimitiveArray<f32>;
pub type Float64Array = PrimitiveArray<f64>;
pub type TimestampSecondsArray = PrimitiveArray<i64>;
pub type TimestampMillsecondsArray = PrimitiveArray<i64>;
pub type TimestampMicrosecondsArray = PrimitiveArray<i64>;
pub type TimestampNanosecondsArray = PrimitiveArray<i64>;
pub type Date32Array = PrimitiveArray<i32>;
pub type Date64Array = PrimitiveArray<i64>;
pub type IntervalArray = PrimitiveArray<Interval>;

impl<T> PrimitiveArray<T> {
    pub fn new(values: Vec<T>, validity: Option<Bitmap>) -> Self {
        PrimitiveArray {
            values: values.into(),
            validity,
        }
    }

    pub fn len(&self) -> usize {
        self.values.as_ref().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the value at the given index.
    ///
    /// This does not take validity into account.
    pub fn value(&self, idx: usize) -> Option<&T> {
        if idx >= self.len() {
            return None;
        }

        self.values.as_ref().get(idx)
    }

    /// Get the validity at the given index.
    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(is_valid(self.validity.as_ref(), idx))
    }

    /// Get a reference to the underlying validity bitmap.
    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    /// Get a reference to the underlying primitive values.
    pub fn values(&self) -> &PrimitiveStorage<T> {
        &self.values
    }

    /// Get a mutable reference to the underlying primitive values.
    pub(crate) fn values_mut(&mut self) -> &mut PrimitiveStorage<T> {
        &mut self.values
    }

    pub fn iter(&self) -> PrimitiveArrayIter<T> {
        PrimitiveArrayIter {
            idx: 0,
            values: self.values.as_ref(),
        }
    }
}

impl<A> FromIterator<A> for PrimitiveArray<A> {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let values = PrimitiveStorage::from(iter.into_iter().collect::<Vec<_>>());
        PrimitiveArray {
            validity: None,
            values,
        }
    }
}

impl<A: Default> FromIterator<Option<A>> for PrimitiveArray<A> {
    fn from_iter<T: IntoIterator<Item = Option<A>>>(iter: T) -> Self {
        let mut validity = Bitmap::default();
        let mut values = Vec::new();

        for item in iter {
            match item {
                Some(value) => {
                    validity.push(true);
                    values.push(value);
                }
                None => {
                    validity.push(false);
                    values.push(A::default());
                }
            }
        }

        PrimitiveArray {
            validity: Some(validity),
            values: values.into(),
        }
    }
}

impl<T> From<Vec<T>> for PrimitiveArray<T> {
    fn from(value: Vec<T>) -> Self {
        PrimitiveArray {
            values: PrimitiveStorage::Vec(value),
            validity: None,
        }
    }
}

#[derive(Debug)]
pub struct PrimitiveArrayIter<'a, T> {
    idx: usize,
    values: &'a [T],
}

impl<T: Copy> Iterator for PrimitiveArrayIter<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.values.len() {
            None
        } else {
            let val = self.values[self.idx];
            self.idx += 1;
            Some(val)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.values.len() - self.idx,
            Some(self.values.len() - self.idx),
        )
    }
}

impl<'a, T: Copy> ArrayAccessor<T> for &'a PrimitiveArray<T> {
    type ValueIter = PrimitiveArrayIter<'a, T>;

    fn len(&self) -> usize {
        self.values.as_ref().len()
    }

    fn values_iter(&self) -> Self::ValueIter {
        PrimitiveArrayIter {
            idx: 0,
            values: self.values.as_ref(),
        }
    }

    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }
}

#[derive(Debug)]
pub struct PrimitiveArrayBuilder<T> {
    values: Vec<T>,
    validity: Option<Bitmap>,
}

impl<T> PrimitiveArrayBuilder<T> {
    pub fn with_capacity(cap: usize) -> Self {
        PrimitiveArrayBuilder {
            values: Vec::with_capacity(cap),
            validity: None,
        }
    }

    pub fn into_typed_array(self) -> PrimitiveArray<T> {
        PrimitiveArray {
            validity: self.validity,
            values: self.values.into(),
        }
    }
}

/// Wrapper around a primitive array for storing the precision+scale for a
/// decimal type.
#[derive(Debug, PartialEq)]
pub struct DecimalArray<T> {
    precision: u8,
    scale: i8,
    array: PrimitiveArray<T>,
}

pub type Decimal64Array = DecimalArray<i64>;
pub type Decimal128Array = DecimalArray<i128>;

impl<T> DecimalArray<T> {
    pub fn new(precision: u8, scale: i8, array: PrimitiveArray<T>) -> Self {
        DecimalArray {
            precision,
            scale,
            array,
        }
    }

    pub fn get_primitive(&self) -> &PrimitiveArray<T> {
        &self.array
    }

    pub fn precision(&self) -> u8 {
        self.precision
    }

    pub fn scale(&self) -> i8 {
        self.scale
    }
}
