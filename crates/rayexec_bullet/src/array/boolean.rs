use crate::bitmap::Bitmap;
use std::fmt::Debug;

/// A logical array for representing bools.
#[derive(Debug, PartialEq)]
pub struct BooleanArray {
    validity: Option<Bitmap>,
    values: Bitmap,
}

impl BooleanArray {
    pub fn new_with_values(values: Bitmap) -> Self {
        BooleanArray {
            validity: None,
            values,
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        let valid = self
            .validity
            .as_ref()
            .map(|bm| bm.value(idx))
            .unwrap_or(true);

        Some(valid)
    }

    pub fn value(&self, idx: usize) -> Option<bool> {
        if idx >= self.len() {
            return None;
        }

        Some(self.values.value(idx))
    }

    /// Get the number of non-null true values in the array.
    pub fn true_count(&self) -> usize {
        match &self.validity {
            Some(validity) => {
                assert_eq!(validity.len(), self.values.len());
                // TODO: Could probably go byte by byte instead bit by bit.
                self.values
                    .iter()
                    .zip(validity.iter())
                    .fold(
                        0,
                        |acc, (valid, is_true)| if valid && is_true { acc + 1 } else { acc },
                    )
            }
            None => self.values.popcnt(),
        }
    }

    pub fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    pub fn values(&self) -> &Bitmap {
        &self.values
    }
}

impl FromIterator<bool> for BooleanArray {
    fn from_iter<T: IntoIterator<Item = bool>>(iter: T) -> Self {
        Self::new_with_values(Bitmap::from_iter(iter))
    }
}

impl FromIterator<Option<bool>> for BooleanArray {
    fn from_iter<T: IntoIterator<Item = Option<bool>>>(iter: T) -> Self {
        let mut validity = Bitmap::default();
        let mut bools = Bitmap::default();

        for item in iter {
            match item {
                Some(value) => {
                    validity.push(true);
                    bools.push(value);
                }
                None => {
                    validity.push(false);
                    bools.push(false);
                }
            }
        }

        BooleanArray {
            validity: Some(validity),
            values: bools,
        }
    }
}

#[derive(Debug)]
pub struct BooleanArrayIter<'a> {
    idx: usize,
    values: &'a Bitmap,
    validity: Option<&'a Bitmap>,
}

impl Iterator for BooleanArrayIter<'_> {
    type Item = Option<bool>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.values.len() {
            None
        } else if self.validity.map(|v| v.value(self.idx)).unwrap_or(true) {
            let val = self.values.value(self.idx);
            self.idx += 1;
            Some(Some(val))
        } else {
            Some(None)
        }
    }
}
