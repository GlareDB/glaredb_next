use crate::{
    array::{Array, Array2},
    row::ScalarRow,
};
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;

/// A batch of same-length arrays.
#[derive(Debug, Clone, PartialEq)]
pub struct Batch {
    /// Columns that make up this batch.
    cols2: Vec<Arc<Array2>>,

    cols: Vec<Array>,

    /// Number of rows in this batch. Needed to allow for a batch that has no
    /// columns but a non-zero number of rows.
    num_rows: usize,
}

impl Batch {
    pub const fn empty() -> Self {
        Batch {
            cols2: Vec::new(),
            cols: Vec::new(),
            num_rows: 0,
        }
    }

    pub fn empty_with_num_rows(num_rows: usize) -> Self {
        Batch {
            cols2: Vec::new(),
            cols: Vec::new(),
            num_rows,
        }
    }

    pub fn try_new(cols: impl IntoIterator<Item = Array>) -> Result<Self> {
        let cols: Vec<_> = cols.into_iter().collect();
        let len = match cols.first() {
            Some(arr) => arr.logical_len(),
            None => return Ok(Self::empty()),
        };

        for (idx, col) in cols.iter().enumerate() {
            if col.logical_len() != len {
                return Err(RayexecError::new(format!(
                    "Expected column length to be {len}, got {}. Column idx: {idx}",
                    col.logical_len()
                )));
            }
        }

        Ok(Batch {
            cols2: Vec::new(),
            cols,
            num_rows: len,
        })
    }

    /// Create a new batch from some number of arrays.
    ///
    /// All arrays should be of the same length.
    pub fn try_new2<A>(cols: impl IntoIterator<Item = A>) -> Result<Self>
    where
        A: Into<Arc<Array2>>,
    {
        let cols: Vec<_> = cols.into_iter().map(|arr| arr.into()).collect();
        let len = match cols.first() {
            Some(arr) => arr.len(),
            None => return Ok(Self::empty()),
        };

        for (idx, col) in cols.iter().enumerate() {
            if col.len() != len {
                return Err(RayexecError::new(format!(
                    "Expected column length to be {len}, got {}. Column idx: {idx}",
                    col.len()
                )));
            }
        }

        Ok(Batch {
            cols2: cols,
            cols: Vec::new(),
            num_rows: len,
        })
    }

    /// Project a batch using the provided indices.
    ///
    /// Panics if any index is out of bounds.
    pub fn project2(&self, indices: &[usize]) -> Self {
        let cols = indices.iter().map(|idx| self.cols2[*idx].clone()).collect();

        Batch {
            cols2: cols,
            cols: Vec::new(),
            num_rows: self.num_rows,
        }
    }

    /// Get the row at some index.
    pub fn row2(&self, idx: usize) -> Option<ScalarRow> {
        if idx >= self.num_rows {
            return None;
        }

        // Non-zero number of rows, but no actual columns. Just return an empty
        // row.
        if self.cols2.is_empty() {
            return Some(ScalarRow::empty());
        }

        let row = self.cols2.iter().map(|col| col.scalar(idx).unwrap());

        Some(ScalarRow::from_iter(row))
    }

    pub fn column2(&self, idx: usize) -> Option<&Arc<Array2>> {
        self.cols2.get(idx)
    }

    pub fn columns2(&self) -> &[Arc<Array2>] {
        &self.cols2
    }

    pub fn columns(&self) -> &[Array] {
        &self.cols
    }

    pub fn num_columns(&self) -> usize {
        self.cols.len()
    }

    pub fn num_columns2(&self) -> usize {
        self.cols2.len()
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        array::{Int32Array, Utf8Array},
        scalar::ScalarValue,
    };

    use super::*;

    #[test]
    fn get_row_simple() {
        let batch = Batch::try_new2([
            Array2::Int32(Int32Array::from_iter([1, 2, 3])),
            Array2::Utf8(Utf8Array::from_iter(["a", "b", "c"])),
        ])
        .unwrap();

        // Expected rows at index 0, 1, and 2
        let expected = [
            ScalarRow::from_iter([ScalarValue::Int32(1), ScalarValue::Utf8("a".into())]),
            ScalarRow::from_iter([ScalarValue::Int32(2), ScalarValue::Utf8("b".into())]),
            ScalarRow::from_iter([ScalarValue::Int32(3), ScalarValue::Utf8("c".into())]),
        ];

        for idx in 0..3 {
            let got = batch.row2(idx).unwrap();
            assert_eq!(expected[idx], got);
        }
    }

    #[test]
    fn get_row_out_of_bounds() {
        let batch = Batch::try_new2([
            Array2::Int32(Int32Array::from_iter([1, 2, 3])),
            Array2::Utf8(Utf8Array::from_iter(["a", "b", "c"])),
        ])
        .unwrap();

        let got = batch.row2(3);
        assert_eq!(None, got);
    }

    #[test]
    fn get_row_no_columns_non_zero_rows() {
        let batch = Batch::empty_with_num_rows(3);

        for idx in 0..3 {
            let got = batch.row2(idx).unwrap();
            assert_eq!(ScalarRow::empty(), got);
        }
    }
}
