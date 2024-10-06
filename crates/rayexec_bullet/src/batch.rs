use crate::{
    array::{Array, Array2, Selection},
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

    /// Create a new batch from some number of arrays.
    ///
    /// All arrays should have the same logical length.
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

    // TODO: Owned variant
    pub fn project(&self, indices: &[usize]) -> Self {
        let cols = indices.iter().map(|idx| self.cols[*idx].clone()).collect();

        Batch {
            cols2: Vec::new(),
            cols,
            num_rows: self.num_rows,
        }
    }

    pub fn slice(&self, offset: usize, count: usize) -> Self {
        let cols = self.cols.iter().map(|c| c.slice(offset, count)).collect();
        Batch {
            cols2: Vec::new(),
            cols,
            num_rows: count,
        }
    }

    /// Selects rows in the batch.
    pub fn select(&self, selection: impl Into<Selection>) -> Batch {
        let selection = selection.into();
        let cols = self
            .cols
            .iter()
            .map(|c| {
                let mut col = c.clone();
                col.select_mut(&selection);
                col
            })
            .collect();

        Batch {
            cols2: Vec::new(),
            cols,
            num_rows: selection.as_ref().num_rows(),
        }
    }

    /// Get the row at some index.
    pub fn row(&self, idx: usize) -> Option<ScalarRow> {
        if idx >= self.num_rows {
            return None;
        }

        // Non-zero number of rows, but no actual columns. Just return an empty
        // row.
        if self.cols.is_empty() {
            return Some(ScalarRow::empty());
        }

        let row = self.cols.iter().map(|col| col.logical_value(idx).unwrap());

        Some(ScalarRow::from_iter(row))
    }

    pub fn column(&self, idx: usize) -> Option<&Array> {
        self.cols.get(idx)
    }

    pub fn columns(&self) -> &[Array] {
        &self.cols
    }

    pub fn num_columns(&self) -> usize {
        self.cols.len()
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    pub fn into_arrays(self) -> Vec<Array> {
        self.cols
    }
}
