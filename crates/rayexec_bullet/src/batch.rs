use crate::array::Array;
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;

/// A batch of same-length arrays.
#[derive(Debug, PartialEq)]
pub struct Batch {
    /// Columns that make up this batch.
    cols: Vec<Arc<Array>>,

    /// Number of rows in this batch. Needed to allow for a batch that has no
    /// columns but a non-zero number of rows.
    num_rows: usize,
}

impl Batch {
    pub fn empty() -> Self {
        Batch {
            cols: Vec::new(),
            num_rows: 0,
        }
    }

    pub fn empty_with_num_rows(num_rows: usize) -> Self {
        Batch {
            cols: Vec::new(),
            num_rows,
        }
    }

    pub fn try_new(cols: Vec<Array>) -> Result<Self> {
        let len = match cols.first() {
            Some(arr) => arr.len(),
            None => return Ok(Self::empty()),
        };

        for col in &cols {
            if col.len() != len {
                return Err(RayexecError::new(format!(
                    "Expected column length to be {len}, got {}",
                    col.len()
                )));
            }
        }

        let cols = cols.into_iter().map(|col| Arc::new(col)).collect();

        Ok(Batch {
            cols,
            num_rows: len,
        })
    }

    /// Project a batch using the provided indices.
    ///
    /// Panics if any index is out of bounds.
    pub fn project(&self, indices: &[usize]) -> Self {
        let cols = indices.iter().map(|idx| self.cols[*idx].clone()).collect();

        Batch {
            cols,
            num_rows: self.num_rows,
        }
    }

    pub fn column(&self, idx: usize) -> Option<&Arc<Array>> {
        self.cols.get(idx)
    }

    pub fn columns(&self) -> &[Arc<Array>] {
        &self.cols
    }

    pub fn num_columns(&self) -> usize {
        self.cols.len()
    }

    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}
