use std::ops::Range;

/// Maps a logical row index to the physical location in the array.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionVector {
    indices: Vec<usize>,
}

impl SelectionVector {
    /// Create a new empty selection vector. Logically this indices no rows.
    pub const fn empty() -> Self {
        SelectionVector {
            indices: Vec::new(),
        }
    }

    /// Create a selection vector with a linear mapping to a range of rows.
    pub fn with_range(range: Range<usize>) -> Self {
        SelectionVector {
            indices: range.collect(),
        }
    }

    /// Get the location of a logical index.
    ///
    /// Panics if `idx` is out of bounds.
    #[inline]
    pub fn get_unchecked(&self, idx: usize) -> usize {
        self.indices[idx]
    }

    /// Sets the location for a logical index.
    ///
    /// Panics if `idx` is out of bounds.
    pub fn set_unchecked(&mut self, idx: usize, location: usize) {
        self.indices[idx] = location
    }

    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.indices.iter().copied()
    }

    pub fn num_rows(&self) -> usize {
        self.indices.len()
    }
}

/// Gets the physical row index for a logical index.
///
/// If `selection` is None, the index maps directly to the physical location.
#[inline]
pub fn get_unchecked(selection: Option<&SelectionVector>, idx: usize) -> usize {
    match selection {
        Some(s) => s.get_unchecked(idx),
        None => idx,
    }
}
