use std::ops::Range;

/// Maps a logical row index to the physical location in the array.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionVector {
    indices: Vec<usize>,
}

impl SelectionVector {
    /// Create a new empty selection vector. Logically this means an array has
    /// no rows even if the array physically contains data.
    pub const fn empty() -> Self {
        SelectionVector {
            indices: Vec::new(),
        }
    }

    /// Creates a selection vector that that has all indices in the range [0,n)
    /// point to the same physical index.
    pub fn constant(len: usize, idx: usize) -> Self {
        SelectionVector {
            indices: vec![idx; len],
        }
    }

    /// Create a selection vector with a linear mapping to a range of rows.
    pub fn with_range(range: Range<usize>) -> Self {
        SelectionVector {
            indices: range.collect(),
        }
    }

    /// Try to get the location of an index, returning None if the index is out
    /// of bounds.
    pub fn get(&self, idx: usize) -> Option<usize> {
        self.indices.get(idx).copied()
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

    pub fn slice_unchecked(&self, offset: usize, count: usize) -> Self {
        let indices = self.indices[offset..(offset + count)].to_vec();
        SelectionVector { indices }
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
