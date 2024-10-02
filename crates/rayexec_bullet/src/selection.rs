use std::ops::Range;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionVector {
    indices: Vec<usize>,
}

impl SelectionVector {
    pub const fn new() -> Self {
        SelectionVector {
            indices: Vec::new(),
        }
    }

    pub fn with_range(range: Range<usize>) -> Self {
        SelectionVector {
            indices: range.collect(),
        }
    }

    #[inline]
    pub fn get_unchecked(&self, idx: usize) -> usize {
        self.indices[idx]
    }

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

#[inline]
pub fn get_unchecked(selection: Option<&SelectionVector>, idx: usize) -> usize {
    match selection {
        Some(s) => s.get_unchecked(idx),
        None => idx,
    }
}
