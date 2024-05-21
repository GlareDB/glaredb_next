/// A logical array for representing some number of Nulls.
#[derive(Debug, PartialEq)]
pub struct NullArray {
    len: usize,
}

impl NullArray {
    pub fn new(len: usize) -> Self {
        NullArray { len }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_valid(&self, idx: usize) -> Option<bool> {
        if idx >= self.len {
            return None;
        }
        Some(false)
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        if len < self.len {
            self.len = len;
        }
    }
}
