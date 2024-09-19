#[derive(Debug, PartialEq, Eq)]
pub struct RowSelection {
    /// Selected rows to evaluate.
    rows: Vec<usize>,
}

impl RowSelection {
    pub fn new_for_row_count(num_rows: usize) -> Self {
        RowSelection {
            rows: (0..num_rows).collect(),
        }
    }
}

/// An iterator that returns true/false for each index that we iterate over.
#[derive(Debug)]
pub struct RowSelectionSelectedIter<'a> {
    curr: usize,
    sel: &'a RowSelection,
}
