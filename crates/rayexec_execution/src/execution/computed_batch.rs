use std::collections::VecDeque;

use rayexec_bullet::{batch::Batch, bitmap::Bitmap, compute::filter::filter};
use rayexec_error::{RayexecError, Result};

// TODO: Remove
/// A computed batch from an operator.
#[derive(Debug, Clone, PartialEq)]
pub struct ComputedBatch {
    pub(crate) batch: Batch,
    /// An optional selection bitmap indicating the logical rows for this batch.
    ///
    /// If None, all rows are considered selected.
    pub(crate) selection: Option<Bitmap>,
}

impl ComputedBatch {
    pub const fn empty() -> Self {
        ComputedBatch {
            batch: Batch::empty(),
            selection: None,
        }
    }

    pub fn try_with_selection(batch: Batch, selection: Bitmap) -> Result<Self> {
        if batch.num_rows() != selection.len() {
            return Err(RayexecError::new(format!(
                "Num rows does not equal selection length, num rows: {}, selection len: {}",
                batch.num_rows(),
                selection.len()
            )));
        }

        Ok(ComputedBatch {
            batch,
            selection: Some(selection),
        })
    }

    pub fn num_selected_rows(&self) -> usize {
        match &self.selection {
            Some(selection) => selection.count_trues(),
            None => self.batch.num_rows(),
        }
    }

    pub fn try_materialize(self) -> Result<Batch> {
        match self.selection {
            Some(selection) => {
                let arrays = self
                    .batch
                    .columns2()
                    .iter()
                    .map(|a| filter(a, &selection))
                    .collect::<Result<Vec<_>, _>>()?;

                let batch = if arrays.is_empty() {
                    // If we're working on an empty input batch, just produce an new
                    // empty batch with num rows equaling the number of trues in the
                    // selection.
                    Batch::empty_with_num_rows(selection.count_trues())
                } else {
                    // Otherwise use the actual filtered arrays.
                    Batch::try_new2(arrays)?
                };

                Ok(batch)
            }
            None => Ok(self.batch),
        }
    }
}

impl From<Batch> for ComputedBatch {
    fn from(value: Batch) -> Self {
        ComputedBatch {
            batch: value,
            selection: None,
        }
    }
}

/// Computed batch results from an operator.
#[derive(Debug, Clone, PartialEq)] // TODO: Remove clone.
pub enum ComputedBatches {
    /// A single batch was computed.
    Single(ComputedBatch),
    /// Multiple batches were computed.
    ///
    /// These should be ordered by which batch should be pushed to next operator
    /// first.
    Multi(VecDeque<ComputedBatch>),
    /// No batches computed.
    None,
    // TODO: Spill references
}

impl ComputedBatches {
    pub fn new_multi<B>(batches: impl IntoIterator<Item = B>) -> Self
    where
        B: Into<ComputedBatch>,
    {
        Self::Multi(batches.into_iter().map(|b| b.into()).collect())
    }

    /// Checks if this collection of batches is empty.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Multi(batches) => batches.is_empty(),
            Self::None => true,
            Self::Single(_) => false,
        }
    }

    /// Checks if this collection of batches actually contains a batch.
    pub fn has_batches(&self) -> bool {
        !self.is_empty()
    }

    /// Takes the current collection of batches, and replaces it with None.
    pub fn take(&mut self) -> Self {
        std::mem::replace(self, ComputedBatches::None)
    }

    /// Tries to get the next batch from this collection, returning None when no
    /// batches remain.
    pub fn try_next(&mut self) -> Result<Option<ComputedBatch>> {
        match self {
            Self::Single(_) => {
                let orig = std::mem::replace(self, Self::None);
                let batch = match orig {
                    Self::Single(batch) => batch,
                    _ => unreachable!(),
                };

                Ok(Some(batch))
            }
            Self::Multi(batches) => Ok(batches.pop_front()),
            Self::None => Ok(None),
        }
    }
}

impl From<ComputedBatch> for ComputedBatches {
    fn from(value: ComputedBatch) -> Self {
        Self::Single(value)
    }
}

impl From<Batch> for ComputedBatches {
    fn from(value: Batch) -> Self {
        Self::Single(value.into())
    }
}
