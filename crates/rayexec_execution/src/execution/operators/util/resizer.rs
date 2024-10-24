use std::sync::Arc;

use rayexec_bullet::batch::Batch;
use rayexec_bullet::selection::SelectionVector;
use rayexec_error::Result;

use crate::execution::computed_batch::ComputedBatches;

/// Resize input batches to produce output batches of a target size.
#[derive(Debug)]
pub struct BatchResizer {
    /// Target batch size.
    target: usize,
    /// Pending input batches.
    pending: Vec<Batch>,
    /// Current total row count for all batches.
    pending_row_count: usize,
}

impl BatchResizer {
    /// Try to push a new batch to the resizer, returning possibly resized
    /// batches.
    ///
    /// Typically this will return either no batches or a single batch. However
    /// there is a case where this can return multiple batches if 'len(input) +
    /// pending_row_count > target * 2' (aka very large input batch).
    pub fn try_push(&mut self, batch: Batch) -> Result<ComputedBatches> {
        if self.pending_row_count + batch.num_rows() == self.target {
            self.pending.push(batch);

            // Concat, return, set pending len = 0

            unimplemented!()
        }

        if self.pending_row_count + batch.num_rows() > self.target {
            let diff = (self.pending_row_count + batch.num_rows()) - self.target;

            // TODO: May need to continually split batch b.

            // Generate selection vectors that logically slice this batch.
            //
            // Batch 'a' will be included in the current set of batches that
            // will concatenated, batch 'b' will initialize the next set.
            let sel_a = SelectionVector::with_range(0..diff);
            let sel_b = SelectionVector::with_range(diff..batch.num_rows());

            let batch_a = batch.select(Arc::new(sel_a));
            let batch_b = batch.select(Arc::new(sel_b));

            self.pending.push(batch_a);

            {
                // Concat, clear vec, push batch b, set pending len = len(b)
                unimplemented!()
            }
        }

        // Otherwise just add to pending batches.
        self.pending_row_count += batch.num_rows();
        self.pending.push(batch);

        Ok(ComputedBatches::None)
    }

    pub fn flush_remaining(&mut self) -> Result<ComputedBatches> {
        // Concat ...
        unimplemented!()
    }
}
