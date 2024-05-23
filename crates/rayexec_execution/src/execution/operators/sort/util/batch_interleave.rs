use rayexec_bullet::{array::Array, batch::Batch, compute};
use rayexec_error::{RayexecError, Result};

/// Helper for interleaving batches.
pub fn interleave_batches(batches: &[Batch], indices: &[(usize, usize)]) -> Result<Batch> {
    let num_cols = match batches.first() {
        Some(batch) => batch.num_columns(),
        None => return Err(RayexecError::new("Cannot interleave zero batches")),
    };

    let mut all_cols: Vec<Vec<&Array>> = Vec::with_capacity(num_cols);

    for idx in 0..num_cols {
        let mut cols = Vec::with_capacity(batches.len());
        for batch in batches {
            let col = batch
                .column(idx)
                .ok_or_else(|| {
                    RayexecError::new(format!("Missing column for batch at idx: {idx}"))
                })?
                .as_ref();
            cols.push(col);
        }
        all_cols.push(cols);
    }

    let mut merged_cols = Vec::with_capacity(num_cols);
    for cols in all_cols {
        let merged = compute::interleave::interleave(&cols, indices)?;
        merged_cols.push(merged);
    }

    let batch = Batch::try_new(merged_cols)?;

    Ok(batch)
}

/// Helper for interleaving optional batches.
pub fn interleave_optional_batches(
    batches: &[Option<Batch>],
    indices: &[(usize, usize)],
) -> Result<Batch> {
    unimplemented!()
}
