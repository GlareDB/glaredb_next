use rayexec_bullet::{
    array::{Array, BooleanArray},
    batch::Batch,
    compute::take::take,
};
use rayexec_error::{RayexecError, Result};
use std::sync::Arc;

use crate::{
    expr::{comparison_expr::ComparisonOperator, physical::PhysicalScalarExpression},
    functions::scalar::{boolean::AndImpl, PlannedScalarFunction},
};

/// Join condition with the left side precomputed.
///
/// When inserting into the hash table for the left side, the left scalar
/// expression should be executed on the batch with the result being pushed to
/// `lefts`.
#[derive(Debug)]
pub struct LeftPrecomputedJoinCondition {
    /// Precomputed results for left batches.
    pub left_precomputed: Vec<Arc<Array>>,
    /// The left expression.
    pub left: PhysicalScalarExpression,
    /// The right expression.
    pub right: PhysicalScalarExpression,
    /// The function to use for the comparison.
    ///
    /// This should be planned function for the comparison operator this
    /// condition was created for. Assumed to take exactly two inputs (left and
    /// right).
    pub function: Box<dyn PlannedScalarFunction>,
}

/// All conditions for a single join.
#[derive(Debug)]
pub struct LeftPrecomputedJoinConditions {
    pub conditions: Vec<LeftPrecomputedJoinCondition>,
}

impl LeftPrecomputedJoinConditions {
    pub fn precompute_for_left_batch(&mut self, left: &Batch) -> Result<()> {
        for condition in &mut self.conditions {
            let precomputed = condition.left.eval(left)?;
            condition.left_precomputed.push(precomputed)
        }

        Ok(())
    }

    pub fn compute_selection_for_probe(
        &self,
        left_batch_idx: usize,
        left_rows: &[usize],
        right: &Batch,
    ) -> Result<BooleanArray> {
        debug_assert_eq!(left_rows.len(), right.num_rows());

        let mut results = Vec::with_capacity(self.conditions.len());

        for condition in &self.conditions {
            let left_precomputed =
                condition
                    .left_precomputed
                    .get(left_batch_idx)
                    .ok_or_else(|| {
                        RayexecError::new(format!(
                            "Missing left precomputed array: {left_batch_idx}"
                        ))
                    })?;

            // TODO: Use selection instead of taking for left.

            let left_input = Arc::new(take(left_precomputed.as_ref(), left_rows)?);
            let right_input = condition.right.eval(right)?;

            let result = condition.function.execute(&[&left_input, &right_input])?;

            results.push(Arc::new(result));
        }

        let refs: Vec<_> = results.iter().collect();
        let out = match AndImpl.execute(&refs)? {
            Array::Boolean(arr) => arr,
            other => {
                return Err(RayexecError::new(format!(
                    "Expect boolean array as result for condition, got {}",
                    other.datatype()
                )))
            }
        };

        Ok(out)
    }
}
