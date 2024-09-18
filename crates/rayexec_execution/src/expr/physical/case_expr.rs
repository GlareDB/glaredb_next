use std::{fmt, sync::Arc};

use rayexec_bullet::{array::Array, batch::Batch, bitmap::Bitmap, compute};
use rayexec_error::{RayexecError, Result};

use super::PhysicalScalarExpression;

#[derive(Debug, Clone)]
pub struct PhyscialWhenThen {
    pub when: PhysicalScalarExpression,
    pub then: PhysicalScalarExpression,
}

impl fmt::Display for PhyscialWhenThen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WHEN {} THEN {}", self.when, self.then)
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalCaseExpr {
    pub cases: Vec<PhyscialWhenThen>,
    pub else_expr: Box<PhysicalScalarExpression>,
}

/// Array output for a single case alongside the selection bitmap used.
struct CaseOutput {
    when_bitmap: Bitmap,
    arr: Arc<Array>,
}

impl PhysicalCaseExpr {
    pub fn eval(&self, batch: &Batch, selection: Option<&Bitmap>) -> Result<Arc<Array>> {
        // TODO: Don't think this is necessary, we could probably just encode
        // the selection in the initial 'needs_results' bitmap.
        let batch = match selection {
            Some(selection) => Batch::try_new(
                batch
                    .columns()
                    .iter()
                    .map(|c| compute::filter::filter(c.as_ref(), selection))
                    .collect::<Result<Vec<_>>>()?,
            )?,
            None => batch.clone(),
        };

        let mut case_outputs = Vec::new();
        let mut needs_results = Bitmap::all_true(batch.num_rows());

        for case in &self.cases {
            // No need to evaluate any more cases.
            if needs_results.count_trues() == 0 {
                break;
            }

            let when_result = case.when.eval(&batch, Some(&needs_results))?;
            let when_bitmap = match when_result.as_ref() {
                Array::Boolean(arr) => arr.clone().into_selection_bitmap(),
                other => {
                    return Err(RayexecError::new(format!(
                        "WHEN returned non-bool results: {}",
                        other.datatype()
                    )))
                }
            };

            // No cases returned true.
            if when_bitmap.count_trues() == 0 {
                continue;
            }

            let then_result = case.then.eval(&batch, Some(&when_bitmap))?;

            // Update bitmap to skip these rows in the next case.
            needs_results.bit_and_not_mut(&when_bitmap)?;

            case_outputs.push(CaseOutput {
                when_bitmap,
                arr: then_result,
            })
        }

        // Evaluate any remaining rows.
        if needs_results.count_trues() != 0 {
            let else_result = self.else_expr.eval(&batch, Some(&needs_results))?;
            case_outputs.push(CaseOutput {
                when_bitmap: needs_results,
                arr: else_result,
            })
        }

        // All rows accounted for, compute 'interleave' indices for building the
        // final batch.
        let indices: Vec<(usize, usize)> = case_outputs
            .iter()
            .enumerate()
            .flat_map(|(arr_idx, case)| {
                case.when_bitmap
                    .index_iter()
                    .map(move |row_idx| (arr_idx, row_idx))
            })
            .collect();

        let arrs: Vec<_> = case_outputs.iter().map(|case| case.arr.as_ref()).collect();
        let out = compute::interleave::interleave(&arrs, &indices)?;

        Ok(Arc::new(out))
    }
}

impl fmt::Display for PhysicalCaseExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CASE ")?;
        for case in &self.cases {
            write!(f, "{}", case)?;
        }
        write!(f, "ELSE {}", self.else_expr)?;

        Ok(())
    }
}
