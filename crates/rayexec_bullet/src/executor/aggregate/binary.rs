use rayexec_error::Result;

use crate::{
    array::{validity::union_validities, ArrayAccessor, ValuesBuffer},
    bitmap::{zip::ZipBitmapsIter, Bitmap},
};

use super::AggregateState;

/// Updates aggregate states for an aggregate that accepts two inputs.
#[derive(Debug, Clone, Copy)]
pub struct BinaryUpdater;

impl BinaryUpdater {
    pub fn update<Array1, Type1, Iter1, Array2, Type2, Iter2, State, Output>(
        row_selection: &Bitmap,
        first: Array1,
        second: Array2,
        mapping: &[usize],
        target_states: &mut [State],
    ) -> Result<()>
    where
        Array1: ArrayAccessor<Type1, ValueIter = Iter1>,
        Iter1: Iterator<Item = Type1>,
        Array2: ArrayAccessor<Type2, ValueIter = Iter2>,
        Iter2: Iterator<Item = Type2>,
        State: AggregateState<(Type1, Type2), Output>,
    {
        debug_assert_eq!(
            row_selection.count_trues(),
            mapping.len(),
            "number of rows selected in input must equal length of mappings"
        );

        // Unions both validities, essentially skipping rows where at least one
        // argument is null. This matches the behavior of postgres.
        let validity = union_validities([first.validity(), second.validity()])?;

        let first = first.values_iter();
        let second = second.values_iter();

        match validity {
            Some(validity) => {
                let mut mapping_idx = 0;
                for ((selected, (first, second)), valid) in row_selection
                    .iter()
                    .zip(first.zip(second))
                    .zip(validity.iter())
                {
                    if !selected || !valid {
                        continue;
                    }
                    let target = &mut target_states[mapping[mapping_idx]];
                    target.update((first, second))?;
                    mapping_idx += 1;
                }
            }
            None => {
                let mut mapping_idx = 0;
                for (selected, (first, second)) in row_selection.iter().zip(first.zip(second)) {
                    if !selected {
                        continue;
                    }
                    let target = &mut target_states[mapping[mapping_idx]];
                    target.update((first, second))?;
                    mapping_idx += 1;
                }
            }
        }

        Ok(())
    }
}
