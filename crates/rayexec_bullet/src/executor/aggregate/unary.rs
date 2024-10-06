use rayexec_error::Result;

use crate::{
    array::{Array, ArrayAccessor},
    bitmap::{zip::ZipBitmapsIter, Bitmap},
    executor::physical_type::PhysicalStorage,
    selection,
    storage::AddressableStorage,
};

use super::{AggregateState, RowToStateMapping};

#[derive(Debug, Clone, Copy)]
pub struct UnaryNonNullUpdater;

impl UnaryNonNullUpdater {
    pub fn update<'a, S, I, State, Output>(
        array: &'a Array,
        mapping: I,
        states: &mut [State],
    ) -> Result<()>
    where
        S: PhysicalStorage<'a>,
        I: IntoIterator<Item = RowToStateMapping>,
        State: AggregateState<<S::Storage as AddressableStorage>::T, Output>,
    {
        let selection = array.selection_vector();

        match &array.validity {
            Some(validity) => {
                let values = S::get_storage(&array.data)?;

                for mapping in mapping {
                    let sel = selection::get_unchecked(selection, mapping.from_row);
                    if !validity.value_unchecked(sel) {
                        // Null, continue.
                        continue;
                    }

                    let val = unsafe { values.get_unchecked(sel) };
                    let state = &mut states[mapping.to_state];

                    state.update(val)?;
                }
            }
            None => {
                let values = S::get_storage(&array.data)?;

                for mapping in mapping {
                    let sel = selection::get_unchecked(selection, mapping.from_row);
                    let val = unsafe { values.get_unchecked(sel) };
                    let state = &mut states[mapping.to_state];

                    state.update(val)?;
                }
            }
        }

        Ok(())
    }
}

/// Updates aggregate states for an aggregate that accepts one input.
#[derive(Debug, Clone, Copy)]
pub struct UnaryNonNullUpdate2;

impl UnaryNonNullUpdate2 {
    /// Updates a list of target states from some inputs.
    ///
    /// The row selection bitmap indicates which rows from the input to use for
    /// the update, and the mapping slice maps rows to target states.
    ///
    /// Values that are considered null (not valid) will not be passed to the
    /// state for udpates.
    pub fn update<Array, Type, Iter, State, Output>(
        row_selection: &Bitmap,
        inputs: Array,
        mapping: &[usize],
        target_states: &mut [State],
    ) -> Result<()>
    where
        Array: ArrayAccessor<Type, ValueIter = Iter>,
        Iter: Iterator<Item = Type>,
        State: AggregateState<Type, Output>,
    {
        debug_assert_eq!(
            row_selection.count_trues(),
            mapping.len(),
            "number of rows selected in input must equal length of mappings"
        );

        match inputs.validity() {
            Some(validity) => {
                // Skip rows that are not selected or are not valid.
                let should_compute = ZipBitmapsIter::try_new([row_selection, validity])?;

                let mut mapping_idx = 0;
                for (input, should_compute) in inputs.values_iter().zip(should_compute) {
                    if !should_compute {
                        continue;
                    }
                    let target = &mut target_states[mapping[mapping_idx]];
                    target.update(input)?;
                    mapping_idx += 1;
                }
            }
            None => {
                let mut mapping_idx = 0;
                for (selected, input) in row_selection.iter().zip(inputs.values_iter()) {
                    if !selected {
                        continue;
                    }
                    let target = &mut target_states[mapping[mapping_idx]];
                    target.update(input)?;
                    mapping_idx += 1;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::physical_type::PhysicalI32;

    use super::*;

    #[derive(Debug, Default)]
    struct TestSumState {
        val: i32,
    }

    impl AggregateState<i32, i32> for TestSumState {
        fn merge(&mut self, other: Self) -> Result<()> {
            self.val += other.val;
            Ok(())
        }

        fn update(&mut self, input: i32) -> Result<()> {
            self.val += input;
            Ok(())
        }

        fn finalize(self) -> Result<(i32, bool)> {
            Ok((self.val, true))
        }
    }

    #[test]
    fn unary_single_state() {
        let mut states = [TestSumState::default()];
        let array = Array::from_iter([1, 2, 3, 4, 5]);
        let mapping = [
            RowToStateMapping {
                from_row: 1,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 3,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 4,
                to_state: 0,
            },
        ];

        UnaryNonNullUpdater::update::<PhysicalI32, _, _, _>(&array, mapping, &mut states).unwrap();

        assert_eq!(11, states[0].val);
    }

    #[test]
    fn unary_single_state_skip_null() {
        let mut states = [TestSumState::default()];
        let array = Array::from_iter([Some(1), Some(2), Some(3), None, Some(5)]);
        let mapping = [
            RowToStateMapping {
                from_row: 1,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 3,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 4,
                to_state: 0,
            },
        ];

        UnaryNonNullUpdater::update::<PhysicalI32, _, _, _>(&array, mapping, &mut states).unwrap();

        assert_eq!(7, states[0].val);
    }

    #[test]
    fn unary_multiple_state() {
        let mut states = [TestSumState::default(), TestSumState::default()];
        let array = Array::from_iter([1, 2, 3, 4, 5]);
        let mapping = [
            RowToStateMapping {
                from_row: 1,
                to_state: 1,
            },
            RowToStateMapping {
                from_row: 0,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 3,
                to_state: 0,
            },
            RowToStateMapping {
                from_row: 4,
                to_state: 1,
            },
        ];

        UnaryNonNullUpdater::update::<PhysicalI32, _, _, _>(&array, mapping, &mut states).unwrap();

        assert_eq!(5, states[0].val);
        assert_eq!(7, states[1].val);
    }
}
