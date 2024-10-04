use rayexec_error::Result;

use crate::{
    array::Array,
    executor::physical_type::{PhysicalBool, PhysicalStorage},
    selection::{self, SelectionVector},
    storage::AddressableStorage,
};

#[derive(Debug, Clone)]
pub struct SelectExecutor;

impl SelectExecutor {
    /// Writes row selections to `output_sel`.
    ///
    /// Errors if the provided array isn't a boolean array.
    pub fn select(bool_array: &Array, output_sel: &mut SelectionVector) -> Result<()> {
        output_sel.clear();
        let selection = bool_array.selection_vector();
        let len = bool_array.logical_len();

        match &bool_array.validity {
            Some(validity) => {
                let values = PhysicalBool::get_storage(&bool_array.data)?;

                for idx in 0..len {
                    if validity.value_unchecked(idx) {
                        continue;
                    }

                    let sel = selection::get_unchecked(selection, idx);
                    let val = unsafe { values.get_unchecked(sel) };

                    if val {
                        output_sel.push_location(idx);
                    }
                }
            }
            None => {
                let values = PhysicalBool::get_storage(&bool_array.data)?;

                for idx in 0..len {
                    let sel = selection::get_unchecked(selection, idx);
                    let val = unsafe { values.get_unchecked(sel) };

                    if val {
                        output_sel.push_location(idx);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_simple() {
        let arr = Array::from_iter([false, true, true, false, true]);
        let mut selection = SelectionVector::with_capacity(5);

        SelectExecutor::select(&arr, &mut selection).unwrap();

        let expected = SelectionVector::from_iter([1, 2, 4]);
        assert_eq!(selection, expected)
    }

    #[test]
    fn select_wrong_type() {
        let arr = Array::from_iter([1, 2, 3, 4, 5]);
        let mut selection = SelectionVector::with_capacity(5);

        SelectExecutor::select(&arr, &mut selection).unwrap_err();
    }
}
