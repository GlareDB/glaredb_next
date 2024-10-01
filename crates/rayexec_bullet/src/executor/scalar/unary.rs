use crate::{
    array::{Array, ArrayAccessor, ValuesBuffer},
    executor::{
        builder::{ArrayBuilder, ArrayDataBuffer},
        physical_type::PhysicalType,
    },
    selection,
    storage::AddressableStorage,
};
use rayexec_error::Result;

#[derive(Debug, Clone, Copy)]
pub struct UnaryExecutor;

impl UnaryExecutor {
    pub fn execute<'a, T, O, B>(
        array: &'a Array,
        mut builder: ArrayBuilder<B>,
        op: &mut impl Fn(&mut B::State, &<T::Storage as AddressableStorage>::T) -> O,
    ) -> Result<Array>
    where
        T: PhysicalType<'a>,
        B: ArrayDataBuffer<'a, Type = O>,
    {
        let selection = array.selection_vector();
        let mut state = builder.buffer.state();

        match &array.validity {
            Some(validity) => {
                let values = T::get_storage(&array.data)?;
                let len = values.len();
                for idx in 0..len {
                    if !validity.value_unchecked(idx) {
                        continue;
                    }

                    let sel = selection::get_unchecked(selection, idx);
                    let val = unsafe { values.get_unchecked(sel) };
                    let val = op(&mut state, val);

                    builder.buffer.put(idx, val)
                }
            }
            None => {
                let values = T::get_storage(&array.data)?;
                let len = values.len();
                for idx in 0..len {
                    let sel = selection::get_unchecked(selection, idx);
                    let val = unsafe { values.get_unchecked(sel) };
                    let val = op(&mut state, val);

                    builder.buffer.put(idx, val)
                }
            }
        }

        let data = builder.buffer.into_data();

        Ok(Array {
            datatype: builder.datatype,
            selection: None,
            validity: array.validity.clone(),
            data,
        })
    }
}

/// Execute an operation on a single array.
#[derive(Debug, Clone, Copy)]
pub struct UnaryExecutor2;

impl UnaryExecutor2 {
    /// Execute an infallible operation on an array.
    pub fn execute<Array, Type, Iter, Output>(
        array: Array,
        mut operation: impl FnMut(Type) -> Output,
        buffer: &mut impl ValuesBuffer<Output>,
    ) -> Result<()>
    where
        Array: ArrayAccessor<Type, ValueIter = Iter>,
        Iter: Iterator<Item = Type>,
    {
        match array.validity() {
            Some(validity) => {
                for (value, valid) in array.values_iter().zip(validity.iter()) {
                    if valid {
                        let out = operation(value);
                        buffer.push_value(out);
                    } else {
                        buffer.push_null();
                    }
                }
            }
            None => {
                for value in array.values_iter() {
                    let out = operation(value);
                    buffer.push_value(out);
                }
            }
        }

        Ok(())
    }

    /// Execute a potentially fallible operation on an array.
    pub fn try_execute<Array, Type, Iter, Output>(
        array: Array,
        mut operation: impl FnMut(Type) -> Result<Output>,
        buffer: &mut impl ValuesBuffer<Output>,
    ) -> Result<()>
    where
        Array: ArrayAccessor<Type, ValueIter = Iter>,
        Iter: Iterator<Item = Type>,
    {
        match array.validity() {
            Some(validity) => {
                for (value, valid) in array.values_iter().zip(validity.iter()) {
                    if valid {
                        let out = operation(value)?;
                        buffer.push_value(out);
                    } else {
                        buffer.push_null();
                    }
                }
            }
            None => {
                for value in array.values_iter() {
                    let out = operation(value)?;
                    buffer.push_value(out);
                }
            }
        }

        Ok(())
    }
}
