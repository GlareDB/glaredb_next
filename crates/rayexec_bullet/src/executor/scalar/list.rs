use rayexec_error::{RayexecError, Result};

use crate::array::Array;
use crate::executor::builder::{ArrayBuilder, ArrayDataBuffer, OutputBuffer};
use crate::executor::physical_type::PhysicalStorage;
use crate::executor::scalar::{can_skip_validity_check, validate_logical_len};
use crate::storage::AddressableStorage;

#[derive(Debug, Clone, Copy)]
pub struct ListExecutor;

impl ListExecutor {
    pub fn execute_binary_reduce<'a, S, B, Op>(
        array1: &'a Array,
        array2: &'a Array,
        builder: ArrayBuilder<B>,
        mut reduce_op: Op,
    ) -> Result<Array>
    where
        Op: FnMut(
            <S::Storage as AddressableStorage>::T,
            <S::Storage as AddressableStorage>::T,
            &mut OutputBuffer<B>,
        ),
        S: PhysicalStorage<'a>,
        B: ArrayDataBuffer,
    {
        let len = validate_logical_len(&builder.buffer, array1)?;
        let _ = validate_logical_len(&builder.buffer, array2)?;

        let selection1 = array1.selection_vector();
        let selection2 = array2.selection_vector();

        let validity1 = array1.validity();
        let validity2 = array2.validity();

        let mut output_buffer = OutputBuffer {
            idx: 0,
            buffer: builder.buffer,
        };

        if can_skip_validity_check([validity1, validity2]) {
        } else {
            // let mut out_validity = None;
            unimplemented!()
        }

        unimplemented!()
    }
}
