use rayexec_bullet::{
    array::{Array, PrimitiveArrayBuilder},
    executor::aggregate::{AggregateState, StateCombiner, StateFinalizer, UnaryUpdater},
    field::DataType,
};

use super::{
    DefaultGroupedStates, GenericAggregateFunction, GroupedStates, SpecializedAggregateFunction,
};
use crate::functions::{InputTypes, ReturnType, Signature};
use rayexec_error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sum;

impl GenericAggregateFunction for Sum {
    fn name(&self) -> &str {
        "sum"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            input: InputTypes::Exact(&[DataType::Int64]),
            return_type: ReturnType::Static(DataType::Int64), // TODO: Should be big num
        }]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedAggregateFunction>> {
        match &inputs[0] {
            DataType::Int64 => Ok(Box::new(SumI64)),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SumI64;

impl SpecializedAggregateFunction for SumI64 {
    fn new_grouped_state(&self) -> Box<dyn GroupedStates> {
        let update_fn = |arrays: &[Array], mapping: &[usize], states: &mut [SumI64State]| {
            let inputs = match &arrays[0] {
                Array::Int64(arr) => arr,
                other => panic!("unexpected array type: {other:?}"),
            };
            UnaryUpdater::update(inputs, mapping, states)
        };

        let finalize_fn = |states: Vec<_>| {
            let mut builder = PrimitiveArrayBuilder::with_capacity(states.len());
            StateFinalizer::finalize(states, &mut builder)?;
            Ok(Array::Int64(builder.into_typed_array()))
        };

        Box::new(DefaultGroupedStates::new(
            update_fn,
            StateCombiner::combine,
            finalize_fn,
        ))
    }
}

#[derive(Debug, Default)]
pub struct SumI64State {
    sum: i64,
}

impl AggregateState<i64, i64> for SumI64State {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.sum += other.sum;
        Ok(())
    }

    fn update(&mut self, input: i64) -> Result<()> {
        self.sum += input;
        Ok(())
    }

    fn finalize(self) -> Result<i64> {
        Ok(self.sum)
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::Int64Array;

    use super::*;

    #[test]
    fn sum_i32_single_group_two_partitions() {
        // Single group, two partitions, 'SELECT SUM(a) FROM table'

        let partition_1_vals = Array::Int64(Int64Array::from_iter([1, 2, 3]));
        let partition_2_vals = Array::Int64(Int64Array::from_iter([4, 5, 6]));

        let specialized = Sum.specialize(&[DataType::Int64]).unwrap();

        let mut states_1 = specialized.new_grouped_state();
        let mut states_2 = specialized.new_grouped_state();

        let idx_1 = states_1.new_group();
        assert_eq!(0, idx_1);

        let idx_2 = states_2.new_group();
        assert_eq!(0, idx_2);

        // All inputs map to the same group (no GROUP BY clause)
        let mapping_1 = vec![0; partition_1_vals.len()];
        let mapping_2 = vec![0; partition_2_vals.len()];

        states_1
            .update_from_arrays(&[partition_1_vals], &mapping_1)
            .unwrap();
        states_2
            .update_from_arrays(&[partition_2_vals], &mapping_2)
            .unwrap();

        // Combine states.
        states_1.try_combine(states_2).unwrap();

        // Get final output.
        let out = states_1.finalize().unwrap();
        let expected = Array::Int64(Int64Array::from_iter([21]));
        assert_eq!(expected, out);
    }

    #[test]
    fn sum_i32_two_groups_two_partitions() {
        // Two groups, two partitions, 'SELECT SUM(col2) FROM table GROUP BY col1'
        //
        // | col1 | col2 |
        // |------|------|
        // | 'a'  | 1    |
        // | 'a'  | 2    |
        // | 'b'  | 3    |
        // | 'b'  | 4    |
        // | 'b'  | 5    |
        // | 'a'  | 6    |
        //
        // Partition values and mappings represent the positions of the above
        // table. The actual grouping values are stored in the operator, and
        // operator is what computes the mappings.
        let partition_1_vals = Array::Int64(Int64Array::from_iter([1, 2, 3]));
        let partition_2_vals = Array::Int64(Int64Array::from_iter([4, 5, 6]));

        let specialized = Sum.specialize(&[DataType::Int64]).unwrap();

        let mut states_1 = specialized.new_grouped_state();
        let mut states_2 = specialized.new_grouped_state();

        // Both partitions are operating on two groups ('a' and 'b').
        states_1.new_group();
        states_1.new_group();

        states_2.new_group();
        states_2.new_group();

        // Mapping corresponding to the above table. Group 'a' == 0 and group
        // 'b' == 1.
        let mapping_1 = vec![0, 0, 1];
        let mapping_2 = vec![1, 1, 0];

        states_1
            .update_from_arrays(&[partition_1_vals], &mapping_1)
            .unwrap();
        states_2
            .update_from_arrays(&[partition_2_vals], &mapping_2)
            .unwrap();

        // Combine states.
        states_1.try_combine(states_2).unwrap();

        // Get final output.
        let out = states_1.finalize().unwrap();
        let expected = Array::Int64(Int64Array::from_iter([9, 12]));
        assert_eq!(expected, out);
    }
}
