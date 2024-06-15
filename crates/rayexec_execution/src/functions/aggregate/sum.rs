use super::{
    macros::{generate_unary_decimal_aggregate, generate_unary_primitive_aggregate},
    GenericAggregateFunction, SpecializedAggregateFunction,
};
use crate::functions::{
    invalid_input_types_error, specialize_check_num_args, FunctionInfo, InputTypes, ReturnType,
    Signature,
};
use rayexec_bullet::{
    array::{Decimal128Array, Decimal64Array},
    bitmap::Bitmap,
    executor::aggregate::AggregateState,
    field::DataType,
    scalar::decimal::{Decimal128Type, Decimal64Type, DecimalType, DECIMAL_DEFUALT_SCALE},
};
use rayexec_error::Result;
use std::{fmt::Debug, ops::AddAssign, vec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sum;

impl FunctionInfo for Sum {
    fn name(&self) -> &'static str {
        "sum"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Exact(&[DataType::Float64]),
                return_type: ReturnType::Static(DataType::Float64),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Int64]),
                return_type: ReturnType::Static(DataType::Int64), // TODO: Should be big num
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Decimal64(
                    Decimal64Type::MAX_PRECISION,
                    DECIMAL_DEFUALT_SCALE,
                )]),
                return_type: ReturnType::Static(DataType::Decimal64(
                    Decimal64Type::MAX_PRECISION,
                    DECIMAL_DEFUALT_SCALE,
                )),
            },
            Signature {
                input: InputTypes::Exact(&[DataType::Decimal128(
                    Decimal128Type::MAX_PRECISION,
                    DECIMAL_DEFUALT_SCALE,
                )]),
                return_type: ReturnType::Static(DataType::Decimal128(
                    Decimal128Type::MAX_PRECISION,
                    DECIMAL_DEFUALT_SCALE,
                )),
            },
        ]
    }
}

impl GenericAggregateFunction for Sum {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedAggregateFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int64 => Ok(Box::new(SumI64)),
            DataType::Float64 => Ok(Box::new(SumF64)),
            DataType::Decimal64(p, s) => Ok(Box::new(SumDecimal64 {
                precision: *p,
                scale: *s,
            })),
            DataType::Decimal128(p, s) => Ok(Box::new(SumDecimal128 {
                precision: *p,
                scale: *s,
            })),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

generate_unary_primitive_aggregate!(SumI64, Int64, Int64, SumState<i64>);
generate_unary_primitive_aggregate!(SumF64, Float64, Float64, SumState<f64>);

generate_unary_decimal_aggregate!(SumDecimal64, Decimal64, Decimal64Array, SumState<i64>);
generate_unary_decimal_aggregate!(SumDecimal128, Decimal128, Decimal128Array, SumState<i128>);

#[derive(Debug, Default)]
pub struct SumState<T> {
    sum: T,
}

impl<T: AddAssign + Default + Debug> AggregateState<T, T> for SumState<T> {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.sum += other.sum;
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        self.sum += input;
        Ok(())
    }

    fn finalize(self) -> Result<T> {
        Ok(self.sum)
    }
}

#[derive(Debug, Default)]
pub struct CovarSampFloat64 {
    count: usize,
    meanx: f64,
    meany: f64,
    co_moment: f64,
}

impl AggregateState<(f64, f64), f64> for CovarSampFloat64 {
    fn merge(&mut self, other: Self) -> Result<()> {
        let count = self.count + other.count;
        let meanx =
            (other.count as f64 * other.meanx + self.count as f64 * self.meanx) / count as f64;
        let meany =
            (other.count as f64 * other.meany + self.count as f64 * self.meany) / count as f64;

        let deltax = self.meanx - other.meanx;
        let deltay = self.meany - other.meany;

        self.co_moment = other.co_moment
            + self.co_moment
            + deltax * deltay * other.count as f64 * self.count as f64 / count as f64;
        self.meanx = meanx;
        self.meany = meany;
        self.count = count;

        Ok(())
    }

    fn update(&mut self, input: (f64, f64)) -> Result<()> {
        let x = input.1;
        let y = input.0;

        let n = self.count as f64;
        self.count += 1;

        let dx = x - self.meanx;
        let meanx = self.meanx + dx / n;

        let dy = y - self.meany;
        let meany = self.meany + dy / n;

        let co_moment = self.co_moment + dx * (y - meany);

        self.meanx = meanx;
        self.meany = meany;
        self.co_moment = co_moment;

        Ok(())
    }

    fn finalize(self) -> Result<f64> {
        Ok(self.co_moment / (self.count - 1) as f64)
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::array::{Array, Int64Array};

    use super::*;

    #[test]
    fn sum_i64_single_group_two_partitions() {
        // Single group, two partitions, 'SELECT SUM(a) FROM table'

        let partition_1_vals = &Array::Int64(Int64Array::from_iter([1, 2, 3]));
        let partition_2_vals = &Array::Int64(Int64Array::from_iter([4, 5, 6]));

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
            .update_states(&Bitmap::all_true(3), &[partition_1_vals], &mapping_1)
            .unwrap();
        states_2
            .update_states(&Bitmap::all_true(3), &[partition_2_vals], &mapping_2)
            .unwrap();

        // Combine states.
        //
        // Both partitions hold a single state (representing a single group),
        // and those states map to each other.
        let combine_mapping = vec![0];
        states_1.try_combine(states_2, &combine_mapping).unwrap();

        // Get final output.
        let out = states_1.drain_finalize_n(100).unwrap();
        let expected = Array::Int64(Int64Array::from_iter([21]));
        assert_eq!(expected, out.unwrap());
    }

    #[test]
    fn sum_i64_two_groups_two_partitions() {
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
        let partition_1_vals = &Array::Int64(Int64Array::from_iter([1, 2, 3]));
        let partition_2_vals = &Array::Int64(Int64Array::from_iter([4, 5, 6]));

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
            .update_states(&Bitmap::all_true(3), &[partition_1_vals], &mapping_1)
            .unwrap();
        states_2
            .update_states(&Bitmap::all_true(3), &[partition_2_vals], &mapping_2)
            .unwrap();

        // Combine states.
        //
        // The above `mapping_1` and `mapping_2` vectors indices that the state
        // for group 'a' is state 0 in each partition, and group 'b' is state 1
        // in each.
        //
        // The mapping here indicates the the 0th state for both partitions
        // should be combined, and the 1st state for both partitions should be
        // combined.
        let combine_mapping = vec![0, 1];
        states_1.try_combine(states_2, &combine_mapping).unwrap();

        // Get final output.
        let out = states_1.drain_finalize_n(100).unwrap();
        let expected = Array::Int64(Int64Array::from_iter([9, 12]));
        assert_eq!(expected, out.unwrap());
    }

    #[test]
    fn sum_i64_three_groups_two_partitions_with_unseen_group() {
        // Three groups, two partitions, 'SELECT SUM(col2) FROM table GROUP BY col1'
        //
        // This test represents a case where we're merging two aggregate hash
        // maps, where the map we're merging into has seen more groups than the
        // one that's being consumed. The implementation of the hash aggregate
        // operator ensures either this is the case, or that both hash maps have
        // seen the same number of groups.
        //
        // | col1 | col2 |
        // |------|------|
        // | 'x'  | 1    |
        // | 'x'  | 2    |
        // | 'y'  | 3    |
        // | 'z'  | 4    |
        // | 'x'  | 5    |
        // | 'z'  | 6    |
        // | 'z'  | 7    |
        // | 'z'  | 8    |
        //
        // Partition values and mappings represent the positions of the above
        // table. The actual grouping values are stored in the operator, and
        // operator is what computes the mappings.
        let partition_1_vals = &Array::Int64(Int64Array::from_iter([1, 2, 3, 4]));
        let partition_2_vals = &Array::Int64(Int64Array::from_iter([5, 6, 7, 8]));

        let specialized = Sum.specialize(&[DataType::Int64]).unwrap();

        let mut states_1 = specialized.new_grouped_state();
        let mut states_2 = specialized.new_grouped_state();

        // Partition 1 sees groups 'x', 'y', and 'z'.
        states_1.new_group();
        states_1.new_group();
        states_1.new_group();

        // Partition 2 see groups 'x' and 'z' (no 'y').
        states_2.new_group();
        states_2.new_group();

        // For partitions 1: 'x' == 0, 'y' == 1, 'z' == 2
        let mapping_1 = vec![0, 0, 1, 2];
        // For partitions 2: 'x' == 0, 'z' == 1
        let mapping_2 = vec![0, 1, 1, 1];

        states_1
            .update_states(&Bitmap::all_true(4), &[partition_1_vals], &mapping_1)
            .unwrap();
        states_2
            .update_states(&Bitmap::all_true(4), &[partition_2_vals], &mapping_2)
            .unwrap();

        // Combine states.
        //
        // States for 'x' both at the same position.
        //
        // States for 'y' at different positions, partition_2_state[1] => partition_1_state[2]
        let combine_mapping = vec![0, 2];
        states_1.try_combine(states_2, &combine_mapping).unwrap();

        // Get final output.
        let out = states_1.drain_finalize_n(100).unwrap();
        let expected = Array::Int64(Int64Array::from_iter([8, 3, 25]));
        assert_eq!(expected, out.unwrap());
    }

    #[test]
    fn sum_i64_drain_multiple() {
        // Three groups, single partition, test that drain can be called
        // multiple times until states are exhausted.
        let vals = &Array::Int64(Int64Array::from_iter([1, 2, 3, 4, 5, 6]));

        let specialized = Sum.specialize(&[DataType::Int64]).unwrap();
        let mut states = specialized.new_grouped_state();

        states.new_group();
        states.new_group();
        states.new_group();

        let mapping = vec![0, 0, 1, 1, 2, 2];
        states
            .update_states(&Bitmap::all_true(6), &[vals], &mapping)
            .unwrap();

        let expected_1 = Array::Int64(Int64Array::from_iter([3, 7]));
        let out_1 = states.drain_finalize_n(2).unwrap();
        assert_eq!(Some(expected_1), out_1);

        let expected_2 = Array::Int64(Int64Array::from_iter([11]));
        let out_2 = states.drain_finalize_n(2).unwrap();
        assert_eq!(Some(expected_2), out_2);

        let out_3 = states.drain_finalize_n(2).unwrap();
        assert_eq!(None, out_3);
    }
}
