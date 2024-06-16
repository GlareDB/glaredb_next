use num_traits::{NumCast, ToPrimitive};
use rayexec_bullet::{
    array::{Array, PrimitiveArray},
    bitmap::Bitmap,
    datatype::DataType,
    executor::aggregate::{AggregateState, StateCombiner, StateFinalizer, UnaryNonNullUpdater},
};

use super::{
    macros::generate_unary_primitive_aggregate, DefaultGroupedStates, GenericAggregateFunction,
    GroupedStates, SpecializedAggregateFunction,
};
use crate::functions::{
    invalid_input_types_error, specialize_check_num_args, FunctionInfo, Signature,
};
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::{marker::PhantomData, vec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Avg;

impl FunctionInfo for Avg {
    fn name(&self) -> &'static str {
        "avg"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: &[DataType::Float64],
                return_type: DataType::Float64,
            },
            Signature {
                input: &[DataType::Int64],
                return_type: DataType::Float64, // TODO: Should be decimal
            },
        ]
    }
}

impl GenericAggregateFunction for Avg {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedAggregateFunction>> {
        specialize_check_num_args(self, inputs, 1)?;
        match &inputs[0] {
            DataType::Int64 => Ok(Box::new(AvgI64)),
            DataType::Float64 => Ok(Box::new(AvgF64)),
            other => Err(invalid_input_types_error(self, &[other])),
        }
    }
}

generate_unary_primitive_aggregate!(AvgF64, Float64, Float64, AvgStateF64);
generate_unary_primitive_aggregate!(AvgI64, Int64, Float64, AvgStateI64);

type AvgStateF64 = AvgState<f64>;
type AvgStateI64 = AvgState<i64>;

#[derive(Debug, Default)]
struct AvgState<T> {
    sum: f64,
    count: i64,
    _type: PhantomData<T>,
}

impl<T: ToPrimitive + Default + Debug> AggregateState<T, f64> for AvgState<T> {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn update(&mut self, input: T) -> Result<()> {
        let input =
            <f64 as NumCast>::from(input).ok_or_else(|| RayexecError::new("failed cast"))?;
        self.sum += input;
        self.count += 1;
        Ok(())
    }

    fn finalize(self) -> Result<f64> {
        Ok(self.sum / self.count as f64)
    }
}
