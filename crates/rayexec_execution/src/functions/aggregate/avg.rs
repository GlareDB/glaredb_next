use num_traits::{NumCast, ToPrimitive};
use rayexec_bullet::{
    array::{Array, PrimitiveArray},
    bitmap::Bitmap,
    executor::aggregate::{AggregateState, StateCombiner, StateFinalizer, UnaryNonNullUpdater},
    field::DataType,
};

use super::{
    DefaultGroupedStates, GenericAggregateFunction, GroupedStates, SpecializedAggregateFunction,
};
use crate::functions::{FunctionInfo, InputTypes, ReturnType, Signature};
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
        &[Signature {
            input: InputTypes::Exact(&[DataType::Int64]),
            return_type: ReturnType::Static(DataType::Int64), // TODO: Should be big num
        }]
    }
}

impl GenericAggregateFunction for Avg {
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedAggregateFunction>> {
        unimplemented!()
    }
}

#[derive(Debug, Default)]
pub struct AvgStateF64<T> {
    sum: f64,
    count: i64,
    _type: PhantomData<T>,
}

impl<T: ToPrimitive + Default + Debug> AggregateState<T, f64> for AvgStateF64<T> {
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
