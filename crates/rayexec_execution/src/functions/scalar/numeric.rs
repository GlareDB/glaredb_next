use super::{GenericScalarFunction, InputTypes, ScalarFn, Signature, SpecializedScalarFunction};
use num_traits::Float;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::Result;
use std::fmt::Debug;
use std::marker::PhantomData;

#[derive(Debug, Clone, Copy)]
pub struct IsNan;

impl GenericScalarFunction for IsNan {
    fn name(&self) -> &str {
        "isnan"
    }

    fn signatures(&self) -> &[Signature] {
        &[
            Signature {
                input: InputTypes::Fixed(&[DataType::Float32]),
                return_type: DataType::Boolean,
            },
            Signature {
                input: InputTypes::Fixed(&[DataType::Float64]),
                return_type: DataType::Boolean,
            },
        ]
    }

    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IsNanSpecialized<F>(PhantomData<F>);

impl<F: Float + Sync + Send + Debug> SpecializedScalarFunction for IsNanSpecialized<F> {
    fn function_impl(&self) -> ScalarFn {
        unimplemented!()
    }
}
