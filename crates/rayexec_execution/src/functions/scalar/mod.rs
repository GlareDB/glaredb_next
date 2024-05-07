pub mod numeric;

use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::Result;
use std::fmt::Debug;

/// A function pointer with the concrete implementation of a scalar function.
pub type ScalarFn = fn(&[&Array]) -> Array;

#[derive(Debug, Clone)]
pub enum InputTypes {
    /// Fixed number of inputs with the given types.
    Fixed(&'static [DataType]),

    /// Variadic number of inputs with the same type.
    Variadic(DataType),
}

#[derive(Debug, Clone)]
pub struct Signature {
    pub input: InputTypes,
    pub return_type: DataType,
}

pub trait GenericScalarFunction: Debug + Sync + Send {
    /// Name of the function.
    fn name(&self) -> &str;

    /// Optional aliases for this function.
    fn aliases(&self) -> &[&str] {
        &[]
    }

    /// Signatures of the function.
    fn signatures(&self) -> &[Signature];

    /// Specialize the function using the given input types.
    fn specialize(&self, inputs: &[DataType]) -> Result<Box<dyn SpecializedScalarFunction>>;
}

/// A specialized scalar function.
///
/// We're using a trait instead of returning the function pointer directly from
/// `GenericScalarFunction` because this will be what's serialized when
/// serializing pipelines for distributed execution.
pub trait SpecializedScalarFunction: Debug + Sync + Send {
    /// Return the function pointer that implements this scalar function.
    fn function_impl(&self) -> ScalarFn;
}
