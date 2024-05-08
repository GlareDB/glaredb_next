pub mod arith;
pub mod comparison;
pub mod numeric;
pub mod string;

use dyn_clone::DynClone;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;

/// A function pointer with the concrete implementation of a scalar function.
pub type ScalarFn = fn(&[&Array]) -> Result<Array>;

#[derive(Debug, Clone)]
pub enum InputTypes {
    /// Exact number of inputs with the given types.
    Exact(&'static [DataType]),

    /// Variadic number of inputs with the same type.
    Variadic(DataType),
}

#[derive(Debug, Clone)]
pub struct Signature {
    pub input: InputTypes,
    pub return_type: DataType,
}

pub trait GenericScalarFunction: Debug + Sync + Send + DynClone {
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

impl Clone for Box<dyn GenericScalarFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

/// A specialized scalar function.
///
/// We're using a trait instead of returning the function pointer directly from
/// `GenericScalarFunction` because this will be what's serialized when
/// serializing pipelines for distributed execution.
pub trait SpecializedScalarFunction: Debug + Sync + Send + DynClone {
    /// Return the function pointer that implements this scalar function.
    fn function_impl(&self) -> ScalarFn;
}

impl Clone for Box<dyn SpecializedScalarFunction> {
    fn clone(&self) -> Self {
        dyn_clone::clone_box(&**self)
    }
}

pub(crate) fn specialize_check_num_args(
    scalar: &impl GenericScalarFunction,
    inputs: &[DataType],
    expected: usize,
) -> Result<()> {
    if inputs.len() != expected {
        return Err(RayexecError::new(format!(
            "Expected {} input for '{}', received {}",
            expected,
            scalar.name(),
            inputs.len(),
        )));
    }
    Ok(())
}

pub(crate) fn specialize_invalid_input_type(
    scalar: &impl GenericScalarFunction,
    got: &[&DataType],
) -> RayexecError {
    let got_types = got
        .iter()
        .map(|d| d.to_string())
        .collect::<Vec<_>>()
        .join(",");
    RayexecError::new(format!(
        "Got invalid type(s) '{}' for '{}'",
        got_types,
        scalar.name()
    ))
}
