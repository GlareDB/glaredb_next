pub mod arith;
pub mod boolean;
pub mod comparison;
pub mod numeric;
pub mod string;

use dyn_clone::DynClone;
use once_cell::sync::Lazy;
use rayexec_bullet::{array::Array, field::DataType};
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;

// List of all scalar functions.
pub static ALL_SCALAR_FUNCTIONS: Lazy<Vec<Box<dyn GenericScalarFunction>>> = Lazy::new(|| {
    vec![
        // Arith
        Box::new(arith::Add),
        Box::new(arith::Sub),
        Box::new(arith::Mul),
        Box::new(arith::Div),
        Box::new(arith::Rem),
        // Boolean
        Box::new(boolean::And),
        Box::new(boolean::Or),
        // Comparison
        Box::new(comparison::Eq),
        Box::new(comparison::Neq),
        Box::new(comparison::Lt),
        Box::new(comparison::LtEq),
        Box::new(comparison::Gt),
        Box::new(comparison::GtEq),
        // Numeric
        Box::new(numeric::Ceil),
        Box::new(numeric::Floor),
        Box::new(numeric::IsNan),
        // String
        Box::new(string::Repeat),
    ]
});

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

/// A generic scalar function that can specialize into a more specific function
/// depending on input types.
///
/// Generic scalar functions must be cheaply cloneable.
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
    /// The return type of the function.
    fn return_type(&self) -> DataType;

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
