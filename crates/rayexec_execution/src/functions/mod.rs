pub mod aggregate;
pub mod implicit;
pub mod scalar;
pub mod table;

use rayexec_bullet::datatype::DataType;
use rayexec_error::{RayexecError, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum InputTypes {
    /// Exact number of inputs with the given types.
    Exact(&'static [DataType]),

    /// Variadic number of inputs with the same type.
    Variadic(DataType),

    /// Input is not statically determined. Further checks need to be done.
    Dynamic,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ReturnType {
    /// Return type is statically known.
    Static(DataType),

    /// Return type depends entirely on the input, and we can't know ahead of
    /// time.
    ///
    /// This is typically used for compound types.
    Dynamic,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Signature {
    pub input: InputTypes,
    pub return_type: ReturnType,
}

impl Signature {
    /// Return if inputs given data types satisfy this signature.
    fn inputs_satisfy_signature(&self, inputs: &[DataType]) -> bool {
        match &self.input {
            InputTypes::Exact(expected) => {
                if expected.len() != inputs.len() {
                    return false;
                }
                for (expected, input) in expected.iter().zip(inputs.iter()) {
                    if !expected.eq_no_meta(input) {
                        return false;
                    }
                }
                true
            }
            InputTypes::Variadic(typ) => inputs.iter().all(|input| typ.eq_no_meta(input)),
            InputTypes::Dynamic => true,
        }
    }
}

/// Trait for defining informating about functions.
pub trait FunctionInfo {
    /// Name of the function.
    fn name(&self) -> &'static str;

    /// Aliases for the function.
    ///
    /// When the system catalog is initialized, the function will be placed into
    /// the catalog using both its name and all of its aliases.
    fn aliases(&self) -> &'static [&'static str] {
        &[]
    }

    /// Signature for the function.
    ///
    /// This is used during binding/planning to determine the return type for a
    /// function given some inputs, and how we should handle implicit casting.
    fn signatures(&self) -> &[Signature];

    /// Get the return type for this function.
    ///
    /// This is expected to be overridden by functions that return a dynamic
    /// type based on input. The default implementation can only determine the
    /// output if it can be statically determined.
    // TODO: Maybe remove
    fn return_type_for_inputs(&self, inputs: &[DataType]) -> Option<DataType> {
        let sig = self
            .signatures()
            .iter()
            .find(|sig| sig.inputs_satisfy_signature(inputs))?;

        match &sig.return_type {
            ReturnType::Static(datatype) => Some(datatype.clone()),
            ReturnType::Dynamic => None,
        }
    }
}

/// Check the number of arguments provided, erroring if it doesn't match the
/// expected number of arguments.
pub fn specialize_check_num_args(
    func: &impl FunctionInfo,
    inputs: &[DataType],
    expected: usize,
) -> Result<()> {
    if inputs.len() != expected {
        return Err(RayexecError::new(format!(
            "Expected {} input for '{}', received {}",
            expected,
            func.name(),
            inputs.len(),
        )));
    }
    Ok(())
}

/// Return an error indicating the input types we got are not ones we can
/// handle.
// TODO: Include valid signatures in the error
pub fn invalid_input_types_error(func: &impl FunctionInfo, got: &[&DataType]) -> RayexecError {
    let got_types = got
        .iter()
        .map(|d| d.to_string())
        .collect::<Vec<_>>()
        .join(",");
    RayexecError::new(format!(
        "Got invalid type(s) '{}' for '{}'",
        got_types,
        func.name()
    ))
}
